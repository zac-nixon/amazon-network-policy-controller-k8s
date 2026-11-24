/*
Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"sync"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

// probeServer serves /healthz and /readyz for the life of the process. Run rebuilds the controller
// manager in place when the controller is switched on or off, and a probe endpoint owned by the
// manager would go away each time. The EKS health check starts probing the moment the ConfigMap
// turns the controller on, so it would see that gap as an unhealthy controller.
//
// It serves the checks of the manager that is running. While none is, before the first or between
// two, it serves a single check named noManagerCheck instead: /readyz fails, and /healthz passes
// until no manager has run for maxTimeWithoutManager.
type probeServer struct {
	mu      sync.RWMutex
	running bool
	// idleSince is when the last manager stopped, or when the server was made if none has run.
	idleSince time.Time
	healthz   map[string]healthz.Checker
	readyz    map[string]healthz.Checker
}

const noManagerCheck = "controller-manager"

var errNoManager = errors.New("no controller manager is running")

func newProbeServer() *probeServer {
	return &probeServer{idleSince: time.Now()}
}

// managerRunning serves a manager's checks. The maps are copied, so callers may reuse theirs.
func (p *probeServer) managerRunning(healthzChecks, readyzChecks map[string]healthz.Checker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = true
	p.healthz = maps.Clone(healthzChecks)
	p.readyz = maps.Clone(readyzChecks)
}

// managerStopped stops serving the manager's checks.
func (p *probeServer) managerStopped() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running = false
	p.idleSince = time.Now()
	p.healthz = nil
	p.readyz = nil
}

// healthzChecks returns the checks /healthz serves now. Like readyzChecks, it never returns a map
// that is written to later, so callers may read it without the lock.
func (p *probeServer) healthzChecks() map[string]healthz.Checker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.running {
		return p.healthz
	}
	idleSince := p.idleSince
	return map[string]healthz.Checker{noManagerCheck: func(*http.Request) error {
		if idle := time.Since(idleSince); idle > maxTimeWithoutManager {
			return fmt.Errorf("%w for %s", errNoManager, idle.Round(time.Second))
		}
		return nil
	}}
}

func (p *probeServer) readyzChecks() map[string]healthz.Checker {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.running {
		return p.readyz
	}
	return map[string]healthz.Checker{noManagerCheck: func(*http.Request) error { return errNoManager }}
}

// handler routes the endpoints the way the controller-runtime manager does, including a subpath
// per check, such as /healthz/policy-controller.
func (p *probeServer) handler() http.Handler {
	mux := http.NewServeMux()
	for endpoint, checks := range map[string]func() map[string]healthz.Checker{
		"/healthz": p.healthzChecks,
		"/readyz":  p.readyzChecks,
	} {
		h := http.StripPrefix(endpoint, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			(&healthz.Handler{Checks: checks()}).ServeHTTP(w, r)
		}))
		mux.Handle(endpoint, h)
		mux.Handle(endpoint+"/", h)
	}
	return mux
}

// serve listens on addr and serves the probes until stop is called. An addr of "" or "0" turns the
// probes off, as it does for the controller-runtime manager.
func (p *probeServer) serve(addr string) (stop func(), err error) {
	if addr == "" || addr == "0" {
		return func() {}, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("error listening on %s: %w", addr, err)
	}
	// The same limits the controller-runtime manager puts on its probe server.
	srv := &http.Server{
		Handler:           p.handler(),
		MaxHeaderBytes:    1 << 20,
		IdleTimeout:       90 * time.Second,
		ReadHeaderTimeout: 32 * time.Second,
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			setupLog.Error(err, "health probe server stopped")
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-served
	}, nil
}

// probeCollectingManager is the manager given to setup code. It keeps the health and readiness
// checks that setup registers so Run can serve them from its probe server, which outlives the manager.
type probeCollectingManager struct {
	ctrl.Manager
	mu        sync.Mutex
	healthz   map[string]healthz.Checker
	readyz    map[string]healthz.Checker
	published bool
}

// errChecksPublished matches the manager's own refusal to take checks once it serves them.
var errChecksPublished = errors.New("unable to add new checker because healthz endpoint has already been created")

func newProbeCollectingManager(mgr ctrl.Manager) *probeCollectingManager {
	return &probeCollectingManager{
		Manager: mgr,
		healthz: map[string]healthz.Checker{},
		readyz:  map[string]healthz.Checker{},
	}
}

func (m *probeCollectingManager) AddHealthzCheck(name string, check healthz.Checker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.published {
		return errChecksPublished
	}
	m.healthz[name] = check
	return nil
}

func (m *probeCollectingManager) AddReadyzCheck(name string, check healthz.Checker) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.published {
		return errChecksPublished
	}
	m.readyz[name] = check
	return nil
}

// checks returns the registered checks for serving. No more can be added after it is called.
func (m *probeCollectingManager) checks() (healthzChecks, readyzChecks map[string]healthz.Checker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = true
	return maps.Clone(m.healthz), maps.Clone(m.readyz)
}
