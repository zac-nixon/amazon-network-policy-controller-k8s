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

// Package app runs the network policy controller manager.
package app

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	"github.com/aws/amazon-network-policy-controller-k8s/internal/controllers"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/config"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/crd"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/k8s"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/metrics"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/policyendpoints"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/utils/configmap"
)

var setupLog = ctrl.Log.WithName("setup")

// Run's timing, in variables so tests can shorten it.
var (
	// maxTimeWithoutManager is how long /healthz stays healthy while no controller manager is
	// running: long enough to cover building a new one, short enough that a setup that never
	// finishes is reported unhealthy and the controller is restarted.
	maxTimeWithoutManager = 2 * time.Minute
	// Restarts that follow a manager that ran less than restartBackoffResetAfter wait, starting at
	// restartBackoffBase and doubling up to restartBackoffMax, so a ConfigMap flipping back and forth
	// does not rebuild the manager, and relist every watched object, back to back.
	restartBackoffBase       = time.Second
	restartBackoffMax        = time.Minute
	restartBackoffResetAfter = 5 * time.Minute
)

// Dependencies are the parts of Run that reach the cluster, kept separate so tests can replace them.
type Dependencies struct {
	RestConfig *rest.Config
	ClientSet  kubernetes.Interface
	Scheme     *runtime.Scheme
	// NewCRDManager builds the manager that installs and watches the CRDs. It calls cancelFn when
	// the CRDs change in a way that needs the controller to start over.
	NewCRDManager func(cancelFn context.CancelFunc) crd.CRDManager
	// SetupPolicyController registers the network policy controller with mgr.
	SetupPolicyController func(ctx context.Context, mgr ctrl.Manager) error
}

// NewDependencies returns the Dependencies that run against a real cluster.
func NewDependencies(controllerCFG config.ControllerConfig, restCFG *rest.Config, clientSet kubernetes.Interface,
	scheme *runtime.Scheme) Dependencies {
	return Dependencies{
		RestConfig: restCFG,
		ClientSet:  clientSet,
		Scheme:     scheme,
		NewCRDManager: func(cancelFn context.CancelFunc) crd.CRDManager {
			return crd.NewCRDManager(restCFG, setupLog.WithName("crd-manager"), cancelFn)
		},
		SetupPolicyController: func(ctx context.Context, mgr ctrl.Manager) error {
			policyEndpointsManager := policyendpoints.NewPolicyEndpointsManager(mgr.GetClient(),
				controllerCFG.EndpointChunkSize, ctrl.Log.WithName("endpoints-manager"))
			finalizerManager := k8s.NewDefaultFinalizerManager(mgr.GetClient(), ctrl.Log.WithName("finalizer-manager"))
			policyController := controllers.NewPolicyReconciler(mgr.GetClient(), policyEndpointsManager,
				controllerCFG, finalizerManager, ctrl.Log.WithName("controllers").WithName("policy"))
			return policyController.SetupWithManager(ctx, mgr)
		},
	}
}

// Run runs the controller until ctx is cancelled.
//
// The amazon-vpc-cni ConfigMap switches the controller on and off, and a deleted CRD needs the
// controller to start over. Run handles both by stopping the controller manager and building a new
// one in this process. It used to exit and rely on a container restart, but then the health probe
// endpoint was down until the new process was serving, and the EKS health check, which starts
// probing as soon as the ConfigMap turns the controller on, could land in that gap and report the
// controller unhealthy. The probe endpoint now belongs to Run and stays up across managers.
func Run(ctx context.Context, controllerCFG config.ControllerConfig, deps Dependencies) error {
	setupLog.Info("Checking args for enabling CM", "ConfigMapEnabled", controllerCFG.EnableConfigMapCheck)
	setupLog.Info("Checking args for PE chunk size", "PEChunkSize", controllerCFG.EndpointChunkSize)
	setupLog.Info("Checking args for policy batch time", "NPBatchTime", controllerCFG.PodUpdateBatchPeriodDuration)
	setupLog.Info("Checking args for reconciler count", "ReconcilerCount", controllerCFG.MaxConcurrentReconciles)

	probes := newProbeServer()
	stopProbes, err := probes.serve(controllerCFG.RuntimeConfig.HealthProbeBindAddress)
	if err != nil {
		return fmt.Errorf("unable to start health probe server: %w", err)
	}
	defer stopProbes()

	var backoff time.Duration
	for {
		started := time.Now()
		if err := runManager(ctx, controllerCFG, deps, probes); err != nil {
			return err
		}
		if ctx.Err() != nil {
			setupLog.Info("controller manager stopped")
			return nil
		}
		backoff = nextRestartBackoff(backoff, time.Since(started))
		setupLog.Info("Restarting controller manager", "after", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			setupLog.Info("controller manager stopped")
			return nil
		}
	}
}

// nextRestartBackoff returns how long to wait before building the next manager, given the last
// wait and how long the manager that just stopped ran.
func nextRestartBackoff(last, ran time.Duration) time.Duration {
	if ran >= restartBackoffResetAfter {
		return 0
	}
	return min(max(2*last, restartBackoffBase), restartBackoffMax)
}

// runManager runs one controller manager until ctx is cancelled or the controller needs to start
// over, which it signals by returning nil while ctx is still live.
func runManager(ctx context.Context, controllerCFG config.ControllerConfig, deps Dependencies, probes *probeServer) error {
	// restart ends this manager. The configmap and CRD managers call it when the controller has to
	// start over.
	ctx, restart := context.WithCancel(ctx)
	defer restart()

	// setupFailed passes on a setup step's error, unless the step failed because ctx was cancelled
	// under it. That is a ConfigMap change or CRD deletion asking for another manager, or shutdown,
	// and Run tells them apart. A step that fails on its own still stops the controller.
	setupFailed := func(err error) error {
		if ctx.Err() != nil {
			setupLog.Info("Controller manager setup interrupted", "err", err)
			return nil
		}
		return err
	}

	// The gauges counting policies belong to the manager that reconciles them, and a new process
	// used to start without any. Every manager before this one has stopped.
	metrics.ResetPolicyPopulation()

	enableNetworkPolicyController := true
	if controllerCFG.EnableConfigMapCheck {
		setupLog.Info("Enable network policy controller based on configuration", "configmap", configmap.GetControllerConfigMapId())
		configMapManager := config.NewConfigmapManager(configmap.GetControllerConfigMapId(),
			deps.ClientSet, restart, configmap.GetConfigmapCheckFn(), ctrl.Log.WithName("configmap-manager"))
		if err := configMapManager.MonitorConfigMap(ctx); err != nil {
			return setupFailed(fmt.Errorf("unable to monitor configmap for checking if controller is enabled: %w", err))
		}
		enableNetworkPolicyController = configMapManager.IsControllerEnabled()
		if !enableNetworkPolicyController {
			setupLog.Info("Disabling leader election since network policy controller is not enabled")
			controllerCFG.RuntimeConfig.EnableLeaderElection = false
		}
	}

	crdManager := deps.NewCRDManager(restart)
	if err := crdManager.Start(ctx); err != nil {
		return setupFailed(fmt.Errorf("unable to ensure CRDs exist: %w", err))
	}
	rtOpts := config.BuildRuntimeOptions(controllerCFG.RuntimeConfig, deps.Scheme)
	// Run serves the probes, so that they outlive this manager.
	rtOpts.HealthProbeBindAddress = "0"
	// Every manager in this process registers the policy controller under the same name, and
	// controller-runtime rejects a name it has already seen. Only one manager runs at a time, so the
	// metrics the name labels never come from two controllers at once.
	rtOpts.Controller.SkipNameValidation = ptr.To(true)

	ctrlMgr, err := ctrl.NewManager(deps.RestConfig, rtOpts)
	if err != nil {
		return fmt.Errorf("unable to create controller manager: %w", err)
	}
	mgr := newProbeCollectingManager(ctrlMgr)

	if enableNetworkPolicyController {
		setupLog.Info("Network Policy controller is enabled, starting watches")
		if err := deps.SetupPolicyController(ctx, mgr); err != nil {
			return setupFailed(fmt.Errorf("unable to setup network policy controller: %w", err))
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up health check: %w", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return fmt.Errorf("unable to set up ready check: %w", err)
	}

	setupLog.Info("starting controller manager")

	// Initialize metrics after manager cache is ready
	// only if network policy controller is enabled
	if enableNetworkPolicyController {
		initDone := make(chan struct{})
		// Wait for the counters to stop being written before the next manager resets them.
		defer func() {
			restart()
			<-initDone
		}()
		go func() {
			defer close(initDone)
			select {
			case <-mgr.Elected():
			case <-ctx.Done():
				return
			}
			mgr.GetCache().WaitForCacheSync(ctx)
			setupLog.Info("Initializing policy object counters...")
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := metrics.InitializePolicyObjectCounts(ctx, mgr.GetClient()); err != nil {
						continue
					}
					setupLog.Info("Initialized policy object counters")
					return
				}
			}
		}()
	}

	probes.managerRunning(mgr.checks())
	defer probes.managerStopped()
	if err := mgr.Start(ctx); err != nil {
		return fmt.Errorf("problem running controller manager: %w", err)
	}
	return nil
}
