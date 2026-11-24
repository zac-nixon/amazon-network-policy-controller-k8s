package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	policyinfo "github.com/aws/amazon-network-policy-controller-k8s/api/v1alpha1"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/config"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/crd"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/metrics"
)

func TestMain(m *testing.M) {
	logOut := io.Discard
	for _, arg := range os.Args {
		if arg == "-test.v=true" {
			logOut = os.Stderr
		}
	}
	ctrl.SetLogger(zap.New(zap.WriteTo(logOut), zap.UseDevMode(true)))
	os.Exit(m.Run())
}

func vpcCNIConfigMap(enabled bool) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "amazon-vpc-cni"},
		Data:       map[string]string{"enable-network-policy-controller": strconv.FormatBool(enabled)},
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving a port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

// statusOf returns the HTTP status of url, or -1 when nothing answered.
func statusOf(url string) int {
	client := http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// newDiscoveryServer answers the API discovery requests the manager makes to map the types it caches,
// and 404s everything else. The tests need no other API.
func newDiscoveryServer(t *testing.T) *httptest.Server {
	t.Helper()
	resources := func(gv string, rs ...metav1.APIResource) metav1.APIResourceList {
		for i := range rs {
			rs[i].Verbs = metav1.Verbs{"get", "list", "watch"}
		}
		return metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
			GroupVersion: gv, APIResources: rs}
	}
	group := func(name, version string) metav1.APIGroup {
		gv := metav1.GroupVersionForDiscovery{GroupVersion: name + "/" + version, Version: version}
		return metav1.APIGroup{Name: name, Versions: []metav1.GroupVersionForDiscovery{gv}, PreferredVersion: gv}
	}
	docs := map[string]any{
		"/api": metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}},
		"/apis": metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
			Groups: []metav1.APIGroup{group("networking.k8s.io", "v1"), group("networking.k8s.aws", "v1alpha1")}},
		"/api/v1": resources("v1",
			metav1.APIResource{Name: "pods", Kind: "Pod", Namespaced: true},
			metav1.APIResource{Name: "services", Kind: "Service", Namespaced: true},
			metav1.APIResource{Name: "namespaces", Kind: "Namespace"},
			metav1.APIResource{Name: "configmaps", Kind: "ConfigMap", Namespaced: true}),
		"/apis/networking.k8s.io/v1": resources("networking.k8s.io/v1",
			metav1.APIResource{Name: "networkpolicies", Kind: "NetworkPolicy", Namespaced: true}),
		"/apis/networking.k8s.aws/v1alpha1": resources("networking.k8s.aws/v1alpha1",
			metav1.APIResource{Name: "policyendpoints", Kind: "PolicyEndpoint", Namespaced: true},
			metav1.APIResource{Name: "applicationnetworkpolicies", Kind: "ApplicationNetworkPolicy", Namespaced: true},
			metav1.APIResource{Name: "clusternetworkpolicies", Kind: "ClusterNetworkPolicy"},
			metav1.APIResource{Name: "clusterpolicyendpoints", Kind: "ClusterPolicyEndpoint"}),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		doc, ok := docs[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(srv.Close)
	return srv
}

type fakeCRDManager struct {
	start func(ctx context.Context) error
}

func (m fakeCRDManager) Start(ctx context.Context) error { return m.start(ctx) }

type harnessOptions struct {
	enabled bool
	// crdStart stands in for the CRD manager's Start in the given run, counting from 1. By default
	// the CRDs are ready at once.
	crdStart func(ctx context.Context, run int) error
	// setup stands in for the policy controller's setup. By default it registers the controller's
	// own health check, which is what the probe paths depend on.
	setup func(ctx context.Context, mgr ctrl.Manager) error
}

// harness runs Run against a fake clientset that holds the amazon-vpc-cni ConfigMap and a stub
// API server that only serves discovery. Nothing else reaches a cluster.
type harness struct {
	t         *testing.T
	clientSet *fake.Clientset
	addr      string
	cancel    context.CancelFunc
	returned  chan struct{} // closed when Run returns, with its result in err
	err       error
	rv        int
	watches   atomic.Int32
	mu        sync.Mutex
	runStarts []time.Time
}

func startRun(t *testing.T, o harnessOptions) *harness {
	t.Helper()
	// The fake clientset does not send the bookmark that ends a watch-list stream, so the
	// ConfigMap reflector would never sync. Plain list-then-watch works against it.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)

	h := &harness{t: t, clientSet: fake.NewClientset(vpcCNIConfigMap(o.enabled)), addr: freeAddr(t),
		returned: make(chan struct{}), rv: 100}
	h.clientSet.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, watch.Interface, error) {
		h.watches.Add(1)
		return false, nil, nil
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := policyinfo.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cfg := config.ControllerConfig{
		EnableConfigMapCheck:    true,
		EndpointChunkSize:       1000,
		MaxConcurrentReconciles: 1,
		RuntimeConfig: config.RuntimeConfig{
			HealthProbeBindAddress: h.addr,
			MetricsBindAddress:     "0",
			WatchNamespace:         corev1.NamespaceAll,
		},
	}
	setup := o.setup
	if setup == nil {
		setup = func(_ context.Context, mgr ctrl.Manager) error {
			return mgr.AddHealthzCheck("policy-controller", healthz.Ping)
		}
	}
	deps := Dependencies{
		RestConfig: &rest.Config{Host: newDiscoveryServer(t).URL},
		ClientSet:  h.clientSet,
		Scheme:     scheme,
		NewCRDManager: func(context.CancelFunc) crd.CRDManager {
			h.mu.Lock()
			h.runStarts = append(h.runStarts, time.Now())
			run := len(h.runStarts)
			h.mu.Unlock()
			return fakeCRDManager{start: func(ctx context.Context) error {
				if o.crdStart == nil {
					return nil
				}
				return o.crdStart(ctx, run)
			}}
		},
		SetupPolicyController: setup,
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		defer close(h.returned)
		h.err = Run(ctx, cfg, deps)
	}()
	t.Cleanup(func() { _ = h.stop() })
	return h
}

// stop cancels Run's context and returns what Run returned.
func (h *harness) stop() error {
	h.cancel()
	select {
	case <-h.returned:
		return h.err
	case <-time.After(30 * time.Second):
		h.t.Fatal("Run did not return after its context was cancelled")
		return nil
	}
}

func (h *harness) url(path string) string { return "http://" + h.addr + path }

// runs returns how many managers Run has begun to build.
func (h *harness) runs() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.runStarts)
}

func (h *harness) runStart(run int) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.runStarts[run-1]
}

// failIfRunReturned fails the test if Run has returned, which it must only do on cancellation.
func (h *harness) failIfRunReturned() {
	h.t.Helper()
	select {
	case <-h.returned:
		h.t.Fatalf("Run returned (err=%v); it must keep running until its context is cancelled", h.err)
	default:
	}
}

// waitFor polls cond until it holds, failing if Run returns first or check fails along the way.
func (h *harness) waitFor(what string, timeout time.Duration, cond func() bool, check func()) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		h.failIfRunReturned()
		if check != nil {
			check()
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForManager waits until the n-th manager is running in the given state and its configmap
// manager is watching for changes. The readyz check tells a running manager apart from the moment
// between two, when no checks are registered and the disabled state's 404 would look the same.
func (h *harness) waitForManager(run int, enabled bool, check func()) {
	h.t.Helper()
	want := http.StatusNotFound
	if enabled {
		want = http.StatusOK
	}
	h.waitFor("manager "+strconv.Itoa(run), 15*time.Second, func() bool {
		return h.runs() == run && int(h.watches.Load()) >= run &&
			statusOf(h.url("/healthz/policy-controller")) == want &&
			statusOf(h.url("/readyz/readyz")) == http.StatusOK &&
			statusOf(h.url("/healthz/policy-controller")) == want
	}, check)
}

func (h *harness) setEnabled(enabled bool) {
	h.t.Helper()
	cm := vpcCNIConfigMap(enabled)
	// The fake leaves resourceVersion to the caller; the reflector needs one, as a real API server
	// would set.
	h.rv++
	cm.ResourceVersion = strconv.Itoa(h.rv)
	if _, err := h.clientSet.CoreV1().ConfigMaps("kube-system").Update(context.Background(), cm, metav1.UpdateOptions{}); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) healthzStaysUp() func() {
	return func() {
		h.t.Helper()
		if s := statusOf(h.url("/healthz")); s != http.StatusOK {
			h.t.Fatalf("healthz answered %d; it must keep answering 200 while the controller switches state", s)
		}
	}
}

func gaugeValue(g prometheus.Gauge) float64 {
	m := &dto.Metric{}
	_ = g.Write(m)
	return m.GetGauge().GetValue()
}

func seriesCount(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 64)
	c.Collect(ch)
	close(ch)
	return len(ch)
}

// setTiming overrides one of Run's timing variables for the test.
func setTiming(t *testing.T, v *time.Duration, d time.Duration) {
	old := *v
	*v = d
	t.Cleanup(func() { *v = old })
}

// The amazon-vpc-cni ConfigMap turns the controller on and off. When it changes, the controller
// must take on the new state without its health endpoint going away: the EKS health check starts
// probing the moment the ConfigMap enables the controller, so any gap here shows up as an
// unhealthy controller on a cluster that just turned network policy on.
func TestRun_HealthzStaysUpWhenConfigMapTogglesController(t *testing.T) {
	for _, initiallyEnabled := range []bool{false, true} {
		t.Run("from enabled="+strconv.FormatBool(initiallyEnabled), func(t *testing.T) {
			h := startRun(t, harnessOptions{enabled: initiallyEnabled})
			h.waitForManager(1, initiallyEnabled, nil)
			h.setEnabled(!initiallyEnabled)
			h.waitForManager(2, !initiallyEnabled, h.healthzStaysUp())
		})
	}
}

// A ConfigMap change can land while a new manager is still being set up. The step it interrupts
// fails because its context was cancelled, which is a request for another manager, not a reason
// to exit.
func TestRun_ToggleDuringSetupStartsAnotherManager(t *testing.T) {
	inCRDStep := make(chan struct{})
	h := startRun(t, harnessOptions{crdStart: func(ctx context.Context, run int) error {
		if run > 1 {
			return nil
		}
		close(inCRDStep)
		<-ctx.Done()
		// What the real CRD manager reports when its wait for the CRDs is cut short.
		return errors.New("timeout waiting for CRDs to be established")
	}})
	h.waitFor("the first manager's CRD step", 15*time.Second, func() bool {
		select {
		case <-inCRDStep:
			return h.watches.Load() >= 1
		default:
			return false
		}
	}, nil)

	h.setEnabled(true)
	h.waitForManager(2, true, h.healthzStaysUp())
}

// Shutting down while a manager is being set up is a clean stop, not a failure.
func TestRun_ShutdownDuringSetupIsClean(t *testing.T) {
	inCRDStep := make(chan struct{})
	h := startRun(t, harnessOptions{crdStart: func(ctx context.Context, run int) error {
		close(inCRDStep)
		<-ctx.Done()
		return errors.New("timeout waiting for CRDs to be established")
	}})
	g := NewWithT(t)
	g.Eventually(inCRDStep).WithTimeout(15 * time.Second).Should(BeClosed())

	g.Expect(h.stop()).To(Succeed())
}

// Readiness means a manager has finished setup, so /readyz must fail until one is running.
func TestRun_NotReadyWhileNoManagerIsRunning(t *testing.T) {
	release := make(chan struct{})
	h := startRun(t, harnessOptions{crdStart: func(ctx context.Context, run int) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}})
	g := NewWithT(t)
	g.Eventually(func() int { return statusOf(h.url("/healthz")) }).WithTimeout(15 * time.Second).
		Should(Equal(http.StatusOK))
	g.Consistently(func() int { return statusOf(h.url("/readyz")) }).WithTimeout(300 * time.Millisecond).
		Should(Equal(http.StatusInternalServerError))

	close(release)
	h.waitForManager(1, false, nil)
	g.Expect(statusOf(h.url("/readyz"))).To(Equal(http.StatusOK))
}

// /healthz covers the short gap between managers, but a setup that never finishes, for example
// because the ConfigMap cannot be read, must still be reported unhealthy so the controller is
// restarted, as it was when nothing listened on the port until a manager was serving.
func TestRun_UnhealthyWhenNoManagerRunsForTooLong(t *testing.T) {
	setTiming(t, &maxTimeWithoutManager, 300*time.Millisecond)
	h := startRun(t, harnessOptions{crdStart: func(ctx context.Context, run int) error {
		<-ctx.Done()
		return ctx.Err()
	}})
	g := NewWithT(t)
	g.Eventually(func() int { return statusOf(h.url("/healthz")) }).WithTimeout(15 * time.Second).
		Should(Equal(http.StatusOK))
	g.Eventually(func() int { return statusOf(h.url("/healthz")) }).WithTimeout(5 * time.Second).
		Should(Equal(http.StatusInternalServerError))
}

// The policy population gauges are process globals that a new process used to start without. A
// manager must not inherit the previous one's: after the controller is switched off, or a replica
// loses the lease to another, it would keep exporting a population it no longer tracks.
func TestRun_ResetsPolicyPopulationMetricsBetweenManagers(t *testing.T) {
	split := types.NamespacedName{Namespace: "app", Name: "split"}
	h := startRun(t, harnessOptions{enabled: true, setup: func(_ context.Context, mgr ctrl.Manager) error {
		// What a running controller records about the policies it reconciles.
		metrics.SetPolicyObjectCount("NetworkPolicy", 3)
		metrics.SetPolicyObjectCount("ApplicationNetworkPolicy", 1)
		metrics.UpdateAdvancedNetworkPolicyEnabled()
		metrics.SetPolicyTargetGroups(split, 2)
		return mgr.AddHealthzCheck("policy-controller", healthz.Ping)
	}})
	h.waitForManager(1, true, nil)
	g := NewWithT(t)
	g.Expect(gaugeValue(metrics.SplitPolicyCount)).To(Equal(1.0))

	h.setEnabled(false)
	h.waitForManager(2, false, nil)
	g.Expect(seriesCount(metrics.PolicyObjectCount)).To(BeZero(), "network_policy_objects_total")
	g.Expect(gaugeValue(metrics.AdvancedNetworkPolicyEnabled)).To(BeZero(), "advanced_network_policy_enabled")
	g.Expect(gaugeValue(metrics.SplitPolicyCount)).To(BeZero(), "network_policy_split_policies_total")
	g.Expect(gaugeValue(metrics.MaxPolicyTargetGroups)).To(BeZero(), "network_policy_target_groups_max")

	// The split-policy gauges are computed from a map that must have been cleared too.
	metrics.SetPolicyTargetGroups(types.NamespacedName{Namespace: "app", Name: "other"}, 3)
	g.Expect(gaugeValue(metrics.SplitPolicyCount)).To(Equal(1.0))
	metrics.ForgetPolicyTargetGroups(types.NamespacedName{Namespace: "app", Name: "other"})
}

// A ConfigMap that keeps flipping, for example because two systems manage it, must not rebuild the
// manager back to back: each rebuild relists every watched object. The container restart this
// replaces was throttled by its restart backoff.
func TestRun_BacksOffRapidRestarts(t *testing.T) {
	setTiming(t, &restartBackoffBase, 300*time.Millisecond)
	setTiming(t, &restartBackoffMax, 600*time.Millisecond)
	setTiming(t, &restartBackoffResetAfter, time.Minute)
	h := startRun(t, harnessOptions{})
	g := NewWithT(t)
	h.waitForManager(1, false, nil)

	enabled := false
	for _, step := range []struct {
		run       int
		wantDelay time.Duration
	}{{2, 300 * time.Millisecond}, {3, 600 * time.Millisecond}, {4, 600 * time.Millisecond}} {
		enabled = !enabled
		toggled := time.Now()
		h.setEnabled(enabled)
		h.waitForManager(step.run, enabled, h.healthzStaysUp())
		g.Expect(h.runStart(step.run).Sub(toggled)).To(BeNumerically(">=", step.wantDelay),
			"manager %d started too soon after the ConfigMap changed", step.run)
	}
}

// A manager that ran a while is not part of a flapping ConfigMap, so the next one starts at once.
func TestRun_RestartsAtOnceAfterAStableManager(t *testing.T) {
	setTiming(t, &restartBackoffBase, time.Second)
	setTiming(t, &restartBackoffResetAfter, 0)
	h := startRun(t, harnessOptions{})
	h.waitForManager(1, false, nil)

	toggled := time.Now()
	h.setEnabled(true)
	h.waitForManager(2, true, h.healthzStaysUp())
	NewWithT(t).Expect(h.runStart(2).Sub(toggled)).To(BeNumerically("<", time.Second))
}
