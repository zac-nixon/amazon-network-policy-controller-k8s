package app

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
)

type noopCRDManager struct{}

func (noopCRDManager) Start(context.Context) error { return nil }

func vpcCNIConfigMap(enabled bool) *corev1.ConfigMap {
	value := "false"
	if enabled {
		value = "true"
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "amazon-vpc-cni"},
		Data:       map[string]string{"enable-network-policy-controller": value},
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

// newDiscoveryServer answers the API discovery requests the manager makes to map the types it caches,
// and 404s everything else. The test needs no other API.
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

// The amazon-vpc-cni ConfigMap turns the controller on and off. When it changes, the controller
// must take on the new state without its health endpoint going away: the EKS health check starts
// probing the moment the ConfigMap enables the controller, so any gap here shows up as an
// unhealthy controller on a cluster that just turned network policy on.
func TestRun_HealthzStaysUpWhenConfigMapTogglesController(t *testing.T) {
	logOut := io.Discard
	if testing.Verbose() {
		logOut = os.Stderr
	}
	ctrl.SetLogger(zap.New(zap.WriteTo(logOut), zap.UseDevMode(true)))
	// The fake clientset does not send the bookmark that ends a watch-list stream, so the
	// ConfigMap reflector would never sync. Plain list-then-watch works against it.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)

	tests := []struct {
		name             string
		initiallyEnabled bool
	}{
		{name: "disabled to enabled", initiallyEnabled: false},
		{name: "enabled to disabled", initiallyEnabled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)

			clientSet := fake.NewClientset(vpcCNIConfigMap(tt.initiallyEnabled))
			watching := make(chan struct{})
			var once sync.Once
			clientSet.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, watch.Interface, error) {
				once.Do(func() { close(watching) })
				return false, nil, nil
			})

			scheme := runtime.NewScheme()
			g.Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
			g.Expect(policyinfo.AddToScheme(scheme)).To(Succeed())

			addr := freeAddr(t)
			cfg := config.ControllerConfig{
				EnableConfigMapCheck:    true,
				EndpointChunkSize:       1000,
				MaxConcurrentReconciles: 1,
				RuntimeConfig: config.RuntimeConfig{
					HealthProbeBindAddress: addr,
					MetricsBindAddress:     "0",
					WatchNamespace:         corev1.NamespaceAll,
				},
			}
			deps := Dependencies{
				// The manager only needs discovery from the API server; the ConfigMap comes from clientSet.
				RestConfig: &rest.Config{Host: newDiscoveryServer(t).URL},
				ClientSet:  clientSet,
				Scheme:     scheme,
				NewCRDManager: func(context.CancelFunc) crd.CRDManager {
					return noopCRDManager{}
				},
				// Stands in for the policy controller's own registration, which adds this check.
				SetupPolicyController: func(_ context.Context, mgr ctrl.Manager) error {
					return mgr.AddHealthzCheck("policy-controller", healthz.Ping)
				},
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runErr := make(chan error, 1)
			go func() { runErr <- Run(ctx, cfg, deps) }()

			healthzURL := "http://" + addr + "/healthz"
			policyControllerURL := healthzURL + "/policy-controller"
			readyzURL := "http://" + addr + "/readyz/readyz"
			stateStatus := func(enabled bool) int {
				if enabled {
					return http.StatusOK
				}
				return http.StatusNotFound
			}

			g.Eventually(func(g Gomega) {
				select {
				case err := <-runErr:
					t.Fatalf("Run returned before the controller started (err=%v)", err)
				default:
				}
				g.Expect(statusOf(healthzURL)).To(Equal(http.StatusOK))
			}).WithTimeout(10 * time.Second).WithPolling(20 * time.Millisecond).Should(Succeed())
			g.Eventually(watching).WithTimeout(10 * time.Second).Should(BeClosed())
			// Each manager registers a "readyz" check, so this answers once one is running.
			g.Eventually(func() int { return statusOf(readyzURL) }).
				WithTimeout(10 * time.Second).WithPolling(20 * time.Millisecond).Should(Equal(http.StatusOK))
			g.Expect(statusOf(policyControllerURL)).To(Equal(stateStatus(tt.initiallyEnabled)))

			// The fake leaves resourceVersion to the caller; the reflector needs one, as a real API
			// server would set.
			toggled := vpcCNIConfigMap(!tt.initiallyEnabled)
			toggled.ResourceVersion = "100"
			_, err := clientSet.CoreV1().ConfigMaps("kube-system").Update(ctx, toggled, metav1.UpdateOptions{})
			g.Expect(err).NotTo(HaveOccurred())

			// Poll until a manager in the new state is running, failing on the first probe that gets
			// no 200. The readyz check tells a running manager apart from the moment between two,
			// when no checks are registered and the disabled state's 404 would look the same.
			switched := func() bool {
				return statusOf(policyControllerURL) == stateStatus(!tt.initiallyEnabled) &&
					statusOf(readyzURL) == http.StatusOK &&
					statusOf(policyControllerURL) == stateStatus(!tt.initiallyEnabled)
			}
			deadline := time.Now().Add(15 * time.Second)
			for !switched() {
				select {
				case err := <-runErr:
					t.Fatalf("Run returned (err=%v) after the ConfigMap changed; the controller must switch state without exiting", err)
				default:
				}
				g.Expect(statusOf(healthzURL)).To(Equal(http.StatusOK),
					"healthz must keep answering while the controller switches state")
				g.Expect(time.Now().Before(deadline)).To(BeTrue(), "the controller never switched to the new state")
				time.Sleep(5 * time.Millisecond)
			}
			g.Expect(statusOf(healthzURL)).To(Equal(http.StatusOK))

			cancel()
			g.Eventually(runErr).WithTimeout(30 * time.Second).Should(Receive(BeNil()))
		})
	}
}
