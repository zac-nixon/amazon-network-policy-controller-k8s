package app

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	policyinfo "github.com/aws/amazon-network-policy-controller-k8s/api/v1alpha1"
	"github.com/aws/amazon-network-policy-controller-k8s/pkg/config"
)

// The real controller, CRD manager, leader election and metrics server, against an API server:
// switching the controller off and back on must leave it reconciling, and only one manager at a
// time. This is what the fakes in app_test.go cannot show, such as a second manager registering
// the policy controller under a name the process has already used.
func TestRun_RealControllerSwitchesOffAndOn(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run with make test")
	}
	env := &envtest.Environment{}
	restCfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	ctx := context.Background()
	clientSet := kubernetes.NewForConfigOrDie(restCfg)
	for _, ns := range []string{"kube-system", "app"} {
		if _, err := clientSet.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
			metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatal(err)
		}
	}
	if _, err := clientSet.CoreV1().ConfigMaps("kube-system").Create(ctx, vpcCNIConfigMap(true), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	setEnabled := func(enabled bool) {
		t.Helper()
		cm, err := clientSet.CoreV1().ConfigMaps("kube-system").Get(ctx, "amazon-vpc-cni", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		cm.Data = vpcCNIConfigMap(enabled).Data
		if _, err := clientSet.CoreV1().ConfigMaps("kube-system").Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := policyinfo.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	k8sClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	probeAddr, metricsAddr := freeAddr(t), freeAddr(t)
	cfg := config.ControllerConfig{
		EnableConfigMapCheck:         true,
		EndpointChunkSize:            200,
		MaxConcurrentReconciles:      3,
		PodUpdateBatchPeriodDuration: time.Second,
		RuntimeConfig: config.RuntimeConfig{
			HealthProbeBindAddress:  probeAddr,
			MetricsBindAddress:      metricsAddr,
			EnableLeaderElection:    true,
			LeaderElectionID:        "amazon-network-policy-controller-k8s-test",
			LeaderElectionNamespace: "kube-system",
			WatchNamespace:          corev1.NamespaceAll,
		},
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	returned := make(chan struct{})
	var runErr error
	go func() {
		defer close(returned)
		runErr = Run(runCtx, cfg, NewDependencies(cfg, restCfg, clientSet, scheme))
	}()

	url := func(path string) string { return "http://" + probeAddr + path }
	waitFor := func(what string, timeout time.Duration, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for !cond() {
			select {
			case <-returned:
				t.Fatalf("Run returned while waiting for %s: %v", what, runErr)
			default:
			}
			if s := statusOf(url("/healthz")); s != http.StatusOK {
				t.Fatalf("healthz answered %d while waiting for %s", s, what)
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitForManager := func(enabled bool) {
		t.Helper()
		want := http.StatusNotFound
		if enabled {
			want = http.StatusOK
		}
		waitFor("manager with enabled="+strconv.FormatBool(enabled), 2*time.Minute, func() bool {
			return statusOf(url("/healthz/policy-controller")) == want && statusOf(url("/readyz")) == http.StatusOK
		})
	}
	createPod := func(name, app, ip string) {
		t.Helper()
		pod, err := clientSet.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: name, Labels: map[string]string{"app": app}},
			Spec:       corev1.PodSpec{NodeName: "node-1", Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = ip
		pod.Status.PodIPs = []corev1.PodIP{{IP: ip}}
		if _, err := clientSet.CoreV1().Pods("app").UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	ingressAllows := func(ip string) func() bool {
		return func() bool {
			var pes policyinfo.PolicyEndpointList
			if err := k8sClient.List(ctx, &pes, client.InNamespace("app")); err != nil {
				return false
			}
			for _, pe := range pes.Items {
				for _, in := range pe.Spec.Ingress {
					if string(in.CIDR) == ip || string(in.CIDR) == ip+"/32" {
						return true
					}
				}
			}
			return false
		}
	}
	metricsHave := func(prefix string) bool {
		resp, err := http.Get("http://" + metricsAddr + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		body, _ := io.ReadAll(resp.Body)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, prefix) {
				return true
			}
		}
		return false
	}

	waitForManager(true)
	createPod("web", "web", "10.0.0.1")
	createPod("client-1", "client", "10.0.0.2")
	port := intstr.FromInt32(80)
	if err := k8sClient.Create(ctx, &networking.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "allow-client"},
		Spec: networking.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Ingress: []networking.NetworkPolicyIngressRule{{
				From:  []networking.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "client"}}}},
				Ports: []networking.NetworkPolicyPort{{Port: &port}},
			}},
			PolicyTypes: []networking.PolicyType{networking.PolicyTypeIngress},
		},
	}); err != nil {
		t.Fatal(err)
	}
	waitFor("the policy to allow client-1", time.Minute, ingressAllows("10.0.0.2"))
	waitFor("the policy count metric", time.Minute, func() bool { return metricsHave("network_policy_objects_total{") })

	setEnabled(false)
	waitForManager(false)
	waitFor("the metrics server", 30*time.Second, func() bool { return statusOf("http://"+metricsAddr+"/metrics") == http.StatusOK })
	if metricsHave("network_policy_objects_total{") {
		t.Error("the disabled controller still exports the policy count it had while enabled")
	}

	setEnabled(true)
	waitForManager(true)
	// The new manager must reconcile changes made after it started.
	createPod("client-2", "client", "10.0.0.3")
	waitFor("the policy to allow client-2", 2*time.Minute, ingressAllows("10.0.0.3"))

	cancel()
	select {
	case <-returned:
		if runErr != nil {
			t.Fatalf("Run returned an error on shutdown: %v", runErr)
		}
	case <-time.After(time.Minute):
		t.Fatal("Run did not return after its context was cancelled")
	}
}
