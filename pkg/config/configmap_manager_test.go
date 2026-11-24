package config

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// A change that lands while the manager is still reading the ConfigMap's starting state must not
// be compared against a state not yet known. Either the change is the starting state, or it
// cancels the manager context so the controller restarts in the new state.
func TestConfigmapManager_ChangeDuringInitialSync(t *testing.T) {
	g := NewWithT(t)
	// The fake clientset does not send the bookmark that ends a watch-list stream.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)

	ref := types.NamespacedName{Namespace: "kube-system", Name: "amazon-vpc-cni"}
	cm := func(enabled string) *corev1.ConfigMap {
		return &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name},
			Data:       map[string]string{"enable-network-policy-controller": enabled},
		}
	}
	clientSet := fake.NewClientset(cm("false"))
	// Enable the controller as soon as the reflector starts watching, before the starting state
	// has been read.
	var once sync.Once
	clientSet.PrependWatchReactor("configmaps", func(k8stesting.Action) (bool, watch.Interface, error) {
		once.Do(func() {
			go func() {
				// The fake leaves resourceVersion to the caller; the reflector needs one, as a real
				// API server would set.
				enabled := cm("true")
				enabled.ResourceVersion = "100"
				_, _ = clientSet.CoreV1().ConfigMaps(ref.Namespace).Update(context.Background(), enabled, metav1.UpdateOptions{})
			}()
		})
		return false, nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarted := make(chan struct{})
	var restartOnce sync.Once
	m := NewConfigmapManager(ref, clientSet, func() { restartOnce.Do(func() { close(restarted) }) },
		func(cm *corev1.ConfigMap) bool { return cm.Data["enable-network-policy-controller"] == "true" },
		logr.Discard())

	g.Expect(m.MonitorConfigMap(ctx)).To(Succeed())
	if !m.IsControllerEnabled() {
		g.Eventually(restarted).WithTimeout(5*time.Second).Should(BeClosed(),
			"the controller started disabled, so enabling it must restart it")
	}
}

// The initial list notifies like any change. Without a change to the ConfigMap that must not
// cancel the manager context, or the controller would restart as soon as it started.
func TestConfigmapManager_NoRestartWithoutChange(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ref := types.NamespacedName{Namespace: "kube-system", Name: "amazon-vpc-cni"}
	for _, enabled := range []string{"true", "false"} {
		t.Run("enabled="+enabled, func(t *testing.T) {
			g := NewWithT(t)
			clientSet := fake.NewClientset(&corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name},
				Data:       map[string]string{"enable-network-policy-controller": enabled},
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			restarted := make(chan struct{})
			var restartOnce sync.Once
			m := NewConfigmapManager(ref, clientSet, func() { restartOnce.Do(func() { close(restarted) }) },
				func(cm *corev1.ConfigMap) bool { return cm.Data["enable-network-policy-controller"] == "true" },
				logr.Discard())

			g.Expect(m.MonitorConfigMap(ctx)).To(Succeed())
			g.Expect(m.IsControllerEnabled()).To(Equal(enabled == "true"))
			g.Consistently(restarted).WithTimeout(500 * time.Millisecond).ShouldNot(BeClosed())
		})
	}
}
