package k8s

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Changes must not block on a listener that has stopped reading: the reflector writing them would
// hang, holding its watch open, after its configmap manager is gone.
func TestConfigMapStore_NotifiesWithoutBlockingOnAStoppedListener(t *testing.T) {
	g := NewWithT(t)
	notify := make(chan struct{}, 1)
	store := NewConfigMapStore(notify)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "amazon-vpc-cni"}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		g.Expect(store.Add(cm)).To(Succeed())
		g.Expect(store.Update(cm.DeepCopy())).To(Succeed())
		g.Expect(store.Delete(cm)).To(Succeed())
	}()
	g.Eventually(done).WithTimeout(time.Second).Should(BeClosed())

	// The notifications coalesce into one, so a listener that comes back still rereads the store.
	g.Expect(notify).To(Receive())
	g.Expect(notify).NotTo(Receive())
}
