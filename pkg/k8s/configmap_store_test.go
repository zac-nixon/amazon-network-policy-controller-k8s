package k8s

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The reflector writes changes into the store before anyone may be reading notifications, as
// during the initial sync, so notifying must not block it. Changes coalesce into one pending
// notification, which is enough: the listener rereads the store when it wakes.
func TestConfigMapStore_NotifiesWithoutBlocking(t *testing.T) {
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

	g.Expect(notify).To(Receive())
	g.Expect(notify).NotTo(Receive())
}

// A reflector that relists, after its watch expires, hands the store the whole list through
// Replace. A toggle made while it was not watching only shows up there, so Replace must notify.
func TestConfigMapStore_ReplaceNotifies(t *testing.T) {
	g := NewWithT(t)
	notify := make(chan struct{}, 1)
	store := NewConfigMapStore(notify)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "amazon-vpc-cni"}}

	g.Expect(store.Replace([]interface{}{cm}, "2")).To(Succeed())
	g.Expect(notify).To(Receive())
}
