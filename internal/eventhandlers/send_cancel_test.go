package eventhandlers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	policyinfo "github.com/aws/amazon-network-policy-controller-k8s/api/v1alpha1"
)

// referringResolver reports one NetworkPolicy, ANP and CNP referring to every pod, namespace and service.
type referringResolver struct{}

func (referringResolver) nps() []networking.NetworkPolicy {
	return []networking.NetworkPolicy{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "np"}}}
}
func (referringResolver) anps() []policyinfo.ApplicationNetworkPolicy {
	return []policyinfo.ApplicationNetworkPolicy{{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "anp"}}}
}
func (referringResolver) cnps() []policyinfo.ClusterNetworkPolicy {
	return []policyinfo.ClusterNetworkPolicy{{ObjectMeta: metav1.ObjectMeta{Name: "cnp"}}}
}
func (r referringResolver) GetReferredPoliciesForPod(context.Context, *corev1.Pod, *corev1.Pod) ([]networking.NetworkPolicy, error) {
	return r.nps(), nil
}
func (r referringResolver) GetReferredPoliciesForNamespace(context.Context, *corev1.Namespace, *corev1.Namespace) ([]networking.NetworkPolicy, error) {
	return r.nps(), nil
}
func (r referringResolver) GetReferredPoliciesForService(context.Context, *corev1.Service, *corev1.Service) ([]networking.NetworkPolicy, error) {
	return r.nps(), nil
}
func (r referringResolver) GetReferredApplicationNetworkPoliciesForPod(context.Context, *corev1.Pod, *corev1.Pod) ([]policyinfo.ApplicationNetworkPolicy, error) {
	return r.anps(), nil
}
func (r referringResolver) GetReferredApplicationNetworkPoliciesForNamespace(context.Context, *corev1.Namespace, *corev1.Namespace) ([]policyinfo.ApplicationNetworkPolicy, error) {
	return r.anps(), nil
}
func (r referringResolver) GetReferredApplicationNetworkPoliciesForService(context.Context, *corev1.Service, *corev1.Service) ([]policyinfo.ApplicationNetworkPolicy, error) {
	return r.anps(), nil
}
func (r referringResolver) GetReferredClusterPoliciesForPod(context.Context, *corev1.Pod, *corev1.Pod) ([]policyinfo.ClusterNetworkPolicy, error) {
	return r.cnps(), nil
}
func (r referringResolver) GetReferredClusterPoliciesForNamespace(context.Context, *corev1.Namespace, *corev1.Namespace) ([]policyinfo.ClusterNetworkPolicy, error) {
	return r.cnps(), nil
}
func (r referringResolver) GetReferredClusterPoliciesForService(context.Context, *corev1.Service, *corev1.Service) ([]policyinfo.ClusterNetworkPolicy, error) {
	return r.cnps(), nil
}

// The policy event channel's reader stops with the controller. A handler still sending to it
// must give up when its context ends, or the informer calling it, and the manager waiting on that
// informer, can never stop.
func TestHandlers_StopSendingWhenContextEnds(t *testing.T) {
	tests := []struct {
		name    string
		handler func(chan<- event.GenericEvent) handler.EventHandler
		event   event.DeleteEvent
	}{
		{
			name: "pod",
			handler: func(ch chan<- event.GenericEvent) handler.EventHandler {
				return NewEnqueueRequestForPodEvent(ch, nil, referringResolver{}, logr.Discard())
			},
			// The handler skips pods without an IP.
			event: event.DeleteEvent{Object: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web"},
				Status: corev1.PodStatus{PodIP: "10.0.0.1", PodIPs: []corev1.PodIP{{IP: "10.0.0.1"}}}}},
		},
		{
			name: "namespace",
			handler: func(ch chan<- event.GenericEvent) handler.EventHandler {
				return NewEnqueueRequestForNamespaceEvent(ch, nil, referringResolver{}, logr.Discard())
			},
			event: event.DeleteEvent{Object: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}},
		},
		{
			name: "service",
			handler: func(ch chan<- event.GenericEvent) handler.EventHandler {
				return NewEnqueueRequestForServiceEvent(ch, nil, referringResolver{}, logr.Discard())
			},
			event: event.DeleteEvent{Object: &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewWithT(t)
			// Unbuffered and never read, as after the controller's channel source has stopped.
			ch := make(chan event.GenericEvent)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				tt.handler(ch).Delete(ctx, tt.event, nil)
			}()
			g.Eventually(done).WithTimeout(2*time.Second).Should(BeClosed(),
				"the handler is still blocked sending to a channel nobody reads")
		})
	}
}
