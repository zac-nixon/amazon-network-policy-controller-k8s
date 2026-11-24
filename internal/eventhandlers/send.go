package eventhandlers

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// sendPolicyEvent queues obj on the policy event channel, giving up if ctx ends first, and reports
// whether it was sent. The channel's reader stops with the controller, so a send that could not give
// up would block the informer calling the handler, and with it the manager's shutdown.
func sendPolicyEvent(ctx context.Context, ch chan<- event.GenericEvent, obj client.Object) bool {
	select {
	case ch <- event.GenericEvent{Object: obj}:
		return true
	case <-ctx.Done():
		return false
	}
}
