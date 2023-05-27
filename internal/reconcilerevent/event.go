package reconcilerevent

import (
	"k8s.io/client-go/tools/record"
)

// 定义了一个接口ReconcilerEvent，这个接口表示了操作员实际做了任何有意义的改变。任何有意义的改变都应该产生一个这样的事件。
const (
	K8sEventTypeNormal  = "Normal"
	K8sEventTypeWarning = "Warning"
)

// ReconcilerEvent represents the action of the operator having actually done anything. Any meaningful change should
// result in one of these.
type ReconcilerEvent interface {

	// Record this into an event recorder as a Kubernetes API event
	Record(recorder record.EventRecorder)
}
