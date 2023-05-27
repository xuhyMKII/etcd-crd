package webhooks

import (
	"context"
	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

type EtcdClusterWebhook struct {
	client  client.Client
	decoder *admission.Decoder
}

// EtcdClusterWebhook 实现了 admission.Handler 接口
var _ admission.Handler = &EtcdClusterWebhook{}

// Handle 方法是 webhook 的核心逻辑
func (a *EtcdClusterWebhook) Handle(ctx context.Context, req admission.Request) admission.Response {
	etcdCluster := &etcdv1alpha1.EtcdCluster{}

	err := a.decoder.Decode(req, etcdCluster)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// 在这里，我们可以添加任何我们需要的验证逻辑，例如检查 EtcdCluster 的字段是否符合要求
	// 如果不符合要求，我们可以返回一个错误响应
	if etcdCluster.Spec.Size < 1 {
		return admission.Denied("spec.size must be greater than 0")
	}

	// 我们也可以在这里设置默认值
	if etcdCluster.Spec.Version == "" {
		etcdCluster.Spec.Version = "3.4.13"
	}

	// 如果一切都符合要求，我们返回一个允许的响应
	return admission.Allowed("")
}

// InjectDecoder injects the decoder into the EtcdClusterWebhook
func (a *EtcdClusterWebhook) InjectDecoder(d *admission.Decoder) error {
	a.decoder = d
	return nil
}
