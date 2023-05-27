package webhooks

import (
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	"github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
)

// EtcdCluster validating webhook
//+kubebuilder:webhook:path=/validate-etcd-improbable-io-v1alpha1-etcdcluster,verbs=create;update,mutating=false,failurePolicy=fail,groups=etcd.improbable.io,resources=etcdclusters,versions=v1alpha1,name=validation.etcdclusters.etcd.improbable.io
// EtcdCluster defaulting webhook
//+kubebuilder:webhook:path=/mutate-etcd-improbable-io-v1alpha1-etcdcluster,verbs=create;update,mutating=true,failurePolicy=fail,groups=etcd.improbable.io,resources=etcdclusters,versions=v1alpha1,name=default.etcdclusters.etcd.improbable.io
// EtcdPeer validating webhook
//+kubebuilder:webhook:path=/validate-etcd-improbable-io-v1alpha1-etcdpeer,verbs=create;update,mutating=false,failurePolicy=fail,groups=etcd.improbable.io,resources=etcdpeers,versions=v1alpha1,name=validation.etcdpeers.etcd.improbable.io
// EtcdPeer defaulting webhook
//+kubebuilder:webhook:path=/mutate-etcd-improbable-io-v1alpha1-etcdpeer,verbs=create;update,mutating=true,failurePolicy=fail,groups=etcd.improbable.io,resources=etcdpeers,versions=v1alpha1,name=default.etcdpeers.etcd.improbable.io

// 定义了EtcdCluster和EtcdPeer的webhook。Webhook是Kubernetes API的一种扩展机制，
// 可以在API请求处理的不同阶段插入自定义的逻辑。在这个文件中，定义了EtcdCluster和EtcdPeer的验证和默认值设置的webhook。

// 这两个方法 SetupWithManager 是在对应的控制器 EtcdCluster 和 EtcdPeer 的 SetupWithManager 方法中被调用的。这两个方法的主要目的是将 webhook 配置到 Kubernetes 的管理器中。
//
// 在这两个方法中，ctrl.NewWebhookManagedBy(mgr) 首先创建了一个由指定管理器管理的新 webhook，然后 .For(&v1alpha1.EtcdCluster{}) 和 .For(&v1alpha1.EtcdPeer{}) 分别指定了 webhook 对象的类型，最后 .Complete() 完成了 webhook 的设置。
//
// 具体来说，这两个方法的执行逻辑如下：
//
// 创建一个新的 webhook，由 Kubernetes 的管理器 mgr 管理。
// 指定 webhook 的对象类型，分别为 EtcdCluster 和 EtcdPeer。
// 完成 webhook 的设置。
// 这两个方法的执行会使得在 Kubernetes 中，当有对 EtcdCluster 和 EtcdPeer 对象的操作时（例如创建、更新等），会触发相应的 webhook，执行在 webhook 中定义的逻辑，例如验证对象的字段是否符合要求，设置对象的默认值等。
type EtcdCluster struct {
	Log logr.Logger
}

//func (o *EtcdCluster) SetupWithManager(mgr ctrl.Manager) error {
//	return ctrl.NewWebhookManagedBy(mgr).
//		For(&v1alpha1.EtcdCluster{}).
//		Complete()
//}

func (o *EtcdCluster) SetupWithManager(mgr ctrl.Manager) error {
	webhookHandler := &EtcdClusterWebhook{
		client: mgr.GetClient(),
	}

	mgr.GetWebhookServer().Register("/mutate-v1alpha1-etcdcluster", &webhook.Admission{Handler: webhookHandler})

	return nil
}

type EtcdPeer struct {
	Log logr.Logger
}

func (o *EtcdPeer) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&v1alpha1.EtcdPeer{}).
		Complete()
}
