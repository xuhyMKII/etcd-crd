package controllers

import (
	"context"
	"fmt"
	"html/template"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
	"github.com/improbable-eng/etcd-cluster-operator/internal/reconcilerevent"
)

//这个文件是Etcd备份控制器的实现。它负责处理EtcdBackup资源的创建和更新，并触发相应的备份操作。

// 这是 Etcd 备份控制器的主要结构体，它包含了 client、log、scheme 等字段，用于执行 Etcd 备份的相关操作。
// EtcdBackupReconciler reconciles a EtcdBackup object
type EtcdBackupReconciler struct {
	client.Client
	Log              logr.Logger
	Scheme           *runtime.Scheme
	BackupAgentImage string
	ProxyURL         string
	Recorder         record.EventRecorder
}

// 这是一个容器，包含了 EtcdBackup、其依赖项的实际状态和期望状态。它包含了决定下一步操作所需的所有状态。
// backupState is a container for the Backup, the actual status of its
// dependencies and the desired dependencies.
// It has all the state necessary in deciding what action to perform next.
type backupState struct {
	backup  *etcdv1alpha1.EtcdBackup
	actual  *backupStateContainer
	desired *backupStateContainer
}

// 这是 EtcdBackup 的依赖项的容器，包含了 serviceAccount 和 pod。
// backupStateContainer is a container for the dependencies of the EtcdBackup.
type backupStateContainer struct {
	serviceAccount *corev1.ServiceAccount
	pod            *corev1.Pod
}

// 这个方法通过只读 API 请求来填充 backupState.actual。
// setStateActual populates backupState.actual by making read-only API requests.
func (r *EtcdBackupReconciler) setStateActual(ctx context.Context, state *backupState) error {
	var actual backupStateContainer

	key := client.ObjectKey{
		Name:      state.backup.Name,
		Namespace: state.backup.Namespace,
	}

	actual.serviceAccount = &corev1.ServiceAccount{}
	if err := r.Get(ctx, key, actual.serviceAccount); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("error while getting service account: %s", err)
		}
		actual.serviceAccount = nil
	}
	actual.pod = &corev1.Pod{}
	if err := r.Get(ctx, key, actual.pod); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("error while getting pod: %s", err)
		}
		actual.pod = nil
	}

	state.actual = &actual
	return nil
}

// 这个方法填充 backupState.desired。它根据提供的 backupState.backup 计算这些资源，
// 并为每个期望的资源设置一个控制器引用，以便在 EtcdBack 被删除时它们会被垃圾收集。
// setStateDesired populates the backupState.desired.
// It computes these resources based on the supplied backupState.backup
// And it should set a controller reference for each desired resource so that
// they will be garbage collected when the EtcdBack is deleted.
// 如果一个资源没有设置控制器引用（也称为所有者引用），那么当控制器对象（如 Deployment、StatefulSet、DaemonSet 等）被删除时，Kubernetes 不会自动删除这个资源。
// 控制器引用是 Kubernetes 垃圾收集机制的一部分。当一个对象被删除时，Kubernetes 会查看该对象的控制器引用字段，找到所有设置了该对象为所有者的依赖对象，并将这些依赖对象也删除。这就是所谓的级联删除。
// 例如，一个 Pod 可能被一个 ReplicaSet 控制，该 ReplicaSet 又可能被一个 Deployment 控制。
// 如果你删除了 Deployment，那么 Kubernetes 会自动删除 ReplicaSet，然后再删除 Pod。这是因为在创建这些对象时，Kubernetes 自动设置了控制器引用。
// 如果没有设置控制器引用，那么即使你删除了 Deployment，ReplicaSet 和 Pod 也会保留在系统中。这可能会导致资源泄漏，因为这些孤立的资源可能不再被使用，但仍然会占用系统资源。
func (r *EtcdBackupReconciler) setStateDesired(state *backupState) error {
	var desired backupStateContainer

	desired.serviceAccount = serviceAccountForBackup(state.backup)
	if err := controllerutil.SetControllerReference(state.backup, desired.serviceAccount, r.Scheme); err != nil {
		return fmt.Errorf("error setting service account controller reference: %s", err)
	}

	pod, err := podForBackup(state.backup, r.BackupAgentImage, r.ProxyURL, desired.serviceAccount.Name)
	if err != nil {
		return fmt.Errorf("error %q computing pod for backup", err)
	}
	if err := controllerutil.SetControllerReference(state.backup, pod, r.Scheme); err != nil {
		return fmt.Errorf("error setting pod controller reference: %s", err)
	}
	desired.pod = pod
	state.desired = &desired
	return nil
}

// 这个方法通过只读 API 请求创建一个 backupState。返回的状态稍后用于计算要执行的下一步操作。
// getState creates a backupState by making read-only API requests.
// The returned state is used later to calculate the next action to perform.
func (r EtcdBackupReconciler) getState(ctx context.Context, req ctrl.Request) (*backupState, error) {
	var state backupState

	state.backup = &etcdv1alpha1.EtcdBackup{}
	if err := r.Get(ctx, req.NamespacedName, state.backup); err != nil {
		if client.IgnoreNotFound(err) != nil {
			return nil, fmt.Errorf("error while getting backup: %s", err)
		}
		state.backup = nil
		return &state, nil
	}

	if err := r.setStateActual(ctx, &state); err != nil {
		return nil, fmt.Errorf("error setting actual state: %s", err)
	}

	if err := r.setStateDesired(&state); err != nil {
		return nil, fmt.Errorf("error setting desired state: %s", err)
	}

	return &state, nil
}

// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdbackups,verbs=get;list;watch
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;create
func (r *EtcdBackupReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := r.Log.WithValues("etcdbackup-name", req.NamespacedName)

	// Get the state of the EtcdBackup and all its dependencies.
	state, err := r.getState(ctx, req)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("error getting state: %s", err)
	}

	// Calculate a single action to perform next, based on the state.
	var (
		action Action
		event  reconcilerevent.ReconcilerEvent
	)
	switch {
	case state.backup == nil:
		log.Info("Backup resource not found. Ignoring.")
	case !state.backup.DeletionTimestamp.IsZero():
		log.Info("Backup resource has been deleted. Ignoring.")
	case state.backup.Status.Phase == "":
		log.Info("Backup starting. Updating status.")
		new := state.backup.DeepCopy()
		new.Status.Phase = etcdv1alpha1.EtcdBackupPhaseBackingUp
		action = &PatchStatus{client: r.Client, original: state.backup, new: new}
	case state.backup.Status.Phase == etcdv1alpha1.EtcdBackupPhaseFailed:
		log.Info("Backup has failed. Ignoring.")
	case state.backup.Status.Phase == etcdv1alpha1.EtcdBackupPhaseCompleted:
		log.Info("Backup has completed. Ignoring.")
	case state.actual.serviceAccount == nil:
		log.Info("Service account does not exist. Creating.")
		action = &CreateRuntimeObject{client: r.Client, obj: state.desired.serviceAccount}
	case state.actual.pod == nil:
		log.Info("Pod does not exist. Creating.")
		action = &CreateRuntimeObject{client: r.Client, obj: state.desired.pod}
	case state.actual.pod.Status.Phase == corev1.PodFailed:
		log.Info("Backup agent failed. Updating status.")
		new := state.backup.DeepCopy()
		new.Status.Phase = etcdv1alpha1.EtcdBackupPhaseFailed
		action = &PatchStatus{client: r.Client, original: state.backup, new: new}
		event = &reconcilerevent.BackupFailed{For: state.backup}
	case state.actual.pod.Status.Phase == corev1.PodSucceeded:
		log.Info("Backup agent succeeded. Updating status.")
		new := state.backup.DeepCopy()
		new.Status.Phase = etcdv1alpha1.EtcdBackupPhaseCompleted
		action = &PatchStatus{client: r.Client, original: state.backup, new: new}
		event = &reconcilerevent.BackupSucceeded{For: state.backup}
	}
	// 这是控制器的主要方法，它根据 EtcdBackup 的状态和其依赖项的状态来决定下一步要执行的操作。
	//这个方法首先获取 EtcdBackup 的状态和所有依赖项的状态，然后根据这些状态计算出要执行的下一步操作。
	//这个操作可能是创建 ServiceAccount、Pod，或者更新 EtcdBackup 的状态。
	//这个方法并不直接调用 Kubernetes API 来创建或更新资源，而是通过执行 action 来完成这些操作。
	//这些 action 是通过 CreateRuntimeObject 或 PatchStatus 这样的结构体来实现的，这些结构体实现了 Action 接口，可以执行具体的操作。
	// Execute the action
	if action != nil {
		err := action.Execute(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("error executing action: %s", err)
		}
		// Generate a generic event for CreateRuntimeObject actions
		if event == nil {
			switch o := action.(type) {
			case *CreateRuntimeObject:
				event = &reconcilerevent.ObjectCreatedEvent{Log: log, For: state.backup, Object: o.obj}
			}
		}
	}

	// Record an event
	if event != nil {
		event.Record(r.Recorder)
	}

	return ctrl.Result{}, nil
}

// 这个方法用于将 EtcdBackupReconciler 与 manager 关联，以便可以创建新的控制器。
func (r *EtcdBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdBackup{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

// 这个方法根据 EtcdBackup 创建一个受限的 ServiceAccount，这个 ServiceAccount 会被备份代理 Pod 使用。
// serviceAccountForBackup creates a restriced service-account by the backup-agent pod.
func serviceAccountForBackup(backup *etcdv1alpha1.EtcdBackup) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
	}
}

// 这个方法根据 EtcdBackup 创建一个运行备份代理镜像的 Pod。这个 Pod 不需要与 API 交互，因此不应该有权限这样做。
// podForBackup creates a pod for running the backup-agent image.
// It does not need to interact with the API and should not have permissions to
// do so.
func podForBackup(backup *etcdv1alpha1.EtcdBackup, image, proxyURL, serviceAccount string) (*corev1.Pod, error) {
	tmpl, err := template.New("template").Parse(backup.Spec.Destination.ObjectURLTemplate)
	if err != nil {
		return nil, fmt.Errorf("error %q parsing object URL template", err)
	}
	var objectURL strings.Builder
	if err := tmpl.Execute(&objectURL, backup); err != nil {
		return nil, fmt.Errorf("error %q executing template", err)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backup.Name,
			Namespace: backup.Namespace,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: serviceAccount,
			Containers: []corev1.Container{
				{
					Name:  "backup-agent",
					Image: image,
					Args: []string{
						"--proxy-url", proxyURL,
						"--backup-url", objectURL.String(),
						"--etcd-url", backup.Spec.Source.ClusterURL,
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("50Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("100m"),
							corev1.ResourceMemory: resource.MustParse("50Mi"),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}, nil
}
