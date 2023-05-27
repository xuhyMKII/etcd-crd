package controllers

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/go-logr/logr"
	etcdclient "go.etcd.io/etcd/client"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
	"github.com/improbable-eng/etcd-cluster-operator/internal/etcd"
	"github.com/improbable-eng/etcd-cluster-operator/internal/etcdenvvar"
)

// EtcdPeerReconciler reconciles a EtcdPeer object
type EtcdPeerReconciler struct {
	client.Client
	Log            logr.Logger
	Etcd           etcd.APIBuilder
	EtcdRepository string
}

const (
	etcdScheme          = "http"
	peerLabel           = "etcd.improbable.io/peer-name"
	pvcCleanupFinalizer = "etcdpeer.etcd.improbable.io/pvc-cleanup"
)

//EtcdClusterController和EtcdPeerController是两个关键的控制器，
//它们各自负责管理EtcdCluster和EtcdPeer这两种自定义资源。这两个控制器之间的关系主要体现在EtcdClusterController通过管理EtcdPeer资源来管理etcd集群。
//
//EtcdClusterController：这个控制器主要负责管理EtcdCluster资源，
//它会监控EtcdCluster资源的变化，并根据EtcdCluster的规格（spec）来创建、更新或删除EtcdPeer资源。
//例如，当EtcdCluster的副本数增加时，EtcdClusterController会创建新的EtcdPeer资源来扩大etcd集群。
//反之，当EtcdCluster的副本数减少时，EtcdClusterController会删除多余的EtcdPeer资源来缩小etcd集群。
//
//EtcdPeerController：这个控制器主要负责管理EtcdPeer资源，它会监控EtcdPeer资源的变化，
//并根据EtcdPeer的规格（spec）来创建、更新或删除etcd Pod。
//例如，当一个新的EtcdPeer资源被创建时，EtcdPeerController会创建一个新的etcd Pod。
//反之，当一个EtcdPeer资源被删除时，EtcdPeerController会删除对应的etcd Pod。
//
//总的来说，EtcdClusterController和EtcdPeerController之间的关系是协同工作的关系，
//EtcdClusterController通过管理EtcdPeer资源来管理etcd集群，而EtcdPeerController则负责具体的etcd Pod的生命周期管理。

//EtcdPeer是一个自定义资源（Custom Resource），它代表了一个etcd实例。每个EtcdPeer资源对应一个etcd Pod，这个Pod运行一个etcd服务器。
//
//EtcdPeer的主要作用如下：
//
//表示etcd实例：每个EtcdPeer资源代表一个etcd实例，它包含了运行这个实例所需要的配置信息，如etcd版本、数据目录、监听地址等。
//
//控制etcd实例的生命周期：EtcdPeer资源的创建、更新和删除会触发相应的控制器逻辑，从而创建、更新或删除对应的etcd Pod。
//
//实现etcd集群的扩缩容：通过增加或减少EtcdPeer资源的数量，可以实现etcd集群的扩缩容。
//
//实现etcd实例的故障恢复：如果一个etcd Pod发生故障，对应的EtcdPeer控制器会检测到这个情况，并重新创建一个新的Pod来替换故障的Pod，从而实现故障恢复。

// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdpeers,verbs=get;list;watch;patch
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdpeers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=list;get;create;watch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=list;get;create;watch;delete

func initialMemberURL(member etcdv1alpha1.InitialClusterMember) *url.URL {
	return &url.URL{
		Scheme: etcdScheme,
		Host:   fmt.Sprintf("%s:%d", member.Host, etcdPeerPort),
	}
}

// staticBootstrapInitialCluster returns the value of `ETCD_INITIAL_CLUSTER`
// environment variable.
func staticBootstrapInitialCluster(static etcdv1alpha1.StaticBootstrap) string {
	s := make([]string, len(static.InitialCluster))
	// Put our peers in as the other entries
	for i, member := range static.InitialCluster {
		s[i] = fmt.Sprintf("%s=%s",
			member.Name,
			initialMemberURL(member).String())
	}
	return strings.Join(s, ",")
}

// advertiseURL builds the canonical URL of this peer from it's name and the
// cluster name.
func advertiseURL(etcdPeer etcdv1alpha1.EtcdPeer, port int32) *url.URL {
	return &url.URL{
		Scheme: etcdScheme,
		Host: fmt.Sprintf(
			"%s.%s:%d",
			etcdPeer.Name,
			etcdPeer.Spec.ClusterName,
			port,
		),
	}
}

func bindAllAddress(port int) *url.URL {
	return &url.URL{
		Scheme: etcdScheme,
		Host:   fmt.Sprintf("0.0.0.0:%d", port),
	}
}

func clusterStateValue(cs etcdv1alpha1.InitialClusterState) string {
	if cs == etcdv1alpha1.InitialClusterStateNew {
		return "new"
	} else if cs == etcdv1alpha1.InitialClusterStateExisting {
		return "existing"
	} else {
		return ""
	}
}

// goMaxProcs calculates an appropriate Golang thread limit (GOMAXPROCS) for the
// configured CPU limit.
//
// GOMAXPROCS defaults to the number of CPUs on the Kubelet host which may be
// much higher than the requests and limits defined for the pod,
// See https://github.com/golang/go/issues/33803
// If resources have been set and if CPU limit is > 0 then set GOMAXPROCS to an
// integer between 1 and floor(cpuLimit).
// Etcd might one day set its own GOMAXPROCS based on CPU quota:
// See: https://github.com/etcd-io/etcd/issues/11508
// 计算适合配置的Go线程限制，这个值会影响到etcd的性能。
func goMaxProcs(cpuLimit resource.Quantity) *int64 {
	switch cpuLimit.Sign() {
	case -1, 0:
		return nil
	}
	goMaxProcs := cpuLimit.MilliValue() / 1000
	if goMaxProcs < 1 {
		goMaxProcs = 1
	}
	return pointer.Int64Ptr(goMaxProcs)
}

// 定义一个ReplicaSet，ReplicaSet是kubernetes中用于保证指定数量的Pod副本运行的资源。
func defineReplicaSet(peer etcdv1alpha1.EtcdPeer, etcdRepository string, log logr.Logger) appsv1.ReplicaSet {
	var replicas int32 = 1

	// We use the same labels for the replica set itself, the selector on
	// the replica set, and the pod template under the replica set.
	labels := map[string]string{
		appLabel:     appName,
		clusterLabel: peer.Spec.ClusterName,
		peerLabel:    peer.Name,
	}

	etcdContainer := corev1.Container{
		Name:  appName,
		Image: fmt.Sprintf("%s:v%s", etcdRepository, peer.Spec.Version),
		Env: []corev1.EnvVar{
			{
				Name:  etcdenvvar.InitialCluster,
				Value: staticBootstrapInitialCluster(*peer.Spec.Bootstrap.Static),
			},
			{
				Name:  etcdenvvar.Name,
				Value: peer.Name,
			},
			{
				Name:  etcdenvvar.InitialClusterToken,
				Value: peer.Spec.ClusterName,
			},
			{
				Name:  etcdenvvar.InitialAdvertisePeerURLs,
				Value: advertiseURL(peer, etcdPeerPort).String(),
			},
			{
				Name:  etcdenvvar.AdvertiseClientURLs,
				Value: advertiseURL(peer, etcdClientPort).String(),
			},
			{
				Name:  etcdenvvar.ListenPeerURLs,
				Value: bindAllAddress(etcdPeerPort).String(),
			},
			{
				Name:  etcdenvvar.ListenClientURLs,
				Value: bindAllAddress(etcdClientPort).String(),
			},
			{
				Name:  etcdenvvar.InitialClusterState,
				Value: clusterStateValue(peer.Spec.Bootstrap.InitialClusterState),
			},
			{
				Name:  etcdenvvar.DataDir,
				Value: etcdDataMountPath,
			},
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "etcd-client",
				ContainerPort: etcdClientPort,
			},
			{
				Name:          "etcd-peer",
				ContainerPort: etcdPeerPort,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "etcd-data",
				MountPath: etcdDataMountPath,
			},
		},
	}
	if peer.Spec.PodTemplate != nil {
		if peer.Spec.PodTemplate.Resources != nil {
			etcdContainer.Resources = *peer.Spec.PodTemplate.Resources.DeepCopy()
			if value := goMaxProcs(*etcdContainer.Resources.Limits.Cpu()); value != nil {
				etcdContainer.Env = append(
					etcdContainer.Env,
					corev1.EnvVar{
						Name:  "GOMAXPROCS",
						Value: fmt.Sprintf("%d", *value),
					},
				)
			}
		}
	}
	replicaSet := appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          labels,
			Annotations:     make(map[string]string),
			Name:            peer.Name,
			Namespace:       peer.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&peer, etcdv1alpha1.GroupVersion.WithKind("EtcdPeer"))},
		},
		Spec: appsv1.ReplicaSetSpec{
			// This will *always* be 1. Other peers are handled by other EtcdPeers.
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: make(map[string]string),
					Name:        peer.Name,
					Namespace:   peer.Namespace,
				},
				Spec: corev1.PodSpec{
					Hostname:  peer.Name,
					Subdomain: peer.Spec.ClusterName,
					HostAliases: []corev1.HostAlias{
						{
							IP: "127.0.0.1",
							Hostnames: []string{
								fmt.Sprintf("%s.%s", peer.Name, peer.Spec.ClusterName),
							},
						},
					},
					Containers: []corev1.Container{etcdContainer},
					Volumes: []corev1.Volume{
						{
							Name: "etcd-data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: peer.Name,
								},
							},
						},
					},
				},
			},
		},
	}

	if peer.Spec.PodTemplate != nil {
		if peer.Spec.PodTemplate.Metadata != nil {
			// Stamp annotations
			for name, value := range peer.Spec.PodTemplate.Metadata.Annotations {
				if !etcdv1alpha1.IsInvalidUserProvidedAnnotationName(name) {
					if _, found := replicaSet.Spec.Template.Annotations[name]; !found {
						replicaSet.Spec.Template.Annotations[name] = value
					} else {
						// This will only check against an annotation that we set ourselves.
						log.V(2).Info("Ignoring annotation, we already have one with that name",
							"annotation-name", name)
					}
				} else {
					// In theory, this code is unreachable as we check this validation at the start of the reconcile
					// loop. See https://xkcd.com/2200
					log.V(2).Info("Ignoring annotation, applying etcd.improbable.io/ annotations is not supported",
						"annotation-name", name)
				}
			}
		}
		if peer.Spec.PodTemplate.Affinity != nil {
			replicaSet.Spec.Template.Spec.Affinity = peer.Spec.PodTemplate.Affinity
		}
	}

	return replicaSet
}

func pvcForPeer(peer *etcdv1alpha1.EtcdPeer) *corev1.PersistentVolumeClaim {
	labels := map[string]string{
		appLabel:     appName,
		clusterLabel: peer.Spec.ClusterName,
		peerLabel:    peer.Name,
	}

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      peer.Name,
			Namespace: peer.Namespace,
			Labels:    labels,
		},
		Spec: *peer.Spec.Storage.VolumeClaimTemplate.DeepCopy(),
	}
}

func (r *EtcdPeerReconciler) maybeCreatePvc(ctx context.Context, peer *etcdv1alpha1.EtcdPeer) (created bool, err error) {
	objectKey := client.ObjectKey{
		Name:      peer.Name,
		Namespace: peer.Namespace,
	}
	// Check for existing object
	pvc := &corev1.PersistentVolumeClaim{}
	err = r.Get(ctx, objectKey, pvc)
	// Object exists
	if err == nil {
		return false, nil
	}
	// Error when fetching the object
	if !apierrs.IsNotFound(err) {
		return false, err
	}
	// Object does not exist
	err = r.Create(ctx, pvcForPeer(peer))
	// Maybe a stale cache.
	if apierrs.IsAlreadyExists(err) {
		return false, fmt.Errorf("stale cache error: object was not found in cache but creation failed with AlreadyExists error: %w", err)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func hasPvcDeletionFinalizer(peer etcdv1alpha1.EtcdPeer) bool {
	return sets.NewString(peer.ObjectMeta.Finalizers...).Has(pvcCleanupFinalizer)
}

// PeerPVCDeleter deletes the PVC for an EtcdPeer and removes the PVC deletion
// finalizer.
type PeerPVCDeleter struct {
	log    logr.Logger
	client client.Client
	peer   *etcdv1alpha1.EtcdPeer
}

//这段代码定义了一个名为Execute的方法，它属于PeerPVCDeleter结构体。
//这个方法的主要作用是在删除EtcdPeer资源之前，先删除与之关联的PersistentVolumeClaim（PVC）。
//
//具体逻辑如下：
//
//首先，它创建了一个预期的PVC对象expectedPvc，这个对象是基于EtcdPeer资源生成的。
//
//然后，它尝试从Kubernetes API服务器获取这个PVC对象。如果获取成功，那么就会检查这个PVC是否已经被标记为删除。
//如果没有，那么就会调用client.Delete方法来删除这个PVC。
//
//如果在获取PVC时返回了"Not Found"错误，那么就认为这个PVC已经被删除或者从未被创建。
//
//如果在获取PVC时返回了其他错误，那么就直接返回这个错误。
//
//最后，无论PVC是否存在，都会尝试从EtcdPeer资源中移除一个名为pvcCleanupFinalizer的终结器。
//这个终结器的作用是防止EtcdPeer资源在其关联的PVC被删除之前被垃圾收集器删除。
//
//在整个项目中，PeerPVCDeleter的作用是确保在删除EtcdPeer资源时，其关联的PVC也能被正确地删除。
//这是因为在Kubernetes中，当一个资源被删除时，其关联的PVC并不会自动被删除。如果不手动删除这些PVC，
//那么它们就会成为孤儿资源，占用集群的存储资源。因此，PeerPVCDeleter在资源清理和集群存储管理方面起到了重要的作用。

// Execute performs the deletiong and finalizer removal
func (o *PeerPVCDeleter) Execute(ctx context.Context) error {
	o.log.V(2).Info("Deleting PVC for peer prior to deletion")
	expectedPvc := pvcForPeer(o.peer)
	expectedPvcNamespacedName, err := client.ObjectKeyFromObject(expectedPvc)
	if err != nil {
		return fmt.Errorf("unable to get ObjectKey from PVC: %s", err)
	}
	var actualPvc corev1.PersistentVolumeClaim
	err = o.client.Get(ctx, expectedPvcNamespacedName, &actualPvc)
	switch {
	case err == nil:
		// PVC exists.
		// Check whether it has already been deleted (probably by us).
		// It won't actually be deleted until the garbage collector
		// deletes the Pod which is using it.
		if actualPvc.ObjectMeta.DeletionTimestamp.IsZero() {
			o.log.V(2).Info("Deleting PVC for peer")
			err := o.client.Delete(ctx, expectedPvc)
			if err == nil {
				o.log.V(2).Info("Deleted PVC for peer")
				return nil
			}
			return fmt.Errorf("failed to delete PVC for peer: %w", err)
		}
		o.log.V(2).Info("PVC for peer has already been marked for deletion")

	case apierrors.IsNotFound(err):
		o.log.V(2).Info("PVC not found for peer. Already deleted or never created.")

	case err != nil:
		return fmt.Errorf("failed to get PVC for deleted peer: %w", err)

	}

	// If we reach this stage, the PVC has been deleted or didn't need
	// deleting.
	// Remove the finalizer so that the EtcdPeer can be garbage
	// collected along with its replicaset, pod...and with that the PVC
	// will finally be deleted by the garbage collector.
	o.log.V(2).Info("Removing PVC cleanup finalizer")
	updated := o.peer.DeepCopy()
	controllerutil.RemoveFinalizer(updated, pvcCleanupFinalizer)
	if err := o.client.Patch(ctx, updated, client.MergeFrom(o.peer)); err != nil {
		return fmt.Errorf("failed to remove PVC cleanup finalizer: %w", err)
	}
	o.log.V(2).Info("Removed PVC cleanup finalizer")
	return nil
}

func (r *EtcdPeerReconciler) updateStatus(peer *etcdv1alpha1.EtcdPeer, serverVersion string) {
	peer.Status.ServerVersion = serverVersion
}

// 这段代码定义了EtcdPeerReconciler的Reconcile方法，这是Kubernetes控制器的核心方法，它负责处理EtcdPeer资源的状态并确保其与预期状态一致。
//
// 具体逻辑如下：
//
// 首先，它创建了一个带有超时的上下文，并获取了对应的EtcdPeer资源。
//
// 然后，它对EtcdPeer资源进行默认值填充和验证。
//
// 在每次reconcile操作后，它都会尝试更新EtcdPeer的状态。
//
// 接着，它尝试连接到etcd集群，并获取集群的版本信息。
//
// 如果EtcdPeer资源被标记为删除，并且已经添加了PVC删除的终结器，那么就会执行PeerPVCDeleter的Execute方法来删除与EtcdPeer关联的PVC。
//
// 如果EtcdPeer资源没有被标记为删除，那么就会尝试创建与之关联的PVC。
//
// 最后，它会检查是否存在与EtcdPeer关联的ReplicaSet，如果不存在，就会创建一个新的ReplicaSet。
//
// 在整个项目中，EtcdPeerReconciler的作用是管理EtcdPeer资源。EtcdPeer资源代表了etcd集群中的一个成员，
// 每个EtcdPeer资源都会创建一个ReplicaSet和一个PVC。ReplicaSet负责管理Pod，确保Pod的数量始终为1，
// 而PVC则用于存储etcd成员的数据。通过管理EtcdPeer资源，EtcdPeerReconciler能够管理etcd集群中的每一个成员。
func (r *EtcdPeerReconciler) Reconcile(req ctrl.Request) (_ ctrl.Result, reterr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log := r.Log.WithValues("peer", req.NamespacedName)

	var peer etcdv1alpha1.EtcdPeer
	if err := r.Get(ctx, req.NamespacedName, &peer); err != nil {
		// NotFound errors occur when the EtcdPeer has been deleted but a PVC is
		// left behind.
		// Ignore these and do not requeue in this case.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.V(2).Info("Found EtcdPeer resource")

	// Apply defaults in case a defaulting webhook has not been deployed.
	peer.Default()

	// Validate in case a validating webhook has not been deployed
	err := peer.ValidateCreate()
	if err != nil {
		log.Error(err, "invalid EtcdPeer")
		return ctrl.Result{}, nil
	}

	original := peer.DeepCopy()

	// Always attempt to patch the status after each reconciliation.
	defer func() {
		if reflect.DeepEqual(original.Status, peer.Status) {
			return
		}
		if err := r.Client.Status().Patch(ctx, &peer, client.MergeFrom(original)); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, fmt.Errorf("error while patching EtcdPeer.Status: %s ", err)})
		}
	}()

	// Attempt to dial the etcd cluster, recording the cluster response if we can
	var (
		serverVersion string
	)
	etcdConfig := etcdclient.Config{
		Endpoints: []string{
			fmt.Sprintf("%s://%s.%s.%s:%d", etcdScheme, peer.Name, peer.Spec.ClusterName, peer.Namespace, etcdClientPort),
		},
		Transport:               etcdclient.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * 1,
	}

	if c, err := r.Etcd.New(etcdConfig); err != nil {
		log.V(2).Info("Unable to connect to etcd", "error", err)
	} else {
		if version, err := c.GetVersion(ctx); err != nil {
			log.V(2).Info("Unable to get Etcd version", "error", err)
		} else {
			serverVersion = version.Server
		}
	}

	r.updateStatus(&peer, serverVersion)

	// Always requeue after ten seconds, as we don't watch on the membership list. So we don't auto-detect changes made
	// to the etcd membership API.
	// TODO(#76) Implement custom watch on etcd membership API, and remove this `requeueAfter`
	result := ctrl.Result{RequeueAfter: time.Second * 10}

	// Check if the peer has been marked for deletion
	if !peer.ObjectMeta.DeletionTimestamp.IsZero() {
		if hasPvcDeletionFinalizer(peer) {
			action := &PeerPVCDeleter{
				log:    log,
				client: r.Client,
				peer:   &peer,
			}
			err := action.Execute(ctx)
			return result, err
		}
		return result, nil
	}

	created, err := r.maybeCreatePvc(ctx, &peer)
	if err != nil || created {
		return result, err
	}

	var existingReplicaSet appsv1.ReplicaSet
	err = r.Get(
		ctx,
		client.ObjectKey{
			Namespace: peer.Namespace,
			Name:      peer.Name,
		},
		&existingReplicaSet,
	)

	if apierrs.IsNotFound(err) {
		replicaSet := defineReplicaSet(peer, r.EtcdRepository, log)
		log.V(1).Info("Replica set does not exist, creating",
			"replica-set", replicaSet.Name)
		if err := r.Create(ctx, &replicaSet); err != nil {
			log.Error(err, "unable to create ReplicaSet for EtcdPeer", "replica-set", replicaSet)
			return result, err
		}
		return result, nil
	}

	// Check for some other error from the previous `r.Get`
	if err != nil {
		log.Error(err, "unable to query for replica sets")
		return result, err
	}

	log.V(2).Info("Replica set already exists", "replica-set", existingReplicaSet.Name)

	return result, nil
}

//在Kubernetes的operator模式中，Map方法通常不会被直接调用。它是handler.Mapper接口的一部分，这个接口被用于设置Kubernetes的watch机制。
//当你设置一个watcher来观察某种Kubernetes资源的变化时，你可以提供一个实现了handler.Mapper接口的对象。当被观察的资源发生变化时，
//Kubernetes的controller-runtime库会自动调用Map方法，将发生变化的资源转换为一个或多个reconcile.Request，然后这些请求会被发送到对应的reconciler进行处理。
//
//在这个项目中，pvcMapper的Map方法被用于将PersistentVolumeClaim(PVC)资源的变化映射到对应的EtcdPeer资源。
//当一个PVC发生变化时，Map方法会被自动调用，生成一个包含对应EtcdPeer的reconcile.Request，
//然后这个请求会被发送到EtcdPeer的reconciler进行处理。这样可以确保当PVC的状态发生变化时，对应的EtcdPeer的状态也会被及时更新。
//
//这段代码定义了pvcMapper的Map方法，这是一个实现了handler.Mapper接口的方法，用于将一个Kubernetes对象映射到一个或多个reconcile.Request。
//
//具体逻辑如下：
//
//首先，它创建了一个空的reconcile.Request切片。
//
//然后，它获取了传入的Kubernetes对象的标签。
//
//如果对象的标签中包含peerLabel，那么就会创建一个新的reconcile.Request，
//其中NamespacedName的Name和Namespace分别设置为peerLabel的值和对象的命名空间，并将这个reconcile.Request添加到切片中。
//
//最后，它返回这个reconcile.Request切片。
//
//在整个项目中，pvcMapper的作用是将PVC对象映射到与之关联的EtcdPeer资源。
//当PVC发生变化时，pvcMapper会生成一个reconcile.Request，这个请求会触发EtcdPeer控制器的Reconcile方法，从而使EtcdPeer的状态与PVC的状态保持一致。

type pvcMapper struct{}

var _ handler.Mapper = &pvcMapper{}

// Map looks up the peer name label from the PVC and generates a reconcile
// request for *that* name in the namespace of the pvc.
// This mapper ensures that we only wake up the Reconcile function for changes
// to PVCs related to EtcdPeer resources.
// PVCs are deliberately not owned by the peer, to ensure that they are not
// garbage collected along with the peer.
// So we can't use OwnerReference handler here.
func (m *pvcMapper) Map(o handler.MapObject) []reconcile.Request {
	requests := []reconcile.Request{}
	labels := o.Meta.GetLabels()
	if peerName, found := labels[peerLabel]; found {
		requests = append(
			requests,
			reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      peerName,
					Namespace: o.Meta.GetNamespace(),
				},
			},
		)
	}
	return requests
}

func (r *EtcdPeerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdPeer{}).
		// Watch for changes to ReplicaSet resources that an EtcdPeer owns.
		Owns(&appsv1.ReplicaSet{}).
		// We can use a simple EnqueueRequestForObject handler here as the PVC
		// has the same name as the EtcdPeer resource that needs to be enqueued
		Watches(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, &handler.EnqueueRequestsFromMapFunc{
			ToRequests: &pvcMapper{},
		}).
		Complete(r)
}
