package controllers

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
	"github.com/improbable-eng/etcd-cluster-operator/internal/reconcilerevent"
)

//etcdrestore_controller.go 是一个 Kubernetes 控制器，它负责处理 EtcdRestore 资源。EtcdRestore 是一个自定义资源（CRD），
//用于描述如何从备份中恢复 etcd 集群的状态。当用户创建一个新的 EtcdRestore 资源时，etcdrestore_controller.go 会启动一个恢复过程，
//这个过程包括创建一个新的 etcd 集群，并从备份中恢复其状态。
//
//以下是这五个控制器的主要职责和相互关系：
//
//etcdpeer_controller.go：负责管理 EtcdPeer 资源，每个 EtcdPeer 资源代表 etcd 集群中的一个节点。
//EtcdPeer 控制器会确保每个节点的状态与其对应的 EtcdPeer 资源描述的状态一致。
//
//etcdcluster_controller.go：负责管理 EtcdCluster 资源，每个 EtcdCluster 资源代表一个 etcd 集群。
//EtcdCluster 控制器会根据 EtcdCluster 资源的描述创建、更新或删除 EtcdPeer 资源，从而管理 etcd 集群的节点。
//
//etcdbackupschedule_controller.go：负责管理 EtcdBackupSchedule 资源，
//每个 EtcdBackupSchedule 资源描述了如何定期备份一个 etcd 集群。
//EtcdBackupSchedule 控制器会根据 EtcdBackupSchedule 资源的描述定期创建 EtcdBackup 资源，从而触发备份操作。
//
//etcdbackup_controller_new.go：负责管理 EtcdBackup 资源，
//每个 EtcdBackup 资源描述了如何备份一个 etcd 集群。EtcdBackup 控制器会根据 EtcdBackup 资源的描述执行备份操作。
//
//etcdrestore_controller.go：如前所述，负责处理 EtcdRestore 资源，执行从备份中恢复 etcd 集群的操作。
//
//这五个控制器共同协作，提供了管理 etcd 集群的完整生命周期，包括创建、更新、备份、恢复和删除。

// EtcdRestoreReconciler reconciles a EtcdRestore object
type EtcdRestoreReconciler struct {
	client.Client
	Log             logr.Logger
	Recorder        record.EventRecorder
	RestorePodImage string
	ProxyURL        string
}

// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdrestores,verbs=get;list;watch
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdclusters,verbs=get;list;watch;create;patch
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=persistantvolumeclaims/status,verbs=get;watch;update;patch;create
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;watch;list;create

func name(o metav1.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: o.GetNamespace(),
		Name:      o.GetName(),
	}
}

const (
	restoreContainerName      = "etcd-restore"
	restoredFromLabel         = "etcd.improbable.io/restored-from"
	restorePodLabel           = "etcd.improbable.io/restore-pod"
	pvcCreatedEventReason     = "PVCCreated"
	pvcInvalidEventReason     = "PVCInvalid"
	podCreatedEventReason     = "PodCreated"
	podInvalidEventReason     = "PodInvalid"
	podFailedEventReason      = "PodFailed"
	clusterCreatedEventReason = "ClusterCreated"
)

// markPVC：这个方法给 PersistentVolumeClaim (PVC) 添加一个标签，标签的键是 restoredFromLabel，
// 值是 EtcdRestore 资源的名字。这个标签表示这个 PVC 是由哪个恢复操作创建的。
func markPVC(restore etcdv1alpha1.EtcdRestore, pvc *corev1.PersistentVolumeClaim) {
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}
	pvc.Labels[restoredFromLabel] = restore.Name
}

// IsOurPVC：这个方法检查一个 PVC 是否被标记为由特定的 EtcdRestore 操作创建。它通过检查 PVC 的标签来实现。
func IsOurPVC(restore etcdv1alpha1.EtcdRestore, pvc corev1.PersistentVolumeClaim) bool {
	return pvc.Labels[restoredFromLabel] == restore.Name
}

// IsOurPVC：这个方法检查一个 PVC 是否被标记为由特定的 EtcdRestore 操作创建。它通过检查 PVC 的标签来实现。
func markPod(restore etcdv1alpha1.EtcdRestore, pod *corev1.Pod) {
	if pod.Labels == nil {
		pod.Labels = make(map[string]string)
	}
	pod.Labels[restoredFromLabel] = restore.Name
	pod.Labels[restorePodLabel] = "true"
}

// IsOurPod：这个方法检查一个 Pod 是否被标记为由特定的 EtcdRestore 操作创建。它通过检查 Pod 的标签来实现。
func IsOurPod(restore etcdv1alpha1.EtcdRestore, pod corev1.Pod) bool {
	return pod.Labels[restoredFromLabel] == restore.Name &&
		pod.Labels[restorePodLabel] == "true"
}

// markCluster：这个方法给 EtcdCluster 添加一个标签，标签的键是 restoredFromLabel，值是 EtcdRestore 资源的名字。
// 这个标签表示这个 EtcdCluster 是由哪个恢复操作创建的。
func markCluster(restore etcdv1alpha1.EtcdRestore, cluster *etcdv1alpha1.EtcdCluster) {
	if cluster.Labels == nil {
		cluster.Labels = make(map[string]string)
	}
	cluster.Labels[restoredFromLabel] = restore.Name
}

func (r *EtcdRestoreReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := r.Log.WithValues("etcdrestore", req.NamespacedName)

	log.Info("Begin restore reconcile")

	//获取 EtcdRestore 资源。如果找不到资源，可能是因为它已被删除，那么就没有其他操作需要执行。
	var restore etcdv1alpha1.EtcdRestore
	if err := r.Get(ctx, req.NamespacedName, &restore); err != nil {
		// Can't find the resource. We could have just been deleted? If so, no need to do anything as the owner
		// references will clean everything up with a cascading delete.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	} else {
		log.Info("Found restore object in Kubernetes, continuing")
		// Found the resource, continue with reconciliation.
	}

	//检查 EtcdRestore 的状态。如果状态已经是 Completed 或 Failed，则不需要执行任何操作。如果状态是 Pending，则继续执行恢复操作。
	if restore.Status.Phase == etcdv1alpha1.EtcdRestorePhaseCompleted ||
		restore.Status.Phase == etcdv1alpha1.EtcdRestorePhaseFailed {
		// Do nothing here. We're already finished.
		log.Info("Phase is set to an end state. Taking no further action", "phase", restore.Status.Phase)
		return ctrl.Result{}, nil
	} else if restore.Status.Phase == etcdv1alpha1.EtcdRestorePhasePending {
		// In any other state continue with the reconciliation. In particular we don't *read* the other possible states
		// as once we know that we need to reconcile at all we will use the observed sate of the cluster and not our own
		// status field.
		log.Info("Phase is not set to an end state. Continuing reconciliation.", "phase", restore.Status.Phase)
	} else {
		log.Info("Restore has no phase, setting to pending")
		restore.Status.Phase = etcdv1alpha1.EtcdRestorePhasePending
		err := r.Client.Status().Update(ctx, &restore)
		return ctrl.Result{}, err
	}

	// Simulate the cluster, peer, and PVC we'll create. We won't ever *create* any of these other than the PVC, but
	// we need them to generate the PVC using the same code-path that the main operator does. Yes, in theory if you
	// upgraded the operator part-way through a restore things could get super fun if the way of making these changed.
	//模拟将要创建的 EtcdCluster，EtcdPeer 和 PersistentVolumeClaim（PVC）。这些资源并不会真正被创建（除了 PVC），但是需要它们来生成 PVC。
	expectedCluster := clusterForRestore(restore)
	expectedCluster.Default()

	expectedPeerNames := expectedPeerNamesForCluster(expectedCluster)
	expectedPeers := make([]etcdv1alpha1.EtcdPeer, len(expectedPeerNames))
	for i, peerName := range expectedPeerNames {
		expectedPeers[i] = *peerForCluster(expectedCluster, peerName)
		expectedPeers[i].Default()
		configurePeerBootstrap(&expectedPeers[i], expectedCluster)
	}
	expectedPVCs := make([]corev1.PersistentVolumeClaim, len(expectedPeers))
	for i, peer := range expectedPeers {
		expectedPVCs[i] = *pvcForPeer(&peer)
		markPVC(restore, &expectedPVCs[i])
	}

	//创建 PVC。对于每个预期的 PVC，检查它是否已经存在。如果不存在，就创建它。如果已经存在，检查它是否属于当前的恢复操作。
	//对于预期创建的每一个 PVC（expectedPVCs），首先尝试获取它。这里的 expectedPVCs 是根据恢复操作预期的 EtcdPeer 资源生成的。
	// Step 1. Create the PVCs.
	for _, expectedPVC := range expectedPVCs {
		var pvc corev1.PersistentVolumeClaim
		err := r.Get(ctx, name(&expectedPVC), &pvc)

		//如果 PVC 不存在（返回 NotFound 错误），则创建它。创建成功后，会记录一个事件，表示已经创建了 PVC。
		if apierrors.IsNotFound(err) {
			// No PVC. Make it!
			log.Info("Creating PVC", "pvc-name", name(&expectedPVC))
			err := r.Create(ctx, &expectedPVC)
			if err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(&restore,
				reconcilerevent.K8sEventTypeNormal,
				pvcCreatedEventReason,
				"Created PVC '%s/%s'",
				pvc.Namespace,
				pvc.Name)
			return ctrl.Result{}, nil
			//如果在获取 PVC 时遇到其他错误（非 NotFound 错误），则直接返回错误，因为这种情况无法处理。
		} else if err != nil {
			// There was some other, non-notfound error. Exit as we can't handle this case.
			log.Info("Encountered error while finding PVC")
			return ctrl.Result{}, err
		}
		//如果 PVC 已经存在，那么就需要验证它是否属于当前的恢复操作。这是通过检查 PVC 的标签来完成的。
		//如果 PVC 的 restoredFromLabel 标签的值等于当前恢复操作的名称，那么就认为这个 PVC 属于当前的恢复操作。
		log.Info("PVC already exists. continuing", "pvc-name", name(&pvc))
		// The PVC is there already. Continue.

		// Check to make sure we're expecting to restore into this PVC
		//如果 PVC 不属于当前的恢复操作，那么就不能对它进行操作，因为这可能会影响到其他的恢复操作或者现有的集群。这种情况下，会记录一个事件，并将恢复操作的状态设置为 Failed。
		if !IsOurPVC(restore, pvc) {
			// It's unsafe to take any further action. We don't control the PVC.
			log.Info("PVC is not marked as our PVC. Failing", "pvc-name", name(&pvc))
			// Construct error reason string
			var reason string
			if restoredFrom, found := pvc.Labels[restoredFromLabel]; found {
				reason = fmt.Sprintf("Label %s indicates a different restore %s made this PVC.",
					restoredFromLabel,
					restoredFrom)
			} else {
				reason = fmt.Sprintf("Label %s not set, indicating this is a pre-existing PVC.",
					restoredFromLabel)
			}
			r.Recorder.Eventf(&restore,
				reconcilerevent.K8sEventTypeWarning,
				pvcInvalidEventReason,
				"Found a PVC '%s/%s' as expected, but it wasn't labeled as ours: %s",
				pvc.Namespace,
				pvc.Name,
				reason)
			restore.Status.Phase = etcdv1alpha1.EtcdRestorePhaseFailed
			err := r.Client.Status().Update(ctx, &restore)
			return ctrl.Result{}, err
		}
		log.Info("PVC correctly marked as ours", "pvc-name", name(&pvc))
		// Great, this PVC is marked as *our* restore (and not any random PVC, or one for an existing cluster, or
		// a conflicting restore).
	}

	//如果 PVC 属于当前的恢复操作，那么就可以继续后续的操作。
	// So the peer restore is like a distributed fork/join. We need to launch n restore pods, where n is the number of
	// PVCs. Then wait for all of them to be complete. If any fail we abort.

	log.Info("Searching for restore pods")
	restorePods := make([]*corev1.Pod, len(expectedPeerNames))
	// Go find the restore pods for each peer
	for i, peer := range expectedPeers {
		log.Info("Searching for restore pod for peer", "peer-name", peer.Name)
		pod := corev1.Pod{}
		err := r.Get(ctx, restorePodNamespacedName(peer), &pod)
		if client.IgnoreNotFound(err) != nil {
			// This was a non-notfound error. Fail!
			return ctrl.Result{}, err
		} else if err == nil {
			restorePods[i] = &pod
		}
	}

	//查找和验证恢复 Pod。对于每个预期的恢复 Pod，检查它是否已经存在。如果不存在，就创建它。如果已经存在，检查它是否属于当前的恢复操作。
	log.Info("Finished querying for restore pods")
	// Verify each pod we found is actually ours
	for _, restorePod := range restorePods {
		if restorePod != nil && !IsOurPod(restore, *restorePod) {
			// Construct error reason string
			var reason string
			if restoredFrom, found := restorePod.Labels[restoredFromLabel]; found {
				reason = fmt.Sprintf("Label %s indicates a different restore %s made this Pod.",
					restoredFromLabel,
					restoredFrom)
			} else {
				reason = fmt.Sprintf("Label %s not set, indicating this is a pre-existing Pod.",
					restoredFromLabel)
			}
			r.Recorder.Eventf(&restore,
				reconcilerevent.K8sEventTypeWarning,
				podInvalidEventReason,
				"Found a Pod '%s/%s' as expected, but it wasn't labeled as ours: %s",
				restorePod.Namespace,
				restorePod.Name,
				reason)
			log.Info("Restore Pod isn't ours", "pod-name", name(restorePod))
			restore.Status.Phase = etcdv1alpha1.EtcdRestorePhaseFailed
			err := r.Client.Status().Update(ctx, &restore)
			return ctrl.Result{}, err
		}
	}

	//检查所有的恢复 Pod 是否已经完成。如果所有的 Pod 都成功完成，就创建 EtcdCluster。如果有任何一个 Pod 失败，就标记恢复操作为失败。如果还有 Pod 在运行，就等待它们完成。
	log.Info("Verified all existing restore pods are ours")
	// Launch any that are not found
	for i, restorePod := range restorePods {
		if restorePod == nil {
			// We couldn't find this restore pod. Create it.
			log.Info("Defining restore pod", "pod-index", i)
			toCreatePod := r.podForRestore(restore, expectedPeers[i], expectedPVCs[i].Name)
			log.Info("Launching restore pod", "pod-name", name(toCreatePod))
			// No Pod. Launch it! We then exit. Next time we'll make the next one.
			err := r.Create(ctx, toCreatePod)
			if err != nil {
				return ctrl.Result{}, err
			}
			r.Recorder.Eventf(&restore,
				reconcilerevent.K8sEventTypeNormal,
				podCreatedEventReason,
				"Launched Pod '%s/%s' to restore data to PVC '%s/%s'",
				toCreatePod.Namespace,
				toCreatePod.Name,
				expectedPVCs[i].Namespace,
				expectedPVCs[i].Name)
			return ctrl.Result{}, nil
		}
	}
	log.Info("All restore pods are launched")
	// All restore pods exist and our ours, so have they finished? This would be the join part of the "funky fork/join".
	anyFailed := false
	allSuccess := true
	for _, restorePod := range restorePods {
		phase := restorePod.Status.Phase
		if phase == corev1.PodSucceeded {
			// Success is what we want. Continue with reconciliation.
			log.Info("Restore Pod has succeeded", "pod-name", name(restorePod))
		} else if phase == corev1.PodFailed || phase == corev1.PodUnknown {
			// Pod has failed, so we fail
			log.Info("Restore Pod was not successful", "pod-name", name(restorePod), "phase", phase)
			allSuccess = false
			anyFailed = true
		} else {
			// This covers the "Pending" and "Running" phases. Do nothing and wait for the Pod to finish.
			log.Info("Restore Pod still running.", "pod-name", name(restorePod), "phase", phase)
			allSuccess = false
		}
	}

	if anyFailed {
		log.Info("One or more restore pods were not successful. Marking the restore as failed.")
		restore.Status.Phase = etcdv1alpha1.EtcdRestorePhaseFailed
		err := r.Client.Status().Update(ctx, &restore)
		if err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Event(&restore,
			reconcilerevent.K8sEventTypeWarning,
			podFailedEventReason,
			"One or more Pods failed, please inspect the Pod logs for further details.")
		return ctrl.Result{}, err
	}
	if !allSuccess {
		log.Info("One or more restore pods are still executing, and none have failed. Waiting...")
		// Hrm. Not all finished yet. Exit and do nothing.
		return ctrl.Result{}, nil
	}

	//创建 EtcdCluster。如果 EtcdCluster 不存在，就创建它。
	// Create the cluster
	log.Info("All restore pods are complete, creating etcd cluster")
	cluster := etcdv1alpha1.EtcdCluster{}
	err := r.Get(ctx, name(expectedCluster), &cluster)
	if apierrors.IsNotFound(err) {
		// No Cluster. Create it
		log.Info("Creating new cluster", "cluster-name", name(expectedCluster))
		err := r.Create(ctx, expectedCluster)
		if err != nil {
			return ctrl.Result{}, err
		}
		r.Recorder.Eventf(&restore,
			reconcilerevent.K8sEventTypeNormal,
			clusterCreatedEventReason,
			"Created EtcdCluster '%s/%s'.",
			expectedCluster.Namespace,
			expectedCluster.Name)
		return ctrl.Result{}, err
	} else if err != nil {
		log.Info("Error creating cluster", "cluster-name", name(expectedCluster))
		return ctrl.Result{}, err
	}

	//更新 EtcdRestore 的状态为 Completed。
	// This is the end. Our cluster exists.
	log.Info("Verified cluster exists, exiting.")
	restore.Status.Phase = etcdv1alpha1.EtcdRestorePhaseCompleted
	err = r.Client.Status().Update(ctx, &restore)
	return ctrl.Result{}, err
}

// 这个函数根据给定的 EtcdPeer 资源生成一个恢复 Pod 的名称，名称的格式为 "{peer-name}-restore"。
func restorePodName(peer etcdv1alpha1.EtcdPeer) string {
	return fmt.Sprintf("%s-restore", peer.GetName())
}

// 这个函数根据给定的 EtcdPeer 资源生成一个恢复 Pod 的 NamespacedName 对象，其中包含了 Pod 的名称和命名空间。
func restorePodNamespacedName(peer etcdv1alpha1.EtcdPeer) types.NamespacedName {
	return types.NamespacedName{
		Namespace: peer.GetNamespace(),
		Name:      restorePodName(peer),
	}
}

// 这个函数从给定的 URL 中移除端口号。
func stripPortFromURL(advertiseURL *url.URL) string {
	return strings.Split(advertiseURL.Host, ":")[0]
}

// 这个函数根据给定的 EtcdRestore 资源、EtcdPeer 资源和 PVC 的名称，创建一个用于恢复的 Pod。这个 Pod 会被配置为从备份中恢复数据到 PVC。
func (r *EtcdRestoreReconciler) podForRestore(restore etcdv1alpha1.EtcdRestore, peer etcdv1alpha1.EtcdPeer, pvcName string) *corev1.Pod {
	const snapshotDir = "/tmp/snapshot"
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            restorePodName(peer),
			Namespace:       restore.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&restore, etcdv1alpha1.GroupVersion.WithKind("EtcdRestore"))},
		},
		Spec: corev1.PodSpec{
			HostAliases: []corev1.HostAlias{
				{
					// We've got a problematic Catch-22 here. We need to set the peer advertise URL to be the real URL
					// that the eventual peer will use. We know that URL at this point (and provide it as an environment
					// variable later on).
					//
					// However without the Kubernetes service to provide DNS and the Hostnames set on the pods the URL
					// can't actually be resolved. Unfortunately, the etcd API will try to resolve the advertise URL to
					// check it's real.
					//
					// So, we need to fake it so it resolves to something. It doesn't need to have anything on the other
					// end. So long as an IP address comes back the etcd API is satisfied.
					IP: net.IPv4(127, 0, 0, 1).String(),
					Hostnames: []string{
						stripPortFromURL(advertiseURL(peer, etcdPeerPort)),
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "etcd-data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
				{
					// This is scratch space to download the snapshot file into
					Name: "snapshot",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: nil,
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name:      restoreContainerName,
					Image:     r.RestorePodImage,
					Args:      []string{},
					Resources: *restore.Spec.ClusterTemplate.Spec.PodTemplate.Resources,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "etcd-data",
							ReadOnly:  false,
							MountPath: etcdDataMountPath,
						},
						{
							Name:      "snapshot",
							ReadOnly:  false,
							MountPath: snapshotDir,
						},
					},
					// TODO: Add resource requests and affinity rules which
					// match the eventual cluster peer pod.
					// See https://github.com/improbable-eng/etcd-cluster-operator/issues/172
				},
			},
			// If we fail, we fail
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	// Create a helper to append flags
	stringFlag := func(flag string, value string) {
		// We know there's only one container and it's the first in the list
		pod.Spec.Containers[0].Args = append(pod.Spec.Containers[0].Args, fmt.Sprintf("--%s=%s", flag, value))
	}
	stringFlag("etcd-peer-name", peer.Name)
	stringFlag("etcd-cluster-name", restore.Spec.ClusterTemplate.ClusterName)
	stringFlag("etcd-initial-cluster", staticBootstrapInitialCluster(*peer.Spec.Bootstrap.Static))
	stringFlag("etcd-peer-advertise-url", advertiseURL(peer, etcdPeerPort).String())
	stringFlag("etcd-data-dir", etcdDataMountPath)
	stringFlag("snapshot-dir", snapshotDir)
	stringFlag("backup-url", restore.Spec.Source.ObjectURL)
	stringFlag("proxy-url", r.ProxyURL)

	markPod(restore, &pod)
	return &pod
}

// 这个函数根据给定的 EtcdRestore 资源创建一个 EtcdCluster 资源。这个 EtcdCluster 资源的配置会基于 EtcdRestore 中的 ClusterTemplate。
func clusterForRestore(restore etcdv1alpha1.EtcdRestore) *etcdv1alpha1.EtcdCluster {
	cluster := &etcdv1alpha1.EtcdCluster{
		ObjectMeta: v1.ObjectMeta{
			Name:      restore.Spec.ClusterTemplate.ClusterName,
			Namespace: restore.Namespace,
		},
		Spec: restore.Spec.ClusterTemplate.Spec,
	}
	// Slap a label on it. This is pretty informational. Just so a user can tell where this thing came from later on.
	markCluster(restore, cluster)
	return cluster
}

// 这是一个实现了 handler.Mapper 接口的结构体。
// 它的 Map 方法会根据给定的对象（如 PVC 或 EtcdCluster）的标签生成一个或多个 reconcile 请求。
// 这些请求会触发 EtcdRestore 的 Reconcile 方法。
type restoredFromMapper struct{}

var _ handler.Mapper = &restoredFromMapper{}

// Map looks up the peer name label from the PVC and generates a reconcile
// request for *that* name in the namespace of the pvc.
// This mapper ensures that we only wake up the Reconcile function for changes
// to PVCs related to EtcdPeer resources.
// PVCs are deliberately not owned by the peer, to ensure that they are not
// garbage collected along with the peer.
// So we can't use OwnerReference handler here.

// 在 Kubernetes 的控制器模型中，控制器通过监听（watch）特定的 Kubernetes 资源（如 Pod、PVC、自定义资源等）的变化来进行工作。
// 当这些资源发生变化时，控制器会收到一个通知，然后执行相应的逻辑。
//
// 在你提供的代码片段中，restoredFromMapper 结构体实现了 handler.Mapper 接口。
// 这个接口的 Map 方法会根据给定的 Kubernetes 资源（通过 handler.MapObject 参数传入）生成一个或多个 reconcile.Request。
// 这些请求会触发控制器的 Reconcile 方法。
//
// 具体来说，Map 方法首先获取资源的标签，然后检查是否存在 restoredFromLabel 标签。
// 如果存在，就会为该标签对应的值（即恢复的名称）创建一个 reconcile.Request，并将其添加到请求列表中。这个请求会触发对应名称的 EtcdRestore 资源的 Reconcile 方法。
//
// 这种模式使得控制器可以在相关资源发生变化时做出响应，从而实现各个控制器之间的联动。
// 例如，当一个 PVC 或 EtcdCluster 资源发生变化时，可以触发对应的 EtcdRestore 资源进行恢复操作。
func (m *restoredFromMapper) Map(o handler.MapObject) []reconcile.Request {
	var requests []reconcile.Request
	labels := o.Meta.GetLabels()
	if restoreName, found := labels[restoredFromLabel]; found {
		requests = append(
			requests,
			reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      restoreName,
					Namespace: o.Meta.GetNamespace(),
				},
			},
		)
	}
	return requests
}

// 这个方法用于将 EtcdRestoreReconciler 与给定的控制器管理器（Manager）关联起来。
// 它会设置一些观察者（Watchers），用于监听 EtcdRestore 资源、EtcdRestore 所拥有的 Pod 资源，
// 以及带有 'restored-from' 标签的 PVC 和 EtcdCluster 资源的变化。当这些资源发生变化时，会触发 EtcdRestore 的 Reconcile 方法。
func (r *EtcdRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdRestore{}).
		// Watch for changes to Pod resources that an EtcdRestore owns.
		Owns(&corev1.Pod{}).
		// Watch for changes to PVCs with a 'restored-from' label.
		Watches(&source.Kind{Type: &corev1.PersistentVolumeClaim{}}, &handler.EnqueueRequestsFromMapFunc{
			ToRequests: &restoredFromMapper{},
		}).
		Watches(&source.Kind{Type: &etcdv1alpha1.EtcdCluster{}}, &handler.EnqueueRequestsFromMapFunc{
			ToRequests: &restoredFromMapper{},
		}).
		Complete(r)
}
