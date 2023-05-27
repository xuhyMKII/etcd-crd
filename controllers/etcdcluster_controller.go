package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	etcdclient "go.etcd.io/etcd/client"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
	"github.com/improbable-eng/etcd-cluster-operator/internal/etcd"
	"github.com/improbable-eng/etcd-cluster-operator/internal/reconcilerevent"
)

const (
	clusterNameSpecField = "spec.clusterName"
)

// 这是一个重要的结构体，它实现了对 EtcdCluster 的 reconcile 操作。它包含一个 client.Client 成员，用于与 Kubernetes API 交互。
// EtcdClusterReconciler reconciles a EtcdCluster object
type EtcdClusterReconciler struct {
	client.Client
	Log      logr.Logger
	Recorder record.EventRecorder
	Etcd     etcd.APIBuilder
}

func headlessServiceForCluster(cluster *etcdv1alpha1.EtcdCluster) *v1.Service {
	name := serviceName(cluster)
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name.Name,
			Namespace: name.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, etcdv1alpha1.GroupVersion.WithKind("EtcdCluster")),
			},
			Labels: map[string]string{
				appLabel:     appName,
				clusterLabel: cluster.Name,
			},
		},
		Spec: v1.ServiceSpec{
			ClusterIP:                v1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector: map[string]string{
				appLabel:     appName,
				clusterLabel: cluster.Name,
			},
			Ports: []v1.ServicePort{
				{
					Name:     "etcd-client",
					Protocol: "TCP",
					Port:     etcdClientPort,
				},
				{
					Name:     "etcd-peer",
					Protocol: "TCP",
					Port:     etcdPeerPort,
				},
			},
		},
	}
}

func serviceName(cluster *etcdv1alpha1.EtcdCluster) types.NamespacedName {
	return types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}
}

// hasService determines if the Kubernetes Service exists. Error is returned if there is some problem communicating with
// Kubernetes.
func (r *EtcdClusterReconciler) hasService(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (bool, error) {
	service := &v1.Service{}
	err := r.Get(ctx, serviceName(cluster), service)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// We got the expected error, which is that it's not found
			return false, nil
		}
		// Unexpected error, some other problem?
		return false, err
	}
	// We found it because we got no error
	return true, nil
}

// createService makes the headless service in Kubernetes
func (r *EtcdClusterReconciler) createService(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster) (*reconcilerevent.ServiceCreatedEvent, error) {
	service := headlessServiceForCluster(cluster)
	if err := r.Create(ctx, service); err != nil {
		return nil, err
	}
	return &reconcilerevent.ServiceCreatedEvent{Object: cluster, ServiceName: service.Name}, nil
}

// 这段代码是用来创建一个新的EtcdPeer资源的，这个新的EtcdPeer资源将作为Etcd集群的一个新成员。这个函数的执行流程大致如下：
//
// 首先，它会计算出一个可用的EtcdPeer资源的名字。这个名字是根据EtcdCluster资源和现有的EtcdPeer资源列表来计算的，以确保名字的唯一性。
//
// 然后，它会创建一个新的EtcdPeer资源对象，并设置这个对象的属性，使其能够作为EtcdCluster资源的一个成员。
//
// 接着，它会调用configurePeerBootstrap函数来配置EtcdPeer资源的引导参数。这些参数是用来启动Etcd服务器的。
//
// 然后，它会调用Kubernetes API来创建EtcdPeer资源。如果创建失败，它会返回一个错误。
//
// 最后，如果EtcdPeer资源成功创建，它会返回一个表示EtcdPeer资源已创建的事件。
//
// 在整个项目中，这个函数的作用是在Etcd集群需要扩容时，创建一个新的EtcdPeer资源，以便启动一个新的Etcd服务器。
func (r *EtcdClusterReconciler) createBootstrapPeer(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, peers *etcdv1alpha1.EtcdPeerList) (*reconcilerevent.PeerCreatedEvent, error) {
	peerName := nextAvailablePeerName(cluster, peers.Items)
	peer := peerForCluster(cluster, peerName)
	configurePeerBootstrap(peer, cluster)
	err := r.Create(ctx, peer)
	if err != nil {
		return nil, err
	}
	return &reconcilerevent.PeerCreatedEvent{Object: cluster, PeerName: peer.Name}, nil
}

func (r *EtcdClusterReconciler) hasPeerForMember(peers *etcdv1alpha1.EtcdPeerList, member etcdclient.Member) (bool, error) {
	expectedPeerName, err := peerNameForMember(member)
	if err != nil {
		return false, err
	}
	for _, peer := range peers.Items {
		if expectedPeerName == peer.Name {
			return true, nil
		}
	}
	return false, nil
}

func (r *EtcdClusterReconciler) createPeerForMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, members []etcdclient.Member, member etcdclient.Member) (*reconcilerevent.PeerCreatedEvent, error) {
	// Use this instead of member.Name. If we've just added the peer for this member then the member might not
	// have that field set, so instead figure it out from the peerURL.
	expectedPeerName, err := peerNameForMember(member)
	if err != nil {
		return nil, err
	}
	peer := peerForCluster(cluster, expectedPeerName)
	configureJoinExistingCluster(peer, cluster, members)

	err = r.Create(ctx, peer)
	if err != nil {
		return nil, err
	}
	return &reconcilerevent.PeerCreatedEvent{Object: cluster, PeerName: peer.Name}, nil
}

func (r *EtcdClusterReconciler) addNewMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, peers *etcdv1alpha1.EtcdPeerList) (*reconcilerevent.MemberAddedEvent, error) {
	peerName := nextAvailablePeerName(cluster, peers.Items)
	peerURL := &url.URL{
		Scheme: etcdScheme,
		Host:   fmt.Sprintf("%s:%d", expectedURLForPeer(cluster, peerName), etcdPeerPort),
	}
	member, err := r.addEtcdMember(ctx, cluster, peerURL.String())
	if err != nil {
		return nil, err
	}
	return &reconcilerevent.MemberAddedEvent{Object: cluster, Member: member, Name: peerName}, nil
}

// MembersByName provides a sort.Sort interface for etcdClient.Member.Name
type MembersByName []etcdclient.Member

func (a MembersByName) Len() int           { return len(a) }
func (a MembersByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MembersByName) Less(i, j int) bool { return a[i].Name < a[j].Name }

// removeMember 选择名称序数最大的 Etcd 节点，
// 然后执行运行时重新配置以从 Etcd 中删除该成员
// 集群，并生成一个事件来记录该操作。
// TODO(wallrj) 考虑删除非领导成员以避免破坏
// 缩减时领导者选举。
// 请参阅 https://github.com/improbable-eng/etcd-cluster-operator/issues/97
func (r *EtcdClusterReconciler) removeMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, members []etcdclient.Member) (*reconcilerevent.MemberRemovedEvent, error) {
	sortedMembers := append(members[:0:0], members...)
	sort.Sort(sort.Reverse(MembersByName(sortedMembers)))
	member := sortedMembers[0]
	err := r.removeEtcdMember(ctx, cluster, member.ID)
	if err != nil {
		return nil, err
	}
	return &reconcilerevent.MemberRemovedEvent{Object: cluster, Member: &member, Name: member.Name}, nil
}

// updateStatus 将 EtcdCluster 资源的状态更新为集群的当前值。
func (r *EtcdClusterReconciler) updateStatus(
	cluster *etcdv1alpha1.EtcdCluster,
	members *[]etcdclient.Member,
	peers *etcdv1alpha1.EtcdPeerList,
	clusterVersion string) {

	if members != nil {
		cluster.Status.Members = make([]etcdv1alpha1.EtcdMember, len(*members))
		for i, member := range *members {
			cluster.Status.Members[i] = etcdv1alpha1.EtcdMember{
				Name: member.Name,
				ID:   member.ID,
			}
		}
	} else {
		if cluster.Status.Members == nil {
			cluster.Status.Members = make([]etcdv1alpha1.EtcdMember, 0)
		} else {
			// In this case we don't have up-to date members for whatever reason, which could be a temporary disruption
			// such as a network issue. But we also know the status on the resource already has a member list. So just
			// leave that (stale) data where it is and don't change it.
		}
	}
	cluster.Status.Replicas = int32(len(peers.Items))
	cluster.Status.ClusterVersion = clusterVersion
}

// 这是 EtcdClusterReconciler 的核心方法，它实现了对 EtcdCluster 的 reconcile 操作。
// 这个方法首先获取 EtcdCluster 对象，然后根据 EtcdCluster 的状态进行相应的操作，如创建或更新 etcd pods，更新 EtcdCluster 的状态等。
// reconcile (little r) does the 'dirty work' of deciding if we wish to perform an action upon the Kubernetes cluster to
// match our desired state. We will always perform exactly one or zero actions. The intent is that when multiple actions
// are required we will do only one, then requeue this function to perform the next one. This helps to keep the code
// simple, as every 'go around' only performs a single change.
//
// We strictly use the term "member" to refer to an item in the etcd membership list, and "peer" to refer to an
// EtcdPeer resource created by us that ultimately creates a pod running the etcd binary.
//
// In order to allow other parts of this operator to know what we've done, we return a `reconcilerevent.ReconcilerEvent`
// that should describe to the rest of this codebase what we have done.
func (r *EtcdClusterReconciler) reconcile(
	ctx context.Context,
	members *[]etcdclient.Member,
	peers *etcdv1alpha1.EtcdPeerList,
	cluster *etcdv1alpha1.EtcdCluster,
) (
	ctrl.Result,
	reconcilerevent.ReconcilerEvent,
	error,
) {
	// Use a logger enriched with information about the cluster we'er reconciling. Note we generally do not log on
	// errors as we pass the error up to the manager anyway.
	log := r.Log.WithValues("cluster", types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	})

	// Always requeue after ten seconds, as we don't watch on the membership list. So we don't auto-detect changes made
	// to the etcd membership API.
	// TODO(#76) Implement custom watch on etcd membership API, and remove this `requeueAfter`
	result := ctrl.Result{RequeueAfter: time.Second * 10}

	// The first port of call is to verify that the service exists and create it if it does not.
	//
	// This service is headless, and is designed to provide the underlying etcd pods with a consistent network identity
	// such that they can dial each other. It *can* be used for client access to etcd if a user desires, but users are
	// also encouraged to create their own services for their client access if that is more convenient.
	serviceExists, err := r.hasService(ctx, cluster)
	if err != nil {
		return result, nil, fmt.Errorf("unable to fetch service from Kubernetes API: %w", err)
	}
	if !serviceExists {
		serviceCreatedEvent, err := r.createService(ctx, cluster)
		if err != nil {
			return result, nil, fmt.Errorf("unable to create service: %w", err)
		}
		log.V(1).Info("Created Service", "service", serviceCreatedEvent.ServiceName)
		return result, serviceCreatedEvent, nil
	}

	// There are two big branches of behaviour: Either we (the operator) can contact the etcd cluster, or we can't. If
	// the `members` pointer is nil that means we were unsuccessful in our attempts to connect. Otherwise it indicates
	// a list of existing members.
	if members == nil {
		// We don't have communication with the cluster. This could be for a number of reasons, for example:
		//
		// * The cluster isn't up yet because we've not yet made the peers (during bootstrapping)
		// * Pod rescheduling due to node failure or administrator action has made the cluster non-quorate
		// * Network disruption inside the Kubernetes cluster has prevented us from connecting
		//
		// We treat the member list from etcd as a canonical list of what members should exist, so without it we're
		// unable to reconcile what peers we should create. Most of the cases listed above will be solved by outside
		// action. Either a network disruption will heal, or the scheduler will reschedule etcd pods. So no action is
		// required.
		//
		// Bootstrapping is a notable exception. We have to create our peer resources so that the pods will be created
		// in the first place. Which for us presents a 'chicken-and-egg' problem. Our solution is to take special
		// action during a bootstrapping situation. We can't accurately detect that the cluster has actually just been
		// bootstrapped in a reliable way (e.g., searching for the creation time on the cluster resource we're
		// reconciling). So our 'best effort' is to see if our desired peers is less than our actual peers. If so, we
		// just create new peers with bootstrap instructions.
		//
		// This may mean that we take a 'strange' action in some cases, such as a network disruption during a scale-up
		// event where we may add peers without first adding them to the etcd internal membership list (like we would
		// during normal operation). However, this situation will eventually heal:
		//
		// 1. We incorrectly create a blank peer
		// 2. The network split heals
		// 3. The cluster rejects the peer as it doesn't know about it
		// 4. The operator reconciles, and removes the peer that the membership list doesn't see
		// 5. We enter the normal scale-up behaviour, adding the peer to the membership list *first*.
		//
		// We are also careful to never *remove* a peer in this 'no contact' state. So while we may erroneously add
		// peers in some edge cases, we will never remove a peer without first confirming the action is correct with
		// the cluster.

		//如果无法与 Etcd 集群通信（例如，集群还未启动，或者网络中断），
		//那么这个方法会检查当前的 EtcdPeer 资源数量是否少于期望的副本数量。
		//如果是，那么会创建一个新的 EtcdPeer 资源。这个逻辑主要是为了处理集群启动的情况。
		if hasTooFewPeers(cluster, peers) {
			// We have fewer peers than our replicas, so create a new peer. We do this one at a time, so only make
			// *one* peer.
			peerCreatedEvent, err := r.createBootstrapPeer(ctx, cluster, peers)
			if err != nil {
				return result, nil, fmt.Errorf("failed to create EtcdPeer %w", err)
			}
			log.V(1).Info(
				"Insufficient peers for replicas and unable to contact etcd. Assumed we're bootstrapping created new peer. (If we're not we'll heal this later.)",
				"current-peers", len(peers.Items),
				"desired-peers", cluster.Spec.Replicas,
				"peer", peerCreatedEvent.PeerName)
			return result, peerCreatedEvent, nil
		} else {
			// We can't contact the cluster, and we don't *think* we need more peers. So it's unsafe to take any
			// action.
		}
	} else {
		// In this branch we *do* have communication with the cluster, and have retrieved a list of members. We treat
		// this list as canonical, and therefore reconciliation occurs in two cycles:
		//
		// 1. We reconcile the membership list to match our expected number of replicas (`spec.replicas`), adding
		//    members one at a time until reaching the correct number.
		// 2. We reconcile the number of peer resources to match the membership list.
		//
		// This 'two-tier' process may seem odd, but is designed to mirror etcd's operations guide for adding and
		// removing members. During a scale-up, etcd itself expects that new peers will be announced to it *first* using
		// the membership API, and then the etcd process started on the new machine. During a scale down the same is
		// true, where the membership API should be told that a member is to be removed before it is shut down.
		log.V(2).Info("Cluster communication established")

		//如果可以与 Etcd 集群通信，那么这个方法会首先确保每个 Etcd 集群成员都有对应的 EtcdPeer 资源。然后，如果所有的 Etcd 集群成员都处于稳定状态，这个方法会执行以下操作（按顺序）：
		//
		//删除多余的 EtcdPeer 资源（这些资源对应的 Etcd 集群成员可能已经被删除）。
		//升级 Etcd 集群（删除版本过低的 EtcdPeer 资源，这些资源会在下一次 reconcile 时被重新创建，并使用新的版本）。
		//扩大 Etcd 集群（如果当前的 Etcd 集群成员数量少于期望的副本数量，那么会添加新的成员）。
		//缩小 Etcd 集群（如果当前的 Etcd 集群成员数量多于期望的副本数量，那么会删除多余的成员）。

		// We reconcile the peer resources against the membership list before changing the membership list. This is the
		// opposite way around to what we just said. But is important. It means that if you perform a scale-up to five
		// from three, we'll add the fourth node to the membership list, then add the peer for the fourth node *before*
		// we consider adding the fifth node to the membership list.

		//这段代码是在检查 Etcd 集群的每个成员是否都有对应的 EtcdPeer 资源。
		//如果发现有 Etcd 集群成员没有对应的 EtcdPeer 资源，那么就会为这个成员创建一个新的 EtcdPeer 资源。
		//确保 Etcd 集群的每个成员都有对应的 EtcdPeer 资源。
		//这是因为 EtcdPeer 资源是用来管理 Etcd 集群成员的生命周期的，如果有成员没有对应的 EtcdPeer 资源，那么这个成员的生命周期就无法被正确管理。
		// Add EtcdPeer resources for members that do not have one.
		for _, member := range *members {
			hasPeer, err := r.hasPeerForMember(peers, member)
			if err != nil {
				return result, nil, err
			}
			if !hasPeer {
				// We have found a member without a peer. We should add an EtcdPeer resource for this member
				peerCreatedEvent, err := r.createPeerForMember(ctx, cluster, *members, member)
				if err != nil {
					return result, nil, fmt.Errorf("failed to create EtcdPeer %w", err)
				}
				log.V(1).Info(
					"Found member in etcd's API that has no EtcdPeer resource representation, added one.",
					"member-name", peerCreatedEvent.PeerName)
				return result, peerCreatedEvent, nil
			}
		}

		// If we've reached this point we're sure that there are EtcdPeer resources for every member in the Etcd
		// membership API.
		// We do not perform any other operations until all the EtcdPeer pods have started and
		// all the etcd processes have joined the cluster.
		if isAllMembersStable(*members) {
			// The remaining operations can now be performed. In order:
			// * Remove surplus EtcdPeers
			// * Upgrade Etcd
			// * Scale-up
			// * Scale-down
			//
			// Remove surplus EtcdPeers
			// There may be surplus EtcdPeers for members that have already been removed during a scale-down.
			// Remove EtcdPeer resources which do not have members.

			//这段代码的主要目的是删除那些在etcd集群成员列表中不存在，但在EtcdPeer资源列表中存在的EtcdPeer资源。具体步骤如下：
			//首先，它创建了一个新的字符串集合memberNames，并将etcd集群成员列表中的所有成员名称添加到这个集合中。
			//然后，它遍历EtcdPeer资源列表。对于每一个EtcdPeer资源，它检查该资源的名称是否在memberNames集合中。如果不在，说明这个EtcdPeer资源对应的etcd集群成员已经不存在，因此需要删除这个EtcdPeer资源。
			//在删除EtcdPeer资源之前，它首先检查该资源是否已经被标记为待删除（即检查其DeletionTimestamp字段是否已经被设置）。如果已经被标记为待删除，那么就跳过这个EtcdPeer资源，处理下一个。
			//如果EtcdPeer资源没有被标记为待删除，它会检查该资源是否已经添加了PVC清理的终结器（finalizer）。如果没有，它会添加这个终结器，并更新这个EtcdPeer资源。
			//如果EtcdPeer资源已经添加了PVC清理的终结器，那么就可以安全地删除这个EtcdPeer资源了。删除操作成功后，它会返回一个表示已删除EtcdPeer的事件。
			//在整个项目中，这段代码的作用是确保EtcdPeer资源列表与etcd集群成员列表保持一致。当etcd集群的成员发生变化时（例如，成员被删除），相应的EtcdPeer资源也应该被删除。

			memberNames := sets.NewString()
			for _, member := range *members {
				memberNames.Insert(member.Name)
			}

			for _, peer := range peers.Items {
				if !memberNames.Has(peer.Name) {
					if !peer.DeletionTimestamp.IsZero() {
						log.V(2).Info("Peer is already marked for deletion", "peer", peer.Name)
						continue
					}
					if !hasPvcDeletionFinalizer(peer) {
						updated := peer.DeepCopy()
						controllerutil.AddFinalizer(updated, pvcCleanupFinalizer)
						err := r.Patch(ctx, updated, client.MergeFrom(&peer))
						if err != nil {
							return result, nil, fmt.Errorf("failed to add PVC cleanup finalizer: %w", err)
						}
						log.V(2).Info("Added PVC cleanup finalizer", "peer", peer.Name)
						return result, nil, nil
					}
					err := r.Delete(ctx, &peer)
					if err != nil {
						return result, nil, fmt.Errorf("failed to remove peer: %w", err)
					}
					peerRemovedEvent := &reconcilerevent.PeerRemovedEvent{
						Object:   &peer,
						PeerName: peer.Name,
					}
					return result, peerRemovedEvent, nil
				}
			}

			// Upgrade Etcd.
			// Remove EtcdPeers which  have a different version than EtcdCluster; in reverse name order, one-at-a-time.
			// The EtcdPeers will be recreated in the next reconcile, with the correct version.

			//这段代码的主要目的是处理Etcd集群的版本升级。具体步骤如下：
			//首先，它调用nextOutdatedPeer函数来获取下一个需要升级的EtcdPeer资源。这个函数会从EtcdPeer资源列表中找出第一个其版本与EtcdCluster资源的版本不一致的EtcdPeer资源。
			//如果所有的EtcdPeer资源都已经是最新版本（即nextOutdatedPeer函数返回nil），那么就不需要做任何事情，直接返回。
			//如果找到了需要升级的EtcdPeer资源，那么就需要进行一些处理。首先，它会检查这个EtcdPeer资源的版本是否已经与EtcdCluster资源的版本一致。
			//如果一致，说明这个EtcdPeer资源已经升级完成，但可能还没有更新其状态报告，因此需要等待一段时间，然后再重新检查。
			//如果EtcdPeer资源的版本与EtcdCluster资源的版本不一致，那么就需要删除这个EtcdPeer资源，以便重新创建一个新版本的EtcdPeer资源。
			//在删除EtcdPeer资源之前，它会检查这个资源是否已经被标记为待删除（即检查其DeletionTimestamp字段是否已经被设置）。如果已经被标记为待删除，那么就需要等待这个EtcdPeer资源被完全删除，然后再重新检查。
			//如果EtcdPeer资源没有被标记为待删除，那么就可以安全地删除这个EtcdPeer资源了。删除操作成功后，它会返回一个表示已删除EtcdPeer的事件。
			//在整个项目中，这段代码的作用是处理Etcd集群的版本升级。当EtcdCluster资源的版本发生变化时，需要逐个升级EtcdPeer资源，以确保整个Etcd集群都运行在最新的版本上。

			if peer := nextOutdatedPeer(cluster, peers); peer != nil {
				if peer.Spec.Version == cluster.Spec.Version {
					log.Info("Waiting for EtcdPeer to report expected server version", "etcdpeer-name", peer.Name)
					return result, nil, nil
				}
				if !peer.DeletionTimestamp.IsZero() {
					log.Info("Waiting for EtcdPeer be deleted", "etcdpeer-name", peer.Name)
					return result, nil, nil
				}
				log.Info("Deleting EtcdPeer", "etcdpeer-name", peer.Name)
				err := r.Client.Delete(ctx, peer)
				if err != nil {
					return result, nil, fmt.Errorf("failed to delete peer: %s", err)
				}
				peerRemovedEvent := &reconcilerevent.PeerRemovedEvent{
					Object:   peer,
					PeerName: peer.Name,
				}
				return result, peerRemovedEvent, nil
			}

			// It's finally time to see if we need to mutate the membership API to bring it in-line with our expected
			// replicas. There are three cases, we have enough members, too few, or too many. The order we check these
			// cases in is irrelevant as only one can possibly be true.
			//这段代码主要处理Etcd集群的扩容和缩容操作。
			//
			//首先，它检查当前Etcd集群的成员数量是否少于预期的副本数（即EtcdCluster资源的spec.replicas字段）。如果是，那么就需要进行扩容操作。它调用addNewMember函数来添加一个新的成员到Etcd集群，并返回一个表示已添加新成员的事件。如果添加新成员失败，它会返回一个错误。
			//
			//如果当前Etcd集群的成员数量不少于预期的副本数，那么它会检查是否多于预期的副本数。如果是，那么就需要进行缩容操作。它调用removeMember函数来从Etcd集群中移除一个成员，并返回一个表示已移除成员的事件。如果移除成员失败，它会返回一个错误。
			//
			//如果当前Etcd集群的成员数量正好等于预期的副本数，那么就不需要做任何事情。它会记录一条日志，表示预期的副本数与实际的成员数量一致。
			//
			//在整个项目中，这段代码的作用是根据EtcdCluster资源的spec.replicas字段来调整Etcd集群的成员数量，以实现Etcd集群的动态扩容和缩容。
			if hasTooFewMembers(cluster, *members) {
				// Scale-up.
				// There are too few members for the expected number of replicas. Add a new member.
				memberAddedEvent, err := r.addNewMember(ctx, cluster, peers)
				if err != nil {
					return result, nil, fmt.Errorf("failed to add new member: %w", err)
				}

				log.V(1).Info("Too few members for expected replica count, adding new member.",
					"expected-replicas", *cluster.Spec.Replicas,
					"actual-members", len(*members),
					"peer-name", memberAddedEvent.Name,
					"peer-url", memberAddedEvent.Member.PeerURLs[0])
				return result, memberAddedEvent, nil
			} else if hasTooManyMembers(cluster, *members) {
				// Scale-down.
				// There are too many members for the expected number of replicas.
				// Remove the member with the highest ordinal.
				memberRemovedEvent, err := r.removeMember(ctx, cluster, *members)
				if err != nil {
					return result, nil, fmt.Errorf("failed to remove member: %w", err)
				}

				log.V(1).Info("Too many members for expected replica count, removing member.",
					"expected-replicas", *cluster.Spec.Replicas,
					"actual-members", len(*members),
					"peer-name", memberRemovedEvent.Name,
					"peer-url", memberRemovedEvent.Member.PeerURLs[0])

				return result, memberRemovedEvent, nil
			} else {
				// Exactly the correct number of members. Make no change, all is well with the world.
				log.V(2).Info("Expected number of replicas aligned with actual number.",
					"expected-replicas", *cluster.Spec.Replicas,
					"actual-members", len(*members))
			}
		}
	}
	// This is the fall-through 'all is right with the world' case.
	return result, nil, nil
}

// nextOutdatedPeer returns an EtcdPeer which has a different version than the
// EtcdCluster.
// It searches EtcdPeers in reverse name order.
func nextOutdatedPeer(cluster *etcdv1alpha1.EtcdCluster, peers *etcdv1alpha1.EtcdPeerList) *etcdv1alpha1.EtcdPeer {
	peerNames := make([]string, len(peers.Items))
	peersByName := map[string]etcdv1alpha1.EtcdPeer{}
	for i, peer := range peers.Items {
		name := peer.Name
		peerNames[i] = name
		peersByName[name] = peers.Items[i]
	}
	sort.Sort(sort.Reverse(sort.StringSlice(peerNames)))
	for _, peerName := range peerNames {
		peer, found := peersByName[peerName]
		if !found {
			continue
		}
		if peer.Status.ServerVersion != cluster.Spec.Version {
			return peer.DeepCopy()
		}
	}
	return nil
}

func hasTooFewPeers(cluster *etcdv1alpha1.EtcdCluster, peers *etcdv1alpha1.EtcdPeerList) bool {
	return cluster.Spec.Replicas != nil && int32(len(peers.Items)) < *cluster.Spec.Replicas
}

func hasTooFewMembers(cluster *etcdv1alpha1.EtcdCluster, members []etcdclient.Member) bool {
	return *cluster.Spec.Replicas > int32(len(members))
}

func hasTooManyMembers(cluster *etcdv1alpha1.EtcdCluster, members []etcdclient.Member) bool {
	return *cluster.Spec.Replicas < int32(len(members))
}

func isAllMembersStable(members []etcdclient.Member) bool {
	// We define 'stable' as 'has ever connected', which we detect by looking for the name on the member
	for _, member := range members {
		if member.Name == "" {
			// No name, means it's not up, so the cluster isn't stable.
			return false
		}
	}
	// Didn't find any members with no name, so stable.
	return true
}

func peerNameForMember(member etcdclient.Member) (string, error) {
	if member.Name != "" {
		return member.Name, nil
	} else if len(member.PeerURLs) > 0 {
		// Just take the first
		peerURL := member.PeerURLs[0]
		u, err := url.Parse(peerURL)
		if err != nil {
			return "", err
		}
		return strings.Split(u.Host, ".")[0], nil
	} else {
		return "", errors.New("nothing on member that could give a name")
	}
}

// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdpeers,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

//这段代码是EtcdCluster资源的主要调谐函数。它的主要任务是确保EtcdCluster资源的实际状态与期望状态一致。这个函数的执行流程大致如下：
//
//创建一个带有超时的上下文（context），用于后续的所有Kubernetes API调用。
//
//获取EtcdCluster资源的当前状态。如果获取失败，它会返回一个错误，除非这个错误是因为EtcdCluster资源不存在。
//
//应用EtcdCluster资源的默认值，并进行验证。这是为了防止默认值注入和验证的Webhook没有被部署。
//
//创建EtcdCluster资源的一个深拷贝，以便在函数结束时比较其状态是否发生了变化。如果发生了变化，就需要更新EtcdCluster资源的状态。
//
//尝试连接到Etcd集群，并获取其成员列表和版本信息。如果连接失败，它会记录一条日志，但不会返回错误。
//
//获取EtcdCluster资源对应的所有EtcdPeer资源。如果获取失败，它会返回一个错误。
//
//更新EtcdCluster资源的状态。这一步主要是根据Etcd集群的成员列表和版本信息，以及EtcdPeer资源的列表，来更新EtcdCluster资源的状态。
//
//调用reconcile函数来进行实际的调谐操作。这一步可能会对Etcd集群进行扩容、缩容或升级等操作。
//
//如果reconcile函数返回了一个事件，那么就使用事件记录器（recorder）来记录这个事件。
//
//返回调谐的结果和任何错误。
//
//在整个项目中，这个函数的作用是维护EtcdCluster资源的生命周期，确保Etcd集群的状态与EtcdCluster资源的期望状态一致。

func (r *EtcdClusterReconciler) Reconcile(req ctrl.Request) (_ ctrl.Result, reterr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := r.Log.WithValues("cluster", req.NamespacedName)

	var cluster etcdv1alpha1.EtcdCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Apply defaults in case a defaulting webhook has not been deployed.
	cluster.Default()

	// Validate in case a validating webhook has not been deployed
	err := cluster.ValidateCreate()
	if err != nil {
		log.Error(err, "invalid EtcdCluster")
		return ctrl.Result{}, nil
	}

	original := cluster.DeepCopy()

	// Always attempt to patch the status after each reconciliation.
	defer func() {
		if reflect.DeepEqual(original.Status, cluster.Status) {
			return
		}
		if err := r.Client.Status().Patch(ctx, &cluster, client.MergeFrom(original)); err != nil {
			reterr = kerrors.NewAggregate([]error{reterr, fmt.Errorf("error while patching EtcdCluster.Status: %s ", err)})
		}
	}()

	// Attempt to dial the etcd cluster, recording the cluster response if we can
	var (
		members        *[]etcdclient.Member
		clusterVersion string
	)
	if c, err := r.Etcd.New(etcdClientConfig(&cluster)); err != nil {
		log.V(2).Info("Unable to connect to etcd", "error", err)
	} else {
		if memberSlice, err := c.List(ctx); err != nil {
			log.V(2).Info("Unable to list etcd cluster members", "error", err)
		} else {
			members = &memberSlice
		}
		if version, err := c.GetVersion(ctx); err != nil {
			log.V(2).Info("Unable to get cluster version", "error", err)
		} else {
			clusterVersion = version.Cluster
		}
	}

	// List peers
	var peers etcdv1alpha1.EtcdPeerList
	if err := r.List(ctx,
		&peers,
		client.InNamespace(cluster.Namespace),
		client.MatchingFields{clusterNameSpecField: cluster.Name}); err != nil {
		return ctrl.Result{}, fmt.Errorf("unable to list peers: %w", err)
	}

	// The update status takes in the cluster definition, and the member list from etcd as of *before we ran reconcile*.
	r.updateStatus(&cluster, members, &peers, clusterVersion)

	// Perform a reconcile, getting back the desired result, any utilerrors, and a clusterEvent. This is an internal concept
	// and is not the same as the Kubernetes event, although it is used to make one later.
	result, clusterEvent, err := r.reconcile(ctx, members, &peers, &cluster)
	if err != nil {
		err = fmt.Errorf("Failed to reconcile: %s", err)
	}
	// Finally, the event is used to generate a Kubernetes event by calling `Record` and passing in the recorder.
	if clusterEvent != nil {
		clusterEvent.Record(r.Recorder)
	}

	return result, err
}

func (r *EtcdClusterReconciler) addEtcdMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, peerURL string) (*etcdclient.Member, error) {
	c, err := r.Etcd.New(etcdClientConfig(cluster))
	if err != nil {
		return nil, fmt.Errorf("unable to connect to etcd: %w", err)
	}

	members, err := c.Add(ctx, peerURL)
	if err != nil {
		return nil, fmt.Errorf("unable to add member to etcd cluster: %w", err)
	}

	return members, nil
}

// removeEtcdMember performs a runtime reconfiguration of the Etcd cluster to
// remove a member from the cluster.
func (r *EtcdClusterReconciler) removeEtcdMember(ctx context.Context, cluster *etcdv1alpha1.EtcdCluster, memberID string) error {
	c, err := r.Etcd.New(etcdClientConfig(cluster))
	if err != nil {
		return fmt.Errorf("unable to connect to etcd: %w", err)
	}
	err = c.Remove(ctx, memberID)
	if err != nil {
		return fmt.Errorf("unable to remove member from etcd cluster: %w", err)
	}

	return nil
}

func etcdClientConfig(cluster *etcdv1alpha1.EtcdCluster) etcdclient.Config {
	serviceURL := &url.URL{
		Scheme: etcdScheme,
		// We (the operator) are quite probably in a different namespace to the cluster, so we need to use a fully
		// defined URL.
		Host: fmt.Sprintf("%s.%s:%d", cluster.Name, cluster.Namespace, etcdClientPort),
	}
	return etcdclient.Config{
		Endpoints:               []string{serviceURL.String()},
		Transport:               etcdclient.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second * 1,
	}
}

func peerForCluster(cluster *etcdv1alpha1.EtcdCluster, peerName string) *etcdv1alpha1.EtcdPeer {
	peer := &etcdv1alpha1.EtcdPeer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      peerName,
			Namespace: cluster.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(cluster, etcdv1alpha1.GroupVersion.WithKind("EtcdCluster")),
			},
			Labels: map[string]string{
				appLabel:     appName,
				clusterLabel: cluster.Name,
			},
		},
		Spec: etcdv1alpha1.EtcdPeerSpec{
			ClusterName: cluster.Name,
			Version:     cluster.Spec.Version,
			Storage:     cluster.Spec.Storage.DeepCopy(),
		},
	}

	if cluster.Spec.PodTemplate != nil {
		peer.Spec.PodTemplate = cluster.Spec.PodTemplate
	}

	return peer
}

// configurePeerBootstrap is used during *cluster* bootstrap to configure the peers to be able to see each other.
func configurePeerBootstrap(peer *etcdv1alpha1.EtcdPeer, cluster *etcdv1alpha1.EtcdCluster) {
	peer.Spec.Bootstrap = &etcdv1alpha1.Bootstrap{
		Static: &etcdv1alpha1.StaticBootstrap{
			InitialCluster: initialClusterMembers(cluster),
		},
		InitialClusterState: etcdv1alpha1.InitialClusterStateNew,
	}
}

func configureJoinExistingCluster(peer *etcdv1alpha1.EtcdPeer, cluster *etcdv1alpha1.EtcdCluster, members []etcdclient.Member) {
	peer.Spec.Bootstrap = &etcdv1alpha1.Bootstrap{
		Static: &etcdv1alpha1.StaticBootstrap{
			InitialCluster: initialClusterMembersFromMemberList(cluster, members),
		},
		InitialClusterState: etcdv1alpha1.InitialClusterStateExisting,
	}
}

func nthPeerName(cluster *etcdv1alpha1.EtcdCluster, i int) string {
	return fmt.Sprintf("%s-%d", cluster.Name, i)
}

func initialClusterMembers(cluster *etcdv1alpha1.EtcdCluster) []etcdv1alpha1.InitialClusterMember {
	names := expectedPeerNamesForCluster(cluster)
	members := make([]etcdv1alpha1.InitialClusterMember, len(names))
	for i := range members {
		members[i] = etcdv1alpha1.InitialClusterMember{
			Name: names[i],
			Host: expectedURLForPeer(cluster, names[i]),
		}
	}
	return members
}

func initialClusterMembersFromMemberList(cluster *etcdv1alpha1.EtcdCluster, members []etcdclient.Member) []etcdv1alpha1.InitialClusterMember {
	initialMembers := make([]etcdv1alpha1.InitialClusterMember, len(members))
	for i, member := range members {
		name, err := peerNameForMember(member)
		if err != nil {
			// We were not able to figure out what this peer's name should be. There is precious little we can do here
			// so skip over this member.
			continue
		}
		initialMembers[i] = etcdv1alpha1.InitialClusterMember{
			Name: name,
			// The member actually will always have at least one peer URL set (in the case where we're inspecting a
			// member in the membership list that has never joined the cluster that is the only thing we'll see. But
			// using that is actually painful as it's often strangely escaped. We bypass that issue by regenerating our
			// expected url from the name.
			Host: expectedURLForPeer(cluster, name),
		}
	}
	return initialMembers
}

func nextAvailablePeerName(cluster *etcdv1alpha1.EtcdCluster, peers []etcdv1alpha1.EtcdPeer) string {
	for i := 0; ; i++ {
		candidateName := nthPeerName(cluster, i)
		nameClash := false
		for _, peer := range peers {
			if peer.Name == candidateName {
				nameClash = true
				break
			}
		}
		if !nameClash {
			return candidateName
		}
	}
}

func expectedPeerNamesForCluster(cluster *etcdv1alpha1.EtcdCluster) (names []string) {
	names = make([]string, int(*cluster.Spec.Replicas))
	for i := range names {
		names[i] = nthPeerName(cluster, i)
	}
	return names
}

// expectedURLForPeer returns the Kubernetes-internal DNS name that the given peer can be contacted on, from any
// namespace. This does not include a port or scheme, so can be used as either a "peer" URL using the peer port or the
// client URL using the client port.
func expectedURLForPeer(cluster *etcdv1alpha1.EtcdCluster, peerName string) string {
	return fmt.Sprintf("%s.%s",
		peerName,
		cluster.Name,
	)
}

// 这个方法用于将 EtcdClusterReconciler 添加到 controller manager 中，这样当 EtcdCluster 对象发生变化时，Reconcile 方法就会被调用。
func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(&etcdv1alpha1.EtcdPeer{},
		clusterNameSpecField,
		func(obj runtime.Object) []string {
			peer, ok := obj.(*etcdv1alpha1.EtcdPeer)
			if !ok {
				// Fail? We've been asked to index the cluster name for something that isn't a peer.
				return nil
			}
			return []string{peer.Spec.ClusterName}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdCluster{}).
		Owns(&v1.Service{}).
		Owns(&etcdv1alpha1.EtcdPeer{}).
		Complete(r)
}
