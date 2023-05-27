package controllers

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
)

type CronScheduler interface {
	AddFunc(spec string, cmd func()) (cron.EntryID, error)
	Remove(id cron.EntryID)
}

//etcdbackup_controller_new.go 和 etcdbackupschedule_controller.go 之间的调用关系：
//
//etcdbackupschedule_controller.go 是负责管理 EtcdBackupSchedule CRD 的控制器，
//它的主要职责是根据 EtcdBackupSchedule 的调度策略创建 EtcdBackup CRD。
//当 EtcdBackupSchedule 控制器触发一个备份操作时，它会创建一个新的 EtcdBackup CRD。
//
//etcdbackup_controller_new.go 是负责管理 EtcdBackup CRD 的控制器，
//它的主要职责是根据 EtcdBackup 的状态和其依赖项的状态来决定下一步要执行的操作。
//当 EtcdBackup CRD 被创建时，EtcdBackup 控制器会被触发，
//然后它会创建必要的 Kubernetes 资源（如 ServiceAccount 和 Pod）来执行备份操作。
//
//所以，etcdbackupschedule_controller.go 和 etcdbackup_controller_new.go 之间的调用关系是：EtcdBackupSchedule 控制器创建 EtcdBackup CRD，
//然后 EtcdBackup 控制器响应这个创建操作，执行备份操作。
//
//etcdbackupschedule_controller.go 中的定时任务是通过创建 EtcdBackup CRD 来触发 etcdbackup_controller_new.go 的。
//当定时任务到期，etcdbackupschedule_controller.go 会创建一个新的 EtcdBackup CRD，
//然后 Kubernetes 的控制循环会触发与这个 CRD 相关的控制器，也就是 etcdbackup_controller_new.go，
//然后 etcdbackup_controller_new.go 会执行相应的操作来处理这个新创建的 EtcdBackup CRD。

// 这是 Etcd 备份调度控制器的主要结构体，它包含了 client、log、CronHandler（用于调度 cron 任务）和 Schedules（用于存储备份调度的映射）。
// EtcdBackupScheduleReconciler reconciles a EtcdBackupSchedule object
type EtcdBackupScheduleReconciler struct {
	client.Client
	Log logr.Logger

	// CronHandler is able to schedule cronjobs to occur at given times.
	CronHandler CronScheduler

	// Schedules holds a mapping of resources to the object responsible for scheduling the backup to be taken.
	Schedules *ScheduleMap
}

// 这个结构体包含了一个 cron.EntryID，表示一个 cron 任务的 ID。
type Schedule struct {
	cronEntry cron.EntryID
}

// ScheduleMap is a thread-safe mapping of backup schedules.
type ScheduleMap struct {
	sync.RWMutex
	data map[string]Schedule
}

// 这是一个线程安全的映射，用于存储备份调度。它提供了读、写和删除操作。
func NewScheduleMap() *ScheduleMap {
	return &ScheduleMap{
		RWMutex: sync.RWMutex{},
		data:    map[string]Schedule{},
	}
}

func (s *ScheduleMap) Read(key string) (Schedule, bool) {
	s.RLock()
	defer s.RUnlock()
	value, found := s.data[key]
	return value, found
}

func (s *ScheduleMap) Write(key string, value Schedule) {
	s.Lock()
	defer s.Unlock()
	s.data[key] = value
}

func (s *ScheduleMap) Delete(key string) {
	s.Lock()
	defer s.Unlock()
	delete(s.data, key)
}

//这是控制器的主要方法，它根据 EtcdBackupSchedule 的状态和其依赖项的状态来决定下一步要执行的操作。
//这个方法首先获取 EtcdBackupSchedule 的状态，然后根据这些状态计算出要执行的下一步操作。
//这个操作可能是创建 ServiceAccount、Pod，或者更新 EtcdBackupSchedule 的状态。
//这个方法并不直接调用 Kubernetes API 来创建或更新资源，而是通过执行 action 来完成这些操作。
//这些 action 是通过 CreateRuntimeObject 或 PatchStatus 这样的结构体来实现的，这些结构体实现了 Action 接口，可以执行具体的操作。
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdbackupschedules,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdbackupschedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=etcd.improbable.io,resources=etcdbackups,verbs=create

func (r *EtcdBackupScheduleReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	log := r.Log.WithValues("etcdbackupschedule", req.NamespacedName)

	resource := &etcdv1alpha1.EtcdBackupSchedule{}
	if err := r.Get(ctx, req.NamespacedName, resource); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add a finalizer if one does not exist.
	finalizerIsSet := false
	for _, f := range resource.ObjectMeta.Finalizers {
		if f == scheduleCancelFinalizer {
			finalizerIsSet = true
			break
		}
	}
	if !finalizerIsSet {
		resource.ObjectMeta.Finalizers = append(resource.ObjectMeta.Finalizers, scheduleCancelFinalizer)
		if err := r.Update(ctx, resource); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	schedule, found := r.Schedules.Read(string(resource.UID))
	if !found {
		//在 etcdbackupschedule_controller.go 中，当一个 EtcdBackupSchedule CRD 被创建或更新时，它会设置一个定时任务，
		//这个定时任务会在指定的调度时间触发。当定时任务被触发时，它会执行一个函数，这个函数会创建一个新的 EtcdBackup CRD。
		//这段代码创建了一个新的定时任务，这个任务在 resource.Spec.Schedule 指定的时间触发。
		//触发时，它会执行一个匿名函数，这个函数调用 r.fire(req.NamespacedName) 来创建一个新的 EtcdBackup CRD。
		//EtcdBackupSchedule 控制器会在指定的调度时间创建新的 EtcdBackup CRD，然后 EtcdBackup 控制器会响应这个新创建的 CRD，执行备份操作。
		id, err := r.CronHandler.AddFunc(resource.Spec.Schedule, func() {
			log.Info("Creating EtcdBackup resource")
			err := r.fire(req.NamespacedName)
			if err != nil {
				log.Error(err, "Backup resource creation failed")
			}
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		r.Schedules.Write(string(resource.UID), Schedule{
			cronEntry: id,
		})
		return ctrl.Result{}, nil
	}

	// If the object is being deleted, clean it up from the cron pool.
	if !resource.ObjectMeta.DeletionTimestamp.IsZero() {
		r.CronHandler.Remove(schedule.cronEntry)
		r.Schedules.Delete(string(resource.UID))

		// Remove the finalizer.
		var newFinalizers []string
		for _, f := range resource.ObjectMeta.Finalizers {
			if f != scheduleCancelFinalizer {
				newFinalizers = append(newFinalizers, f)
			}
		}
		resource.ObjectMeta.Finalizers = newFinalizers
		if err := r.Update(ctx, resource); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// 这个方法用于触发备份操作。它创建一个新的 EtcdBackup 资源，并将 EtcdBackupSchedule 设置为其所有者。
func (r *EtcdBackupScheduleReconciler) fire(resourceNamespacedName types.NamespacedName) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	resource := &etcdv1alpha1.EtcdBackupSchedule{}
	if err := r.Get(ctx, resourceNamespacedName, resource); err != nil {
		return client.IgnoreNotFound(err)
	}

	if err := r.Client.Create(ctx, &etcdv1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: resource.ObjectMeta.Name + "-",
			Namespace:    resource.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(resource, etcdv1alpha1.GroupVersion.WithKind("EtcdBackupSchedule")),
			},
			Labels: map[string]string{
				scheduleLabel: resource.ObjectMeta.Name,
			},
		},
		Spec: resource.Spec.BackupTemplate,
	}); err != nil {
		return err
	}

	return nil
}

// 这个方法用于将 EtcdBackupScheduleReconciler 与 manager 关联，以便可以创建新的控制器。
func (r *EtcdBackupScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdv1alpha1.EtcdBackupSchedule{}).
		Complete(r)
}
