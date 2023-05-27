package main

import (
	"fmt"
	"os"
	"strings"

	flag "github.com/spf13/pflag"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	etcdv1alpha1 "github.com/improbable-eng/etcd-cluster-operator/api/v1alpha1"
	"github.com/improbable-eng/etcd-cluster-operator/controllers"
	"github.com/improbable-eng/etcd-cluster-operator/internal/etcd"
	"github.com/improbable-eng/etcd-cluster-operator/version"
	"github.com/improbable-eng/etcd-cluster-operator/webhooks"
	"github.com/robfig/cron/v3"
	// +kubebuilder:scaffold:imports
)

var (
	scheme                = runtime.NewScheme()
	setupLog              = ctrl.Log.WithName("setup")
	defaultEtcdRepository = "quay.io/coreos/etcd"
	// These are replaced as part of the build so that the images match the name
	// prefix and version of the controller image. See Dockerfile.
	defaultBackupAgentImage  = "REPLACE_ME"
	defaultRestoreAgentImage = "REPLACE_ME"
)

const (
	defaultProxyPort = 80
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdv1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

//这个 main.go 文件是整个 Etcd 集群操作器项目的主入口点。它的主要功能是启动和管理 Etcd 集群操作器的各个组件。以下是它的主要逻辑和功能：
//
//初始化：首先，它初始化了一些全局变量和配置选项，例如指标地址、领导选举配置、Etcd 仓库地址、备份代理镜像、恢复代理镜像等。
//
//创建管理器：然后，它创建了一个新的控制器运行时管理器，这个管理器负责管理和协调所有的控制器。
//
//注册控制器：接下来，它将各个控制器注册到管理器中。
//这些控制器包括 EtcdPeerReconciler、EtcdClusterReconciler、EtcdBackupReconciler、EtcdBackupScheduleReconciler 和 EtcdRestoreReconciler。
//这些控制器负责管理 Etcd 集群的各个方面，例如节点管理、集群管理、备份管理、备份调度管理和恢复管理。
//
//设置 Webhooks：如果没有禁用 Webhooks，它还会设置 EtcdCluster 和 EtcdPeer 的 Webhooks。这些 Webhooks 用于处理 EtcdCluster 和 EtcdPeer 资源的创建、更新和删除请求。
//
//启动管理器：最后，它启动了管理器，并开始监听和处理 Kubernetes 事件。如果管理器在运行过程中遇到任何问题，它会记录错误信息并退出程序。
//
//在整个项目中，这个 main.go 文件的作用是启动和管理 Etcd 集群操作器。它负责初始化配置、创建和注册控制器、设置 Webhooks，并启动管理器。这是整个 Etcd 集群操作器运行的起点。

func main() {
	var (
		enableLeaderElection bool
		leaderElectionID     string
		metricsAddr          string
		printVersion         bool
		proxyURL             string
		etcdRepository       string
		restoreAgentImage    string
		backupAgentImage     string
	)

	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&leaderElectionID, "leader-election-id", "etcd-cluster-operator-controller-leader-election-helper",
		"The name of the configmap that leader election will use for holding the leader lock.")
	flag.StringVar(&etcdRepository, "etcd-repository", defaultEtcdRepository, "The Docker repository to use for the etcd image")
	flag.StringVar(&backupAgentImage, "backup-agent-image", defaultBackupAgentImage, "The Docker image for the backup-agent")
	flag.StringVar(&restoreAgentImage, "restore-agent-image", defaultRestoreAgentImage, "The Docker image to use to perform a restore")
	flag.StringVar(&proxyURL, "proxy-url", "", "The URL of the upload/download proxy")
	flag.BoolVar(&printVersion, "version", false,
		"Print version to stdout and exit")
	flag.Parse()

	if printVersion {
		fmt.Println(version.Version)
		return
	}
	// TODO: Allow users to configure JSON logging.
	// See https://github.com/improbable-eng/etcd-cluster-operator/issues/171
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	setupLog.Info(
		"Starting manager",
		"version", version.Version,
		"etcd-repository", etcdRepository,
		"backup-agent-image", backupAgentImage,
		"restore-agent-image", restoreAgentImage,
		"proxy-url", proxyURL,
	)

	if !strings.Contains(proxyURL, ":") {
		// gRPC needs a port, and this address doesn't seem to have one.
		proxyURL = fmt.Sprintf("%s:%d", proxyURL, defaultProxyPort)
		setupLog.Info("Defaulting port on configured Proxy URL",
			"default-proxy-port", defaultProxyPort,
			"proxy-url", proxyURL)
	}
	//CRD控制器注册到管理器的逻辑

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: metricsAddr,
		LeaderElection:     enableLeaderElection,
		LeaderElectionID:   leaderElectionID,
		Port:               9443,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&controllers.EtcdPeerReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("controllers").WithName("EtcdPeer"),
		Etcd:           &etcd.ClientEtcdAPIBuilder{},
		EtcdRepository: etcdRepository,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdPeer")
		os.Exit(1)
	}
	if err = (&controllers.EtcdClusterReconciler{
		Client:   mgr.GetClient(),
		Log:      ctrl.Log.WithName("controllers").WithName("EtcdCluster"),
		Recorder: mgr.GetEventRecorderFor("etcdcluster-reconciler"),
		Etcd:     &etcd.ClientEtcdAPIBuilder{},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdCluster")
		os.Exit(1)
	}
	if err = (&controllers.EtcdBackupReconciler{
		Client:           mgr.GetClient(),
		Log:              ctrl.Log.WithName("controllers").WithName("EtcdBackup"),
		Scheme:           mgr.GetScheme(),
		BackupAgentImage: backupAgentImage,
		ProxyURL:         proxyURL,
		Recorder:         mgr.GetEventRecorderFor("etcdbackup-reconciler"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdBackup")
		os.Exit(1)
	}
	cronHandler := cron.New()
	if err = (&controllers.EtcdBackupScheduleReconciler{
		Client:      mgr.GetClient(),
		Log:         ctrl.Log.WithName("controllers").WithName("EtcdBackupSchedule"),
		CronHandler: cronHandler,
		Schedules:   controllers.NewScheduleMap(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdBackupSchedule")
		os.Exit(1)
	}
	if err = (&controllers.EtcdRestoreReconciler{
		Client:          mgr.GetClient(),
		Log:             ctrl.Log.WithName("controllers").WithName("EtcdRestore"),
		Recorder:        mgr.GetEventRecorderFor("etcdrestore-reconciler"),
		RestorePodImage: restoreAgentImage,
		ProxyURL:        proxyURL,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdRestore")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder
	if os.Getenv("DISABLE_WEBHOOKS") != "" {
		setupLog.Info("Skipping webhook set up.")
	} else {
		setupLog.Info("Setting up webhooks.")
		if err = (&webhooks.EtcdCluster{
			Log: ctrl.Log.WithName("webhooks").WithName("EtcdCluster"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Unable to create webhook.", "webhook", "EtcdCluster")
			os.Exit(1)
		}
		if err = (&webhooks.EtcdPeer{
			Log: ctrl.Log.WithName("webhooks").WithName("EtcdPeer"),
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Unable to create webhook.", "webhook", "EtcdPeer")
			os.Exit(1)
		}
	}

	setupLog.Info("Starting cron handler")
	cronHandler.Start()
	defer func() {
		setupLog.Info("Stopping cron handler")
		<-cronHandler.Stop().Done()
	}()
	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
