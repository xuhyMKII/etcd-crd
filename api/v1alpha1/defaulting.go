package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

var (
	defaultVolumeMode = corev1.PersistentVolumeFilesystem
)

//defaulting.go：这个文件定义了EtcdCluster和EtcdPeer的默认设置。
//当创建一个新的EtcdCluster或EtcdPeer时，如果某些字段没有被明确设置，
//这个文件中的代码会为这些字段设置默认值。这样可以避免在后续的代码中出现nil指针异常。

var _ webhook.Defaulter = &EtcdCluster{}

// Default sets default values for optional EtcdPeer fields.
// This is used in webhooks and in the Reconciler to ensure that nil pointers
// have been replaced with concrete pointers.
// This avoids nil pointer panics later on.
func (o *EtcdCluster) Default() {
	if o == nil {
		return
	}
	o.Spec.Storage.setDefaults()
}

var _ webhook.Defaulter = &EtcdPeer{}

// Default sets default values for optional EtcdPeer fields.
// This is used in webhooks and in the Reconciler to ensure that nil pointers
// have been replaced with concrete pointers.
// This avoids nil pointer panics later on.
func (o *EtcdPeer) Default() {
	if o == nil {
		return
	}
	o.Spec.Storage.setDefaults()
}

func (o *EtcdPeerStorage) setDefaults() {
	if o == nil {
		return
	}
	if o.VolumeClaimTemplate != nil {
		if o.VolumeClaimTemplate.AccessModes == nil {
			o.VolumeClaimTemplate.AccessModes = []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			}
		}

		if o.VolumeClaimTemplate.VolumeMode == nil {
			o.VolumeClaimTemplate.VolumeMode = &defaultVolumeMode
		}
	}
}
