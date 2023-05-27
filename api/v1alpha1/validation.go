package v1alpha1

import (
	"fmt"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

//validation.go：这个文件定义了EtcdCluster和EtcdPeer的验证逻辑。
//当创建或更新一个EtcdCluster或EtcdPeer时，这个文件中的代码会检查用户提供的字段是否符合要求。
//例如，它会检查Etcd版本是否被支持，存储类名是否被提供，以及注解名是否是保留的。如果字段不符合要求，它会返回一个错误，阻止创建或更新操作。

func validateSemanticVersion(version string, path *field.Path) (allErrs field.ErrorList) {
	v, err := semver.NewVersion(version)
	if err != nil {
		allErrs = append(
			allErrs,
			field.Invalid(path, version, err.Error()),
		)
		return
	}
	if v.Major != etcdSupportedVersionMajor {
		allErrs = append(
			allErrs,
			field.Invalid(path, version, "only Etcd version 3 is supported"),
		)
	}
	return
}

var _ webhook.Validator = &EtcdCluster{}

// ValidateCreate validates that all required fields are present and valid.
func (o *EtcdCluster) ValidateCreate() error {
	path := field.NewPath("spec")
	var allErrs field.ErrorList
	allErrs = append(
		allErrs,
		validateSemanticVersion(o.Spec.Version, path.Child("version"))...,
	)
	allErrs = append(
		allErrs,
		o.Spec.Storage.validate(path.Child("storage"))...)
	allErrs = append(
		allErrs,
		o.Spec.PodTemplate.validate(path.Child("podTemplate"))...)
	return allErrs.ToAggregate()
}

// ValidateCreate validates that deletion is allowed
// TODO: Not yet implemented
func (o *EtcdCluster) ValidateDelete() error {
	var allErrs field.ErrorList
	return allErrs.ToAggregate()
}

// ValidateUpdate validates that only supported fields are changed
func (o *EtcdCluster) ValidateUpdate(old runtime.Object) error {
	oldO, ok := old.(*EtcdCluster)
	if !ok {
		return fmt.Errorf("Unexpected type for old: %#v", old)
	}
	oldO = oldO.DeepCopy()

	// Overwrite the fields which are allowed to change
	oldO.Spec.Replicas = o.Spec.Replicas
	oldO.Spec.Version = o.Spec.Version

	if diff := cmp.Diff(oldO.Spec, o.Spec); diff != "" {
		return fmt.Errorf("Unsupported changes: (- current, + new) %s", diff)
	}

	err := o.ValidateCreate()
	if err != nil {
		err = fmt.Errorf("Unsupported changes: %s", err)
	}
	return err
}

var _ webhook.Validator = &EtcdPeer{}

// ValidateCreate validates that all required fields are present and valid.
func (o *EtcdPeer) ValidateCreate() error {
	path := field.NewPath("spec")
	var allErrs field.ErrorList
	allErrs = append(
		allErrs,
		validateSemanticVersion(o.Spec.Version, path.Child("version"))...,
	)
	allErrs = append(
		allErrs,
		o.Spec.Storage.validate(path.Child("storage"))...,
	)
	allErrs = append(
		allErrs,
		o.Spec.PodTemplate.validate(path.Child("podTemplate"))...)
	return allErrs.ToAggregate()
}

// ValidateCreate validates that deletion is allowed
// TODO: Not yet implemented
func (o *EtcdPeer) ValidateDelete() error {
	var allErrs field.ErrorList
	return allErrs.ToAggregate()
}

// ValidateUpdate validates that only supported fields are changed
func (o *EtcdPeer) ValidateUpdate(old runtime.Object) error {
	oldO, ok := old.(*EtcdPeer)
	if !ok {
		return fmt.Errorf("Unexpected type for old: %#v", old)
	}
	oldO = oldO.DeepCopy()

	// Overwrite any the fields which are allowed to change
	// oldO.Spec.Foo = o.Spec.Foo

	if diff := cmp.Diff(oldO.Spec, o.Spec); diff != "" {
		return fmt.Errorf("Unsupported changes: (- current, + new) %s", diff)
	}
	return nil
}

func validatePersistentVolumeClaimSpec(path *field.Path, o *corev1.PersistentVolumeClaimSpec) field.ErrorList {
	var allErrs field.ErrorList
	if o == nil {
		allErrs = append(allErrs, field.Required(path, ""))
		return allErrs
	}
	if o.StorageClassName == nil {
		allErrs = append(allErrs, field.Required(path.Child("storageClassName"), ""))
		return allErrs
	}
	if _, ok := o.Resources.Requests["storage"]; !ok {
		allErrs = append(allErrs, field.Required(path.Child("resources", "requests", "storage"), ""))
		return allErrs
	}
	return allErrs
}

func (o *EtcdPeerStorage) validate(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if o == nil {
		allErrs = append(allErrs, field.Required(path, ""))
		return allErrs
	}
	allErrs = append(
		allErrs,
		validatePersistentVolumeClaimSpec(path.Child("volumeClaimTemplate"), o.VolumeClaimTemplate)...,
	)
	return allErrs
}

// IsInvalidUserProvidedAnnotation tests to see if the given annotation name is one reserved by the operator
func IsInvalidUserProvidedAnnotationName(annotationName string) bool {
	return strings.HasPrefix(annotationName, "etcd.improbable.io/")
}

func (o *EtcdPodTemplateObjectMeta) validate(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if o != nil {
		for name := range o.Annotations {
			if IsInvalidUserProvidedAnnotationName(name) {
				allErrs = append(
					allErrs,
					field.Invalid(path, name, "Annotation name is a reserved name ('etcd.improbable.io' prefix)"))
			}
		}
	} else {
		// We can be nil, that's fine.
	}
	return allErrs
}

func (o *EtcdPodTemplateSpec) validate(path *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if o != nil {
		allErrs = append(allErrs, o.Metadata.validate(path.Child("metadata"))...)
	} else {
		// Not mandatory, being nil is fine.
	}
	return allErrs
}
