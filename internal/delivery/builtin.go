package delivery

// Built-in workload adapters (ADR-006 first batch): Deployment, StatefulSet
// and DaemonSet. All three declare their images the same way — pod-template
// containers located by name — so one shared pod-template implementation
// serves the three registered GVKs. Locator discipline (spec decision 6):
// containerNames are matched exactly against spec.template.spec.containers[]
// entries; a missing name is an honest ContainersNotFoundError, never a
// default guess (no first-container fallback, no repo-name matching).
//
// Scope note: initContainers are deliberately NOT searched. ADR-006 freezes
// the built-in write surface as "pod template 的 container image 字段"; the
// failure surface for a name that exists only in initContainers is the same
// explicit ContainerNotFound report (message lists the present container
// names, making the mismatch diagnosable).
//
// The apply configuration each adapter builds is an *unstructured.Unstructured
// carrying TypeMeta + object metadata + ONLY the declared container images
// (plus the static audit annotation). SSA merges it into the live object by
// the containers list-map key (name), so sibling containers and every
// unrelated field stay untouched (ADR-005 §1 minimal field update).

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// Pod-template field names of the built-in apply surface. Constants keep the
// navigation literals single-sourced (and the package inside the goconst
// threshold shared with the test files).
const (
	fieldSpec       = "spec"
	fieldTemplate   = "template"
	fieldContainers = "containers"
	fieldName       = "name"
	fieldImage      = "image"
)

// podTemplateContainersPath is the unstructured navigation path of the
// pod-template container list for standard workloads (Deployment, StatefulSet,
// DaemonSet).
var podTemplateContainersPath = []string{fieldSpec, fieldTemplate, fieldSpec, fieldContainers}

// cronJobContainersPath is the unstructured navigation path of the container
// list inside a CronJob's nested jobTemplate.
var cronJobContainersPath = []string{fieldSpec, "jobTemplate", fieldSpec, fieldTemplate, fieldSpec, fieldContainers}

// podTemplateContainerPath renders the canonical ADR-005-grammar path of one
// pod-template container image field. The grammar is the single vocabulary for
// field identity: the ownership inspection (ownership.go) and the third-party
// resolver seam (T10) compare exactly these path forms.
func podTemplateContainerPath(container string) string {
	return fmt.Sprintf(".spec.template.spec.containers[name=%s].image", container)
}

// cronJobContainerPath renders the ADR-005-grammar path for a container image
// field inside a CronJob's nested jobTemplate.
func cronJobContainerPath(container string) string {
	return fmt.Sprintf(".spec.jobTemplate.spec.template.spec.containers[name=%s].image", container)
}

type podTemplateAdapter struct {
	gvk            schema.GroupVersionKind
	containersPath []string
	containerPath  func(string) string
	applyNest      func(containers []interface{}) map[string]interface{}
}

func (a podTemplateAdapter) GVK() schema.GroupVersionKind { return a.gvk }

// Plan locates containerNames in the pod template and builds the minimal SSA
// apply configuration declaring image on all of them.
func (a podTemplateAdapter) Plan(
	obj *unstructured.Unstructured,
	containerNames []string,
	image string,
) (*Plan, error) {
	if len(containerNames) == 0 {
		// CRD MinItems=1 plus the T3 webhook make this unreachable for
		// admitted objects; stay honest instead of writing nothing silently.
		return nil, fmt.Errorf("%s: containerNames must be non-empty (no default guessing, "+
			"spec decision 6)", a.gvk.Kind)
	}
	present := a.containerImages(obj)
	var missing []string
	for _, name := range containerNames {
		if _, ok := present[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, &ContainersNotFoundError{
			Kind:    a.gvk.Kind,
			Missing: missing,
			Present: a.presentNames(obj),
		}
	}

	plan := &Plan{
		Apply: a.applyConfiguration(obj, containerNames, image),
	}
	for _, name := range containerNames {
		raw := a.containerPath(name)
		path, err := imagefields.Parse(raw)
		if err != nil {
			// Unreachable for DNS-1123 container names (the grammar's value
			// charset is a superset); fail loudly rather than declare an
			// unlocatable field.
			return nil, fmt.Errorf("%s: internal error rendering path %q: %w", a.gvk.Kind, raw, err)
		}
		pc := present[name]
		plan.Fields = append(plan.Fields, FieldPlan{
			Path:      path,
			Container: name,
			Observed:  pc.image,
			Found:     pc.found,
		})
	}
	return plan, nil
}

// applyConfiguration builds the minimal SSA payload: TypeMeta, object
// identity, the audit annotation and the declared container images.
func (a podTemplateAdapter) applyConfiguration(
	obj *unstructured.Unstructured,
	containerNames []string,
	image string,
) *unstructured.Unstructured {
	containers := make([]interface{}, 0, len(containerNames))
	for _, name := range containerNames {
		containers = append(containers, map[string]interface{}{
			fieldName:  name,
			fieldImage: image,
		})
	}
	ac := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": a.gvk.GroupVersion().String(),
		"kind":       a.gvk.Kind,
		"metadata": map[string]interface{}{
			fieldName:   obj.GetName(),
			"namespace": obj.GetNamespace(),
			"annotations": map[string]interface{}{
				ManagedByAnnotation: ManagedByAnnotationValue,
			},
		},
		fieldSpec: a.applyNest(containers),
	}}
	return ac
}

// containerImages reads the containers list at the adapter's configured path
// into a name -> image map. Entries without a name are skipped; non-string
// images (impossible behind the Kubernetes schema) stay absent so
// FieldPlan.Found reports the mismatch honestly.
func (a podTemplateAdapter) containerImages(obj *unstructured.Unstructured) map[string]presentContainer {
	out := map[string]presentContainer{}
	containers, found, err := unstructured.NestedSlice(obj.Object, a.containersPath...)
	if err != nil || !found {
		return out
	}
	for _, entry := range containers {
		m, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := m[fieldName].(string)
		if name == "" {
			continue
		}
		image, isString := m[fieldImage].(string)
		out[name] = presentContainer{image: image, found: isString}
	}
	return out
}

type presentContainer struct {
	image string
	found bool
}

// presentNames lists the container names in object order at the adapter's
// configured path, for the diagnosable ContainersNotFound message.
func (a podTemplateAdapter) presentNames(obj *unstructured.Unstructured) []string {
	var names []string
	containers, found, err := unstructured.NestedSlice(obj.Object, a.containersPath...)
	if err != nil || !found {
		return names
	}
	for _, entry := range containers {
		m, ok := entry.(map[string]interface{})
		if !ok {
			continue
		}
		if name, _ := m[fieldName].(string); name != "" {
			names = append(names, name)
		}
	}
	return names
}

// standardApplyNest returns the spec nesting function for Deployment,
// StatefulSet and DaemonSet: spec.template.spec.containers.
func standardApplyNest(containers []interface{}) map[string]interface{} {
	return map[string]interface{}{
		fieldTemplate: map[string]interface{}{
			fieldSpec: map[string]interface{}{
				fieldContainers: containers,
			},
		},
	}
}

// cronJobApplyNest returns the spec nesting function for CronJob:
// spec.jobTemplate.spec.template.spec.containers.
func cronJobApplyNest(containers []interface{}) map[string]interface{} {
	return map[string]interface{}{
		"jobTemplate": map[string]interface{}{
			fieldSpec: map[string]interface{}{
				fieldTemplate: map[string]interface{}{
					fieldSpec: map[string]interface{}{
						fieldContainers: containers,
					},
				},
			},
		},
	}
}

func init() {
	appsV1 := schema.GroupVersion{Group: "apps", Version: "v1"}
	for _, kind := range []string{"Deployment", "StatefulSet", "DaemonSet"} {
		RegisterKindAdapter(podTemplateAdapter{
			gvk:            appsV1.WithKind(kind),
			containersPath: podTemplateContainersPath,
			containerPath:  podTemplateContainerPath,
			applyNest:      standardApplyNest,
		})
	}
	// batch/v1 Job uses the same pod-template container path as the apps/v1
	// workloads (.spec.template.spec.containers[name=X].image).
	batchV1 := schema.GroupVersion{Group: "batch", Version: "v1"}
	RegisterKindAdapter(podTemplateAdapter{
		gvk:            batchV1.WithKind("Job"),
		containersPath: podTemplateContainersPath,
		containerPath:  podTemplateContainerPath,
		applyNest:      standardApplyNest,
	})
	// CronJob uses a nested jobTemplate path (T3):
	// .spec.jobTemplate.spec.template.spec.containers[name=X].image
	RegisterKindAdapter(podTemplateAdapter{
		gvk:            batchV1.WithKind("CronJob"),
		containersPath: cronJobContainersPath,
		containerPath:  cronJobContainerPath,
		applyNest:      cronJobApplyNest,
	})
}
