package delivery

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	"github.com/appfun-dev/imageflow/internal/imagefields"
)

// T9 unit tests of the delivery building blocks: field-manager identity,
// built-in adapter plans, unsupported-kind classification and the
// managedFields ownership inspection (ADR-005/006/008).

const (
	t9uKindDeployment  = "Deployment"
	t9uKindStatefulSet = "StatefulSet"
	t9uKindDaemonSet   = "DaemonSet"
	t9uKindCronJob     = "CronJob"
	t9uAPIAppsV1       = "apps/v1"
	t9uAPIBatchV1      = "batch/v1"
	t9uContainerApp    = "app"
	t9uContainerSide   = "sidecar"
	t9uImageField      = "image"
)

var (
	t9uGVKDeployment  = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: t9uKindDeployment}
	t9uGVKStatefulSet = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: t9uKindStatefulSet}
	t9uGVKDaemonSet   = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: t9uKindDaemonSet}
	t9uGVKJob         = schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "Job"}
	t9uGVKCronJob     = schema.GroupVersionKind{Group: "batch", Version: "v1", Kind: "CronJob"}
)

const t9uDigestRef = "quay.io/example/ledger@sha256:" +
	"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

// t9uWorkload renders one pod-template workload fixture (shared by the three
// built-in kinds; YAML keeps the Go string-literal budget of this package
// inside the goconst threshold).
func t9uWorkload(t *testing.T, kind string, containers ...[2]string) *unstructured.Unstructured {
	t.Helper()
	return t9uWorkloadAPI(t, "apps/v1", kind, containers...)
}

// t9uWorkloadAPI renders one pod-template workload fixture with an explicit
// apiVersion, used for kinds outside apps/v1 (e.g. batch/v1 Job).
func t9uWorkloadAPI(t *testing.T, apiVersion, kind string, containers ...[2]string) *unstructured.Unstructured {
	t.Helper()
	var list strings.Builder
	for _, c := range containers {
		list.WriteString("      - name: " + c[0] + "\n        image: " + c[1] + "\n")
	}
	doc := "apiVersion: " + apiVersion + "\nkind: " + kind + "\nmetadata:\n  name: t9-ledger\n  namespace: t9-apps\n" +
		"spec:\n  template:\n    spec:\n      containers:\n" + list.String()
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal([]byte(doc), &obj.Object); err != nil {
		t.Fatalf("render %s fixture: %v", kind, err)
	}
	return obj
}

func TestFieldManagerForKeepsTheFrozenFormat(t *testing.T) {
	got := FieldManagerFor("payments", "ledger-stable")
	want := FieldManagerPrefix + "payments-ledger-stable"
	if got != want {
		t.Fatalf("FieldManagerFor = %q, want the frozen ADR-008 format %q", got, want)
	}
	if !IsImageFlowFieldManager(got) {
		t.Fatalf("IsImageFlowFieldManager(%q) = false", got)
	}
	if IsImageFlowFieldManager("kubectl") || IsImageFlowFieldManager("helm") {
		t.Fatal("foreign managers must not classify as ImageFlow")
	}
}

func TestFieldManagerForBoundsLongIdentitiesStably(t *testing.T) {
	ns := strings.Repeat("n", 63)
	name := strings.Repeat("m", 253)
	first := FieldManagerFor(ns, name)
	second := FieldManagerFor(ns, name)
	if first != second {
		t.Fatalf("derivation is not stable: %q != %q", first, second)
	}
	if len(first) > fieldManagerMaxLength {
		t.Fatalf("field manager %d bytes exceeds the API server limit %d",
			len(first), fieldManagerMaxLength)
	}
	if !strings.HasPrefix(first, FieldManagerPrefix) {
		t.Fatalf("frozen prefix lost: %q", first)
	}
	if !IsImageFlowFieldManager(first) {
		t.Fatalf("derived identity must stay inside the ImageFlow family: %q", first)
	}
	other := FieldManagerFor(ns, name[:len(name)-1])
	if other == first {
		t.Fatal("distinct subscriptions must not share a derived field manager")
	}
	// Identities inside the limit are never derived (exact frozen format).
	short := FieldManagerFor(ns[:30], name[:40])
	if strings.Contains(short, "-h") && len(short) <= fieldManagerMaxLength &&
		len(FieldManagerPrefix)+30+1+40 <= fieldManagerMaxLength {
		t.Fatalf("short identity was derived despite fitting the limit: %q", short)
	}
}

func TestBuiltInAdaptersAreRegisteredForTheFirstBatch(t *testing.T) {
	for _, gvk := range []schema.GroupVersionKind{t9uGVKDeployment, t9uGVKStatefulSet, t9uGVKDaemonSet, t9uGVKJob} {
		if LookupKindAdapter(gvk) == nil {
			t.Fatalf("no adapter registered for %s (ADR-006 first batch)", gvk)
		}
	}
	if LookupKindAdapter(schema.GroupVersionKind{Group: "apps", Version: "v1beta1",
		Kind: t9uKindDeployment}) != nil {
		t.Fatal("adapters are registered per exact GVK: no cross-version guessing")
	}
}

func TestBuiltInPlanDeclaresOnlyTheNamedContainerImages(t *testing.T) {
	cases := []struct {
		kind       string
		gvk        schema.GroupVersionKind
		apiVersion string
	}{
		{t9uKindDeployment, t9uGVKDeployment, t9uAPIAppsV1},
		{t9uKindStatefulSet, t9uGVKStatefulSet, t9uAPIAppsV1},
		{t9uKindDaemonSet, t9uGVKDaemonSet, t9uAPIAppsV1},
		{"Job", t9uGVKJob, "batch/v1"},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			adapter := LookupKindAdapter(tc.gvk)
			var obj *unstructured.Unstructured
			if tc.apiVersion == t9uAPIAppsV1 {
				obj = t9uWorkload(t, tc.kind,
					[2]string{t9uContainerApp, "quay.io/example/ledger:v1"},
					[2]string{t9uContainerSide, "quay.io/platform/mesh:v9"})
			} else {
				obj = t9uWorkloadAPI(t, tc.apiVersion, tc.kind,
					[2]string{t9uContainerApp, "quay.io/example/ledger:v1"},
					[2]string{t9uContainerSide, "quay.io/platform/mesh:v9"})
			}
			plan, err := adapter.Plan(obj, []string{t9uContainerApp}, t9uDigestRef)
			if err != nil {
				t.Fatalf("Plan: %v", err)
			}
			if len(plan.Fields) != 1 {
				t.Fatalf("fields = %d, want 1", len(plan.Fields))
			}
			f := plan.Fields[0]
			if f.Container != t9uContainerApp || !f.Found || f.Observed != "quay.io/example/ledger:v1" {
				t.Fatalf("unexpected field plan: %+v", f)
			}
			wantPath := ".spec.template.spec.containers[name=app].image"
			if f.Path.String() != wantPath {
				t.Fatalf("path = %q, want %q", f.Path.String(), wantPath)
			}

			ac := plan.Apply
			if ac.GetKind() != tc.kind || ac.GroupVersionKind().GroupVersion().String() != tc.apiVersion {
				t.Fatalf("apply configuration TypeMeta = %v, want %s %s", ac.GroupVersionKind(), tc.apiVersion, tc.kind)
			}
			if ac.GetName() != obj.GetName() || ac.GetNamespace() != obj.GetNamespace() {
				t.Fatal("apply configuration must carry the target identity")
			}
			if got := ac.GetAnnotations()[ManagedByAnnotation]; got != ManagedByAnnotationValue {
				t.Fatalf("audit annotation = %q, want the static %q", got, ManagedByAnnotationValue)
			}
			containers, found, err := unstructured.NestedSlice(ac.Object,
				fieldSpec, fieldTemplate, fieldSpec, fieldContainers)
			if err != nil || !found || len(containers) != 1 {
				t.Fatalf("declared containers = %v (found=%v err=%v)", containers, found, err)
			}
			entry := containers[0].(map[string]interface{})
			if entry[fieldName] != t9uContainerApp || entry[fieldImage] != t9uDigestRef {
				t.Fatalf("declared container = %v", entry)
			}
			if len(entry) != 2 {
				t.Fatalf("apply configuration must declare ONLY name+image, got %v", entry)
			}
			// The sibling container must not appear in the configuration.
			encoded, _ := yaml.Marshal(ac.Object)
			if strings.Contains(string(encoded), t9uContainerSide) {
				t.Fatalf("sibling container leaked into the apply configuration:\n%s", encoded)
			}
		})
	}
}

func TestBuiltInPlanReportsMissingContainersHonestly(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKDeployment)
	obj := t9uWorkload(t, t9uKindDeployment, [2]string{t9uContainerApp, "quay.io/example/ledger:v1"})
	_, err := adapter.Plan(obj, []string{t9uContainerApp, "ghost"}, t9uDigestRef)
	var missing *ContainersNotFoundError
	if !asContainersNotFound(err, &missing) {
		t.Fatalf("err = %v, want *ContainersNotFoundError", err)
	}
	if len(missing.Missing) != 1 || missing.Missing[0] != "ghost" {
		t.Fatalf("missing = %v", missing.Missing)
	}
	if len(missing.Present) != 1 || missing.Present[0] != t9uContainerApp {
		t.Fatalf("present = %v", missing.Present)
	}
	if !strings.Contains(missing.Error(), "ghost") || !strings.Contains(missing.Error(), t9uContainerApp) {
		t.Fatalf("message must be diagnosable: %q", missing.Error())
	}

	if _, err := adapter.Plan(obj, nil, t9uDigestRef); err == nil {
		t.Fatal("empty containerNames must be rejected (no default guessing, spec decision 6)")
	}
}

func asContainersNotFound(err error, target **ContainersNotFoundError) bool {
	e, ok := err.(*ContainersNotFoundError)
	if ok {
		*target = e
	}
	return ok
}

// TestJobDeliveryAdapterSingleContainer verifies Plan generation for a Job
// with a single container — the adapter locates the container by name and
// builds the correct SSA apply configuration targeting batch/v1.
func TestJobDeliveryAdapterSingleContainer(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKJob)
	if adapter == nil {
		t.Fatal("batch/v1 Job adapter must be registered")
	}
	obj := t9uWorkloadAPI(t, "batch/v1", "Job",
		[2]string{"worker", "quay.io/example/worker:v1"})
	plan, err := adapter.Plan(obj, []string{"worker"}, t9uDigestRef)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Fields) != 1 {
		t.Fatalf("fields = %d, want 1", len(plan.Fields))
	}
	f := plan.Fields[0]
	if f.Container != "worker" || !f.Found || f.Observed != "quay.io/example/worker:v1" {
		t.Fatalf("unexpected field plan: %+v", f)
	}
	wantPath := ".spec.template.spec.containers[name=worker].image"
	if f.Path.String() != wantPath {
		t.Fatalf("path = %q, want %q", f.Path.String(), wantPath)
	}
	ac := plan.Apply
	if ac.GetKind() != "Job" || ac.GroupVersionKind().GroupVersion().String() != "batch/v1" {
		t.Fatalf("apply configuration TypeMeta = %v, want batch/v1 Job", ac.GroupVersionKind())
	}
	containers, found, err := unstructured.NestedSlice(ac.Object,
		fieldSpec, fieldTemplate, fieldSpec, fieldContainers)
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("declared containers = %v (found=%v err=%v)", containers, found, err)
	}
	entry := containers[0].(map[string]interface{})
	if entry[fieldName] != "worker" || entry[fieldImage] != t9uDigestRef {
		t.Fatalf("declared container = %v", entry)
	}
}

// TestJobDeliveryAdapterMultipleContainers verifies Plan generation for a Job
// with multiple containers — all named containers are declared in the apply
// configuration, and sibling containers not in the request are excluded.
func TestJobDeliveryAdapterMultipleContainers(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKJob)
	if adapter == nil {
		t.Fatal("batch/v1 Job adapter must be registered")
	}
	obj := t9uWorkloadAPI(t, "batch/v1", "Job",
		[2]string{"worker", "quay.io/example/worker:v1"},
		[2]string{"sidecar", "quay.io/platform/mesh:v9"},
		[2]string{"logger", "quay.io/platform/logger:v2"})
	plan, err := adapter.Plan(obj, []string{"worker", "logger"}, t9uDigestRef)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Fields) != 2 {
		t.Fatalf("fields = %d, want 2", len(plan.Fields))
	}
	if plan.Fields[0].Container != "worker" || plan.Fields[1].Container != "logger" {
		t.Fatalf("field order = [%s, %s], want [worker, logger]",
			plan.Fields[0].Container, plan.Fields[1].Container)
	}
	ac := plan.Apply
	containers, found, err := unstructured.NestedSlice(ac.Object,
		fieldSpec, fieldTemplate, fieldSpec, fieldContainers)
	if err != nil || !found || len(containers) != 2 {
		t.Fatalf("declared containers count = %d, want 2", len(containers))
	}
	// The sidecar container must NOT appear in the apply configuration.
	encoded, _ := yaml.Marshal(ac.Object)
	if strings.Contains(string(encoded), "sidecar") {
		t.Fatalf("sibling container leaked into the apply configuration:\n%s", encoded)
	}
}

// TestJobDeliveryAdapterContainersNotFound verifies that requesting a
// container name not present in the Job's pod template returns an honest
// ContainersNotFoundError (no guessing, spec decision 6).
func TestJobDeliveryAdapterContainersNotFound(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKJob)
	if adapter == nil {
		t.Fatal("batch/v1 Job adapter must be registered")
	}
	obj := t9uWorkloadAPI(t, "batch/v1", "Job",
		[2]string{"worker", "quay.io/example/worker:v1"})
	_, err := adapter.Plan(obj, []string{"worker", "ghost"}, t9uDigestRef)
	var missing *ContainersNotFoundError
	if !asContainersNotFound(err, &missing) {
		t.Fatalf("err = %v, want *ContainersNotFoundError", err)
	}
	if missing.Kind != "Job" {
		t.Fatalf("error kind = %q, want Job", missing.Kind)
	}
	if len(missing.Missing) != 1 || missing.Missing[0] != "ghost" {
		t.Fatalf("missing = %v, want [ghost]", missing.Missing)
	}
	if len(missing.Present) != 1 || missing.Present[0] != "worker" {
		t.Fatalf("present = %v, want [worker]", missing.Present)
	}
	if !strings.Contains(missing.Error(), "ghost") || !strings.Contains(missing.Error(), "worker") {
		t.Fatalf("message must be diagnosable: %q", missing.Error())
	}
}

func TestClassifyServesFirstBatchAndRefusesTheRest(t *testing.T) {
	for _, gvk := range []schema.GroupVersionKind{t9uGVKDeployment, t9uGVKJob} {
		if _, _, _, ok := Classify(gvk, false); !ok {
			t.Fatalf("%s with containerNames must be served", gvk)
		}
	}

	pod := schema.GroupVersionKind{Version: "v1", Kind: "Pod"}
	rc := schema.GroupVersionKind{Version: "v1", Kind: "ReplicationController"}
	for _, gvk := range []schema.GroupVersionKind{pod, rc} {
		_, _, class, ok := Classify(gvk, false)
		if ok || class != ClassificationUnsupportedKind {
			t.Fatalf("%s: ok=%v class=%v, want ClassificationUnsupportedKind (ADR-006)", gvk, ok, class)
		}
		if msg := UnsupportedMessage(gvk, class); !strings.Contains(msg, "ADR-006") {
			t.Fatalf("%s: message must cite the ADR: %q", gvk, msg)
		}
	}

	widget := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	if _, _, class, ok := Classify(widget, false); ok || class != ClassificationNoAdapter {
		t.Fatalf("unknown kind with containerNames: ok=%v class=%v, want ClassificationNoAdapter", ok, class)
	}
	if _, _, class, ok := Classify(widget, true); ok || class != ClassificationNoPathAdapter {
		t.Fatalf("imageFields without a resolver: ok=%v class=%v, want ClassificationNoPathAdapter", ok, class)
	}
	if msg := UnsupportedMessage(widget, ClassificationNoPathAdapter); !strings.Contains(msg, "resolver") {
		t.Fatalf("message must name the missing resolver: %q", msg)
	}
}

type t9uStubPathAdapter struct{ called bool }

func (s *t9uStubPathAdapter) Plan(
	*unstructured.Unstructured, []string, string,
) (*Plan, error) {
	s.called = true
	return &Plan{}, nil
}

func TestPathAdapterSlotIsTheT10Seam(t *testing.T) {
	if LookupPathAdapter() != nil {
		t.Fatal("the generic resolver slot must be empty until T10 registers it")
	}
	stub := &t9uStubPathAdapter{}
	RegisterPathAdapter(stub)
	defer RegisterPathAdapter(nil)
	if LookupPathAdapter() != stub {
		t.Fatal("RegisterPathAdapter must install the resolver T10 provides")
	}
	if _, p, _, ok := Classify(schema.GroupVersionKind{Group: "example.com", Version: "v1",
		Kind: "Widget"}, true); !ok || p != stub {
		t.Fatal("imageFields targets must classify onto the registered resolver")
	}
}

func TestFieldsV1OwnsPath(t *testing.T) {
	// Representative managedFields encodings of the pod-template image space.
	appOwned := []byte(`{"f:spec":{"f:template":{"f:spec":{"f:containers":{` +
		`"k:{\"name\":\"app\"}":{"f:image":{}}}}}}}`)
	appAndSide := []byte(`{"f:spec":{"f:template":{"f:spec":{"f:containers":{` +
		`"k:{\"name\":\"app\"}":{"f:image":{}},"k:{\"name\":\"sidecar\"}":{"f:image":{}}}}}}}`)
	siblingOnly := []byte(`{"f:spec":{"f:template":{"f:spec":{"f:containers":{` +
		`"k:{\"name\":\"app\"}":{"f:imagePullPolicy":{}}}}}}}`)

	appPath := mustParse(t, ".spec.template.spec.containers[name=app].image")
	sidePath := mustParse(t, ".spec.template.spec.containers[name=sidecar].image")

	if !FieldsV1OwnsPath(appOwned, appPath) {
		t.Fatal("the owner of containers[name=app].image must be detected")
	}
	if FieldsV1OwnsPath(appOwned, sidePath) {
		t.Fatal("a different container element must not match (field-level exclusivity)")
	}
	if FieldsV1OwnsPath(siblingOnly, appPath) {
		t.Fatal("a sibling leaf (imagePullPolicy) must not match the image leaf")
	}
	if !FieldsV1OwnsPath(appAndSide, appPath) || !FieldsV1OwnsPath(appAndSide, sidePath) {
		t.Fatal("both elements of a multi-container entry must match")
	}
	if FieldsV1OwnsPath([]byte(`{`), appPath) || FieldsV1OwnsPath(nil, appPath) {
		t.Fatal("malformed fieldsV1 must answer false (conservative)")
	}
	if FieldsV1OwnsPath([]byte(`{"f:spec":{}}`), appPath) {
		t.Fatal("a leaf-shaped parent must not match a deeper path (conservative)")
	}
}

func mustParse(t *testing.T, raw string) *imagefields.Path {
	t.Helper()
	p, err := imagefields.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	return p
}

func TestInspectOwnershipClassifiesManagerClasses(t *testing.T) {
	const (
		self    = FieldManagerPrefix + "t9-apps-sub-self"
		other   = FieldManagerPrefix + "t9-other-sub-other"
		foreign = "kubectl"
	)
	ownedBy := func(manager, raw string) managedFieldsFixture {
		return managedFieldsFixture{manager: manager, fieldsV1: raw}
	}
	appImage := `{"f:spec":{"f:template":{"f:spec":{"f:containers":{` +
		`"k:{\"name\":\"app\"}":{"f:image":{}}}}}}}`
	otherField := `{"f:metadata":{"f:labels":{}}}`
	path := mustParse(t, ".spec.template.spec.containers[name=app].image")
	fields := []FieldPlan{{Path: path, Container: t9uContainerApp}}

	cases := []struct {
		name    string
		entries []managedFieldsFixture
		want    Ownership
		wantSub []string
	}{
		{"unowned", nil, OwnershipUnowned, nil},
		{"self only", []managedFieldsFixture{ownedBy(self, appImage)}, OwnershipSelf, nil},
		{"self plus foreign", []managedFieldsFixture{
			ownedBy(self, appImage), ownedBy(foreign, appImage)}, OwnershipSelf, nil},
		{"foreign only", []managedFieldsFixture{ownedBy(foreign, appImage)}, OwnershipForeign, nil},
		{"other subscription", []managedFieldsFixture{ownedBy(other, appImage)},
			OwnershipOtherSubscription, []string{other}},
		{"other subscription plus foreign", []managedFieldsFixture{
			ownedBy(foreign, appImage), ownedBy(other, appImage)},
			OwnershipOtherSubscription, []string{other}},
		{"foreign on an unrelated field", []managedFieldsFixture{ownedBy(foreign, otherField)},
			OwnershipUnowned, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := InspectOwnership(t9uEntries(tc.entries), fields, self)
			if len(got) != 1 {
				t.Fatalf("inspected %d paths, want 1", len(got))
			}
			if got[0].Ownership != tc.want {
				t.Fatalf("ownership = %v, want %v", got[0].Ownership, tc.want)
			}
			if strings.Join(got[0].SubscriptionManagers, ",") != strings.Join(tc.wantSub, ",") {
				t.Fatalf("subscription managers = %v, want %v", got[0].SubscriptionManagers, tc.wantSub)
			}
			if got[0].Path != path.String() {
				t.Fatalf("path = %q", got[0].Path)
			}
		})
	}
}

// managedFieldsFixture is one managedFields entry of a test object.
type managedFieldsFixture struct {
	manager  string
	fieldsV1 string
}

// t9uEntries renders fixtures into the API shape InspectOwnership consumes.
func t9uEntries(fixtures []managedFieldsFixture) []metav1.ManagedFieldsEntry {
	entries := make([]metav1.ManagedFieldsEntry, 0, len(fixtures))
	for _, f := range fixtures {
		entries = append(entries, metav1.ManagedFieldsEntry{
			Manager:    f.manager,
			Operation:  metav1.ManagedFieldsOperationApply,
			FieldsType: "FieldsV1",
			FieldsV1:   &metav1.FieldsV1{Raw: []byte(f.fieldsV1)},
		})
	}
	return entries
}

// --- CronJob delivery adapter (T3) ------------------------------------------

// t9uCronJob renders a batch/v1 CronJob fixture with containers nested under
// .spec.jobTemplate.spec.template.spec.containers — the path that distinguishes
// CronJob from Deployment/StatefulSet/DaemonSet.
func t9uCronJob(t *testing.T, containers ...[2]string) *unstructured.Unstructured {
	t.Helper()
	var list strings.Builder
	for _, c := range containers {
		list.WriteString("            - name: " + c[0] + "\n              image: " + c[1] + "\n")
	}
	doc := "apiVersion: batch/v1\nkind: CronJob\nmetadata:\n  name: t9-nightly\n  namespace: t9-apps\n" +
		"spec:\n  schedule: \"0 2 * * *\"\n  jobTemplate:\n    spec:\n      template:\n        spec:\n          containers:\n" + list.String()
	obj := &unstructured.Unstructured{}
	if err := yaml.Unmarshal([]byte(doc), &obj.Object); err != nil {
		t.Fatalf("render CronJob fixture: %v", err)
	}
	return obj
}

func TestCronJobAdapterIsRegistered(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKCronJob)
	if adapter == nil {
		t.Fatal("batch/v1 CronJob must have a registered delivery adapter (T3)")
	}
	if adapter.GVK() != t9uGVKCronJob {
		t.Fatalf("adapter GVK = %v, want %v", adapter.GVK(), t9uGVKCronJob)
	}
}

func TestCronJobPlanSingleContainer(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKCronJob)
	obj := t9uCronJob(t,
		[2]string{t9uContainerApp, "quay.io/example/batch:v1"},
		[2]string{t9uContainerSide, "quay.io/platform/logger:v2"})

	plan, err := adapter.Plan(obj, []string{t9uContainerApp}, t9uDigestRef)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Fields) != 1 {
		t.Fatalf("fields = %d, want 1", len(plan.Fields))
	}
	f := plan.Fields[0]
	if f.Container != t9uContainerApp || !f.Found || f.Observed != "quay.io/example/batch:v1" {
		t.Fatalf("unexpected field plan: %+v", f)
	}
	wantPath := ".spec.jobTemplate.spec.template.spec.containers[name=app].image"
	if f.Path.String() != wantPath {
		t.Fatalf("path = %q, want %q", f.Path.String(), wantPath)
	}

	// Verify apply configuration has the nested jobTemplate structure.
	ac := plan.Apply
	if ac.GetKind() != t9uKindCronJob || ac.GroupVersionKind().GroupVersion().String() != t9uAPIBatchV1 {
		t.Fatalf("apply configuration TypeMeta = %v", ac.GroupVersionKind())
	}
	if ac.GetName() != obj.GetName() || ac.GetNamespace() != obj.GetNamespace() {
		t.Fatal("apply configuration must carry the target identity")
	}
	if got := ac.GetAnnotations()[ManagedByAnnotation]; got != ManagedByAnnotationValue {
		t.Fatalf("audit annotation = %q, want %q", got, ManagedByAnnotationValue)
	}
	// Navigate the nested path: spec.jobTemplate.spec.template.spec.containers
	containers, found, err := unstructured.NestedSlice(ac.Object,
		fieldSpec, "jobTemplate", fieldSpec, fieldTemplate, fieldSpec, fieldContainers)
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("declared containers = %v (found=%v err=%v)", containers, found, err)
	}
	entry := containers[0].(map[string]interface{})
	if entry[fieldName] != t9uContainerApp || entry[fieldImage] != t9uDigestRef {
		t.Fatalf("declared container = %v", entry)
	}
	if len(entry) != 2 {
		t.Fatalf("apply configuration must declare ONLY name+image, got %v", entry)
	}
	// Sibling container must not leak.
	encoded, _ := yaml.Marshal(ac.Object)
	if strings.Contains(string(encoded), t9uContainerSide) {
		t.Fatalf("sibling container leaked into the apply configuration:\n%s", encoded)
	}
}

func TestCronJobPlanMultipleContainers(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKCronJob)
	obj := t9uCronJob(t,
		[2]string{t9uContainerApp, "quay.io/example/batch:v1"},
		[2]string{t9uContainerSide, "quay.io/platform/logger:v2"})

	plan, err := adapter.Plan(obj, []string{t9uContainerApp, t9uContainerSide}, t9uDigestRef)
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(plan.Fields) != 2 {
		t.Fatalf("fields = %d, want 2", len(plan.Fields))
	}
	for i, want := range []struct {
		container string
		observed  string
		path      string
	}{
		{t9uContainerApp, "quay.io/example/batch:v1",
			".spec.jobTemplate.spec.template.spec.containers[name=app].image"},
		{t9uContainerSide, "quay.io/platform/logger:v2",
			".spec.jobTemplate.spec.template.spec.containers[name=sidecar].image"},
	} {
		f := plan.Fields[i]
		if f.Container != want.container || !f.Found || f.Observed != want.observed {
			t.Fatalf("field[%d] = %+v, want container=%s observed=%s", i, f, want.container, want.observed)
		}
		if f.Path.String() != want.path {
			t.Fatalf("field[%d] path = %q, want %q", i, f.Path.String(), want.path)
		}
	}

	// Apply configuration must declare both containers.
	ac := plan.Apply
	containers, found, err := unstructured.NestedSlice(ac.Object,
		fieldSpec, "jobTemplate", fieldSpec, fieldTemplate, fieldSpec, fieldContainers)
	if err != nil || !found || len(containers) != 2 {
		t.Fatalf("declared containers count = %d (found=%v err=%v)", len(containers), found, err)
	}
}

func TestCronJobPlanContainersNotFoundError(t *testing.T) {
	adapter := LookupKindAdapter(t9uGVKCronJob)
	obj := t9uCronJob(t, [2]string{t9uContainerApp, "quay.io/example/batch:v1"})

	_, err := adapter.Plan(obj, []string{t9uContainerApp, "ghost"}, t9uDigestRef)
	var missing *ContainersNotFoundError
	if !asContainersNotFound(err, &missing) {
		t.Fatalf("err = %v, want *ContainersNotFoundError", err)
	}
	if len(missing.Missing) != 1 || missing.Missing[0] != "ghost" {
		t.Fatalf("missing = %v", missing.Missing)
	}
	if len(missing.Present) != 1 || missing.Present[0] != t9uContainerApp {
		t.Fatalf("present = %v", missing.Present)
	}
	if missing.Kind != t9uKindCronJob {
		t.Fatalf("error kind = %q, want %q", missing.Kind, t9uKindCronJob)
	}
	if !strings.Contains(missing.Error(), "ghost") || !strings.Contains(missing.Error(), t9uContainerApp) {
		t.Fatalf("message must be diagnosable: %q", missing.Error())
	}

	// Empty containerNames must also be rejected.
	if _, err := adapter.Plan(obj, nil, t9uDigestRef); err == nil {
		t.Fatal("empty containerNames must be rejected (no default guessing, spec decision 6)")
	}
}

func TestClassifyServesCronJob(t *testing.T) {
	adapter, _, class, ok := Classify(t9uGVKCronJob, false)
	if !ok || class != 0 {
		t.Fatalf("CronJob with containerNames must be served: ok=%v class=%v", ok, class)
	}
	if adapter == nil || adapter.GVK() != t9uGVKCronJob {
		t.Fatalf("adapter mismatch: %v", adapter)
	}
}
