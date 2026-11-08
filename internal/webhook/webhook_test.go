package webhook

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	imageflowv1alpha1 "github.com/appfun-dev/imageflow/api/v1alpha1"
)

const (
	createdByPatchPath = "/metadata/annotations/imageflow.appfun.dev~1created-by"
	testUserAlice      = "alice@example.com"
	testSACIUsername   = "system:serviceaccount:team-a:ci-bot"
	encodedSACreatedBy = "serviceAccount:team-a:ci-bot"
	thirdPartyPath     = ".spec.runtime.containers[name=edge].image"
)

func testSubscription() *imageflowv1alpha1.ImageSubscription {
	return &imageflowv1alpha1.ImageSubscription{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "team-a",
			Name:        "payments-stable",
			Annotations: map[string]string{"test": "keep"},
		},
		Spec: imageflowv1alpha1.ImageSubscriptionSpec{
			Source: imageflowv1alpha1.SubscriptionSourceRef{
				Kind:      sourceKindImageStreamTag,
				Name:      "payments-api:stable",
				Namespace: "imageflow-system",
			},
			Targets: []imageflowv1alpha1.SubscriptionTarget{{
				TargetRef: imageflowv1alpha1.TargetReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Namespace:  "team-a",
					Name:       "payments-api",
				},
				ContainerNames: []string{"payments-api"},
			}},
		},
	}
}

func thirdPartyTarget() imageflowv1alpha1.SubscriptionTarget {
	return imageflowv1alpha1.SubscriptionTarget{
		TargetRef: imageflowv1alpha1.TargetReference{
			APIVersion: "example.io/v1",
			Kind:       "WebService",
			Namespace:  "team-a",
			Name:       "payments-edge",
		},
		ImageFields: []string{thirdPartyPath},
	}
}

func TestCreatedByValueEncodesBothIdentityKinds(t *testing.T) {
	tests := []struct {
		name     string
		username string
		want     string
	}{
		{"user identity", testUserAlice, "user:" + testUserAlice},
		{"user identity with colons falls back verbatim", "system:node:n-0", "user:system:node:n-0"},
		{"service account identity", testSACIUsername, encodedSACreatedBy},
		{"malformed service account prefix stays a user", "system:serviceaccount:only-one-part",
			"user:system:serviceaccount:only-one-part"},
		{"empty username yields no value", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CreatedByValue(authenticationv1.UserInfo{Username: tc.username})
			if got != tc.want {
				t.Fatalf("CreatedByValue(%q) = %q, want %q", tc.username, got, tc.want)
			}
		})
	}
}

func TestParseCreatedByRoundTripsBothKinds(t *testing.T) {
	user, err := ParseCreatedBy("user:" + testUserAlice)
	if err != nil {
		t.Fatalf("ParseCreatedBy(user) failed: %v", err)
	}
	if user.Kind != CreatedByKindUser || user.Username != testUserAlice {
		t.Fatalf("unexpected user identity %+v", user)
	}
	if user.ServiceAccountNamespace != "" || user.ServiceAccountName != "" {
		t.Fatalf("user identity must not carry service account fields: %+v", user)
	}

	sa, err := ParseCreatedBy(encodedSACreatedBy)
	if err != nil {
		t.Fatalf("ParseCreatedBy(serviceAccount) failed: %v", err)
	}
	if sa.Kind != CreatedByKindServiceAccount {
		t.Fatalf("unexpected kind %q", sa.Kind)
	}
	// The SAR username must reconstruct to the canonical form (T8 contract).
	if sa.Username != testSACIUsername {
		t.Fatalf("Username = %q, want %q", sa.Username, testSACIUsername)
	}
	if sa.ServiceAccountNamespace != "team-a" || sa.ServiceAccountName != "ci-bot" {
		t.Fatalf("unexpected service account parts %+v", sa)
	}

	// Round-trip through the encoder.
	if value := CreatedByValue(authenticationv1.UserInfo{Username: testSACIUsername}); true {
		again, err := ParseCreatedBy(value)
		if err != nil || again.Username != testSACIUsername {
			t.Fatalf("round-trip failed: value=%q err=%v again=%+v", value, err, again)
		}
	}

	rejected := []string{
		"",
		"alice",
		"user:",
		"serviceAccount:team-a",
		"serviceAccount::ci-bot",
		"serviceAccount:team-a:ci:bot",
		"group:alice",
	}
	for _, value := range rejected {
		if _, err := ParseCreatedBy(value); err == nil {
			t.Errorf("ParseCreatedBy(%q) must fail", value)
		}
	}
}

func TestValidateSubscriptionShapeAccepts(t *testing.T) {
	accepted := map[string]func(sub *imageflowv1alpha1.ImageSubscription){
		"built-in kind by container names": func(*imageflowv1alpha1.ImageSubscription) {},
		"third-party kind by image fields": func(sub *imageflowv1alpha1.ImageSubscription) {
			sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{thirdPartyTarget()}
		},
		"several array elements, sibling leaves": func(sub *imageflowv1alpha1.ImageSubscription) {
			target := thirdPartyTarget()
			target.ImageFields = []string{
				".spec.containers[name=a].image",
				".spec.containers[name=b].image",
				".spec.containers[name=a].imagePullPolicy",
			}
			sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{target}
		},
		"non-first-batch kind keeps runtime Unsupported path": func(sub *imageflowv1alpha1.ImageSubscription) {
			target := thirdPartyTarget()
			target.TargetRef = imageflowv1alpha1.TargetReference{
				APIVersion: "batch/v1", Kind: "Job", Namespace: "team-a", Name: "batch-job",
			}
			target.ImageFields = nil
			target.ContainerNames = []string{"job"}
			sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{target}
		},
		"built-in kind name from another group is third-party": func(sub *imageflowv1alpha1.ImageSubscription) {
			target := thirdPartyTarget()
			target.TargetRef = imageflowv1alpha1.TargetReference{
				APIVersion: "example.io/v1", Kind: "Deployment", Namespace: "team-a", Name: "custom",
			}
			sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{target}
		},
		"multiple targets across namespaces": func(sub *imageflowv1alpha1.ImageSubscription) {
			other := thirdPartyTarget()
			other.TargetRef.Namespace = "team-b"
			sub.Spec.Targets = append(sub.Spec.Targets, other)
		},
	}
	for name, mutate := range accepted {
		t.Run(name, func(t *testing.T) {
			sub := testSubscription()
			mutate(sub)
			if err := validateSubscriptionShape(sub); err != nil {
				t.Fatalf("validateSubscriptionShape() = %v, want acceptance", err)
			}
		})
	}
}

func TestValidateSubscriptionShapeRejects(t *testing.T) {
	rejected := map[string]struct {
		mutate      func(sub *imageflowv1alpha1.ImageSubscription)
		wantMessage string
	}{
		"source without colon": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = "payments-api" },
			`<stream>:<tag>`,
		},
		"source with two colons": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = "a:b:c" },
			`exactly one ":"`,
		},
		"source with empty stream part": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = ":stable" },
			"valid ImageStream name",
		},
		"source with empty tag part": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = "payments-api:" },
			"OCI tag grammar",
		},
		"source stream part with separator": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = "pay--ments:stable" },
			`must not contain "--"`,
		},
		"source stream part not DNS-1123": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = "Payments_API:stable" },
			"RFC 1123",
		},
		"source tag with illegal characters": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Name = "payments-api:st@ble" },
			"OCI tag grammar",
		},
		"source kind other than ImageStreamTag": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Source.Kind = "ImageTag" },
			"Unsupported value",
		},
		"both locators set": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets[0].ImageFields = []string{thirdPartyPath}
			},
			"mutually exclusive",
		},
		"neither locator set": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Targets[0].ContainerNames = nil },
			"exactly one locator is required",
		},
		"built-in kind located by imageFields": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets[0].ContainerNames = nil
				sub.Spec.Targets[0].ImageFields = []string{".spec.template.spec.containers[name=app].image"}
			},
			"must locate images by containerNames",
		},
		"empty container name": {
			func(sub *imageflowv1alpha1.ImageSubscription) { sub.Spec.Targets[0].ContainerNames = []string{""} },
			"DNS-1123",
		},
		"invalid container name": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets[0].ContainerNames = []string{"Not A Label"}
			},
			"DNS-1123",
		},
		"duplicate container names": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets[0].ContainerNames = []string{"app", "app"}
			},
			"Duplicate",
		},
		"imageFields wildcard": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{thirdPartyTarget()}
				sub.Spec.Targets[0].ImageFields = []string{".spec.containers[*].image"}
			},
			"wildcards",
		},
		"imageFields numeric subscript": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{thirdPartyTarget()}
				sub.Spec.Targets[0].ImageFields = []string{".spec.containers[0].image"}
			},
			"numeric subscript",
		},
		"imageFields duplicate paths": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{thirdPartyTarget()}
				sub.Spec.Targets[0].ImageFields = []string{thirdPartyPath, thirdPartyPath}
			},
			"duplicates imageFields[0]",
		},
		"imageFields overlapping paths": {
			func(sub *imageflowv1alpha1.ImageSubscription) {
				sub.Spec.Targets = []imageflowv1alpha1.SubscriptionTarget{thirdPartyTarget()}
				sub.Spec.Targets[0].ImageFields = []string{".spec.runtime", thirdPartyPath}
			},
			"overlaps imageFields[0]",
		},
	}
	for name, tc := range rejected {
		t.Run(name, func(t *testing.T) {
			sub := testSubscription()
			tc.mutate(sub)
			err := validateSubscriptionShape(sub)
			if err == nil {
				t.Fatal("validateSubscriptionShape() = nil, want rejection")
			}
			if !apierrors.IsInvalid(err) {
				t.Fatalf("error must surface as Invalid (422): %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("error %q must contain %q", err.Error(), tc.wantMessage)
			}
		})
	}
}

// --- T3 (#24): imageReferenceFormat-conditional desiredImage validation ---

func TestValidateDesiredImageFormatsAccepts(t *testing.T) {
	const digestRef = "registry.example.com/payments/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	accepted := map[string]struct {
		format       imageflowv1alpha1.ImageReferenceFormat
		desiredImage string
	}{
		"empty desiredImage passes under every format (nothing delivered yet)": {
			imageflowv1alpha1.ImageReferenceFormatDigest, "",
		},
		"empty format means Digest (mutating defaulting, ADR-012 §3)": {
			"", digestRef,
		},
		"Digest format with digest reference": {
			imageflowv1alpha1.ImageReferenceFormatDigest, digestRef,
		},
		"Digest pattern tolerates a registry port (spec decision 3 regex)": {
			imageflowv1alpha1.ImageReferenceFormatDigest,
			"registry.example.com:5000/payments/api@sha256:" +
				"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		"Tag format with tag reference": {
			imageflowv1alpha1.ImageReferenceFormatTag, "registry.example.com/payments/api:stable",
		},
		"DigestWithTag format with composite reference": {
			imageflowv1alpha1.ImageReferenceFormatDigestWithTag,
			"registry.example.com/payments/api:stable@sha256:" +
				"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
	}
	for name, tc := range accepted {
		t.Run(name, func(t *testing.T) {
			sub := testSubscription()
			sub.Spec.ImageReferenceFormat = tc.format
			sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
				TargetRef:    sub.Spec.Targets[0].TargetRef,
				DesiredImage: tc.desiredImage,
			}}
			validator := &subscriptionValidator{}
			if _, err := validator.ValidateCreate(context.Background(), sub); err != nil {
				t.Fatalf("ValidateCreate() = %v, want acceptance", err)
			}
		})
	}
}

func TestValidateDesiredImageFormatsRejects(t *testing.T) {
	const digestRef = "registry.example.com/payments/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	rejected := map[string]struct {
		format       imageflowv1alpha1.ImageReferenceFormat
		desiredImage string
		wantMessage  string
	}{
		"Digest format rejects a bare tag reference": {
			imageflowv1alpha1.ImageReferenceFormatDigest,
			"registry.example.com/payments/api:stable",
			"imageReferenceFormat=Digest",
		},
		"Digest format rejects a short hex digest": {
			imageflowv1alpha1.ImageReferenceFormatDigest,
			"registry.example.com/payments/api@sha256:0123abcd",
			"imageReferenceFormat=Digest",
		},
		"Tag format rejects any digest-bearing reference": {
			imageflowv1alpha1.ImageReferenceFormatTag,
			digestRef,
			"imageReferenceFormat=Tag",
		},
		"Tag format rejects the composite reference": {
			imageflowv1alpha1.ImageReferenceFormatTag,
			"registry.example.com/payments/api:stable@sha256:" +
				"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"imageReferenceFormat=Tag",
		},
		"Tag format rejects whitespace": {
			imageflowv1alpha1.ImageReferenceFormatTag,
			"registry.example.com/payments/api:sta ble",
			"imageReferenceFormat=Tag",
		},
		"DigestWithTag format rejects a bare tag reference": {
			imageflowv1alpha1.ImageReferenceFormatDigestWithTag,
			"registry.example.com/payments/api:stable",
			"imageReferenceFormat=DigestWithTag",
		},
		"DigestWithTag format rejects a digest-only reference": {
			imageflowv1alpha1.ImageReferenceFormatDigestWithTag,
			digestRef,
			"imageReferenceFormat=DigestWithTag",
		},
		"unknown format is rejected as secondary enum defense": {
			"digest",
			digestRef,
			"Unsupported value",
		},
	}
	for name, tc := range rejected {
		t.Run(name, func(t *testing.T) {
			sub := testSubscription()
			sub.Spec.ImageReferenceFormat = tc.format
			sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
				TargetRef:    sub.Spec.Targets[0].TargetRef,
				DesiredImage: tc.desiredImage,
			}}
			validator := &subscriptionValidator{}
			_, err := validator.ValidateCreate(context.Background(), sub)
			if err == nil {
				t.Fatal("ValidateCreate() = nil, want rejection")
			}
			if !apierrors.IsInvalid(err) {
				t.Fatalf("error must surface as Invalid (422): %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("error %q must contain %q", err.Error(), tc.wantMessage)
			}
		})
	}
}

func TestValidateFormatTransition(t *testing.T) {
	const (
		digest        = imageflowv1alpha1.ImageReferenceFormatDigest
		tag           = imageflowv1alpha1.ImageReferenceFormatTag
		digestWithTag = imageflowv1alpha1.ImageReferenceFormatDigestWithTag
	)
	tests := []struct {
		name        string
		oldFormat   imageflowv1alpha1.ImageReferenceFormat
		newFormat   imageflowv1alpha1.ImageReferenceFormat
		wantReject  bool
		wantMessage string
	}{
		{"unchanged Digest is allowed", digest, digest, false, ""},
		{"unchanged Tag is allowed", tag, tag, false, ""},
		{"upgrade Digest to Tag is allowed", digest, tag, false, ""},
		{"upgrade Digest to DigestWithTag is allowed", digest, digestWithTag, false, ""},
		{"lateral Tag to DigestWithTag is allowed", tag, digestWithTag, false, ""},
		{"empty (pre-v1.2) to Digest is allowed", "", digest, false, ""},
		{"downgrade Tag to Digest is rejected", tag, digest, true, "downgrade"},
		{"downgrade DigestWithTag to Digest is rejected", digestWithTag, digest, true, "downgrade"},
		{"clearing Tag back to empty is a downgrade and is rejected", tag, "", true, "downgrade"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldSub := testSubscription()
			oldSub.Spec.ImageReferenceFormat = tc.oldFormat
			newSub := oldSub.DeepCopy()
			newSub.Spec.ImageReferenceFormat = tc.newFormat

			validator := &subscriptionValidator{}
			_, err := validator.ValidateUpdate(context.Background(), oldSub, newSub)
			if !tc.wantReject {
				if err != nil {
					t.Fatalf("ValidateUpdate() = %v, want acceptance", err)
				}
				return
			}
			if err == nil {
				t.Fatal("ValidateUpdate() = nil, want rejection")
			}
			if !apierrors.IsInvalid(err) {
				t.Fatalf("error must surface as Invalid (422): %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("error %q must contain %q", err.Error(), tc.wantMessage)
			}
		})
	}
}

// Stale desiredImage values written under a previous format must not block a
// spec update (the API server drops status on main-resource updates anyway);
// only values the request actually changes are validated against the current
// format (T3, #24).
func TestValidateDesiredImageStaleStatus(t *testing.T) {
	const digestRef = "registry.example.com/payments/api@sha256:" +
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	withStatus := func(sub *imageflowv1alpha1.ImageSubscription, desired string) *imageflowv1alpha1.ImageSubscription {
		sub.Status.Targets = []imageflowv1alpha1.TargetDeliveryStatus{{
			TargetRef:    sub.Spec.Targets[0].TargetRef,
			DesiredImage: desired,
		}}
		return sub
	}

	t.Run("stale digest value does not block the Digest to Tag upgrade", func(t *testing.T) {
		oldSub := withStatus(testSubscription(), digestRef)
		newSub := oldSub.DeepCopy()
		newSub.Spec.ImageReferenceFormat = imageflowv1alpha1.ImageReferenceFormatTag
		validator := &subscriptionValidator{}
		if _, err := validator.ValidateUpdate(context.Background(), oldSub, newSub); err != nil {
			t.Fatalf("ValidateUpdate() = %v, want acceptance (status is stale, not written)", err)
		}
	})

	t.Run("a changed value is validated against the current format", func(t *testing.T) {
		oldSub := withStatus(testSubscription(), digestRef)
		newSub := oldSub.DeepCopy()
		newSub.Spec.ImageReferenceFormat = imageflowv1alpha1.ImageReferenceFormatTag
		newSub.Status.Targets[0].DesiredImage = "registry.example.com/payments/api:stable@sha256:" +
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
		validator := &subscriptionValidator{}
		_, err := validator.ValidateUpdate(context.Background(), oldSub, newSub)
		if err == nil {
			t.Fatal("ValidateUpdate() = nil, want rejection of the newly written value")
		}
		if !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "imageReferenceFormat=Tag") {
			t.Fatalf("error must name the failing format: %v", err)
		}
	})
}

func TestValidateImageStreamShape(t *testing.T) {
	stream := &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: "imageflow-system", Name: "payments-api"},
	}
	if err := validateImageStreamShape(stream); err != nil {
		t.Fatalf("valid name rejected: %v", err)
	}

	stream.Name = "pay--ments"
	err := validateImageStreamShape(stream)
	if err == nil {
		t.Fatal(`name with "--" must be rejected`)
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("error must surface as Invalid: %v", err)
	}
	if !strings.Contains(err.Error(), `must not contain "--"`) {
		t.Fatalf("error %q must name the rule", err.Error())
	}
}

func TestImageStreamDefaulterFillsMissingInterval(t *testing.T) {
	stream := &imageflowv1alpha1.ImageStream{
		ObjectMeta: metav1.ObjectMeta{Namespace: "imageflow-system", Name: "payments-api"},
		Spec: imageflowv1alpha1.ImageStreamSpec{
			Tags: []imageflowv1alpha1.TagReference{
				{Name: "stable", ImportPolicy: imageflowv1alpha1.TagImportPolicy{Scheduled: ptr.To(true)}},
				{Name: "pinned", ImportPolicy: imageflowv1alpha1.TagImportPolicy{IntervalSeconds: ptr.To(int32(60))}},
				{Name: "latest"},
			},
		},
	}
	defaulter := &imageStreamDefaulter{}
	if err := defaulter.Default(context.Background(), stream); err != nil {
		t.Fatalf("Default() = %v", err)
	}
	if got := stream.Spec.Tags[0].ImportPolicy.IntervalSeconds; got == nil || *got != DefaultImportIntervalSeconds {
		t.Fatalf("missing interval must default to %d, got %v", DefaultImportIntervalSeconds, got)
	}
	if got := stream.Spec.Tags[1].ImportPolicy.IntervalSeconds; got == nil || *got != 60 {
		t.Fatalf("explicit interval must be kept, got %v", got)
	}
	if got := stream.Spec.Tags[2].ImportPolicy.IntervalSeconds; got == nil || *got != DefaultImportIntervalSeconds {
		t.Fatalf("absent importPolicy must still default to %d, got %v", DefaultImportIntervalSeconds, got)
	}
}

// --- subscriptionMutator.Handle unit tests (no envtest needed) ---

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := imageflowv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return s
}

func mustMarshal(t *testing.T, obj interface{}) []byte {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func newMutator(t *testing.T) *subscriptionMutator {
	t.Helper()
	return &subscriptionMutator{decoder: admission.NewDecoder(testScheme(t))}
}

func admissionRequest(op admissionv1.Operation, obj *imageflowv1alpha1.ImageSubscription,
	username string) admission.Request {
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: op,
		Object:    runtime.RawExtension{Raw: mustMarshalForRequest(obj)},
		UserInfo:  authenticationv1.UserInfo{Username: username},
	}}
}

func mustMarshalForRequest(obj *imageflowv1alpha1.ImageSubscription) []byte {
	raw, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	return raw
}

// createdByAfterPatches extracts the created-by annotation state from the
// JSON patch response. The test objects always carry a second annotation, so
// the diff is guaranteed to be per-key on the created-by path.
func createdByAfterPatches(t *testing.T, original []byte, resp admission.Response) (string, bool) {
	t.Helper()
	if !resp.Allowed {
		t.Fatalf("response not allowed: %+v", resp.Result)
	}
	fromOriginal := map[string]interface{}{}
	if err := json.Unmarshal(original, &fromOriginal); err != nil {
		t.Fatalf("unmarshal original: %v", err)
	}
	current, present := extractAnnotation(fromOriginal)
	for _, op := range resp.Patches {
		if op.Path != createdByPatchPath {
			continue
		}
		switch op.Operation {
		case "add", "replace":
			value, ok := op.Value.(string)
			if !ok {
				t.Fatalf("patch value is not a string: %+v", op.Value)
			}
			current, present = value, true
		case "remove":
			current, present = "", false
		default:
			t.Fatalf("unexpected patch operation %q", op.Operation)
		}
	}
	return current, present
}

func extractAnnotation(obj map[string]interface{}) (string, bool) {
	metadata, _ := obj["metadata"].(map[string]interface{})
	if metadata == nil {
		return "", false
	}
	annotations, _ := metadata["annotations"].(map[string]interface{})
	if annotations == nil {
		return "", false
	}
	value, ok := annotations[CreatedByAnnotation].(string)
	return value, ok
}

func TestSubscriptionMutatorInjectsCreatedByOnCreate(t *testing.T) {
	tests := []struct {
		name        string
		claimed     string // pre-set created-by annotation on the request object
		username    string
		wantValue   string
		wantPresent bool
	}{
		{"user identity is injected", "", testUserAlice, "user:" + testUserAlice, true},
		{"service account identity is injected", "", testSACIUsername, encodedSACreatedBy, true},
		{"user-supplied claim is overwritten", "user:evil", testUserAlice, "user:" + testUserAlice, true},
		{"claim without authenticated identity is stripped", "user:evil", "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sub := testSubscription()
			if tc.claimed != "" {
				sub.Annotations[CreatedByAnnotation] = tc.claimed
			}
			original := mustMarshal(t, sub)
			req := admissionRequest(admissionv1.Create, sub, tc.username)

			resp := newMutator(t).Handle(context.Background(), req)
			got, present := createdByAfterPatches(t, original, resp)
			if present != tc.wantPresent || got != tc.wantValue {
				t.Fatalf("created-by after patches = (%q, %v), want (%q, %v); patches %+v",
					got, present, tc.wantValue, tc.wantPresent, resp.Patches)
			}
		})
	}
}

func TestSubscriptionMutatorKeepsCreatedByImmutableOnUpdate(t *testing.T) {
	stored := testSubscription()
	stored.Annotations[CreatedByAnnotation] = encodedSACreatedBy

	t.Run("a writer cannot re-attribute the creator", func(t *testing.T) {
		updated := stored.DeepCopy()
		updated.Annotations[CreatedByAnnotation] = "user:mallory"
		original := mustMarshal(t, updated)
		req := admissionRequest(admissionv1.Update, updated, "mallory")
		req.OldObject = runtime.RawExtension{Raw: mustMarshal(t, stored)}

		resp := newMutator(t).Handle(context.Background(), req)
		got, present := createdByAfterPatches(t, original, resp)
		if !present || got != encodedSACreatedBy {
			t.Fatalf("created-by after patches = (%q, %v), want the stored creator", got, present)
		}
	})

	t.Run("a writer cannot drop the annotation", func(t *testing.T) {
		updated := stored.DeepCopy()
		delete(updated.Annotations, CreatedByAnnotation)
		original := mustMarshal(t, updated)
		req := admissionRequest(admissionv1.Update, updated, "mallory")
		req.OldObject = runtime.RawExtension{Raw: mustMarshal(t, stored)}

		resp := newMutator(t).Handle(context.Background(), req)
		got, present := createdByAfterPatches(t, original, resp)
		if !present || got != encodedSACreatedBy {
			t.Fatalf("created-by after patches = (%q, %v), want the stored creator restored", got, present)
		}
	})

	t.Run("objects predating the webhook bootstrap from the updater", func(t *testing.T) {
		legacy := testSubscription() // no created-by annotation
		updated := legacy.DeepCopy()
		original := mustMarshal(t, updated)
		req := admissionRequest(admissionv1.Update, updated, testUserAlice)
		req.OldObject = runtime.RawExtension{Raw: mustMarshal(t, legacy)}

		resp := newMutator(t).Handle(context.Background(), req)
		got, present := createdByAfterPatches(t, original, resp)
		if !present || got != "user:"+testUserAlice {
			t.Fatalf("created-by after patches = (%q, %v), want bootstrap injection", got, present)
		}
	})
}

// TestStatusPolicyRefNamespaceValidation covers the T5 admission gate
// (ADR-013 D9): statusPolicyRef.namespace must be "imageflow-system".
// Policies are platform configuration; centralizing them in one namespace
// reduces the trust surface and matches the RBAC boundary.
func TestStatusPolicyRefNamespaceValidation(t *testing.T) {
	tests := []struct {
		name       string
		policyRef  *imageflowv1alpha1.StatusPolicyRef
		wantReject bool
	}{
		{
			name:       "nil statusPolicyRef is accepted",
			policyRef:  nil,
			wantReject: false,
		},
		{
			name: "imageflow-system namespace is accepted",
			policyRef: &imageflowv1alpha1.StatusPolicyRef{
				Name:      "my-policy",
				Namespace: "imageflow-system",
				Key:       "policy.cue",
			},
			wantReject: false,
		},
		{
			name: "non-imageflow-system namespace is rejected",
			policyRef: &imageflowv1alpha1.StatusPolicyRef{
				Name:      "my-policy",
				Namespace: "team-a",
				Key:       "policy.cue",
			},
			wantReject: true,
		},
		{
			name: "empty namespace is rejected",
			policyRef: &imageflowv1alpha1.StatusPolicyRef{
				Name:      "my-policy",
				Namespace: "",
				Key:       "policy.cue",
			},
			wantReject: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sub := testSubscription()
			sub.Spec.Targets[0].StatusPolicyRef = tc.policyRef
			v := &subscriptionValidator{}
			_, err := v.ValidateCreate(context.Background(), sub)
			if tc.wantReject {
				if err == nil {
					t.Fatal("expected rejection, got nil error")
				}
				if !apierrors.IsInvalid(err) {
					t.Fatalf("expected Invalid error, got %T: %v", err, err)
				}
				if !strings.Contains(err.Error(), "imageflow-system") {
					t.Errorf("error must reference imageflow-system constraint: %v", err)
				}
				if !strings.Contains(err.Error(), "ADR-013") {
					t.Errorf("error must reference ADR-013 D9: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected rejection: %v", err)
				}
			}
		})
	}
}

// TestStatusPolicyRefNamespaceValidationOnUpdate verifies the same namespace
// gate fires on update — the webhook marker already routes both verbs, but
// the validation function must also be called from ValidateUpdate.
func TestStatusPolicyRefNamespaceValidationOnUpdate(t *testing.T) {
	oldSub := testSubscription()
	newSub := testSubscription()
	newSub.Spec.Targets[0].StatusPolicyRef = &imageflowv1alpha1.StatusPolicyRef{
		Name:      "rogue-policy",
		Namespace: "default",
		Key:       "evil.cue",
	}
	v := &subscriptionValidator{}
	_, err := v.ValidateUpdate(context.Background(), oldSub, newSub)
	if err == nil {
		t.Fatal("update with non-imageflow-system statusPolicyRef.namespace must be rejected")
	}
	if !strings.Contains(err.Error(), "ADR-013") {
		t.Errorf("error must reference ADR-013 D9: %v", err)
	}
}

func TestSubscriptionMutatorRejectsUndecodableRequests(t *testing.T) {
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Object:    runtime.RawExtension{Raw: []byte("{not json")},
	}}
	resp := newMutator(t).Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatal("undecodable object must not be allowed")
	}
}
