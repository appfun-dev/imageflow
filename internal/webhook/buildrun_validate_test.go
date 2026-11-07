package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// BuildRun target annotation contract (ADR-011 §2): the Shipwright BuildRun
// type is deliberately NOT imported — ImageFlow only reads metadata, so the
// tests exercise the handler through unstructured objects exactly as the
// API server would send them.
func testBuildRun(t *testing.T, annotations map[string]string) *unstructured.Unstructured {
	t.Helper()
	metadata := map[string]interface{}{
		"namespace": "team-a",
		"name":      "kaniko-build-abc12",
	}
	if annotations != nil {
		asAny := make(map[string]interface{}, len(annotations))
		for k, v := range annotations {
			asAny[k] = v
		}
		metadata["annotations"] = asAny
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "shipwright.io/v1beta1",
		"kind":       "BuildRun",
		"metadata":   metadata,
	}}
}

func buildRunRequest(t *testing.T, op admissionv1.Operation, obj *unstructured.Unstructured) admission.Request {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal BuildRun: %v", err)
	}
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: op,
		Object:    runtime.RawExtension{Raw: raw},
	}}
}

func newBuildRunValidator(t *testing.T) *buildRunValidator {
	t.Helper()
	return &buildRunValidator{decoder: admission.NewDecoder(testScheme(t))}
}

func TestBuildRunValidatorAccepts(t *testing.T) {
	accepted := map[string]map[string]string{
		"no annotations at all": nil,
		"only unrelated annotations": {
			"shipwright.io/buildRun.name": "kaniko-build",
		},
		"valid stream and tag": {
			BuildRunTargetAnnotation: `{"stream":"my-stream","tag":"latest"}`,
		},
		"dotted names are RFC 1123": {
			BuildRunTargetAnnotation: `{"stream":"my.stream","tag":"v1.2.3"}`,
		},
		"unknown extra fields are forward-compatible": {
			BuildRunTargetAnnotation: `{"stream":"my-stream","tag":"latest","future":"extension"}`,
		},
	}
	for name, annotations := range accepted {
		t.Run(name, func(t *testing.T) {
			req := buildRunRequest(t, admissionv1.Create, testBuildRun(t, annotations))
			resp := newBuildRunValidator(t).Handle(context.Background(), req)
			if !resp.Allowed {
				t.Fatalf("request must be allowed, got %v", resp.Result)
			}
		})
	}
}

func TestBuildRunValidatorRejects(t *testing.T) {
	rejected := map[string]struct {
		annotation  string
		wantMessage string
	}{
		"malformed JSON": {
			`{"stream":"my-stream",`,
			"JSON object",
		},
		"JSON string instead of object": {
			`"my-stream:latest"`,
			"JSON object",
		},
		"JSON array instead of object": {
			`["my-stream","latest"]`,
			"JSON object",
		},
		"missing stream": {
			`{"tag":"latest"}`,
			"stream",
		},
		"missing tag": {
			`{"stream":"my-stream"}`,
			"tag",
		},
		"empty stream": {
			`{"stream":"","tag":"latest"}`,
			"stream",
		},
		"empty tag": {
			`{"stream":"my-stream","tag":""}`,
			"tag",
		},
		"stream with uppercase": {
			`{"stream":"My-Stream","tag":"latest"}`,
			"RFC 1123",
		},
		"stream with illegal characters": {
			`{"stream":"my_stream","tag":"latest"}`,
			"RFC 1123",
		},
		"tag with illegal characters": {
			`{"stream":"my-stream","tag":"st@ble"}`,
			"RFC 1123",
		},
		"tag with uppercase": {
			`{"stream":"my-stream","tag":"LATEST"}`,
			"RFC 1123",
		},
		"over-long stream name": {
			`{"stream":"` + strings.Repeat("a", 254) + `","tag":"latest"}`,
			"RFC 1123",
		},
		"non-string field value": {
			`{"stream":1,"tag":"latest"}`,
			"JSON object",
		},
	}
	for name, tc := range rejected {
		t.Run(name, func(t *testing.T) {
			req := buildRunRequest(t, admissionv1.Create, testBuildRun(t, map[string]string{
				BuildRunTargetAnnotation: tc.annotation,
			}))
			resp := newBuildRunValidator(t).Handle(context.Background(), req)
			if resp.Allowed {
				t.Fatalf("annotation %q must be rejected", tc.annotation)
			}
			if resp.Result == nil {
				t.Fatal("rejection must carry a status result")
			}
			if !strings.Contains(resp.Result.Message, tc.wantMessage) {
				t.Fatalf("rejection message %q must contain %q (it must name the failing field or constraint)",
					resp.Result.Message, tc.wantMessage)
			}
		})
	}
}

func TestBuildRunValidatorRejectsOnUpdate(t *testing.T) {
	req := buildRunRequest(t, admissionv1.Update, testBuildRun(t, map[string]string{
		BuildRunTargetAnnotation: `{"tag":"latest"}`,
	}))
	resp := newBuildRunValidator(t).Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatal("updates that introduce an invalid target annotation must be rejected")
	}
	if !strings.Contains(resp.Result.Message, "stream") {
		t.Fatalf("rejection message %q must name the missing field", resp.Result.Message)
	}
}

func TestBuildRunValidatorRejectionSurfacesAsInvalid(t *testing.T) {
	req := buildRunRequest(t, admissionv1.Create, testBuildRun(t, map[string]string{
		BuildRunTargetAnnotation: `{"stream":"My-Stream","tag":"latest"}`,
	}))
	resp := newBuildRunValidator(t).Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatal("invalid annotation must be rejected")
	}
	if resp.Result.Code != http.StatusUnprocessableEntity {
		t.Fatalf("rejection must surface as 422 Invalid like the other ImageFlow validators, got %d (%s)",
			resp.Result.Code, resp.Result.Message)
	}
}

func TestBuildRunValidatorRejectsUndecodableRequests(t *testing.T) {
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Object:    runtime.RawExtension{Raw: []byte("{not json")},
	}}
	resp := newBuildRunValidator(t).Handle(context.Background(), req)
	if resp.Allowed {
		t.Fatal("undecodable object must not be allowed")
	}
}
