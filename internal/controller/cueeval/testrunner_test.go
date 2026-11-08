/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cueeval

import (
	"testing"
)

// rolloutPolicyCUE is a reusable policy for test cases.
const rolloutPolicyCUE = `
#Input: {
	replicas:      int
	readyReplicas: int
}
_allReady: #Input.replicas == #Input.readyReplicas
status:    "Succeeded" | "Progressing"
reason:    string
if _allReady {
	status: "Succeeded"
	reason: "AllReplicasReady"
}
if !_allReady {
	status: "Progressing"
	reason: "WaitingForReplicas"
}
`

// TestRunTests_AllPass verifies all test cases pass when expectations match.
func TestRunTests_AllPass(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 3
    readyReplicas: 3
  expected:
    status: Succeeded
    reason: AllReplicasReady
- input:
    replicas: 3
    readyReplicas: 1
  expected:
    status: Progressing
    reason: WaitingForReplicas
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	for i, r := range results {
		if !r.Passed {
			t.Errorf("test case %d: expected pass, got fail (actual=%+v, expected=%+v, err=%v)",
				i, r.Actual, r.Expected, r.Error)
		}
		if r.Error != nil {
			t.Errorf("test case %d: unexpected error: %v", i, r.Error)
		}
	}
}

// TestRunTests_SomeFail verifies failed test cases are correctly identified.
func TestRunTests_SomeFail(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 3
    readyReplicas: 3
  expected:
    status: Progressing
    reason: WrongReason
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected test case to fail (wrong expected status)")
	}
	if results[0].Actual.Status != "Succeeded" {
		t.Errorf("expected actual status Succeeded, got %s", results[0].Actual.Status)
	}
}

// TestRunTests_WildcardStatus verifies missing expected.status matches any status.
func TestRunTests_WildcardStatus(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 3
    readyReplicas: 3
  expected:
    reason: AllReplicasReady
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !results[0].Passed {
		t.Errorf("expected wildcard status match to pass, got fail")
	}
}

// TestRunTests_WildcardReason verifies missing expected.reason matches any reason.
func TestRunTests_WildcardReason(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 3
    readyReplicas: 3
  expected:
    status: Succeeded
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !results[0].Passed {
		t.Errorf("expected wildcard reason match to pass, got fail")
	}
}

// TestRunTests_WildcardBoth verifies both fields omitted matches anything.
func TestRunTests_WildcardBoth(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 3
    readyReplicas: 3
  expected: {}
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !results[0].Passed {
		t.Errorf("expected full wildcard match to pass, got fail")
	}
}

// TestRunTests_MalformedYAML verifies malformed YAML returns error, not panic.
func TestRunTests_MalformedYAML(t *testing.T) {
	e := NewEvaluator()

	malformedYAML := `
this is not valid yaml: [
  unclosed bracket
`
	_, err := e.RunTests(rolloutPolicyCUE, malformedYAML)
	if err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}

// TestRunTests_EmptyYAML verifies empty YAML string returns error.
func TestRunTests_EmptyYAML(t *testing.T) {
	e := NewEvaluator()

	_, err := e.RunTests(rolloutPolicyCUE, "")
	if err == nil {
		t.Fatal("expected error for empty YAML")
	}
}

// TestRunTests_EmptyArray verifies empty array returns error.
func TestRunTests_EmptyArray(t *testing.T) {
	e := NewEvaluator()

	_, err := e.RunTests(rolloutPolicyCUE, "[]")
	if err == nil {
		t.Fatal("expected error for empty test cases array")
	}
}

// TestRunTests_EvalErrorInTestCase verifies evaluation errors are captured in TestResult.Error.
func TestRunTests_EvalErrorInTestCase(t *testing.T) {
	e := NewEvaluator()

	// Input missing required field 'replicas'
	testCasesYAML := `
- input:
    readyReplicas: 3
  expected:
    status: Succeeded
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("RunTests should not return error for eval failures: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Passed {
		t.Error("expected test case to fail due to eval error")
	}
	if results[0].Error == nil {
		t.Error("expected non-nil Error for failed evaluation")
	}
}

// TestRunTests_IndexTracking verifies test case indices are correct.
func TestRunTests_IndexTracking(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 1
    readyReplicas: 1
  expected:
    status: Succeeded
- input:
    replicas: 2
    readyReplicas: 1
  expected:
    status: Progressing
- input:
    replicas: 3
    readyReplicas: 3
  expected:
    status: Succeeded
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Index != i {
			t.Errorf("result %d: expected Index=%d, got %d", i, i, r.Index)
		}
	}
}

// TestRunTests_InvalidPolicyWithValidTestCases verifies compile errors propagate per-case.
func TestRunTests_InvalidPolicyWithValidTestCases(t *testing.T) {
	e := NewEvaluator()

	badPolicy := `this is {{{ not valid CUE`
	testCasesYAML := `
- input:
    foo: bar
  expected:
    status: Succeeded
`
	results, err := e.RunTests(badPolicy, testCasesYAML)
	if err != nil {
		t.Fatalf("RunTests should not return error for compile failures: %v", err)
	}
	if results[0].Passed {
		t.Error("expected test case to fail due to compile error")
	}
	if results[0].Error == nil {
		t.Error("expected non-nil Error for compile failure")
	}

	evalErr, ok := AsEvalError(results[0].Error)
	if !ok {
		t.Fatalf("expected EvaluationError, got %T", results[0].Error)
	}
	if evalErr.Class != CompileError {
		t.Errorf("expected CompileError class, got %s", evalErr.Class)
	}
}

// TestRunTests_MixedPassFail verifies mixed results are reported correctly.
func TestRunTests_MixedPassFail(t *testing.T) {
	e := NewEvaluator()

	testCasesYAML := `
- input:
    replicas: 3
    readyReplicas: 3
  expected:
    status: Succeeded
- input:
    replicas: 3
    readyReplicas: 1
  expected:
    status: Succeeded
- input:
    replicas: 3
    readyReplicas: 1
  expected:
    status: Progressing
`
	results, err := e.RunTests(rolloutPolicyCUE, testCasesYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Case 0: pass (3/3 → Succeeded matches)
	if !results[0].Passed {
		t.Error("case 0: expected pass")
	}
	// Case 1: fail (1/3 → Progressing, but expected Succeeded)
	if results[1].Passed {
		t.Error("case 1: expected fail")
	}
	// Case 2: pass (1/3 → Progressing matches)
	if !results[2].Passed {
		t.Error("case 2: expected pass")
	}
}

// TestRunTests_InvalidStructure verifies non-array YAML returns error.
func TestRunTests_InvalidStructure(t *testing.T) {
	e := NewEvaluator()

	// Valid YAML but not an array
	notArray := `
key: value
another: thing
`
	_, err := e.RunTests(rolloutPolicyCUE, notArray)
	if err == nil {
		t.Fatal("expected error for non-array YAML structure")
	}
}

// TestMatchesExpected_UnitTests tests the matchesExpected helper directly.
func TestMatchesExpected_UnitTests(t *testing.T) {
	tests := []struct {
		name     string
		actual   Result
		expected ExpectedResult
		want     bool
	}{
		{
			name:     "exact match",
			actual:   Result{Status: "Succeeded", Reason: "Done"},
			expected: ExpectedResult{Status: "Succeeded", Reason: "Done"},
			want:     true,
		},
		{
			name:     "status mismatch",
			actual:   Result{Status: "Failed", Reason: "Done"},
			expected: ExpectedResult{Status: "Succeeded", Reason: "Done"},
			want:     false,
		},
		{
			name:     "reason mismatch",
			actual:   Result{Status: "Succeeded", Reason: "Wrong"},
			expected: ExpectedResult{Status: "Succeeded", Reason: "Done"},
			want:     false,
		},
		{
			name:     "wildcard status",
			actual:   Result{Status: "Anything", Reason: "Done"},
			expected: ExpectedResult{Reason: "Done"},
			want:     true,
		},
		{
			name:     "wildcard reason",
			actual:   Result{Status: "Succeeded", Reason: "Anything"},
			expected: ExpectedResult{Status: "Succeeded"},
			want:     true,
		},
		{
			name:     "full wildcard",
			actual:   Result{Status: "X", Reason: "Y"},
			expected: ExpectedResult{},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesExpected(&tt.actual, tt.expected)
			if got != tt.want {
				t.Errorf("matchesExpected() = %v, want %v", got, tt.want)
			}
		})
	}
}
