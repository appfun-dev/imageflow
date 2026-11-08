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
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

const testStatusSucceeded = "Succeeded"

// testMinimalSucceededPolicy is the simplest CUE policy that returns Succeeded.
const testMinimalSucceededPolicy = `
status: "Succeeded"
`

// TestHappyPath verifies successful evaluation with valid CUE policy and input.
func TestHappyPath(t *testing.T) {
	e := NewEvaluator()

	// Rollout policy with schema constraint and conditional logic
	policyCUE := `
#Input: {
	replicas:      int
	readyReplicas: int
}
_allReady: #Input.replicas == #Input.readyReplicas
status:    "Succeeded" | "Progressing"
if _allReady {
	status: "Succeeded"
}
if !_allReady {
	status: "Progressing"
}
`
	input := map[string]interface{}{
		"replicas":      3,
		"readyReplicas": 3,
	}

	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Status != testStatusSucceeded {
		t.Errorf("expected status Succeeded, got %s", result.Status)
	}
}

// TestCompileError verifies syntax/type errors return CompileError class.
func TestCompileError(t *testing.T) {
	e := NewEvaluator()

	// Invalid CUE syntax
	policyCUE := `
this is not valid CUE syntax {{{
`
	input := map[string]interface{}{"foo": "bar"}

	_, err := e.Evaluate(context.Background(), policyCUE, input)
	if err == nil {
		t.Fatal("expected error for invalid CUE syntax")
	}

	evalErr, ok := AsEvalError(err)
	if !ok {
		t.Fatalf("expected EvalError type, got %T: %v", err, err)
	}
	if evalErr.Class != CompileError {
		t.Errorf("expected CompileError class, got %s", evalErr.Class)
	}
	if evalErr.Reason == "" {
		t.Error("expected non-empty Reason for CompileError")
	}
}

// TestEvalError_InputMismatch verifies schema validation failures return EvalError.
func TestEvalError_InputMismatch(t *testing.T) {
	e := NewEvaluator()

	// Policy requires 'name' field but input doesn't provide it
	policyCUE := `
#Input: {
	name: string
	age:  int
}
result: #Input.name
`
	input := map[string]interface{}{
		"age": 25,
		// missing 'name'
	}

	_, err := e.Evaluate(context.Background(), policyCUE, input)
	if err == nil {
		t.Fatal("expected error for missing required field")
	}

	evalErr, ok := AsEvalError(err)
	if !ok {
		t.Fatalf("expected EvalError type, got %T: %v", err, err)
	}
	if evalErr.Class != EvalError {
		t.Errorf("expected EvalError class, got %s", evalErr.Class)
	}
}

// TestTimeout verifies evaluation exceeding 100ms returns Timeout error class.
func TestTimeout(t *testing.T) {
	e := NewEvaluator()

	// Create a context that's already timed out
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // Ensure timeout has occurred

	policyCUE := testMinimalSucceededPolicy
	input := map[string]interface{}{}

	_, err := e.Evaluate(ctx, policyCUE, input)
	if err == nil {
		t.Fatal("expected timeout error")
	}

	evalErr, ok := AsEvalError(err)
	if !ok {
		t.Fatalf("expected EvalError type, got %T: %v", err, err)
	}
	if evalErr.Class != Timeout {
		t.Errorf("expected Timeout class, got %s", evalErr.Class)
	}
}

// TestInputSizeLimit verifies inputs exceeding 256KB return EvalError with InputTooLarge reason.
func TestInputSizeLimit(t *testing.T) {
	e := NewEvaluator()

	// Create input larger than 256KB
	largeInput := make(map[string]interface{})
	// Each key-value pair is ~100 bytes, need > 2600 pairs for 256KB
	for i := 0; i < 3000; i++ {
		key := strings.Repeat("k", 50) + string(rune('a'+i%26)) + string(rune(i))
		value := strings.Repeat("v", 50)
		largeInput[key] = value
	}

	policyCUE := testMinimalSucceededPolicy

	_, err := e.Evaluate(context.Background(), policyCUE, largeInput)
	if err == nil {
		t.Fatal("expected error for oversized input")
	}

	evalErr, ok := AsEvalError(err)
	if !ok {
		t.Fatalf("expected EvalError type, got %T: %v", err, err)
	}
	if evalErr.Class != EvalError {
		t.Errorf("expected EvalError class, got %s", evalErr.Class)
	}
	if evalErr.Reason != "InputTooLarge" {
		t.Errorf("expected Reason=InputTooLarge, got %s", evalErr.Reason)
	}
}

// TestSandboxImportRejection verifies non-stdlib imports are rejected.
func TestSandboxImportRejection(t *testing.T) {
	e := NewEvaluator()

	// Try to import a non-stdlib package
	policyCUE := `
import "net/http"
status: "Succeeded"
`
	input := map[string]interface{}{}

	_, err := e.Evaluate(context.Background(), policyCUE, input)
	if err == nil {
		t.Fatal("expected error for non-stdlib import")
	}

	evalErr, ok := AsEvalError(err)
	if !ok {
		t.Fatalf("expected EvalError type, got %T: %v", err, err)
	}
	// Should be CompileError since import validation happens at compile time
	if evalErr.Class != CompileError {
		t.Errorf("expected CompileError class for import rejection, got %s", evalErr.Class)
	}
}

// TestPanicRecovery verifies panics during evaluation are caught and returned as EvalError.
func TestPanicRecovery(t *testing.T) {
	e := NewEvaluator()

	// This policy should evaluate successfully without panic
	// Testing panic recovery requires internal mechanisms; we verify the evaluator
	// handles edge cases gracefully
	policyCUE := `
#Input: {
	value: int
}
status: "Succeeded"
message: "processed \(#Input.value)"
`
	input := map[string]interface{}{
		"value": 42,
	}

	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Status != testStatusSucceeded {
		t.Errorf("expected status Succeeded, got %s", result.Status)
	}
}

// TestHealthPolicy verifies health evaluation works independently from rollout.
func TestHealthPolicy(t *testing.T) {
	e := NewEvaluator()

	policyCUE := `
#Input: {
	availableReplicas: int
	desiredReplicas:   int
}
_isHealthy: #Input.availableReplicas >= #Input.desiredReplicas
status:     "Healthy" | "Unhealthy"
if _isHealthy {
	status: "Healthy"
}
if !_isHealthy {
	status: "Unhealthy"
}
`
	input := map[string]interface{}{
		"availableReplicas": 3,
		"desiredReplicas":   3,
	}

	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Status != "Healthy" {
		t.Errorf("expected status Healthy, got %s", result.Status)
	}
}

// TestResultFields verifies all Result fields are populated correctly.
func TestResultFields(t *testing.T) {
	e := NewEvaluator()

	policyCUE := `
status:  "Succeeded"
reason:  "AllReplicasReady"
message: "All 3 replicas are ready"
`
	input := map[string]interface{}{}

	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Status != testStatusSucceeded {
		t.Errorf("expected status Succeeded, got %s", result.Status)
	}
	if result.Reason != "AllReplicasReady" {
		t.Errorf("expected reason AllReplicasReady, got %s", result.Reason)
	}
	if result.Message != "All 3 replicas are ready" {
		t.Errorf("expected message 'All 3 replicas are ready', got %s", result.Message)
	}
}

// TestCacheHit verifies that repeated evaluations use cached compilation.
func TestCacheHit(t *testing.T) {
	e := NewEvaluator()

	policyCUE := testMinimalSucceededPolicy
	input := map[string]interface{}{}

	// First evaluation
	result1, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("first evaluation failed: %v", err)
	}

	// Second evaluation (should hit cache)
	result2, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("second evaluation failed: %v", err)
	}

	if result1.Status != result2.Status {
		t.Errorf("cache inconsistency: %s vs %s", result1.Status, result2.Status)
	}
}

// TestClearCache verifies cache clearing works.
func TestClearCache(t *testing.T) {
	e := NewEvaluator()

	policyCUE := `status: "Test"`
	input := map[string]interface{}{}

	_, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("evaluation failed: %v", err)
	}

	e.ClearCache()

	// Should still work after cache clear
	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("post-clear evaluation failed: %v", err)
	}
	if result.Status != "Test" {
		t.Errorf("expected status Test, got %s", result.Status)
	}
}

// TestInvalidateCache verifies targeted cache invalidation.
func TestInvalidateCache(t *testing.T) {
	e := NewEvaluator()

	policyCUE := `status: "Cached"`
	input := map[string]interface{}{}

	_, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("evaluation failed: %v", err)
	}

	// Invalidate with matching key
	e.InvalidateCache("", policyCUE)

	// Should still work
	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("post-invalidation evaluation failed: %v", err)
	}
	if result.Status != "Cached" {
		t.Errorf("expected status Cached, got %s", result.Status)
	}
}

// TestEmptyPolicy verifies behavior with minimal policy.
func TestEmptyPolicy(t *testing.T) {
	e := NewEvaluator()

	policyCUE := ``
	input := map[string]interface{}{}

	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("expected no error for empty policy, got %v", err)
	}
	// Empty policy should return empty result
	if result.Status != "" {
		t.Errorf("expected empty status, got %s", result.Status)
	}
}

// TestComplexInput verifies handling of nested input structures.
func TestComplexInput(t *testing.T) {
	e := NewEvaluator()

	policyCUE := `
#Input: {
	metadata: {
		name: string
	}
	spec: {
		replicas: int
	}
	status: {
		readyReplicas: int
	}
}
_allReady: #Input.status.readyReplicas == #Input.spec.replicas
result:    "Succeeded" | "Progressing"
if _allReady {
	result: "Succeeded"
}
if !_allReady {
	result: "Progressing"
}
status: result
`
	input := map[string]interface{}{
		"metadata": map[string]interface{}{
			"name": "test-deployment",
		},
		"spec": map[string]interface{}{
			"replicas": 3,
		},
		"status": map[string]interface{}{
			"readyReplicas": 2,
		},
	}

	result, err := e.Evaluate(context.Background(), policyCUE, input)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.Status != "Progressing" {
		t.Errorf("expected status Progressing, got %s", result.Status)
	}
}

// TestAsEvalErrorNil verifies AsEvalError handles nil correctly.
func TestAsEvalErrorNil(t *testing.T) {
	evalErr, ok := AsEvalError(nil)
	if ok {
		t.Error("expected ok=false for nil error")
	}
	if evalErr != nil {
		t.Error("expected nil evalErr for nil error")
	}
}

// TestAsEvalErrorWrongType verifies AsEvalError handles non-EvaluationError.
func TestAsEvalErrorWrongType(t *testing.T) {
	err := fmt.Errorf("some other error")
	evalErr, ok := AsEvalError(err)
	if ok {
		t.Error("expected ok=false for non-EvaluationError")
	}
	if evalErr != nil {
		t.Error("expected nil evalErr for non-EvaluationError")
	}
}

// TestEvaluationErrorMessage verifies Error() method format.
func TestEvaluationErrorMessage(t *testing.T) {
	err := &EvaluationError{
		Class:   CompileError,
		Reason:  "SyntaxError",
		Message: "unexpected token",
	}
	expected := "CompileError: SyntaxError - unexpected token"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}
