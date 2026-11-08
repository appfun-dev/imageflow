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

	"sigs.k8s.io/yaml"
)

// TestCase represents a single test case from test-cases.yaml.
type TestCase struct {
	Input    map[string]interface{} `json:"input"`
	Expected ExpectedResult         `json:"expected"`
}

// ExpectedResult defines what a test case expects from evaluation.
// Missing fields are treated as wildcard (not checked).
type ExpectedResult struct {
	Status string `json:"status,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// TestResult holds the outcome of running a single test case.
type TestResult struct {
	// Index is the zero-based position of this test case in the YAML array.
	Index int
	// Passed indicates whether the actual result matches the expected result.
	Passed bool
	// Expected is the expected result from the test case definition.
	Expected Result
	// Actual is the result produced by evaluation (zero value if evaluation failed).
	Actual Result
	// Error is non-nil if evaluation itself failed (compile error, eval error, timeout).
	Error error
}

// RunTests executes test cases against a CUE policy and returns results.
// Per ADR-013 D7 and spec §Implementation Decisions #6:
//   - testCasesYAML is parsed from ConfigMap data key "test-cases.yaml"
//   - Each test case runs through the same evaluation pipeline (compile, validate, evaluate)
//   - Malformed YAML returns an error, not a panic
//   - Missing expected fields are treated as wildcard match (any value accepted)
func (e *Evaluator) RunTests(policyCUE string, testCasesYAML string) ([]TestResult, error) {
	if testCasesYAML == "" {
		return nil, fmt.Errorf("empty test cases YAML")
	}

	var testCases []TestCase
	if err := yaml.Unmarshal([]byte(testCasesYAML), &testCases); err != nil {
		return nil, fmt.Errorf("failed to parse test-cases.yaml: %w", err)
	}

	if len(testCases) == 0 {
		return nil, fmt.Errorf("test-cases.yaml contains no test cases")
	}

	results := make([]TestResult, len(testCases))
	ctx := context.Background()

	for i, tc := range testCases {
		tr := TestResult{
			Index: i,
			Expected: Result{
				Status: tc.Expected.Status,
				Reason: tc.Expected.Reason,
			},
		}

		actual, err := e.Evaluate(ctx, policyCUE, tc.Input)
		if err != nil {
			tr.Error = err
			// If we expected a specific status/reason but got an error, it's a failure
			tr.Passed = false
		} else {
			tr.Actual = *actual
			tr.Passed = matchesExpected(actual, tc.Expected)
		}

		results[i] = tr
	}

	return results, nil
}

// matchesExpected checks if the actual result matches the expected result.
// Missing expected fields are treated as wildcards (any value accepted).
func matchesExpected(actual *Result, expected ExpectedResult) bool {
	if expected.Status != "" && actual.Status != expected.Status {
		return false
	}
	if expected.Reason != "" && actual.Reason != expected.Reason {
		return false
	}
	return true
}
