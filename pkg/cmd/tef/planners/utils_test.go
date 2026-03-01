// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide tests for utility functions.
// These tests verify plan ID generation, validation, extraction,
// and function name retrieval functionality.
package planners

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPlanID(t *testing.T) {
	t.Run("generates plan ID with variant", func(t *testing.T) {
		planner := &BasePlanner{
			Name: "test_plan",
		}

		planID := GetPlanID(planner.Name, "worker1")
		require.Equal(t, "tef.test_plan.worker1", planID)
	})

	t.Run("generates plan ID with empty variant", func(t *testing.T) {
		planner := &BasePlanner{
			Name: "test_plan",
		}

		planID := GetPlanID(planner.Name, "")
		require.Equal(t, "tef.test_plan.", planID)
	})

	t.Run("generates plan ID with complex variant", func(t *testing.T) {
		planner := &BasePlanner{
			Name: "my_complex_plan",
		}

		planID := GetPlanID(planner.Name, "worker_123_abc")
		require.Equal(t, "tef.my_complex_plan.worker_123_abc", planID)
	})
}

func TestExtractPlanNameAndVariant(t *testing.T) {
	t.Run("extracts name and variant successfully", func(t *testing.T) {
		planID := "tef.test_plan.worker1"

		name, variant, err := ExtractPlanNameAndVariant(planID)
		require.NoError(t, err)
		require.Equal(t, "test_plan", name)
		require.Equal(t, "worker1", variant)
	})

	t.Run("extracts name with empty variant", func(t *testing.T) {
		planID := "tef.test_plan."

		name, variant, err := ExtractPlanNameAndVariant(planID)
		require.NoError(t, err)
		require.Equal(t, "test_plan", name)
		require.Equal(t, "", variant)
	})

	t.Run("extracts complex name and variant", func(t *testing.T) {
		planID := "tef.my_complex_plan.worker_123_abc"

		name, variant, err := ExtractPlanNameAndVariant(planID)
		require.NoError(t, err)
		require.Equal(t, "my_complex_plan", name)
		require.Equal(t, "worker_123_abc", variant)
	})

	t.Run("errors on invalid prefix", func(t *testing.T) {
		planID := "invalid_test_plan.worker1"

		_, _, err := ExtractPlanNameAndVariant(planID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid plan ID format")
	})

	t.Run("errors on missing delimiter", func(t *testing.T) {
		planID := "tef.test_plan"

		_, _, err := ExtractPlanNameAndVariant(planID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid plan ID format")
	})

	t.Run("roundtrip GetPlanID and ExtractPlanNameAndVariant", func(t *testing.T) {
		planner := &BasePlanner{
			Name: "test_plan",
		}
		variant := "worker1"

		planID := GetPlanID(planner.Name, variant)
		name, extractedVariant, err := ExtractPlanNameAndVariant(planID)

		require.NoError(t, err)
		require.Equal(t, planner.Name, name)
		require.Equal(t, variant, extractedVariant)
	})
}

func TestGetFunctionName(t *testing.T) {
	t.Run("returns function name for valid function", func(t *testing.T) {
		name := GetFunctionName(testExecutor1)
		require.Equal(t, "testExecutor1", name)
	})

	t.Run("returns empty string for nil function", func(t *testing.T) {
		name := GetFunctionName(nil)
		require.Equal(t, "", name)
	})

	t.Run("returns function name for different functions", func(t *testing.T) {
		name1 := GetFunctionName(testExecutor1)
		name2 := GetFunctionName(testExecutor2)
		name3 := GetFunctionName(testBoolExecutor)

		require.Equal(t, "testExecutor1", name1)
		require.Equal(t, "testExecutor2", name2)
		require.Equal(t, "testBoolExecutor", name3)
	})

	t.Run("returns non-function marker for non-function", func(t *testing.T) {
		name := GetFunctionName("not a function")
		require.Equal(t, "<non-function>", name)
	})

	t.Run("handles function with no package path", func(t *testing.T) {
		// Test with a closure that might have a simpler name structure
		simpleFn := func() {}
		name := GetFunctionName(simpleFn)
		// The name should still be extracted successfully
		require.NotEmpty(t, name)
	})

	t.Run("handles method on a type", func(t *testing.T) {
		type testStruct struct{}
		methodFn := func(ts testStruct) {}
		name := GetFunctionName(methodFn)
		require.NotEmpty(t, name)
	})

	t.Run("returns unknown-function when funcForPC returns nil", func(t *testing.T) {
		// Save original funcForPC
		originalFuncForPC := funcForPC
		defer func() {
			// Restore original funcForPC after test
			funcForPC = originalFuncForPC
		}()

		// Override funcForPC to return nil to simulate an edge case
		funcForPC = func(_ uintptr) *runtime.Func {
			return nil
		}

		// Test with any function
		testFn := func() {}
		name := GetFunctionName(testFn)
		require.Equal(t, "<unknown-function>", name)
	})
}

func TestIsValidPlanID(t *testing.T) {
	t.Run("returns true for valid plan ID", func(t *testing.T) {
		require.True(t, IsValidPlanID("tef.test_plan.worker1"))
		require.True(t, IsValidPlanID("tef.plan.variant"))
		require.True(t, IsValidPlanID("tef.my_plan."))
	})

	t.Run("returns false for invalid plan ID", func(t *testing.T) {
		require.False(t, IsValidPlanID("invalid.test_plan.worker1"))
		require.False(t, IsValidPlanID("test_plan.worker1"))
		require.False(t, IsValidPlanID(""))
		require.False(t, IsValidPlanID("tef"))
	})

	t.Run("returns false for plan ID without prefix", func(t *testing.T) {
		require.False(t, IsValidPlanID("plan.variant"))
	})
}

func TestGetChildWFID(t *testing.T) {
	t.Run("constructs child workflow ID", func(t *testing.T) {
		wfID := GetChildWFID("wf123", "task1", "plan_name", "variant1")
		require.Equal(t, "wf123.task1.plan_name.variant1", wfID)
	})

	t.Run("constructs child workflow ID with empty variant", func(t *testing.T) {
		wfID := GetChildWFID("wf123", "task1", "plan_name", "")
		require.Equal(t, "wf123.task1.plan_name.", wfID)
	})

	t.Run("constructs child workflow ID with complex names", func(t *testing.T) {
		wfID := GetChildWFID("workflow_123_abc", "my_task_456", "complex_plan", "worker_789")
		require.Equal(t, "workflow_123_abc.my_task_456.complex_plan.worker_789", wfID)
	})

	t.Run("constructs child workflow ID with all empty strings", func(t *testing.T) {
		wfID := GetChildWFID("", "", "", "")
		require.Equal(t, "...", wfID)
	})
}
