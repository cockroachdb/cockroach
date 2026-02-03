// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provides tests for utility functions.
// These tests verify plan ID generation, validation, extraction,
// and function name retrieval functionality.
package planners

import (
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
}
