// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provide utility functions for the Task Execution Framework (TEF).
// This file contains helper functions for plan ID generation, validation, function name
// extraction, and child workflow ID construction. These utilities support plan execution
// and identification across the TEF system.
package planners

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

const (
	// PlanIDPrefix is prepended to plan names.
	PlanIDPrefix = "tef."
)

// funcForPC is a variable that holds the function for getting function info from a PC.
// This allows for testing edge cases where runtime.FuncForPC might return nil.
var funcForPC = runtime.FuncForPC

// GetPlanID generates a plan ID by combining a prefix, the planner's name, and a given variant for unique identification.
func GetPlanID(planName, planVariant string) string {
	return fmt.Sprintf("%s%s.%s", PlanIDPrefix, planName, planVariant)
}

// IsValidPlanID returns true if the plan ID starts with the expected prefix.
func IsValidPlanID(planID string) bool {
	return strings.HasPrefix(planID, PlanIDPrefix)
}

// ExtractPlanNameAndVariant extracts the plan name and variant from a plan ID.
// Plan IDs are in the format "tef_plan_{planName}.{variant}".
// This function extracts the plan name and variant portions.
func ExtractPlanNameAndVariant(planID string) (string, string, error) {
	if !IsValidPlanID(planID) {
		return "", "", fmt.Errorf("invalid plan ID format: %s", planID)
	}

	// Remove the prefix
	remainder := strings.TrimPrefix(planID, PlanIDPrefix)

	// Find the first "." after the plan name
	// The plan name ends at the first "." (if there's a variant)
	// or at the end of the string (if there's no variant)
	parts := strings.SplitN(remainder, ".", 2)
	if len(parts) <= 1 {
		return "", "", fmt.Errorf("invalid plan ID format: %s", planID)
	}

	return parts[0], parts[1], nil
}

// GetFunctionName returns the actual function name using reflection.
// This is what Temporal uses as the activity name.
func GetFunctionName(fn interface{}) string {
	if fn == nil {
		return ""
	}
	// Validate that fn is a function before calling Pointer().
	fnVal := reflect.ValueOf(fn)
	if fnVal.Kind() != reflect.Func {
		return "<non-function>"
	}
	fnPtr := fnVal.Pointer()
	// funcForPC can return nil for certain edge cases (e.g., invalid PC values).
	runtimeFunc := funcForPC(fnPtr)
	if runtimeFunc == nil {
		return "<unknown-function>"
	}
	fullName := runtimeFunc.Name()
	// Extract just the function name without the package path.
	// e.g., "github.com/cockroachdb/cockroach/pkg/cmd/tef/planners.setupCluster" -> "setupCluster"
	// Note: strings.Split always returns at least one element, so parts[len(parts)-1] is always safe.
	parts := strings.Split(fullName, ".")
	return parts[len(parts)-1]
}

// GetChildWFID constructs a dot-separated string from workflowID, taskName, planeName, and planVariant.
func GetChildWFID(childWorkflowID, taskName, planName, planVariant string) string {
	return strings.Join([]string{childWorkflowID, taskName, planName, planVariant}, ".")
}
