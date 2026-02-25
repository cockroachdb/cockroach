// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package planners provides utility functions for the Task Execution Framework (TEF).
package planners

import (
	"reflect"
	"runtime"
	"strings"
)

// GetFunctionName returns the name of a function using reflection.
// This is used to extract executor function names for display purposes.
func GetFunctionName(fn interface{}) string {
	if fn == nil {
		return ""
	}
	// Get the function's full path name
	fullName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	// Extract just the function name (last component after final '/')
	parts := strings.Split(fullName, "/")
	if len(parts) > 0 {
		// Get the last part and remove any package prefix before the final dot
		lastPart := parts[len(parts)-1]
		dotIndex := strings.LastIndex(lastPart, ".")
		if dotIndex >= 0 && dotIndex < len(lastPart)-1 {
			return lastPart[dotIndex+1:]
		}
		return lastPart
	}
	return fullName
}
