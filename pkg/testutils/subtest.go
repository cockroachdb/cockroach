// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"fmt"
	"testing"
)

// RunTrueAndFalse calls the provided function in a subtest, first with the
// boolean argument set to false and next with the boolean argument set to true.
func RunTrueAndFalse(t *testing.T, name string, fn func(t *testing.T, b bool)) {
	for _, b := range []bool{false, true} {
		t.Run(fmt.Sprintf("%s=%t", name, b), func(t *testing.T) {
			fn(t, b)
		})
	}
}

// RunValues calls the provided function in a subtest for each of the
// provided values.
func RunValues(t *testing.T, name string, values []interface{}, fn func(*testing.T, interface{})) {
	t.Helper()
	for _, v := range values {
		t.Run(fmt.Sprintf("%s=%v", name, v), func(t *testing.T) {
			fn(t, v)
		})
	}
}
