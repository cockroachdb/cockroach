// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import "fmt"

// RunTrueAndFalse calls the provided function in a subtest, first with the
// boolean argument set to false and next with the boolean argument set to true.
func RunTrueAndFalse[T testingTB[T]](t T, name string, fn func(t T, b bool)) {
	t.Helper()
	RunValues(t, name, []bool{false, true}, fn)
}

// RunValues calls the provided function in a subtest for each of the
// provided values.
func RunValues[T testingTB[T], V any](t T, name string, values []V, fn func(T, V)) {
	t.Helper()
	for _, v := range values {
		t.Run(fmt.Sprintf("%s=%v", name, v), func(t T) {
			fn(t, v)
		})
	}
}

// testingTB is an interface that matches *testing.T and *testing.B, without
// incurring the package dependency.
type testingTB[T any] interface {
	Run(name string, f func(t T)) bool
	Helper()
}
