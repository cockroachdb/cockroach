// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

// TestFataler is a slimmed down version of testing.TB for use in helper functions
// by testing contexts which do not come from the stdlib testing package.
type TestFataler interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Helper()
}

// TestErrorer is like Fataler but it only needs a Error method.
type TestErrorer interface {
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Helper()
}

// TestFatalerLogger is like Fataler but it also needs a Log method.
type TestFatalerLogger interface {
	TestFataler
	Logf(format string, args ...interface{})
}

// TestNamedFatalerLogger is like Fataler but it also needs a Name method.
type TestNamedFatalerLogger interface {
	TestFatalerLogger
	Name() string
}
