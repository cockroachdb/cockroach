// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
