// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

// TB is a slimmed down version of testing.TB for use in helper functions
// by testing contexts which do not come from the stdlib testing package.
type TB interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	FailNow()
	Helper()
}
