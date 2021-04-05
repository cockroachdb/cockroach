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

// TB is a slimmed down version of testing.TB for use in helper functions
// by testing contexts which do not come from the stdlib testing package.
type TB interface {
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	FailNow()
	Helper()
}
