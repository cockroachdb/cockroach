// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nakedgotest

func toot() {
	// The nolint comment below should have no effect.
	// For some reason though it shows up in the CommentMap
	// for the *ast.GoStmt, though. No idea why.

	//nolint:nakedgo should not help anyone
}

func A() {
	//nolint: I'm a noop
	go func() {}()                              // want `Use of go keyword not allowed, use a Stopper instead`
	go toot()                                   // want `Use of go keyword not allowed, use a Stopper instead`
	go /* nolint: nakedgo not helping */ toot() // want `Use of go keyword not allowed, use a Stopper instead`
	/* nolint: nakedgo nope */ go toot() // want `Use of go keyword not allowed, use a Stopper instead`
	//nolint:nakedgo nope, this one neither

	go func() {}() // want `Use of go keyword not allowed, use a Stopper instead`

	go func() {}() //nolint:nakedgo

	go toot() //nolint:nakedgo

	go func() { /* want `Use of go keyword not allowed, use a Stopper instead` */ //nolint:nakedgo
		_ = 0
	}()

	go func() { // want `Use of go keyword not allowed, use a Stopper instead`
		_ = 0 //nolint:nakedgo
	}()

	// Finally, doing it right!

	go func() {
		_ = 0
	}() //nolint:nakedgo
}
