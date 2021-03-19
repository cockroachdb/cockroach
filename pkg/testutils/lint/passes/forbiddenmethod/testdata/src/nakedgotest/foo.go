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

func toot() {}

func F() {
	go func() {}() // want `Use of go keyword not allowed, use a Stopper instead`
	go toot()      // want `Use of go keyword not allowed, use a Stopper instead`

	go func() { // want `Use of go keyword not allowed, use a Stopper instead`
		var _ int // just something

		//nolint:nakedgo
	}()

	// NB: if we tried to use passesutil.HasNoLintComment, the nolint in the above func
	// would cause the below invocation to pass the linter (!)
	// I assume this has something to do with HasNoLintComment also grabbing the "enclosing"
	// scope of node under investigation. Either way, it is also not desirable for a nolint
	// comment somewhere within the invoked function to necessarily allow the go keyword;
	// for the time being we live without an ability to silence this lint.

	go func() { // want `Use of go keyword not allowed, use a Stopper instead`
	}()
}
