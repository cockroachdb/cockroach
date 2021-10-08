// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build crdb_test

package memo

import "github.com/cockroachdb/errors"

// FiltersExprMutateChecker is used to check if a FiltersExpr has been
// erroneously mutated. This code is called in crdb_test builds so that the
// check is run for tests, but the overhead is not incurred for non-test builds.
type FiltersExprMutateChecker struct {
	hasher hasher
	hash   internHash
}

// Init initializes a FiltersExprMutateChecker with the original filters.
func (fmc *FiltersExprMutateChecker) Init(filters FiltersExpr) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*fmc = FiltersExprMutateChecker{}
	fmc.hasher.Init()
	fmc.hasher.HashFiltersExpr(filters)
	fmc.hash = fmc.hasher.hash
}

// CheckForMutation panics if the given filters are not equal to the filters
// passed for the previous Init function call.
func (fmc *FiltersExprMutateChecker) CheckForMutation(filters FiltersExpr) {
	fmc.hasher.Init()
	fmc.hasher.HashFiltersExpr(filters)
	if fmc.hash != fmc.hasher.hash {
		panic(errors.AssertionFailedf("filters should not be mutated"))
	}
}
