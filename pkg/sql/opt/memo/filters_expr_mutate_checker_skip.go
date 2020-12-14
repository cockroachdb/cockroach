// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !crdb_test

package memo

type FiltersExprMutateChecker struct{}

// Init is a no-op in non-test builds.
func (fmc *FiltersExprMutateChecker) Init(filters FiltersExpr) {}

// CheckForMutation is a no-op in non-test builds.
func (fmc *FiltersExprMutateChecker) CheckForMutation(filters FiltersExpr) {}
