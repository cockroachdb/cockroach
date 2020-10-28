// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvdoc_test

import "github.com/cockroachdb/cockroach/pkg/roachpb"

func ExampleTransactions_ParallelCommits() {
	var et *roachpb.EndTxnRequest

	// A parallel commit is a commit that has the following field populated; see
	// the comment within.
	_ = et.InFlightWrites
	// See also:
	_ = et.IsParallelCommit
}
