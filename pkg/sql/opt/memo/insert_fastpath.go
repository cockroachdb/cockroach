// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This module holds functions related to building insert fast path
// foreign key and uniqueness checks and deal mostly with structures
// defined in the memo package.

package memo

import "github.com/cockroachdb/cockroach/pkg/sql/mutations"

// ValuesLegalForInsertFastPath tests if `values` is a Values expression that
// has no subqueries or UDFs and has less rows than the max number of entries in
// a KV batch for a mutation operation.
func ValuesLegalForInsertFastPath(values *ValuesExpr) bool {
	//  - The input is Values with at most mutations.MaxBatchSize, and there are no
	//    subqueries;
	//    (note that mutations.MaxBatchSize() is a quantity of keys in the batch
	//     that we send, not a number of rows. We use this as a guideline only,
	//     and there is no guarantee that we won't produce a bigger batch.)
	if values.ChildCount() > mutations.MaxBatchSize(false /* forceProductionMaxBatchSize */) ||
		values.Relational().HasSubquery ||
		values.Relational().HasUDF {
		return false
	}
	return true
}
