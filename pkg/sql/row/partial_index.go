// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// PartialIndexUpdateHelper keeps track of partial indexes that should be not
// be updated during a mutation. When a newly inserted or updated row does not
// satisfy a partial index predicate, it should not be added to the index.
// Likewise, deleting a row from a partial index should not be attempted during
// an update or a delete when the row did not already exist in the partial
// index.
type PartialIndexUpdateHelper struct {
	// IgnoreForPut is a set of index IDs to ignore for Put operations.
	IgnoreForPut util.FastIntSet

	// IgnoreForDel is a set of index IDs to ignore for Del operations.
	IgnoreForDel util.FastIntSet
}

// Init initializes a PartialIndexUpdateHelper to track partial index IDs that
// should be ignored for Put and Del operations. The partialIndexPutVals and
// partialIndexDelVals arguments must be lists of boolean expressions where the
// i-th element corresponds to the i-th partial index defined on the table. If
// the expression evaluates to false, the index should be ignored.
//
// For example, partialIndexPutVals[2] evaluating to true indicates that the
// second partial index of the table should not be ignored for Put operations.
// Meanwhile, partialIndexPutVals[3] evaluating to false indicates that the
// third partial index should be ignored.
func (pm *PartialIndexUpdateHelper) Init(
	partialIndexPutVals tree.Datums, partialIndexDelVals tree.Datums, tabDesc catalog.TableDescriptor,
) error {
	colIdx := 0
	partialIndexOrds := tabDesc.PartialIndexOrds()
	indexes := tabDesc.DeletableIndexes()

	for i, ok := partialIndexOrds.Next(0); ok; i, ok = partialIndexOrds.Next(i + 1) {
		index := &indexes[i]
		if index.IsPartial() {

			// Check the boolean partial index put column, if it exists.
			if colIdx < len(partialIndexPutVals) {
				val, err := tree.GetBool(partialIndexPutVals[colIdx])
				if err != nil {
					return err
				}
				if !val {
					// If the value of the column for the index predicate
					// expression is false, the row should not be added to the
					// partial index.
					pm.IgnoreForPut.Add(int(index.ID))
				}
			}

			// Check the boolean partial index del column, if it exists.
			if colIdx < len(partialIndexDelVals) {
				val, err := tree.GetBool(partialIndexDelVals[colIdx])
				if err != nil {
					return err
				}
				if !val {
					// If the value of the column for the index predicate
					// expression is false, the row should not be removed from
					// the partial index.
					pm.IgnoreForDel.Add(int(index.ID))
				}
			}

			colIdx++
		}
	}

	return nil
}
