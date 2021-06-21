// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// IsInterleaved returns whether the index is interleaved or not.
func (desc *IndexDescriptor) IsInterleaved() bool {
	return len(desc.Interleave.Ancestors) > 0 || len(desc.InterleavedBy) > 0
}

// IsSharded returns whether the index is hash sharded or not.
func (desc *IndexDescriptor) IsSharded() bool {
	return desc.Sharded.IsSharded
}

// IsPartial returns true if the index is a partial index.
func (desc *IndexDescriptor) IsPartial() bool {
	return desc.Predicate != ""
}

// ExplicitColumnStartIdx returns the start index of any explicit columns.
func (desc *IndexDescriptor) ExplicitColumnStartIdx() int {
	start := int(desc.Partitioning.NumImplicitColumns)
	// We do not currently handle implicit columns along with hash sharded indexes.
	// Thus, safe to override this to 1.
	if desc.IsSharded() {
		start = 1
	}
	return start
}

// FillColumns sets the column names and directions in desc.
func (desc *IndexDescriptor) FillColumns(elems tree.IndexElemList) error {
	desc.KeyColumnNames = make([]string, 0, len(elems))
	desc.KeyColumnDirections = make([]IndexDescriptor_Direction, 0, len(elems))
	for _, c := range elems {
		if c.Expr != nil {
			return unimplemented.NewWithIssuef(9682, "only simple columns are supported as index elements")
		}
		desc.KeyColumnNames = append(desc.KeyColumnNames, string(c.Column))
		switch c.Direction {
		case tree.Ascending, tree.DefaultDirection:
			desc.KeyColumnDirections = append(desc.KeyColumnDirections, IndexDescriptor_ASC)
		case tree.Descending:
			desc.KeyColumnDirections = append(desc.KeyColumnDirections, IndexDescriptor_DESC)
		default:
			return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
		}
	}
	return nil
}

// IsValidOriginIndex returns whether the index can serve as an origin index for a foreign
// key constraint with the provided set of originColIDs.
func (desc *IndexDescriptor) IsValidOriginIndex(originColIDs ColumnIDs) bool {
	return !desc.IsPartial() && ColumnIDs(desc.KeyColumnIDs).HasPrefix(originColIDs)
}

// IsValidReferencedUniqueConstraint  is part of the UniqueConstraint interface.
// It returns whether the index can serve as a referenced index for a foreign
// key constraint with the provided set of referencedColumnIDs.
func (desc *IndexDescriptor) IsValidReferencedUniqueConstraint(referencedColIDs ColumnIDs) bool {
	return desc.Unique &&
		!desc.IsPartial() &&
		ColumnIDs(desc.KeyColumnIDs[desc.Partitioning.NumImplicitColumns:]).PermutationOf(referencedColIDs)
}

// GetName is part of the UniqueConstraint interface.
func (desc *IndexDescriptor) GetName() string {
	return desc.Name
}

// InvertedColumnID returns the ColumnID of the inverted column of the inverted
// index. This is always the last column in ColumnIDs. Panics if the index is
// not inverted.
func (desc *IndexDescriptor) InvertedColumnID() ColumnID {
	if desc.Type != IndexDescriptor_INVERTED {
		panic(errors.AssertionFailedf("index is not inverted"))
	}
	return desc.KeyColumnIDs[len(desc.KeyColumnIDs)-1]
}

// InvertedColumnName returns the name of the inverted column of the inverted
// index. This is always the last column in KeyColumnNames. Panics if the index is
// not inverted.
func (desc *IndexDescriptor) InvertedColumnName() string {
	if desc.Type != IndexDescriptor_INVERTED {
		panic(errors.AssertionFailedf("index is not inverted"))
	}
	return desc.KeyColumnNames[len(desc.KeyColumnNames)-1]
}
