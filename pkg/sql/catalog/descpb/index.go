// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descpb

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

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
	// Currently, we only allow implicit partitioning on hash sharded index. When
	// that happens, the shard column always comes after implicit partition
	// columns.
	if desc.IsSharded() {
		start++
	}
	return start
}

// FillColumns sets the column names and directions in desc. Note that it does
// no validation with regards to the existence of the listed columns. It also
// delegates filling in any IDs until later.
func (desc *IndexDescriptor) FillColumns(elems tree.IndexElemList) error {
	desc.KeyColumnNames = make([]string, 0, len(elems))
	desc.KeyColumnDirections = make([]catenumpb.IndexColumn_Direction, 0, len(elems))
	for _, c := range elems {
		if c.Expr != nil {
			return errors.AssertionFailedf("index elem expression should have been replaced with a column")
		}
		desc.KeyColumnNames = append(desc.KeyColumnNames, string(c.Column))
		switch c.Direction {
		case tree.Ascending, tree.DefaultDirection:
			desc.KeyColumnDirections = append(desc.KeyColumnDirections, catenumpb.IndexColumn_ASC)
		case tree.Descending:
			desc.KeyColumnDirections = append(desc.KeyColumnDirections, catenumpb.IndexColumn_DESC)
		default:
			return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
		}
	}
	return nil
}

// explicitColumnIDsWithoutShardColumn returns explicit column ids of the index
// excluding the shard column.
func (desc *IndexDescriptor) explicitColumnIDsWithoutShardColumn() ColumnIDs {
	explicitColIDs := desc.KeyColumnIDs[desc.ExplicitColumnStartIdx():]
	explicitColNames := desc.KeyColumnNames[desc.ExplicitColumnStartIdx():]
	colIDs := make(ColumnIDs, 0, len(explicitColIDs))
	for i := range explicitColNames {
		if !desc.IsSharded() || explicitColNames[i] != desc.Sharded.Name {
			colIDs = append(colIDs, explicitColIDs[i])
		}
	}
	return colIDs
}

// implicitColumnIDs returns the implicit column ids of the index.
func (desc *IndexDescriptor) implicitColumnIDs() ColumnIDs {
	return desc.KeyColumnIDs[:desc.Partitioning.NumImplicitColumns]
}

// IsValidReferencedUniqueConstraint returns whether the index can serve
// as a referenced index for a foreign key constraint with the provided set
// of referencedColumnIDs.
func (desc *IndexDescriptor) IsValidReferencedUniqueConstraint(referencedColIDs ColumnIDs) bool {
	explicitColumnIDs := desc.explicitColumnIDsWithoutShardColumn()
	allColumnIDs := append(explicitColumnIDs, desc.implicitColumnIDs()...)
	return desc.Unique &&
		!desc.IsPartial() &&
		(explicitColumnIDs.PermutationOf(referencedColIDs) ||
			allColumnIDs.PermutationOf(referencedColIDs))
}

// GetName is part of the UniqueConstraint interface.
func (desc *IndexDescriptor) GetName() string {
	return desc.Name
}

// InvertedColumnID returns the ColumnID of the inverted column of the inverted
// index. This is always the last column in KeyColumnIDs. Panics if the index is
// not inverted.
func (desc *IndexDescriptor) InvertedColumnID() ColumnID {
	if desc.Type != idxtype.INVERTED {
		panic(errors.AssertionFailedf("index is not inverted"))
	}
	return desc.KeyColumnIDs[len(desc.KeyColumnIDs)-1]
}

// InvertedColumnName returns the name of the inverted column of the inverted
// index. This is always the last column in KeyColumnNames. Panics if the index is
// not inverted.
func (desc *IndexDescriptor) InvertedColumnName() string {
	if desc.Type != idxtype.INVERTED {
		panic(errors.AssertionFailedf("index is not inverted"))
	}
	return desc.KeyColumnNames[len(desc.KeyColumnNames)-1]
}

// InvertedColumnKeyType returns the type of the data element that is encoded
// as the inverted index key. This is currently always EncodedKey.
//
// Panics if the index is not inverted.
func (desc *IndexDescriptor) InvertedColumnKeyType() *types.T {
	if desc.Type != idxtype.INVERTED {
		panic(errors.AssertionFailedf("index is not inverted"))
	}
	return types.EncodedKey
}

// VectorColumnID returns the ColumnID of the vector column of the vector index.
// This is always the last column in KeyColumnIDs. Panics if the index is not a
// vector index.
func (desc *IndexDescriptor) VectorColumnID() ColumnID {
	if desc.Type != idxtype.VECTOR {
		panic(errors.AssertionFailedf("index is not a vector index"))
	}
	return desc.KeyColumnIDs[len(desc.KeyColumnIDs)-1]
}

// VectorColumnName returns the name of the vector column of the vector index.
// This is always the last column in KeyColumnNames. Panics if the index is
// not a vector index.
func (desc *IndexDescriptor) VectorColumnName() string {
	if desc.Type != idxtype.VECTOR {
		panic(errors.AssertionFailedf("index is not a vector index"))
	}
	return desc.KeyColumnNames[len(desc.KeyColumnNames)-1]
}
