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
)

// RunOverAllColumns applies its argument fn to each of the column IDs in desc.
// If there is an error, that error is returned immediately.
func (desc *IndexDescriptor) RunOverAllColumns(fn func(id ColumnID) error) error {
	for _, colID := range desc.ColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.ExtraColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	for _, colID := range desc.StoreColumnIDs {
		if err := fn(colID); err != nil {
			return err
		}
	}
	return nil
}

// GetEncodingType returns the encoding type of this index. For backward
// compatibility reasons, this might not match what is stored in
// desc.EncodingType. The primary index's ID must be passed so we can check if
// this index is primary or secondary.
func (desc *IndexDescriptor) GetEncodingType(primaryIndexID IndexID) IndexDescriptorEncodingType {
	if desc.ID == primaryIndexID {
		// Primary indexes always use the PrimaryIndexEncoding, regardless of what
		// desc.EncodingType indicates.
		return PrimaryIndexEncoding
	}
	return desc.EncodingType
}

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

// ColNamesFormat writes a string describing the column names and directions
// in this index to the given buffer.
func (desc *IndexDescriptor) ColNamesFormat(ctx *tree.FmtCtx) {
	start := 0
	if desc.IsSharded() {
		start = 1
	}
	for i := start; i < len(desc.ColumnNames); i++ {
		if i > start {
			ctx.WriteString(", ")
		}
		ctx.FormatNameP(&desc.ColumnNames[i])
		if desc.Type != IndexDescriptor_INVERTED {
			ctx.WriteByte(' ')
			ctx.WriteString(desc.ColumnDirections[i].String())
		}
	}
}

// FillColumns sets the column names and directions in desc.
func (desc *IndexDescriptor) FillColumns(elems tree.IndexElemList) error {
	desc.ColumnNames = make([]string, 0, len(elems))
	desc.ColumnDirections = make([]IndexDescriptor_Direction, 0, len(elems))
	for _, c := range elems {
		desc.ColumnNames = append(desc.ColumnNames, string(c.Column))
		switch c.Direction {
		case tree.Ascending, tree.DefaultDirection:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_ASC)
		case tree.Descending:
			desc.ColumnDirections = append(desc.ColumnDirections, IndexDescriptor_DESC)
		default:
			return fmt.Errorf("invalid direction %s for column %s", c.Direction, c.Column)
		}
	}
	return nil
}

type returnTrue struct{}

func (returnTrue) Error() string { panic("unimplemented") }

var returnTruePseudoError error = returnTrue{}

// ContainsColumnID returns true if the index descriptor contains the specified
// column ID either in its explicit column IDs, the extra column IDs, or the
// stored column IDs.
func (desc *IndexDescriptor) ContainsColumnID(colID ColumnID) bool {
	return desc.RunOverAllColumns(func(id ColumnID) error {
		if id == colID {
			return returnTruePseudoError
		}
		return nil
	}) != nil
}

// FullColumnIDs returns the index column IDs including any extra (implicit or
// stored (old STORING encoding)) column IDs for non-unique indexes. It also
// returns the direction with which each column was encoded.
func (desc *IndexDescriptor) FullColumnIDs() ([]ColumnID, []IndexDescriptor_Direction) {
	if desc.Unique {
		return desc.ColumnIDs, desc.ColumnDirections
	}
	// Non-unique indexes have some of the primary-key columns appended to
	// their key.
	columnIDs := append([]ColumnID(nil), desc.ColumnIDs...)
	columnIDs = append(columnIDs, desc.ExtraColumnIDs...)
	dirs := append([]IndexDescriptor_Direction(nil), desc.ColumnDirections...)
	for range desc.ExtraColumnIDs {
		// Extra columns are encoded in ascending order.
		dirs = append(dirs, IndexDescriptor_ASC)
	}
	return columnIDs, dirs
}

// TODO (tyler): Issue #39771 This method needs more thorough testing, probably
// in structured_test.go. Or possibly replace it with a format method taking
// a format context as argument.

// ColNamesString returns a string describing the column names and directions
// in this index.
func (desc *IndexDescriptor) ColNamesString() string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	desc.ColNamesFormat(f)
	return f.CloseAndGetString()
}

// TODO (tyler): Issue #39771 Same comment as ColNamesString above.

// SQLString returns the SQL string describing this index. If non-empty,
// "ON tableName" is included in the output in the correct place.
func (desc *IndexDescriptor) SQLString(tableName *tree.TableName) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	if desc.Unique {
		f.WriteString("UNIQUE ")
	}
	if desc.Type == IndexDescriptor_INVERTED {
		f.WriteString("INVERTED ")
	}
	f.WriteString("INDEX ")
	f.FormatNameP(&desc.Name)
	if *tableName != AnonymousTable {
		f.WriteString(" ON ")
		f.FormatNode(tableName)
	}
	f.WriteString(" (")
	desc.ColNamesFormat(f)
	f.WriteByte(')')

	if desc.IsSharded() {
		fmt.Fprintf(f, " USING HASH WITH BUCKET_COUNT = %v",
			desc.Sharded.ShardBuckets)
	}

	if len(desc.StoreColumnNames) > 0 {
		f.WriteString(" STORING (")
		for i := range desc.StoreColumnNames {
			if i > 0 {
				f.WriteString(", ")
			}
			f.FormatNameP(&desc.StoreColumnNames[i])
		}
		f.WriteByte(')')
	}

	if desc.IsPartial() {
		f.WriteString(" WHERE ")
		f.WriteString(desc.Predicate)
	}

	return f.CloseAndGetString()
}

// IsValidOriginIndex returns whether the index can serve as an origin index for a foreign
// key constraint with the provided set of originColIDs.
func (desc *IndexDescriptor) IsValidOriginIndex(originColIDs ColumnIDs) bool {
	return ColumnIDs(desc.ColumnIDs).HasPrefix(originColIDs)
}

// IsValidReferencedIndex returns whether the index can serve as a referenced index for a foreign
// key constraint with the provided set of referencedColumnIDs.
func (desc *IndexDescriptor) IsValidReferencedIndex(referencedColIDs ColumnIDs) bool {
	return desc.Unique && ColumnIDs(desc.ColumnIDs).Equals(referencedColIDs)
}

// HasOldStoredColumns returns whether the index has stored columns in the old
// format (data encoded the same way as if they were in an implicit column).
func (desc *IndexDescriptor) HasOldStoredColumns() bool {
	return len(desc.ExtraColumnIDs) > 0 && len(desc.StoreColumnIDs) < len(desc.StoreColumnNames)
}
