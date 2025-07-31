// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"bytes"
	"cmp"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CheckLogicalReplicationCompatibility verifies that the destination table
// descriptor is a valid target for logical replication and is equivalent to the
// source table.
func CheckLogicalReplicationCompatibility(
	src, dst *descpb.TableDescriptor, skipTableEquivalenceCheck bool,
) error {
	const cannotLDRMsg = "cannot create logical replication stream"
	if !skipTableEquivalenceCheck {
		if err := checkSrcDstColsMatch(src, dst); err != nil {
			return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
		}
	}
	if err := checkColumnFamilies(dst); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
	}

	if err := checkCompositeTypesInPrimaryKey(dst); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
	}

	if err := checkExpressionEvaluation(dst); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
	}
	if err := checkUniqueWithoutIndex(dst); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
	}
	if !skipTableEquivalenceCheck {
		if err := checkUniqueIndexesMatch(src, dst); err != nil {
			return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
		}

		if err := checkCheckConstraintsMatch(src, dst); err != nil {
			return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
		}
	}
	if err := checkOutboundReferences(dst); err != nil {
		return pgerror.Wrapf(err, pgcode.InvalidTableDefinition, cannotLDRMsg)
	}

	return nil
}

// checkOutboundReferences verifies that the table descriptor does not
// reference any user-defined functions, sequences, or triggers.
func checkOutboundReferences(dst *descpb.TableDescriptor) error {
	for _, col := range dst.Columns {
		if len(col.UsesSequenceIds) > 0 {
			return errors.Newf("table %s references sequences with IDs %v", dst.Name, col.UsesSequenceIds)
		}
		if len(col.UsesFunctionIds) > 0 {
			return errors.Newf("table %s references functions with IDs %v", dst.Name, col.UsesFunctionIds)
		}
	}
	if len(dst.Triggers) > 0 {
		triggerNames := make([]string, len(dst.Triggers))
		for i, trigger := range dst.Triggers {
			triggerNames[i] = trigger.Name
		}
		return errors.Newf("table %s references triggers [%s]", dst.Name, strings.Join(triggerNames, ", "))
	}
	return nil
}

// We disallow expression evaluation (e.g., virtual columns that appear in an
// index) because the LDR KV write path does not understand how to evaluate
// expressions. The writer expects to receive the full set of columns, even the
// computed ones, along with a list of columns that we've already determined
// should be updated.
func checkExpressionEvaluation(dst *descpb.TableDescriptor) error {
	// Disallow partial indexes.
	for _, idx := range dst.Indexes {
		if idx.IsPartial() {
			return errors.Newf("table %s has a partial index %s", dst.Name, idx.Name)
		}
	}

	// Disallow virtual columns if they are a key of an index.
	// NB: it is impossible for a virtual column to be stored in an index.
	columns := make([]catalog.Column, len(dst.Columns))
	for i, col := range dst.Columns {
		columns[i] = column{desc: &col, ordinal: i}
	}
	colOrd := catalog.ColumnIDToOrdinalMap(columns)
	for _, pkColID := range dst.PrimaryIndex.KeyColumnIDs {
		pkColOrd, ok := colOrd.Get(pkColID)
		if ok && columns[pkColOrd].IsComputed() && columns[pkColOrd].IsVirtual() {
			return errors.Newf(
				"table %s has a virtual computed column %s that appears in the primary key",
				dst.Name, columns[pkColOrd].GetName(),
			)
		}
	}
	for _, idx := range dst.Indexes {
		for _, keyColID := range idx.KeyColumnIDs {
			keyColOrd, ok := colOrd.Get(keyColID)
			if ok && columns[keyColOrd].IsComputed() && columns[keyColOrd].IsVirtual() {
				return errors.Newf(
					"table %s has a virtual computed column %s that is a key of index %s",
					dst.Name, columns[keyColOrd].GetName(), idx.Name,
				)
			}
		}
	}

	return nil
}

// Decoding a primary key with a composite type requires reading the current
// value. When the rangefeed sends over a delete, however, we do not see the
// current value. While we could rely on the prev value sent over the rangefeed,
// we currently have no way to handle phantom deletes (i.e. a delete on a key
// with no previous value)
func checkCompositeTypesInPrimaryKey(dst *descpb.TableDescriptor) error {
	for _, compositeColID := range dst.PrimaryIndex.CompositeColumnIDs {
		for _, keyColID := range dst.PrimaryIndex.KeyColumnIDs {
			if compositeColID == keyColID {
				colName := ""
				for _, col := range dst.Columns {
					if col.ID == keyColID {
						colName = col.Name
					}
				}
				return errors.Newf(
					"table %s has a primary key column (%s) with composite encoding",
					dst.Name, colName,
				)
			}
		}
	}
	return nil
}

// Logical replication does not run unique without index checks.
func checkUniqueWithoutIndex(dst *descpb.TableDescriptor) error {
	if len(dst.UniqueWithoutIndexConstraints) > 0 {
		names := make([]string, 0, len(dst.UniqueWithoutIndexConstraints))
		for _, c := range dst.UniqueWithoutIndexConstraints {
			names = append(names, c.Name)
		}
		return errors.Newf(
			"table %s has UNIQUE WITHOUT INDEX constraints: %s",
			dst.Name, strings.Join(names, ","),
		)
	}
	return nil
}

// Replication does not work if a column in a family has a not null
// constraint. Even if all columns are nullable, it is very hard to
// differentiate between a delete and an update which nils out values in a
// given column family.
func checkColumnFamilies(dst *descpb.TableDescriptor) error {
	if len(dst.Families) > 1 {
		return errors.Newf("table %s has more than one column family", dst.Name)
	}
	return nil
}

// All column names and types must match with the source table’s columns. The KV
// and SQL write path ingestion side logic assumes that src and dst columns
// match. If they don’t, the LDR job will DLQ these rows and move on.
func checkSrcDstColsMatch(src *descpb.TableDescriptor, dst *descpb.TableDescriptor) error {
	if len(src.Columns) != len(dst.Columns) {
		return errors.Newf(
			"destination table %s has %d columns, but the source table %s has %d columns",
			dst.Name, len(dst.Columns), src.Name, len(src.Columns),
		)
	}
	for i := range src.Columns {
		srcCol := src.Columns[i]
		dstCol := dst.Columns[i]

		if srcCol.Name != dstCol.Name {
			return errors.Newf(
				"destination table %s column %s at position %d does not match source table %s column %s",
				dst.Name, dstCol.Name, i, src.Name, srcCol.Name,
			)
		}

		if srcCol.Nullable != dstCol.Nullable {
			return errors.Newf(
				"destination table %s column %s has nullable=%t, but the source table %s has nullable=%t",
				dst.Name, dstCol.Name, dstCol.Nullable, src.Name, srcCol.Nullable,
			)
		}

		if err := checkTypesMatch(srcCol.Type, dstCol.Type); err != nil {
			return errors.Wrapf(err,
				"destination table %s column %s has type %s, but the source table %s has type %s",
				dst.Name, dstCol.Name, dstCol.Type.SQLStringForError(), src.Name, srcCol.Type.SQLStringForError(),
			)
		}
	}
	return nil
}

// checkTypesMatch checks that the source and destination types match. Enums
// need to be equal in both physical and logical representations.
func checkTypesMatch(srcTyp *types.T, dstTyp *types.T) error {
	switch {
	case dstTyp.TypeMeta.EnumData != nil:
		if srcTyp.TypeMeta.EnumData == nil {
			return errors.Newf(
				"destination type %s is an ENUM, but the source type %s is not",
				dstTyp.SQLStringForError(), srcTyp.SQLStringForError(),
			)
		}
		if !slices.Equal(srcTyp.TypeMeta.EnumData.LogicalRepresentations, dstTyp.TypeMeta.EnumData.LogicalRepresentations) {
			return errors.Newf(
				"destination type %s has logical representations %v, but the source type %s has %v",
				dstTyp.SQLStringForError(), dstTyp.TypeMeta.EnumData.LogicalRepresentations,
				srcTyp.SQLStringForError(), srcTyp.TypeMeta.EnumData.LogicalRepresentations,
			)
		}
		if !slices.EqualFunc(
			srcTyp.TypeMeta.EnumData.PhysicalRepresentations, dstTyp.TypeMeta.EnumData.PhysicalRepresentations,
			func(x, y []byte) bool { return bytes.Equal(x, y) },
		) {
			return errors.Newf(
				"destination type %s and source type %s have mismatched physical representations",
				dstTyp.SQLStringForError(), srcTyp.SQLStringForError(),
			)
		}

	case len(dstTyp.TupleContents()) > 0:
		if len(srcTyp.TupleContents()) == 0 {
			return errors.Newf(
				"destination type %s is a tuple, but the source type %s is not",
				dstTyp.SQLStringForError(), srcTyp.SQLStringForError(),
			)
		}
		if len(dstTyp.TupleContents()) != len(srcTyp.TupleContents()) {
			return errors.Newf(
				"destination type %s has %d tuple elements, but the source type %s has %d tuple elements",
				dstTyp.SQLStringForError(), len(dstTyp.TupleContents()),
				srcTyp.SQLStringForError(), len(srcTyp.TupleContents()),
			)
		}
		for i := range dstTyp.TupleContents() {
			if err := checkTypesMatch(srcTyp.TupleContents()[i], dstTyp.TupleContents()[i]); err != nil {
				return errors.Wrapf(err,
					"destination type %s tuple element %d does not match source type %s tuple element %d",
					dstTyp.SQLStringForError(), i, srcTyp.SQLStringForError(), i,
				)
			}
		}

	default:
		if !srcTyp.Identical(dstTyp) {
			return errors.Newf(
				"destination type %s does not match source type %s",
				dstTyp.SQLStringForError(), srcTyp.SQLStringForError(),
			)
		}
	}

	return nil
}

// The unique indexes on the source and destination tables must have the same
// key columns.
func checkUniqueIndexesMatch(src *descpb.TableDescriptor, dst *descpb.TableDescriptor) error {
	srcColumns := make([]catalog.Column, len(src.Columns))
	dstColumns := make([]catalog.Column, len(dst.Columns))
	for i, col := range src.Columns {
		srcColumns[i] = column{desc: &col, ordinal: i}
	}
	for i, col := range dst.Columns {
		dstColumns[i] = column{desc: &col, ordinal: i}
	}
	srcColOrd := catalog.ColumnIDToOrdinalMap(srcColumns)
	dstColOrd := catalog.ColumnIDToOrdinalMap(dstColumns)

	// We need to compare column names here, not the internal column IDs.
	// Internal IDs are allowed to differ between the source and destination, but
	// names are not.
	sortedSrcUniqueColumns := make([][]string, 0, len(src.Indexes))
	sortedDstUniqueColumns := make([][]string, 0, len(dst.Indexes))
	for _, idx := range src.Indexes {
		if idx.Unique {
			uniqueCols := make([]string, len(idx.KeyColumnIDs))
			for i, colID := range idx.KeyColumnIDs {
				colOrd, ok := srcColOrd.Get(colID)
				if !ok {
					return errors.Newf(
						"source table %s has a UNIQUE index %s with column %d that does not exist",
						src.Name, idx.Name, colID,
					)
				}
				uniqueCols[i] = srcColumns[colOrd].GetName()
			}
			sortedSrcUniqueColumns = append(sortedSrcUniqueColumns, uniqueCols)
		}
	}
	for _, idx := range dst.Indexes {
		if idx.Unique {
			uniqueCols := make([]string, len(idx.KeyColumnIDs))
			for i, colID := range idx.KeyColumnIDs {
				colOrd, ok := dstColOrd.Get(colID)
				if !ok {
					return errors.Newf(
						"destination table %s has a UNIQUE index %s with column %d that does not exist",
						dst.Name, idx.Name, colID,
					)
				}
				uniqueCols[i] = srcColumns[colOrd].GetName()
			}
			sortedDstUniqueColumns = append(sortedDstUniqueColumns, uniqueCols)
		}
	}
	slices.SortFunc(sortedSrcUniqueColumns, slices.Compare)
	slices.SortFunc(sortedDstUniqueColumns, slices.Compare)
	if len(sortedSrcUniqueColumns) != len(sortedDstUniqueColumns) {
		return errors.Newf(
			"destination table %s has %d UNIQUE indexes, but the source table %s has %d UNIQUE indexes",
			dst.Name, len(sortedDstUniqueColumns), src.Name, len(sortedSrcUniqueColumns),
		)
	}

	for i := range sortedDstUniqueColumns {
		srcUniqueCols := sortedSrcUniqueColumns[i]
		dstUniqueCols := sortedDstUniqueColumns[i]
		if !slices.Equal(srcUniqueCols, dstUniqueCols) {
			return errors.Newf(
				"destination table %s UNIQUE indexes do not match source table %s",
				dst.Name, src.Name,
			)
		}
	}
	return nil
}

// The CHECK constraints on the source and destination tables must match.
// Otherwise the LDR job might generate invalid rows.
func checkCheckConstraintsMatch(src *descpb.TableDescriptor, dst *descpb.TableDescriptor) error {
	if len(src.Checks) != len(dst.Checks) {
		return errors.Newf(
			"destination table %s has %d CHECK constraints, but the source table %s has %d CHECK constraints",
			dst.Name, len(dst.Checks), src.Name, len(src.Checks),
		)
	}

	sortedSrcChecks := make([]*descpb.TableDescriptor_CheckConstraint, len(src.Checks))
	sortedDstChecks := make([]*descpb.TableDescriptor_CheckConstraint, len(dst.Checks))
	copy(sortedSrcChecks, src.Checks)
	copy(sortedDstChecks, dst.Checks)
	slices.SortFunc(sortedSrcChecks, func(x, y *descpb.TableDescriptor_CheckConstraint) int {
		return cmp.Compare(x.Expr, y.Expr)
	})
	slices.SortFunc(sortedDstChecks, func(x, y *descpb.TableDescriptor_CheckConstraint) int {
		return cmp.Compare(x.Expr, y.Expr)
	})

	for i := range sortedDstChecks {
		srcCheck := sortedSrcChecks[i]
		dstCheck := sortedDstChecks[i]

		if srcCheck.Expr != dstCheck.Expr || srcCheck.IsNonNullConstraint != dstCheck.IsNonNullConstraint {
			return errors.Newf(
				"destination table %s CHECK constraints do not match source table %s",
				dst.Name, src.Name,
			)
		}
	}
	return nil
}
