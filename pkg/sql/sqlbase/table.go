// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/language"
)

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	expr tree.Expr,
	expectedType *types.T,
	context string,
	semaCtx *tree.SemaContext,
	allowImpure bool,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(expr) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"variable sub-expressions are not allowed in %s", context)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called from another context
	// which uses the properties field.
	defer semaCtx.Properties.Restore(semaCtx.Properties)

	// Ensure that the expression doesn't contain special functions.
	flags := tree.RejectSpecial
	if !allowImpure {
		flags |= tree.RejectImpureFunctions
	}
	semaCtx.Properties.Require(context, flags)

	typedExpr, err := tree.TypeCheck(expr, semaCtx, expectedType)
	if err != nil {
		return nil, err
	}

	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value.
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

// ValidateColumnDefType returns an error if the type of a column definition is
// not valid. It is checked when a column is created or altered.
func ValidateColumnDefType(t *types.T) error {
	switch t.Family() {
	case types.StringFamily, types.CollatedStringFamily:
		if t.Family() == types.CollatedStringFamily {
			if _, err := language.Parse(t.Locale()); err != nil {
				return pgerror.Newf(pgcode.Syntax, `invalid locale %s`, t.Locale())
			}
		}

	case types.DecimalFamily:
		switch {
		case t.Precision() == 0 && t.Scale() > 0:
			// TODO (seif): Find right range for error message.
			return errors.New("invalid NUMERIC precision 0")
		case t.Precision() < t.Scale():
			return fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				t.Scale(), t.Precision())
		}

	case types.ArrayFamily:
		if t.ArrayContents().Family() == types.ArrayFamily {
			// Nested arrays are not supported as a column type.
			return errors.Errorf("nested array unsupported as column type: %s", t.String())
		}
		if err := types.CheckArrayElementType(t.ArrayContents()); err != nil {
			return err
		}
		return ValidateColumnDefType(t.ArrayContents())

	case types.BitFamily, types.IntFamily, types.FloatFamily, types.BoolFamily, types.BytesFamily, types.DateFamily,
		types.INetFamily, types.IntervalFamily, types.JsonFamily, types.OidFamily, types.TimeFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.UuidFamily:
		// These types are OK.

	default:
		return pgerror.Newf(pgcode.InvalidTableDefinition,
			"value type %s cannot be used for table columns", t.String())
	}

	return nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
//
// If the column type *may* be SERIAL (or SERIAL-like), it is the
// caller's responsibility to call sql.processSerialInColumnDef() and
// sql.doCreateSequence() before MakeColumnDefDescs() to remove the
// SERIAL type and replace it with a suitable integer type and default
// expression.
//
// semaCtx can be nil if no default expression is used for the
// column.
//
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeColumnDefDescs(
	d *tree.ColumnTableDef, semaCtx *tree.SemaContext,
) (*ColumnDescriptor, *IndexDescriptor, tree.TypedExpr, error) {
	if d.IsSerial {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, nil, nil, pgerror.New(pgcode.FeatureNotSupported,
			"SERIAL cannot be used in this context")
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	col := &ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey,
	}

	// Validate and assign column type.
	err := ValidateColumnDefType(d.Type)
	if err != nil {
		return nil, nil, nil, err
	}
	col.Type = *d.Type

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		var err error
		if typedExpr, err = SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, d.Type, "DEFAULT", semaCtx, true, /* allowImpure */
		); err != nil {
			return nil, nil, nil, err
		}
		// We keep the type checked expression so that the type annotation
		// gets properly stored.
		d.DefaultExpr.Expr = typedExpr

		s := tree.Serialize(d.DefaultExpr.Expr)
		col.DefaultExpr = &s
	}

	if d.IsComputed() {
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
	}

	var idx *IndexDescriptor
	if d.PrimaryKey || d.Unique {
		idx = &IndexDescriptor{
			Unique:           true,
			ColumnNames:      []string{string(d.Name)},
			ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
		}
	}

	return col, idx, typedExpr, nil
}

// EncodeColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []ColumnID,
	directions directions,
	colMap map[ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findColumnValue(id, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, containsNull, err
		}

		if key, err = EncodeTableKey(key, val, dir); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

// GetColumnTypes returns the types of the columns with the given IDs.
func GetColumnTypes(desc *TableDescriptor, columnIDs []ColumnID) ([]types.T, error) {
	types := make([]types.T, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		types[i] = col.Type
	}
	return types, nil
}

// ConstraintType is used to identify the type of a constraint.
type ConstraintType string

const (
	// ConstraintTypePK identifies a PRIMARY KEY constraint.
	ConstraintTypePK ConstraintType = "PRIMARY KEY"
	// ConstraintTypeFK identifies a FOREIGN KEY constraint.
	ConstraintTypeFK ConstraintType = "FOREIGN KEY"
	// ConstraintTypeUnique identifies a FOREIGN constraint.
	ConstraintTypeUnique ConstraintType = "UNIQUE"
	// ConstraintTypeCheck identifies a CHECK constraint.
	ConstraintTypeCheck ConstraintType = "CHECK"
)

// ConstraintDetail describes a constraint.
type ConstraintDetail struct {
	Kind        ConstraintType
	Columns     []string
	Details     string
	Unvalidated bool

	// Only populated for PK and Unique Constraints.
	Index *IndexDescriptor

	// Only populated for FK Constraints.
	FK              *ForeignKeyConstraint
	ReferencedTable *TableDescriptor

	// Only populated for Check Constraints.
	CheckConstraint *TableDescriptor_CheckConstraint
}

type tableLookupFn func(ID) (*TableDescriptor, error)

// GetConstraintInfo returns a summary of all constraints on the table.
func (desc *TableDescriptor) GetConstraintInfo(
	ctx context.Context, txn *client.Txn,
) (map[string]ConstraintDetail, error) {
	var tableLookup tableLookupFn
	if txn != nil {
		tableLookup = func(id ID) (*TableDescriptor, error) {
			return GetTableDescFromID(ctx, txn, id)
		}
	}
	return desc.collectConstraintInfo(tableLookup)
}

// GetConstraintInfoWithLookup returns a summary of all constraints on the
// table using the provided function to fetch a TableDescriptor from an ID.
func (desc *TableDescriptor) GetConstraintInfoWithLookup(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc *TableDescriptor) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc *TableDescriptor) collectConstraintInfo(
	tableLookup tableLookupFn,
) (map[string]ConstraintDetail, error) {
	info := make(map[string]ConstraintDetail)

	// Indexes provide PK, Unique and FK constraints.
	indexes := desc.AllNonDropIndexes()
	for _, index := range indexes {
		if index.ID == desc.PrimaryIndex.ID {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			colHiddenMap := make(map[ColumnID]bool, len(desc.Columns))
			for i := range desc.Columns {
				col := &desc.Columns[i]
				colHiddenMap[col.ID] = col.Hidden
			}
			// Don't include constraints against only hidden columns.
			// This prevents the auto-created rowid primary key index from showing up
			// in show constraints.
			hidden := true
			for _, id := range index.ColumnIDs {
				if !colHiddenMap[id] {
					hidden = false
					break
				}
			}
			if hidden {
				continue
			}
			detail := ConstraintDetail{Kind: ConstraintTypePK}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeUnique}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		}
	}

	fks := desc.AllActiveAndInactiveForeignKeys()
	for _, fk := range fks {
		if _, ok := info[fk.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", fk.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeFK}
		// Constraints in the Validating state are considered Unvalidated for this purpose
		detail.Unvalidated = fk.Validity != ConstraintValidity_Validated
		var err error
		detail.Columns, err = desc.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return nil, err
		}
		detail.FK = fk

		if tableLookup != nil {
			other, err := tableLookup(fk.ReferencedTableID)
			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"error resolving table %d referenced in foreign key",
					log.Safe(fk.ReferencedTableID))
			}
			referencedColumnNames, err := other.NamesForColumnIDs(fk.ReferencedColumnIDs)
			if err != nil {
				return nil, err
			}
			detail.Details = fmt.Sprintf("%s.%v", other.Name, referencedColumnNames)
			detail.ReferencedTable = other
		}
		info[fk.Name] = detail
	}

	for _, c := range desc.AllActiveAndInactiveChecks() {
		if _, ok := info[c.Name]; ok {
			return nil, errors.Errorf("duplicate constraint name: %q", c.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeCheck}
		// Constraints in the Validating state are considered Unvalidated for this purpose
		detail.Unvalidated = c.Validity != ConstraintValidity_Validated
		detail.CheckConstraint = c
		detail.Details = c.Expr
		if tableLookup != nil {
			colsUsed, err := c.ColumnsUsed(desc)
			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"error computing columns used in check constraint %q", c.Name)
			}
			for _, colID := range colsUsed {
				col, err := desc.FindColumnByID(colID)
				if err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err,
						"error finding column %d in table %s", log.Safe(colID), desc.Name)
				}
				detail.Columns = append(detail.Columns, col.Name)
			}
		}
		info[c.Name] = detail
	}
	return info, nil
}

// FindFKReferencedIndex finds the first index in the supplied referencedTable
// that can satisfy a foreign key of the supplied column ids.
func FindFKReferencedIndex(
	referencedTable *TableDescriptor, referencedColIDs ColumnIDs,
) (*IndexDescriptor, error) {
	// Search for a unique index on the referenced table that matches our foreign
	// key columns.
	if referencedColIDs.EqualSets(referencedTable.PrimaryIndex.ColumnIDs) {
		return &referencedTable.PrimaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the referenced column.
	for _, idx := range referencedTable.Indexes {
		if idx.Unique && referencedColIDs.EqualSets(idx.ColumnIDs) {
			return &idx, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no unique constraint matching given keys for referenced table %s",
		referencedTable.Name,
	)
}

// FindFKOriginIndex finds the first index in the supplied originTable
// that can satisfy an outgoing foreign key of the supplied column ids.
// If there's no found index, nil is returned.
func FindFKOriginIndex(
	originTable *TableDescriptor, originColIDs ColumnIDs,
) (*IndexDescriptor, error) {
	// Search for an index on the origin table that matches our foreign
	// key columns.
	if ColumnIDs(originTable.PrimaryIndex.ColumnIDs).HasPrefix(originColIDs) || originColIDs.EqualSets(originTable.PrimaryIndex.ColumnIDs) {
		return &originTable.PrimaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the origin column.
	for _, idx := range originTable.Indexes {
		if ColumnIDs(idx.ColumnIDs).HasPrefix(originColIDs) || originColIDs.EqualSets(idx.ColumnIDs) {
			return &idx, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no index matching given keys for referenced table %s",
		originTable.Name,
	)
}
