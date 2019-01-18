// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlbase

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	expr tree.Expr,
	expectedType types.T,
	context string,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	allowImpure bool,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(evalCtx, expr) {
		return nil, pgerror.NewErrorf(pgerror.CodeSyntaxError,
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

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
//
// If the column type *may* be SERIAL (or SERIAL-like), it is the
// caller's responsibility to call sql.processSerialInColumnDef() and
// sql.doCreateSequence() before MakeColumnDefDescs() to remove the
// SERIAL type and replace it with a suitable integer type and default
// expression.
//
// semaCtx and evalCtx can be nil if no default expression is used for the
// column.
//
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeColumnDefDescs(
	d *tree.ColumnTableDef, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) (*ColumnDescriptor, *IndexDescriptor, tree.TypedExpr, error) {
	if _, ok := d.Type.(*coltypes.TSerial); ok {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, nil, nil, pgerror.NewError(pgerror.CodeFeatureNotSupportedError,
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

	// Set Type.SemanticType and Type.Locale.
	colDatumType := coltypes.CastTargetToDatumType(d.Type)
	colTyp, err := DatumTypeToColumnType(colDatumType)
	if err != nil {
		return nil, nil, nil, err
	}

	col.Type, err = PopulateTypeAttrs(colTyp, d.Type)
	if err != nil {
		return nil, nil, nil, err
	}

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		if typedExpr, err = SanitizeVarFreeExpr(
			d.DefaultExpr.Expr, colDatumType, "DEFAULT", semaCtx, evalCtx, true, /* allowImpure */
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
func GetColumnTypes(desc *TableDescriptor, columnIDs []ColumnID) ([]ColumnType, error) {
	types := make([]ColumnType, len(columnIDs))
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

	// Only populated for FK, PK, and Unique Constraints.
	Index *IndexDescriptor

	// Only populated for FK Constraints.
	FK              *ForeignKeyReference
	ReferencedTable *TableDescriptor
	ReferencedIndex *IndexDescriptor

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
	for i := range indexes {
		index := &indexes[i]
		if index.ID == desc.PrimaryIndex.ID {
			if _, ok := info[index.Name]; ok {
				return nil, errors.Errorf("duplicate constraint name: %q", index.Name)
			}
			colHiddenMap := make(map[ColumnID]bool, len(desc.Columns))
			for i, column := range desc.Columns {
				colHiddenMap[column.ID] = desc.Columns[i].Hidden
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
			if tableLookup != nil {
				detail.Columns = index.ColumnNames
				detail.Index = index
			}
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, errors.Errorf("duplicate constraint name: %q", index.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeUnique}
			if tableLookup != nil {
				detail.Columns = index.ColumnNames
				detail.Index = index
			}
			info[index.Name] = detail
		}

		if index.ForeignKey.IsSet() {
			if _, ok := info[index.ForeignKey.Name]; ok {
				return nil, errors.Errorf("duplicate constraint name: %q", index.ForeignKey.Name)
			}
			detail := ConstraintDetail{Kind: ConstraintTypeFK}
			detail.Unvalidated = index.ForeignKey.Validity == ConstraintValidity_Unvalidated
			numCols := len(index.ColumnIDs)
			if index.ForeignKey.SharedPrefixLen > 0 {
				numCols = int(index.ForeignKey.SharedPrefixLen)
			}
			detail.Columns = index.ColumnNames[:numCols]
			detail.Index = index

			if tableLookup != nil {
				other, err := tableLookup(index.ForeignKey.Table)
				if err != nil {
					return nil, errors.Wrapf(err, "error resolving table %d referenced in foreign key",
						index.ForeignKey.Table)
				}
				otherIdx, err := other.FindIndexByID(index.ForeignKey.Index)
				if err != nil {
					return nil, errors.Wrapf(err, "error resolving index %d in table %s referenced "+
						"in foreign key", index.ForeignKey.Index, other.Name)
				}
				detail.Details = fmt.Sprintf("%s.%v", other.Name, otherIdx.ColumnNames)
				detail.FK = &index.ForeignKey
				detail.ReferencedTable = other
				detail.ReferencedIndex = otherIdx
			}
			info[index.ForeignKey.Name] = detail
		}
	}

	for _, c := range desc.Checks {
		if _, ok := info[c.Name]; ok {
			return nil, errors.Errorf("duplicate constraint name: %q", c.Name)
		}
		detail := ConstraintDetail{Kind: ConstraintTypeCheck}
		detail.Unvalidated = c.Validity == ConstraintValidity_Unvalidated
		if tableLookup != nil {
			detail.Details = c.Expr
			detail.CheckConstraint = c

			colsUsed, err := c.ColumnsUsed(desc)
			if err != nil {
				return nil, errors.Wrapf(err, "error computing columns used in check constraint %q", c.Name)
			}
			for _, colID := range colsUsed {
				col, err := desc.FindColumnByID(colID)
				if err != nil {
					return nil, errors.Wrapf(err, "error finding column %d in table %s", colID, desc.Name)
				}
				detail.Columns = append(detail.Columns, col.Name)
			}
		}
		info[c.Name] = detail
	}
	return info, nil
}
