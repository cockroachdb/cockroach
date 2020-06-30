// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ComputedColumnValidator validates that an expression is a valid computed
// column. See Validate for more details.
type ComputedColumnValidator struct {
	ctx       context.Context
	desc      *sqlbase.MutableTableDescriptor
	semaCtx   *tree.SemaContext
	tableName *tree.TableName
}

// NewComputedColumnValidator returns an ComputedColumnValidator struct that can
// be used to validate computed columns. See Validate for more details.
func NewComputedColumnValidator(
	ctx context.Context,
	desc *sqlbase.MutableTableDescriptor,
	semaCtx *tree.SemaContext,
	tn *tree.TableName,
) ComputedColumnValidator {
	return ComputedColumnValidator{
		ctx:       ctx,
		desc:      desc,
		semaCtx:   semaCtx,
		tableName: tn,
	}
}

// Validate verifies that an expression is a valid computed column expression.
// If it is not valid, an error is returned.
//
// A computed column expression is valid if all of the following are true:
//
//   - It does not have a default value.
//   - It does not reference other computed columns.
//
// It additionally updates the target computed column with the serialized
// typed expression.
// TODO(mgartner): Add unit tests for Validate.
func (v *ComputedColumnValidator) Validate(d *tree.ColumnTableDef) error {
	if d.HasDefaultExpr() {
		return pgerror.New(
			pgcode.InvalidTableDefinition,
			"computed columns cannot have default values",
		)
	}

	var depColIDs sqlbase.TableColSet
	// First, check that no column in the expression is a computed column.
	err := iterColDescriptors(v.desc, d.Computed.Expr, func(c *sqlbase.ColumnDescriptor) error {
		if c.IsComputed() {
			return pgerror.New(pgcode.InvalidTableDefinition,
				"computed columns cannot reference other computed columns")
		}
		depColIDs.Add(c.ID)

		return nil
	})
	if err != nil {
		return err
	}

	// TODO(justin,bram): allow depending on columns like this. We disallow it
	// for now because cascading changes must hook into the computed column
	// update path.
	for i := range v.desc.OutboundFKs {
		fk := &v.desc.OutboundFKs[i]
		for _, id := range fk.OriginColumnIDs {
			if !depColIDs.Contains(id) {
				// We don't depend on this column.
				continue
			}
			for _, action := range []sqlbase.ForeignKeyReference_Action{
				fk.OnDelete,
				fk.OnUpdate,
			} {
				switch action {
				case sqlbase.ForeignKeyReference_CASCADE,
					sqlbase.ForeignKeyReference_SET_NULL,
					sqlbase.ForeignKeyReference_SET_DEFAULT:
					return pgerror.New(pgcode.InvalidTableDefinition,
						"computed columns cannot reference non-restricted FK columns")
				}
			}
		}
	}

	// Resolve the type of the computed column expression.
	defType, err := tree.ResolveType(v.ctx, d.Type, v.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}

	// Check that the type of the expression is of type defType and that there
	// are no variable expressions (besides dummyColumnItems) and no impure
	// functions.
	typedExpr, _, err := DequalifyAndValidateExpr(
		v.ctx,
		v.desc,
		d.Computed.Expr,
		defType,
		"computed column",
		v.semaCtx,
		tree.VolatilityImmutable,
		v.tableName,
	)
	if err != nil {
		return err
	}

	// Get the column that this definition points to.
	targetCol, _, err := v.desc.FindColumnByName(d.Name)
	if err != nil {
		return err
	}
	// In order to safely serialize user defined types and their members, we
	// need to serialize the typed expression here.
	s := tree.Serialize(typedExpr)
	targetCol.ComputeExpr = &s

	return nil
}

// ValidateNoDependents verifies that the input column is not dependent on a
// computed column. The function errs if any existing computed columns or
// computed columns being added reference the given column.
// TODO(mgartner): Add unit tests for ValidateNoDependents.
func (v *ComputedColumnValidator) ValidateNoDependents(col *sqlbase.ColumnDescriptor) error {
	checkComputed := func(c *sqlbase.ColumnDescriptor) error {
		if !c.IsComputed() {
			return nil
		}

		expr, err := parser.ParseExpr(*c.ComputeExpr)
		if err != nil {
			// At this point, we should be able to parse the computed expression.
			return errors.WithAssertionFailure(err)
		}

		return iterColDescriptors(v.desc, expr, func(colVar *sqlbase.ColumnDescriptor) error {
			if colVar.ID == col.ID {
				return pgerror.Newf(
					pgcode.InvalidColumnReference,
					"column %q is referenced by computed column %q",
					col.Name,
					c.Name,
				)
			}
			return nil
		})
	}

	for i := range v.desc.Columns {
		if err := checkComputed(&v.desc.Columns[i]); err != nil {
			return err
		}
	}

	for i := range v.desc.Mutations {
		mut := &v.desc.Mutations[i]
		mutCol := mut.GetColumn()
		if mut.Direction == sqlbase.DescriptorMutation_ADD && mutCol != nil {
			if err := checkComputed(mutCol); err != nil {
				return err
			}
		}
	}

	return nil
}

// descContainer is a helper type that implements tree.IndexedVarContainer; it
// is used to type check computed columns and does not support evaluation.
type descContainer struct {
	cols []sqlbase.ColumnDescriptor
}

func (j *descContainer) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic("unsupported")
}

func (j *descContainer) IndexedVarResolvedType(idx int) *types.T {
	return j.cols[idx].Type
}

func (*descContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// MakeComputedExprs returns a slice of the computed expressions for the
// slice of input column descriptors, or nil if none of the input column
// descriptors have computed expressions.
// The length of the result slice matches the length of the input column
// descriptors. For every column that has no computed expression, a NULL
// expression is reported.
// addingCols indicates if the input column descriptors are being added
// and allows type checking of the compute expressions to reference
// input columns earlier in the slice.
func MakeComputedExprs(
	ctx context.Context,
	cols []sqlbase.ColumnDescriptor,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	tn *tree.TableName,
	txCtx *transform.ExprTransformContext,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	addingCols bool,
) ([]tree.TypedExpr, error) {
	// Check to see if any of the columns have computed expressions. If there
	// are none, we don't bother with constructing the map as the expressions
	// are all NULL.
	haveComputed := false
	for i := range cols {
		if cols[i].IsComputed() {
			haveComputed = true
			break
		}
	}
	if !haveComputed {
		return nil, nil
	}

	// Build the computed expressions map from the parsed statement.
	computedExprs := make([]tree.TypedExpr, 0, len(cols))
	exprStrings := make([]string, 0, len(cols))
	for i := range cols {
		col := &cols[i]
		if col.IsComputed() {
			exprStrings = append(exprStrings, *col.ComputeExpr)
		}
	}
	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, err
	}

	// We need an ivarHelper and sourceInfo, unlike DEFAULT, since computed
	// columns can reference other columns and thus need to be able to resolve
	// column names (at this stage they only need to resolve the types so that
	// the expressions can be typechecked - we have no need to evaluate them).
	iv := &descContainer{tableDesc.Columns}
	ivarHelper := tree.MakeIndexedVarHelper(iv, len(tableDesc.Columns))

	source := sqlbase.NewSourceInfoForSingleTable(*tn, sqlbase.ResultColumnsFromColDescs(tableDesc.GetID(), tableDesc.Columns))
	semaCtx.IVarContainer = iv

	addColumnInfo := func(col *sqlbase.ColumnDescriptor) {
		ivarHelper.AppendSlot()
		iv.cols = append(iv.cols, *col)
		newCols := sqlbase.ResultColumnsFromColDescs(tableDesc.GetID(), []sqlbase.ColumnDescriptor{*col})
		source.SourceColumns = append(source.SourceColumns, newCols...)
	}

	compExprIdx := 0
	for i := range cols {
		col := &cols[i]
		if !col.IsComputed() {
			computedExprs = append(computedExprs, tree.DNull)
			if addingCols {
				addColumnInfo(col)
			}
			continue
		}
		expr, err := sqlbase.ResolveNames(
			exprs[compExprIdx], source, ivarHelper, evalCtx.SessionData.SearchPath)
		if err != nil {
			return nil, err
		}

		typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, col.Type)
		if err != nil {
			return nil, err
		}
		if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, err
		}
		computedExprs = append(computedExprs, typedExpr)
		compExprIdx++
		if addingCols {
			addColumnInfo(col)
		}
	}
	return computedExprs, nil
}
