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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// ValidateComputedColumnExpression verifies that an expression is a valid
// computed column expression. It returns the serialized expression and its type
// if valid, and an error otherwise. The returned type is only useful if d has
// type Any which indicates the expression's type is unknown and does not have
// to match a specific type.
//
// A computed column expression is valid if all of the following are true:
//
//   - It does not have a default value.
//   - It does not reference other computed columns.
//
// TODO(mgartner): Add unit tests for Validate.
func ValidateComputedColumnExpression(
	ctx context.Context,
	desc catalog.TableDescriptor,
	d *tree.ColumnTableDef,
	tn *tree.TableName,
	context string,
	semaCtx *tree.SemaContext,
) (serializedExpr string, _ *types.T, _ error) {
	if d.HasDefaultExpr() {
		return "", nil, pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"%s cannot have default values",
			context,
		)
	}

	var depColIDs catalog.TableColSet
	// First, check that no column in the expression is an inaccessible or
	// computed column.
	err := iterColDescriptors(desc, d.Computed.Expr, func(c catalog.Column) error {
		if c.IsInaccessible() {
			return pgerror.Newf(
				pgcode.UndefinedColumn,
				"column %q is inaccessible and cannot be referenced in a computed column expression",
				c.GetName(),
			)
		}
		if c.IsComputed() {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"%s expression cannot reference computed columns",
				context,
			)
		}
		depColIDs.Add(c.GetID())

		return nil
	})
	if err != nil {
		return "", nil, err
	}

	// Resolve the type of the computed column expression.
	defType, err := tree.ResolveType(ctx, d.Type, semaCtx.GetTypeResolver())
	if err != nil {
		return "", nil, err
	}

	// Check that the type of the expression is of type defType and that there
	// are no variable expressions (besides dummyColumnItems) and no impure
	// functions. In order to safely serialize user defined types and their
	// members, we need to serialize the typed expression here.
	expr, typ, _, err := DequalifyAndValidateExpr(
		ctx,
		desc,
		d.Computed.Expr,
		defType,
		context,
		semaCtx,
		tree.VolatilityImmutable,
		tn,
	)
	if err != nil {
		return "", nil, err
	}

	// Virtual computed columns must not refer to mutation columns because it
	// would not be safe in the case that the mutation column was being backfilled
	// and the virtual computed column value needed to be computed for the purpose
	// of writing to a secondary index.
	if d.IsVirtual() {
		var mutationColumnNames []string
		var err error
		depColIDs.ForEach(func(colID descpb.ColumnID) {
			if err != nil {
				return
			}
			var col catalog.Column
			if col, err = desc.FindColumnWithID(colID); err != nil {
				err = errors.WithAssertionFailure(err)
				return
			}
			if !col.Public() {
				mutationColumnNames = append(mutationColumnNames,
					strconv.Quote(col.GetName()))
			}
		})
		if err != nil {
			return "", nil, err
		}
		if len(mutationColumnNames) > 0 {
			return "", nil, unimplemented.Newf(
				"virtual computed columns referencing mutation columns",
				"virtual computed column %q referencing columns (%s) added in the "+
					"current transaction", d.Name, strings.Join(mutationColumnNames, ", "))
		}
	}
	return expr, typ, nil
}

// ValidateColumnHasNoDependents verifies that the input column has no dependent
// computed columns. It returns an error if any existing or ADD mutation
// computed columns reference the given column.
// TODO(mgartner): Add unit tests.
func ValidateColumnHasNoDependents(desc catalog.TableDescriptor, col catalog.Column) error {
	for _, c := range desc.NonDropColumns() {
		if !c.IsComputed() {
			continue
		}

		expr, err := parser.ParseExpr(c.GetComputeExpr())
		if err != nil {
			// At this point, we should be able to parse the computed expression.
			return errors.WithAssertionFailure(err)
		}

		err = iterColDescriptors(desc, expr, func(colVar catalog.Column) error {
			if colVar.GetID() == col.GetID() {
				return pgerror.Newf(
					pgcode.InvalidColumnReference,
					"column %q is referenced by computed column %q",
					col.GetName(),
					c.GetName(),
				)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// MakeComputedExprs returns a slice of the computed expressions for the
// slice of input column descriptors, or nil if none of the input column
// descriptors have computed expressions. The caller provides the set of
// sourceColumns to which the expr may refer.
//
// The length of the result slice matches the length of the input column
// descriptors. For every column that has no computed expression, a NULL
// expression is reported.
//
// Note that the order of input is critical. Expressions cannot reference
// columns that come after them in input.
func MakeComputedExprs(
	ctx context.Context,
	input, sourceColumns []catalog.Column,
	tableDesc catalog.TableDescriptor,
	tn *tree.TableName,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
) (_ []tree.TypedExpr, refColIDs catalog.TableColSet, _ error) {
	// Check to see if any of the columns have computed expressions. If there
	// are none, we don't bother with constructing the map as the expressions
	// are all NULL.
	haveComputed := false
	for i := range input {
		if input[i].IsComputed() {
			haveComputed = true
			break
		}
	}
	if !haveComputed {
		return nil, catalog.TableColSet{}, nil
	}

	// Build the computed expressions map from the parsed statement.
	computedExprs := make([]tree.TypedExpr, 0, len(input))
	exprStrings := make([]string, 0, len(input))
	for _, col := range input {
		if col.IsComputed() {
			exprStrings = append(exprStrings, col.GetComputeExpr())
		}
	}

	exprs, err := parser.ParseExprs(exprStrings)
	if err != nil {
		return nil, catalog.TableColSet{}, err
	}

	nr := newNameResolver(evalCtx, tableDesc.GetID(), tn, sourceColumns)
	nr.addIVarContainerToSemaCtx(semaCtx)

	var txCtx transform.ExprTransformContext
	compExprIdx := 0
	for _, col := range input {
		if !col.IsComputed() {
			computedExprs = append(computedExprs, tree.DNull)
			nr.addColumn(col)
			continue
		}

		// Collect all column IDs that are referenced in the partial index
		// predicate expression.
		colIDs, err := ExtractColumnIDs(tableDesc, exprs[compExprIdx])
		if err != nil {
			return nil, refColIDs, err
		}
		refColIDs.UnionWith(colIDs)

		expr, err := nr.resolveNames(exprs[compExprIdx])
		if err != nil {
			return nil, catalog.TableColSet{}, err
		}

		typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, col.GetType())
		if err != nil {
			return nil, catalog.TableColSet{}, err
		}
		if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
			return nil, catalog.TableColSet{}, err
		}
		computedExprs = append(computedExprs, typedExpr)
		compExprIdx++
		nr.addColumn(col)
	}
	return computedExprs, refColIDs, nil
}
