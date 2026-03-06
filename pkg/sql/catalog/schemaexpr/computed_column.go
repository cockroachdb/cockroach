// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"context"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// ValidateComputedColumnExpression verifies that an expression is a valid
// computed column expression. It returns the serialized expression and its type
// if valid, and an error otherwise. The returned type is only useful if d has
// type AnyElement which indicates the expression's type is unknown and does not
// have to match a specific type.
//
// A computed column expression is valid if all of the following are true:
//
//   - It does not have a default value.
//   - It does not reference other computed columns.
//   - It does not reference inaccessible columns.
//   - It does not depend on the region column name if the table is REGIONAL BY
//     ROW and uses a foreign key to populate the region column.
//
// TODO(mgartner): Add unit tests for Validate.
func ValidateComputedColumnExpression(
	ctx context.Context,
	desc catalog.TableDescriptor,
	d *tree.ColumnTableDef,
	tn *tree.TableName,
	context tree.SchemaExprContext,
	semaCtx *tree.SemaContext,
	version clusterversion.ClusterVersion,
) (serializedExpr string, _ *types.T, _ error) {
	// Create helper functions from the descriptor and delegate to the
	// lookup-based validation function.
	getAllNonDropColumnsFn := func() colinfo.ResultColumns {
		cols := desc.NonDropColumns()
		ret := make(colinfo.ResultColumns, len(cols))
		for i, col := range cols {
			ret[i] = colinfo.ResultColumn{
				Name:           col.GetName(),
				Typ:            col.GetType(),
				Hidden:         col.IsHidden(),
				TableID:        desc.GetID(),
				PGAttributeNum: uint32(col.GetPGAttributeNum()),
			}
		}
		return ret
	}

	columnLookupFn := makeColumnLookupFnForTableDesc(desc)

	serializedExpr, typ, err := ValidateComputedColumnExpressionWithLookup(
		ctx,
		desc,
		d,
		tn,
		context,
		semaCtx,
		version,
		getAllNonDropColumnsFn,
		columnLookupFn,
	)
	if err != nil {
		return "", nil, err
	}

	var depColIDs catalog.TableColSet
	if err := iterColDescriptors(desc, d.Computed.Expr, func(c catalog.Column) error {
		depColIDs.Add(c.GetID())
		return nil
	}); err != nil {
		return "", nil, err
	}

	// Virtual computed columns must not refer to mutation columns because it
	// would not be safe in the case that the mutation column was being
	// backfilled and the virtual computed column value needed to be computed
	// for the purpose of writing to a secondary index.
	// This check is specific to the legacy schema changer.
	if d.IsVirtual() {
		var mutationColumnNames []string
		var err error
		depColIDs.ForEach(func(colID descpb.ColumnID) {
			if err != nil {
				return
			}
			var col catalog.Column
			if col, err = catalog.MustFindColumnByID(desc, colID); err != nil {
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
			if context == tree.ExpressionIndexElementExpr {
				return "", nil, unimplemented.Newf(
					"index element expression referencing mutation columns",
					"index element expression referencing columns (%s) added in the current transaction",
					strings.Join(mutationColumnNames, ", "))
			}
			return "", nil, unimplemented.Newf(
				"virtual computed columns referencing mutation columns",
				"virtual computed column %q referencing columns (%s) added in the "+
					"current transaction", d.Name, strings.Join(mutationColumnNames, ", "))
		}
	}

	return serializedExpr, typ, nil
}

// ValidateComputedColumnExpressionWithLookup verifies that an expression is a
// valid computed column expression using a column lookup function. It returns
// the serialized expression and its type if valid, and an error otherwise.
//
// This is similar to ValidateComputedColumnExpression but uses a ColumnLookupFn
// instead of a catalog.TableDescriptor, allowing it to work with declarative
// schema changer elements.
func ValidateComputedColumnExpressionWithLookup(
	ctx context.Context,
	desc catalog.TableDescriptor,
	d *tree.ColumnTableDef,
	tn *tree.TableName,
	context tree.SchemaExprContext,
	semaCtx *tree.SemaContext,
	version clusterversion.ClusterVersion,
	getAllNonDropColumnsFn func() colinfo.ResultColumns,
	columnLookupFn ColumnLookupFn,
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
	err := iterColsWithLookupFn(d.Computed.Expr, columnLookupFn,
		func(columnName tree.Name, id catid.ColumnID, typ *types.T, isAccessible, isComputed bool) error {
			if !isAccessible {
				return pgerror.Newf(
					pgcode.UndefinedColumn,
					"column %q is inaccessible and cannot be referenced in a computed column expression",
					columnName,
				)
			}
			if isComputed {
				return pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"%s expression cannot reference computed columns",
					context,
				)
			}
			depColIDs.Add(id)
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
	// functions. We use DequalifyAndValidateExprImpl with the lookup function.
	expr, typ, _, err := DequalifyAndValidateExprImpl(
		ctx,
		d.Computed.Expr,
		defType,
		context,
		semaCtx,
		volatility.Immutable,
		tn,
		version,
		getAllNonDropColumnsFn,
		columnLookupFn,
	)
	if err != nil {
		return "", nil, err
	}

	// If this is a REGIONAL BY ROW table using a foreign key to populate the
	// region column, we need to check that the expression does not reference
	// the region column. This is because the values of every (possibly computed)
	// foreign-key column must be known in order to determine the value for the
	// region column.
	if desc.GetRegionalByRowUsingConstraint() != descpb.ConstraintID(0) {
		regionColName, err := desc.GetRegionalByRowTableRegionColumnName()
		if err != nil {
			return "", nil, err
		}
		col, err := catalog.MustFindColumnByName(desc, string(regionColName))
		if err != nil {
			return "", nil, errors.WithAssertionFailure(err)
		}
		if depColIDs.Contains(col.GetID()) {
			return "", nil, sqlerrors.NewComputedColReferencesRegionColError(d.Name, col.ColName())
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

		expr, err := parserutils.ParseExpr(c.GetComputeExpr())
		if err != nil {
			// At this point, we should be able to parse the computed expression.
			return errors.WithAssertionFailure(err)
		}

		err = iterColDescriptors(desc, expr, func(colVar catalog.Column) error {
			if colVar.GetID() == col.GetID() {
				return sqlerrors.NewColumnReferencedByComputedColumnError(col.GetName(), c.GetName())
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
	evalCtx *eval.Context,
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

	exprs, err := parserutils.ParseExprs(exprStrings)
	if err != nil {
		return nil, catalog.TableColSet{}, err
	}

	nr := newNameResolver(tableDesc.GetID(), tn, sourceColumns)
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

		// Inline any UDF calls so that the expression can be evaluated
		// by eval.Expr. Contexts that bypass the optimizer (IMPORT,
		// backfill) cannot evaluate functions with SQL bodies directly.
		// This must happen before the assignment cast so the cast
		// operates on the resolved expression, not a FuncExpr wrapper.
		typedExpr, err = inlineUDFCalls(ctx, typedExpr, semaCtx)
		if err != nil {
			return nil, catalog.TableColSet{}, err
		}

		// If the expression has a type that is not identical to the
		// column's type, wrap the computed column expression in an assignment cast.
		typedExpr, err = wrapWithAssignmentCast(ctx, typedExpr, col, semaCtx)
		if err != nil {
			return nil, catalog.TableColSet{}, err
		}

		if typedExpr, err = txCtx.NormalizeExpr(ctx, evalCtx, typedExpr); err != nil {
			return nil, catalog.TableColSet{}, err
		}
		computedExprs = append(computedExprs, typedExpr)
		compExprIdx++
		nr.addColumn(col)
	}
	return computedExprs, refColIDs, nil
}

// inlineUDFCalls walks a TypedExpr tree and replaces FuncExpr nodes that
// reference user-defined functions (those with SQL bodies) with the inlined
// body expression. This is necessary because eval.Expr cannot evaluate
// functions with SQL bodies — that capability is only available through
// the optimizer. Contexts that evaluate computed column expressions without
// the optimizer (IMPORT, schema change backfill) rely on this inlining.
//
// Only single-statement SQL-language UDFs can be inlined. Multi-statement
// or PL/pgSQL functions will produce an error.
func inlineUDFCalls(
	ctx context.Context, expr tree.TypedExpr, semaCtx *tree.SemaContext,
) (tree.TypedExpr, error) {
	newExpr, err := tree.SimpleVisit(expr, func(e tree.Expr) (bool, tree.Expr, error) {
		funcExpr, ok := e.(*tree.FuncExpr)
		if !ok {
			return true, e, nil
		}
		fn := funcExpr.ResolvedOverload()
		if fn == nil || fn.Body == "" {
			return true, e, nil
		}
		if fn.Language != tree.RoutineLangSQL {
			return false, nil, unimplemented.Newf(
				"computed_column_plpgsql_udf",
				"PL/pgSQL user-defined functions in computed columns "+
					"cannot be evaluated in this context",
			)
		}

		// Parse the function body.
		stmts, err := parserutils.Parse(fn.Body)
		if err != nil {
			return false, nil, errors.Wrap(err, "parsing UDF body for inlining")
		}
		if len(stmts) != 1 {
			return false, nil, unimplemented.Newf(
				"computed_column_multi_stmt_udf",
				"multi-statement user-defined functions in computed columns "+
					"cannot be evaluated in this context",
			)
		}

		// Extract the result expression from the SELECT statement.
		sel, ok := stmts[0].AST.(*tree.Select)
		if !ok {
			return false, nil, errors.Newf(
				"expected SELECT in UDF body, got %T", stmts[0].AST,
			)
		}
		selClause, ok := sel.Select.(*tree.SelectClause)
		if !ok || len(selClause.Exprs) != 1 {
			return false, nil, errors.Newf(
				"expected single-expression SELECT in UDF body",
			)
		}
		bodyExpr := selClause.Exprs[0].Expr

		// Build a mapping from parameter names to the actual argument
		// expressions from the call site. The body references parameters
		// by name (e.g. "x"), and we replace those with the
		// already-typed argument expressions (e.g. IndexedVar).
		paramMap := make(map[string]tree.Expr, len(fn.RoutineParams))
		for i, p := range fn.RoutineParams {
			if i < len(funcExpr.Exprs) {
				paramMap[string(p.Name)] = funcExpr.Exprs[i]
			}
		}

		// Substitute parameter references in the body.
		inlined, err := tree.SimpleVisit(
			bodyExpr,
			func(inner tree.Expr) (bool, tree.Expr, error) {
				if name, ok := inner.(*tree.UnresolvedName); ok &&
					name.NumParts == 1 {
					if arg, exists := paramMap[name.Parts[0]]; exists {
						return false, arg, nil
					}
				}
				if ci, ok := inner.(*tree.ColumnItem); ok {
					if arg, exists := paramMap[string(ci.ColumnName)]; exists {
						return false, arg, nil
					}
				}
				return true, inner, nil
			},
		)
		if err != nil {
			return false, nil, err
		}

		// For strict functions (RETURNS NULL ON NULL INPUT), wrap the
		// inlined body to return NULL when any argument is NULL,
		// preserving the function's null-handling semantics.
		if !fn.CalledOnNullInput && len(funcExpr.Exprs) > 0 {
			var nullCheck tree.TypedExpr
			for i, arg := range funcExpr.Exprs {
				isNull := &tree.IsNullExpr{Expr: arg}
				if i == 0 {
					nullCheck = isNull
				} else {
					nullCheck = tree.NewTypedOrExpr(nullCheck, isNull)
				}
			}
			inlined = &tree.CaseExpr{
				Whens: []*tree.When{{
					Cond: nullCheck,
					Val:  tree.DNull,
				}},
				Else: inlined,
			}
		}

		// Type-check the inlined expression against the function's
		// return type.
		typedInlined, err := tree.TypeCheck(
			ctx, inlined, semaCtx, funcExpr.ResolvedType(),
		)
		if err != nil {
			return false, nil, errors.Wrap(err, "type-checking inlined UDF body")
		}

		return false, typedInlined, nil
	})
	if err != nil {
		return nil, err
	}
	return newExpr.(tree.TypedExpr), nil
}
