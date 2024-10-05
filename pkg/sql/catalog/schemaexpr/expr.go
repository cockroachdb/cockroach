// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/cast"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// DequalifyAndTypeCheckExpr type checks the given expression and returns the
// type-checked expression disregarding volatility. The typed expression, which contains dummyColumns,
// does not support evaluation.
func DequalifyAndTypeCheckExpr(
	ctx context.Context,
	desc catalog.TableDescriptor,
	expr tree.Expr,
	semaCtx *tree.SemaContext,
	tn *tree.TableName,
) (tree.TypedExpr, error) {
	nonDropColumns := desc.NonDropColumns()
	sourceInfo := colinfo.NewSourceInfoForSingleTable(
		*tn, colinfo.ResultColumnsFromColumns(desc.GetID(), nonDropColumns),
	)
	expr, err := dequalifyColumnRefs(ctx, sourceInfo, expr)
	if err != nil {
		return nil, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, _, err := replaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}

	typedExpr, err := tree.TypeCheck(ctx, replacedExpr, semaCtx, types.Any)
	if err != nil {
		return nil, err
	}

	return typedExpr, nil
}

// DequalifyAndValidateExprImpl validates that an expression has the given type and
// contains no functions with a volatility greater than maxVolatility. The
// type-checked and constant-folded expression, the type of the expression, and
// the set of column IDs within the expression are returned, if valid.
//
// The serialized expression is returned because returning the created
// tree.TypedExpr would be dangerous. It contains dummyColumns which do not
// support evaluation and are not useful outside the context of type-checking
// the expression.
func DequalifyAndValidateExprImpl(
	ctx context.Context,
	expr tree.Expr,
	typ *types.T,
	context tree.SchemaExprContext,
	semaCtx *tree.SemaContext,
	maxVolatility volatility.V,
	tn *tree.TableName,
	version clusterversion.ClusterVersion,
	getAllNonDropColumnsFn func() colinfo.ResultColumns,
	columnLookupByNameFn func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T),
) (string, *types.T, catalog.TableColSet, error) {
	var colIDs catalog.TableColSet
	sourceInfo := colinfo.NewSourceInfoForSingleTable(*tn, getAllNonDropColumnsFn())
	expr, err := dequalifyColumnRefs(ctx, sourceInfo, expr)
	if err != nil {
		return "", nil, colIDs, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, colIDs, err := ReplaceColumnVars(expr, columnLookupByNameFn)
	if err != nil {
		return "", nil, colIDs, err
	}

	typedExpr, err := SanitizeVarFreeExpr(
		ctx,
		replacedExpr,
		typ,
		context,
		semaCtx,
		maxVolatility,
		false, /*allowAssignmentCast*/
	)

	if err != nil {
		return "", nil, colIDs, err
	}

	if err := funcdesc.MaybeFailOnUDFUsage(typedExpr, context, version); err != nil {
		return "", nil, colIDs, unimplemented.NewWithIssue(83234, "usage of user-defined function from relations not supported")
	}

	// We need to do the rewrite here before the expression is serialized because
	// the serialization would drop the prefixes to functions.
	//
	typedExpr, err = MaybeReplaceUDFNameWithOIDReferenceInTypedExpr(typedExpr)
	if err != nil {
		return "", nil, colIDs, err
	}

	return tree.Serialize(typedExpr), typedExpr.ResolvedType(), colIDs, nil
}

// DequalifyAndValidateExpr is a convenience function to DequalifyAndValidateExprImpl.
// It delegates to DequalifyAndValidateExprImpl by providing two functions to
// retrieve column information from `desc`:
//  1. `getAllNonDropColumnsFn`: get all non-drop columns in `desc`;
//  2. `columnLookupByNameFn`: look up a column by name in `desc`.
func DequalifyAndValidateExpr(
	ctx context.Context,
	desc catalog.TableDescriptor,
	expr tree.Expr,
	typ *types.T,
	context tree.SchemaExprContext,
	semaCtx *tree.SemaContext,
	maxVolatility volatility.V,
	tn *tree.TableName,
	version clusterversion.ClusterVersion,
) (string, *types.T, catalog.TableColSet, error) {
	getAllNonDropColumnsFn := func() colinfo.ResultColumns {
		return colinfo.ResultColumnsFromColumns(desc.GetID(), desc.NonDropColumns())
	}
	columnLookupByNameFn := func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
		col, err := catalog.MustFindColumnByTreeName(desc, columnName)
		if err != nil || col.Dropped() {
			return false, false, 0, nil
		}
		return true, !col.IsInaccessible(), col.GetID(), col.GetType()
	}

	return DequalifyAndValidateExprImpl(ctx, expr, typ, context, semaCtx, maxVolatility, tn, version,
		getAllNonDropColumnsFn, columnLookupByNameFn)
}

// ExtractColumnIDs returns the set of column IDs within the given expression.
func ExtractColumnIDs(
	desc catalog.TableDescriptor, rootExpr tree.Expr,
) (catalog.TableColSet, error) {
	var colIDs catalog.TableColSet

	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		col, err := catalog.MustFindColumnByTreeName(desc, c.ColumnName)
		if err != nil {
			return false, nil, err
		}

		colIDs.Add(col.GetID())
		return false, expr, nil
	})

	return colIDs, err
}

type returnFalse struct{}

func (returnFalse) Error() string { panic("unimplemented") }

var returnFalsePseudoError error = returnFalse{}

// HasValidColumnReferences returns true if all columns referenced in rootExpr
// exist in desc.
func HasValidColumnReferences(desc catalog.TableDescriptor, rootExpr tree.Expr) (bool, error) {
	_, err := tree.SimpleVisit(rootExpr, func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		vBase, ok := expr.(tree.VarName)
		if !ok {
			return true, expr, nil
		}

		v, err := vBase.NormalizeVarName()
		if err != nil {
			return false, nil, err
		}

		c, ok := v.(*tree.ColumnItem)
		if !ok {
			return true, expr, nil
		}

		if catalog.FindColumnByTreeName(desc, c.ColumnName) == nil {
			return false, expr, returnFalsePseudoError
		}

		return false, expr, nil
	})
	if errors.Is(err, returnFalsePseudoError) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// FormatExprForDisplay formats a schema expression string for display. It
// accepts formatting flags to control things like showing type annotations or
// type casts.
func FormatExprForDisplay(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	fmtFlags tree.FmtFlags,
) (string, error) {
	return formatExprForDisplayImpl(
		ctx,
		desc,
		exprStr,
		semaCtx,
		sessionData,
		fmtFlags,
		false, /* wrapNonFuncExprs */
	)
}

// FormatExprForExpressionIndexDisplay formats an expression index's expression
// element string for display. It is similar to FormatExprForDisplay. The only
// difference is that non-function expressions will be wrapped in parentheses to
// match the parsing requirements for expression indexes.
func FormatExprForExpressionIndexDisplay(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	fmtFlags tree.FmtFlags,
) (string, error) {
	return formatExprForDisplayImpl(
		ctx,
		desc,
		exprStr,
		semaCtx,
		sessionData,
		fmtFlags,
		true, /* wrapNonFuncExprs */
	)
}

func formatExprForDisplayImpl(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	fmtFlags tree.FmtFlags,
	wrapNonFuncExprs bool,
) (string, error) {
	expr, err := deserializeExprForFormatting(ctx, desc, exprStr, semaCtx, fmtFlags)
	if err != nil {
		return "", err
	}
	// Replace any IDs in the expr with their fully qualified names.
	replacedExpr, err := ReplaceSequenceIDsWithFQNames(ctx, expr, semaCtx)
	if err != nil {
		return "", err
	}
	f := tree.NewFmtCtx(
		fmtFlags,
		tree.FmtDataConversionConfig(sessionData.DataConversionConfig),
		tree.FmtLocation(sessionData.Location),
	)
	_, isFunc := expr.(*tree.FuncExpr)
	if wrapNonFuncExprs && !isFunc {
		f.WriteByte('(')
	}
	f.FormatNode(replacedExpr)
	if wrapNonFuncExprs && !isFunc {
		f.WriteByte(')')
	}
	return f.CloseAndGetString(), nil
}

func deserializeExprForFormatting(
	ctx context.Context,
	desc catalog.TableDescriptor,
	exprStr string,
	semaCtx *tree.SemaContext,
	fmtFlags tree.FmtFlags,
) (tree.Expr, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return nil, err
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, _, err := replaceColumnVars(desc, expr)
	if err != nil {
		return nil, err
	}

	// Type-check the expression to resolve user defined types.
	typedExpr, err := replacedExpr.TypeCheck(ctx, semaCtx, types.Any)
	if err != nil {
		return nil, err
	}

	// In pg_catalog, we need to make sure we always display constants instead of
	// expressions, when possible (e.g., turn Array expr into a DArrray). This is
	// best-effort, so if there is any error, it is safe to fallback to the
	// typedExpr.
	if fmtFlags == tree.FmtPGCatalog {
		sanitizedExpr, err := SanitizeVarFreeExpr(ctx, expr, typedExpr.ResolvedType(), "FORMAT", semaCtx,
			volatility.Immutable, false /*allowAssignmentCast*/)
		// If the expr has no variables and has Immutable, we can evaluate
		// it and turn it into a constant.
		if err == nil {
			// An empty EvalContext is fine here since the expression has
			// Immutable.
			d, err := eval.Expr(ctx, &eval.Context{}, sanitizedExpr)
			if err == nil {
				return d, nil
			}
		}
	}

	return typedExpr, nil
}

// nameResolver is used to replace unresolved names in expressions with
// IndexedVars.
type nameResolver struct {
	evalCtx    *eval.Context
	tableID    descpb.ID
	source     *colinfo.DataSourceInfo
	nrc        *nameResolverIVarContainer
	ivarHelper *tree.IndexedVarHelper
}

// newNameResolver creates and returns a nameResolver.
func newNameResolver(
	evalCtx *eval.Context, tableID descpb.ID, tn *tree.TableName, cols []catalog.Column,
) *nameResolver {
	source := colinfo.NewSourceInfoForSingleTable(
		*tn,
		colinfo.ResultColumnsFromColumns(tableID, cols),
	)
	nrc := &nameResolverIVarContainer{append(make([]catalog.Column, 0, len(cols)), cols...)}
	ivarHelper := tree.MakeIndexedVarHelper(nrc, len(cols))

	return &nameResolver{
		evalCtx:    evalCtx,
		tableID:    tableID,
		source:     source,
		nrc:        nrc,
		ivarHelper: &ivarHelper,
	}
}

// resolveNames returns an expression equivalent to the input expression with
// unresolved names replaced with IndexedVars.
func (nr *nameResolver) resolveNames(expr tree.Expr) (tree.Expr, error) {
	var v NameResolutionVisitor
	return ResolveNamesUsingVisitor(&v, expr, nr.source, *nr.ivarHelper)
}

// addColumn adds a new column to the nameResolver so that it can be resolved in
// future calls to resolveNames.
func (nr *nameResolver) addColumn(col catalog.Column) {
	nr.ivarHelper.AppendSlot()
	nr.nrc.cols = append(nr.nrc.cols, col)
	newCols := colinfo.ResultColumnsFromColumns(nr.tableID, []catalog.Column{col})
	nr.source.SourceColumns = append(nr.source.SourceColumns, newCols...)
}

// addIVarContainerToSemaCtx assigns semaCtx's IVarContainer as the
// nameResolver's internal indexed var container.
func (nr *nameResolver) addIVarContainerToSemaCtx(semaCtx *tree.SemaContext) {
	semaCtx.IVarContainer = nr.nrc
}

// nameResolverIVarContainer is a help type that implements
// tree.IndexedVarContainer. It is used to resolve and type check columns in
// expressions. It does not support evaluation.
type nameResolverIVarContainer struct {
	cols []catalog.Column
}

var _ eval.IndexedVarContainer = &nameResolverIVarContainer{}

// IndexedVarEval implements the eval.IndexedVarContainer interface.
// Evaluation is not supported, so this function panics.
func (nrc *nameResolverIVarContainer) IndexedVarEval(
	ctx context.Context, idx int, e tree.ExprEvaluator,
) (tree.Datum, error) {
	panic("unsupported")
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (nrc *nameResolverIVarContainer) IndexedVarResolvedType(idx int) *types.T {
	return nrc.cols[idx].GetType()
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (nrc *nameResolverIVarContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	ctx context.Context,
	expr tree.Expr,
	expectedType *types.T,
	context tree.SchemaExprContext,
	semaCtx *tree.SemaContext,
	maxVolatility volatility.V,
	allowAssignmentCast bool,
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

	switch maxVolatility {
	case volatility.Immutable:
		flags |= tree.RejectStableOperators
		fallthrough

	case volatility.Stable:
		flags |= tree.RejectVolatileFunctions

	case volatility.Volatile:
		// Allow anything (no flags needed).

	default:
		panic(errors.AssertionFailedf("maxVolatility %s not supported", maxVolatility))
	}
	semaCtx.Properties.Require(string(context), flags)

	typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, expectedType)
	if err != nil {
		return nil, err
	}

	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value or assignment casts are allowed.
		if allowAssignmentCast {
			if ok := cast.ValidCast(actualType, expectedType, cast.ContextAssignment); ok {
				return typedExpr, nil
			}
		}
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

// ValidateTTLExpressionDoesNotDependOnColumn verifies that the
// ttl_expiration_expression, if any, does not reference the given column.
func ValidateTTLExpressionDoesNotDependOnColumn(
	tableDesc catalog.TableDescriptor, rowLevelTTL *catpb.RowLevelTTL, col catalog.Column,
) error {
	if rowLevelTTL == nil || !rowLevelTTL.HasExpirationExpr() {
		return nil
	}
	expirationExpr := rowLevelTTL.ExpirationExpr
	expr, err := parser.ParseExpr(string(expirationExpr))
	if err != nil {
		// At this point, we should be able to parse the expiration expression.
		return errors.WithAssertionFailure(err)
	}
	referencedCols, err := ExtractColumnIDs(tableDesc, expr)
	if err != nil {
		return err
	}
	if referencedCols.Contains(col.GetID()) {
		return pgerror.Newf(
			pgcode.InvalidColumnReference,
			"column %q is referenced by row-level TTL expiration expression %q",
			col.ColName(), expirationExpr,
		)
	}
	return nil
}

// ValidateTTLExpirationExpression verifies that the ttl_expiration_expression,
// if any, is valid according to the following rules:
// * type-checks as a TIMESTAMPTZ.
// * is not volatile.
// * references valid columns in the table.
func ValidateTTLExpirationExpression(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	semaCtx *tree.SemaContext,
	tableName *tree.TableName,
	ttl *catpb.RowLevelTTL,
	version clusterversion.ClusterVersion,
) error {

	if !ttl.HasExpirationExpr() {
		return nil
	}

	exprs, err := parser.ParseExprs([]string{string(ttl.ExpirationExpr)})
	if err != nil {
		return pgerror.Wrapf(
			err,
			pgcode.InvalidParameterValue,
			`ttl_expiration_expression %q must be a valid expression`,
			ttl.ExpirationExpr,
		)
	} else if len(exprs) != 1 {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`ttl_expiration_expression %q must be a single expression`,
			ttl.ExpirationExpr,
		)
	}

	// ttl_expiration_expression requires a maximum volatility of stable to
	// handle one of its main use cases: timestamptz + interval.
	// Altering config while the job is running can affect some (instead of all)
	// SELECT and DELETE statements because the statements in the job are NOT run
	// inside a single transaction.
	// Only config changes can affect the results of Stable functions in the TTL
	// job because session data cannot be modified.
	if _, _, _, err := DequalifyAndValidateExpr(
		ctx,
		tableDesc,
		exprs[0],
		types.TimestampTZ,
		tree.TTLExpirationExpr,
		semaCtx,
		volatility.Stable,
		tableName,
		version,
	); err != nil {
		return pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
	}

	// todo: check dropped column here?
	return nil
}

func MaybeReplaceUDFNameWithOIDReferenceInTypedExpr(
	typedExpr tree.TypedExpr,
) (tree.TypedExpr, error) {
	replaceFunc := func(ex tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		funcExpr, ok := ex.(*tree.FuncExpr)
		if !ok {
			return true, ex, nil
		}
		if funcExpr.ResolvedOverload() == nil {
			return false, nil, errors.AssertionFailedf("function expression has not been type checked")
		}
		// We only want to replace with OID reference when it's a UDF.
		if funcExpr.ResolvedOverload().Type != tree.UDFRoutine {
			return true, ex, nil
		}
		newFuncExpr := *funcExpr
		newFuncExpr.Func = tree.ResolvableFunctionReference{
			FunctionReference: &tree.FunctionOID{OID: funcExpr.ResolvedOverload().Oid},
		}
		return true, &newFuncExpr, nil
	}

	newExpr, err := tree.SimpleVisit(typedExpr, replaceFunc)
	if err != nil {
		return nil, err
	}
	return newExpr.(tree.TypedExpr), nil
}

// GetUDFIDs extracts all UDF descriptor ids from the given expression,
// assuming that the UDF names has been replaced with OID references.
func GetUDFIDs(e tree.Expr) (catalog.DescriptorIDSet, error) {
	var fnIDs catalog.DescriptorIDSet
	if _, err := tree.SimpleVisit(e, func(ckExpr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		switch t := ckExpr.(type) {
		case *tree.FuncExpr:
			if ref, ok := t.Func.FunctionReference.(*tree.FunctionOID); ok && funcdesc.IsOIDUserDefinedFunc(ref.OID) {
				fnIDs.Add(funcdesc.UserDefinedFunctionOIDToID(ref.OID))
			}
		}
		return true, ckExpr, nil
	}); err != nil {
		return catalog.DescriptorIDSet{}, err
	}
	return fnIDs, nil
}

// GetUDFIDsFromExprStr extracts all UDF descriptor ids from the given
// expression string, assuming that the UDF names has been replaced with OID
// references. It's a convenient wrapper of GetUDFIDs.
func GetUDFIDsFromExprStr(exprStr string) (catalog.DescriptorIDSet, error) {
	expr, err := parser.ParseExpr(exprStr)
	if err != nil {
		return catalog.DescriptorIDSet{}, err
	}
	return GetUDFIDs(expr)
}
