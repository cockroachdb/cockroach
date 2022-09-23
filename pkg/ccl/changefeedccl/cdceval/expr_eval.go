// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// Evaluator is a responsible for evaluating expressions in CDC.
type Evaluator struct {
	selectors []tree.SelectExpr
	from      tree.TableExpr
	where     tree.Expr

	evalCtx *eval.Context
	// Current evaluator.  Re-initialized whenever event descriptor
	// version changes.
	evaluator *exprEval
}

// NewEvaluator returns evaluator configured to process specified
// select expression.
func NewEvaluator(evalCtx *eval.Context, sc *tree.SelectClause) (*Evaluator, error) {
	e := &Evaluator{evalCtx: evalCtx.Copy()}

	if len(sc.From.Tables) > 0 { // 0 tables used only in tests.
		if len(sc.From.Tables) != 1 {
			return nil, errors.AssertionFailedf("expected 1 table, found %d", len(sc.From.Tables))
		}
		e.from = sc.From.Tables[0]
	}

	if err := e.initSelectClause(evalCtx.Ctx(), sc); err != nil {
		return nil, err
	}

	return e, nil
}

// ComputeVirtualColumns updates row with computed values for all virtual columns.
func (e *Evaluator) ComputeVirtualColumns(ctx context.Context, row *cdcevent.Row) error {
	return errors.AssertionFailedf("unimplemented yet")
}

// MatchesFilter returns true if row matches evaluator filter expression.
func (e *Evaluator) MatchesFilter(
	ctx context.Context, updatedRow cdcevent.Row, mvccTS hlc.Timestamp, prevRow cdcevent.Row,
) (bool, error) {
	if e.where == nil {
		return true, nil
	}

	if err := e.initEval(ctx, updatedRow.EventDescriptor); err != nil {
		return false, err
	}

	return e.evaluator.matchesFilter(ctx, updatedRow, mvccTS, prevRow)
}

// Projection performs evalProjection operation on the updated row.
// mvccTS is an mvcc timestamp of updated row, and prevRow may optionally contain
// the value of the previous row.
// Returns cdcevent.Row representing evalProjection.
func (e *Evaluator) Projection(
	ctx context.Context, updatedRow cdcevent.Row, mvccTS hlc.Timestamp, prevRow cdcevent.Row,
) (cdcevent.Row, error) {
	if len(e.selectors) == 0 {
		return updatedRow, nil
	}

	if err := e.initEval(ctx, updatedRow.EventDescriptor); err != nil {
		return cdcevent.Row{}, err
	}

	return e.evaluator.evalProjection(ctx, updatedRow, mvccTS, prevRow)
}

// initSelectClause configures this evaluator to evaluate specified select clause.
func (e *Evaluator) initSelectClause(ctx context.Context, sc *tree.SelectClause) error {
	if len(sc.Exprs) == 0 { // Shouldn't happen, but be defensive.
		return pgerror.New(pgcode.InvalidParameterValue,
			"expected at least 1 projection")
	}

	semaCtx := newSemaCtx()
	e.selectors = sc.Exprs
	for _, se := range e.selectors {
		expr, err := validateExpressionForCDC(ctx, se.Expr, semaCtx)
		if err != nil {
			return err
		}
		se.Expr = expr
	}

	if sc.Where != nil {
		expr, err := validateExpressionForCDC(ctx, sc.Where.Expr, semaCtx)
		if err != nil {
			return err
		}
		e.where = expr
	}

	return nil
}

// initEval initializes evaluator for the specified event descriptor.
func (e *Evaluator) initEval(ctx context.Context, d *cdcevent.EventDescriptor) error {
	if e.evaluator != nil {
		sameVersion, sameTypes := d.EqualsWithUDTCheck(e.evaluator.EventDescriptor)
		if sameVersion && sameTypes {
			// Event descriptor and UDT types are the same -- re-use the same evaluator.
			return nil
		}

		if sameVersion {
			// Here, we know that even though descriptor versions are the same, the
			// check for equality with UDT type check failed.  Thus, we know some user
			// defined types have changed.
			// The previously parsed select & filter expressions have type annotations,
			// and those may now be incorrect.  So, parse and re-initialize evaluator
			// expressions.
			var where *tree.Where
			if e.where != nil {
				where = tree.NewWhere(tree.AstWhere, e.where)
			}
			sc, err := ParseChangefeedExpression(AsStringUnredacted(&tree.SelectClause{
				From:  tree.From{Tables: tree.TableExprs{e.from}},
				Exprs: e.selectors,
				Where: where,
			}))
			if err != nil {
				return err
			}
			if err := e.initSelectClause(ctx, sc); err != nil {
				return err
			}
			// Fall through to re-create e.evaluator.
		}
	}

	evaluator := newExprEval(e.evalCtx, d, tableNameOrAlias(d.TableName, e.from))
	for _, selector := range e.selectors {
		if err := evaluator.addSelector(ctx, selector, len(e.selectors)); err != nil {
			return err
		}
	}

	if err := evaluator.addFilter(ctx, e.where); err != nil {
		return err
	}

	e.evaluator = evaluator
	return nil
}

type exprEval struct {
	*cdcevent.EventDescriptor
	semaCtx *tree.SemaContext
	evalCtx *eval.Context

	evalHelper *rowContainer         // evalHelper is a container tree.IndexedVarContainer.
	iVarHelper tree.IndexedVarHelper // iVarHelper helps create indexed variables bound to evalHelper.
	resolver   cdcNameResolver       // resolver responsible for performing function name resolution.

	starProjection bool                // Set to true if we have a single '*' projection.
	selectors      []tree.TypedExpr    // set of expressions to evaluate when performing evalProjection.
	projection     cdcevent.Projection // cdcevent.Projects helps construct projection results.
	filter         tree.TypedExpr      // where clause filter

	// keep track of number of times particular column name was used
	// in selectors.  Since the data produced by CDC gets converted
	// to the formats (JSON, avro, etc.) that may not like having multiple
	// fields named the same way, this map helps us unique-ify those columns.
	nameUseCount map[string]int

	// rowEvalCtx contains state necessary to evaluate expressions.
	// updated for each row.
	rowEvalCtx rowEvalContext
}

func newExprEval(
	evalCtx *eval.Context, ed *cdcevent.EventDescriptor, tableName *tree.TableName,
) *exprEval {
	cols := ed.ResultColumns()
	e := &exprEval{
		EventDescriptor: ed,
		semaCtx:         newSemaCtxWithTypeResolver(ed),
		evalCtx:         evalCtx.Copy(),
		evalHelper:      &rowContainer{cols: cols},
		projection:      cdcevent.MakeProjection(ed),
		nameUseCount:    make(map[string]int),
	}

	evalCtx = nil // From this point, only e.evalCtx should be used.

	// Configure semantic context.
	e.semaCtx.SearchPath = &sessiondata.DefaultSearchPath
	e.semaCtx.FunctionResolver = &CDCFunctionResolver{}
	e.semaCtx.Properties.Require("cdc",
		tree.RejectAggregates|tree.RejectGenerators|tree.RejectWindowApplications|tree.RejectNestedGenerators,
	)
	e.semaCtx.Annotations = tree.MakeAnnotations(cdcAnnotationAddr)
	e.semaCtx.IVarContainer = e.evalHelper

	// Configure evaluation context.
	e.evalCtx.Annotations = &e.semaCtx.Annotations
	e.evalCtx.Annotations.Set(cdcAnnotationAddr, &e.rowEvalCtx)
	e.evalCtx.IVarContainer = e.evalHelper

	// Extract colinfo.ResultColumn from cdcevent.ResultColumn
	nakedResultColumns := func() (rc []colinfo.ResultColumn) {
		rc = make([]colinfo.ResultColumn, len(cols))
		for i := 0; i < len(cols); i++ {
			rc[i] = cols[i].ResultColumn
		}
		return rc
	}

	e.iVarHelper = tree.MakeIndexedVarHelper(e.evalHelper, len(cols))
	e.resolver = cdcNameResolver{
		EventDescriptor: ed,
		NameResolutionVisitor: schemaexpr.MakeNameResolutionVisitor(
			colinfo.NewSourceInfoForSingleTable(*tableName, nakedResultColumns()),
			e.iVarHelper,
		),
	}

	return e
}

// rowEvalContext represents the context needed to evaluate row expressions.
type rowEvalContext struct {
	mvccTS     hlc.Timestamp
	updatedRow cdcevent.Row
	prevRow    cdcevent.Row
	memo       struct {
		prevJSON tree.Datum
	}
}

// setupContext configures evaluation context with the provided row information.
func (e *exprEval) setupContext(
	updatedRow cdcevent.Row, mvccTS hlc.Timestamp, prevRow cdcevent.Row,
) {
	e.rowEvalCtx.updatedRow = updatedRow
	e.rowEvalCtx.prevRow = prevRow
	e.rowEvalCtx.mvccTS = mvccTS
	e.evalCtx.TxnTimestamp = mvccTS.GoTime()
	e.evalCtx.StmtTimestamp = mvccTS.GoTime()

	// Clear out all memo records
	e.rowEvalCtx.memo.prevJSON = nil
}

// evalProjection responsible for evaluating projection expression.
// Returns new projection Row.
func (e *exprEval) evalProjection(
	ctx context.Context, updatedRow cdcevent.Row, mvccTS hlc.Timestamp, prevRow cdcevent.Row,
) (cdcevent.Row, error) {
	if e.starProjection {
		return updatedRow, nil
	}

	e.setupContext(updatedRow, mvccTS, prevRow)

	for i, expr := range e.selectors {
		d, err := e.evalExpr(ctx, expr, types.Any)
		if err != nil {
			return cdcevent.Row{}, err
		}
		if err := e.projection.SetValueDatumAt(e.evalCtx, i, d); err != nil {
			return cdcevent.Row{}, err
		}
	}

	return e.projection.Project(updatedRow)
}

// matchesFilter returns true if row matches configured filter.
func (e *exprEval) matchesFilter(
	ctx context.Context, updatedRow cdcevent.Row, mvccTS hlc.Timestamp, prevRow cdcevent.Row,
) (bool, error) {
	if e.filter == nil {
		return true, nil
	}

	e.setupContext(updatedRow, mvccTS, prevRow)
	d, err := e.evalExpr(ctx, e.filter, types.Bool)
	if err != nil {
		return false, err
	}
	return d == tree.DBoolTrue, nil
}

// computeRenderColumnName returns render name for a selector, adjusted for CDC use case.
func (e *exprEval) computeRenderColumnName(selector tree.SelectExpr) (string, error) {
	as, err := func() (string, error) {
		if selector.As != "" {
			return string(selector.As), nil
		}
		// We use ComputeColNameInternal instead of GetRenderName because the latter, if it can't
		// figure out the name, returns "?column?" as the name; but we want to name things slightly
		// different in that case.
		_, s, err := tree.ComputeColNameInternal(e.evalCtx.Ctx(), e.semaCtx.SearchPath, selector.Expr, e.semaCtx.FunctionResolver)
		return s, err
	}()
	if err != nil {
		return "", err
	}

	if as == "" {
		as = fmt.Sprintf("column_%d", 1+len(e.selectors))
	}
	return e.makeUniqueName(as), nil
}

// makeUniqueName returns a unique name for the specified name.
// We do this because seeing same named fields in JSON might be confusing.
func (e *exprEval) makeUniqueName(as string) string {
	useCount := e.nameUseCount[as]
	e.nameUseCount[as]++
	if useCount > 0 {
		// Unique-ify evalProjection name.
		as = fmt.Sprintf("%s_%d", as, useCount)
	}
	return as
}

// addSelector adds specified select expression to evalProjection set.
func (e *exprEval) addSelector(
	ctx context.Context, selector tree.SelectExpr, numSelectors int,
) error {
	as, err := e.computeRenderColumnName(selector)
	if err != nil {
		return err
	}

	typedExpr, err := e.typeCheck(ctx, selector.Expr, types.Any)
	if err != nil {
		return err
	}

	// Expand "*".  We walked expression during type check above, so we only expect to
	// see UnqualifiedStar.
	if _, isStar := typedExpr.(tree.UnqualifiedStar); isStar {
		if numSelectors == 1 {
			// Single star gets special treatment.
			e.starProjection = true
		} else {
			for ord, col := range e.ResultColumns() {
				e.addProjection(e.iVarHelper.IndexedVar(ord), e.makeUniqueName(col.Name))
			}
		}
	} else {
		e.addProjection(typedExpr, as)
	}

	return nil
}

// addFilter adds where clause filter.
func (e *exprEval) addFilter(ctx context.Context, where tree.Expr) error {
	if where == nil {
		return nil
	}
	typedExpr, err := e.typeCheck(ctx, where, types.Bool)
	if err != nil {
		return err
	}

	if typedExpr == tree.DBoolTrue {
		if log.V(1) {
			log.Infof(ctx, "ignoring tautological filter %q", where)
		}
		return nil
	}

	if typedExpr == tree.DBoolFalse {
		return errors.Newf("filter %q is a contradiction", where)
	}

	e.filter = typedExpr
	return nil
}

// addProjection adds expression to be returned by evalProjection.
func (e *exprEval) addProjection(expr tree.TypedExpr, as string) {
	e.selectors = append(e.selectors, expr)
	e.projection.AddValueColumn(as, expr.ResolvedType())
}

// typeCheck converts expression to the expression of specified target type.
func (e *exprEval) typeCheck(
	ctx context.Context, expr tree.Expr, targetType *types.T,
) (tree.TypedExpr, error) {
	// If we have variable free immutable expressions, then we can just evaluate it right away.
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		ctx, expr, targetType, "cdc", e.semaCtx,
		volatility.Immutable, true)
	if err == nil {
		d, err := eval.Expr(e.evalCtx, typedExpr)
		if err != nil {
			return nil, err
		}
		return d, nil
	}

	// We must work harder.  Bind variables and resolve names.
	expr, _ = tree.WalkExpr(&e.resolver, expr)
	if e.resolver.err != nil {
		return nil, e.resolver.err
	}

	if star, isStar := expr.(tree.UnqualifiedStar); isStar {
		// Can't type check star -- we'll handle it later during eval.
		return star, nil
	}

	// Run type check & normalize.
	typedExpr, err = expr.TypeCheck(ctx, e.semaCtx, targetType)
	if err != nil {
		return nil, err
	}
	return normalize.Expr(e.evalCtx, typedExpr)
}

// evalExpr evaluates typed expression and returns resulting datum.
// must be called after setupContext has been called.
func (e *exprEval) evalExpr(
	ctx context.Context, expr tree.TypedExpr, targetType *types.T,
) (tree.Datum, error) {
	switch t := expr.(type) {
	case tree.Datum:
		return t, nil
	case *tree.IndexedVar:
		d, err := e.rowEvalCtx.updatedRow.DatumAt(t.Idx)
		if err != nil {
			return nil, err
		}
		return d, nil
	default:
		v := replaceIndexVarVisitor{row: e.rowEvalCtx.updatedRow}
		newExpr, _ := tree.WalkExpr(&v, expr)
		if v.err != nil {
			return nil, v.err
		}

		typedExpr, err := tree.TypeCheck(ctx, newExpr, e.semaCtx, targetType)
		if err != nil {
			return nil, err
		}
		d, err := eval.Expr(e.evalCtx, typedExpr)
		if err != nil {
			return nil, err
		}
		return d, nil
	}
}

// cdcExprVisitor is a visitor responsible for analyzing expression to determine
// if it consists of expressions supported by CDC.
// This visitor is used early to sanity check expression.
type cdcExprVisitor struct {
	semaCtx *tree.SemaContext
	ctx     context.Context
	err     error
}

var _ tree.Visitor = (*cdcExprVisitor)(nil)

// validateExpressionForCDC runs quick checks to make sure that expr is valid for
// CDC use case.  This doesn't catch all the invalid cases, but is a place to pick up
// obviously wrong expressions.
func validateExpressionForCDC(
	ctx context.Context, expr tree.Expr, semaCtx *tree.SemaContext,
) (tree.Expr, error) {
	v := cdcExprVisitor{semaCtx: semaCtx, ctx: ctx}
	expr, _ = tree.WalkExpr(&v, expr)
	if v.err != nil {
		return nil, v.err
	}
	return expr, nil
}

// VisitPre implements tree.Visitor interface.
func (v *cdcExprVisitor) VisitPre(expr tree.Expr) (bool, tree.Expr) {
	return v.err == nil, expr
}

// VisitPost implements tree.Visitor interface.
func (v *cdcExprVisitor) VisitPost(expr tree.Expr) tree.Expr {
	switch t := expr.(type) {
	case *tree.FuncExpr:
		fn, err := checkFunctionSupported(v.ctx, t, v.semaCtx)
		if err != nil {
			v.err = err
			return expr
		}
		return fn
	case *tree.Subquery:
		v.err = pgerror.New(pgcode.FeatureNotSupported, "subquery expressions not supported by CDC")
		return expr
	default:
		return expr
	}
}

// cdcNameResolver is a visitor that resolves names in the expression
// and associates them with the EventDescriptor columns.
type cdcNameResolver struct {
	schemaexpr.NameResolutionVisitor
	*cdcevent.EventDescriptor
	err error
}

// tag errors generated by cdcNameResolver.
type cdcResolverError struct {
	error
}

func (v *cdcNameResolver) wrapError() func() {
	// NameResolutionVisitor returns "column X does not exist" error if expression references
	// column that was not configured.  This is a bit confusing for CDC since a column
	// may exist in the table, but not be available for a particular family.  So, annotate
	// the error to make it more obvious.
	// We only want to do this for errors returned by NameResolutionVisitor, and not errors
	// that we generate ourselves.
	if v.err == nil {
		return func() {
			if v.NameResolutionVisitor.Err() != nil && v.err == nil {
				v.err = errors.WithHintf(v.Err(),
					"object does not exist in table %q, family %q", v.TableName, v.FamilyName)
			}
		}
	}
	return func() {}
}

// VisitPre implements tree.Visitor interface.
func (v *cdcNameResolver) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	defer v.wrapError()()
	recurse, newExpr = v.NameResolutionVisitor.VisitPre(expr)
	return v.err == nil, newExpr
}

// VisitPost implements tree.Visitor interface.
func (v *cdcNameResolver) VisitPost(expr tree.Expr) tree.Expr {
	defer v.wrapError()()
	expr = v.NameResolutionVisitor.VisitPost(expr)

	switch t := expr.(type) {
	case *tree.AllColumnsSelector:
		// AllColumnsSelector occurs when "x.*" is used.  We have a simple 1 table support,
		// so make sure table names match.
		if t.TableName.String() != v.TableName {
			v.err = &cdcResolverError{
				error: pgerror.Newf(pgcode.UndefinedTable, "no data source matches pattern: %s", t.String()),
			}
			return t
		}
		// Now that we know table names match, turn this into unqualified star.
		return tree.UnqualifiedStar{}
	default:
		return expr
	}
}

func checkFunctionSupported(
	ctx context.Context, fnCall *tree.FuncExpr, semaCtx *tree.SemaContext,
) (*tree.FuncExpr, error) {
	var fnName string
	var fnClass tree.FunctionClass
	var fnVolatility volatility.V

	unsupportedFunctionErr := func() error {
		if fnName == "" {
			fnName = fnCall.Func.String()
		}
		return &cdcResolverError{
			error: pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fnName),
		}
	}

	funcDef, err := fnCall.Func.Resolve(ctx, semaCtx.SearchPath, semaCtx.FunctionResolver)
	if err != nil {
		return nil, unsupportedFunctionErr()
	}

	if _, isCDCFn := cdcFunctions[funcDef.Name]; isCDCFn {
		return fnCall, nil
	}

	fnClass, err = funcDef.GetClass()
	if err != nil {
		return nil, err
	}
	fnName = funcDef.Name
	if fnCall.ResolvedOverload() != nil {
		fnVolatility = fnCall.ResolvedOverload().Volatility
	} else {
		// Pick highest volatility overload.
		for i := range funcDef.Overloads {
			overload := funcDef.Overloads[i].Overload
			if overload.Volatility > fnVolatility {
				fnVolatility = overload.Volatility
			}
		}
	}

	// Aggregates, generators and window functions are not supported.
	switch fnClass {
	case tree.AggregateClass, tree.GeneratorClass, tree.WindowClass:
		return nil, unsupportedFunctionErr()
	}

	if fnVolatility <= volatility.Immutable {
		// Remaining immutable functions are safe.
		return fnCall, nil
	}

	// We have a non-immutable function -- make sure it is supported.
	_, isSafe := supportedVolatileBuiltinFunctions[fnName]
	if !isSafe {
		return nil, unsupportedFunctionErr()
	}
	return fnCall, nil
}

// rowContainer is a structure to assist with evaluation of CDC expressions.
type rowContainer struct {
	cols []cdcevent.ResultColumn
}

var _ tree.IndexedVarContainer = (*rowContainer)(nil)

// IndexedVarResolvedType implements tree.IndexedVarContainer
func (c *rowContainer) IndexedVarResolvedType(idx int) *types.T {
	return c.cols[idx].Typ
}

// IndexedVarNodeFormatter implements tree.IndexedVarContainer
func (c *rowContainer) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return nil
}

type replaceIndexVarVisitor struct {
	row cdcevent.Row
	err error
}

var _ tree.Visitor = (*replaceIndexVarVisitor)(nil)

// VisitPre implements tree.Visitor interface.
func (v *replaceIndexVarVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if iVar, ok := expr.(*tree.IndexedVar); ok {
		datum, err := v.row.DatumAt(iVar.Idx)
		if err != nil {
			v.err = pgerror.Wrapf(err, pgcode.NumericValueOutOfRange, "variable @%d out of bounds", iVar.Idx)
			return false, expr
		}
		return true, datum
	}
	return true, expr
}

// VisitPost implements tree.Visitor interface.
func (v *replaceIndexVarVisitor) VisitPost(expr tree.Expr) (newNode tree.Expr) {
	return expr
}

// cdcAnnotationAddr is the address used to store relevant information
// in the Annotation field of evalCtx when evaluating expressions.
const cdcAnnotationAddr tree.AnnotationIdx = iota + 1

// rowEvalContextFromEvalContext returns rowEvalContext stored as an annotation
// in evalCtx.
func rowEvalContextFromEvalContext(evalCtx *eval.Context) *rowEvalContext {
	return evalCtx.Annotations.Get(cdcAnnotationAddr).(*rowEvalContext)
}

const rejectInvalidCDCExprs = (tree.RejectAggregates | tree.RejectGenerators |
	tree.RejectWindowApplications | tree.RejectNestedGenerators)

// newSemaCtx returns new tree.SemaCtx configured for cdc without type resolver.
func newSemaCtx() *tree.SemaContext {
	sema := tree.MakeSemaContext()
	sema.SearchPath = &sessiondata.DefaultSearchPath
	sema.FunctionResolver = &CDCFunctionResolver{}
	sema.Properties.Require("cdc", rejectInvalidCDCExprs)
	return &sema
}

// newSemaCtxWithTypeResolver returns new tree.SemaCtx configured for cdc.
func newSemaCtxWithTypeResolver(d *cdcevent.EventDescriptor) *tree.SemaContext {
	sema := newSemaCtx()
	if d.HasUserDefinedTypes() {
		sema.TypeResolver = newTypeReferenceResolver(d)
	}
	return sema
}

// cdcTypeReferenceReesolver is responsible for resolving user defined types.
type cdcTypeReferenceResolver struct {
	byName map[string]*types.T
	byOID  map[oid.Oid]*types.T
}

var _ tree.TypeReferenceResolver = (*cdcTypeReferenceResolver)(nil)

func newTypeReferenceResolver(d *cdcevent.EventDescriptor) tree.TypeReferenceResolver {
	// Because EventDescriptor is built with hydrated table descriptors, and the
	// expression must have been normalized, we don't need to do any fancy
	// resolution; just go through user defined columns in the descriptor and
	// build the lookup maps.
	r := &cdcTypeReferenceResolver{
		byName: make(map[string]*types.T),
		byOID:  make(map[oid.Oid]*types.T),
	}

	for _, c := range d.ResultColumns() {
		if c.Typ.UserDefined() {
			r.byName[c.Typ.TypeMeta.Name.FQName()] = c.Typ
			r.byOID[c.Typ.Oid()] = c.Typ
		}
	}
	return r
}

// ResolveType implements tree.TypeReferenceResolver.
func (r *cdcTypeReferenceResolver) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	// NB: normalization step fully qualifies types, so use the full name to
	// lookup.
	if typ, found := r.byName[name.String()]; found {
		return typ, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedObject, "undefined object %s", name)
}

// ResolveTypeByOID implements tree.TypeReferenceResolver.
func (r *cdcTypeReferenceResolver) ResolveTypeByOID(
	ctx context.Context, oid oid.Oid,
) (*types.T, error) {
	if typ, found := r.byOID[oid]; found {
		return typ, nil
	}
	return nil, pgerror.Newf(pgcode.UndefinedObject, "undefined object with OID %d", oid)
}
