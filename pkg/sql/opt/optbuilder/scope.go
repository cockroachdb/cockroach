// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// scope is used for the build process and maintains the variables that have
// been bound within the current scope as columnProps. Variables bound in the
// parent scope are also visible in this scope.
//
// See builder.go for more details.
type scope struct {
	builder *Builder
	parent  *scope
	cols    []scopeColumn

	// groupby is the structure that keeps the grouping metadata when this scope
	// includes aggregate functions or GROUP BY.
	groupby *groupby

	// inAgg is true within the body of an aggregate function. inAgg is used
	// to ensure that nested aggregates are disallowed.
	// TODO(radu): this, together with some other fields below, belongs in a
	// context that is threaded through the calls instead of setting and resetting
	// it in the scope.
	inAgg bool

	// windows contains the set of window functions encountered while building
	// the current SELECT statement.
	windows []scopeColumn

	// windowDefs is the set of named window definitions present in the nearest
	// SELECT.
	windowDefs []*tree.WindowDef

	// ordering records the ORDER BY columns associated with this scope. Each
	// column is either in cols or in extraCols.
	// Must not be modified in-place after being set.
	ordering opt.Ordering

	// distinctOnCols records the DISTINCT ON columns by ID.
	distinctOnCols opt.ColSet

	// extraCols contains columns specified by the ORDER BY or DISTINCT ON clauses
	// which don't appear in cols.
	extraCols []scopeColumn

	// expr is the SQL node built with this scope.
	expr memo.RelExpr

	// Desired number of columns for subqueries found during name resolution and
	// type checking. This only applies to the top-level subqueries that are
	// anchored directly to a relational expression.
	columns int

	// If replaceSRFs is true, replace raw SRFs with an srf struct. See
	// the replaceSRF() function for more details.
	replaceSRFs bool

	// singleSRFColumn is true if this scope has a single column that comes from
	// an SRF. The flag is used to allow renaming the column to the table alias.
	singleSRFColumn bool

	// srfs contains all the SRFs that were replaced in this scope. It will be
	// used by the Builder to convert the input from the FROM clause to a lateral
	// cross join between the input and a Zip of all the srfs in this slice.
	srfs []*srf

	// ctes contains the CTEs which were created at this scope. This set
	// is not exhaustive because expressions can reference CTEs from parent
	// scopes.
	ctes map[string]*cteSource

	// context is the current context in the SQL query (e.g., "SELECT" or
	// "HAVING"). It is used for error messages and to identify scoping errors
	// (e.g., aggregates are not allowed in the FROM clause of their own query
	// level).
	context exprKind

	// atRoot is whether we are currently at a root context.
	atRoot bool
}

// exprKind is used to represent the kind of the current expression in the
// SQL query.
type exprKind int8

const (
	exprKindNone exprKind = iota
	exprKindAlterTableSplitAt
	exprKindDistinctOn
	exprKindFrom
	exprKindGroupBy
	exprKindHaving
	exprKindLateralJoin
	exprKindLimit
	exprKindOffset
	exprKindOn
	exprKindOrderBy
	exprKindReturning
	exprKindSelect
	exprKindStoreID
	exprKindValues
	exprKindWhere
	exprKindWindowFrameStart
	exprKindWindowFrameEnd
)

var exprKindName = [...]string{
	exprKindNone:              "",
	exprKindAlterTableSplitAt: "ALTER TABLE SPLIT AT",
	exprKindDistinctOn:        "DISTINCT ON",
	exprKindFrom:              "FROM",
	exprKindGroupBy:           "GROUP BY",
	exprKindHaving:            "HAVING",
	exprKindLateralJoin:       "LATERAL JOIN",
	exprKindLimit:             "LIMIT",
	exprKindOffset:            "OFFSET",
	exprKindOn:                "ON",
	exprKindOrderBy:           "ORDER BY",
	exprKindReturning:         "RETURNING",
	exprKindSelect:            "SELECT",
	exprKindStoreID:           "RELOCATE STORE ID",
	exprKindValues:            "VALUES",
	exprKindWhere:             "WHERE",
	exprKindWindowFrameStart:  "WINDOW FRAME START",
	exprKindWindowFrameEnd:    "WINDOW FRAME END",
}

func (k exprKind) String() string {
	if k < 0 || k > exprKind(len(exprKindName)-1) {
		return fmt.Sprintf("exprKind(%d)", k)
	}
	return exprKindName[k]
}

// initGrouping initializes the groupby information for this scope.
func (s *scope) initGrouping() {
	if s.groupby != nil {
		panic(errors.AssertionFailedf("grouping initialized twice"))
	}
	s.groupby = &groupby{
		aggInScope:  s.replace(),
		aggOutScope: s.replace(),
	}
}

// inGroupingContext returns true if initGrouping was called. This is the
// case when the builder is building expressions in a SELECT list, and
// aggregates, GROUP BY, or HAVING are present. This is also true when the
// builder is building expressions inside the HAVING clause. When
// inGroupingContext returns true, groupByStrSet will be utilized to enforce
// scoping rules. See the comment above groupByStrSet for more details.
func (s *scope) inGroupingContext() bool {
	return s.groupby != nil
}

// push creates a new scope with this scope as its parent.
func (s *scope) push() *scope {
	r := s.builder.allocScope()
	r.parent = s
	return r
}

// replace creates a new scope with the parent of this scope as its parent.
func (s *scope) replace() *scope {
	r := s.builder.allocScope()
	r.parent = s.parent
	return r
}

// appendColumnsFromScope adds newly bound variables to this scope.
// The expressions in the new columns are reset to nil.
func (s *scope) appendColumnsFromScope(src *scope) {
	l := len(s.cols)
	s.cols = append(s.cols, src.cols...)
	// We want to reset the expressions, as these become pass-through columns in
	// the new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

// appendOrdinaryColumnsFromTable adds all non-mutation and non-system columns from the
// given table metadata to this scope.
func (s *scope) appendOrdinaryColumnsFromTable(tabMeta *opt.TableMeta, alias *tree.TableName) {
	tab := tabMeta.Table
	if s.cols == nil {
		s.cols = make([]scopeColumn, 0, tab.ColumnCount())
	}
	for i, n := 0, tab.ColumnCount(); i < n; i++ {
		tabCol := tab.Column(i)
		if tabCol.Kind() != cat.Ordinary {
			continue
		}
		s.cols = append(s.cols, scopeColumn{
			name:       scopeColName(tabCol.ColName()),
			table:      *alias,
			typ:        tabCol.DatumType(),
			id:         tabMeta.MetaID.ColumnID(i),
			visibility: columnVisibility(tabCol.Visibility()),
		})
	}
}

// appendColumns adds newly bound variables to this scope.
// The expressions in the new columns are reset to nil.
func (s *scope) appendColumns(cols []scopeColumn) {
	l := len(s.cols)
	s.cols = append(s.cols, cols...)
	// We want to reset the expressions, as these become pass-through columns in
	// the new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

// appendColumn adds a newly bound variable to this scope.
// The expression in the new column is reset to nil.
func (s *scope) appendColumn(col *scopeColumn) {
	s.cols = append(s.cols, *col)
	// We want to reset the expression, as this becomes a pass-through column in
	// the new scope.
	s.cols[len(s.cols)-1].scalar = nil
}

// addExtraColumns adds the given columns as extra columns, ignoring any
// duplicate columns that are already in the scope.
func (s *scope) addExtraColumns(cols []scopeColumn) {
	existing := s.colSetWithExtraCols()
	for i := range cols {
		if !existing.Contains(cols[i].id) {
			s.extraCols = append(s.extraCols, cols[i])
		}
	}
}

// addColumn adds a column to scope with the given name and typed expression.
// It returns a pointer to the new column. The column ID and group are left
// empty so they can be filled in later.
func (s *scope) addColumn(name scopeColumnName, expr tree.TypedExpr) *scopeColumn {
	s.cols = append(s.cols, scopeColumn{
		name: name,
		typ:  expr.ResolvedType(),
		expr: expr,
	})
	return &s.cols[len(s.cols)-1]
}

// setOrdering sets the ordering in the physical properties and adds any new
// columns as extra columns.
func (s *scope) setOrdering(cols []scopeColumn, ord opt.Ordering) {
	s.addExtraColumns(cols)
	s.ordering = ord
}

// copyOrdering copies the ordering and the ORDER BY columns from the src scope.
// The groups in the new columns are reset to 0.
func (s *scope) copyOrdering(src *scope) {
	s.ordering = src.ordering
	if src.ordering.Empty() {
		return
	}
	// Copy any columns that the scope doesn't already have.
	existing := s.colSetWithExtraCols()
	for _, ordCol := range src.ordering {
		if !existing.Contains(ordCol.ID()) {
			col := *src.getColumn(ordCol.ID())
			// We want to reset the group, as this becomes a pass-through column in
			// the new scope.
			col.scalar = nil
			s.extraCols = append(s.extraCols, col)
		}
	}
}

// getColumn returns the scopeColumn with the given id (either in cols or
// extraCols).
func (s *scope) getColumn(col opt.ColumnID) *scopeColumn {
	for i := range s.cols {
		if s.cols[i].id == col {
			return &s.cols[i]
		}
	}
	for i := range s.extraCols {
		if s.extraCols[i].id == col {
			return &s.extraCols[i]
		}
	}
	return nil
}

// getColumnWithIDAndReferenceName returns the scopeColumn with the given id and
// reference name (either in cols or extraCols).
func (s *scope) getColumnWithIDAndReferenceName(col opt.ColumnID, refName tree.Name) *scopeColumn {
	for i := range s.cols {
		if s.cols[i].id == col && s.cols[i].name.MatchesReferenceName(refName) {
			return &s.cols[i]
		}
	}
	for i := range s.extraCols {
		if s.extraCols[i].id == col && s.cols[i].name.MatchesReferenceName(refName) {
			return &s.extraCols[i]
		}
	}
	return nil
}

// getColumnForTableOrdinal returns the column with a specific tableOrdinal
// value, or nil if it doesn't exist.
func (s *scope) getColumnForTableOrdinal(tabOrd int) *scopeColumn {
	for i := range s.cols {
		if s.cols[i].tableOrdinal == tabOrd {
			return &s.cols[i]
		}
	}
	return nil
}

func (s *scope) makeColumnTypes() []*types.T {
	res := make([]*types.T, len(s.cols))
	for i := range res {
		res[i] = s.cols[i].typ
	}
	return res
}

// makeOrderingChoice returns an OrderingChoice that corresponds to s.ordering.
func (s *scope) makeOrderingChoice() props.OrderingChoice {
	var oc props.OrderingChoice
	oc.FromOrdering(s.ordering)
	return oc
}

// makePhysicalProps constructs physical properties using the columns in the
// scope for presentation and s.ordering for required ordering. The distribution
// is determined based on the locality of the gateway node, since data must
// always be returned to the gateway.
func (s *scope) makePhysicalProps() *physical.Required {
	p := &physical.Required{
		Presentation: s.makePresentation(),
	}
	p.Ordering.FromOrdering(s.ordering)
	p.Distribution.FromLocality(s.builder.evalCtx.Locality)
	return p
}

func (s *scope) makePresentation() physical.Presentation {
	if len(s.cols) == 0 {
		return nil
	}
	presentation := make(physical.Presentation, 0, len(s.cols))
	for i := range s.cols {
		col := &s.cols[i]
		if col.visibility == visible {
			presentation = append(presentation, opt.AliasedColumn{
				Alias: string(col.name.ReferenceName()),
				ID:    col.id,
			})
		}
	}
	return presentation
}

// makePresentationWithHiddenCols is only used when constructing the
// presentation for a [ ... ]-style data source.
func (s *scope) makePresentationWithHiddenCols() physical.Presentation {
	if len(s.cols) == 0 {
		return nil
	}
	presentation := make(physical.Presentation, 0, len(s.cols))
	for i := range s.cols {
		col := &s.cols[i]
		presentation = append(presentation, opt.AliasedColumn{
			Alias: string(col.name.ReferenceName()),
			ID:    col.id,
		})
	}
	return presentation
}

// walkExprTree walks the given expression and performs name resolution,
// replaces unresolved column names with columnProps, and replaces subqueries
// with typed subquery structs.
func (s *scope) walkExprTree(expr tree.Expr) tree.Expr {
	// TODO(peter): The caller should specify the desired number of columns. This
	// is needed when a subquery is used by an UPDATE statement.
	// TODO(andy): shouldn't this be part of the desired type rather than yet
	// another parameter?
	s.columns = 1

	expr, _ = tree.WalkExpr(s, expr)
	s.builder.semaCtx.IVarContainer = s
	return expr
}

// resolveCTE looks up a CTE name in this and the parent scopes, returning nil
// if it's not found.
func (s *scope) resolveCTE(name *tree.TableName) *cteSource {
	var nameStr string
	seenCTEs := false
	for s != nil {
		if s.ctes != nil {
			// Only compute the stringified name if we see any CTEs.
			if !seenCTEs {
				nameStr = name.String()
				seenCTEs = true
			}
			if cte, ok := s.ctes[nameStr]; ok {
				if cte.onRef != nil {
					cte.onRef()
				}
				return cte
			}
		}
		s = s.parent
	}
	return nil
}

// resolveType converts the given expr to a tree.TypedExpr. As part of the
// conversion, it performs name resolution, replaces unresolved column names
// with columnProps, and replaces subqueries with typed subquery structs.
//
// The desired type is a suggestion, but resolveType does not throw an error if
// the resolved type turns out to be different from desired (in contrast to
// resolveAndRequireType, which throws an error). If the result type is
// types.Unknown, then resolveType will wrap the expression in a type cast in
// order to produce the desired type.
func (s *scope) resolveType(expr tree.Expr, desired *types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheck(s.builder.ctx, expr, s.builder.semaCtx, desired)
	if err != nil {
		panic(err)
	}
	return s.ensureNullType(texpr, desired)
}

// resolveAndRequireType converts the given expr to a tree.TypedExpr. As part
// of the conversion, it performs name resolution, replaces unresolved
// column names with columnProps, and replaces subqueries with typed subquery
// structs.
//
// If the resolved type does not match the desired type, resolveAndRequireType
// throws an error (in contrast to resolveType, which returns the typed
// expression with no error). If the result type is types.Unknown, then
// resolveType will wrap the expression in a type cast in order to produce the
// desired type.
func (s *scope) resolveAndRequireType(expr tree.Expr, desired *types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheckAndRequire(s.builder.ctx, expr, s.builder.semaCtx, desired, s.context.String())
	if err != nil {
		panic(err)
	}
	return s.ensureNullType(texpr, desired)
}

// ensureNullType tests the type of the given expression. If types.Unknown, then
// ensureNullType wraps the expression in a CAST to the desired type (assuming
// it is not types.Any). types.Unknown is a special type used for null values,
// and can be cast to any other type.
func (s *scope) ensureNullType(texpr tree.TypedExpr, desired *types.T) tree.TypedExpr {
	if desired.Family() != types.AnyFamily && texpr.ResolvedType().Family() == types.UnknownFamily {
		texpr = tree.NewTypedCastExpr(texpr, desired)
	}
	return texpr
}

// isOuterColumn returns true if the given column is not present in the current
// scope (it may or may not be present in an ancestor scope).
func (s *scope) isOuterColumn(id opt.ColumnID) bool {
	for i := range s.cols {
		col := &s.cols[i]
		if col.id == id {
			return false
		}
	}

	for i := range s.windows {
		w := &s.windows[i]
		if w.id == id {
			return false
		}
	}

	return true
}

// colSet returns a ColSet of all the columns in this scope,
// excluding extraCols.
func (s *scope) colSet() opt.ColSet {
	var colSet opt.ColSet
	for i := range s.cols {
		colSet.Add(s.cols[i].id)
	}
	return colSet
}

// colSetWithExtraCols returns a ColSet of all the columns in this scope,
// including extraCols.
func (s *scope) colSetWithExtraCols() opt.ColSet {
	colSet := s.colSet()
	for i := range s.extraCols {
		colSet.Add(s.extraCols[i].id)
	}
	return colSet
}

// colList returns a ColList of all the columns in this scope,
// excluding extraCols.
func (s *scope) colList() opt.ColList {
	colList := make(opt.ColList, len(s.cols))
	for i := range s.cols {
		colList[i] = s.cols[i].id
	}
	return colList
}

// hasSameColumns returns true if this scope has the same columns
// as the other scope.
//
// NOTE: This function is currently only called by
// Builder.constructProjectForScope, which uses it to determine whether or not
// to construct a projection. Since the projection includes the extra columns,
// this check is sufficient to determine whether or not the projection is
// necessary. Be careful if using this function for another purpose.
func (s *scope) hasSameColumns(other *scope) bool {
	return s.colSetWithExtraCols().Equals(other.colSetWithExtraCols())
}

// removeHiddenCols removes hidden columns from the scope (and moves them to
// extraCols, in case they are referenced by ORDER BY or DISTINCT ON).
func (s *scope) removeHiddenCols() {
	n := 0
	for i := range s.cols {
		if s.cols[i].visibility != visible {
			s.extraCols = append(s.extraCols, s.cols[i])
		} else {
			if n != i {
				s.cols[n] = s.cols[i]
			}
			n++
		}
	}
	s.cols = s.cols[:n]
}

// isAnonymousTable returns true if the table name of the first column
// in this scope is empty.
func (s *scope) isAnonymousTable() bool {
	return len(s.cols) > 0 && s.cols[0].table.ObjectName == ""
}

// setTableAlias qualifies the names of all columns in this scope with the
// given alias name, as if they were part of a table with that name. If the
// alias is the empty string, then setTableAlias removes any existing column
// qualifications, as if the columns were part of an "anonymous" table.
func (s *scope) setTableAlias(alias tree.Name) {
	tn := tree.MakeUnqualifiedTableName(alias)
	for i := range s.cols {
		s.cols[i].table = tn
	}
}

// See (*scope).findExistingCol.
func findExistingColInList(
	expr tree.TypedExpr, cols []scopeColumn, allowSideEffects bool, evalCtx *tree.EvalContext,
) *scopeColumn {
	exprStr := symbolicExprStr(expr)
	for i := range cols {
		col := &cols[i]
		if expr == col {
			return col
		}
		if exprStr == col.getExprStr() {
			if allowSideEffects || col.scalar == nil {
				return col
			}
			var p props.Shared
			memo.BuildSharedProps(col.scalar, &p, evalCtx)
			if !p.VolatilitySet.HasVolatile() {
				return col
			}
		}
	}
	return nil
}

// findExistingCol finds the given expression among the bound variables in this
// scope. Returns nil if the expression is not found (or an expression is found
// but it has side-effects and allowSideEffects is false).
// If a column is found and we are tracking view dependencies, we add the column
// to the view dependencies since it means this column is being referenced.
func (s *scope) findExistingCol(expr tree.TypedExpr, allowSideEffects bool) *scopeColumn {
	col := findExistingColInList(expr, s.cols, allowSideEffects, s.builder.evalCtx)
	if col != nil {
		s.builder.trackReferencedColumnForViews(col)
	}
	return col
}

// startAggFunc is called when the builder starts building an aggregate
// function. It is used to disallow nested aggregates and ensure that a
// grouping error is not called on the aggregate arguments. For example:
//   SELECT max(v) FROM kv GROUP BY k
// should not throw an error, even though v is not a grouping column.
// Non-grouping columns are allowed inside aggregate functions.
//
// startAggFunc returns a temporary scope for building the aggregate arguments.
// It is not possible to know the correct scope until the arguments are fully
// built. At that point, endAggFunc can be used to find the correct scope.
// If endAggFunc returns a different scope than startAggFunc, the columns
// will be transferred to the correct scope by buildAggregateFunction.
func (s *scope) startAggFunc() *scope {
	if s.inAgg {
		panic(sqlerrors.NewAggInAggError())
	}
	s.inAgg = true

	if s.groupby == nil {
		return s.builder.allocScope()
	}
	return s.groupby.aggInScope
}

// endAggFunc is called when the builder finishes building an aggregate
// function. It is used in combination with startAggFunc to disallow nested
// aggregates and prevent grouping errors while building aggregate arguments.
//
// In addition, endAggFunc finds the correct groupby structure, given
// that the aggregate references the columns in cols. The reference scope
// is the one closest to the current scope which contains at least one of the
// variables referenced by the aggregate (or the current scope if the aggregate
// references no variables). endAggFunc also ensures that aggregate functions
// are only used in a groupings scope.
func (s *scope) endAggFunc(cols opt.ColSet) (g *groupby) {
	if !s.inAgg {
		panic(errors.AssertionFailedf("mismatched calls to start/end aggFunc"))
	}
	s.inAgg = false

	for curr := s; curr != nil; curr = curr.parent {
		if cols.Len() == 0 || cols.Intersects(curr.colSet()) {
			curr.verifyAggregateContext()
			if curr.groupby == nil {
				curr.initGrouping()
			}
			return curr.groupby
		}
	}

	panic(errors.AssertionFailedf("aggregate function is not allowed in this context"))
}

// verifyAggregateContext checks that the current scope is allowed to contain
// aggregate functions.
func (s *scope) verifyAggregateContext() {
	if s.inAgg {
		panic(sqlerrors.NewAggInAggError())
	}

	switch s.context {
	case exprKindLateralJoin:
		panic(pgerror.Newf(pgcode.Grouping,
			"aggregate functions are not allowed in FROM clause of their own query level",
		))

	case exprKindOn:
		panic(pgerror.Newf(pgcode.Grouping,
			"aggregate functions are not allowed in JOIN conditions",
		))

	case exprKindWhere:
		panic(tree.NewInvalidFunctionUsageError(tree.AggregateClass, s.context.String()))
	}
}

// scope implements the tree.Visitor interface so that it can walk through
// a tree.Expr tree, perform name resolution, and replace unresolved column
// names with a scopeColumn. The info stored in scopeColumn is necessary for
// Builder.buildScalar to construct a "variable" memo expression.
var _ tree.Visitor = &scope{}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*scope) ColumnSourceMeta() {}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*scopeColumn) ColumnSourceMeta() {}

// ColumnResolutionResult implements the tree.ColumnResolutionResult interface.
func (*scopeColumn) ColumnResolutionResult() {}

// columnMatchClass identifies a class of column matches.
//
// We have three classes of column matches:
//  1. Anonymous source columns
//  2. Qualified source columns
//  3. Hidden columns
//
// The classes have strict precedence; within a given scope, the resolution
// outcome is determined by the first class that has at least one match. If
// there is exactly one match in that class, resolution is successful. If there
// are more matches, it is an ambiguity error.
type columnMatchClass int8

const (
	anonymousSourceMatch columnMatchClass = iota
	qualifiedSourceMatch
	hiddenMatch
)

// candidateTracker keeps track of a candidate *scopeColumn, its match class,
// and whether there is an ambiguity within that match class. Only information
// relevant to the "best" match class encountered is retained.
type candidateTracker struct {
	col        *scopeColumn
	ambiguous  bool
	matchClass columnMatchClass
}

// Add a potential candidate.
func (c *candidateTracker) Add(col *scopeColumn, matchClass columnMatchClass) {
	if c.col == nil || c.matchClass > matchClass {
		c.col = col
		c.ambiguous = false
		c.matchClass = matchClass
	} else if c.matchClass == matchClass {
		c.ambiguous = true
	}
}

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colName tree.Name,
) (prefix *tree.TableName, srcMeta colinfo.ColumnSourceMeta, colHint int, err error) {
	// We start from the current scope; if we find at least one match we are done
	// (either with a result or an ambiguity error). Otherwise, we search the
	// parent scope.

	// In case we find no matches anywhere, we remember if we saw an inaccessible
	// mutation column on the way, in which case we can present a more useful
	// error.
	reportBackfillError := false

	// We only allow hidden columns in the current scope. Hidden columns
	// in parent scopes are not accessible.
	allowHidden := true
	for ; s != nil; s, allowHidden = s.parent, false {
		var candidate candidateTracker

		for i := range s.cols {
			col := &s.cols[i]
			if !col.name.MatchesReferenceName(colName) {
				continue
			}

			switch col.visibility {
			case inaccessible:
				if col.mutation {
					reportBackfillError = true
				}

			case visible:
				if col.table.ObjectName == "" {
					candidate.Add(col, anonymousSourceMatch)
				} else {
					candidate.Add(col, qualifiedSourceMatch)
				}

			case accessibleByName, accessibleByQualifiedStar:
				if allowHidden {
					candidate.Add(col, hiddenMatch)
				}
			}
		}

		if col := candidate.col; col != nil {
			if candidate.ambiguous {
				return nil, nil, -1, s.newAmbiguousColumnError(colName, candidate.matchClass)
			}
			return &col.table, col, int(col.id), nil
		}
		// No matches in this scope; proceed to the parent scope.
	}

	// Make a copy of colName so that passing a reference to tree.ErrString does
	// not cause colName to be allocated on the heap in the happy (no error) path
	// above.
	tmpName := colName
	if reportBackfillError {
		return nil, nil, -1, makeBackfillError(tmpName)
	}
	return nil, nil, -1, colinfo.NewUndefinedColumnError(tree.ErrString(&tmpName))
}

// newAmbiguousColumnError returns an error with a helpful error message to be
// used in case of an ambiguous column reference.
func (s *scope) newAmbiguousColumnError(n tree.Name, matchClass columnMatchClass) error {
	colString := tree.ErrString(&n)
	var msgBuf bytes.Buffer
	// Search the scope for columns that match our column name and the match
	// class.
	for i := range s.cols {
		col := &s.cols[i]
		if !col.name.MatchesReferenceName(n) {
			continue
		}
		var match bool
		switch matchClass {
		case anonymousSourceMatch:
			match = (col.visibility == visible && col.table.ObjectName == "")

		case qualifiedSourceMatch:
			match = (col.visibility == visible && col.table.ObjectName != "")

		case hiddenMatch:
			match = (col.visibility == accessibleByName || col.visibility == accessibleByQualifiedStar)
		}

		if match {
			srcName := tree.ErrString(&col.table)
			if len(srcName) == 0 {
				srcName = "<anonymous>"
			}
			if msgBuf.Len() > 0 {
				msgBuf.WriteString(", ")
			}
			fmt.Fprintf(&msgBuf, "%s.%s", srcName, colString)
			if matchClass == anonymousSourceMatch {
				// All anonymous sources are identical; only print the first one.
				break
			}
		}
	}

	return pgerror.Newf(pgcode.AmbiguousColumn,
		"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String(),
	)
}

// FindSourceMatchingName is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (
	res colinfo.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta colinfo.ColumnSourceMeta,
	err error,
) {
	// If multiple sources match tn in the same scope, we return an error
	// due to ambiguity. If no sources match in the current scope, we
	// search the parent scope. If the source is not found in any of the
	// ancestor scopes, we return an error.
	var source tree.TableName
	for ; s != nil; s = s.parent {
		sources := make(map[tree.TableName]struct{})
		for i := range s.cols {
			sources[s.cols[i].table] = struct{}{}
		}

		found := false
		for src := range sources {
			if !sourceNameMatches(src, tn) {
				continue
			}
			if found {
				return colinfo.MoreThanOne, nil, s, newAmbiguousSourceError(&tn)
			}
			found = true
			source = src
		}

		if found {
			return colinfo.ExactlyOne, &source, s, nil
		}
	}

	return colinfo.NoResults, nil, s, nil
}

// sourceNameMatches checks whether a request for table name toFind
// can be satisfied by the FROM source name srcName.
//
// For example:
// - a request for "kv" is matched by a source named "db1.public.kv"
// - a request for "public.kv" is not matched by a source named just "kv"
func sourceNameMatches(srcName tree.TableName, toFind tree.TableName) bool {
	if srcName.ObjectName != toFind.ObjectName {
		return false
	}
	if toFind.ExplicitSchema {
		if srcName.SchemaName != toFind.SchemaName {
			return false
		}
		if toFind.ExplicitCatalog {
			if srcName.CatalogName != toFind.CatalogName {
				return false
			}
		}
	}
	return true
}

// Resolve is part of the tree.ColumnItemResolver interface.
func (s *scope) Resolve(
	_ context.Context,
	prefix *tree.TableName,
	srcMeta colinfo.ColumnSourceMeta,
	colHint int,
	colName tree.Name,
) (colinfo.ColumnResolutionResult, error) {
	if colHint >= 0 {
		// Column was found by FindSourceProvidingColumn above.
		return srcMeta.(*scopeColumn), nil
	}

	// Otherwise, a table is known but not the column yet.
	inScope := srcMeta.(*scope)
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if col.visibility != inaccessible &&
			col.name.MatchesReferenceName(colName) &&
			sourceNameMatches(*prefix, col.table) {
			return col, nil
		}
	}

	return nil, colinfo.NewUndefinedColumnError(tree.ErrString(tree.NewColumnItem(prefix, colName)))
}

func makeUntypedTuple(labels []string, texprs []tree.TypedExpr) *tree.Tuple {
	exprs := make(tree.Exprs, len(texprs))
	for i, e := range texprs {
		exprs[i] = e
	}
	return &tree.Tuple{Exprs: exprs, Labels: labels}
}

// VisitPre is part of the Visitor interface.
//
// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector, *tree.TupleStar:
		// AllColumnsSelectors and TupleStars at the top level of a SELECT clause
		// are replaced when the select's renders are prepared. If we
		// encounter one here during expression analysis, it's being used
		// as an argument to an inner expression/function. In that case,
		// treat it as a tuple of the expanded columns.
		//
		// Hence:
		//    SELECT kv.* FROM kv                 -> SELECT k, v FROM kv
		//    SELECT (kv.*) FROM kv               -> SELECT (k, v) FROM kv
		//    SELECT COUNT(DISTINCT kv.*) FROM kv -> SELECT COUNT(DISTINCT (k, v)) FROM kv
		//
		labels, exprs := s.builder.expandStar(expr, s)
		// We return an untyped tuple because name resolution occurs
		// before type checking, and type checking will resolve the
		// tuple's type. However we need to preserve the labels in
		// case of e.g. `SELECT (kv.*).v`.
		return false, makeUntypedTuple(labels, exprs)

	case *tree.UnresolvedName:
		vn, err := t.NormalizeVarName()
		if err != nil {
			panic(err)
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		colI, resolveErr := colinfo.ResolveColumnItem(s.builder.ctx, s, t)
		if resolveErr != nil {
			if sqlerrors.IsUndefinedColumnError(resolveErr) {
				// Attempt to resolve as columnname.*, which allows items
				// such as SELECT row_to_json(tbl_name) FROM tbl_name to work.
				return func() (bool, tree.Expr) {
					defer wrapColTupleStarPanic(resolveErr)
					return s.VisitPre(columnNameAsTupleStar(string(t.ColumnName)))
				}()
			}
			panic(resolveErr)
		}
		return false, colI.(*scopeColumn)

	case *tree.FuncExpr:
		def, err := t.Func.Resolve(s.builder.semaCtx.SearchPath)
		if err != nil {
			panic(err)
		}

		if isGenerator(def) && s.replaceSRFs {
			expr = s.replaceSRF(t, def)
			break
		}

		if isAggregate(def) && t.WindowDef == nil {
			expr = s.replaceAggregate(t, def)
			break
		}

		if t.WindowDef != nil {
			expr = s.replaceWindowFn(t, def)
			break
		}

		if isSQLFn(def) {
			expr = s.replaceSQLFn(t, def)
			break
		}

	case *tree.ArrayFlatten:
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			// Copy the ArrayFlatten expression so that the tree isn't mutated.
			copy := *t
			copy.Subquery = s.replaceSubquery(
				sub, false /* wrapInTuple */, 1 /* desiredNumColumns */, extraColsAllowed,
			)
			expr = &copy
		}

	case *tree.ComparisonExpr:
		switch t.Operator.Symbol {
		case treecmp.In, treecmp.NotIn, treecmp.Any, treecmp.Some, treecmp.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				// Copy the Comparison expression so that the tree isn't mutated.
				copy := *t
				copy.Right = s.replaceSubquery(
					sub, true /* wrapInTuple */, -1 /* desiredNumColumns */, noExtraColsAllowed,
				)
				expr = &copy
			}
		}

	case *tree.Subquery:
		if t.Exists {
			expr = s.replaceSubquery(
				t, true /* wrapInTuple */, -1 /* desiredNumColumns */, noExtraColsAllowed,
			)
		} else {
			expr = s.replaceSubquery(
				t, false /* wrapInTuple */, s.columns /* desiredNumColumns */, noExtraColsAllowed,
			)
		}
	}

	// Reset the desired number of columns since if the subquery is a child of
	// any other expression, type checking will verify the number of columns.
	s.columns = -1
	return true, expr
}

// replaceSRF returns an srf struct that can be used to replace a raw SRF. When
// this struct is encountered during the build process, it is replaced with a
// reference to the column returned by the SRF (if the SRF returns a single
// column) or a tuple of column references (if the SRF returns multiple
// columns).
//
// replaceSRF also stores a pointer to the new srf struct in this scope's srfs
// slice. The slice is used later by the Builder to convert the input from
// the FROM clause to a lateral cross join between the input and a Zip of all
// the srfs in the s.srfs slice. See Builder.buildProjectSet in srfs.go for
// more details.
func (s *scope) replaceSRF(f *tree.FuncExpr, def *tree.FunctionDefinition) *srf {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require(s.context.String(),
		tree.RejectAggregates|tree.RejectWindowApplications|tree.RejectNestedGenerators)

	expr := f.Walk(s)
	typedFunc, err := tree.TypeCheck(s.builder.ctx, expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}

	srfScope := s.push()
	var outCol *scopeColumn

	var typedFuncExpr = typedFunc.(*tree.FuncExpr)
	if s.builder.shouldCreateDefaultColumn(typedFuncExpr) {
		outCol = srfScope.addColumn(scopeColName(tree.Name(def.Name)), typedFunc)
	}
	out := s.builder.buildFunction(typedFuncExpr, s, srfScope, outCol, nil)
	srf := &srf{
		FuncExpr: typedFuncExpr,
		cols:     srfScope.cols,
		fn:       out,
	}
	s.srfs = append(s.srfs, srf)

	// Add the output columns to this scope, so the column references added
	// by the build process will not be treated as outer columns.
	s.cols = append(s.cols, srf.cols...)
	return srf
}

// isOrderedSetAggregate returns if the input function definition is an
// ordered-set aggregate, and the overridden function definition if so.
func isOrderedSetAggregate(def *tree.FunctionDefinition) (*tree.FunctionDefinition, bool) {
	// The impl functions are private because they should never be run directly.
	// Thus, they need to be marked as non-private before using them.
	switch def {
	case tree.FunDefs["percentile_disc"]:
		newDef := *tree.FunDefs["percentile_disc_impl"]
		newDef.Private = false
		return &newDef, true
	case tree.FunDefs["percentile_cont"]:
		newDef := *tree.FunDefs["percentile_cont_impl"]
		newDef.Private = false
		return &newDef, true
	}
	return def, false
}

// replaceAggregate returns an aggregateInfo that can be used to replace a raw
// aggregate function. When an aggregateInfo is encountered during the build
// process, it is replaced with a reference to the column returned by the
// aggregation.
//
// replaceAggregate also stores the aggregateInfo in the aggregation scope for
// this aggregate, using the aggOutScope.groupby.aggs slice. The aggregation
// scope is the one closest to the current scope which contains at least one of
// the variables referenced by the aggregate (or the current scope if the
// aggregate references no variables). The aggOutScope.groupby.aggs slice is
// used later by the Builder to build aggregations in the aggregation scope.
func (s *scope) replaceAggregate(f *tree.FuncExpr, def *tree.FunctionDefinition) tree.Expr {
	f, def = s.replaceCount(f, def)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require("aggregate",
		tree.RejectNestedAggregates|tree.RejectWindowApplications|tree.RejectGenerators)

	// Make a copy of f so we can modify it if needed.
	fCopy := *f
	// Override ordered-set aggregates to use their impl counterparts.
	if orderedSetDef, found := isOrderedSetAggregate(def); found {
		// Ensure that the aggregation is well formed.
		if f.AggType != tree.OrderedSetAgg || len(f.OrderBy) != 1 {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"ordered-set aggregations must have a WITHIN GROUP clause containing one ORDER BY column"))
		}

		// Override function definition.
		def = orderedSetDef
		fCopy.Func.FunctionReference = orderedSetDef

		// Copy Exprs slice.
		oldExprs := f.Exprs
		fCopy.Exprs = make(tree.Exprs, len(oldExprs))
		copy(fCopy.Exprs, oldExprs)

		// Add implicit column to the input expressions.
		fCopy.Exprs = append(fCopy.Exprs, s.resolveType(fCopy.OrderBy[0].Expr, types.Any))
	}

	expr := fCopy.Walk(s)

	// Update this scope to indicate that we are now inside an aggregate function
	// so that any nested aggregates referencing this scope from a subquery will
	// return an appropriate error. The returned tempScope will be used for
	// building aggregate function arguments below in buildAggregateFunction.
	tempScope := s.startAggFunc()

	// We need to do this check here to ensure that we check the usage of special
	// functions with the right error message.
	if f.Filter != nil {
		func() {
			oldProps := s.builder.semaCtx.Properties
			defer func() { s.builder.semaCtx.Properties.Restore(oldProps) }()

			s.builder.semaCtx.Properties.Require("FILTER", tree.RejectSpecial)
			_, err := tree.TypeCheck(s.builder.ctx, expr.(*tree.FuncExpr).Filter, s.builder.semaCtx, types.Any)
			if err != nil {
				panic(err)
			}
		}()
	}

	typedFunc, err := tree.TypeCheck(s.builder.ctx, expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}
	if typedFunc == tree.DNull {
		return tree.DNull
	}

	f = typedFunc.(*tree.FuncExpr)

	private := memo.FunctionPrivate{
		Name:       def.Name,
		Properties: &def.FunctionProperties,
		Overload:   f.ResolvedOverload(),
	}

	return s.builder.buildAggregateFunction(f, &private, tempScope, s)
}

func (s *scope) lookupWindowDef(name tree.Name) *tree.WindowDef {
	for i := range s.windowDefs {
		if s.windowDefs[i].Name == name {
			return s.windowDefs[i]
		}
	}
	panic(pgerror.Newf(pgcode.UndefinedObject, "window %q does not exist", name))
}

func (s *scope) constructWindowDef(def tree.WindowDef) tree.WindowDef {
	switch {
	case def.RefName != "":
		// SELECT rank() OVER (w) FROM t WINDOW w AS (...)
		// We copy the referenced window specification, and modify it if necessary.
		result, err := tree.OverrideWindowDef(s.lookupWindowDef(def.RefName), def)
		if err != nil {
			panic(err)
		}
		return result

	case def.Name != "":
		// SELECT rank() OVER w FROM t WINDOW w AS (...)
		// Note the lack of parens around w, compared to the first case.
		// We use the referenced window specification directly, without modification.
		return *s.lookupWindowDef(def.Name)

	default:
		return def
	}
}

func (s *scope) replaceWindowFn(f *tree.FuncExpr, def *tree.FunctionDefinition) tree.Expr {
	f, def = s.replaceCount(f, def)

	if err := tree.CheckIsWindowOrAgg(def); err != nil {
		panic(err)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require("window",
		tree.RejectNestedWindowFunctions)

	// Make a copy of f so we can modify the WindowDef.
	fCopy := *f
	newWindowDef := s.constructWindowDef(*f.WindowDef)
	fCopy.WindowDef = &newWindowDef

	expr := fCopy.Walk(s)

	typedFunc, err := tree.TypeCheck(s.builder.ctx, expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}
	if typedFunc == tree.DNull {
		return tree.DNull
	}

	f = typedFunc.(*tree.FuncExpr)

	// We will be performing type checking on expressions from PARTITION BY and
	// ORDER BY clauses below, and we need the semantic context to know that we
	// are in a window function. InWindowFunc is updated when type checking
	// FuncExpr above, but it is reset upon returning from that, so we need to do
	// this update manually.
	defer func(ctx *tree.SemaContext, prevWindow bool) {
		ctx.Properties.Derived.InWindowFunc = prevWindow
	}(
		s.builder.semaCtx,
		s.builder.semaCtx.Properties.Derived.InWindowFunc,
	)
	s.builder.semaCtx.Properties.Derived.InWindowFunc = true

	oldPartitions := f.WindowDef.Partitions
	f.WindowDef.Partitions = make(tree.Exprs, len(oldPartitions))
	for i, e := range oldPartitions {
		typedExpr := s.resolveType(e, types.Any)
		f.WindowDef.Partitions[i] = typedExpr
	}

	oldOrderBy := f.WindowDef.OrderBy
	f.WindowDef.OrderBy = make(tree.OrderBy, len(oldOrderBy))
	for i := range oldOrderBy {
		ord := *oldOrderBy[i]
		if ord.OrderType != tree.OrderByColumn {
			panic(errOrderByIndexInWindow)
		}
		typedExpr := s.resolveType(ord.Expr, types.Any)
		ord.Expr = typedExpr
		f.WindowDef.OrderBy[i] = &ord
	}

	if f.WindowDef.Frame != nil {
		if err := analyzeWindowFrame(s, f.WindowDef); err != nil {
			panic(err)
		}
	}

	info := windowInfo{
		FuncExpr: f,
		def: memo.FunctionPrivate{
			Name:       def.Name,
			Properties: &def.FunctionProperties,
			Overload:   f.ResolvedOverload(),
		},
	}

	if col := findExistingColInList(
		&info,
		s.windows,
		false, /* allowSideEffects */
		s.builder.evalCtx,
	); col != nil {
		return col.expr
	}

	info.col = &scopeColumn{
		name: scopeColName(tree.Name(def.Name)),
		typ:  f.ResolvedType(),
		id:   s.builder.factory.Metadata().AddColumn(def.Name, f.ResolvedType()),
		expr: &info,
	}

	s.windows = append(s.windows, *info.col)

	return &info
}

// replaceSQLFn replaces a tree.SQLClass function with a sqlFnInfo struct. See
// comments above tree.SQLClass and sqlFnInfo for details.
func (s *scope) replaceSQLFn(f *tree.FuncExpr, def *tree.FunctionDefinition) tree.Expr {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require("SQL function", tree.RejectSpecial)

	expr := f.Walk(s)
	typedFunc, err := tree.TypeCheck(s.builder.ctx, expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(err)
	}
	if typedFunc == tree.DNull {
		return tree.DNull
	}

	f = typedFunc.(*tree.FuncExpr)
	args := make(memo.ScalarListExpr, len(f.Exprs))
	for i, arg := range f.Exprs {
		args[i] = s.builder.buildScalar(arg.(tree.TypedExpr), s, nil, nil, nil)
	}

	info := sqlFnInfo{
		FuncExpr: f,
		def: memo.FunctionPrivate{
			Name:       def.Name,
			Properties: &def.FunctionProperties,
			Overload:   f.ResolvedOverload(),
		},
		args: args,
	}
	return &info
}

var (
	errOrderByIndexInWindow = pgerror.New(pgcode.FeatureNotSupported, "ORDER BY INDEX in window definition is not supported")
)

// analyzeWindowFrame performs semantic analysis of offset expressions of
// the window frame.
func analyzeWindowFrame(s *scope, windowDef *tree.WindowDef) error {
	frame := windowDef.Frame
	bounds := frame.Bounds
	startBound, endBound := bounds.StartBound, bounds.EndBound
	var requiredType *types.T
	switch frame.Mode {
	case treewindow.ROWS:
		// In ROWS mode, offsets must be non-null, non-negative integers. Non-nullity
		// and non-negativity will be checked later.
		requiredType = types.Int
	case treewindow.RANGE:
		// In RANGE mode, offsets must be non-null and non-negative datums of a type
		// dependent on the type of the ordering column. Non-nullity and
		// non-negativity will be checked later.
		if bounds.HasOffset() {
			// At least one of the bounds is of type 'value' PRECEDING or 'value' FOLLOWING.
			// We require ordering on a single column that supports addition/subtraction.
			if len(windowDef.OrderBy) != 1 {
				return pgerror.Newf(pgcode.Windowing,
					"RANGE with offset PRECEDING/FOLLOWING requires exactly one ORDER BY column")
			}
			requiredType = windowDef.OrderBy[0].Expr.(tree.TypedExpr).ResolvedType()
			if !types.IsAdditiveType(requiredType) {
				return pgerror.Newf(pgcode.Windowing,
					"RANGE with offset PRECEDING/FOLLOWING is not supported for column type %s",
					redact.Safe(requiredType))
			}
			if types.IsDateTimeType(requiredType) {
				// Spec: for datetime ordering columns, the required type is an 'interval'.
				requiredType = types.Interval
			}
		}
	case treewindow.GROUPS:
		if len(windowDef.OrderBy) == 0 {
			return pgerror.Newf(pgcode.Windowing, "GROUPS mode requires an ORDER BY clause")
		}
		// In GROUPS mode, offsets must be non-null, non-negative integers.
		// Non-nullity and non-negativity will be checked later.
		requiredType = types.Int
	default:
		return errors.AssertionFailedf("unexpected WindowFrameMode: %d", errors.Safe(frame.Mode))
	}
	if startBound != nil && startBound.OffsetExpr != nil {
		oldContext := s.context
		s.context = exprKindWindowFrameStart
		startBound.OffsetExpr = s.resolveAndRequireType(startBound.OffsetExpr, requiredType)
		s.context = oldContext
	}
	if endBound != nil && endBound.OffsetExpr != nil {
		oldContext := s.context
		s.context = exprKindWindowFrameEnd
		endBound.OffsetExpr = s.resolveAndRequireType(endBound.OffsetExpr, requiredType)
		s.context = oldContext
	}
	return nil
}

// replaceCount replaces count(*) with count_rows().
func (s *scope) replaceCount(
	f *tree.FuncExpr, def *tree.FunctionDefinition,
) (*tree.FuncExpr, *tree.FunctionDefinition) {
	if len(f.Exprs) != 1 {
		return f, def
	}
	vn, ok := f.Exprs[0].(tree.VarName)
	if !ok {
		return f, def
	}
	vn, err := vn.NormalizeVarName()
	if err != nil {
		panic(err)
	}
	f.Exprs[0] = vn

	if strings.EqualFold(def.Name, "count") && f.Type == 0 {
		if _, ok := vn.(tree.UnqualifiedStar); ok {
			if f.Filter != nil {
				// If we have a COUNT(*) with a FILTER, we need to synthesize an input
				// for the aggregation to be over, because otherwise we have no input
				// to hang the AggFilter off of.
				// Thus, we convert
				//   COUNT(*) FILTER (WHERE foo)
				// to
				//   COUNT(true) FILTER (WHERE foo).
				cpy := *f
				e := &cpy
				e.Exprs = tree.Exprs{tree.DBoolTrue}

				newDef, err := e.Func.Resolve(s.builder.semaCtx.SearchPath)
				if err != nil {
					panic(err)
				}

				return e, newDef
			}

			// Special case handling for COUNT(*) with no FILTER. This is a special
			// construct to count the number of rows; in this case * does NOT refer
			// to a set of columns. A * is invalid elsewhere (and will be caught by
			// TypeCheck()).  Replace the function with COUNT_ROWS (which doesn't
			// take any arguments).
			e := &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: &tree.UnresolvedName{
						NumParts: 1, Parts: tree.NameParts{"count_rows"},
					},
				},
			}
			// We call TypeCheck to fill in FuncExpr internals. This is a fixed
			// expression; we should not hit an error here.
			semaCtx := tree.MakeSemaContext()
			if _, err := e.TypeCheck(s.builder.ctx, &semaCtx, types.Any); err != nil {
				panic(err)
			}
			newDef, err := e.Func.Resolve(s.builder.semaCtx.SearchPath)
			if err != nil {
				panic(err)
			}
			e.Filter = f.Filter
			e.WindowDef = f.WindowDef
			return e, newDef
		}
		// TODO(rytaft): Add handling for tree.AllColumnsSelector to support
		// expressions like SELECT COUNT(kv.*) FROM kv
		// Similar to the work done in PR #17833.
	}

	return f, def
}

const (
	extraColsAllowed   = true
	noExtraColsAllowed = false
)

// Replace a raw tree.Subquery node with a lazily typed subquery. wrapInTuple
// specifies whether the return type of the subquery should be wrapped in a
// tuple. wrapInTuple is true for subqueries that may return multiple rows in
// comparison expressions (e.g., IN, ANY, ALL) and EXISTS expressions.
// desiredNumColumns specifies the desired number of columns for the subquery.
// Specifying -1 for desiredNumColumns allows the subquery to return any
// number of columns and is used when the normal type checking machinery will
// verify that the correct number of columns is returned.
// If extraColsAllowed is true, extra columns built from the subquery (such as
// columns for which orderings have been requested) will not be stripped away.
// It is the duty of the caller to ensure that those columns are eventually
// dealt with.
func (s *scope) replaceSubquery(
	sub *tree.Subquery, wrapInTuple bool, desiredNumColumns int, extraColsAllowed bool,
) *subquery {
	return &subquery{
		Subquery:          sub,
		wrapInTuple:       wrapInTuple,
		desiredNumColumns: desiredNumColumns,
		extraColsAllowed:  extraColsAllowed,
		scope:             s,
	}
}

// VisitPost is part of the Visitor interface.
func (*scope) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// scope implements the IndexedVarContainer interface so it can be used as
// semaCtx.IVarContainer. This allows tree.TypeCheck to determine the correct
// type for any IndexedVars.
var _ tree.IndexedVarContainer = &scope{}

// IndexedVarEval is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	panic(errors.AssertionFailedf("unimplemented: scope.IndexedVarEval"))
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarResolvedType(idx int) *types.T {
	if idx >= len(s.cols) {
		if len(s.cols) == 0 {
			panic(pgerror.Newf(pgcode.UndefinedColumn,
				"column reference @%d not allowed in this context", idx+1))
		}
		panic(pgerror.Newf(pgcode.UndefinedColumn,
			"invalid column ordinal: @%d", idx+1))
	}
	return s.cols[idx].typ
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	panic(errors.AssertionFailedf("unimplemented: scope.IndexedVarNodeFormatter"))
}

// newAmbiguousSourceError returns an error with a helpful error message to be
// used in case of an ambiguous table name.
func newAmbiguousSourceError(tn *tree.TableName) error {
	if tn.Catalog() == "" {
		return pgerror.Newf(pgcode.AmbiguousAlias,
			"ambiguous source name: %q", tree.ErrString(tn))

	}
	return pgerror.Newf(pgcode.AmbiguousAlias,
		"ambiguous source name: %q (within database %q)",
		tree.ErrString(&tn.ObjectName), tree.ErrString(&tn.CatalogName))
}

func (s *scope) String() string {
	var buf bytes.Buffer

	if s.parent != nil {
		buf.WriteString(s.parent.String())
		buf.WriteString("->")
	}

	buf.WriteByte('(')
	for i, c := range s.cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%s:%d", c.name.ReferenceName(), c.id)
	}
	for i, c := range s.extraCols {
		if i > 0 || len(s.cols) > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "%s:%d!extra", c.name.ReferenceName(), c.id)
	}
	buf.WriteByte(')')

	return buf.String()
}

func columnNameAsTupleStar(colName string) *tree.TupleStar {
	return &tree.TupleStar{
		Expr: &tree.UnresolvedName{
			Star:     true,
			NumParts: 2,
			Parts:    tree.NameParts{"", colName},
		},
	}
}

// wrapColTupleStarPanic checks for panics and if the pgcode is
// UndefinedTable panics with the originalError.
// Otherwise, it will panic with the recovered error.
func wrapColTupleStarPanic(originalError error) {
	if r := recover(); r != nil {
		if err, ok := r.(error); ok {
			if sqlerrors.IsUndefinedRelationError(err) {
				panic(originalError)
			}
		}
		panic(r)
	}
}
