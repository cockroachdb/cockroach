// Copyright 2018 The Cockroach Authors.
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

package optbuilder

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
	groupby groupby

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

	// srfs contains all the SRFs that were replaced in this scope. It will be
	// used by the Builder to convert the input from the FROM clause to a lateral
	// cross join between the input and a Zip of all the srfs in this slice.
	srfs []*srf

	// ctes contains the CTEs which were created at this scope. This set
	// is not exhaustive because expressions can reference CTEs from parent
	// scopes.
	ctes map[string]*cteSource

	// context is the current context in the SQL query (e.g., "SELECT" or
	// "HAVING"). It is used for error messages.
	context string
}

// cteSource represents a CTE in the given query.
type cteSource struct {
	name tree.AliasClause
	cols []scopeColumn
	expr memo.RelExpr

	// used tracks if this CTE has been referenced.  We are currently limited
	// to only having a single reference to a given CTE, so if this is set then
	// this CTE has already been referenced and may not be referenced again.
	used bool
}

// groupByStrSet is a set of stringified GROUP BY expressions that map to the
// grouping column in an aggOutScope scope that projects that expression. It
// is used to enforce scoping rules, since any non-aggregate, variable
// expression in the SELECT list must be a GROUP BY expression or be composed
// of GROUP BY expressions. For example, this query is legal:
//
//   SELECT COUNT(*), k + v FROM kv GROUP by k, v
//
// but this query is not:
//
//   SELECT COUNT(*), k + v FROM kv GROUP BY k - v
//
type groupByStrSet map[string]*scopeColumn

// exists is a 0-byte dummy value used in a map that's being used to track
// whether keys exist (i.e. where only the key matters).
var exists = struct{}{}

// inGroupingContext returns true when the aggInScope is not nil. This is the
// case when the builder is building expressions in a SELECT list, and
// aggregates, GROUP BY, or HAVING are present. This is also true when the
// builder is building expressions inside the HAVING clause. When
// inGroupingContext returns true, groupByStrSet will be utilized to enforce
// scoping rules. See the comment above groupByStrSet for more details.
func (s *scope) inGroupingContext() bool {
	return s.groupby.aggInScope != nil
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
// The groups in the new columns are reset to 0.
func (s *scope) appendColumnsFromScope(src *scope) {
	l := len(s.cols)
	s.cols = append(s.cols, src.cols...)
	// We want to reset the groups, as these become pass-through columns in the
	// new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

// appendColumns adds newly bound variables to this scope.
// The groups in the new columns are reset to 0.
func (s *scope) appendColumns(cols []scopeColumn) {
	l := len(s.cols)
	s.cols = append(s.cols, cols...)
	// We want to reset the groups, as these become pass-through columns in the
	// new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].scalar = nil
	}
}

// addExtraColumns adds the given columns as extra columns, ignoring any
// duplicate columns that are already in the scope.
func (s *scope) addExtraColumns(cols []scopeColumn) {
	existing := s.colSetWithExtraCols()
	for i := range cols {
		if !existing.Contains(int(cols[i].id)) {
			s.extraCols = append(s.extraCols, cols[i])
		}
	}
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
		if !existing.Contains(int(ordCol.ID())) {
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

// makeOrderingChoice returns an OrderingChoice that corresponds to s.ordering.
func (s *scope) makeOrderingChoice() physical.OrderingChoice {
	var oc physical.OrderingChoice
	oc.FromOrdering(s.ordering)
	return oc
}

// makePhysicalProps constructs physical properties using the columns in the
// scope for presentation and s.ordering for required ordering.
func (s *scope) makePhysicalProps() *physical.Required {
	p := &physical.Required{}

	if len(s.cols) > 0 {
		p.Presentation = make(physical.Presentation, 0, len(s.cols))
		for i := range s.cols {
			col := &s.cols[i]
			if !col.hidden {
				p.Presentation = append(p.Presentation, opt.AliasedColumn{
					Alias: string(col.name),
					ID:    col.id,
				})
			}
		}
	}

	p.Ordering.FromOrdering(s.ordering)
	return p
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
// resolveAndRequireType, which panics with a builderError). If the result
// type is types.Unknown, then resolveType will wrap the expression in a type
// cast in order to produce the desired type.
func (s *scope) resolveType(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheck(expr, s.builder.semaCtx, desired)
	if err != nil {
		panic(builderError{err})
	}
	return s.ensureNullType(texpr, desired)
}

// resolveAndRequireType converts the given expr to a tree.TypedExpr. As part
// of the conversion, it performs name resolution, replaces unresolved
// column names with columnProps, and replaces subqueries with typed subquery
// structs.
//
// If the resolved type does not match the desired type, resolveAndRequireType
// panics with a builderError (in contrast to resolveType, which returns the
// typed expression with no error). If the result type is types.Unknown, then
// resolveType will wrap the expression in a type cast in order to produce the
// desired type.
func (s *scope) resolveAndRequireType(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheckAndRequire(expr, s.builder.semaCtx, desired, s.context)
	if err != nil {
		panic(builderError{err})
	}
	return s.ensureNullType(texpr, desired)
}

// ensureNullType tests the type of the given expression. If types.Unknown, then
// ensureNullType wraps the expression in a CAST to the desired type (assuming
// it is not types.Any). types.Unknown is a special type used for null values,
// and can be cast to any other type.
func (s *scope) ensureNullType(texpr tree.TypedExpr, desired types.T) tree.TypedExpr {
	if desired != types.Any && texpr.ResolvedType() == types.Unknown {
		// Should always be able to convert null value to any other type.
		colType, err := coltypes.DatumTypeToColumnType(desired)
		if err != nil {
			panic(err)
		}
		texpr, err = tree.NewTypedCastExpr(texpr, colType)
		if err != nil {
			panic(err)
		}
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

	return true
}

// colSet returns a ColSet of all the columns in this scope,
// excluding orderByCols.
func (s *scope) colSet() opt.ColSet {
	var colSet opt.ColSet
	for i := range s.cols {
		colSet.Add(int(s.cols[i].id))
	}
	return colSet
}

// colSetWithExtraCols returns a ColSet of all the columns in this scope,
// including extraCols.
func (s *scope) colSetWithExtraCols() opt.ColSet {
	colSet := s.colSet()
	for i := range s.extraCols {
		colSet.Add(int(s.extraCols[i].id))
	}
	return colSet
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

// removeHiddenCols removes hidden columns from the scope.
func (s *scope) removeHiddenCols() {
	n := 0
	for i := range s.cols {
		if !s.cols[i].hidden {
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
	return len(s.cols) > 0 && s.cols[0].table.TableName == ""
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

// findExistingCol finds the given expression among the bound variables
// in this scope. Returns nil if the expression is not found.
func (s *scope) findExistingCol(expr tree.TypedExpr) *scopeColumn {
	exprStr := symbolicExprStr(expr)
	for i := range s.cols {
		col := &s.cols[i]
		if expr == col || exprStr == col.getExprStr() {
			return col
		}
	}
	return nil
}

// getAggregateCols returns the columns in this scope corresponding
// to aggregate functions. This call is only valid on an aggOutScope.
func (s *scope) getAggregateCols() []scopeColumn {
	// Aggregates are always clustered at the beginning of the column list, in
	// the same order as s.groupby.aggs.
	return s.cols[:len(s.groupby.aggs)]
}

// getAggregateArgCols returns the columns in this scope corresponding
// to arguments to aggregate functions. This call is only valid on an
// aggInScope.
func (s *scope) getAggregateArgCols(groupingsLen int) []scopeColumn {
	// Aggregate args are always clustered at the beginning of the column list.
	return s.cols[:len(s.cols)-groupingsLen]
}

// getGroupingCols returns the columns in this scope corresponding
// to grouping columns. This call is valid on an aggInScope or aggOutScope.
func (s *scope) getGroupingCols(groupingsLen int) []scopeColumn {
	// Grouping cols are always clustered at the end of the column list.
	return s.cols[len(s.cols)-groupingsLen:]
}

// hasAggregates returns true if this scope contains aggregate functions.
func (s *scope) hasAggregates() bool {
	aggOutScope := s.groupby.aggOutScope
	return aggOutScope != nil && len(aggOutScope.groupby.aggs) > 0
}

// findAggregate finds the given aggregate among the bound variables
// in this scope. Returns nil if the aggregate is not found.
func (s *scope) findAggregate(agg aggregateInfo) *scopeColumn {
	if s.groupby.aggs == nil {
		return nil
	}

	for i, a := range s.groupby.aggs {
		// Find an existing aggregate that uses the same function overload.
		if a.def.Overload == agg.def.Overload && a.distinct == agg.distinct {
			// Now check that the arguments are identical.
			if len(a.args) == len(agg.args) {
				match := true
				for j, arg := range a.args {
					if arg != agg.args[j] {
						match = false
						break
					}
				}
				if match {
					// Aggregate already exists, so return information about the
					// existing column that computes it.
					return &s.getAggregateCols()[i]
				}
			}
		}
	}

	return nil
}

// startAggFunc is called when the builder starts building an aggregate
// function. It is used to disallow nested aggregates and ensure that a
// grouping error is not called on the aggregate arguments. For example:
//   SELECT max(v) FROM kv GROUP BY k
// should mot throw an error, even though v is not a grouping column.
// Non-grouping columns are allowed inside aggregate functions.
//
// startAggFunc returns a temporary scope for building the aggregate arguments.
// It is not possible to know the correct scope until the arguments are fully
// built. At that point, endAggFunc can be used to find the correct scope.
// If endAggFunc returns a different scope than startAggFunc, the columns
// will be transferred to the correct scope by buildAggregateFunction.
func (s *scope) startAggFunc() *scope {
	if s.groupby.inAgg {
		panic(builderError{sqlbase.NewAggInAggError()})
	}
	s.groupby.inAgg = true

	if s.groupby.aggInScope == nil {
		return s.builder.allocScope()
	}
	return s.groupby.aggInScope
}

// endAggFunc is called when the builder finishes building an aggregate
// function. It is used in combination with startAggFunc to disallow nested
// aggregates and prevent grouping errors while building aggregate arguments.
//
// In addition, endAggFunc finds the correct aggInScope and aggOutScope, given
// that the aggregate references the columns in cols. The reference scope
// is the one closest to the current scope which contains at least one of the
// variables referenced by the aggregate (or the current scope if the aggregate
// references no variables). endAggFunc also ensures that aggregate functions
// are only used in a groupings scope.
func (s *scope) endAggFunc(cols opt.ColSet) (aggInScope, aggOutScope *scope) {
	if !s.groupby.inAgg {
		panic(fmt.Errorf("mismatched calls to start/end aggFunc"))
	}
	s.groupby.inAgg = false

	for curr := s; curr != nil; curr = curr.parent {
		if cols.Len() == 0 || cols.Intersects(curr.colSet()) {
			if curr.groupby.aggInScope == nil {
				curr.groupby.aggInScope = curr.replace()
			}
			if curr.groupby.aggOutScope == nil {
				curr.groupby.aggOutScope = curr.replace()
			}
			return curr.groupby.aggInScope, curr.groupby.aggOutScope
		}
	}

	panic(fmt.Errorf("aggregate function is not allowed in this context"))
}

// startBuildingGroupingCols is called when the builder starts building the
// grouping columns. It is used to ensure that a grouping error is not called
// prematurely. For example:
//   SELECT count(*), k FROM kv GROUP BY k
// is legal, but
//   SELECT count(*), v FROM kv GROUP BY k
// will throw the error, `column "v" must appear in the GROUP BY clause or be
// used in an aggregate function`. The builder cannot know whether there is
// a grouping error until the grouping columns are fully built.
func (s *scope) startBuildingGroupingCols() {
	s.groupby.buildingGroupingCols = true
}

// endBuildingGroupingCols is called when the builder finishes building the
// grouping columns. It is used in combination with startBuildingGroupingCols
// to ensure that a grouping error is not called prematurely.
func (s *scope) endBuildingGroupingCols() {
	if !s.groupby.buildingGroupingCols {
		panic(fmt.Errorf("mismatched calls to start/end groupings"))
	}
	s.groupby.buildingGroupingCols = false
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

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colName tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	var candidateFromAnonSource *scopeColumn
	var candidateWithPrefix *scopeColumn
	var hiddenCandidate *scopeColumn
	var moreThanOneCandidateFromAnonSource bool
	var moreThanOneCandidateWithPrefix bool
	var moreThanOneHiddenCandidate bool

	// We only allow hidden columns in the current scope. Hidden columns
	// in parent scopes are not accessible.
	allowHidden := true

	// If multiple columns match c in the same scope, we return an error
	// due to ambiguity. If no columns match in the current scope, we
	// search the parent scope. If the column is not found in any of the
	// ancestor scopes, we return an error.
	reportBackfillError := false
	for ; s != nil; s, allowHidden = s.parent, false {
		for i := range s.cols {
			col := &s.cols[i]
			if col.name != colName {
				continue
			}

			// If the matching column is a mutation column, then act as if it's not
			// present so that matches in higher scopes can be found. However, if
			// no match is found in higher scopes, report a backfill error rather
			// than a "not found" error.
			if col.mutation {
				reportBackfillError = true
				continue
			}

			if col.table.TableName == "" && !col.hidden {
				if candidateFromAnonSource != nil {
					moreThanOneCandidateFromAnonSource = true
					break
				}
				candidateFromAnonSource = col
			} else if !col.hidden {
				if candidateWithPrefix != nil {
					moreThanOneCandidateWithPrefix = true
				}
				candidateWithPrefix = col
			} else if allowHidden {
				if hiddenCandidate != nil {
					moreThanOneHiddenCandidate = true
				}
				hiddenCandidate = col
			}
		}

		// The table name was unqualified, so if a single anonymous source exists
		// with a matching non-hidden column, use that.
		if moreThanOneCandidateFromAnonSource {
			return nil, nil, -1, s.newAmbiguousColumnError(
				colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
			)
		}
		if candidateFromAnonSource != nil {
			return &candidateFromAnonSource.table, candidateFromAnonSource, int(candidateFromAnonSource.id), nil
		}

		// Else if a single named source exists with a matching non-hidden column,
		// use that.
		if candidateWithPrefix != nil && !moreThanOneCandidateWithPrefix {
			return &candidateWithPrefix.table, candidateWithPrefix, int(candidateWithPrefix.id), nil
		}
		if moreThanOneCandidateWithPrefix || moreThanOneHiddenCandidate {
			return nil, nil, -1, s.newAmbiguousColumnError(
				colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
			)
		}

		// One last option: if a single source exists with a matching hidden
		// column, use that.
		if hiddenCandidate != nil {
			return &hiddenCandidate.table, hiddenCandidate, int(hiddenCandidate.id), nil
		}
	}

	// Make a copy of colName so that passing a reference to tree.ErrString does
	// not cause colName to be allocated on the heap in the happy (no error) path
	// above.
	tmpName := colName
	if reportBackfillError {
		return nil, nil, -1, makeBackfillError(tmpName)
	}
	return nil, nil, -1, sqlbase.NewUndefinedColumnError(tree.ErrString(&tmpName))
}

// FindSourceMatchingName is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (
	res tree.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
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
			sources[s.cols[i].table] = exists
		}

		found := false
		for src := range sources {
			if !sourceNameMatches(src, tn) {
				continue
			}
			if found {
				return tree.MoreThanOne, nil, s, newAmbiguousSourceError(&tn)
			}
			found = true
			source = src
		}

		if found {
			return tree.ExactlyOne, &source, s, nil
		}
	}

	return tree.NoResults, nil, s, nil
}

// sourceNameMatches checks whether a request for table name toFind
// can be satisfied by the FROM source name srcName.
//
// For example:
// - a request for "kv" is matched by a source named "db1.public.kv"
// - a request for "public.kv" is not matched by a source named just "kv"
func sourceNameMatches(srcName tree.TableName, toFind tree.TableName) bool {
	if srcName.TableName != toFind.TableName {
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
	srcMeta tree.ColumnSourceMeta,
	colHint int,
	colName tree.Name,
) (tree.ColumnResolutionResult, error) {
	if colHint >= 0 {
		// Column was found by FindSourceProvidingColumn above.
		return srcMeta.(*scopeColumn), nil
	}

	// Otherwise, a table is known but not the column yet.
	inScope := srcMeta.(*scope)
	for i := range inScope.cols {
		col := &inScope.cols[i]
		if col.name == colName && sourceNameMatches(*prefix, col.table) {
			return col, nil
		}
	}

	return nil, sqlbase.NewUndefinedColumnError(tree.ErrString(tree.NewColumnItem(prefix, colName)))
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
			panic(builderError{err})
		}
		return s.VisitPre(vn)

	case *tree.ColumnItem:
		colI, err := t.Resolve(s.builder.ctx, s)
		if err != nil {
			panic(builderError{err})
		}
		return false, colI.(*scopeColumn)

	case *tree.FuncExpr:
		if t.WindowDef != nil {
			panic(unimplementedf("window functions are not supported"))
		}

		def, err := t.Func.Resolve(s.builder.semaCtx.SearchPath)
		if err != nil {
			panic(builderError{err})
		}

		if isGenerator(def) && s.replaceSRFs {
			expr = s.replaceSRF(t, def)
			break
		}

		if isAggregate(def) {
			expr = s.replaceAggregate(t, def)
			break
		}

	case *tree.ArrayFlatten:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171 and #24170.
			break
		}

		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			// Copy the ArrayFlatten expression so that the tree isn't mutated.
			copy := *t
			copy.Subquery = s.replaceSubquery(sub, false /* wrapInTuple */, 1 /* desiredColumns */, extraColsAllowed)
			expr = &copy
		}

	case *tree.ComparisonExpr:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171 and #24170.
			break
		}

		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				// Copy the Comparison expression so that the tree isn't mutated.
				copy := *t
				copy.Right = s.replaceSubquery(sub, true /* wrapInTuple */, -1 /* desiredColumns */, noExtraColsAllowed)
				expr = &copy
			}
		}

	case *tree.Subquery:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171, #24170 and #24225.
			return false, expr
		}

		if t.Exists {
			expr = s.replaceSubquery(t, true /* wrapInTuple */, -1 /* desiredColumns */, noExtraColsAllowed)
		} else {
			expr = s.replaceSubquery(t, false /* wrapInTuple */, s.columns /* desiredColumns */, noExtraColsAllowed)
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
// the srfs in the s.srfs slice. See Builder.constructProjectSet in srfs.go for
// more details.
func (s *scope) replaceSRF(f *tree.FuncExpr, def *tree.FunctionDefinition) *srf {
	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require(s.context,
		tree.RejectAggregates|tree.RejectWindowApplications|tree.RejectNestedGenerators)

	expr := f.Walk(s)
	typedFunc, err := tree.TypeCheck(expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(builderError{err})
	}

	srfScope := s.push()
	var outCol *scopeColumn
	if len(def.ReturnLabels) == 1 {
		outCol = s.builder.addColumn(srfScope, def.Name, typedFunc)
	}
	out := s.builder.buildFunction(typedFunc.(*tree.FuncExpr), s, srfScope, outCol, nil)
	srf := &srf{
		FuncExpr: typedFunc.(*tree.FuncExpr),
		cols:     srfScope.cols,
		fn:       out,
	}
	s.srfs = append(s.srfs, srf)

	// Add the output columns to this scope, so the column references added
	// by the build process will not be treated as outer columns.
	s.cols = append(s.cols, srf.cols...)
	return srf
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
	if f.Filter != nil {
		panic(unimplementedf("aggregates with FILTER are not supported yet"))
	}

	f, def = s.replaceCount(f, def)

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called within a subquery
	// context.
	defer s.builder.semaCtx.Properties.Restore(s.builder.semaCtx.Properties)

	s.builder.semaCtx.Properties.Require(s.context,
		tree.RejectNestedAggregates|tree.RejectWindowApplications)

	expr := f.Walk(s)
	typedFunc, err := tree.TypeCheck(expr, s.builder.semaCtx, types.Any)
	if err != nil {
		panic(builderError{err})
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

	return s.builder.buildAggregateFunction(f, &private, s)
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
		panic(builderError{err})
	}
	f.Exprs[0] = vn

	if strings.EqualFold(def.Name, "count") && f.Type == 0 {
		if _, ok := vn.(tree.UnqualifiedStar); ok {
			// Special case handling for COUNT(*). This is a special construct to
			// count the number of rows; in this case * does NOT refer to a set of
			// columns. A * is invalid elsewhere (and will be caught by TypeCheck()).
			// Replace the function with COUNT_ROWS (which doesn't take any
			// arguments).
			e := &tree.FuncExpr{
				Func: tree.ResolvableFunctionReference{
					FunctionReference: &tree.UnresolvedName{
						NumParts: 1, Parts: tree.NameParts{"count_rows"},
					},
				},
			}
			// We call TypeCheck to fill in FuncExpr internals. This is a fixed
			// expression; we should not hit an error here.
			if _, err := e.TypeCheck(&tree.SemaContext{}, types.Any); err != nil {
				panic(builderError{err})
			}
			newDef, err := e.Func.Resolve(s.builder.semaCtx.SearchPath)
			if err != nil {
				panic(builderError{err})
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

// Replace a raw subquery node with a typed subquery. wrapInTuple specifies
// whether the return type of the subquery should be wrapped in a tuple.
// wrapInTuple is true for subqueries that may return multiple rows in
// comparison expressions (e.g., IN, ANY, ALL) and EXISTS expressions.
// desiredColumns specifies the desired number of columns for the
// subquery. Specifying -1 for desiredColumns allows the subquery to return any
// number of columns and is used when the normal type checking machinery will
// verify that the correct number of columns is returned.
// If extraColsAllowed is true, extra columns built from the subquery (such as
// columns for which orderings have been requested) will not be stripped away.
// It is the duty of the caller to ensure that those columns are eventually
// dealt with.
func (s *scope) replaceSubquery(
	sub *tree.Subquery, wrapInTuple bool, desiredColumns int, extraColsAllowed bool,
) *subquery {
	if s.replaceSRFs {
		// We need to save and restore the previous value of the replaceSRFs field in
		// case we are recursively called within a subquery context.
		defer func() { s.replaceSRFs = true }()
		s.replaceSRFs = false
	}

	subq := subquery{
		Subquery:    sub,
		wrapInTuple: wrapInTuple,
	}

	// Save and restore the previous value of s.builder.subquery in case we are
	// recursively called within a subquery context.
	outer := s.builder.subquery
	defer func() { s.builder.subquery = outer }()
	s.builder.subquery = &subq

	outScope := s.builder.buildStmt(sub.Select, s)
	ord := outScope.ordering

	// Treat the subquery result as an anonymous data source (i.e. column names
	// are not qualified). Remove hidden columns, as they are not accessible
	// outside the subquery.
	outScope.setTableAlias("")
	outScope.removeHiddenCols()

	if desiredColumns > 0 && len(outScope.cols) != desiredColumns {
		n := len(outScope.cols)
		switch desiredColumns {
		case 1:
			panic(builderError{pgerror.NewErrorf(pgerror.CodeSyntaxError,
				"subquery must return only one column, found %d", n)})
		default:
			panic(builderError{pgerror.NewErrorf(pgerror.CodeSyntaxError,
				"subquery must return %d columns, found %d", desiredColumns, n)})
		}
	}

	if len(outScope.extraCols) > 0 && !extraColsAllowed {
		// We need to add a projection to remove the extra columns.
		projScope := outScope.push()
		projScope.appendColumnsFromScope(outScope)
		projScope.expr = s.builder.constructProject(outScope.expr.(memo.RelExpr), projScope.cols)
		outScope = projScope
	}

	subq.cols = outScope.cols
	subq.node = outScope.expr.(memo.RelExpr)
	subq.ordering = ord
	return &subq
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
	panic("unimplemented: scope.IndexedVarEval")
}

// IndexedVarResolvedType is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarResolvedType(idx int) types.T {
	if idx >= len(s.cols) {
		if len(s.cols) == 0 {
			panic(builderError{pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
				"column reference @%d not allowed in this context", idx+1)})
		}
		panic(builderError{pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
			"invalid column ordinal: @%d", idx+1)})
	}
	return s.cols[idx].typ
}

// IndexedVarNodeFormatter is part of the IndexedVarContainer interface.
func (s *scope) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	panic("unimplemented: scope.IndexedVarNodeFormatter")
}

// newAmbiguousColumnError returns an error with a helpful error message to be
// used in case of an ambiguous column reference.
func (s *scope) newAmbiguousColumnError(
	n tree.Name,
	allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate bool,
) error {
	colString := tree.ErrString(&n)
	var msgBuf bytes.Buffer
	sep := ""
	fmtCandidate := func(tn tree.TableName) {
		name := tree.ErrString(&tn)
		if len(name) == 0 {
			name = "<anonymous>"
		}
		fmt.Fprintf(&msgBuf, "%s%s.%s", sep, name, colString)
		sep = ", "
	}
	for i := range s.cols {
		col := &s.cols[i]
		if col.name == n && (allowHidden || !col.hidden) {
			if col.table.TableName == "" && !col.hidden {
				if moreThanOneCandidateFromAnonSource {
					// Only print first anonymous source, since other(s) are identical.
					fmtCandidate(col.table)
					break
				}
			} else if !col.hidden {
				if moreThanOneCandidateWithPrefix && !moreThanOneCandidateFromAnonSource {
					fmtCandidate(col.table)
				}
			} else {
				if moreThanOneHiddenCandidate && !moreThanOneCandidateWithPrefix && !moreThanOneCandidateFromAnonSource {
					fmtCandidate(col.table)
				}
			}
		}
	}

	return pgerror.NewErrorf(pgerror.CodeAmbiguousColumnError,
		"column reference %q is ambiguous (candidates: %s)", colString, msgBuf.String(),
	)
}

// newAmbiguousSourceError returns an error with a helpful error message to be
// used in case of an ambiguous table name.
func newAmbiguousSourceError(tn *tree.TableName) error {
	if tn.Catalog() == "" {
		return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
			"ambiguous source name: %q", tree.ErrString(tn))

	}
	return pgerror.NewErrorf(pgerror.CodeAmbiguousAliasError,
		"ambiguous source name: %q (within database %q)",
		tree.ErrString(&tn.TableName), tree.ErrString(&tn.CatalogName))
}
