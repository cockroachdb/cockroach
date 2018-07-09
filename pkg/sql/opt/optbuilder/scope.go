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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
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
	builder       *Builder
	parent        *scope
	cols          []scopeColumn
	groupby       groupby
	physicalProps props.Physical

	// orderByCols contains all the columns specified by the ORDER BY clause.
	// There may be some overlap with the columns in cols.
	orderByCols []scopeColumn

	// group is the memo.GroupID of the relational operator built with this scope.
	group memo.GroupID

	// Desired number of columns for subqueries found during name resolution and
	// type checking. This only applies to the top-level subqueries that are
	// anchored directly to a relational expression.
	columns int

	// This is a temporary flag, which is currently used to allow generator
	// functions in the FROM clause, but not in the SELECT list.
	allowGeneratorFunc bool
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
	return &scope{builder: s.builder, parent: s}
}

// replace creates a new scope with the parent of this scope as its parent.
func (s *scope) replace() *scope {
	return &scope{builder: s.builder, parent: s.parent}
}

// appendColumns adds newly bound variables to this scope.
// The groups in the new columns are reset to 0.
func (s *scope) appendColumns(src *scope) {
	l := len(s.cols)
	s.cols = append(s.cols, src.cols...)
	// We want to reset the groups, as these become pass-through columns in the
	// new scope.
	for i := l; i < len(s.cols); i++ {
		s.cols[i].group = 0
	}
}

// appendColumn adds a new column to the scope with an optional new label.
// It returns a pointer to the new column.  The group in the new column is reset
// to 0.
func (s *scope) appendColumn(col *scopeColumn, label string) *scopeColumn {
	s.cols = append(s.cols, *col)
	newCol := &s.cols[len(s.cols)-1]
	// We want to reset the group, as this becomes a pass-through column in the
	// new scope.
	newCol.group = 0
	if label != "" {
		newCol.name = tree.Name(label)
	}
	return newCol
}

// copyPhysicalProps copies the physicalProps from the src scope to this scope.
func (s *scope) copyPhysicalProps(src *scope) {
	s.physicalProps.Presentation = src.physicalProps.Presentation
	s.copyOrdering(src)
}

// copyOrdering copies the ordering and orderByCols from the src scope to this
// scope. The groups in the new columns are reset to 0.
func (s *scope) copyOrdering(src *scope) {
	s.physicalProps.Ordering = src.physicalProps.Ordering

	l := len(s.orderByCols)
	s.orderByCols = append(s.orderByCols, src.orderByCols...)
	// We want to reset the groups, as these become pass-through columns in the
	// new scope.
	for i := l; i < len(s.orderByCols); i++ {
		s.orderByCols[i].group = 0
	}
}

// setPresentation sets s.physicalProps.Presentation (if not already set).
func (s *scope) setPresentation() {
	if s.physicalProps.Presentation != nil {
		return
	}
	presentation := make(props.Presentation, 0, len(s.cols))
	for i := range s.cols {
		col := &s.cols[i]
		if !col.hidden {
			presentation = append(presentation, opt.LabeledColumn{Label: string(col.name), ID: col.id})
		}
	}
	s.physicalProps.Presentation = presentation
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

// resolveType converts the given expr to a tree.TypedExpr. As part of the
// conversion, it performs name resolution, replaces unresolved column names
// with columnProps, and replaces subqueries with typed subquery structs.
//
// The desired type is a suggestion, but resolveType does not throw an error if
// the resolved type turns out to be different from desired (in contrast to
// resolveAndRequireType, which panics with a builderError).
func (s *scope) resolveType(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheck(expr, s.builder.semaCtx, desired)
	if err != nil {
		panic(builderError{err})
	}

	return texpr
}

// resolveAndRequireType converts the given expr to a tree.TypedExpr. As part
// of the conversion, it performs name resolution, replaces unresolved
// column names with columnProps, and replaces subqueries with typed subquery
// structs.
//
// If the resolved type does not match the desired type, resolveAndRequireType
// panics with a builderError (in contrast to resolveType, which returns the
// typed expression with no error).
//
// typingContext is a string used for error reporting in case the resolved
// type and desired type do not match. It shows the context in which
// this function was called (e.g., "LIMIT", "OFFSET").
func (s *scope) resolveAndRequireType(
	expr tree.Expr, desired types.T, typingContext string,
) tree.TypedExpr {
	expr = s.walkExprTree(expr)
	texpr, err := tree.TypeCheckAndRequire(expr, s.builder.semaCtx, desired, typingContext)
	if err != nil {
		panic(builderError{err})
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

// hasColumn returns true if the given column id is found within this scope.
func (s *scope) hasColumn(id opt.ColumnID) bool {
	// We only allow hidden columns in the current scope. Hidden columns
	// in parent scopes are not accessible.
	allowHidden := true

	for curr := s; curr != nil; curr, allowHidden = curr.parent, false {
		for i := range curr.cols {
			col := &curr.cols[i]
			if col.id == id && (allowHidden || !col.hidden) {
				return true
			}
		}
	}

	return false
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

// colSetWithOrderBy returns a ColSet of all the columns in this scope,
// including orderByCols.
func (s *scope) colSetWithOrderBy() opt.ColSet {
	var colSet opt.ColSet
	for i := range s.cols {
		colSet.Add(int(s.cols[i].id))
	}
	for i := range s.orderByCols {
		colSet.Add(int(s.orderByCols[i].id))
	}
	return colSet
}

// hasSameColumns returns true if this scope has the same columns
// as the other scope.
//
// NOTE: This function is currently only called by
// Builder.constructProjectForScope, which uses it to determine whether or not
// to construct a projection. Since the projection includes all the order by
// columns, this check is sufficient to determine whether or not the projection
// is necessary. Be careful if using this function for another purpose.
func (s *scope) hasSameColumns(other *scope) bool {
	return s.colSetWithOrderBy().Equals(other.colSetWithOrderBy())
}

// hasExtraOrderByCols returns true if there are some ORDER BY columns in
// s.orderByCols that are not included in s.cols.
func (s *scope) hasExtraOrderByCols() bool {
	if len(s.orderByCols) > 0 {
		cols := s.colSet()
		for _, c := range s.orderByCols {
			if !cols.Contains(int(c.id)) {
				return true
			}
		}
	}
	return false
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
// to aggregate functions.
func (s *scope) getAggregateCols() []scopeColumn {
	// Aggregates are always clustered at the end of the column list, in the
	// same order as s.groupby.aggs.
	return s.cols[len(s.cols)-len(s.groupby.aggs):]
}

// findAggregate finds the given aggregate among the bound variables
// in this scope. Returns nil if the aggregate is not found.
func (s *scope) findAggregate(agg aggregateInfo) *scopeColumn {
	for i, a := range s.groupby.aggs {
		// Find an existing aggregate that has the same function and the same
		// arguments.
		if a.def == agg.def && len(a.args) == len(agg.args) {
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

	return nil
}

// startAggFunc is called when the builder starts building an aggregate
// function. It is used to disallow nested aggregates and ensure that aggregate
// functions are only used in a groupings scope.
func (s *scope) startAggFunc() (aggInScope *scope, aggOutScope *scope) {
	for curr := s; curr != nil; curr = curr.parent {
		if curr.groupby.inAgg {
			panic(builderError{sqlbase.NewAggInAggError()})
		}

		if curr.groupby.aggInScope != nil {
			// The aggregate will be added to the innermost groupings scope.
			s.groupby.inAgg = true
			return curr.groupby.aggInScope, curr.groupby.aggOutScope
		}
	}

	panic(fmt.Errorf("aggregate function is not allowed in this context"))
}

// endAggFunc is called when the builder finishes building an aggregate
// function. It is used in combination with startAggFunc to disallow nested
// aggregates and ensure that aggregate functions are only used in a groupings
// scope. It returns the reference scope to which the new aggregate should be
// added.
func (s *scope) endAggFunc() {
	if !s.groupby.inAgg {
		panic(fmt.Errorf("mismatched calls to start/end aggFunc"))
	}
	s.groupby.inAgg = false
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
	for ; s != nil; s, allowHidden = s.parent, false {
		for i := range s.cols {
			col := &s.cols[i]
			// TODO(rytaft): Do not return a match if this column is being
			// backfilled, or the column expression being resolved is not from
			// a selector column expression from an UPDATE/DELETE.
			if col.name == colName {
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
		}

		// The table name was unqualified, so if a single anonymous source exists
		// with a matching non-hidden column, use that.
		if moreThanOneCandidateFromAnonSource {
			return nil, nil, -1, s.newAmbiguousColumnError(
				&colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
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
				&colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
			)
		}

		// One last option: if a single source exists with a matching hidden
		// column, use that.
		if hiddenCandidate != nil {
			return &hiddenCandidate.table, hiddenCandidate, int(hiddenCandidate.id), nil
		}
	}

	return nil, nil, -1, sqlbase.NewUndefinedColumnError(tree.ErrString(&colName))
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
		for _, col := range s.cols {
			sources[col.table] = exists
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
	case *tree.TupleStar:
		// TupleStars at the top level of a SELECT clause are replaced
		// when the select's renders are prepared. If we encounter one
		// here during expression analysis, it's being used as an argument
		// to an inner expression. In that case, we just report its tuple
		// operand unchanged.
		return true, t.Expr

	case *tree.AllColumnsSelector:
		// AllColumnsSelectors at the top level of a SELECT clause are
		// replaced when the select's renders are prepared. If we
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
		def, err := t.Func.Resolve(s.builder.semaCtx.SearchPath)
		if err != nil {
			panic(builderError{err})
		}
		if !s.allowGeneratorFunc && isGenerator(def) {
			panic(unimplementedf("generator functions are not supported"))
		}

		// Disallow nested SRFs. This field is currently set in Builder.buildZip().
		// TODO(rytaft): This is a temporary solution and will need to change once
		// we support SRFs in the SELECT list.
		s.allowGeneratorFunc = false

		if len(t.Exprs) != 1 {
			break
		}
		vn, ok := t.Exprs[0].(tree.VarName)
		if !ok {
			break
		}
		vn, err = vn.NormalizeVarName()
		if err != nil {
			panic(builderError{err})
		}
		t.Exprs[0] = vn

		if strings.EqualFold(def.Name, "count") && t.Type == 0 {
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
				e.Filter = t.Filter
				e.WindowDef = t.WindowDef
				return true, e
			}
			// TODO(rytaft): Add handling for tree.AllColumnsSelector to support
			// expressions like SELECT COUNT(kv.*) FROM kv
			// Similar to the work done in PR #17833.
		}

	case *tree.ArrayFlatten:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171 and #24170.
			break
		}

		// TODO(peter): the ARRAY flatten operator requires a single column from
		// the subquery.
		if sub, ok := t.Subquery.(*tree.Subquery); ok {
			// Copy the ArrayFlatten expression so that the tree isn't mutated.
			copy := *t
			copy.Subquery = s.replaceSubquery(sub, true /* multi-row */, 1 /* desired-columns */)
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
				copy.Right = s.replaceSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
				expr = &copy
			}
		}

	case *tree.Subquery:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171, #24170 and #24225.
			return false, expr
		}

		if t.Exists {
			expr = s.replaceSubquery(t, true /* multi-row */, -1 /* desired-columns */)
		} else {
			expr = s.replaceSubquery(t, false /* multi-row */, s.columns /* desired-columns */)
		}
	}

	// Reset the desired number of columns since if the subquery is a child of
	// any other expression, type checking will verify the number of columns.
	s.columns = -1
	return true, expr
}

// Replace a raw subquery node with a typed subquery. multiRow specifies
// whether the subquery is occurring in a single-row or multi-row
// context. desiredColumns specifies the desired number of columns for the
// subquery. Specifying -1 for desiredColumns allows the subquery to return any
// number of columns and is used when the normal type checking machinery will
// verify that the correct number of columns is returned.
func (s *scope) replaceSubquery(sub *tree.Subquery, multiRow bool, desiredColumns int) *subquery {
	outScope := s.builder.buildStmt(sub.Select, s)

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

	if outScope.hasExtraOrderByCols() {
		// We need to add a projection to remove the ORDER BY columns.
		projScope := outScope.push()
		projScope.appendColumns(outScope)
		projScope.group = s.builder.constructProject(outScope.group, projScope.cols)
		outScope = projScope
	}

	return &subquery{
		cols:     outScope.cols,
		group:    outScope.group,
		multiRow: multiRow,
		expr:     sub,
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
	n *tree.Name,
	allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate bool,
) error {
	colString := tree.ErrString(n)
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
		if col.name == *n && (allowHidden || !col.hidden) {
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
