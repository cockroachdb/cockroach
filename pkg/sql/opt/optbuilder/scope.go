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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
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
	physicalProps memo.PhysicalProps

	// group is the memo.GroupID of the relational operator built with this scope.
	group memo.GroupID

	// Desired number of columns for subqueries found during name resolution and
	// type checking. This only applies to the top-level subqueries that are
	// anchored directly to a relational expression.
	columns int
}

// groupByStrSet is a set of stringified GROUP BY expressions.
type groupByStrSet map[string]struct{}

// exists is the 0-byte value of each element in groupByStrSet.
var exists = struct{}{}

// inGroupingContext returns true when the aggInScope is not nil. This is the
// case when the builder is building expressions in a SELECT list, and
// aggregates, GROUP BY, or HAVING are present. This is also true when the
// builder is building expressions inside the HAVING clause. When
// inGroupingContext returns true, varsUsed will be utilized to enforce scoping
// rules. See the comment above varsUsed for more details.
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
func (s *scope) appendColumns(src *scope) {
	s.cols = append(s.cols, src.cols...)
}

// appendColumn adds a new column to the scope with an optional new label.
// It returns a pointer to the new column.
func (s *scope) appendColumn(col *scopeColumn, label string) *scopeColumn {
	s.cols = append(s.cols, *col)
	newCol := &s.cols[len(s.cols)-1]
	if label != "" {
		newCol.name = tree.Name(label)
	}
	return newCol
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

// hasSameColumns returns true if this scope has the same columns
// as the other scope (in the same order).
func (s *scope) hasSameColumns(other *scope) bool {
	if len(s.cols) != len(other.cols) {
		return false
	}
	for i := range s.cols {
		if s.cols[i].id != other.cols[i].id {
			return false
		}
	}
	return true
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

// findExistingCol finds the given expression among the bound variables
// in this scope. Returns nil if the expression is not found.
func (s *scope) findExistingCol(expr tree.TypedExpr) *scopeColumn {
	exprStr := symbolicExprStr(expr)
	for i := range s.cols {
		col := &s.cols[i]
		if exprStr == col.getExprStr() {
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

// findGrouping finds the given grouping expression among the bound variables
// in the groupingsScope. Returns nil if the grouping is not found.
func (s *scope) findGrouping(grouping memo.GroupID) *scopeColumn {
	for i, g := range s.groupby.groupings {
		if g == grouping {
			// Grouping already exists, so return information about the
			// existing column that computes it. The first columns in aggInScope
			// are always listed in the same order as s.groupby.groupings.
			return &s.groupby.aggInScope.cols[i]
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
			panic(errorf("aggregate function cannot be nested within another aggregate function"))
		}

		if curr.groupby.aggInScope != nil {
			// The aggregate will be added to the innermost groupings scope.
			s.groupby.inAgg = true
			return curr.groupby.aggInScope, curr.groupby.aggOutScope
		}
	}

	panic(errorf("aggregate function is not allowed in this context"))
}

// endAggFunc is called when the builder finishes building an aggregate
// function. It is used in combination with startAggFunc to disallow nested
// aggregates and ensure that aggregate functions are only used in a groupings
// scope. It returns the reference scope to which the new aggregate should be
// added.
func (s *scope) endAggFunc() {
	if !s.groupby.inAgg {
		panic(errorf("mismatched calls to start/end aggFunc"))
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

	return nil, nil, -1, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
		"column name %q not found", tree.ErrString(&colName))
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
	sources := make(map[tree.TableName]struct{})
	for _, col := range s.cols {
		sources[col.table] = exists
	}

	found := false
	var source tree.TableName
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

	if !found {
		return tree.NoResults, nil, s, nil
	}
	return tree.ExactlyOne, &source, s, nil
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
		if !srcName.ExplicitSchema || srcName.SchemaName != toFind.SchemaName {
			return false
		}
		if toFind.ExplicitCatalog {
			if !srcName.ExplicitCatalog || srcName.CatalogName != toFind.CatalogName {
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
		col := &s.cols[i]
		if col.name == colName && sourceNameMatches(col.table, *prefix) {
			return col, nil
		}
	}

	return nil, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
		"column name %q not found", tree.ErrString(tree.NewColumnItem(prefix, colName)))
}

func makeUntypedTuple(texprs []tree.TypedExpr) *tree.Tuple {
	exprs := make(tree.Exprs, len(texprs))
	for i, e := range texprs {
		exprs[i] = e
	}
	return &tree.Tuple{Exprs: exprs}
}

// VisitPre is part of the Visitor interface.
//
// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.AllColumnsSelector:
		// AllColumnsSelector at the top level of a SELECT clause are
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
		exprs := s.builder.expandStar(expr, s)
		return false, makeUntypedTuple(exprs)

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
			t.Subquery = s.replaceSubquery(sub, true /* multi-row */, 1 /* desired-columns */)
		}

	case *tree.ComparisonExpr:
		if s.builder.AllowUnsupportedExpr {
			// TODO(rytaft): Temporary fix for #24171 and #24170.
			break
		}

		switch t.Operator {
		case tree.In, tree.NotIn, tree.Any, tree.Some, tree.All:
			if sub, ok := t.Right.(*tree.Subquery); ok {
				t.Right = s.replaceSubquery(sub, true /* multi-row */, -1 /* desired-columns */)
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
	if desiredColumns > 0 && len(outScope.cols) != desiredColumns {
		n := len(outScope.cols)
		switch desiredColumns {
		case 1:
			panic(errorf("subquery must return only one column, found %d", n))
		default:
			panic(errorf("subquery must return %d columns, found %d", desiredColumns, n))
		}
	}

	return &subquery{
		cols:     outScope.cols,
		group:    outScope.group,
		multiRow: multiRow,
		exists:   sub.Exists,
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
					fmtCandidate(col.table)
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
