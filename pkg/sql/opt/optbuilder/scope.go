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
	"context"
	"strings"

	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
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
	builder  *Builder
	parent   *scope
	cols     []columnProps
	groupby  groupby
	ordering opt.Ordering
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

// resolveType converts the given expr to a tree.TypedExpr. As part of the
// conversion, it performs name resolution and replaces unresolved column names
// with columnProps.
func (s *scope) resolveType(expr tree.Expr, desired types.T) tree.TypedExpr {
	expr, _ = tree.WalkExpr(s, expr)
	s.builder.semaCtx.IVarContainer = s
	texpr, err := tree.TypeCheck(expr, &s.builder.semaCtx, desired)
	if err != nil {
		panic(builderError{err})
	}

	return texpr
}

// hasColumn returns true if the given column index is found within this scope.
func (s *scope) hasColumn(index opt.ColumnIndex) bool {
	for curr := s; curr != nil; curr = curr.parent {
		for i := range curr.cols {
			col := &curr.cols[i]
			if col.index == index {
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
		if s.cols[i].index != other.cols[i].index {
			return false
		}
	}
	return true
}

// getAggregateCols returns the columns in this scope corresponding
// to aggregate functions.
func (s *scope) getAggregateCols() []columnProps {
	// Aggregates are always clustered at the end of the column list, in the
	// same order as s.groupby.aggs.
	return s.cols[len(s.cols)-len(s.groupby.aggs):]
}

// findAggregate finds the given aggregate among the bound variables
// in this scope. Returns nil if the aggregate is not found.
func (s *scope) findAggregate(agg aggregateInfo) *columnProps {
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
func (s *scope) findGrouping(grouping opt.GroupID) *columnProps {
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
// names with a columnProps. The info stored in columnProps is necessary for
// Builder.buildScalar to construct a "variable" memo expression.
var _ tree.Visitor = &scope{}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*scope) ColumnSourceMeta() {}

// ColumnSourceMeta implements the tree.ColumnSourceMeta interface.
func (*columnProps) ColumnSourceMeta() {}

// ColumnResolutionResult implements the tree.ColumnResolutionResult interface.
func (*columnProps) ColumnResolutionResult() {}

// FindSourceProvidingColumn is part of the tree.ColumnItemResolver interface.
func (s *scope) FindSourceProvidingColumn(
	_ context.Context, colName tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	var candidateFromAnonSource *columnProps
	var candidateWithPrefix *columnProps
	var hiddenCandidate *columnProps
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
			return &candidateFromAnonSource.table, candidateFromAnonSource, int(candidateFromAnonSource.index), nil
		}

		// Else if a single named source exists with a matching non-hidden column,
		// use that.
		if candidateWithPrefix != nil && !moreThanOneCandidateWithPrefix {
			return &candidateWithPrefix.table, candidateWithPrefix, int(candidateWithPrefix.index), nil
		}
		if moreThanOneCandidateWithPrefix || moreThanOneHiddenCandidate {
			return nil, nil, -1, s.newAmbiguousColumnError(
				&colName, allowHidden, moreThanOneCandidateFromAnonSource, moreThanOneCandidateWithPrefix, moreThanOneHiddenCandidate,
			)
		}

		// One last option: if a single source exists with a matching hidden
		// column, use that.
		if hiddenCandidate != nil {
			return &hiddenCandidate.table, hiddenCandidate, int(hiddenCandidate.index), nil
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
		return srcMeta.(*columnProps), nil
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

// VisitPre is part of the Visitor interface.
//
// NB: This code is adapted from sql/select_name_resolution.go and
// sql/subquery.go.
func (s *scope) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
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
		return false, colI.(*columnProps)

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

		// TODO(rytaft): Implement subquery replacement.
	}

	return true, expr
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
