// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// joinPredicate implements the predicate logic for joins.
type joinPredicate struct {
	joinType sqlbase.JoinType

	// numLeft/RightCols are the number of columns in the left and right
	// operands.
	numLeftCols, numRightCols int

	// left/rightEqualityIndices give the position of equality columns
	// on the left and right input row arrays, respectively.
	// Left/right columns that have an equality constraint in the ON
	// condition also have their indices appended when tryAddEqualityFilter
	// is invoked (see planner.addJoinFilter).
	// Only columns with the same left and right value types can be equality
	// columns.
	leftEqualityIndices  []int
	rightEqualityIndices []int

	// The list of names for the columns listed in leftEqualityIndices.
	// Used mainly for pretty-printing.
	leftColNames tree.NameList
	// The list of names for the columns listed in rightEqualityIndices.
	// Used mainly for pretty-printing.
	rightColNames tree.NameList

	// For ON predicates or joins with an added filter expression,
	// we need an IndexedVarHelper, the DataSourceInfo, a row buffer
	// and the expression itself.
	iVarHelper tree.IndexedVarHelper
	info       *sqlbase.DataSourceInfo
	curRow     tree.Datums
	// The ON condition that needs to be evaluated (in addition to the
	// equality columns).
	onCond tree.TypedExpr

	// This struct must be allocated on the heap and its location stay
	// stable after construction because it implements
	// IndexedVarContainer and the IndexedVar objects in sub-expressions
	// will link to it by reference after checkRenderStar / analyzeExpr.
	// Enforce this using NoCopy.
	noCopy util.NoCopy
}

// tryAddEqualityFilter attempts to turn the given filter expression into
// an equality predicate. It returns true iff the transformation succeeds.
func (p *joinPredicate) tryAddEqualityFilter(
	filter tree.Expr, left, right *sqlbase.DataSourceInfo,
) bool {
	c, ok := filter.(*tree.ComparisonExpr)
	if !ok || c.Operator != tree.EQ {
		return false
	}
	lhs, ok := c.Left.(*tree.IndexedVar)
	if !ok {
		return false
	}
	rhs, ok := c.Right.(*tree.IndexedVar)
	if !ok {
		return false
	}

	sourceBoundary := len(left.SourceColumns)
	if (lhs.Idx >= sourceBoundary && rhs.Idx >= sourceBoundary) ||
		(lhs.Idx < sourceBoundary && rhs.Idx < sourceBoundary) {
		// Both variables are on the same side of the join (e.g. `a JOIN b ON a.x = a.y`).
		return false
	}

	if lhs.Idx > rhs.Idx {
		lhs, rhs = rhs, lhs
	}

	// At this point we have an equality, so we can add it to the list
	// of equality columns.

	// To do this we must be a bit careful: the expression contains
	// IndexedVars, and the column indices at this point will refer to
	// the full column set of the joinPredicate, including the
	// merged columns.
	leftColIdx := lhs.Idx
	rightColIdx := rhs.Idx - len(left.SourceColumns)

	// Also, we will want to avoid redundant equality checks.
	for i := range p.leftEqualityIndices {
		if p.leftEqualityIndices[i] == leftColIdx && p.rightEqualityIndices[i] == rightColIdx {
			// The filter is already there; simply absorb it and say we succeeded.
			return true
		}
	}

	if !lhs.ResolvedType().Equivalent(rhs.ResolvedType()) {
		// Issue #22519: we can't have two equality columns of mismatched types
		// because the hash-joiner assumes the encodings are the same.
		return false
	}

	p.leftEqualityIndices = append(p.leftEqualityIndices, leftColIdx)
	p.rightEqualityIndices = append(p.rightEqualityIndices, rightColIdx)
	p.leftColNames = append(p.leftColNames, tree.Name(left.SourceColumns[leftColIdx].Name))
	p.rightColNames = append(p.rightColNames, tree.Name(right.SourceColumns[rightColIdx].Name))

	return true
}

// makePredicate constructs a joinPredicate object for joins. The join condition
// includes equality between usingColumns.
func makePredicate(
	joinType sqlbase.JoinType, left, right *sqlbase.DataSourceInfo, usingColumns []usingColumn,
) (*joinPredicate, error) {
	// Prepare the metadata for the result columns.
	// The structure of the join data source results is like this:
	// - all the left columns,
	// - then all the right columns,
	columns := make(sqlbase.ResultColumns, 0, len(left.SourceColumns)+len(right.SourceColumns))
	columns = append(columns, left.SourceColumns...)
	columns = append(columns, right.SourceColumns...)

	// Compute the mappings from table aliases to column sets from
	// both sides into a new alias-columnset mapping for the result
	// rows. We need to be extra careful about the aliases
	// for the anonymous table, which needs to be merged.
	aliases := make(sqlbase.SourceAliases, 0, len(left.SourceAliases)+len(right.SourceAliases))

	var anonymousCols util.FastIntSet

	collectAliases := func(sourceAliases sqlbase.SourceAliases, offset int) {
		for _, alias := range sourceAliases {
			newSet := alias.ColumnSet.Shift(offset)
			if alias.Name == sqlbase.AnonymousTable {
				anonymousCols.UnionWith(newSet)
			} else {
				aliases = append(aliases, sqlbase.SourceAlias{Name: alias.Name, ColumnSet: newSet})
			}
		}
	}
	collectAliases(left.SourceAliases, 0)
	collectAliases(right.SourceAliases, len(left.SourceColumns))
	if !anonymousCols.Empty() {
		aliases = append(aliases, sqlbase.SourceAlias{
			Name:      sqlbase.AnonymousTable,
			ColumnSet: anonymousCols,
		})
	}

	pred := &joinPredicate{
		joinType:     joinType,
		numLeftCols:  len(left.SourceColumns),
		numRightCols: len(right.SourceColumns),
		info: &sqlbase.DataSourceInfo{
			SourceColumns: columns,
			SourceAliases: aliases,
		},
	}
	// We must initialize the indexed var helper in all cases, even when
	// there is no on condition, so that getNeededColumns() does not get
	// confused.
	pred.iVarHelper = tree.MakeIndexedVarHelper(pred, len(columns))

	// Prepare the arrays populated below.
	pred.leftEqualityIndices = make([]int, 0, len(usingColumns))
	pred.rightEqualityIndices = make([]int, 0, len(usingColumns))
	colNames := make(tree.NameList, 0, len(usingColumns))

	// Find out which columns are involved in EqualityPredicate.
	for i := range usingColumns {
		uc := &usingColumns[i]

		if !uc.leftType.Equivalent(uc.rightType) {
			// Issue #22519: we can't have two equality columns of mismatched types
			// because the hash-joiner assumes the encodings are the same. Move the
			// equality to the ON condition.

			// First, check if the comparison would even be valid.
			_, found := tree.FindEqualComparisonFunction(uc.leftType, uc.rightType)
			if !found {
				return nil, pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError,
					"JOIN/USING types %s for left and %s for right cannot be matched for column %s",
					uc.leftType, uc.rightType, uc.name,
				)
			}
			expr := tree.NewTypedComparisonExpr(
				tree.EQ,
				pred.iVarHelper.IndexedVar(uc.leftIdx),
				pred.iVarHelper.IndexedVar(uc.rightIdx+pred.numLeftCols),
			)
			pred.onCond = mergeConj(pred.onCond, expr)
			continue
		}

		// Remember the indices.
		pred.leftEqualityIndices = append(pred.leftEqualityIndices, uc.leftIdx)
		pred.rightEqualityIndices = append(pred.rightEqualityIndices, uc.rightIdx)
		colNames = append(colNames, uc.name)
	}
	pred.leftColNames = colNames
	pred.rightColNames = colNames

	return pred, nil
}

// IndexedVarEval implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return p.curRow[idx].Eval(ctx)
}

// IndexedVarResolvedType implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarResolvedType(idx int) types.T {
	return p.info.SourceColumns[idx].Typ
}

// IndexedVarNodeFormatter implements the tree.IndexedVarContainer interface.
func (p *joinPredicate) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	return p.info.NodeFormatter(idx)
}

// eval for joinPredicate runs the on condition across the columns that do
// not participate in the equality (the equality columns are checked
// in the join algorithm already).
// Returns true if there is no on condition or the on condition accepts the
// row.
func (p *joinPredicate) eval(
	ctx *tree.EvalContext, result, leftRow, rightRow tree.Datums,
) (bool, error) {
	if p.onCond != nil {
		p.curRow = result
		copy(p.curRow[:len(leftRow)], leftRow)
		copy(p.curRow[len(leftRow):], rightRow)
		ctx.PushIVarContainer(p.iVarHelper.Container())
		pred, err := sqlbase.RunFilter(p.onCond, ctx)
		ctx.PopIVarContainer()
		return pred, err
	}
	return true, nil
}

// getNeededColumns figures out the columns needed for the two
// sources.  This takes into account both the equality columns and the
// predicate expression.
func (p *joinPredicate) getNeededColumns(neededJoined []bool) ([]bool, []bool) {
	// Reset the helper and rebind the variable to detect which columns
	// are effectively needed.
	p.onCond = p.iVarHelper.Rebind(p.onCond, true, false)

	// The columns that are part of the expression are always needed.
	neededJoined = append([]bool(nil), neededJoined...)
	for i := range neededJoined {
		if p.iVarHelper.IndexedVarUsed(i) {
			neededJoined[i] = true
		}
	}
	leftNeeded := neededJoined[:p.numLeftCols]
	rightNeeded := neededJoined[p.numLeftCols:]

	// The equality columns are always needed.
	for i := range p.leftEqualityIndices {
		leftNeeded[p.leftEqualityIndices[i]] = true
		rightNeeded[p.rightEqualityIndices[i]] = true
	}
	return leftNeeded, rightNeeded
}

// prepareRow prepares the output row by combining values from the
// input data sources.
func (p *joinPredicate) prepareRow(result, leftRow, rightRow tree.Datums) {
	copy(result[:len(leftRow)], leftRow)
	copy(result[len(leftRow):], rightRow)
}

// encode returns the encoding of a row from a given side (left or right),
// according to the columns specified by the equality constraints.
func (p *joinPredicate) encode(b []byte, row tree.Datums, cols []int) ([]byte, bool, error) {
	var err error
	containsNull := false
	for _, colIdx := range cols {
		if row[colIdx] == tree.DNull {
			containsNull = true
		}
		b, err = sqlbase.EncodeDatum(b, row[colIdx])
		if err != nil {
			return nil, false, err
		}
	}
	return b, containsNull, nil
}

// usingColumns captures the information about equality columns
// from USING and NATURAL JOIN statements.
type usingColumn struct {
	name tree.Name
	// Index and type of the column in the left source.
	leftIdx  int
	leftType types.T
	// Index and type of the column in the right source.
	rightIdx  int
	rightType types.T
}

func makeUsingColumns(
	leftCols, rightCols sqlbase.ResultColumns, usingColNames tree.NameList,
) ([]usingColumn, error) {
	if len(usingColNames) == 0 {
		return nil, nil
	}

	// Check for duplicate columns, e.g. USING(x,x).
	seenNames := make(map[string]struct{})
	for _, syntaxColName := range usingColNames {
		colName := string(syntaxColName)
		if _, ok := seenNames[colName]; ok {
			return nil, pgerror.NewErrorf(pgerror.CodeDuplicateColumnError,
				"column %q appears more than once in USING clause", colName)
		}
		seenNames[colName] = struct{}{}
	}

	res := make([]usingColumn, len(usingColNames))
	for i, name := range usingColNames {
		res[i].name = name
		var err error
		// Find the column name on the left.
		res[i].leftIdx, res[i].leftType, err = pickUsingColumn(leftCols, string(name), "left")
		if err != nil {
			return nil, err
		}

		// Find the column name on the right.
		res[i].rightIdx, res[i].rightType, err = pickUsingColumn(rightCols, string(name), "right")
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// pickUsingColumn searches for a column whose name matches colName.
// The column index and type are returned if found, otherwise an error
// is reported.
func pickUsingColumn(
	cols sqlbase.ResultColumns, colName string, context string,
) (int, types.T, error) {
	idx := invalidColIdx
	for j, col := range cols {
		if col.Hidden {
			continue
		}
		if col.Name == colName {
			idx = j
			break
		}
	}
	if idx == invalidColIdx {
		return idx, nil, pgerror.NewErrorf(pgerror.CodeUndefinedColumnError,
			"column \"%s\" specified in USING clause does not exist in %s table", colName, context)
	}
	return idx, cols[idx].Typ, nil
}

const invalidColIdx = -1
