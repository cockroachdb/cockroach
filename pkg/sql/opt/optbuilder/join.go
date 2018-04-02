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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// buildJoin builds a set of memo groups that represent the given join table
// expression.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildJoin(join *tree.JoinTableExpr, inScope *scope) (outScope *scope) {
	leftScope := b.buildTable(join.Left, inScope)
	rightScope := b.buildTable(join.Right, inScope)

	// Check that the same table name is not used on both sides.
	leftTables := make(map[tree.TableName]struct{})
	for _, leftCol := range leftScope.cols {
		leftTables[leftCol.table] = exists
	}

	for _, rightCol := range rightScope.cols {
		t := rightCol.table
		if t.TableName == "" {
			// Allow joins of sources that define columns with no
			// associated table name. At worst, the USING/NATURAL
			// detection code or expression analysis for ON will detect an
			// ambiguity later.
			continue
		}
		if _, ok := leftTables[t]; ok {
			panic(errorf("cannot join columns from the same source name %q (missing AS clause)", t.TableName))
		}
	}

	joinType := sqlbase.JoinTypeFromAstString(join.Join)

	switch cond := join.Cond.(type) {
	case tree.NaturalJoinCond, *tree.UsingJoinCond:
		var usingColNames tree.NameList

		switch t := cond.(type) {
		case tree.NaturalJoinCond:
			usingColNames = commonColumns(leftScope, rightScope)
		case *tree.UsingJoinCond:
			usingColNames = t.Cols
		}

		return b.buildUsingJoin(joinType, usingColNames, leftScope, rightScope, inScope)

	case *tree.OnJoinCond, nil:
		// Append columns added by the children, as they are visible to the filter.
		outScope = inScope.push()
		outScope.appendColumns(leftScope)
		outScope.appendColumns(rightScope)

		var filter memo.GroupID
		if on, ok := cond.(*tree.OnJoinCond); ok {
			filter = b.buildScalar(outScope.resolveType(on.Expr, types.Bool), outScope)
		} else {
			filter = b.factory.ConstructTrue()
		}

		outScope.group = b.constructJoin(joinType, leftScope.group, rightScope.group, filter)
		return outScope

	default:
		panic(fmt.Sprintf("unsupported join condition %#v", cond))
	}
}

// commonColumns returns the names of columns common on the
// left and right sides, for use by NATURAL JOIN.
func commonColumns(leftScope, rightScope *scope) (common tree.NameList) {
	for _, leftCol := range leftScope.cols {
		if leftCol.hidden {
			continue
		}
		for _, rightCol := range rightScope.cols {
			if rightCol.hidden {
				continue
			}

			if leftCol.name == rightCol.name {
				common = append(common, leftCol.name)
				break
			}
		}
	}

	return common
}

// buildUsingJoin builds a set of memo groups that represent the given join
// table expression with the given `USING` column names. It is used for both
// USING and NATURAL joins.
//
// joinType    The join type (inner, left, right or outer)
// names       The list of `USING` column names
// leftScope   The outScope from the left table
// rightScope  The outScope from the right table
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUsingJoin(
	joinType sqlbase.JoinType, names tree.NameList, leftScope, rightScope, inScope *scope,
) (outScope *scope) {
	// Build the join predicate.
	mergedCols, filter, outScope := b.buildUsingJoinPredicate(
		joinType, leftScope.cols, rightScope.cols, names, inScope,
	)

	outScope.group = b.constructJoin(joinType, leftScope.group, rightScope.group, filter)

	if len(mergedCols) > 0 {
		// Wrap in a projection to include the merged columns.
		for i, col := range outScope.cols {
			if mergedCol, ok := mergedCols[col.id]; ok {
				outScope.cols[i].group = mergedCol
			} else {
				v := b.factory.ConstructVariable(b.factory.InternColumnID(col.id))
				outScope.cols[i].group = v
			}
		}

		p := b.constructList(opt.ProjectionsOp, outScope.cols)
		outScope.group = b.factory.ConstructProject(outScope.group, p)
	}

	return outScope
}

// buildUsingJoinPredicate builds a set of memo groups that represent the join
// conditions for a USING join or natural join. It finds the columns in the
// left and right relations that match the columns provided in the names
// parameter, and creates equality predicate(s) with those columns. It also
// ensures that there is a single output column for each name in `names`
// (other columns with the same name are hidden).
//
// -- Merged columns --
//
// With NATURAL JOIN or JOIN USING (a,b,c,...), SQL allows us to refer to the
// columns a,b,c directly; these columns have the following semantics:
//   a = IFNULL(left.a, right.a)
//   b = IFNULL(left.b, right.b)
//   c = IFNULL(left.c, right.c)
//   ...
//
// Furthermore, a star has to resolve the columns in the following order:
// merged columns, non-equality columns from the left table, non-equality
// columns from the right table. To perform this rearrangement, we use a
// projection on top of the join. Note that the original columns must
// still be accessible via left.a, right.a (they will just be hidden).
//
// For inner or left outer joins, a is always the same as left.a.
//
// For right outer joins, a is always equal to right.a; but for some types
// (like collated strings), this doesn't mean it is the same as right.a. In
// this case we must still use the IFNULL construct.
//
// Example:
//
//  left has columns (a,b,x)
//  right has columns (a,b,y)
//
//  - SELECT * FROM left JOIN right USING(a,b)
//
//  join has columns:
//    1: left.a
//    2: left.b
//    3: left.x
//    4: right.a
//    5: right.b
//    6: right.y
//
//  projection has columns and corresponding variable expressions:
//    1: a aka left.a        @1
//    2: b aka left.b        @2
//    3: left.x              @3
//    4: right.a (hidden)    @4
//    5: right.b (hidden)    @5
//    6: right.y             @6
//
// If the join was a FULL OUTER JOIN, the columns would be:
//    1: a                   IFNULL(@1,@4)
//    2: b                   IFNULL(@2,@5)
//    3: left.a (hidden)     @1
//    4: left.b (hidden)     @2
//    5: left.x              @3
//    6: right.a (hidden)    @4
//    7: right.b (hidden)    @5
//    8: right.y             @6
//
// If new merged columns are created (as in the FULL OUTER JOIN example above),
// the return value mergedCols contains a mapping from the column id to the
// memo group ID of the IFNULL expression. out contains the top-level memo
// group ID of the join predicate.
//
// See Builder.buildStmt for a description of the remaining input and
// return values.
func (b *Builder) buildUsingJoinPredicate(
	joinType sqlbase.JoinType,
	leftCols []scopeColumn,
	rightCols []scopeColumn,
	names tree.NameList,
	inScope *scope,
) (mergedCols map[opt.ColumnID]memo.GroupID, out memo.GroupID, outScope *scope) {
	joined := make(map[tree.Name]*scopeColumn, len(names))
	conditions := make([]memo.GroupID, 0, len(names))
	mergedCols = make(map[opt.ColumnID]memo.GroupID)
	outScope = inScope.push()

	for _, name := range names {
		if _, ok := joined[name]; ok {
			panic(errorf("column %q appears more than once in USING clause", name))
		}

		// For every adjacent pair of tables, add an equality predicate.
		leftCol := findUsingColumn(leftCols, name, "left")
		rightCol := findUsingColumn(rightCols, name, "right")

		if !leftCol.typ.Equivalent(rightCol.typ) {
			// First, check if the comparison would even be valid.
			if _, found := tree.FindEqualComparisonFunction(leftCol.typ, rightCol.typ); !found {
				panic(errorf(
					"JOIN/USING types %s for left and %s for right cannot be matched for column %s",
					leftCol.typ, rightCol.typ, leftCol.name,
				))
			}
		}

		// Construct the predicate.
		leftVar := b.factory.ConstructVariable(b.factory.InternColumnID(leftCol.id))
		rightVar := b.factory.ConstructVariable(b.factory.InternColumnID(rightCol.id))
		eq := b.factory.ConstructEq(leftVar, rightVar)
		conditions = append(conditions, eq)

		// Add the merged column to the scope, constructing a new column if needed.
		if joinType == sqlbase.InnerJoin || joinType == sqlbase.LeftOuterJoin {
			// The merged column is the same as the corresponding column from the
			// left side.
			outScope.cols = append(outScope.cols, *leftCol)
		} else if joinType == sqlbase.RightOuterJoin &&
			!sqlbase.DatumTypeHasCompositeKeyEncoding(leftCol.typ) {
			// The merged column is the same as the corresponding column from the
			// right side.
			outScope.cols = append(outScope.cols, *rightCol)
		} else {
			// Construct a new merged column to represent IFNULL(left, right).
			var typ types.T
			if leftCol.typ != types.Unknown {
				typ = leftCol.typ
			} else {
				typ = rightCol.typ
			}
			texpr := tree.NewTypedCoalesceExpr(tree.TypedExprs{leftCol, rightCol}, typ)
			merged := b.factory.ConstructCoalesce(b.factory.InternList([]memo.GroupID{leftVar, rightVar}))
			col := b.synthesizeColumn(outScope, string(leftCol.name), typ, texpr, merged)
			mergedCols[col.id] = merged
		}

		joined[name] = &outScope.cols[len(outScope.cols)-1]
	}

	// Hide other columns that have the same name as the merged columns.
	hideMatchingColumns(leftCols, joined, outScope)
	hideMatchingColumns(rightCols, joined, outScope)

	return mergedCols, b.constructFilter(conditions), outScope
}

// hideMatchingColumns iterates through each of the columns in cols and
// performs one of the following actions:
// (1) If the column is equal to one of the columns in `joined`, it is skipped
//     since it was one of the merged columns already added to the scope.
// (2) If the column has the same name as one of the columns in `joined` but is
//     not equal, it is marked as hidden and added to the scope.
// (3) All other columns are added to the scope without modification.
func hideMatchingColumns(cols []scopeColumn, joined map[tree.Name]*scopeColumn, scope *scope) {
	for _, col := range cols {
		if foundCol, ok := joined[col.name]; ok {
			// Hide other columns with the same name.
			if col == *foundCol {
				continue
			}
			col.hidden = true
		}
		scope.cols = append(scope.cols, col)
	}
}

// constructFilter builds a set of memo groups that represent the given
// list of filter conditions. It returns the top-level memo group ID for the
// filter.
func (b *Builder) constructFilter(conditions []memo.GroupID) memo.GroupID {
	switch len(conditions) {
	case 0:
		return b.factory.ConstructTrue()
	case 1:
		return conditions[0]
	default:
		return b.factory.ConstructAnd(b.factory.InternList(conditions))
	}
}

func (b *Builder) constructJoin(
	joinType sqlbase.JoinType, left, right, filter memo.GroupID,
) memo.GroupID {
	switch joinType {
	case sqlbase.InnerJoin:
		return b.factory.ConstructInnerJoin(left, right, filter)
	case sqlbase.LeftOuterJoin:
		return b.factory.ConstructLeftJoin(left, right, filter)
	case sqlbase.RightOuterJoin:
		return b.factory.ConstructRightJoin(left, right, filter)
	case sqlbase.FullOuterJoin:
		return b.factory.ConstructFullJoin(left, right, filter)
	default:
		panic(fmt.Errorf("unsupported JOIN type %d", joinType))
	}
}

// findUsingColumn finds the column in cols that has the given name. If the
// column exists it is returned. Otherwise, an error is thrown.
//
// context is a string ("left" or "right") used to indicate in the error
// message whether the name is missing from the left or right side of the join.
func findUsingColumn(cols []scopeColumn, name tree.Name, context string) *scopeColumn {
	for i := range cols {
		col := &cols[i]
		if !col.hidden && col.name == name {
			return col
		}
	}

	panic(errorf("column \"%s\" specified in USING clause does not exist in %s table", name, context))
}
