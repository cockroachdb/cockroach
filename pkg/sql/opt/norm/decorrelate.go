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

package norm

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// hoistSelectSubquery searches the Select operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply operators:
//
//   SELECT * FROM xy WHERE (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//   =>
//   SELECT xy.*
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//   WHERE u IS NULL
//
func (f *Factory) hoistSelectSubquery(input, filter memo.GroupID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(f.evalCtx, f, input)
	replaced := hoister.hoistAll(filter)
	out := f.ConstructSelect(hoister.input(), replaced)
	return f.ConstructProject(out, f.projectColumns(f.outputCols(input)))
}

// hoistProjectSubquery searches the Project operator's projections for
// correlated subqueries. Any found queries are hoisted into LeftJoinApply
// operators:
//
//   SELECT (SELECT u FROM uv WHERE u=x LIMIT 1) FROM xy
//   =>
//   SELECT u
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//
func (f *Factory) hoistProjectSubquery(input, projections memo.GroupID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(f.evalCtx, f, input)
	replaced := hoister.hoistAll(projections)
	return f.ConstructProject(hoister.input(), replaced)
}

// hoistJoinSubquery searches the Join operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply operators:
//
//   SELECT y, z
//   FROM xy
//   FULL JOIN yz
//   ON (SELECT u FROM uv WHERE u=x LIMIT 1) IS NULL
//   =>
//   SELECT y, z
//   FROM xy
//   FULL JOIN LATERAL
//   (
//     SELECT *
//     FROM yz
//     LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//     ON True
//   )
//   ON u IS NULL
//
func (f *Factory) hoistJoinSubquery(op opt.Operator, left, right, on memo.GroupID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(f.evalCtx, f, right)
	replaced := hoister.hoistAll(on)
	out := f.constructApplyJoin(op, left, hoister.input(), replaced)
	projections := f.projectColumns(f.outputCols(left).Union(f.outputCols(right)))
	return f.ConstructProject(out, projections)
}

// hoistValuesSubquery searches the Values operator's projections for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply operators:
//
//   SELECT (VALUES (SELECT u FROM uv WHERE u=x LIMIT 1)) FROM xy
//   =>
//   SELECT
//   (
//     SELECT vals.*
//     FROM (VALUES ())
//     LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//     ON True
//     INNER JOIN LATERAL (VALUES (u)) vals
//     ON True
//   )
//   FROM xy
//
// The dummy VALUES clause with a singleton empty row is added to the tree in
// order to use the hoister, which requires an initial input query. While a
// right join would be slightly better here, this is such a fringe case that
// it's not worth the extra code complication.
func (f *Factory) hoistValuesSubquery(rows memo.ListID, cols memo.PrivateID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(f.evalCtx, f, f.constructEmptyRow())

	replaced := make([]memo.GroupID, rows.Length)
	for i, item := range f.mem.LookupList(rows) {
		replaced[i] = hoister.hoistAll(item)
	}

	out := f.ConstructValues(f.mem.InternList(replaced), cols)
	projections := f.projectColumns(f.mem.GroupProperties(out).Relational.OutputCols)
	out = f.ConstructInnerJoinApply(hoister.input(), out, f.ConstructTrue())
	return f.ConstructProject(out, projections)
}

// projectColumns creates a Projections operator with one column for each of the
// columns in the given set.
func (f *Factory) projectColumns(colSet opt.ColSet) memo.GroupID {
	items := make([]memo.GroupID, 0, colSet.Len())
	colSet.ForEach(func(i int) {
		items = append(items, f.ConstructVariable(f.mem.InternColumnID(opt.ColumnID(i))))
	})
	colList := opt.ColSetToList(colSet)
	return f.ConstructProjections(f.mem.InternList(items), f.mem.InternColList(colList))
}

// constructNonApplyJoin constructs the non-apply join operator that corresponds
// to the given join operator type.
func (f *Factory) constructNonApplyJoin(
	joinOp opt.Operator, left, right, filter memo.GroupID,
) memo.GroupID {
	switch joinOp {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return f.ConstructInnerJoin(left, right, filter)
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return f.ConstructLeftJoin(left, right, filter)
	case opt.RightJoinOp, opt.RightJoinApplyOp:
		return f.ConstructRightJoin(left, right, filter)
	case opt.FullJoinOp, opt.FullJoinApplyOp:
		return f.ConstructFullJoin(left, right, filter)
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return f.ConstructSemiJoin(left, right, filter)
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return f.ConstructAntiJoin(left, right, filter)
	}
	panic(fmt.Sprintf("unexpected join operator: %v", joinOp))
}

// constructApplyJoin constructs the apply join operator that corresponds
// to the given join operator type.
func (f *Factory) constructApplyJoin(
	joinOp opt.Operator, left, right, filter memo.GroupID,
) memo.GroupID {
	switch joinOp {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return f.ConstructInnerJoinApply(left, right, filter)
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return f.ConstructLeftJoinApply(left, right, filter)
	case opt.RightJoinOp, opt.RightJoinApplyOp:
		return f.ConstructRightJoinApply(left, right, filter)
	case opt.FullJoinOp, opt.FullJoinApplyOp:
		return f.ConstructFullJoinApply(left, right, filter)
	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return f.ConstructSemiJoinApply(left, right, filter)
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return f.ConstructAntiJoinApply(left, right, filter)
	}
	panic(fmt.Sprintf("unexpected join operator: %v", joinOp))
}

// constructEmptyRow returns a Values operator having a single row with zero
// columns.
func (f *Factory) constructEmptyRow() memo.GroupID {
	rows := []memo.GroupID{f.ConstructTuple(f.InternList(nil))}
	return f.ConstructValues(f.InternList(rows), f.InternColList(opt.ColList{}))
}

// subqueryHoister searches scalar expression trees looking for correlated
// subqueries which will be pulled up and joined to a higher level relational
// query. See the  hoistAll comment for more details on how this is done.
type subqueryHoister struct {
	evalCtx *tree.EvalContext
	f       *Factory
	mem     *memo.Memo
	hoisted memo.GroupID
}

func (r *subqueryHoister) init(evalCtx *tree.EvalContext, f *Factory, input memo.GroupID) {
	r.evalCtx = evalCtx
	r.f = f
	r.mem = f.mem
	r.hoisted = input
}

// input returns a single expression tree that contains the input expression
// provided to the init method, but wrapped with any subqueries hoisted out of
// the scalar expression tree. See the hoistAll comment for more details.
func (r *subqueryHoister) input() memo.GroupID {
	return r.hoisted
}

// hoistAll searches the given subtree for correlated Subquery nodes. Each
// matching Subquery is replaced by a Variable operator that refers to its first
// (and only) column. hoistAll returns the root of a new expression tree that
// incorporates the new Variable operators. Each removed subquery wraps the one
// before, with the input query at the base. Each subquery adds a single column
// to its input and uses LeftJoinApply to ensure that it has no effect on the
// cardinality of its input.
//
//   (LeftJoinApply
//     (LeftJoinApply
//       <input>
//       <subquery1>
//       (True)
//     )
//     <subquery2>
//     (True)
//   )
//
// The input wrapped with the hoisted subqueries can be accessed via the input
// method.
func (r *subqueryHoister) hoistAll(root memo.GroupID) memo.GroupID {
	// Match correlated subqueries.
	ev := memo.MakeNormExprView(r.mem, root)
	if ev.Operator() == opt.SubqueryOp && !ev.Logical().OuterCols().Empty() {
		// Hoist the subquery into a single expression that can be accessed via
		// the subqueries method.
		subquery := ev.ChildGroup(0)

		if r.hoisted == 0 {
			r.hoisted = subquery
		} else {
			r.hoisted = r.f.ConstructLeftJoinApply(r.hoisted, subquery, r.f.ConstructTrue())
		}

		// Replace the Subquery operator with a Variable operator referring to
		// the first (and only) column in the hoisted query.
		colID, _ := r.mem.GroupProperties(subquery).Relational.OutputCols.Next(0)
		return r.f.ConstructVariable(r.mem.InternColumnID(opt.ColumnID(colID)))
	}

	return ev.Replace(r.evalCtx, func(child memo.GroupID) memo.GroupID {
		// Recursively hoist subqueries in each child that contains them.
		scalar := r.mem.GroupProperties(child).Scalar
		if scalar != nil && scalar.HasCorrelatedSubquery {
			return r.hoistAll(child)
		}

		// Return unchanged child.
		return child
	}).Group()
}
