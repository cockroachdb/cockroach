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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// hoistSelectSubquery searches the Select operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
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
// or InnerJoinApply operators, depending on subquery cardinality:
//
//   SELECT (SELECT MAX(u) FROM uv WHERE u=x) FROM xy
//   =>
//   SELECT u
//   FROM xy
//   INNER JOIN LATERAL (SELECT MAX(u) FROM uv WHERE u=x)
//   ON True
//
func (f *Factory) hoistProjectSubquery(input, projections memo.GroupID) memo.GroupID {
	var hoister subqueryHoister
	hoister.init(f.evalCtx, f, input)
	replaced := hoister.hoistAll(projections)
	return f.ConstructProject(hoister.input(), replaced)
}

// hoistJoinSubquery searches the Join operator's filter for correlated
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
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
// subqueries. Any found queries are hoisted into LeftJoinApply or
// InnerJoinApply operators, depending on subquery cardinality:
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

// referenceSingleColumn returns a Variable operator that refers to the one and
// only column that is projected by the given group.
func (f *Factory) referenceSingleColumn(group memo.GroupID) memo.GroupID {
	cols := f.mem.GroupProperties(group).Relational.OutputCols
	if cols.Len() != 1 {
		panic("expression does not have exactly one column")
	}
	colID, _ := cols.Next(0)
	return f.ConstructVariable(f.InternColumnID(opt.ColumnID(colID)))
}

// constructAnyCondition builds an expression that compares the given scalar
// expression with the first (and only) column of the input rowset, using the
// given comparison operator.
func (f *Factory) constructAnyCondition(
	input, scalar memo.GroupID, cmp memo.PrivateID,
) memo.GroupID {
	inputVar := f.referenceSingleColumn(input)
	return f.constructBinary(f.mem.LookupPrivate(cmp).(opt.Operator), scalar, inputVar)
}

// constructBinary builds a dynamic binary expression, given the binary
// operator's type and its two arguments.
func (f *Factory) constructBinary(op opt.Operator, left, right memo.GroupID) memo.GroupID {
	return f.DynamicConstruct(
		op,
		memo.DynamicOperands{
			memo.DynamicID(left),
			memo.DynamicID(right),
		},
	)
}

// subqueryHoister searches scalar expression trees looking for correlated
// subqueries which will be pulled up and joined to a higher level relational
// query. See the  hoistAll comment for more details on how this is done.
type subqueryHoister struct {
	evalCtx *tree.EvalContext
	f       *Factory
	mem     *memo.Memo
	hoisted memo.GroupID
	items   []memo.GroupID
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

// hoistAll searches the given subtree for each correlated Subquery, Exists, or
// Any operator, and lifts its subquery operand out of the scalar context and
// joins it with a higher-level relational expression. The original subquery
// operand is replaced by a Variable operator that refers to the first (and
// only) column of the hoisted relational expression.
//
// hoistAll returns the root of a new expression tree that incorporates the new
// Variable operators. The hoisted subqueries can be accessed via the input
// method. Each removed subquery wraps the one before, with the input query at
// the base. Each subquery adds a single column to its input and uses a
// JoinApply operator to ensure that it has no effect on the cardinality of its
// input. For example:
//
//   SELECT *
//   FROM xy
//   WHERE
//     (SELECT u FROM uv WHERE u=x LIMIT 1) IS NOT NULL OR
//     EXISTS(SELECT * FROM jk WHERE j=x)
//   =>
//   SELECT xy.*
//   FROM xy
//   LEFT JOIN LATERAL (SELECT u FROM uv WHERE u=x LIMIT 1)
//   ON True
//   INNER JOIN LATERAL (SELECT EXISTS_AGG(True) exists FROM jk WHERE j=x)
//   ON True
//   WHERE u IS NOT NULL OR exists
//
// The choice of whether to use LeftJoinApply or InnerJoinApply depends on the
// cardinality of the hoisted subquery. If zero rows can be returned from the
// subquery, then LeftJoinApply must be used in order to preserve the
// cardinality of the input expression. Otherwise, InnerJoinApply can be used
// instead. In either case, the wrapped subquery must never return more than one
// row, so as not to change the cardinality of the result.
//
// See the comments for constructGroupByExists and constructGroupByAny for more
// details on how EXISTS and ANY subqueries are hoisted, including usage of the
// EXISTS_AGG function.
func (r *subqueryHoister) hoistAll(root memo.GroupID) memo.GroupID {
	// Match correlated subqueries.
	ev := memo.MakeNormExprView(r.mem, root)
	switch ev.Operator() {
	case opt.SubqueryOp, opt.ExistsOp, opt.AnyOp:
		if ev.Logical().OuterCols().Empty() {
			break
		}

		subquery := ev.ChildGroup(0)
		switch ev.Operator() {
		case opt.ExistsOp:
			subquery = r.constructGroupByExists(subquery)

		case opt.AnyOp:
			input := ev.ChildGroup(0)
			scalar := ev.ChildGroup(1)
			cmp := ev.Private().(opt.Operator)
			subquery = r.constructGroupByAny(scalar, cmp, input)
		}

		// Hoist the subquery into a single expression that can be accessed via
		// the subqueries method.
		subqueryProps := r.mem.GroupProperties(subquery).Relational
		if subqueryProps.Cardinality.CanBeZero() {
			// Zero cardinality allowed, so must use left outer join to preserve
			// outer row (padded with nulls) in case the subquery returns zero rows.
			r.hoisted = r.f.ConstructLeftJoinApply(r.hoisted, subquery, r.f.ConstructTrue())
		} else {
			// Zero cardinality not allowed, so inner join suffices. Inner joins
			// are preferable to left joins since null handling is much simpler
			// and they allow the optimizer more choices.
			r.hoisted = r.f.ConstructInnerJoinApply(r.hoisted, subquery, r.f.ConstructTrue())
		}

		// Replace the Subquery operator with a Variable operator referring to
		// the first (and only) column in the hoisted query.
		colID, _ := subqueryProps.OutputCols.Next(0)
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

// constructGroupByExists transforms a scalar Exists expression like this:
//
//   EXISTS(SELECT * FROM a WHERE a.x=b.x)
//
// into a scalar GroupBy expression that returns a one row, one column relation:
//
//   SELECT EXISTS_AGG(True) FROM (SELECT * FROM a WHERE a.x=b.x)
//
// The expression uses an internally-defined EXISTS_AGG aggregation function for
// brevity and easier implementation, but the above expression is equivalent to:
//
//   SELECT COUNT(True) > 0 FROM (SELECT * FROM a WHERE a.x=b.x)
//
// EXISTS_AGG (and COUNT) always return exactly one boolean value in the context
// of a scalar GroupBy expression. Because it's operand is always True, the only
// way it can return False is if the input set is empty.
//
// However, later on, the TryDecorrelateScalarGroupBy rule will push a left join
// into the GroupBy, and null values produced by the join will flow into the
// EXISTS_AGG. It's defined to ignore those nulls so that its result will be
// unaffected.
func (r *subqueryHoister) constructGroupByExists(subquery memo.GroupID) memo.GroupID {
	constColID := r.f.Metadata().AddColumn("true", types.Bool)
	constCols := r.f.InternColList(opt.ColList{constColID})
	aggColID := r.f.Metadata().AddColumn("exists_agg", types.Bool)
	aggCols := r.f.InternColList(opt.ColList{aggColID})
	return r.f.ConstructGroupBy(
		r.f.ConstructProject(
			subquery,
			r.f.ConstructProjections(
				r.internSingletonList(r.f.ConstructConst(r.f.InternDatum(tree.DBoolTrue))),
				constCols,
			),
		),
		r.f.ConstructAggregations(
			r.internSingletonList(
				r.f.ConstructExistsAgg(
					r.f.ConstructVariable(r.f.InternColumnID(constColID)),
				),
			),
			aggCols,
		),
		r.f.InternColSet(opt.ColSet{}),
	)
}

// constructGroupByAny transforms a scalar Any expression like this:
//
//   z = ANY(SELECT x FROM xy)
//
// into a scalar GroupBy expression that returns a one row, one column relation
// that is equivalent to this:
//
//   SELECT
//     CASE
//       WHEN BOOL_OR(notnull) AND z IS NOT Null THEN True
//       ELSE BOOL_OR(notnull) IS NULL THEN False
//       ELSE Null
//     END
//   FROM
//   (
//     SELECT x IS NOT Null AS notnull
//     FROM xy
//     WHERE (z=x) IS NOT False
//   )
//
// BOOL_OR returns true if any input is true, else false if any input is false,
// else null. This is a mismatch with ANY, which returns true if any input is
// true, else null if any input is null, else false. In addition, the expression
// needs to be easy to decorrelate, which means that the outer column reference
// ("z" in the example) should not be part of a projection (since projections
// are difficult to hoist above left joins). The following procedure solves the
// mismatch between BOOL_OR and ANY, as well as avoids correlated projections:
//
//   1. Filter out false comparison rows with an initial filter. The result of
//      ANY does not change, no matter how many false rows are added or removed.
//      This step has the effect of mapping a set containing only false
//      comparison rows to the empty set (which is desirable).
//
//   2. Step #1 leaves only true and null comparison rows. A null comparison row
//      occurs when either the left or right comparison operand is null (Any
//      only allows comparison operators that propagate nulls). Map each null
//      row to a false row, but only in the case where the right operand is null
//      (i.e. the operand that came from the subquery). The case where the left
//      operand is null will be handled later.
//
//   3. Use the BOOL_OR aggregation function on the true/false values from step
//      #2. If there is at least one true value, then BOOL_OR returns true. If
//      there are no values (the empty set case), then BOOL_OR returns null.
//      Because of the previous steps, this indicates that the original set
//      contained only false values (or no values at all).
//
//   4. A True result from BOOL_OR is ambiguous. It could mean that the
//      comparison returned true for one of the rows in the group. Or, it could
//      mean that the left operand was null. The CASE statement ensures that
//      True is only returned if the left operand was not null.
//
//   5. In addition, the CASE statement maps a null return value to false, and
//      false to null. This matches ANY behavior.
//
// The following is a table showing the various interesting cases:
//
//                  | before  | after   | after
//   subquery       | BOOL_OR | BOOL_OR | CASE
//   ---------------+---------+---------+-------
//   x=1, z=1       | true    | true    | true
//   x=1, z=null    | true    | true    | null
//   x=null, z=1    | false   | false   | null
//   x=null, z=null | false   | false   | null
//   x=1, z=2       | (empty) | null    | false
//   (empty)        | (empty) | null    | false
//
// It is important that the set given to BOOL_OR does not contain any null
// values (the reason for step #2). Null is reserved for use by the
// TryDecorrelateScalarGroupBy rule, which will push a left join into the
// GroupBy. Null values produced by the left join will simply be ignored by
// BOOL_OR, and so cannot be used for any other purpose.
func (r *subqueryHoister) constructGroupByAny(
	scalar memo.GroupID, cmp opt.Operator, input memo.GroupID,
) memo.GroupID {
	// When the scalar value is not a simple variable or constant expression,
	// then cache its value using a projection, since it will be referenced
	// multiple times.
	scalarExpr := r.mem.NormExpr(scalar)
	if scalarExpr.Operator() != opt.VariableOp && !scalarExpr.IsConstValue() {
		typ := r.mem.GroupProperties(scalar).Scalar.Type
		scalarColID := r.f.Metadata().AddColumn("scalar", typ)
		r.hoisted = r.f.projectExtraCol(r.hoisted, scalar, scalarColID)
		scalar = r.f.ConstructVariable(r.f.InternColumnID(scalarColID))
	}

	inputVar := r.f.referenceSingleColumn(input)

	notNullColID := r.f.Metadata().AddColumn("notnull", types.Bool)
	notNullVar := r.f.ConstructVariable(r.f.InternColumnID(notNullColID))

	aggColID := r.f.Metadata().AddColumn("bool_or", types.Bool)
	aggVar := r.f.ConstructVariable(r.f.InternColumnID(aggColID))

	caseColID := r.f.Metadata().AddColumn("case", types.Bool)
	caseCols := r.f.InternColList(opt.ColList{caseColID})

	nullVal := r.f.ConstructNull(r.f.InternType(types.Unknown))

	return r.f.ConstructProject(
		r.f.ConstructGroupBy(
			r.f.ConstructProject(
				r.f.ConstructSelect(
					input,
					r.f.ConstructIsNot(
						r.f.constructBinary(cmp, inputVar, scalar),
						r.f.ConstructFalse(),
					),
				),
				r.f.ConstructProjections(
					r.internSingletonList(r.f.ConstructIsNot(inputVar, nullVal)),
					r.f.InternColList(opt.ColList{notNullColID}),
				),
			),
			r.f.ConstructAggregations(
				r.internSingletonList(r.f.ConstructBoolOr(notNullVar)),
				r.f.InternColList(opt.ColList{aggColID}),
			),
			r.f.InternColSet(opt.ColSet{}),
		),
		r.f.ConstructProjections(
			r.internSingletonList(
				r.f.ConstructCase(
					r.f.ConstructTrue(),
					r.f.InternList([]memo.GroupID{
						r.f.ConstructWhen(
							r.f.ConstructAnd(r.f.InternList([]memo.GroupID{
								aggVar,
								r.f.ConstructIsNot(scalar, nullVal),
							})),
							r.f.ConstructTrue(),
						),
						r.f.ConstructWhen(
							r.f.ConstructIs(aggVar, nullVal),
							r.f.ConstructFalse(),
						),
						nullVal,
					}),
				),
			),
			caseCols,
		),
	)
}

func (r *subqueryHoister) internSingletonList(item memo.GroupID) memo.ListID {
	r.items = append(r.items[:0], item)
	return r.f.InternList(r.items)
}
