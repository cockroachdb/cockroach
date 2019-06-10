// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// joinNode is a planNode whose rows are the result of an inner or
// left/right outer join.
type joinNode struct {
	joinType sqlbase.JoinType

	// The data sources.
	left  planDataSource
	right planDataSource

	// pred represents the join predicate.
	pred *joinPredicate

	// mergeJoinOrdering is set during expandPlan if the left and right sides have
	// similar ordering on the equality columns (or a subset of them). The column
	// indices refer to equality columns: a ColIdx of i refers to left column
	// pred.leftEqualityIndices[i] and right column pred.rightEqualityIndices[i].
	// See computeMergeJoinOrdering. This information is used by distsql planning.
	mergeJoinOrdering sqlbase.ColumnOrdering

	// ordering is set during expandPlan based on mergeJoinOrdering, but later
	// trimmed.
	props physicalProps

	// columns contains the metadata for the results of this node.
	columns sqlbase.ResultColumns
}

// makeJoinPredicate builds a joinPredicate from a join condition. Also returns
// any USING or NATURAL JOIN columns (these need to be merged into one column
// after the join).
func (p *planner) makeJoinPredicate(
	ctx context.Context,
	left *sqlbase.DataSourceInfo,
	right *sqlbase.DataSourceInfo,
	joinType sqlbase.JoinType,
	cond tree.JoinCond,
) (*joinPredicate, []usingColumn, error) {
	switch cond.(type) {
	case tree.NaturalJoinCond, *tree.UsingJoinCond:
		var usingColNames tree.NameList

		switch t := cond.(type) {
		case tree.NaturalJoinCond:
			usingColNames = commonColumns(left, right)
		case *tree.UsingJoinCond:
			usingColNames = t.Cols
		}

		usingColumns, err := makeUsingColumns(
			left.SourceColumns, right.SourceColumns, usingColNames,
		)
		if err != nil {
			return nil, nil, err
		}
		pred, err := makePredicate(joinType, left, right, usingColumns)
		if err != nil {
			return nil, nil, err
		}
		return pred, usingColumns, nil

	case nil, *tree.OnJoinCond:
		pred, err := makePredicate(joinType, left, right, nil /* usingColumns */)
		if err != nil {
			return nil, nil, err
		}
		switch t := cond.(type) {
		case *tree.OnJoinCond:
			// Do not allow special functions in the ON clause.
			p.semaCtx.Properties.Require("ON", tree.RejectSpecial)

			// Determine the on condition expression. Note that the predicate can't
			// already have onCond set (we haven't passed any usingColumns).
			pred.onCond, err = p.analyzeExpr(
				ctx,
				t.Expr,
				sqlbase.MultiSourceInfo{pred.info},
				pred.iVarHelper,
				types.Bool,
				true, /* requireType */
				"ON",
			)
			if err != nil {
				return nil, nil, err
			}
		}
		return pred, nil /* usingColumns */, nil

	default:
		panic(fmt.Sprintf("unsupported join condition %#v", cond))
	}
}

func (p *planner) makeJoinNode(
	left planDataSource, right planDataSource, pred *joinPredicate,
) *joinNode {
	n := &joinNode{
		left:     left,
		right:    right,
		joinType: pred.joinType,
		pred:     pred,
		columns:  pred.info.SourceColumns,
	}
	return n
}

// makeJoin constructs a planDataSource for a JOIN.
// The source might be a joinNode, or it could be a renderNode on top of a
// joinNode (in the case of outer natural joins).
func (p *planner) makeJoin(
	ctx context.Context,
	joinType sqlbase.JoinType,
	left planDataSource,
	right planDataSource,
	cond tree.JoinCond,
) (planDataSource, error) {
	// Check that the same table name is not used on both sides.
	for _, alias := range right.info.SourceAliases {
		if _, ok := left.info.SourceAliases.SrcIdx(alias.Name); ok {
			t := alias.Name.Table()
			if t == "" {
				// Allow joins of sources that define columns with no
				// associated table name. At worst, the USING/NATURAL
				// detection code or expression analysis for ON will detect an
				// ambiguity later.
				continue
			}
			return planDataSource{}, pgerror.Newf(
				pgcode.DuplicateAlias,
				"source name %q specified more than once (missing AS clause)",
				t,
			)
		}
	}

	pred, usingColumns, err := p.makeJoinPredicate(ctx, left.info, right.info, joinType, cond)
	if err != nil {
		return planDataSource{}, err
	}
	n := p.makeJoinNode(left, right, pred)
	joinDataSource := planDataSource{info: pred.info, plan: n}

	if len(usingColumns) == 0 {
		// No merged columns, we are done.
		return joinDataSource, nil
	}

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
	// renderNode on top of the joinNode. Note that the original columns must
	// still be accessible via left.a, right.a (they will just be hidden).
	//
	// For inner or left outer joins, a is always the same with left.a.
	//
	// For right outer joins, a is always equal to right.a; but for some types
	// (like collated strings), this doesn't mean it is the same with right.a. In
	// this case we must still use the IFNULL construct.
	//
	// Example:
	//
	//  left has columns (a,b,x)
	//  right has columns (a,b,y)
	//
	//  - SELECT * FROM left JOIN right ON(a,b)
	//
	//  joinNode has columns:
	//    1: left.a
	//    2: left.b
	//    3: left.x
	//    4: right.a
	//    5: right.b
	//    6: right.y
	//
	//  renderNode has columns and render expressions:
	//    1: a aka left.a        @1
	//    2: b aka left.b        @2
	//    3: left.x              @3
	//    4: right.a (hidden)    @4
	//    5: right.b (hidden)    @5
	//    6: right.y             @6
	//
	// If the join was OUTER, the columns would be:
	//    1: a                   IFNULL(@1,@4)
	//    2: b                   IFNULL(@2,@5)
	//    3: left.a (hidden)     @1
	//    4: left.b (hidden)     @2
	//    5: left.x              @3
	//    6: right.a (hidden)    @4
	//    7: right.b (hidden)    @5
	//    8: right.y             @6

	r := &renderNode{
		source:     joinDataSource,
		sourceInfo: sqlbase.MultiSourceInfo{pred.info},
	}
	r.ivarHelper = tree.MakeIndexedVarHelper(r, len(pred.info.SourceColumns))
	numLeft := len(left.info.SourceColumns)
	numRight := len(right.info.SourceColumns)
	rInfo := &sqlbase.DataSourceInfo{
		SourceAliases: make(sqlbase.SourceAliases, 0, len(pred.info.SourceAliases)),
	}

	var leftHidden, rightHidden util.FastIntSet

	// In the example above, we "remapped" left.a to column 1 instead of including
	// the column twice. We keep track of these columns so we can adjust the
	// aliases accordingly: column i of the joinNode becomes column remapped[i] of
	// the renderNode.
	remapped := make([]int, numLeft+numRight)
	for i := range remapped {
		remapped[i] = -1
	}
	for i := range usingColumns {
		leftCol := usingColumns[i].leftIdx
		rightCol := usingColumns[i].rightIdx
		leftHidden.Add(leftCol)
		rightHidden.Add(rightCol)
		var expr tree.TypedExpr
		if n.joinType == sqlbase.InnerJoin || n.joinType == sqlbase.LeftOuterJoin {
			// The merged column is the same with the corresponding column from the
			// left side.
			expr = r.ivarHelper.IndexedVar(leftCol)
			remapped[leftCol] = i
		} else if n.joinType == sqlbase.RightOuterJoin &&
			!sqlbase.DatumTypeHasCompositeKeyEncoding(left.info.SourceColumns[leftCol].Typ) {
			// The merged column is the same with the corresponding column from the
			// right side.
			expr = r.ivarHelper.IndexedVar(numLeft + rightCol)
			remapped[numLeft+rightCol] = i
		} else {
			c := &tree.CoalesceExpr{
				Name: "IFNULL",
				Exprs: []tree.Expr{
					r.ivarHelper.IndexedVar(leftCol),
					r.ivarHelper.IndexedVar(numLeft + rightCol),
				},
			}
			var err error
			p.semaCtx.IVarContainer = r.ivarHelper.Container()
			expr, err = c.TypeCheck(&p.semaCtx, types.Any)
			p.semaCtx.IVarContainer = nil
			if err != nil {
				return planDataSource{}, err
			}
		}
		// Issue #23609: the type of the left side might be NULL; so use the type of
		// the IFNULL expression instead of the type of the left source column.
		r.addRenderColumn(expr, symbolicExprStr(expr), sqlbase.ResultColumn{
			Name: left.info.SourceColumns[leftCol].Name,
			Typ:  expr.ResolvedType(),
		})
	}
	for i, c := range left.info.SourceColumns {
		if remapped[i] != -1 {
			// Column already included.
			continue
		}
		remapped[i] = len(r.render)
		expr := r.ivarHelper.IndexedVar(i)
		if leftHidden.Contains(i) {
			c.Hidden = true
		}
		r.addRenderColumn(expr, symbolicExprStr(expr), c)
	}
	for i, c := range right.info.SourceColumns {
		if remapped[numLeft+i] != -1 {
			// Column already included.
			continue
		}
		remapped[numLeft+i] = len(r.render)
		expr := r.ivarHelper.IndexedVar(numLeft + i)
		if rightHidden.Contains(i) {
			c.Hidden = true
		}
		r.addRenderColumn(expr, symbolicExprStr(expr), c)
	}
	rInfo.SourceColumns = r.columns

	// Copy the aliases, remapping the columns as necessary. We extract any
	// anonymous aliases for special handling.
	anonymousAlias := sqlbase.SourceAlias{Name: sqlbase.AnonymousTable}
	for _, a := range pred.info.SourceAliases {
		var colSet util.FastIntSet
		for col, ok := a.ColumnSet.Next(0); ok; col, ok = a.ColumnSet.Next(col + 1) {
			colSet.Add(remapped[col])
		}
		if a.Name == sqlbase.AnonymousTable {
			anonymousAlias.ColumnSet = colSet
			continue
		}
		rInfo.SourceAliases = append(
			rInfo.SourceAliases,
			sqlbase.SourceAlias{Name: a.Name, ColumnSet: colSet},
		)
	}

	for i := range usingColumns {
		anonymousAlias.ColumnSet.Add(i)
	}

	// Remove any anonymous aliases that refer to hidden equality columns (i.e.
	// those that weren't equivalent to the merged column).
	for i, col := range n.pred.leftEqualityIndices {
		if target := remapped[col]; target != i {
			anonymousAlias.ColumnSet.Remove(target)
		}
	}
	for i, col := range n.pred.rightEqualityIndices {
		if target := remapped[numLeft+col]; target != i {
			anonymousAlias.ColumnSet.Remove(remapped[numLeft+col])
		}
	}
	rInfo.SourceAliases = append(rInfo.SourceAliases, anonymousAlias)
	return planDataSource{info: rInfo, plan: r}, nil
}

func (n *joinNode) startExec(params runParams) error {
	panic("joinNode cannot be run in local mode")
}

// Next implements the planNode interface.
func (n *joinNode) Next(params runParams) (res bool, err error) {
	panic("joinNode cannot be run in local mode")
}

// Values implements the planNode interface.
func (n *joinNode) Values() tree.Datums {
	panic("joinNode cannot be run in local mode")
}

// Close implements the planNode interface.
func (n *joinNode) Close(ctx context.Context) {
	n.right.plan.Close(ctx)
	n.left.plan.Close(ctx)
}

// commonColumns returns the names of columns common on the
// right and left sides, for use by NATURAL JOIN.
func commonColumns(left, right *sqlbase.DataSourceInfo) tree.NameList {
	var res tree.NameList
	for _, cLeft := range left.SourceColumns {
		if cLeft.Hidden {
			continue
		}
		for _, cRight := range right.SourceColumns {
			if cRight.Hidden {
				continue
			}

			if cLeft.Name == cRight.Name {
				res = append(res, tree.Name(cLeft.Name))
			}
		}
	}
	return res
}

func (n *joinNode) joinOrdering() physicalProps {
	if len(n.mergeJoinOrdering) == 0 {
		return physicalProps{}
	}
	info := physicalProps{}

	// n.Columns has the following schema on equality JOINs:
	//
	// 0                         numLeftCols
	// |                         |                          |
	//  --- Columns from left --- --- Columns from right ---

	leftCol := func(leftColIdx int) int {
		return leftColIdx
	}
	rightCol := func(rightColIdx int) int {
		return n.pred.numLeftCols + rightColIdx
	}

	leftOrd := planPhysicalProps(n.left.plan)
	rightOrd := planPhysicalProps(n.right.plan)

	// Propagate the equivalency groups for the left columns.
	for i := 0; i < n.pred.numLeftCols; i++ {
		if group := leftOrd.eqGroups.Find(i); group != i {
			info.eqGroups.Union(leftCol(group), rightCol(group))
		}
	}
	// Propagate the equivalency groups for the right columns.
	for i := 0; i < n.pred.numRightCols; i++ {
		if group := rightOrd.eqGroups.Find(i); group != i {
			info.eqGroups.Union(rightCol(group), rightCol(i))
		}
	}

	// TODO(arjun): Support order propagation for other JOIN types.
	if n.joinType != sqlbase.InnerJoin {
		return info
	}

	// Set equivalency between the equality column pairs (and merged column if
	// appropriate).
	for i, leftIdx := range n.pred.leftEqualityIndices {
		rightIdx := n.pred.rightEqualityIndices[i]
		info.eqGroups.Union(leftCol(leftIdx), rightCol(rightIdx))
	}

	// Any constant columns stay constant after an inner join.
	for l, ok := leftOrd.constantCols.Next(0); ok; l, ok = leftOrd.constantCols.Next(l + 1) {
		info.addConstantColumn(leftCol(l))
	}
	for r, ok := rightOrd.constantCols.Next(0); ok; r, ok = rightOrd.constantCols.Next(r + 1) {
		info.addConstantColumn(rightCol(r))
	}

	// If the equality columns form a key on both sides, then each row (from
	// either side) is incorporated into at most one result row; so any key sets
	// remain valid and can be propagated.

	var leftEqSet, rightEqSet util.FastIntSet
	for i, leftIdx := range n.pred.leftEqualityIndices {
		leftEqSet.Add(leftIdx)
		info.addNotNullColumn(leftCol(leftIdx))

		rightIdx := n.pred.rightEqualityIndices[i]
		rightEqSet.Add(rightIdx)
		info.addNotNullColumn(rightCol(rightIdx))
	}

	if leftOrd.isKey(leftEqSet) && rightOrd.isKey(rightEqSet) {
		for _, k := range leftOrd.weakKeys {
			// Translate column indices.
			var s util.FastIntSet
			for c, ok := k.Next(0); ok; c, ok = k.Next(c + 1) {
				s.Add(leftCol(c))
			}
			info.addWeakKey(s)
		}
		for _, k := range rightOrd.weakKeys {
			// Translate column indices.
			var s util.FastIntSet
			for c, ok := k.Next(0); ok; c, ok = k.Next(c + 1) {
				s.Add(rightCol(c))
			}
			info.addWeakKey(s)
		}
	}

	info.ordering = make(sqlbase.ColumnOrdering, len(n.mergeJoinOrdering))
	for i, col := range n.mergeJoinOrdering {
		leftGroup := leftOrd.eqGroups.Find(n.pred.leftEqualityIndices[col.ColIdx])
		info.ordering[i].ColIdx = leftCol(leftGroup)
		info.ordering[i].Direction = col.Direction
	}
	info.ordering = info.reduce(info.ordering)
	return info
}

// interleavedNodes returns the ancestor on which an interleaved join is
// defined as well as the descendants of this ancestor which participate in
// the join. One of the left/right scan nodes is the ancestor and the other
// descendant. Nils are returned if there is no interleaved relationship.
// TODO(richardwu): For sibling joins, both left and right tables are
// "descendants" while the ancestor is some common ancestor. We will need to
// probably return descendants as a slice.
//
// An interleaved join has an equality on some columns of the interleave prefix.
// The "interleaved join ancestor" is the ancestor which contains all these
// join columns in its primary key.
// TODO(richardwu): For prefix/subset joins, this ancestor will be the furthest
// ancestor down the interleaved hierarchy which contains all the columns of
// the maximal join prefix (see maximalJoinPrefix in distsql_join.go).
func (n *joinNode) interleavedNodes() (ancestor *scanNode, descendant *scanNode) {
	leftScan, leftOk := n.left.plan.(*scanNode)
	rightScan, rightOk := n.right.plan.(*scanNode)

	if !leftOk || !rightOk {
		return nil, nil
	}

	leftAncestors := leftScan.index.Interleave.Ancestors
	rightAncestors := rightScan.index.Interleave.Ancestors

	// A join between an ancestor and descendant: the descendant of the two
	// tables must have have more interleaved ancestors than the other,
	// which makes the other node the potential interleaved ancestor.
	// TODO(richardwu): The general case where we can have a join
	// on a common ancestor's primary key requires traversing both
	// ancestor slices.
	if len(leftAncestors) > len(rightAncestors) {
		ancestor = rightScan
		descendant = leftScan
	} else {
		ancestor = leftScan
		descendant = rightScan
	}

	// We check the ancestors of the potential descendant to see if any of
	// them match the potential ancestor.
	for _, descAncestor := range descendant.index.Interleave.Ancestors {
		if descAncestor.TableID == ancestor.desc.ID && descAncestor.IndexID == ancestor.index.ID {
			// If the tables are indeed interleaved, then we return
			// the potentials as confirmed ancestor-descendant.
			return ancestor, descendant
		}
	}

	// We could not establish an ancestor-descendant relationship so we
	// return nils for both.
	return nil, nil
}
