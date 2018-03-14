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
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
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

	run joinRun
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

	n.run.buffer = &RowBuffer{
		RowContainer: sqlbase.NewRowContainer(
			p.EvalContext().Mon.MakeBoundAccount(), sqlbase.ColTypeInfoFromResCols(planColumns(n)), 0,
		),
	}

	n.run.bucketsMemAcc = p.EvalContext().Mon.MakeBoundAccount()
	n.run.buckets = buckets{
		buckets: make(map[string]*bucket),
		rowContainer: sqlbase.NewRowContainer(
			p.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(planColumns(n.right.plan)),
			0,
		),
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
			return planDataSource{}, fmt.Errorf(
				"cannot join columns from the same source name %q (missing AS clause)", t)
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

// joinRun contains the run-time state of joinNode during local execution.
type joinRun struct {
	// output contains the last generated row of results from this node.
	output tree.Datums

	// buffer is our intermediate row store where we effectively 'stash' a batch
	// of results at once, this is then used for subsequent calls to Next() and
	// Values().
	buffer *RowBuffer

	buckets       buckets
	bucketsMemAcc mon.BoundAccount

	// emptyRight contain tuples of NULL values to use on the right for left and
	// full outer joins when the on condition fails.
	emptyRight tree.Datums

	// emptyLeft contains tuples of NULL values to use on the left for right and
	// full outer joins when the on condition fails.
	emptyLeft tree.Datums

	// finishedOutput indicates that we've finished writing all of the rows for
	// this join and that we can quit as soon as our buffer is empty.
	finishedOutput bool
}

func (n *joinNode) startExec(params runParams) error {
	if err := n.hashJoinStart(params); err != nil {
		return err
	}

	// Pre-allocate the space for output rows.
	n.run.output = make(tree.Datums, len(n.columns))

	// If needed, pre-allocate left and right rows of NULL tuples for when the
	// join predicate fails to match.
	if n.joinType == sqlbase.LeftOuterJoin || n.joinType == sqlbase.FullOuterJoin {
		n.run.emptyRight = make(tree.Datums, len(planColumns(n.right.plan)))
		for i := range n.run.emptyRight {
			n.run.emptyRight[i] = tree.DNull
		}
	}
	if n.joinType == sqlbase.RightOuterJoin || n.joinType == sqlbase.FullOuterJoin {
		n.run.emptyLeft = make(tree.Datums, len(planColumns(n.left.plan)))
		for i := range n.run.emptyLeft {
			n.run.emptyLeft[i] = tree.DNull
		}
	}

	return nil
}

func (n *joinNode) hashJoinStart(params runParams) error {
	var scratch []byte
	// Load all the rows from the right side and build our hashmap.
	ctx := params.ctx
	for {
		hasRow, err := n.right.plan.Next(params)
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
		row := n.right.plan.Values()
		encoding, _, err := n.pred.encode(scratch, row, n.pred.rightEqualityIndices)
		if err != nil {
			return err
		}

		if err := n.run.buckets.AddRow(ctx, &n.run.bucketsMemAcc, encoding, row); err != nil {
			return err
		}

		scratch = encoding[:0]
	}
	if n.joinType == sqlbase.FullOuterJoin || n.joinType == sqlbase.RightOuterJoin {
		return n.run.buckets.InitSeen(ctx, &n.run.bucketsMemAcc)
	}
	return nil
}

// Next implements the planNode interface.
func (n *joinNode) Next(params runParams) (res bool, err error) {
	// If results available from from previously computed results, we just
	// return true.
	if n.run.buffer.Next() {
		return true, nil
	}

	// If the buffer is empty and we've finished outputting, we're done.
	if n.run.finishedOutput {
		return false, nil
	}

	wantUnmatchedLeft := n.joinType == sqlbase.LeftOuterJoin || n.joinType == sqlbase.FullOuterJoin
	wantUnmatchedRight := n.joinType == sqlbase.RightOuterJoin || n.joinType == sqlbase.FullOuterJoin

	if len(n.run.buckets.Buckets()) == 0 {
		if !wantUnmatchedLeft {
			// No rows on right; don't even try.
			return false, nil
		}
	}

	// Compute next batch of matching rows.
	var scratch []byte
	for {
		if err := params.p.cancelChecker.Check(); err != nil {
			return false, err
		}

		leftHasRow, err := n.left.plan.Next(params)
		if err != nil {
			return false, err
		}
		if !leftHasRow {
			break
		}

		lrow := n.left.plan.Values()
		encoding, containsNull, err := n.pred.encode(scratch, lrow, n.pred.leftEqualityIndices)
		if err != nil {
			return false, err
		}

		// We make the explicit check for whether or not lrow contained a NULL
		// tuple. The reasoning here is because of the way we expect NULL
		// equality checks to behave (i.e. NULL != NULL) and the fact that we
		// use the encoding of any given row as key into our bucket. Thus if we
		// encountered a NULL row when building the hashmap we have to store in
		// order to use it for RIGHT OUTER joins but if we encounter another
		// NULL row when going through the left stream (probing phase), matching
		// this with the first NULL row would be incorrect.
		//
		// If we have have the following:
		// CREATE TABLE t(x INT); INSERT INTO t(x) VALUES (NULL);
		//    |  x   |
		//     ------
		//    | NULL |
		//
		// For the following query:
		// SELECT * FROM t AS a FULL OUTER JOIN t AS b USING(x);
		//
		// We expect:
		//    |  x   |
		//     ------
		//    | NULL |
		//    | NULL |
		//
		// The following examples illustrates the behavior when joining on two
		// or more columns, and only one of them contains NULL.
		// If we have have the following:
		// CREATE TABLE t(x INT, y INT);
		// INSERT INTO t(x, y) VALUES (44,51), (NULL,52);
		//    |  x   |  y   |
		//     ------
		//    |  44  |  51  |
		//    | NULL |  52  |
		//
		// For the following query:
		// SELECT * FROM t AS a FULL OUTER JOIN t AS b USING(x, y);
		//
		// We expect:
		//    |  x   |  y   |
		//     ------
		//    |  44  |  51  |
		//    | NULL |  52  |
		//    | NULL |  52  |
		if containsNull {
			if !wantUnmatchedLeft {
				scratch = encoding[:0]
				// Failed to match -- no matching row, nothing to do.
				continue
			}
			// We append an empty right row to the left row, adding the result
			// to our buffer for the subsequent call to Next().
			n.pred.prepareRow(n.run.output, lrow, n.run.emptyRight)
			if _, err := n.run.buffer.AddRow(params.ctx, n.run.output); err != nil {
				return false, err
			}
			return n.run.buffer.Next(), nil
		}

		b, ok := n.run.buckets.Fetch(encoding)
		if !ok {
			if !wantUnmatchedLeft {
				scratch = encoding[:0]
				continue
			}
			// Left or full outer join: unmatched rows are padded with NULLs.
			// Given that we did not find a matching right row we append an
			// empty right row to the left row, adding the result to our buffer
			// for the subsequent call to Next().
			n.pred.prepareRow(n.run.output, lrow, n.run.emptyRight)
			if _, err := n.run.buffer.AddRow(params.ctx, n.run.output); err != nil {
				return false, err
			}
			return n.run.buffer.Next(), nil
		}

		// We iterate through all the rows in the bucket attempting to match the
		// on condition, if the on condition passes we add it to the buffer.
		foundMatch := false
		for idx, rrow := range b.Rows() {
			passesOnCond, err := n.pred.eval(params.EvalContext(), n.run.output, lrow, rrow)
			if err != nil {
				return false, err
			}

			if !passesOnCond {
				continue
			}
			foundMatch = true

			n.pred.prepareRow(n.run.output, lrow, rrow)
			if wantUnmatchedRight {
				// Mark the row as seen if we need to retrieve the rows
				// without matches for right or full joins later.
				b.MarkSeen(idx)
			}
			if _, err := n.run.buffer.AddRow(params.ctx, n.run.output); err != nil {
				return false, err
			}
		}
		if !foundMatch && wantUnmatchedLeft {
			// If none of the rows matched the on condition and we are computing a
			// left or full outer join, we need to add a row with an empty
			// right side.
			n.pred.prepareRow(n.run.output, lrow, n.run.emptyRight)
			if _, err := n.run.buffer.AddRow(params.ctx, n.run.output); err != nil {
				return false, err
			}
		}
		if n.run.buffer.Next() {
			return true, nil
		}
		scratch = encoding[:0]
	}

	// no more lrows, we go through the unmatched rows in the internal hashmap.
	if !wantUnmatchedRight {
		return false, nil
	}

	for _, b := range n.run.buckets.Buckets() {
		for idx, rrow := range b.Rows() {
			if err := params.p.cancelChecker.Check(); err != nil {
				return false, err
			}
			if !b.Seen(idx) {
				n.pred.prepareRow(n.run.output, n.run.emptyLeft, rrow)
				if _, err := n.run.buffer.AddRow(params.ctx, n.run.output); err != nil {
					return false, err
				}
			}
		}
	}
	n.run.finishedOutput = true

	return n.run.buffer.Next(), nil
}

// Values implements the planNode interface.
func (n *joinNode) Values() tree.Datums {
	return n.run.buffer.Values()
}

// Close implements the planNode interface.
func (n *joinNode) Close(ctx context.Context) {
	n.run.buffer.Close(ctx)
	n.run.buffer = nil
	n.run.buckets.Close(ctx)
	n.run.bucketsMemAcc.Close(ctx)

	n.right.plan.Close(ctx)
	n.left.plan.Close(ctx)
}

// bucket here is the set of rows for a given group key (comprised of
// columns specified by the join constraints), 'seen' is used to determine if
// there was a matching row in the opposite stream.
type bucket struct {
	rows []tree.Datums
	seen []bool
}

func (b *bucket) Seen(i int) bool {
	return b.seen[i]
}

func (b *bucket) Rows() []tree.Datums {
	return b.rows
}

func (b *bucket) MarkSeen(i int) {
	b.seen[i] = true
}

func (b *bucket) AddRow(row tree.Datums) {
	b.rows = append(b.rows, row)
}

type buckets struct {
	buckets      map[string]*bucket
	rowContainer *sqlbase.RowContainer
}

func (b *buckets) Buckets() map[string]*bucket {
	return b.buckets
}

func (b *buckets) AddRow(
	ctx context.Context, acc *mon.BoundAccount, encoding []byte, row tree.Datums,
) error {
	bk, ok := b.buckets[string(encoding)]
	if !ok {
		bk = &bucket{}
	}

	rowCopy, err := b.rowContainer.AddRow(ctx, row)
	if err != nil {
		return err
	}
	if err := acc.Grow(ctx, sqlbase.SizeOfDatums); err != nil {
		return err
	}
	bk.AddRow(rowCopy)

	if !ok {
		b.buckets[string(encoding)] = bk
	}
	return nil
}

const sizeOfBoolSlice = unsafe.Sizeof([]bool{})
const sizeOfBool = unsafe.Sizeof(true)

// InitSeen initializes the seen array for each of the buckets. It must be run
// before the buckets' seen state is used.
func (b *buckets) InitSeen(ctx context.Context, acc *mon.BoundAccount) error {
	for _, bucket := range b.buckets {
		if err := acc.Grow(
			ctx, int64(sizeOfBoolSlice+uintptr(len(bucket.rows))*sizeOfBool),
		); err != nil {
			return err
		}
		bucket.seen = make([]bool, len(bucket.rows))
	}
	return nil
}

func (b *buckets) Close(ctx context.Context) {
	b.rowContainer.Close(ctx)
	b.rowContainer = nil
	b.buckets = nil
}

func (b *buckets) Fetch(encoding []byte) (*bucket, bool) {
	bk, ok := b.buckets[string(encoding)]
	return bk, ok
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
