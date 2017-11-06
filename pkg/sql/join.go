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
	"fmt"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

type joinType int

const (
	joinTypeInner joinType = iota
	joinTypeLeftOuter
	joinTypeRightOuter
	joinTypeFullOuter
)

// bucket here is the set of rows for a given group key (comprised of
// columns specified by the join constraints), 'seen' is used to determine if
// there was a matching row in the opposite stream.
type bucket struct {
	rows []parser.Datums
	seen []bool
}

func (b *bucket) Seen(i int) bool {
	return b.seen[i]
}

func (b *bucket) Rows() []parser.Datums {
	return b.rows
}

func (b *bucket) MarkSeen(i int) {
	b.seen[i] = true
}

func (b *bucket) AddRow(row parser.Datums) {
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
	ctx context.Context, acc WrappedMemoryAccount, encoding []byte, row parser.Datums,
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
func (b *buckets) InitSeen(ctx context.Context, acc WrappedMemoryAccount) error {
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

// joinNode is a planNode whose rows are the result of an inner or
// left/right outer join.
type joinNode struct {
	planner  *planner
	joinType joinType

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

	// output contains the last generated row of results from this node.
	output parser.Datums

	// buffer is our intermediate row store where we effectively 'stash' a batch
	// of results at once, this is then used for subsequent calls to Next() and
	// Values().
	buffer *RowBuffer

	buckets       buckets
	bucketsMemAcc WrappableMemoryAccount

	// emptyRight contain tuples of NULL values to use on the right for left and
	// full outer joins when the on condition fails.
	emptyRight parser.Datums

	// emptyLeft contains tuples of NULL values to use on the left for right and
	// full outer joins when the on condition fails.
	emptyLeft parser.Datums

	// finishedOutput indicates that we've finished writing all of the rows for
	// this join and that we can quit as soon as our buffer is empty.
	finishedOutput bool
}

// commonColumns returns the names of columns common on the
// right and left sides, for use by NATURAL JOIN.
func commonColumns(left, right *dataSourceInfo) parser.NameList {
	var res parser.NameList
	for _, cLeft := range left.sourceColumns {
		if cLeft.Hidden {
			continue
		}
		for _, cRight := range right.sourceColumns {
			if cRight.Hidden {
				continue
			}

			if cLeft.Name == cRight.Name {
				res = append(res, parser.Name(cLeft.Name))
			}
		}
	}
	return res
}

// makeJoin constructs a planDataSource for a JOIN.
// The source might be a joinNode, or it could be a renderNode on top of a
// joinNode (in the case of outer natural joins).
func (p *planner) makeJoin(
	ctx context.Context,
	astJoinType string,
	left planDataSource,
	right planDataSource,
	cond parser.JoinCond,
) (planDataSource, error) {
	var typ joinType
	switch astJoinType {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		typ = joinTypeInner
	case "LEFT JOIN":
		typ = joinTypeLeftOuter
	case "RIGHT JOIN":
		typ = joinTypeRightOuter
	case "FULL JOIN":
		typ = joinTypeFullOuter
	default:
		return planDataSource{}, errors.Errorf("unsupported JOIN type %T", astJoinType)
	}

	leftInfo, rightInfo := left.info, right.info

	// Check that the same table name is not used on both sides.
	for _, alias := range rightInfo.sourceAliases {
		if _, ok := leftInfo.sourceAliases.srcIdx(alias.name); ok {
			t := alias.name.Table()
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

	var (
		info          *dataSourceInfo
		pred          *joinPredicate
		err           error
		mergedColumns parser.NameList
	)

	if cond == nil {
		pred, info, err = makeCrossPredicate(typ, leftInfo, rightInfo)
	} else {
		switch t := cond.(type) {
		case *parser.OnJoinCond:
			pred, info, err = p.makeOnPredicate(ctx, typ, leftInfo, rightInfo, t.Expr)
		case parser.NaturalJoinCond:
			cols := commonColumns(leftInfo, rightInfo)
			mergedColumns = cols
			pred, info, err = makeUsingPredicate(typ, leftInfo, rightInfo, mergedColumns)
		case *parser.UsingJoinCond:
			mergedColumns = t.Cols
			pred, info, err = makeUsingPredicate(typ, leftInfo, rightInfo, mergedColumns)
		}
	}
	if err != nil {
		return planDataSource{}, err
	}

	n := &joinNode{
		planner:  p,
		left:     left,
		right:    right,
		joinType: typ,
		pred:     pred,
		columns:  info.sourceColumns,
	}

	n.buffer = &RowBuffer{
		RowContainer: sqlbase.NewRowContainer(
			p.session.TxnState.makeBoundAccount(), sqlbase.ColTypeInfoFromResCols(planColumns(n)), 0,
		),
	}

	n.bucketsMemAcc = p.session.TxnState.OpenAccount()
	n.buckets = buckets{
		buckets: make(map[string]*bucket),
		rowContainer: sqlbase.NewRowContainer(
			p.session.TxnState.makeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(planColumns(n.right.plan)),
			0,
		),
	}

	joinDataSource := planDataSource{info: info, plan: n}

	if mergedColumns == nil {
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
		planner:    p,
		source:     joinDataSource,
		sourceInfo: multiSourceInfo{info},
	}
	r.ivarHelper = parser.MakeIndexedVarHelper(r, len(info.sourceColumns))
	numLeft := len(leftInfo.sourceColumns)
	numRight := len(rightInfo.sourceColumns)
	rInfo := &dataSourceInfo{
		sourceAliases: make(sourceAliases, 0, len(info.sourceAliases)),
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
	for i := range mergedColumns {
		leftCol := n.pred.leftEqualityIndices[i]
		rightCol := n.pred.rightEqualityIndices[i]
		leftHidden.Add(leftCol)
		rightHidden.Add(rightCol)
		var expr parser.TypedExpr
		if n.joinType == joinTypeInner || n.joinType == joinTypeLeftOuter {
			// The merged column is the same with the corresponding column from the
			// left side.
			expr = r.ivarHelper.IndexedVar(leftCol)
			remapped[leftCol] = i
		} else if n.joinType == joinTypeRightOuter &&
			!sqlbase.DatumTypeHasCompositeKeyEncoding(leftInfo.sourceColumns[leftCol].Typ) {
			// The merged column is the same with the corresponding column from the
			// right side.
			expr = r.ivarHelper.IndexedVar(numLeft + rightCol)
			remapped[numLeft+rightCol] = i
		} else {
			c := &parser.CoalesceExpr{
				Name: "IFNULL",
				Exprs: []parser.Expr{
					r.ivarHelper.IndexedVar(leftCol),
					r.ivarHelper.IndexedVar(numLeft + rightCol),
				},
			}
			var err error
			expr, err = c.TypeCheck(&p.semaCtx, types.TypeAny)
			if err != nil {
				return planDataSource{}, err
			}
		}
		r.addRenderColumn(expr, symbolicExprStr(expr), leftInfo.sourceColumns[leftCol])
	}
	for i, c := range leftInfo.sourceColumns {
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
	for i, c := range rightInfo.sourceColumns {
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
	rInfo.sourceColumns = r.columns

	// Copy the aliases, remapping the columns as necessary. We extract any
	// anonymous aliases for special handling.
	anonymousAlias := sourceAlias{name: anonymousTable}
	for _, a := range info.sourceAliases {
		var colSet util.FastIntSet
		for col, ok := a.columnSet.Next(0); ok; col, ok = a.columnSet.Next(col + 1) {
			colSet.Add(remapped[col])
		}
		if a.name == anonymousTable {
			anonymousAlias.columnSet = colSet
			continue
		}
		rInfo.sourceAliases = append(rInfo.sourceAliases, sourceAlias{name: a.name, columnSet: colSet})
	}

	for i := range mergedColumns {
		anonymousAlias.columnSet.Add(i)
	}

	// Remove any anonymous aliases that refer to hidden equality columns (i.e.
	// those that weren't equivalent to the merged column).
	for i, col := range n.pred.leftEqualityIndices {
		if target := remapped[col]; target != i {
			anonymousAlias.columnSet.Remove(target)
		}
	}
	for i, col := range n.pred.rightEqualityIndices {
		if target := remapped[numLeft+col]; target != i {
			anonymousAlias.columnSet.Remove(remapped[numLeft+col])
		}
	}
	rInfo.sourceAliases = append(rInfo.sourceAliases, anonymousAlias)
	return planDataSource{info: rInfo, plan: r}, nil
}

// Start implements the planNode interface.
func (n *joinNode) Start(params runParams) error {
	if err := n.left.plan.Start(params); err != nil {
		return err
	}
	if err := n.right.plan.Start(params); err != nil {
		return err
	}

	if err := n.hashJoinStart(params); err != nil {
		return err
	}

	// Pre-allocate the space for output rows.
	n.output = make(parser.Datums, len(n.columns))

	// If needed, pre-allocate left and right rows of NULL tuples for when the
	// join predicate fails to match.
	if n.joinType == joinTypeLeftOuter || n.joinType == joinTypeFullOuter {
		n.emptyRight = make(parser.Datums, len(planColumns(n.right.plan)))
		for i := range n.emptyRight {
			n.emptyRight[i] = parser.DNull
		}
	}
	if n.joinType == joinTypeRightOuter || n.joinType == joinTypeFullOuter {
		n.emptyLeft = make(parser.Datums, len(planColumns(n.left.plan)))
		for i := range n.emptyLeft {
			n.emptyLeft[i] = parser.DNull
		}
	}

	return nil
}

func (n *joinNode) hashJoinStart(params runParams) error {
	var scratch []byte
	// Load all the rows from the right side and build our hashmap.
	acc := n.bucketsMemAcc.Wtxn(n.planner.session)
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

		if err := n.buckets.AddRow(ctx, acc, encoding, row); err != nil {
			return err
		}

		scratch = encoding[:0]
	}
	if n.joinType == joinTypeFullOuter || n.joinType == joinTypeRightOuter {
		return n.buckets.InitSeen(ctx, acc)
	}
	return nil
}

// Next implements the planNode interface.
func (n *joinNode) Next(params runParams) (res bool, err error) {
	// If results available from from previously computed results, we just
	// return true.
	if n.buffer.Next() {
		return true, nil
	}

	// If the buffer is empty and we've finished outputting, we're done.
	if n.finishedOutput {
		return false, nil
	}

	wantUnmatchedLeft := n.joinType == joinTypeLeftOuter || n.joinType == joinTypeFullOuter
	wantUnmatchedRight := n.joinType == joinTypeRightOuter || n.joinType == joinTypeFullOuter

	if len(n.buckets.Buckets()) == 0 {
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
			return false, nil
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
		// The following examples illustrates the behaviour when joining on two
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
			n.pred.prepareRow(n.output, lrow, n.emptyRight)
			if _, err := n.buffer.AddRow(params.ctx, n.output); err != nil {
				return false, err
			}
			return n.buffer.Next(), nil
		}

		b, ok := n.buckets.Fetch(encoding)
		if !ok {
			if !wantUnmatchedLeft {
				scratch = encoding[:0]
				continue
			}
			// Left or full outer join: unmatched rows are padded with NULLs.
			// Given that we did not find a matching right row we append an
			// empty right row to the left row, adding the result to our buffer
			// for the subsequent call to Next().
			n.pred.prepareRow(n.output, lrow, n.emptyRight)
			if _, err := n.buffer.AddRow(params.ctx, n.output); err != nil {
				return false, err
			}
			return n.buffer.Next(), nil
		}

		// We iterate through all the rows in the bucket attempting to match the
		// on condition, if the on condition passes we add it to the buffer.
		foundMatch := false
		for idx, rrow := range b.Rows() {
			passesOnCond, err := n.pred.eval(&n.planner.evalCtx, n.output, lrow, rrow)
			if err != nil {
				return false, err
			}

			if !passesOnCond {
				continue
			}
			foundMatch = true

			n.pred.prepareRow(n.output, lrow, rrow)
			if wantUnmatchedRight {
				// Mark the row as seen if we need to retrieve the rows
				// without matches for right or full joins later.
				b.MarkSeen(idx)
			}
			if _, err := n.buffer.AddRow(params.ctx, n.output); err != nil {
				return false, err
			}
		}
		if !foundMatch && wantUnmatchedLeft {
			// If none of the rows matched the on condition and we are computing a
			// left or full outer join, we need to add a row with an empty
			// right side.
			n.pred.prepareRow(n.output, lrow, n.emptyRight)
			if _, err := n.buffer.AddRow(params.ctx, n.output); err != nil {
				return false, err
			}
		}
		if n.buffer.Next() {
			return true, nil
		}
		scratch = encoding[:0]
	}

	// no more lrows, we go through the unmatched rows in the internal hashmap.
	if !wantUnmatchedRight {
		return false, nil
	}

	for _, b := range n.buckets.Buckets() {
		for idx, rrow := range b.Rows() {
			if err := params.p.cancelChecker.Check(); err != nil {
				return false, err
			}
			if !b.Seen(idx) {
				n.pred.prepareRow(n.output, n.emptyLeft, rrow)
				if _, err := n.buffer.AddRow(params.ctx, n.output); err != nil {
					return false, err
				}
			}
		}
	}
	n.finishedOutput = true

	return n.buffer.Next(), nil
}

// Values implements the planNode interface.
func (n *joinNode) Values() parser.Datums {
	return n.buffer.Values()
}

// Close implements the planNode interface.
func (n *joinNode) Close(ctx context.Context) {
	n.buffer.Close(ctx)
	n.buffer = nil
	n.buckets.Close(ctx)
	n.bucketsMemAcc.Wtxn(n.planner.session).Close(ctx)

	n.right.plan.Close(ctx)
	n.left.plan.Close(ctx)
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
	if n.joinType != joinTypeInner {
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
