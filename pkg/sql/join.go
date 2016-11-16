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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)
// Author: Irfan Sharif (irfansharif@cockroachlabs.com)

package sql

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
)

type joinType int
type joinAlgorithm int

const (
	joinTypeInner joinType = iota
	joinTypeLeftOuter
	joinTypeFullOuter
)

const (
	nestedLoopJoin joinAlgorithm = iota
	hashJoin
)

// bucket here is the set of rows for a given group key (comprised of
// columns specified by the join constraints), 'seen' is used to determine if
// there was a matching group (with the same group key) in the opposite stream.
type bucket struct {
	rows []parser.DTuple
	seen bool
}

func (b *bucket) Seen() bool {
	return b.seen
}

func (b *bucket) Rows() []parser.DTuple {
	return b.rows
}

func (b *bucket) MarkSeen() {
	b.seen = true
}

func (b *bucket) AddRow(row parser.DTuple) {
	b.rows = append(b.rows, row)
}

type buckets struct {
	buckets      map[string]*bucket
	memAcc       WrappableMemoryAccount
	rowContainer *RowContainer
}

func (b *buckets) Buckets() map[string]*bucket {
	return b.buckets
}

func (b *buckets) AddRow(s *Session, encoding []byte, row parser.DTuple) error {
	bk, ok := b.buckets[string(encoding)]
	if !ok {
		bk = &bucket{}
	}

	rowCopy, err := b.rowContainer.AddRow(row)
	if err != nil {
		return err
	}
	if err := b.memAcc.Wtxn(s).Grow(sizeOfDTuple); err != nil {
		return err
	}
	bk.AddRow(rowCopy)

	if !ok {
		b.buckets[string(encoding)] = bk
	}
	return nil
}

func (b *buckets) Close(s *Session) {
	b.memAcc.Wtxn(s).Close()
	b.rowContainer.Close()
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
	planner       *planner
	joinType      joinType
	joinAlgorithm joinAlgorithm

	// The data sources.
	left    planDataSource
	right   planDataSource
	swapped bool

	// pred represents the join predicate.
	pred joinPredicate

	// columns contains the metadata for the results of this node.
	columns ResultColumns

	// output contains the last generated row of results from this node.
	output parser.DTuple

	// rightRows contains a copy of all rows from the data source on the
	// right of the join.
	rightRows *valuesNode

	// rightMatched remembers which of the right rows have matched in a
	// full outer join. Not applicable for Hash Joins.
	rightMatched []bool

	// rightIdx indicates the current right row.
	// In a full outer join, the value becomes negative during the last pass
	// searching for unmatched right rows. Not applicable for Hash Joins.
	rightIdx int

	// passedFilter turns to true when the current left row has matched
	// at least one right row. Not applicable for Hash Joins.
	passedFilter bool

	// TODO(irfansharif): Re-write nested loop joins to use the internal buffer
	// instead and avoid all the tricky state management in the outer loop.

	// buffer is our intermediate row store where we effectively 'stash' a batch
	// of results at once, this is then used for subsequent calls to Next() and
	// Values().
	buffer *RowBuffer

	buckets buckets

	// emptyRight contain tuples of NULL values to use on the
	// right for outer joins when the filter fails.
	emptyRight parser.DTuple

	// emptyLeft contains tuples of NULL values to use
	// on the left for full outer joins when the filter fails.
	emptyLeft parser.DTuple

	// explain indicates whether this node is running on behalf of
	// EXPLAIN(DEBUG).
	explain explainMode

	// doneReadingRight is used by debugNext() and DebugValues() when
	// explain == explainDebug.
	doneReadingRight bool
}

// commonColumns returns the names of columns common on the
// right and left sides, for use by NATURAL JOIN.
func commonColumns(left, right *dataSourceInfo) parser.NameList {
	var res parser.NameList
	for _, cLeft := range left.sourceColumns {
		if cLeft.hidden {
			continue
		}
		for _, cRight := range right.sourceColumns {
			if cRight.hidden {
				continue
			}

			if parser.ReNormalizeName(cLeft.Name) == parser.ReNormalizeName(cRight.Name) {
				res = append(res, parser.Name(cLeft.Name))
			}
		}
	}
	return res
}

// makeJoin constructs a planDataSource for a JOIN node.
// The tableInfo field from the left node is taken over (overwritten)
// by the new node.
func (p *planner) makeJoin(
	astJoinType string, left planDataSource, right planDataSource, cond parser.JoinCond,
) (planDataSource, error) {
	leftInfo, rightInfo := left.info, right.info

	swapped := false
	var typ joinType
	switch astJoinType {
	case "JOIN", "INNER JOIN", "CROSS JOIN":
		typ = joinTypeInner
	case "LEFT JOIN":
		typ = joinTypeLeftOuter
	case "RIGHT JOIN":
		left, right = right, left // swap
		typ = joinTypeLeftOuter
		swapped = true
	case "FULL JOIN":
		typ = joinTypeFullOuter
	default:
		return planDataSource{}, errors.Errorf("unsupported JOIN type %T", astJoinType)
	}

	// Check that the same table name is not used on both sides.
	for alias := range right.info.sourceAliases {
		if _, ok := left.info.sourceAliases[alias]; ok {
			t := alias.Table()
			if t == "" {
				return planDataSource{}, errors.New(
					"cannot join columns from multiple anonymous sources (missing AS clause)")
			}
			return planDataSource{}, fmt.Errorf(
				"cannot join columns from the same source name %q (missing AS clause)", t)
		}
	}

	// TODO(irfansharif): How do we choose which algorithm to use? Right
	// now we simply default to hash joins if using NATURAL JOIN or
	// USING, nested loop joins for ON predicates.
	var (
		info      *dataSourceInfo
		pred      joinPredicate
		algorithm joinAlgorithm
		err       error
	)

	if cond == nil {
		pred = &crossPredicate{}
		info, err = concatDataSourceInfos(leftInfo, rightInfo)
	} else {
		switch t := cond.(type) {
		case *parser.OnJoinCond:
			pred, info, err = p.makeOnPredicate(leftInfo, rightInfo, t.Expr)
			algorithm = nestedLoopJoin
		case parser.NaturalJoinCond:
			cols := commonColumns(leftInfo, rightInfo)
			pred, info, err = p.makeUsingPredicate(leftInfo, rightInfo, cols)
			algorithm = hashJoin
		case *parser.UsingJoinCond:
			pred, info, err = p.makeUsingPredicate(leftInfo, rightInfo, t.Cols)
			algorithm = hashJoin
		}
	}
	if err != nil {
		return planDataSource{}, err
	}

	n := &joinNode{
		planner:       p,
		left:          left,
		right:         right,
		joinType:      typ,
		joinAlgorithm: algorithm,
		pred:          pred,
		columns:       info.sourceColumns,
		swapped:       swapped,
	}

	if algorithm == nestedLoopJoin {
		n.rightRows = p.newContainerValuesNode(right.plan.Columns(), 0)
	} else if algorithm == hashJoin {
		n.buffer = &RowBuffer{
			RowContainer: NewRowContainer(p.session.TxnState.makeBoundAccount(), n.Columns(), 0),
		}

		n.buckets = buckets{
			buckets:      make(map[string]*bucket),
			memAcc:       p.session.TxnState.OpenAccount(),
			rowContainer: NewRowContainer(p.session.TxnState.makeBoundAccount(), n.right.plan.Columns(), 0),
		}
	}

	return planDataSource{
		info: info,
		plan: n,
	}, nil
}

// ExplainTypes implements the planNode interface.
func (n *joinNode) ExplainTypes(regTypes func(string, string)) {
	n.pred.explainTypes(regTypes)
}

// SetLimitHint implements the planNode interface.
func (n *joinNode) SetLimitHint(numRows int64, soft bool) {}

// expandPlan implements the planNode interface.
func (n *joinNode) expandPlan() error {
	if err := n.pred.expand(); err != nil {
		return err
	}
	if err := n.left.plan.expandPlan(); err != nil {
		return err
	}
	return n.right.plan.expandPlan()
}

// ExplainPlan implements the planNode interface.
func (n *joinNode) ExplainPlan(v bool) (name, description string, children []planNode) {
	var buf bytes.Buffer
	switch n.joinAlgorithm {
	case nestedLoopJoin:
		buf.WriteString("NESTED LOOP JOIN, ")
	case hashJoin:
		buf.WriteString("HASH JOIN, ")
	}
	switch n.joinType {
	case joinTypeInner:
		jType := "INNER"
		if _, ok := n.pred.(*crossPredicate); ok {
			jType = "CROSS"
		}
		buf.WriteString(jType)
	case joinTypeLeftOuter:
		if !n.swapped {
			buf.WriteString("LEFT OUTER")
		} else {
			buf.WriteString("RIGHT OUTER")
		}
	case joinTypeFullOuter:
		buf.WriteString("FULL OUTER")
	}

	n.pred.format(&buf)

	subplans := []planNode{n.left.plan, n.right.plan}
	if n.swapped {
		subplans[0], subplans[1] = subplans[1], subplans[0]
	}
	if p, ok := n.pred.(*onPredicate); ok {
		subplans = p.p.collectSubqueryPlans(p.filter, subplans)
	}

	return "join", buf.String(), subplans
}

// Columns implements the planNode interface.
func (n *joinNode) Columns() ResultColumns { return n.columns }

// Ordering implements the planNode interface.
func (n *joinNode) Ordering() orderingInfo { return n.left.plan.Ordering() }

// MarkDebug implements the planNode interface.
func (n *joinNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.left.plan.MarkDebug(mode)
	n.right.plan.MarkDebug(mode)
}

// Start implements the planNode interface.
func (n *joinNode) Start() error {
	if err := n.pred.start(); err != nil {
		return err
	}

	if err := n.left.plan.Start(); err != nil {
		return err
	}
	if err := n.right.plan.Start(); err != nil {
		return err
	}

	if n.explain != explainDebug {
		switch n.joinAlgorithm {
		case nestedLoopJoin:
			if err := n.nestedLoopJoinStart(); err != nil {
				return err
			}
		case hashJoin:
			if err := n.hashJoinStart(); err != nil {
				return err
			}
		default:
			panic("unsupported JOIN algorithm")
		}
	}

	// Pre-allocate the space for output rows.
	n.output = make(parser.DTuple, len(n.columns))

	// If needed, pre-allocate left and right rows of NULL tuples for when the
	// join predicate fails to match.
	if n.joinType == joinTypeLeftOuter || n.joinType == joinTypeFullOuter {
		n.emptyRight = make(parser.DTuple, len(n.right.plan.Columns()))
		for i := range n.emptyRight {
			n.emptyRight[i] = parser.DNull
		}
		n.emptyLeft = make(parser.DTuple, len(n.left.plan.Columns()))
		for i := range n.emptyLeft {
			n.emptyLeft[i] = parser.DNull
		}
	}
	// If needed, allocate an array of booleans to remember which
	// right rows have matched.
	if n.joinAlgorithm == nestedLoopJoin && n.joinType == joinTypeFullOuter && n.rightRows != nil {
		n.rightMatched = make([]bool, n.rightRows.rows.Len())
	}

	return nil
}

func (n *joinNode) nestedLoopJoinStart() error {
	// Load all the rows from the right side in memory.
	for {
		hasRow, err := n.right.plan.Next()
		if err != nil {
			return err
		}
		if !hasRow {
			break
		}
		row := n.right.plan.Values()
		if _, err := n.rightRows.rows.AddRow(row); err != nil {
			return err
		}
	}
	if n.rightRows.Len() == 0 {
		n.rightRows.Close()
		n.rightRows = nil
	}
	return nil
}

func (n *joinNode) hashJoinStart() error {
	var scratch []byte
	// Load all the rows from the right side and build our hashmap.
	for {
		hasRow, err := n.right.plan.Next()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		row := n.right.plan.Values()
		encoding, _, err := n.pred.encode(scratch, row, rightSide)
		if err != nil {
			return err
		}

		if err := n.buckets.AddRow(n.planner.session, encoding, row); err != nil {
			return err
		}

		scratch = encoding[:0]
	}
}

func (n *joinNode) debugNext() (bool, error) {
	if !n.doneReadingRight {
		hasRightRow, err := n.right.plan.Next()
		if err != nil {
			return false, err
		}
		if hasRightRow {
			return true, nil
		}
		n.doneReadingRight = true
	}

	return n.left.plan.Next()
}

// Next implements the planNode interface.
func (n *joinNode) Next() (res bool, err error) {
	if n.explain == explainDebug {
		return n.debugNext()
	}
	switch n.joinAlgorithm {
	case nestedLoopJoin:
		return n.nestedLoopJoinNext()
	case hashJoin:
		return n.hashJoinNext()
	default:
		panic("unsupported JOIN algorithm")
	}
}

func (n *joinNode) nestedLoopJoinNext() (bool, error) {
	var leftRow, rightRow parser.DTuple
	var nRightRows int

	if n.rightRows == nil {
		if n.joinType != joinTypeLeftOuter && n.joinType != joinTypeFullOuter {
			// No rows on right; don't even try.
			return false, nil
		}
		nRightRows = 0
	} else {
		nRightRows = n.rightRows.Len()
	}

	// We fetch one row at a time until we find one that passes the filter.
	for {
		curRightIdx := n.rightIdx

		if curRightIdx < 0 {
			// Going through the remaining right row of a full outer join.
			curRightIdx = (-curRightIdx) - 1
			n.rightIdx--
			if curRightIdx < nRightRows {
				if n.rightMatched[curRightIdx] {
					continue
				}
				leftRow = n.emptyLeft
				rightRow = n.rightRows.rows.At(curRightIdx)
				break
			} else {
				// Both right and left exhausted.
				return false, nil
			}
		}

		if curRightIdx == 0 {
			leftHasRow, err := n.left.plan.Next()
			if err != nil {
				return false, err
			}

			if !leftHasRow && n.rightMatched != nil {
				// Go through the remaining right rows.
				n.left.plan.Close()
				n.rightIdx = -1
				continue
			}

			if !leftHasRow {
				// Both left and right are exhausted; done.
				return false, nil
			}
		}

		leftRow = n.left.plan.Values()

		if curRightIdx >= nRightRows {
			n.rightIdx = 0
			if (n.joinType == joinTypeLeftOuter || n.joinType == joinTypeFullOuter) && !n.passedFilter {
				// If nothing was emitted in the previous batch of right rows,
				// insert a tuple of NULLs on the right.
				rightRow = n.emptyRight
				if n.swapped {
					leftRow, rightRow = rightRow, leftRow
				}
				break
			}
			n.passedFilter = false
			continue
		}

		emptyRight := false
		if nRightRows > 0 {
			rightRow = n.rightRows.rows.At(curRightIdx)
			n.rightIdx = curRightIdx + 1
		} else {
			emptyRight = true
			rightRow = n.emptyRight
		}

		if n.swapped {
			leftRow, rightRow = rightRow, leftRow
		}
		passesFilter, err := n.pred.eval(n.output, leftRow, rightRow)
		if err != nil {
			return false, err
		}

		if passesFilter {
			n.passedFilter = true
			if n.rightMatched != nil && !emptyRight {
				// FULL OUTER JOIN, mark the rows as matched.
				n.rightMatched[curRightIdx] = true
			}
			break
		}
	}

	// Got a row from both right and left; prepareRow the result.
	n.pred.prepareRow(n.output, leftRow, rightRow)
	return true, nil
}

// TODO(irfansharif): Currently hash joins are implemented only for USING and
// NATURAL, predicate extraction for ON ignored.
func (n *joinNode) hashJoinNext() (bool, error) {
	if len(n.buckets.Buckets()) == 0 {
		if n.joinType != joinTypeLeftOuter && n.joinType != joinTypeFullOuter {
			// No rows on right; don't even try.
			return false, nil
		}
	}

	// If results available from from previously computed results, we just
	// return true.
	if n.buffer.Next() {
		return true, nil
	}

	// Compute next batch of matching rows.
	var scratch []byte
	for {
		leftHasRow, err := n.left.plan.Next()
		if err != nil {
			return false, nil
		}
		if !leftHasRow {
			break
		}

		lrow := n.left.plan.Values()
		encoding, containsNull, err := n.pred.encode(scratch, lrow, leftSide)
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
			if n.joinType == joinTypeInner {
				scratch = encoding[:0]
				// Failed to match -- no matching row, nothing to do.
				continue
			}
			// We append an empty right row to the left row, adding the result
			// to our buffer for the subsequent call to Next().
			n.pred.prepareRow(n.output, lrow, n.emptyRight)
			if _, err := n.buffer.AddRow(n.output); err != nil {
				return false, err
			}
			return n.buffer.Next(), nil
		}

		b, ok := n.buckets.Fetch(encoding)
		if !ok {
			if n.joinType == joinTypeInner {
				scratch = encoding[:0]
				continue
			}
			// Outer join: unmatched rows are padded with NULLs.
			// Given that we did not find a matching right row we append an
			// empty right row to the left row, adding the result to our buffer
			// for the subsequent call to Next().
			n.pred.prepareRow(n.output, lrow, n.emptyRight)
			if _, err := n.buffer.AddRow(n.output); err != nil {
				return false, err
			}
			return n.buffer.Next(), nil
		}
		b.MarkSeen()

		// We iterate through all the rows in the bucket attempting to match the
		// filter, if the filter passes we add it to the buffer.
		for _, rrow := range b.Rows() {
			passesFilter, err := n.pred.eval(n.output, lrow, rrow)
			if err != nil {
				return false, err
			}

			if !passesFilter {
				scratch = encoding[:0]
				continue
			}

			n.pred.prepareRow(n.output, lrow, rrow)
			if _, err := n.buffer.AddRow(n.output); err != nil {
				return false, err
			}
		}
		if n.buffer.Next() {
			return true, nil
		}
	}

	// no more lrows, we go through the unmatched rows in the internal hashmap.
	if n.joinType != joinTypeFullOuter {
		return false, nil
	}

	for _, b := range n.buckets.Buckets() {
		if !b.Seen() {
			for _, rrow := range b.Rows() {
				n.pred.prepareRow(n.output, n.emptyLeft, rrow)
				if _, err := n.buffer.AddRow(n.output); err != nil {
					return false, err
				}
			}
			b.MarkSeen()
		}
	}

	return n.buffer.Next(), nil
}

// Values implements the planNode interface.
func (n *joinNode) Values() parser.DTuple {
	switch n.joinAlgorithm {
	case nestedLoopJoin:
		return n.output
	case hashJoin:
		return n.buffer.Values()
	default:
		panic("unsupported JOIN algorithm")
	}
}

// DebugValues implements the planNode interface.
func (n *joinNode) DebugValues() debugValues {
	var res debugValues
	if !n.doneReadingRight {
		res = n.right.plan.DebugValues()
	} else {
		res = n.left.plan.DebugValues()
	}
	if res.output == debugValueRow {
		res.output = debugValueBuffered
	}
	return res
}

// Close implements the planNode interface.
func (n *joinNode) Close() {
	switch n.joinAlgorithm {
	case nestedLoopJoin:
		if n.rightRows != nil {
			n.rightRows.Close()
			n.rightRows = nil
		}
		n.rightMatched = nil
	case hashJoin:
		n.buffer.Close()
		n.buffer = nil
		n.buckets.Close(n.planner.session)
	default:
		panic("unsupported JOIN algorithm")
	}
	n.right.plan.Close()
	n.left.plan.Close()
}
