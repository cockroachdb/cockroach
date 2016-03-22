// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package sql

import (
	"container/heap"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
)

// orderBy constructs a sortNode based on the ORDER BY clause.
//
// In the general case (SELECT/UNION/VALUES), we can sort by a column index or a
// column name.
//
// However, for a SELECT, we can also sort by the pre-alias column name (SELECT
// a AS b ORDER BY b) as well as expressions (SELECT a, b, ORDER BY a+b). In
// this case, construction of the sortNode might adjust the number of render
// targets in the selectNode if any ordering expressions are specified.
//
// TODO(dan): SQL also allows sorting a VALUES or UNION by an expression.
// Support this. It will reduce some of the special casing below, but requires a
// generalization of how to add derived columns to a SelectStatement.
func (p *planner) orderBy(orderBy parser.OrderBy, n planNode) (*sortNode, *roachpb.Error) {
	if orderBy == nil {
		return nil, nil
	}

	// We grab a copy of columns here because we might add new render targets
	// below. This is the set of columns requested by the query.
	columns := n.Columns()
	numOriginalCols := len(columns)
	if s, ok := n.(*selectNode); ok {
		numOriginalCols = s.numOriginalCols
	}
	var ordering columnOrdering

	for _, o := range orderBy {
		index := -1

		// Normalize the expression which has the side-effect of evaluating
		// constant expressions and unwrapping expressions like "((a))" to "a".
		expr, err := p.parser.NormalizeExpr(p.evalCtx, o.Expr)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		if qname, ok := expr.(*parser.QualifiedName); ok {
			if len(qname.Indirect) == 0 {
				// Look for an output column that matches the qualified name. This
				// handles cases like:
				//
				//   SELECT a AS b FROM t ORDER BY b
				target := NormalizeName(string(qname.Base))
				for j, col := range columns {
					if NormalizeName(col.Name) == target {
						index = j
						break
					}
				}
			}

			if s, ok := n.(*selectNode); ok && index == -1 {
				// No output column matched the qualified name, so look for an existing
				// render target that matches the column name. This handles cases like:
				//
				//   SELECT a AS b FROM t ORDER BY a
				if err := qname.NormalizeColumnName(); err != nil {
					return nil, roachpb.NewError(err)
				}
				if qname.Table() == "" || equalName(s.table.alias, qname.Table()) {
					qnameCol := NormalizeName(qname.Column())
					for j, r := range s.render {
						if qval, ok := r.(*qvalue); ok {
							if NormalizeName(qval.colRef.get().Name) == qnameCol {
								index = j
								break
							}
						}
					}
				}
			}
		}

		if index == -1 {
			// The order by expression matched neither an output column nor an
			// existing render target.
			if col, err := colIndex(numOriginalCols, expr); err != nil {
				return nil, roachpb.NewError(err)
			} else if col >= 0 {
				index = col
			} else if s, ok := n.(*selectNode); ok {
				// TODO(dan): Once we support VALUES (1), (2) ORDER BY 3*4, this type
				// check goes away.

				// Add a new render expression to use for ordering. This handles cases
				// were the expression is either not a qualified name or is a qualified
				// name that is otherwise not referenced by the query:
				//
				//   SELECT a FROM t ORDER by b
				//   SELECT a, b FROM t ORDER by a+b
				if err := s.addRender(parser.SelectExpr{Expr: expr}); err != nil {
					return nil, err
				}
				index = len(s.columns) - 1
			} else {
				return nil, roachpb.NewErrorf("column %s does not exist", expr)
			}
		}
		direction := encoding.Ascending
		if o.Direction == parser.Descending {
			direction = encoding.Descending
		}
		ordering = append(ordering, columnOrderInfo{index, direction})
	}

	return &sortNode{columns: columns, ordering: ordering}, nil
}

// colIndex takes an expression that refers to a column using an integer, verifies it refers to a
// valid render target and returns the corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first render target "a". The returned index is 0.
func colIndex(numOriginalCols int, expr parser.Expr) (int, error) {
	switch i := expr.(type) {
	case parser.DInt:
		index := int(i)
		if numCols := numOriginalCols; index < 1 || index > numCols {
			return -1, fmt.Errorf("invalid column index: %d not in range [1, %d]", index, numCols)
		}
		return index - 1, nil

	case parser.Datum:
		return -1, fmt.Errorf("non-integer constant column index: %s", expr)

	default:
		// expr doesn't look like a col index (i.e. not a constant).
		return -1, nil
	}
}

type sortNode struct {
	plan     planNode
	columns  []ResultColumn
	ordering columnOrdering
	pErr     *roachpb.Error

	needSort     bool
	sortStrategy sortingStrategy
	valueIter    valueIterator

	explain   explainMode
	debugVals debugValues
}

func (n *sortNode) Columns() []ResultColumn {
	return n.columns
}

func (n *sortNode) Ordering() orderingInfo {
	if n == nil {
		return orderingInfo{}
	}
	return orderingInfo{exactMatchCols: nil, ordering: n.ordering}
}

func (n *sortNode) Values() parser.DTuple {
	// If an ordering expression was used the number of columns in each row might
	// differ from the number of columns requested, so trim the result.
	return n.valueIter.Values()[:len(n.columns)]
}

func (n *sortNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.plan.MarkDebug(mode)
}

func (n *sortNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *sortNode) PErr() *roachpb.Error {
	return n.pErr
}

func (n *sortNode) ExplainPlan() (name, description string, children []planNode) {
	if n.needSort {
		name = "sort"
	} else {
		name = "nosort"
	}

	columns := n.plan.Columns()
	strs := make([]string, len(n.ordering))
	for i, o := range n.ordering {
		prefix := '+'
		if o.direction == encoding.Descending {
			prefix = '-'
		}
		strs[i] = fmt.Sprintf("%c%s", prefix, columns[o.colIdx].Name)
	}
	description = strings.Join(strs, ",")

	switch ss := n.sortStrategy.(type) {
	case *iterativeSortStrategy:
		description = fmt.Sprintf("%s (iterative)", description)
	case *sortTopKStrategy:
		description = fmt.Sprintf("%s (top %d)", description, ss.topK)
	}

	return name, description, []planNode{n.plan}
}

func (n *sortNode) SetLimitHint(numRows int64, soft bool) {
	if !n.needSort {
		// The limit is only useful to the wrapped node if we don't need to sort.
		n.plan.SetLimitHint(numRows, soft)
	} else {
		v := &valuesNode{ordering: n.ordering}
		if soft {
			n.sortStrategy = newIterativeSortStrategy(v)
		} else {
			n.sortStrategy = newSortTopKStrategy(v, numRows)
		}
	}
}

// wrap the supplied planNode with the sortNode if sorting is required.
func (n *sortNode) wrap(plan planNode) planNode {
	if n != nil {
		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		existingOrdering := plan.Ordering()
		if log.V(2) {
			log.Infof("Sort: existing=%d desired=%d", existingOrdering, n.ordering)
		}
		match := computeOrderingMatch(n.ordering, existingOrdering, false)
		if match < len(n.ordering) {
			n.plan = plan
			n.needSort = true
			return n
		}

		if len(n.columns) < len(plan.Columns()) {
			// No sorting required, but we have to strip off the extra render
			// expressions we added.
			n.plan = plan
			return n
		}
	}

	if log.V(2) {
		log.Infof("Sort: no sorting required")
	}
	return plan
}

func (n *sortNode) Next() bool {
	if n.pErr != nil {
		return false
	}

	for n.needSort {
		if v, ok := n.plan.(*valuesNode); ok {
			// The plan we wrap is already a values node. Just sort it.
			v.ordering = n.ordering
			n.sortStrategy = newSortAllStrategy(v)
			n.sortStrategy.Finish()
			n.needSort = false
			break
		} else if n.sortStrategy == nil {
			v := &valuesNode{ordering: n.ordering}
			n.sortStrategy = newSortAllStrategy(v)
		}

		// TODO(andrei): If we're scanning an index with a prefix matching an
		// ordering prefix, we should only accumulate values for equal fields
		// in this prefix, then sort the accumulated chunk and output.
		if !n.plan.Next() {
			n.pErr = n.plan.PErr()
			if n.pErr != nil {
				return false
			}

			n.sortStrategy.Finish()
			n.valueIter = n.sortStrategy
			n.needSort = false
			break
		}

		if n.explain == explainDebug {
			n.debugVals = n.plan.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Pass through non-row debug values.
				return true
			}
		}

		values := n.plan.Values()
		n.sortStrategy.Add(values)

		if n.explain == explainDebug {
			// Emit a "buffered" row.
			n.debugVals.output = debugValueBuffered
			return true
		}
	}

	if n.valueIter == nil {
		n.valueIter = n.plan
	}
	if !n.valueIter.Next() {
		if n.valueIter == n.plan {
			n.pErr = n.plan.PErr()
		}
		return false
	}
	if n.explain == explainDebug {
		n.debugVals = n.valueIter.DebugValues()
	}
	return true
}

// valueIterator provides iterative access to a value source's values and
// debug values. It is a subset of the planNode interface, so all methods
// should conform to the comments expressed in the planNode definition.
type valueIterator interface {
	Next() bool
	Values() parser.DTuple
	DebugValues() debugValues
}

type sortingStrategy interface {
	valueIterator
	// Add adds a single value to the sortingStrategy. It guarantees that
	// if it decided to store the provided value, that it will make a deep
	// copy of it.
	Add(parser.DTuple)
	// Finish terminates the sorting strategy, allowing for postprocessing
	// after all values have been provided to the strategy. The method should
	// not be called more than once, and should only be called after all Add
	// calls have occurred.
	Finish()
}

// sortAllStrategy reads in all values into the wrapped valuesNode and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// The strategy is intended to be used when all values need to be sorted.
type sortAllStrategy struct {
	vNode *valuesNode
}

func newSortAllStrategy(vNode *valuesNode) sortingStrategy {
	return &sortAllStrategy{
		vNode: vNode,
	}
}

func (ss *sortAllStrategy) Add(values parser.DTuple) {
	valuesCopy := make(parser.DTuple, len(values))
	copy(valuesCopy, values)
	ss.vNode.rows = append(ss.vNode.rows, valuesCopy)
}

func (ss *sortAllStrategy) Finish() {
	ss.vNode.SortAll()
}

func (ss *sortAllStrategy) Next() bool {
	return ss.vNode.Next()
}

func (ss *sortAllStrategy) Values() parser.DTuple {
	return ss.vNode.Values()
}

func (ss *sortAllStrategy) DebugValues() debugValues {
	return ss.vNode.DebugValues()
}

// iterativeSortStrategy reads in all values into the wrapped valuesNode
// and turns the underlying slice into a min-heap. It then pops a value
// off of the heap for each call to Next, meaning that it only needs to
// sort the number of values needed, instead of the entire slice. If the
// underlying value source provides n rows and the strategy produce only
// k rows, it has a worst-case time complexity of O(n + k*log(n)) and a
// worst-case space complexity of O(n).
//
// The strategy is intended to be used when an unknown number of values
// need to be sorted, but that most likely not all values need to be sorted.
type iterativeSortStrategy struct {
	vNode      *valuesNode
	lastVal    parser.DTuple
	nextRowIdx int
}

func newIterativeSortStrategy(vNode *valuesNode) sortingStrategy {
	return &iterativeSortStrategy{
		vNode: vNode,
	}
}

func (ss *iterativeSortStrategy) Add(values parser.DTuple) {
	valuesCopy := make(parser.DTuple, len(values))
	copy(valuesCopy, values)
	ss.vNode.rows = append(ss.vNode.rows, valuesCopy)
}

func (ss *iterativeSortStrategy) Finish() {
	ss.vNode.InitMinHeap()
}

func (ss *iterativeSortStrategy) Next() bool {
	if ss.vNode.Len() == 0 {
		return false
	}
	ss.lastVal = ss.vNode.PopValues()
	ss.nextRowIdx++
	return true
}

func (ss *iterativeSortStrategy) Values() parser.DTuple {
	return ss.lastVal
}

func (ss *iterativeSortStrategy) DebugValues() debugValues {
	return debugValues{
		rowIdx: ss.nextRowIdx - 1,
		key:    strconv.Itoa(ss.nextRowIdx - 1),
		value:  ss.lastVal.String(),
		output: debugValueRow,
	}
}

// sortTopKStrategy creates a max-heap in its wrapped valuesNode and keeps
// this heap populated with only the top k values seen. It accomplishes this
// by comparing new values (before the deep copy) with the top of the heap.
// If the new value is less than the current top, the top will be replaced
// and the heap will be fixed. If not, the new value is dropped. When finished,
// all values in the heap are popped, sorting the values correctly in-place.
// It has a worst-case time complexity of O(n*log(k)) and a worst-case space
// complexity of O(k).
//
// The strategy is intended to be used when exactly k values need to be sorted,
// where k is known before sorting begins.
//
// TODO(nvanbenschoten) There are better algorithms that can achieve a sorted
// top k in a worst-case time complexity of O(n + k*log(k)) while maintaining
// a worst-case space complexity of O(k). For instance, the top k can be found
// in linear time, and then this can be sorted in linearithmic time.
type sortTopKStrategy struct {
	vNode *valuesNode
	topK  int64
}

func newSortTopKStrategy(vNode *valuesNode, topK int64) sortingStrategy {
	ss := &sortTopKStrategy{
		vNode: vNode,
		topK:  topK,
	}
	ss.vNode.InitMaxHeap()
	return ss
}

func (ss *sortTopKStrategy) Add(values parser.DTuple) {
	switch {
	case int64(ss.vNode.Len()) < ss.topK:
		// The first k values all go into the max-heap.
		valuesCopy := make(parser.DTuple, len(values))
		copy(valuesCopy, values)

		ss.vNode.PushValues(valuesCopy)
	case ss.vNode.ValuesLess(values, ss.vNode.rows[0]):
		// Once the heap is full, only replace the top
		// value if a new value is less than it. If so
		// replace and fix the heap.
		valuesCopy := make(parser.DTuple, len(values))
		copy(valuesCopy, values)

		ss.vNode.rows[0] = valuesCopy
		heap.Fix(ss.vNode, 0)
	}
}

func (ss *sortTopKStrategy) Finish() {
	// Pop all values in the heap, resulting in the inverted ordering
	// being sorted in reverse. Therefore, the slice is ordered correctly
	// in-place.
	origLen := ss.vNode.Len()
	for ss.vNode.Len() > 0 {
		heap.Pop(ss.vNode)
	}
	ss.vNode.rows = ss.vNode.rows[:origLen]
}

func (ss *sortTopKStrategy) Next() bool {
	return ss.vNode.Next()
}

func (ss *sortTopKStrategy) Values() parser.DTuple {
	return ss.vNode.Values()
}

func (ss *sortTopKStrategy) DebugValues() debugValues {
	return ss.vNode.DebugValues()
}

// TODO(pmattis): If the result set is large, we might need to perform the
// sort on disk. There is no point in doing this while we're buffering the
// entire result set in memory. If/when we start streaming results back to
// the client we should revisit.
//
// type onDiskSortStrategy struct{}
