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
	"bytes"
	"container/heap"
	"fmt"
	"math"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// sortNode represents a node that sorts the rows returned by its
// sub-node.
type sortNode struct {
	ctx      context.Context
	p        *planner
	plan     planNode
	columns  ResultColumns
	ordering sqlbase.ColumnOrdering

	needSort     bool
	sortStrategy sortingStrategy
	valueIter    valueIterator

	explain   explainMode
	debugVals debugValues
}

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
func (p *planner) orderBy(orderBy parser.OrderBy, n planNode) (*sortNode, error) {
	if orderBy == nil {
		return nil, nil
	}

	// Multiple tests below use selectNode as a special case.
	// So factor the cast.
	s, _ := n.(*selectNode)

	// We grab a copy of columns here because we might add new render targets
	// below. This is the set of columns requested by the query.
	columns := n.Columns()
	numOriginalCols := len(columns)
	if s != nil {
		numOriginalCols = s.numOriginalCols
	}
	var ordering sqlbase.ColumnOrdering

	for _, o := range orderBy {
		direction := encoding.Ascending
		if o.Direction == parser.Descending {
			direction = encoding.Descending
		}

		index := -1

		// Unwrap parenthesized expressions like "((a))" to "a".
		expr := parser.StripParens(o.Expr)

		// The logical data source for ORDER BY is the list of render
		// expressions for a SELECT (or an entire UNION or VALUES clause).
		// Alas, SQL has some historical baggage and there is some
		// special case:
		//
		// 1) column ordinals. If a simple integer literal is used,
		//    optionally enclosed within parentheses but *not subject to
		//    any arithmetic*, then this refers to one of the columns of
		//    the data source. Then use the render expression at that
		//    ordinal position as sort key.
		//
		// 2) if the expression is the aliased (AS) name of a render
		//    expression in a SELECT clause, then use that
		//    render as sort key.
		//    e.g. SELECT a AS b, b AS c ORDER BY b
		//    this sorts on the first render.
		//
		// 3) otherwise, if the expression is already rendered,
		//    then use that render as sort key.
		//    e.g. SELECT b AS c ORDER BY b
		//    this sorts on the first render.
		//    (this is an optimization)
		//
		// 4) if the sort key is not dependent on the data source (no IndexedVar)
		//    then simply do not sort.
		//    (this is an optimization)
		//
		// 5) otherwise, add a new render with the ORDER BY expression
		//    and use that as sort key.
		//    e.g. SELECT a FROM t ORDER by b
		//    e.g. SELECT a, b FROM t ORDER by a+b
		//
		// So first, deal with column ordinals.
		col, err := p.colIndex(numOriginalCols, expr, "ORDER BY")
		if err != nil {
			return nil, err
		}
		if col != -1 {
			index = col
		}

		// Now, deal with render aliases.
		if vBase, ok := expr.(parser.VarName); index == -1 && ok {
			v, err := vBase.NormalizeVarName()
			if err != nil {
				return nil, err
			}

			if c, ok := v.(*parser.ColumnItem); ok && c.TableName.Table() == "" {
				// Look for an output column that matches the name. This
				// handles cases like:
				//
				//   SELECT a AS b FROM t ORDER BY b
				target := c.ColumnName.Normalize()
				for j, col := range columns {
					if parser.ReNormalizeName(col.Name) == target {
						index = j
						break
					}
				}
			}
		}

		if s != nil {
			// Try to optimize constant sorts. For this we need to resolve
			// names to IndexedVars then see if there were any vars
			// substituted.
			sortExpr := expr
			if index != -1 {
				// We found a render above, so fetch it. This is needed
				// because if the sort expression is a reference to a render
				// alias, resolveNames below would be incorrect.
				sortExpr = s.render[index]
			}
			resolved, hasVars, err := p.resolveNames(sortExpr, s.sourceInfo, s.ivarHelper)
			if err != nil {
				return nil, err
			}
			if !hasVars {
				// No sorting needed!
				continue
			}

			// Now, try to find an equivalent render. We use the syntax
			// representation as approximation of equivalence.
			// We also use the expression after name resolution so
			// that comparison occurs after replacing ordinal references
			// to IndexedVars.
			if index == -1 {
				exprStr := parser.AsStringWithFlags(resolved, parser.FmtSymbolicVars)
				for j, render := range s.render {
					if parser.AsStringWithFlags(render, parser.FmtSymbolicVars) == exprStr {
						index = j
						break
					}
				}
			}
		}

		// Finally, if we haven't found anything so far, we really
		// need a new render.
		// TODO(knz/dan) currently this is only possible for selectNode.
		// If we are dealing with a UNION or something else we would need
		// to fabricate an intermediate selectNode to add the new render.
		if index == -1 && s != nil {
			// We need to add a new render, but let's be careful: if there's
			// a star in there, we're really adding multiple columns. These
			// all become sort columns! And if no columns are expanded, then
			// no columns become sort keys.
			nextRenderIdx := len(s.render)

			// TODO(knz) the optimizations above which reuse existing
			// renders and skip ordering for constant expressions are not
			// applied by addRender(). In other words, "ORDER BY foo.*" will
			// not be optimized as well as "ORDER BY foo.x, foo.y".  We
			// could do this either here or as a separate later
			// optimization.
			if err := s.addRender(parser.SelectExpr{Expr: expr}, nil); err != nil {
				return nil, err
			}

			for extraIdx := nextRenderIdx; extraIdx < len(s.render)-1; extraIdx++ {
				// If more than 1 column were expanded, turn them into sort columns too.
				// Except the last one, which will be added below.
				ordering = append(ordering, sqlbase.ColumnOrderInfo{ColIdx: extraIdx, Direction: direction})
			}
			if len(s.render) == nextRenderIdx {
				// Nothing was expanded! So there is no order here.
				continue
			}
			index = len(s.render) - 1
		}

		if index == -1 {
			return nil, errors.Errorf("column %s does not exist", expr)
		}
		ordering = append(ordering, sqlbase.ColumnOrderInfo{ColIdx: index, Direction: direction})
	}

	if ordering == nil {
		// All the sort expressions are constant. Simply drop the sort node.
		return nil, nil
	}
	return &sortNode{ctx: p.ctx(), p: p, columns: columns, ordering: ordering}, nil
}

// colIndex takes an expression that refers to a column using an integer, verifies it refers to a
// valid render target and returns the corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first render target "a". The returned index is 0.
func (p *planner) colIndex(numOriginalCols int, expr parser.Expr, context string) (int, error) {
	ord := int64(-1)
	switch i := expr.(type) {
	case *parser.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				return -1, err
			}
			ord = val
		}
	case *parser.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	default:
		if p.parser.IsConstExpr(expr) {
			return -1, errors.Errorf("non-integer constant in %s: %s", context, expr)
		}
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			return -1, errors.Errorf("%s position %s is not in select list", context, expr)
		}
		ord--
	}
	return int(ord), nil
}

func (n *sortNode) Columns() ResultColumns {
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

func (n *sortNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	if n.needSort {
		name = "sort"
	} else {
		name = "nosort"
	}

	var buf bytes.Buffer
	var columns ResultColumns
	if n.plan != nil {
		columns = n.plan.Columns()
	}
	n.Ordering().Format(&buf, columns)

	switch ss := n.sortStrategy.(type) {
	case *iterativeSortStrategy:
		buf.WriteString(" (iterative)")
	case *sortTopKStrategy:
		fmt.Fprintf(&buf, " (top %d)", ss.topK)
	}
	description = buf.String()

	return name, description, []planNode{n.plan}
}

func (n *sortNode) ExplainTypes(_ func(string, string)) {}

func (n *sortNode) SetLimitHint(numRows int64, soft bool) {
	if !n.needSort {
		// The limit is only useful to the wrapped node if we don't need to sort.
		n.plan.SetLimitHint(numRows, soft)
	} else {
		// The special value math.MaxInt64 means "no limit".
		if numRows != math.MaxInt64 {
			v := n.p.newContainerValuesNode(n.plan.Columns(), int(numRows))
			v.ordering = n.ordering
			if soft {
				n.sortStrategy = newIterativeSortStrategy(v)
			} else {
				n.sortStrategy = newSortTopKStrategy(v, numRows)
			}
		}
	}
}

// wrap the supplied planNode with the sortNode if sorting is required.
// The first returned value is "true" if the sort node can be squashed
// in the selectTopNode (sorting unneeded).
func (n *sortNode) wrap(plan planNode) (bool, planNode) {
	if n != nil {
		// Check to see if the requested ordering is compatible with the existing
		// ordering.
		existingOrdering := plan.Ordering()
		if log.V(2) {
			log.Infof(n.ctx, "Sort: existing=%+v desired=%+v", existingOrdering, n.ordering)
		}
		match := computeOrderingMatch(n.ordering, existingOrdering, false)
		if match < len(n.ordering) {
			n.plan = plan
			n.needSort = true
			return false, n
		}

		if len(n.columns) < len(plan.Columns()) {
			// No sorting required, but we have to strip off the extra render
			// expressions we added.
			n.plan = plan
			return false, n
		}

		if log.V(2) {
			log.Infof(n.ctx, "Sort: no sorting required")
		}
	}

	return true, plan
}

func (n *sortNode) expandPlan() error {
	// We do not need to recurse into the child node here; selectTopNode
	// does this for us.
	return nil
}

func (n *sortNode) Start() error {
	return n.plan.Start()
}

func (n *sortNode) Next() (bool, error) {
	for n.needSort {
		if v, ok := n.plan.(*valuesNode); ok {
			// The plan we wrap is already a values node. Just sort it.
			v.ordering = n.ordering
			n.sortStrategy = newSortAllStrategy(v)
			n.sortStrategy.Finish()
			n.needSort = false
			break
		} else if n.sortStrategy == nil {
			v := n.p.newContainerValuesNode(n.plan.Columns(), 0)
			v.ordering = n.ordering
			n.sortStrategy = newSortAllStrategy(v)
		}

		// TODO(andrei): If we're scanning an index with a prefix matching an
		// ordering prefix, we should only accumulate values for equal fields
		// in this prefix, then sort the accumulated chunk and output.
		// TODO(irfansharif): matching column ordering speed-ups from distsql,
		// when implemented, could be repurposed and used here.
		next, err := n.plan.Next()
		if err != nil {
			return false, err
		}
		if !next {
			n.sortStrategy.Finish()
			n.valueIter = n.sortStrategy
			n.needSort = false
			break
		}

		if n.explain == explainDebug {
			n.debugVals = n.plan.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Pass through non-row debug values.
				return true, nil
			}
		}

		values := n.plan.Values()
		if err := n.sortStrategy.Add(values); err != nil {
			return false, err
		}

		if n.explain == explainDebug {
			// Emit a "buffered" row.
			n.debugVals.output = debugValueBuffered
			return true, nil
		}
	}

	if n.valueIter == nil {
		n.valueIter = n.plan
	}
	next, err := n.valueIter.Next()
	if !next {
		return false, err
	}
	if n.explain == explainDebug {
		n.debugVals = n.valueIter.DebugValues()
	}
	return true, nil
}

func (n *sortNode) Close() {
	n.plan.Close()
	if n.sortStrategy != nil {
		n.sortStrategy.Close()
	}
	if n.valueIter != nil {
		n.valueIter.Close()
	}
}

// valueIterator provides iterative access to a value source's values and
// debug values. It is a subset of the planNode interface, so all methods
// should conform to the comments expressed in the planNode definition.
type valueIterator interface {
	Next() (bool, error)
	Values() parser.DTuple
	DebugValues() debugValues
	Close()
}

type sortingStrategy interface {
	valueIterator
	// Add adds a single value to the sortingStrategy. It guarantees that
	// if it decided to store the provided value, that it will make a deep
	// copy of it.
	Add(parser.DTuple) error
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

func (ss *sortAllStrategy) Add(values parser.DTuple) error {
	_, err := ss.vNode.rows.AddRow(values)
	return err
}

func (ss *sortAllStrategy) Finish() {
	ss.vNode.SortAll()
}

func (ss *sortAllStrategy) Next() (bool, error) {
	return ss.vNode.Next()
}

func (ss *sortAllStrategy) Values() parser.DTuple {
	return ss.vNode.Values()
}

func (ss *sortAllStrategy) DebugValues() debugValues {
	return ss.vNode.DebugValues()
}

func (ss *sortAllStrategy) Close() {
	ss.vNode.Close()
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

func (ss *iterativeSortStrategy) Add(values parser.DTuple) error {
	_, err := ss.vNode.rows.AddRow(values)
	return err
}

func (ss *iterativeSortStrategy) Finish() {
	ss.vNode.InitMinHeap()
}

func (ss *iterativeSortStrategy) Next() (bool, error) {
	if ss.vNode.Len() == 0 {
		return false, nil
	}
	ss.lastVal = ss.vNode.PopValues()
	ss.nextRowIdx++
	return true, nil
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

func (ss *iterativeSortStrategy) Close() {
	ss.vNode.Close()
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

func (ss *sortTopKStrategy) Add(values parser.DTuple) error {
	switch {
	case int64(ss.vNode.Len()) < ss.topK:
		// The first k values all go into the max-heap.
		if err := ss.vNode.PushValues(values); err != nil {
			return err
		}
	case ss.vNode.ValuesLess(values, ss.vNode.rows.At(0)):
		// Once the heap is full, only replace the top
		// value if a new value is less than it. If so
		// replace and fix the heap.
		if err := ss.vNode.rows.Replace(0, values); err != nil {
			return err
		}
		heap.Fix(ss.vNode, 0)
	}
	return nil
}

func (ss *sortTopKStrategy) Finish() {
	// Pop all values in the heap, resulting in the inverted ordering
	// being sorted in reverse. Therefore, the slice is ordered correctly
	// in-place.
	for ss.vNode.Len() > 0 {
		heap.Pop(ss.vNode)
	}
	ss.vNode.ResetLen()
}

func (ss *sortTopKStrategy) Next() (bool, error) {
	return ss.vNode.Next()
}

func (ss *sortTopKStrategy) Values() parser.DTuple {
	return ss.vNode.Values()
}

func (ss *sortTopKStrategy) DebugValues() debugValues {
	return ss.vNode.DebugValues()
}

func (ss *sortTopKStrategy) Close() {
	ss.vNode.Close()
}

// TODO(pmattis): If the result set is large, we might need to perform the
// sort on disk. There is no point in doing this while we're buffering the
// entire result set in memory. If/when we start streaming results back to
// the client we should revisit.
//
// type onDiskSortStrategy struct{}
