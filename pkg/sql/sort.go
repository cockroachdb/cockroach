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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// sortNode represents a node that sorts the rows returned by its
// sub-node.
type sortNode struct {
	p        *planner
	plan     planNode
	columns  sqlbase.ResultColumns
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
// targets in the renderNode if any ordering expressions are specified.
//
// TODO(dan): SQL also allows sorting a VALUES or UNION by an expression.
// Support this. It will reduce some of the special casing below, but requires a
// generalization of how to add derived columns to a SelectStatement.
func (p *planner) orderBy(
	ctx context.Context, orderBy parser.OrderBy, n planNode,
) (*sortNode, error) {
	if orderBy == nil {
		return nil, nil
	}

	// Multiple tests below use renderNode as a special case.
	// So factor the cast.
	s, _ := n.(*renderNode)

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

		// Unwrap parenthesized expressions like "((a))" to "a".
		expr := parser.StripParens(o.Expr)

		// The logical data source for ORDER BY is the list of render
		// expressions for a SELECT, as specified in the input SQL text
		// (or an entire UNION or VALUES clause).  Alas, SQL has some
		// historical baggage from SQL92 and there are some special cases:
		//
		// SQL92 rules:
		//
		// 1) if the expression is the aliased (AS) name of a render
		//    expression in a SELECT clause, then use that
		//    render as sort key.
		//    e.g. SELECT a AS b, b AS c ORDER BY b
		//    this sorts on the first render.
		//
		// 2) column ordinals. If a simple integer literal is used,
		//    optionally enclosed within parentheses but *not subject to
		//    any arithmetic*, then this refers to one of the columns of
		//    the data source. Then use the render expression at that
		//    ordinal position as sort key.
		//
		// SQL99 rules:
		//
		// 3) otherwise, if the expression is already in the render list,
		//    then use that render as sort key.
		//    e.g. SELECT b AS c ORDER BY b
		//    this sorts on the first render.
		//    (this is an optimization)
		//
		// 4) if the sort key is not dependent on the data source (no
		//    IndexedVar) then simply do not sort. (this is an optimization)
		//
		// 5) otherwise, add a new render with the ORDER BY expression
		//    and use that as sort key.
		//    e.g. SELECT a FROM t ORDER by b
		//    e.g. SELECT a, b FROM t ORDER by a+b

		index := -1

		// First, deal with render aliases.
		if vBase, ok := expr.(parser.VarName); ok {
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
						if index != -1 {
							// There is more than one render alias that matches the ORDER BY
							// clause. Here, SQL92 is specific as to what should be done:
							// if the underlying expression is known (we're on a renderNode)
							// and it is equivalent, then just accept that and ignore the ambiguity.
							// This plays nice with `SELECT b, * FROM t ORDER BY b`. Otherwise,
							// reject with an ambiguity error.
							if s == nil || !s.equivalentRenders(j, index) {
								return nil, errors.Errorf("ORDER BY \"%s\" is ambiguous", target)
							}
							// Note that in this case we want to use the index of the first
							// matching column. This is because renderNode.computeOrdering
							// also prefers the first column, and we want the orderings to
							// match as much as possible.
							continue
						}
						index = j
					}
				}
			}
		}

		// So Then, deal with column ordinals.
		if index == -1 {
			col, err := p.colIndex(numOriginalCols, expr, "ORDER BY")
			if err != nil {
				return nil, err
			}
			if col != -1 {
				index = col
			}
		}

		if s != nil {
			// Try to optimize constant sorts. For this we need to resolve
			// names to IndexedVars then see if there were any row-dependent
			// expressions found in the sort expression.
			sortExpr := expr
			if index != -1 {
				// We found a render above, so fetch it. This is needed
				// because if the sort expression is a reference to a render
				// alias, resolveNames below would be incorrect.
				sortExpr = s.render[index]
			}
			_, hasRowDependentValues, err := p.resolveNames(sortExpr, s.sourceInfo, s.ivarHelper)
			if err != nil {
				return nil, err
			}
			if !hasRowDependentValues {
				// No sorting needed!
				continue
			}
		}

		// Finally, if we haven't found anything so far, we really
		// need a new render.
		// TODO(knz/dan): currently this is only possible for renderNode.
		// If we are dealing with a UNION or something else we would need
		// to fabricate an intermediate renderNode to add the new render.
		if index == -1 && s != nil {
			cols, exprs, hasStar, err := p.computeRenderAllowingStars(
				ctx, parser.SelectExpr{Expr: expr}, parser.TypeAny,
				s.sourceInfo, s.ivarHelper, autoGenerateRenderOutputName)
			if err != nil {
				return nil, err
			}
			s.isStar = s.isStar || hasStar

			if len(cols) == 0 {
				// Nothing was expanded! No order here.
				continue
			}

			colIdxs := s.addOrMergeRenders(cols, exprs, true)
			for i := 0; i < len(colIdxs)-1; i++ {
				// If more than 1 column were expanded, turn them into sort columns too.
				// Except the last one, which will be added below.
				ordering = append(ordering,
					sqlbase.ColumnOrderInfo{ColIdx: colIdxs[i], Direction: direction})
			}
			index = colIdxs[len(colIdxs)-1]
		}

		if index == -1 {
			return nil, errors.Errorf("column %s does not exist", expr)
		}
		ordering = append(ordering,
			sqlbase.ColumnOrderInfo{ColIdx: index, Direction: direction})
	}

	if ordering == nil {
		// All the sort expressions are constant. Simply drop the sort node.
		return nil, nil
	}
	return &sortNode{p: p, columns: columns, ordering: ordering}, nil
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
		} else {
			return -1, errors.Errorf("non-integer constant in %s: %s", context, expr)
		}
	case *parser.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *parser.StrVal:
		return -1, errors.Errorf("non-integer constant in %s: %s", context, expr)
	case parser.Datum:
		return -1, errors.Errorf("non-integer constant in %s: %s", context, expr)
	}
	if ord != -1 {
		if ord < 1 || ord > int64(numOriginalCols) {
			return -1, errors.Errorf("%s position %s is not in select list", context, expr)
		}
		ord--
	}
	return int(ord), nil
}

func (n *sortNode) Columns() sqlbase.ResultColumns {
	return n.columns
}

func (n *sortNode) Ordering() orderingInfo {
	if n == nil {
		return orderingInfo{}
	}

	underlying := n.plan.Ordering()

	var ord orderingInfo
	if n.needSort {
		// We will sort and can guarantee the desired ordering.
		ord.ordering = make(sqlbase.ColumnOrdering, 0, len(n.ordering))
		for _, o := range n.ordering {
			// Skip any exact match columns.
			if _, ok := underlying.exactMatchCols[o.ColIdx]; !ok {
				ord.ordering = append(ord.ordering, o)
			}
		}
	} else {
		// If we aren't sorting, the underlying plan's ordering can be more specific
		// than the sortNode's ordering, so we want to use that. E.g:
		//   CREATE INDEX foo ON t (a, b);
		//   SELECT a, b, c FROM t ORDER BY a;
		// We want to use (a, b) instead of just (a).
		ord.ordering = underlying.ordering
	}

	// Preserve exact match columns.
	if len(underlying.exactMatchCols) != 0 {
		ord.exactMatchCols = make(map[int]struct{})
		for c := range underlying.exactMatchCols {
			// Skip columns not in the output.
			if c < len(n.columns) {
				ord.exactMatchCols[c] = struct{}{}
			}
		}
	}

	// Remove all the columns after the first one that's not present in
	// the result columns.
	for i, o := range ord.ordering {
		if o.ColIdx >= len(n.columns) {
			// If something is ordered by columns A, then B, then C, if I
			// don't have column B I can't say it's ordered by columns A,
			// then C. Example:
			// A | B | C          A | C
			// ---------          -----
			// 1 | 1 | 2   --->   1 | 2
			// 1 | 2 | 1          1 | 1
			// 1 | 2 | 3          1 | 3
			// So we need to break the orderingInfo here.
			ord.ordering = ord.ordering[:i]
			break
		}
	}
	return ord
}

func (n *sortNode) Values() parser.Datums {
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

func (n *sortNode) Spans(ctx context.Context) (_, _ roachpb.Spans, _ error) {
	return n.plan.Spans(ctx)
}

func (n *sortNode) Start(ctx context.Context) error {
	return n.plan.Start(ctx)
}

func (n *sortNode) Next(ctx context.Context) (bool, error) {
	for n.needSort {
		if v, ok := n.plan.(*valuesNode); ok {
			// The plan we wrap is already a values node. Just sort it.
			v.ordering = n.ordering
			n.sortStrategy = newSortAllStrategy(v)
			n.sortStrategy.Finish(ctx)
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
		next, err := n.plan.Next(ctx)
		if err != nil {
			return false, err
		}
		if !next {
			n.sortStrategy.Finish(ctx)
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
		if err := n.sortStrategy.Add(ctx, values); err != nil {
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
	next, err := n.valueIter.Next(ctx)
	if !next {
		return false, err
	}
	if n.explain == explainDebug {
		n.debugVals = n.valueIter.DebugValues()
	}
	return true, nil
}

func (n *sortNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	if n.sortStrategy != nil {
		n.sortStrategy.Close(ctx)
	}
	if n.valueIter != nil {
		n.valueIter.Close(ctx)
	}
}

// valueIterator provides iterative access to a value source's values and
// debug values. It is a subset of the planNode interface, so all methods
// should conform to the comments expressed in the planNode definition.
type valueIterator interface {
	Next(ctx context.Context) (bool, error)
	Values() parser.Datums
	DebugValues() debugValues
	Close(ctx context.Context)
}

type sortingStrategy interface {
	valueIterator
	// Add adds a single value to the sortingStrategy. It guarantees that
	// if it decided to store the provided value, that it will make a deep
	// copy of it.
	Add(context.Context, parser.Datums) error
	// Finish terminates the sorting strategy, allowing for postprocessing
	// after all values have been provided to the strategy. The method should
	// not be called more than once, and should only be called after all Add
	// calls have occurred.
	Finish(context.Context)
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

func (ss *sortAllStrategy) Add(ctx context.Context, values parser.Datums) error {
	_, err := ss.vNode.rows.AddRow(ctx, values)
	return err
}

func (ss *sortAllStrategy) Finish(context.Context) {
	ss.vNode.SortAll()
}

func (ss *sortAllStrategy) Next(ctx context.Context) (bool, error) {
	return ss.vNode.Next(ctx)
}

func (ss *sortAllStrategy) Values() parser.Datums {
	return ss.vNode.Values()
}

func (ss *sortAllStrategy) DebugValues() debugValues {
	return ss.vNode.DebugValues()
}

func (ss *sortAllStrategy) Close(ctx context.Context) {
	ss.vNode.Close(ctx)
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
	lastVal    parser.Datums
	nextRowIdx int
}

func newIterativeSortStrategy(vNode *valuesNode) sortingStrategy {
	return &iterativeSortStrategy{
		vNode: vNode,
	}
}

func (ss *iterativeSortStrategy) Add(ctx context.Context, values parser.Datums) error {
	_, err := ss.vNode.rows.AddRow(ctx, values)
	return err
}

func (ss *iterativeSortStrategy) Finish(context.Context) {
	ss.vNode.InitMinHeap()
}

func (ss *iterativeSortStrategy) Next(context.Context) (bool, error) {
	if ss.vNode.Len() == 0 {
		return false, nil
	}
	ss.lastVal = ss.vNode.PopValues()
	ss.nextRowIdx++
	return true, nil
}

func (ss *iterativeSortStrategy) Values() parser.Datums {
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

func (ss *iterativeSortStrategy) Close(ctx context.Context) {
	ss.vNode.Close(ctx)
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
// TODO(nvanbenschoten): There are better algorithms that can achieve a sorted
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

func (ss *sortTopKStrategy) Add(ctx context.Context, values parser.Datums) error {
	switch {
	case int64(ss.vNode.Len()) < ss.topK:
		// The first k values all go into the max-heap.
		if err := ss.vNode.PushValues(ctx, values); err != nil {
			return err
		}
	case ss.vNode.ValuesLess(values, ss.vNode.rows.At(0)):
		// Once the heap is full, only replace the top
		// value if a new value is less than it. If so
		// replace and fix the heap.
		if err := ss.vNode.rows.Replace(ctx, 0, values); err != nil {
			return err
		}
		heap.Fix(ss.vNode, 0)
	}
	return nil
}

func (ss *sortTopKStrategy) Finish(context.Context) {
	// Pop all values in the heap, resulting in the inverted ordering
	// being sorted in reverse. Therefore, the slice is ordered correctly
	// in-place.
	for ss.vNode.Len() > 0 {
		heap.Pop(ss.vNode)
	}
	ss.vNode.ResetLen()
}

func (ss *sortTopKStrategy) Next(ctx context.Context) (bool, error) {
	return ss.vNode.Next(ctx)
}

func (ss *sortTopKStrategy) Values() parser.Datums {
	return ss.vNode.Values()
}

func (ss *sortTopKStrategy) DebugValues() debugValues {
	return ss.vNode.DebugValues()
}

func (ss *sortTopKStrategy) Close(ctx context.Context) {
	ss.vNode.Close(ctx)
}

// TODO(pmattis): If the result set is large, we might need to perform the
// sort on disk. There is no point in doing this while we're buffering the
// entire result set in memory. If/when we start streaming results back to
// the client we should revisit.
//
// type onDiskSortStrategy struct{}
