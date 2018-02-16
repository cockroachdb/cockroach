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

package sql

import (
	"container/heap"
	"context"
	"sort"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// sortNode represents a node that sorts the rows returned by its
// sub-node.
type sortNode struct {
	plan     planNode
	columns  sqlbase.ResultColumns
	ordering sqlbase.ColumnOrdering

	needSort bool

	run sortRun
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
	ctx context.Context, orderBy tree.OrderBy, n planNode,
) (*sortNode, error) {
	if orderBy == nil {
		return nil, nil
	}

	// Multiple tests below use renderNode as a special case.
	// So factor the cast.
	s, _ := n.(*renderNode)

	// We grab a copy of columns here because we might add new render targets
	// below. This is the set of columns requested by the query.
	columns := planColumns(n)
	numOriginalCols := len(columns)
	if s != nil {
		numOriginalCols = s.numOriginalCols
	}
	var ordering sqlbase.ColumnOrdering

	var err error
	orderBy, err = p.rewriteIndexOrderings(ctx, orderBy)
	if err != nil {
		return nil, err
	}

	for _, o := range orderBy {
		direction := encoding.Ascending
		if o.Direction == tree.Descending {
			direction = encoding.Descending
		}

		// Unwrap parenthesized expressions like "((a))" to "a".
		expr := tree.StripParens(o.Expr)

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

		// First, deal with render aliases.
		index, err := s.colIdxByRenderAlias(expr, columns, "ORDER BY")
		if err != nil {
			return nil, err
		}

		// If the expression does not refer to an alias, deal with
		// column ordinals.
		if index == -1 {
			col, err := p.colIndex(numOriginalCols, expr, "ORDER BY")
			if err != nil {
				return nil, err
			}
			if col != -1 {
				index = col
			}
		}

		// If we're ordering by using one of the existing renders, ensure it's a
		// column type we can order on.
		if index != -1 {
			if err := ensureColumnOrderable(columns[index]); err != nil {
				return nil, err
			}
		}

		// Finally, if we haven't found anything so far, we really
		// need a new render.
		// TODO(knz/dan): currently this is only possible for renderNode.
		// If we are dealing with a UNION or something else we would need
		// to fabricate an intermediate renderNode to add the new render.
		if index == -1 && s != nil {
			cols, exprs, hasStar, err := p.computeRenderAllowingStars(
				ctx, tree.SelectExpr{Expr: expr}, types.Any,
				s.sourceInfo, s.ivarHelper, autoGenerateRenderOutputName)
			if err != nil {
				return nil, err
			}
			p.curPlan.hasStar = p.curPlan.hasStar || hasStar

			if len(cols) == 0 {
				// Nothing was expanded! No order here.
				continue
			}

			// ORDER BY (a, b) -> ORDER BY a, b
			cols, exprs = flattenTuples(cols, exprs, &s.ivarHelper)

			colIdxs := s.addOrReuseRenders(cols, exprs, true)
			for i := 0; i < len(colIdxs)-1; i++ {
				// If more than 1 column were expanded, turn them into sort columns too.
				// Except the last one, which will be added below.
				ordering = append(ordering,
					sqlbase.ColumnOrderInfo{ColIdx: colIdxs[i], Direction: direction})
			}
			index = colIdxs[len(colIdxs)-1]
			// Ensure our newly rendered columns are ok to order by.
			renderCols := planColumns(s)
			for _, colIdx := range colIdxs {
				if err := ensureColumnOrderable(renderCols[colIdx]); err != nil {
					return nil, err
				}
			}
		}

		if index == -1 {
			return nil, sqlbase.NewUndefinedColumnError(expr.String())
		}
		ordering = append(ordering,
			sqlbase.ColumnOrderInfo{ColIdx: index, Direction: direction})
	}

	if ordering == nil {
		// No ordering; simply drop the sort node.
		return nil, nil
	}
	return &sortNode{columns: columns, ordering: ordering}, nil
}

// sortRun contains the run-time state of sortNode during local
// execution.
type sortRun struct {
	sortStrategy sortingStrategy
	valueIter    valueIterator
}

func (n *sortNode) Next(params runParams) (bool, error) {
	cancelChecker := params.p.cancelChecker

	for n.needSort {
		if vn, ok := n.plan.(*valuesNode); ok {
			// The plan we wrap is already a values node. Just sort it.
			v := &sortValues{
				ordering: n.ordering,
				rows:     vn.rows,
				evalCtx:  params.EvalContext(),
			}
			n.run.sortStrategy = newSortAllStrategy(v)
			n.run.sortStrategy.Finish(params.ctx, cancelChecker)
			// Sorting is done. Relinquish the reference on the row container,
			// so as to avoid closing it twice.
			n.run.sortStrategy = nil
			// Fall through -- the remainder of the work is done by the
			// valuesNode itself.
			n.needSort = false
			break
		} else if n.run.sortStrategy == nil {
			v := params.p.newSortValues(n.ordering, planColumns(n.plan), 0 /*capacity*/)
			n.run.sortStrategy = newSortAllStrategy(v)
		}

		if err := cancelChecker.Check(); err != nil {
			return false, err
		}

		// TODO(andrei): If we're scanning an index with a prefix matching an
		// ordering prefix, we should only accumulate values for equal fields
		// in this prefix, then sort the accumulated chunk and output.
		// TODO(irfansharif): matching column ordering speed-ups from distsql,
		// when implemented, could be repurposed and used here.
		next, err := n.plan.Next(params)
		if err != nil {
			return false, err
		}
		if !next {
			n.run.sortStrategy.Finish(params.ctx, cancelChecker)
			n.run.valueIter = n.run.sortStrategy
			n.needSort = false
			break
		}

		values := n.plan.Values()
		if err := n.run.sortStrategy.Add(params.ctx, values); err != nil {
			return false, err
		}
	}

	// Check again, in case sort returned early due to a cancellation.
	if err := cancelChecker.Check(); err != nil {
		return false, err
	}

	if n.run.valueIter == nil {
		n.run.valueIter = n.plan
	}
	return n.run.valueIter.Next(params)
}

func (n *sortNode) Values() tree.Datums {
	// If an ordering expression was used the number of columns in each row might
	// differ from the number of columns requested, so trim the result.
	return n.run.valueIter.Values()[:len(n.columns)]
}

func (n *sortNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
	if n.run.sortStrategy != nil {
		n.run.sortStrategy.Close(ctx)
	}
	// n.run.valueIter points to either n.plan or n.run.sortStrategy and thus has already
	// been closed.
}

func ensureColumnOrderable(c sqlbase.ResultColumn) error {
	if _, ok := c.Typ.(types.TArray); ok || c.Typ == types.JSON {
		return pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError, "can't order by column type %s", c.Typ)
	}
	return nil
}

// flattenTuples extracts the members of tuples into a list of columns.
func flattenTuples(
	cols sqlbase.ResultColumns, exprs []tree.TypedExpr, ivarHelper *tree.IndexedVarHelper,
) (sqlbase.ResultColumns, []tree.TypedExpr) {
	// We want to avoid allocating new slices unless strictly necessary.
	var newExprs []tree.TypedExpr
	var newCols sqlbase.ResultColumns
	for i, e := range exprs {
		if t, ok := e.(*tree.Tuple); ok {
			if newExprs == nil {
				// All right, it was necessary to allocate the slices after all.
				newExprs = make([]tree.TypedExpr, i, len(exprs))
				newCols = make(sqlbase.ResultColumns, i, len(cols))
				copy(newExprs, exprs[:i])
				copy(newCols, cols[:i])
			}

			newCols, newExprs = flattenTuple(t, newCols, newExprs, ivarHelper)
		} else if newExprs != nil {
			newExprs = append(newExprs, e)
			newCols = append(newCols, cols[i])
		}
	}
	if newExprs != nil {
		return newCols, newExprs
	}
	return cols, exprs
}

// flattenTuple extracts the members of one tuple into a list of columns.
func flattenTuple(
	t *tree.Tuple,
	cols sqlbase.ResultColumns,
	exprs []tree.TypedExpr,
	ivarHelper *tree.IndexedVarHelper,
) (sqlbase.ResultColumns, []tree.TypedExpr) {
	for _, e := range t.Exprs {
		if eT, ok := e.(*tree.Tuple); ok {
			cols, exprs = flattenTuple(eT, cols, exprs, ivarHelper)
		} else {
			expr := e.(tree.TypedExpr)
			exprs = append(exprs, expr)
			cols = append(cols, sqlbase.ResultColumn{
				Name: expr.String(),
				Typ:  expr.ResolvedType(),
			})
		}
	}
	return cols, exprs
}

// rewriteIndexOrderings rewrites an ORDER BY clause that uses the
// extended INDEX or PRIMARY KEY syntax into an ORDER BY clause that
// doesn't: each INDEX or PRIMARY KEY order specification is replaced
// by the list of columns in the specified index.
// For example,
//   ORDER BY PRIMARY KEY kv -> ORDER BY kv.k ASC
//   ORDER BY INDEX kv@primary -> ORDER BY kv.k ASC
// With an index foo(a DESC, b ASC):
//   ORDER BY INDEX t@foo ASC -> ORDER BY t.a DESC, t.a ASC
//   ORDER BY INDEX t@foo DESC -> ORDER BY t.a ASC, t.b DESC
func (p *planner) rewriteIndexOrderings(
	ctx context.Context, orderBy tree.OrderBy,
) (tree.OrderBy, error) {
	// The loop above *may* allocate a new slice, but this is only
	// needed if the INDEX / PRIMARY KEY syntax is used. In case the
	// ORDER BY clause only uses the column syntax, we should reuse the
	// same slice.
	newOrderBy := orderBy
	rewrite := false
	for i, o := range orderBy {
		switch o.OrderType {
		case tree.OrderByColumn:
			// Nothing to do, just propagate the setting.
			if rewrite {
				// We only need to-reappend if we've allocated a new slice
				// (see below).
				newOrderBy = append(newOrderBy, o)
			}

		case tree.OrderByIndex:
			tn, err := o.Table.Normalize()
			if err != nil {
				return nil, err
			}
			desc, err := ResolveExistingObject(ctx, p, tn, true /*required*/, requireTableDesc)
			if err != nil {
				return nil, err
			}

			// Force the name to be fully qualified so that the column references
			// below only match against real tables in the FROM clause.
			tn.ExplicitCatalog = true
			tn.ExplicitSchema = true

			var idxDesc *sqlbase.IndexDescriptor
			if o.Index == "" || string(o.Index) == desc.PrimaryIndex.Name {
				// ORDER BY PRIMARY KEY / ORDER BY INDEX t@primary
				idxDesc = &desc.PrimaryIndex
			} else {
				// ORDER BY INDEX t@somename
				// We need to search for the index with that name.
				for i := range desc.Indexes {
					if string(o.Index) == desc.Indexes[i].Name {
						idxDesc = &desc.Indexes[i]
						break
					}
				}
				if idxDesc == nil {
					return nil, errors.Errorf("index %q not found", tree.ErrString(&o.Index))
				}
			}

			// Now, expand the ORDER BY clause by an equivalent clause using that
			// index's columns.

			if !rewrite {
				// First, make a final slice that's intelligently larger.
				newOrderBy = make(tree.OrderBy, i, len(orderBy)+len(idxDesc.ColumnNames)-1)
				copy(newOrderBy, orderBy[:i])
				rewrite = true
			}

			// Now expand the clause.
			for k, colName := range idxDesc.ColumnNames {
				newOrderBy = append(newOrderBy, &tree.Order{
					OrderType: tree.OrderByColumn,
					Expr:      tree.NewColumnItem(tn, tree.Name(colName)),
					Direction: chooseDirection(o.Direction == tree.Descending, idxDesc.ColumnDirections[k]),
				})
			}

		default:
			return nil, errors.Errorf("unknown ORDER BY specification: %s", tree.ErrString(&orderBy))
		}
	}

	if log.V(2) {
		log.Infof(ctx, "rewritten ORDER BY clause: %s", tree.ErrString(&newOrderBy))
	}
	return newOrderBy, nil
}

// chooseDirection translates the specified IndexDescriptor_Direction
// into a tree.Direction. If invert is true, the idxDir is inverted.
func chooseDirection(invert bool, idxDir sqlbase.IndexDescriptor_Direction) tree.Direction {
	if (idxDir == sqlbase.IndexDescriptor_ASC) != invert {
		return tree.Ascending
	}
	return tree.Descending
}

// colIndex takes an expression that refers to a column using an integer, verifies it refers to a
// valid render target and returns the corresponding column index. For example:
//    SELECT a from T ORDER by 1
// Here "1" refers to the first render target "a". The returned index is 0.
func (p *planner) colIndex(numOriginalCols int, expr tree.Expr, context string) (int, error) {
	ord := int64(-1)
	switch i := expr.(type) {
	case *tree.NumVal:
		if i.ShouldBeInt64() {
			val, err := i.AsInt64()
			if err != nil {
				return -1, err
			}
			ord = val
		} else {
			return -1, errors.Errorf("non-integer constant in %s: %s", context, expr)
		}
	case *tree.DInt:
		if *i >= 0 {
			ord = int64(*i)
		}
	case *tree.StrVal:
		return -1, errors.Errorf("non-integer constant in %s: %s", context, expr)
	case tree.Datum:
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

// valueIterator provides iterative access to a value source's values and
// debug values. It is a subset of the planNode interface, so all methods
// should conform to the comments expressed in the planNode definition.
type valueIterator interface {
	Next(runParams) (bool, error)
	Values() tree.Datums
	Close(ctx context.Context)
}

type sortingStrategy interface {
	valueIterator
	// Add adds a single value to the sortingStrategy. It guarantees that
	// if it decided to store the provided value, that it will make a deep
	// copy of it.
	Add(context.Context, tree.Datums) error
	// Finish terminates the sorting strategy, allowing for postprocessing
	// after all values have been provided to the strategy. The method should
	// not be called more than once, and should only be called after all Add
	// calls have occurred.
	Finish(context.Context, *sqlbase.CancelChecker)
}

// sortAllStrategy reads in all values into the wrapped valuesNode and
// uses sort.Sort to sort all values in-place. It has a worst-case time
// complexity of O(n*log(n)) and a worst-case space complexity of O(n).
//
// The strategy is intended to be used when all values need to be sorted.
type sortAllStrategy struct {
	vNode *sortValues
}

func newSortAllStrategy(vNode *sortValues) sortingStrategy {
	return &sortAllStrategy{
		vNode: vNode,
	}
}

func (ss *sortAllStrategy) Add(ctx context.Context, values tree.Datums) error {
	_, err := ss.vNode.rows.AddRow(ctx, values)
	return err
}

func (ss *sortAllStrategy) Finish(ctx context.Context, cancelChecker *sqlbase.CancelChecker) {
	ss.vNode.SortAll(cancelChecker)
}

func (ss *sortAllStrategy) Next(params runParams) (bool, error) {
	return ss.vNode.Next(params)
}

func (ss *sortAllStrategy) Values() tree.Datums {
	return ss.vNode.Values()
}

func (ss *sortAllStrategy) Close(ctx context.Context) {
	ss.vNode.Close(ctx)
}

// iterativeSortStrategy reads in all values into the wrapped sortValues
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
	vNode      *sortValues
	lastVal    tree.Datums
	nextRowIdx int
}

func newIterativeSortStrategy(vNode *sortValues) sortingStrategy {
	return &iterativeSortStrategy{
		vNode: vNode,
	}
}

func (ss *iterativeSortStrategy) Add(ctx context.Context, values tree.Datums) error {
	_, err := ss.vNode.rows.AddRow(ctx, values)
	return err
}

func (ss *iterativeSortStrategy) Finish(context.Context, *sqlbase.CancelChecker) {
	ss.vNode.InitMinHeap()
}

func (ss *iterativeSortStrategy) Next(runParams) (bool, error) {
	if ss.vNode.Len() == 0 {
		return false, nil
	}
	ss.lastVal = ss.vNode.PopValues()
	ss.nextRowIdx++
	return true, nil
}

func (ss *iterativeSortStrategy) Values() tree.Datums {
	return ss.lastVal
}

func (ss *iterativeSortStrategy) Close(ctx context.Context) {
	ss.vNode.Close(ctx)
}

// sortTopKStrategy creates a max-heap in its wrapped sortValues and keeps
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
	vNode *sortValues
	topK  int64
}

func newSortTopKStrategy(vNode *sortValues, topK int64) sortingStrategy {
	ss := &sortTopKStrategy{
		vNode: vNode,
		topK:  topK,
	}
	ss.vNode.InitMaxHeap()
	return ss
}

func (ss *sortTopKStrategy) Add(ctx context.Context, values tree.Datums) error {
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

func (ss *sortTopKStrategy) Finish(ctx context.Context, cancelChecker *sqlbase.CancelChecker) {
	// Pop all values in the heap, resulting in the inverted ordering
	// being sorted in reverse. Therefore, the slice is ordered correctly
	// in-place.
	for ss.vNode.Len() > 0 {
		if cancelChecker.Check() != nil {
			return
		}
		heap.Pop(ss.vNode)
	}
	ss.vNode.ResetLen()
}

func (ss *sortTopKStrategy) Next(params runParams) (bool, error) {
	return ss.vNode.Next(params)
}

func (ss *sortTopKStrategy) Values() tree.Datums {
	return ss.vNode.Values()
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

// sortValues is a reduced form of valuesNode for use in a sortStrategy.
type sortValues struct {
	// invertSorting inverts the sorting predicate.
	invertSorting bool
	// ordering indicates the desired ordering of each column in the rows
	ordering sqlbase.ColumnOrdering
	// rows contains the columns during sorting.
	rows *sqlbase.RowContainer

	// evalCtx is needed because we use datum.Compare() which needs it,
	// and the sort.Interface and heap.Interface do not allow
	// introducing extra parameters into the Less() method.
	evalCtx *tree.EvalContext

	// rowsPopped is used for heaps, it indicates the number of rows
	// that were "popped". These rows are still part of the underlying
	// sqlbase.RowContainer, in the range [rows.Len()-n.rowsPopped,
	// rows.Len).
	rowsPopped int

	// nextRow is used while iterating.
	nextRow int
}

var _ valueIterator = &sortValues{}
var _ sort.Interface = &sortValues{}
var _ heap.Interface = &sortValues{}

func (p *planner) newSortValues(
	ordering sqlbase.ColumnOrdering, columns sqlbase.ResultColumns, capacity int,
) *sortValues {
	return &sortValues{
		ordering: ordering,
		rows: sqlbase.NewRowContainer(
			p.EvalContext().Mon.MakeBoundAccount(),
			sqlbase.ColTypeInfoFromResCols(columns),
			capacity,
		),
		evalCtx: p.EvalContext(),
	}
}

// Values implement the valuesIterator interface.
func (n *sortValues) Values() tree.Datums {
	return n.rows.At(n.nextRow - 1)
}

// Next implements the valuesIterator interface.
func (n *sortValues) Next(runParams) (bool, error) {
	if n.nextRow >= n.rows.Len() {
		return false, nil
	}
	n.nextRow++
	return true, nil
}

// Close implements the valuesIterator interface.
func (n *sortValues) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

// Len implements the sort.Interface interface.
func (n *sortValues) Len() int {
	return n.rows.Len() - n.rowsPopped
}

// ValuesLess returns the comparison result between the two provided
// Datums slices in the context of the sortValues ordering.
func (n *sortValues) ValuesLess(ra, rb tree.Datums) bool {
	return sqlbase.CompareDatums(n.ordering, n.evalCtx, ra, rb) < 0
}

// Less implements the sort.Interface interface.
func (n *sortValues) Less(i, j int) bool {
	// TODO(pmattis): An alternative to this type of field-based comparison would
	// be to construct a sort-key per row using encodeTableKey(). Using a
	// sort-key approach would likely fit better with a disk-based sort.
	ra, rb := n.rows.At(i), n.rows.At(j)
	return n.invertSorting != n.ValuesLess(ra, rb)
}

// Swap implements the sort.Interface interface.
func (n *sortValues) Swap(i, j int) {
	n.rows.Swap(i, j)
}

// Push implements the heap.Interface interface.
func (n *sortValues) Push(x interface{}) {
	// We can't push to the heap via heap.Push because that doesn't
	// allow us to return an error. Instead we use PushValues(), which
	// adds the value *then* calls heap.Push.  By the time control
	// arrives here, the value is already added, so there is nothing
	// left to do.
}

// PushValues pushes the given Datums value into the heap
// representation of the sortValues.
func (n *sortValues) PushValues(ctx context.Context, values tree.Datums) error {
	_, err := n.rows.AddRow(ctx, values)
	// We still need to call heap.Push() to sort the heap.
	heap.Push(n, nil)
	return err
}

// Pop implements the heap.Interface interface.
func (n *sortValues) Pop() interface{} {
	if n.rowsPopped >= n.rows.Len() {
		panic("no more rows to pop")
	}
	n.rowsPopped++
	// Returning a Datums as an interface{} involves an allocation. Luckily, the
	// value of Pop is only used for the return value of heap.Pop, which we can
	// avoid using.
	return nil
}

// PopValues pops the top Datums value off the heap representation
// of the sortValues. We avoid using heap.Pop() directly to
// avoid allocating an interface{}.
func (n *sortValues) PopValues() tree.Datums {
	heap.Pop(n)
	// Return the last popped row.
	return n.rows.At(n.rows.Len() - n.rowsPopped)
}

// ResetLen resets the length to that of the underlying row
// container. This resets the effect that popping values had on the
// sortValues's visible length.
func (n *sortValues) ResetLen() {
	n.rowsPopped = 0
}

// SortAll sorts all values in the sortValues.rows slice.
func (n *sortValues) SortAll(cancelChecker *sqlbase.CancelChecker) {
	n.invertSorting = false
	sqlbase.Sort(n, cancelChecker)
}

// InitMaxHeap initializes the sortValues.rows slice as a max-heap.
func (n *sortValues) InitMaxHeap() {
	n.invertSorting = true
	heap.Init(n)
}

// InitMinHeap initializes the sortValues.rows slice as a min-heap.
func (n *sortValues) InitMinHeap() {
	n.invertSorting = false
	heap.Init(n)
}
