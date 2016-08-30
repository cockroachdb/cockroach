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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// window constructs a windowNode according to window function applications. This may
// adjust the render targets in the selectNode as necessary.
func (p *planner) window(n *parser.SelectClause, s *selectNode) (*windowNode, error) {
	// Determine if a window-function is being applied. We use the selectNode's
	// renders because window functions will never have be simplified out since
	// parsing, but they may have been added to the selectNode by an ORDER BY clause.
	if containsWindowFn := p.parser.WindowFuncInExprs(s.render); !containsWindowFn {
		return nil, nil
	}

	window := &windowNode{
		planner:      p,
		values:       valuesNode{columns: s.columns},
		windowRender: make([]parser.TypedExpr, len(s.render)),
	}

	visitor := extractWindowFuncsVisitor{
		n:              window,
		aggregatesSeen: make(map[*parser.FuncExpr]struct{}),
	}

	// Loop over the render expressions and extract any window functions. While looping
	// over the renders, each window function will be replaced by a separate render for
	// each of its (possibly 0) arguments in the selectNode.
	windowCount := 0
	oldRenders := s.render
	oldColumns := s.columns
	s.render = make([]parser.TypedExpr, 0, len(oldRenders))
	s.columns = make([]ResultColumn, 0, len(oldColumns))
	for i := range oldRenders {
		typedExpr, sawWindow, err := visitor.extract(oldRenders[i])
		if err != nil {
			return nil, err
		}
		if !sawWindow {
			// No window functions in render.
			s.render = append(s.render, oldRenders[i])
			s.columns = append(s.columns, oldColumns[i])
		} else {
			// One or more window functions in render. Create a new render in
			// selectNode for each window function argument.
			window.windowRender[i] = typedExpr
			for ; windowCount < len(window.funcs); windowCount++ {
				window.funcs[windowCount].funcIdx = windowCount
				window.funcs[windowCount].subRenderCol = len(s.render)
				arg := window.funcs[windowCount].arg
				s.render = append(s.render, arg)
				s.columns = append(s.columns, ResultColumn{
					Name: arg.String(),
					Typ:  arg.ReturnType(),
				})
			}
		}
	}

	if err := window.constructWindowDefinitions(n, s); err != nil {
		return nil, err
	}
	return window, nil
}

// constructWindowDefinitions creates window definitions for each window
// function application by combining specific window specifications for a
// given window function application with referenced window specifications
// on the SelectClause.
func (n *windowNode) constructWindowDefinitions(sc *parser.SelectClause, s *selectNode) error {
	// Process each window definition on the select clause.
	windowSpecs := make(map[string]*parser.WindowDef, len(sc.Window))
	for _, windowDef := range sc.Window {
		name := strings.ToLower(windowDef.Name)
		if _, ok := windowSpecs[name]; ok {
			return errors.Errorf("window %q is already defined", name)
		}
		windowSpecs[name] = windowDef
	}

	// Construct window definitions for each window function application.
	for _, windowFn := range n.funcs {
		windowDef := *windowFn.expr.WindowDef

		// Override referenced window specifications if applicable.
		modifyRef := false
		var refName string
		switch {
		case windowDef.RefName != "":
			// SELECT rank() OVER (w) FROM t WINDOW w as (...)
			// We copy the referenced window specification, and modify it if necessary.
			refName = strings.ToLower(windowDef.RefName)
			modifyRef = true
		case windowDef.Name != "":
			// SELECT rank() OVER w FROM t WINDOW w as (...)
			// We use the referenced window specification directly. without modification.
			refName = strings.ToLower(windowDef.Name)
		}
		if refName != "" {
			referencedSpec, ok := windowSpecs[refName]
			if !ok {
				return errors.Errorf("window %q does not exist", refName)
			}
			if modifyRef {
				// referencedSpec.Partitions is always used.
				if windowDef.Partitions != nil {
					return errors.Errorf("cannot override PARTITION BY clause of window %q", refName)
				}
				windowDef.Partitions = referencedSpec.Partitions

				// referencedSpec.OrderBy is used if set.
				if referencedSpec.OrderBy != nil {
					if windowDef.OrderBy != nil {
						return errors.Errorf("cannot override ORDER BY clause of window %q", refName)
					}
					windowDef.OrderBy = referencedSpec.OrderBy
				}
			} else {
				windowDef = *referencedSpec
			}
		}

		// TODO(nvanbenschoten) below we add renders to the selectNode for each
		// partition and order expression. We should handle cases where the expression
		// is already referenced by the query like sortNode does.

		// Validate PARTITION BY clause.
		for _, partition := range windowDef.Partitions {
			windowFn.partitionIdxs = append(windowFn.partitionIdxs, len(s.render))
			if err := s.addRender(parser.SelectExpr{Expr: partition}, nil); err != nil {
				return err
			}
		}

		// Validate ORDER BY clause.
		for _, orderBy := range windowDef.OrderBy {
			direction := encoding.Ascending
			if orderBy.Direction == parser.Descending {
				direction = encoding.Descending
			}
			ordering := sqlbase.ColumnOrderInfo{
				ColIdx:    len(s.render),
				Direction: direction,
			}
			windowFn.columnOrdering = append(windowFn.columnOrdering, ordering)
			if err := s.addRender(parser.SelectExpr{Expr: orderBy.Expr}, nil); err != nil {
				return err
			}
		}

		windowFn.windowDef = windowDef
	}
	return nil
}

// A windowNode implements the planNode interface and handles windowing logic.
// It "wraps" a planNode which is used to retrieve the un-windowed results.
type windowNode struct {
	planner *planner

	// The "wrapped" node (which returns un-windowed results).
	plan          planNode
	wrappedValues []parser.DTuple

	// A sparse array holding renders specific to this windowNode. This will contain
	// nil entries for renders that do not contain window functions, and which therefore
	// can be propagated directly from the "wrapped" node.
	windowRender []parser.TypedExpr

	// The populated values for this windowNode.
	values    valuesNode
	populated bool

	// The window functions handled by this windowNode. computeWindows will populate
	// an entire column in windowValues for each windowFuncHolder, in order.
	funcs        []*windowFuncHolder
	windowValues [][]parser.Datum
	curRowIdx    int

	explain explainMode
}

func (n *windowNode) Columns() []ResultColumn {
	return n.values.Columns()
}

func (n *windowNode) Ordering() orderingInfo {
	// Window partitions are returned un-ordered.
	return orderingInfo{}
}

func (n *windowNode) Values() parser.DTuple {
	return n.values.Values()
}

func (n *windowNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.plan.MarkDebug(mode)
}

func (n *windowNode) DebugValues() debugValues {
	if n.populated {
		return n.values.DebugValues()
	}

	// We are emitting a "buffered" row.
	vals := n.plan.DebugValues()
	if vals.output == debugValueRow {
		vals.output = debugValueBuffered
	}
	return vals
}

func (n *windowNode) expandPlan() error {
	// We do not need to recurse into the child node here; selectTopNode
	// does this for us.

	for _, e := range n.windowRender {
		if err := n.planner.expandSubqueryPlans(e); err != nil {
			return err
		}
	}

	return nil
}

func (n *windowNode) Start() error {
	if err := n.plan.Start(); err != nil {
		return err
	}

	for _, e := range n.windowRender {
		if err := n.planner.startSubqueryPlans(e); err != nil {
			return err
		}
	}

	return nil
}

func (n *windowNode) Next() (bool, error) {
	for !n.populated {
		next, err := n.plan.Next()
		if err != nil {
			return false, err
		}
		if !next {
			n.populated = true
			if err := n.computeWindows(); err != nil {
				return false, err
			}
			if err := n.populateValues(); err != nil {
				return false, err
			}
			break
		}
		if n.explain == explainDebug && n.plan.DebugValues().output != debugValueRow {
			// Pass through non-row debug values.
			return true, nil
		}

		// Add a copy of the row to wrappedValues.
		values := n.plan.Values()
		valuesCopy := make(parser.DTuple, len(values))
		copy(valuesCopy, values)
		n.wrappedValues = append(n.wrappedValues, valuesCopy)

		if n.explain == explainDebug {
			// Emit a "buffered" row.
			return true, nil
		}
	}

	return n.values.Next()
}

type partitionEntry struct {
	idx   int
	datum parser.DTuple
}

type partitionSorter struct {
	rows     []partitionEntry
	ordering sqlbase.ColumnOrdering
}

// partitionSorter implements the sort.Interface interface.
func (n *partitionSorter) Len() int           { return len(n.rows) }
func (n *partitionSorter) Swap(i, j int)      { n.rows[i], n.rows[j] = n.rows[j], n.rows[i] }
func (n *partitionSorter) Less(i, j int) bool { return n.Compare(i, j) < 0 }

// partitionSorter implements the peerGroupChecker interface.
func (n *partitionSorter) InSameGroup(i, j int) bool { return n.Compare(i, j) == 0 }

func (n *partitionSorter) Compare(i, j int) int {
	ra, rb := n.rows[i].datum, n.rows[j].datum
	for _, c := range n.ordering {
		var da, db parser.Datum
		if c.Direction == encoding.Ascending {
			da = ra[c.ColIdx]
			db = rb[c.ColIdx]
		} else {
			da = rb[c.ColIdx]
			db = ra[c.ColIdx]
		}
		if c := da.Compare(db); c != 0 {
			return c
		}
	}
	return 0
}

type allPeers struct{}

// allPeers implements the peerGroupChecker interface.
func (allPeers) InSameGroup(i, j int) bool { return true }

// peerGroupChecker can check if a pair of row indexes within a partition are
// in the same peer group.
type peerGroupChecker interface {
	InSameGroup(i, j int) bool
}

// computeWindows populates n.windowValues, adding a column of values to the
// 2D-slice for each window function in n.funcs.
func (n *windowNode) computeWindows() error {
	n.windowValues = make([][]parser.Datum, len(n.wrappedValues))
	for i := range n.wrappedValues {
		n.windowValues[i] = make([]parser.Datum, len(n.funcs))
	}

	var scratch []byte
	scratchDatum := make(parser.DTuple, 0, len(n.wrappedValues))
	for windowIdx, windowFn := range n.funcs {
		partitions := make(map[string][]partitionEntry)
		scratchDatum = scratchDatum[:len(windowFn.partitionIdxs)]

		// Partition rows into separate partitions based on hash values of the
		// window function's PARTITION BY attribute.
		//
		// TODO(nvanbenschoten) Window functions with the same window specification
		// can share partition and sorting work.
		// See Cao et al. [http://vldb.org/pvldb/vol5/p1244_yucao_vldb2012.pdf]
		for rowI, row := range n.wrappedValues {
			entry := partitionEntry{idx: rowI, datum: row}
			if len(windowFn.partitionIdxs) == 0 {
				// If no partition indexes are included for the window function, all
				// rows are added to the same partition.
				if rowI == 0 {
					partitions[""] = make([]partitionEntry, len(n.wrappedValues))
				}
				partitions[""][rowI] = entry
			} else {
				// If the window function has partition indexes, we hash the values of each
				// of these indexes for each row, and partition based on this hashed value.
				for i, idx := range windowFn.partitionIdxs {
					scratchDatum[i] = row[idx]
				}

				encoded, err := sqlbase.EncodeDTuple(scratch, scratchDatum)
				if err != nil {
					return err
				}

				partitions[string(encoded)] = append(partitions[string(encoded)], entry)
				scratch = encoded[:0]
			}
		}

		// For each partition, perform necessary sorting based on the window function's
		// ORDER BY attribute. After this, perform the window function computation for
		// each tuple and save the result in n.windowValues.
		//
		// TODO(nvanbenschoten)
		// - Investigate inter- and intra-partition parallelism
		// - Investigate more efficient aggregation techniques
		//   * Removable Cumulative
		//   * Segment Tree
		// See Leis et al. [http://www.vldb.org/pvldb/vol8/p1058-leis.pdf]
		for _, partition := range partitions {
			// TODO(nvanbenschoten) Handle framing here. Right now we only handle the default
			// framing option of RANGE UNBOUNDED PRECEDING. With ORDER BY, this sets the frame
			// to be all rows from the partition start up through the current row's last ORDER BY
			// peer. Without ORDER BY, all rows of the partition are included in the window frame,
			// since all rows become peers of the current row. Once we add better framing support,
			// we should flesh this logic out more.
			//
			// TODO(nvanbenschoten) Handle built-in window functions in addtition to
			// aggregate functions.
			agg := windowFn.expr.GetAggregateConstructor()()

			// Since we only support two types of window frames (see TODO above), we only
			// need two possible types of peerGroupChecker's to help determine peer groups
			// for given tuples.
			var peerGrouper peerGroupChecker
			if windowFn.columnOrdering != nil {
				// If an ORDER BY clause is provided, order the partition and use the
				// sorter as our peerGroupChecker.
				sorter := &partitionSorter{rows: partition, ordering: windowFn.columnOrdering}
				sort.Sort(sorter)
				peerGrouper = sorter
			} else {
				// If no ORDER BY clause is provided, all rows in the partition are peers.
				peerGrouper = allPeers{}
			}

			// Iterate over peer groups within partition.
			rowIdx := 0
			for rowIdx < len(partition) {
				// Compute the size of a peer group while accumulating in aggregate.
				peerGroupSize := 0
				for ; rowIdx < len(partition); rowIdx++ {
					if peerGroupSize > 0 {
						if !peerGrouper.InSameGroup(rowIdx-1, rowIdx) {
							break
						}
					}
					row := partition[rowIdx]
					agg.Add(row.datum[windowFn.subRenderCol])
					peerGroupSize++
				}

				// Set aggregate result for entire peer group.
				res := agg.Result()
				for ; peerGroupSize > 0; peerGroupSize-- {
					row := partition[rowIdx-peerGroupSize]
					n.windowValues[row.idx][windowIdx] = res
				}
			}
		}
	}

	return nil
}

// populateValues populated n.values with final datum values after computing
// window result values in n.windowValues.
func (n *windowNode) populateValues() error {
	n.values.rows = make([]parser.DTuple, len(n.wrappedValues))

	for i := range n.values.rows {
		curRow := make(parser.DTuple, len(n.windowRender))
		n.values.rows[i] = curRow

		n.curRowIdx = i // Point all windowFuncHolders to the correct row values.
		curColIdx := 0
		curFnIdx := 0
		for j := range n.values.rows[i] {
			if curWindowRender := n.windowRender[j]; curWindowRender == nil {
				// If the windowRender at this index is nil, propagate the datum
				// directly from the wrapped planNode. It wasn't changed by windowNode.
				curRow[j] = n.wrappedValues[i][curColIdx]
				curColIdx++
			} else {
				// If the windowRender is not nil, ignore 0 or more columns from the wrapped
				// planNode. These were used as arguments to window functions all beneath
				// a single windowRender.
				// SELECT rank() over () from t; -> ignore 0 from wrapped values
				// SELECT (avg(a) over () + avg(b) over ()) from t; -> ignore 2 from wrapped values
				for ; curFnIdx < len(n.funcs); curFnIdx++ {
					if n.funcs[curFnIdx].subRenderCol != curColIdx {
						break
					}
					curColIdx++
				}
				// Instead, we evaluate the current window render, which depends on at least
				// one window function, at the given row.
				res, err := curWindowRender.Eval(&n.planner.evalCtx)
				if err != nil {
					return err
				}
				curRow[j] = res
			}
		}
	}

	return nil
}

func (n *windowNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	name = "window"
	var buf bytes.Buffer
	for i, f := range n.funcs {
		if i > 0 {
			buf.WriteString(", ")
		}
		f.Format(&buf, parser.FmtSimple)
	}

	subplans := []planNode{n.plan}
	for _, e := range n.windowRender {
		subplans = n.planner.collectSubqueryPlans(e, subplans)
	}
	return name, buf.String(), subplans
}

func (n *windowNode) ExplainTypes(regTypes func(string, string)) {
	cols := n.Columns()
	for i, rexpr := range n.windowRender {
		if rexpr != nil {
			regTypes(fmt.Sprintf("render %s", cols[i].Name),
				parser.AsStringWithFlags(rexpr, parser.FmtShowTypes))
		}
	}
}

func (*windowNode) SetLimitHint(_ int64, _ bool) {}

// wrap the supplied planNode with the windowNode if windowing is required.
func (n *windowNode) wrap(plan planNode) planNode {
	if n == nil {
		return plan
	}
	n.plan = plan
	return n
}

type extractWindowFuncsVisitor struct {
	n *windowNode

	// Avoids allocations.
	subWindowVisitor parser.ContainsWindowVisitor

	// Persisted visitor state.
	aggregatesSeen map[*parser.FuncExpr]struct{}
	addedWindowFn  bool
	err            error
}

var _ parser.Visitor = &extractWindowFuncsVisitor{}

func (v *extractWindowFuncsVisitor) VisitPre(expr parser.Expr) (recurse bool, newExpr parser.Expr) {
	if v.err != nil {
		return false, expr
	}

	switch t := expr.(type) {
	case *parser.FuncExpr:
		switch {
		case t.IsWindowFunction():
			// Check if a parent node above this window function is an aggregate.
			if len(v.aggregatesSeen) > 0 {
				v.err = errors.Errorf("aggregate function calls cannot contain window function "+
					"call %s", t.Name)
				return false, expr
			}

			// Make sure the window function application is of either a built-in window
			// function or of a built-in aggregate function.
			if t.GetWindowConstructor() == nil && t.GetAggregateConstructor() == nil {
				v.err = fmt.Errorf("OVER specified, but %s is not a window function nor an "+
					"aggregate function", t.Name)
				return false, expr
			}

			// TODO(nvanbenschoten) Once we support built-in window functions instead of
			// just aggregates over a window, we'll need to support a variable number of
			// arguments.
			argExpr := t.Exprs[0].(parser.TypedExpr)

			// Make sure this window function does not contain another window function.
			if v.subWindowVisitor.ContainsWindowFunc(argExpr) {
				v.err = fmt.Errorf("window function calls cannot be nested under %s", t.Name)
				return false, expr
			}

			f := &windowFuncHolder{
				expr:   t,
				arg:    argExpr,
				window: v.n,
			}
			v.addedWindowFn = true
			v.n.funcs = append(v.n.funcs, f)
			return false, f
		case t.GetAggregateConstructor() != nil:
			// If we see an aggregation that is not used in a window function, we save it
			// in the visitor's seen aggregate set. The aggregate function will remain in
			// this set until the recursion into its children is complete.
			v.aggregatesSeen[t] = struct{}{}
		}
	}
	return true, expr
}

func (v *extractWindowFuncsVisitor) VisitPost(expr parser.Expr) parser.Expr {
	if fn, ok := expr.(*parser.FuncExpr); ok {
		delete(v.aggregatesSeen, fn)
	}
	return expr
}

// Extract windowFuncHolders from exprs that use window functions and check if they are valid.
// It will return the new expression tree, along with a flag signifying if a window function
// was seen.
// A window function is valid if:
// - it is not contained in an aggregate function
// - it does not contain another window function
// - it is either the application of a built-in window function
//   or of a built-in aggregate function
//
// For example:
// Invalid: `SELECT AVG(AVG(k) OVER ()) FROM kv`
// - The avg aggregate wraps the window function.
// Valid:      `SELECT AVG(k) OVER () FROM kv`
// Also valid: `SELECT AVG(AVG(k)) OVER () FROM kv`
// - Window functions can wrap aggregates.
// Invalid:    `SELECT NOW() OVER () FROM kv`
// - NOW() is not an aggregate or a window function.
func (v extractWindowFuncsVisitor) extract(typedExpr parser.TypedExpr) (parser.TypedExpr, bool, error) {
	expr, _ := parser.WalkExpr(&v, typedExpr)
	if v.err != nil {
		return nil, false, v.err
	}
	return expr.(parser.TypedExpr), v.addedWindowFn, nil
}

var _ parser.TypedExpr = &windowFuncHolder{}
var _ parser.VariableExpr = &windowFuncHolder{}

type windowFuncHolder struct {
	window *windowNode

	expr *parser.FuncExpr
	arg  parser.TypedExpr

	funcIdx      int // index of the windowFuncHolder in window.funcs
	subRenderCol int // column of the windowFuncHolder in window.wrappedValues

	windowDef      parser.WindowDef
	partitionIdxs  []int
	columnOrdering sqlbase.ColumnOrdering
}

func (*windowFuncHolder) Variable() {}

func (w *windowFuncHolder) Format(buf *bytes.Buffer, f parser.FmtFlags) {
	w.expr.Format(buf, f)
}

func (w *windowFuncHolder) String() string { return parser.AsString(w) }

func (w *windowFuncHolder) Walk(v parser.Visitor) parser.Expr { return w }

func (w *windowFuncHolder) TypeCheck(_ *parser.SemaContext, desired parser.Datum) (parser.TypedExpr, error) {
	return w, nil
}

func (w *windowFuncHolder) Eval(ctx *parser.EvalContext) (parser.Datum, error) {
	// Index into the windowValues computed in windowNode.computeWindows
	// to determine the Datum value to return. Evaluating this datum
	// is almost certainly the identity.
	return w.window.windowValues[w.window.curRowIdx][w.funcIdx].Eval(ctx)
}

func (w *windowFuncHolder) ReturnType() parser.Datum {
	return w.expr.ReturnType()
}
