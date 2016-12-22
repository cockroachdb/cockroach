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

package sql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// planObserver is the interface to implement by components that need
// to visit a planNode tree.
// Used mainly by EXPLAIN, but also for the collector of back-references
// for view definitions.
type planObserver interface {
	// node is invoked upon entering a tree node. It can return false to
	// stop the recursion at this node.
	node(nodeName string, plan planNode) bool

	// expr is invoked for each expression field in each node.
	expr(nodeName, fieldName string, n int, expr parser.Expr)

	// attr is invoked for non-expression metadata in each node.
	attr(nodeName, fieldName, attr string)

	// leave is invoked upon leaving a tree node.
	leave(nodeName string)
}

// planVisitor is the support structure for visit().
type planVisitor struct {
	p        *planner
	observer planObserver
	nodeName string
}

// visit performs a depth-first traversal of the plan given as
// argument, informing the planObserver of the node details at each
// level.
func (v *planVisitor) visit(plan planNode) {
	if plan == nil {
		return
	}
	prevName := v.nodeName
	defer func() {
		v.observer.leave(v.nodeName)
		v.nodeName = prevName
	}()

	v.nodeName = formatNodeName(plan)
	switch n := plan.(type) {
	case *emptyNode:
		if n.results {
			v.nodeName = "nullrow"
		}
		v.node(plan)

	case *valuesNode:
		if v.node(plan) {
			suffix := "not yet populated"
			if n.rows != nil {
				suffix = fmt.Sprintf("%d row%s",
					n.rows.Len(), util.Pluralize(int64(n.rows.Len())))
			} else if n.tuples != nil {
				suffix = fmt.Sprintf("%d row%s",
					len(n.tuples), util.Pluralize(int64(len(n.tuples))))
			}
			description := fmt.Sprintf("%d column%s, %s",
				len(n.columns), util.Pluralize(int64(len(n.columns))), suffix)
			v.attr("size", description)

			var subplans []planNode
			for i, tuple := range n.tuples {
				for j, expr := range tuple {
					subplans = v.expr(fmt.Sprintf("row %d, expr", i), j, expr, subplans)
				}
			}
			v.subqueries(subplans)
		}

	case *valueGenerator:
		v.nodeName = "generator"
		if v.node(plan) {
			subplans := v.expr("expr", -1, n.expr, nil)
			v.subqueries(subplans)
		}

	case *scanNode:
		if n.reverse {
			v.nodeName = "revscan"
		}
		if v.node(plan) {
			v.attr("table", fmt.Sprintf("%s@%s", n.desc.Name, n.index.Name))
			spans := sqlbase.PrettySpans(n.spans, 2)
			if spans != "" {
				if spans == "-" {
					spans = "ALL"
				}
				v.attr("spans", spans)
			}
			if n.limitHint > 0 && !n.limitSoft {
				v.attr("limit", fmt.Sprintf("%d", n.limitHint))
			}
			subplans := v.expr("filter", -1, n.filter, nil)
			v.subqueries(subplans)
		}

	case *selectNode:
		v.nodeName = "render/filter"
		if v.node(plan) {
			subplans := v.expr("filter", -1, n.filter, nil)
			for i, r := range n.render {
				subplans = v.expr("render", i, r, subplans)
			}
			if n.explain != explainNone {
				v.attr("mode", explainStrings[n.explain])
			}
			v.subqueries(subplans)
			v.visit(n.source.plan)
		}

	case *indexJoinNode:
		v.nodeName = "index-join"
		if v.node(plan) {
			v.visit(n.index)
			v.visit(n.table)
		}

	case *joinNode:
		if v.node(plan) {
			jType := ""
			switch n.joinType {
			case joinTypeInner:
				jType = "inner"
				if len(n.pred.leftColNames) == 0 && n.pred.filter == nil {
					jType = "cross"
				}
			case joinTypeLeftOuter:
				jType = "left outer"
			case joinTypeRightOuter:
				jType = "right outer"
			case joinTypeFullOuter:
				jType = "full outer"
			}
			v.attr("type", jType)

			if len(n.pred.leftColNames) > 0 {
				var buf bytes.Buffer
				buf.WriteByte('(')
				n.pred.leftColNames.Format(&buf, parser.FmtSimple)
				buf.WriteString(") = (")
				n.pred.rightColNames.Format(&buf, parser.FmtSimple)
				buf.WriteByte(')')
				v.attr("equality", buf.String())
			}
			subplans := v.expr("filter", -1, n.pred.filter, nil)
			v.subqueries(subplans)
			v.visit(n.left.plan)
			v.visit(n.right.plan)
		}

	case *selectTopNode:
		v.nodeName = "select"
		if v.node(plan) {
			if n.plan != nil {
				v.visit(n.plan)
			} else {
				if n.limit != nil {
					v.visit(n.limit)
				}
				if n.distinct != nil {
					v.visit(n.distinct)
				}
				if n.sort != nil {
					v.visit(n.sort)
				}
				if n.window != nil {
					v.visit(n.window)
				}
				if n.group != nil {
					v.visit(n.group)
				}
				v.visit(n.source)
			}
		}

	case *limitNode:
		if v.node(plan) {
			subplans := v.expr("count", -1, n.countExpr, nil)
			subplans = v.expr("offset", -1, n.offsetExpr, subplans)
			v.subqueries(subplans)
			v.visit(n.plan)
		}

	case *distinctNode:
		if v.node(plan) {
			if n.columnsInOrder != nil {
				var buf bytes.Buffer
				prefix := ""
				columns := n.Columns()
				for i, key := range n.columnsInOrder {
					if key {
						buf.WriteString(prefix)
						buf.WriteString(columns[i].Name)
						prefix = ", "
					}
				}
				v.attr("key", buf.String())
			}
			v.visit(n.plan)
		}

	case *sortNode:
		if !n.needSort {
			v.nodeName = "nosort"
		}
		if v.node(plan) {
			var columns ResultColumns
			if n.plan != nil {
				columns = n.plan.Columns()
			}
			// We use n.ordering and not plan.Ordering() because
			// plan.Ordering() does not include the added sort columns not
			// present in the output.
			order := orderingInfo{ordering: n.ordering}
			v.attr("order", order.AsString(columns))
			switch ss := n.sortStrategy.(type) {
			case *iterativeSortStrategy:
				v.attr("strategy", "iterative")
			case *sortTopKStrategy:
				v.attr("strategy", fmt.Sprintf("top %d", ss.topK))
			}
			v.visit(n.plan)
		}

	case *groupNode:
		if v.node(plan) {
			var subplans []planNode
			for i, agg := range n.funcs {
				subplans = v.expr("aggregate", i, agg.expr, subplans)
			}
			for i, rexpr := range n.render {
				subplans = v.expr("render", i, rexpr, subplans)
			}
			subplans = v.expr("having", -1, n.having, subplans)
			v.subqueries(subplans)
			v.visit(n.plan)
		}

	case *windowNode:
		if v.node(plan) {
			var subplans []planNode
			for i, agg := range n.funcs {
				subplans = v.expr("window", i, agg.expr, subplans)
			}
			for i, rexpr := range n.windowRender {
				subplans = v.expr("render", i, rexpr, subplans)
			}
			v.subqueries(subplans)
			v.visit(n.plan)
		}

	case *unionNode:
		if n.emitAll {
			v.nodeName = "append"
		}
		if v.node(plan) {
			v.visit(n.left)
			v.visit(n.right)
		}

	case *splitNode:
		if v.node(plan) {
			var subplans []planNode
			for i, e := range n.exprs {
				subplans = v.expr("expr", i, e, subplans)
			}
			v.subqueries(subplans)
		}

	case *insertNode:
		if v.node(plan) {
			var buf bytes.Buffer
			buf.WriteString(n.tableDesc.Name)
			buf.WriteByte('(')
			for i, col := range n.insertCols {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.Name)
			}
			buf.WriteByte(')')
			v.attr("into", buf.String())

			var subplans []planNode
			for i, dexpr := range n.defaultExprs {
				subplans = v.expr("default", i, dexpr, subplans)
			}
			for i, cexpr := range n.checkHelper.exprs {
				subplans = v.expr("check", i, cexpr, subplans)
			}
			for i, rexpr := range n.rh.exprs {
				subplans = v.expr("returning", i, rexpr, subplans)
			}
			v.subqueries(subplans)
			v.visit(n.run.rows)
		}

	case *updateNode:
		if v.node(plan) {
			v.attr("table", n.tableDesc.Name)
			if len(n.tw.ru.updateCols) > 0 {
				var buf bytes.Buffer
				for i, col := range n.tw.ru.updateCols {
					if i > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(col.Name)
				}
				v.attr("set", buf.String())
			}
			var subplans []planNode
			for i, rexpr := range n.rh.exprs {
				subplans = v.expr("returning", i, rexpr, subplans)
			}
			v.subqueries(subplans)
			v.visit(n.run.rows)
		}

	case *deleteNode:
		if v.node(plan) {
			v.attr("from", n.tableDesc.Name)
			var subplans []planNode
			for i, rexpr := range n.rh.exprs {
				subplans = v.expr("returning", i, rexpr, subplans)
			}
			v.subqueries(subplans)
			v.visit(n.run.rows)
		}

	case *createTableNode:
		if v.node(plan) {
			if n.n.As() {
				v.visit(n.sourcePlan)
			}
		}

	case *createViewNode:
		if v.node(plan) {
			v.attr("query", n.sourceQuery)
			v.visit(n.sourcePlan)
		}

	case *delayedNode:
		v.nodeName = "virtual table"
		if v.node(plan) {
			v.attr("source", n.name)
			v.visit(n.plan)
		}

	case *explainDebugNode:
		if v.node(plan) {
			v.visit(n.plan)
		}

	case *ordinalityNode:
		if v.node(plan) {
			v.visit(n.source)
		}

	case *explainTraceNode:
		if v.node(plan) {
			v.visit(n.plan)
		}

	case *explainPlanNode:
		if v.node(plan) {
			v.attr("expanded", strconv.FormatBool(n.expanded))
			v.visit(n.plan)
		}

	default:
		v.node(n)
	}
}

// node wraps observer.node() and provides it with the current node's name.
func (v *planVisitor) node(plan planNode) bool {
	recurse := v.observer.node(v.nodeName, plan)
	if plan == nil {
		recurse = false
	}
	return recurse
}

// attr wraps observer.attr() and provides it with the current node's name.
func (v *planVisitor) attr(name, value string) {
	v.observer.attr(v.nodeName, name, value)
}

// subqueries informs the observer that the following sub-plans are
// for sub-queries.
func (v *planVisitor) subqueries(subplans []planNode) {
	if len(subplans) == 0 {
		return
	}
	v.attr("subqueries", strconv.Itoa(len(subplans)))
	for _, p := range subplans {
		v.visit(p)
	}
}

// expr wraps observer.expr() and provides it with the current node's
// name. It also collects the plans for the sub-queries.
func (v *planVisitor) expr(
	fieldName string, n int, expr parser.Expr, subplans []planNode,
) []planNode {
	v.observer.expr(v.nodeName, fieldName, n, expr)
	subplans = v.p.collectSubqueryPlans(expr, subplans)
	return subplans
}

// formatNodeName simplifies plan node type names of the form
// "*sql.SomeNameNode" to "some name".
func formatNodeName(plan planNode) string {
	var buf bytes.Buffer
	tname := fmt.Sprintf("%T", plan)
	tname = strings.TrimPrefix(tname, "*sql.")
	tname = strings.TrimSuffix(tname, "Node")
	for i := range tname {
		c := tname[i]
		if c >= 'A' && c <= 'Z' {
			buf.WriteByte(' ')
			buf.WriteByte(c - 'A' + 'a')
		} else if c >= 'a' && c <= 'z' {
			buf.WriteByte(c)
		}
	}
	return buf.String()
}
