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
	"reflect"
	"strconv"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// planObserver is the interface to implement by components that need
// to visit a planNode tree.
// Used mainly by EXPLAIN, but also for the collector of back-references
// for view definitions.
type planObserver struct {
	// enterNode is invoked upon entering a tree node. It can return false to
	// stop the recursion at this node.
	enterNode func(ctx context.Context, nodeName string, plan planNode) bool

	// expr is invoked for each expression field in each node.
	expr func(nodeName, fieldName string, n int, expr parser.Expr)

	// attr is invoked for non-expression metadata in each node.
	attr func(nodeName, fieldName, attr string)

	// leaveNode is invoked upon leaving a tree node.
	leaveNode func(nodeName string)

	// subqueryNode is invoked for each sub-query node. It can return
	// an error to stop the recursion entirely.
	subqueryNode func(ctx context.Context, sq *subquery) error
}

// walkPlan performs a depth-first traversal of the plan given as
// argument, informing the planObserver of the node details at each
// level.
func walkPlan(ctx context.Context, plan planNode, observer planObserver) error {
	v := makePlanVisitor(ctx, observer)
	v.visit(plan)
	return v.err
}

// planVisitor is the support structure for walkPlan().
type planVisitor struct {
	observer planObserver
	ctx      context.Context

	// subplans is a temporary accumulator array used when collecting
	// sub-query plans at each planNode.
	subplans []planNode
	err      error
}

// makePlanVisitor creates a planVisitor instance.
// ctx will be stored in the planVisitor and used when visiting planNode's and
// expressions..
func makePlanVisitor(ctx context.Context, observer planObserver) planVisitor {
	return planVisitor{observer: observer, ctx: ctx}
}

// visit is the recursive function that supports walkPlan().
func (v *planVisitor) visit(plan planNode) {
	if v.err != nil {
		return
	}

	name := nodeName(plan)
	recurse := true
	if v.observer.enterNode != nil {
		recurse = v.observer.enterNode(v.ctx, name, plan)
	}
	if v.observer.leaveNode != nil {
		defer v.observer.leaveNode(name)
	}

	if !recurse {
		return
	}

	switch n := plan.(type) {
	case *valuesNode:
		if v.observer.attr != nil {
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
			v.observer.attr(name, "size", description)
		}

		var subplans []planNode
		for i, tuple := range n.tuples {
			for j, expr := range tuple {
				if n.columns[j].Omitted {
					continue
				}
				var fieldName string
				if v.observer.attr != nil {
					fieldName = fmt.Sprintf("row %d, expr", i)
				}
				subplans = v.expr(name, fieldName, j, expr, subplans)
			}
		}
		v.subqueries(name, subplans)

	case *valueGenerator:
		subplans := v.expr(name, "expr", -1, n.expr, nil)
		v.subqueries(name, subplans)

	case *scanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", fmt.Sprintf("%s@%s", n.desc.Name, n.index.Name))
			if n.noIndexJoin {
				v.observer.attr(name, "hint", "no index join")
			}
			if n.specifiedIndex != nil {
				v.observer.attr(name, "hint", fmt.Sprintf("force index @%s", n.specifiedIndex.Name))
			}
			spans := sqlbase.PrettySpans(n.spans, 2)
			if spans != "" {
				if spans == "-" {
					spans = "ALL"
				}
				v.observer.attr(name, "spans", spans)
			}
			if n.hardLimit > 0 && isFilterTrue(n.filter) {
				v.observer.attr(name, "limit", fmt.Sprintf("%d", n.hardLimit))
			}
		}
		subplans := v.expr(name, "filter", -1, n.filter, nil)
		v.subqueries(name, subplans)

	case *filterNode:
		subplans := v.expr(name, "filter", -1, n.filter, nil)
		if n.explain != explainNone && v.observer.attr != nil {
			v.observer.attr(name, "mode", explainStrings[n.explain])
		}
		v.subqueries(name, subplans)
		v.visit(n.source.plan)

	case *renderNode:
		var subplans []planNode
		for i, r := range n.render {
			subplans = v.expr(name, "render", i, r, subplans)
		}
		v.subqueries(name, subplans)
		v.visit(n.source.plan)

	case *indexJoinNode:
		v.visit(n.index)
		v.visit(n.table)

	case *joinNode:
		if v.observer.attr != nil {
			jType := ""
			switch n.joinType {
			case joinTypeInner:
				jType = "inner"
				if len(n.pred.leftColNames) == 0 && n.pred.onCond == nil {
					jType = "cross"
				}
			case joinTypeLeftOuter:
				jType = "left outer"
			case joinTypeRightOuter:
				jType = "right outer"
			case joinTypeFullOuter:
				jType = "full outer"
			}
			v.observer.attr(name, "type", jType)

			if len(n.pred.leftColNames) > 0 {
				var buf bytes.Buffer
				buf.WriteByte('(')
				n.pred.leftColNames.Format(&buf, parser.FmtSimple)
				buf.WriteString(") = (")
				n.pred.rightColNames.Format(&buf, parser.FmtSimple)
				buf.WriteByte(')')
				v.observer.attr(name, "equality", buf.String())
			}
		}
		subplans := v.expr(name, "pred", -1, n.pred.onCond, nil)
		v.subqueries(name, subplans)
		v.visit(n.left.plan)
		v.visit(n.right.plan)

	case *limitNode:
		subplans := v.expr(name, "count", -1, n.countExpr, nil)
		subplans = v.expr(name, "offset", -1, n.offsetExpr, subplans)
		v.subqueries(name, subplans)
		v.visit(n.plan)

	case *distinctNode:
		if n.columnsInOrder != nil && v.observer.attr != nil {
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
			v.observer.attr(name, "key", buf.String())
		}
		v.visit(n.plan)

	case *sortNode:
		if v.observer.attr != nil {
			var columns sqlbase.ResultColumns
			if n.plan != nil {
				columns = n.plan.Columns()
			}
			// We use n.ordering and not plan.Ordering() because
			// plan.Ordering() does not include the added sort columns not
			// present in the output.
			order := orderingInfo{ordering: n.ordering}
			v.observer.attr(name, "order", order.AsString(columns))
			switch ss := n.sortStrategy.(type) {
			case *iterativeSortStrategy:
				v.observer.attr(name, "strategy", "iterative")
			case *sortTopKStrategy:
				v.observer.attr(name, "strategy", fmt.Sprintf("top %d", ss.topK))
			}
		}
		v.visit(n.plan)

	case *groupNode:
		var subplans []planNode
		for i, agg := range n.funcs {
			subplans = v.expr(name, "aggregate", i, agg.expr, subplans)
		}
		for i, rexpr := range n.render {
			subplans = v.expr(name, "render", i, rexpr, subplans)
		}
		subplans = v.expr(name, "having", -1, n.having, subplans)
		v.subqueries(name, subplans)
		v.visit(n.plan)

	case *windowNode:
		var subplans []planNode
		for i, agg := range n.funcs {
			subplans = v.expr(name, "window", i, agg.expr, subplans)
		}
		for i, rexpr := range n.windowRender {
			subplans = v.expr(name, "render", i, rexpr, subplans)
		}
		v.subqueries(name, subplans)
		v.visit(n.plan)

	case *unionNode:
		v.visit(n.left)
		v.visit(n.right)

	case *splitNode:
		v.visit(n.rows)

	case *relocateNode:
		v.visit(n.rows)

	case *insertNode:
		if v.observer.attr != nil {
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
			v.observer.attr(name, "into", buf.String())
		}

		var subplans []planNode
		for i, dexpr := range n.defaultExprs {
			subplans = v.expr(name, "default", i, dexpr, subplans)
		}
		for i, cexpr := range n.checkHelper.exprs {
			subplans = v.expr(name, "check", i, cexpr, subplans)
		}
		for i, rexpr := range n.rh.exprs {
			subplans = v.expr(name, "returning", i, rexpr, subplans)
		}
		n.tw.walkExprs(func(d string, i int, e parser.TypedExpr) {
			subplans = v.expr(name, d, i, e, subplans)
		})
		v.subqueries(name, subplans)
		v.visit(n.run.rows)

	case *updateNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", n.tableDesc.Name)
			if len(n.tw.ru.UpdateCols) > 0 {
				var buf bytes.Buffer
				for i, col := range n.tw.ru.UpdateCols {
					if i > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(col.Name)
				}
				v.observer.attr(name, "set", buf.String())
			}
		}
		var subplans []planNode
		for i, rexpr := range n.rh.exprs {
			subplans = v.expr(name, "returning", i, rexpr, subplans)
		}
		n.tw.walkExprs(func(d string, i int, e parser.TypedExpr) {
			subplans = v.expr(name, d, i, e, subplans)
		})
		v.subqueries(name, subplans)
		v.visit(n.run.rows)

	case *deleteNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "from", n.tableDesc.Name)
		}
		var subplans []planNode
		for i, rexpr := range n.rh.exprs {
			subplans = v.expr(name, "returning", i, rexpr, subplans)
		}
		n.tw.walkExprs(func(d string, i int, e parser.TypedExpr) {
			subplans = v.expr(name, d, i, e, subplans)
		})
		v.subqueries(name, subplans)
		v.visit(n.run.rows)

	case *createTableNode:
		if n.n.As() {
			v.visit(n.sourcePlan)
		}

	case *createViewNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "query", n.sourceQuery)
		}
		v.visit(n.sourcePlan)

	case *delayedNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "source", n.name)
		}
		if n.plan != nil {
			v.visit(n.plan)
		}

	case *explainDebugNode:
		v.visit(n.plan)

	case *explainDistSQLNode:
		v.visit(n.plan)

	case *ordinalityNode:
		v.visit(n.source)

	case *explainTraceNode:
		v.visit(n.plan)

	case *explainPlanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "expanded", strconv.FormatBool(n.expanded))
		}
		v.visit(n.plan)
	}
}

// subqueries informs the observer that the following sub-plans are
// for sub-queries.
func (v *planVisitor) subqueries(nodeName string, subplans []planNode) {
	if len(subplans) == 0 || v.err != nil {
		return
	}
	if v.observer.attr != nil {
		v.observer.attr(nodeName, "subqueries", strconv.Itoa(len(subplans)))
	}
	for _, p := range subplans {
		v.visit(p)
	}
}

// expr wraps observer.expr() and provides it with the current node's
// name. It also collects the plans for the sub-queries.
func (v *planVisitor) expr(
	nodeName string, fieldName string, n int, expr parser.Expr, subplans []planNode,
) []planNode {
	if v.err != nil {
		return subplans
	}

	if v.observer.expr != nil {
		v.observer.expr(nodeName, fieldName, n, expr)
	}

	if expr != nil {
		// Note: the recursion through WalkExprConst does nothing else
		// than calling observer.subqueryNode() and collect subplans in
		// v.subplans, in particular it does not recurse into the
		// collected subplans (this recursion is performed by visit() only
		// after all the subplans have been collected). Therefore, there
		// is no risk that v.subplans will be clobbered by a recursion
		// into visit().
		v.subplans = subplans
		parser.WalkExprConst(v, expr)
		subplans = v.subplans
		v.subplans = nil
	}
	return subplans
}

// planVisitor is also an Expr visitor whose task is to collect
// sub-query plans for the surrounding planNode.
var _ parser.Visitor = &planVisitor{}

func (v *planVisitor) VisitPre(expr parser.Expr) (bool, parser.Expr) {
	if v.err != nil {
		return false, expr
	}
	if sq, ok := expr.(*subquery); ok {
		if v.observer.subqueryNode != nil {
			if err := v.observer.subqueryNode(v.ctx, sq); err != nil {
				v.err = err
				return false, expr
			}
		}
		if sq.plan != nil {
			v.subplans = append(v.subplans, sq.plan)
		}
		return false, expr
	}
	return true, expr
}
func (v *planVisitor) VisitPost(expr parser.Expr) parser.Expr { return expr }

// nodeName returns the name of the given planNode as string.  The
// node's current state is taken into account, e.g. sortNode has
// either name "sort" or "nosort" depending on whether sorting is
// needed.
func nodeName(plan planNode) string {
	// Some nodes have custom names depending on attributes.
	switch n := plan.(type) {
	case *emptyNode:
		if n.results {
			return "nullrow"
		}
	case *sortNode:
		if !n.needSort {
			return "nosort"
		}
	case *scanNode:
		if n.reverse {
			return "revscan"
		}
	case *unionNode:
		if n.emitAll {
			return "append"
		}
	}

	name, ok := planNodeNames[reflect.TypeOf(plan)]
	if !ok {
		panic(fmt.Sprintf("name missing for type %T", plan))
	}

	return name
}

// planNodeNames is the mapping from node type to strings.  The
// strings are constant and not precomptued so that the type names can
// be changed without changing the output of "EXPLAIN".
var planNodeNames = map[reflect.Type]string{
	reflect.TypeOf(&alterTableNode{}):     "alter table",
	reflect.TypeOf(&copyNode{}):           "copy",
	reflect.TypeOf(&createDatabaseNode{}): "create database",
	reflect.TypeOf(&createIndexNode{}):    "create index",
	reflect.TypeOf(&createTableNode{}):    "create table",
	reflect.TypeOf(&createUserNode{}):     "create user",
	reflect.TypeOf(&createViewNode{}):     "create view",
	reflect.TypeOf(&delayedNode{}):        "virtual table",
	reflect.TypeOf(&deleteNode{}):         "delete",
	reflect.TypeOf(&distinctNode{}):       "distinct",
	reflect.TypeOf(&dropDatabaseNode{}):   "drop database",
	reflect.TypeOf(&dropIndexNode{}):      "drop index",
	reflect.TypeOf(&dropTableNode{}):      "drop table",
	reflect.TypeOf(&dropViewNode{}):       "drop view",
	reflect.TypeOf(&emptyNode{}):          "empty",
	reflect.TypeOf(&explainDebugNode{}):   "explain debug",
	reflect.TypeOf(&explainDistSQLNode{}): "explain dist_sql",
	reflect.TypeOf(&explainPlanNode{}):    "explain plan",
	reflect.TypeOf(&explainTraceNode{}):   "explain trace",
	reflect.TypeOf(&filterNode{}):         "filter",
	reflect.TypeOf(&groupNode{}):          "group",
	reflect.TypeOf(&hookFnNode{}):         "plugin",
	reflect.TypeOf(&indexJoinNode{}):      "index-join",
	reflect.TypeOf(&insertNode{}):         "insert",
	reflect.TypeOf(&joinNode{}):           "join",
	reflect.TypeOf(&limitNode{}):          "limit",
	reflect.TypeOf(&ordinalityNode{}):     "ordinality",
	reflect.TypeOf(&relocateNode{}):       "relocate",
	reflect.TypeOf(&renderNode{}):         "render",
	reflect.TypeOf(&scanNode{}):           "scan",
	reflect.TypeOf(&scatterNode{}):        "scatter",
	reflect.TypeOf(&showRangesNode{}):     "showRanges",
	reflect.TypeOf(&sortNode{}):           "sort",
	reflect.TypeOf(&splitNode{}):          "split",
	reflect.TypeOf(&unionNode{}):          "union",
	reflect.TypeOf(&updateNode{}):         "update",
	reflect.TypeOf(&valueGenerator{}):     "generator",
	reflect.TypeOf(&valuesNode{}):         "values",
	reflect.TypeOf(&windowNode{}):         "window",
}
