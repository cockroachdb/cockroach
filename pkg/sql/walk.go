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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// planObserver is the interface to implement by components that need
// to visit a planNode tree.
// Used mainly by EXPLAIN, but also for the collector of back-references
// for view definitions.
type planObserver interface {
	// enterNode is invoked upon entering a tree node. It can return false to
	// stop the recursion at this node.
	enterNode(nodeName string, plan planNode) bool

	// expr is invoked for each expression field in each node.
	expr(nodeName, fieldName string, n int, expr parser.Expr)

	// attr is invoked for non-expression metadata in each node.
	attr(nodeName, fieldName, attr string)

	// leaveNode is invoked upon leaving a tree node.
	leaveNode(nodeName string)
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

	lv := *v
	lv.nodeName = nodeName(plan)
	recurse := v.observer.enterNode(lv.nodeName, plan)
	defer lv.observer.leaveNode(lv.nodeName)

	if !recurse {
		return
	}

	switch n := plan.(type) {
	case *valuesNode:
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
		lv.attr("size", description)

		var subplans []planNode
		for i, tuple := range n.tuples {
			for j, expr := range tuple {
				subplans = lv.expr(fmt.Sprintf("row %d, expr", i), j, expr, subplans)
			}
		}
		lv.subqueries(subplans)

	case *valueGenerator:
		subplans := lv.expr("expr", -1, n.expr, nil)
		lv.subqueries(subplans)

	case *scanNode:
		lv.attr("table", fmt.Sprintf("%s@%s", n.desc.Name, n.index.Name))
		spans := sqlbase.PrettySpans(n.spans, 2)
		if spans != "" {
			if spans == "-" {
				spans = "ALL"
			}
			lv.attr("spans", spans)
		}
		if n.limitHint > 0 && !n.limitSoft {
			lv.attr("limit", fmt.Sprintf("%d", n.limitHint))
		}
		subplans := lv.expr("filter", -1, n.filter, nil)
		lv.subqueries(subplans)

	case *selectNode:
		subplans := lv.expr("filter", -1, n.filter, nil)
		for i, r := range n.render {
			subplans = lv.expr("render", i, r, subplans)
		}
		if n.explain != explainNone {
			lv.attr("mode", explainStrings[n.explain])
		}
		lv.subqueries(subplans)
		lv.visit(n.source.plan)

	case *indexJoinNode:
		lv.visit(n.index)
		lv.visit(n.table)

	case *joinNode:
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
		lv.attr("type", jType)

		if len(n.pred.leftColNames) > 0 {
			var buf bytes.Buffer
			buf.WriteByte('(')
			n.pred.leftColNames.Format(&buf, parser.FmtSimple)
			buf.WriteString(") = (")
			n.pred.rightColNames.Format(&buf, parser.FmtSimple)
			buf.WriteByte(')')
			lv.attr("equality", buf.String())
		}
		subplans := lv.expr("filter", -1, n.pred.filter, nil)
		lv.subqueries(subplans)
		lv.visit(n.left.plan)
		lv.visit(n.right.plan)

	case *selectTopNode:
		if n.plan != nil {
			lv.visit(n.plan)
		} else {
			if n.limit != nil {
				lv.visit(n.limit)
			}
			if n.distinct != nil {
				lv.visit(n.distinct)
			}
			if n.sort != nil {
				lv.visit(n.sort)
			}
			if n.window != nil {
				lv.visit(n.window)
			}
			if n.group != nil {
				lv.visit(n.group)
			}
			lv.visit(n.source)
		}

	case *limitNode:
		subplans := lv.expr("count", -1, n.countExpr, nil)
		subplans = lv.expr("offset", -1, n.offsetExpr, subplans)
		lv.subqueries(subplans)
		lv.visit(n.plan)

	case *distinctNode:
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
			lv.attr("key", buf.String())
		}
		lv.visit(n.plan)

	case *sortNode:
		var columns ResultColumns
		if n.plan != nil {
			columns = n.plan.Columns()
		}
		// We use n.ordering and not plan.Ordering() because
		// plan.Ordering() does not include the added sort columns not
		// present in the output.
		order := orderingInfo{ordering: n.ordering}
		lv.attr("order", order.AsString(columns))
		switch ss := n.sortStrategy.(type) {
		case *iterativeSortStrategy:
			lv.attr("strategy", "iterative")
		case *sortTopKStrategy:
			lv.attr("strategy", fmt.Sprintf("top %d", ss.topK))
		}
		lv.visit(n.plan)

	case *groupNode:
		var subplans []planNode
		for i, agg := range n.funcs {
			subplans = lv.expr("aggregate", i, agg.expr, subplans)
		}
		for i, rexpr := range n.render {
			subplans = lv.expr("render", i, rexpr, subplans)
		}
		subplans = lv.expr("having", -1, n.having, subplans)
		lv.subqueries(subplans)
		lv.visit(n.plan)

	case *windowNode:
		var subplans []planNode
		for i, agg := range n.funcs {
			subplans = lv.expr("window", i, agg.expr, subplans)
		}
		for i, rexpr := range n.windowRender {
			subplans = lv.expr("render", i, rexpr, subplans)
		}
		lv.subqueries(subplans)
		lv.visit(n.plan)

	case *unionNode:
		lv.visit(n.left)
		lv.visit(n.right)

	case *splitNode:
		var subplans []planNode
		for i, e := range n.exprs {
			subplans = lv.expr("expr", i, e, subplans)
		}
		lv.subqueries(subplans)

	case *insertNode:
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
		lv.attr("into", buf.String())

		var subplans []planNode
		for i, dexpr := range n.defaultExprs {
			subplans = lv.expr("default", i, dexpr, subplans)
		}
		for i, cexpr := range n.checkHelper.exprs {
			subplans = lv.expr("check", i, cexpr, subplans)
		}
		for i, rexpr := range n.rh.exprs {
			subplans = lv.expr("returning", i, rexpr, subplans)
		}
		lv.subqueries(subplans)
		lv.visit(n.run.rows)

	case *updateNode:
		lv.attr("table", n.tableDesc.Name)
		if len(n.tw.ru.updateCols) > 0 {
			var buf bytes.Buffer
			for i, col := range n.tw.ru.updateCols {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.Name)
			}
			lv.attr("set", buf.String())
		}
		var subplans []planNode
		for i, rexpr := range n.rh.exprs {
			subplans = lv.expr("returning", i, rexpr, subplans)
		}
		lv.subqueries(subplans)
		lv.visit(n.run.rows)

	case *deleteNode:
		lv.attr("from", n.tableDesc.Name)
		var subplans []planNode
		for i, rexpr := range n.rh.exprs {
			subplans = lv.expr("returning", i, rexpr, subplans)
		}
		lv.subqueries(subplans)
		lv.visit(n.run.rows)

	case *createTableNode:
		if n.n.As() {
			lv.visit(n.sourcePlan)
		}

	case *createViewNode:
		lv.attr("query", n.sourceQuery)
		lv.visit(n.sourcePlan)

	case *delayedNode:
		lv.attr("source", n.name)
		lv.visit(n.plan)

	case *explainDebugNode:
		lv.visit(n.plan)

	case *ordinalityNode:
		lv.visit(n.source)

	case *explainTraceNode:
		lv.visit(n.plan)

	case *explainPlanNode:
		lv.attr("expanded", strconv.FormatBool(n.expanded))
		lv.visit(n.plan)
	}
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
	reflect.TypeOf(&createDatabaseNode{}): "create database",
	reflect.TypeOf(&createIndexNode{}):    "create index",
	reflect.TypeOf(&createTableNode{}):    "create table",
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
	reflect.TypeOf(&explainTraceNode{}):   "explain trace",
	reflect.TypeOf(&groupNode{}):          "group",
	reflect.TypeOf(&indexJoinNode{}):      "index-join",
	reflect.TypeOf(&insertNode{}):         "insert",
	reflect.TypeOf(&joinNode{}):           "join",
	reflect.TypeOf(&limitNode{}):          "limit",
	reflect.TypeOf(&ordinalityNode{}):     "ordinality",
	reflect.TypeOf(&scanNode{}):           "scan",
	reflect.TypeOf(&selectNode{}):         "render/filter",
	reflect.TypeOf(&selectTopNode{}):      "select",
	reflect.TypeOf(&sortNode{}):           "sort",
	reflect.TypeOf(&splitNode{}):          "split",
	reflect.TypeOf(&unionNode{}):          "union",
	reflect.TypeOf(&updateNode{}):         "update",
	reflect.TypeOf(&valueGenerator{}):     "generator",
	reflect.TypeOf(&valuesNode{}):         "values",
	reflect.TypeOf(&windowNode{}):         "window",
}
