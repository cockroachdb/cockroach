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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	enterNode func(ctx context.Context, nodeName string, plan planNode) (bool, error)

	// expr is invoked for each expression field in each node.
	expr func(nodeName, fieldName string, n int, expr tree.Expr)

	// attr is invoked for non-expression metadata in each node.
	attr func(nodeName, fieldName, attr string)

	// leaveNode is invoked upon leaving a tree node.
	leaveNode func(nodeName string, plan planNode) error
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
		recurse, v.err = v.observer.enterNode(v.ctx, name, plan)
		if v.err != nil {
			return
		}
	}
	if v.observer.leaveNode != nil {
		defer func() {
			if v.err != nil {
				return
			}
			v.err = v.observer.leaveNode(name, plan)
		}()
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

		if v.observer.expr != nil {
			for i, tuple := range n.tuples {
				for j, expr := range tuple {
					if n.columns[j].Omitted {
						continue
					}
					var fieldName string
					if v.observer.attr != nil {
						fieldName = fmt.Sprintf("row %d, expr", i)
					}
					v.expr(name, fieldName, j, expr)
				}
			}
		}

	case *valueGenerator:
		if v.observer.expr != nil {
			v.expr(name, "expr", -1, n.expr)
		}

	case *scanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", fmt.Sprintf("%s@%s", n.desc.Name, n.index.Name))
			if n.noIndexJoin {
				v.observer.attr(name, "hint", "no index join")
			}
			if n.specifiedIndex != nil {
				v.observer.attr(name, "hint", fmt.Sprintf("force index @%s", n.specifiedIndex.Name))
			}
			spans := sqlbase.PrettySpans(n.index, n.spans, 2)
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
		if v.observer.expr != nil {
			v.expr(name, "filter", -1, n.filter)
		}

	case *filterNode:
		if v.observer.expr != nil {
			v.expr(name, "filter", -1, n.filter)
		}
		v.visit(n.source.plan)

	case *renderNode:
		if v.observer.expr != nil {
			for i, r := range n.render {
				v.expr(name, "render", i, r)
			}
		}
		v.visit(n.source.plan)

	case *indexJoinNode:
		v.visit(n.index)
		v.visit(n.table)

	case *joinNode:
		if v.observer.attr != nil {
			jType := ""
			switch n.joinType {
			case sqlbase.InnerJoin:
				jType = "inner"
				if len(n.pred.leftColNames) == 0 && n.pred.onCond == nil {
					jType = "cross"
				}
			case sqlbase.LeftOuterJoin:
				jType = "left outer"
			case sqlbase.RightOuterJoin:
				jType = "right outer"
			case sqlbase.FullOuterJoin:
				jType = "full outer"
			}
			v.observer.attr(name, "type", jType)

			if len(n.pred.leftColNames) > 0 {
				f := tree.NewFmtCtxWithBuf(tree.FmtSimple)
				f.WriteByte('(')
				f.FormatNode(&n.pred.leftColNames)
				f.WriteString(") = (")
				f.FormatNode(&n.pred.rightColNames)
				f.WriteByte(')')
				v.observer.attr(name, "equality", f.CloseAndGetString())
			}
			if len(n.mergeJoinOrdering) > 0 {
				// The ordering refers to equality columns
				eqCols := make(sqlbase.ResultColumns, len(n.pred.leftEqualityIndices))
				for i := range eqCols {
					eqCols[i].Name = fmt.Sprintf("(%s=%s)", n.pred.leftColNames[i], n.pred.rightColNames[i])
				}
				var order physicalProps
				for _, o := range n.mergeJoinOrdering {
					order.addOrderColumn(o.ColIdx, o.Direction)
				}
				v.observer.attr(name, "mergeJoinOrder", order.AsString(eqCols))
			}
		}
		if v.observer.expr != nil {
			v.expr(name, "pred", -1, n.pred.onCond)
		}
		v.visit(n.left.plan)
		v.visit(n.right.plan)

	case *limitNode:
		if v.observer.expr != nil {
			v.expr(name, "count", -1, n.countExpr)
			v.expr(name, "offset", -1, n.offsetExpr)
		}
		v.visit(n.plan)

	case *distinctNode:
		if v.observer.attr == nil {
			v.visit(n.plan)
			break
		}

		if !n.distinctOnColIdxs.Empty() {
			var buf bytes.Buffer
			prefix := ""
			columns := planColumns(n.plan)
			n.distinctOnColIdxs.ForEach(func(col int) {
				buf.WriteString(prefix)
				buf.WriteString(columns[col].Name)
				prefix = ", "
			})
			v.observer.attr(name, "distinct on", buf.String())
		}

		if n.columnsInOrder != nil {
			var buf bytes.Buffer
			prefix := ""
			columns := planColumns(n.plan)
			for i, key := range n.columnsInOrder {
				if key {
					buf.WriteString(prefix)
					buf.WriteString(columns[i].Name)
					prefix = ", "
				}
			}
			v.observer.attr(name, "order key", buf.String())
		}

		v.visit(n.plan)

	case *sortNode:
		if v.observer.attr != nil {
			var columns sqlbase.ResultColumns
			if n.plan != nil {
				columns = planColumns(n.plan)
			}
			// We use n.ordering and not plan.Ordering() because
			// plan.Ordering() does not include the added sort columns not
			// present in the output.
			var order physicalProps
			for _, o := range n.ordering {
				order.addOrderColumn(o.ColIdx, o.Direction)
			}
			v.observer.attr(name, "order", order.AsString(columns))
			switch ss := n.run.sortStrategy.(type) {
			case *iterativeSortStrategy:
				v.observer.attr(name, "strategy", "iterative")
			case *sortTopKStrategy:
				v.observer.attr(name, "strategy", fmt.Sprintf("top %d", ss.topK))
			}
		}
		v.visit(n.plan)

	case *groupNode:
		if v.observer.attr != nil {
			inputCols := planColumns(n.plan)
			for i, agg := range n.funcs {
				var buf bytes.Buffer
				if agg.isIdentAggregate() {
					buf.WriteString(inputCols[agg.argRenderIdx].Name)
				} else {
					fmt.Fprintf(&buf, "%s(", agg.funcName)
					if agg.argRenderIdx != noRenderIdx {
						if agg.isDistinct() {
							buf.WriteString("DISTINCT ")
						}
						buf.WriteString(inputCols[agg.argRenderIdx].Name)
					}
					buf.WriteByte(')')
					if agg.filterRenderIdx != noRenderIdx {
						fmt.Fprintf(&buf, " FILTER (WHERE %s)", inputCols[agg.filterRenderIdx].Name)
					}
				}
				v.observer.attr(name, fmt.Sprintf("aggregate %d", i), buf.String())
			}
			if len(n.groupCols) > 0 {
				// Display the grouping columns as @1-@x if possible.
				shorthand := true
				for i, idx := range n.groupCols {
					if idx != i {
						shorthand = false
						break
					}
				}
				if shorthand && len(n.groupCols) > 1 {
					v.observer.attr(name, "group by", fmt.Sprintf("@1-@%d", len(n.groupCols)))
				} else {
					var buf bytes.Buffer
					for i, idx := range n.groupCols {
						if i > 0 {
							buf.WriteByte(',')
						}
						fmt.Fprintf(&buf, "@%d", idx+1)
					}
					v.observer.attr(name, "group by", buf.String())
				}
			}
		}

		v.visit(n.plan)

	case *windowNode:
		if v.observer.expr != nil {
			for i, agg := range n.funcs {
				v.expr(name, "window", i, agg.expr)
			}
			for i, rexpr := range n.windowRender {
				v.expr(name, "render", i, rexpr)
			}
		}
		v.visit(n.plan)

	case *unionNode:
		v.visit(n.left)
		v.visit(n.right)

	case *splitNode:
		v.visit(n.rows)

	case *testingRelocateNode:
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

		if v.observer.expr != nil {
			for i, dexpr := range n.defaultExprs {
				v.expr(name, "default", i, dexpr)
			}
			for i, cexpr := range n.checkHelper.Exprs {
				v.expr(name, "check", i, cexpr)
			}
			for i, rexpr := range n.rh.exprs {
				v.expr(name, "returning", i, rexpr)
			}
		}
		v.visit(n.run.rows)

	case *upsertNode:
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

		if v.observer.expr != nil {
			for i, dexpr := range n.defaultExprs {
				v.expr(name, "default", i, dexpr)
			}
			for i, cexpr := range n.checkHelper.Exprs {
				v.expr(name, "check", i, cexpr)
			}
			for i, rexpr := range n.rh.exprs {
				v.expr(name, "returning", i, rexpr)
			}
			n.tw.walkExprs(func(d string, i int, e tree.TypedExpr) {
				v.expr(name, d, i, e)
			})
		}
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
		if v.observer.expr != nil {
			for i, rexpr := range n.rh.exprs {
				v.expr(name, "returning", i, rexpr)
			}
			n.tw.walkExprs(func(d string, i int, e tree.TypedExpr) {
				v.expr(name, d, i, e)
			})
		}
		v.visit(n.run.rows)

	case *deleteNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "from", n.tableDesc.Name)
		}
		if v.observer.expr != nil {
			for i, rexpr := range n.rh.exprs {
				v.expr(name, "returning", i, rexpr)
			}
			n.tw.walkExprs(func(d string, i int, e tree.TypedExpr) {
				v.expr(name, d, i, e)
			})
		}
		v.visit(n.run.rows)

	case *createTableNode:
		if n.n.As() {
			v.visit(n.sourcePlan)
		}

	case *createViewNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "query", tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable))
		}

	case *setVarNode:
		if v.observer.expr != nil {
			for i, texpr := range n.typedValues {
				v.expr(name, "value", i, texpr)
			}
		}

	case *setClusterSettingNode:
		if v.observer.expr != nil && n.value != nil {
			v.expr(name, "value", -1, n.value)
		}

	case *delayedNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "source", n.name)
		}
		if n.plan != nil {
			v.visit(n.plan)
		}

	case *explainDistSQLNode:
		v.visit(n.plan)

	case *ordinalityNode:
		v.visit(n.source)

	case *spoolNode:
		if n.hardLimit > 0 && v.observer.attr != nil {
			v.observer.attr(name, "limit", fmt.Sprintf("%d", n.hardLimit))
		}
		v.visit(n.source)

	case *showTraceNode:
		v.visit(n.plan)

	case *showTraceReplicaNode:
		v.visit(n.plan)

	case *explainPlanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "expanded", strconv.FormatBool(n.expanded))
		}
		v.visit(n.plan)

	case *cancelQueryNode:
		if v.observer.expr != nil {
			v.expr(name, "queryID", -1, n.queryID)
		}

	case *cancelSessionNode:
		if v.observer.expr != nil {
			v.expr(name, "sessionID", -1, n.sessionID)
		}

	case *controlJobNode:
		if v.observer.expr != nil {
			v.expr(name, "jobID", -1, n.jobID)
		}
	}
}

// expr wraps observer.expr() and provides it with the current node's
// name.
func (v *planVisitor) expr(nodeName string, fieldName string, n int, expr tree.Expr) {
	if v.err != nil {
		return
	}
	v.observer.expr(nodeName, fieldName, n, expr)
}

// nodeName returns the name of the given planNode as string.  The
// node's current state is taken into account, e.g. sortNode has
// either name "sort" or "nosort" depending on whether sorting is
// needed.
func nodeName(plan planNode) string {
	// Some nodes have custom names depending on attributes.
	switch n := plan.(type) {
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
// strings are constant and not precomputed so that the type names can
// be changed without changing the output of "EXPLAIN".
var planNodeNames = map[reflect.Type]string{
	reflect.TypeOf(&alterIndexNode{}):           "alter index",
	reflect.TypeOf(&alterTableNode{}):           "alter table",
	reflect.TypeOf(&alterSequenceNode{}):        "alter sequence",
	reflect.TypeOf(&alterUserSetPasswordNode{}): "alter user",
	reflect.TypeOf(&cancelQueryNode{}):          "cancel query",
	reflect.TypeOf(&cancelSessionNode{}):        "cancel session",
	reflect.TypeOf(&controlJobNode{}):           "control job",
	reflect.TypeOf(&createDatabaseNode{}):       "create database",
	reflect.TypeOf(&createIndexNode{}):          "create index",
	reflect.TypeOf(&createTableNode{}):          "create table",
	reflect.TypeOf(&CreateUserNode{}):           "create user | role",
	reflect.TypeOf(&createViewNode{}):           "create view",
	reflect.TypeOf(&createSequenceNode{}):       "create sequence",
	reflect.TypeOf(&createStatsNode{}):          "create statistics",
	reflect.TypeOf(&delayedNode{}):              "virtual table",
	reflect.TypeOf(&deleteNode{}):               "delete",
	reflect.TypeOf(&distinctNode{}):             "distinct",
	reflect.TypeOf(&dropDatabaseNode{}):         "drop database",
	reflect.TypeOf(&dropIndexNode{}):            "drop index",
	reflect.TypeOf(&dropTableNode{}):            "drop table",
	reflect.TypeOf(&dropViewNode{}):             "drop view",
	reflect.TypeOf(&dropSequenceNode{}):         "drop sequence",
	reflect.TypeOf(&DropUserNode{}):             "drop user | role",
	reflect.TypeOf(&explainDistSQLNode{}):       "explain dist_sql",
	reflect.TypeOf(&explainPlanNode{}):          "explain plan",
	reflect.TypeOf(&showTraceNode{}):            "show trace for",
	reflect.TypeOf(&showTraceReplicaNode{}):     "show trace for",
	reflect.TypeOf(&filterNode{}):               "filter",
	reflect.TypeOf(&groupNode{}):                "group",
	reflect.TypeOf(&unaryNode{}):                "emptyrow",
	reflect.TypeOf(&hookFnNode{}):               "plugin",
	reflect.TypeOf(&indexJoinNode{}):            "index-join",
	reflect.TypeOf(&insertNode{}):               "insert",
	reflect.TypeOf(&joinNode{}):                 "join",
	reflect.TypeOf(&limitNode{}):                "limit",
	reflect.TypeOf(&ordinalityNode{}):           "ordinality",
	reflect.TypeOf(&testingRelocateNode{}):      "testingRelocate",
	reflect.TypeOf(&renderNode{}):               "render",
	reflect.TypeOf(&scanNode{}):                 "scan",
	reflect.TypeOf(&scatterNode{}):              "scatter",
	reflect.TypeOf(&scrubNode{}):                "scrub",
	reflect.TypeOf(&sequenceSelectNode{}):       "sequence select",
	reflect.TypeOf(&setVarNode{}):               "set",
	reflect.TypeOf(&setClusterSettingNode{}):    "set cluster setting",
	reflect.TypeOf(&setZoneConfigNode{}):        "configure zone",
	reflect.TypeOf(&showZoneConfigNode{}):       "show zone configuration",
	reflect.TypeOf(&showRangesNode{}):           "showRanges",
	reflect.TypeOf(&showFingerprintsNode{}):     "showFingerprints",
	reflect.TypeOf(&sortNode{}):                 "sort",
	reflect.TypeOf(&splitNode{}):                "split",
	reflect.TypeOf(&spoolNode{}):                "spool",
	reflect.TypeOf(&unionNode{}):                "union",
	reflect.TypeOf(&updateNode{}):               "update",
	reflect.TypeOf(&upsertNode{}):               "upsert",
	reflect.TypeOf(&valueGenerator{}):           "generator",
	reflect.TypeOf(&valuesNode{}):               "values",
	reflect.TypeOf(&windowNode{}):               "window",
	reflect.TypeOf(&zeroNode{}):                 "norows",
}
