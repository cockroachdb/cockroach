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

type observeVerbosity int

const (
	observeMetadata observeVerbosity = iota
	observeAlways
)

// planObserver is the interface to implement by components that need
// to visit a planNode tree.
// Used mainly by EXPLAIN, but also for the collector of back-references
// for view definitions.
type planObserver struct {
	// replaceNode is invoked upon entering a tree node. It can replace the
	// current planNode in the tree by returning a non-nil planNode. Returning
	// nil will continue the recursion and not modify the current node.
	replaceNode func(ctx context.Context, nodeName string, plan planNode) (planNode, error)

	// enterNode is invoked upon entering a tree node. It can return false to
	// stop the recursion at this node.
	enterNode func(ctx context.Context, nodeName string, plan planNode) (bool, error)

	// expr is invoked for each expression field in each node.
	expr func(verbosity observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr)

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
func (v *planVisitor) visit(plan planNode) planNode {
	if v.err != nil {
		return plan
	}

	name := nodeName(plan)

	if v.observer.replaceNode != nil {
		newNode, err := v.observer.replaceNode(v.ctx, name, plan)
		if err != nil {
			v.err = err
			return plan
		}
		if newNode != nil {
			return newNode
		}
	}
	v.visitInternal(plan, name)
	return plan
}

// visitConcrete is like visit, but provided for the case where a planNode is
// trying to recurse into a concrete planNode type, and not a planNode
// interface.
func (v *planVisitor) visitConcrete(plan planNode) {
	if v.err != nil {
		return
	}

	name := nodeName(plan)
	v.visitInternal(plan, name)
}

func (v *planVisitor) visitInternal(plan planNode, name string) {
	if v.err != nil {
		return
	}
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
					v.metadataExpr(name, fieldName, j, expr)
				}
			}
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
		n.source.plan = v.visit(n.source.plan)

	case *renderNode:
		if v.observer.expr != nil {
			for i, r := range n.render {
				v.metadataExpr(name, "render", i, r)
			}
		}
		n.source.plan = v.visit(n.source.plan)

	case *indexJoinNode:
		v.visitConcrete(n.index)
		v.visitConcrete(n.table)

	case *lookupJoinNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "type", joinTypeStr(n.joinType))
		}
		if v.observer.expr != nil && n.onCond != nil && n.onCond != tree.DBoolTrue {
			v.expr(name, "pred", -1, n.onCond)
		}
		n.input = v.visit(n.input)
		v.visitConcrete(n.table)

	case *joinNode:
		if v.observer.attr != nil {
			jType := joinTypeStr(n.joinType)
			if n.joinType == sqlbase.InnerJoin && len(n.pred.leftColNames) == 0 && n.pred.onCond == nil {
				jType = "cross"
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
				if n.pred.leftEqKey {
					v.observer.attr(name, "left cols are key", "")
				}
				if n.pred.rightEqKey {
					v.observer.attr(name, "right cols are key", "")
				}
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
		n.left.plan = v.visit(n.left.plan)
		n.right.plan = v.visit(n.right.plan)

	case *limitNode:
		if v.observer.expr != nil {
			v.expr(name, "count", -1, n.countExpr)
			v.expr(name, "offset", -1, n.offsetExpr)
		}
		n.plan = v.visit(n.plan)

	case *distinctNode:
		if v.observer.attr == nil {
			n.plan = v.visit(n.plan)
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

		if !n.columnsInOrder.Empty() {
			var buf bytes.Buffer
			prefix := ""
			columns := planColumns(n.plan)
			for i, ok := n.columnsInOrder.Next(0); ok; i, ok = n.columnsInOrder.Next(i + 1) {
				buf.WriteString(prefix)
				buf.WriteString(columns[i].Name)
				prefix = ", "
			}
			v.observer.attr(name, "order key", buf.String())
		}

		n.plan = v.visit(n.plan)

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
		n.plan = v.visit(n.plan)

	case *groupNode:
		if v.observer.attr != nil {
			inputCols := planColumns(n.plan)
			for i, agg := range n.funcs {
				var buf bytes.Buffer
				if groupingCol, ok := n.aggIsGroupingColumn(i); ok {
					buf.WriteString(inputCols[groupingCol].Name)
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
				v.observer.attr(name, "group by", colListStr(n.groupCols))
			}
			if len(n.orderedGroupCols) > 0 {
				v.observer.attr(name, "ordered", colListStr(n.orderedGroupCols))
			}
			if n.isScalar {
				v.observer.attr(name, "scalar", "")
			}
		}

		n.plan = v.visit(n.plan)

	case *windowNode:
		if v.observer.expr != nil {
			for i, agg := range n.funcs {
				v.metadataExpr(name, "window", i, agg.expr)
			}
			for i, rexpr := range n.windowRender {
				v.metadataExpr(name, "render", i, rexpr)
			}
		}
		n.plan = v.visit(n.plan)

	case *unionNode:
		n.left = v.visit(n.left)
		n.right = v.visit(n.right)

	case *splitNode:
		n.rows = v.visit(n.rows)

	case *relocateNode:
		n.rows = v.visit(n.rows)

	case *insertNode:
		if v.observer.attr != nil {
			var buf bytes.Buffer
			buf.WriteString(n.run.ti.tableDesc().Name)
			buf.WriteByte('(')
			for i, col := range n.run.insertCols {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.Name)
			}
			buf.WriteByte(')')
			v.observer.attr(name, "into", buf.String())
		}

		if v.observer.expr != nil {
			for i, dexpr := range n.run.defaultExprs {
				v.metadataExpr(name, "default", i, dexpr)
			}
			for i, cexpr := range n.run.checkHelper.Exprs {
				v.metadataExpr(name, "check", i, cexpr)
			}
		}
		n.source = v.visit(n.source)

	case *upsertNode:
		if v.observer.attr != nil {
			var buf bytes.Buffer
			buf.WriteString(n.run.tw.tableDesc().Name)
			buf.WriteByte('(')
			for i, col := range n.run.insertCols {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(col.Name)
			}
			buf.WriteByte(')')
			v.observer.attr(name, "into", buf.String())
		}

		if v.observer.expr != nil {
			for i, dexpr := range n.run.defaultExprs {
				v.metadataExpr(name, "default", i, dexpr)
			}
			for i, cexpr := range n.run.checkHelper.Exprs {
				v.metadataExpr(name, "check", i, cexpr)
			}
			n.run.tw.walkExprs(func(d string, i int, e tree.TypedExpr) {
				v.metadataExpr(name, d, i, e)
			})
		}
		n.source = v.visit(n.source)

	case *updateNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "table", n.run.tu.tableDesc().Name)
			if len(n.run.tu.ru.UpdateCols) > 0 {
				var buf bytes.Buffer
				for i, col := range n.run.tu.ru.UpdateCols {
					if i > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(col.Name)
				}
				v.observer.attr(name, "set", buf.String())
			}
		}
		if v.observer.expr != nil {
			for i, cexpr := range n.run.computeExprs {
				v.metadataExpr(name, "computed", i, cexpr)
			}
			for i, cexpr := range n.run.checkHelper.Exprs {
				v.metadataExpr(name, "check", i, cexpr)
			}
		}
		// An updater has no sub-expressions, so nothing special to do here.
		n.source = v.visit(n.source)

	case *deleteNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "from", n.run.td.tableDesc().Name)
		}
		// A deleter has no sub-expressions, so nothing special to do here.
		n.source = v.visit(n.source)

	case *serializeNode:
		v.visitConcrete(n.source)

	case *rowCountNode:
		v.visitConcrete(n.source)

	case *createTableNode:
		if n.n.As() {
			n.sourcePlan = v.visit(n.sourcePlan)
		}

	case *createViewNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "query", tree.AsStringWithFlags(n.n.AsSource, tree.FmtParsable))
		}

	case *setVarNode:
		if v.observer.expr != nil {
			for i, texpr := range n.typedValues {
				v.metadataExpr(name, "value", i, texpr)
			}
		}

	case *setClusterSettingNode:
		if v.observer.expr != nil && n.value != nil {
			v.metadataExpr(name, "value", -1, n.value)
		}

	case *delayedNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "source", n.name)
		}
		if n.plan != nil {
			n.plan = v.visit(n.plan)
		}

	case *explainDistSQLNode:
		n.plan = v.visit(n.plan)

	case *ordinalityNode:
		n.source = v.visit(n.source)

	case *spoolNode:
		if n.hardLimit > 0 && v.observer.attr != nil {
			v.observer.attr(name, "limit", fmt.Sprintf("%d", n.hardLimit))
		}
		n.source = v.visit(n.source)

	case *showTraceReplicaNode:
		n.plan = v.visit(n.plan)

	case *explainPlanNode:
		if v.observer.attr != nil {
			v.observer.attr(name, "expanded", strconv.FormatBool(n.expanded))
		}
		n.plan = v.visit(n.plan)

	case *cancelQueriesNode:
		n.rows = v.visit(n.rows)

	case *cancelSessionsNode:
		n.rows = v.visit(n.rows)

	case *controlJobsNode:
		n.rows = v.visit(n.rows)

	case *setZoneConfigNode:
		if v.observer.expr != nil {
			v.metadataExpr(name, "yaml", -1, n.yamlConfig)
		}

	case *projectSetNode:
		if v.observer.expr != nil {
			for i, texpr := range n.exprs {
				v.metadataExpr(name, "render", i, texpr)
			}
		}
		n.source = v.visit(n.source)
	}
}

// expr wraps observer.expr() and provides it with the current node's
// name.
func (v *planVisitor) expr(nodeName string, fieldName string, n int, expr tree.Expr) {
	if v.err != nil {
		return
	}
	v.observer.expr(observeAlways, nodeName, fieldName, n, expr)
}

// metadata wraps observer.expr() and provides it with the current node's
// name, with verbosity = metadata.
func (v *planVisitor) metadataExpr(nodeName string, fieldName string, n int, expr tree.Expr) {
	if v.err != nil {
		return
	}
	v.observer.expr(observeMetadata, nodeName, fieldName, n, expr)
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

func joinTypeStr(t sqlbase.JoinType) string {
	switch t {
	case sqlbase.InnerJoin:
		return "inner"
	case sqlbase.LeftOuterJoin:
		return "left outer"
	case sqlbase.RightOuterJoin:
		return "right outer"
	case sqlbase.FullOuterJoin:
		return "full outer"
	case sqlbase.LeftSemiJoin:
		return "semi"
	case sqlbase.LeftAntiJoin:
		return "anti"
	}
	panic(fmt.Sprintf("unknown join type %s", t))
}

func colListStr(cols []int) string {
	// Display the columns as @1-@x if possible.
	shorthand := true
	for i, idx := range cols {
		if idx != i {
			shorthand = false
			break
		}
	}
	if shorthand && len(cols) > 1 {
		return fmt.Sprintf("@1-@%d", len(cols))
	}

	var buf bytes.Buffer
	for i, idx := range cols {
		if i > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, "@%d", idx+1)
	}
	return buf.String()
}

// planNodeNames is the mapping from node type to strings.  The
// strings are constant and not precomputed so that the type names can
// be changed without changing the output of "EXPLAIN".
var planNodeNames = map[reflect.Type]string{
	reflect.TypeOf(&alterIndexNode{}):           "alter index",
	reflect.TypeOf(&alterSequenceNode{}):        "alter sequence",
	reflect.TypeOf(&alterTableNode{}):           "alter table",
	reflect.TypeOf(&alterUserSetPasswordNode{}): "alter user",
	reflect.TypeOf(&commentOnTableNode{}):       "comment on table",
	reflect.TypeOf(&cancelQueriesNode{}):        "cancel queries",
	reflect.TypeOf(&cancelSessionsNode{}):       "cancel sessions",
	reflect.TypeOf(&controlJobsNode{}):          "control jobs",
	reflect.TypeOf(&createDatabaseNode{}):       "create database",
	reflect.TypeOf(&createIndexNode{}):          "create index",
	reflect.TypeOf(&createSequenceNode{}):       "create sequence",
	reflect.TypeOf(&createStatsNode{}):          "create statistics",
	reflect.TypeOf(&createTableNode{}):          "create table",
	reflect.TypeOf(&CreateUserNode{}):           "create user/role",
	reflect.TypeOf(&createViewNode{}):           "create view",
	reflect.TypeOf(&delayedNode{}):              "virtual table",
	reflect.TypeOf(&deleteNode{}):               "delete",
	reflect.TypeOf(&distinctNode{}):             "distinct",
	reflect.TypeOf(&dropDatabaseNode{}):         "drop database",
	reflect.TypeOf(&dropIndexNode{}):            "drop index",
	reflect.TypeOf(&dropSequenceNode{}):         "drop sequence",
	reflect.TypeOf(&dropTableNode{}):            "drop table",
	reflect.TypeOf(&DropUserNode{}):             "drop user/role",
	reflect.TypeOf(&dropViewNode{}):             "drop view",
	reflect.TypeOf(&explainDistSQLNode{}):       "explain distsql",
	reflect.TypeOf(&explainPlanNode{}):          "explain plan",
	reflect.TypeOf(&filterNode{}):               "filter",
	reflect.TypeOf(&groupNode{}):                "group",
	reflect.TypeOf(&hookFnNode{}):               "plugin",
	reflect.TypeOf(&indexJoinNode{}):            "index-join",
	reflect.TypeOf(&insertNode{}):               "insert",
	reflect.TypeOf(&joinNode{}):                 "join",
	reflect.TypeOf(&limitNode{}):                "limit",
	reflect.TypeOf(&lookupJoinNode{}):           "lookup-join",
	reflect.TypeOf(&ordinalityNode{}):           "ordinality",
	reflect.TypeOf(&projectSetNode{}):           "project set",
	reflect.TypeOf(&relocateNode{}):             "relocate",
	reflect.TypeOf(&renameColumnNode{}):         "rename column",
	reflect.TypeOf(&renameDatabaseNode{}):       "rename database",
	reflect.TypeOf(&renameIndexNode{}):          "rename index",
	reflect.TypeOf(&renameTableNode{}):          "rename table",
	reflect.TypeOf(&renderNode{}):               "render",
	reflect.TypeOf(&rowCountNode{}):             "count",
	reflect.TypeOf(&rowSourceToPlanNode{}):      "row source to plan node",
	reflect.TypeOf(&scanNode{}):                 "scan",
	reflect.TypeOf(&scatterNode{}):              "scatter",
	reflect.TypeOf(&scrubNode{}):                "scrub",
	reflect.TypeOf(&sequenceSelectNode{}):       "sequence select",
	reflect.TypeOf(&serializeNode{}):            "run",
	reflect.TypeOf(&setClusterSettingNode{}):    "set cluster setting",
	reflect.TypeOf(&setVarNode{}):               "set",
	reflect.TypeOf(&setZoneConfigNode{}):        "configure zone",
	reflect.TypeOf(&showFingerprintsNode{}):     "showFingerprints",
	reflect.TypeOf(&showRangesNode{}):           "showRanges",
	reflect.TypeOf(&showTraceNode{}):            "show trace for",
	reflect.TypeOf(&showTraceReplicaNode{}):     "replica trace",
	reflect.TypeOf(&showZoneConfigNode{}):       "show zone configuration",
	reflect.TypeOf(&sortNode{}):                 "sort",
	reflect.TypeOf(&splitNode{}):                "split",
	reflect.TypeOf(&spoolNode{}):                "spool",
	reflect.TypeOf(&truncateNode{}):             "truncate",
	reflect.TypeOf(&unaryNode{}):                "emptyrow",
	reflect.TypeOf(&unionNode{}):                "union",
	reflect.TypeOf(&updateNode{}):               "update",
	reflect.TypeOf(&upsertNode{}):               "upsert",
	reflect.TypeOf(&valuesNode{}):               "values",
	reflect.TypeOf(&virtualTableNode{}):         "virtual table values",
	reflect.TypeOf(&windowNode{}):               "window",
	reflect.TypeOf(&zeroNode{}):                 "norows",
}
