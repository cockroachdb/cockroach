// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package explain

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

// Emit produces the EXPLAIN output against the given OutputBuilder. The
// OutputBuilder flags are taken into account.
func Emit(plan *Plan, ob *OutputBuilder, spanFormatFn SpanFormatFn) error {
	e := makeEmitter(ob, spanFormatFn)
	var walk func(n *Node) error
	walk = func(n *Node) error {
		// In non-verbose mode, we skip all projections.
		// In verbose mode, we only skip trivial projections (which just rearrange
		// or rename the columns).
		if !ob.flags.Verbose {
			if n.op == serializingProjectOp || n.op == simpleProjectOp {
				return walk(n.children[0])
			}
		}
		n, columns, ordering := omitTrivialProjections(n)
		name, err := e.nodeName(n)
		if err != nil {
			return err
		}
		ob.EnterNode(name, columns, ordering)
		if err := e.emitNodeAttributes(n); err != nil {
			return err
		}
		for _, c := range n.children {
			if err := walk(c); err != nil {
				return err
			}
		}
		ob.LeaveNode()
		return nil
	}

	if len(plan.Subqueries) == 0 && len(plan.Cascades) == 0 && len(plan.Checks) == 0 {
		return walk(plan.Root)
	}
	ob.EnterNode("root", plan.Root.Columns(), plan.Root.Ordering())
	if err := walk(plan.Root); err != nil {
		return err
	}
	for i, s := range plan.Subqueries {
		ob.EnterMetaNode("subquery")
		ob.Attr("id", fmt.Sprintf("@S%d", i+1))

		// This field contains the original subquery (which could have been modified
		// by optimizer transformations).
		if s.ExprNode != nil {
			ob.Attr("original sql", tree.AsStringWithFlags(s.ExprNode, tree.FmtSimple))
		}
		var mode string
		switch s.Mode {
		case exec.SubqueryExists:
			mode = "exists"
		case exec.SubqueryOneRow:
			mode = "one row"
		case exec.SubqueryAnyRows:
			mode = "any rows"
		case exec.SubqueryAllRows:
			mode = "all rows"
		default:
			return errors.Errorf("invalid SubqueryMode %d", s.Mode)
		}

		ob.Attr("exec mode", mode)
		if err := walk(s.Root.(*Node)); err != nil {
			return err
		}
		ob.LeaveNode()
	}

	for i := range plan.Cascades {
		ob.EnterMetaNode("fk-cascade")
		ob.Attr("fk", plan.Cascades[i].FKName)
		if buffer := plan.Cascades[i].Buffer; buffer != nil {
			ob.Attr("input", buffer.(*Node).args.(*bufferArgs).Label)
		}
		ob.LeaveNode()
	}
	for _, n := range plan.Checks {
		ob.EnterMetaNode("constraint-check")
		if err := walk(n); err != nil {
			return err
		}
		ob.LeaveNode()
	}
	ob.LeaveNode()
	return nil
}

// SpanFormatFn is a function used to format spans for EXPLAIN. Only called on
// non-virtual tables, when there is an index constraint or an inverted
// constraint.
type SpanFormatFn func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string

// omitTrivialProjections returns the given node and its result columns and
// ordering, unless the node is an identity projection (which just renames
// columns) - in which case we return the child node and the renamed columns.
func omitTrivialProjections(n *Node) (*Node, colinfo.ResultColumns, colinfo.ColumnOrdering) {
	var projection []exec.NodeColumnOrdinal
	switch n.op {
	case serializingProjectOp:
		projection = n.args.(*serializingProjectArgs).Cols
	case simpleProjectOp:
		projection = n.args.(*simpleProjectArgs).Cols
	default:
		return n, n.Columns(), n.Ordering()
	}

	input, inputColumns, inputOrdering := omitTrivialProjections(n.children[0])

	// Check if the projection is a bijection (i.e. permutation of all input
	// columnns), and construct the inverse projection.
	if len(projection) != len(inputColumns) {
		return n, n.Columns(), n.Ordering()
	}
	inverse := make([]int, len(inputColumns))
	for i := range inverse {
		inverse[i] = -1
	}
	for i, col := range projection {
		inverse[int(col)] = i
	}
	for i := range inverse {
		if inverse[i] == -1 {
			return n, n.Columns(), n.Ordering()
		}
	}
	// We will show the child node and its ordering, but with the columns
	// reordered and renamed according to the parent.
	ordering := make(colinfo.ColumnOrdering, len(inputOrdering))
	for i, o := range inputOrdering {
		ordering[i].ColIdx = inverse[o.ColIdx]
		ordering[i].Direction = o.Direction
	}
	return input, n.Columns(), ordering
}

// emitter is a helper for emitting explain information for all the operators.
type emitter struct {
	ob           *OutputBuilder
	spanFormatFn SpanFormatFn
}

func makeEmitter(ob *OutputBuilder, spanFormatFn SpanFormatFn) emitter {
	return emitter{ob: ob, spanFormatFn: spanFormatFn}
}

func (e *emitter) nodeName(n *Node) (string, error) {
	switch n.op {
	case scanOp:
		a := n.args.(*scanArgs)
		if a.Table.IsVirtualTable() {
			return "virtual table", nil
		}
		if a.Params.Reverse {
			return "revscan", nil
		}
		return "scan", nil

	case valuesOp:
		a := n.args.(*valuesArgs)
		switch {
		case len(a.Rows) == 0:
			return "norows", nil
		case len(a.Rows) == 1 && len(a.Columns) == 0:
			return "emptyrow", nil
		default:
			return "values", nil
		}

	case hashJoinOp:
		a := n.args.(*hashJoinArgs)
		if len(n.args.(*hashJoinArgs).LeftEqCols) == 0 {
			return e.joinNodeName("cross", a.JoinType), nil
		}
		return e.joinNodeName("hash", a.JoinType), nil

	case mergeJoinOp:
		a := n.args.(*mergeJoinArgs)
		return e.joinNodeName("merge", a.JoinType), nil

	case lookupJoinOp:
		a := n.args.(*lookupJoinArgs)
		if a.Table.IsVirtualTable() {
			return e.joinNodeName("virtual table lookup", a.JoinType), nil
		}
		return e.joinNodeName("lookup", a.JoinType), nil

	case invertedJoinOp:
		a := n.args.(*invertedJoinArgs)
		return e.joinNodeName("inverted", a.JoinType), nil

	case applyJoinOp:
		a := n.args.(*applyJoinArgs)
		return e.joinNodeName("apply", a.JoinType), nil

	case hashSetOpOp:
		a := n.args.(*hashSetOpArgs)
		name := strings.ToLower(a.Typ.String())
		if a.All {
			name += " all"
		}
		return name, nil

	case streamingSetOpOp:
		a := n.args.(*streamingSetOpArgs)
		name := strings.ToLower(a.Typ.String())
		if a.All {
			name += " all"
		}
		return name, nil

	case opaqueOp:
		a := n.args.(*opaqueArgs)
		return strings.ToLower(a.Metadata.String()), nil
	}

	if n.op < 0 || int(n.op) >= len(nodeNames) || nodeNames[n.op] == "" {
		return "", errors.AssertionFailedf("unhandled op %d", n.op)
	}
	return nodeNames[n.op], nil
}

var nodeNames = [...]string{
	alterTableRelocateOp:   "relocate",
	alterTableSplitOp:      "split",
	alterTableUnsplitAllOp: "unsplit all",
	alterTableUnsplitOp:    "unsplit",
	applyJoinOp:            "", // This node does not have a fixed name.
	bufferOp:               "buffer",
	cancelQueriesOp:        "cancel queries",
	cancelSessionsOp:       "cancel sessions",
	controlJobsOp:          "control jobs",
	controlSchedulesOp:     "control schedules",
	createStatisticsOp:     "create statistics",
	createTableOp:          "create table",
	createTableAsOp:        "create table as",
	createViewOp:           "create view",
	deleteOp:               "delete",
	deleteRangeOp:          "delete range",
	distinctOp:             "distinct",
	errorIfRowsOp:          "error if rows",
	explainOp:              "explain",
	explainOptOp:           "explain",
	exportOp:               "export",
	filterOp:               "filter",
	groupByOp:              "group",
	hashJoinOp:             "", // This node does not have a fixed name.
	indexJoinOp:            "index join",
	insertFastPathOp:       "insert fast path",
	insertOp:               "insert",
	invertedFilterOp:       "inverted filter",
	invertedJoinOp:         "inverted join",
	limitOp:                "limit",
	lookupJoinOp:           "", // This node does not have a fixed name.
	max1RowOp:              "max1row",
	mergeJoinOp:            "", // This node does not have a fixed name.
	opaqueOp:               "", // This node does not have a fixed name.
	ordinalityOp:           "ordinality",
	projectSetOp:           "project set",
	recursiveCTEOp:         "recursive cte",
	renderOp:               "render",
	saveTableOp:            "save table",
	scalarGroupByOp:        "group (scalar)",
	scanBufferOp:           "scan buffer",
	scanOp:                 "", // This node does not have a fixed name.
	sequenceSelectOp:       "sequence select",
	hashSetOpOp:            "", // This node does not have a fixed name.
	streamingSetOpOp:       "", // This node does not have a fixed name.
	unionAllOp:             "union all",
	showTraceOp:            "show trace",
	simpleProjectOp:        "project",
	serializingProjectOp:   "project",
	sortOp:                 "sort",
	updateOp:               "update",
	upsertOp:               "upsert",
	valuesOp:               "", // This node does not have a fixed name.
	windowOp:               "window",
	zigzagJoinOp:           "zigzag join",
}

func (e *emitter) joinNodeName(algo string, joinType descpb.JoinType) string {
	var typ string
	switch joinType {
	case descpb.InnerJoin:
		// Omit "inner" in non-verbose mode.
		if !e.ob.flags.Verbose {
			return fmt.Sprintf("%s join", algo)
		}
		typ = "inner"

	case descpb.LeftOuterJoin:
		typ = "left outer"
	case descpb.RightOuterJoin:
		typ = "right outer"
	case descpb.FullOuterJoin:
		typ = "full outer"
	case descpb.LeftSemiJoin:
		typ = "semi"
	case descpb.LeftAntiJoin:
		typ = "anti"
	case descpb.RightSemiJoin:
		typ = "right semi"
	case descpb.RightAntiJoin:
		typ = "right anti"
	default:
		typ = fmt.Sprintf("invalid: %d", joinType)
	}
	return fmt.Sprintf("%s join (%s)", algo, typ)
}

func (e *emitter) emitNodeAttributes(n *Node) error {
	if stats, ok := n.annotations[exec.ExecutionStatsID]; ok {
		s := stats.(*exec.ExecutionStats)
		if len(s.Nodes) > 0 {
			e.ob.AddRedactableField(RedactNodes, "nodes", strings.Join(s.Nodes, ", "))
		}
		if len(s.Regions) > 0 {
			e.ob.AddRedactableField(RedactNodes, "regions", strings.Join(s.Regions, ", "))
		}
		if s.RowCount.HasValue() {
			e.ob.AddField("actual row count", humanizeutil.Count(s.RowCount.Value()))
		}
		// Omit vectorized batches in non-verbose mode.
		if e.ob.flags.Verbose {
			if s.VectorizedBatchCount.HasValue() {
				e.ob.AddField("vectorized batch count", humanizeutil.Count(s.VectorizedBatchCount.Value()))
			}
		}
		if s.KVTime.HasValue() {
			e.ob.AddField("KV time", humanizeutil.Duration(s.KVTime.Value()))
		}
		if s.KVContentionTime.HasValue() {
			e.ob.AddField("KV contention time", humanizeutil.Duration(s.KVContentionTime.Value()))
		}
		if s.KVRowsRead.HasValue() {
			e.ob.AddField("KV rows read", humanizeutil.Count(s.KVRowsRead.Value()))
		}
		if s.KVBytesRead.HasValue() {
			e.ob.AddField("KV bytes read", humanize.IBytes(s.KVBytesRead.Value()))
		}
	}

	if stats, ok := n.annotations[exec.EstimatedStatsID]; ok {
		s := stats.(*exec.EstimatedStats)

		// Show the estimated row count (except Values, where it is redundant).
		if n.op != valuesOp {
			count := uint64(math.Round(s.RowCount))
			if s.TableStatsAvailable {
				if n.op == scanOp && s.TableStatsRowCount != 0 {
					percentage := s.RowCount / float64(s.TableStatsRowCount) * 100
					// We want to print the percentage in a user-friendly way; we include
					// decimals depending on how small the value is.
					var percentageStr string
					switch {
					case percentage >= 10.0:
						percentageStr = fmt.Sprintf("%.0f", percentage)
					case percentage >= 1.0:
						percentageStr = fmt.Sprintf("%.1f", percentage)
					case percentage >= 0.01:
						percentageStr = fmt.Sprintf("%.2f", percentage)
					default:
						percentageStr = "<0.01"
					}

					var duration string
					if e.ob.flags.Redact.Has(RedactVolatile) {
						duration = "<hidden>"
					} else {
						timeSinceStats := timeutil.Since(s.TableStatsCreatedAt)
						if timeSinceStats < 0 {
							timeSinceStats = 0
						}
						duration = humanizeutil.LongDuration(timeSinceStats)
					}
					e.ob.AddField("estimated row count", fmt.Sprintf(
						"%s (%s%% of the table; stats collected %s ago)",
						humanizeutil.Count(count), percentageStr,
						duration,
					))
				} else {
					e.ob.AddField("estimated row count", humanizeutil.Count(count))
				}
			} else {
				// No stats available.
				if e.ob.flags.Verbose {
					e.ob.Attrf("estimated row count", "%s (missing stats)", humanizeutil.Count(count))
				} else if n.op == scanOp {
					// In non-verbose mode, don't show the row count (which is not based
					// on reality); only show a "missing stats" field for scans. Don't
					// show it for virtual tables though, where we expect no stats.
					if !n.args.(*scanArgs).Table.IsVirtualTable() {
						e.ob.AddField("missing stats", "")
					}
				}
			}
		}
		// TODO(radu): we may want to emit estimated cost in Verbose mode.
	}

	ob := e.ob
	switch n.op {
	case scanOp:
		a := n.args.(*scanArgs)
		e.emitTableAndIndex("table", a.Table, a.Index)
		// Omit spans for virtual tables, unless we actually have a constraint.
		if !(a.Table.IsVirtualTable() && a.Params.IndexConstraint == nil) {
			e.emitSpans("spans", a.Table, a.Index, a.Params)
		}

		if a.Params.HardLimit > 0 {
			ob.Attr("limit", a.Params.HardLimit)
		}

		if a.Params.Parallelize {
			ob.VAttr("parallel", "")
		}
		e.emitLockingPolicy(a.Params.Locking)

	case valuesOp:
		a := n.args.(*valuesArgs)
		// Don't emit anything for the "norows" and "emptyrow" cases.
		if len(a.Rows) > 0 && (len(a.Rows) > 1 || len(a.Columns) > 0) {
			e.emitTuples(a.Rows, len(a.Columns))
		}

	case filterOp:
		ob.Expr("filter", n.args.(*filterArgs).Filter, n.Columns())

	case renderOp:
		if ob.flags.Verbose {
			a := n.args.(*renderArgs)
			for i := range a.Exprs {
				ob.Expr(fmt.Sprintf("render %s", a.Columns[i].Name), a.Exprs[i], a.Input.Columns())
			}
		}

	case limitOp:
		a := n.args.(*limitArgs)
		if a.Limit != nil {
			ob.Expr("count", a.Limit, nil /* columns */)
		}
		if a.Offset != nil {
			ob.Expr("offset", a.Offset, nil /* columns */)
		}

	case sortOp:
		a := n.args.(*sortArgs)
		ob.Attr("order", colinfo.ColumnOrdering(a.Ordering).String(n.Columns()))
		if p := a.AlreadyOrderedPrefix; p > 0 {
			ob.Attr("already ordered", colinfo.ColumnOrdering(a.Ordering[:p]).String(n.Columns()))
		}

	case unionAllOp:
		a := n.args.(*unionAllArgs)
		if a.HardLimit > 0 {
			ob.Attr("limit", a.HardLimit)
		}

	case indexJoinOp:
		a := n.args.(*indexJoinArgs)
		ob.Attrf("table", "%s@%s", a.Table.Name(), a.Table.Index(0).Name())
		cols := make([]string, len(a.KeyCols))
		inputCols := a.Input.Columns()
		for i, c := range a.KeyCols {
			cols[i] = inputCols[c].Name
		}
		ob.VAttr("key columns", strings.Join(cols, ", "))

	case groupByOp:
		a := n.args.(*groupByArgs)
		e.emitGroupByAttributes(
			a.Input.Columns(),
			a.Aggregations, a.GroupCols, a.GroupColOrdering, false, /* isScalar */
		)

	case scalarGroupByOp:
		a := n.args.(*scalarGroupByArgs)
		e.emitGroupByAttributes(
			a.Input.Columns(),
			a.Aggregations, nil /* groupCols */, nil /* groupColOrdering */, true, /* isScalar */
		)

	case distinctOp:
		a := n.args.(*distinctArgs)
		inputCols := a.Input.Columns()
		ob.Attr("distinct on", printColumnSet(inputCols, a.DistinctCols))
		if a.NullsAreDistinct {
			ob.Attr("nulls are distinct", "")
		}
		if a.ErrorOnDup != "" {
			ob.Attr("error on duplicate", "")
		}
		if !a.OrderedCols.Empty() {
			ob.Attr("order key", printColumnSet(inputCols, a.OrderedCols))
		}

	case hashJoinOp:
		a := n.args.(*hashJoinArgs)
		e.emitJoinAttributes(
			a.Left.Columns(), a.Right.Columns(),
			a.LeftEqCols, a.RightEqCols,
			a.LeftEqColsAreKey, a.RightEqColsAreKey,
			a.ExtraOnCond,
		)

	case mergeJoinOp:
		a := n.args.(*mergeJoinArgs)
		leftCols := a.Left.Columns()
		rightCols := a.Right.Columns()
		leftEqCols := make([]exec.NodeColumnOrdinal, len(a.LeftOrdering))
		rightEqCols := make([]exec.NodeColumnOrdinal, len(a.LeftOrdering))
		for i := range leftEqCols {
			leftEqCols[i] = exec.NodeColumnOrdinal(a.LeftOrdering[i].ColIdx)
			rightEqCols[i] = exec.NodeColumnOrdinal(a.RightOrdering[i].ColIdx)
		}
		e.emitJoinAttributes(
			leftCols, rightCols,
			leftEqCols, rightEqCols,
			a.LeftEqColsAreKey, a.RightEqColsAreKey,
			a.OnCond,
		)
		eqCols := make(colinfo.ResultColumns, len(leftEqCols))
		mergeOrd := make(colinfo.ColumnOrdering, len(eqCols))
		for i := range eqCols {
			eqCols[i].Name = fmt.Sprintf(
				"(%s=%s)", leftCols[leftEqCols[i]].Name, rightCols[rightEqCols[i]].Name,
			)
			mergeOrd[i].ColIdx = i
			mergeOrd[i].Direction = a.LeftOrdering[i].Direction
		}
		ob.VAttr("merge ordering", mergeOrd.String(eqCols))

	case applyJoinOp:
		a := n.args.(*applyJoinArgs)
		if a.OnCond != nil {
			ob.Expr("pred", a.OnCond, appendColumns(a.Left.Columns(), a.RightColumns...))
		}

	case lookupJoinOp:
		a := n.args.(*lookupJoinArgs)
		e.emitTableAndIndex("table", a.Table, a.Index)
		inputCols := a.Input.Columns()
		if len(a.EqCols) > 0 {
			rightEqCols := make([]string, len(a.EqCols))
			for i := range rightEqCols {
				rightEqCols[i] = string(a.Index.Column(i).ColName())
			}
			ob.Attrf(
				"equality", "(%s) = (%s)",
				printColumnList(inputCols, a.EqCols),
				strings.Join(rightEqCols, ","),
			)
		}
		if a.EqColsAreKey {
			ob.Attr("equality cols are key", "")
		}
		ob.Expr("lookup condition", a.LookupExpr, appendColumns(inputCols, tableColumns(a.Table, a.LookupCols)...))
		ob.Expr("remote lookup condition", a.RemoteLookupExpr, appendColumns(inputCols, tableColumns(a.Table, a.LookupCols)...))
		ob.Expr("pred", a.OnCond, appendColumns(inputCols, tableColumns(a.Table, a.LookupCols)...))
		e.emitLockingPolicy(a.Locking)

	case zigzagJoinOp:
		a := n.args.(*zigzagJoinArgs)
		leftCols := tableColumns(a.LeftTable, a.LeftCols)
		rightCols := tableColumns(a.RightTable, a.RightCols)
		// TODO(radu): we should be passing nil instead of true.
		if a.OnCond != tree.DBoolTrue {
			ob.Expr("pred", a.OnCond, appendColumns(leftCols, rightCols...))
		}
		e.emitTableAndIndex("left table", a.LeftTable, a.LeftIndex)
		ob.Attrf("left columns", "(%s)", printColumns(leftCols))
		if n := len(a.LeftFixedVals); n > 0 {
			ob.Attrf("left fixed values", "%d column%s", n, util.Pluralize(int64(n)))
		}
		e.emitTableAndIndex("right table", a.RightTable, a.RightIndex)
		ob.Attrf("right columns", "(%s)", printColumns(rightCols))
		if n := len(a.RightFixedVals); n > 0 {
			ob.Attrf("right fixed values", "%d column%s", n, util.Pluralize(int64(n)))
		}

	case invertedFilterOp:
		a := n.args.(*invertedFilterArgs)
		ob.Attr("inverted column", a.Input.Columns()[a.InvColumn].Name)
		ob.Attr("num spans", len(a.InvFilter.SpansToRead))

	case invertedJoinOp:
		a := n.args.(*invertedJoinArgs)
		e.emitTableAndIndex("table", a.Table, a.Index)
		cols := appendColumns(a.Input.Columns(), tableColumns(a.Table, a.LookupCols)...)
		ob.VExpr("inverted expr", a.InvertedExpr, cols)
		// TODO(radu): we should be passing nil instead of true.
		if a.OnCond != tree.DBoolTrue {
			ob.Expr("on", a.OnCond, cols)
		}

	case projectSetOp:
		a := n.args.(*projectSetArgs)
		if ob.flags.Verbose {
			for i := range a.Exprs {
				ob.Expr(fmt.Sprintf("render %d", i), a.Exprs[i], a.Input.Columns())
			}
		}

	case windowOp:
		a := n.args.(*windowArgs)
		if ob.flags.Verbose {
			for i := range a.Window.Exprs {
				ob.Expr(fmt.Sprintf("window %d", i), a.Window.Exprs[i], a.Input.Columns())
			}
		}

	case bufferOp:
		a := n.args.(*bufferArgs)
		ob.Attr("label", a.Label)

	case scanBufferOp:
		a := n.args.(*scanBufferArgs)
		ob.Attr("label", a.Label)

	case insertOp:
		a := n.args.(*insertArgs)
		ob.Attrf(
			"into", "%s(%s)",
			a.Table.Name(),
			printColumns(tableColumns(a.Table, a.InsertCols)),
		)
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}
		if len(a.ArbiterIndexes) > 0 {
			var sb strings.Builder
			for i, idx := range a.ArbiterIndexes {
				index := a.Table.Index(idx)
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(string(index.Name()))
			}
			ob.Attr("arbiter indexes", sb.String())
		}
		if len(a.ArbiterConstraints) > 0 {
			var sb strings.Builder
			for i, uc := range a.ArbiterConstraints {
				uniqueConstraint := a.Table.Unique(uc)
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(uniqueConstraint.Name())
			}
			ob.Attr("arbiter constraints", sb.String())
		}

	case insertFastPathOp:
		a := n.args.(*insertFastPathArgs)
		ob.Attrf(
			"into", "%s(%s)",
			a.Table.Name(),
			printColumns(tableColumns(a.Table, a.InsertCols)),
		)
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}
		for _, fk := range a.FkChecks {
			ob.Attr(
				"FK check", fmt.Sprintf("%s@%s", fk.ReferencedTable.Name(), fk.ReferencedIndex.Name()),
			)
		}
		if len(a.Rows) > 0 {
			e.emitTuples(a.Rows, len(a.Rows[0]))
		}

	case upsertOp:
		a := n.args.(*upsertArgs)
		ob.Attrf(
			"into", "%s(%s)",
			a.Table.Name(),
			printColumns(tableColumns(a.Table, a.InsertCols)),
		)
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}
		if len(a.ArbiterIndexes) > 0 {
			var sb strings.Builder
			for i, idx := range a.ArbiterIndexes {
				index := a.Table.Index(idx)
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(string(index.Name()))
			}
			ob.Attr("arbiter indexes", sb.String())
		}
		if len(a.ArbiterConstraints) > 0 {
			var sb strings.Builder
			for i, uc := range a.ArbiterConstraints {
				uniqueConstraint := a.Table.Unique(uc)
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(uniqueConstraint.Name())
			}
			ob.Attr("arbiter constraints", sb.String())
		}

	case updateOp:
		a := n.args.(*updateArgs)
		ob.Attrf("table", "%s", a.Table.Name())
		ob.Attr("set", printColumns(tableColumns(a.Table, a.UpdateCols)))
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}

	case deleteOp:
		a := n.args.(*deleteArgs)
		ob.Attrf("from", "%s", a.Table.Name())
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}

	case deleteRangeOp:
		a := n.args.(*deleteRangeArgs)
		ob.Attrf("from", "%s", a.Table.Name())
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}
		// TODO(radu): this is hacky.
		params := exec.ScanParams{
			NeededCols:      a.Needed,
			IndexConstraint: a.IndexConstraint,
		}
		e.emitSpans("spans", a.Table, a.Table.Index(cat.PrimaryIndex), params)

	case simpleProjectOp,
		serializingProjectOp,
		ordinalityOp,
		max1RowOp,
		hashSetOpOp,
		streamingSetOpOp,
		explainOptOp,
		explainOp,
		showTraceOp,
		createTableOp,
		createTableAsOp,
		createViewOp,
		sequenceSelectOp,
		saveTableOp,
		errorIfRowsOp,
		opaqueOp,
		alterTableSplitOp,
		alterTableUnsplitOp,
		alterTableUnsplitAllOp,
		alterTableRelocateOp,
		recursiveCTEOp,
		controlJobsOp,
		controlSchedulesOp,
		cancelQueriesOp,
		cancelSessionsOp,
		createStatisticsOp,
		exportOp:

	default:
		return errors.AssertionFailedf("unhandled op %d", n.op)
	}
	return nil
}

func (e *emitter) emitTableAndIndex(field string, table cat.Table, index cat.Index) {
	partial := ""
	if _, isPartial := index.Predicate(); isPartial {
		partial = " (partial index)"
	}
	e.ob.Attrf(field, "%s@%s%s", table.Name(), index.Name(), partial)
}

func (e *emitter) emitSpans(
	field string, table cat.Table, index cat.Index, scanParams exec.ScanParams,
) {
	e.ob.Attr(field, e.spansStr(table, index, scanParams))
}

func (e *emitter) spansStr(table cat.Table, index cat.Index, scanParams exec.ScanParams) string {
	if scanParams.InvertedConstraint == nil && scanParams.IndexConstraint == nil {
		if scanParams.HardLimit > 0 {
			return "LIMITED SCAN"
		}
		return "FULL SCAN"
	}

	// In verbose mode show the physical spans, unless the table is virtual.
	if e.ob.flags.Verbose && !table.IsVirtualTable() {
		return e.spanFormatFn(table, index, scanParams)
	}

	// For inverted constraints, just print the number of spans. Inverted key
	// values are generally not user-readable.
	if scanParams.InvertedConstraint != nil {
		n := len(scanParams.InvertedConstraint)
		return fmt.Sprintf("%d span%s", n, util.Pluralize(int64(n)))
	}

	// If we must hide values, only show the count.
	if e.ob.flags.HideValues {
		n := scanParams.IndexConstraint.Spans.Count()
		return fmt.Sprintf("%d span%s", n, util.Pluralize(int64(n)))
	}

	sp := &scanParams.IndexConstraint.Spans
	// Show up to 4 logical spans.
	if maxSpans := 4; sp.Count() > maxSpans {
		trunc := &constraint.Spans{}
		trunc.Alloc(maxSpans)
		for i := 0; i < maxSpans; i++ {
			trunc.Append(sp.Get(i))
		}
		return fmt.Sprintf("%s … (%d more)", trunc.String(), sp.Count()-maxSpans)
	}
	return sp.String()
}

func (e *emitter) emitLockingPolicy(locking *tree.LockingItem) {
	if locking == nil {
		return
	}
	strength := descpb.ToScanLockingStrength(locking.Strength)
	waitPolicy := descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	if strength != descpb.ScanLockingStrength_FOR_NONE {
		e.ob.Attr("locking strength", strength.PrettyString())
	}
	if waitPolicy != descpb.ScanLockingWaitPolicy_BLOCK {
		e.ob.Attr("locking wait policy", waitPolicy.PrettyString())
	}
}

func (e *emitter) emitTuples(rows [][]tree.TypedExpr, numColumns int) {
	e.ob.Attrf(
		"size", "%d column%s, %d row%s",
		numColumns, util.Pluralize(int64(numColumns)),
		len(rows), util.Pluralize(int64(len(rows))),
	)
	if e.ob.flags.Verbose {
		for i := range rows {
			for j, expr := range rows[i] {
				e.ob.Expr(fmt.Sprintf("row %d, expr %d", i, j), expr, nil /* varColumns */)
			}
		}
	}
}

func (e *emitter) emitGroupByAttributes(
	inputCols colinfo.ResultColumns,
	aggs []exec.AggInfo,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering colinfo.ColumnOrdering,
	isScalar bool,
) {
	if e.ob.flags.Verbose {
		for i, agg := range aggs {
			var buf bytes.Buffer
			fmt.Fprintf(&buf, "%s(", agg.FuncName)
			if agg.Distinct {
				buf.WriteString("DISTINCT ")
			}
			buf.WriteString(printColumnList(inputCols, agg.ArgCols))
			buf.WriteByte(')')
			if agg.Filter != -1 {
				fmt.Fprintf(&buf, " FILTER (WHERE %s)", inputCols[agg.Filter].Name)
			}
			e.ob.Attr(fmt.Sprintf("aggregate %d", i), buf.String())
		}
	}
	if len(groupCols) > 0 {
		e.ob.Attr("group by", printColumnList(inputCols, groupCols))
	}
	if len(groupColOrdering) > 0 {
		e.ob.Attr("ordered", groupColOrdering.String(inputCols))
	}
}

func (e *emitter) emitJoinAttributes(
	leftCols, rightCols colinfo.ResultColumns,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) {
	if len(leftEqCols) > 0 {
		e.ob.Attrf("equality", "(%s) = (%s)", printColumnList(leftCols, leftEqCols), printColumnList(rightCols, rightEqCols))
		if leftEqColsAreKey {
			e.ob.Attr("left cols are key", "")
		}
		if rightEqColsAreKey {
			e.ob.Attr("right cols are key", "")
		}
	}
	e.ob.Expr("pred", extraOnCond, appendColumns(leftCols, rightCols...))
}

func printColumns(inputCols colinfo.ResultColumns) string {
	var buf bytes.Buffer
	for i, col := range inputCols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.Name)
	}
	return buf.String()
}

func printColumnList(inputCols colinfo.ResultColumns, cols []exec.NodeColumnOrdinal) string {
	var buf bytes.Buffer
	for i, col := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(inputCols[col].Name)
	}
	return buf.String()
}

func printColumnSet(inputCols colinfo.ResultColumns, cols exec.NodeColumnOrdinalSet) string {
	var buf bytes.Buffer
	prefix := ""
	cols.ForEach(func(col int) {
		buf.WriteString(prefix)
		prefix = ", "
		buf.WriteString(inputCols[col].Name)
	})
	return buf.String()
}
