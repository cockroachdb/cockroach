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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// Emit produces the EXPLAIN output against the given OutputBuilder. The
// OutputBuilder flags are taken into account.
func Emit(plan *Plan, ob *OutputBuilder, spanFormatFn SpanFormatFn) error {
	e := makeEmitter(ob, spanFormatFn)
	var walk func(n *Node) error
	walk = func(n *Node) error {
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
		ob.Attr("original sql", tree.AsStringWithFlags(s.ExprNode, tree.FmtSimple))
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
		ob.Attr("input", plan.Cascades[i].Buffer.(*Node).args.(*bufferArgs).Label)
		ob.LeaveNode()
	}
	for _, n := range plan.Checks {
		ob.EnterMetaNode("fk-check")
		if err := walk(n); err != nil {
			return err
		}
		ob.LeaveNode()
	}
	ob.LeaveNode()
	return nil
}

// SpanFormatFn is a function used to format spans for EXPLAIN. Only called when
// there is an index constraint or an inverted constraint.
type SpanFormatFn func(table cat.Table, index cat.Index, scanParams exec.ScanParams) string

// omitTrivialProjections returns the given node and its result columns and
// ordering, unless the node is an identity projection (which just renames
// columns) - in which case we return the child node and the renamed columns.
func omitTrivialProjections(n *Node) (*Node, sqlbase.ResultColumns, sqlbase.ColumnOrdering) {
	if n.op != serializingProjectOp {
		return n, n.Columns(), n.Ordering()
	}

	projection := n.args.(*serializingProjectArgs).Cols

	input := n.children[0]
	// Check if the projection is the identity.
	// TODO(radu): extend this to omit projections that only reorder or rename columns.
	if len(projection) != len(input.Columns()) {
		return n, n.Columns(), n.Ordering()
	}
	for i := range projection {
		if int(projection[i]) != i {
			return n, n.Columns(), n.Ordering()
		}
	}
	return input, n.Columns(), input.Ordering()
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
		if len(n.args.(*hashJoinArgs).LeftEqCols) == 0 {
			return "cross join", nil
		}
		return "hash join", nil

	case lookupJoinOp:
		if n.args.(*lookupJoinArgs).Table.IsVirtualTable() {
			return "virtual table lookup join", nil
		}
		return "lookup join", nil

	case setOpOp:
		a := n.args.(*setOpArgs)
		// TODO(radu): fix these terrible names.
		if a.Typ == tree.UnionOp && a.All {
			return "append", nil
		}
		return "union", nil

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
	applyJoinOp:            "apply join",
	bufferOp:               "buffer",
	cancelQueriesOp:        "cancel queries",
	cancelSessionsOp:       "cancel sessions",
	controlJobsOp:          "control jobs",
	controlSchedulesOp:     "control schedules",
	createTableOp:          "create table",
	createTableAsOp:        "create table as",
	createViewOp:           "create view",
	deleteOp:               "delete",
	deleteRangeOp:          "delete range",
	distinctOp:             "distinct",
	errorIfRowsOp:          "error if rows",
	explainOp:              "explain",
	explainOptOp:           "explain",
	explainPlanOp:          "explain",
	exportOp:               "export",
	filterOp:               "filter",
	groupByOp:              "group",
	hashJoinOp:             "", // This node does not have a fixed name.
	indexJoinOp:            "index join",
	insertFastPathOp:       "insert fast path",
	insertOp:               "insert",
	interleavedJoinOp:      "interleaved join",
	invertedFilterOp:       "inverted filter",
	invertedJoinOp:         "inverted join",
	limitOp:                "limit",
	lookupJoinOp:           "", // This node does not have a fixed name.
	max1RowOp:              "max1row",
	mergeJoinOp:            "merge join",
	opaqueOp:               "", // This node does not have a fixed name.
	ordinalityOp:           "ordinality",
	projectSetOp:           "project set",
	recursiveCTEOp:         "recursive cte",
	renderOp:               "render",
	saveTableOp:            "save table",
	scalarGroupByOp:        "group",
	scanBufferOp:           "scan buffer",
	scanOp:                 "", // This node does not have a fixed name.
	sequenceSelectOp:       "sequence select",
	setOpOp:                "", // This node does not have a fixed name.
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

func (e *emitter) emitNodeAttributes(n *Node) error {
	if stats, ok := n.annotations[exec.EstimatedStatsID]; ok {
		s := stats.(*exec.EstimatedStats)

		// In verbose mode, we show the estimated row count for all nodes (except
		// Values, where it is redundant). In non-verbose mode, we only show it for
		// scans (and when it is based on real statistics), where it is most useful
		// and accurate.
		if n.op != valuesOp && (e.ob.flags.Verbose || n.op == scanOp) {
			count := int(math.Round(s.RowCount))
			if s.TableStatsAvailable {
				e.ob.Attr("estimated row count", count)
			} else {
				// No stats available.
				if e.ob.flags.Verbose {
					e.ob.Attrf("estimated row count", "%d (missing stats)", count)
				} else {
					// In non-verbose mode, don't show the row count (which is not based
					// on reality); only show a "missing stats" field. Don't show it for
					// virtual tables though, where we expect no stats.
					if !n.args.(*scanArgs).Table.IsVirtualTable() {
						e.ob.Attr("missing stats", "")
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
			// TODO(radu): should be Vattr.
			ob.Attr("parallel", "")
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
				ob.Expr(fmt.Sprintf("render %d", i), a.Exprs[i], a.Input.Columns())
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
		ob.Attr("order", sqlbase.ColumnOrdering(a.Ordering).String(n.Columns()))
		if p := a.AlreadyOrderedPrefix; p > 0 {
			ob.Attr("already ordered", sqlbase.ColumnOrdering(a.Ordering[:p]).String(n.Columns()))
		}

	case indexJoinOp:
		a := n.args.(*indexJoinArgs)
		ob.Attrf("table", "%s@%s", a.Table.Name(), a.Table.Index(0).Name())
		cols := make([]string, len(a.KeyCols))
		inputCols := a.Input.Columns()
		for i, c := range a.KeyCols {
			cols[i] = inputCols[c].Name
		}
		// TODO(radu): should be verbose only.
		ob.Attr("key columns", strings.Join(cols, ", "))

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
			a.JoinType,
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
			a.JoinType,
			leftCols, rightCols,
			leftEqCols, rightEqCols,
			a.LeftEqColsAreKey, a.RightEqColsAreKey,
			a.OnCond,
		)
		eqCols := make(sqlbase.ResultColumns, len(leftEqCols))
		mergeOrd := make(sqlbase.ColumnOrdering, len(eqCols))
		for i := range eqCols {
			eqCols[i].Name = fmt.Sprintf(
				"(%s=%s)", leftCols[leftEqCols[i]].Name, rightCols[rightEqCols[i]].Name,
			)
			mergeOrd[i].ColIdx = i
			mergeOrd[i].Direction = a.LeftOrdering[i].Direction
		}
		// TODO(radu): fix this field name,
		ob.Attr("mergeJoinOrder", mergeOrd.String(eqCols))

	case applyJoinOp:
		a := n.args.(*applyJoinArgs)
		e.emitJoinType(a.JoinType, false /* hasEqCols */)
		if a.OnCond != nil {
			ob.Expr("pred", a.OnCond, appendColumns(a.Left.Columns(), a.RightColumns...))
		}

	case lookupJoinOp:
		a := n.args.(*lookupJoinArgs)
		e.emitTableAndIndex("table", a.Table, a.Index)
		e.emitJoinType(a.JoinType, true /* hasEqCols */)
		inputCols := a.Input.Columns()
		rightEqCols := make([]string, len(a.EqCols))
		for i := range rightEqCols {
			rightEqCols[i] = string(a.Index.Column(i).ColName())
		}
		ob.Attrf(
			"equality", "(%s) = (%s)",
			printColumnList(inputCols, a.EqCols),
			strings.Join(rightEqCols, ","),
		)
		if a.EqColsAreKey {
			ob.Attr("equality cols are key", "")
			// TODO(radu): clean this up.
			ob.Attr("parallel", "")
		}
		ob.Expr("pred", a.OnCond, appendColumns(inputCols, tableColumns(a.Table, a.LookupCols)...))

	case interleavedJoinOp:
		a := n.args.(*interleavedJoinArgs)
		leftCols := tableColumns(a.LeftTable, a.LeftParams.NeededCols)
		rightCols := tableColumns(a.RightTable, a.RightParams.NeededCols)
		e.emitJoinType(a.JoinType, true /* hasEqCols */)
		e.emitTableAndIndex("left table", a.LeftTable, a.LeftIndex)
		e.emitSpans("left spans", a.LeftTable, a.LeftIndex, a.LeftParams)
		ob.Expr("left filter", a.LeftFilter, leftCols)
		e.emitTableAndIndex("right table", a.RightTable, a.RightIndex)
		e.emitSpans("right spans", a.RightTable, a.RightIndex, a.RightParams)
		ob.Expr("right filter", a.RightFilter, rightCols)
		ob.Expr("pred", a.OnCond, appendColumns(leftCols, rightCols...))

		if a.LeftIsAncestor {
			ob.Attr("ancestor", "left")
			e.emitLockingPolicy(a.LeftParams.Locking)
		} else {
			ob.Attr("ancestor", "right")
			e.emitLockingPolicy(a.RightParams.Locking)
		}

	case zigzagJoinOp:
		a := n.args.(*zigzagJoinArgs)
		// TODO(radu): remove this.
		e.emitJoinType(descpb.InnerJoin, true /* hasEqCols */)
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
		e.emitJoinType(a.JoinType, true /* hasEqCols */)
		cols := appendColumns(a.Input.Columns(), tableColumns(a.Table, a.LookupCols)...)
		// TODO(radu): add field name.
		ob.Expr("", a.InvertedExpr, cols)
		// TODO(radu): we should be passing nil instead of true.
		if a.OnCond != tree.DBoolTrue {
			ob.Expr("onExpr", a.OnCond, cols)
		}
		ob.Attr("parallel", "")

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
		e.emitTuples(a.Rows, len(a.Rows[0]))

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
		setOpOp,
		ordinalityOp,
		max1RowOp,
		explainOptOp,
		explainOp,
		explainPlanOp,
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
	if scanParams.InvertedConstraint == nil && scanParams.IndexConstraint == nil {
		if scanParams.HardLimit > 0 {
			e.ob.Attr(field, "LIMITED SCAN")
		} else {
			e.ob.Attr(field, "FULL SCAN")
		}
	} else {
		if e.ob.flags.HideValues {
			n := len(scanParams.InvertedConstraint)
			if scanParams.IndexConstraint != nil {
				n = scanParams.IndexConstraint.Spans.Count()
			}
			e.ob.Attrf(field, "%d span%s", n, util.Pluralize(int64(n)))
		} else {
			e.ob.Attr(field, e.spanFormatFn(table, index, scanParams))
		}
	}
}

func (e *emitter) emitLockingPolicy(locking *tree.LockingItem) {
	if locking == nil {
		return
	}
	strength := descpb.ToScanLockingStrength(locking.Strength)
	waitPolicy := descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	if strength != descpb.ScanLockingStrength_FOR_NONE {
		// TODO(radu): should be Vattr.
		e.ob.Attr("locking strength", strength.PrettyString())
	}
	if waitPolicy != descpb.ScanLockingWaitPolicy_BLOCK {
		// TODO(radu): should be Vattr.
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
	inputCols sqlbase.ResultColumns,
	aggs []exec.AggInfo,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	isScalar bool,
) {
	// TODO(radu): make this loop verbose only.
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
	if len(groupCols) > 0 {
		e.ob.Attr("group by", printColumnList(inputCols, groupCols))
	}
	if len(groupColOrdering) > 0 {
		e.ob.Attr("ordered", groupColOrdering.String(inputCols))
	}
	if isScalar {
		e.ob.Attr("scalar", "")
	}
}

func (e *emitter) emitJoinAttributes(
	joinType descpb.JoinType,
	leftCols, rightCols sqlbase.ResultColumns,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) {
	hasEqCols := len(leftEqCols) > 0
	e.emitJoinType(joinType, hasEqCols)
	if hasEqCols {
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

func (e *emitter) emitJoinType(joinType descpb.JoinType, hasEqCols bool) {
	ob := e.ob
	switch joinType {
	case descpb.InnerJoin:
		if !hasEqCols {
			ob.Attr("type", "cross")
		} else {
			// TODO(radu): omit this?
			ob.Attr("type", "inner")
		}
	case descpb.LeftOuterJoin:
		ob.Attr("type", "left outer")
	case descpb.RightOuterJoin:
		ob.Attr("type", "right outer")
	case descpb.FullOuterJoin:
		ob.Attr("type", "full outer")
	case descpb.LeftSemiJoin:
		ob.Attr("type", "semi")
	case descpb.LeftAntiJoin:
		ob.Attr("type", "anti")
	default:
		ob.Attrf("type", "invalid (%d)", joinType)
	}
}

func printColumns(inputCols sqlbase.ResultColumns) string {
	var buf bytes.Buffer
	for i, col := range inputCols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.Name)
	}
	return buf.String()
}

func printColumnList(inputCols sqlbase.ResultColumns, cols []exec.NodeColumnOrdinal) string {
	var buf bytes.Buffer
	for i, col := range cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(inputCols[col].Name)
	}
	return buf.String()
}

func printColumnSet(inputCols sqlbase.ResultColumns, cols exec.NodeColumnOrdinalSet) string {
	var buf bytes.Buffer
	prefix := ""
	cols.ForEach(func(col int) {
		buf.WriteString(prefix)
		prefix = ", "
		buf.WriteString(inputCols[col].Name)
	})
	return buf.String()
}
