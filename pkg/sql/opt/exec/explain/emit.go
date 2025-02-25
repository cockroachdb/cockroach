// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	humanize "github.com/dustin/go-humanize"
)

// Emit produces the EXPLAIN output against the given OutputBuilder. The
// OutputBuilder flags are taken into account.
func Emit(
	ctx context.Context,
	evalCtx *eval.Context,
	plan *Plan,
	ob *OutputBuilder,
	spanFormatFn SpanFormatFn,
	createPostQueryPlanIfMissing bool,
) error {
	return emitInternal(ctx, evalCtx, plan, ob, spanFormatFn, nil /* visitedFKsByCascades */, createPostQueryPlanIfMissing)
}

// MaybeAdjustVirtualIndexScan is injected from the sql package.
//
// This function clarifies usage of the virtual indexes for EXPLAIN purposes.
var MaybeAdjustVirtualIndexScan func(
	ctx context.Context, evalCtx *eval.Context, index cat.Index, params exec.ScanParams,
) (_ cat.Index, _ exec.ScanParams, extraAttribute string)

// joinIndexNames emits a string of index names on table 'table' as specified in
// 'ords', with each name separated by 'sep'.
func joinIndexNames(table cat.Table, ords cat.IndexOrdinals, sep string) string {
	var sb strings.Builder
	for i, idx := range ords {
		index := table.Index(idx)
		if i > 0 {
			sb.WriteString(sep)
		}
		sb.WriteString(string(index.Name()))
	}
	return sb.String()
}

// - visitedFKsByCascades is updated on recursive calls for each cascade plan.
// Can be nil if the plan doesn't have any cascades. In this map the key is the
// "id" of the FK constraint that we construct as OriginTableID || Name.
func emitInternal(
	ctx context.Context,
	evalCtx *eval.Context,
	plan *Plan,
	ob *OutputBuilder,
	spanFormatFn SpanFormatFn,
	visitedFKsByCascades map[string]struct{},
	createPostQueryPlanIfMissing bool,
) error {
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
		if err := e.emitNodeAttributes(ctx, evalCtx, n); err != nil {
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

	if len(plan.Subqueries) == 0 && len(plan.Cascades) == 0 &&
		len(plan.Checks) == 0 && len(plan.Triggers) == 0 {
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
			flags := tree.FmtSimple | tree.FmtShortenConstants
			if e.ob.flags.HideValues {
				flags |= tree.FmtHideConstants
			}
			if e.ob.flags.RedactValues {
				flags |= tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
			}
			ob.Attr("original sql", tree.AsStringWithFlags(s.ExprNode, flags))
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
		case exec.SubqueryDiscardAllRows:
			mode = "discard all rows"
		default:
			return errors.Errorf("invalid SubqueryMode %d", s.Mode)
		}

		ob.Attr("exec mode", mode)
		if err := walk(s.Root.(*Node)); err != nil {
			return err
		}
		ob.LeaveNode()
	}
	emitPostQuery := func(pq exec.PostQuery, pqPlan exec.Plan, alreadyEmitted bool) error {
		if pqPlan != nil {
			return emitInternal(ctx, evalCtx, pqPlan.(*Plan), ob, spanFormatFn, visitedFKsByCascades, createPostQueryPlanIfMissing)
		}
		if !alreadyEmitted {
			// The plan wasn't produced which means its execution was
			// short-circuited.
			ob.Attr("short-circuited", "")
		}
		if buffer := pq.Buffer; buffer != nil {
			ob.Attr("input", buffer.(*Node).args.(*bufferArgs).Label)
		}
		return nil
	}
	for _, cascade := range plan.Cascades {
		ob.EnterMetaNode("fk-cascade")
		ob.Attr("fk", cascade.FKConstraint.Name())
		if visitedFKsByCascades == nil {
			visitedFKsByCascades = make(map[string]struct{})
		}
		// Come up with a custom "id" for this FK.
		fk := cascade.FKConstraint
		fkID := fmt.Sprintf("%d%s", fk.OriginTableID(), fk.Name())
		var err error
		var cascadePlan exec.Plan
		var alreadyEmitted bool
		if _, alreadyEmitted = visitedFKsByCascades[fkID]; !alreadyEmitted {
			cascadePlan, err = cascade.GetExplainPlan(ctx, createPostQueryPlanIfMissing)
			if err != nil {
				return err
			}
			visitedFKsByCascades[fkID] = struct{}{}
			defer delete(visitedFKsByCascades, fkID) //nolint:deferloop
		}
		if err = emitPostQuery(cascade, cascadePlan, alreadyEmitted); err != nil {
			return err
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
	for _, afterTriggers := range plan.Triggers {
		ob.EnterMetaNode("after-triggers")
		for _, trigger := range afterTriggers.Triggers {
			ob.Attr("trigger", trigger.Name())
		}
		afterTriggersPlan, err := afterTriggers.GetExplainPlan(ctx, createPostQueryPlanIfMissing)
		if err != nil {
			return err
		}
		if err = emitPostQuery(afterTriggers, afterTriggersPlan, false /* alreadyEmitted */); err != nil {
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
	// columns), and construct the inverse projection.
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

func (e *emitter) nodeName(n *Node) (name string, _ error) {
	defer func() {
		if stats, ok := n.annotations[exec.ExecutionStatsID]; ok && !omitStats(n) {
			if stats.(*exec.ExecutionStats).UsedStreamer {
				name += " (streamer)"
			}
		}
	}()

	switch n.op {
	case scanOp:
		a := n.args.(*scanArgs)
		if a.Table == nil {
			return "scan", nil
		}
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

	case groupByOp:
		a := n.args.(*groupByArgs)
		switch a.groupingOrderType {
		case exec.Streaming:
			return "group (streaming)", nil
		case exec.PartialStreaming:
			return "group (partial streaming)", nil
		case exec.NoStreaming:
			return "group (hash)", nil
		default:
			return "", errors.AssertionFailedf("unhandled group by order type %d", a.groupingOrderType)
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
		if a.Metadata == nil {
			return "<unknown>", nil
		}
		return strings.ToLower(a.Metadata.String()), nil
	}

	if n.op < 0 || int(n.op) >= len(nodeNames) || nodeNames[n.op] == "" {
		return "", errors.AssertionFailedf("unhandled op %d", n.op)
	}
	return nodeNames[n.op], nil
}

var nodeNames = [...]string{
	alterRangeRelocateOp:   "relocate range",
	alterTableRelocateOp:   "relocate table",
	alterTableSplitOp:      "split",
	alterTableUnsplitAllOp: "unsplit all",
	alterTableUnsplitOp:    "unsplit",
	applyJoinOp:            "", // This node does not have a fixed name.
	bufferOp:               "buffer",
	callOp:                 "call",
	cancelQueriesOp:        "cancel queries",
	cancelSessionsOp:       "cancel sessions",
	controlJobsOp:          "control jobs",
	controlSchedulesOp:     "control schedules",
	createStatisticsOp:     "create statistics",
	createFunctionOp:       "create function",
	createTableOp:          "create table",
	createTableAsOp:        "create table as",
	createTriggerOp:        "create trigger",
	createViewOp:           "create view",
	deleteOp:               "delete",
	deleteRangeOp:          "delete range",
	distinctOp:             "distinct",
	errorIfRowsOp:          "error if rows",
	explainOp:              "explain",
	explainOptOp:           "explain",
	exportOp:               "export",
	filterOp:               "filter",
	groupByOp:              "", // This node does not have a fixed name.
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
	showCompletionsOp:      "show completions",
	showTraceOp:            "show trace",
	simpleProjectOp:        "project",
	serializingProjectOp:   "project",
	sortOp:                 "sort",
	topKOp:                 "top-k",
	updateOp:               "update",
	upsertOp:               "upsert",
	valuesOp:               "", // This node does not have a fixed name.
	vectorSearchOp:         "vector search",
	vectorMutationSearchOp: "vector mutation search",
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

// omitStats returns true if n should not be annotated with the execution
// statistics nor estimates.
func omitStats(n *Node) bool {
	// Some simple nodes don't have their own statistics, yet they share the
	// stats with their children. In such scenarios, we skip stats for the
	// "simple" node to avoid confusion. The rule of thumb for including a node
	// into this list is checking whether it is handled during the
	// post-processing stage by the DistSQL engine.
	switch n.op {
	case simpleProjectOp,
		serializingProjectOp,
		renderOp,
		limitOp:
		return true
	}
	return false
}

func (e *emitter) emitNodeAttributes(ctx context.Context, evalCtx *eval.Context, n *Node) error {
	var actualRowCount uint64
	var hasActualRowCount bool
	if stats, ok := n.annotations[exec.ExecutionStatsID]; ok && !omitStats(n) {
		s := stats.(*exec.ExecutionStats)
		if len(s.SQLNodes) > 0 {
			e.ob.AddFlakyField(DeflakeNodes, "sql nodes", strings.Join(s.SQLNodes, ", "))
		}
		if len(s.KVNodes) > 0 {
			e.ob.AddFlakyField(DeflakeNodes, "kv nodes", strings.Join(s.KVNodes, ", "))
		}
		if len(s.Regions) > 0 {
			e.ob.AddFlakyField(DeflakeNodes, "regions", strings.Join(s.Regions, ", "))
		}
		if s.UsedFollowerRead {
			e.ob.AddField("used follower read", "")
		}
		if s.RowCount.HasValue() {
			actualRowCount = s.RowCount.Value()
			hasActualRowCount = true
			e.ob.AddField("actual row count", string(humanizeutil.Count(actualRowCount)))
		}
		// Omit vectorized batches in non-verbose mode.
		if e.ob.flags.Verbose {
			if s.VectorizedBatchCount.HasValue() {
				e.ob.AddField("vectorized batch count",
					string(humanizeutil.Count(s.VectorizedBatchCount.Value())))
			}
		}
		if s.KVTime.HasValue() {
			e.ob.AddField("KV time", string(humanizeutil.Duration(s.KVTime.Value())))
		}
		if s.KVContentionTime.HasValue() {
			e.ob.AddField("KV contention time", string(humanizeutil.Duration(s.KVContentionTime.Value())))
		}
		if s.KVRowsRead.HasValue() {
			e.ob.AddField("KV rows decoded", string(humanizeutil.Count(s.KVRowsRead.Value())))
		}
		if s.KVPairsRead.HasValue() {
			pairs := s.KVPairsRead.Value()
			rows := s.KVRowsRead.Value()
			if pairs != rows || e.ob.flags.Verbose {
				// Only show the number of KV pairs read when it's different
				// from the number of rows decoded or if verbose output is
				// requested.
				e.ob.AddField("KV pairs read", string(humanizeutil.Count(s.KVPairsRead.Value())))
			}
		}
		if s.KVBytesRead.HasValue() {
			e.ob.AddField("KV bytes read", humanize.IBytes(s.KVBytesRead.Value()))
		}
		if s.KVBatchRequestsIssued.HasValue() {
			e.ob.AddField("KV gRPC calls", string(humanizeutil.Count(s.KVBatchRequestsIssued.Value())))
		}
		if s.MaxAllocatedMem.HasValue() {
			e.ob.AddField("estimated max memory allocated", humanize.IBytes(s.MaxAllocatedMem.Value()))
		}
		if s.MaxAllocatedDisk.HasValue() {
			e.ob.AddField("estimated max sql temp disk usage", humanize.IBytes(s.MaxAllocatedDisk.Value()))
		}
		if s.SQLCPUTime.HasValue() {
			e.ob.AddField("sql cpu time", string(humanizeutil.Duration(s.SQLCPUTime.Value())))
		}
		if e.ob.flags.Verbose {
			if s.StepCount.HasValue() {
				e.ob.AddField("MVCC step count (ext/int)", fmt.Sprintf("%s/%s",
					humanizeutil.Count(s.StepCount.Value()), humanizeutil.Count(s.InternalStepCount.Value()),
				))
			}
			if s.SeekCount.HasValue() {
				e.ob.AddField("MVCC seek count (ext/int)", fmt.Sprintf("%s/%s",
					humanizeutil.Count(s.SeekCount.Value()), humanizeutil.Count(s.InternalSeekCount.Value()),
				))
			}
		}
	}

	var inaccurateEstimate bool
	const inaccurateFactor = 2
	const inaccurateAdditive = 100
	if stats, ok := n.annotations[exec.EstimatedStatsID]; ok && !omitStats(n) {
		s := stats.(*exec.EstimatedStats)

		var estimatedRowCountString string
		if s.LimitHint > 0 && s.LimitHint != s.RowCount {
			maxEstimatedRowCount := uint64(math.Ceil(math.Max(s.LimitHint, s.RowCount)))
			minEstimatedRowCount := uint64(math.Ceil(math.Min(s.LimitHint, s.RowCount)))
			estimatedRowCountString = fmt.Sprintf("%s - %s", humanizeutil.Count(minEstimatedRowCount), humanizeutil.Count(maxEstimatedRowCount))
			if hasActualRowCount && s.TableStatsAvailable {
				// If we have both the actual row count and the table stats
				// available, check whether the estimate is inaccurate.
				inaccurateEstimate = actualRowCount*inaccurateFactor+inaccurateAdditive < minEstimatedRowCount ||
					maxEstimatedRowCount*inaccurateFactor+inaccurateAdditive < actualRowCount
			}
		} else {
			estimatedRowCount := uint64(math.Round(s.RowCount))
			estimatedRowCountString = string(humanizeutil.Count(estimatedRowCount))
			if hasActualRowCount && s.TableStatsAvailable {
				// If we have both the actual row count and the table stats
				// available, check whether the estimate is inaccurate.
				inaccurateEstimate = actualRowCount*inaccurateFactor+inaccurateAdditive < estimatedRowCount ||
					estimatedRowCount*inaccurateFactor+inaccurateAdditive < actualRowCount
			}
		}

		// Show the estimated row count (except Values, where it is redundant).
		if n.op != valuesOp && !e.ob.flags.OnlyShape {
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
					if e.ob.flags.Deflake.HasAny(DeflakeVolatile) {
						duration = "<hidden>"
					} else {
						timeSinceStats := timeutil.Since(s.TableStatsCreatedAt)
						if timeSinceStats < 0 {
							timeSinceStats = 0
						}
						duration = string(humanizeutil.LongDuration(timeSinceStats))
					}

					var forecastStr string
					if s.Forecast {
						if e.ob.flags.Deflake.HasAny(DeflakeVolatile) {
							forecastStr = "; using stats forecast"
						} else {
							timeSinceStats := timeutil.Since(s.ForecastAt)
							if timeSinceStats >= 0 {
								forecastStr = fmt.Sprintf(
									"; using stats forecast for %s ago", humanizeutil.LongDuration(timeSinceStats),
								)
							} else {
								timeSinceStats *= -1
								forecastStr = fmt.Sprintf(
									"; using stats forecast for %s in the future",
									humanizeutil.LongDuration(timeSinceStats),
								)
							}
						}
					}

					e.ob.AddField("estimated row count", fmt.Sprintf(
						"%s (%s%% of the table; stats collected %s ago%s)",
						estimatedRowCountString, percentageStr,
						duration, forecastStr,
					))
				} else {
					e.ob.AddField("estimated row count", estimatedRowCountString)
				}
			} else {
				// No stats available.
				if e.ob.flags.Verbose {
					e.ob.Attrf("estimated row count", "%s (missing stats)", estimatedRowCountString)
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
		var suffix string
		if inaccurateEstimate {
			suffix = fmt.Sprintf(
				"  ----------------------  WARNING: the row count estimate is inaccurate, "+
					"consider running 'ANALYZE %s'", a.Table.Name(),
			)
			ob.AddWarning(fmt.Sprintf(
				"WARNING: the row count estimate on table %[1]q is inaccurate, "+
					"consider running 'ANALYZE %[1]s'", a.Table.Name(),
			))
		}
		var extraAttribute string
		if a.Table.IsVirtualTable() && MaybeAdjustVirtualIndexScan != nil {
			a.Index, a.Params, extraAttribute = MaybeAdjustVirtualIndexScan(ctx, evalCtx, a.Index, a.Params)
		}
		e.emitTableAndIndex("table", a.Table, a.Index, suffix)
		// Omit spans for virtual tables, unless we actually have a constraint.
		if a.Table != nil && !(a.Table.IsVirtualTable() && a.Params.IndexConstraint == nil) {
			e.emitSpans("spans", a.Table, a.Index, a.Params)
		}
		if extraAttribute != "" {
			ob.Attr(extraAttribute, "")
		}

		if a.Params.HardLimit > 0 {
			ob.Attr("limit", a.Params.HardLimit)
		} else if a.Params.HardLimit == -1 {
			ob.Attr("limit", "")
		}

		if a.Params.Parallelize {
			ob.VAttr("parallel", "")
		}
		e.emitLockingPolicy(a.Params.Locking)

		if val, ok := n.annotations[exec.PolicyInfoID]; ok {
			e.emitPolicies(ob, a.Table, val.(*exec.RLSPoliciesApplied))
		}

	case valuesOp:
		a := n.args.(*valuesArgs)
		// Don't emit anything, except policy info, for the "norows" and "emptyrow" cases.
		if len(a.Rows) > 0 && (len(a.Rows) > 1 || len(a.Columns) > 0) {
			e.emitTuples(tree.RawRows(a.Rows), len(a.Columns))
		} else if len(a.Rows) == 0 {
			if val, ok := n.annotations[exec.PolicyInfoID]; ok {
				e.emitPolicies(ob, nil, val.(*exec.RLSPoliciesApplied))
			}
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

	case topKOp:
		a := n.args.(*topKArgs)
		ob.Attr("order", colinfo.ColumnOrdering(a.Ordering).String(n.Columns()))
		if a.K > 0 {
			ob.Attr("k", a.K)
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
			if len(inputCols) > int(c) {
				cols[i] = inputCols[c].Name
			} else {
				cols[i] = "_"
			}
		}
		ob.VAttr("key columns", strings.Join(cols, ", "))
		e.emitLockingPolicy(a.Locking)

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
		e.emitTableAndIndex("table", a.Table, a.Index, "" /* suffix */)
		inputCols := a.Input.Columns()
		if len(a.EqCols) > 0 {
			rightEqCols := make([]string, len(a.EqCols))
			for i := range rightEqCols {
				rightEqCols[i] = string(a.Index.Column(i).ColName())
			}
			ob.Attrf(
				"equality", "(%s) = (%s)",
				printColumnList(inputCols, a.EqCols),
				strings.Join(rightEqCols, ", "),
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
		e.emitTableAndIndex("left table", a.LeftTable, a.LeftIndex, "" /* suffix */)
		ob.Attrf("left columns", "(%s)", printColumns(leftCols))
		if n := len(a.LeftFixedVals); n > 0 {
			ob.Attrf("left fixed values", "%d column%s", n, util.Pluralize(int64(n)))
		}
		e.emitLockingPolicyWithPrefix("left ", a.LeftLocking)
		e.emitTableAndIndex("right table", a.RightTable, a.RightIndex, "" /* suffix */)
		ob.Attrf("right columns", "(%s)", printColumns(rightCols))
		if n := len(a.RightFixedVals); n > 0 {
			ob.Attrf("right fixed values", "%d column%s", n, util.Pluralize(int64(n)))
		}
		e.emitLockingPolicyWithPrefix("right ", a.RightLocking)

	case invertedFilterOp:
		a := n.args.(*invertedFilterArgs)
		if a.InvColumn != 0 {
			ob.Attr("inverted column", a.Input.Columns()[a.InvColumn].Name)
		}
		if a.InvFilter != nil && len(a.InvFilter.SpansToRead) > 0 {
			ob.Attr("num spans", len(a.InvFilter.SpansToRead))
		}

	case invertedJoinOp:
		a := n.args.(*invertedJoinArgs)
		e.emitTableAndIndex("table", a.Table, a.Index, "" /* suffix */)
		cols := appendColumns(a.Input.Columns(), tableColumns(a.Table, a.LookupCols)...)
		ob.VExpr("inverted expr", a.InvertedExpr, cols)
		// TODO(radu): we should be passing nil instead of true.
		if a.OnCond != tree.DBoolTrue {
			ob.Expr("on", a.OnCond, cols)
		}
		e.emitLockingPolicy(a.Locking)

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

	case vectorSearchOp:
		a := n.args.(*vectorSearchArgs)
		e.emitTableAndIndex("table", a.Table, a.Index, "" /* suffix */)
		ob.Attr("target count", a.TargetNeighborCount)
		if ob.flags.Verbose {
			if !a.PrefixKey.IsEmpty() {
				ob.Attr("prefix key", a.PrefixKey)
			}
			ob.Expr("query vector", a.QueryVector, nil /* varColumns */)
		}

	case vectorMutationSearchOp:
		a := n.args.(*vectorMutationSearchArgs)
		if a.IsIndexPut {
			ob.Attr("mutation type", "put")
		} else {
			ob.Attr("mutation type", "del")
		}
		e.emitTableAndIndex("table", a.Table, a.Index, "" /* suffix */)
		if ob.flags.Verbose {
			if len(a.PrefixKeyCols) > 0 {
				e.ob.Attr("prefix key cols", printColumnList(a.Input.Columns(), a.PrefixKeyCols))
			}
			e.ob.Attr("query vector col", a.Input.Columns()[a.QueryVectorCol].Name)
			if len(a.SuffixKeyCols) > 0 {
				e.ob.Attr("suffix key cols", printColumnList(a.Input.Columns(), a.SuffixKeyCols))
			}
		}

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
		if arbind := joinIndexNames(a.Table, a.ArbiterIndexes, ", "); arbind != "" {
			ob.Attr("arbiter indexes", arbind)
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
		if uniqWithTombstoneIndexes := joinIndexNames(a.Table, a.UniqueWithTombstonesIndexes, ", "); uniqWithTombstoneIndexes != "" {
			ob.Attr("uniqueness checks (tombstones)", uniqWithTombstoneIndexes)
		}
		beforeTriggers := cat.GetRowLevelTriggers(
			a.Table, tree.TriggerActionTimeBefore, tree.MakeTriggerEventTypeSet(tree.TriggerEventInsert),
		)
		if len(beforeTriggers) > 0 {
			ob.EnterMetaNode("before-triggers")
			for _, trigger := range beforeTriggers {
				ob.Attrf("trigger", "%s", trigger.Name())
			}
			ob.LeaveNode()
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
			e.emitLockingPolicyWithPrefix("FK check ", fk.Locking)
		}
		for _, uniq := range a.UniqChecks {
			ob.Attr(
				"uniqueness check", fmt.Sprintf("%s@%s", uniq.ReferencedTable.Name(), uniq.ReferencedIndex.Name()),
			)
			e.emitLockingPolicyWithPrefix("uniqueness check ", uniq.Locking)
		}
		if uniqWithTombstoneIndexes := joinIndexNames(a.Table, a.UniqueWithTombstonesIndexes, ", "); uniqWithTombstoneIndexes != "" {
			ob.Attr("uniqueness checks (tombstones)", uniqWithTombstoneIndexes)
		}
		if len(a.Rows) > 0 {
			e.emitTuples(tree.RawRows(a.Rows), len(a.Rows[0]))
		}
		if cat.HasRowLevelTriggers(a.Table, tree.TriggerActionTimeBefore, tree.TriggerEventInsert) {
			// The insert fast path should not be planned if there are applicable
			// triggers.
			return errors.AssertionFailedf("insert fast path with before-triggers")
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
		if arbind := joinIndexNames(a.Table, a.ArbiterIndexes, ", "); arbind != "" {
			ob.Attr("arbiter indexes", arbind)
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
		if uniqWithTombstoneIndexes := joinIndexNames(a.Table, a.UniqueWithTombstonesIndexes, ", "); uniqWithTombstoneIndexes != "" {
			ob.Attr("uniqueness checks (tombstones)", uniqWithTombstoneIndexes)
		}
		beforeTriggers := cat.GetRowLevelTriggers(
			a.Table, tree.TriggerActionTimeBefore,
			tree.MakeTriggerEventTypeSet(tree.TriggerEventInsert, tree.TriggerEventUpdate),
		)
		if len(beforeTriggers) > 0 {
			ob.EnterMetaNode("before-triggers")
			for _, trigger := range beforeTriggers {
				ob.Attrf("trigger", "%s", trigger.Name())
			}
			ob.LeaveNode()
		}

	case updateOp:
		a := n.args.(*updateArgs)
		ob.Attrf("table", "%s", a.Table.Name())
		if uniqWithTombstoneIndexes := joinIndexNames(a.Table, a.UniqueWithTombstonesIndexes, ", "); uniqWithTombstoneIndexes != "" {
			ob.Attr("uniqueness checks (tombstones)", uniqWithTombstoneIndexes)
		}
		ob.Attr("set", printColumns(tableColumns(a.Table, a.UpdateCols)))
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}
		beforeTriggers := cat.GetRowLevelTriggers(
			a.Table, tree.TriggerActionTimeBefore, tree.MakeTriggerEventTypeSet(tree.TriggerEventUpdate),
		)
		if len(beforeTriggers) > 0 {
			ob.EnterMetaNode("before-triggers")
			for _, trigger := range beforeTriggers {
				ob.Attrf("trigger", "%s", trigger.Name())
			}
			ob.LeaveNode()
		}

	case deleteOp:
		a := n.args.(*deleteArgs)
		ob.Attrf("from", "%s", a.Table.Name())
		if a.AutoCommit {
			ob.Attr("auto commit", "")
		}
		beforeTriggers := cat.GetRowLevelTriggers(
			a.Table, tree.TriggerActionTimeBefore, tree.MakeTriggerEventTypeSet(tree.TriggerEventDelete),
		)
		if len(beforeTriggers) > 0 {
			ob.EnterMetaNode("before-triggers")
			for _, trigger := range beforeTriggers {
				ob.Attrf("trigger", "%s", trigger.Name())
			}
			ob.LeaveNode()
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
		if cat.HasRowLevelTriggers(a.Table, tree.TriggerActionTimeBefore, tree.TriggerEventDelete) {
			// DeleteRange should not be planned if there are applicable triggers.
			return errors.AssertionFailedf("delete range with before-triggers")
		}

	case showCompletionsOp:
		a := n.args.(*showCompletionsArgs)
		if a.Command != nil {
			ob.Attrf("offset", "%s", a.Command.Offset.OrigString())
			ob.Attrf("syntax", "%q", shorten(a.Command.Statement.RawString()))
		}

	case alterTableSplitOp:
		a := n.args.(*alterTableSplitArgs)
		ob.Attrf("index", "%s@%s", a.Index.Table().Name(), a.Index.Name())
		ob.Expr("expiry", a.Expiration, nil /* columns */)

	case alterTableUnsplitOp:
		a := n.args.(*alterTableUnsplitArgs)
		ob.Attrf("index", "%s@%s", a.Index.Table().Name(), a.Index.Name())

	case alterTableUnsplitAllOp:
		a := n.args.(*alterTableUnsplitAllArgs)
		ob.Attrf("index", "%s@%s", a.Index.Table().Name(), a.Index.Name())

	case alterTableRelocateOp:
		a := n.args.(*alterTableRelocateArgs)
		ob.Attrf("index", "%s@%s", a.Index.Table().Name(), a.Index.Name())

	case recursiveCTEOp:
		a := n.args.(*recursiveCTEArgs)
		if e.ob.flags.Verbose && a.Deduplicate {
			ob.Attrf("deduplicate", "")
		}

	case alterRangeRelocateOp:
		a := n.args.(*alterRangeRelocateArgs)
		ob.Attr("replicas", a.subjectReplicas)
		ob.Expr("to", a.toStoreID, nil /* columns */)
		if a.subjectReplicas != tree.RelocateLease {
			ob.Expr("from", a.fromStoreID, nil /* columns */)
		}

	case callOp:
		a := n.args.(*callArgs)
		ob.Expr("procedure", a.Proc, nil /* columns */)

	case simpleProjectOp,
		serializingProjectOp,
		ordinalityOp,
		max1RowOp,
		hashSetOpOp,
		streamingSetOpOp,
		explainOptOp,
		explainOp,
		showTraceOp,
		createFunctionOp,
		createTableOp,
		createTableAsOp,
		createTriggerOp,
		createViewOp,
		sequenceSelectOp,
		saveTableOp,
		errorIfRowsOp,
		opaqueOp,
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

func (e *emitter) emitTableAndIndex(field string, table cat.Table, index cat.Index, suffix string) {
	partial := ""
	if _, isPartial := index.Predicate(); isPartial {
		partial = " (partial index)"
	}
	e.ob.Attrf(field, "%s@%s%s%s", table.Name(), index.Name(), partial, suffix)
}

func (e *emitter) emitSpans(
	field string, table cat.Table, index cat.Index, scanParams exec.ScanParams,
) {
	e.ob.Attr(field, e.spansStr(table, index, scanParams))
}

func (e *emitter) spansStr(table cat.Table, index cat.Index, scanParams exec.ScanParams) string {
	if scanParams.InvertedConstraint == nil && scanParams.IndexConstraint == nil {
		// HardLimit can be -1 to signal unknown limit (for gists).
		if scanParams.HardLimit != 0 {
			return "LIMITED SCAN"
		}
		if scanParams.SoftLimit > 0 {
			return "FULL SCAN (SOFT LIMIT)"
		}
		return "FULL SCAN"
	}

	// If we are only showing the shape, show a non-specific number of spans.
	if e.ob.flags.OnlyShape {
		return "1+ spans"
	}

	// In verbose mode show the physical spans, unless the table is virtual.
	if e.ob.flags.Verbose && !e.ob.flags.HideValues && !e.ob.flags.RedactValues &&
		!table.IsVirtualTable() {
		return e.spanFormatFn(table, index, scanParams)
	}

	// For inverted constraints, just print the number of spans. Inverted key
	// values are generally not user-readable.
	if scanParams.InvertedConstraint != nil {
		n := len(scanParams.InvertedConstraint)
		return fmt.Sprintf("%d span%s", n, util.Pluralize(int64(n)))
	}

	// If we must hide or redact values, only show the count.
	if e.ob.flags.HideValues || e.ob.flags.RedactValues {
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

func (e *emitter) emitLockingPolicy(locking opt.Locking) {
	e.emitLockingPolicyWithPrefix("", locking)
}

func (e *emitter) emitLockingPolicyWithPrefix(keyPrefix string, locking opt.Locking) {
	strength := descpb.ToScanLockingStrength(locking.Strength)
	waitPolicy := descpb.ToScanLockingWaitPolicy(locking.WaitPolicy)
	form := locking.Form
	durability := locking.Durability
	if strength != descpb.ScanLockingStrength_FOR_NONE {
		e.ob.Attr(keyPrefix+"locking strength", strength.PrettyString())
	}
	if waitPolicy != descpb.ScanLockingWaitPolicy_BLOCK {
		e.ob.Attr(keyPrefix+"locking wait policy", waitPolicy.PrettyString())
	}
	if form != tree.LockRecord {
		e.ob.Attr(keyPrefix+"locking form", form.String())
	}
	if durability != tree.LockDurabilityBestEffort {
		e.ob.Attr(keyPrefix+"locking durability", durability.String())
	}
}

func (e *emitter) emitTuples(rows tree.ExprContainer, numColumns int) {
	e.ob.Attrf(
		"size", "%d column%s, %d row%s",
		numColumns, util.Pluralize(int64(numColumns)),
		rows.NumRows(), util.Pluralize(int64(rows.NumRows())),
	)
	if e.ob.flags.Verbose {
		const maxLines = 30
		if rows.NumRows()*rows.NumCols() <= maxLines || rows.NumRows() <= 2 {
			// Emit all rows fully when we'll use a handful of lines, or we have
			// at most two rows.
			e.emitTuplesRange(rows, 0 /* rowStartIdx */, rows.NumRows())
		} else {
			// We have at least three rows and need to collapse the output.
			//
			// Always emit the first and the last rows.
			headEndIdx, tailStartIdx := 1, rows.NumRows()-1
			// Split the remaining "line budget" evenly, favoring the "head" a
			// bit, without exceeding the limit.
			availableLines := (maxLines - 2*rows.NumCols()) / rows.NumCols()
			extraHeadLength, extraTailLength := availableLines-availableLines/2, availableLines/2
			headEndIdx += extraHeadLength
			tailStartIdx -= extraTailLength
			if headEndIdx >= tailStartIdx {
				// This should never happen, but just to be safe we'll handle
				// the case when "head" and "tail" combine, and we end up
				// emitting all rows.
				e.emitTuplesRange(rows, 0 /* rowStartIdx */, rows.NumRows())
			} else {
				e.emitTuplesRange(rows, 0 /* rowStartIdx */, headEndIdx)
				e.ob.AddField("...", "")
				e.emitTuplesRange(rows, tailStartIdx, rows.NumRows())
			}
		}
	}
}

// emitTuplesRange emits all tuples in the [rowStartIdx, rowEndIdx) range from
// the given container.
func (e *emitter) emitTuplesRange(rows tree.ExprContainer, rowStartIdx, rowEndIdx int) {
	for i := rowStartIdx; i < rowEndIdx; i++ {
		for j := 0; j < rows.NumCols(); j++ {
			expr := rows.Get(i, j).(tree.TypedExpr)
			e.ob.Expr(fmt.Sprintf("row %d, expr %d", i, j), expr, nil /* varColumns */)
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

func (e *emitter) emitPolicies(
	ob *OutputBuilder, table cat.Table, applied *exec.RLSPoliciesApplied,
) {
	if !ob.flags.ShowPolicyInfo {
		return
	}

	if applied.PoliciesSkippedForRole {
		ob.AddField("policies", "exempt for role")
	} else if applied.Policies.Len() == 0 {
		ob.AddField("policies", "row-level security enabled, no policies applied.")
	} else {
		var sb strings.Builder
		for i := 0; i < table.PolicyCount(tree.PolicyTypePermissive); i++ {
			policy := table.Policy(tree.PolicyTypePermissive, i)
			if applied.Policies.Contains(policy.ID) {
				if sb.Len() > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(policy.Name.Normalize())
			}
		}
		ob.AddField("policies", sb.String())
	}
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
		if len(inputCols) > 0 && len(inputCols[col].Name) > 0 {
			buf.WriteString(inputCols[col].Name)
		} else {
			buf.WriteString("_")
		}
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

func shorten(s string) string {
	if len(s) > 20 {
		return s[:20] + "…"
	}
	return s
}
