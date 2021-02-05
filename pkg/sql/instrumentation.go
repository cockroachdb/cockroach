// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

var collectTxnStatsSampleRate = settings.RegisterFloatSetting(
	"sql.txn_stats.sample_rate",
	"the probability that a given transaction will collect execution statistics (displayed in the DB Console)",
	0,
	func(f float64) error {
		if f < 0 || f > 1 {
			return errors.New("value must be between 0 and 1 inclusive")
		}
		return nil
	},
)

// instrumentationHelper encapsulates the logic around extracting information
// about the execution of a statement, like bundles and traces. Typical usage:
//
//  - SetOutputMode() can be used as necessary if we are running an EXPLAIN
//    ANALYZE variant.
//
//  - Setup() is called before query execution.
//
//  - SetDiscardRows(), ShouldDiscardRows(), ShouldSaveFlows(),
//    ShouldBuildExplainPlan(), RecordExplainPlan(), RecordPlanInfo(),
//    PlanForStats() can be called at any point during execution.
//
//  - Finish() is called after query execution.
//
type instrumentationHelper struct {
	outputMode outputMode
	// explainFlags is used when outputMode is explainAnalyzePlanOutput or
	// explainAnalyzeDistSQLOutput.
	explainFlags explain.Flags

	// Query fingerprint (anonymized statement).
	fingerprint string
	implicitTxn bool
	codec       keys.SQLCodec

	// -- The following fields are initialized by Setup() --

	// collectBundle is set when we are collecting a diagnostics bundle for a
	// statement; it triggers saving of extra information like the plan string.
	collectBundle bool

	// collectExecStats is set when we are collecting execution statistics for a
	// statement.
	collectExecStats bool

	// discardRows is set if we want to discard any results rather than sending
	// them back to the client. Used for testing/benchmarking. Note that the
	// resulting schema or the plan are not affected.
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	diagRequestID               stmtdiagnostics.RequestID
	finishCollectionDiagnostics func()
	withStatementTrace          func(trace tracing.Recording, stmt string)

	sp      *tracing.Span
	origCtx context.Context
	evalCtx *tree.EvalContext

	// If savePlanForStats is true, the explainPlan will be collected and returned
	// via PlanForStats().
	savePlanForStats bool

	explainPlan  *explain.Plan
	distribution physicalplan.PlanDistribution
	vectorized   bool

	traceMetadata execNodeTraceMetadata
}

// outputMode indicates how the statement output needs to be populated (for
// EXPLAIN ANALYZE variants).
type outputMode int8

const (
	unmodifiedOutput outputMode = iota
	explainAnalyzeDebugOutput
	explainAnalyzePlanOutput
	explainAnalyzeDistSQLOutput
)

// SetOutputMode can be called before Setup, if we are running an EXPLAIN
// ANALYZE variant.
func (ih *instrumentationHelper) SetOutputMode(outputMode outputMode, explainFlags explain.Flags) {
	ih.outputMode = outputMode
	ih.explainFlags = explainFlags
}

// Setup potentially enables verbose tracing for the statement, depending on
// output mode or statement diagnostic activation requests. Finish() must be
// called after the statement finishes execution (unless needFinish=false, in
// which case Finish() is a no-op).
func (ih *instrumentationHelper) Setup(
	ctx context.Context,
	cfg *ExecutorConfig,
	appStats *appStats,
	p *planner,
	stmtDiagnosticsRecorder *stmtdiagnostics.Registry,
	fingerprint string,
	implicitTxn bool,
	collectExecStats bool,
) (newCtx context.Context, needFinish bool) {
	ih.fingerprint = fingerprint
	ih.implicitTxn = implicitTxn
	ih.codec = cfg.Codec

	switch ih.outputMode {
	case explainAnalyzeDebugOutput:
		ih.collectBundle = true
		// EXPLAIN ANALYZE (DEBUG) does not return the rows for the given query;
		// instead it returns some text which includes a URL.
		// TODO(radu): maybe capture some of the rows and include them in the
		// bundle.
		ih.discardRows = true

	case explainAnalyzePlanOutput, explainAnalyzeDistSQLOutput:
		ih.discardRows = true

	default:
		ih.collectBundle, ih.diagRequestID, ih.finishCollectionDiagnostics =
			stmtDiagnosticsRecorder.ShouldCollectDiagnostics(ctx, fingerprint)
	}

	ih.withStatementTrace = cfg.TestingKnobs.WithStatementTrace

	ih.savePlanForStats = appStats.shouldSaveLogicalPlanDescription(fingerprint, implicitTxn)

	if sp := tracing.SpanFromContext(ctx); sp != nil && sp.IsVerbose() {
		// If verbose tracing was enabled at a higher level, stats collection is
		// enabled so that stats are shown in the traces, but no extra work is
		// needed by the instrumentationHelper.
		ih.collectExecStats = true
		return ctx, false
	}

	ih.collectExecStats = collectExecStats

	if !ih.collectBundle && ih.withStatementTrace == nil && ih.outputMode == unmodifiedOutput {
		if ih.collectExecStats {
			// If we need to collect stats, create a non-verbose child span. Stats
			// will be added as structured metadata and processed in Finish.
			ih.origCtx = ctx
			newCtx, ih.sp = tracing.EnsureChildSpan(ctx, cfg.AmbientCtx.Tracer, "traced statement")
			return newCtx, true
		}
		return ctx, false
	}

	ih.collectExecStats = true
	ih.traceMetadata = make(execNodeTraceMetadata)
	ih.origCtx = ctx
	ih.evalCtx = p.EvalContext()
	newCtx, ih.sp = tracing.StartVerboseTrace(ctx, cfg.AmbientCtx.Tracer, "traced statement")
	return newCtx, true
}

func (ih *instrumentationHelper) Finish(
	cfg *ExecutorConfig,
	appStats *appStats,
	txnStats *execstats.QueryLevelStats,
	statsCollector *sqlStatsCollector,
	p *planner,
	ast tree.Statement,
	stmtRawSQL string,
	res RestrictedCommandResult,
	retErr error,
) error {
	if ih.sp == nil {
		return retErr
	}

	// Record the statement information that we've collected.
	// Note that in case of implicit transactions, the trace contains the auto-commit too.
	ih.sp.Finish()
	ctx := ih.origCtx

	trace := ih.sp.GetRecording()

	if ih.withStatementTrace != nil {
		ih.withStatementTrace(trace, stmtRawSQL)
	}

	if ih.traceMetadata != nil && ih.explainPlan != nil {
		ih.traceMetadata.annotateExplain(ih.explainPlan, trace, cfg.TestingKnobs.DeterministicExplainAnalyze)
	}

	// TODO(radu): this should be unified with other stmt stats accesses.
	stmtStats, _ := appStats.getStatsForStmt(ih.fingerprint, ih.implicitTxn, retErr, false)
	if stmtStats != nil {
		var flowMetadata []*execstats.FlowMetadata
		for _, flowInfo := range p.curPlan.distSQLFlowInfos {
			flowMetadata = append(flowMetadata, flowInfo.flowMetadata)
		}
		queryLevelStats, err := execstats.GetQueryLevelStats(trace, cfg.TestingKnobs.DeterministicExplainAnalyze, flowMetadata)
		if err != nil {
			log.VInfof(ctx, 1, "error getting query level stats for statement %s: %+v", ast, err)
		} else {
			stmtStats.mu.Lock()
			stmtStats.mu.data.ExecStatCollectionCount++
			// Record trace-related statistics.
			stmtStats.mu.data.BytesSentOverNetwork.Record(
				stmtStats.mu.data.ExecStatCollectionCount, float64(queryLevelStats.NetworkBytesSent),
			)
			stmtStats.mu.data.MaxMemUsage.Record(
				stmtStats.mu.data.ExecStatCollectionCount, float64(queryLevelStats.MaxMemUsage),
			)
			stmtStats.mu.data.ContentionTime.Record(
				stmtStats.mu.data.ExecStatCollectionCount, queryLevelStats.ContentionTime.Seconds(),
			)
			stmtStats.mu.Unlock()
		}
		txnStats.Accumulate(queryLevelStats)
	}

	var bundle diagnosticsBundle
	if ih.collectBundle {
		ie := p.extendedEvalCtx.InternalExecutor.(*InternalExecutor)
		placeholders := p.extendedEvalCtx.Placeholders
		planStr := ih.planStringForBundle(&statsCollector.phaseTimes)
		bundle = buildStatementBundle(
			ih.origCtx, cfg.DB, ie, &p.curPlan, planStr, trace, placeholders,
		)
		bundle.insert(ctx, ih.fingerprint, ast, cfg.StmtDiagnosticsRecorder, ih.diagRequestID)
		if ih.finishCollectionDiagnostics != nil {
			ih.finishCollectionDiagnostics()
			telemetry.Inc(sqltelemetry.StatementDiagnosticsCollectedCounter)
		}
	}

	// If there was a communication error already, no point in setting any
	// results.
	if retErr != nil {
		return retErr
	}

	switch ih.outputMode {
	case explainAnalyzeDebugOutput:
		return setExplainBundleResult(ctx, res, bundle, cfg)

	case explainAnalyzePlanOutput, explainAnalyzeDistSQLOutput:
		phaseTimes := &statsCollector.phaseTimes
		var flows []flowInfo
		if ih.outputMode == explainAnalyzeDistSQLOutput {
			flows = p.curPlan.distSQLFlowInfos
		}
		return ih.setExplainAnalyzeResult(ctx, res, phaseTimes, flows, trace)

	default:
		return nil
	}
}

// SetDiscardRows should be called when we want to discard rows for a
// non-ANALYZE statement (via EXECUTE .. DISCARD ROWS).
func (ih *instrumentationHelper) SetDiscardRows() {
	ih.discardRows = true
}

// ShouldDiscardRows returns true if this is an EXPLAIN ANALYZE variant or
// SetDiscardRows() was called.
func (ih *instrumentationHelper) ShouldDiscardRows() bool {
	return ih.discardRows
}

// ShouldSaveFlows is true if we should save the flow specifications (to be able
// to generate diagrams).
func (ih *instrumentationHelper) ShouldSaveFlows() bool {
	return ih.collectBundle || ih.outputMode == explainAnalyzeDistSQLOutput || ih.collectExecStats
}

// ShouldUseJobForCreateStats indicates if we should run CREATE STATISTICS as a
// job (normally true). It is false if we are running a statement under
// EXPLAIN ANALYZE, in which case we want to run the CREATE STATISTICS plan
// directly.
func (ih *instrumentationHelper) ShouldUseJobForCreateStats() bool {
	return ih.outputMode == unmodifiedOutput
}

// ShouldBuildExplainPlan returns true if we should build an explain plan and
// call RecordExplainPlan.
func (ih *instrumentationHelper) ShouldBuildExplainPlan() bool {
	return ih.collectBundle || ih.savePlanForStats || ih.outputMode == explainAnalyzePlanOutput ||
		ih.outputMode == explainAnalyzeDistSQLOutput
}

// ShouldCollectExecStats returns true if we should collect statement execution
// statistics.
func (ih *instrumentationHelper) ShouldCollectExecStats() bool {
	return ih.collectExecStats
}

// RecordExplainPlan records the explain.Plan for this query.
func (ih *instrumentationHelper) RecordExplainPlan(explainPlan *explain.Plan) {
	ih.explainPlan = explainPlan
}

// RecordPlanInfo records top-level information about the plan.
func (ih *instrumentationHelper) RecordPlanInfo(
	distribution physicalplan.PlanDistribution, vectorized bool,
) {
	ih.distribution = distribution
	ih.vectorized = vectorized
}

// PlanForStats returns the plan as an ExplainTreePlanNode tree, if it was
// collected (nil otherwise). It should be called after RecordExplainPlan() and
// RecordPlanInfo().
func (ih *instrumentationHelper) PlanForStats(ctx context.Context) *roachpb.ExplainTreePlanNode {
	if ih.explainPlan == nil {
		return nil
	}

	ob := explain.NewOutputBuilder(explain.Flags{
		HideValues: true,
	})
	ob.AddDistribution(ih.distribution.String())
	ob.AddVectorized(ih.vectorized)
	if err := emitExplain(ob, ih.evalCtx, ih.codec, ih.explainPlan); err != nil {
		log.Warningf(ctx, "unable to emit explain plan tree: %v", err)
		return nil
	}
	return ob.BuildProtoTree()
}

// planStringForBundle generates the plan tree as a string; used internally for bundles.
func (ih *instrumentationHelper) planStringForBundle(phaseTimes *phaseTimes) string {
	if ih.explainPlan == nil {
		return ""
	}
	ob := explain.NewOutputBuilder(explain.Flags{
		Verbose:   true,
		ShowTypes: true,
	})
	ob.AddPlanningTime(phaseTimes.getPlanningLatency())
	ob.AddExecutionTime(phaseTimes.getRunLatency())
	ob.AddDistribution(ih.distribution.String())
	ob.AddVectorized(ih.vectorized)
	if err := emitExplain(ob, ih.evalCtx, ih.codec, ih.explainPlan); err != nil {
		return fmt.Sprintf("error emitting plan: %v", err)
	}
	return ob.BuildString()
}

// planRowsForExplainAnalyze generates the plan tree as a list of strings (one
// for each line).
// Used in explainAnalyzePlanOutput and explainAnalyzeDistSQLOutput modes.
func (ih *instrumentationHelper) planRowsForExplainAnalyze(phaseTimes *phaseTimes) []string {
	if ih.explainPlan == nil {
		return nil
	}
	ob := explain.NewOutputBuilder(ih.explainFlags)
	ob.AddPlanningTime(phaseTimes.getPlanningLatency())
	ob.AddExecutionTime(phaseTimes.getRunLatency())
	ob.AddDistribution(ih.distribution.String())
	ob.AddVectorized(ih.vectorized)
	if err := emitExplain(ob, ih.evalCtx, ih.codec, ih.explainPlan); err != nil {
		return []string{fmt.Sprintf("error emitting plan: %v", err)}
	}
	return ob.BuildStringRows()
}

// setExplainAnalyzeResult sets the result for an EXPLAIN ANALYZE or EXPLAIN
// ANALYZE (DISTSQL) statement (in the former case, distSQLFlowInfos and trace
// are nil).
// Returns an error only if there was an error adding rows to the result.
func (ih *instrumentationHelper) setExplainAnalyzeResult(
	ctx context.Context,
	res RestrictedCommandResult,
	phaseTimes *phaseTimes,
	distSQLFlowInfos []flowInfo,
	trace tracing.Recording,
) (commErr error) {
	res.ResetStmtType(&tree.ExplainAnalyze{})
	res.SetColumns(ctx, colinfo.ExplainPlanColumns)

	if res.Err() != nil {
		// Can't add rows if there was an error.
		return nil //nolint:returnerrcheck
	}

	rows := ih.planRowsForExplainAnalyze(phaseTimes)
	if distSQLFlowInfos != nil {
		rows = append(rows, "")
		for i, d := range distSQLFlowInfos {
			var buf bytes.Buffer
			if len(distSQLFlowInfos) > 1 {
				fmt.Fprintf(&buf, "Diagram %d (%s): ", i+1, d.typ)
			} else {
				buf.WriteString("Diagram: ")
			}
			d.diagram.AddSpans(trace)
			_, url, err := d.diagram.ToURL()
			if err != nil {
				buf.WriteString(err.Error())
			} else {
				buf.WriteString(url.String())
			}
			rows = append(rows, buf.String())
		}
	}
	rows = append(rows, "")
	rows = append(rows, "WARNING: this statement is experimental!")
	for _, row := range rows {
		if err := res.AddRow(ctx, tree.Datums{tree.NewDString(row)}); err != nil {
			return err
		}
	}
	return nil
}

// execNodeTraceMetadata associates exec.Nodes with metadata for corresponding
// execution components.
// Currently, we only store info about processors. A node can correspond to
// multiple processors if the plan is distributed.
//
// TODO(radu): we perform similar processing of execution traces in various
// parts of the code. Come up with some common infrastructure that makes this
// easier.
type execNodeTraceMetadata map[exec.Node]execComponents

type execComponents []execinfrapb.ComponentID

// associateNodeWithComponents is called during planning, as processors are
// planned for an execution operator.
func (m execNodeTraceMetadata) associateNodeWithComponents(
	node exec.Node, components execComponents,
) {
	m[node] = components
}

// annotateExplain aggregates the statistics in the trace and annotates
// explain.Nodes with execution stats.
func (m execNodeTraceMetadata) annotateExplain(
	plan *explain.Plan, spans []tracingpb.RecordedSpan, makeDeterministic bool,
) {
	statsMap := execinfrapb.ExtractStatsFromSpans(spans, makeDeterministic)

	var walk func(n *explain.Node)
	walk = func(n *explain.Node) {
		wrapped := n.WrappedNode()
		if components, ok := m[wrapped]; ok {
			var nodeStats exec.ExecutionStats

			incomplete := false
			for i := range components {
				stats := statsMap[components[i]]
				if stats == nil {
					incomplete = true
					break
				}
				nodeStats.RowCount.MaybeAdd(stats.Output.NumTuples)
				nodeStats.KVBytesRead.MaybeAdd(stats.KV.BytesRead)
				nodeStats.KVRowsRead.MaybeAdd(stats.KV.TuplesRead)
			}
			// If we didn't get statistics for all processors, we don't show the
			// incomplete results. In the future, we may consider an incomplete flag
			// if we want to show them with a warning.
			if !incomplete {
				n.Annotate(exec.ExecutionStatsID, &nodeStats)
			}
		}

		for i := 0; i < n.ChildCount(); i++ {
			walk(n.Child(i))
		}
	}

	walk(plan.Root)
	for i := range plan.Subqueries {
		walk(plan.Subqueries[i].Root.(*explain.Node))
	}
	for i := range plan.Checks {
		walk(plan.Checks[i])
	}
}
