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
	"sort"

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
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

var collectTxnStatsSampleRate = settings.RegisterFloatSetting(
	"sql.txn_stats.sample_rate",
	"the probability that a given transaction will collect execution statistics (displayed in the DB Console)",
	0.01,
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

	// regions used only on EXPLAIN ANALYZE to be displayed as top-level stat.
	regions []string
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
	statsCollector sqlstats.StatsCollector,
	p *planner,
	stmtDiagnosticsRecorder *stmtdiagnostics.Registry,
	fingerprint string,
	implicitTxn bool,
	collectTxnExecStats bool,
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

	ih.savePlanForStats =
		statsCollector.ShouldSaveLogicalPlanDesc(fingerprint, implicitTxn, p.SessionData().Database)

	if sp := tracing.SpanFromContext(ctx); sp != nil {
		if sp.IsVerbose() {
			// If verbose tracing was enabled at a higher level, stats
			// collection is enabled so that stats are shown in the traces, but
			// no extra work is needed by the instrumentationHelper.
			ih.collectExecStats = true
			return ctx, false
		}
	} else {
		if util.CrdbTestBuild {
			panic(errors.AssertionFailedf("the context doesn't have a tracing span"))
		}
	}

	ih.collectExecStats = collectTxnExecStats

	if !collectTxnExecStats && ih.savePlanForStats {
		// We don't collect the execution stats for statements in this txn, but
		// this is the first time we see this statement ever, so we'll collect
		// its execution stats anyway (unless the user disabled txn stats
		// collection entirely).
		statsCollectionDisabled := collectTxnStatsSampleRate.Get(&cfg.Settings.SV) == 0
		ih.collectExecStats = !statsCollectionDisabled
	}

	if !ih.collectBundle && ih.withStatementTrace == nil && ih.outputMode == unmodifiedOutput {
		if ih.collectExecStats {
			// If we need to collect stats, create a non-verbose child span. Stats
			// will be added as structured metadata and processed in Finish.
			ih.origCtx = ctx
			newCtx, ih.sp = tracing.EnsureChildSpan(ctx, cfg.AmbientCtx.Tracer, "traced statement", tracing.WithForceRealSpan())
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
	statsCollector sqlstats.StatsCollector,
	txnStats *execstats.QueryLevelStats,
	collectTxnExecStats bool,
	p *planner,
	ast tree.Statement,
	stmtRawSQL string,
	res RestrictedCommandResult,
	retErr error,
) error {
	ctx := ih.origCtx
	if ih.sp == nil {
		return retErr
	}
	ih.sp.Finish()

	// Record the statement information that we've collected.
	// Note that in case of implicit transactions, the trace contains the auto-commit too.
	trace := ih.sp.GetRecording()

	if ih.withStatementTrace != nil {
		ih.withStatementTrace(trace, stmtRawSQL)
	}

	if ih.traceMetadata != nil && ih.explainPlan != nil {
		ih.regions = ih.traceMetadata.annotateExplain(
			ih.explainPlan,
			trace,
			cfg.TestingKnobs.DeterministicExplain,
			p,
		)
	}

	// Get the query-level stats.
	var flowsMetadata []*execstats.FlowsMetadata
	for _, flowInfo := range p.curPlan.distSQLFlowInfos {
		flowsMetadata = append(flowsMetadata, flowInfo.flowsMetadata)
	}
	queryLevelStats, err := execstats.GetQueryLevelStats(trace, cfg.TestingKnobs.DeterministicExplain, flowsMetadata)
	if err != nil {
		const msg = "error getting query level stats for statement: %s: %+v"
		if util.CrdbTestBuild {
			panic(fmt.Sprintf(msg, ih.fingerprint, err))
		}
		log.VInfof(ctx, 1, msg, ih.fingerprint, err)
	} else {
		stmtStatsKey := roachpb.StatementStatisticsKey{
			Query:       ih.fingerprint,
			ImplicitTxn: ih.implicitTxn,
			Database:    p.SessionData().Database,
			Failed:      retErr != nil,
		}
		err = statsCollector.RecordStatementExecStats(stmtStatsKey, queryLevelStats)
		if err != nil {
			if log.V(2 /* level */) {
				log.Warningf(ctx, "unable to record statement exec stats: %s", err)
			}
		}
		if collectTxnExecStats || ih.implicitTxn {
			txnStats.Accumulate(queryLevelStats)
		}
	}

	var bundle diagnosticsBundle
	if ih.collectBundle {
		ie := p.extendedEvalCtx.InternalExecutor.(*InternalExecutor)
		placeholders := p.extendedEvalCtx.Placeholders
		ob := ih.buildExplainAnalyzePlan(
			explain.Flags{Verbose: true, ShowTypes: true},
			statsCollector.PhaseTimes(),
			&queryLevelStats,
		)
		bundle = buildStatementBundle(
			ih.origCtx, cfg.DB, ie, &p.curPlan, ob.BuildString(), trace, placeholders,
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
		var flows []flowInfo
		if ih.outputMode == explainAnalyzeDistSQLOutput {
			flows = p.curPlan.distSQLFlowInfos
		}
		return ih.setExplainAnalyzeResult(ctx, res, statsCollector.PhaseTimes(), &queryLevelStats, flows, trace)

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
// to generate diagrams - when shouldSaveDiagrams() returns true - and to get
// query level stats when sampling statements).
func (ih *instrumentationHelper) ShouldSaveFlows() bool {
	return ih.collectBundle || ih.outputMode == explainAnalyzeDistSQLOutput || ih.collectExecStats
}

// shouldSaveDiagrams returns whether saveFlows() function should also be saving
// diagrams in flowInfo objects.
func (ih *instrumentationHelper) shouldSaveDiagrams() bool {
	return ih.collectBundle || ih.outputMode != unmodifiedOutput
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

// ShouldSaveMemo returns true if we should save the memo and catalog in planTop.
func (ih *instrumentationHelper) ShouldSaveMemo() bool {
	return ih.ShouldBuildExplainPlan()
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

// buildExplainAnalyzePlan creates an explain.OutputBuilder and populates it
// with the EXPLAIN ANALYZE plan. BuildString/BuildStringRows can be used on the
// result.
func (ih *instrumentationHelper) buildExplainAnalyzePlan(
	flags explain.Flags, phaseTimes *sessionphase.Times, queryStats *execstats.QueryLevelStats,
) *explain.OutputBuilder {
	ob := explain.NewOutputBuilder(flags)
	if ih.explainPlan == nil {
		// Return an empty builder if there is no plan.
		return ob
	}
	ob.AddPlanningTime(phaseTimes.GetPlanningLatency())
	ob.AddExecutionTime(phaseTimes.GetRunLatency())
	ob.AddDistribution(ih.distribution.String())
	ob.AddVectorized(ih.vectorized)

	if queryStats.KVRowsRead != 0 {
		ob.AddKVReadStats(queryStats.KVRowsRead, queryStats.KVBytesRead)
	}
	if queryStats.KVTime != 0 {
		ob.AddKVTime(queryStats.KVTime)
	}
	if queryStats.ContentionTime != 0 {
		ob.AddContentionTime(queryStats.ContentionTime)
	}

	ob.AddMaxMemUsage(queryStats.MaxMemUsage)
	ob.AddNetworkStats(queryStats.NetworkMessages, queryStats.NetworkBytesSent)
	ob.AddMaxDiskUsage(queryStats.MaxDiskUsage)

	if len(ih.regions) > 0 {
		ob.AddRegionsStats(ih.regions)
	}

	if err := emitExplain(ob, ih.evalCtx, ih.codec, ih.explainPlan); err != nil {
		ob.AddTopLevelField("error emitting plan", fmt.Sprint(err))
	}
	return ob
}

// setExplainAnalyzeResult sets the result for an EXPLAIN ANALYZE or EXPLAIN
// ANALYZE (DISTSQL) statement (in the former case, distSQLFlowInfos and trace
// are nil).
// Returns an error only if there was an error adding rows to the result.
func (ih *instrumentationHelper) setExplainAnalyzeResult(
	ctx context.Context,
	res RestrictedCommandResult,
	phaseTimes *sessionphase.Times,
	queryLevelStats *execstats.QueryLevelStats,
	distSQLFlowInfos []flowInfo,
	trace tracing.Recording,
) (commErr error) {
	res.ResetStmtType(&tree.ExplainAnalyze{})
	res.SetColumns(ctx, colinfo.ExplainPlanColumns)

	if res.Err() != nil {
		// Can't add rows if there was an error.
		return nil //nolint:returnerrcheck
	}

	ob := ih.buildExplainAnalyzePlan(ih.explainFlags, phaseTimes, queryLevelStats)
	rows := ob.BuildStringRows()
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
// It returns a list of all regions on which any of the statements
// where executed on.
func (m execNodeTraceMetadata) annotateExplain(
	plan *explain.Plan, spans []tracingpb.RecordedSpan, makeDeterministic bool, p *planner,
) []string {
	statsMap := execinfrapb.ExtractStatsFromSpans(spans, makeDeterministic)
	var allRegions []string

	// Retrieve which region each node is on.
	regionsInfo := make(map[int64]string)
	descriptors, _ := getAllNodeDescriptors(p)
	for _, descriptor := range descriptors {
		for _, tier := range descriptor.Locality.Tiers {
			if tier.Key == "region" {
				regionsInfo[int64(descriptor.NodeID)] = tier.Value
			}
		}
	}

	var walk func(n *explain.Node)
	walk = func(n *explain.Node) {
		wrapped := n.WrappedNode()
		if components, ok := m[wrapped]; ok {
			var nodeStats exec.ExecutionStats

			incomplete := false
			var nodes util.FastIntSet
			regionsMap := make(map[string]struct{})
			for _, c := range components {
				if c.Type == execinfrapb.ComponentID_PROCESSOR {
					nodes.Add(int(c.SQLInstanceID))
					regionsMap[regionsInfo[int64(c.SQLInstanceID)]] = struct{}{}
				}
				stats := statsMap[c]
				if stats == nil {
					incomplete = true
					break
				}
				nodeStats.RowCount.MaybeAdd(stats.Output.NumTuples)
				nodeStats.KVTime.MaybeAdd(stats.KV.KVTime)
				nodeStats.KVContentionTime.MaybeAdd(stats.KV.ContentionTime)
				nodeStats.KVBytesRead.MaybeAdd(stats.KV.BytesRead)
				nodeStats.KVRowsRead.MaybeAdd(stats.KV.TuplesRead)
				nodeStats.VectorizedBatchCount.MaybeAdd(stats.Output.NumBatches)
			}
			// If we didn't get statistics for all processors, we don't show the
			// incomplete results. In the future, we may consider an incomplete flag
			// if we want to show them with a warning.
			if !incomplete {
				for i, ok := nodes.Next(0); ok; i, ok = nodes.Next(i + 1) {
					nodeStats.Nodes = append(nodeStats.Nodes, fmt.Sprintf("n%d", i))
				}
				regions := make([]string, 0, len(regionsMap))
				for r := range regionsMap {
					// Add only if the region is not an empty string (it will be an
					// empty string if the region is not setup).
					if r != "" {
						regions = append(regions, r)
					}
				}
				sort.Strings(regions)
				nodeStats.Regions = regions
				allRegions = util.CombineUniqueString(allRegions, regions)
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

	return allRegions
}
