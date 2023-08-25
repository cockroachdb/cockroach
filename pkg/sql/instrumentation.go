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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/execstats"
	"github.com/cockroachdb/cockroach/pkg/sql/idxrecommendations"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec/explain"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/indexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/fsm"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

var collectTxnStatsSampleRate = settings.RegisterFloatSetting(
	settings.TenantWritable,
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
//   - SetOutputMode() can be used as necessary if we are running an EXPLAIN
//     ANALYZE variant.
//
//   - Setup() is called before query execution.
//
//   - SetDiscardRows(), ShouldDiscardRows(), ShouldSaveFlows(),
//     ShouldBuildExplainPlan(), RecordExplainPlan(), RecordPlanInfo(),
//     PlanForStats() can be called at any point during execution.
//
//   - Finish() is called after query execution.
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

	// isTenant is set when the query is being executed on behalf of a tenant.
	isTenant bool

	// discardRows is set if we want to discard any results rather than sending
	// them back to the client. Used for testing/benchmarking. Note that the
	// resulting schema or the plan are not affected.
	// See EXECUTE .. DISCARD ROWS.
	discardRows bool

	diagRequestID           stmtdiagnostics.RequestID
	diagRequest             stmtdiagnostics.Request
	stmtDiagnosticsRecorder *stmtdiagnostics.Registry
	withStatementTrace      func(trace tracingpb.Recording, stmt string)

	// sp is always populated by the instrumentationHelper Setup method, except in
	// the scenario where we do not need tracing information. This scenario occurs
	// with the confluence of:
	// - not collecting a bundle (collectBundle is false)
	// - withStatementTrace is nil (only populated by testing knobs)
	// - outputMode is unmodifiedOutput (i.e. outputMode not specified)
	// - not collecting execution statistics (collectExecStats is false)
	// TODO(yuzefovich): refactor statement span creation #85820
	sp *tracing.Span

	// shouldFinishSpan determines whether sp needs to be finished in
	// instrumentationHelper.Finish.
	shouldFinishSpan bool
	origCtx          context.Context
	evalCtx          *eval.Context

	inFlightTraceCollector

	queryLevelStatsWithErr *execstats.QueryLevelStatsWithErr

	// If savePlanForStats is true and the explainPlan was collected, the
	// serialized version of the plan will be returned via PlanForStats().
	savePlanForStats bool

	explainPlan  *explain.Plan
	distribution physicalplan.PlanDistribution
	vectorized   bool

	traceMetadata execNodeTraceMetadata

	// regions used only on EXPLAIN ANALYZE to be displayed as top-level stat.
	regions []string

	// planGist is a compressed version of plan that can be converted (lossily)
	// back into a logical plan or be used to get a plan hash.
	planGist explain.PlanGist

	// costEstimate is the cost of the query as estimated by the optimizer.
	costEstimate float64

	// indexRecs contains index recommendations for the planned statement. It
	// will only be populated if the statement is an EXPLAIN statement, or if
	// recommendations are requested for the statement for populating the
	// statement_statistics table.
	indexRecs []indexrec.Rec

	// maxFullScanRows is the maximum number of rows scanned by a full scan, as
	// estimated by the optimizer.
	maxFullScanRows float64

	// totalScanRows is the total number of rows read by all scans in the query,
	// as estimated by the optimizer.
	totalScanRows float64

	// totalScanRowsWithoutForecasts is the total number of rows read by all scans
	// in the query, as estimated by the optimizer without using forecasts. (If
	// forecasts were not used, this should be the same as totalScanRows.)
	totalScanRowsWithoutForecasts float64

	// outputRows is the number of rows output by the query, as estimated by the
	// optimizer.
	outputRows float64

	// statsAvailable is true if table statistics were available to the optimizer
	// when planning the query.
	statsAvailable bool

	// nanosSinceStatsCollected is the maximum number of nanoseconds that have
	// passed since stats were collected on any table scanned by this query.
	nanosSinceStatsCollected time.Duration

	// nanosSinceStatsForecasted is the greatest quantity of nanoseconds that have
	// passed since the forecast time (or until the forecast time, if it is in the
	// future, in which case it will be negative) for any table with forecasted
	// stats scanned by this query.
	nanosSinceStatsForecasted time.Duration

	// joinTypeCounts records the number of times each type of logical join was
	// used in the query.
	joinTypeCounts map[descpb.JoinType]int

	// joinAlgorithmCounts records the number of times each type of join algorithm
	// was used in the query.
	joinAlgorithmCounts map[exec.JoinAlgorithm]int

	// scanCounts records the number of times scans were used in the query.
	scanCounts [exec.NumScanCountTypes]int
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

// GetQueryLevelStats gets the QueryLevelStats if they are available.
// The query level stats are only available if tracing is enabled.
func (ih *instrumentationHelper) GetQueryLevelStats() (stats *execstats.QueryLevelStats, ok bool) {
	statsWithErr := ih.queryLevelStatsWithErr

	if statsWithErr == nil || statsWithErr.Err != nil {
		return nil, false
	}

	return &statsWithErr.Stats, true
}

// Tracing returns the current value of the instrumentation helper's span,
// along with a boolean that determines whether the span is populated.
func (ih *instrumentationHelper) Tracing() (sp *tracing.Span, ok bool) {
	if ih.sp != nil {
		return ih.sp, true
	}
	return nil, false
}

// SetOutputMode can be called before Setup, if we are running an EXPLAIN
// ANALYZE variant.
func (ih *instrumentationHelper) SetOutputMode(outputMode outputMode, explainFlags explain.Flags) {
	ih.outputMode = outputMode
	ih.explainFlags = explainFlags
}

type inFlightTraceCollector struct {
	// cancel is nil if the collector goroutine is not started.
	cancel context.CancelFunc
	// waitCh will be closed by the collector goroutine when it exits. It serves
	// as a synchronization mechanism for accessing trace and errors fields.
	waitCh chan struct{}
	// trace contains the latest recording that the collector goroutine polled.
	trace []traceFromSQLInstance
	// errors accumulates all errors that the collector ran into throughout its
	// lifecycle.
	errors []error
	// timeoutTrace, if set, contains the trace recording obtained by the main
	// goroutine in case the query execution was canceled due to a statement
	// timeout.
	timeoutTrace []traceFromSQLInstance
}

type traceFromSQLInstance struct {
	nodeID int64
	trace  string
	jaeger string
}

const inFlightTraceOpName = "bundle-in-flight-trace"

// pollTrace queries the crdb_internal.cluster_inflight_traces virtual table to
// retrieve the trace for the provided traceID.
func pollInFlightTrace(
	ctx context.Context, ie sqlutil.InternalExecutor, traceID tracingpb.TraceID,
) ([]traceFromSQLInstance, error) {
	it, err := ie.QueryIterator(
		ctx, inFlightTraceOpName, nil, /* txn */
		"SELECT node_id, trace_str, jaeger_json FROM crdb_internal.cluster_inflight_traces WHERE trace_id = $1",
		traceID,
	)
	var trace []traceFromSQLInstance
	if err == nil {
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			trace = append(trace, traceFromSQLInstance{
				nodeID: int64(tree.MustBeDInt(row[0])),
				trace:  string(tree.MustBeDString(row[1])),
				jaeger: string(tree.MustBeDString(row[2])),
			})
		}
	}
	return trace, err
}

func (c inFlightTraceCollector) finish() {
	if c.cancel == nil {
		// The in-flight trace collector goroutine wasn't started.
		return
	}
	// Cancel the collector goroutine and block until it exits.
	c.cancel()
	<-c.waitCh
}

var inFlightTraceCollectorPollInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.stmt_diagnostics.in_flight_trace_collector.poll_interval",
	"determines the interval between polling done by the in-flight trace "+
		"collector for the statement bundle, set to zero to disable",
	0,
	settings.NonNegativeDuration,
)

var timeoutTraceCollectionEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"sql.stmt_diagnostics.timeout_trace_collection.enabled",
	"determines whether the in-flight trace collection is performed when building "+
		"the statement bundle if the timeout is detected",
	true,
)

// startInFlightTraceCollector starts another goroutine that will periodically
// query the crdb_internal virtual table to obtain the in-flight trace for the
// span of the instrumentationHelper. It should only be called when we're
// collecting a bundle and inFlightTraceCollector.finish must be called when
// building the bundle.
func (ih *instrumentationHelper) startInFlightTraceCollector(
	ctx context.Context, ie sqlutil.InternalExecutor, pollInterval time.Duration,
) {
	traceID := ih.sp.TraceID()
	c := &ih.inFlightTraceCollector
	ctx, c.cancel = context.WithCancel(ctx)
	c.waitCh = make(chan struct{})
	go func(ctx context.Context) {
		defer close(c.waitCh)
		// Derive a detached tracing span that won't pollute the recording we're
		// collecting for the bundle.
		var sp *tracing.Span
		ctx, sp = ih.sp.Tracer().StartSpanCtx(ctx, inFlightTraceOpName, tracing.WithDetachedRecording())
		defer sp.Finish()

		timer := time.NewTimer(pollInterval)
		defer timer.Stop()

		for {
			// Now sleep for the duration of the poll interval (or until we're
			// canceled).
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				timer.Reset(pollInterval)
			}

			trace, err := pollInFlightTrace(ctx, ie, traceID)
			if err == nil {
				c.trace = trace
			} else if ctx.Err() == nil {
				// When the context is canceled, we expect to receive an error.
				// Note that the context cancellation occurs in the "happy" case
				// too when the execution of the traced query finished, and
				// we're building the bundle, so we're ignoring such an error.
				c.errors = append(c.errors, err)
			}
		}
	}(ctx)
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
	ih.origCtx = ctx
	ih.isTenant = multitenant.TenantRUEstimateEnabled.Get(cfg.SV()) && cfg.DistSQLSrv != nil &&
		cfg.DistSQLSrv.TenantCostController != nil

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
		ih.collectBundle, ih.diagRequestID, ih.diagRequest =
			stmtDiagnosticsRecorder.ShouldCollectDiagnostics(ctx, fingerprint)
	}

	ih.stmtDiagnosticsRecorder = stmtDiagnosticsRecorder
	ih.withStatementTrace = cfg.TestingKnobs.WithStatementTrace

	var previouslySampled bool
	previouslySampled, ih.savePlanForStats = statsCollector.ShouldSample(fingerprint, implicitTxn, p.SessionData().Database)

	defer func() {
		if ih.ShouldBuildExplainPlan() {
			// Populate traceMetadata at the end once we have all properties of
			// the helper setup.
			ih.traceMetadata = make(execNodeTraceMetadata)
		}
		if ih.collectBundle {
			if pollInterval := inFlightTraceCollectorPollInterval.Get(cfg.SV()); pollInterval > 0 {
				ie := p.extendedEvalCtx.ExecCfg.InternalExecutorFactory.NewInternalExecutor(
					p.SessionData(),
				)
				ih.startInFlightTraceCollector(ctx, ie, pollInterval)
			}
		}
	}()

	if sp := tracing.SpanFromContext(ctx); sp != nil {
		if sp.IsVerbose() {
			// If verbose tracing was enabled at a higher level, stats
			// collection is enabled so that stats are shown in the traces, but
			// no extra work is needed by the instrumentationHelper.
			ih.collectExecStats = true
			// We always want to finish the instrumentationHelper in order
			// to record the execution statistics. Note that we capture the
			// span in order to fetch the trace from it, but the span won't be
			// finished.
			ih.sp = sp
			return ctx, true /* needFinish */
		}
	} else {
		if buildutil.CrdbTestBuild {
			panic(errors.AssertionFailedf("the context doesn't have a tracing span"))
		}
	}

	ih.collectExecStats = collectTxnExecStats

	if !collectTxnExecStats && !previouslySampled {
		// We don't collect the execution stats for statements in this txn, but
		// this is the first time we see this statement ever, so we'll collect
		// its execution stats anyway (unless the user disabled txn stats
		// collection entirely).
		statsCollectionDisabled := collectTxnStatsSampleRate.Get(&cfg.Settings.SV) == 0
		ih.collectExecStats = !statsCollectionDisabled
	}

	if !ih.collectBundle && ih.withStatementTrace == nil && ih.outputMode == unmodifiedOutput {
		if ih.collectExecStats {
			// If we need to collect stats, create a child span with structured
			// recording. Stats will be added as structured metadata and processed in
			// Finish.
			newCtx, ih.sp = tracing.EnsureChildSpan(ctx, cfg.AmbientCtx.Tracer, "traced statement",
				tracing.WithRecording(tracingpb.RecordingStructured))
			ih.shouldFinishSpan = true
			return newCtx, true
		}
		return ctx, false
	}

	ih.collectExecStats = true
	ih.evalCtx = p.EvalContext()
	newCtx, ih.sp = tracing.EnsureChildSpan(ctx, cfg.AmbientCtx.Tracer, "traced statement", tracing.WithRecording(tracingpb.RecordingVerbose))
	ih.shouldFinishSpan = true
	return newCtx, true
}

func (ih *instrumentationHelper) Finish(
	cfg *ExecutorConfig,
	statsCollector sqlstats.StatsCollector,
	txnStats *execstats.QueryLevelStats,
	collectExecStats bool,
	p *planner,
	ast tree.Statement,
	stmtRawSQL string,
	res RestrictedCommandResult,
	retPayload fsm.EventPayload,
	retErr error,
) error {
	ctx := ih.origCtx
	if _, ok := ih.Tracing(); !ok {
		return retErr
	}

	if ih.shouldFinishSpan {
		// Make sure that we always finish the tracing spans if we need to. Note
		// that we defer this so that we can collect the timeout trace if
		// necessary when building the bundle.
		defer ih.sp.Finish()
	}

	// Record the statement information that we've collected.
	// Note that in case of implicit transactions, the trace contains the auto-commit too.
	traceID := ih.sp.TraceID()
	trace := ih.sp.GetConfiguredRecording()

	if ih.withStatementTrace != nil {
		ih.withStatementTrace(trace, stmtRawSQL)
	}

	queryLevelStats, ok := ih.GetQueryLevelStats()
	// Accumulate txn stats if no error was encountered while collecting
	// query-level statistics.
	if ok {
		if collectExecStats || ih.implicitTxn {
			txnStats.Accumulate(*queryLevelStats)
		}
	}

	var bundle diagnosticsBundle
	var warnings []string
	if ih.collectBundle {
		ih.inFlightTraceCollector.finish()
		ie := p.extendedEvalCtx.ExecCfg.InternalExecutorFactory.NewInternalExecutor(
			p.SessionData(),
		)
		phaseTimes := statsCollector.PhaseTimes()
		execLatency := phaseTimes.GetServiceLatencyNoOverhead()
		if ih.stmtDiagnosticsRecorder.IsConditionSatisfied(ih.diagRequest, execLatency) {
			placeholders := p.extendedEvalCtx.Placeholders
			ob := ih.emitExplainAnalyzePlanToOutputBuilder(
				explain.Flags{Verbose: true, ShowTypes: true},
				phaseTimes,
				queryLevelStats,
			)
			warnings = ob.GetWarnings()
			var payloadErr error
			if pwe, ok := retPayload.(payloadWithError); ok {
				payloadErr = pwe.errorCause()
			}
			bundleCtx := ctx
			if bundleCtx.Err() != nil {
				// The only two possible errors on the context are the context
				// cancellation or the context deadline being exceeded. The
				// former seems more likely, and the cancellation is most likely
				// to have occurred due to a statement timeout, so we still want
				// to proceed with saving the statement bundle. Thus, we
				// override the canceled context, but first we'll log the error
				// as a warning.
				log.Warningf(
					bundleCtx, "context has an error when saving the bundle, proceeding "+
						"with the background one (with deadline of 10 seconds): %v", bundleCtx.Err(),
				)
				// We want to be conservative, so we add a deadline of 10
				// seconds on top of the background context.
				var cancel context.CancelFunc
				bundleCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second) // nolint:context
				defer cancel()
				if timeoutTraceCollectionEnabled.Get(cfg.SV()) {
					if tr, err := pollInFlightTrace(bundleCtx, ie, traceID); err == nil {
						// Ignore any errors since this is done on the
						// best-effort basis.
						ih.inFlightTraceCollector.timeoutTrace = tr
					}
				}
			}
			bundle = buildStatementBundle(
				bundleCtx, cfg.DB, ie.(*InternalExecutor), stmtRawSQL, &p.curPlan,
				ob.BuildString(), trace, placeholders, res.Err(), payloadErr, retErr,
				&p.extendedEvalCtx.Settings.SV, ih.inFlightTraceCollector,
			)
			// Include all non-critical errors as warnings. Note that these
			// error strings might contain PII, but the warnings are only shown
			// to the current user and aren't included into the bundle.
			warnings = append(warnings, bundle.errorStrings...)
			bundle.insert(
				bundleCtx, ih.fingerprint, ast, cfg.StmtDiagnosticsRecorder, ih.diagRequestID, ih.diagRequest,
			)
			telemetry.Inc(sqltelemetry.StatementDiagnosticsCollectedCounter)
		}
		ih.stmtDiagnosticsRecorder.MaybeRemoveRequest(ih.diagRequestID, ih.diagRequest, execLatency)
	}

	// If there was a communication error already, no point in setting any
	// results.
	if retErr != nil {
		return retErr
	}

	switch ih.outputMode {
	case explainAnalyzeDebugOutput:
		return setExplainBundleResult(ctx, res, bundle, cfg, warnings)

	case explainAnalyzePlanOutput, explainAnalyzeDistSQLOutput:
		var flows []flowInfo
		if ih.outputMode == explainAnalyzeDistSQLOutput {
			flows = p.curPlan.distSQLFlowInfos
		}
		return ih.setExplainAnalyzeResult(ctx, res, statsCollector.PhaseTimes(), queryLevelStats, flows, trace)

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
	return ih.collectBundle || ih.collectExecStats ||
		ih.outputMode == explainAnalyzePlanOutput ||
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
	if ih.explainPlan == nil || !ih.savePlanForStats {
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

// emitExplainAnalyzePlanToOutputBuilder creates an explain.OutputBuilder and
// populates it with the EXPLAIN ANALYZE plan. BuildString/BuildStringRows can
// be used on the result.
func (ih *instrumentationHelper) emitExplainAnalyzePlanToOutputBuilder(
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

	if queryStats != nil {
		if queryStats.KVRowsRead != 0 {
			ob.AddKVReadStats(queryStats.KVRowsRead, queryStats.KVBytesRead, queryStats.KVBatchRequestsIssued)
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
		if ih.isTenant && ih.outputMode != unmodifiedOutput && ih.vectorized {
			// Only output RU estimate if this is a tenant running EXPLAIN ANALYZE.
			// Additionally, RUs aren't correctly propagated in all cases for plans
			// that aren't vectorized - for example, EXPORT statements. For now,
			// only output RU estimates for vectorized plans.
			ob.AddRUEstimate(queryStats.RUEstimate)
		}
	}

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
	trace tracingpb.Recording,
) (commErr error) {
	res.ResetStmtType(&tree.ExplainAnalyze{})
	res.SetColumns(ctx, colinfo.ExplainPlanColumns)

	if res.Err() != nil {
		// Can't add rows if there was an error.
		return nil //nolint:returnerrcheck
	}

	ob := ih.emitExplainAnalyzePlanToOutputBuilder(ih.explainFlags, phaseTimes, queryLevelStats)
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

// getAssociateNodeWithComponentsFn returns a function, unsafe for concurrent
// usage, that maintains a mapping from planNode to tracing metadata. It might
// return nil in which case this mapping is not needed.
func (ih *instrumentationHelper) getAssociateNodeWithComponentsFn() func(exec.Node, execComponents) {
	if ih.traceMetadata == nil {
		return nil
	}
	return ih.traceMetadata.associateNodeWithComponents
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
				nodeStats.KVBatchRequestsIssued.MaybeAdd(stats.KV.BatchRequestsIssued)
				nodeStats.UsedStreamer = stats.KV.UsedStreamer
				nodeStats.StepCount.MaybeAdd(stats.KV.NumInterfaceSteps)
				nodeStats.InternalStepCount.MaybeAdd(stats.KV.NumInternalSteps)
				nodeStats.SeekCount.MaybeAdd(stats.KV.NumInterfaceSeeks)
				nodeStats.InternalSeekCount.MaybeAdd(stats.KV.NumInternalSeeks)
				nodeStats.VectorizedBatchCount.MaybeAdd(stats.Output.NumBatches)
				nodeStats.MaxAllocatedMem.MaybeAdd(stats.Exec.MaxAllocatedMem)
				nodeStats.MaxAllocatedDisk.MaybeAdd(stats.Exec.MaxAllocatedDisk)
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

// SetIndexRecommendations checks if we should generate a new index recommendation.
// If true it will generate and update the idx recommendations cache,
// if false, uses the value on index recommendations cache and updates its counter.
func (ih *instrumentationHelper) SetIndexRecommendations(
	ctx context.Context, idxRec *idxrecommendations.IndexRecCache, planner *planner, isInternal bool,
) {
	stmtType := planner.stmt.AST.StatementType()

	reset := false
	var recommendations []indexrec.Rec
	if idxRec.ShouldGenerateIndexRecommendation(
		ih.fingerprint,
		ih.planGist.Hash(),
		planner.SessionData().Database,
		stmtType,
		isInternal,
	) {
		opc := &planner.optPlanningCtx
		opc.reset(ctx)
		f := opc.optimizer.Factory()
		// EvalContext() has the context with the already closed span, so we
		// need to update with the current context.
		// The replacement of the context here isn't ideal, but the current
		// implementation of contexts would need to change
		// significantly to accommodate this case.
		evalCtx := opc.p.EvalContext()
		oldCtx := evalCtx.Context
		evalCtx.Context = ctx
		defer func() {
			evalCtx.Context = oldCtx
		}()

		f.Init(evalCtx, &opc.catalog)
		f.FoldingControl().AllowStableFolds()
		bld := optbuilder.New(ctx, &opc.p.semaCtx, evalCtx, &opc.catalog, f, opc.p.stmt.AST)
		err := bld.Build()
		if err != nil {
			log.Warningf(ctx, "unable to build memo: %s", err)
		} else {
			err = opc.makeQueryIndexRecommendation(ctx)
			if err != nil {
				log.Warningf(ctx, "unable to generate index recommendations: %s", err)
			}
		}
		reset = true
		recommendations = ih.indexRecs
	}
	ih.indexRecs = idxRec.UpdateIndexRecommendations(
		ih.fingerprint,
		ih.planGist.Hash(),
		planner.SessionData().Database,
		stmtType,
		isInternal,
		recommendations,
		reset,
	)
}
