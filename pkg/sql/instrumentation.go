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
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// instrumentationHelper encapsulates the logic around extracting information
// about the execution of a statement, like bundles and traces. Typical usage:
//
//  - SetOutputMode() can be used as necessary if we are running an EXPLAIN
//    ANALYZE variant.
//
//  - Setup() is called before query execution.
//
//  - SetDiscardRows(), ShouldDiscardRows(), ShouldCollectBundle() can be called
//    at any point during execution.
//
//  - Finish() is called after query execution.
//
type instrumentationHelper struct {
	outputMode outputMode

	// Query fingerprint (anonymized statement).
	fingerprint string
	implicitTxn bool

	// -- The following fields are initialized by Setup() --

	// collectBundle is set when we are collecting a diagnostics bundle for a
	// statement; it triggers saving of extra information like the plan string.
	collectBundle bool

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
}

// outputMode indicates how the statement output needs to be populated (for
// EXPLAIN ANALYZE variants).
type outputMode int8

const (
	unmodifiedOutput outputMode = iota
	explainAnalyzeDebugOutput
)

// SetOutputMode can be called before Setup, if we are running an EXPLAIN
// ANALYZE variant.
func (ih *instrumentationHelper) SetOutputMode(outputMode outputMode) {
	ih.outputMode = outputMode
}

// Setup potentially enables snowball tracing for the statement, depending on
// output mode or statement diagnostic activation requests. Finish() must be
// called after the statement finishes execution (unless needFinish=false, in
// which case Finish() is a no-op).
func (ih *instrumentationHelper) Setup(
	ctx context.Context,
	cfg *ExecutorConfig,
	p *planner,
	stmtDiagnosticsRecorder *stmtdiagnostics.Registry,
	fingerprint string,
	implicitTxn bool,
) (newCtx context.Context, needFinish bool) {
	ih.fingerprint = fingerprint
	ih.implicitTxn = implicitTxn

	if ih.outputMode == explainAnalyzeDebugOutput {
		ih.collectBundle = true
		// EXPLAIN ANALYZE (DEBUG) does not return the rows for the given query;
		// instead it returns some text which includes a URL.
		// TODO(radu): maybe capture some of the rows and include them in the
		// bundle.
		ih.discardRows = true
	} else {
		ih.collectBundle, ih.diagRequestID, ih.finishCollectionDiagnostics =
			stmtDiagnosticsRecorder.ShouldCollectDiagnostics(ctx, fingerprint)
	}

	ih.withStatementTrace = cfg.TestingKnobs.WithStatementTrace

	// TODO(radu): logic around saving plans for stats should be here.

	if !ih.collectBundle && ih.withStatementTrace == nil {
		return ctx, false
	}

	ih.origCtx = ctx
	newCtx, ih.sp = tracing.StartSnowballTrace(ctx, cfg.AmbientCtx.Tracer, "traced statement")
	return newCtx, true
}

func (ih *instrumentationHelper) Finish(
	cfg *ExecutorConfig,
	appStats *appStats,
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
	ie := p.extendedEvalCtx.InternalExecutor.(*InternalExecutor)
	placeholders := p.extendedEvalCtx.Placeholders
	bundle := buildStatementBundle(
		ih.origCtx, cfg.DB, ie, &p.curPlan, trace, placeholders,
	)
	bundle.insert(ctx, ih.fingerprint, ast, cfg.StmtDiagnosticsRecorder, ih.diagRequestID)
	if ih.finishCollectionDiagnostics != nil {
		ih.finishCollectionDiagnostics()
		telemetry.Inc(sqltelemetry.StatementDiagnosticsCollectedCounter)
	}
	if ih.withStatementTrace != nil {
		ih.withStatementTrace(trace, stmtRawSQL)
	}

	// If there was a communication error already, no point in setting any results.
	if retErr == nil {
		switch ih.outputMode {
		case explainAnalyzeDebugOutput:
			// Handle EXPLAIN ANALYZE (DEBUG).
			retErr = setExplainBundleResult(ctx, res, bundle, cfg)
		}
	}

	// TODO(radu): this should be unified with other stmt stats accesses.
	stmtStats, _ := appStats.getStatsForStmt(ih.fingerprint, ih.implicitTxn, retErr, false)
	if stmtStats != nil {
		networkBytesSent := int64(0)
		for _, flowInfo := range p.curPlan.distSQLFlowInfos {
			analyzer := flowInfo.analyzer
			if err := analyzer.AddTrace(trace); err != nil {
				log.VInfof(ctx, 1, "error analyzing trace statistics for stmt %s: %v", ast, err)
				continue
			}

			networkBytesSentGroupedByNode, err := analyzer.GetNetworkBytesSent()
			if err != nil {
				log.VInfof(ctx, 1, "error calculating network bytes sent for stmt %s: %v", ast, err)
				continue
			}
			for _, bytesSentByNode := range networkBytesSentGroupedByNode {
				networkBytesSent += bytesSentByNode
			}
		}

		stmtStats.mu.Lock()
		// Record trace-related statistics. A count of 1 is passed given that this
		// statistic is only recorded when statement diagnostics are enabled.
		// TODO(asubiotto): NumericStat properties will be properly calculated
		//  once this statistic is always collected.
		stmtStats.mu.data.BytesSentOverNetwork.Record(1 /* count */, float64(networkBytesSent))
		stmtStats.mu.Unlock()
	}

	return retErr
}

// SetDiscardRows should be called when we want to discard rows for a
// non-ANALYZE statement (via EXECUTE .. DISCARD ROWS.
func (ih *instrumentationHelper) SetDiscardRows() {
	ih.discardRows = true
}

// ShouldDiscardRows returns true if this is an EXPLAIN ANALYZE variant or
// SetDiscardRows() was called.
func (ih *instrumentationHelper) ShouldDiscardRows() bool {
	return ih.discardRows
}

// ShouldCollectBundle is true if we are collecting a support bundle.
func (ih *instrumentationHelper) ShouldCollectBundle() bool {
	return ih.collectBundle
}
