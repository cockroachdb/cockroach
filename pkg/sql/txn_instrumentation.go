// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

type txnDiagnosticsCollector struct {
	request           stmtdiagnostics.TxnRequest
	requestId         stmtdiagnostics.RequestID
	z                 memzipper.Zipper
	stmtBundles       []stmtdiagnostics.StmtDiagnostic
	stmtsFpsToCapture []uint64
	readyToFinalize   bool
	span              *tracing.Span
}

func newTxnDiagnosticsCollector(
	request stmtdiagnostics.TxnRequest, requestId stmtdiagnostics.RequestID, span *tracing.Span,
) txnDiagnosticsCollector {
	collector := txnDiagnosticsCollector{
		stmtsFpsToCapture: request.StmtFingerprintIds(),
		request:           request,
		stmtBundles:       nil,
		requestId:         requestId,
		readyToFinalize:   false,
		span:              span,
	}
	collector.z.Init()
	return collector
}

func (tds *txnDiagnosticsCollector) ShouldCollect(executionDuration time.Duration) bool {
	if !tds.readyToFinalize {
		return false
	}

	return executionDuration >= tds.request.MinExecutionLatency()
}

func (tds *txnDiagnosticsCollector) AddStatementBundle(
	stmtFingerprintId uint64, stmt tree.Statement, bundle stmtdiagnostics.StmtDiagnostic,
) (added bool, err error) {
	if tds.readyToFinalize {
		return false, errors.Newf("Illegal state, cannot add bundle to completed txn diagnostic")
	}
	if len(tds.stmtsFpsToCapture) > 0 {
		nextStmtToCapture := tds.stmtsFpsToCapture[0]
		if nextStmtToCapture != stmtFingerprintId {
			return false, nil // Unexpected statement fingerprint - return (false, nil)
		}
		tds.stmtsFpsToCapture = tds.stmtsFpsToCapture[1:]
	} else {
		if !isCommit(stmt) && !isRollback(stmt) {
			return false, errors.Newf("Expected commit or rollback, got %s", stmt)
		}
		tds.readyToFinalize = true
	}
	tds.stmtBundles = append(tds.stmtBundles, bundle)

	return true, nil
}

func (tds *txnDiagnosticsCollector) collectTrace() {
	if tds.request.IsRedacted() {
		tds.z.AddFile("trace.txt", "trace not collected due to redacted request")
		return
	}
	span := tds.span
	trace := span.GetRecording(span.RecordingType())

	if len(trace) == 0 {
		tds.z.AddFile("trace.txt", "trace empty")
		return
	}

	traceJSONStr, err := tracing.TraceToJSON(trace)
	if err != nil {
		tds.z.AddFile("trace.err", err.Error())
	} else {
		tds.z.AddFile("trace.json", traceJSONStr)
	}

	idStr := sqlstatsutil.EncodeTxnFingerprintIDToString(
		appstatspb.TransactionFingerprintID(tds.request.TxnFingerprintId()),
	)
	// The JSON is not very human-readable, so we include another format too.
	tds.z.AddFile("trace.txt", fmt.Sprintf("%s\n\n\n\n%s", idStr, trace.String()))

	// Note that we're going to include the non-anonymized statement in the trace.
	// But then again, nothing in the trace is anonymized.
	comment := fmt.Sprintf(`This is a trace for Txn fingerprint: %s
This trace can be imported into Jaeger for visualization. From the Jaeger Search screen, select the JSON File.
Jaeger can be started using docker with: docker run -d --name jaeger -p 16686:16686 jaegertracing/all-in-one:1.17
The UI can then be accessed at http://localhost:16686/search`, idStr)
	jaegerJSON, err := trace.ToJaegerJSON("txn-trace", comment, "", true /* indent */)
	if err != nil {
		tds.z.AddFile("trace-jaeger.err", err.Error())
	} else {
		tds.z.AddFile("trace-jaeger.json", jaegerJSON)
	}
}

type txnInstrumentationHelper struct {
	TxnDiagnosticsRecorder *stmtdiagnostics.TxnRegistry
	diagnosticsCollector   txnDiagnosticsCollector
}

func (h *txnInstrumentationHelper) StartDiagnostics(
	txnRequest stmtdiagnostics.TxnRequest, reqID stmtdiagnostics.RequestID, span *tracing.Span,
) {
	h.diagnosticsCollector = newTxnDiagnosticsCollector(txnRequest, reqID, span)
}

// DiagnosticsInProgress returns true if diagnostics collection is considered
// to be progress. This is determined by the following:
// - there is an active request id
// - there has been at least 1 statement bundle collected
// - there are still statements to be collected
// If none of these are true, then diagnostics collection is not in progress.
//
// NB: Currently, transaction diagnostics collection only happens through
// requests, so the first condition is sufficient. If, in the future, new
// methods of collecting transaction diagnostics are added, such as through
// "explain analyze (debug)", then the other two conditions will be necessary.
func (h *txnInstrumentationHelper) DiagnosticsInProgress() bool {
	return h.diagnosticsCollector.requestId != 0 ||
		len(h.diagnosticsCollector.stmtBundles) > 0 ||
		len(h.diagnosticsCollector.stmtsFpsToCapture) > 0
}

func (h *txnInstrumentationHelper) Reset() {
	if h.diagnosticsCollector.requestId != 0 {
		h.TxnDiagnosticsRecorder.ResetTxnRequest(h.diagnosticsCollector.requestId)
	}

	h.diagnosticsCollector.span.Finish()
	h.diagnosticsCollector = txnDiagnosticsCollector{}
}

func (h *txnInstrumentationHelper) AddStatementBundle(
	ctx context.Context,
	stmt tree.Statement,
	stmtFingerprintId uint64,
	stmtFingerprint string,
	bundle diagnosticsBundle,
) {
	stmtDiag := stmtdiagnostics.NewStmtDiagnostic(
		stmtdiagnostics.RequestID(0),
		stmtdiagnostics.Request{},
		stmtFingerprint,
		tree.AsString(stmt),
		bundle.zip,
		bundle.collectionErr,
	)
	if added, err := h.diagnosticsCollector.AddStatementBundle(stmtFingerprintId, stmt, stmtDiag); !added {
		if err != nil {
			log.Ops.VWarningf(ctx, 2, "Failed to add statement bundle: %s", err)
		}
		h.Reset()
	}
}

// MaybeStartDiagnostics checks whether diagnostics collection should be
// started. If a new diagnostics collection is started, it returns a new
// context that should be used to capture transaction traces.
func (h *txnInstrumentationHelper) MaybeStartDiagnostics(
	ctx context.Context, stmtFpId uint64, tracer *tracing.Tracer,
) (newCtx context.Context, diagnosticsStarted bool) {
	if !h.DiagnosticsInProgress() {
		collectDiagnostics, requestId, req := h.TxnDiagnosticsRecorder.ShouldStartTxnDiagnostic(ctx, stmtFpId)
		if collectDiagnostics {
			var sp *tracing.Span
			if !req.IsRedacted() {
				ctx, sp = tracing.EnsureChildSpan(ctx, tracer, "txn-diag-bundle",
					tracing.WithRecording(tracingpb.RecordingVerbose))
			}
			h.StartDiagnostics(req, requestId, sp)
			return ctx, true
		}
	}
	return ctx, false
}

func (h *txnInstrumentationHelper) ShouldContinueDiagnostics(
	ctx context.Context, stmt tree.Statement, stmtFpId uint64,
) (newCtx context.Context, shouldContinue bool) {
	if !h.DiagnosticsInProgress() {
		return ctx, false
	}

	if len(h.diagnosticsCollector.stmtsFpsToCapture) != 0 {
		shouldContinue = h.diagnosticsCollector.stmtsFpsToCapture[0] == stmtFpId
	} else {
		shouldContinue = isCommit(stmt) || isRollback(stmt)
	}

	if !shouldContinue {
		h.Reset()
	} else {
		// TODO (kyle.wong): due to the existing hierarchy of spans, we have to
		//  manually manage the span by putting it in the context. Ideally, we
		//  wouldn't need to do this, but it would require a larger refactor to
		//  the spans created in the call stack.
		ctx = tracing.ContextWithSpan(ctx, h.diagnosticsCollector.span)
	}

	return ctx, shouldContinue
}

func (h *txnInstrumentationHelper) ShouldRedact() bool {
	return h.diagnosticsCollector.request.IsRedacted()
}

func (h *txnInstrumentationHelper) Finalize(ctx context.Context, executionDuration time.Duration) {
	defer h.Reset()
	collector := h.diagnosticsCollector
	if collector.ShouldCollect(executionDuration) {
		collector.collectTrace()
		buf, err := collector.z.Finalize()
		var b []byte
		if err != nil {
			log.Ops.Errorf(ctx, "Error finalizing txn collector zip for request: %d. err: %s", collector.requestId, err.Error())
		} else {
			b = buf.Bytes()
		}
		txnDiag := stmtdiagnostics.NewTxnDiagnostic(collector.stmtBundles, b)
		_, err = h.TxnDiagnosticsRecorder.InsertTxnDiagnostic(ctx, collector.requestId, collector.request, txnDiag)
		if err != nil {
			log.Ops.Errorf(ctx, "Error inserting diagnostics for request: %d. err: %s", collector.requestId, err.Error())
		}
	}
}
