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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type txnDiagnosticsState int

const (
	txnDiagnosticsReset txnDiagnosticsState = iota
	txnDiagnosticsNotStarted
	txnDiagnosticsInProgress
	txnDiagnosticsReadyToFinalize
	txnDiagnosticsSkipped
)

type txnDiagnosticsCollector struct {
	request           stmtdiagnostics.TxnRequest
	requestId         stmtdiagnostics.RequestID
	z                 memzipper.Zipper
	stmtBundles       []stmtdiagnostics.StmtDiagnostic
	stmtsFpsToCapture []uint64
	span              *tracing.Span
	state             txnDiagnosticsState
}

func newTxnDiagnosticsCollector(
	request stmtdiagnostics.TxnRequest, requestId stmtdiagnostics.RequestID, span *tracing.Span,
) txnDiagnosticsCollector {
	collector := txnDiagnosticsCollector{
		stmtsFpsToCapture: request.StmtFingerprintIds(),
		request:           request,
		stmtBundles:       nil,
		requestId:         requestId,
		span:              span,
	}
	collector.UpdateState(txnDiagnosticsInProgress)
	collector.z.Init()
	return collector
}

func (tds *txnDiagnosticsCollector) IsReset() bool {
	return tds.state == txnDiagnosticsReset
}

func (tds *txnDiagnosticsCollector) IsSkipped() bool {
	return tds.state == txnDiagnosticsSkipped
}

func (tds *txnDiagnosticsCollector) NotStarted() bool {
	return tds.state == txnDiagnosticsNotStarted
}

func (tds *txnDiagnosticsCollector) InProgress() bool {
	return tds.state == txnDiagnosticsInProgress
}

func (tds *txnDiagnosticsCollector) ReadyToFinalize() bool {
	return tds.state == txnDiagnosticsReadyToFinalize
}

func (tds *txnDiagnosticsCollector) UpdateState(state txnDiagnosticsState) {
	tds.state = state
}

func (tds *txnDiagnosticsCollector) ShouldCollect(executionDuration time.Duration) bool {
	if !tds.ReadyToFinalize() {
		return false
	}

	return executionDuration >= tds.request.MinExecutionLatency()
}

// AddStatementBundle adds a statement diagnostic bundle to the transaction
// diagnostics collector. It returns true if the bundle was added, and
// false otherwise. If a statement bundle is added while the collector is not
// in progress, an error is also returned.
func (tds *txnDiagnosticsCollector) AddStatementBundle(
	stmtFingerprintId uint64, stmt tree.Statement, bundle stmtdiagnostics.StmtDiagnostic,
) (added bool, err error) {
	if !tds.InProgress() {
		return false, errors.Newf("Illegal state: transaction diagnostics collector is not in progress")
	}

	if !tds.shouldAllowStatement(stmt) {
		if len(tds.stmtsFpsToCapture) > 0 {
			nextStmtToCapture := tds.stmtsFpsToCapture[0]
			if nextStmtToCapture != stmtFingerprintId {
				return false, nil
			}
			tds.stmtsFpsToCapture = tds.stmtsFpsToCapture[1:]
		} else {
			if !isTerminalStatement(stmt) {
				return false, errors.Newf("Expected a terminal statement, got %s", stmt)
			}
			tds.UpdateState(txnDiagnosticsReadyToFinalize)
		}
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
	// txnFingerprintHash points to the FNV64 hash accumulating statement
	// fingerprint IDs for the current transaction. This is set from the
	// connExecutor during transaction setup and allows Finalize to compute
	// the actual transaction fingerprint for logging.
	txnFingerprintHash *util.FNV64
}

func (h *txnInstrumentationHelper) StartDiagnostics(
	txnRequest stmtdiagnostics.TxnRequest, reqID stmtdiagnostics.RequestID, span *tracing.Span,
) {
	h.diagnosticsCollector = newTxnDiagnosticsCollector(txnRequest, reqID, span)
}

func (h *txnInstrumentationHelper) Reset() {
	if h.diagnosticsCollector.requestId != 0 {
		h.TxnDiagnosticsRecorder.ResetTxnRequest(h.diagnosticsCollector.requestId)
	}

	h.diagnosticsCollector.span.Finish()
	h.diagnosticsCollector = txnDiagnosticsCollector{}
}

// Skip marks the transaction diagnostics collection as skipped. This means
// that the current transaction will not be considered for diagnostics
// collection.
func (h *txnInstrumentationHelper) Skip() {
	h.diagnosticsCollector.UpdateState(txnDiagnosticsSkipped)
}

// AddStatementBundle adds a statement diagnostic bundle to the transaction
// diagnostics collector. If the bundle cannot be added, the transaction
// diagnostics collection is reset.
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
	ctx context.Context,
	stmt tree.Statement,
	stmtFpId uint64,
	tracer *tracing.Tracer,
	txnID uuid.UUID,
) (newCtx context.Context, diagnosticsStarted bool) {
	if h.diagnosticsCollector.NotStarted() {
		if h.diagnosticsCollector.shouldAllowStatement(stmt) {
			// If shouldAllowStatement is true, diagnostics won't be started, but it
			// can still be started by future statements.
			return ctx, false
		} else {
			// Otherwise, check if there are transaction diagnostics requests for the
			// provided statement fingerprint. If there are, start collecting diagnostics.
			// Otherwise, reset the diagnostics collector to avoid diagnostics
			// collection for the rest of the transaction.
			collectDiagnostics, requestId, req := h.TxnDiagnosticsRecorder.ShouldStartTxnDiagnostic(ctx, stmtFpId)
			if collectDiagnostics {
				log.Dev.VInfof(ctx, 2, "txn diag: starting collection for request %d, "+
					"txn %s, triggered by stmt fingerprint %s",
					requestId,
					txnID,
					sqlstatsutil.EncodeStmtFingerprintIDToString(
						appstatspb.StmtFingerprintID(stmtFpId)))
				var sp *tracing.Span
				if !req.IsRedacted() {
					ctx, sp = tracing.EnsureChildSpan(ctx, tracer, "txn-diag-bundle",
						tracing.WithRecording(tracingpb.RecordingVerbose))
				}
				h.StartDiagnostics(req, requestId, sp)
				return ctx, true
			} else {
				h.Skip()
			}
		}
	}
	return ctx, false
}

// MaybeContinueDiagnostics checks whether diagnostics collection should
// continue. If diagnostics collection is not currently in progress, nothing
// happens. If diagnostics collection should continue, it returns a new
// context with the diagnostics recording span. Otherwise, collection is
// aborted and future statements will not be considered for diagnostics.
func (h *txnInstrumentationHelper) MaybeContinueDiagnostics(
	ctx context.Context, stmt tree.Statement, stmtFpId uint64, txnID uuid.UUID,
) (newCtx context.Context, shouldContinue bool) {
	if !h.diagnosticsCollector.InProgress() {
		return ctx, false
	}

	if h.diagnosticsCollector.shouldAllowStatement(stmt) {
		shouldContinue = true
	} else if len(h.diagnosticsCollector.stmtsFpsToCapture) != 0 {
		shouldContinue = h.diagnosticsCollector.stmtsFpsToCapture[0] == stmtFpId
	} else {
		shouldContinue = isTerminalStatement(stmt)
	}

	if !shouldContinue {
		var expected string
		if len(h.diagnosticsCollector.stmtsFpsToCapture) != 0 {
			expected = sqlstatsutil.EncodeStmtFingerprintIDToString(
				appstatspb.StmtFingerprintID(h.diagnosticsCollector.stmtsFpsToCapture[0]))
		} else {
			expected = "terminal statement"
		}
		log.Dev.VInfof(ctx, 2, "txn diag: aborting collection for request %d, "+
			"txn %s, stmt fingerprint %s did not match (expected %s), collected %d bundles so far",
			h.diagnosticsCollector.requestId,
			txnID,
			sqlstatsutil.EncodeStmtFingerprintIDToString(
				appstatspb.StmtFingerprintID(stmtFpId)),
			expected,
			len(h.diagnosticsCollector.stmtBundles))
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

// ShouldRedact returns whether diagnostics being collected should be redacted
func (h *txnInstrumentationHelper) ShouldRedact() bool {
	return h.diagnosticsCollector.request.IsRedacted()
}

// Finalize finalizes the transaction diagnostics collection by writing all
// the collected data to the underlying system tables.
func (h *txnInstrumentationHelper) Finalize(
	ctx context.Context, executionDuration time.Duration, txnID uuid.UUID,
) {
	defer h.Reset()
	collector := h.diagnosticsCollector
	// If the collection was skipped or never started, then we shouldn't have
	// done any work, so there's nothing to finalize.
	if collector.IsSkipped() || collector.NotStarted() {
		return
	}
	var txnFp appstatspb.TransactionFingerprintID

	if h.txnFingerprintHash != nil {
		txnFp = appstatspb.TransactionFingerprintID(h.txnFingerprintHash.Sum())
	}
	txnFpStr := sqlstatsutil.EncodeTxnFingerprintIDToString(txnFp)
	if collector.IsReset() {
		log.Dev.VInfof(ctx, 2, "txn diag: finalize aborted collection "+
			"txn %s, txn fingerprint %s",
			txnID, txnFpStr)
		return
	}
	if collector.InProgress() {
		log.Dev.VInfof(ctx, 2, "txn diag: finalizing collection for "+
			"request %d, txn %s, txn fingerprint %s",
			collector.requestId, txnID, txnFpStr)
		collector.UpdateState(txnDiagnosticsReadyToFinalize)
	}
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

func isTerminalStatement(stmt tree.Statement) bool {
	switch s := stmt.(type) {
	case *tree.CommitTransaction:
		return true
	case *tree.ReleaseSavepoint:
		// commitOnReleaseSavepointName is a special savepoint that commits
		// the underlying kv transaction.
		if s.Savepoint == commitOnReleaseSavepointName {
			return true
		}
		return false
	case *tree.ShowCommitTimestamp:
		return true
	case *tree.RollbackTransaction:
		return true
	default:
		return false
	}
}

// shouldAllowStatement returns true if the statement should always be
// recorded. These will not be part of a transaction's fingerprint but may be
// executed within a transaction. In this case, the statements should be
// allowed, and they should not stop the collection of the transaction
// diagnostics.
func (tds *txnDiagnosticsCollector) shouldAllowStatement(stmt tree.Statement) bool {
	switch s := stmt.(type) {
	case *tree.Savepoint:
		return true
	case *tree.ReleaseSavepoint:
		// commitOnReleaseSavepointName is a special savepoint that commits
		// the underlying kv transaction.
		if s.Savepoint == commitOnReleaseSavepointName {
			return false
		}
		return true
	case *tree.RollbackToSavepoint:
		return true
	case *tree.PrepareTransaction:
		return true
	case *tree.Prepare:
		return true
	default:
		return false
	}
}
