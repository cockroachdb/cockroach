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

type txnDiagnosticsState int

const (
	txnDiagnosticsReset txnDiagnosticsState = iota
	txnDiagnosticsNotStarted
	txnDiagnosticsInProgress
	txnDiagnosticsReadyToFinalize
)

// candidateRequest tracks a single diagnostic request candidate through
// progressive fingerprint matching. As each statement executes, candidates
// whose next expected fingerprint doesn't match are eliminated.
type candidateRequest struct {
	id                stmtdiagnostics.RequestID
	request           stmtdiagnostics.TxnRequest
	stmtsFpsToCapture []uint64
}

type txnDiagnosticsCollector struct {
	candidates  []candidateRequest
	z           memzipper.Zipper
	stmtBundles []stmtdiagnostics.StmtDiagnostic
	span        *tracing.Span
	state       txnDiagnosticsState
}

func newTxnDiagnosticsCollector(
	matches []stmtdiagnostics.TxnRequestMatch, span *tracing.Span,
) txnDiagnosticsCollector {
	candidates := make([]candidateRequest, len(matches))
	for i, m := range matches {
		// Copy the fingerprint slice so each candidate tracks its own
		// progress independently.
		fps := m.Request.StmtFingerprintIds()
		fpsCopy := make([]uint64, len(fps))
		copy(fpsCopy, fps)
		candidates[i] = candidateRequest{
			id:                m.ID,
			request:           m.Request,
			stmtsFpsToCapture: fpsCopy,
		}
	}
	collector := txnDiagnosticsCollector{
		candidates:  candidates,
		stmtBundles: nil,
		span:        span,
	}
	collector.UpdateState(txnDiagnosticsInProgress)
	collector.z.Init()
	return collector
}

func (tds *txnDiagnosticsCollector) IsReset() bool {
	return tds.state == txnDiagnosticsReset
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

// ShouldCollect returns true if the collector is ready to finalize and at
// least one candidate's MinExecutionLatency constraint is satisfied.
func (tds *txnDiagnosticsCollector) ShouldCollect(executionDuration time.Duration) bool {
	if !tds.ReadyToFinalize() {
		return false
	}
	for i := range tds.candidates {
		if executionDuration >= tds.candidates[i].request.MinExecutionLatency() {
			return true
		}
	}
	return false
}

// AddStatementBundle adds a statement diagnostic bundle to the transaction
// diagnostics collector. It filters candidates: those whose next expected
// fingerprint doesn't match are eliminated. If no candidates survive, it
// returns (false, nil) to signal the caller to reset. If a statement bundle
// is added while the collector is not in progress, an error is returned.
func (tds *txnDiagnosticsCollector) AddStatementBundle(
	stmtFingerprintId uint64, stmt tree.Statement, bundle stmtdiagnostics.StmtDiagnostic,
) (added bool, err error) {
	if !tds.InProgress() {
		return false, errors.Newf("Illegal state: transaction diagnostics collector is not in progress")
	}

	if !tds.shouldAllowStatement(stmt) {
		// Filter candidates based on expected fingerprint.
		surviving := tds.candidates[:0]
		for _, c := range tds.candidates {
			if len(c.stmtsFpsToCapture) > 0 {
				if c.stmtsFpsToCapture[0] == stmtFingerprintId {
					c.stmtsFpsToCapture = c.stmtsFpsToCapture[1:]
					surviving = append(surviving, c)
				}
				// else: mismatch, candidate eliminated
			} else {
				// No more fingerprints expected; only terminal statements pass.
				if isTerminalStatement(stmt) {
					surviving = append(surviving, c)
				}
			}
		}
		tds.candidates = surviving

		if len(tds.candidates) == 0 {
			return false, nil
		}

		// Check if all surviving candidates have exhausted their fingerprint
		// list and this is a terminal statement — if so, transition to ready.
		allDone := true
		for _, c := range tds.candidates {
			if len(c.stmtsFpsToCapture) > 0 {
				allDone = false
				break
			}
		}
		if allDone && isTerminalStatement(stmt) {
			tds.UpdateState(txnDiagnosticsReadyToFinalize)
		}
	}

	tds.stmtBundles = append(tds.stmtBundles, bundle)
	return true, nil
}

// collectTrace collects the transaction trace and adds it to the zip bundle.
// The redacted parameter controls whether the trace is actually collected.
func (tds *txnDiagnosticsCollector) collectTrace(redacted bool, txnFingerprintId uint64) {
	if redacted {
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
		appstatspb.TransactionFingerprintID(txnFingerprintId),
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
	matches []stmtdiagnostics.TxnRequestMatch, span *tracing.Span,
) {
	h.diagnosticsCollector = newTxnDiagnosticsCollector(matches, span)
}

func (h *txnInstrumentationHelper) Reset() {
	h.diagnosticsCollector.span.Finish()
	h.diagnosticsCollector = txnDiagnosticsCollector{}
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
	ctx context.Context, stmt tree.Statement, stmtFpId uint64, tracer *tracing.Tracer,
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
			matches := h.TxnDiagnosticsRecorder.ShouldStartTxnDiagnostic(ctx, stmtFpId)
			if len(matches) > 0 {
				var sp *tracing.Span
				// Start verbose tracing if any candidate is non-redacted.
				anyNonRedacted := false
				for _, m := range matches {
					if !m.Request.IsRedacted() {
						anyNonRedacted = true
						break
					}
				}
				if anyNonRedacted {
					ctx, sp = tracing.EnsureChildSpan(ctx, tracer, "txn-diag-bundle",
						tracing.WithRecording(tracingpb.RecordingVerbose))
				}
				h.StartDiagnostics(matches, sp)
				return ctx, true
			} else {
				h.Reset()
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
	ctx context.Context, stmt tree.Statement, stmtFpId uint64,
) (newCtx context.Context, shouldContinue bool) {
	if !h.diagnosticsCollector.InProgress() {
		return ctx, false
	}

	if h.diagnosticsCollector.shouldAllowStatement(stmt) {
		shouldContinue = true
	} else {
		// Check if any candidate can accept this statement.
		for _, c := range h.diagnosticsCollector.candidates {
			if len(c.stmtsFpsToCapture) != 0 {
				if c.stmtsFpsToCapture[0] == stmtFpId {
					shouldContinue = true
					break
				}
			} else {
				if isTerminalStatement(stmt) {
					shouldContinue = true
					break
				}
			}
		}
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

// ShouldRedact returns whether diagnostics being collected should be redacted.
// Returns true if ANY candidate requests redaction (conservative approach).
func (h *txnInstrumentationHelper) ShouldRedact() bool {
	for _, c := range h.diagnosticsCollector.candidates {
		if c.request.IsRedacted() {
			return true
		}
	}
	return false
}

// Finalize finalizes the transaction diagnostics collection by writing all
// the collected data to the underlying system tables. It picks the first
// surviving candidate whose MinExecutionLatency constraint is satisfied.
func (h *txnInstrumentationHelper) Finalize(ctx context.Context, executionDuration time.Duration) {
	defer h.Reset()
	collector := &h.diagnosticsCollector
	if collector.InProgress() {
		collector.UpdateState(txnDiagnosticsReadyToFinalize)
	}
	if !collector.ShouldCollect(executionDuration) {
		return
	}

	// Pick the first candidate whose latency constraint is satisfied.
	var chosen *candidateRequest
	for i := range collector.candidates {
		if executionDuration >= collector.candidates[i].request.MinExecutionLatency() {
			chosen = &collector.candidates[i]
			break
		}
	}
	if chosen == nil {
		return
	}

	collector.collectTrace(chosen.request.IsRedacted(), chosen.request.TxnFingerprintId())
	buf, err := collector.z.Finalize()
	var b []byte
	if err != nil {
		log.Ops.Errorf(ctx, "Error finalizing txn collector zip for request: %d. err: %s", chosen.id, err.Error())
	} else {
		b = buf.Bytes()
	}
	txnDiag := stmtdiagnostics.NewTxnDiagnostic(collector.stmtBundles, b)
	_, err = h.TxnDiagnosticsRecorder.InsertTxnDiagnostic(ctx, chosen.id, chosen.request, txnDiag)
	if err != nil {
		log.Ops.Errorf(ctx, "Error inserting diagnostics for request: %d. err: %s", chosen.id, err.Error())
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
