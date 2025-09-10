// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type txnDiagnosticsCollector struct {
	request           stmtdiagnostics.TxnRequest
	requestId         stmtdiagnostics.RequestID
	z                 memzipper.Zipper
	stmtBundles       []stmtdiagnostics.StmtDiagnostic
	stmtsFpsToCapture []uint64
	readyToFinalize   bool
}

func newTxnDiagnosticsCollector(
	request stmtdiagnostics.TxnRequest, requestId stmtdiagnostics.RequestID,
) txnDiagnosticsCollector {
	collector := txnDiagnosticsCollector{
		stmtsFpsToCapture: request.StmtFingerprintIds(),
		request:           request,
		stmtBundles:       nil,
		requestId:         requestId,
		readyToFinalize:   false,
	}
	collector.z.Init()
	return collector
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

func (tds *txnDiagnosticsCollector) AddTrace(trace tracingpb.Recording) {
	if tds.request.IsRedacted() || len(trace) == 0 {
		return
	}

	traceJSONStr, err := tracing.TraceToJSON(trace)
	if err != nil {
		tds.z.AddFile("trace.json", err.Error())
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
		tds.z.AddFile("trace-jaeger.txt", err.Error())
	} else {
		tds.z.AddFile("trace-jaeger.json", jaegerJSON)
	}
}

type txnInstrumentationHelper struct {
	TxnDiagnosticsRecorder *stmtdiagnostics.TxnRegistry
	diagnosticsCollector   txnDiagnosticsCollector
}

func (h *txnInstrumentationHelper) StartDiagnostics(
	txnRequest stmtdiagnostics.TxnRequest, reqID stmtdiagnostics.RequestID,
) {
	h.diagnosticsCollector = newTxnDiagnosticsCollector(txnRequest, reqID)
}

func (h *txnInstrumentationHelper) DiagnosticsInProgress() bool {
	return h.diagnosticsCollector.requestId != 0
}

func (h *txnInstrumentationHelper) Reset() {
	if h.diagnosticsCollector.requestId != 0 {
		h.TxnDiagnosticsRecorder.ResetTxnRequest(h.diagnosticsCollector.requestId)
	}

	h.diagnosticsCollector = txnDiagnosticsCollector{}
}

func (h *txnInstrumentationHelper) AddStatementBundle(
	ctx context.Context,
	stmt tree.Statement,
	stmtFingerprintId appstatspb.StmtFingerprintID,
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
	if added, err := h.diagnosticsCollector.AddStatementBundle(uint64(stmtFingerprintId), stmt, stmtDiag); !added {
		if err != nil {
			log.Ops.VWarningf(ctx, 2, "Failed to add statement bundle: %s", err)
		}
		h.Reset()
	}
}

func (h *txnInstrumentationHelper) ShouldContinueDiagnostics(
	stmt tree.Statement, stmtFpId appstatspb.StmtFingerprintID,
) bool {
	if !h.DiagnosticsInProgress() {
		return false
	}

	if len(h.diagnosticsCollector.stmtsFpsToCapture) != 0 {
		return h.diagnosticsCollector.stmtsFpsToCapture[0] == uint64(stmtFpId)
	} else {
		return isCommit(stmt) || isRollback(stmt)
	}
}

func (h *txnInstrumentationHelper) ShouldRedact() bool {
	return h.diagnosticsCollector.request.IsRedacted()
}

func (h *txnInstrumentationHelper) Finalize(ctx context.Context, txnID uuid.UUID) {
	defer h.Reset()
	collector := h.diagnosticsCollector
	if collector.readyToFinalize {
		buf, err := collector.z.Finalize()
		var b []byte
		if err != nil {
			log.Ops.Errorf(ctx, "Error finalizing txn collector zip for request: %d. err: %s", collector.requestId, err.Error())
		} else {
			b = buf.Bytes()
		}
		txnDiag := stmtdiagnostics.NewTxnDiagnostic(collector.stmtBundles, b)
		id, err := h.TxnDiagnosticsRecorder.InsertTxnDiagnostic(ctx, collector.requestId, collector.request, txnDiag)
		if err != nil {
			log.Ops.Errorf(ctx, "Error inserting diagnostics for request: %d. err: %s", collector.requestId, err.Error())
		} else {
			log.Dev.Infof(ctx, "Inserted transaction diagnostics with id %d", id)
		}
	}
}
