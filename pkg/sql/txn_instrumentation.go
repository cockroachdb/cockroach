// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type txnDiagnosticsCollector struct {
	request           stmtdiagnostics.TxnRequest
	requestId         stmtdiagnostics.RequestID
	stmtBundles       []stmtdiagnostics.StmtDiagnostic
	stmtsFpsToCapture []uint64
	readyToFinalize   bool
}

func newTxnDiagnosticsCollector(
	request stmtdiagnostics.TxnRequest, requestId stmtdiagnostics.RequestID,
) txnDiagnosticsCollector {
	return txnDiagnosticsCollector{
		stmtsFpsToCapture: request.StmtFingerprintIds(),
		request:           request,
		stmtBundles:       nil,
		requestId:         requestId,
		readyToFinalize:   false,
	}
}

func (tds *txnDiagnosticsCollector) AddStatementBundle(
	stmtFingerprintId uint64, stmt tree.Statement, bundle stmtdiagnostics.StmtDiagnostic,
) (bool, error) {
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
			h.Reset()
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
		txnDiag := stmtdiagnostics.NewTxnDiagnostic(collector.stmtBundles)
		id, err := h.TxnDiagnosticsRecorder.InsertTxnDiagnostic(ctx, collector.requestId, collector.request, txnDiag)
		if err != nil {
			log.Ops.Errorf(ctx, "Error inserting diagnostics: %s", err.Error())
		}
		// TODO: 1. Add support for persisting the transaction trace.
		//			 2. Update transaction_diagnostics table with data
		//       3. Update the transaction_diagnostics_requests to mark the current request as complete
		//
		log.Dev.Infof(ctx, "Inserted transaction diagnostics with id %d", id)
	}
}
