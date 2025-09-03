// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTxnDiagnosticsCollector_AddStatementBundle_NoMoreFps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "stmt", []tree.Statement{&tree.CommitTransaction{}, &tree.RollbackTransaction{}},
		func(t *testing.T, stmt tree.Statement) {
			request := stmtdiagnostics.TxnRequest{}
			requestID := stmtdiagnostics.RequestID(42)
			collector := newTxnDiagnosticsCollector(request, requestID)

			// Since stmtsFpsToCapture is empty, only commit/rollback should be accepted
			bundle := stmtdiagnostics.StmtDiagnostic{}
			success, err := collector.AddStatementBundle(0, stmt, bundle)
			require.NoError(t, err)
			require.True(t, success)
			require.Len(t, collector.stmtBundles, 1)
			require.True(t, collector.readyToFinalize)
		})
}

func TestTxnDiagnosticsCollector_AddStatementBundle_ExpectedCommitOrRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID)

	stmt := &tree.Select{
		Select: &tree.SelectClause{},
	}
	bundle := stmtdiagnostics.StmtDiagnostic{}
	success, err := collector.AddStatementBundle(123, stmt, bundle)
	require.Error(t, err)
	require.False(t, success)
	require.Contains(t, err.Error(), "Expected commit or rollback")
}

func TestTxnDiagnosticsCollector_AddStatementBundle_AlreadyFinalized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID)
	collector.readyToFinalize = true

	stmt := &tree.Select{
		Select: &tree.SelectClause{},
	}
	bundle := stmtdiagnostics.StmtDiagnostic{}
	success, err := collector.AddStatementBundle(123, stmt, bundle)
	require.Error(t, err)
	require.False(t, success)
	require.Contains(t, err.Error(), "Illegal state, cannot add bundle to completed txn diagnostic")
}

func TestTxnDiagnosticsCollector_AddStatementBundle_WithStmtsToCapture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID)
	collector.stmtsFpsToCapture = []uint64{123, 456}

	stmt1 := &tree.Select{
		Select: &tree.SelectClause{},
	}
	bundle1 := stmtdiagnostics.StmtDiagnostic{}

	success, err := collector.AddStatementBundle(123, stmt1, bundle1)
	require.NoError(t, err)
	require.True(t, success)
	require.Len(t, collector.stmtBundles, 1)
	require.Equal(t, []uint64{456}, collector.stmtsFpsToCapture)
	require.False(t, collector.readyToFinalize)

	stmt2 := &tree.Insert{}
	bundle2 := stmtdiagnostics.StmtDiagnostic{}

	success, err = collector.AddStatementBundle(456, stmt2, bundle2)
	require.NoError(t, err)
	require.True(t, success)
	require.Len(t, collector.stmtBundles, 2)
	require.Empty(t, collector.stmtsFpsToCapture)
	require.False(t, collector.readyToFinalize)
}

func TestTxnDiagnosticsCollector_AddStatementBundle_WrongFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID)

	collector.stmtsFpsToCapture = []uint64{123, 456}

	stmt := &tree.Select{
		Select: &tree.SelectClause{},
	}
	bundle := stmtdiagnostics.StmtDiagnostic{}

	success, err := collector.AddStatementBundle(999, stmt, bundle)
	require.NoError(t, err)
	require.False(t, success)
	require.Empty(t, collector.stmtBundles)
}

func TestTxnInstrumentationHelper_StartDiagnostics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)

	helper.StartDiagnostics(request, requestID)

	require.Equal(t, request, helper.diagnosticsCollector.request)
	require.Equal(t, requestID, helper.diagnosticsCollector.requestId)
	require.True(t, helper.DiagnosticsInProgress())
}

func TestTxnInstrumentationHelper_DiagnosticsInProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}

	require.False(t, helper.DiagnosticsInProgress())

	helper.diagnosticsCollector.requestId = stmtdiagnostics.RequestID(42)
	require.True(t, helper.DiagnosticsInProgress())
}

func TestTxnInstrumentationHelper_AddStatementBundle_Success(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	ctx := context.Background()
	stmt := &tree.CommitTransaction{} // Use commit since we have no expected statements
	stmtFingerprintId := appstatspb.StmtFingerprintID(0)
	stmtFingerprint := "COMMIT"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 1)
	require.True(t, helper.diagnosticsCollector.readyToFinalize)
}

func TestTxnInstrumentationHelper_AddStatementBundle_Error(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}
	helper.TxnDiagnosticsRecorder = stmtdiagnostics.NewTxnRegistry(nil, nil, nil)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Set collector to already finalized to trigger error
	helper.diagnosticsCollector.readyToFinalize = true

	ctx := context.Background()
	stmt := &tree.Select{Select: &tree.SelectClause{}}
	stmtFingerprintId := appstatspb.StmtFingerprintID(123)
	stmtFingerprint := "SELECT * FROM table"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	// After error, helper should be reset
	require.False(t, helper.DiagnosticsInProgress())
}

func TestTxnInstrumentationHelper_AddStatementBundle_NoSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}
	helper.TxnDiagnosticsRecorder = stmtdiagnostics.NewTxnRegistry(nil, nil, nil)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Set up expected fingerprints so we can trigger no-success case
	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{123}

	ctx := context.Background()
	stmt := &tree.Select{Select: &tree.SelectClause{}}
	stmtFingerprintId := appstatspb.StmtFingerprintID(999) // Wrong fingerprint
	stmtFingerprint := "SELECT * FROM table"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	// After no success, helper should be reset
	require.False(t, helper.DiagnosticsInProgress())
}

func TestTxnInstrumentationHelper_ShouldContinueDiagnostics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}

	require.False(t, helper.ShouldContinueDiagnostics(&tree.Select{Select: &tree.SelectClause{}}, 123))

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Manually set up statements to capture
	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{123, 456}

	require.True(t, helper.ShouldContinueDiagnostics(&tree.Select{Select: &tree.SelectClause{}}, 123))
	require.False(t, helper.ShouldContinueDiagnostics(&tree.Select{Select: &tree.SelectClause{}}, 999))

	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{}

	require.True(t, helper.ShouldContinueDiagnostics(&tree.CommitTransaction{}, 0))
	require.True(t, helper.ShouldContinueDiagnostics(&tree.RollbackTransaction{}, 0))
	require.False(t, helper.ShouldContinueDiagnostics(&tree.Select{Select: &tree.SelectClause{}}, 123))
}

func TestTxnInstrumentationHelper_ShouldRedact(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "shouldRedact", func(t *testing.T, shouldRedact bool) {
		helper := &txnInstrumentationHelper{}

		request := stmtdiagnostics.NewTxnRequest(0, nil, shouldRedact, "", time.Time{}, 0, 0)
		requestID := stmtdiagnostics.RequestID(42)
		helper.StartDiagnostics(request, requestID)

		require.Equal(t, shouldRedact, helper.ShouldRedact())
	})
}

func TestTxnInstrumentationHelper_WithStatementFingerprints_Sequential(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test the complete flow with multiple statement fingerprints using only SELECT statements to avoid formatting issues
	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Simulate a TxnRequest with multiple statement fingerprints
	expectedFingerprints := []uint64{100, 200, 300}
	helper.diagnosticsCollector.stmtsFpsToCapture = expectedFingerprints

	ctx := context.Background()

	// Process first statement
	stmt1 := &tree.Select{Select: &tree.SelectClause{}}
	require.True(t, helper.ShouldContinueDiagnostics(stmt1, 100))

	bundle1 := diagnosticsBundle{zip: []byte("stmt1-zip")}
	helper.AddStatementBundle(ctx, stmt1, appstatspb.StmtFingerprintID(100), "SELECT * FROM table1", bundle1)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 1)
	require.Equal(t, []uint64{200, 300}, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process second statement
	stmt2 := &tree.Select{Select: &tree.SelectClause{}}
	require.True(t, helper.ShouldContinueDiagnostics(stmt2, 200))

	bundle2 := diagnosticsBundle{zip: []byte("stmt2-zip")}
	helper.AddStatementBundle(ctx, stmt2, appstatspb.StmtFingerprintID(200), "SELECT * FROM table2", bundle2)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 2)
	require.Equal(t, []uint64{300}, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process third statement
	stmt3 := &tree.Select{Select: &tree.SelectClause{}}
	require.True(t, helper.ShouldContinueDiagnostics(stmt3, 300))

	bundle3 := diagnosticsBundle{zip: []byte("stmt3-zip")}
	helper.AddStatementBundle(ctx, stmt3, appstatspb.StmtFingerprintID(300), "SELECT * FROM table3", bundle3)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 3)
	require.Empty(t, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process commit statement
	commit := &tree.CommitTransaction{}
	require.True(t, helper.ShouldContinueDiagnostics(commit, 0))

	bundleCommit := diagnosticsBundle{zip: []byte("commit-zip")}
	helper.AddStatementBundle(ctx, commit, appstatspb.StmtFingerprintID(0), "COMMIT", bundleCommit)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 4)
	require.True(t, helper.diagnosticsCollector.readyToFinalize)
}

func TestTxnInstrumentationHelper_WithStatementFingerprints_OutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test error handling when statements arrive out of order
	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Simulate a TxnRequest expecting fingerprints in specific order
	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{100, 200, 300}

	// Try to process statement with wrong fingerprint (200 instead of expected 100)
	stmt := &tree.Select{Select: &tree.SelectClause{}}
	require.False(t, helper.ShouldContinueDiagnostics(stmt, 200)) // Should return false for wrong fingerprint

	// Should still expect the first fingerprint
	require.True(t, helper.ShouldContinueDiagnostics(stmt, 100))
}

func TestTxnInstrumentationHelper_WithStatementFingerprints_EarlyCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test behavior when commit arrives before all expected statements
	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Simulate a TxnRequest expecting multiple statements
	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{100, 200}

	ctx := context.Background()

	// Process only first statement
	stmt1 := &tree.Select{Select: &tree.SelectClause{}}
	bundle1 := diagnosticsBundle{zip: []byte("stmt1-zip")}
	helper.AddStatementBundle(ctx, stmt1, appstatspb.StmtFingerprintID(100), "SELECT * FROM table1", bundle1)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 1)
	require.Equal(t, []uint64{200}, helper.diagnosticsCollector.stmtsFpsToCapture)

	// Try to commit early (before getting statement 200)
	commit := &tree.CommitTransaction{}
	require.False(t, helper.ShouldContinueDiagnostics(commit, 0)) // Should return false since we're still expecting statement 200

	// Verify we still expect statement 200
	require.True(t, helper.ShouldContinueDiagnostics(&tree.Select{Select: &tree.SelectClause{}}, 200))
}

func TestTxnInstrumentationHelper_WithStatementFingerprints_RollbackInstead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test behavior with rollback instead of commit
	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID)

	// Simulate a TxnRequest with statement fingerprints
	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{100, 200}

	ctx := context.Background()

	// Process expected statements
	stmt1 := &tree.Select{Select: &tree.SelectClause{}}
	bundle1 := diagnosticsBundle{zip: []byte("stmt1-zip")}
	helper.AddStatementBundle(ctx, stmt1, appstatspb.StmtFingerprintID(100), "SELECT * FROM table1", bundle1)

	stmt2 := &tree.Select{Select: &tree.SelectClause{}}
	bundle2 := diagnosticsBundle{zip: []byte("stmt2-zip")}
	helper.AddStatementBundle(ctx, stmt2, appstatspb.StmtFingerprintID(200), "SELECT * FROM table2", bundle2)

	require.Empty(t, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process rollback instead of commit
	rollback := &tree.RollbackTransaction{}
	require.True(t, helper.ShouldContinueDiagnostics(rollback, 0))

	bundleRollback := diagnosticsBundle{zip: []byte("rollback-zip")}
	helper.AddStatementBundle(ctx, rollback, appstatspb.StmtFingerprintID(0), "ROLLBACK", bundleRollback)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 3)
	require.True(t, helper.diagnosticsCollector.readyToFinalize)
}
