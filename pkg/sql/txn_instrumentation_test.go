// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"archive/zip"
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
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
	stmtFingerprintId := uint64(0)
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
	stmtFingerprintId := uint64(123)
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
	stmtFingerprintId := uint64(999) // Wrong fingerprint
	stmtFingerprint := "SELECT * FROM table"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	// After no success, helper should be reset
	require.False(t, helper.DiagnosticsInProgress())
}

func TestTxnInstrumentationHelper_ShouldContinueDiagnostics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type statement struct {
		stmt tree.Statement
		fpId uint64
	}
	for _, tc := range []struct {
		name           string
		stmt           statement
		stmtsToCapture []uint64
		shouldContinue bool
	}{
		{
			name:           "matching fingerprint",
			stmt:           statement{stmt: &tree.Select{Select: &tree.SelectClause{}}, fpId: 123},
			stmtsToCapture: []uint64{123, 456},
			shouldContinue: true,
		},
		{
			name:           "commit",
			stmt:           statement{stmt: &tree.CommitTransaction{}, fpId: 9999},
			stmtsToCapture: []uint64{},
			shouldContinue: true,
		}, {
			name:           "rollback",
			stmt:           statement{stmt: &tree.RollbackTransaction{}, fpId: 9999},
			stmtsToCapture: []uint64{},
			shouldContinue: true,
		}, {
			name:           "fp should not continue",
			stmt:           statement{stmt: &tree.Select{Select: &tree.SelectClause{}}, fpId: 123},
			stmtsToCapture: []uint64{456},
			shouldContinue: false,
		}, {
			name:           "non terminal statement",
			stmt:           statement{stmt: &tree.Select{Select: &tree.SelectClause{}}, fpId: 123},
			stmtsToCapture: []uint64{},
			shouldContinue: false,
		}, {
			name:           "commit not expected",
			stmt:           statement{stmt: &tree.CommitTransaction{}, fpId: 999},
			stmtsToCapture: []uint64{123},
			shouldContinue: false,
		}, {
			name:           "rollback not expected",
			stmt:           statement{stmt: &tree.RollbackTransaction{}, fpId: 999},
			stmtsToCapture: []uint64{123},
			shouldContinue: false,
		},
	} {
		helper := &txnInstrumentationHelper{
			TxnDiagnosticsRecorder: stmtdiagnostics.NewTxnRegistry(nil, nil, nil),
		}
		request := stmtdiagnostics.NewTxnRequest(
			111,
			tc.stmtsToCapture,
			false,
			"",
			time.Time{},
			0,
			0,
		)
		requestID := stmtdiagnostics.RequestID(42)
		helper.StartDiagnostics(request, requestID)
		actual := helper.ShouldContinueDiagnostics(tc.stmt.stmt, tc.stmt.fpId)
		require.Equal(t, tc.shouldContinue, actual)

		if !tc.shouldContinue {
			require.Zero(t, helper.diagnosticsCollector)
			require.False(t, helper.DiagnosticsInProgress())
		}
	}
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

func TestTxnInstrumentationHelper_collectionFlow(t *testing.T) {
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
	helper.AddStatementBundle(ctx, stmt1, uint64(100), "SELECT * FROM table1", bundle1)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 1)
	require.Equal(t, []uint64{200, 300}, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process second statement
	stmt2 := &tree.Select{Select: &tree.SelectClause{}}
	require.True(t, helper.ShouldContinueDiagnostics(stmt2, 200))

	bundle2 := diagnosticsBundle{zip: []byte("stmt2-zip")}
	helper.AddStatementBundle(ctx, stmt2, uint64(200), "SELECT * FROM table2", bundle2)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 2)
	require.Equal(t, []uint64{300}, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process third statement
	stmt3 := &tree.Select{Select: &tree.SelectClause{}}
	require.True(t, helper.ShouldContinueDiagnostics(stmt3, 300))

	bundle3 := diagnosticsBundle{zip: []byte("stmt3-zip")}
	helper.AddStatementBundle(ctx, stmt3, uint64(300), "SELECT * FROM table3", bundle3)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 3)
	require.Empty(t, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.readyToFinalize)

	// Process commit statement
	commit := &tree.CommitTransaction{}
	require.True(t, helper.ShouldContinueDiagnostics(commit, 0))

	bundleCommit := diagnosticsBundle{zip: []byte("commit-zip")}
	helper.AddStatementBundle(ctx, commit, uint64(0), "COMMIT", bundleCommit)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 4)
	require.True(t, helper.diagnosticsCollector.readyToFinalize)
}

func TestTxnDiagnosticsCollector_AddTrace_RedactedRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test that AddTrace does nothing when request is redacted
	request := stmtdiagnostics.NewTxnRequest(
		1111,
		[]uint64{1111, 2222},
		true, // redacted = true
		"testuser",
		time.Time{},
		0,
		0,
	)
	collector := newTxnDiagnosticsCollector(request, stmtdiagnostics.RequestID(42))

	// Create a mock trace
	trace := tracingpb.Recording{
		{
			TraceID:   1,
			SpanID:    1,
			Operation: "test-operation",
		},
	}

	// Add trace - should be a no-op for redacted requests
	collector.AddTrace(trace)

	// Verify no files were added by finalizing and checking if zip is empty
	buf, err := collector.z.Finalize()
	require.NoError(t, err)

	// Read the zip to check if it's empty
	reader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, reader.File, 0, "No files should be added for redacted requests")
}

func TestTxnDiagnosticsCollector_AddTrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test handling of valid trace
	request := stmtdiagnostics.NewTxnRequest(
		1111,
		[]uint64{1111, 2222},
		false,
		"testuser",
		time.Time{},
		0,
		0,
	)
	collector := newTxnDiagnosticsCollector(request, stmtdiagnostics.RequestID(42))

	// Create a valid trace with proper span
	trace := tracingpb.Recording{
		{
			TraceID:   1,
			SpanID:    1,
			Operation: "test-operation",
			StartTime: timeutil.Now(),
			Duration:  time.Millisecond,
		},
	}

	// Add trace
	collector.AddTrace(trace)

	// Verify files were added
	buf, err := collector.z.Finalize()
	require.NoError(t, err)

	reader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)

	fileNames := make(map[string]bool)
	for _, f := range reader.File {
		fileNames[f.Name] = true
	}

	require.True(t, fileNames["trace.json"])
	require.True(t, fileNames["trace.txt"])
	require.True(t, fileNames["trace-jaeger.json"])
}

func TestTxnDiagnosticsCollector_AddTrace_notrace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test handling of valid trace
	request := stmtdiagnostics.NewTxnRequest(
		1111,
		[]uint64{1111, 2222},
		false,
		"testuser",
		time.Time{},
		0,
		0,
	)
	collector := newTxnDiagnosticsCollector(request, stmtdiagnostics.RequestID(42))

	// Add trace
	collector.AddTrace(nil)

	// Verify files were added
	buf, err := collector.z.Finalize()
	require.NoError(t, err)

	reader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, reader.File, 0, "No files should be added for empty trace")
}
