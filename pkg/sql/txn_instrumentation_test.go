// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestTxnDiagnosticsCollector_AddStatementBundle_NoMoreFps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "stmt", []tree.Statement{&tree.CommitTransaction{}, &tree.RollbackTransaction{}},
		func(t *testing.T, stmt tree.Statement) {
			request := stmtdiagnostics.TxnRequest{}
			requestID := stmtdiagnostics.RequestID(42)
			collector := newTxnDiagnosticsCollector(request, requestID, nil)

			// Since stmtsFpsToCapture is empty, only commit/rollback should be accepted
			bundle := stmtdiagnostics.StmtDiagnostic{}
			success, err := collector.AddStatementBundle(0, stmt, bundle)
			require.NoError(t, err)
			require.True(t, success)
			require.Len(t, collector.stmtBundles, 1)
			require.True(t, collector.ReadyToFinalize())
		})
}

func TestTxnDiagnosticsCollector_AddStatementBundle_ExpectedTerminalStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID, nil)

	stmt := &tree.Select{
		Select: &tree.SelectClause{},
	}
	bundle := stmtdiagnostics.StmtDiagnostic{}
	success, err := collector.AddStatementBundle(123, stmt, bundle)
	require.Error(t, err)
	require.False(t, success)
	require.Contains(t, err.Error(), "Expected a terminal statement")
}

func TestTxnDiagnosticsCollector_AddStatementBundle_AlreadyFinalized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID, nil)
	collector.UpdateState(txnDiagnosticsReadyToFinalize)

	stmt := &tree.Select{
		Select: &tree.SelectClause{},
	}
	bundle := stmtdiagnostics.StmtDiagnostic{}
	success, err := collector.AddStatementBundle(123, stmt, bundle)
	require.Error(t, err)
	require.False(t, success)
	require.Contains(t, err.Error(), "Illegal state: transaction diagnostics collector is not in progress")
}

func TestTxnDiagnosticsCollector_AddStatementBundle_WithStmtsToCapture(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID, nil)
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
	require.False(t, collector.ReadyToFinalize())

	stmt2 := &tree.Insert{}
	bundle2 := stmtdiagnostics.StmtDiagnostic{}

	success, err = collector.AddStatementBundle(456, stmt2, bundle2)
	require.NoError(t, err)
	require.True(t, success)
	require.Len(t, collector.stmtBundles, 2)
	require.Empty(t, collector.stmtsFpsToCapture)
	require.False(t, collector.ReadyToFinalize())
}

func TestTxnDiagnosticsCollector_AddStatementBundle_WrongFingerprint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	collector := newTxnDiagnosticsCollector(request, requestID, nil)

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

	span := &tracing.Span{}
	helper.StartDiagnostics(request, requestID, span)

	require.Equal(t, request, helper.diagnosticsCollector.request)
	require.Equal(t, requestID, helper.diagnosticsCollector.requestId)
	require.Equal(t, span, helper.diagnosticsCollector.span)
	require.True(t, helper.diagnosticsCollector.InProgress())
}

func TestTxnInstrumentationHelper_AddStatementBundle_Success(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID, nil)

	ctx := context.Background()
	stmt := &tree.CommitTransaction{} // Use commit since we have no expected statements
	stmtFingerprintId := uint64(0)
	stmtFingerprint := "COMMIT"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 1)
	require.True(t, helper.diagnosticsCollector.ReadyToFinalize())
}

func TestTxnInstrumentationHelper_AddStatementBundle_Error(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}
	helper.TxnDiagnosticsRecorder = stmtdiagnostics.NewTxnRegistry(nil, nil, nil, timeutil.DefaultTimeSource{})

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID, nil)

	// Set collector to already finalized to trigger error
	helper.diagnosticsCollector.UpdateState(txnDiagnosticsReadyToFinalize)

	ctx := context.Background()
	stmt := &tree.Select{Select: &tree.SelectClause{}}
	stmtFingerprintId := uint64(123)
	stmtFingerprint := "SELECT * FROM table"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	// After error, helper should be reset
	require.False(t, helper.diagnosticsCollector.InProgress())
}

func TestTxnInstrumentationHelper_AddStatementBundle_NoSuccess(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	helper := &txnInstrumentationHelper{}
	helper.TxnDiagnosticsRecorder = stmtdiagnostics.NewTxnRegistry(nil, nil, nil, timeutil.DefaultTimeSource{})

	request := stmtdiagnostics.TxnRequest{}
	requestID := stmtdiagnostics.RequestID(42)
	helper.StartDiagnostics(request, requestID, nil)

	// Set up expected fingerprints so we can trigger no-success case
	helper.diagnosticsCollector.stmtsFpsToCapture = []uint64{123}

	ctx := context.Background()
	stmt := &tree.Select{Select: &tree.SelectClause{}}
	stmtFingerprintId := uint64(999) // Wrong fingerprint
	stmtFingerprint := "SELECT * FROM table"
	bundle := diagnosticsBundle{zip: []byte("test-zip")}

	helper.AddStatementBundle(ctx, stmt, stmtFingerprintId, stmtFingerprint, bundle)

	// After no success, helper should be reset
	require.False(t, helper.diagnosticsCollector.InProgress())
}

func TestTxnInstrumentationHelper_MaybeContinueDiagnostics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
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
			TxnDiagnosticsRecorder: stmtdiagnostics.NewTxnRegistry(nil, nil, nil, timeutil.DefaultTimeSource{}),
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
		tracer := tracing.NewTracer()
		sp := tracer.StartSpan("parent-span", tracing.WithRecording(tracingpb.RecordingVerbose))
		helper.StartDiagnostics(request, requestID, sp)
		newCtx, actual := helper.MaybeContinueDiagnostics(ctx, tc.stmt.stmt, tc.stmt.fpId, uuid.MakeV4())
		require.Equal(t, tc.shouldContinue, actual)

		if !tc.shouldContinue {
			require.True(t, helper.diagnosticsCollector.IsReset())
			require.False(t, helper.diagnosticsCollector.InProgress())
			require.Equal(t, ctx, newCtx)
		} else {
			require.NotEqual(t, ctx, newCtx)
			returnedSpan := tracing.SpanFromContext(newCtx)
			require.Equal(t, sp, returnedSpan)
			require.True(t, helper.diagnosticsCollector.InProgress())
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
		helper.StartDiagnostics(request, requestID, nil)

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
	helper.StartDiagnostics(request, requestID, nil)

	// Simulate a TxnRequest with multiple statement fingerprints
	expectedFingerprints := []uint64{100, 200, 300}
	helper.diagnosticsCollector.stmtsFpsToCapture = expectedFingerprints

	ctx := context.Background()
	testTxnID := uuid.MakeV4()

	// Process first statement
	stmt1 := &tree.Select{Select: &tree.SelectClause{}}
	_, shouldContinue := helper.MaybeContinueDiagnostics(ctx, stmt1, 100, testTxnID)
	require.True(t, shouldContinue)

	bundle1 := diagnosticsBundle{zip: []byte("stmt1-zip")}
	helper.AddStatementBundle(ctx, stmt1, uint64(100), "SELECT * FROM table1", bundle1)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 1)
	require.Equal(t, []uint64{200, 300}, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.ReadyToFinalize())

	// Process second statement
	stmt2 := &tree.Select{Select: &tree.SelectClause{}}
	_, shouldContinue = helper.MaybeContinueDiagnostics(ctx, stmt2, 200, testTxnID)
	require.True(t, shouldContinue)

	bundle2 := diagnosticsBundle{zip: []byte("stmt2-zip")}
	helper.AddStatementBundle(ctx, stmt2, uint64(200), "SELECT * FROM table2", bundle2)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 2)
	require.Equal(t, []uint64{300}, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.ReadyToFinalize())

	// Process third statement
	stmt3 := &tree.Select{Select: &tree.SelectClause{}}
	_, shouldContinue = helper.MaybeContinueDiagnostics(ctx, stmt2, 300, testTxnID)
	require.True(t, shouldContinue)

	bundle3 := diagnosticsBundle{zip: []byte("stmt3-zip")}
	helper.AddStatementBundle(ctx, stmt3, uint64(300), "SELECT * FROM table3", bundle3)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 3)
	require.Empty(t, helper.diagnosticsCollector.stmtsFpsToCapture)
	require.False(t, helper.diagnosticsCollector.ReadyToFinalize())

	// Process commit statement
	commit := &tree.CommitTransaction{}
	_, shouldContinue = helper.MaybeContinueDiagnostics(ctx, commit, 0, testTxnID)
	require.True(t, shouldContinue)

	bundleCommit := diagnosticsBundle{zip: []byte("commit-zip")}
	helper.AddStatementBundle(ctx, commit, uint64(0), "COMMIT", bundleCommit)

	require.Len(t, helper.diagnosticsCollector.stmtBundles, 4)
	require.True(t, helper.diagnosticsCollector.ReadyToFinalize())
}

func TestTxnDiagnosticsCollector_collectTrace_RedactedRequest(t *testing.T) {
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
	collector := newTxnDiagnosticsCollector(request, stmtdiagnostics.RequestID(42), nil)
	collector.collectTrace()

	buf, err := collector.z.Finalize()
	require.NoError(t, err)

	// Read the zip to check if it's empty
	reader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, reader.File, 1)

	// Check that the file contains the expected message
	rc, err := reader.File[0].Open()
	require.NoError(t, err)
	defer rc.Close()

	content, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "trace not collected due to redacted request", string(content))
}

func TestTxnDiagnosticsCollector_collectTrace(t *testing.T) {
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

	tracer := tracing.NewTracer()
	span := tracer.StartSpan("test-tracer", tracing.WithRecording(tracingpb.RecordingVerbose))
	span.Record("test msg")
	collector := newTxnDiagnosticsCollector(request, stmtdiagnostics.RequestID(42), span)

	// Add trace
	collector.collectTrace()

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

func TestTxnDiagnosticsCollector_collectTrace_emptyRecordings(t *testing.T) {
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

	collector := newTxnDiagnosticsCollector(request, stmtdiagnostics.RequestID(42), &tracing.Span{})
	collector.collectTrace()

	buf, err := collector.z.Finalize()
	require.NoError(t, err)

	// Read the zip to check if it's empty
	reader, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	require.Len(t, reader.File, 1)

	// Check that the file contains the expected message
	rc, err := reader.File[0].Open()
	require.NoError(t, err)
	defer rc.Close()

	content, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "trace empty", string(content))
}

func TestTxnDiagnosticsCollector_MaybeStartDiagnostics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseHelper := txnInstrumentationHelper{
		TxnDiagnosticsRecorder: stmtdiagnostics.NewTxnRegistry(nil, nil, nil, timeutil.DefaultTimeSource{}),
	}
	t.Run("not started", func(t *testing.T) {
		ctx := context.Background()
		for _, tc := range []struct {
			name          string
			stmt          tree.Statement
			expectedState txnDiagnosticsState
		}{
			{
				name:          "should skip",
				stmt:          &tree.Select{Select: &tree.SelectClause{}},
				expectedState: txnDiagnosticsSkipped,
			}, {
				name:          "should stay not started",
				stmt:          &tree.Savepoint{},
				expectedState: txnDiagnosticsNotStarted,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				helper := baseHelper
				helper.diagnosticsCollector.UpdateState(txnDiagnosticsNotStarted)
				testTxnID := uuid.MakeV4()
				newCtx, started := helper.MaybeStartDiagnostics(ctx, tc.stmt, 123, nil, testTxnID)
				require.False(t, started)
				require.Equal(t, ctx, newCtx)
				require.Equal(t, tc.expectedState, helper.diagnosticsCollector.state)
			})
		}
	})

	t.Run("already in progress", func(t *testing.T) {
		testutils.RunValues(t,
			"with state",
			[]txnDiagnosticsState{txnDiagnosticsInProgress, txnDiagnosticsReadyToFinalize},
			func(t *testing.T, state txnDiagnosticsState) {
				ts := serverutils.StartServerOnly(t, base.TestServerArgs{})
				ctx := context.Background()
				defer ts.Stopper().Stop(ctx)

				helper := baseHelper
				helper.TxnDiagnosticsRecorder = stmtdiagnostics.NewTxnRegistry(ts.InternalDB().(isql.DB), nil, nil, timeutil.DefaultTimeSource{})

				_, err := helper.TxnDiagnosticsRecorder.InsertTxnRequest(ctx, 1, []uint64{123}, "testuser", 0, 0, 0, false)
				require.NoError(t, err)
				helper.diagnosticsCollector.UpdateState(state)
				testTxnID := uuid.MakeV4()
				newCtx, started := helper.MaybeStartDiagnostics(ctx, &tree.Select{Select: &tree.SelectClause{}}, 123, ts.Tracer(), testTxnID)
				require.False(t, started)
				require.Equal(t, ctx, newCtx)
			})
	})

	t.Run("started", func(t *testing.T) {
		testutils.RunTrueAndFalse(t, "redacted", func(t *testing.T, redacted bool) {

			ts := serverutils.StartServerOnly(t, base.TestServerArgs{})
			ctx := context.Background()
			defer ts.Stopper().Stop(ctx)

			helper := baseHelper
			helper.diagnosticsCollector.UpdateState(txnDiagnosticsNotStarted)
			helper.TxnDiagnosticsRecorder = stmtdiagnostics.NewTxnRegistry(ts.InternalDB().(isql.DB), nil, nil, timeutil.DefaultTimeSource{})

			_, err := helper.TxnDiagnosticsRecorder.InsertTxnRequest(ctx, 1, []uint64{123}, "testuser", 0, 0, 0, redacted)
			require.NoError(t, err)

			testTxnID := uuid.MakeV4()
			newCtx, started := helper.MaybeStartDiagnostics(ctx, &tree.Select{Select: &tree.SelectClause{}}, 123, ts.Tracer(), testTxnID)
			require.True(t, started)
			require.True(t, helper.diagnosticsCollector.InProgress())
			if redacted {
				require.Equal(t, ctx, newCtx)
				require.Nil(t, helper.diagnosticsCollector.span)
			} else {
				require.NotEqual(t, ctx, newCtx)
				require.NotNil(t, helper.diagnosticsCollector.span)
				require.Equal(t, tracing.SpanFromContext(newCtx), helper.diagnosticsCollector.span)
			}
		})
	})
}
