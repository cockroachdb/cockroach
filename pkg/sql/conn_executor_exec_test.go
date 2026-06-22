// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestEpochBumpReadSeqRestore exercises stepReadSequenceWithRestore when the
// outer txn's epoch advances between the read-sequence capture and the
// deferred restore. The AfterExecute hook runs in that window and forces a
// retryable error on the shared outer txn handle, bumping the epoch and
// resetting writeSeq to 0. Without the epoch gate on the restore, the
// cleanup would trip the readSeq <= writeSeq invariant in TxnCoordSender.
func TestEpochBumpReadSeqRestore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const targetMarker = "epoch-bump-target"
	const targetStmt = "SELECT '" + targetMarker + "'"

	// fired gates the bump so it happens exactly once across all retry
	// iterations of the outer txn. bumpErr holds the result of the bump so
	// the test can confirm a retryable error was actually installed.
	type bumpResult struct{ err error }
	var outerTxn atomic.Pointer[kv.Txn]
	var fired atomic.Bool
	var bumpErr atomic.Pointer[bumpResult]

	knobs := &sql.ExecutorTestingKnobs{
		AfterExecute: func(ctx context.Context, stmt string, isInternal bool, _ error) {
			if !isInternal || !strings.Contains(stmt, targetMarker) {
				return
			}
			txn := outerTxn.Load()
			if txn == nil {
				return
			}
			if !fired.CompareAndSwap(false, true) {
				return
			}
			bumpErr.Store(&bumpResult{
				err: txn.GenerateForcedRetryableErr(ctx, "force epoch bump"),
			})
		},
	}

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{SQLExecutor: knobs},
	})
	defer srv.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, `CREATE TABLE defaultdb.public.t (a INT PRIMARY KEY)`)

	idb := srv.ApplicationLayer().InternalDB().(isql.DB)
	err := idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		outerTxn.Store(txn.KV())

		// Seed write under the outer txn so writeSeq > 0 by the time the
		// target statement captures prevSeqNum.
		if _, err := txn.ExecEx(
			ctx, "seed", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`INSERT INTO defaultdb.public.t VALUES (1)`,
		); err != nil {
			return err
		}

		_, err := txn.QueryRowEx(
			ctx, "target", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			targetStmt,
		)
		return err
	})

	require.NoError(t, err)

	result := bumpErr.Load()
	require.NotNil(t, result, "AfterExecute knob did not fire; test did not exercise the epoch gate")
	var retryErr *kvpb.TransactionRetryWithProtoRefreshError
	require.ErrorAs(t, result.err, &retryErr,
		"GenerateForcedRetryableErr did not install a retryable error; test would no longer exercise the epoch gate")
}

// TestEpochBumpReadSeqRestore_Prepare covers the execPrepare call site of
// stepReadSequenceWithRestore (the call site the issue's production stack traces
// hit). An internal executor running under an outer txn routes any statement
// carrying query arguments through the prepare path (see internal.go, which
// pushes a PrepareStmt rather than a bare ExecStmt for arg-bearing statements).
// While the arg-bearing target statement is being prepared, BeforePrepare forces
// a retryable error on the shared outer txn, bumping the epoch (writeSeq to 0)
// inside the read-sequence capture/restore window; the epoch gate must then skip
// the restore instead of tripping the readSeq <= writeSeq invariant.
//
// The target statement carries a unique alias so the hook can identify it and
// fire exactly once, ignoring any other internal prepares under the outer txn.
func TestEpochBumpReadSeqRestore_Prepare(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// targetMarker is an alias on the target statement so the hook can recognize
	// the prepare we care about. The placeholder forces the internal executor
	// down the prepare path.
	const targetMarker = "epoch_bump_prepare_target"
	const targetStmt = "SELECT $1::STRING AS " + targetMarker

	// fired gates the bump so it happens exactly once across all retry iterations
	// of the outer txn. bumpErr holds the result so the test can confirm a
	// retryable error was actually installed.
	type bumpResult struct{ err error }
	var outerTxn atomic.Pointer[kv.Txn]
	var fired atomic.Bool
	var bumpErr atomic.Pointer[bumpResult]

	knobs := &sql.ExecutorTestingKnobs{
		BeforePrepare: func(ctx context.Context, stmt string, txn *kv.Txn) error {
			if txn == nil || !strings.Contains(stmt, targetMarker) {
				return nil
			}
			outer := outerTxn.Load()
			if outer == nil || txn.ID() != outer.ID() {
				return nil
			}
			if !fired.CompareAndSwap(false, true) {
				return nil
			}
			bumpErr.Store(&bumpResult{
				err: txn.GenerateForcedRetryableErr(ctx, "force epoch bump in prepare window"),
			})
			return nil
		},
	}

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{SQLExecutor: knobs},
	})
	defer srv.Stopper().Stop(ctx)

	r := sqlutils.MakeSQLRunner(sqlDB)
	r.Exec(t, `CREATE TABLE defaultdb.public.t (a INT PRIMARY KEY)`)

	idb := srv.ApplicationLayer().InternalDB().(isql.DB)
	err := idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		outerTxn.Store(txn.KV())

		// Seed a write under the outer txn so writeSeq > 0 by the time the
		// target statement captures prevSeqNum. This statement has no args, so
		// it takes the bare-exec path and does not trigger BeforePrepare.
		if _, err := txn.ExecEx(
			ctx, "seed", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			`INSERT INTO defaultdb.public.t VALUES (1)`,
		); err != nil {
			return err
		}

		// The argument routes this statement through the internal executor's
		// prepare path, where BeforePrepare fires inside the capture/restore
		// window.
		_, err := txn.QueryRowEx(
			ctx, "target", txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
			targetStmt, "marker-value",
		)
		return err
	})

	require.NoError(t, err)

	result := bumpErr.Load()
	require.NotNil(t, result,
		"BeforePrepare knob did not fire; test did not exercise the prepare-path epoch gate")
	var retryErr *kvpb.TransactionRetryWithProtoRefreshError
	require.ErrorAs(t, result.err, &retryErr,
		"GenerateForcedRetryableErr did not install a retryable error; "+
			"test would no longer exercise the epoch gate")
}
