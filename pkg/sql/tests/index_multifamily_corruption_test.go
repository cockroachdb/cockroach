// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestReadCommittedMultiFamilyIndexLockLoss is a regression test for issue
// #173610 on a NON-partial secondary index that STORES a column from a
// different column family than its key. Unlike the logic tests, which
// approximate lock loss by disabling implicit FOR UPDATE locking and rely on
// pg_sleep windows, this drops a real unreplicated lock with a range split and
// traps the write batch, so the interleaving is deterministic.
//
// Sequence (row starts as id=1, status='A', claim=NULL):
//  1. Txn A (READ COMMITTED) UPDATE ... SET status='B'. At its snapshot 'claim'
//     is NULL, so its old-key delete-set is just the f0 sentinel (no fclaim
//     KV). Its write batch is trapped before it evaluates.
//  2. The range is split at the row, dropping A's unreplicated lock.
//  3. Txn B (serializable) UPDATE ... SET claim='c2'. The row's status is still
//     'A' to B, so B adds /idx/'A'/fclaim='c2' to the OLD index key.
//  4. A is released and evaluates at its pre-B snapshot.
//
// Without the forced conflict write, step 4 deletes /idx/'A'/f0 and writes
// /idx/'B'/f0 with a stale NULL claim, but never deletes /idx/'A'/fclaim
// because it never knew that KV existed. The orphan survives: an index entry
// for status='A' that the primary no longer holds.
//
// The fix has A write the fclaim family of the primary row as well — a Del,
// since claim is NULL at A's snapshot — which lands on the same key B wrote. A
// is pushed, re-reads, and rebuilds the index entry from claim='c2'.
func TestReadCommittedMultiFamilyIndexLockLoss(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var trapArmed atomic.Bool
	var tablePrefix atomic.Value // roachpb.Key
	var trappedOnce atomic.Bool
	trapped := make(chan struct{})
	release := make(chan struct{})

	filter := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if !trapArmed.Load() {
			return nil
		}
		if ba.Txn == nil || !ba.Txn.IsoLevel.PerStatementReadSnapshot() {
			return nil
		}
		prefixVal := tablePrefix.Load()
		if prefixVal == nil {
			return nil
		}
		prefix := prefixVal.(roachpb.Key)
		writesTable := false
		for _, ru := range ba.Requests {
			req := ru.GetInner()
			switch req.Method() {
			case kvpb.Put, kvpb.ConditionalPut:
				if bytes.HasPrefix(req.Header().Key, prefix) {
					writesTable = true
				}
			}
		}
		if !writesTable {
			return nil
		}
		if trappedOnce.CompareAndSwap(false, true) {
			close(trapped)
			// Give up on ctx cancellation so that a failure before the test releases
			// the trap surfaces as that failure rather than wedging the stopper.
			select {
			case <-release:
			case <-ctx.Done():
				return kvpb.NewError(ctx.Err())
			}
		}
		return nil
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{TestingRequestFilter: filter},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Model a release without unreplicated-lock reliability: a range split
	// drops the mutation's best-effort lock rather than carrying it over.
	for _, stmt := range []string{
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.split.enabled = false`,
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.lease_transfer.enabled = false`,
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.merge.enabled = false`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	// A nullable index key (status) in family f0 and a racing STORING column
	// (claim) in its own family fclaim. The secondary index is NON-partial.
	for _, stmt := range []string{
		`CREATE TABLE t (
			id     INT PRIMARY KEY,
			status STRING,
			claim  STRING,
			FAMILY f0 (id, status),
			FAMILY fclaim (claim),
			INDEX idx (status) STORING (claim)
		)`,
		`INSERT INTO t (id, status) VALUES (1, 'A')`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	var tableID uint32
	require.NoError(t, db.QueryRow(`SELECT 't'::regclass::oid`).Scan(&tableID))
	tablePrefix.Store(s.Codec().TablePrefix(tableID))

	connA, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = connA.Close() }()

	_, err = connA.ExecContext(ctx, `SET kv_transaction_buffered_writes_enabled = false`)
	require.NoError(t, err)
	_, err = connA.ExecContext(ctx, `SET enable_implicit_select_for_update = true`)
	require.NoError(t, err)

	_, err = connA.ExecContext(ctx, `BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED`)
	require.NoError(t, err)

	trapArmed.Store(true)
	updateErrC := make(chan error, 1)
	updateDone := make(chan struct{})
	go func() {
		defer close(updateDone)
		_, err := connA.ExecContext(ctx, `UPDATE t SET status = 'B' WHERE id = 1`)
		updateErrC <- err
	}()

	// Registered after the stopper's defer so that LIFO runs it first: the trap
	// must be released and txn A joined before the server shuts down, on the
	// failure paths as well as the happy one.
	releaseTrap := sync.OnceFunc(func() { close(release) })
	defer func() {
		releaseTrap()
		select {
		case <-updateDone:
		case <-time.After(45 * time.Second):
			t.Error("txn A's UPDATE did not finish after the trap was released")
		}
	}()

	select {
	case <-trapped:
	case err := <-updateErrC:
		t.Fatalf("update finished without being trapped: %v", err)
	case <-time.After(45 * time.Second):
		t.Fatal("timed out waiting for txn A's write to be trapped")
	}

	// Split at the row: with reliability disabled, this drops A's lock.
	_, err = db.Exec(`ALTER TABLE t SPLIT AT VALUES (1)`)
	require.NoError(t, err)

	// Txn B writes only the claim family; not blocked now that A's lock is gone.
	// Bound it: if the split ever stops dropping A's lock, B blocks on it while A
	// waits for the trap to be released, and the two deadlock.
	ctxB, cancelB := context.WithTimeout(ctx, 45*time.Second)
	defer cancelB()
	_, err = db.ExecContext(ctxB, `UPDATE t SET claim = 'c2' WHERE id = 1`)
	require.NoError(t, err, "txn B was not unblocked by the split")

	// Release A's trapped write; it evaluates at its pre-B snapshot.
	releaseTrap()
	select {
	case err := <-updateErrC:
		require.NoError(t, err)
	case <-time.After(45 * time.Second):
		t.Fatal("timed out waiting for txn A's update to finish")
	}

	_, err = connA.ExecContext(ctx, `COMMIT`)
	require.NoError(t, err)

	// Primary row (source of truth).
	var pStatus, pClaim string
	require.NoError(t, db.QueryRow(
		`SELECT status, claim FROM t@t_pkey WHERE id = 1`,
	).Scan(&pStatus, &pClaim))
	require.Equal(t, "B", pStatus)
	require.Equal(t, "c2", pClaim)

	// Secondary index (must agree with the primary). Without the fix the orphaned
	// /idx/'A'/fclaim entry sorts first, so this reads status='A' — a value the
	// primary no longer holds.
	var iStatus string
	var iClaimNull []byte
	require.NoError(t, db.QueryRow(
		`SELECT status, claim FROM t@idx WHERE id = 1`,
	).Scan(&iStatus, &iClaimNull))
	iClaim := "NULL"
	if iClaimNull != nil {
		iClaim = string(iClaimNull)
	}

	t.Logf("primary: status=%s claim=%s", pStatus, pClaim)
	t.Logf("index:   status=%s claim=%s", iStatus, iClaim)

	require.Equalf(t, fmt.Sprintf("%s/%s", pStatus, pClaim),
		fmt.Sprintf("%s/%s", iStatus, iClaim),
		"secondary index disagrees with the primary on a non-partial multi-family "+
			"STORING index under READ COMMITTED, which means the claim family was "+
			"not force-written (issue #173610)")
}
