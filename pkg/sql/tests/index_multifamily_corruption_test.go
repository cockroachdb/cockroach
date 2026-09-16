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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
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

// TestReadCommittedFastPathDeletePrimaryCorruption is a regression test for
// issue #175530: PRIMARY-index corruption from the DELETE-range fast path
// racing a READ COMMITTED UPDATE that materializes a previously-absent column
// family. Unlike the logic tests, which approximate lock loss by disabling
// implicit FOR UPDATE locking and rely on pg_sleep windows, this drops a real
// unreplicated lock with a range split and traps the write batch, so the
// interleaving is deterministic.
//
// The DELETE fast path (a KV DeleteRange, used only when the table has a single
// index — see canUseDeleteRange in execbuilder/mutation.go) removes a row by
// scanning the present family KVs and deleting them; it writes nothing at the
// key of a currently-absent family (a nullable column that is NULL).
//
// Sequence (row starts as id=1, biz='b1' NOT NULL, claim=NULL, fclaim absent):
//  1. Txn A (READ COMMITTED) UPDATE ... SET claim='c2' derives a Put of the
//     previously-absent fclaim family under a best-effort unreplicated lock.
//     Its write batch is trapped before it evaluates.
//  2. The range is split at the row, dropping A's unreplicated lock.
//  3. Txn B DELETE FROM t WHERE id=1 via the fast path deletes f0 and fbiz, but
//     not the absent fclaim key.
//  4. A is released and evaluates at its pre-B snapshot.
//
// Without the forced sentinel write, step 4 lands Put(fclaim='c2') with no
// WriteTooOld (no version at that key conflicts) and A commits, leaving the row
// with fclaim only — no sentinel and no NOT NULL fbiz family — a corrupt
// primary index. Row-by-row deletes (used whenever the table has a secondary
// index) Del every family key, so they conflict with the Put and avoid this.
//
// The fix has A also write family 0 (the row sentinel), which lands on the same
// key B's delete wrote. A hits an error, re-reads, finds the row deleted, and
// affects no rows.
func TestReadCommittedFastPathDeletePrimaryCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var trapArmed atomic.Bool
	var tablePrefix atomic.Value // roachpb.Key
	var trappedOnce atomic.Bool
	trapped := make(chan struct{})
	release := make(chan struct{})

	filter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
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
			<-release
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

	// Model a release without unreplicated-lock reliability: a range split drops
	// the mutation's best-effort lock rather than carrying it over.
	for _, stmt := range []string{
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.split.enabled = false`,
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.lease_transfer.enabled = false`,
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.merge.enabled = false`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	// A NOT NULL column (biz) and a nullable column (claim), each in its own
	// family. The table has no secondary indexes, so DELETE uses the
	// delete-range fast path.
	for _, stmt := range []string{
		`CREATE TABLE t (
			id    INT PRIMARY KEY,
			biz   STRING NOT NULL,
			claim STRING,
			FAMILY f0 (id),
			FAMILY fbiz (biz),
			FAMILY fclaim (claim)
		)`,
		`INSERT INTO t (id, biz) VALUES (1, 'b1')`,
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
		// Adds the previously-absent fclaim family.
		_, err := connA.ExecContext(ctx, `UPDATE t SET claim = 'c2' WHERE id = 1`)
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

	// Fast-path DELETE: removes f0 and fbiz, but not the absent fclaim key.
	_, err = db.Exec(`DELETE FROM t WHERE id = 1`)
	require.NoError(t, err)

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

	// A full scan of the primary index decodes the row.
	_, scanErr := db.Exec(`SELECT id, biz, claim FROM t`)
	require.NoErrorf(t, scanErr,
		"fast-path DELETE racing a READ COMMITTED add-family UPDATE left a partial "+
			"row in the primary index (missing the NOT NULL fbiz family), which means "+
			"the row sentinel was not force-written (issue #175530)")
}

// TestReadCommittedFastPathDeleteDescriptorVersionSkew is a regression test for
// the cross-statement descriptor-version skew that could defeat the forced
// sentinel write for issue #175530. It is a sibling of
// TestReadCommittedFastPathDeletePrimaryCorruption above: same would-be
// corruption (primary-index partial row) via the same fast-path DELETE vs.
// materializing READ COMMITTED UPDATE race, but here the UPDATE and DELETE
// deliberately run against two different leased descriptor versions of the same
// table, one with a secondary index and one without (see
// maybeForceSentinelWrite for why it does not gate on the index count). The
// window is bounded by how long a transaction holds a lease on the old version
// after the new one is published, not by the index-build/backfill time (the
// backfill runs only after the schema changer has drained all old-version
// leases).
//
// Sequence (row starts as id=1, biz='b1' NOT NULL, claim=NULL, fclaim absent):
//  1. Txn B (SERIALIZABLE) opens and reads t, pinning a lease on version V, at
//     which the table has no secondary index.
//  2. CREATE INDEX runs and is paused at the first post-commit stage. Version
//     V+1 is now committed with the new (and temporary) index in a mutation
//     state, so DeletableIndexCount() > 1 there.
//  3. Txn A (READ COMMITTED) leases V+1 and UPDATEs claim='c2'. Materializing
//     the absent fclaim family, it also force-writes family 0 (the sentinel)
//     regardless of the index count. Its write batch is trapped before it
//     evaluates.
//  4. The range is split at the row, dropping A's unreplicated lock.
//  5. Txn B, still pinned to V (count 1), DELETEs the row via the fast path:
//     it removes f0 and fbiz but not the absent fclaim key, then commits.
//  6. A is released. Its forced family-0 write lands on the same key B deleted,
//     so A conflicts, re-reads, finds the row gone, and affects no rows.
//
// If the sentinel write wasn't forced, A on V+1 would leave the same corrupt
// partial row (fclaim only, no sentinel, no NOT NULL fbiz) as
// TestReadCommittedFastPathDeletePrimaryCorruption.
func TestReadCommittedFastPathDeleteDescriptorVersionSkew(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var trapArmed atomic.Bool
	var tablePrefix atomic.Value // roachpb.Key
	var trappedOnce atomic.Bool
	trapped := make(chan struct{})
	release := make(chan struct{})

	filter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
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
			<-release
		}
		return nil
	}

	// Machinery to pause the CREATE INDEX schema change at its first post-commit
	// stage, at which point version V+1 (index as a mutation) is committed and
	// leasable but the schema changer has not yet advanced further.
	var scArmed atomic.Bool
	var scTableID atomic.Uint32
	var scPausedOnce atomic.Bool
	scReached := make(chan struct{})
	scRelease := make(chan struct{})

	scKnobs := &scexec.TestingKnobs{
		BeforeStage: func(p scplan.Plan, stageIdx int) error {
			if !scArmed.Load() || p.InRollback {
				return nil
			}
			stage := p.Stages[stageIdx]
			if stage.Phase != scop.PostCommitPhase || stage.Ordinal != 1 {
				return nil
			}
			forThisTable := false
			for _, tgt := range p.Targets {
				if uint32(screl.GetDescID(tgt.Element())) == scTableID.Load() {
					forThisTable = true
					break
				}
			}
			if !forThisTable {
				return nil
			}
			if scPausedOnce.CompareAndSwap(false, true) {
				close(scReached)
				<-scRelease
			}
			return nil
		},
	}

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store:                       &kvserver.StoreTestingKnobs{TestingRequestFilter: filter},
			SQLDeclarativeSchemaChanger: scKnobs,
		},
	})
	defer s.Stopper().Stop(ctx)

	// Model a release without unreplicated-lock reliability: a range split drops
	// the mutation's best-effort lock rather than carrying it over.
	for _, stmt := range []string{
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.split.enabled = false`,
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.lease_transfer.enabled = false`,
		`SET CLUSTER SETTING kv.lock_table.unreplicated_lock_reliability.merge.enabled = false`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	// A NOT NULL column (biz) and a nullable column (claim), each in its own
	// family. The table starts with no secondary index; CREATE INDEX below adds
	// one on biz.
	for _, stmt := range []string{
		`CREATE TABLE t (
			id    INT PRIMARY KEY,
			biz   STRING NOT NULL,
			claim STRING,
			FAMILY f0 (id),
			FAMILY fbiz (biz),
			FAMILY fclaim (claim)
		)`,
		`INSERT INTO t (id, biz) VALUES (1, 'b1')`,
	} {
		_, err := db.Exec(stmt)
		require.NoError(t, err)
	}

	var tableID uint32
	require.NoError(t, db.QueryRow(`SELECT 't'::regclass::oid`).Scan(&tableID))
	tablePrefix.Store(s.Codec().TablePrefix(tableID))
	scTableID.Store(tableID)

	// Txn B pins a lease on the current (no-secondary-index) version V by
	// reading t inside an explicit transaction and keeping it open. A descriptor
	// collection caches the leased version for the whole transaction, so B's
	// later DELETE still plans against V even after CREATE INDEX publishes V+1.
	connB, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = connB.Close() }()
	_, err = connB.ExecContext(ctx, `SET kv_transaction_buffered_writes_enabled = false`)
	require.NoError(t, err)
	_, err = connB.ExecContext(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = connB.ExecContext(ctx, `SELECT count(*) FROM t WHERE id = 999`)
	require.NoError(t, err)

	// Launch CREATE INDEX; it pauses at its first post-commit stage.
	scArmed.Store(true)
	createIdxErrC := make(chan error, 1)
	go func() {
		connSC, err := db.Conn(ctx)
		if err != nil {
			createIdxErrC <- err
			return
		}
		defer func() { _ = connSC.Close() }()
		_, err = connSC.ExecContext(ctx, `CREATE INDEX idx ON t (biz)`)
		createIdxErrC <- err
	}()

	// Registered after the stopper's defer so that LIFO runs it first: the
	// traps must be released, txn A and CREATE INDEX joined before the server
	// shuts down, on the failure paths as well as the happy one.
	scReleaseOnce := sync.OnceFunc(func() { close(scRelease) })
	defer func() {
		scReleaseOnce()
		select {
		case <-createIdxErrC:
		case <-time.After(45 * time.Second):
			t.Error("CREATE INDEX did not finish after the schema change was released")
		}
	}()
	releaseTrap := sync.OnceFunc(func() { close(release) })
	updateErrC := make(chan error, 1)
	updateDone := make(chan struct{})
	defer func() {
		releaseTrap()
		select {
		case <-updateDone:
		case <-time.After(45 * time.Second):
			t.Error("txn A's UPDATE did not finish after the trap was released")
		}
	}()

	select {
	case <-scReached:
	case err := <-createIdxErrC:
		t.Fatalf("CREATE INDEX finished before reaching the paused stage: %v", err)
	case <-time.After(45 * time.Second):
		t.Fatal("timed out waiting for CREATE INDEX to reach the first post-commit stage")
	}

	// Txn A leases V+1 (index in a mutation state, DeletableIndexCount() > 1) and
	// materializes the absent fclaim family, force-writing family 0 despite the
	// index count. Its write batch (the fclaim Put and the family-0 write) is
	// trapped.
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
	go func() {
		defer close(updateDone)
		_, err := connA.ExecContext(ctx, `UPDATE t SET claim = 'c2' WHERE id = 1`)
		updateErrC <- err
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

	// Txn B, still on version V (no secondary index, count 1), deletes via the
	// fast path: it removes f0 and fbiz but not the absent fclaim key.
	_, err = connB.ExecContext(ctx, `DELETE FROM t WHERE id = 1`)
	require.NoError(t, err)
	_, err = connB.ExecContext(ctx, `COMMIT`)
	require.NoError(t, err)

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

	// A full scan of the primary index decodes the row.
	_, scanErr := db.Exec(`SELECT id, biz, claim FROM t`)
	require.NoErrorf(t, scanErr,
		"a fast-path DELETE on descriptor version V raced a READ COMMITTED "+
			"add-family UPDATE on version V+1 (index mid-add) and left a partial row "+
			"in the primary index: the sentinel write must be forced on multi-family "+
			"tables regardless of DeletableIndexCount(), which the two-version lease "+
			"skew would otherwise defeat")
}
