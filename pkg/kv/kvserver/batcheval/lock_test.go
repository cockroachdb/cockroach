// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// instrumentedEngine wraps a storage.Engine and allows for various methods in
// the interface to be instrumented for testing purposes.
type instrumentedEngine struct {
	storage.Engine

	onNewIterator func(storage.IterOptions)
	// ... can be extended ...
}

func (ie *instrumentedEngine) NewMVCCIterator(
	ctx context.Context, iterKind storage.MVCCIterKind, opts storage.IterOptions,
) (storage.MVCCIterator, error) {
	if ie.onNewIterator != nil {
		ie.onNewIterator(opts)
	}
	return ie.Engine.NewMVCCIterator(ctx, iterKind, opts)
}

// TestCollectIntentsUsesSameIterator tests that all uses of CollectIntents
// (currently only by READ_UNCOMMITTED Gets, Scans, and ReverseScans) use the
// same cached iterator (prefix or non-prefix) for their initial read and their
// provisional value collection for any intents they find.
func TestCollectIntentsUsesSameIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	key := roachpb.Key("key")
	ts := hlc.Timestamp{WallTime: 123}
	header := kvpb.Header{
		Timestamp:       ts,
		ReadConsistency: kvpb.READ_UNCOMMITTED,
	}
	evalCtx := (&MockEvalCtx{ClusterSettings: cluster.MakeTestingClusterSettings()}).EvalContext()

	testCases := []struct {
		name              string
		run               func(*testing.T, storage.ReadWriter) (intents []roachpb.KeyValue, _ error)
		expPrefixIters    int
		expNonPrefixIters int
	}{
		{
			name: "get",
			run: func(t *testing.T, db storage.ReadWriter) ([]roachpb.KeyValue, error) {
				req := &kvpb.GetRequest{
					RequestHeader: kvpb.RequestHeader{Key: key},
				}
				var resp kvpb.GetResponse
				if _, err := Get(ctx, db, CommandArgs{Args: req, Header: header, EvalCtx: evalCtx}, &resp); err != nil {
					return nil, err
				}
				if resp.IntentValue == nil {
					return nil, nil
				}
				return []roachpb.KeyValue{{Key: key, Value: *resp.IntentValue}}, nil
			},
			expPrefixIters:    2,
			expNonPrefixIters: 0,
		},
		{
			name: "scan",
			run: func(t *testing.T, db storage.ReadWriter) ([]roachpb.KeyValue, error) {
				req := &kvpb.ScanRequest{
					RequestHeader: kvpb.RequestHeader{Key: key, EndKey: key.Next()},
				}
				var resp kvpb.ScanResponse
				if _, err := Scan(ctx, db, CommandArgs{Args: req, Header: header, EvalCtx: evalCtx}, &resp); err != nil {
					return nil, err
				}
				return resp.IntentRows, nil
			},
			expPrefixIters:    0,
			expNonPrefixIters: 2,
		},
		{
			name: "reverse scan",
			run: func(t *testing.T, db storage.ReadWriter) ([]roachpb.KeyValue, error) {
				req := &kvpb.ReverseScanRequest{
					RequestHeader: kvpb.RequestHeader{Key: key, EndKey: key.Next()},
				}
				var resp kvpb.ReverseScanResponse
				if _, err := ReverseScan(ctx, db, CommandArgs{Args: req, Header: header, EvalCtx: evalCtx}, &resp); err != nil {
					return nil, err
				}
				return resp.IntentRows, nil
			},
			expPrefixIters:    0,
			expNonPrefixIters: 2,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// Test with and without deletion intents. If a READ_UNCOMMITTED request
			// encounters an intent whose provisional value is a deletion tombstone,
			// the request should ignore the intent and should not return any
			// corresponding intent row.
			testutils.RunTrueAndFalse(t, "deletion intent", func(t *testing.T, delete bool) {
				db := &instrumentedEngine{Engine: storage.NewDefaultInMemForTesting()}
				defer db.Close()

				// Write an intent.
				val := roachpb.MakeValueFromBytes([]byte("val"))
				txn := roachpb.MakeTransaction("test", key, isolation.Serializable, roachpb.NormalUserPriority, ts, 0, 1, 0, false /* omitInRangefeeds */)
				var err error
				if delete {
					_, _, err = storage.MVCCDelete(ctx, db, key, ts, storage.MVCCWriteOptions{Txn: &txn})
				} else {
					_, err = storage.MVCCPut(ctx, db, key, ts, val, storage.MVCCWriteOptions{Txn: &txn})
				}
				require.NoError(t, err)

				// Instrument iterator creation, count prefix vs. non-prefix iters.
				var prefixIters, nonPrefixIters int
				db.onNewIterator = func(opts storage.IterOptions) {
					if opts.Prefix {
						prefixIters++
					} else {
						nonPrefixIters++
					}
				}

				intents, err := c.run(t, db)
				require.NoError(t, err)

				// Assert proper intent values.
				if delete {
					require.Len(t, intents, 0)
				} else {
					expIntentVal := val
					expIntentVal.Timestamp = ts
					expIntentKeyVal := roachpb.KeyValue{Key: key, Value: expIntentVal}
					require.Len(t, intents, 1)
					require.Equal(t, expIntentKeyVal, intents[0])
				}

				// Assert proper iterator use.
				require.Equal(t, c.expPrefixIters, prefixIters)
				require.Equal(t, c.expNonPrefixIters, nonPrefixIters)
				require.Equal(t, c.expNonPrefixIters, nonPrefixIters)
			})
		})
	}
}

func TestTxnBoundReplicatedLockTableView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()

	engine, err := storage.Open(ctx, storage.InMemory(), st,
		storage.CacheSize(1<<20 /* 1 MiB */),
	)
	require.NoError(t, err)
	defer engine.Close()

	makeTS := func(nanos int64, logical int32) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: nanos,
			Logical:  logical,
		}
	}

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")
	txn1 := roachpb.MakeTransaction("txn1", keyA, isolation.Serializable, roachpb.NormalUserPriority, makeTS(100, 0), 0, 0, 0, false)
	txn2 := roachpb.MakeTransaction("txn2", keyA, isolation.Serializable, roachpb.NormalUserPriority, makeTS(100, 0), 0, 0, 0, false)

	// Have txn1 acquire 2 locks with different strengths.
	err = storage.MVCCAcquireLock(ctx, engine, &txn1.TxnMeta, txn1.IgnoredSeqNums, lock.Exclusive, keyA, nil, 0, 0)
	require.NoError(t, err)
	err = storage.MVCCAcquireLock(ctx, engine, &txn1.TxnMeta, txn1.IgnoredSeqNums, lock.Shared, keyB, nil, 0, 0)
	require.NoError(t, err)

	reader := engine.NewReader(storage.StandardDurability)
	defer reader.Close()

	txn1LTView := newTxnBoundReplicatedLockTableView(reader, &txn1)
	txn2LTView := newTxnBoundReplicatedLockTableView(reader, &txn2)

	// 1. Lock strength Exclusive:
	// 1a. KeyA:
	locked, txn, err := txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyA, lock.Exclusive)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyA, lock.Exclusive)
	require.NoError(t, err)
	require.True(t, locked)
	require.Equal(t, &txn1.TxnMeta, txn)
	// 1b. KeyB:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyB, lock.Exclusive)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyB, lock.Exclusive)
	require.NoError(t, err)
	require.True(t, locked)
	require.Equal(t, &txn1.TxnMeta, txn)
	// 1c. KeyC:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyC, lock.Exclusive)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyC, lock.Exclusive)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)

	// 2. Lock strength Shared:
	// 2a. KeyA:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyA, lock.Shared)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyA, lock.Shared)
	require.NoError(t, err)
	require.True(t, locked)
	require.Equal(t, &txn1.TxnMeta, txn)
	// 2b. KeyB:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyB, lock.Shared)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyB, lock.Shared)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	// 2c. KeyC:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyC, lock.Shared)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyC, lock.Shared)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)

	// 3. Lock strength None:
	// 3a. KeyA:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyA, lock.None)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyA, lock.None)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	// 3b. KeyB:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyB, lock.None)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyB, lock.None)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	// 3c. KeyC:
	locked, txn, err = txn1LTView.IsKeyLockedByConflictingTxn(ctx, keyC, lock.None)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
	locked, txn, err = txn2LTView.IsKeyLockedByConflictingTxn(ctx, keyC, lock.None)
	require.NoError(t, err)
	require.False(t, locked)
	require.Nil(t, txn)
}

func TestRequestBoundLockTableView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "replicatedLockConflicts", func(t *testing.T, replicatedLockConflicts bool) {
		lockHolderTxnID := uuid.MakeV4()
		keyA := roachpb.Key("a")
		keyB := roachpb.Key("b")
		keyC := roachpb.Key("c")

		m := newMockTxnBoundLockTableView(lockHolderTxnID)
		m.addLock(keyA, lock.Shared)
		m.addLock(keyB, lock.Exclusive)
		mockTxnBoundReplicatedLTV := newMockTxnBoundReplicatedLockTableView(replicatedLockConflicts)

		ctx := context.Background()

		// Non-locking request.
		ltView := makeRequestBoundLockTableView(m, mockTxnBoundReplicatedLTV, lock.None)
		locked, _, err := ltView.IsKeyLockedByConflictingTxn(ctx, keyA)
		require.NoError(t, err)
		require.False(t, locked)

		locked, _, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyB)
		require.NoError(t, err)
		require.False(t, locked)

		locked, _, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyC)
		require.NoError(t, err)
		require.False(t, locked)

		// Shared locking request.
		ltView = makeRequestBoundLockTableView(m, mockTxnBoundReplicatedLTV, lock.Shared)
		locked, _, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyA)
		require.NoError(t, err)
		require.Equal(t, locked, replicatedLockConflicts)

		locked, txn, err := ltView.IsKeyLockedByConflictingTxn(ctx, keyB)
		require.NoError(t, err)
		require.True(t, locked)
		require.Equal(t, txn.ID, lockHolderTxnID)

		locked, _, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyC)
		require.NoError(t, err)
		require.Equal(t, locked, replicatedLockConflicts)

		// Exclusive locking request.
		ltView = makeRequestBoundLockTableView(m, mockTxnBoundReplicatedLTV, lock.Exclusive)
		locked, txn, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyA)
		require.NoError(t, err)
		require.True(t, locked)
		require.Equal(t, txn.ID, lockHolderTxnID)

		locked, txn, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyB)
		require.NoError(t, err)
		require.True(t, locked)
		require.Equal(t, txn.ID, lockHolderTxnID)

		locked, _, err = ltView.IsKeyLockedByConflictingTxn(ctx, keyC)
		require.NoError(t, err)
		require.Equal(t, locked, replicatedLockConflicts)
	})
}

// mockTxnBoundLockTableView is a mocked version of the txnBoundLockTableView
// interface.
type mockTxnBoundLockTableView struct {
	locks           map[string]lock.Strength
	lockHolderTxnID uuid.UUID // txnID of all held locks
}

var _ txnBoundLockTableView = &mockTxnBoundLockTableView{}

// newMockTxnBoundLockTableView constructs and returns a
// mockTxnBoundLockTableView.
func newMockTxnBoundLockTableView(lockHolderTxnID uuid.UUID) *mockTxnBoundLockTableView {
	return &mockTxnBoundLockTableView{
		locks:           make(map[string]lock.Strength),
		lockHolderTxnID: lockHolderTxnID,
	}
}

// addLock adds a lock on the supplied key with the given lock strength. The
// lock is held by m.TxnID.
func (m mockTxnBoundLockTableView) addLock(key roachpb.Key, str lock.Strength) {
	m.locks[key.String()] = str
}

// IsKeyLockedByConflictingTxn implements the txnBoundLockTableView interface.
func (m mockTxnBoundLockTableView) IsKeyLockedByConflictingTxn(
	_ context.Context, key roachpb.Key, str lock.Strength,
) (bool, *enginepb.TxnMeta, error) {
	lockStr, locked := m.locks[key.String()]
	if !locked {
		return false, nil, nil
	}
	var conflicts bool
	switch str {
	case lock.None:
		conflicts = false
		return false, nil, nil
	case lock.Shared:
		conflicts = lockStr == lock.Exclusive
	case lock.Exclusive:
		conflicts = true
	default:
		panic("unknown lock strength")
	}
	if conflicts {
		return true, &enginepb.TxnMeta{ID: m.lockHolderTxnID}, nil
	}
	return false, nil, nil
}

type mockTxnBoundReplicatedLockTableView bool

func newMockTxnBoundReplicatedLockTableView(conflicts bool) txnBoundLockTableView {
	return mockTxnBoundReplicatedLockTableView(conflicts)
}

func (m mockTxnBoundReplicatedLockTableView) IsKeyLockedByConflictingTxn(
	_ context.Context, _ roachpb.Key, str lock.Strength,
) (bool, *enginepb.TxnMeta, error) {
	if str == lock.None { // non-locking reads don't conflict with replicated locks
		return false, nil, nil
	}
	return bool(m), &enginepb.TxnMeta{ID: uuid.Max}, nil
}
