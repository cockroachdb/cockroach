// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func beginTransaction(
	t *testing.T, store *Store, pri roachpb.UserPriority, key roachpb.Key, putKey bool,
) *roachpb.Transaction {
	txn := newTransaction("test", key, pri, store.Clock())
	if !putKey {
		return txn
	}

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	ba.Add(&put)
	assignSeqNumsForReqs(txn, &put)
	br, pErr := store.TestSender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	return br.Txn
}

// TestContendedIntentWithDependencyCycle verifies that a queue of
// writers on a contended key will still notice a dependency cycle.
// In this case, txn3 writes "a", then txn1 writes "b" and "a", then
// txn2 writes "b", then txn3 writes "b". The deadlock is broken by
// an aborted transaction.
//
// Additional non-transactional reads on the same contended key are
// inserted to verify they do not interfere with writing transactions
// always pushing to ensure the dependency cycle can be detected.
//
// This test is something of an integration test which exercises the
// IntentResolver as well as the concurrency Manager's lockTable and
// txnWaitQueue.
func TestContendedIntentWithDependencyCycle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: true}, stopper)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	spanA := roachpb.Span{Key: keyA}
	spanB := roachpb.Span{Key: keyB}

	// Create the three transactions; at this point, none of them have
	// conflicts. Txn1 has written "b", Txn3 has written "a".
	txn1 := beginTransaction(t, store, -3, keyB, true /* putKey */)
	txn2 := beginTransaction(t, store, -2, keyB, false /* putKey */)
	txn3 := beginTransaction(t, store, -1, keyA, true /* putKey */)

	// Send txn1 put, followed by an end transaction.
	txnCh1 := make(chan error, 1)
	go func() {
		put := putArgs(keyA, []byte("value"))
		assignSeqNumsForReqs(txn1, &put)
		if _, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn1}, &put); pErr != nil {
			txnCh1 <- pErr.GoError()
			return
		}

		et, h := endTxnArgs(txn1, true)
		et.LockSpans = []roachpb.Span{spanA, spanB}
		assignSeqNumsForReqs(txn1, &et)
		h.CanForwardReadTimestamp = true
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), h, &et)
		txnCh1 <- pErr.GoError()
	}()

	// Send a non-transactional read to keyB. This adds an early waiter
	// to the intent resolver on keyB which txn2 must skip in order to
	// properly register itself as a dependency by pushing txn1.
	readCh1 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), &get)
		readCh1 <- pErr.GoError()
	}()

	// Send txn2 put, followed by an end transaction.
	txnCh2 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn2, &put)
		repl, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn2}, &put)
		if pErr != nil {
			txnCh2 <- pErr.GoError()
			return
		}

		txn2Copy := *repl.Header().Txn
		txn2 = &txn2Copy
		et, h := endTxnArgs(txn2, true)
		et.LockSpans = []roachpb.Span{spanB}
		assignSeqNumsForReqs(txn2, &et)
		h.CanForwardReadTimestamp = true
		_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), h, &et)
		txnCh2 <- pErr.GoError()
	}()

	// Send another non-transactional read to keyB to add a waiter in
	// between txn2 and txn3. Txn3 must wait on txn2, instead of getting
	// queued behind this reader, in order to establish the dependency cycle.
	readCh2 := make(chan error, 1)
	go func() {
		get := getArgs(keyB)
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), &get)
		readCh2 <- pErr.GoError()
	}()

	// Send txn3. Pause for 10ms to make it more likely that we have a
	// dependency cycle of length 3, although we don't mind testing
	// either way.
	time.Sleep(10 * time.Millisecond)
	txnCh3 := make(chan error, 1)
	go func() {
		put := putArgs(keyB, []byte("value"))
		assignSeqNumsForReqs(txn3, &put)
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Txn: txn3}, &put)
		txnCh3 <- pErr.GoError()
	}()

	// The third transaction will always be aborted.
	err := <-txnCh3
	if !errors.HasType(err, (*roachpb.UnhandledRetryableError)(nil)) {
		t.Fatalf("expected transaction aborted error; got %T", err)
	}
	if err := <-txnCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-txnCh2; err != nil {
		t.Fatal(err)
	}
	if err := <-readCh1; err != nil {
		t.Fatal(err)
	}
	if err := <-readCh2; err != nil {
		t.Fatal(err)
	}
}

// Regression test for https://github.com/cockroachdb/cockroach/issues/64092
// which makes sure that synchronous ranged intent resolution during rollback
// completes in a reasonable time.
func TestRollbackSyncRangedIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t, "timing-sensitive test")

	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &StoreTestingKnobs{
				DisableLoadBasedSplitting: true,
				IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
					ForceSyncIntentResolution: true,
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	txn := srv.DB().NewTxn(ctx, "test")
	batch := txn.NewBatch()
	for i := 0; i < 100000; i++ {
		batch.Put([]byte(fmt.Sprintf("key%v", i)), []byte("value"))
	}
	require.NoError(t, txn.Run(ctx, batch))
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, txn.Rollback(ctx))
	require.NoError(t, ctx.Err())
}

// Tests that intents and transaction records are cleaned up within a reasonable
// timeframe in various scenarios.
func TestReliableIntentCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 65447, "fixing the flake uncovered additional bugs in #65458")
	skip.UnderRace(t, "timing-sensitive test")
	skip.UnderStress(t, "multi-node test")

	testutils.RunTrueAndFalse(t, "ForceSyncIntentResolution", func(t *testing.T, sync bool) {
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		clusterArgs := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: settings,
				Knobs: base.TestingKnobs{
					Store: &StoreTestingKnobs{
						IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
							ForceSyncIntentResolution: sync,
						},
					},
				},
			},
		}
		tc := serverutils.StartNewTestCluster(t, 3, clusterArgs)
		defer tc.Stopper().Stop(ctx)

		srv := tc.Server(0)
		db := srv.DB()
		store, err := srv.GetStores().(*Stores).GetStore(srv.GetFirstStoreID())
		require.NoError(t, err)
		engine := store.Engine()
		clock := srv.Clock()

		// Set up a key prefix, and split off 16 ranges by the first hex digit (4
		// bits) following the prefix: key\x00\x00 key\x00\x10 key\x00\x20 ...
		prefix := roachpb.Key([]byte("key\x00"))
		for i := 0; i < 16; i++ {
			require.NoError(t, db.AdminSplit(ctx, append(prefix, byte(i<<4)), hlc.MaxTimestamp))
		}
		require.NoError(t, tc.WaitForFullReplication())

		// Set up random key generator which only generates unique keys.
		genKeySeen := map[string]bool{}
		genKey := func(singleRange bool) roachpb.Key {
			key := make([]byte, len(prefix)+4)
			copy(key, prefix)
			for {
				r := rand.Uint32()
				if singleRange {
					r = r >> 4 // zero out four first bits, puts key in first range
				}
				binary.BigEndian.PutUint32(key[len(prefix):], r)
				if !genKeySeen[string(key)] {
					genKeySeen[string(key)] = true
					return key
				}
			}
		}

		// assertIntentCleanup checks that intents get cleaned up within a
		// reasonable time.
		assertIntentCleanup := func(t *testing.T) {
			t.Helper()
			var result storage.MVCCScanResult
			if !assert.Eventually(t, func() bool {
				result, err = storage.MVCCScan(ctx, engine, prefix, prefix.PrefixEnd(),
					hlc.MaxTimestamp, storage.MVCCScanOptions{Inconsistent: true})
				require.NoError(t, err)
				return len(result.Intents) == 0
			}, 30*time.Second, 200*time.Millisecond, "intent cleanup timed out") {
				require.Fail(t, "found stale intents", "%v intents", len(result.Intents))
			}
		}

		// assertTxnCleanup checks that the txn record is cleaned up within a
		// reasonable time.
		assertTxnCleanup := func(t *testing.T, txnKey roachpb.Key, txnID uuid.UUID) {
			t.Helper()
			var txnEntry roachpb.Transaction
			if !assert.Eventually(t, func() bool {
				key := keys.TransactionKey(txnKey, txnID)
				ok, err := storage.MVCCGetProto(ctx, engine, key, hlc.MaxTimestamp, &txnEntry,
					storage.MVCCGetOptions{})
				require.NoError(t, err)
				return !ok
			}, 10*time.Second, 100*time.Millisecond, "txn entry cleanup timed out") {
				require.Fail(t, "found stale txn entry", "%v", txnEntry)
			}
		}

		// removeKeys cleans up all entries in the key range.
		removeKeys := func(t *testing.T) {
			t.Helper()
			batch := &kv.Batch{}
			batch.AddRawRequest(&roachpb.ClearRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    prefix,
					EndKey: prefix.PrefixEnd(),
				},
			})
			require.NoError(t, db.Run(ctx, batch))
			genKeySeen = map[string]bool{} // reset random key generator
		}

		// testTxn runs an intent cleanup test using a transaction.
		type testTxnSpec struct {
			numKeys     int    // number of keys per transaction
			singleRange bool   // if true, put intents in a single range at key\x00\x00
			finalize    string // commit, rollback, cancel, abort (via push)
		}
		testTxn := func(t *testing.T, spec testTxnSpec) {
			t.Helper()
			t.Cleanup(func() { removeKeys(t) })
			const batchSize = 10000

			// Write numKeys KV pairs in batches of batchSize as a single txn.
			var txnKey roachpb.Key
			txn := db.NewTxn(ctx, "test")
			batch := txn.NewBatch()
			for i := 0; i < spec.numKeys; i++ {
				key := genKey(spec.singleRange)
				batch.Put(key, []byte("value"))
				if (i > 0 && i%batchSize == 0) || i == spec.numKeys-1 {
					require.NoError(t, txn.Run(ctx, batch))
					batch = txn.NewBatch()
				}
				if i == 0 {
					txnKey = make([]byte, len(key))
					copy(txnKey, key)
				}
			}

			// Finalize the txn according to the spec.
			switch spec.finalize {
			case "commit":
				require.NoError(t, txn.Commit(ctx))

			case "rollback":
				require.NoError(t, txn.Rollback(ctx))

			case "cancel":
				rollbackCtx, cancel := context.WithCancel(ctx)
				cancel()
				if err := txn.Rollback(rollbackCtx); !errors.Is(err, context.Canceled) {
					require.NoError(t, err)
				}

			case "abort":
				now := clock.NowAsClockTimestamp()
				pusherProto := roachpb.MakeTransaction(
					"pusher",
					nil, // baseKey
					roachpb.MaxUserPriority,
					now.ToTimestamp(),
					clock.MaxOffset().Nanoseconds(),
				)
				pusher := kv.NewTxnFromProto(ctx, db, srv.NodeID(), now, kv.RootTxn, &pusherProto)
				require.NoError(t, pusher.Put(ctx, txnKey, []byte("pushit")))

				err := txn.Commit(ctx)
				require.Error(t, err)
				require.IsType(t, &roachpb.TransactionRetryWithProtoRefreshError{}, err)
				// if is required by linter, even though we know it will always succeed.
				if retryErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(err, &retryErr) {
					require.True(t, retryErr.PrevTxnAborted())
				}
				require.NoError(t, pusher.Rollback(ctx))

			default:
				require.Fail(t, "invalid finalize value %q", spec.finalize)
			}

			assertIntentCleanup(t)
			assertTxnCleanup(t, txnKey, txn.ID())
		}

		// testNonTxn runs an intent cleanup test without an explicit transaction.
		type testNonTxnSpec struct {
			numKeys     int  // number of keys per transaction
			singleRange bool // if true, put intents in a single range at key\x00\x00
		}
		testNonTxn := func(t *testing.T, spec testNonTxnSpec) {
			t.Helper()
			t.Cleanup(func() { removeKeys(t) })

			batch := &kv.Batch{}
			for i := 0; i < spec.numKeys; i++ {
				batch.Put(genKey(spec.singleRange), []byte("value"))
			}
			require.NoError(t, db.Run(ctx, batch))

			assertIntentCleanup(t)
		}

		testutils.RunValues(t, "numKeys", []interface{}{1, 100, 100000}, func(t *testing.T, numKeys interface{}) {
			testutils.RunTrueAndFalse(t, "singleRange", func(t *testing.T, singleRange bool) {
				testutils.RunTrueAndFalse(t, "txn", func(t *testing.T, txn bool) {
					if txn {
						finalize := []interface{}{"commit", "rollback", "cancel", "abort"}
						testutils.RunValues(t, "finalize", finalize, func(t *testing.T, finalize interface{}) {
							testTxn(t, testTxnSpec{
								numKeys:     numKeys.(int),
								singleRange: singleRange,
								finalize:    finalize.(string),
							})
						})
					} else {
						testNonTxn(t, testNonTxnSpec{
							numKeys:     numKeys.(int),
							singleRange: singleRange,
						})
					}
				})
			})
		})
	})
}
