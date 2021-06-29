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
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// Tests that intents and transaction records are cleaned up within a reasonable
// timeframe in various scenarios.
func TestReliableIntentCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 66895, "Flaky due to non-deterministic txn cleanup")
	skip.UnderShort(t) // takes 294s
	skip.UnderRace(t, "timing-sensitive test")
	skip.UnderStress(t, "memory-hungry test")

	prefix := roachpb.Key([]byte("key\x00"))

	testutils.RunTrueAndFalse(t, "ForceSyncIntentResolution", func(t *testing.T, forceSync bool) {
		// abortHeartbeats is used to abort txn heartbeats, returning
		// TransactionAbortedError. Key is txn anchor key, value is a chan
		// struct{} that will be closed when the next heartbeat aborts.
		var abortHeartbeats sync.Map

		abortHeartbeat := func(t *testing.T, txnKey roachpb.Key) <-chan struct{} {
			abortedC := make(chan struct{})
			abortHeartbeats.Store(string(txnKey), abortedC)
			t.Cleanup(func() {
				abortHeartbeats.Delete(string(txnKey))
			})
			return abortedC
		}

		// blockPuts is used to block Put responses for a given txn. The key is
		// a txn anchor key, and the value is a chan chan<- struct{} that, when
		// the Put is ready, will be used to send an unblock channel. The
		// unblock channel can be closed to unblock the Put.
		var blockPuts sync.Map

		blockPut := func(t *testing.T, txnKey roachpb.Key) <-chan chan<- struct{} {
			readyC := make(chan chan<- struct{})
			blockPuts.Store(string(txnKey), readyC)
			t.Cleanup(func() {
				blockPuts.Delete(string(txnKey))
			})
			return readyC
		}

		// blockPutEvals is used to block Put command evaluation in Raft for a
		// given txn. The key is a txn anchor key, and the value is a chan
		// chan<- struct{} that, when the Put is ready, will be used to send an
		// unblock channel. The unblock channel can be closed to unblock the
		// Put.
		var blockPutEvals sync.Map

		blockPutEval := func(t *testing.T, txnKey roachpb.Key) <-chan chan<- struct{} {
			readyC := make(chan chan<- struct{})
			blockPutEvals.Store(string(txnKey), readyC)
			t.Cleanup(func() {
				blockPutEvals.Delete(string(txnKey))
			})
			return readyC
		}

		requestFilter := func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			// If we receive a heartbeat from a txn in abortHeartbeats,
			// close the aborted channel and return an error response.
			if _, ok := ba.GetArg(roachpb.HeartbeatTxn); ok && ba.Txn != nil {
				if abortedC, ok := abortHeartbeats.LoadAndDelete(string(ba.Txn.Key)); ok {
					close(abortedC.(chan struct{}))
					return roachpb.NewError(roachpb.NewTransactionAbortedError(
						roachpb.ABORT_REASON_NEW_LEASE_PREVENTS_TXN))
				}
			}
			return nil
		}

		evalFilter := func(args kvserverbase.FilterArgs) *roachpb.Error {
			// If we receive a Put request from a txn in blockPutEvals, signal
			// the caller that the Put is ready to block by passing it an
			// unblock channel, and wait for it to close.
			if put, ok := args.Req.(*roachpb.PutRequest); ok && args.Hdr.Txn != nil {
				if bytes.HasPrefix(put.Key, prefix) {
					if ch, ok := blockPutEvals.LoadAndDelete(string(args.Hdr.Txn.Key)); ok {
						readyC := ch.(chan chan<- struct{})
						unblockC := make(chan struct{})
						readyC <- unblockC
						close(readyC)
						<-unblockC
					}
				}
			}
			return nil
		}

		responseFilter := func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
			// If we receive a Put request from a txn in blockPuts, signal
			// the caller that the Put is ready to block by passing it an
			// unblock channel, and wait for it to close.
			if arg, ok := ba.GetArg(roachpb.Put); ok && ba.Txn != nil {
				if bytes.HasPrefix(arg.(*roachpb.PutRequest).Key, prefix) {
					if ch, ok := blockPuts.LoadAndDelete(string(ba.Txn.Key)); ok {
						readyC := ch.(chan chan<- struct{})
						unblockC := make(chan struct{})
						readyC <- unblockC
						close(readyC)
						<-unblockC
					}
				}
			}
			return nil
		}

		// Set up three-node cluster, which will be shared by subtests.
		ctx := context.Background()
		clusterArgs := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &StoreTestingKnobs{
						IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
							ForceSyncIntentResolution: forceSync,
						},
						TestingRequestFilter:  requestFilter,
						TestingResponseFilter: responseFilter,
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							TestingPostEvalFilter: evalFilter,
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

		// Split off 16 ranges by the first hex digit (4 bits) after prefix:
		// key\x00\x00 key\x00\x10 key\x00\x20 key\x00\x30 ...
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

		// assertIntentCleanup checks that intents get cleaned up. It errors
		// ~fast if the intent count does not decrease, but gives it ample time
		// to complete when progress is being made.
		assertIntentCleanup := func(t *testing.T) {
			t.Helper()
			const (
				checkInterval  = time.Second
				stuckTimeout   = 30 * time.Second // intent count not decreasing
				overallTimeout = 5 * time.Minute
			)
			var (
				lastIntentDecrease = timeutil.Now()
				lastIntentCount    = math.MaxInt64
				started            = timeutil.Now()
			)
			for {
				result, err := storage.MVCCScan(ctx, store.Engine(), prefix, prefix.PrefixEnd(),
					hlc.MaxTimestamp, storage.MVCCScanOptions{Inconsistent: true})
				require.NoError(t, err)
				intentCount := len(result.Intents)
				if intentCount == 0 {
					return
				}
				if intentCount < lastIntentCount {
					lastIntentDecrease = timeutil.Now()
				}
				lastIntentCount = intentCount
				if timeutil.Since(lastIntentDecrease) >= stuckTimeout {
					require.Fail(t, "found stale intents", "count=%v first=%v last=%v",
						len(result.Intents), result.Intents[0], result.Intents[len(result.Intents)-1])
				}
				if timeutil.Since(started) >= overallTimeout {
					require.Fail(t, "intent cleanup timed out", "count=%v first=%v last=%v",
						len(result.Intents), result.Intents[0], result.Intents[len(result.Intents)-1])
				}
				time.Sleep(checkInterval)
			}
		}

		// assertTxnCleanup checks that the txn record is cleaned up within a
		// reasonable time. We give it a long timeout, since there are cases
		// where it will attempt to clean up all the intents again (even though
		// they have already been removed), which takes time to process. This
		// can happen due to multiple rollbacks being sent by different actors.
		assertTxnCleanup := func(t *testing.T, txnKey roachpb.Key, txnID uuid.UUID) {
			t.Helper()
			var txnEntry roachpb.Transaction
			if !assert.Eventually(t, func() bool {
				key := keys.TransactionKey(txnKey, txnID)
				ok, err := storage.MVCCGetProto(ctx, store.Engine(), key, hlc.MaxTimestamp, &txnEntry,
					storage.MVCCGetOptions{})
				require.NoError(t, err)
				return !ok
			}, time.Minute, 100*time.Millisecond, "txn record cleanup timed out") {
				require.Fail(t, "found stale txn record", "%v", txnEntry)
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

		// testTxnSpec specifies a testTxn test.
		type testTxnSpec struct {
			numKeys     int    // number of keys per transaction
			singleRange bool   // if true, put intents in a single range at key\x00\x00
			finalize    string // commit, rollback, cancel, cancelAsync
			abort       string // heartbeat, push
		}

		// testTxnExecute executes a transaction for testTxn. It is a separate function
		// so that any transaction errors can be retried as appropriate.
		testTxnExecute := func(t *testing.T, spec testTxnSpec, txn *kv.Txn, txnKey roachpb.Key) error {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			if spec.finalize == "cancelAsync" && spec.abort != "" {
				// This would require coordinating the abort, cancel, and put
				// goroutines. Doesn't seem worth the complexity.
				require.Fail(t, "Can't combine finalize=cancelAsync and abort")
			}

			// If requested, spin off txn aborter goroutines, returning errors
			// (if any) via abortErrC.
			//
			// We execute aborts while a Put request is in-flight, by blocking
			// the put response until the abort completes, as a regression test:
			// https://github.com/cockroachdb/cockroach/issues/65458
			abortErrC := make(chan error, 1)
			switch spec.abort {
			case "heartbeat":
				// Waits for a heartbeat and returns an abort error. Blocks put
				// meanwhile, returning when heartbeat was aborted.
				abortedC := abortHeartbeat(t, txnKey)
				readyC := blockPut(t, txnKey)
				require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "unblock", func(ctx context.Context) {
					<-abortedC
					unblockC := <-readyC
					time.Sleep(100 * time.Millisecond)
					close(unblockC)
				}))
				close(abortErrC) // can't error

			case "push":
				// Push txn with a high-priority write once put is blocked.
				readyC := blockPut(t, txnKey)
				require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "push", func(ctx context.Context) {
					unblockC := <-readyC
					defer close(unblockC)
					defer close(abortErrC)

					now := srv.Clock().NowAsClockTimestamp()
					pusherProto := roachpb.MakeTransaction(
						"pusher",
						nil, // baseKey
						roachpb.MaxUserPriority,
						now.ToTimestamp(),
						srv.Clock().MaxOffset().Nanoseconds(),
					)
					pusher := kv.NewTxnFromProto(ctx, db, srv.NodeID(), now, kv.RootTxn, &pusherProto)
					if err := pusher.Put(ctx, txnKey, []byte("pushit")); err != nil {
						abortErrC <- err
						return
					}
					if err := pusher.Rollback(ctx); err != nil {
						abortErrC <- err
						return
					}
					time.Sleep(100 * time.Millisecond)
				}))

			case "":
				close(abortErrC)

			default:
				require.Fail(t, "invalid abort type", "abort=%v", spec.abort)
			}

			// If requested, cancel the context while the put is being
			// evaluated in Raft.
			if spec.finalize == "cancelAsync" {
				readyC := blockPutEval(t, txnKey)
				require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "cancel", func(ctx context.Context) {
					unblockC := <-readyC
					defer close(unblockC)
					cancel()
					time.Sleep(100 * time.Millisecond)
				}))
			}

			// Write numKeys KV pairs in batches of batchSize as a single txn.
			const batchSize = 10000
			for b := 0; b*batchSize < spec.numKeys; b++ {
				batch := txn.NewBatch()
				for i := 0; i < batchSize && b*batchSize+i < spec.numKeys; i++ {
					key := genKey(spec.singleRange)
					if b == 0 && i == 0 {
						copy(key, txnKey) // txnKey must be the first written key
					}
					batch.Put(key, []byte("value"))
				}
				err := txn.Run(ctx, batch)
				if ctx.Err() != nil {
					// If context was canceled (see cancelAsync), we just bail
					// out and ignore the error (as the client would).
					break
				} else if err != nil {
					return err
				}
			}

			// Make sure the abort didn't error.
			if err := <-abortErrC; err != nil {
				return err
			}

			// Finalize the txn according to the spec.
			switch spec.finalize {
			case "commit":
				return txn.Commit(ctx)
			case "rollback":
				return txn.Rollback(ctx)
			case "cancel", "cancelAsync":
				// Rollback with canceled context, as the SQL connection would.
				cancel()
				if err := txn.Rollback(ctx); err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
			default:
				require.Fail(t, "invalid finalize type", "finalize=%v", spec.finalize)
			}
			return nil
		}

		// testTxn runs an intent cleanup test using a transaction.
		testTxn := func(t *testing.T, spec testTxnSpec) {
			t.Cleanup(func() {
				removeKeys(t)
			})

			// Execute the transaction and retry any transaction errors (unless
			// the test expects an error). These errors can be caused by async
			// processes such as lease transfers, range merges/splits, or
			// stats jobs).
			txns := map[uuid.UUID]roachpb.Key{}
			txn := db.NewTxn(ctx, "test") // reuse *kv.Txn across retries, will be updated
			for attempt := 1; ; attempt++ {
				txnKey := genKey(spec.singleRange)
				txns[txn.ID()] = txnKey // before testTxnExecute, id may change on errors

				err := testTxnExecute(t, spec, txn, txnKey)
				if err == nil {
					break
				} else if spec.abort != "" {
					require.Error(t, err)
					require.IsType(t, &roachpb.TransactionRetryWithProtoRefreshError{}, err, "err: %v", err)
					require.True(t, err.(*roachpb.TransactionRetryWithProtoRefreshError).PrevTxnAborted())
					break
				} else if retryErr, ok := err.(*roachpb.TransactionRetryWithProtoRefreshError); !ok {
					require.NoError(t, err)
				} else if attempt >= 3 {
					require.Fail(t, "too many txn retries", "attempt %v errored: %v", attempt, err)
				} else {
					t.Logf("retrying unexpected txn error: %v", err)
					require.True(t, txn.IsRetryableErrMeantForTxn(*retryErr))
				}
			}

			assertIntentCleanup(t)
			for txnID, txnKey := range txns {
				// TODO(erikgrinaker): for some reason (probably a request race),
				// finalize=cancelAsync triggers a slow path in CI -- seems to be
				// because the EndTxn doing record cleanup runs concurrently with or
				// after a separate intent cleanup process, such that it has to send
				// lots of ResolveIntent calls even though there are no intents to clean
				// up. We're not too concerned with txn record cleanup, so we skip the
				// check in these cases.
				if spec.finalize == "cancelAsync" {
					continue
				}
				assertTxnCleanup(t, txnKey, txnID)
			}
		}

		// testNonTxn runs an intent cleanup test without an explicit transaction.
		type testNonTxnSpec struct {
			numKeys     int  // number of keys per transaction
			singleRange bool // if true, put intents in a single range at key\x00\x00
		}
		testNonTxn := func(t *testing.T, spec testNonTxnSpec) {
			t.Cleanup(func() { removeKeys(t) })

			batch := &kv.Batch{}
			for i := 0; i < spec.numKeys; i++ {
				batch.Put(genKey(spec.singleRange), []byte("value"))
			}
			require.NoError(t, db.Run(ctx, batch))

			assertIntentCleanup(t)
		}

		// The actual tests are run here, using all combinations of parameters.
		testutils.RunValues(t, "numKeys", []interface{}{1, 100, 100000}, func(t *testing.T, numKeys interface{}) {
			testutils.RunTrueAndFalse(t, "singleRange", func(t *testing.T, singleRange bool) {
				testutils.RunTrueAndFalse(t, "txn", func(t *testing.T, txn bool) {
					if !txn {
						testNonTxn(t, testNonTxnSpec{
							numKeys:     numKeys.(int),
							singleRange: singleRange,
						})
						return
					}
					finalize := []interface{}{"commit", "rollback", "cancel", "cancelAsync"}
					testutils.RunValues(t, "finalize", finalize, func(t *testing.T, finalize interface{}) {
						if finalize == "cancelAsync" {
							// cancelAsync can't run together with abort.
							testTxn(t, testTxnSpec{
								numKeys:     numKeys.(int),
								singleRange: singleRange,
								finalize:    finalize.(string),
							})
							return
						}
						abort := []interface{}{"no", "push", "heartbeat"}
						testutils.RunValues(t, "abort", abort, func(t *testing.T, abort interface{}) {
							if abort.(string) == "no" {
								abort = "" // "no" just makes the test output better
							}
							testTxn(t, testTxnSpec{
								numKeys:     numKeys.(int),
								singleRange: singleRange,
								finalize:    finalize.(string),
								abort:       abort.(string),
							})
						})
					})
				})
			})
		})
	})
}
