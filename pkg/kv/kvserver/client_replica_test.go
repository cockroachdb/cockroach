// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/leases"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/kv/kvtestutils"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigptsreader"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplicaClockUpdates verifies that the leaseholder updates its clocks
// when executing a command to the command's timestamp, as long as the
// request timestamp is from a clock (i.e. is not in the future).
func TestReplicaClockUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, write bool, futureTime bool) {
		const numNodes = 3
		var manuals []*hlc.HybridManualClock
		var clocks []*hlc.Clock
		for i := 0; i < numNodes; i++ {
			manuals = append(manuals, hlc.NewHybridManualClock())
		}

		serverArgs := make(map[int]base.TestServerArgs)
		for i := 0; i < numNodes; i++ {
			serverArgs[i] = base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manuals[i],
					},
				},
			}
		}
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, numNodes,
			base.TestClusterArgs{
				ReplicationMode:   base.ReplicationManual,
				ServerArgsPerNode: serverArgs,
			})
		defer tc.Stopper().Stop(ctx)

		for _, s := range tc.Servers {
			clocks = append(clocks, s.Clock())
		}
		store := tc.GetFirstStoreFromServer(t, 0)
		reqKey := roachpb.Key("a")
		tc.SplitRangeOrFatal(t, reqKey)
		tc.AddVotersOrFatal(t, reqKey, tc.Targets(1, 2)...)

		// Pause the hybrid clocks, so we can make an exact measurement.
		for _, clock := range manuals {
			clock.Pause()
		}
		// Pick a timestamp in the future of all nodes by less than the
		// MaxOffset.
		reqTS := clocks[0].Now().Add(clocks[0].MaxOffset().Nanoseconds()/2, 0)
		h := kvpb.Header{Timestamp: reqTS}
		if !futureTime {
			h.Now = hlc.ClockTimestamp(reqTS)
		}

		// Execute the command.
		var req kvpb.Request
		if write {
			req = incrementArgs(reqKey, 5)
		} else {
			req = getArgs(reqKey)
		}
		if _, err := kv.SendWrappedWith(ctx, store.TestSender(), h, req); err != nil {
			t.Fatal(err)
		}

		// Verify that clocks were updated as expected. Only the leaseholder should
		// have updated its clock for either a read or a write. In theory, we should
		// be able to assert that _only_ the leaseholder's clock is updated, but in
		// practice an assertion against followers' clocks being updated is very
		// difficult to make without being flaky because it's difficult to prevent
		// other channels (background work, etc.) from carrying the clock update.
		expUpdated := !futureTime
		require.Equal(t, expUpdated, reqTS.Less(clocks[0].Now()))
	}

	testutils.RunTrueAndFalse(t, "write", func(t *testing.T, write bool) {
		testutils.RunTrueAndFalse(t, "future-time", func(t *testing.T, futureTime bool) {
			run(t, write, futureTime)
		})
	})
}

// TestLeaseholdersRejectClockUpdateWithJump verifies that leaseholders reject
// commands that would cause a large time jump.
func TestLeaseholdersRejectClockUpdateWithJump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := hlc.NewHybridManualClock()
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manual,
			},
			Store: &kvserver.StoreTestingKnobs{DisableCanAckBeforeApplication: true},
		},
	})
	defer s.Stopper().Stop(ctx)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	manual.Pause()
	ts1 := hlc.Timestamp{WallTime: manual.UnixNano()}
	// NB: it's possible that HLC ran in front of manual.Now() after the Pause()
	// call. Particularly, if the wall clock regressed during Pause(), and there
	// was a concurrent Now() with a pre-regression higher timestamp. See #119362.

	key := roachpb.Key("a")
	incArgs := incrementArgs(key, 5)

	// Commands with a future timestamp that is within the MaxOffset
	// bound will be accepted and will cause the clock to advance.
	const numCmds = 3
	clockOffset := s.Clock().MaxOffset() / numCmds
	for i := int64(1); i <= numCmds; i++ {
		_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{
			Now: hlc.ClockTimestamp(ts1.Add(i*clockOffset.Nanoseconds(), 0)),
		}, incArgs)
		require.NoError(t, pErr.GoError())
	}

	// Expect the clock to advance.
	ts2 := s.Clock().Now()
	require.Equal(t, numCmds*clockOffset, ts2.GoTime().Sub(ts1.GoTime()))

	// Once the accumulated offset reaches MaxOffset, commands will be rejected.
	tsFuture := hlc.ClockTimestamp(ts1.Add(s.Clock().MaxOffset().Nanoseconds()+1, 0))
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Now: tsFuture}, incArgs)
	require.True(t, testutils.IsPError(pErr, "remote wall time is too far ahead"))

	// The clock did not advance and the final command was not executed.
	ts3 := s.Clock().Now()
	require.Zero(t, ts3.GoTime().Sub(ts2.GoTime()))
	valRes, err := storage.MVCCGet(context.Background(), store.StateEngine(), key, ts3,
		storage.MVCCGetOptions{})
	require.NoError(t, err)
	require.Equal(t, incArgs.Increment*numCmds, mustGetInt(valRes.Value.ToPointer()))
}

// TestTxnPutOutOfOrder tests a case where a put operation of an older
// timestamp comes after a put operation of a newer timestamp in a
// txn. The test ensures such an out-of-order put succeeds and
// overrides an old value. The test uses a "Writer" and a "Reader"
// to reproduce an out-of-order put.
//
//  1. The Writer executes a cput operation and writes a write intent with
//     time T in a txn.
//  2. Before the Writer's txn is committed, the Reader sends a high priority
//     get operation with time T+100. This pushes the Writer txn timestamp to
//     T+100. The Reader also writes to the same key the Writer did a cput to
//     in order to trigger the restart of the Writer's txn. The original
//     write intent timestamp is also updated to T+100.
//  3. The Writer starts a new epoch of the txn, but before it writes, the
//     Reader sends another high priority get operation with time T+200. This
//     pushes the Writer txn timestamp to T+200 to trigger a restart of the
//     Writer txn. The Writer will not actually restart until it tries to commit
//     the current epoch of the transaction. The Reader updates the timestamp of
//     the write intent to T+200. The test deliberately fails the Reader get
//     operation, and cockroach doesn't update its timestamp cache.
//  4. The Writer executes the put operation again. This put operation comes
//     out-of-order since its timestamp is T+100, while the intent timestamp
//     updated at Step 3 is T+200.
//  5. The put operation overrides the old value using timestamp T+100.
//  6. When the Writer attempts to commit its txn, the txn will be restarted
//     again at a new epoch timestamp T+200, which will finally succeed.
func TestTxnPutOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// key is selected to fall within the meta range in order for the later
	// routing of requests to range 1 to work properly. Removing the routing
	// of all requests to range 1 would allow us to make the key more normal.
	const (
		key        = "key"
		restartKey = "restart"
	)
	// Set up a filter to so that the get operation at Step 3 will return an error.
	var shouldFailGet atomic.Value

	testingEvalFilter := func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
		if _, ok := filterArgs.Req.(*kvpb.GetRequest); ok &&
			filterArgs.Req.Header().Key.Equal(roachpb.Key(key)) &&
			filterArgs.Hdr.Txn == nil {
			if shouldFail := shouldFailGet.Load(); shouldFail != nil && shouldFail.(bool) {
				return kvpb.NewErrorWithTxn(errors.Errorf("Test"), filterArgs.Hdr.Txn)
			}
		}
		return nil
	}
	manual := hlc.NewHybridManualClock()
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manual,
			},
			Store: &kvserver.StoreTestingKnobs{
				// Splits can cause our chosen key to end up on a range other than range 1,
				// and trying to handle that complicates the test without providing any
				// added benefit.
				DisableSplitQueue: true,
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingEvalFilter: testingEvalFilter,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Put an initial value.
	initVal := []byte("initVal")
	err = store.DB().Put(context.Background(), key, initVal)
	if err != nil {
		t.Fatalf("failed to put: %+v", err)
	}

	manual.Pause()

	waitPut := make(chan struct{})
	waitFirstGet := make(chan struct{})
	waitTxnRestart := make(chan struct{})
	waitSecondGet := make(chan struct{})
	errChan := make(chan error)

	// Start the Writer.
	go func() {
		epoch := -1
		// Start a txn that does read-after-write.
		// The txn will be restarted twice, and the out-of-order put
		// will happen in the second epoch.
		errChan <- store.DB().Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			epoch++

			if epoch == 1 {
				// Wait until the second get operation is issued.
				close(waitTxnRestart)
				<-waitSecondGet
			}

			// Get a key which we can write to from the Reader in order to force a restart.
			if _, err := txn.Get(ctx, restartKey); err != nil {
				return err
			}

			updatedVal := []byte("updatedVal")
			if err := txn.CPut(ctx, key, updatedVal, kvclientutils.StrToCPutExistingValue("initVal")); err != nil {
				log.KvExec.Errorf(context.Background(), "failed put value: %+v", err)
				return err
			}

			// Make sure a get will return the value that was just written.
			actual, err := txn.Get(ctx, key)
			if err != nil {
				return err
			}
			if !bytes.Equal(actual.ValueBytes(), updatedVal) {
				return errors.Errorf("unexpected get result: %s", actual)
			}

			if epoch == 0 {
				// Wait until the first get operation will push the txn timestamp.
				close(waitPut)
				<-waitFirstGet
			}

			b := txn.NewBatch()
			return txn.CommitInBatch(ctx, b)
		})

		if epoch != 2 {
			file, line, _ := caller.Lookup(0)
			errChan <- errors.Errorf("%s:%d unexpected number of txn retries. "+
				"Expected epoch 2, got: %d.", file, line, epoch)
		} else {
			errChan <- nil
		}
	}()

	<-waitPut

	// Start the Reader.

	// Advance the clock and send a get operation with higher
	// priority to trigger the txn restart.
	manual.Increment(100)

	priority := roachpb.UserPriority(-math.MaxInt32)
	requestHeader := kvpb.RequestHeader{
		Key: roachpb.Key(key),
	}
	h := kvpb.Header{
		Timestamp:    s.Clock().Now(),
		UserPriority: priority,
	}
	if _, err := kv.SendWrappedWith(
		context.Background(), store.TestSender(), h, &kvpb.GetRequest{RequestHeader: requestHeader},
	); err != nil {
		t.Fatalf("failed to get: %+v", err)
	}
	// Write to the restart key so that the Writer's txn must restart.
	putReq := &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(restartKey)},
		Value:         roachpb.MakeValueFromBytes([]byte("restart-value")),
	}
	if _, err := kv.SendWrappedWith(context.Background(), store.TestSender(), h, putReq); err != nil {
		t.Fatalf("failed to put: %+v", err)
	}

	// Wait until the writer restarts the txn.
	close(waitFirstGet)
	<-waitTxnRestart

	// Advance the clock and send a get operation again. This time
	// we use TestingCommandFilter so that a get operation is not
	// processed after the write intent is resolved (to prevent the
	// timestamp cache from being updated).
	manual.Increment(100)

	h.Timestamp = s.Clock().Now()
	shouldFailGet.Store(true)
	if _, err := kv.SendWrappedWith(
		context.Background(), store.TestSender(), h, &kvpb.GetRequest{RequestHeader: requestHeader},
	); err == nil {
		t.Fatal("unexpected success of get")
	}
	if _, err := kv.SendWrappedWith(context.Background(), store.TestSender(), h, putReq); err != nil {
		t.Fatalf("failed to put: %+v", err)
	}

	close(waitSecondGet)
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}
}

// TestTxnReadWithinUncertaintyInterval tests cases where a transaction observes
// a committed value below its global uncertainty limit while performing a read.
//
// In one variant of the test, the transaction does not have an observed
// timestamp from the KV node serving the read, so its uncertainty interval
// during its read extends all the way to its global uncertainty limit. As a
// result, it observes the committed value in its uncertain future and receives
// a ReadWithinUncertaintyIntervalError.
//
// In a second variant of the test, the transaction does have an observed
// timestamp from the KV node serving the read, so its uncertainty interval
// during its read extends only up to this observed timestamp. This observed
// timestamp is below the committed value's timestamp. As a result, the
// transaction observes the committed value in its certain future, allowing it
// to avoid the ReadWithinUncertaintyIntervalError.
//
// Each test variant is run using the three different forms of transactional
// read-only operations: Get, Scan, and ReverseScan.
func TestTxnReadWithinUncertaintyInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "observedTS", func(t *testing.T, observedTS bool) {
		readOps := []string{"get", "scan", "revScan"}
		testutils.RunValues(t, "readOp", readOps, func(t *testing.T, readOp string) {
			testTxnReadWithinUncertaintyInterval(t, observedTS, readOp)
		})
	})
}

func testTxnReadWithinUncertaintyInterval(t *testing.T, observedTS bool, readOp string) {
	ctx := context.Background()
	manual := hlc.NewHybridManualClock()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				WallClock: manual,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Split off a scratch range.
	key, err := s.ScratchRange()
	require.NoError(t, err)

	// Pause the server's clocks going forward.
	manual.Pause()

	// Create a new transaction.
	now := s.Clock().Now()
	maxOffset := s.Clock().MaxOffset().Nanoseconds()
	require.NotZero(t, maxOffset)
	txn := roachpb.MakeTransaction("test", key, isolation.Serializable, 1, now, maxOffset, int32(s.SQLInstanceID()), 0, false /* omitInRangefeeds */)
	require.True(t, txn.ReadTimestamp.Less(txn.GlobalUncertaintyLimit))
	require.Len(t, txn.ObservedTimestamps, 0)

	// If the test variant wants an observed timestamp from the server at a
	// timestamp below the value, collect one now.
	if observedTS {
		get := getArgs(key)
		resp, pErr := kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: &txn}, get)
		require.Nil(t, pErr)
		txn.Update(resp.Header().Txn)
		require.Len(t, txn.ObservedTimestamps, 1)
	}

	// Perform a non-txn write. This will grab a timestamp from the clock.
	put := putArgs(key, []byte("val"))
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), put)
	require.Nil(t, pErr)

	// Perform another read on the same key. Depending on whether or not the txn
	// had collected an observed timestamp, it may or may not observe the value
	// in its uncertainty interval and throw an error.

	var read kvpb.Request
	switch readOp {
	case "get":
		read = getArgs(key)
	case "scan":
		read = scanArgs(key, key.Next())
	case "revScan":
		read = revScanArgs(key, key.Next())
	default:
		t.Fatalf("unknown op: %q", readOp)
	}
	_, pErr = kv.SendWrappedWith(ctx, store.TestSender(), kvpb.Header{Txn: &txn}, read)
	if observedTS {
		require.Nil(t, pErr)
	} else {
		require.NotNil(t, pErr)
		require.IsType(t, &kvpb.ReadWithinUncertaintyIntervalError{}, pErr.GetDetail())
	}
}

// TestTxnReadWithinUncertaintyIntervalAfterIntentResolution tests cases where a
// reader transaction observes a committed value that was committed before the
// reader began, but that was resolved after the reader began. The test ensures
// that even if the reader has collected an observed timestamp from the node
// that holds the intent, and even if this observed timestamp is less than the
// timestamp that the intent is eventually committed at, the reader still
// considers the value to be in its uncertainty interval. Not doing so could
// allow for stale read, which would be a violation of linearizability.
//
// This is a regression test for #36431. Before this issue was addressed,
// it was possible for the following series of events to lead to a stale
// read:
//   - txn W is coordinated by node B. It lays down an intent on node A (key k) at
//     ts 95.
//   - txn W gets pushed to ts 105 (taken from B's clock). It refreshes
//     successfully and commits at 105. Node A's clock is at, say, 100; this is
//     within clock offset bounds.
//   - after all this, txn R starts on node A. It gets assigned ts 100. The txn
//     has no uncertainty for node A.
//   - txn W's async intent resolution comes around and resolves the intent on
//     node A, moving the value fwd from ts 95 to 105.
//   - txn R reads key k and doesn't see anything. There's a value at 105, but the
//     txn have no uncertainty due to an observed timestamp. This is a stale read.
//
// The test's rangedResolution parameter dictates whether the intent is
// asynchronously resolved using point or ranged intent resolution.
//
// The test's movedWhilePending parameter dictates whether the intent is moved
// to a higher timestamp first by a PENDING intent resolution and then COMMITTED
// at that same timestamp, or whether it is moved to a higher timestamp at the
// same time as it is COMMITTED.
//
// The test's alreadyResolved parameter dictates whether the intent is
// already resolved by the time the reader observes it, or whether the
// reader must resolve the intent itself.
func TestTxnReadWithinUncertaintyIntervalAfterIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "rangedResolution", func(t *testing.T, rangedResolution bool) {
		testutils.RunTrueAndFalse(t, "movedWhilePending", func(t *testing.T, movedWhilePending bool) {
			testutils.RunTrueAndFalse(t, "alreadyResolved", func(t *testing.T, alreadyResolved bool) {
				testTxnReadWithinUncertaintyIntervalAfterIntentResolution(
					t, rangedResolution, movedWhilePending, alreadyResolved,
				)
			})
		})
	})
}

func testTxnReadWithinUncertaintyIntervalAfterIntentResolution(
	t *testing.T, rangedResolution, movedWhilePending, alreadyResolved bool,
) {
	const numNodes = 2
	var manuals []*hlc.HybridManualClock
	var clocks []*hlc.Clock
	for i := 0; i < numNodes; i++ {
		manuals = append(manuals, hlc.NewHybridManualClock())
	}
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manuals[i],
				},
				Store: &kvserver.StoreTestingKnobs{
					IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
						// Disable async intent resolution, so that the test can carefully
						// control when intent resolution occurs.
						DisableAsyncIntentResolution: true,
					},
				},
			},
		}
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	// Split off two scratch ranges.
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	tc.SplitRangeOrFatal(t, keyA)
	_, keyBDesc := tc.SplitRangeOrFatal(t, keyB)
	// Place key A's sole replica on node 1 and key B's sole replica on node 2.
	tc.AddVotersOrFatal(t, keyB, tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, keyBDesc, tc.Target(1))
	tc.RemoveVotersOrFatal(t, keyB, tc.Target(0))

	// Pause the servers' clocks going forward.
	var maxNanos int64
	for i, m := range manuals {
		m.Pause()
		if cur := m.UnixNano(); cur > maxNanos {
			maxNanos = cur
		}
		clocks = append(clocks, tc.Servers[i].Clock())
	}
	// After doing so, perfectly synchronize them.
	for _, m := range manuals {
		m.Increment(maxNanos - m.UnixNano())
	}

	// Create a new writer transaction.
	maxOffset := clocks[0].MaxOffset().Nanoseconds()
	require.NotZero(t, maxOffset)
	writerTxn := roachpb.MakeTransaction("test_writer", keyA, isolation.Serializable, 1, clocks[0].Now(), maxOffset, int32(tc.Servers[0].NodeID()), 0, false /* omitInRangefeeds */)

	// Write to key A and key B in the writer transaction.
	for _, key := range []roachpb.Key{keyA, keyB} {
		put := putArgs(key, []byte("val"))
		resp, pErr := kv.SendWrappedWith(ctx, tc.Servers[0].DistSenderI().(kv.Sender), kvpb.Header{Txn: &writerTxn}, put)
		require.Nil(t, pErr)
		writerTxn.Update(resp.Header().Txn)
	}

	// Move the clock on just the first server and bump the transaction commit
	// timestamp to this value. The clock on the second server will trail behind.
	manuals[0].Increment(100)
	require.True(t, writerTxn.WriteTimestamp.Forward(clocks[0].Now()))

	// Refresh the writer transaction's timestamp.
	writerTxn.ReadTimestamp.Forward(writerTxn.WriteTimestamp)

	// Commit the writer transaction. Key A will be synchronously resolved because
	// it is on the same range as the transaction record. However, key B will be
	// handed to the IntentResolver for asynchronous resolution. Because we
	// disabled async resolution, it will not be resolved yet.
	et, etH := endTxnArgs(&writerTxn, true /* commit */)
	et.LockSpans = []roachpb.Span{
		{Key: keyA}, {Key: keyB},
	}
	if rangedResolution {
		for i := range et.LockSpans {
			et.LockSpans[i].EndKey = et.LockSpans[i].Key.Next()
		}
	}
	etResp, pErr := kv.SendWrappedWith(ctx, tc.Servers[0].DistSenderI().(kv.Sender), etH, et)
	require.Nil(t, pErr)
	writerTxn.Update(etResp.Header().Txn)

	// Create a new reader transaction. The reader uses the second server as a
	// gateway, so its initial read timestamp actually trails the commit timestamp
	// of the writer transaction due to clock skew between the two servers. This
	// is the classic case where the reader's uncertainty interval is needed to
	// avoid stale reads. Remember that the reader transaction began after the
	// writer transaction committed and received an ack, so it must observe the
	// writer's writes if it is to respect real-time ordering.
	//
	// NB: we use writerTxn.MinTimestamp instead of clocks[1].Now() so that a
	// stray clock update doesn't influence the reader's read timestamp.
	readerTxn := roachpb.MakeTransaction("test_reader", keyA, isolation.Serializable, 1, writerTxn.MinTimestamp, maxOffset, int32(tc.Servers[1].NodeID()), 0, false /* omitInRangefeeds */)
	require.True(t, readerTxn.ReadTimestamp.Less(writerTxn.WriteTimestamp))
	require.False(t, readerTxn.GlobalUncertaintyLimit.Less(writerTxn.WriteTimestamp))

	// Collect an observed timestamp from each of the nodes. We read the key
	// following (Key.Next) each of the written keys to avoid conflicting with
	// read values. We read keyB first to avoid advancing the clock on node 2
	// before we collect an observed timestamp from it.
	//
	// NOTE: this wasn't even a necessary step to hit #36431, because new
	// transactions are always an observed timestamp from their own gateway node.
	for i, key := range []roachpb.Key{keyB, keyA} {
		get := getArgs(key.Next())
		resp, pErr := kv.SendWrappedWith(ctx, tc.Servers[1].DistSenderI().(kv.Sender), kvpb.Header{Txn: &readerTxn}, get)
		require.Nil(t, pErr)
		require.Nil(t, resp.(*kvpb.GetResponse).Value)
		readerTxn.Update(resp.Header().Txn)
		require.Len(t, readerTxn.ObservedTimestamps, i+1)
	}

	// Resolve the intent on key B zero, one, or two times.
	{
		resolveIntentArgs := func(status roachpb.TransactionStatus) kvpb.Request {
			if rangedResolution {
				return &kvpb.ResolveIntentRangeRequest{
					RequestHeader: kvpb.RequestHeader{Key: keyB, EndKey: keyB.Next()},
					IntentTxn:     writerTxn.TxnMeta,
					Status:        status,
				}
			} else {
				return &kvpb.ResolveIntentRequest{
					RequestHeader: kvpb.RequestHeader{Key: keyB},
					IntentTxn:     writerTxn.TxnMeta,
					Status:        status,
				}
			}
		}

		if movedWhilePending {
			// First change the intent's timestamp without committing it. This
			// exercises the case where the intent's timestamp is moved forward by a
			// PENDING intent resolution request and kept the same when the intent is
			// eventually COMMITTED. This PENDING intent resolution may still be
			// evaluated after the transaction commit has been acknowledged in
			// real-time, so it still needs to lead to the committed value retaining
			// its original local timestamp.
			//
			// For instance, consider the following timeline:
			//
			//  1. txn W writes intent on key A @ time 10
			//  2. txn W writes intent on key B @ time 10
			//  3. high priority reader @ 15 reads key B
			//  4. high priority reader pushes txn W to time 15
			//  5. txn W commits @ 15 and resolves key A synchronously
			//  6. txn R begins and collects observed timestamp from key B's node @
			//     time 11
			//  7. high priority reader moves intent on key B to time 15
			//  8. async intent resolution commits intent on key B, still @ time 15
			//  9. txn R reads key B with read ts 11, observed ts 11, and uncertainty
			//     interval [11, 21]. If step 7 updated the intent's local timestamp
			//     to the current time when changing its version timestamp, txn R
			//     could use its observed timestamp to avoid an uncertainty error,
			//     leading to a stale read.
			//
			resolve := resolveIntentArgs(roachpb.PENDING)
			_, pErr = kv.SendWrapped(ctx, tc.Servers[0].DistSenderI().(kv.Sender), resolve)
			require.Nil(t, pErr)
		}

		if alreadyResolved {
			// Resolve the committed value on key B to COMMITTED.
			resolve := resolveIntentArgs(roachpb.COMMITTED)
			_, pErr = kv.SendWrapped(ctx, tc.Servers[0].DistSenderI().(kv.Sender), resolve)
			require.Nil(t, pErr)
		}
	}

	// Read key A and B in the reader transaction. Both should produce
	// ReadWithinUncertaintyIntervalErrors.
	for _, key := range []roachpb.Key{keyA, keyB} {
		get := getArgs(key)
		_, pErr := kv.SendWrappedWith(ctx, tc.Servers[0].DistSenderI().(kv.Sender), kvpb.Header{Txn: &readerTxn}, get)
		require.NotNil(t, pErr)
		var rwuiErr *kvpb.ReadWithinUncertaintyIntervalError
		require.True(t, errors.As(pErr.GetDetail(), &rwuiErr))
		require.Equal(t, readerTxn.ReadTimestamp, rwuiErr.ReadTimestamp)
		require.Equal(t, readerTxn.GlobalUncertaintyLimit, rwuiErr.GlobalUncertaintyLimit)
		require.Equal(t, readerTxn.ObservedTimestamps, rwuiErr.ObservedTimestamps)
		require.Equal(t, writerTxn.WriteTimestamp, rwuiErr.ValueTimestamp)
	}
}

// TestTxnReadWithinUncertaintyIntervalAfterLeaseTransfer tests a case where a
// transaction observes a committed value in its uncertainty interval that was
// written under a previous leaseholder. In the test, the transaction does
// collect an observed timestamp from the KV node that eventually serves the
// read, but this observed timestamp is not allowed to be used to constrain its
// local uncertainty limit below the lease start time and avoid the uncertainty
// error. As a result, it observes the committed value in its uncertain future
// and receives a ReadWithinUncertaintyIntervalError, which avoids a stale read.
//
// See TestRangeLocalUncertaintyLimitAfterNewLease for a similar test.
func TestTxnReadWithinUncertaintyIntervalAfterLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the verbosity of the test to help investigate failures.
	testutils.SetVModule(t, "replica_range_lease=3,raft=4,txn=3,txn_coord_sender=3")
	const numNodes = 2
	var manuals []*hlc.HybridManualClock
	var clocks []*hlc.Clock
	for i := 0; i < numNodes; i++ {
		manuals = append(manuals, hlc.NewHybridManualClock())
	}
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manuals[i],
				},
			},
		}
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	// Split off two scratch ranges.
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
	tc.SplitRangeOrFatal(t, keyA)
	keyADesc, keyBDesc := tc.SplitRangeOrFatal(t, keyB)
	t.Logf("split range at keyB=%s, got descriptors: keyADesc=%+v, keyBDesc=%+v",
		keyB, keyADesc, keyBDesc)

	// Place key A's sole replica on node 1 and key B's sole replica on node 2.
	tc.AddVotersOrFatal(t, keyB, tc.Target(1))
	t.Logf("added voter for keyB=%s to node 2", keyB)
	tc.TransferRangeLeaseOrFatal(t, keyBDesc, tc.Target(1))
	t.Logf("transferred lease for keyB range to node 2")
	tc.RemoveVotersOrFatal(t, keyB, tc.Target(0))
	t.Logf("removed voter for keyB=%s from node 1", keyB)

	// Pause the servers' clocks going forward.
	for i, m := range manuals {
		m.Pause()
		clocks = append(clocks, tc.Servers[i].Clock())
	}

	// Synchronize the clocks. We wrap this in a SucceedsSoon to avoid messages
	// between the nodes to cause them to remain out of sync.
	var nowN1, nowN2 hlc.Timestamp
	testutils.SucceedsSoon(t, func() error {
		// Find the maximum clock value.
		maxNanos := manuals[0].UnixNano()
		for _, m := range manuals[1:] {
			maxNanos = max(maxNanos, m.UnixNano())
		}
		// After doing so, perfectly synchronize them.
		for _, m := range manuals {
			m.Increment(maxNanos - m.UnixNano())
		}

		nowN1, nowN2 = clocks[0].Now(), clocks[1].Now()
		if nowN1.WallTime != nowN2.WallTime {
			return errors.Errorf("clocks are not synchronized: n1's clock: %+v, n2's clock: %+v",
				nowN1, nowN2)
		}
		return nil
	})

	t.Logf("all clocks synchronized: n1's clock: %+v, n2's clock: %+v", nowN1, nowN2)

	// Create a new transaction using the second node as the gateway.
	maxOffset := clocks[1].MaxOffset().Nanoseconds()
	require.NotZero(t, maxOffset)
	txn := roachpb.MakeTransaction("test", keyB, isolation.Serializable, 1, nowN2, maxOffset, int32(tc.Servers[1].SQLInstanceID()), 0, false /* omitInRangefeeds */)
	t.Logf("created transaction: %+v", txn)
	require.True(t, txn.ReadTimestamp.Less(txn.GlobalUncertaintyLimit))
	require.Len(t, txn.ObservedTimestamps, 0)

	// Collect an observed timestamp in that transaction from node 2.
	t.Logf("collecting observed timestamp from node 2 for keyB=%s", keyB)
	getB := getArgs(keyB)
	resp, pErr := kv.SendWrappedWith(ctx, tc.Servers[1].DistSenderI().(kv.Sender), kvpb.Header{Txn: &txn}, getB)
	require.Nil(t, pErr)
	t.Logf("successfully read keyB=%s, response: %+v", keyB, resp)
	txn.Update(resp.Header().Txn)
	require.Len(t, txn.ObservedTimestamps, 1)
	t.Logf("updated transaction with observed timestamp, now has %+v observed timestamps", txn.ObservedTimestamps)

	// Advance the clock on the first node.
	manuals[0].Increment(100)
	nowN1, nowN2 = clocks[0].Now(), clocks[1].Now()
	t.Logf("advanced n1's clock by 100ns: n1's clock: %+v, n2's clock: %+v", nowN1, nowN2)

	// Perform a non-txn write on node 1. This will grab a timestamp from node 1's
	// clock, which leads the clock on node 2.
	//
	// NOTE: we perform the clock increment and write _after_ creating the
	// transaction and collecting an observed timestamp. Ideally, we would write
	// this test such that we did this before beginning the transaction on node 2,
	// so that the absence of an uncertainty error would be a true "stale read".
	// However, doing so causes the test to be flaky because background operations
	// can leak the clock signal from node 1 to node 2 between the time that we
	// write and the time that the transaction begins. If we had a way to disable
	// all best-effort HLC clock stabilization channels and only propagate clock
	// signals when strictly necessary then it's possible that we could avoid
	// flakiness. For now, we just re-order the operations and assert that we
	// receive an uncertainty error even though its absence would not be a true
	// stale read.
	t.Logf("Performing non-txn write on node 0 for keyA=%s", keyA)
	ba := &kvpb.BatchRequest{}
	ba.Add(putArgs(keyA, []byte("val")))
	br, pErr := tc.Servers[0].DistSenderI().(kv.Sender).Send(ctx, ba)
	require.Nil(t, pErr)
	writeTs := br.Timestamp
	t.Logf("Successfully wrote keyA=%s, write timestamp: %s", keyA, writeTs)

	// The transaction has a read timestamp beneath the write's commit timestamp
	// but a global uncertainty limit above the write's commit timestamp. The
	// observed timestamp collected is also beneath the write's commit timestamp.
	nowN1, nowN2 = clocks[0].Now(), clocks[1].Now()
	t.Logf("validating the clocks before test assertions: n1's clock: %+v, n2's clock: %+v",
		nowN1, nowN2)
	assert.True(t, txn.ReadTimestamp.Less(writeTs))
	assert.True(t, writeTs.Less(txn.GlobalUncertaintyLimit))
	assert.True(t, txn.ObservedTimestamps[0].Timestamp.ToTimestamp().Less(writeTs))

	if t.Failed() {
		t.Logf("writeTs=%s, txn=%+v, obsTs=%+v", writeTs, txn, txn.ObservedTimestamps)
		t.FailNow()
	}

	// Add a replica for key A's range to node 2. Transfer the lease.
	tc.AddVotersOrFatal(t, keyA, tc.Target(1))
	tc.TransferRangeLeaseOrFatal(t, keyADesc, tc.Target(1))

	// Perform another read in the transaction, this time on key A. This will be
	// routed to node 2, because it now holds the lease for key A. Even though the
	// transaction has collected an observed timestamp from node 2, it cannot use
	// it to constrain its local uncertainty limit below the lease start time and
	// avoid the uncertainty error. This is a good thing, as doing so would allow
	// for a stale read.
	getA := getArgs(keyA)
	_, pErr = kv.SendWrappedWith(ctx, tc.Servers[1].DistSenderI().(kv.Sender), kvpb.Header{Txn: &txn}, getA)
	require.NotNil(t, pErr)
	require.IsType(t, &kvpb.ReadWithinUncertaintyIntervalError{}, pErr.GetDetail())
}

// TestTxnReadWithinUncertaintyIntervalAfterRangeMerge verifies that on a merge of two
// ranges, the limiting of the uncertainty timestamp from the RHS is preserved.
// The following series demonstrates the problem.
//
// +-----------+            +-----------+        +-----------+       +----------+
// | S1* (A-B) |            | S2 (A-B)  |        | S3* (C-D) |       | S4 (C-D) |
// +-----------+            +-----------+        +-----------+       +----------+
// | - Time 0               | - Time 0           | - Time 100        | - Time 0
// |                        |                    |                   |
// |                        |                    | - Put(C)          |
// | - Get(A, TX1)          |                    |                   |
// |  Observed TS (1)       |                    |                   |
// |                        |                    |                   |
// |<-----------------------|--------------------|-- Transfer(C-D) --X
// |                        X -- Transfer(A-B) ->|
// |                                             |
// |<----------------------- Merge (A-D) --------X - no longer leaseholder
// | - Time bumped to 100
// |
// | - Get(C, TX1)
// | - BUG - Not found!
//
// This is a bug because from a causality perspective, the put of C
// happened before the later get of C. It is not found because the transaction
// uncertainty window is incorrectly (0). Note that leaseholder protection would
// not help here as there are no lease changes or transfers.
//
// The underlying issue is that the observed timestamp on S0 becomes "invalid"
// after the merge. This is one scenario where this can occur, but not the only one.
// Fundamentally the guarantee the observed timestamp is intended to provide,
// that no writes for any names were written before the time T0 is broken.
// The underlying cause for this is an implicit, but incorrect mapping between
// leaseholders and leases which is stored by the client.
//
// The situation for the test is 4 nodes total, 2 holding the "LHS" and 2
// holding the RHS of the range that is going to merge. The nodes are merged so
// that the leaseholders are unaligned at the time of the merge, and the RHS
// leaseholder clock is ahead of the LHS.
func TestTxnReadWithinUncertaintyIntervalAfterRangeMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test has always been flaky under deadlock since its introduction. Due
	// to its complexity, we are not going to spend time on improving it now.
	skip.UnderDuress(t)

	run := func(t *testing.T, alignLeaseholders bool, alsoSplit bool) {

		// The stores 0 and 1 are the "LHS", and the stores 2 and 3 are the RHS.
		// The stores 0 and 2 are leaseholders, and the clocks on the replicas are fast
		// Before the merge operation, need to align replicas, so 3 => 0 and 1 => 2 to
		// avoid moving leases and triggering leaseholder protection.
		// After the merge, the only leaseholder is store 0. Store 2 is the replica of
		// that store.
		const numServers = 4
		// First set up all the four stores with manual clocks
		var manuals [numServers]*hlc.HybridManualClock
		for i := 0; i < numServers; i++ {
			manuals[i] = hlc.NewHybridManualClock()
		}
		serverArgs := make(map[int]base.TestServerArgs)
		for i := 0; i < numServers; i++ {
			serverArgs[i] = base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manuals[i],
					},
				},
			}
		}
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, numServers,
			base.TestClusterArgs{
				ReplicationMode:   base.ReplicationManual,
				ServerArgsPerNode: serverArgs,
			})
		defer tc.Stopper().Stop(ctx)

		// Now, generate two scratch ranges that will later be merged together.
		keyA, keyC := roachpb.Key("A"), roachpb.Key("C")
		tc.SplitRangeOrFatal(t, keyA)
		// Split the range in half to create the ranges that will later be merged.
		_, keyCDesc := tc.SplitRangeOrFatal(t, keyC)
		// Next, we place key A's replicas on nodes 0 and 1 and key C's replicas on
		// nodes 2 and 3.
		tc.AddVotersOrFatal(t, keyA, tc.Target(1))
		tc.AddVotersOrFatal(t, keyC, tc.Target(2))
		tc.AddVotersOrFatal(t, keyC, tc.Target(3))
		// Finally, transfer the lease for the RHS to node 2.
		tc.TransferRangeLeaseOrFatal(t, keyCDesc, tc.Target(2))
		// Make sure the lease transfer completes before we read the clock times.
		testutils.SucceedsSoon(t, func() error {
			repl := tc.GetFirstStoreFromServer(t, 2).LookupReplica(keyCDesc.StartKey)
			lease, _ := repl.GetLease()
			if lease.Replica.NodeID != repl.NodeID() {
				return errors.Errorf("expected lease transfer to node 2: %s", lease)
			}
			return nil
		})

		tc.RemoveVotersOrFatal(t, keyC, tc.Target(0))
		// At this point all the ranges are in the right places.

		// Pause the servers' clocks going forward.
		var maxNanos int64
		for _, m := range manuals {
			m.Pause()
			if cur := m.UnixNano(); cur > maxNanos {
				maxNanos = cur
			}
		}
		// After doing so, perfectly synchronize them.
		for _, m := range manuals {
			m.Increment(maxNanos - m.UnixNano())
		}

		// Grab the clock times before we increment the other clock. Otherwise, there
		// is a chance that server 0 will see server 2's clock and update itself prior
		// to reading these values.
		now := tc.Servers[0].Clock().Now()
		maxOffset := tc.Servers[0].Clock().MaxOffset().Nanoseconds()
		instanceId := int32(tc.Servers[0].SQLInstanceID())

		// Move the RHS leaseholders clocks forward past the observed timestamp before
		// writing.
		manuals[2].Increment(2000)

		defer func() {
			if !t.Failed() {
				return
			}
			t.Logf("maxNanos=%d", maxNanos)
			t.Logf("manuals[2]=%d", manuals[2].UnixNano())
		}()

		// Write the data from a different transaction to establish the time for the
		// key as 10 ns in the future.
		{
			ctx, fagrs := tracing.ContextWithRecordingSpan(ctx, tc.Servers[2].Tracer(), "keyC-write")
			resp, pErr := kv.SendWrapped(ctx, tc.Servers[2].DistSenderI().(kv.Sender), putArgs(keyC, []byte("value")))
			rec := fagrs()
			require.Nil(t, pErr, "%v", rec)
			defer func() {
				if !t.Failed() {
					return
				}
				t.Logf("keyC-write: %+v", resp)
				t.Logf("keyC-write: %s", rec)
			}()
		}

		// Create two identical transactions. The first one will perform a read to a
		// store before the merge, the second will only read after the merge.
		txn := roachpb.MakeTransaction("txn1", keyA, isolation.Serializable, 1, now, maxOffset, instanceId, 0, false /* omitInRangefeeds */)
		txn2 := roachpb.MakeTransaction("txn2", keyA, isolation.Serializable, 1, now, maxOffset, instanceId, 0, false /* omitInRangefeeds */)

		// Simulate a read which will cause the observed time to be set to now.
		{
			ctx, fagrs := tracing.ContextWithRecordingSpan(ctx, tc.Servers[1].Tracer(), "txn1-keyA-get")
			resp, pErr := kv.SendWrappedWith(ctx, tc.Servers[1].DistSenderI().(kv.Sender), kvpb.Header{Txn: &txn}, getArgs(keyA))
			rec := fagrs()
			require.Nil(t, pErr, "%v", rec)
			// The client needs to update its transaction to the returned transaction which has observed timestamps in it
			txn = *resp.Header().Txn
			defer func() {
				if !t.Failed() {
					return
				}
				t.Logf("txn1-keyA-get: %+v", resp)
				t.Logf("txn1-keyA-get: %s", rec)
				t.Logf("txn1-keyA-get resp txn: %s", txn)
			}()
		}

		// Now move the ranges, being careful not to move either leaseholder
		// C: 3 (RHS - replica) => 0 (LHS leaseholder)
		tc.AddVotersOrFatal(t, keyC, tc.Target(0))
		tc.RemoveVotersOrFatal(t, keyC, tc.Target(3))
		// A: 1 (LHS - replica) => 2 (RHS leaseholder)
		tc.AddVotersOrFatal(t, keyA, tc.Target(2))
		tc.RemoveVotersOrFatal(t, keyA, tc.Target(1))

		if alignLeaseholders {
			tc.TransferRangeLeaseOrFatal(t, keyCDesc, tc.Target(0))
		}

		// Finally, perform the actual merge operation on server 0
		require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, keyA))

		if alsoSplit {
			require.NoError(t, tc.Server(0).DB().AdminSplit(ctx, keyC, hlc.MaxTimestamp))
		}

		// Try and read the transaction from the context of a new transaction. This
		// will fail as expected as the observed timestamp will not be set.
		{
			ctx, fagrs := tracing.ContextWithRecordingSpan(ctx, tc.Servers[0].Tracer(), "txn2get-should-rwue")
			_, pErr := kv.SendWrappedWith(ctx, tc.Servers[0].DistSenderI().(kv.Sender), kvpb.Header{Txn: &txn2},
				getArgs(keyC))
			rec := fagrs()
			require.IsType(t, &kvpb.ReadWithinUncertaintyIntervalError{}, pErr.GetDetail(), "%s", rec)
		}

		// Try and read the key from the existing transaction. This should fail the
		// same way.
		// There are four possible outcomes:
		// - Uncertainty (Good) - Expected since the read and write timestamps cross.
		// - Other error (Bad) - We expect an uncertainty error so the client can choose a new timestamp and retry.
		// - Not found (Bad) - Error because the data was written before us.
		// - Found (Bad) - The write HLC timestamp is after our timestamp.
		{
			ctx, fagrs := tracing.ContextWithRecordingSpan(ctx, tc.Servers[0].Tracer(), "txn1get-should-rwue")
			_, pErr := kv.SendWrappedWith(ctx, tc.Servers[0].DistSenderI().(kv.Sender), kvpb.Header{Txn: &txn}, getArgs(keyC))
			rec := fagrs()
			require.IsType(t, &kvpb.ReadWithinUncertaintyIntervalError{}, pErr.GetDetail(), "%s", rec)
		}
	}

	testutils.RunTrueAndFalse(t, "alignLeaseholders", func(t *testing.T, alignLeaseholders bool) {
		testutils.RunTrueAndFalse(t, "alsoSplit", func(t *testing.T, alsoSplit bool) {
			run(t, alignLeaseholders, alsoSplit)
		})
	})
}

// TestNonTxnReadWithinUncertaintyIntervalAfterLeaseTransfer tests a case where
// a non-transactional request defers its timestamp allocation to a replica that
// does not hold the lease at the time of receiving the request, but does by the
// time that the request consults the lease. In the test, a value is written on
// the previous leaseholder at a higher timestamp than that assigned to the non-
// transactional request. After the lease transfer, the non-txn request is
// required by uncertainty.ComputeInterval to forward its local uncertainty
// limit to the new lease start time. This prevents the read from ignoring the
// previous write, which avoids a stale read. Instead, the non-txn read hits an
// uncertainty error, performs a server-side retry, and re-evaluates with a
// timestamp above the write.
//
// This test exercises the hazard described in "reason #1" of uncertainty.D7.
func TestNonTxnReadWithinUncertaintyIntervalAfterLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Inject a request filter, which intercepts the server-assigned timestamp
	// of a non-transactional request and then blocks that request until after
	// the lease has been transferred to the server.
	type nonTxnGetKey struct{}
	nonTxnOrigTsC := make(chan hlc.Timestamp, 1)
	nonTxnBlockerC := make(chan struct{})
	requestFilter := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if ctx.Value(nonTxnGetKey{}) != nil {
			// Give the test the server-assigned timestamp.
			require.NotNil(t, ba.TimestampFromServerClock)
			nonTxnOrigTsC <- ba.Timestamp
			// Wait for the test to give the go-ahead.
			select {
			case <-nonTxnBlockerC:
			case <-ctx.Done():
			case <-time.After(testutils.DefaultSucceedsSoonDuration):
			}
		}
		return nil
	}
	var uncertaintyErrs int32
	concurrencyRetryFilter := func(ctx context.Context, _ *kvpb.BatchRequest, pErr *kvpb.Error) {
		if ctx.Value(nonTxnGetKey{}) != nil {
			if _, ok := pErr.GetDetail().(*kvpb.ReadWithinUncertaintyIntervalError); ok {
				atomic.AddInt32(&uncertaintyErrs, 1)
			}
		}
	}

	const numNodes = 2
	var manuals []*hlc.HybridManualClock
	for i := 0; i < numNodes; i++ {
		manuals = append(manuals, hlc.NewHybridManualClock())
	}
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numNodes; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manuals[i],
				},
				Store: &kvserver.StoreTestingKnobs{
					TestingRequestFilter:          requestFilter,
					TestingConcurrencyRetryFilter: concurrencyRetryFilter,
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual,
		ServerArgsPerNode: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	// Split off a scratch range and upreplicate to node 2.
	key := tc.ScratchRange(t)
	desc := tc.LookupRangeOrFatal(t, key)
	tc.AddVotersOrFatal(t, key, tc.Target(1))

	// Pause the servers' clocks going forward.
	var maxNanos int64
	for _, m := range manuals {
		m.Pause()
		maxNanos = max(maxNanos, m.UnixNano())
	}
	// After doing so, perfectly synchronize them.
	for _, m := range manuals {
		m.Increment(maxNanos - m.UnixNano())
	}

	// Initiate a non-txn read on node 2. The request will be intercepted after
	// the request has received a server-assigned timestamp, but before it has
	// consulted the lease. We'll transfer the lease to node 2 before the request
	// checks, so that it ends up evaluating on node 2.
	type resp struct {
		*kvpb.BatchResponse
		*kvpb.Error
	}
	nonTxnRespC := make(chan resp, 1)
	_ = tc.Stopper().RunAsyncTask(ctx, "non-txn get", func(ctx context.Context) {
		ctx = context.WithValue(ctx, nonTxnGetKey{}, "foo")
		ba := &kvpb.BatchRequest{}
		ba.RangeID = desc.RangeID
		ba.Add(getArgs(key))
		br, pErr := kvserver.ToSenderForTesting(tc.GetFirstStoreFromServer(t, 1)).Send(ctx, ba)
		nonTxnRespC <- resp{br, pErr}
	})

	// Wait for the non-txn read to get stuck.
	var nonTxnOrigTs hlc.Timestamp
	select {
	case nonTxnOrigTs = <-nonTxnOrigTsC:
	case nonTxnResp := <-nonTxnRespC:
		t.Fatalf("unexpected response %+v", nonTxnResp)
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatalf("timeout")
	}

	// Advance the clock on node 1. This should now lead the clock on node 2 and
	// the timestamp assigned to the non-txn read, because the two manual clocks
	// were paused and synchronized up above.
	manuals[0].Increment(100)
	clockTs := tc.Servers[0].Clock().Now()
	require.True(t, nonTxnOrigTs.Less(clockTs), "nonTxnOrigTs: %v, clockTs: %v", nonTxnOrigTs, clockTs)

	// Perform a non-txn write on node 1. This will grab a timestamp from node 1's
	// clock, which leads the clock on node 2 and the timestamp assigned to the
	// non-txn read.
	//
	// NOTE: we perform the clock increment and write _after_ sending the non-txn
	// read. Ideally, we would write this test such that we did this before
	// beginning the read on node 2, so that the absence of an uncertainty error
	// would be a true "stale read". However, doing so causes the test to be flaky
	// because background operations can leak the clock signal from node 1 to node
	// 2 between the time that we write and the time that the non-txn read request
	// is sent. If we had a way to disable all best-effort HLC clock stabilization
	// channels and only propagate clock signals when strictly necessary then it's
	// possible that we could avoid flakiness. For now, we just re-order the
	// operations and assert that we observe an uncertainty error even though its
	// absence would not be a true stale read.
	ba := &kvpb.BatchRequest{}
	ba.RangeID = desc.RangeID
	ba.Add(putArgs(key, []byte("val")))
	br, pErr := kvserver.ToSenderForTesting(tc.GetFirstStoreFromServer(t, 0)).Send(ctx, ba)
	require.Nil(t, pErr)
	writeTs := br.Timestamp
	require.True(t, nonTxnOrigTs.Less(writeTs), "nonTxnOrigTs: %v, writeTs: %v", nonTxnOrigTs, writeTs)

	// Then transfer the lease to node 2. The new lease should end up with a start
	// time above the timestamp assigned to the non-txn read.
	var lease roachpb.Lease
	tc.TransferRangeLeaseOrFatal(t, desc, tc.Target(1))
	testutils.SucceedsSoon(t, func() error {
		repl := tc.GetFirstStoreFromServer(t, 1).LookupReplica(desc.StartKey)
		lease, _ = repl.GetLease()
		if lease.Replica.NodeID != repl.NodeID() {
			return errors.Errorf("expected lease transfer to node 2: %s", lease)
		}
		return nil
	})
	require.True(t, nonTxnOrigTs.Less(lease.Start.ToTimestamp()))

	// Let the non-txn read proceed. It should complete, but only after hitting a
	// ReadWithinUncertaintyInterval, performing a server-side retry, reading
	// again at a higher timestamp, and returning the written value.
	close(nonTxnBlockerC)
	nonTxnResp := <-nonTxnRespC
	require.Nil(t, nonTxnResp.Error)
	br = nonTxnResp.BatchResponse
	require.NotNil(t, br)
	require.True(t, nonTxnOrigTs.Less(br.Timestamp))
	require.True(t, writeTs.LessEq(br.Timestamp))
	require.Len(t, br.Responses, 1)
	require.NotNil(t, br.Responses[0].GetGet())
	require.NotNil(t, br.Responses[0].GetGet().Value)
	require.Equal(t, writeTs, br.Responses[0].GetGet().Value.Timestamp)
	require.Equal(t, int32(1), atomic.LoadInt32(&uncertaintyErrs))
}

// TestRangeLookupUseReverse tests whether the results and the results count
// are correct when scanning in reverse order.
func TestRangeLookupUseReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Init test ranges:
	// ["","a"), ["a","c"), ["c","e"), ["e","g") and ["g","\xff\xff").
	splits := []*kvpb.AdminSplitRequest{
		adminSplitArgs(roachpb.Key("g")),
		adminSplitArgs(roachpb.Key("e")),
		adminSplitArgs(roachpb.Key("c")),
		adminSplitArgs(roachpb.Key("a")),
	}

	repl := store.LookupReplica(roachpb.RKey("a"))
	require.NotNil(t, repl)
	keyMin := repl.Desc().StartKey
	keyMax := repl.Desc().EndKey

	for _, split := range splits {
		_, pErr := kv.SendWrapped(context.Background(), store.TestSender(), split)
		if pErr != nil {
			t.Fatalf("%q: split unexpected error: %s", split.SplitKey, pErr)
		}
	}

	// Resolve the intents.
	scanArgs := kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    keys.RangeMetaKey(roachpb.RKeyMin.Next()).AsRawKey(),
			EndKey: keys.RangeMetaKey(roachpb.RKeyMax).AsRawKey(),
		},
	}
	testutils.SucceedsSoon(t, func() error {
		_, pErr := kv.SendWrapped(context.Background(), store.TestSender(), &scanArgs)
		return pErr.GoError()
	})

	testCases := []struct {
		key         roachpb.RKey
		maxResults  int64
		expected    []roachpb.RangeDescriptor
		expectedPre []roachpb.RangeDescriptor
	}{
		// Test key in the middle of the range.
		{
			key:        roachpb.RKey("f"),
			maxResults: 2,
			// ["e","g") and ["c","e").
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("e"), EndKey: roachpb.RKey("g")},
			},
			expectedPre: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
			},
		},
		// Test key in the end key of the range.
		{
			key:        roachpb.RKey("g"),
			maxResults: 3,
			// ["e","g"), ["c","e") and ["a","c").
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("e"), EndKey: roachpb.RKey("g")},
			},
			expectedPre: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
				{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
			},
		},
		{
			key:        roachpb.RKey("e"),
			maxResults: 2,
			// ["c","e") and ["a","c").
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
			},
			expectedPre: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
			},
		},
		// Test keyMax.
		{
			key:        keyMax,
			maxResults: 2,
			// ["e","g") and ["g","\xff\xff")
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("g"), EndKey: keyMax},
			},
			expectedPre: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("e"), EndKey: roachpb.RKey("g")},
			},
		},
		// Test the start.
		{
			key:        roachpb.RKey("a"),
			maxResults: 1,
			// ["","a")
			expected: []roachpb.RangeDescriptor{
				{StartKey: keyMin, EndKey: roachpb.RKey("a")},
			},
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("key=%s", test.key), func(t *testing.T) {
			rs, preRs, err := kv.RangeLookup(context.Background(), store.TestSender(),
				test.key.AsRawKey(), kvpb.READ_UNCOMMITTED, test.maxResults-1, true /* prefetchReverse */)
			if err != nil {
				t.Fatalf("LookupRange error: %+v", err)
			}

			// Checks the results count.
			if rsLen, preRsLen := len(rs), len(preRs); int64(rsLen+preRsLen) != test.maxResults {
				t.Fatalf("returned results count, expected %d, but got %d+%d", test.maxResults, rsLen, preRsLen)
			}
			// Checks the range descriptors.
			for _, rngSlice := range []struct {
				expect, reply []roachpb.RangeDescriptor
			}{
				{test.expected, rs},
				{test.expectedPre, preRs},
			} {
				for i, rng := range rngSlice.expect {
					if !(rng.StartKey.Equal(rngSlice.reply[i].StartKey) && rng.EndKey.Equal(rngSlice.reply[i].EndKey)) {
						t.Fatalf("returned range is not correct, expected %v, but got %v", rng, rngSlice.reply[i])
					}
				}
			}
		})
	}
}

type leaseTransferTest struct {
	tc *testcluster.TestCluster
	// replicas of range covering key "a" on the first and the second stores.
	replica0, replica1         *kvserver.Replica
	replica0Desc, replica1Desc roachpb.ReplicaDescriptor
	leftKey                    roachpb.Key
	filterMu                   syncutil.Mutex
	evalFilter                 kvserverbase.ReplicaCommandFilter
	propFilter                 kvserverbase.ReplicaProposalFilter
	waitForTransferBlocked     atomic.Value
	transferBlocked            chan struct{}
	manualClock                *hlc.HybridManualClock
}

func setupLeaseTransferTest(t *testing.T) *leaseTransferTest {
	l := &leaseTransferTest{
		leftKey:     roachpb.Key("a"),
		manualClock: hlc.NewHybridManualClock(),
	}

	testingEvalFilter := func(args kvserverbase.FilterArgs) *kvpb.Error {
		l.filterMu.Lock()
		filterCopy := l.evalFilter
		l.filterMu.Unlock()
		if filterCopy != nil {
			return filterCopy(args)
		}
		return nil
	}

	testingProposalFilter := func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		l.filterMu.Lock()
		filterCopy := l.propFilter
		l.filterMu.Unlock()
		if filterCopy != nil {
			return filterCopy(args)
		}
		return nil
	}

	l.waitForTransferBlocked.Store(false)
	l.transferBlocked = make(chan struct{})
	leaseTransferBlockedOnExtensionEvent := func(
		_ roachpb.ReplicaDescriptor) {
		if l.waitForTransferBlocked.Load().(bool) {
			l.transferBlocked <- struct{}{}
			l.waitForTransferBlocked.Store(false)
		}
	}

	const numNodes = 2
	serverArgs := make(map[int]base.TestServerArgs, numNodes)
	for i := 0; i < numNodes; i++ {
		st := cluster.MakeClusterSettings()
		// leaseTransferTest tests manually control the clock and may induce clock
		// jumps. Disable the suspect timer to prevent nodes from becoming suspect
		// and being excluded as lease transfer targets when we bump clocks.
		liveness.TimeAfterNodeSuspect.Override(context.Background(), &st.SV, 0)

		serverArgs[i] = base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: testingEvalFilter,
					},
					TestingProposalFilter:                testingProposalFilter,
					LeaseTransferBlockedOnExtensionEvent: leaseTransferBlockedOnExtensionEvent,
				},
				Server: &server.TestingKnobs{
					WallClock: l.manualClock,
				},
			},
		}
	}

	l.tc = testcluster.StartTestCluster(t, numNodes,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: serverArgs,
		})
	key := l.tc.ScratchRangeWithExpirationLease(t)
	l.tc.AddVotersOrFatal(t, key, l.tc.Target(1))
	require.NoError(t, l.tc.WaitForVoters(key, l.tc.Targets(0, 1)...))

	// First, do a write; we'll use it to determine when the dust has settled.
	l.leftKey = key
	incArgs := incrementArgs(l.leftKey, 1)
	if _, pErr := kv.SendWrapped(context.Background(), l.tc.Servers[0].DistSenderI().(kv.Sender), incArgs); pErr != nil {
		t.Fatal(pErr)
	}
	l.replica0 = l.tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))
	l.replica1 = l.tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(key))
	{
		var err error
		if l.replica0Desc, err = l.replica0.GetReplicaDescriptor(); err != nil {
			t.Fatal(err)
		}
		if l.replica1Desc, err = l.replica1.GetReplicaDescriptor(); err != nil {
			t.Fatal(err)
		}
	}

	// Check that replica0 can serve reads OK.
	if pErr := l.sendRead(t, 0); pErr != nil {
		t.Fatal(pErr)
	}
	return l
}

func (l *leaseTransferTest) sendRead(t *testing.T, storeIdx int) *kvpb.Error {
	desc := l.tc.GetFirstStoreFromServer(t, storeIdx).LookupReplica(keys.MustAddr(l.leftKey))
	replicaDesc, err := desc.GetReplicaDescriptor()
	if err != nil {
		return kvpb.NewError(err)
	}
	_, pErr := kv.SendWrappedWith(
		context.Background(),
		l.tc.GetFirstStoreFromServer(t, storeIdx).TestSender(),
		kvpb.Header{RangeID: desc.RangeID, Replica: replicaDesc},
		getArgs(l.leftKey),
	)
	if pErr != nil {
		log.KvExec.Warningf(context.Background(), "%v", pErr)
	}
	return pErr
}

// checkHasLease checks that a lease for the left range is owned by a
// replica. The check is executed in a retry loop because the lease may not
// have been applied yet.
func (l *leaseTransferTest) checkHasLease(t *testing.T, storeIdx int) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		return l.sendRead(t, storeIdx).GoError()
	})
}

// setFilter is a helper function to enable/disable the blocking of
// RequestLeaseRequests on replica1. This function will notify that an
// extension is blocked on the passed in channel and will wait on the same
// channel to unblock the extension. Note that once an extension is blocked,
// the filter is cleared.
func (l *leaseTransferTest) setFilter(setTo bool, extensionSem chan struct{}) {
	l.filterMu.Lock()
	defer l.filterMu.Unlock()
	if !setTo {
		l.evalFilter = nil
		return
	}
	l.evalFilter = func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
		if filterArgs.Sid != l.tc.Target(1).StoreID {
			return nil
		}
		llReq, ok := filterArgs.Req.(*kvpb.RequestLeaseRequest)
		if !ok {
			return nil
		}
		if llReq.Lease.Replica == l.replica1Desc {
			// Notify the main thread that the extension is in progress and wait for
			// the signal to proceed.
			l.filterMu.Lock()
			l.evalFilter = nil
			l.filterMu.Unlock()
			extensionSem <- struct{}{}
			log.KvExec.Infof(filterArgs.Ctx, "filter blocking request: %s", llReq)
			<-extensionSem
			log.KvExec.Infof(filterArgs.Ctx, "filter unblocking lease request")
		}
		return nil
	}
}

// forceLeaseExtension moves the clock forward close to the lease's expiration,
// and then performs a read on the range, which will force the lease to be
// renewed. This assumes the lease is not epoch-based.
func (l *leaseTransferTest) forceLeaseExtension(
	t *testing.T, storeIdx int, lease roachpb.Lease,
) error {
	// Set the clock far enough forward to cause a lease extension, but not far
	// enough to make the node appear unhealthy.
	l.manualClock.Increment((lease.Expiration.WallTime - l.manualClock.UnixNano()) / 2)
	err := l.sendRead(t, storeIdx).GoError()
	// We can sometimes receive an error from our renewal attempt because the
	// lease transfer ends up causing the renewal to re-propose and second
	// attempt fails because it's already been renewed. This used to work
	// before we compared the proposer's lease with the actual lease because
	// the renewed lease still encompassed the previous request.
	if errors.HasType(err, (*kvpb.NotLeaseHolderError)(nil)) {
		err = nil
	}
	return err
}

// ensureLeaderAndRaftState is a helper function that blocks until leader is
// the raft leader and follower is up to date.
func (l *leaseTransferTest) ensureLeaderAndRaftState(
	t *testing.T, leader *kvserver.Replica, follower roachpb.ReplicaDescriptor,
) {
	t.Helper()
	leaderDesc, err := leader.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		r := l.tc.GetRaftLeader(t, l.replica0.Desc().StartKey)
		if r == nil {
			return errors.Errorf("could not find raft leader replica for range %d", l.replica0.RangeID)
		}
		desc, err := r.GetReplicaDescriptor()
		if err != nil {
			return errors.Wrap(err, "could not get replica descriptor")
		}
		if desc != leaderDesc {
			return errors.Errorf(
				"expected replica with id %v to be raft leader, instead got id %v",
				leaderDesc.ReplicaID,
				desc.ReplicaID,
			)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		status := leader.RaftStatus()
		progress, ok := status.Progress[raftpb.PeerID(follower.ReplicaID)]
		if !ok {
			return errors.Errorf(
				"replica %v progress not found in progress map: %v",
				follower.ReplicaID,
				status.Progress,
			)
		}
		if progress.Match < status.Commit {
			return errors.Errorf("replica %v failed to catch up", follower.ReplicaID)
		}
		return nil
	})
}

func TestLeaseExpirationBasedRangeTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	l := setupLeaseTransferTest(t)
	defer l.tc.Stopper().Stop(ctx)
	origLease, _ := l.replica0.GetLease()
	{
		// Transferring the lease to ourself should be a no-op.
		if err := l.replica0.AdminTransferLease(ctx, l.replica0Desc.StoreID, false /* bypassSafetyChecks */); err != nil {
			t.Fatal(err)
		}
		newLease, _ := l.replica0.GetLease()
		if !origLease.Equivalent(newLease, true /* expToEpochEquiv */) {
			t.Fatalf("original lease %v and new lease %v not equivalent", origLease, newLease)
		}
	}

	{
		// An invalid target should result in an error.
		const expected = "lease target replica not found in RangeDescriptor"
		if err := l.replica0.AdminTransferLease(ctx, 1000, false /* bypassSafetyChecks */); !testutils.IsError(err, expected) {
			t.Fatalf("expected %s, but found %v", expected, err)
		}
	}

	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID, false /* bypassSafetyChecks */); err != nil {
		t.Fatal(err)
	}

	// Check that replica0 doesn't serve reads any more.
	pErr := l.sendRead(t, 0)
	nlhe, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected %T, got %s", &kvpb.NotLeaseHolderError{}, pErr)
	}
	if !nlhe.Lease.Replica.Equal(&l.replica1Desc) {
		t.Fatalf("expected lease holder %+v, got %+v",
			l.replica1Desc, nlhe.Lease.Replica)
	}

	// Check that replica1 now has the lease.
	l.checkHasLease(t, 1)

	replica1Lease, _ := l.replica1.GetLease()

	// We'd like to verify the timestamp cache's low water mark, but this is
	// impossible to determine precisely in all cases because it may have
	// been subsumed by future tscache accesses. So instead of checking the
	// low water mark, we make sure that the high water mark is equal to or
	// greater than the new lease start time, which is less than the
	// previous lease's expiration time.
	if highWater := l.replica1.GetTSCacheHighWater(); highWater.Less(replica1Lease.Start.ToTimestamp()) {
		t.Fatalf("expected timestamp cache high water %s, but found %s",
			replica1Lease.Start, highWater)
	}

}

// TestLeaseExpirationBasedRangeTransferWithExtension make replica1
// extend its lease and transfer the lease immediately after
// that. Test that the transfer still happens (it'll wait until the
// extension is done).
func TestLeaseExpirationBasedRangeTransferWithExtension(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	l := setupLeaseTransferTest(t)
	defer l.tc.Stopper().Stop(ctx)
	// Ensure that replica1 has the lease.
	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID, false /* bypassSafetyChecks */); err != nil {
		t.Fatal(err)
	}
	l.checkHasLease(t, 1)

	extensionSem := make(chan struct{})
	l.setFilter(true, extensionSem)

	// Initiate an extension.
	renewalErrCh := make(chan error)
	go func() {
		lease, _ := l.replica1.GetLease()
		renewalErrCh <- l.forceLeaseExtension(t, 1, lease)
	}()

	// Wait for extension to be blocked.
	<-extensionSem
	l.waitForTransferBlocked.Store(true)
	// Initiate a transfer.
	transferErrCh := make(chan error)
	go func() {
		// Transfer back from replica1 to replica0.
		err := l.replica1.AdminTransferLease(context.Background(), l.replica0Desc.StoreID, false /* bypassSafetyChecks */)
		// Ignore not leaseholder errors which can arise due to re-proposals.
		if errors.HasType(err, (*kvpb.NotLeaseHolderError)(nil)) {
			err = nil
		}
		transferErrCh <- err
	}()
	// Wait for the transfer to be blocked by the extension.
	<-l.transferBlocked
	// Now unblock the extension.
	extensionSem <- struct{}{}
	l.checkHasLease(t, 0)
	l.setFilter(false, nil)

	if err := <-renewalErrCh; err != nil {
		t.Errorf("unexpected error from lease renewal: %+v", err)
	}
	if err := <-transferErrCh; err != nil {
		t.Errorf("unexpected error from lease transfer: %+v", err)
	}
}

// TestLeaseExpirationBasedDrainTransfer verifies that a draining store attempts to transfer away
// range leases owned by its replicas.
func TestLeaseExpirationBasedDrainTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	l := setupLeaseTransferTest(t)
	defer l.tc.Stopper().Stop(ctx)
	// We have to ensure that replica0 is the raft leader and that replica1 has
	// caught up to replica0 as draining code doesn't transfer leases to
	// behind replicas.
	l.ensureLeaderAndRaftState(t, l.replica0, l.replica1Desc)
	l.tc.GetFirstStoreFromServer(t, 0).SetDraining(true, nil /* reporter */, false /* verbose */)

	// Check that replica0 doesn't serve reads any more.
	pErr := l.sendRead(t, 0)
	nlhe, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected %T, got %s", &kvpb.NotLeaseHolderError{}, pErr)
	}
	if nlhe.Lease.Empty() || !nlhe.Lease.Replica.Equal(&l.replica1Desc) {
		t.Fatalf("expected lease holder %+v, got %+v",
			l.replica1Desc, nlhe.Lease.Replica)
	}

	// Check that replica1 now has the lease.
	l.checkHasLease(t, 1)

	l.tc.GetFirstStoreFromServer(t, 0).SetDraining(false, nil /* reporter */, false /* verbose */)
}

// TestLeaseExpirationBasedDrainTransferWithExtension verifies that
// a draining store waits for any in-progress lease requests to
// complete before transferring away the new lease.
func TestLeaseExpirationBasedDrainTransferWithExtension(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	l := setupLeaseTransferTest(t)
	defer l.tc.Stopper().Stop(ctx)
	// Ensure that replica1 has the lease.
	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID, false /* bypassSafetyChecks */); err != nil {
		t.Fatal(err)
	}
	l.checkHasLease(t, 1)

	extensionSem := make(chan struct{})
	l.setFilter(true, extensionSem)

	// Initiate an extension.
	renewalErrCh := make(chan error)
	go func() {
		lease, _ := l.replica1.GetLease()
		renewalErrCh <- l.forceLeaseExtension(t, 1, lease)
	}()

	// Wait for extension to be blocked.
	<-extensionSem

	// Make sure that replica 0 is up to date enough to receive the lease.
	l.ensureLeaderAndRaftState(t, l.replica1, l.replica0Desc)

	// Drain node 1 with an extension in progress.
	go func() {
		l.tc.GetFirstStoreFromServer(t, 1).SetDraining(true, nil /* reporter */, false /* verbose */)
	}()
	// Now unblock the extension.
	extensionSem <- struct{}{}

	l.checkHasLease(t, 0)
	l.setFilter(false, nil)

	if err := <-renewalErrCh; err != nil {
		t.Errorf("unexpected error from lease renewal: %+v", err)
	}
}

// TestLeaseExpirationBasedDrainTransferWithProscribed verifies that a draining
// store reacquires proscribed leases for ranges before transferring those
// leases away.
func TestLeaseExpirationBasedDrainTransferWithProscribed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	l := setupLeaseTransferTest(t)
	defer l.tc.Stopper().Stop(ctx)
	// Ensure that replica1 has the lease.
	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID, false /* bypassSafetyChecks */); err != nil {
		t.Fatal(err)
	}
	l.checkHasLease(t, 1)

	var failedOnce sync.Once
	failedCh := make(chan struct{})
	failLeaseTransfers := func(fail bool) {
		l.filterMu.Lock()
		defer l.filterMu.Unlock()
		if !fail {
			l.propFilter = nil
			return
		}
		l.propFilter = func(filterArgs kvserverbase.ProposalFilterArgs) *kvpb.Error {
			if filterArgs.Req.IsSingleTransferLeaseRequest() {
				target := filterArgs.Req.Requests[0].GetTransferLease().Lease.Replica
				if target == l.replica0Desc {
					failedOnce.Do(func() { close(failedCh) })
					return kvpb.NewError(leases.NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(
						target, raftutil.ReplicaStateProbe))
				}
			}
			return nil
		}
	}

	// Fail lease transfers on the target range after the previous lease has been
	// revoked (after evaluation, during raft proposal). In doing so, we leave the
	// range with a PROSCRIBED lease.
	failLeaseTransfers(true /* fail */)

	// Drain node 1.
	drainedCh := make(chan struct{})
	go func() {
		l.tc.GetFirstStoreFromServer(t, 1).SetDraining(true, nil /* reporter */, false /* verbose */)
		close(drainedCh)
	}()

	// Wait until the lease transfer has failed at least once.
	<-failedCh

	// The drain should be unable to succeed.
	select {
	case <-drainedCh:
		t.Fatalf("drain unexpectedly succeeded")
	case <-time.After(10 * time.Millisecond):
	}

	// Stop failing lease transfers.
	failLeaseTransfers(false /* fail */)

	// The drain should succeed.
	<-drainedCh
	l.checkHasLease(t, 0)
}

// TestLeaseExpirationBelowFutureTimeRequest tests two cases where a
// request is sent to a range with a future-time timestamp that is past
// the current expiration time of the range's lease. In the first case,
// the request timestamp is close enough to present time to be allowed
// to evaluate on the range after a lease expiration. In the second
// case, the request timestamp is too far in the future, so it is
// rejected.
func TestLeaseExpirationBelowFutureTimeRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "tooFarInFuture", func(t *testing.T, tooFarInFuture bool) {
		ctx := context.Background()
		l := setupLeaseTransferTest(t)
		defer l.tc.Stopper().Stop(ctx)

		// Ensure that replica1 has the lease, and that replica0 has also picked up
		// on the lease transfer.
		require.NoError(t, l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID, false /* bypassSafetyChecks */))
		l.checkHasLease(t, 1)
		preLease, _ := l.replica1.GetLease()
		require.Eventually(t, func() bool {
			lease, _ := l.replica0.GetLease()
			return lease.Replica.StoreID == l.replica1.StoreID()
		}, 5*time.Second, 100*time.Millisecond, "timed out waiting for replica 0 to pick up new lease")

		// Pause the cluster's clocks.
		l.manualClock.Pause()
		atPause := l.manualClock.UnixNano()

		// Move the clock up near (but below) the lease expiration.
		l.manualClock.Increment((preLease.Expiration.WallTime - 10) - atPause)
		now := l.tc.Servers[1].Clock().Now()

		// Construct a future-time request timestamp past the current lease's
		// expiration. See Replica.checkRequestTime for the determination
		// of whether a request timestamp is too far in the future or not.
		leaseRenewal := l.tc.Servers[1].RaftConfig().RangeLeaseRenewalDuration()
		leaseRenewalMinusStasis := leaseRenewal - l.tc.Servers[1].Clock().MaxOffset()
		reqTime := now.Add(leaseRenewalMinusStasis.Nanoseconds()-10, 0)
		if tooFarInFuture {
			reqTime = reqTime.Add(20, 0)
		}

		// Issue a get with the request timestamp.
		args := getArgs(l.leftKey)
		_, pErr := kv.SendWrappedWith(ctx, l.tc.GetFirstStoreFromServer(t, 1).TestSender(), kvpb.Header{
			RangeID: l.replica0.RangeID, Replica: l.replica1Desc, Timestamp: reqTime,
		}, args)

		if tooFarInFuture {
			// The request should have been rejected.
			require.NotNil(t, pErr)
			require.Regexp(t, "request timestamp .* too far in future", pErr)

			// Checking that the lease hasn't been extended is flaky, because it
			// may end up being extended by some other request. That fact that
			// our request was rejected is good enough.
		} else {
			// The request should have been rejected.
			require.Nil(t, pErr)

			// The lease should have been extended.
			l.checkHasLease(t, 1)
			postLease, _ := l.replica1.GetLease()
			require.True(t, preLease.Expiration.Less(*postLease.Expiration), "expected extension")
		}
	})
}

// TestRangeLocalUncertaintyLimitAfterNewLease verifies that on lease
// transfer, the normal limiting of a request's local uncertainty limit
// to the first observed timestamp on a node is extended to include the
// lease start timestamp. This disallows the possibility that a write to
// another replica of the range (on node n1) happened at a later
// timestamp than the originally observed timestamp for the node which
// now owns the lease (n2). This can happen if the replication of the
// write doesn't make it from n1 to n2 before the transaction observes
// n2's clock time.
func TestRangeLocalUncertaintyLimitAfterNewLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numServers := 2
	var manuals []*hlc.HybridManualClock
	for i := 0; i < numServers; i++ {
		manuals = append(manuals, hlc.NewHybridManualClock())
	}
	serverArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		serverArgs[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manuals[i],
				},
			},
		}
	}
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: serverArgs,
		})
	defer tc.Stopper().Stop(ctx)

	keyA := tc.ScratchRange(t)

	for _, m := range manuals {
		m.Pause()
	}
	// Set the first clock at lease t+2 compared to the second nodes clock.
	if manuals[0].UnixNano() <= manuals[1].UnixNano() {
		manuals[0].Increment((manuals[1].UnixNano() - manuals[0].UnixNano()) + 2)
	}

	// Start a transaction using node2 as a gateway.
	txn := roachpb.MakeTransaction("test", keyA, isolation.Serializable, 1, tc.Servers[1].Clock().Now(), tc.Servers[1].Clock().MaxOffset().Nanoseconds(), int32(tc.Servers[1].SQLInstanceID()), 0, false /* omitInRangefeeds */)
	// Simulate a read to another range on node2 by setting the observed timestamp.
	txn.UpdateObservedTimestamp(2, tc.Servers[1].Clock().NowAsClockTimestamp())

	// Do a write on node1 to establish a key with its timestamp at now.
	if _, pErr := kv.SendWrapped(
		ctx, tc.Servers[0].DistSenderI().(kv.Sender), putArgs(keyA, []byte("value")),
	); pErr != nil {
		t.Fatal(pErr)
	}

	// Up-replicate the data in the range to node2.
	tc.AddVotersOrFatal(t, keyA, tc.Target(1))
	replica2 := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(keyA))

	// Transfer the lease from node1 to node2.
	node1Before := tc.Servers[0].Clock().Now()
	tc.TransferRangeLeaseOrFatal(t, *replica2.Desc(), tc.Target(1))
	testutils.SucceedsSoon(t, func() error {
		lease, _ := replica2.GetLease()
		if lease.Replica.NodeID != replica2.NodeID() {
			return errors.Errorf("expected lease transfer to apply on node2: %s", lease)
		}
		return nil
	})
	// Verify that after the lease transfer, node2's clock has advanced to at
	// least match node1's from before the lease transfer.
	node2After := tc.Servers[1].Clock().Now()
	if node2After.Less(node1Before) {
		t.Fatalf("expected node2's clock walltime to be >= %s; got %s", node1Before, node2After)
	}

	// Send a get request for keyA to node2, which is now the
	// leaseholder. If the max timestamp were not being properly limited,
	// we would end up incorrectly reading nothing for keyA. Instead we
	// expect to see an uncertainty interval error.
	h := kvpb.Header{Txn: &txn}
	if _, pErr := kv.SendWrappedWith(
		ctx, tc.Servers[0].DistSenderI().(kv.Sender), h, getArgs(keyA),
	); !testutils.IsPError(pErr, "uncertainty") {
		t.Fatalf("expected an uncertainty interval error; got %v", pErr)
	}
}

// TestLeaseMetricsOnSplitAndTransfer verifies that lease-related metrics
// are updated after splitting a range and then initiating one successful
// and one failing lease transfer.
func TestLeaseMetricsOnSplitAndTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunValues(t, "lease-type", roachpb.EpochAndLeaderLeaseType(), func(t *testing.T, leaseType roachpb.LeaseType) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)

		var injectLeaseTransferError atomic.Value
		testingEvalFilter := func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if args, ok := filterArgs.Req.(*kvpb.TransferLeaseRequest); ok {
				if val := injectLeaseTransferError.Load(); val != nil && val.(bool) {
					// Note that we can't just return an error here as we only
					// end up counting failures in the metrics if the command
					// makes it through to being executed. So use a fake replica ID.
					args.Lease.Replica.ReplicaID = 1000
				}
			}
			return nil
		}

		manualClock := hlc.NewHybridManualClock()
		tc := testcluster.StartTestCluster(t, 2,
			base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Settings: st,
					Knobs: base.TestingKnobs{
						Store: &kvserver.StoreTestingKnobs{
							EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
								TestingEvalFilter: testingEvalFilter,
							},
						},
						Server: &server.TestingKnobs{
							WallClock: manualClock,
						},
					},
				},
			})
		defer tc.Stopper().Stop(ctx)

		// Up-replicate to two replicas.
		expirationKey := tc.ScratchRangeWithExpirationLease(t)
		expirationDesc := tc.LookupRangeOrFatal(t, expirationKey)
		tc.AddVotersOrFatal(t, expirationKey, tc.Target(1))

		key := tc.ScratchRange(t)
		tc.AddVotersOrFatal(t, key, tc.Target(1))

		// Now, a successful transfer from LHS replica 0 to replica 1.
		injectLeaseTransferError.Store(false)
		tc.TransferRangeLeaseOrFatal(t, expirationDesc, tc.Target(1))
		// Wait for all replicas to process.
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < 2; i++ {
				r := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(expirationKey))
				if l, _ := r.GetLease(); l.Replica.StoreID != tc.Target(1).StoreID {
					return errors.Errorf("expected lease to transfer to replica 2: got %s", l)
				}
			}
			return nil
		})

		// Next a failed transfer from RHS replica 0 to replica 1.
		injectLeaseTransferError.Store(true)
		splitDesc := tc.LookupRangeOrFatal(t, key)
		err := tc.TransferRangeLease(splitDesc, tc.Target(1))
		// We expect this to fail.
		require.Error(t, err)

		metrics := tc.GetFirstStoreFromServer(t, 0).Metrics()
		if a, e := metrics.LeaseTransferSuccessCount.Count(), int64(1); a != e {
			t.Errorf("expected %d lease transfer successes; got %d", e, a)
		}
		// We mostly expect precisely one error, but there's a retry loop in
		// `AdminTransferLease` that prevents transfers to followers who might need a
		// snapshot. This can sometimes lead to additional errors being reported.
		if a := metrics.LeaseTransferErrorCount.Count(); a == 0 {
			t.Errorf("expected at least one lease transfer errors; got %d", a)
		}

		// Expire current leases and put a key to the epoch based scratch range to
		// get a lease.
		testutils.SucceedsSoon(t, func() error {
			manualClock.Increment(tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().LeaseExpiration())
			if err := tc.GetFirstStoreFromServer(t, 0).DB().Put(context.Background(), key, "foo"); err != nil {
				return err
			}

			// Update replication gauages for all stores. Then, depending on the version
			// of the test, verify they're correct. In particular:
			// - For the epoch lease variant, we expect 1 epoch lease and 1 expiration
			// lease. No leader leases.
			// - For the leader leases variant, we expect 1 leader lease.
			var expirationLeases int64
			var epochLeases int64
			var leaderLeases int64
			for i := range tc.Servers {
				if err := tc.GetFirstStoreFromServer(t, i).ComputeMetrics(context.Background()); err != nil {
					return err
				}
				metrics = tc.GetFirstStoreFromServer(t, i).Metrics()
				expirationLeases += metrics.LeaseExpirationCount.Value()
				epochLeases += metrics.LeaseEpochCount.Value()
				leaderLeases += metrics.LeaseLeaderCount.Value()
			}
			switch leaseType {
			case roachpb.LeaseLeader:
				if a, e := expirationLeases, int64(0); a != e {
					return errors.Errorf("expected %d expiration lease count; got %d", e, a)
				}
				if a, e := epochLeases, int64(0); a != e {
					return errors.Errorf("expected %d epoch lease count; got %d", e, a)
				}
				if a, e := leaderLeases, int64(1); a < e {
					return errors.Errorf("expected greater than %d leader lease count; got %d", e, a)
				}
			case roachpb.LeaseEpoch:
				if a, e := expirationLeases, int64(1); a != e {
					// For the NodeLiveness range.
					return errors.Errorf("expected %d expiration lease count; got %d", e, a)
				}
				if a, e := epochLeases, int64(1); a < e {
					return errors.Errorf("expected greater than %d epoch lease count; got %d", e, a)
				}
				if a, e := leaderLeases, int64(0); a != e {
					return errors.Errorf("expected exactly %d leader lease count; got %d", e, a)
				}
			default:
				panic("unexpected lease type")
			}
			return nil
		})
	})
}

// Test that leases held before a restart are not used after the restart.
// See replica.mu.minLeaseProposedTS for the reasons why this isn't allowed.
func TestLeaseNotUsedAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	var leaseAcquisitionTrap atomic.Value
	ctx := context.Background()
	manual := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgs: base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:    true,
						StickyVFSID: "1",
					},
				},
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock:         manual,
						StickyVFSRegistry: stickyVFSRegistry,
					},
					Store: &kvserver.StoreTestingKnobs{
						LeaseRequestEvent: func(ts hlc.Timestamp, _ roachpb.StoreID, _ roachpb.RangeID) *kvpb.Error {
							val := leaseAcquisitionTrap.Load()
							if val == nil {
								return nil
							}
							trapCallback := val.(func(ts hlc.Timestamp))
							if trapCallback != nil {
								trapCallback(ts)
							}
							return nil
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	key := []byte("a")
	// Send a read, to acquire a lease.
	getArgs := getArgs(key)
	if _, err := kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, 0).TestSender(), getArgs); err != nil {
		t.Fatal(err)
	}

	preRestartLease, _ := tc.GetFirstStoreFromServer(t, 0).LookupReplica(key).GetLease()

	manual.Increment(1e9)

	// Restart the mtc. Before we do that, we're installing a callback used to
	// assert that a new lease has been requested. The callback is installed
	// before the restart, as the lease might be requested at any time and for
	// many reasons by background processes, even before we send the read below.
	leaseAcquisitionCh := make(chan error)
	var once sync.Once
	leaseAcquisitionTrap.Store(func(_ hlc.Timestamp) {
		once.Do(func() {
			close(leaseAcquisitionCh)
		})
	})

	log.KvExec.Info(ctx, "restarting")
	require.NoError(t, tc.Restart())

	// Send another read and check that the pre-existing lease has not been used.
	// Concretely, we check that a new lease is requested.
	if _, err := kv.SendWrapped(ctx, tc.GetFirstStoreFromServer(t, 0).TestSender(), getArgs); err != nil {
		t.Fatal(err)
	}
	// Check that the Send above triggered a lease acquisition.
	select {
	case <-leaseAcquisitionCh:
	case <-time.After(time.Second):
		t.Fatalf("read did not acquire a new lease")
	}

	postRestartLease, _ := tc.GetFirstStoreFromServer(t, 0).LookupReplica(key).GetLease()

	// Verify that not only is a new lease requested, it also gets a new sequence
	// number. This makes sure that previously proposed commands actually fail at
	// apply time.
	if preRestartLease.Sequence == postRestartLease.Sequence {
		t.Fatalf("lease was not replaced:\nprev: %v\nnow:  %v", preRestartLease, postRestartLease)
	}
}

// Test that a lease extension (a RequestLeaseRequest that doesn't change the
// lease holder) is not blocked by ongoing reads. The test relies on the fact
// that RequestLeaseRequest does not declare to touch the whole key span of the
// range, and thus don't conflict through latches with other reads.
func TestLeaseExtensionNotBlockedByRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	readBlocked := make(chan struct{})
	cmdFilter := func(fArgs kvserverbase.FilterArgs) *kvpb.Error {
		if fArgs.Hdr.UserPriority == 42 {
			// Signal that the read is blocked.
			readBlocked <- struct{}{}
			// Wait for read to be unblocked.
			<-readBlocked
		}
		return nil
	}
	s := serverutils.StartServerOnly(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: cmdFilter,
					},
				},
			},
		})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}

	// Start a read and wait for it to block.
	key := roachpb.Key("a")
	errChan := make(chan error)
	go func() {
		getReq := kvpb.GetRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: key,
			},
		}
		if _, pErr := kv.SendWrappedWith(ctx, s.DB().NonTransactionalSender(),
			kvpb.Header{UserPriority: 42},
			&getReq); pErr != nil {
			errChan <- pErr.GoError()
		}
	}()

	select {
	case err := <-errChan:
		t.Fatal(err)
	case <-readBlocked:
		// Send the lease request.
		rKey, err := keys.Addr(key)
		if err != nil {
			t.Fatal(err)
		}
		repl := store.LookupReplica(rKey)
		if repl == nil {
			t.Fatalf("replica for key %s not found", rKey)
		}
		replDesc, found := repl.Desc().GetReplicaDescriptor(store.StoreID())
		if !found {
			t.Fatalf("replica descriptor for key %s not found", rKey)
		}

		now := s.Clock().NowAsClockTimestamp()
		leaseReq := kvpb.RequestLeaseRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: key,
			},
			Lease: roachpb.Lease{
				Start:      now,
				Expiration: now.ToTimestamp().Add(time.Second.Nanoseconds(), 0).Clone(),
				Replica:    replDesc,
				ProposedTS: now,
			},
		}

		for {
			leaseInfo, _, err := s.GetRangeLease(ctx, key, roachpb.AllowQueryToBeForwardedToDifferentNode)
			if err != nil {
				t.Fatal(err)
			}
			leaseReq.PrevLease = leaseInfo.CurrentOrProspective()
			leaseReq.Lease.Sequence = leaseReq.PrevLease.Sequence + 1

			_, pErr := kv.SendWrapped(ctx, s.DB().NonTransactionalSender(), &leaseReq)
			if _, ok := pErr.GetDetail().(*kvpb.AmbiguousResultError); ok {
				log.KvExec.Infof(ctx, "retrying lease after %s", pErr)
				continue
			}
			if _, ok := pErr.GetDetail().(*kvpb.LeaseRejectedError); ok {
				// Lease rejected? Try again. The extension should work because
				// extending is idempotent (assuming the PrevLease matches).
				log.KvExec.Infof(ctx, "retrying lease after %s", pErr)
				continue
			}
			if pErr != nil {
				t.Errorf("%T %s", pErr.GetDetail(), pErr) // NB: don't fatal or shutdown hangs
			}
			break
		}
		// Unblock the read.
		readBlocked <- struct{}{}
	}
}
func validateLeaseholderSoon(
	t *testing.T, db *kv.DB, key roachpb.Key, replica roachpb.ReplicaDescriptor, isTarget bool,
) {
	testutils.SucceedsSoon(t, func() error {
		leaseInfo := getLeaseInfoOrFatal(t, context.Background(), db, key)
		if isTarget && leaseInfo.Lease.Replica != replica {
			return fmt.Errorf("lease holder should be replica %+v, but is: %+v",
				replica, leaseInfo.Lease.Replica)
		} else if !isTarget && leaseInfo.Lease.Replica == replica {
			return fmt.Errorf("lease holder still on replica: %+v", replica)
		}
		return nil
	})
}

func getLeaseInfoOrFatal(
	t *testing.T, ctx context.Context, db *kv.DB, key roachpb.Key,
) *kvpb.LeaseInfoResponse {
	header := kvpb.Header{
		// INCONSISTENT read with a NEAREST routing policy, since we want to make
		// sure that the node used to send this is the one that processes the
		// command, regardless of whether it is the leaseholder.
		ReadConsistency: kvpb.INCONSISTENT,
		RoutingPolicy:   kvpb.RoutingPolicy_NEAREST,
	}
	leaseInfoReq := &kvpb.LeaseInfoRequest{RequestHeader: kvpb.RequestHeader{Key: key}}
	reply, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), header, leaseInfoReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	return reply.(*kvpb.LeaseInfoResponse)
}

func TestRemoveLeaseholder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.Background())
	_, rhsDesc := tc.SplitRangeOrFatal(t, bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec))

	// We start with having the range under test on (1,2,3).
	tc.AddVotersOrFatal(t, rhsDesc.StartKey.AsRawKey(), tc.Targets(1, 2)...)

	// Make sure the lease is on 1.
	tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(0))
	leaseHolder, err := tc.FindRangeLeaseHolder(rhsDesc, nil)
	require.NoError(t, err)
	require.Equal(t, tc.Target(0), leaseHolder)

	// Remove server 1.
	tc.RemoveLeaseHolderOrFatal(t, rhsDesc, tc.Target(0), tc.Target(1))

	// Check that the lease moved away from 1.
	leaseHolder, err = tc.FindRangeLeaseHolder(rhsDesc, nil)
	require.NoError(t, err)
	require.NotEqual(t, tc.Target(0), leaseHolder)
}

// TestConsistencyQueueDelaysProcessingNewRanges verifies that the consistency
// queue delays processing of new ranges.
func TestConsistencyQueueDelaysProcessingNewRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// checkConsistency runs a consistency check on the specified key range and
	// verifies that all ranges are consistent. Useful to verify that we don't
	// break the consistency after splits/merges.
	checkConsistency := func() error {
		req := kvpb.CheckConsistencyRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    roachpb.Key("a"),
				EndKey: roachpb.Key("z"),
			},
			Mode: kvpb.ChecksumMode_CHECK_FULL,
		}

		b := kv.Batch{}
		b.AddRawRequest(&req)
		err := s.DB().Run(ctx, &b)
		require.NoError(t, err)

		if len(b.RawResponse().Responses) == 0 {
			return errors.Errorf("received 0 responses")
		}

		constResp := b.RawResponse().Responses[0].GetInner().(*kvpb.CheckConsistencyResponse)
		for i := range len(b.RawResponse().Responses) {
			if constResp.Result[i].Status != kvpb.CheckConsistencyResponse_RANGE_CONSISTENT &&
				constResp.Result[i].Status !=
					kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_ESTIMATED {
				return errors.Errorf("expected range to be consistent, but found: %+v", constResp.Result[i])
			}
		}
		return nil
	}

	// splitHelper helps create splits for TestServerInterface.
	splitHelper := func(key roachpb.Key) error {
		rngID := store.LookupReplica(roachpb.RKey(key)).RangeID
		h := kvpb.Header{RangeID: rngID}
		args := adminSplitArgs(key)
		if _, pErr := kv.SendWrappedWith(ctx, kvserver.ToSenderForTesting(store), h, args); pErr != nil {
			return pErr.GoError()
		}
		return nil
	}

	// splitHelper helps create merges for TestServerInterface.
	mergeHelper := func(key roachpb.Key) error {
		rngID := store.LookupReplica(roachpb.RKey(key)).RangeID
		h := kvpb.Header{RangeID: rngID}
		args := adminMergeArgs(key)
		if _, pErr := kv.SendWrappedWith(ctx, kvserver.ToSenderForTesting(store), h, args); pErr != nil {
			return pErr.GoError()
		}
		return nil
	}

	keyA := roachpb.Key("a")
	require.NoError(t, splitHelper(keyA))
	_, replA := getFirstStoreReplica(t, s, keyA)
	lastConsistencyTSReplA, err := replA.GetQueueLastProcessed(ctx, "consistencyChecker")
	require.NoError(t, err)

	// Assert that the last consistency check was set to a recent timestamp.
	require.LessOrEqual(t, timeutil.Since(lastConsistencyTSReplA.GoTime()), time.Minute)

	// Assert that the range is consistent.
	require.NoError(t, checkConsistency())

	// Assert that splitting the range copied the last consistency check timestamp
	// from the LHS to the RHS.
	keyB := roachpb.Key("b")
	require.NoError(t, splitHelper(keyB))
	_, replB := getFirstStoreReplica(t, s, keyB)
	lastConsistencyTSReplB, err := replB.GetQueueLastProcessed(ctx, "consistencyChecker")
	require.NoError(t, err)
	require.Equal(t, lastConsistencyTSReplA, lastConsistencyTSReplB)

	// Assert that ranges are still consistent.
	require.NoError(t, checkConsistency())

	// isEligibleForConsistencyQueue returns true if the range is eligible for the consistency queue.
	isEligibleForConsistencyQueue := func(
		ctx context.Context, manualClock *hlc.HybridManualClock, desc *roachpb.RangeDescriptor,
	) bool {
		getQueueLastProcessed := func(ctx context.Context) (hlc.Timestamp, error) {
			_, repl := getFirstStoreReplica(t, s, roachpb.Key(desc.StartKey))
			lastConsistencyTSRepl, err := repl.GetQueueLastProcessed(ctx, "consistencyChecker")
			require.NoError(t, err)
			return lastConsistencyTSRepl, nil
		}

		isNodeAvailable := func(nodeID roachpb.NodeID) bool {
			return true
		}

		shouldQ, _ := kvserver.ConsistencyQueueShouldQueue(
			ctx, hlc.ClockTimestamp{WallTime: manualClock.Now().UnixNano()}, desc, getQueueLastProcessed,
			isNodeAvailable, false, 24*time.Hour)
		return shouldQ
	}

	// Assert that the ranges are not eligible for the consistency queue.
	manualClock := hlc.NewHybridManualClock()
	lhsDesc := store.LookupReplica(roachpb.RKey(keyA)).Desc()
	rhsDesc := store.LookupReplica(roachpb.RKey(keyB)).Desc()
	require.False(t, isEligibleForConsistencyQueue(context.Background(), manualClock, lhsDesc))
	require.False(t, isEligibleForConsistencyQueue(context.Background(), manualClock, rhsDesc))

	// Advance the clock to simulate enough time passing to make the ranges
	// eligible for the consistency queue.
	manualClock.Increment(24 * time.Hour.Nanoseconds())
	require.True(t, isEligibleForConsistencyQueue(context.Background(), manualClock, lhsDesc))
	require.True(t, isEligibleForConsistencyQueue(context.Background(), manualClock, rhsDesc))

	// Merge the two ranges together, and make sure that the last consistency check remains the same.
	require.NoError(t, mergeHelper(keyA))
	_, replMerged := getFirstStoreReplica(t, s, keyA)
	lastConsistencyTSMergedRepl, err := replMerged.GetQueueLastProcessed(ctx, "consistencyChecker")
	require.NoError(t, err)
	require.Equal(t, lastConsistencyTSReplA, lastConsistencyTSMergedRepl)

	// Assert that ranges are still consistent.
	require.NoError(t, checkConsistency())
}

func TestLeaseInfoRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	kvDB0 := tc.Servers[0].DB()

	key := []byte("a")
	rangeDesc, err := tc.LookupRange(key)
	if err != nil {
		t.Fatal(err)
	}
	replicas := make([]roachpb.ReplicaDescriptor, 3)
	for i := 0; i < 3; i++ {
		var ok bool
		replicas[i], ok = rangeDesc.GetReplicaDescriptor(tc.Servers[i].GetFirstStoreID())
		if !ok {
			t.Fatalf("expected to find replica in server %d", i)
		}
	}

	// Transfer the lease to Servers[0] so we start in a known state. Otherwise,
	// there might be already a lease owned by a random node.
	tc.TransferRangeLeaseOrFatal(t, rangeDesc, tc.Target(0))

	// Now test the LeaseInfo. We might need to loop until the node we query has applied the lease.
	validateLeaseholderSoon(t, kvDB0, rangeDesc.StartKey.AsRawKey(), replicas[0], true)

	// Transfer the lease to Server 1 and check that LeaseInfoRequest gets the
	// right answer.
	tc.TransferRangeLeaseOrFatal(t, rangeDesc, tc.Target(1))

	// An inconsistent LeaseInfoReqeust on the old lease holder should give us the
	// right answer immediately, since the old holder has definitely applied the
	// transfer before TransferRangeLease returned.
	leaseInfo := getLeaseInfoOrFatal(t, context.Background(), kvDB0, rangeDesc.StartKey.AsRawKey())
	if !leaseInfo.Lease.Replica.Equal(replicas[1]) {
		t.Fatalf("lease holder should be replica %+v, but is: %+v",
			replicas[1], leaseInfo.Lease.Replica)
	}

	// A read on the new lease holder does not necessarily succeed immediately,
	// since it might take a while for it to apply the transfer.
	// We can't reliably do a CONSISTENT read here, even though we're reading
	// from the supposed lease holder, because this node might initially be
	// unaware of the new lease and so the request might bounce around for a
	// while (see #8816).
	validateLeaseholderSoon(t, kvDB0, rangeDesc.StartKey.AsRawKey(), replicas[1], true)

	// Transfer the lease to Server 2 and check that LeaseInfoRequest gets the
	// right answer.
	tc.TransferRangeLeaseOrFatal(t, rangeDesc, tc.Target(2))

	// We're now going to ask servers[1] for the lease info. We don't use kvDB1;
	// instead we go directly to the store because otherwise the DistSender might
	// use an old, cached, version of the range descriptor that doesn't have the
	// local replica in it (and so the request would be routed away).
	// TODO(andrei): Add a batch option to not use the range cache.
	s, err := tc.Servers[1].GetStores().(*kvserver.Stores).GetStore(tc.Servers[1].GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}
	leaseInfoReq := &kvpb.LeaseInfoRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: rangeDesc.StartKey.AsRawKey(),
		},
	}
	reply, pErr := kv.SendWrappedWith(
		context.Background(), kvserver.ToSenderForTesting(s), kvpb.Header{
			RangeID:         rangeDesc.RangeID,
			ReadConsistency: kvpb.INCONSISTENT,
		}, leaseInfoReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	resp := *(reply.(*kvpb.LeaseInfoResponse))

	if !resp.Lease.Replica.Equal(replicas[2]) {
		t.Fatalf("lease holder should be replica %s, but is: %s", replicas[2], resp.Lease.Replica)
	}

	// TODO(andrei): test the side-effect of LeaseInfoRequest when there's no
	// active lease - the node getting the request is supposed to acquire the
	// lease. This requires a way to expire leases; the TestCluster probably needs
	// to use a mock clock.
}

// Test that an error encountered by a read-only "NonKV" command is not
// swallowed, and doesn't otherwise cause a panic.
// We had a bug cause by the fact that errors for these commands aren't passed
// through the epilogue returned by replica.beginCommands() and were getting
// swallowed.
func TestErrorHandlingForNonKVCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	cmdFilter := func(fArgs kvserverbase.FilterArgs) *kvpb.Error {
		if fArgs.Hdr.UserPriority == 42 {
			return kvpb.NewErrorf("injected error")
		}
		return nil
	}
	s := serverutils.StartServerOnly(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: cmdFilter,
					},
				},
			},
		})
	defer s.Stopper().Stop(context.Background())

	// Send the lease request.
	key := roachpb.Key("a")
	leaseReq := kvpb.LeaseInfoRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	}
	_, pErr := kv.SendWrappedWith(
		context.Background(),
		s.DB().NonTransactionalSender(),
		kvpb.Header{UserPriority: 42},
		&leaseReq,
	)
	if !testutils.IsPError(pErr, "injected error") {
		t.Fatalf("expected error %q, got: %s", "injected error", pErr)
	}
}

// Test that, if a client makes a request to a range that has recently split and
// the client indicates that it has pre-split info, the serve replies with
// updated info on both sides of the split. The server has a heuristic for
// figuring out what info to return to the client.
func TestRangeInfoAfterSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	rkey := keys.MustAddr(key)
	r := store.LookupReplica(rkey)
	require.NotNil(t, r)
	preSplitDesc := r.Desc()

	lDesc, rDesc, err := s.SplitRange(key.Next())
	require.NoError(t, err)

	tests := []struct {
		name    string
		key     roachpb.RKey
		rangeID roachpb.RangeID
	}{
		{
			name:    "query left",
			key:     lDesc.StartKey,
			rangeID: lDesc.RangeID,
		},
		{
			name: "query right",
			key:  rDesc.StartKey,
			// This test is not realistic since, if the client has the pre-split
			// descriptor, it wouldn't know to put the correct RangeID when trying to
			// address the RHS. As such, it would send the request with the pre-split
			// range ID, which corresponds to the LHS' id (after the split), and, at
			// least as of this writing, it would receive a RangeKeyMismatchError. We
			// test the situation where the request is routed correctly to the RHS
			// anyway.
			rangeID: rDesc.RangeID,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ba := &kvpb.BatchRequest{
				Header: kvpb.Header{
					RangeID: tc.rangeID,
					ClientRangeInfo: roachpb.ClientRangeInfo{
						DescriptorGeneration: preSplitDesc.Generation,
					},
				},
			}
			gArgs := &kvpb.GetRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: tc.key.AsRawKey(),
				},
			}
			ba.Add(gArgs)
			br, pErr := kvserver.ToSenderForTesting(store).Send(ctx, ba)
			require.NoError(t, pErr.GoError())
			descs := make([]roachpb.RangeDescriptor, len(br.RangeInfos))
			for i, ri := range br.RangeInfos {
				descs[i] = ri.Desc
			}

			// Sort the descriptors we got because their order is inconsistent between
			// the subtests.
			sort.Slice(descs, func(i, j int) bool {
				return descs[i].RangeID < descs[j].RangeID
			})

			require.Equal(t, []roachpb.RangeDescriptor{lDesc, rDesc}, descs)
		})
	}
}

// TestDrainRangeRejection verifies that an attempt to transfer a range to a
// draining store fails.
func TestDrainRangeRejection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))

	drainingIdx := 1
	tc.GetFirstStoreFromServer(t, 1).SetDraining(true, nil /* reporter */, false /* verbose */)
	chgs := kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(drainingIdx))
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserverpb.ReasonRangeUnderReplicated, "", chgs); !testutils.IsError(err, "store is draining") {
		t.Fatalf("unexpected error: %+v", err)
	}
}

func TestChangeReplicasGeneration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(key))

	oldGeneration := repl.Desc().Generation
	chgs := kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1))
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.EqualValues(t, repl.Desc().Generation, oldGeneration+2)

	oldGeneration = repl.Desc().Generation
	oldDesc := repl.Desc()
	chgs[0].ChangeType = roachpb.REMOVE_VOTER
	newDesc, err := repl.ChangeReplicas(context.Background(), oldDesc, kvserverpb.ReasonRangeOverReplicated, "", chgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Generation changes:
	// +1 for entering joint config due to demotion
	// +1 for transitioning out of joint config
	// +1 for removing learner
	assert.EqualValues(t, repl.Desc().Generation, oldGeneration+3, "\nold: %+v\nnew: %+v", oldDesc, newDesc)
}

// TestLossQuorumCauseLeaderlessWatcherToSignalUnavailable checks that if a
// range lost its quorum, the remaining replicas in that range will have their
// leaderlessWatcher indicate that the range is unavailable. Also, it checks
// that when the range regains quorum, the leaderlessWatcher will indicate that
// the range is available.
func TestLossQuorumCauseLeaderlessWatcherToSignalUnavailable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Increase the verbosity of the test to help investigate failures.
	testutils.SetVModule(t, "replica_range_lease=3,raft=4")

	ctx := context.Background()
	stickyVFSRegistry := fs.NewStickyRegistry()
	lisReg := listenerutil.NewListenerRegistry()
	defer lisReg.Close()

	// Perform a basic test setup with two nodes.
	const numServers int = 2
	st := cluster.MakeTestingClusterSettings()

	// Set `kv.replica_raft.leaderless_unavailable_threshold` to 10 seconds.
	threshold := time.Second * 10
	kvserver.ReplicaLeaderlessUnavailableThreshold.Override(ctx, &st.SV, threshold)

	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			Settings: st,
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:    true,
					StickyVFSID: strconv.FormatInt(int64(i), 10),
				},
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Make sure that we don't depend on the consistency queue
					// running. It could cause replicas to attempt to acquire the
					// lease, which might unintentionally unquiesce replicas. See #146188.
					DisableConsistencyQueue: true,
				},
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:     base.ReplicationManual,
			ReusableListenerReg: lisReg,
			ServerArgsPerNode:   stickyServerArgs,
		})

	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1)...)
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)

	// Make sure that the range is up and functional.
	// Make sure that there is a fully functioning quorum before it introduce a
	// temporary unavailability. This deflakes the test especially for epoch
	// leases because the node that we haven't restarted is guaranteed to not be a
	// learner, and therefore it can campaign and unquiesce the recently restarted
	// node.
	_, pErr := kv.SendWrapped(context.Background(),
		tc.GetFirstStoreFromServer(t, 0).TestSender(), putArgs(key, []byte("init")))
	require.NoError(t, pErr.GoError())

	// Randomly stop server index 0 or 1.
	stoppedNodeInx := rand.Intn(2)
	aliveNodeIdx := 1 - stoppedNodeInx
	log.KvExec.Infof(ctx, "stopping node id: %d", stoppedNodeInx+1)
	tc.StopServer(stoppedNodeInx)
	repl := tc.GetFirstStoreFromServer(t, aliveNodeIdx).LookupReplica(roachpb.RKey(key))

	// The range is available initially.
	require.False(t, repl.LeaderlessWatcher.IsUnavailable())

	// Wait until the remaining replica becomes leaderless.
	testutils.SucceedsSoon(t, func() error {
		if repl.RaftStatus().Lead != raft.None {
			return errors.New("Leader still exists")
		}
		return nil
	})

	// Wait for the leaderlessWatcher to indicate that the range is unavailable.
	testutils.SucceedsSoon(t, func() error {
		if !repl.LeaderlessWatcher.IsUnavailable() {
			return errors.New("range is still available")
		}
		return nil
	})

	sendPutRequestWithTimeout := func(repl *kvserver.Replica, timeout time.Duration) (*kvpb.Error, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		ba := &kvpb.BatchRequest{}
		ba.RangeID = desc.RangeID
		ba.Timestamp = repl.Clock().Now()
		ba.Add(putArgs(key, []byte("foo")))
		_, pErr := kvserver.ToSenderForTesting(repl).Send(ctx, ba)
		return pErr, ctx.Err()
	}

	// Requests should immediately return an error indicating that the range is
	// unavailable.
	pErr, ctxErr := sendPutRequestWithTimeout(repl, 2*time.Second)
	require.NoError(t, ctxErr)
	require.Regexp(t, "replica has been leaderless for 10s", pErr)
	require.True(t, errors.HasType(pErr.GoError(), (*kvpb.ReplicaUnavailableError)(nil)),
		"expected ReplicaUnavailableError, got %v", err)

	// At this point we know that the replica is considered unavailable. Regain
	// the quorum and check that the leaderlessWatcher indicates that the range is
	// available.
	require.NoError(t, tc.RestartServer(stoppedNodeInx))

	testutils.SucceedsSoon(t, func() error {
		repl = tc.GetFirstStoreFromServer(t, aliveNodeIdx).LookupReplica(roachpb.RKey(key))
		tc.GetFirstStoreFromServer(t, aliveNodeIdx).LookupReplica(roachpb.RKey(key))
		if repl.LeaderlessWatcher.IsUnavailable() {
			return errors.New("range is still unavailable")
		}
		return nil
	})

	// Requests should now succeed. We need to try both replicas to avoid
	// NotLeaseHolderErrors.
	testutils.SucceedsSoon(t, func() error {
		for i := range numServers {
			repl := tc.GetFirstStoreFromServer(t, i).LookupReplica(roachpb.RKey(key))
			pErr, ctxErr = sendPutRequestWithTimeout(repl, 2*time.Second)
			if ctxErr == nil && pErr == nil {
				return nil
			}
		}
		// If we reach this point, we know that the request failed, return the
		// error.
		if ctxErr != nil {
			return ctxErr
		}
		return pErr.GoError()
	})
}

// TestLeaderlessWatcherUnavailabilityErrorRefreshedOnUnavailabilityTransition
// ensures that the leaderless watcher constructs a new error every time it
// transitions to the unavailable state. In particular, the descriptor used
// in the error should be the latest descriptor.
// Serves as a regression test for
// https://github.com/cockroachdb/cockroach/issues/144639.
func TestLeaderlessWatcherErrorRefreshedOnUnavailabilityTransition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	manual := hlc.NewHybridManualClock()
	st := cluster.MakeTestingClusterSettings()
	// Set the leaderless threshold to 10 second.
	kvserver.ReplicaLeaderlessUnavailableThreshold.Override(ctx, &st.SV, 10*time.Second)

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					WallClock: manual,
				},
				Store: &kvserver.StoreTestingKnobs{
					// Disable raft ticks from refreshing the leaderless watcher
					// state, allowing the test full control over the leaderless
					// watcher state.
					DisableLeaderlessWatcherRefreshOnRaftTick: true,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1)...)
	repl := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(key))

	// The leaderlessWatcher starts off as available.
	require.False(t, repl.LeaderlessWatcher.IsUnavailable())
	// Let it know it's leaderless.
	repl.TestingRefreshLeaderlessWatcherUnavailableState(ctx, raft.None, manual.Now(), st)
	// Even though the replica is leaderless, enough time hasn't passed for it to
	// be considered unavailable.
	require.False(t, repl.LeaderlessWatcher.IsUnavailable())
	// The error should be nil as we're not considered leaderless at this point.
	require.NoError(t, repl.LeaderlessWatcher.Err())
	// Let enough time pass.
	manual.Increment(10 * time.Second.Nanoseconds())
	repl.TestingRefreshLeaderlessWatcherUnavailableState(ctx, raft.None, manual.Now(), st)
	// Now the replica is considered unavailable.
	require.True(t, repl.LeaderlessWatcher.IsUnavailable())
	require.Error(t, repl.LeaderlessWatcher.Err())
	// Regex to ensure we've got a replica unavailable error with n1 and n2 in the
	// range descriptor.
	require.Regexp(t, "replica unavailable.*n1.*n2.*", repl.LeaderlessWatcher.Err().Error())

	// Next up, let the replica know there's a leader. This should make it
	// available again.
	repl.TestingRefreshLeaderlessWatcherUnavailableState(ctx, 1, manual.Now(), st)
	require.False(t, repl.LeaderlessWatcher.IsUnavailable())
	// Change the range descriptor. Mark it leaderless and let enough time pass
	// for it to be considered unavailable again.
	tc.AddVotersOrFatal(t, key, tc.Targets(2)...)
	repl.TestingRefreshLeaderlessWatcherUnavailableState(ctx, raft.None, manual.Now(), st)
	manual.Increment(10 * time.Second.Nanoseconds())
	repl.TestingRefreshLeaderlessWatcherUnavailableState(ctx, raft.None, manual.Now(), st)
	// The replica should now be considered unavailable again.
	require.True(t, repl.LeaderlessWatcher.IsUnavailable())
	require.Error(t, repl.LeaderlessWatcher.Err())
	// Ensure that the range descriptor now contains n1, n2, and n3 -- i.e, we're
	// updating the error with the latest descriptor on the latest transition.
	require.Regexp(t, "replica unavailable.*n1.*n2.*n3.*", repl.LeaderlessWatcher.Err().Error())
}

func TestClearRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			// This makes sure that our writes are visible when we go
			// straight to the engine to check them.
			DisableCanAckBeforeApplication: true,
		}},
	})
	defer s.Stopper().Stop(ctx)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	clearRange := func(start, end roachpb.Key) {
		t.Helper()
		if _, err := kv.SendWrapped(ctx, store.DB().NonTransactionalSender(), &kvpb.ClearRangeRequest{
			RequestHeader: kvpb.RequestHeader{
				Key:    start,
				EndKey: end,
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	verifyKeysWithPrefix := func(prefix roachpb.Key, expectedKeys []roachpb.Key) {
		t.Helper()
		start := prefix
		end := prefix.PrefixEnd()
		kvs, err := storage.Scan(context.Background(), store.StateEngine(), start, end, 0)
		if err != nil {
			t.Fatal(err)
		}
		var actualKeys []roachpb.Key
		for _, keyValue := range kvs {
			actualKeys = append(actualKeys, keyValue.Key.Key)
		}
		if !reflect.DeepEqual(expectedKeys, actualKeys) {
			t.Fatalf("expected %v, but got %v", expectedKeys, actualKeys)
		}
	}

	rng, _ := randutil.NewTestRand()

	// Write four keys with values small enough to use individual deletions
	// (sm1-sm4) and four keys with values large enough to require a range
	// deletion tombstone (lg1-lg4).
	sm, sm1, sm2, sm3 := roachpb.Key("sm"), roachpb.Key("sm1"), roachpb.Key("sm2"), roachpb.Key("sm3")
	lg, lg1, lg2, lg3 := roachpb.Key("lg"), roachpb.Key("lg1"), roachpb.Key("lg2"), roachpb.Key("lg3")
	for _, key := range []roachpb.Key{sm1, sm2, sm3} {
		if err := store.DB().Put(ctx, key, "sm-val"); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []roachpb.Key{lg1, lg2, lg3} {
		if err := store.DB().Put(
			ctx, key, randutil.RandBytes(rng, batcheval.ClearRangeBytesThreshold),
		); err != nil {
			t.Fatal(err)
		}
	}
	verifyKeysWithPrefix(sm, []roachpb.Key{sm1, sm2, sm3})
	verifyKeysWithPrefix(lg, []roachpb.Key{lg1, lg2, lg3})

	// Verify that a ClearRange request from [sm1, sm3) removes sm1 and sm2.
	clearRange(sm1, sm3)
	verifyKeysWithPrefix(sm, []roachpb.Key{sm3})

	// Verify that a ClearRange request from [lg1, lg3) removes lg1 and lg2.
	clearRange(lg1, lg3)
	verifyKeysWithPrefix(lg, []roachpb.Key{lg3})
}

// TestLeaseTransferInSnapshotUpdatesTimestampCache prevents a regression of
// #34025. A Replica is targeted for a lease transfer target when it needs a
// Raft snapshot to catch up. Normally we try to prevent this case, but it is
// possible and hard to prevent entirely. The Replica will only learn that it is
// the new leaseholder when it applies the snapshot. When doing so, it should
// make sure to apply the lease-related side-effects to its in-memory state.
//
// EDIT: as of June 2022, we have protection that should make this scenario
// significantly more rare. This test uses a knob to disable the new protection
// so that it can create the scenario where a replica learns that it holds the
// lease through a snapshot. We'll want to keep the test and the corresponding
// logic in applySnapshotRaftMuLocked around until we can eliminate the scenario entirely.
// See the commentary in github.com/cockroachdb/cockroach/issues/81561 about
// sending Raft logs in Raft snapshots for a discussion about why this may not
// be worth eliminating.
func TestLeaseTransferInSnapshotUpdatesTimestampCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "future-read", func(t *testing.T, futureRead bool) {
		manualClock := hlc.NewHybridManualClock()
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						WallClock: manualClock,
					},
				},
			},
		})
		defer tc.Stopper().Stop(context.Background())
		store0 := tc.GetFirstStoreFromServer(t, 0)
		store2 := tc.GetFirstStoreFromServer(t, 2)

		keyA := tc.ScratchRange(t)
		keyB := keyA.Next()
		keyC := keyB.Next()

		// First, do a couple of writes; we'll use these to determine when
		// the dust has settled.
		incA := incrementArgs(keyA, 1)
		if _, pErr := kv.SendWrapped(ctx, store0.TestSender(), incA); pErr != nil {
			t.Fatal(pErr)
		}
		incC := incrementArgs(keyC, 2)
		if _, pErr := kv.SendWrapped(ctx, store0.TestSender(), incC); pErr != nil {
			t.Fatal(pErr)
		}

		tc.AddVotersOrFatal(t, keyA, tc.Targets(1, 2)...)
		tc.WaitForValues(t, keyA, []int64{1, 1, 1})
		tc.WaitForValues(t, keyC, []int64{2, 2, 2})

		// Pause the cluster's clock. This ensures that if we perform a read at
		// a future timestamp, the read time remains in the future, regardless
		// of the passage of real time.
		manualClock.Pause()

		// Determine when to read.
		readTS := tc.Servers[0].Clock().Now()
		if futureRead {
			readTS = readTS.Add(500*time.Millisecond.Nanoseconds(), 0)
		}

		// Read the key at readTS.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = readTS
		ba.Add(getArgs(keyA))
		br, pErr := tc.Servers[0].DistSenderI().(kv.Sender).Send(ctx, ba)
		require.Nil(t, pErr)
		require.Equal(t, readTS, br.Timestamp)
		v, err := br.Responses[0].GetGet().Value.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(1), v)

		repl0 := store0.LookupReplica(roachpb.RKey(keyA))

		// Partition node 2 from the rest of its range. Once partitioned, perform
		// another write and truncate the Raft log on the two connected nodes. This
		// ensures that when node 2 comes back up it will require a snapshot from
		// Raft.
		funcs := kvtestutils.NoopRaftHandlerFuncs()
		funcs.DropReq = func(*kvserverpb.RaftMessageRequest) bool {
			return true
		}
		funcs.SnapErr = func(*kvserverpb.SnapshotRequest_Header) error {
			return errors.New("rejected")
		}
		tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store2.StoreID(), &kvtestutils.UnreliableRaftHandler{
			RangeID:                    repl0.GetRangeID(),
			IncomingRaftMessageHandler: store2,
			UnreliableRaftHandlerFuncs: funcs,
		})

		if _, pErr := kv.SendWrapped(ctx, store0.TestSender(), incC); pErr != nil {
			t.Fatal(pErr)
		}
		tc.WaitForValues(t, keyC, []int64{4, 4, 2})

		// Truncate the log at index+1 (log entries < N are removed, so this
		// includes the increment). This necessitates a snapshot when the
		// partitioned replica rejoins the rest of the range.
		index := repl0.GetLastIndex()
		truncArgs := truncateLogArgs(index+1, repl0.GetRangeID())
		truncArgs.Key = keyA
		if _, err := kv.SendWrapped(ctx, store0.TestSender(), truncArgs); err != nil {
			t.Fatal(err)
		}

		// Finally, transfer the lease to node 2 while it is still unavailable and
		// behind. We try to avoid this case when picking new leaseholders in practice,
		// but we're never 100% successful.
		// NOTE: we bypass safety checks because the target node is behind on its log,
		// so the lease transfer would be rejected otherwise.
		err = tc.Servers[0].DB().AdminTransferLeaseBypassingSafetyChecks(ctx,
			repl0.Desc().StartKey.AsRawKey(), tc.Target(2).StoreID)
		require.Nil(t, err)

		// Remove the partition. A snapshot to node 2 should follow. This snapshot
		// will inform node 2 that it is the new leaseholder for the range. Node 2
		// should act accordingly and update its internal state to reflect this.
		tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store2.Ident.StoreID, store2)
		tc.WaitForValues(t, keyC, []int64{4, 4, 4})

		// Attempt to write under the read on the new leaseholder. The batch
		// should get forwarded to a timestamp after the read. With the bug in
		// #34025, the new leaseholder who heard about the lease transfer from a
		// snapshot had an empty timestamp cache and would simply let us write
		// under the previous read.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		ba = &kvpb.BatchRequest{}
		ba.Timestamp = readTS
		ba.Add(incrementArgs(keyA, 1))
		br, pErr = tc.Servers[0].DistSenderI().(kv.Sender).Send(ctx, ba)
		require.Nil(t, pErr)
		require.NotEqual(t, readTS, br.Timestamp)
		require.True(t, readTS.Less(br.Timestamp))
	})
}

// TestLeaseTransferRejectedIfTargetNeedsSnapshot prevents a regression of
// #81561. It verifies that a replica will reject a lease transfer request if it
// can not guarantee that the lease target is sufficiently caught up on its Raft
// log such that it does not need a Raft snapshot. The test does so by inducing
// a partition, truncating the Raft log, and then trying to transfer the lease to
// the replica that is now cut off from the log. The lease transfer request must
// be rejected.
//
// The test has two variants. The !rejectAfterRevoke variant exercises the
// common but best-effort protection against unsafe lease transfers in
// Replica.AdminTransferLease. The rejectAfterRevoke variant exercises the
// uncommon but airtight protection against unsafe lease transfers in
// propBuf.maybeRejectUnsafeProposalLocked.
func TestLeaseTransferRejectedIfTargetNeedsSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "reject-after-revoke", func(t *testing.T, rejectAfterRevoke bool) {
		var transferLeaseReqBlockOnce sync.Once
		transferLeaseReqBlockedC := make(chan struct{})
		transferLeaseReqUnblockedC := make(chan struct{})

		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						// If we're testing the below-raft check, disable the above-raft check.
						// See: https://github.com/cockroachdb/cockroach/pull/107526
						DisableAboveRaftLeaseTransferSafetyChecks: rejectAfterRevoke,
						TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
							if rejectAfterRevoke && ba.IsSingleTransferLeaseRequest() {
								transferLeaseReqBlockOnce.Do(func() {
									close(transferLeaseReqBlockedC)
									<-transferLeaseReqUnblockedC
								})
							}
							return nil
						},
						// Speed up the lease transfer retry loop.
						LeaseTransferRejectedRetryLoopCount: 2,
					},
				},
			},
		})
		defer tc.Stopper().Stop(context.Background())
		store0 := tc.GetFirstStoreFromServer(t, 0)
		store2 := tc.GetFirstStoreFromServer(t, 2)

		keyA := tc.ScratchRange(t)
		keyB := keyA.Next()
		keyC := keyB.Next()

		// First, do a couple of writes; we'll use these to determine when
		// the dust has settled.
		incA := incrementArgs(keyA, 1)
		_, pErr := kv.SendWrapped(ctx, store0.TestSender(), incA)
		require.Nil(t, pErr)
		incC := incrementArgs(keyC, 2)
		_, pErr = kv.SendWrapped(ctx, store0.TestSender(), incC)
		require.Nil(t, pErr)

		tc.AddVotersOrFatal(t, keyA, tc.Targets(1, 2)...)
		tc.WaitForValues(t, keyA, []int64{1, 1, 1})
		tc.WaitForValues(t, keyC, []int64{2, 2, 2})

		repl0 := store0.LookupReplica(roachpb.RKey(keyA))

		// Grab the current lease. We'll use it later.
		preLease, _, err := tc.FindRangeLease(*repl0.Desc(), nil)
		require.NoError(t, err)
		require.Equal(t, store0.StoreID(), preLease.Replica.StoreID)

		// Partition node 2 from the rest of its range. Once partitioned, perform
		// another write and truncate the Raft log on the two connected nodes. This
		// ensures that when node 2 comes back up it will require a snapshot from
		// Raft.
		funcs := kvtestutils.NoopRaftHandlerFuncs()
		funcs.DropReq = func(*kvserverpb.RaftMessageRequest) bool {
			return true
		}
		funcs.SnapErr = func(*kvserverpb.SnapshotRequest_Header) error {
			return errors.New("rejected")
		}
		tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store2.StoreID(), &kvtestutils.UnreliableRaftHandler{
			RangeID:                    repl0.GetRangeID(),
			IncomingRaftMessageHandler: store2,
			UnreliableRaftHandlerFuncs: funcs,
		})

		_, pErr = kv.SendWrapped(ctx, store0.TestSender(), incC)
		require.Nil(t, pErr)
		tc.WaitForValues(t, keyC, []int64{4, 4, 2})

		// If we want the lease transfer rejection to come after the leaseholder has
		// revoked its lease, we launch the lease transfer before the log truncation
		// and block it after it has passed through the best-effort protection in
		// Replica.AdminTransferLease.
		transferErrC := make(chan error, 1)
		if rejectAfterRevoke {
			require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "transfer lease", func(ctx context.Context) {
				transferErrC <- tc.TransferRangeLease(*repl0.Desc(), tc.Target(2))
			}))
			select {
			case <-transferLeaseReqBlockedC:
			// Expected case: lease transfer triggered our interceptor and is now
			// waiting there for transferLeaseReqUnblockedCh.
			case err := <-transferErrC:
				// Unexpected case: lease transfer errored out before making it into the filter.
				t.Fatalf("transferErrC unexpectedly signaled: %v", err)
			}
		}

		// Truncate the log at index+1 (log entries < N are removed, so this
		// includes the increment). This necessitates a snapshot when the
		// partitioned replica rejoins the rest of the range.
		index := repl0.GetLastIndex()
		truncArgs := truncateLogArgs(index+1, repl0.GetRangeID())
		truncArgs.Key = keyA
		_, pErr = kv.SendWrapped(ctx, store0.TestSender(), truncArgs)
		require.Nil(t, pErr)

		// Complete or initiate the lease transfer attempt to node 2, which must not
		// succeed because node 2 now needs a snapshot.
		var transferErr error
		if rejectAfterRevoke {
			close(transferLeaseReqUnblockedC)
			transferErr = <-transferErrC
		} else {
			transferErr = tc.TransferRangeLease(*repl0.Desc(), tc.Target(2))
		}
		isRejectedErr := kvserver.IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(transferErr)
		require.True(t, isRejectedErr, "%+v", transferErr)

		// Remove the partition. A snapshot to node 2 should follow.
		tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store2.Ident.StoreID, store2)
		tc.WaitForValues(t, keyC, []int64{4, 4, 4})

		// Now that node 2 caught up on the log through a snapshot, we should be
		// able to transfer the lease to it successfully.
		// NOTE: we used a testing knob to disable automatic lease transfer retries,
		// so we use a SucceedsSoon loop.
		testutils.SucceedsSoon(t, func() error {
			if err := tc.TransferRangeLease(*repl0.Desc(), tc.Target(2)); err != nil {
				if kvserver.IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(err) {
					return err
				}
				t.Fatal(err)
			}
			return nil
		})

		// Verify that the lease is now held by node 2.
		postLease, _, err := tc.FindRangeLease(*repl0.Desc(), nil)
		require.NoError(t, err)
		require.Equal(t, store2.StoreID(), postLease.Replica.StoreID)

		// Additionally, verify that the lease has the expected sequence number. If
		// the lease transfer rejection came after the previous lease was revoked,
		// then node 0 must have re-acquired the lease (with a new sequence number)
		// in order to transfer it to node 2.
		// NB: we use LessOrEqual and not Equal to avoid flakiness if the lease is
		// lost and reacquired multiple times. This assertion is not the focus of
		// the test.
		expSeq := preLease.Sequence + 1
		if rejectAfterRevoke {
			expSeq++
		}
		require.LessOrEqual(t, expSeq, postLease.Sequence)
	})
}

// TestConcurrentAdminChangeReplicasRequests ensures that when two attempts to
// change replicas for a range race, only one will succeed.
func TestConcurrentAdminChangeReplicasRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// With 5 nodes the test is set up to have 2 actors trying to change the
	// replication concurrently. The first one attempts to change the replication
	// from [1] to [1, 2, 3, 4] and the second one starts by assuming that the
	// first actor succeeded on its first request and expected [1, 2] and tries
	// to move the replication to [1, 2, 4, 5]. One of these actors should
	// succeed.
	const numNodes = 5
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	key := roachpb.Key("a")
	db := tc.Servers[0].DB()
	rangeInfo, err := getRangeInfo(ctx, db, key)
	require.Nil(t, err)
	require.Len(t, rangeInfo.Desc.InternalReplicas, 1)
	targets1, targets2 := makeReplicationTargets(2, 3, 4), makeReplicationTargets(4, 5)
	expects1 := rangeInfo.Desc
	expects2 := rangeInfo.Desc
	expects2.InternalReplicas = append(expects2.InternalReplicas, roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: expects2.NextReplicaID,
	})
	expects2.NextReplicaID++
	var err1, err2 error
	var res1, res2 *roachpb.RangeDescriptor
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		res1, err1 = db.AdminChangeReplicas(
			ctx, key, expects1, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, targets1...))
		wg.Done()
	}()
	go func() {
		res2, err2 = db.AdminChangeReplicas(
			ctx, key, expects2, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, targets2...))
		wg.Done()
	}()
	wg.Wait()

	infoAfter, err := getRangeInfo(ctx, db, key)
	require.Nil(t, err)

	assert.Falsef(t, err1 == nil && err2 == nil,
		"expected one of racing AdminChangeReplicasRequests to fail but neither did")
	// It is possible that an error can occur due to a rejected snapshot from the
	// target range. We don't want to fail the test if we got one of those.
	atLeastOneIsSnapshotErr := kvserver.IsRetriableReplicationChangeError(err1) ||
		kvserver.IsRetriableReplicationChangeError(err2)
	assert.Falsef(t, err1 != nil && err2 != nil && !atLeastOneIsSnapshotErr,
		"expected only one of racing AdminChangeReplicasRequests to fail but both "+
			"had errors and neither were snapshot: %v %v", err1, err2)
	replicaNodeIDs := func(desc roachpb.RangeDescriptor) (ids []int) {
		for _, r := range desc.InternalReplicas {
			ids = append(ids, int(r.NodeID))
		}
		return ids
	}
	if err1 == nil {
		assert.ElementsMatch(t, replicaNodeIDs(infoAfter.Desc), []int{1, 2, 3, 4})
		assert.EqualValues(t, infoAfter.Desc, *res1)
	} else if err2 == nil {
		assert.ElementsMatch(t, replicaNodeIDs(infoAfter.Desc), []int{1, 2, 4, 5})
		assert.EqualValues(t, infoAfter.Desc, *res2)
	}
}

// TestRandomConcurrentAdminChangeReplicasRequests ensures that when multiple
// AdminChangeReplicasRequests are issued concurrently, so long as requests
// provide the value of the RangeDescriptor they will not accidentally
// perform replication changes. In particular this test runs a number of
// concurrent actors which all use the same expectations of the RangeDescriptor
// and verifies that at most one actor succeeds in making all of its changes.
func TestRandomConcurrentAdminChangeReplicasRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const numNodes = 6
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	const actors = 10
	errs := make([]error, actors)
	var wg sync.WaitGroup
	key := roachpb.Key("a")
	db := tc.Servers[0].DB()
	require.Nil(
		t,
		db.AdminRelocateRange(
			ctx, key, makeReplicationTargets(1, 2, 3),
			nil,  /* nonVoterTargets */
			true, /* transferLeaseToFirstVoter */
		),
	)
	// Random targets consisting of a random number of nodes from the set of nodes
	// in the cluster which currently do not have a replica.
	pickTargets := func() []roachpb.ReplicationTarget {
		availableIDs := make([]int, 0, numNodes-3)
		for id := 4; id <= numNodes; id++ {
			availableIDs = append(availableIDs, id)
		}
		rand.Shuffle(len(availableIDs), func(i, j int) {
			availableIDs[i], availableIDs[j] = availableIDs[j], availableIDs[i]
		})
		n := rand.Intn(len(availableIDs)) + 1
		return makeReplicationTargets(availableIDs[:n]...)
	}
	// TODO(ajwerner): consider doing this read inside the addReplicas function
	// and then allowing multiple writes to overlap and validate that the state
	// corresponds to a valid history of events.
	rangeInfo, err := getRangeInfo(ctx, db, key)
	require.Nil(t, err)
	addReplicas := func() error {
		op := roachpb.ADD_VOTER
		if rand.Intn(2) == 0 {
			op = roachpb.ADD_NON_VOTER
		}
		_, err := db.AdminChangeReplicas(ctx, key, rangeInfo.Desc, kvpb.MakeReplicationChanges(op, pickTargets()...))
		return err
	}
	wg.Add(actors)
	for i := 0; i < actors; i++ {
		go func(i int) { errs[i] = addReplicas(); wg.Done() }(i)
	}
	wg.Wait()
	var gotSuccess bool
	for _, err := range errs {
		if err != nil {
			require.Truef(t, kvserver.IsRetriableReplicationChangeError(err), "%s; desc: %v", err, rangeInfo.Desc)
		} else if gotSuccess {
			t.Error("expected only one success")
		} else {
			gotSuccess = true
		}
	}
}

func TestChangeReplicasSwapVoterWithNonVoter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	const numNodes = 7
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	// NB: The test cluster starts with firstVoter having a voting replica (and
	// the lease) for all ranges.
	firstVoter, nonVoter := tc.Target(0), tc.Target(1)
	firstStore, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)
	firstRepl := firstStore.LookupReplica(roachpb.RKey(key))
	require.NotNil(t, firstRepl, "the first node in the TestCluster must have a"+
		" replica for the ScratchRange")

	tc.AddNonVotersOrFatal(t, key, nonVoter)
	// Swap the only voting replica (leaseholder) with a non-voter
	tc.SwapVoterWithNonVoterOrFatal(t, key, firstVoter, nonVoter)
}

// TestReplicaTombstone ensures that tombstones are written when we expect
// them to be. Tombstones are laid down when replicas are removed.
// Replicas are removed for several reasons:
//
//	(1)   In response to a ChangeReplicasTrigger which removes it.
//	(2)   In response to a ReplicaTooOldError from a sent raft message.
//	(3)   Due to the replica GC queue detecting a replica is not in the range.
//	(3.1) When the replica detects the range has been merged away.
//	(4)   Due to a raft message addressed to a newer replica ID.
//	(4.1) When the older replica is not initialized.
//	(5)   Due to a merge.
//	(6)   Due to snapshot which subsumes a range.
//
// This test creates all of these scenarios and ensures that tombstones are
// written at sane values.
func TestReplicaTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testutils.RunValues(t, "lease-type", roachpb.TestingAllLeaseTypes(),
		func(t *testing.T, leaseType roachpb.LeaseType) {
			t.Run("(1) ChangeReplicasTrigger", func(t *testing.T) {
				defer leaktest.AfterTest(t)()
				defer log.Scope(t).Close(t)
				ctx := context.Background()
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
				tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Settings: st,
						Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
						}},
					},
					ReplicationMode: base.ReplicationManual,
				})
				defer tc.Stopper().Stop(ctx)

				key := tc.ScratchRange(t)
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				desc, err := tc.LookupRange(key)
				require.NoError(t, err)
				rangeID := desc.RangeID
				tc.AddVotersOrFatal(t, key, tc.Target(1))
				// Partition node 2 from receiving responses but not requests.
				// This will lead to it applying the ChangeReplicasTrigger which removes
				// it rather than receiving a ReplicaTooOldError.
				store, _ := getFirstStoreReplica(t, tc.Server(1), key)
				funcs := kvtestutils.NoopRaftHandlerFuncs()
				funcs.DropResp = func(*kvserverpb.RaftMessageResponse) bool {
					return true
				}
				tc.Servers[1].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &kvtestutils.UnreliableRaftHandler{
					RangeID:                    desc.RangeID,
					IncomingRaftMessageHandler: store,
					UnreliableRaftHandlerFuncs: funcs,
				})
				tc.RemoveVotersOrFatal(t, key, tc.Target(1))
				tombstone := waitForTombstone(t, store.StateEngine(), rangeID)
				require.Equal(t, roachpb.ReplicaID(3), tombstone.NextReplicaID)
			})
			t.Run("(2) ReplicaTooOldError", func(t *testing.T) {
				defer leaktest.AfterTest(t)()
				defer log.Scope(t).Close(t)
				ctx := context.Background()
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
				tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Settings: st,
						RaftConfig: base.RaftConfig{
							// Make the tick interval short so we don't need to wait too long for
							// the partitioned node to time out.
							RaftTickInterval: time.Millisecond,
						},
						Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
						}},
					},
					ReplicationMode: base.ReplicationManual,
				})
				defer tc.Stopper().Stop(ctx)

				key := tc.ScratchRange(t)
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				desc, err := tc.LookupRange(key)
				require.NoError(t, err)
				rangeID := desc.RangeID
				tc.AddVotersOrFatal(t, key, tc.Target(1), tc.Target(2))
				require.NoError(t,
					tc.WaitForVoters(key, tc.Target(1), tc.Target(2)))
				store, repl := getFirstStoreReplica(t, tc.Server(2), key)
				// Partition the range such that it hears responses but does not hear
				// requests. It should destroy the local replica due to a
				// ReplicaTooOldError.
				sawTooOld := make(chan struct{}, 1)
				raftFuncs := kvtestutils.NoopRaftHandlerFuncs()
				raftFuncs.DropResp = func(resp *kvserverpb.RaftMessageResponse) bool {
					if pErr, ok := resp.Union.GetValue().(*kvpb.Error); ok {
						if _, isTooOld := pErr.GetDetail().(*kvpb.ReplicaTooOldError); isTooOld {
							select {
							case sawTooOld <- struct{}{}:
							default:
							}
						}
					}
					return false
				}
				raftFuncs.DropReq = func(req *kvserverpb.RaftMessageRequest) bool {
					return req.ToReplica.StoreID == store.StoreID()
				}
				tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &kvtestutils.UnreliableRaftHandler{
					RangeID:                    desc.RangeID,
					IncomingRaftMessageHandler: store,
					UnreliableRaftHandlerFuncs: raftFuncs,
				})

				if leaseType == roachpb.LeaseLeader {
					// Partition the store liveness heartbeats as well.
					dropStoreLivenessHeartbeatsFrom(t, tc.Servers[2], desc, []roachpb.ReplicaID{1}, nil)
				}

				tc.RemoveVotersOrFatal(t, key, tc.Target(2))
				testutils.SucceedsSoon(t, func() error {
					repl.MaybeUnquiesce()
					if len(sawTooOld) == 0 {
						return errors.New("still haven't seen ReplicaTooOldError")
					}
					return nil
				})
				// Wait until we're sure that the replica has seen ReplicaTooOld,
				// then go look for the tombstone.
				<-sawTooOld
				tombstone := waitForTombstone(t, store.StateEngine(), rangeID)
				require.Equal(t, roachpb.ReplicaID(4), tombstone.NextReplicaID)
			})
			t.Run("(3) ReplicaGCQueue", func(t *testing.T) {
				defer leaktest.AfterTest(t)()
				defer log.Scope(t).Close(t)

				ctx := context.Background()
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
				tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Settings: st,
						Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
						}},
					},
					ReplicationMode: base.ReplicationManual,
				})
				defer tc.Stopper().Stop(ctx)

				key := tc.ScratchRange(t)
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				desc, err := tc.LookupRange(key)
				require.NoError(t, err)
				rangeID := desc.RangeID
				tc.AddVotersOrFatal(t, key, tc.Target(1), tc.Target(2))
				// Partition node 2 from receiving any raft messages.
				// It will never find out it has been removed. We'll remove it
				// with a manual replica GC.
				store, _ := getFirstStoreReplica(t, tc.Server(2), key)
				tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &kvtestutils.UnreliableRaftHandler{
					RangeID:                    desc.RangeID,
					IncomingRaftMessageHandler: store,
				})
				tc.RemoveVotersOrFatal(t, key, tc.Target(2))
				repl, err := store.GetReplica(desc.RangeID)
				require.NoError(t, err)
				require.NoError(t, store.ManualReplicaGC(repl))
				tombstone := waitForTombstone(t, store.StateEngine(), rangeID)
				require.Equal(t, roachpb.ReplicaID(4), tombstone.NextReplicaID)
			})
			// This case also detects the tombstone for nodes which processed the merge.
			t.Run("(3.1) (5) replica GC queue and merge", func(t *testing.T) {
				defer leaktest.AfterTest(t)()
				defer log.Scope(t).Close(t)

				ctx := context.Background()
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
				tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Settings: st,
						Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
						}},
					},
					ReplicationMode: base.ReplicationManual,
				})
				defer tc.Stopper().Stop(ctx)

				key := tc.ScratchRange(t)
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				tc.AddVotersOrFatal(t, key, tc.Target(1))
				keyA := append(key[:len(key):len(key)], 'a')
				_, desc, err := tc.SplitRange(keyA)
				require.NoError(t, err)
				require.NoError(t, tc.WaitForSplitAndInitialization(keyA))
				tc.AddVotersOrFatal(t, key, tc.Target(3))
				tc.AddVotersOrFatal(t, keyA, tc.Target(2))
				rangeID := desc.RangeID
				// Partition node 2 from all raft communication.
				store, _ := getFirstStoreReplica(t, tc.Server(2), keyA)
				tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &kvtestutils.UnreliableRaftHandler{
					RangeID:                    desc.RangeID,
					IncomingRaftMessageHandler: store,
				})

				// We'll move the range from server 2 to 3 and merge key and keyA.
				// Server 2 won't hear about any of that.
				tc.RemoveVotersOrFatal(t, keyA, tc.Target(2))
				tc.AddVotersOrFatal(t, keyA, tc.Target(3))
				require.NoError(t, tc.WaitForSplitAndInitialization(keyA))
				require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, key))
				// Run replica GC on server 2.
				repl, err := store.GetReplica(desc.RangeID)
				require.NoError(t, err)
				require.NoError(t, store.ManualReplicaGC(repl))
				// Verify the tombstone generated from replica GC of a merged range.
				tombstone := waitForTombstone(t, store.StateEngine(), rangeID)
				require.Equal(t, roachpb.ReplicaID(math.MaxInt32), tombstone.NextReplicaID)
				// Verify the tombstone generated from processing a merge trigger.
				store3, _ := getFirstStoreReplica(t, tc.Server(0), key)
				tombstone = waitForTombstone(t, store3.StateEngine(), rangeID)
				require.Equal(t, roachpb.ReplicaID(math.MaxInt32), tombstone.NextReplicaID)
			})
			t.Run("(4) (4.1) raft messages to newer replicaID ", func(t *testing.T) {
				defer leaktest.AfterTest(t)()
				defer log.Scope(t).Close(t)
				ctx := context.Background()
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
				tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Settings: st,
						RaftConfig: base.RaftConfig{
							// Make the tick interval short so we don't need to wait too long
							// for a heartbeat to be sent.
							RaftTickInterval: time.Millisecond,
						},
						Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
						}},
					},
					ReplicationMode: base.ReplicationManual,
				})
				defer tc.Stopper().Stop(ctx)

				key := tc.ScratchRange(t)
				desc, err := tc.LookupRange(key)
				require.NoError(t, err)
				rangeID := desc.RangeID
				tc.AddVotersOrFatal(t, key, tc.Target(1), tc.Target(2))
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				store, repl := getFirstStoreReplica(t, tc.Server(2), key)
				// Set up a partition for everything but heartbeats on store 2.
				// Make ourselves a tool to block snapshots until we've heard a
				// heartbeat above a certain replica ID.
				var waiter struct {
					syncutil.Mutex
					sync.Cond
					minHeartbeatReplicaID roachpb.ReplicaID
					blockSnapshot         bool
				}
				waiter.L = &waiter.Mutex
				waitForSnapshot := func() {
					waiter.Lock()
					defer waiter.Unlock()
					for waiter.blockSnapshot {
						waiter.Wait()
					}
				}
				recordHeartbeatOrFortification := func(replicaID roachpb.ReplicaID) {
					waiter.Lock()
					defer waiter.Unlock()
					if waiter.blockSnapshot && replicaID >= waiter.minHeartbeatReplicaID {
						waiter.blockSnapshot = false
						waiter.Broadcast()
					}
				}
				setMinHeartbeat := func(replicaID roachpb.ReplicaID) {
					waiter.Lock()
					defer waiter.Unlock()
					waiter.minHeartbeatReplicaID = replicaID
					waiter.blockSnapshot = true
				}
				setMinHeartbeat(repl.ReplicaID() + 1)
				tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &kvtestutils.UnreliableRaftHandler{
					RangeID:                    desc.RangeID,
					IncomingRaftMessageHandler: store,
					UnreliableRaftHandlerFuncs: kvtestutils.UnreliableRaftHandlerFuncs{
						DropResp: func(*kvserverpb.RaftMessageResponse) bool {
							return true
						},
						DropReq: func(req *kvserverpb.RaftMessageRequest) bool {
							if leaseType == roachpb.LeaseLeader &&
								req.Message.Type == raftpb.MsgFortifyLeader {
								// In leader leases, the leader doesn't send heartbeats.
								// However, it will send a MsgFortifyLeader once it becomes a
								// leader.
								recordHeartbeatOrFortification(req.ToReplica.ReplicaID)
								return false
							}
							return true
						},
						DropHB: func(hb *kvserverpb.RaftHeartbeat) bool {
							recordHeartbeatOrFortification(hb.ToReplicaID)
							return false
						},
						SnapErr: func(*kvserverpb.SnapshotRequest_Header) error {
							waitForSnapshot()
							return errors.New("boom")
						},
					},
				})

				// Remove the current replica from the node, it will not hear about this.
				tc.RemoveVotersOrFatal(t, key, tc.Target(2))
				// Try to add it back as a learner. We'll wait until it's heard about
				// this as a heartbeat. This demonstrates case (4) where a raft message
				// to a newer replica ID (in this case a heartbeat) removes an initialized
				// Replica.
				//
				// Don't use tc.AddVoter; this would retry internally as we're faking
				// a snapshot error here (and these are all considered retriable).
				_, err = tc.Servers[0].DB().AdminChangeReplicas(
					ctx, key, tc.LookupRangeOrFatal(t, key), kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(2)),
				)
				require.Regexp(t, "boom", err)
				tombstone := waitForTombstone(t, store.StateEngine(), rangeID)
				require.Equal(t, roachpb.ReplicaID(4), tombstone.NextReplicaID)
				// Try adding it again and again block the snapshot until a heartbeat
				// at a higher ID has been sent. This is case (4.1) where a raft message
				// removes an uninitialized Replica.
				//
				// Note that this case represents a potential memory leak. If we hear about
				// a Replica and then either never receive a snapshot or for whatever reason
				// fail to receive a snapshot and then we never hear from the range again we
				// may leak in-memory state about this replica.
				//
				// We could replica GC these replicas without too much extra work but they
				// also should be rare. Note this is not new with learner replicas.
				setMinHeartbeat(5)
				_, err = tc.Servers[0].DB().AdminChangeReplicas(
					ctx, key, tc.LookupRangeOrFatal(t, key), kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(2)),
				)
				require.Regexp(t, "boom", err)
				// We will start out reading the old tombstone so keep retrying.
				testutils.SucceedsSoon(t, func() error {
					tombstone = waitForTombstone(t, store.StateEngine(), rangeID)
					if tombstone.NextReplicaID != 5 {
						return errors.Errorf("read tombstone with NextReplicaID %d, want %d",
							tombstone.NextReplicaID, 5)
					}
					return nil
				})
			})
			t.Run("(6) subsumption via snapshot", func(t *testing.T) {
				defer leaktest.AfterTest(t)()
				defer log.Scope(t).Close(t)

				ctx := context.Background()
				st := cluster.MakeTestingClusterSettings()
				kvserver.OverrideDefaultLeaseType(ctx, &st.SV, leaseType)
				var proposalFilter atomic.Value
				noopProposalFilter := func(kvserverbase.ProposalFilterArgs) *kvpb.Error {
					return nil
				}
				proposalFilter.Store(noopProposalFilter)
				tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						Settings: st,
						Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
							DisableReplicaGCQueue: true,
							TestingProposalFilter: func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
								return proposalFilter.
									Load().(func(kvserverbase.ProposalFilterArgs) *kvpb.Error)(args)
							},
						}},
					},
					ReplicationMode: base.ReplicationManual,
				})
				defer tc.Stopper().Stop(ctx)

				key := tc.ScratchRange(t)
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				tc.AddVotersOrFatal(t, key, tc.Target(1), tc.Target(2))
				keyA := append(key[:len(key):len(key)], 'a')
				lhsDesc, rhsDesc, err := tc.SplitRange(keyA)
				require.NoError(t, err)
				require.NoError(t, tc.WaitForSplitAndInitialization(key))
				require.NoError(t, tc.WaitForSplitAndInitialization(keyA))
				require.NoError(t, tc.WaitForVoters(key, tc.Target(1), tc.Target(2)))
				require.NoError(t, tc.WaitForVoters(keyA, tc.Target(1), tc.Target(2)))

				// We're going to block the RHS and LHS of node 2 as soon as the merge
				// attempts to propose the command to commit the merge. This should prevent
				// the merge from being applied on node 2. Then we'll manually force a
				// snapshots to be sent to the LHS of store 2 after the merge commits.
				store, repl := getFirstStoreReplica(t, tc.Server(2), key)
				var partActive atomic.Value
				partActive.Store(false)
				raftFuncs := kvtestutils.NoopRaftHandlerFuncs()
				raftFuncs.DropReq = func(req *kvserverpb.RaftMessageRequest) bool {
					return partActive.Load().(bool) && req.Message.Type == raftpb.MsgApp
				}
				tc.Servers[2].RaftTransport().(*kvserver.RaftTransport).ListenIncomingRaftMessages(store.StoreID(), &kvtestutils.UnreliableRaftHandler{
					RangeID:                    lhsDesc.RangeID,
					UnreliableRaftHandlerFuncs: raftFuncs,
					IncomingRaftMessageHandler: &kvtestutils.UnreliableRaftHandler{
						RangeID:                    rhsDesc.RangeID,
						IncomingRaftMessageHandler: store,
						UnreliableRaftHandlerFuncs: raftFuncs,
					},
				})
				proposalFilter.Store(func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
					merge := args.Cmd.ReplicatedEvalResult.Merge
					if merge != nil && merge.LeftDesc.RangeID == lhsDesc.RangeID {
						partActive.Store(true)
					}
					return nil
				})
				require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, key))
				var tombstone kvserverpb.RangeTombstone
				testutils.SucceedsSoon(t, func() (err error) {
					// One of the two other stores better be the raft leader eventually.
					// We keep trying to send snapshots until one takes.
					for i := range []int{0, 1} {
						s, r := getFirstStoreReplica(t, tc.Server(i), key)
						err = s.ManualRaftSnapshot(r, repl.ReplicaID())
						if err == nil {
							break
						}
					}
					if err != nil {
						return err
					}
					ts, err := kvstorage.MakeStateLoader(rhsDesc.RangeID).LoadRangeTombstone(
						context.Background(), store.StateEngine(),
					)
					require.NoError(t, err)
					if ts.NextReplicaID == 0 {
						return errors.New("no tombstone found")
					}
					tombstone = ts
					return nil
				})
				require.Equal(t, roachpb.ReplicaID(math.MaxInt32), tombstone.NextReplicaID)
			})
		})
}

// TestAdminRelocateRangeSafety exercises a situation where calls to
// AdminRelocateRange can race with calls to ChangeReplicas and verifies
// that such races do not leave the range in an under-replicated state.
func TestAdminRelocateRangeSafety(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The test is going to verify that when a replica removal due to a
	// Replica.ChangeReplicas call coincides with the removal phase of an
	// AdminRelocateRangeRequest that one of the removals will fail.
	// In order to ensure that the AdminChangeReplicas command coincides with
	// the remove phase of the AdminRelocateReplicas the test injects a response
	// filter which, when useSeenAdd holds true, signals on seenAdd when it sees
	// an AdminChangeReplicasRequest which added a replica.
	const numNodes = 4
	var useSeenAdd atomic.Value
	useSeenAdd.Store(false)
	seenAdd := make(chan struct{}, 1)
	responseFilter := func(ctx context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) *kvpb.Error {
		if ba.IsSingleRequest() {
			changeReplicas, ok := ba.Requests[0].GetInner().(*kvpb.AdminChangeReplicasRequest)
			if ok && changeReplicas.Changes()[0].ChangeType == roachpb.ADD_VOTER && useSeenAdd.Load().(bool) {
				seenAdd <- struct{}{}
			}
		}
		return nil
	}
	tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingResponseFilter: responseFilter,
				},
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	db := tc.Servers[rand.Intn(numNodes)].DB()

	// The test assumes from the way that the range gets set up that the lease
	// holder is node 1 and from the above relocate call that the range in
	// question has replicas on nodes 1-3. Make the call to AdminRelocate range
	// to set up the replication and then verify the assumed state.

	key := roachpb.Key("a")
	assert.Nil(t, db.AdminRelocateRange(
		ctx,
		key,
		makeReplicationTargets(1, 2, 3),
		makeReplicationTargets(),
		true, /* transferLeaseToFirstVoter */
	),
	)
	rangeInfo, err := getRangeInfo(ctx, db, key)
	assert.Nil(t, err)
	assert.Len(t, rangeInfo.Desc.InternalReplicas, 3)
	assert.Equal(t, rangeInfo.Lease.Replica.NodeID, roachpb.NodeID(1))
	for id := roachpb.StoreID(1); id <= 3; id++ {
		_, hasReplica := rangeInfo.Desc.GetReplicaDescriptor(id)
		assert.Truef(t, hasReplica, "missing replica descriptor for store %d", id)
	}

	// The test now proceeds to use AdminRelocateRange to move a replica from node
	// 3 to node 4. The call should first which will first add 4 and then
	// remove 3. Concurrently a separate goroutine will attempt to remove the
	// replica on node 2. The ResponseFilter passed in the TestingKnobs will
	// prevent the remove call from proceeding until after the Add of 4 has
	// completed.

	// Code above verified r1 is the leaseholder, so use it to ChangeReplicas.
	r1, _, err := tc.Servers[0].GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, rangeInfo.Desc.RangeID)
	assert.Nil(t, err)
	expDescAfterAdd := rangeInfo.Desc // for use with ChangeReplicas
	expDescAfterAdd.NextReplicaID++
	expDescAfterAdd.InternalReplicas = append(expDescAfterAdd.InternalReplicas, roachpb.ReplicaDescriptor{
		NodeID:    4,
		StoreID:   4,
		ReplicaID: 4,
	})
	var relocateErr, changeErr error
	var changedDesc *roachpb.RangeDescriptor // only populated if changeErr == nil
	change := func() {
		<-seenAdd
		chgs := kvpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, makeReplicationTargets(2)...)
		changedDesc, changeErr = r1.ChangeReplicas(ctx, &expDescAfterAdd, "replicate", "testing", chgs)
	}
	relocate := func() {
		relocateErr = db.AdminRelocateRange(
			ctx,
			key,
			makeReplicationTargets(1, 2, 4),
			makeReplicationTargets(),
			true, /* transferLeaseToFirstVoter */
		)
	}
	useSeenAdd.Store(true)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { relocate(); wg.Done() }()
	go func() { change(); wg.Done() }()
	wg.Wait()
	rangeInfo, err = getRangeInfo(ctx, db, key)
	assert.Nil(t, err)
	assert.True(t, len(rangeInfo.Desc.InternalReplicas) >= 3)
	assert.Falsef(t, relocateErr == nil && changeErr == nil,
		"expected one of racing AdminRelocateReplicas and ChangeReplicas "+
			"to fail but neither did")
	assert.Falsef(t, relocateErr != nil && changeErr != nil,
		"expected only one of racing AdminRelocateReplicas and ChangeReplicas "+
			"to fail but both did")
	if changeErr == nil {
		assert.EqualValues(t, *changedDesc, rangeInfo.Desc)
	}
}

// TestChangeReplicasLeaveAtomicRacesWithMerge exercises a hazardous case which
// arises during concurrent AdminChangeReplicas requests. The code reads the
// descriptor from range id local, checks to make sure that the read
// descriptor matches the expectation and then uses the bytes of the read
// bytes in a CPut with the update. The code contains an optimization to
// transition out of joint consensus even if the read descriptor does not match
// the expectation. That optimization did not verify anything about the read
// descriptor, not even if it was nil.
//
// This test wants to exercise this scenario. We need to get the replica in
// a state where it has an outgoing voter and then we need to have two
// different requests trying to make changes and only the merge succeeds. The
// race is that the second command will notice the voter outgoing and will
// attempt to fix it.  In order to do that it reads the range descriptor to
// ensure that it has not changed (and to get the raw bytes of the range
// descriptor for use in a CPut as the current API only uses the in-memory
// value and we need the encoding is not necessarily stable.
//
// The test also contains a variant whereby the range is re-split at the
// same key producing a range descriptor with a different range ID.
//
// See https://github.com/cockroachdb/cockroach/issues/40877.
func TestChangeReplicasLeaveAtomicRacesWithMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "resplit", func(t *testing.T, resplit bool) {
		const numNodes = 4
		var stopAfterJointConfig atomic.Value
		stopAfterJointConfig.Store(false)
		var rangeToBlockRangeDescriptorRead atomic.Value
		rangeToBlockRangeDescriptorRead.Store(roachpb.RangeID(0))
		blockRangeDescriptorReadChan := make(chan struct{}, 1)
		blockOnChangeReplicasRead := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			if req, isGet := ba.GetArg(kvpb.Get); !isGet ||
				ba.RangeID != rangeToBlockRangeDescriptorRead.Load().(roachpb.RangeID) ||
				!ba.IsSingleRequest() ||
				!bytes.HasSuffix(req.(*kvpb.GetRequest).Key,
					keys.LocalRangeDescriptorSuffix) {
				return nil
			}
			select {
			case <-blockRangeDescriptorReadChan:
				<-blockRangeDescriptorReadChan
			case <-ctx.Done():
			default:
			}
			return nil
		}
		tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						TestingRequestFilter: blockOnChangeReplicasRead,
						VoterAddStopAfterJointConfig: func() bool {
							return stopAfterJointConfig.Load().(bool)
						},
					},
				},
			},
			ReplicationMode: base.ReplicationManual,
		})
		ctx := context.Background()
		defer tc.Stopper().Stop(ctx)

		// We want to first get into a joint consensus scenario.
		// Then we want to issue a ChangeReplicasRequest on a goroutine that will
		// block trying to read the RHS's range descriptor. Then we'll merge the RHS
		// away.

		// Set up a userspace range to mess around with.
		lhs := tc.ScratchRange(t)
		_, err := tc.AddVoters(lhs, tc.Targets(1, 2)...)
		require.NoError(t, err)

		// Split it and then we're going to try to up-replicate.
		// We're going to have one goroutine trying to ADD the 4th node.
		// and another goroutine trying to move out of a joint config on both
		// sides and then merge the range. We ensure that the first goroutine
		// blocks and the second one succeeds. This will test that the first
		// goroutine detects reading the nil descriptor.
		rhs := append(lhs[:len(lhs):len(lhs)], 'a')
		lhsDesc, rhsDesc := &roachpb.RangeDescriptor{}, &roachpb.RangeDescriptor{}
		*lhsDesc, *rhsDesc, err = tc.SplitRange(rhs)
		require.NoError(t, err)

		err = tc.WaitForSplitAndInitialization(rhs)
		require.NoError(t, err)

		// Manually construct the batch because the (*DB).AdminChangeReplicas does
		// not yet support atomic replication changes.
		db := tc.Servers[0].DB()
		swapReplicas := func(key roachpb.Key, desc roachpb.RangeDescriptor, add, remove int) (*roachpb.RangeDescriptor, error) {
			return db.AdminChangeReplicas(ctx, key, desc, []kvpb.ReplicationChange{
				{ChangeType: roachpb.ADD_VOTER, Target: tc.Target(add)},
				{ChangeType: roachpb.REMOVE_VOTER, Target: tc.Target(remove)},
			})
		}

		// Move the RHS and LHS to 3 from 2.
		_, err = swapReplicas(lhs, *lhsDesc, 3, 2)
		require.NoError(t, err)
		stopAfterJointConfig.Store(true) // keep the RHS in a joint config.
		rhsDesc, err = swapReplicas(rhs, *rhsDesc, 3, 2)
		require.NoError(t, err)
		stopAfterJointConfig.Store(false)

		// Run a goroutine which sends an AdminChangeReplicasRequest which will try to
		// move the range out of joint config but will end up blocking on
		// blockRangeDescriptorReadChan until we close it later.
		rangeToBlockRangeDescriptorRead.Store(rhsDesc.RangeID)
		blockRangeDescriptorReadChan <- struct{}{}
		var wg sync.WaitGroup

		defer func() {
			// Unblock the original add on the separate goroutine to ensure that it
			// properly handles reading a nil range descriptor.
			close(blockRangeDescriptorReadChan)
			wg.Wait()
		}()
		wg.Add(1)

		go func() {
			defer wg.Done()
			_, err := db.AdminChangeReplicas(
				ctx, rhs, *rhsDesc, kvpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(2)),
			)
			// We'll ultimately fail because we're going to race with the work below.
			msg := `descriptor changed:`
			if resplit {
				// We return a more detailed "descriptor changed" error if the
				// range ID changed.
				msg = `descriptor changed: .* \(range replaced\)`
			}
			require.Regexp(t, msg, err)
		}()
		// Wait until our goroutine is blocked.
		testutils.SucceedsSoon(t, func() error {
			if len(blockRangeDescriptorReadChan) != 0 {
				return errors.New("not blocked yet")
			}
			return nil
		})
		// Remove the learner replica (left because the joint config was demoting
		// a voter) which as a side effect exists the joint config.
		_, err = tc.RemoveVoters(rhs, tc.Target(2))
		require.NoError(t, err)
		// Merge the RHS away.
		err = db.AdminMerge(ctx, lhs)
		require.NoError(t, err)
		if resplit {
			require.NoError(t, db.AdminSplit(ctx, rhs, hlc.Timestamp{WallTime: math.MaxInt64}))
			err = tc.WaitForSplitAndInitialization(rhs)
			require.NoError(t, err)
		}
	})
}

// This test is designed to demonstrate that it is not possible to have pending
// proposals concurrent with a TransferLeaseRequest. This property ensures that
// we cannot possibly receive AmbiguousResultError due to an outgoing leaseholder
// being removed while still having pending proposals for a lease which did not
// expire (i.e. was transferred cooperatively using TransferLease rather than
// being taken with a RequestLease).
//
// At the time of writing this test were three hazardous cases which are now
// avoided:
//
//	(1) The outgoing leaseholder learns about its removal before applying the
//	    lease transfer. This could happen if it has a lot left to apply but it
//	    does indeed know in its log that it is either no longer the leaseholder
//	    or that some of its commands will apply successfully.
//
//	(2) The replica learns about its removal after applying the lease transfer
//	    but it potentially still has pending commands which it thinks might
//	    have been proposed. This can occur if there are commands which are
//	    proposed after the lease transfer has been proposed but before the lease
//	    transfer has applied. This can also occur if commands are re-ordered
//	    by raft due to a leadership change.
//
//	(3) The replica learns about its removal after applying the lease transfer
//	    but proposed a command evaluated under the old lease after the lease
//	    transfer has been applied. This can occur if there are commands evaluate
//	    before the lease transfer is proposed but are not inserted into the
//	    proposal buffer until after it has been applied.
//
// None of these cases are possible any longer as latches now prevent writes
// from occurring concurrently with TransferLeaseRequests. (1) is prevented
// because all proposals will need to apply before the TransferLeaseRequest
// can be evaluated. (2) and (3) are not possible because either the commands
// in question acquire their latches before the TransferLeaseRequest in which
// case they'll apply before the TransferLease can be proposed or they acquire
// their latches after the TransferLease applies in which case they will fail
// due to NotLeaseHolderError prior to application.
func TestTransferLeaseBlocksWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We want to verify that we will not propose a TransferLeaseRequest while
	// there is an outstanding proposal.
	var scratchRangeID atomic.Value
	scratchRangeID.Store(roachpb.RangeID(0))
	blockInc := make(chan chan struct{})
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
				TestingProposalFilter: func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
					if args.Req.RangeID != scratchRangeID.Load().(roachpb.RangeID) {
						return nil
					}
					// Block increment requests on blockInc.
					if _, isInc := args.Req.GetArg(kvpb.Increment); isInc {
						unblock := make(chan struct{})
						blockInc <- unblock
						<-unblock
					}
					return nil
				},
			}},
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(context.Background())

	scratch := tc.ScratchRange(t)
	makeKey := func() roachpb.Key {
		return append(scratch[:len(scratch):len(scratch)], uuid.MakeV4().String()...)
	}
	desc := tc.AddVotersOrFatal(t, scratch, tc.Target(1), tc.Target(2))
	scratchRangeID.Store(desc.RangeID)
	require.NoError(t, tc.WaitForVoters(scratch, tc.Target(1), tc.Target(2)))

	// Launch a goroutine to increment a value, it will block in the proposal
	// filter.
	incErr := make(chan error)
	go func() {
		_, err := tc.Server(1).DB().Inc(context.Background(), makeKey(), 1)
		incErr <- err
	}()

	// Wait for the increment to be blocked on the proposal filter so we know
	// it holds a write latch.
	unblock := <-blockInc

	// Launch a goroutine to transfer the lease to store 1.
	transferErr := make(chan error)
	go func() {
		transferErr <- tc.TransferRangeLease(desc, tc.Target(1))
	}()

	// Ensure that the lease transfer doesn't succeed.
	// We don't wait that long because we don't want this test to take too long.
	// The theory is that if we weren't acquiring latches over the keyspace then
	// the lease transfer could succeed before we unblocked the increment request.
	select {
	case <-time.After(100 * time.Millisecond):
	case err := <-transferErr:
		t.Fatalf("did not expect transfer to complete, got %v", err)
	}

	close(unblock)
	require.NoError(t, <-incErr)
	require.NoError(t, <-transferErr)
}

// TestStrictGCEnforcement ensures that strict GC enforcement is respected and
// furthermore is responsive to changes in protected timestamps and in changes
// to the zone configs.
func TestStrictGCEnforcement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The unfortunate thing about this test is that the gcttl is in seconds and
	// we need to wait for the replica's lease start time to be sufficiently old.
	// It takes about two seconds. All of that time is in setup.
	if testing.Short() {
		return
	}
	ctx := context.Background()

	var mu struct {
		syncutil.Mutex
		blockOnTimestampUpdate func()
	}

	args := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// We don't strictly enforce the GC TTL if the protected timestamp
					// state cached on the replica is older than the lease's start time.
					// We disable the lease queue to prevent flakes right around lease
					// transfers. See getImpliedGCThresholdRLocked for more details.
					DisableLeaseQueue: true,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					KVSubscriberRangeFeedKnobs: &rangefeedcache.TestingKnobs{
						OnTimestampAdvance: func(timestamp hlc.Timestamp) {
							mu.Lock()
							defer mu.Unlock()
							if mu.blockOnTimestampUpdate != nil {
								mu.blockOnTimestampUpdate()
							}
						},
					},
				},
			},
		},
	}

	tc := testcluster.StartTestCluster(t, 3, args)
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY)`)

	var (
		db         = tc.Server(0).DB()
		getTableID = func() (tableID uint32) {
			sqlDB.QueryRow(t, `SELECT table_id FROM crdb_internal.tables`+
				` WHERE name = 'foo' AND database_name = current_database()`).Scan(&tableID)
			return tableID
		}
		tableID       = getTableID()
		tenSecondsAgo hlc.Timestamp // written in setup
		tableKey      = keys.SystemSQLCodec.TablePrefix(tableID)
		tableSpan     = roachpb.Span{Key: tableKey, EndKey: tableKey.PrefixEnd()}
		tableTarget   = ptpb.MakeSchemaObjectsTarget([]descpb.ID{descpb.ID(tableID)})
		mkRecord      = func() ptpb.Record {
			return ptpb.Record{
				ID:        uuid.MakeV4().GetBytes(),
				Timestamp: tenSecondsAgo.Add(-10*time.Second.Nanoseconds(), 0),
				Target:    tableTarget,
			}
		}
		mkStaleTxn = func() *kv.Txn {
			txn := db.NewTxn(ctx, "foo")
			require.NoError(t, txn.SetFixedTimestamp(ctx, tenSecondsAgo))
			return txn
		}
		getRejectedMsg = func() string {
			return tenSecondsAgo.String() + " must be after replica GC threshold "
		}
		performScan = func() error {
			txn := mkStaleTxn()
			_, err := txn.Scan(ctx, tableKey, tableKey.PrefixEnd(), 1)
			return err
		}
		assertScanRejected = func(t *testing.T) {
			t.Helper()
			require.Regexp(t, getRejectedMsg(), performScan())
		}

		assertScanOk = func(t *testing.T) {
			t.Helper()
			require.NoError(t, performScan())
		}
		setGCTTL = func(t *testing.T, object string, exp int) {
			t.Helper()
			testutils.SucceedsSoon(t, func() error {
				sqlDB.Exec(t, `ALTER `+object+` CONFIGURE ZONE USING gc.ttlseconds = `+strconv.Itoa(exp))
				for i := 0; i < tc.NumServers(); i++ {
					s := tc.Server(i)
					_, r := getFirstStoreReplica(t, s, tableKey)
					c, err := r.LoadSpanConfig(ctx)
					if err != nil {
						return err
					}
					if c.TTL().Seconds() != (time.Duration(exp) * time.Second).Seconds() {
						return errors.Errorf("expected %d, got %f", exp, c.TTL().Seconds())
					}
				}
				return nil
			})
		}
		setStrictGC = func(t *testing.T, val bool) {
			t.Helper()
			sqlDB.Exec(t, `SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = `+fmt.Sprint(val))
			testutils.SucceedsSoon(t, func() error {
				for i := 0; i < tc.NumServers(); i++ {
					s, _ := getFirstStoreReplica(t, tc.Server(i), keys.TableDataMin)
					if kvserver.StrictGCEnforcement.Get(&s.ClusterSettings().SV) != val {
						return errors.Errorf("expected %v, got %v", val, !val)
					}
				}
				return nil
			})
		}
		setTableGCTTL = func(t *testing.T, exp int) {
			t.Helper()
			setGCTTL(t, "TABLE foo", exp)
		}
		setSystemGCTTL = func(t *testing.T, exp int) {
			// TODO(ajwerner): adopt this to test the system ranges are unaffected.
			t.Helper()
			setGCTTL(t, "RANGE system", exp)
		}
		refreshPastLeaseStart = func(t *testing.T) {
			for i := 0; i < tc.NumServers(); i++ {
				ptsReader := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().ProtectedTimestampReader
				_, r := getFirstStoreReplica(t, tc.Server(i), tableKey)
				l, _ := r.GetLease()
				require.NoError(
					t,
					spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, l.Start.ToTimestamp().Next()),
				)
				require.NoError(t, r.TestingReadProtectedTimestamps(ctx))
			}
		}
		refreshTo = func(t *testing.T, asOf hlc.Timestamp) {
			for i := 0; i < tc.NumServers(); i++ {
				ptsReader := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().ProtectedTimestampReader
				_, r := getFirstStoreReplica(t, tc.Server(i), tableKey)
				require.NoError(
					t,
					spanconfigptsreader.TestingRefreshPTSState(ctx, ptsReader, asOf),
				)
				require.NoError(t, r.TestingReadProtectedTimestamps(ctx))
			}
		}
		// waitForProtectionAndReadProtectedTimestamps waits until the
		// `protectionTimestamp` has been reconciled to KV, and then updates the
		// replica's view of the protections that apply on it.
		waitForProtectionAndReadProtectedTimestamps = func(t *testing.T, nodeID roachpb.NodeID,
			protectionTimestamp hlc.Timestamp, span roachpb.Span) {
			for i := 0; i < tc.NumServers(); i++ {
				if tc.Server(i).NodeID() != nodeID {
					continue
				}
				ptsReader := tc.GetFirstStoreFromServer(t, i).GetStoreConfig().ProtectedTimestampReader
				_, r := getFirstStoreReplica(t, tc.Server(i), tableKey)
				ptutil.TestingWaitForProtectedTimestampToExistOnSpans(ctx, t, tc.Server(i),
					ptsReader, protectionTimestamp,
					[]roachpb.Span{span})
				require.NoError(t, r.TestingReadProtectedTimestamps(ctx))
			}
		}
		insqlDB = tc.Server(0).InternalDB().(isql.DB)
	)

	{
		// Setup the initial state to be sure that we'll actually strictly enforce
		// gc ttls.
		tc.SplitRangeOrFatal(t, tableKey)
		_, err := tc.AddVoters(tableKey, tc.Target(1), tc.Target(2))
		require.NoError(t, err)
		_, err = tc.AddVoters(keys.TableDataMin, tc.Target(1), tc.Target(2))
		require.NoError(t, err)

		setTableGCTTL(t, 1)

		// Set the poll interval to be very long. This ensures that our cache will
		// not be updated for the duration of the test.
		protectedts.PollInterval.Override(ctx, &tc.Server(0).ClusterSettings().SV, 500*time.Hour)
		defer protectedts.PollInterval.Override(ctx, &tc.Server(0).ClusterSettings().SV, 2*time.Minute)

		// Disable follower reads. When metamorphically enabling expiration-based
		// leases, an expired lease will cause a follower read which bypasses the
		// strict GC enforcement.
		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.follower_reads.enabled = false")

		sqlDB.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '10 ms'")
		defer sqlDB.Exec(t, `SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = DEFAULT`)
		setStrictGC(t, true)
		tenSecondsAgo = tc.Server(0).Clock().Now().Add(-10*time.Second.Nanoseconds(), 0)
	}

	t.Run("strict enforcement", func(t *testing.T) {
		// Refresh past lease start.
		refreshPastLeaseStart(t)
		// The lease start might not pull up the PTS state `readAt` far enough,
		// which could mean that our scan falls in the `readAt - GCTTL` window and
		// succeeds.
		//
		// To guarantee failure, refresh the PTS state to now.
		refreshTo(t, tc.Server(0).Clock().Now())
		assertScanRejected(t)
	})
	t.Run("disable strict enforcement", func(t *testing.T) {
		setStrictGC(t, false)
		defer setStrictGC(t, true)
		assertScanOk(t)
	})
	t.Run("zone config changes are respected", func(t *testing.T) {
		refreshTo(t, tc.Server(0).Clock().Now())
		setTableGCTTL(t, 60)
		assertScanOk(t)
		setTableGCTTL(t, 1)
		assertScanRejected(t)
	})
	t.Run("system ranges are unaffected", func(t *testing.T) {
		setSystemGCTTL(t, 1)
		txn := mkStaleTxn()
		descriptorTable := keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)
		_, err := txn.Scan(ctx, descriptorTable, descriptorTable.PrefixEnd(), 1)
		require.NoError(t, err)
	})

	t.Run("protected timestamps are respected", func(t *testing.T) {
		// Block the KVSubscriber rangefeed from progressing.
		blockKVSubscriberCh := make(chan struct{})
		var isBlocked atomic.Bool
		mu.Lock()
		mu.blockOnTimestampUpdate = func() {
			isBlocked.Store(true)
			<-blockKVSubscriberCh
		}
		mu.Unlock()

		// Ensure that the KVSubscriber has been blocked.
		testutils.SucceedsSoon(t, func() error {
			if !isBlocked.Load() {
				return errors.New("kvsubscriber not blocked yet")
			}
			return nil
		})

		ptp := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		assertScanRejected(t)
		// Create a protected timestamp, and make sure it's not respected since the
		// KVSubscriber is blocked.
		rec := mkRecord()
		require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return ptp.WithTxn(txn).Protect(ctx, &rec)
		}))
		defer func() {
			require.NoError(t, insqlDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return ptp.WithTxn(txn).Release(ctx, rec.ID.GetUUID())
			}))
		}()
		assertScanRejected(t)

		// Unblock the KVSubscriber and wait for the PTS record to reach KV.
		close(blockKVSubscriberCh)
		desc, err := tc.LookupRange(tableKey)
		require.NoError(t, err)
		target, err := tc.FindRangeLeaseHolder(desc, nil)
		require.NoError(t, err)
		waitForProtectionAndReadProtectedTimestamps(t, target.NodeID, rec.Timestamp, tableSpan)
		assertScanOk(t)

		// Transfer the lease and demonstrate that the query succeeds because we're
		// cautious in the face of lease transfers.
		require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))
		assertScanOk(t)
	})
}

// TestProposalOverhead ensures that the command overhead for put operations
// is as expected. It exists to prevent changes which might increase the
// byte overhead of replicating commands.
//
// Note that it intentionally avoids using a system range which incurs the
// overhead due to the logical op log.
func TestProposalOverhead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var overhead uint32
	var key atomic.Value
	key.Store(roachpb.Key{})
	filter := func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if len(args.Req.Requests) != 1 {
			return nil
		}
		req, ok := args.Req.GetArg(kvpb.Put)
		if !ok {
			return nil
		}
		put := req.(*kvpb.PutRequest)
		if !bytes.Equal(put.Key, key.Load().(roachpb.Key)) {
			return nil
		}
		// Sometime the logical portion of the timestamp can be non-zero which makes
		// the overhead non-deterministic.
		args.Cmd.ReplicatedEvalResult.WriteTimestamp.Logical = 0
		atomic.StoreUint32(&overhead, uint32(args.Cmd.Size()-args.Cmd.WriteBatch.Size()))
		// We don't want to print the WriteBatch because it's explicitly
		// excluded from the size computation. Nil'ing it out does not
		// affect the memory held by the caller because neither `args` nor
		// `args.Cmd` are pointers.
		args.Cmd.WriteBatch = nil
		t.Log(pretty.Sprint(args.Cmd))
		return nil
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					TestingProposalFilter: filter,
				},
				SpanConfig: &spanconfig.TestingKnobs{
					ConfigureScratchRange: true,
				},
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	// NB: the expected overhead reflects the space overhead currently present
	// in Raft commands. This test will fail if that overhead changes. Try to
	// make this number go down and not up. It slightly undercounts because our
	// proposal filter is called before MaxLeaseIndex or ClosedTimestamp are
	// filled in. The difference between the user and system overhead is that
	// users ranges do not have rangefeeds on by default whereas system ranges
	// do.
	const (
		expectedUserOverhead uint32 = 42
	)
	t.Run("user-key overhead", func(t *testing.T) {
		userKey := tc.ScratchRange(t)
		testutils.SucceedsSoon(t, func() error {
			repl := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(userKey))
			if repl == nil {
				return errors.New("scratch range replica not found")
			}
			conf, err := repl.LoadSpanConfig(ctx)
			if err != nil {
				return err
			}
			if conf.RangefeedEnabled {
				return errors.New("waiting for span configs to apply")
			}
			return nil
		})
		k := roachpb.Key(encoding.EncodeStringAscending(userKey, "foo"))
		key.Store(k)
		require.NoError(t, db.Put(ctx, k, "v"))
		require.Equal(t, expectedUserOverhead, atomic.LoadUint32(&overhead))
	})

}

// TestDiscoverIntentAcrossLeaseTransferAwayAndBack tests a scenario where a
// read hits an intent, but only informs its lock-table about the discovered
// intent after the corresponding range's lease has been transferred away and
// back. If the intent is replaced during this time and the replacement intent
// has made its way into the lock-table, the initial read's discovery should not
// hit an assertion failure. It used to.
//
// The test uses a TestCluster to mirror the setup from:
//
//	concurrency/testdata/concurrency_manager/discover_lock_after_lease_race
func TestDiscoverIntentAcrossLeaseTransferAwayAndBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 122629)

	ctx := context.Background()

	// Use a manual clock so we can efficiently force leases to expire.
	// Required by TestCluster.MoveRangeLeaseNonCooperatively.
	manual := hlc.NewHybridManualClock()

	// Detect when txn2 has completed its read of txn1's intent and block.
	var txn2ID atomic.Value
	var txn2BBlockOnce sync.Once
	txn2BlockedC := make(chan chan struct{})

	// Detect when txn4 discovers txn3's intent and begins to push.
	var txn4ID atomic.Value
	txn4PushingC := make(chan struct{}, 1)
	requestFilter := func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if !ba.IsSinglePushTxnRequest() {
			return nil
		}
		if ba.Requests[0].GetPushTxn().PusherTxn.ID == txn4ID.Load() {
			select {
			case txn4PushingC <- struct{}{}:
			default:
			}
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: requestFilter,
				TestingConcurrencyRetryFilter: func(ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error) {
					if txn := ba.Txn; txn != nil && txn.ID == txn2ID.Load() {
						txn2BBlockOnce.Do(func() {
							if !errors.HasType(pErr.GoError(), (*kvpb.LockConflictError)(nil)) {
								t.Errorf("expected LockConflictError; got %v", pErr)
							}

							unblockCh := make(chan struct{})
							txn2BlockedC <- unblockCh
							<-unblockCh
						})
					}
				},
				// Required by TestCluster.MoveRangeLeaseNonCooperatively.
				AllowLeaseRequestProposalsWhenNotLeader: true,
			},
			Server: &server.TestingKnobs{
				WallClock: manual,
			},
		}},
	})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Servers[0].DB()

	key := []byte("a")
	rangeDesc, err := tc.LookupRange(key)
	require.NoError(t, err)

	// Transfer the lease to Server 0 so we start in a known state.
	err = tc.TransferRangeLease(rangeDesc, tc.Target(0))
	require.NoError(t, err)

	// txn1 writes the first intent.
	txn1 := kvDB.NewTxn(ctx, "txn1")
	err = txn1.Put(ctx, key, "val1")
	require.NoError(t, err)

	// txn2 reads the first intent. Should block during evaluation.
	txn2 := kvDB.NewTxn(ctx, "txn2")
	txn2ID.Store(txn2.ID())
	err2C := make(chan error)
	go func() {
		_, err := txn2.Get(ctx, key)
		err2C <- err
	}()
	var txn2UnblockC chan struct{}
	select {
	case txn2UnblockC = <-txn2BlockedC:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for txn2 to block")
	}

	// Transfer the lease to Server 1. Do so non-cooperatively instead of using
	// a lease transfer, because the cooperative lease transfer would get stuck
	// acquiring latches, which are held by txn2.
	_, err = tc.MoveRangeLeaseNonCooperatively(t, ctx, rangeDesc, tc.Target(1), manual)
	require.NoError(t, err)

	// Send an arbitrary request to the range to update the range descriptor
	// cache with the new lease. This prevents the rollback from getting stuck
	// waiting on latches held by txn2's read on the old leaseholder.
	_, err = kvDB.Get(ctx, "c")
	require.NoError(t, err)

	// Roll back txn1.
	err = txn1.Rollback(ctx)
	require.NoError(t, err)

	// txn3 writes the second intent.
	txn3 := kvDB.NewTxn(ctx, "txn3")
	err = txn3.Put(ctx, key, "val3")
	require.NoError(t, err)

	// Make sure txn3 creates its record before a lease transfer to avoid it
	// being aborted.
	hb, hbH := heartbeatArgs(txn3.TestingCloneTxn(), kvDB.Clock().Now())
	_, pErr := kv.SendWrappedWith(ctx, kvDB.GetFactory().NonTransactionalSender(), hbH, hb)
	require.NoError(t, pErr.GoError())

	// Transfer the lease back to Server 0.
	err = tc.TransferRangeLease(rangeDesc, tc.Target(0))
	require.NoError(t, err)

	// txn4 reads the second intent. Should discover intent and wait in lockTable.
	txn4 := kvDB.NewTxn(ctx, "txn4")
	txn4ID.Store(txn4.ID())
	err4C := make(chan error)
	go func() {
		_, err := txn4.Get(ctx, key)
		err4C <- err
	}()
	<-txn4PushingC
	close(txn2UnblockC)

	err = txn3.Rollback(ctx)
	require.NoError(t, err)
	require.NoError(t, <-err2C)
	require.NoError(t, <-err4C)
}

// getRangeInfo retrieves range info by performing a RangeStatsRequest against
// the provided key.
func getRangeInfo(
	ctx context.Context, db *kv.DB, key roachpb.Key,
) (ri *roachpb.RangeInfo, err error) {
	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		b.AddRawRequest(&kvpb.RangeStatsRequest{
			RequestHeader: kvpb.RequestHeader{Key: key},
		})
		if err = db.Run(ctx, b); err != nil {
			return err
		}
		ri = &b.RawResponse().Responses[0].GetRangeStats().RangeInfo
		return nil
	})
	return ri, err
}

// makeReplicationTargets creates a slice of replication targets where each
// target has a NodeID and StoreID with a value corresponding to an id in ids.
func makeReplicationTargets(ids ...int) (targets []roachpb.ReplicationTarget) {
	for _, id := range ids {
		targets = append(targets, roachpb.ReplicationTarget{
			NodeID:  roachpb.NodeID(id),
			StoreID: roachpb.StoreID(id),
		})
	}
	return targets
}

// TestTenantID tests that the tenant ID is properly set.
// This test examines the following behaviors:
//
//	(1) When range is split off for a tenant, that it gets the right tenant ID.
//	(2) When a replica is created with a raft message, it does not have a
//	   tenant ID, but then when it is initialized, it gets one.
//	(3) When a store starts up, it assigns the right tenant ID.
func TestTenantID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyVFSRegistry := fs.NewStickyRegistry()
	ctx := context.Background()
	// Create a config with a sticky-in-mem engine so we can restart the server.
	// We also configure the settings to be as robust as possible to problems
	// during stressrace as the setup of the rpc connections seems to somehow
	// fail sometimes when using secure connections.
	stickySpecTestServerArgs := base.TestServerArgs{
		Insecure: true,
		StoreSpecs: []base.StoreSpec{
			{
				InMemory:    true,
				StickyVFSID: "1",
			},
		},
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				StickyVFSRegistry: stickyVFSRegistry,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      stickySpecTestServerArgs,
	})
	defer tc.Stopper().Stop(ctx)

	tenant3 := roachpb.MustMakeTenantID(3)
	tenant3Prefix := keys.MakeTenantPrefix(tenant3)
	t.Run("(1) initial set", func(t *testing.T) {
		// Ensure that a normal range has the system tenant.
		{
			_, repl := getFirstStoreReplica(t, tc.Server(0), bootstrap.TestingUserTableDataMin(keys.SystemSQLCodec))
			tenantId, valid := repl.TenantID()
			require.True(t, valid)
			require.Equal(t, roachpb.SystemTenantID, tenantId, "%v", repl)
		}
		// Ensure that a range with a tenant prefix has the proper tenant ID.
		tc.SplitRangeOrFatal(t, tenant3Prefix)
		{
			_, repl := getFirstStoreReplica(t, tc.Server(0), tenant3Prefix)
			tenantId, valid := repl.TenantID()
			require.True(t, valid)
			require.Equal(t, tenant3, tenantId, "%v", repl)
		}
	})
	t.Run("(2) not set before snapshot", func(t *testing.T) {
		_, repl := getFirstStoreReplica(t, tc.Server(0), tenant3Prefix)
		sawSnapshot := make(chan struct{}, 1)
		blockSnapshot := make(chan struct{})
		tc.AddAndStartServer(t, base.TestServerArgs{
			Insecure: true,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					BeforeSnapshotSSTIngestion: func(
						snapshot kvserver.IncomingSnapshot,
						strings []string,
					) error {
						if snapshot.Desc.RangeID == repl.RangeID {
							select {
							case sawSnapshot <- struct{}{}:
							default:
							}
							<-blockSnapshot
						}
						return nil
					},
				},
			},
		})

		// We're going to block the snapshot. We need to retry adding the replica
		// to the second node as under stressrace, failures can occur due to
		// networking handshake timeouts.
		addReplicaErr := make(chan error)
		addReplica := func() {
			_, err := tc.AddVoters(tenant3Prefix, tc.Target(1))
			addReplicaErr <- err
		}
		go addReplica()
		if err := retry.ForDuration(3*time.Minute, func() error {
			select {
			case <-sawSnapshot:
				return nil
			case err := <-addReplicaErr:
				go addReplica()
				return err
			}
		}); err != nil {
			t.Fatal(err)
		}

		uninitializedRepl, _, err := tc.Server(1).GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, repl.RangeID)
		require.NoError(t, err)
		_, valid := uninitializedRepl.TenantID()
		require.False(t, valid)
		close(blockSnapshot)
		require.NoError(t, <-addReplicaErr)
		tenantID, valid := uninitializedRepl.TenantID() // now initialized
		require.True(t, valid)
		require.Equal(t, tenant3, tenantID)
	})
	t.Run("(3) upon restart", func(t *testing.T) {
		tc.StopServer(0)
		tc.AddAndStartServer(t, stickySpecTestServerArgs)
		_, repl := getFirstStoreReplica(t, tc.Server(2), tenant3Prefix)
		tenantID, _ := repl.TenantID() // now initialized
		require.Equal(t, tenant3, tenantID, "%v", repl)
	})

}

// TestUninitializedMetric ensures that uninitialized replicas are reflected in the UninitializedCount store metric.
func TestUninitializedMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      base.TestServerArgs{Insecure: true},
	})
	defer tc.Stopper().Stop(ctx)

	scratchKey := tc.ScratchRange(t)
	_, repl := getFirstStoreReplica(t, tc.Server(0), scratchKey)
	sawSnapshot := make(chan struct{}, 1)
	blockSnapshot := make(chan struct{})
	tc.AddAndStartServer(t, base.TestServerArgs{
		Insecure: true,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				BeforeSnapshotSSTIngestion: func(
					snapshot kvserver.IncomingSnapshot,
					_ []string,
				) error {
					if snapshot.Desc.RangeID == repl.RangeID {
						select {
						case sawSnapshot <- struct{}{}:
						default:
						}
						<-blockSnapshot
					}
					return nil
				},
			},
		},
	})

	// We're going to block the snapshot. We need to retry adding the replica
	// to the second node as under stressrace, failures can occur due to
	// networking handshake timeouts.
	addReplicaErr := make(chan error)
	addReplica := func() {
		_, err := tc.AddVoters(scratchKey, tc.Target(1))
		addReplicaErr <- err
	}
	go addReplica()
	if err := retry.ForDuration(3*time.Minute, func() error {
		select {
		case <-sawSnapshot:
			return nil
		case err := <-addReplicaErr:
			go addReplica()
			return err
		}
	}); err != nil {
		t.Fatal(err)
	}

	targetStore := tc.GetFirstStoreFromServer(t, 1)

	// Force the store to compute the replica metrics
	require.NoError(t, targetStore.ComputeMetrics(ctx))

	// Blocked snapshot on the second server (1) should realize 1 uninitialized replica.
	require.Equal(t, int64(1), targetStore.Metrics().UninitializedCount.Value())

	// Unblock snapshot on the second server (1), this should initialize replica.
	close(blockSnapshot)
	require.NoError(t, <-addReplicaErr)

	// Again force the store to compute metrics, increment tick counter 0 -> 1
	require.NoError(t, targetStore.ComputeMetrics(ctx))

	// There should now be no uninitialized replicas in the recorded metrics
	require.Equal(t, int64(0), targetStore.Metrics().UninitializedCount.Value())
}

// TestRangeMigration tests the below-raft migration infrastructure. It checks
// to see that the version recorded as part of the in-memory ReplicaState
// is up to date, and in-sync with the persisted state. It also checks to see
// that the right registered migration is invoked.
func TestRangeMigration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We're going to be transitioning from startV to endV. Think a cluster of
	// binaries running vX, but with active version vX-1.
	startV := clusterversion.PreviousRelease.Version()
	endV := clusterversion.Latest.Version()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endV, startV, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClusterVersionOverride:         startV,
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	key := tc.ScratchRange(t)
	desc, err := tc.LookupRange(key)
	require.NoError(t, err)
	rangeID := desc.RangeID

	store := tc.GetFirstStoreFromServer(t, 0)
	assertVersion := func(expV roachpb.Version) {
		repl, err := store.GetReplica(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		if gotV := repl.Version(); gotV != expV {
			t.Fatalf("expected in-memory version %s, got %s", expV, gotV)
		}

		sl := kvstorage.MakeStateLoader(rangeID)
		persistedV, err := sl.LoadVersion(ctx, store.StateEngine())
		if err != nil {
			t.Fatal(err)
		}
		if persistedV != expV {
			t.Fatalf("expected persisted version %s, got %s", expV, persistedV)
		}
	}

	assertVersion(startV)

	migrated := false
	unregister := batcheval.TestingRegisterMigrationInterceptor(endV, func() {
		migrated = true
	})
	defer unregister()

	kvDB := tc.Servers[0].DB()
	req := migrateArgs(desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey(), endV)
	if _, pErr := kv.SendWrappedWith(ctx, kvDB.GetFactory().NonTransactionalSender(), kvpb.Header{RangeID: desc.RangeID}, req); pErr != nil {
		t.Fatal(pErr)
	}

	if !migrated {
		t.Fatalf("expected migration interceptor to have been called")
	}
	assertVersion(endV)
}

// setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB sets up a DB that
// contains writes at keys a and b.
//
// The tests that use this helper are highly sensitive to latches that are
// still active on the keys written here. We disable CanAckBeforeReplication
// and write them via 1PC to ensure that latches are fully released and there
// no errant intent resolutions or anything of the kind is inflight by the time
// this method returns.
//
// See: https://github.com/cockroachdb/cockroach/pull/131071#issuecomment-2449439120.
func setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB(
	t *testing.T,
) (serverutils.TestServerInterface, *kv.DB) {
	ctx := context.Background()
	args := base.TestServerArgs{}
	args.Knobs.Store = &kvserver.StoreTestingKnobs{DisableCanAckBeforeApplication: true}
	s, _, db := serverutils.StartServer(t, args)
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		defer func() {
			if err != nil {
				t.Log(err)
			}
		}()
		b := txn.NewBatch()
		b.Put("a", "a")
		b.Put("b", "b")
		return txn.CommitInBatch(ctx, b)
	}))

	return s, db
}

// TestNonTransactionalLockingRequestsConflictWithReplicated locks ensures that
// non-transactional locking requests check for conflicts with replicated locks
// even though they cannot acquire locks that outlive their request.
//
// Regression test for https://github.com/cockroachdb/cockroach/issues/117628.
func TestNonTransactionalLockingRequestsConflictWithReplicatedLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db := setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	var wg sync.WaitGroup
	wg.Add(1)

	locksHeld := make(chan struct{})
	canReleaseLocks := make(chan struct{})

	// Set up the test by acquiring a replicated exclusive lock on keyA and a
	// replicated shared lock on KeyB. We'll hold these locks for the duration of
	// the test.
	go func() {
		defer wg.Done()

		txn1 := db.NewTxn(ctx, "txn1")
		_, err := txn1.GetForUpdate(ctx, keyA, kvpb.GuaranteedDurability)
		if err != nil {
			t.Error(err)
		}
		_, err = txn1.GetForShare(ctx, keyB, kvpb.GuaranteedDurability)
		if err != nil {
			t.Error(err)
		}

		close(locksHeld)
		<-canReleaseLocks // block for all test cases

		err = txn1.Commit(ctx)
		if err != nil {
			t.Error(err)
		}
	}()

	<-locksHeld

	for i, tc := range []struct {
		setup    func(*kvpb.BatchRequest, bool)
		expBlock bool
	}{
		// 1. Get requests.
		// 1a. Exclusive locking.
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := getArgs(keyA)
				req.KeyLockingStrength = lock.Exclusive
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := getArgs(keyB)
				req.KeyLockingStrength = lock.Exclusive
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := getArgs(keyC)
				req.KeyLockingStrength = lock.Exclusive
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: false,
		},
		// 1b. Shared locking.
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := getArgs(keyA)
				req.KeyLockingStrength = lock.Shared
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := getArgs(keyB)
				req.KeyLockingStrength = lock.Shared
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: false,
		},
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := getArgs(keyC)
				req.KeyLockingStrength = lock.Shared
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: false,
		},
		// 2. Scan requests.
		// 2a. Exclusive locking.
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := scanArgs(keyA, keyC)
				req.KeyLockingStrength = lock.Exclusive
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
		// 2b. Shared locking.
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := scanArgs(keyA, keyC)
				req.KeyLockingStrength = lock.Shared
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
		// 3. ReverseScan requests.
		// 3a. Exclusive locking.
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := revScanArgs(keyA, keyC)
				req.KeyLockingStrength = lock.Exclusive
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
		// 3b. Shared locking.
		{
			setup: func(ba *kvpb.BatchRequest, repl bool) {
				dur := lock.Unreplicated
				if repl {
					dur = lock.Replicated
				}
				req := revScanArgs(keyA, keyC)
				req.KeyLockingStrength = lock.Shared
				req.KeyLockingDurability = dur
				ba.Add(req)
			},
			expBlock: true,
		},
	} {
		testutils.RunTrueAndFalse(t, "replicated", func(t *testing.T, repl bool) {
			t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
				ba := &kvpb.BatchRequest{}
				// Having the request return an error instead of blocking on conflict
				// makes the test easier.
				ba.WaitPolicy = lock.WaitPolicy_Error
				tc.setup(ba, repl)

				store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
				require.NoError(t, err)
				_, pErr := store.TestSender().Send(ctx, ba)

				if tc.expBlock {
					require.NotNil(t, pErr)
					lcErr := new(kvpb.WriteIntentError)
					require.True(t, errors.As(pErr.GoError(), &lcErr))
					require.Equal(t, kvpb.WriteIntentError_REASON_WAIT_POLICY, lcErr.Reason)
				} else {
					require.Nil(t, pErr.GoError())
				}
			})
		})
	}

	close(canReleaseLocks)
	wg.Wait()
}

// TestSharedLocksBasic tests basic shared lock semantics. In particular, it
// tests multiple shared locks are compatible with each other, but exclusive
// locks aren't.
func TestSharedLocksBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db := setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	testutils.RunTrueAndFalse(t, "guaranteed-durability", func(t *testing.T, guaranteedDurability bool) {
		txn1 := db.NewTxn(ctx, "txn1")
		txn2 := db.NewTxn(ctx, "txn2")

		dur := kvpb.BestEffort
		if guaranteedDurability {
			dur = kvpb.GuaranteedDurability
		}

		res, err := txn1.ScanForShare(ctx, "a", "c", 0, dur)
		require.NoError(t, err)
		require.Equal(t, 2, len(res))

		_, err = txn2.ReverseScanForShare(ctx, "a", "c", 0, dur)
		require.NoError(t, err)
		require.Equal(t, 2, len(res))

		ch := make(chan struct{}, 1) // we won't pull off the channel
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			txn3 := db.NewTxn(ctx, "txn3")
			res, err := txn3.GetForUpdate(ctx, "a", dur)
			require.NoError(t, err)
			ch <- struct{}{}
			require.NotNil(t, res.Value)
			require.NoError(t, txn3.Commit(ctx))
		}()

		ensureGetForUpdateIsBlocked := func() {
			select {
			case <-ch:
				t.Fatal("expected GetForUpdate request to block")
			case <-time.After(10 * time.Millisecond):
				// sleep for a bit to allow the GetForUpdate to block.
			}
		}
		ensureGetForUpdateIsBlocked()
		require.NoError(t, txn1.Commit(ctx))
		// Finalizing just one of the shared locking transactions shouldn't unblock
		// the GetForUpdate.
		ensureGetForUpdateIsBlocked()
		require.NoError(t, txn2.Rollback(ctx))

		wg.Wait()
	})
}

// TestOptimisticEvalRetry tests the case where an optimistically evaluated
// scan encounters contention from a concurrent txn holding unreplicated
// exclusive locks, and therefore re-evaluates pessimistically, and eventually
// succeeds once the contending txn commits.
func TestOptimisticEvalRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db := setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	txn1 := db.NewTxn(ctx, "locking txn")
	_, err := txn1.ScanForUpdate(ctx, "a", "c", 0, kvpb.BestEffort)
	require.NoError(t, err)

	readDone := make(chan error)
	go func() {
		readDone <- db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			defer func() {
				t.Log(err)
			}()
			// We can't actually prove that the optimistic evaluation path was
			// taken, but it should happen based on the fact that this is a limited
			// scan with a limit of 1 row, and the replica has 2 rows.
			_, err = txn.Scan(ctx, "a", "c", 1)
			if err != nil {
				return err
			}
			return txn.Commit(ctx)
		})
	}()
	removedLocks := false
	var timer timeutil.Timer
	timer.Reset(time.Second * 2)
	defer timer.Stop()
	done := false
	for !done {
		select {
		case err := <-readDone:
			if !removedLocks {
				t.Fatal("read completed before exclusive locks were released")
			}
			require.NoError(t, err)
			require.True(t, removedLocks)
			done = true
		case <-timer.C:
			require.NoError(t, txn1.Commit(ctx))
			removedLocks = true
		}
	}
}

// TestOptimisticEvalNoContention tests the case where an optimistically
// evaluated scan has a span that overlaps with a concurrent txn holding
// unreplicated exclusive locks, but the actual span that is read does not
// overlap, and therefore the scan succeeds before the lock holding txn
// commits.
func TestOptimisticEvalNoContention(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	s, db := setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	txn1 := db.NewTxn(ctx, "locking txn")
	_, err := txn1.ScanForUpdate(ctx, "b", "c", 0, kvpb.BestEffort)
	require.NoError(t, err)

	readDone := make(chan error)
	go func() {
		readDone <- db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			ctx, sp := tracing.EnsureChildSpan(ctx, s.Tracer(), "limited-scan", tracing.WithForceRealSpan())
			sp.SetRecordingType(tracingpb.RecordingVerbose)
			defer func() {
				rec := sp.FinishAndGetConfiguredRecording()
				if err != nil {
					t.Log(err)
					t.Log(rec)
				}
			}()
			// There is no contention when doing optimistic evaluation, since it can read a
			// which is not locked.
			_, err = txn.Scan(ctx, "a", "c", 1)
			if err != nil {
				return err
			}
			return txn.Commit(ctx)
		})
	}()
	err = <-readDone
	require.NoError(t, err)
	require.NoError(t, txn1.Commit(ctx))
}

// TestOptimisticEvalWithConcurrentWriters tests concurrently running writes
// and optimistic reads where the latter always conflict. This is just a
// sanity check to confirm that nothing fails.
func TestOptimisticEvalWithConcurrentWriters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db := setupDBWithDisableCanAckBeforeApplicationAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	finish := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			for {
				require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
					if err := txn.Put(ctx, "a", "a"); err != nil {
						return err
					}
					return txn.Commit(ctx)
				}))
				select {
				case _, recv := <-finish:
					if !recv {
						workers.Done()
						return
					}
				default:
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			for {
				require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
					_, err = txn.Scan(ctx, "a", "c", 1)
					if err != nil {
						return err
					}
					err = txn.Commit(ctx)
					return err
				}))
				select {
				case _, recv := <-finish:
					if !recv {
						workers.Done()
						return
					}
				default:
				}
			}
		}()
	}
	time.Sleep(10 * time.Second)
	close(finish)
	workers.Wait()
}

func TestLeaseTransferReplicatesLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "can cause uncooperative lease change under leader leases")

	testutils.SetVModule(t, "cmd_lease=2")

	// Test Setup:
	//
	// txn1: holding lock on key k1
	// txn2: waiting on lock on key k1
	//
	// Test Mutation:
	//
	// Lease transfer on leaseholder of range containing k1.
	//
	// Test Assertion:
	//
	// txn2 is never unblocked (from the perspective of the client).
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	concurrency.UnreplicatedLockReliabilityLeaseTransfer.Override(ctx, &st.SV, true)
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				// Suppress timeout-based elections to prevent unexpected
				// leadership changes (and thus uncooperative lease changes
				// under leader leases) that would clear the lock table without
				// exporting unreplicated locks.
				RaftElectionTimeoutTicks: 1000000,
			},
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					RaftTestingKnobs: &raft.TestingKnobs{
						// Due to high RaftElectionTimeoutTicks, we only have
						// one opportunity to campaign which should not be
						// missed. Under leader leases, in a cold cluster, a
						// campaign can fail due to missing store liveness
						// support from the node's peers. Disallow this check to
						// ensure that the campaign succeeds.
						DisablePreCampaignStoreLivenessCheck: true,
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	scratch := tc.ScratchRange(t)
	k1 := append(scratch[:len(scratch):len(scratch)], uuid.MakeV4().String()...)
	// Write a value for the key because at the moment we don't create locks for
	// non-existent keys.
	require.NoError(t, tc.Server(1).DB().Put(ctx, k1, "value"))

	desc, err := tc.LookupRange(scratch)
	require.NoError(t, err)

	// Start with the lease on store 1.
	t.Logf("transferring to s1")
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(0)))
	t.Logf("done transferring to s1")

	// Txn 1:
	// - Acquire lock and block until we are are sure txn2 has returned.
	txn2Started := make(chan struct{})
	txn2StartedOnce := sync.OnceFunc(func() { close(txn2Started) })
	txn2Done := make(chan struct{})
	txn1HasLock := make(chan struct{})
	txn1HasLockOnce := sync.OnceFunc(func() { close(txn1HasLock) })

	g := ctxgroup.WithContext(ctx)
	g.Go(func() error {
		return tc.Server(1).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := txn.GetForUpdate(ctx, k1, kvpb.BestEffort)
			if err != nil {
				return err
			}
			txn1HasLockOnce()
			t.Log("txn1: lock acquired, waiting for txn2 cancellation")
			<-txn2Done
			t.Log("txn1: done")
			return nil
		})
	})

	// Txn 2:
	// - Block on txn 1. If it ever unblocks. We lost the lock.
	txn2Context, txn2Cancel := context.WithCancel(context.Background())
	g.Go(func() error {
		defer close(txn2Done)
		<-txn1HasLock
		t.Log("txn2: tnx1 lock acquisition observed, starting txn")
		err := tc.Server(1).DB().Txn(txn2Context, func(ctx context.Context, txn *kv.Txn) error {
			txn2StartedOnce()
			_, err := txn.GetForUpdate(ctx, k1, kvpb.BestEffort)
			if err != nil {
				return err
			}
			// We should never get here.
			t.Error("txn2: unexpectedly unblocked!")
			return nil
		})

		if errors.Is(err, context.Canceled) {
			return nil
		} else if err != nil {
			t.Logf("txn2: unexpected err: %s", err)
			return err
		} else {
			return nil
		}
	})

	// Move lease to to store 2 once txn1 and txn2 have both started (txn2 waits
	// on tx1 to start).
	<-txn2Started

	t.Log("transferring lease from s1 -> s2")
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))
	time.Sleep(250 * time.Millisecond)
	t.Log("cancelling txn2")
	txn2Cancel()
	require.NoError(t, g.Wait())

	// Check metrics
	locksWritten, err := tc.GetFirstStoreFromServer(t, 0).Metrics().GetStoreMetric("leases.transfers.locks_written")
	require.NoError(t, err)
	require.GreaterOrEqual(t, locksWritten, int64(1))
}

func TestLeaseTransferDropsLocksIfLargerThanCommandSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.SetVModule(t, "cmd_lease=2")

	// Test Plan:
	//
	// - Reduce MaxRaftCommandSize
	// - Move scratch range to known location.
	// - Take out a large number of unreplicated locks
	// - Transfer lease without an error
	//
	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	concurrency.UnreplicatedLockReliabilityLeaseTransfer.Override(ctx, &st.SV, true)
	kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 1<<20)
	// To see the test fail:
	// concurrency.MaxLockFlushSize.Override(ctx, &st.SV, 2<<20)

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{Settings: st},
	})
	defer tc.Stopper().Stop(ctx)

	scratch := tc.ScratchRange(t)
	desc, err := tc.LookupRange(scratch)
	require.NoError(t, err)

	// Start with the lease on store 1.
	t.Logf("transfering to s1")
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(0)))
	t.Logf("done transfering to s1")

	mkRandomScratchKey := func() roachpb.Key {
		return append(scratch.Clone(), uuid.MakeV4().String()...)
	}

	numLocks := 9000
	txn := tc.Server(1).DB().NewTxn(ctx, "test-lots-o-locks")
	b := txn.NewBatch()
	for range numLocks {
		b.GetForUpdate(mkRandomScratchKey(), kvpb.BestEffort)
		b.Requests()[len(b.Requests())-1].GetGet().LockNonExisting = true
	}
	require.NoError(t, txn.Run(ctx, b))

	t.Log("transfering lease from s1 -> s2")
	require.NoError(t, tc.TransferRangeLease(desc, tc.Target(1)))
}

func TestMergeDropsLocksIfLargerThanMax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.SetVModule(t, "cmd_subsume=2")

	// Test Plan:
	//
	// - Reduce MaxRaftCommandSize
	// - Move scratch range to known location.
	// - Take out a large number of unreplicated locks
	// - Merge range without an error
	//
	var (
		splitPoint = "b"
		ctx        = context.Background()
		st         = cluster.MakeClusterSettings()
	)

	concurrency.UnreplicatedLockReliabilityMerge.Override(ctx, &st.SV, true)
	kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 1<<20)
	// To see the test fail:
	// concurrency.MaxLockFlushSize.Override(ctx, &st.SV, 2<<20)

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{Settings: st},
	})
	defer tc.Stopper().Stop(ctx)

	scratch := tc.ScratchRange(t)

	mkKey := func(s string) roachpb.Key {
		prefix := scratch.Clone()
		return append(prefix[:len(prefix):len(prefix)], s...)
	}

	splitKey := mkKey(splitPoint)
	tc.SplitRangeOrFatal(t, splitKey)

	mkRandomScratchKey := func() roachpb.Key {
		return append(mkKey(splitPoint), uuid.MakeV4().String()...)
	}

	numLocks := 6000
	txn := tc.Server(0).DB().NewTxn(ctx, "test-lots-o-locks")
	b := txn.NewBatch()
	for range numLocks {
		b.GetForUpdate(mkRandomScratchKey(), kvpb.BestEffort)
		b.Requests()[len(b.Requests())-1].GetGet().LockNonExisting = true
	}
	require.NoError(t, txn.Run(ctx, b))

	// Merge Range
	t.Logf("merging range %s", scratch)
	_, err := tc.MergeRanges(scratch)
	require.NoError(t, err)
}

func TestMergeReplicatesLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	scope := log.Scope(t)
	defer scope.Close(t)

	skip.UnderDuress(t, "too slow for testrace")

	// Test Setup:
	//
	// txn1: holding lock on key k1
	// txn2: waiting on lock on key k1
	//
	// Test Mutation:
	//
	// Merge range holding k1.
	//
	// Test Assertion:
	//
	// txn2 is never unblocked (from the perspective of the client).
	//
	var (
		lhsKey     = "a"
		splitPoint = "b"
		rhsKey     = "c"

		ctx = context.Background()
		st  = cluster.MakeClusterSettings()
	)
	concurrency.UnreplicatedLockReliabilityMerge.Override(ctx, &st.SV, true)

	for _, rhsLock := range []bool{true, false} {
		name := "lhs-lock"
		lockKeySuffix := lhsKey
		if rhsLock {
			name = "rhs-lock"
			lockKeySuffix = rhsKey
		}
		t.Run(name, func(t *testing.T) {
			nodeCount := 3
			tc := testcluster.StartTestCluster(t, nodeCount, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Settings: st,
				},
			})
			defer tc.Stopper().Stop(ctx)

			defer func() {
				if !t.Failed() {
					return
				}
				d := kvtestutils.RaftLogDumper{Dir: scope.GetDirectory()}
				for _, srv := range tc.Servers {
					require.NoError(t, srv.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
						s.VisitReplicas(func(replica *kvserver.Replica) (wantMore bool) {
							d.Dump(t, s.LogEngine(), s.StoreID(), replica.RangeID)
							return true // more
						})
						return nil
					}))
				}
			}()

			sql := tc.ServerConn(0)
			scratch := tc.ScratchRange(t)
			mkKey := func(s string) roachpb.Key {
				prefix := scratch.Clone()
				return append(prefix[:len(prefix):len(prefix)], s...)
			}

			lockKey := mkKey(lockKeySuffix)
			splitKey := mkKey(splitPoint)
			tc.SplitRangeOrFatal(t, splitKey)
			// Write a value for the key because at the moment we don't create locks for
			// non-existent keys.
			require.NoError(t, tc.Server(1).DB().Put(ctx, lockKey, "value"))
			// Txn 1:
			// - Acquire lock and block until we are are sure txn2 has returned.
			txn2Started := make(chan struct{})
			txn2StartedOnce := sync.OnceFunc(func() { close(txn2Started) })
			txn2Done := make(chan struct{})
			txn1HasLock := make(chan struct{})
			g := ctxgroup.WithContext(ctx)
			g.Go(func() error {
				return tc.Server(1).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					_, err := txn.GetForUpdate(ctx, lockKey, kvpb.BestEffort)
					if err != nil {
						return err
					}
					close(txn1HasLock)
					t.Log("txn1: lock acquired, waiting for txn2 cancellation")
					<-txn2Done
					t.Log("txn1: done")
					return nil
				})
			})
			// Txn 2:
			// - Block on txn 1. If it ever unblocks. We lost the lock.
			txn2Context, txn2Cancel := context.WithCancel(context.Background())
			g.Go(func() error {
				defer close(txn2Done)
				<-txn1HasLock
				t.Log("txn2: tnx1 lock acquisition observed, starting txn")
				err := tc.Server(1).DB().Txn(txn2Context, func(ctx context.Context, txn *kv.Txn) error {
					txn2StartedOnce()
					_, err := txn.GetForUpdate(ctx, lockKey, kvpb.BestEffort)
					if err != nil {
						return err
					}
					// We should never get here.
					t.Error("txn2: unexpectedly unblocked!")
					return nil
				})
				if errors.Is(err, context.Canceled) {
					return nil
				} else if err != nil {
					t.Logf("txn2: unexpected err: %s", err)
					return err
				} else {
					return nil
				}
			})
			<-txn2Started
			t.Log("merging ranges")
			_, err := tc.MergeRanges(scratch)
			require.NoError(t, err)
			time.Sleep(250 * time.Millisecond)
			t.Log("cancelling txn2")
			txn2Cancel()
			require.NoError(t, g.Wait())
			failures := kvtestutils.CheckConsistency(ctx, sql, roachpb.Span{
				Key:    keys.ScratchRangeMin,
				EndKey: keys.ScratchRangeMax,
			})
			for _, err := range failures {
				t.Errorf("consistency failure: %s", err.Error())
			}
			if rhsLock {
				// The range could have been on any node. We just care that this metric
				// is written somewhere.
				var locksWritten int64
				for i := range nodeCount {
					l, err := tc.GetFirstStoreFromServer(t, i).Metrics().GetStoreMetric("subsume.locks_written")
					require.NoError(t, err)
					locksWritten += l
				}
				require.GreaterOrEqual(t, locksWritten, int64(1))
			}
		})
	}
}

// TestCommitTriggerFailuresDontCauseUnexpectedCommittedError tests that errors
// returned by the commit trigger don't cause transaction to unexpectedly
// be marked as COMMITTED in the response.
func TestCommitTriggerFailuresDontCauseUnexpectedCommittedError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Create an atomic boolean to control error injection
	var shouldInjectError atomic.Bool

	// errorInjector returns nil when the bool is false, and returns an error when
	// the bool is true.
	errorInjector := func() error {
		if shouldInjectError.Load() {
			return errors.New("boom")
		}
		return nil
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
				CommitTriggerError: errorInjector,
			},
		}}},
	})
	defer tc.Stopper().Stop(ctx)

	require.NoError(t, tc.WaitForFullReplication())
	db := tc.Server(0).DB()
	tc.ScratchRange(t)

	shouldInjectError.Store(true)
	err := db.AdminSplit(ctx, scratchRKey("c"), hlc.MaxTimestamp)

	// Verify that the error is expected.
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.False(t, errors.HasAssertionFailure(err), "%+v", err)

	// Removing the error injector should allow the split to succeed.
	shouldInjectError.Store(false)
	require.NoError(t, db.AdminSplit(ctx, scratchRKey("c"), hlc.MaxTimestamp))
}
