// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// TestReplicaClockUpdates verifies that the leaseholder and followers both
// update their clocks when executing a command to the command's timestamp, as
// long as the request timestamp is from a clock (i.e. is not synthetic).
func TestReplicaClockUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, write bool, synthetic bool) {
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
						ClockSource: manuals[i].UnixNano,
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
		// MaxOffset. Set the synthetic flag according to the test case.
		reqTS := clocks[0].Now().Add(clocks[0].MaxOffset().Nanoseconds()/2, 0).WithSynthetic(synthetic)
		h := roachpb.Header{Timestamp: reqTS}

		// Execute the command.
		var req roachpb.Request
		if write {
			req = incrementArgs(reqKey, 5)
		} else {
			req = getArgs(reqKey)
		}
		if _, err := kv.SendWrappedWith(ctx, store.TestSender(), h, req); err != nil {
			t.Fatal(err)
		}

		// If writing, wait for that command to execute on all the replicas.
		// Consensus is asynchronous outside of the majority quorum, and Raft
		// application is asynchronous on all nodes.
		if write {
			testutils.SucceedsSoon(t, func() error {
				var values []int64
				for i := range tc.Servers {
					val, _, err := storage.MVCCGet(ctx,
						tc.GetFirstStoreFromServer(t, i).Engine(), reqKey, reqTS,
						storage.MVCCGetOptions{})
					if err != nil {
						return err
					}
					values = append(values, mustGetInt(val))
				}
				if !reflect.DeepEqual(values, []int64{5, 5, 5}) {
					return errors.Errorf("expected (5, 5, 5), got %v", values)
				}
				return nil
			})
		}

		// Verify that clocks were updated as expected. Check all clocks if we
		// issued a write, but only the leaseholder's if we issued a read. In
		// theory, we should be able to assert that _only_ the leaseholder's
		// clock is updated by a read, but in practice an assertion against
		// followers' clocks being updated is very difficult to make without
		// being flaky because it's difficult to prevent other channels
		// (background work, etc.) from carrying the clock update.
		expUpdated := !synthetic
		clocksToCheck := clocks
		if !write {
			clocksToCheck = clocks[:1]
		}
		for _, c := range clocksToCheck {
			require.Equal(t, expUpdated, reqTS.Less(c.Now()))
		}
	}

	testutils.RunTrueAndFalse(t, "write", func(t *testing.T, write bool) {
		testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
			run(t, write, synthetic)
		})
	})
}

// TestFollowersDontRejectClockUpdateWithJump verifies that followers update
// their clocks when executing a command, even if the leaseholder's clock is
// far in the future.
func TestFollowersDontRejectClockUpdateWithJump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
					ClockSource: manuals[i].UnixNano,
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

	store := tc.GetFirstStoreFromServer(t, 0)
	reqKey := roachpb.Key("a")
	tc.SplitRangeOrFatal(t, reqKey)
	tc.AddVotersOrFatal(t, reqKey, tc.Targets(1, 2)...)

	for i, s := range tc.Servers {
		clocks = append(clocks, s.Clock())
		manuals[i].Pause()
	}
	// Advance the lease holder's clock ahead of the followers (by more than
	// MaxOffset but less than the range lease) and execute a command.
	manuals[0].Increment(int64(500 * time.Millisecond))
	incArgs := incrementArgs(reqKey, 5)
	ts := clocks[0].Now()
	if _, err := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{Timestamp: ts}, incArgs); err != nil {
		t.Fatal(err)
	}
	// Wait for that command to execute on all the followers.
	tc.WaitForValues(t, reqKey, []int64{5, 5, 5})

	// Verify that all the followers have accepted the clock update from
	// node 0 even though it comes from outside the usual max offset.
	now := clocks[0].Now()
	for i, clock := range clocks {
		// Only compare the WallTimes: it's normal for clock 0 to be a few logical ticks ahead.
		if clock.Now().WallTime < now.WallTime {
			t.Errorf("clock %d is behind clock 0: %s vs %s", i, clock.Now(), now)
		}
	}
}

// TestLeaseholdersRejectClockUpdateWithJump verifies that leaseholders reject
// commands that would cause a large time jump.
func TestLeaseholdersRejectClockUpdateWithJump(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := hlc.NewHybridManualClock()
	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manual.UnixNano,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	manual.Pause()
	ts1 := s.Clock().Now()

	key := roachpb.Key("a")
	incArgs := incrementArgs(key, 5)

	// Commands with a future timestamp that is within the MaxOffset
	// bound will be accepted and will cause the clock to advance.
	const numCmds = 3
	clockOffset := s.Clock().MaxOffset() / numCmds
	for i := int64(1); i <= numCmds; i++ {
		ts := ts1.Add(i*clockOffset.Nanoseconds(), 0).WithSynthetic(false)
		if _, err := kv.SendWrappedWith(context.Background(), store.TestSender(), roachpb.Header{Timestamp: ts}, incArgs); err != nil {
			t.Fatal(err)
		}
	}

	ts2 := s.Clock().Now()
	if expAdvance, advance := ts2.GoTime().Sub(ts1.GoTime()), numCmds*clockOffset; advance != expAdvance {
		t.Fatalf("expected clock to advance %s; got %s", expAdvance, advance)
	}

	// Once the accumulated offset reaches MaxOffset, commands will be rejected.
	tsFuture := ts1.Add(s.Clock().MaxOffset().Nanoseconds()+1, 0).WithSynthetic(false)
	_, pErr := kv.SendWrappedWith(ctx, store.TestSender(), roachpb.Header{Timestamp: tsFuture}, incArgs)
	if !testutils.IsPError(pErr, "remote wall time is too far ahead") {
		t.Fatalf("unexpected error %v", pErr)
	}

	// The clock did not advance and the final command was not executed.
	ts3 := s.Clock().Now()
	if advance := ts3.GoTime().Sub(ts2.GoTime()); advance != 0 {
		t.Fatalf("expected clock not to advance, but it advanced by %s", advance)
	}
	val, _, err := storage.MVCCGet(context.Background(), store.Engine(), key, ts3,
		storage.MVCCGetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if a, e := mustGetInt(val), incArgs.Increment*numCmds; a != e {
		t.Errorf("expected %d, got %d", e, a)
	}
}

// TestTxnPutOutOfOrder tests a case where a put operation of an older
// timestamp comes after a put operation of a newer timestamp in a
// txn. The test ensures such an out-of-order put succeeds and
// overrides an old value. The test uses a "Writer" and a "Reader"
// to reproduce an out-of-order put.
//
// 1) The Writer executes a cput operation and writes a write intent with
//    time T in a txn.
// 2) Before the Writer's txn is committed, the Reader sends a high priority
//    get operation with time T+100. This pushes the Writer txn timestamp to
//    T+100. The Reader also writes to the same key the Writer did a cput to
//    in order to trigger the restart of the Writer's txn. The original
//    write intent timestamp is also updated to T+100.
// 3) The Writer starts a new epoch of the txn, but before it writes, the
//    Reader sends another high priority get operation with time T+200. This
//    pushes the Writer txn timestamp to T+200 to trigger a restart of the
//    Writer txn. The Writer will not actually restart until it tries to commit
//    the current epoch of the transaction. The Reader updates the timestamp of
//    the write intent to T+200. The test deliberately fails the Reader get
//    operation, and cockroach doesn't update its timestamp cache.
// 4) The Writer executes the put operation again. This put operation comes
//    out-of-order since its timestamp is T+100, while the intent timestamp
//    updated at Step 3 is T+200.
// 5) The put operation overrides the old value using timestamp T+100.
// 6) When the Writer attempts to commit its txn, the txn will be restarted
//    again at a new epoch timestamp T+200, which will finally succeed.
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
	var numGets int32

	testingEvalFilter := func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
		if _, ok := filterArgs.Req.(*roachpb.GetRequest); ok &&
			filterArgs.Req.Header().Key.Equal(roachpb.Key(key)) &&
			filterArgs.Hdr.Txn == nil {
			// The Reader executes two get operations, each of which triggers two get requests
			// (the first request fails and triggers txn push, and then the second request
			// succeeds). Returns an error for the fourth get request to avoid timestamp cache
			// update after the third get operation pushes the txn timestamp.
			if atomic.AddInt32(&numGets, 1) == 4 {
				return roachpb.NewErrorWithTxn(errors.Errorf("Test"), filterArgs.Hdr.Txn)
			}
		}
		return nil
	}
	manual := hlc.NewHybridManualClock()
	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClockSource: manual.UnixNano,
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
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
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
				log.Errorf(context.Background(), "failed put value: %+v", err)
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
	requestHeader := roachpb.RequestHeader{
		Key: roachpb.Key(key),
	}
	h := roachpb.Header{
		Timestamp:    s.Clock().Now(),
		UserPriority: priority,
	}
	if _, err := kv.SendWrappedWith(
		context.Background(), store.TestSender(), h, &roachpb.GetRequest{RequestHeader: requestHeader},
	); err != nil {
		t.Fatalf("failed to get: %+v", err)
	}
	// Write to the restart key so that the Writer's txn must restart.
	putReq := &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(restartKey)},
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
	if _, err := kv.SendWrappedWith(
		context.Background(), store.TestSender(), h, &roachpb.GetRequest{RequestHeader: requestHeader},
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

// TestRangeLookupUseReverse tests whether the results and the results count
// are correct when scanning in reverse order.
func TestRangeLookupUseReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Init test ranges:
	// ["","a"), ["a","c"), ["c","e"), ["e","g") and ["g","\xff\xff").
	splits := []*roachpb.AdminSplitRequest{
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
	scanArgs := roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeader{
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
				test.key.AsRawKey(), roachpb.READ_UNCOMMITTED, test.maxResults-1, true /* prefetchReverse */)
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
	filter                     func(filterArgs kvserverbase.FilterArgs) *roachpb.Error
	waitForTransferBlocked     atomic.Value
	transferBlocked            chan struct{}
	manualClock                *hlc.HybridManualClock
}

func setupLeaseTransferTest(t *testing.T) *leaseTransferTest {
	l := &leaseTransferTest{
		leftKey:     roachpb.Key("a"),
		manualClock: hlc.NewHybridManualClock(),
	}

	testingEvalFilter := func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
		l.filterMu.Lock()
		filterCopy := l.filter
		l.filterMu.Unlock()
		if filterCopy != nil {
			return filterCopy(filterArgs)
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

	l.tc = testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							TestingEvalFilter: testingEvalFilter,
						},
						LeaseTransferBlockedOnExtensionEvent: leaseTransferBlockedOnExtensionEvent,
					},
					Server: &server.TestingKnobs{
						ClockSource: l.manualClock.UnixNano,
					},
				},
			},
		})
	key := l.tc.ScratchRangeWithExpirationLease(t)
	l.tc.AddVotersOrFatal(t, key, l.tc.Target(1))
	require.NoError(t, l.tc.WaitForVoters(key, l.tc.Targets(0, 1)...))

	// First, do a write; we'll use it to determine when the dust has settled.
	l.leftKey = key
	incArgs := incrementArgs(l.leftKey, 1)
	if _, pErr := kv.SendWrapped(context.Background(), l.tc.Servers[0].DistSender(), incArgs); pErr != nil {
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

func (l *leaseTransferTest) sendRead(t *testing.T, storeIdx int) *roachpb.Error {
	desc := l.tc.GetFirstStoreFromServer(t, storeIdx).LookupReplica(keys.MustAddr(l.leftKey))
	replicaDesc, err := desc.GetReplicaDescriptor()
	if err != nil {
		return roachpb.NewError(err)
	}
	_, pErr := kv.SendWrappedWith(
		context.Background(),
		l.tc.GetFirstStoreFromServer(t, storeIdx).TestSender(),
		roachpb.Header{RangeID: desc.RangeID, Replica: replicaDesc},
		getArgs(l.leftKey),
	)
	if pErr != nil {
		log.Warningf(context.Background(), "%v", pErr)
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
		l.filter = nil
		return
	}
	l.filter = func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
		if filterArgs.Sid != l.tc.Target(1).StoreID {
			return nil
		}
		llReq, ok := filterArgs.Req.(*roachpb.RequestLeaseRequest)
		if !ok {
			return nil
		}
		if llReq.Lease.Replica == l.replica1Desc {
			// Notify the main thread that the extension is in progress and wait for
			// the signal to proceed.
			l.filterMu.Lock()
			l.filter = nil
			l.filterMu.Unlock()
			extensionSem <- struct{}{}
			log.Infof(filterArgs.Ctx, "filter blocking request: %s", llReq)
			<-extensionSem
			log.Infof(filterArgs.Ctx, "filter unblocking lease request")
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
	// Set the clock close to the lease's expiration.
	l.manualClock.Increment(lease.Expiration.WallTime - l.manualClock.UnixNano() - 10)
	err := l.sendRead(t, storeIdx).GoError()
	// We can sometimes receive an error from our renewal attempt because the
	// lease transfer ends up causing the renewal to re-propose and second
	// attempt fails because it's already been renewed. This used to work
	// before we compared the proposer's lease with the actual lease because
	// the renewed lease still encompassed the previous request.
	if errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {
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
		progress, ok := status.Progress[uint64(follower.ReplicaID)]
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
		if err := l.replica0.AdminTransferLease(ctx, l.replica0Desc.StoreID); err != nil {
			t.Fatal(err)
		}
		newLease, _ := l.replica0.GetLease()
		if !origLease.Equivalent(newLease) {
			t.Fatalf("original lease %v and new lease %v not equivalent", origLease, newLease)
		}
	}

	{
		// An invalid target should result in an error.
		const expected = "unable to find store .* in range"
		if err := l.replica0.AdminTransferLease(ctx, 1000); !testutils.IsError(err, expected) {
			t.Fatalf("expected %s, but found %v", expected, err)
		}
	}

	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID); err != nil {
		t.Fatal(err)
	}

	// Check that replica0 doesn't serve reads any more.
	pErr := l.sendRead(t, 0)
	nlhe, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, pErr)
	}
	if !nlhe.LeaseHolder.Equal(&l.replica1Desc) {
		t.Fatalf("expected lease holder %+v, got %+v",
			l.replica1Desc, nlhe.LeaseHolder)
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
	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID); err != nil {
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
		err := l.replica1.AdminTransferLease(context.Background(), l.replica0Desc.StoreID)
		// Ignore not leaseholder errors which can arise due to re-proposals.
		if errors.HasType(err, (*roachpb.NotLeaseHolderError)(nil)) {
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
	l.tc.GetFirstStoreFromServer(t, 0).SetDraining(true, nil /* reporter */)

	// Check that replica0 doesn't serve reads any more.
	pErr := l.sendRead(t, 0)
	nlhe, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, pErr)
	}
	if nlhe.LeaseHolder == nil || !nlhe.LeaseHolder.Equal(&l.replica1Desc) {
		t.Fatalf("expected lease holder %+v, got %+v",
			l.replica1Desc, nlhe.LeaseHolder)
	}

	// Check that replica1 now has the lease.
	l.checkHasLease(t, 1)

	l.tc.GetFirstStoreFromServer(t, 0).SetDraining(false, nil /* reporter */)
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
	if err := l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID); err != nil {
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
		l.tc.GetFirstStoreFromServer(t, 1).SetDraining(true, nil /* reporter */)
	}()
	// Now unblock the extension.
	extensionSem <- struct{}{}

	l.checkHasLease(t, 0)
	l.setFilter(false, nil)

	if err := <-renewalErrCh; err != nil {
		t.Errorf("unexpected error from lease renewal: %+v", err)
	}
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
		require.NoError(t, l.replica0.AdminTransferLease(ctx, l.replica1Desc.StoreID))
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
		// expiration. Remember to set the synthetic bit so that it is not used
		// to update the store's clock. See Replica.checkRequestTimeRLocked for
		// the exact determination of whether a request timestamp is too far in
		// the future or not.
		leaseRenewal := l.tc.Servers[1].Cfg.RangeLeaseRenewalDuration()
		leaseRenewalMinusStasis := leaseRenewal - l.tc.Servers[1].Clock().MaxOffset()
		reqTime := now.Add(leaseRenewalMinusStasis.Nanoseconds()-10, 0)
		if tooFarInFuture {
			reqTime = reqTime.Add(20, 0)
		}
		reqTime = reqTime.WithSynthetic(true)

		// Issue a get with the request timestamp.
		args := getArgs(l.leftKey)
		_, pErr := kv.SendWrappedWith(ctx, l.tc.GetFirstStoreFromServer(t, 1).TestSender(), roachpb.Header{
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
					ClockSource: manuals[i].UnixNano,
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

	keyA := tc.ScratchRange(t)

	for _, m := range manuals {
		m.Pause()
	}
	// Set the first clock at lease t+2 compared to the second nodes clock.
	if manuals[0].UnixNano() <= manuals[1].UnixNano() {
		manuals[0].Increment((manuals[1].UnixNano() - manuals[0].UnixNano()) + 2)
	}

	// Start a transaction using node2 as a gateway.
	txn := roachpb.MakeTransaction("test", keyA, 1, tc.Servers[1].Clock().Now(), tc.Servers[1].Clock().MaxOffset().Nanoseconds() /* maxOffsetNs */)
	// Simulate a read to another range on node2 by setting the observed timestamp.
	txn.UpdateObservedTimestamp(2, tc.Servers[1].Clock().NowAsClockTimestamp())

	// Do a write on node1 to establish a key with its timestamp at now.
	if _, pErr := kv.SendWrapped(
		ctx, tc.Servers[0].DistSender(), putArgs(keyA, []byte("value")),
	); pErr != nil {
		t.Fatal(pErr)
	}

	// Up-replicate the data in the range to node2.
	tc.AddVotersOrFatal(t, keyA, tc.Target(1))
	replica1 := tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(keyA))

	// Transfer the lease from node1 to node2.
	replica2 := tc.GetFirstStoreFromServer(t, 1).LookupReplica(roachpb.RKey(keyA))
	replica2Desc, err := replica2.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	node1Before := tc.Servers[0].Clock().Now()
	testutils.SucceedsSoon(t, func() error {
		if err := replica1.AdminTransferLease(ctx, replica2Desc.StoreID); err != nil {
			t.Fatal(err)
		}
		lease, _ := replica2.GetLease()
		if lease.Replica.NodeID != replica2.NodeID() {
			return errors.Errorf("expected lease transfer to node2: %s", lease)
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
	h := roachpb.Header{Txn: &txn}
	if _, pErr := kv.SendWrappedWith(
		ctx, tc.Servers[0].DistSender(), h, getArgs(keyA),
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
	var injectLeaseTransferError atomic.Value
	testingEvalFilter := func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
		if args, ok := filterArgs.Req.(*roachpb.TransferLeaseRequest); ok {
			if val := injectLeaseTransferError.Load(); val != nil && val.(bool) {
				// Note that we can't just return an error here as we only
				// end up counting failures in the metrics if the command
				// makes it through to being executed. So use a fake store ID.
				args.Lease.Replica.StoreID = roachpb.StoreID(1000)
			}
		}
		return nil
	}
	ctx := context.Background()
	manualClock := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
							TestingEvalFilter: testingEvalFilter,
						},
					},
					Server: &server.TestingKnobs{
						ClockSource: manualClock.UnixNano,
					},
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	// Up-replicate to two replicas.
	expirationKey := tc.ScratchRangeWithExpirationLease(t)
	expirationDesc := tc.LookupRangeOrFatal(t, expirationKey)
	tc.AddVotersOrFatal(t, expirationKey, tc.Target(1))

	epochKey := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, epochKey, tc.Target(1))

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
	splitDesc := tc.LookupRangeOrFatal(t, epochKey)
	err := tc.TransferRangeLease(splitDesc, tc.Target(1))
	// We expect this to fail.
	require.Error(t, err)

	metrics := tc.GetFirstStoreFromServer(t, 0).Metrics()
	if a, e := metrics.LeaseTransferSuccessCount.Count(), int64(1); a != e {
		t.Errorf("expected %d lease transfer successes; got %d", e, a)
	}
	if a, e := metrics.LeaseTransferErrorCount.Count(), int64(1); a != e {
		t.Errorf("expected %d lease transfer errors; got %d", e, a)
	}

	// Expire current leases and put a key to the epoch based scratch range to
	// get a lease.
	testutils.SucceedsSoon(t, func() error {
		manualClock.Increment(tc.GetFirstStoreFromServer(t, 0).GetStoreConfig().LeaseExpiration())
		if err := tc.GetFirstStoreFromServer(t, 0).DB().Put(context.Background(), epochKey, "foo"); err != nil {
			return err
		}

		// Update replication gauges for all stores and verify we have 1 each of
		// expiration and epoch leases.
		var expirationLeases int64
		var epochLeases int64
		for i := range tc.Servers {
			if err := tc.GetFirstStoreFromServer(t, i).ComputeMetrics(context.Background(), 0); err != nil {
				return err
			}
			metrics = tc.GetFirstStoreFromServer(t, i).Metrics()
			expirationLeases += metrics.LeaseExpirationCount.Value()
			epochLeases += metrics.LeaseEpochCount.Value()
		}
		if a, e := expirationLeases, int64(1); a != e {
			return errors.Errorf("expected %d expiration lease count; got %d", e, a)
		}
		if a, e := epochLeases, int64(1); a < e {
			return errors.Errorf("expected greater than %d epoch lease count; got %d", e, a)
		}
		return nil
	})
}

// Test that leases held before a restart are not used after the restart.
// See replica.mu.minLeaseProposedTS for the reasons why this isn't allowed.
func TestLeaseNotUsedAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	var leaseAcquisitionTrap atomic.Value
	ctx := context.Background()
	manual := hlc.NewHybridManualClock()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				StoreSpecs: []base.StoreSpec{
					{
						InMemory:               true,
						StickyInMemoryEngineID: "1",
					},
				},
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClockSource:          manual.UnixNano,
						StickyEngineRegistry: stickyEngineRegistry,
					},
					Store: &kvserver.StoreTestingKnobs{
						LeaseRequestEvent: func(ts hlc.Timestamp, _ roachpb.StoreID, _ roachpb.RangeID) *roachpb.Error {
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

	log.Info(ctx, "restarting")
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
	cmdFilter := func(fArgs kvserverbase.FilterArgs) *roachpb.Error {
		if fArgs.Hdr.UserPriority == 42 {
			// Signal that the read is blocked.
			readBlocked <- struct{}{}
			// Wait for read to be unblocked.
			<-readBlocked
		}
		return nil
	}
	srv, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: cmdFilter,
					},
				},
			},
		})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}

	// Start a read and wait for it to block.
	key := roachpb.Key("a")
	errChan := make(chan error)
	go func() {
		getReq := roachpb.GetRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
		}
		if _, pErr := kv.SendWrappedWith(ctx, s.DB().NonTransactionalSender(),
			roachpb.Header{UserPriority: 42},
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

		leaseReq := roachpb.RequestLeaseRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: key,
			},
			Lease: roachpb.Lease{
				Start:      s.Clock().NowAsClockTimestamp(),
				Expiration: s.Clock().Now().Add(time.Second.Nanoseconds(), 0).Clone(),
				Replica:    replDesc,
			},
		}

		for {
			leaseInfo, _, err := s.GetRangeLease(ctx, key, server.AllowQueryToBeForwardedToDifferentNode)
			if err != nil {
				t.Fatal(err)
			}
			leaseReq.PrevLease = leaseInfo.CurrentOrProspective()

			_, pErr := kv.SendWrapped(ctx, s.DB().NonTransactionalSender(), &leaseReq)
			if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); ok {
				log.Infof(ctx, "retrying lease after %s", pErr)
				continue
			}
			if _, ok := pErr.GetDetail().(*roachpb.LeaseRejectedError); ok {
				// Lease rejected? Try again. The extension should work because
				// extending is idempotent (assuming the PrevLease matches).
				log.Infof(ctx, "retrying lease after %s", pErr)
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

func getLeaseInfo(
	ctx context.Context, db *kv.DB, key roachpb.Key,
) (*roachpb.LeaseInfoResponse, error) {
	header := roachpb.Header{ReadConsistency: roachpb.INCONSISTENT}
	leaseInfoReq := &roachpb.LeaseInfoRequest{RequestHeader: roachpb.RequestHeader{Key: key}}
	reply, pErr := kv.SendWrappedWith(ctx, db.NonTransactionalSender(), header, leaseInfoReq)
	if pErr != nil {
		return nil, pErr.GoError()
	}
	return reply.(*roachpb.LeaseInfoResponse), nil
}

func TestLeaseInfoRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(context.Background())

	kvDB0 := tc.Servers[0].DB()
	kvDB1 := tc.Servers[1].DB()

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
	mustGetLeaseInfo := func(db *kv.DB) *roachpb.LeaseInfoResponse {
		resp, err := getLeaseInfo(context.Background(), db, rangeDesc.StartKey.AsRawKey())
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Transfer the lease to Servers[0] so we start in a known state. Otherwise,
	// there might be already a lease owned by a random node.
	err = tc.TransferRangeLease(rangeDesc, tc.Target(0))
	if err != nil {
		t.Fatal(err)
	}

	// Now test the LeaseInfo. We might need to loop until the node we query has
	// applied the lease.
	testutils.SucceedsSoon(t, func() error {
		leaseHolderReplica := mustGetLeaseInfo(kvDB0).Lease.Replica
		if leaseHolderReplica != replicas[0] {
			return fmt.Errorf("lease holder should be replica %+v, but is: %+v",
				replicas[0], leaseHolderReplica)
		}
		return nil
	})

	// Transfer the lease to Server 1 and check that LeaseInfoRequest gets the
	// right answer.
	err = tc.TransferRangeLease(rangeDesc, tc.Target(1))
	if err != nil {
		t.Fatal(err)
	}
	// An inconsistent LeaseInfoReqeust on the old lease holder should give us the
	// right answer immediately, since the old holder has definitely applied the
	// transfer before TransferRangeLease returned.
	leaseHolderReplica := mustGetLeaseInfo(kvDB0).Lease.Replica
	if !leaseHolderReplica.Equal(replicas[1]) {
		t.Fatalf("lease holder should be replica %+v, but is: %+v",
			replicas[1], leaseHolderReplica)
	}

	// A read on the new lease holder does not necessarily succeed immediately,
	// since it might take a while for it to apply the transfer.
	testutils.SucceedsSoon(t, func() error {
		// We can't reliably do a CONSISTENT read here, even though we're reading
		// from the supposed lease holder, because this node might initially be
		// unaware of the new lease and so the request might bounce around for a
		// while (see #8816).
		leaseHolderReplica = mustGetLeaseInfo(kvDB1).Lease.Replica
		if !leaseHolderReplica.Equal(replicas[1]) {
			return errors.Errorf("lease holder should be replica %+v, but is: %+v",
				replicas[1], leaseHolderReplica)
		}
		return nil
	})

	// Transfer the lease to Server 2 and check that LeaseInfoRequest gets the
	// right answer.
	err = tc.TransferRangeLease(rangeDesc, tc.Target(2))
	if err != nil {
		t.Fatal(err)
	}

	// We're now going to ask servers[1] for the lease info. We don't use kvDB1;
	// instead we go directly to the store because otherwise the DistSender might
	// use an old, cached, version of the range descriptor that doesn't have the
	// local replica in it (and so the request would be routed away).
	// TODO(andrei): Add a batch option to not use the range cache.
	s, err := tc.Servers[1].Stores().GetStore(tc.Servers[1].GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}
	leaseInfoReq := &roachpb.LeaseInfoRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: rangeDesc.StartKey.AsRawKey(),
		},
	}
	reply, pErr := kv.SendWrappedWith(
		context.Background(), s, roachpb.Header{
			RangeID:         rangeDesc.RangeID,
			ReadConsistency: roachpb.INCONSISTENT,
		}, leaseInfoReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	resp := *(reply.(*roachpb.LeaseInfoResponse))
	leaseHolderReplica = resp.Lease.Replica

	if !leaseHolderReplica.Equal(replicas[2]) {
		t.Fatalf("lease holder should be replica %s, but is: %s", replicas[2], leaseHolderReplica)
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
	cmdFilter := func(fArgs kvserverbase.FilterArgs) *roachpb.Error {
		if fArgs.Hdr.UserPriority == 42 {
			return roachpb.NewErrorf("injected error")
		}
		return nil
	}
	srv, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
						TestingEvalFilter: cmdFilter,
					},
				},
			},
		})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop(context.Background())

	// Send the lease request.
	key := roachpb.Key("a")
	leaseReq := roachpb.LeaseInfoRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
	_, pErr := kv.SendWrappedWith(
		context.Background(),
		s.DB().NonTransactionalSender(),
		roachpb.Header{UserPriority: 42},
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
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
			ba := roachpb.BatchRequest{
				Header: roachpb.Header{
					RangeID: tc.rangeID,
					ClientRangeInfo: roachpb.ClientRangeInfo{
						DescriptorGeneration: preSplitDesc.Generation,
					},
				},
			}
			gArgs := &roachpb.GetRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: tc.key.AsRawKey(),
				},
			}
			ba.Add(gArgs)
			br, pErr := store.Send(ctx, ba)
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
	tc.GetFirstStoreFromServer(t, 1).SetDraining(true, nil /* reporter */)
	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(drainingIdx))
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); !testutils.IsError(err, "store is draining") {
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
	chgs := roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(1))
	if _, err := repl.ChangeReplicas(context.Background(), repl.Desc(), kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeUnderReplicated, "", chgs); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.EqualValues(t, repl.Desc().Generation, oldGeneration+2)

	oldGeneration = repl.Desc().Generation
	oldDesc := repl.Desc()
	chgs[0].ChangeType = roachpb.REMOVE_VOTER
	newDesc, err := repl.ChangeReplicas(context.Background(), oldDesc, kvserver.SnapshotRequest_REBALANCE, kvserverpb.ReasonRangeOverReplicated, "", chgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Generation changes:
	// +1 for entering joint config due to demotion
	// +1 for transitioning out of joint config
	// +1 for removing learner
	assert.EqualValues(t, repl.Desc().Generation, oldGeneration+3, "\nold: %+v\nnew: %+v", oldDesc, newDesc)
}

func TestSystemZoneConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test is relatively slow and resource intensive. When run under
	// stressrace on a loaded machine (as in the nightly tests), sometimes the
	// SucceedsSoon conditions below take longer than the allotted time (#25273).
	skip.UnderRace(t)
	skip.UnderShort(t)
	skip.UnderStress(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 7, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable LBS because when the scan is happening at the rate it's happening
					// below, it's possible that one of the system ranges trigger a split.
					DisableLoadBasedSplitting: true,
				},
			},
			// Scan like a bat out of hell to ensure replication and replica GC
			// happen in a timely manner.
			ScanInterval: 50 * time.Millisecond,
		},
	})
	defer tc.Stopper().Stop(ctx)
	log.Info(ctx, "TestSystemZoneConfig: test cluster started")

	expectedSystemRanges, err := tc.Servers[0].ExpectedInitialRangeCount()
	if err != nil {
		t.Fatal(err)
	}
	expectedUserRanges := 1
	expectedSystemRanges -= expectedUserRanges
	systemNumReplicas := int(*zonepb.DefaultSystemZoneConfig().NumReplicas)
	userNumReplicas := int(*zonepb.DefaultZoneConfig().NumReplicas)
	expectedReplicas := expectedSystemRanges*systemNumReplicas + expectedUserRanges*userNumReplicas
	log.Infof(ctx, "TestSystemZoneConfig: expecting %d system ranges and %d user ranges",
		expectedSystemRanges, expectedUserRanges)
	log.Infof(ctx, "TestSystemZoneConfig: expected (%dx%d) + (%dx%d) = %d replicas total",
		expectedSystemRanges, systemNumReplicas, expectedUserRanges, userNumReplicas, expectedReplicas)

	waitForReplicas := func() error {
		replicas := make(map[roachpb.RangeID]roachpb.RangeDescriptor)
		for _, s := range tc.Servers {
			if err := kvserver.IterateRangeDescriptors(ctx, s.Engines()[0], func(desc roachpb.RangeDescriptor) error {
				if len(desc.Replicas().LearnerDescriptors()) > 0 {
					return fmt.Errorf("descriptor contains learners: %v", desc)
				}
				if existing, ok := replicas[desc.RangeID]; ok && !existing.Equal(&desc) {
					return fmt.Errorf("mismatch between\n%s\n%s", &existing, &desc)
				}
				replicas[desc.RangeID] = desc
				return nil
			}); err != nil {
				return err
			}
		}
		var totalReplicas int
		for _, desc := range replicas {
			totalReplicas += len(desc.Replicas().VoterDescriptors())
		}
		if totalReplicas != expectedReplicas {
			return fmt.Errorf("got %d voters, want %d; details: %+v", totalReplicas, expectedReplicas, replicas)
		}
		return nil
	}

	// Wait until we're down to the expected number of replicas. This is
	// effectively waiting on replica GC to kick in to destroy any replicas that
	// got removed during rebalancing of the initial ranges, since the testcluster
	// waits until nothing is underreplicated but not until all rebalancing has
	// settled down.
	testutils.SucceedsSoon(t, waitForReplicas)
	log.Info(ctx, "TestSystemZoneConfig: initial replication succeeded")

	// Update the meta zone config to have more replicas and expect the number
	// of replicas to go up accordingly after running all replicas through the
	// replicate queue.
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE meta", "num_replicas: 7")
	expectedReplicas += 2
	testutils.SucceedsSoon(t, waitForReplicas)
	log.Info(ctx, "TestSystemZoneConfig: up-replication of meta ranges succeeded")

	// Do the same thing, but down-replicating the timeseries range.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE timeseries", "num_replicas: 1")
	expectedReplicas -= 2
	testutils.SucceedsSoon(t, waitForReplicas)
	log.Info(ctx, "TestSystemZoneConfig: down-replication of timeseries ranges succeeded")

	// Up-replicate the system.jobs table to demonstrate that it is configured
	// independently from the system database.
	sqlutils.SetZoneConfig(t, sqlDB, "TABLE system.jobs", "num_replicas: 7")
	expectedReplicas += 2
	testutils.SucceedsSoon(t, waitForReplicas)
	log.Info(ctx, "TestSystemZoneConfig: up-replication of jobs table succeeded")

	// Finally, verify the system ranges. Note that in a new cluster there are
	// two system ranges, which we have to take into account here.
	sqlutils.SetZoneConfig(t, sqlDB, "RANGE system", "num_replicas: 7")
	expectedReplicas += 4
	testutils.SucceedsSoon(t, waitForReplicas)
	log.Info(ctx, "TestSystemZoneConfig: up-replication of system ranges succeeded")
}

func TestClearRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	clearRange := func(start, end roachpb.Key) {
		t.Helper()
		if _, err := kv.SendWrapped(ctx, store.DB().NonTransactionalSender(), &roachpb.ClearRangeRequest{
			RequestHeader: roachpb.RequestHeader{
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
		kvs, err := storage.Scan(store.Engine(), start, end, 0 /* maxRows */)
		if err != nil {
			t.Fatal(err)
		}
		var actualKeys []roachpb.Key
		for _, kv := range kvs {
			actualKeys = append(actualKeys, kv.Key.Key)
		}
		if !reflect.DeepEqual(expectedKeys, actualKeys) {
			t.Fatalf("expected %v, but got %v", expectedKeys, actualKeys)
		}
	}

	rng, _ := randutil.NewPseudoRand()

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
						ClockSource: manualClock.UnixNano,
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
			readTS = readTS.Add(500*time.Millisecond.Nanoseconds(), 0).WithSynthetic(true)
		}

		// Read the key at readTS.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		var ba roachpb.BatchRequest
		ba.Timestamp = readTS
		ba.Add(getArgs(keyA))
		br, pErr := tc.Servers[0].DistSender().Send(ctx, ba)
		require.Nil(t, pErr)
		require.Equal(t, readTS, br.Timestamp)
		v, err := br.Responses[0].GetGet().Value.GetInt()
		require.NoError(t, err)
		require.Equal(t, int64(1), v)

		repl0 := store0.LookupReplica(roachpb.RKey(keyA))

		// Partition node 2 from the rest of its range. Once partitioned, perform
		// another write and truncate the Raft log on the two connected nodes. This
		// ensures that that when node 2 comes back up it will require a snapshot
		// from Raft.
		funcs := noopRaftHandlerFuncs()
		funcs.dropReq = func(*kvserver.RaftMessageRequest) bool {
			return true
		}
		tc.Servers[2].RaftTransport().Listen(store2.StoreID(), &unreliableRaftHandler{
			rangeID:                    repl0.GetRangeID(),
			RaftMessageHandler:         store2,
			unreliableRaftHandlerFuncs: funcs,
		})

		if _, pErr := kv.SendWrapped(ctx, store0.TestSender(), incC); pErr != nil {
			t.Fatal(pErr)
		}
		tc.WaitForValues(t, keyC, []int64{4, 4, 2})

		// Truncate the log at index+1 (log entries < N are removed, so this
		// includes the increment). This necessitates a snapshot when the
		// partitioned replica rejoins the rest of the range.
		index, err := repl0.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		truncArgs := truncateLogArgs(index+1, repl0.GetRangeID())
		truncArgs.Key = keyA
		if _, err := kv.SendWrapped(ctx, store0.TestSender(), truncArgs); err != nil {
			t.Fatal(err)
		}

		// Finally, transfer the lease to node 2 while it is still unavailable and
		// behind. We try to avoid this case when picking new leaseholders in practice,
		// but we're never 100% successful.
		if err := repl0.AdminTransferLease(ctx, store2.Ident.StoreID); err != nil {
			t.Fatal(err)
		}

		// Remove the partition. A snapshot to node 2 should follow. This snapshot
		// will inform node 2 that it is the new leaseholder for the range. Node 2
		// should act accordingly and update its internal state to reflect this.
		tc.Servers[2].RaftTransport().Listen(store2.Ident.StoreID, store2)
		tc.WaitForValues(t, keyC, []int64{4, 4, 4})

		// Attempt to write under the read on the new leaseholder. The batch
		// should get forwarded to a timestamp after the read. With the bug in
		// #34025, the new leaseholder who heard about the lease transfer from a
		// snapshot had an empty timestamp cache and would simply let us write
		// under the previous read.
		// NB: don't use SendWrapped because we want access to br.Timestamp.
		ba = roachpb.BatchRequest{}
		ba.Timestamp = readTS
		ba.Add(incrementArgs(keyA, 1))
		br, pErr = tc.Servers[0].DistSender().Send(ctx, ba)
		require.Nil(t, pErr)
		require.NotEqual(t, readTS, br.Timestamp)
		require.True(t, readTS.Less(br.Timestamp))
		require.Equal(t, readTS.Synthetic, br.Timestamp.Synthetic)
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
			ctx, key, expects1, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, targets1...))
		wg.Done()
	}()
	go func() {
		res2, err2 = db.AdminChangeReplicas(
			ctx, key, expects2, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, targets2...))
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
	errors := make([]error, actors)
	var wg sync.WaitGroup
	key := roachpb.Key("a")
	db := tc.Servers[0].DB()
	require.Nil(t, db.AdminRelocateRange(
		ctx, key, makeReplicationTargets(1, 2, 3), nil,
	))
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
		_, err := db.AdminChangeReplicas(ctx, key, rangeInfo.Desc, roachpb.MakeReplicationChanges(op, pickTargets()...))
		return err
	}
	wg.Add(actors)
	for i := 0; i < actors; i++ {
		go func(i int) { errors[i] = addReplicas(); wg.Done() }(i)
	}
	wg.Wait()
	var gotSuccess bool
	for _, err := range errors {
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
	firstVoter, secondVoter, nonVoter := tc.Target(0), tc.Target(1), tc.Target(3)
	firstStore, err := tc.Server(0).GetStores().(*kvserver.Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)
	firstRepl := firstStore.LookupReplica(roachpb.RKey(key))
	require.NotNil(t, firstRepl, "the first node in the TestCluster must have a"+
		" replica for the ScratchRange")

	tc.AddNonVotersOrFatal(t, key, nonVoter)
	// TODO(aayush): Trying to swap the last voting replica with a non-voter hits
	// the safeguard inside Replica.propose() as the last voting replica is always
	// the leaseholder. There are a bunch of subtleties around getting a
	// leaseholder to remove itself without another voter to immediately transfer
	// the lease to. See #40333.
	_, err = tc.SwapVoterWithNonVoter(key, firstVoter, nonVoter)
	require.Regexp(t, "received invalid ChangeReplicasTrigger", err)

	tc.AddVotersOrFatal(t, key, secondVoter)

	tc.SwapVoterWithNonVoterOrFatal(t, key, secondVoter, nonVoter)
}

// TestReplicaTombstone ensures that tombstones are written when we expect
// them to be. Tombstones are laid down when replicas are removed.
// Replicas are removed for several reasons:
//
//  (1)   In response to a ChangeReplicasTrigger which removes it.
//  (2)   In response to a ReplicaTooOldError from a sent raft message.
//  (3)   Due to the replica GC queue detecting a replica is not in the range.
//  (3.1) When the replica detects the range has been merged away.
//  (4)   Due to a raft message addressed to a newer replica ID.
//  (4.1) When the older replica is not initialized.
//  (5)   Due to a merge.
//  (6)   Due to snapshot which subsumes a range.
//
// This test creates all of these scenarios and ensures that tombstones are
// written at sane values.
func TestReplicaTombstone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("(1) ChangeReplicasTrigger", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
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
		funcs := noopRaftHandlerFuncs()
		funcs.dropResp = func(*kvserver.RaftMessageResponse) bool {
			return true
		}
		tc.Servers[1].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
			rangeID:                    desc.RangeID,
			RaftMessageHandler:         store,
			unreliableRaftHandlerFuncs: funcs,
		})
		tc.RemoveVotersOrFatal(t, key, tc.Target(1))
		tombstone := waitForTombstone(t, store.Engine(), rangeID)
		require.Equal(t, roachpb.ReplicaID(3), tombstone.NextReplicaID)
	})
	t.Run("(2) ReplicaTooOldError", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				RaftConfig: base.RaftConfig{
					// Make the tick interval short so we don't need to wait too long for
					// the partitioned node to time out but increase the lease timeout
					// so expiration-based leases still work.
					RaftTickInterval:                        time.Millisecond,
					RangeLeaseRaftElectionTimeoutMultiplier: 10000,
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
		raftFuncs := noopRaftHandlerFuncs()
		raftFuncs.dropResp = func(resp *kvserver.RaftMessageResponse) bool {
			if pErr, ok := resp.Union.GetValue().(*roachpb.Error); ok {
				if _, isTooOld := pErr.GetDetail().(*roachpb.ReplicaTooOldError); isTooOld {
					select {
					case sawTooOld <- struct{}{}:
					default:
					}
				}
			}
			return false
		}
		raftFuncs.dropReq = func(req *kvserver.RaftMessageRequest) bool {
			return req.ToReplica.StoreID == store.StoreID()
		}
		tc.Servers[2].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
			rangeID:                    desc.RangeID,
			RaftMessageHandler:         store,
			unreliableRaftHandlerFuncs: raftFuncs,
		})
		tc.RemoveVotersOrFatal(t, key, tc.Target(2))
		testutils.SucceedsSoon(t, func() error {
			repl.UnquiesceAndWakeLeader()
			if len(sawTooOld) == 0 {
				return errors.New("still haven't seen ReplicaTooOldError")
			}
			return nil
		})
		// Wait until we're sure that the replica has seen ReplicaTooOld,
		// then go look for the tombstone.
		<-sawTooOld
		tombstone := waitForTombstone(t, store.Engine(), rangeID)
		require.Equal(t, roachpb.ReplicaID(4), tombstone.NextReplicaID)
	})
	t.Run("(3) ReplicaGCQueue", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)

		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
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
		tc.Servers[2].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
			rangeID:            desc.RangeID,
			RaftMessageHandler: store,
		})
		tc.RemoveVotersOrFatal(t, key, tc.Target(2))
		repl, err := store.GetReplica(desc.RangeID)
		require.NoError(t, err)
		require.NoError(t, store.ManualReplicaGC(repl))
		tombstone := waitForTombstone(t, store.Engine(), rangeID)
		require.Equal(t, roachpb.ReplicaID(4), tombstone.NextReplicaID)
	})
	// This case also detects the tombstone for nodes which processed the merge.
	t.Run("(3.1) (5) replica GC queue and merge", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)

		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 4, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
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
		tc.Servers[2].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
			rangeID:            desc.RangeID,
			RaftMessageHandler: store,
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
		tombstone := waitForTombstone(t, store.Engine(), rangeID)
		require.Equal(t, roachpb.ReplicaID(math.MaxInt32), tombstone.NextReplicaID)
		// Verify the tombstone generated from processing a merge trigger.
		store3, _ := getFirstStoreReplica(t, tc.Server(0), key)
		tombstone = waitForTombstone(t, store3.Engine(), rangeID)
		require.Equal(t, roachpb.ReplicaID(math.MaxInt32), tombstone.NextReplicaID)
	})
	t.Run("(4) (4.1) raft messages to newer replicaID ", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				RaftConfig: base.RaftConfig{
					// Make the tick interval short so we don't need to wait too long
					// for a heartbeat to be sent. Increase the election timeout so
					// expiration based leases still work.
					RaftTickInterval:                        time.Millisecond,
					RangeLeaseRaftElectionTimeoutMultiplier: 10000,
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
		recordHeartbeat := func(replicaID roachpb.ReplicaID) {
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
		tc.Servers[2].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
			rangeID:            desc.RangeID,
			RaftMessageHandler: store,
			unreliableRaftHandlerFuncs: unreliableRaftHandlerFuncs{
				dropResp: func(*kvserver.RaftMessageResponse) bool {
					return true
				},
				dropReq: func(*kvserver.RaftMessageRequest) bool {
					return true
				},
				dropHB: func(hb *kvserver.RaftHeartbeat) bool {
					recordHeartbeat(hb.ToReplicaID)
					return false
				},
				snapErr: func(*kvserver.SnapshotRequest_Header) error {
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
		// Don't use tc.AddVoter; this would retry internally as we're faking a
		// a snapshot error here (and these are all considered retriable).
		_, err = tc.Servers[0].DB().AdminChangeReplicas(
			ctx, key, tc.LookupRangeOrFatal(t, key), roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(2)),
		)
		require.Regexp(t, "boom", err)
		tombstone := waitForTombstone(t, store.Engine(), rangeID)
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
			ctx, key, tc.LookupRangeOrFatal(t, key), roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(2)),
		)
		require.Regexp(t, "boom", err)
		// We will start out reading the old tombstone so keep retrying.
		testutils.SucceedsSoon(t, func() error {
			tombstone = waitForTombstone(t, store.Engine(), rangeID)
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
		var proposalFilter atomic.Value
		noopProposalFilter := func(kvserverbase.ProposalFilterArgs) *roachpb.Error {
			return nil
		}
		proposalFilter.Store(noopProposalFilter)
		tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
					DisableReplicaGCQueue: true,
					TestingProposalFilter: kvserverbase.ReplicaProposalFilter(
						func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
							return proposalFilter.
								Load().(func(kvserverbase.ProposalFilterArgs) *roachpb.Error)(args)
						},
					),
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
		raftFuncs := noopRaftHandlerFuncs()
		raftFuncs.dropReq = func(req *kvserver.RaftMessageRequest) bool {
			return partActive.Load().(bool) && req.Message.Type == raftpb.MsgApp
		}
		tc.Servers[2].RaftTransport().Listen(store.StoreID(), &unreliableRaftHandler{
			rangeID:                    lhsDesc.RangeID,
			unreliableRaftHandlerFuncs: raftFuncs,
			RaftMessageHandler: &unreliableRaftHandler{
				rangeID:                    rhsDesc.RangeID,
				RaftMessageHandler:         store,
				unreliableRaftHandlerFuncs: raftFuncs,
			},
		})
		proposalFilter.Store(func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
			merge := args.Cmd.ReplicatedEvalResult.Merge
			if merge != nil && merge.LeftDesc.RangeID == lhsDesc.RangeID {
				partActive.Store(true)
			}
			return nil
		})
		require.NoError(t, tc.Server(0).DB().AdminMerge(ctx, key))
		var tombstone roachpb.RangeTombstone
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
			tombstoneKey := keys.RangeTombstoneKey(rhsDesc.RangeID)
			ok, err := storage.MVCCGetProto(
				context.Background(), store.Engine(), tombstoneKey, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
			)
			require.NoError(t, err)
			if !ok {
				return errors.New("no tombstone found")
			}
			return nil
		})
		require.Equal(t, roachpb.ReplicaID(math.MaxInt32), tombstone.NextReplicaID)
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
	responseFilter := func(ctx context.Context, ba roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
		if ba.IsSingleRequest() {
			changeReplicas, ok := ba.Requests[0].GetInner().(*roachpb.AdminChangeReplicasRequest)
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
		ctx, key, makeReplicationTargets(1, 2, 3), makeReplicationTargets(),
	))
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
	r1, _, err := tc.Servers[0].Stores().GetReplicaForRangeID(ctx, rangeInfo.Desc.RangeID)
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
		chgs := roachpb.MakeReplicationChanges(roachpb.REMOVE_VOTER, makeReplicationTargets(2)...)
		changedDesc, changeErr = r1.ChangeReplicas(ctx, &expDescAfterAdd, kvserver.SnapshotRequest_REBALANCE, "replicate", "testing", chgs)
	}
	relocate := func() {
		relocateErr = db.AdminRelocateRange(
			ctx, key, makeReplicationTargets(1, 2, 4), makeReplicationTargets(),
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
// descriptor matches the expectation and then uses the bytes of the read read
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
		blockOnChangeReplicasRead := kvserverbase.ReplicaRequestFilter(func(ctx context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			if req, isGet := ba.GetArg(roachpb.Get); !isGet ||
				ba.RangeID != rangeToBlockRangeDescriptorRead.Load().(roachpb.RangeID) ||
				!ba.IsSingleRequest() ||
				!bytes.HasSuffix([]byte(req.(*roachpb.GetRequest).Key),
					[]byte(keys.LocalRangeDescriptorSuffix)) {
				return nil
			}
			select {
			case <-blockRangeDescriptorReadChan:
				<-blockRangeDescriptorReadChan
			case <-ctx.Done():
			default:
			}
			return nil
		})
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
			return db.AdminChangeReplicas(ctx, key, desc, []roachpb.ReplicationChange{
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
				ctx, rhs, *rhsDesc, roachpb.MakeReplicationChanges(roachpb.ADD_VOTER, tc.Target(2)),
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
//  (1) The outgoing leaseholder learns about its removal before applying the
//      lease transfer. This could happen if it has a lot left to apply but it
//      does indeed know in its log that it is either no longer the leaseholder
//      or that some of its commands will apply successfully.
//
//  (2) The replica learns about its removal after applying the lease transfer
//      but it potentially still has pending commands which it thinks might
//      have been proposed. This can occur if there are commands which are
//      proposed after the lease transfer has been proposed but before the lease
//      transfer has applied. This can also occur if commands are re-ordered
//      by raft due to a leadership change.
//
//  (3) The replica learns about its removal after applying the lease transfer
//      but proposed a command evaluated under the old lease after the lease
//      transfer has been applied. This can occur if there are commands evaluate
//      before the lease transfer is proposed but are not inserted into the
//      proposal buffer until after it has been applied.
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
				TestingProposalFilter: kvserverbase.ReplicaProposalFilter(
					func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
						if args.Req.RangeID != scratchRangeID.Load().(roachpb.RangeID) {
							return nil
						}
						// Block increment requests on blockInc.
						if _, isInc := args.Req.GetArg(roachpb.Increment); isInc {
							unblock := make(chan struct{})
							blockInc <- unblock
							<-unblock
						}
						return nil
					},
				),
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

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
	})
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
		mkRecord      = func() ptpb.Record {
			return ptpb.Record{
				ID:        uuid.MakeV4(),
				Timestamp: tenSecondsAgo.Add(-10*time.Second.Nanoseconds(), 0),
				Spans:     []roachpb.Span{tableSpan},
			}
		}
		mkStaleTxn = func() *kv.Txn {
			txn := db.NewTxn(ctx, "foo")
			txn.SetFixedTimestamp(ctx, tenSecondsAgo)
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
		// Make sure the cache has been updated. Once it has then we know it won't
		// be for minutes. It should read on startup.
		waitForCacheAfter = func(t *testing.T, min hlc.Timestamp) {
			t.Helper()
			testutils.SucceedsSoon(t, func() error {
				for i := 0; i < tc.NumServers(); i++ {
					ptp := tc.Server(i).ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
					if ptp.Iterate(ctx, tableKey, tableKey, func(record *ptpb.Record) (wantMore bool) {
						return false
					}).Less(min) {
						return errors.Errorf("not yet read")
					}
				}
				return nil
			})
		}
		setGCTTL = func(t *testing.T, object string, exp int) {
			t.Helper()
			testutils.SucceedsSoon(t, func() error {
				sqlDB.Exec(t, `ALTER `+object+` CONFIGURE ZONE USING gc.ttlseconds = `+strconv.Itoa(exp))
				for i := 0; i < tc.NumServers(); i++ {
					s := tc.Server(i)
					_, r := getFirstStoreReplica(t, s, tableKey)
					if _, z := r.DescAndZone(); z.GC.TTLSeconds != int32(exp) {
						_, sysCfg := getFirstStoreReplica(t, tc.Server(i), keys.SystemConfigSpan.Key)
						sysCfg.RaftLock()
						require.NoError(t, sysCfg.MaybeGossipSystemConfigRaftMuLocked(ctx))
						sysCfg.RaftUnlock()
						return errors.Errorf("expected %d, got %d", exp, z.GC.TTLSeconds)
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
					s, r := getFirstStoreReplica(t, tc.Server(i), keys.SystemConfigSpan.Key)
					if kvserver.StrictGCEnforcement.Get(&s.ClusterSettings().SV) != val {
						r.RaftLock()
						require.NoError(t, r.MaybeGossipSystemConfigRaftMuLocked(ctx))
						r.RaftUnlock()
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
				ptp := tc.Server(i).ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
				_, r := getFirstStoreReplica(t, tc.Server(i), tableKey)
				l, _ := r.GetLease()
				require.NoError(t, ptp.Refresh(ctx, l.Start.ToTimestamp().Next()))
				r.ReadProtectedTimestamps(ctx)
			}
		}
	)

	{
		// Setup the initial state to be sure that we'll actually strictly enforce
		// gc ttls.
		tc.SplitRangeOrFatal(t, tableKey)
		_, err := tc.AddVoters(tableKey, tc.Target(1), tc.Target(2))
		require.NoError(t, err)
		_, err = tc.AddVoters(keys.SystemConfigSpan.Key, tc.Target(1), tc.Target(2))
		require.NoError(t, err)

		setTableGCTTL(t, 1)
		waitForCacheAfter(t, hlc.Timestamp{})

		defer sqlDB.Exec(t, `SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = DEFAULT`)
		setStrictGC(t, true)
		tenSecondsAgo = tc.Server(0).Clock().Now().Add(-10*time.Second.Nanoseconds(), 0)
	}

	t.Run("strict enforcement", func(t *testing.T) {
		refreshPastLeaseStart(t)
		assertScanRejected(t)
	})
	t.Run("disable strict enforcement", func(t *testing.T) {
		setStrictGC(t, false)
		defer setStrictGC(t, true)
		assertScanOk(t)
	})
	t.Run("zone config changes are respected", func(t *testing.T) {
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
		waitForCacheAfter(t, hlc.Timestamp{})
		ptp := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		assertScanRejected(t)
		// Create a protected timestamp, don't verify it, make sure it's not
		// respected.
		rec := mkRecord()
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return ptp.Protect(ctx, txn, &rec)
		}))
		assertScanRejected(t)

		require.NoError(t, ptp.Verify(ctx, rec.ID))
		assertScanOk(t)

		// Transfer the lease and demonstrate that the query succeeds because we're
		// cautious in the face of lease transfers.
		desc, err := tc.LookupRange(tableKey)
		require.NoError(t, err)
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
	filter := func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
		if len(args.Req.Requests) != 1 {
			return nil
		}
		req, ok := args.Req.GetArg(roachpb.Put)
		if !ok {
			return nil
		}
		put := req.(*roachpb.PutRequest)
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
		t.Logf(pretty.Sprint(args.Cmd))
		return nil
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{TestingProposalFilter: filter},
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
//   concurrency/testdata/concurrency_manager/discover_lock_after_lease_race
func TestDiscoverIntentAcrossLeaseTransferAwayAndBack(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Use a manual clock so we can efficiently force leases to expire.
	// Required by TestCluster.MoveRangeLeaseNonCooperatively.
	manual := hlc.NewHybridManualClock()

	// Detect when txn2 has completed its read of txn1's intent and block.
	var txn2ID atomic.Value
	var txn2BBlockOnce sync.Once
	txn2BlockedC := make(chan chan struct{})
	postEvalFilter := func(args kvserverbase.FilterArgs) *roachpb.Error {
		if txn := args.Hdr.Txn; txn != nil && txn.ID == txn2ID.Load() {
			txn2BBlockOnce.Do(func() {
				if !errors.HasType(args.Err, (*roachpb.WriteIntentError)(nil)) {
					t.Errorf("expected WriteIntentError; got %v", args.Err)
				}

				unblockCh := make(chan struct{})
				txn2BlockedC <- unblockCh
				<-unblockCh
			})
		}
		return nil
	}

	// Detect when txn4 discovers txn3's intent and begins to push.
	var txn4ID atomic.Value
	txn4PushingC := make(chan struct{}, 1)
	requestFilter := func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
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
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					TestingPostEvalFilter: postEvalFilter,
				},
				TestingRequestFilter: requestFilter,
				// Required by TestCluster.MoveRangeLeaseNonCooperatively.
				AllowLeaseRequestProposalsWhenNotLeader: true,
			},
			Server: &server.TestingKnobs{
				ClockSource: manual.UnixNano,
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
	txn2UnblockC := <-txn2BlockedC

	// Transfer the lease to Server 1. Do so non-cooperatively instead of using
	// a lease transfer, because the cooperative lease transfer would get stuck
	// acquiring latches, which are held by txn2.
	_, err = tc.MoveRangeLeaseNonCooperatively(ctx, rangeDesc, tc.Target(1), manual)
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
		b.AddRawRequest(&roachpb.RangeStatsRequest{
			RequestHeader: roachpb.RequestHeader{Key: key},
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
//  (1) When range is split off for a tenant, that it gets the right tenant ID.
//  (2) When a replica is created with a raft message, it does not have a
//     tenant ID, but then when it is initialized, it gets one.
//  (3) When a store starts up, it assigns the right tenant ID.
func TestTenantID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()
	ctx := context.Background()
	// Create a config with a sticky-in-mem engine so we can restart the server.
	// We also configure the settings to be as robust as possible to problems
	// during stressrace as the setup of the rpc connections seems to somehow
	// fail sometimes when using secure connections.
	raftConfig := base.RaftConfig{
		// Prevent failures under stressrace.
		RangeLeaseRaftElectionTimeoutMultiplier: 10000,
	}
	stickySpecTestServerArgs := base.TestServerArgs{
		RaftConfig: raftConfig,
		Insecure:   true,
		StoreSpecs: []base.StoreSpec{
			{
				InMemory:               true,
				StickyInMemoryEngineID: "1",
			},
		},
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				StickyEngineRegistry: stickyEngineRegistry,
			},
		},
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      stickySpecTestServerArgs,
	})
	defer tc.Stopper().Stop(ctx)

	tenant2 := roachpb.MakeTenantID(2)
	tenant2Prefix := keys.MakeTenantPrefix(tenant2)
	t.Run("(1) initial set", func(t *testing.T) {
		// Ensure that a normal range has the system tenant.
		{
			_, repl := getFirstStoreReplica(t, tc.Server(0), keys.UserTableDataMin)
			ri := repl.State(ctx)
			require.Equal(t, roachpb.SystemTenantID.ToUint64(), ri.TenantID, "%v", repl)
		}
		// Ensure that a range with a tenant prefix has the proper tenant ID.
		tc.SplitRangeOrFatal(t, tenant2Prefix)
		{
			_, repl := getFirstStoreReplica(t, tc.Server(0), tenant2Prefix)
			ri := repl.State(ctx)
			require.Equal(t, tenant2.ToUint64(), ri.TenantID, "%v", repl)
		}
	})
	t.Run("(2) not set before snapshot", func(t *testing.T) {
		_, repl := getFirstStoreReplica(t, tc.Server(0), tenant2Prefix)
		sawSnapshot := make(chan struct{}, 1)
		blockSnapshot := make(chan struct{})
		tc.AddAndStartServer(t, base.TestServerArgs{
			RaftConfig: raftConfig,
			Insecure:   true,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					BeforeSnapshotSSTIngestion: func(
						snapshot kvserver.IncomingSnapshot,
						request_type kvserver.SnapshotRequest_Type,
						strings []string,
					) error {
						if snapshot.State.Desc.RangeID == repl.RangeID {
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
			_, err := tc.AddVoters(tenant2Prefix, tc.Target(1))
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
		ri := uninitializedRepl.State(ctx)
		require.Equal(t, uint64(0), ri.TenantID)
		close(blockSnapshot)
		require.NoError(t, <-addReplicaErr)
		ri = uninitializedRepl.State(ctx) // now initialized
		require.Equal(t, tenant2.ToUint64(), ri.TenantID)
	})
	t.Run("(3) upon restart", func(t *testing.T) {
		tc.StopServer(0)
		tc.AddAndStartServer(t, stickySpecTestServerArgs)
		_, repl := getFirstStoreReplica(t, tc.Server(2), tenant2Prefix)
		ri := repl.State(ctx)
		require.Equal(t, tenant2.ToUint64(), ri.TenantID, "%v", repl)
	})

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
	startV := roachpb.Version{Major: 41}
	endV := roachpb.Version{Major: 42}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: cluster.MakeTestingClusterSettingsWithVersions(endV, startV, false),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride:          startV,
					DisableAutomaticVersionUpgrade: 1,
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

		sl := stateloader.Make(rangeID)
		persistedV, err := sl.LoadVersion(ctx, store.Engine())
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
	if _, pErr := kv.SendWrappedWith(ctx, kvDB.GetFactory().NonTransactionalSender(), roachpb.Header{RangeID: desc.RangeID}, req); pErr != nil {
		t.Fatal(pErr)
	}

	if !migrated {
		t.Fatalf("expected migration interceptor to have been called")
	}
	assertVersion(endV)
}

func TestRaftSchedulerPrioritizesNodeLiveness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// Determine the node liveness range ID.
	livenessRepl := store.LookupReplica(roachpb.RKey(keys.NodeLivenessPrefix))
	livenessRangeID := livenessRepl.RangeID

	// Assert that the node liveness range is prioritized.
	priorityID := store.RaftSchedulerPriorityID()
	require.Equal(t, livenessRangeID, priorityID)
}

func setupDBAndWriteAAndB(t *testing.T) (serverutils.TestServerInterface, *kv.DB) {
	ctx := context.Background()
	args := base.TestServerArgs{}
	s, _, db := serverutils.StartServer(t, args)

	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		defer func() {
			t.Log(err)
		}()
		if err := txn.Put(ctx, "a", "a"); err != nil {
			return err
		}
		if err := txn.Put(ctx, "b", "b"); err != nil {
			return err
		}
		return txn.Commit(ctx)
	}))
	tup, err := db.Get(ctx, "a")
	require.NoError(t, err)
	require.NotNil(t, tup.Value)
	tup, err = db.Get(ctx, "b")
	require.NoError(t, err)
	require.NotNil(t, tup.Value)
	return s, db
}

// TestOptimisticEvalRetry tests the case where an optimistically evaluated
// scan encounters contention from a concurrent txn holding unreplicated
// exclusive locks, and therefore re-evaluates pessimistically, and eventually
// succeeds once the contending txn commits.
func TestOptimisticEvalRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db := setupDBAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	txn1 := db.NewTxn(ctx, "locking txn")
	_, err := txn1.ScanForUpdate(ctx, "a", "c", 0)
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
	timer := timeutil.NewTimer()
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

	ctx := context.Background()
	s, db := setupDBAndWriteAAndB(t)
	defer s.Stopper().Stop(ctx)

	txn1 := db.NewTxn(ctx, "locking txn")
	_, err := txn1.ScanForUpdate(ctx, "b", "c", 0)
	require.NoError(t, err)

	readDone := make(chan error)
	go func() {
		readDone <- db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			defer func() {
				t.Log(err)
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
	s, db := setupDBAndWriteAAndB(t)
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

// BenchmarkOptimisticEvalForLocks benchmarks optimistic evaluation when the
// potentially conflicting lock is explicitly held for a duration of time.
func BenchmarkOptimisticEvalForLocks(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	args := base.TestServerArgs{}
	s, _, db := serverutils.StartServer(b, args)
	defer s.Stopper().Stop(ctx)

	require.NoError(b, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		defer func() {
			b.Log(err)
		}()
		if err := txn.Put(ctx, "a", "a"); err != nil {
			return err
		}
		if err := txn.Put(ctx, "b", "b"); err != nil {
			return err
		}
		return txn.Commit(ctx)
	}))
	tup, err := db.Get(ctx, "a")
	require.NoError(b, err)
	require.NotNil(b, tup.Value)
	tup, err = db.Get(ctx, "b")
	require.NoError(b, err)
	require.NotNil(b, tup.Value)

	for _, realContention := range []bool{false, true} {
		b.Run(fmt.Sprintf("real-contention=%t", realContention),
			func(b *testing.B) {
				lockStart := "b"
				if realContention {
					lockStart = "a"
				}
				finishWrites := make(chan struct{})
				var writers sync.WaitGroup
				for i := 0; i < 1; i++ {
					writers.Add(1)
					go func() {
						for {
							txn := db.NewTxn(ctx, "locking txn")
							_, err = txn.ScanForUpdate(ctx, lockStart, "c", 0)
							require.NoError(b, err)
							time.Sleep(5 * time.Millisecond)
							// Normally, it would do a write here, but we don't bother.
							require.NoError(b, txn.Commit(ctx))
							select {
							case _, recv := <-finishWrites:
								if !recv {
									writers.Done()
									return
								}
							default:
							}
						}
					}()
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
						_, err = txn.Scan(ctx, "a", "c", 1)
						if err != nil {
							panic(err)
						}
						err = txn.Commit(ctx)
						if err != nil {
							panic(err)
						}
						return err
					})
				}
				b.StopTimer()
				close(finishWrites)
				writers.Wait()
			})
	}
}

// BenchmarkOptimisticEval benchmarks optimistic evaluation with
// - potentially conflicting latches held by 1PC transactions doing writes.
// - potentially conflicting latches or locks held by transactions doing
//   writes.
func BenchmarkOptimisticEval(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	args := base.TestServerArgs{}

	for _, latches := range []bool{false, true} {
		conflictWith := "latches-and-locks"
		if latches {
			conflictWith = "latches"
		}
		b.Run(conflictWith, func(b *testing.B) {
			for _, realContention := range []bool{false, true} {
				b.Run(fmt.Sprintf("real-contention=%t", realContention), func(b *testing.B) {
					for _, numWriters := range []int{1, 4} {
						b.Run(fmt.Sprintf("num-writers=%d", numWriters), func(b *testing.B) {
							// Since we are doing writes in the benchmark, start with a
							// fresh server each time so that we start with a fresh engine
							// without many versions for a key.
							s, _, db := serverutils.StartServer(b, args)
							defer s.Stopper().Stop(ctx)

							require.NoError(b, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
								if err := txn.Put(ctx, "a", "a"); err != nil {
									return err
								}
								if err := txn.Put(ctx, "b", "b"); err != nil {
									return err
								}
								return txn.Commit(ctx)
							}))
							tup, err := db.Get(ctx, "a")
							require.NoError(b, err)
							require.NotNil(b, tup.Value)
							tup, err = db.Get(ctx, "b")
							require.NoError(b, err)
							require.NotNil(b, tup.Value)

							writeKey := "b"
							if realContention {
								writeKey = "a"
							}
							finishWrites := make(chan struct{})
							var writers sync.WaitGroup
							for i := 0; i < numWriters; i++ {
								writers.Add(1)
								go func() {
									for {
										if latches {
											require.NoError(b, db.Put(ctx, writeKey, "foo"))

										} else {
											require.NoError(b, db.Txn(ctx,
												func(ctx context.Context, txn *kv.Txn) (err error) {
													if err := txn.Put(ctx, writeKey, "foo"); err != nil {
														return err
													}
													return txn.Commit(ctx)
												}))
										}
										select {
										case _, recv := <-finishWrites:
											if !recv {
												writers.Done()
												return
											}
										default:
										}
									}
								}()
							}
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								_ = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
									_, err = txn.Scan(ctx, "a", "c", 1)
									if err != nil {
										panic(err)
									}
									err = txn.Commit(ctx)
									if err != nil {
										panic(err)
									}
									return err
								})
							}
							b.StopTimer()
							close(finishWrites)
							writers.Wait()
						})
					}
				})
			}
		})
	}
}
