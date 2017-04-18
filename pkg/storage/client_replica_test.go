// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Ben Darnell

package storage_test

import (
	"bytes"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TestRangeCommandClockUpdate verifies that followers update their
// clocks when executing a command, even if the lease holder's clock is far
// in the future.
func TestRangeCommandClockUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numNodes = 3
	var manuals []*hlc.ManualClock
	var clocks []*hlc.Clock
	for i := 0; i < numNodes; i++ {
		manuals = append(manuals, hlc.NewManualClock(1))
		clocks = append(clocks, hlc.NewClock(manuals[i].UnixNano, 100*time.Millisecond))
	}
	mtc := &multiTestContext{clocks: clocks}
	defer mtc.Stop()
	mtc.Start(t, numNodes)
	mtc.replicateRange(1, 1, 2)

	// Advance the lease holder's clock ahead of the followers (by more than
	// MaxOffset but less than the range lease) and execute a command.
	manuals[0].Increment(int64(500 * time.Millisecond))
	incArgs := incrementArgs([]byte("a"), 5)
	ts := clocks[0].Now()
	if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{Timestamp: ts}, incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for that command to execute on all the followers.
	testutils.SucceedsSoon(t, func() error {
		values := []int64{}
		for _, eng := range mtc.engines {
			val, _, err := engine.MVCCGet(context.Background(), eng, roachpb.Key("a"), clocks[0].Now(), true, nil)
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

// TestRejectFutureCommand verifies that lease holders reject commands that
// would cause a large time jump.
func TestRejectFutureCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, 100*time.Millisecond)
	mtc := &multiTestContext{clock: clock}
	defer mtc.Stop()
	mtc.Start(t, 1)

	ts1 := clock.Now()

	key := roachpb.Key("a")
	incArgs := incrementArgs(key, 5)

	// Commands with a future timestamp that is within the MaxOffset
	// bound will be accepted and will cause the clock to advance.
	const numCmds = 3
	clockOffset := clock.MaxOffset() / numCmds
	for i := int64(1); i <= numCmds; i++ {
		ts := ts1.Add(i*clockOffset.Nanoseconds(), 0)
		if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{Timestamp: ts}, incArgs); err != nil {
			t.Fatal(err)
		}
	}

	ts2 := clock.Now()
	if expAdvance, advance := ts2.GoTime().Sub(ts1.GoTime()), numCmds*clockOffset; advance != expAdvance {
		t.Fatalf("expected clock to advance %s; got %s", expAdvance, advance)
	}

	// Once the accumulated offset reaches MaxOffset, commands will be rejected.
	_, pErr := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{Timestamp: ts1.Add(clock.MaxOffset().Nanoseconds()+1, 0)}, incArgs)
	if !testutils.IsPError(pErr, "rejecting command with timestamp in the future") {
		t.Fatalf("unexpected error %v", pErr)
	}

	// The clock did not advance and the final command was not executed.
	ts3 := clock.Now()
	if advance := ts3.GoTime().Sub(ts2.GoTime()); advance != 0 {
		t.Fatalf("expected clock not to advance, but it advanced by %s", advance)
	}
	val, _, err := engine.MVCCGet(context.Background(), mtc.engines[0], key, ts3, true, nil)
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
// 1) The Writer executes a put operation and writes a write intent with
//    time T in a txn.
// 2) Before the Writer's txn is committed, the Reader sends a high priority
//    get operation with time T+100. This pushes the Writer txn timestamp to
//    T+100 and triggers the restart of the Writer's txn. The original
//    write intent timestamp is also updated to T+100.
// 3) The Writer starts a new epoch of the txn, but before it writes, the
//    Reader sends another high priority get operation with time T+200. This
//    pushes the Writer txn timestamp to T+200 to trigger a restart of the
//    Writer txn. The Writer will not actually restart until it tries to commit
//    the current epoch of the transaction. The Reader updates the timestamp of
//    the write intent to T+200. The test deliberately fails the Reader get
//    operation, and cockroach doesn't update its read timestamp cache.
// 4) The Writer executes the put operation again. This put operation comes
//    out-of-order since its timestamp is T+100, while the intent timestamp
//    updated at Step 3 is T+200.
// 5) The put operation overrides the old value using timestamp T+100.
// 6) When the Writer attempts to commit its txn, the txn will be restarted
//    again at a new epoch timestamp T+200, which will finally succeed.
func TestTxnPutOutOfOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// key is selected to fall within the meta range in order for the later
	// routing of requests to range 1 to work properly. Removing the routing
	// of all requests to range 1 would allow us to make the key more normal.
	const key = "key"
	// Set up a filter to so that the get operation at Step 3 will return an error.
	var numGets int32

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	manual := hlc.NewManualClock(123)
	cfg := storage.TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	// Splits can cause our chosen key to end up on a range other than range 1,
	// and trying to handle that complicates the test without providing any
	// added benefit.
	cfg.TestingKnobs.DisableSplitQueue = true
	cfg.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
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
	eng := engine.NewInMem(roachpb.Attributes{}, 10<<20)
	stopper.AddCloser(eng)
	store := createTestStoreWithEngine(t,
		eng,
		true,
		cfg,
		stopper,
	)

	// Put an initial value.
	initVal := []byte("initVal")
	err := store.DB().Put(context.TODO(), key, initVal)
	if err != nil {
		t.Fatalf("failed to put: %s", err)
	}

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
		errChan <- store.DB().Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			epoch++

			if epoch == 1 {
				// Wait until the second get operation is issued.
				close(waitTxnRestart)
				<-waitSecondGet
			}

			updatedVal := []byte("updatedVal")
			if err := txn.Put(ctx, key, updatedVal); err != nil {
				return err
			}

			// Make sure a get will return the value that was just written.
			actual, err := txn.Get(ctx, key)
			if err != nil {
				return err
			}
			if !bytes.Equal(actual.ValueBytes(), updatedVal) {
				t.Fatalf("unexpected get result: %s", actual)
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
			errChan <- errors.Errorf("unexpected number of txn retries: %d", epoch)
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
	requestHeader := roachpb.Span{
		Key: roachpb.Key(key),
	}
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Timestamp:    cfg.Clock.Now(),
		UserPriority: priority,
	}, &roachpb.GetRequest{Span: requestHeader}); err != nil {
		t.Fatalf("failed to get: %s", err)
	}

	// Wait until the writer restarts the txn.
	close(waitFirstGet)
	<-waitTxnRestart

	// Advance the clock and send a get operation again. This time
	// we use TestingCommandFilter so that a get operation is not
	// processed after the write intent is resolved (to prevent the
	// timestamp cache from being updated).
	manual.Increment(100)

	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Timestamp:    cfg.Clock.Now(),
		UserPriority: priority,
	}, &roachpb.GetRequest{Span: requestHeader}); err == nil {
		t.Fatal("unexpected success of get")
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
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Init test ranges:
	// ["","a"), ["a","c"), ["c","e"), ["e","g") and ["g","\xff\xff").
	splits := []*roachpb.AdminSplitRequest{
		adminSplitArgs(roachpb.Key("g"), roachpb.Key("g")),
		adminSplitArgs(roachpb.Key("e"), roachpb.Key("e")),
		adminSplitArgs(roachpb.Key("c"), roachpb.Key("c")),
		adminSplitArgs(roachpb.Key("a"), roachpb.Key("a")),
	}

	for _, split := range splits {
		_, pErr := client.SendWrapped(context.Background(), rg1(store), split)
		if pErr != nil {
			t.Fatalf("%q: split unexpected error: %s", split.SplitKey, pErr)
		}
	}

	// Resolve the intents.
	scanArgs := roachpb.ScanRequest{
		Span: roachpb.Span{
			Key:    keys.RangeMetaKey(roachpb.RKeyMin.Next()),
			EndKey: keys.RangeMetaKey(roachpb.RKeyMax),
		},
	}
	testutils.SucceedsSoon(t, func() error {
		_, pErr := client.SendWrapped(context.Background(), rg1(store), &scanArgs)
		return pErr.GoError()
	})

	revScanArgs := func(key []byte, maxResults int32) *roachpb.RangeLookupRequest {
		return &roachpb.RangeLookupRequest{
			Span: roachpb.Span{
				Key: key,
			},
			MaxRanges: maxResults,
			Reverse:   true,
		}

	}

	// Test cases.
	testCases := []struct {
		request     *roachpb.RangeLookupRequest
		expected    []roachpb.RangeDescriptor
		expectedPre []roachpb.RangeDescriptor
	}{
		// Test key in the middle of the range.
		{
			request: revScanArgs(keys.RangeMetaKey(roachpb.RKey("f")), 2),
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
			request: revScanArgs(keys.RangeMetaKey(roachpb.RKey("g")), 3),
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
			request: revScanArgs(keys.RangeMetaKey(roachpb.RKey("e")), 2),
			// ["c","e") and ["a","c").
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("e")},
			},
			expectedPre: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
			},
		},
		// Test Meta2KeyMax.
		{
			request: revScanArgs(keys.Meta2KeyMax, 2),
			// ["e","g") and ["g","\xff\xff")
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("g"), EndKey: roachpb.RKey("\xff\xff")},
			},
			expectedPre: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKey("e"), EndKey: roachpb.RKey("g")},
			},
		},
		// Test Meta1KeyMax.
		{
			request: revScanArgs(keys.Meta1KeyMax, 1),
			// ["","a")
			expected: []roachpb.RangeDescriptor{
				{StartKey: roachpb.RKeyMin, EndKey: roachpb.RKey("a")},
			},
		},
	}

	for testIdx, test := range testCases {
		resp, pErr := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, test.request)
		if pErr != nil {
			t.Fatalf("%d: RangeLookup error: %s", testIdx, pErr)
		}

		rlReply := resp.(*roachpb.RangeLookupResponse)
		// Checks the results count.
		if rsCount, preRSCount := len(rlReply.Ranges), len(rlReply.PrefetchedRanges); int32(rsCount+preRSCount) != test.request.MaxRanges {
			t.Fatalf("%d: returned results count, expected %d, but got %d+%d", testIdx, test.request.MaxRanges, rsCount, preRSCount)
		}
		// Checks the range descriptors.
		for _, rngSlice := range []struct {
			expect, reply []roachpb.RangeDescriptor
		}{
			{test.expected, rlReply.Ranges},
			{test.expectedPre, rlReply.PrefetchedRanges},
		} {
			for i, rng := range rngSlice.expect {
				if !(rng.StartKey.Equal(rngSlice.reply[i].StartKey) && rng.EndKey.Equal(rngSlice.reply[i].EndKey)) {
					t.Fatalf("%d: returned range is not correct, expected %v, but got %v", testIdx, rng, rngSlice.reply[i])
				}
			}
		}
	}
}

func TestRangeTransferLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cfg := storage.TestStoreConfig(nil)
	var filterMu syncutil.Mutex
	var filter func(filterArgs storagebase.FilterArgs) *roachpb.Error
	cfg.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			filterMu.Lock()
			filterCopy := filter
			filterMu.Unlock()
			if filterCopy != nil {
				return filterCopy(filterArgs)
			}
			return nil
		}
	var waitForTransferBlocked atomic.Value
	waitForTransferBlocked.Store(false)
	transferBlocked := make(chan struct{})
	cfg.TestingKnobs.LeaseTransferBlockedOnExtensionEvent = func(
		_ roachpb.ReplicaDescriptor) {
		if waitForTransferBlocked.Load().(bool) {
			transferBlocked <- struct{}{}
			waitForTransferBlocked.Store(false)
		}
	}
	mtc := &multiTestContext{}
	mtc.storeConfig = &cfg
	defer mtc.Stop()
	mtc.Start(t, 2)
	mtc.initGossipNetwork()

	// First, do a write; we'll use it to determine when the dust has settled.
	leftKey := roachpb.Key("a")
	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the left range's ID.
	rangeID := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil).RangeID

	// Replicate the left range onto node 1.
	mtc.replicateRange(rangeID, 1)

	replica0 := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil)
	replica1 := mtc.stores[1].LookupReplica(roachpb.RKey("a"), nil)
	replica0Desc, err := replica0.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	replica1Desc, err := replica1.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	sendRead := func(sender *storage.Stores) *roachpb.Error {
		_, pErr := client.SendWrappedWith(
			context.Background(),
			sender,
			roachpb.Header{Replica: replica0Desc},
			getArgs(leftKey),
		)
		return pErr
	}

	// Check that replica0 can serve reads OK.
	if pErr := sendRead(mtc.senders[0]); pErr != nil {
		t.Fatal(pErr)
	}

	// checkHasLease checks that a lease for the left range is owned by a
	// replica. The check is executed in a retry loop because the lease may not
	// have been applied yet.
	checkHasLease := func(t *testing.T, sender *storage.Stores) {
		testutils.SucceedsSoon(t, func() error {
			return sendRead(sender).GoError()
		})
	}

	// setFilter is a helper function to enable/disable the blocking of
	// RequestLeaseRequests on replica1. This function will notify that an
	// extension is blocked on the passed in channel and will wait on the same
	// channel to unblock the extension. Note that once an extension is blocked,
	// the filter is cleared.
	setFilter := func(setTo bool, extensionSem chan struct{}) {
		filterMu.Lock()
		defer filterMu.Unlock()
		if !setTo {
			filter = nil
			return
		}
		filter = func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Sid != mtc.stores[1].Ident.StoreID {
				return nil
			}
			llReq, ok := filterArgs.Req.(*roachpb.RequestLeaseRequest)
			if !ok {
				return nil
			}
			if llReq.Lease.Replica == replica1Desc {
				// Notify the main thread that the extension is in progress and wait for
				// the signal to proceed.
				filterMu.Lock()
				filter = nil
				filterMu.Unlock()
				extensionSem <- struct{}{}
				<-extensionSem
			}
			return nil
		}
	}

	forceLeaseExtension := func(sender *storage.Stores, lease *roachpb.Lease) error {
		shouldRenewTS := lease.Expiration.Add(-1, 0)
		mtc.manualClock.Set(shouldRenewTS.WallTime + 1)
		return sendRead(sender).GoError()
	}
	t.Run("Transfer", func(t *testing.T) {
		origLease, _ := replica0.GetLease()
		{
			// Transferring the lease to ourself should be a no-op.
			if err := replica0.AdminTransferLease(context.Background(), replica0Desc.StoreID); err != nil {
				t.Fatal(err)
			}
			newLease, _ := replica0.GetLease()
			if err := origLease.Equivalent(*newLease); err != nil {
				t.Fatal(err)
			}
		}

		{
			// An invalid target should result in an error.
			const expected = "unable to find store .* in range"
			if err := replica0.AdminTransferLease(context.Background(), 1000); !testutils.IsError(err, expected) {
				t.Fatalf("expected %s, but found %v", expected, err)
			}
		}

		if err := replica0.AdminTransferLease(context.Background(), replica1Desc.StoreID); err != nil {
			t.Fatal(err)
		}

		// Check that replica0 doesn't serve reads any more.
		pErr := sendRead(mtc.senders[0])
		nlhe, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
		if !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, pErr)
		}
		if *(nlhe.LeaseHolder) != replica1Desc {
			t.Fatalf("expected lease holder %+v, got %+v",
				replica1Desc, nlhe.LeaseHolder)
		}

		// Check that replica1 now has the lease.
		checkHasLease(t, mtc.senders[1])

		replica1Lease, _ := replica1.GetLease()

		// Verify the timestamp cache low water. Because we executed a transfer lease
		// request, the low water should be set to the new lease start time which is
		// less than the previous lease's expiration time.
		if lowWater := replica1.GetTimestampCacheLowWater(); lowWater != replica1Lease.Start {
			t.Fatalf("expected timestamp cache low water %s, but found %s",
				replica1Lease.Start, lowWater)
		}
	})

	// Make replica1 extend its lease and transfer the lease immediately after
	// that. Test that the transfer still happens (it'll wait until the extension
	// is done).
	t.Run("TransferWithExtension", func(t *testing.T) {
		extensionSem := make(chan struct{})
		setFilter(true, extensionSem)

		// Initiate an extension.
		renewalErrCh := make(chan error)
		go func() {
			lease, _ := replica1.GetLease()
			renewalErrCh <- forceLeaseExtension(mtc.senders[1], lease)
		}()

		// Wait for extension to be blocked.
		<-extensionSem
		waitForTransferBlocked.Store(true)
		// Initiate a transfer.
		transferErrCh := make(chan error)
		go func() {
			// Transfer back from replica1 to replica0.
			transferErrCh <- replica1.AdminTransferLease(context.Background(), replica0Desc.StoreID)
		}()
		// Wait for the transfer to be blocked by the extension.
		<-transferBlocked
		// Now unblock the extension.
		extensionSem <- struct{}{}
		checkHasLease(t, mtc.senders[0])
		setFilter(false, nil)

		// We can sometimes receive an error from our renewal attempt
		// because the lease transfer ends up causing the renewal to
		// re-propose and second attempt fails because it's already been
		// renewed. This used to work before we compared the proposer's lease
		// with the actual lease because the renewed lease still encompassed the
		// previous request.
		if err := <-renewalErrCh; err != nil {
			if _, ok := err.(*roachpb.NotLeaseHolderError); !ok {
				t.Errorf("expected not lease holder error due to re-proposal; got %s", err)
			}
		}
		if err := <-transferErrCh; err != nil {
			t.Errorf("unexpected error from lease transfer: %s", err)
		}
	})

	// We have to ensure that replica0 is the raft leader and that replica1 has
	// caught up to replica0 as draining code doesn't transfer leases to
	// behind replicas.
	testutils.SucceedsSoon(t, func() error {
		r := mtc.getRaftLeader(rangeID)
		if r == nil {
			return errors.Errorf("could not find raft leader replica for range %d", rangeID)
		}
		desc, err := r.GetReplicaDescriptor()
		if err != nil {
			return errors.Wrap(err, "could not get replica descriptor")
		}
		if desc != replica0Desc {
			return errors.Errorf("expected replica with id %v to be raft leader, instead got id %v", replica0Desc.ReplicaID, desc.ReplicaID)
		}
		return nil
	})

	testutils.SucceedsSoon(t, func() error {
		status := replica0.RaftStatus()
		progress, ok := status.Progress[uint64(replica1Desc.ReplicaID)]
		if !ok {
			return errors.Errorf("replica1 progress not found in progress map: %v", status.Progress)
		}
		if progress.Match < status.Commit {
			return errors.New("replica1 failed to catch up")
		}
		return nil
	})

	// DrainTransfer verifies that a draining store attempts to transfer away
	// range leases owned by its replicas.
	t.Run("DrainTransfer", func(t *testing.T) {
		mtc.stores[0].SetDraining(true)

		// Check that replica0 doesn't serve reads any more.
		pErr := sendRead(mtc.senders[0])
		nlhe, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
		if !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, pErr)
		}
		if *(nlhe.LeaseHolder) != replica1Desc {
			t.Fatalf("expected lease holder %+v, got %+v",
				replica1Desc, nlhe.LeaseHolder)
		}

		// Check that replica1 now has the lease.
		checkHasLease(t, mtc.senders[1])

		mtc.stores[0].SetDraining(false)
	})

	// DrainTransferWithExtension verifies that a draining store waits for any
	// in-progress lease requests to complete before transferring away the new
	// lease.
	t.Run("DrainTransferWithExtension", func(t *testing.T) {
		extensionSem := make(chan struct{})
		setFilter(true, extensionSem)

		// Initiate an extension.
		renewalErrCh := make(chan error)
		go func() {
			lease, _ := replica1.GetLease()
			renewalErrCh <- forceLeaseExtension(mtc.senders[1], lease)
		}()

		// Wait for extension to be blocked.
		<-extensionSem
		// Drain node 1 with an extension in progress.
		go func() {
			mtc.stores[1].SetDraining(true)
		}()
		// Now unblock the extension.
		extensionSem <- struct{}{}

		checkHasLease(t, mtc.senders[0])
		setFilter(false, nil)

		// We can sometimes receive an error from our renewal attempt
		// because the lease transfer ends up causing the renewal to
		// re-propose and second attempt fails because it's already been
		// renewed. This used to work before we compared the proposer's lease
		// with the actual lease because the renewed lease still encompassed the
		// previous request.
		if err := <-renewalErrCh; err != nil {
			if _, ok := err.(*roachpb.NotLeaseHolderError); !ok {
				t.Errorf("expected not lease holder error due to re-proposal; got %s", err)
			}
		}
	})
}

// TestLeaseMetricsOnSplitAndTransfer verifies that lease-related metrics
// are updated after splitting a range and then initiating one successful
// and one failing lease transfer.
func TestLeaseMetricsOnSplitAndTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var injectLeaseTransferError atomic.Value
	sc := storage.TestStoreConfig(nil)
	sc.TestingKnobs.DisableSplitQueue = true
	sc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
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
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// Up-replicate to two replicas.
	keyMinReplica0 := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	mtc.replicateRange(keyMinReplica0.RangeID, 1)

	// Split the key space at key "a".
	splitKey := roachpb.RKey("a")
	splitArgs := adminSplitArgs(splitKey.AsRawKey(), splitKey.AsRawKey())
	if _, pErr := client.SendWrapped(
		context.Background(), rg1(mtc.stores[0]), splitArgs,
	); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, a successful transfer from LHS replica 0 to replica 1.
	injectLeaseTransferError.Store(false)
	if err := mtc.dbs[0].AdminTransferLease(
		context.TODO(), keyMinReplica0.Desc().StartKey.AsRawKey(), mtc.stores[1].StoreID(),
	); err != nil {
		t.Fatalf("unable to transfer lease to replica 1: %s", err)
	}
	// Wait for all replicas to process.
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < 2; i++ {
			r := mtc.stores[i].LookupReplica(roachpb.RKeyMin, nil)
			if l, _ := r.GetLease(); l.Replica.StoreID != mtc.stores[1].StoreID() {
				return errors.Errorf("expected lease to transfer to replica 2: got %s", l)
			}
		}
		return nil
	})

	// Next a failed transfer from RHS replica 0 to replica 1.
	injectLeaseTransferError.Store(true)
	keyAReplica0 := mtc.stores[0].LookupReplica(splitKey, nil)
	if err := mtc.dbs[0].AdminTransferLease(
		context.TODO(), keyAReplica0.Desc().StartKey.AsRawKey(), mtc.stores[1].StoreID(),
	); err == nil {
		t.Fatal("expected an error transferring to an unknown store ID")
	}

	metrics := mtc.stores[0].Metrics()
	if a, e := metrics.LeaseTransferSuccessCount.Count(), int64(1); a != e {
		t.Errorf("expected %d lease transfer successes; got %d", e, a)
	}
	if a, e := metrics.LeaseTransferErrorCount.Count(), int64(1); a != e {
		t.Errorf("expected %d lease transfer errors; got %d", e, a)
	}

	// Expire current leases and put a key to RHS of split to request
	// an epoch-based lease.
	testutils.SucceedsSoon(t, func() error {
		mtc.advanceClock(context.TODO())
		if err := mtc.stores[0].DB().Put(context.TODO(), "a", "foo"); err != nil {
			return err
		}

		// Update replication gauges on store 1 and verify we have 1 each of
		// expiration and epoch leases. These values are counted from store 1
		// because it will have the higher replica IDs. Expire leases to make
		// sure that epoch-based leases are used for the split range.
		if err := mtc.stores[1].ComputeMetrics(context.Background(), 0); err != nil {
			return err
		}
		metrics = mtc.stores[1].Metrics()
		if a, e := metrics.LeaseExpirationCount.Value(), int64(1); a != e {
			return errors.Errorf("expected %d expiration lease count; got %d", e, a)
		}
		if a, e := metrics.LeaseEpochCount.Value(), int64(1); a != e {
			return errors.Errorf("expected %d epoch lease count; got %d", e, a)
		}
		return nil
	})
}

// Test that leases held before a restart are not used after the restart.
// See replica.mu.minLeaseProposedTS for the reasons why this isn't allowed.
func TestLeaseNotUsedAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	var leaseAcquisitionTrap atomic.Value
	// Disable the split queue so that no ranges are split. This makes it easy
	// below to trap any lease request and infer that it refers to the range we're
	// interested in.
	sc.TestingKnobs.DisableSplitQueue = true
	sc.TestingKnobs.LeaseRequestEvent = func(ts hlc.Timestamp) {
		val := leaseAcquisitionTrap.Load()
		if val == nil {
			return
		}
		trapCallback := val.(func(ts hlc.Timestamp))
		if trapCallback != nil {
			trapCallback(ts)
		}
	}
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 1)

	// Send a read, to acquire a lease.
	getArgs := getArgs([]byte("a"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), getArgs); err != nil {
		t.Fatal(err)
	}

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
	mtc.restart()

	// Send another read and check that the pre-existing lease has not been used.
	// Concretely, we check that a new lease is requested.
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), getArgs); err != nil {
		t.Fatal(err)
	}
	// Check that the Send above triggered a lease acquisition.
	select {
	case <-leaseAcquisitionCh:
	case <-time.After(time.Second):
		t.Fatalf("read did not acquire a new lease")
	}
}

// Test that a lease extension (a RequestLeaseRequest that doesn't change the
// lease holder) is not blocked by ongoing reads.
// The test relies on two things:
// 1) Lease extensions, unlike lease transfers, are not blocked by reads through their
// PostCommitTrigger.noConcurrentReads.
// 2) Requests with the non-KV flag, such as RequestLeaseRequest, do not
// go through the command queue.
func TestLeaseExtensionNotBlockedByRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	readBlocked := make(chan struct{})
	cmdFilter := func(fArgs storagebase.FilterArgs) *roachpb.Error {
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
				Store: &storage.StoreTestingKnobs{
					TestingEvalFilter: cmdFilter,
				},
			},
		})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop(context.TODO())

	// Start a read and wait for it to block.
	key := roachpb.Key("a")
	errChan := make(chan error)
	go func() {
		getReq := roachpb.GetRequest{
			Span: roachpb.Span{
				Key: key,
			},
		}
		if _, pErr := client.SendWrappedWith(context.Background(), s.DistSender(),
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
		_, repDesc, err := s.Stores().LookupReplica(rKey, nil)
		if err != nil {
			t.Fatal(err)
		}
		leaseReq := roachpb.RequestLeaseRequest{
			Span: roachpb.Span{
				Key: key,
			},
			Lease: roachpb.Lease{
				Start:      s.Clock().Now(),
				Expiration: s.Clock().Now().Add(time.Second.Nanoseconds(), 0),
				Replica:    repDesc,
			},
		}
		if _, pErr := client.SendWrapped(context.Background(), s.DistSender(), &leaseReq); pErr != nil {
			t.Fatal(pErr)
		}
		// Unblock the read.
		readBlocked <- struct{}{}
	}
}

// LeaseInfo runs a LeaseInfoRequest using the specified server.
func LeaseInfo(
	t *testing.T,
	db *client.DB,
	rangeDesc roachpb.RangeDescriptor,
	readConsistency roachpb.ReadConsistencyType,
) roachpb.LeaseInfoResponse {
	leaseInfoReq := &roachpb.LeaseInfoRequest{
		Span: roachpb.Span{
			Key: rangeDesc.StartKey.AsRawKey(),
		},
	}
	reply, pErr := client.SendWrappedWith(context.Background(), db.GetSender(), roachpb.Header{
		ReadConsistency: readConsistency,
	}, leaseInfoReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	return *(reply.(*roachpb.LeaseInfoResponse))
}

func TestLeaseInfoRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("#13503")
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.TODO())

	kvDB0 := tc.Servers[0].DB()
	kvDB1 := tc.Servers[1].DB()

	key := []byte("a")
	rangeDesc, err := tc.LookupRange(key)
	if err != nil {
		t.Fatal(err)
	}

	rangeDesc, err = tc.AddReplicas(
		rangeDesc.StartKey.AsRawKey(), tc.Target(1), tc.Target(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rangeDesc.Replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %+v", rangeDesc.Replicas)
	}
	replicas := make([]roachpb.ReplicaDescriptor, 3)
	for i := 0; i < 3; i++ {
		var ok bool
		replicas[i], ok = rangeDesc.GetReplicaDescriptor(tc.Servers[i].GetFirstStoreID())
		if !ok {
			t.Fatalf("expected to find replica in server %d", i)
		}
	}

	// Lease should start on Server 0, since nobody told it to move.
	leaseHolderReplica := LeaseInfo(t, kvDB0, rangeDesc, roachpb.INCONSISTENT).Lease.Replica
	if leaseHolderReplica != replicas[0] {
		t.Fatalf("lease holder should be replica %+v, but is: %+v", replicas[0], leaseHolderReplica)
	}

	// Transfer the lease to Server 1 and check that LeaseInfoRequest gets the
	// right answer.
	err = tc.TransferRangeLease(rangeDesc, tc.Target(1))
	if err != nil {
		t.Fatal(err)
	}
	// An inconsistent LeaseInfoReqeust on the old lease holder should give us the
	// right answer immediately, since the old holder has definitely applied the
	// transfer before TransferRangeLease returned.
	leaseHolderReplica = LeaseInfo(t, kvDB0, rangeDesc, roachpb.INCONSISTENT).Lease.Replica
	if leaseHolderReplica != replicas[1] {
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
		leaseHolderReplica = LeaseInfo(t, kvDB1, rangeDesc, roachpb.INCONSISTENT).Lease.Replica
		if leaseHolderReplica != replicas[1] {
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
	leaseHolderReplica = LeaseInfo(t, kvDB1, rangeDesc, roachpb.INCONSISTENT).Lease.Replica
	if leaseHolderReplica != replicas[2] {
		t.Fatalf("lease holder should be replica %+v, but is: %+v", replicas[2], leaseHolderReplica)
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
	cmdFilter := func(fArgs storagebase.FilterArgs) *roachpb.Error {
		if fArgs.Hdr.UserPriority == 42 {
			return roachpb.NewErrorf("injected error")
		}
		return nil
	}
	srv, _, _ := serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storage.StoreTestingKnobs{
					TestingEvalFilter: cmdFilter,
				},
			},
		})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop(context.TODO())

	// Send the lease request.
	key := roachpb.Key("a")
	leaseReq := roachpb.LeaseInfoRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
	_, pErr := client.SendWrappedWith(
		context.Background(),
		s.DistSender(),
		roachpb.Header{UserPriority: 42},
		&leaseReq,
	)
	if !testutils.IsPError(pErr, "injected error") {
		t.Fatalf("expected error %q, got: %s", "injected error", pErr)
	}
}

func TestRangeInfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 2)

	// Up-replicate to two replicas.
	mtc.replicateRange(mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil).RangeID, 1)

	// Split the key space at key "a".
	splitKey := roachpb.RKey("a")
	splitArgs := adminSplitArgs(splitKey.AsRawKey(), splitKey.AsRawKey())
	if _, pErr := client.SendWrapped(
		context.Background(), rg1(mtc.stores[0]), splitArgs,
	); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the replicas for each side of the split. This is done within
	// a SucceedsSoon loop to ensure the split completes.
	var lhsReplica0, lhsReplica1, rhsReplica0, rhsReplica1 *storage.Replica
	testutils.SucceedsSoon(t, func() error {
		lhsReplica0 = mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
		lhsReplica1 = mtc.stores[1].LookupReplica(roachpb.RKeyMin, nil)
		rhsReplica0 = mtc.stores[0].LookupReplica(splitKey, nil)
		rhsReplica1 = mtc.stores[1].LookupReplica(splitKey, nil)
		if lhsReplica0 == rhsReplica0 || lhsReplica1 == rhsReplica1 {
			return errors.Errorf("replicas not post-split %v, %v, %v, %v",
				lhsReplica0, rhsReplica0, rhsReplica0, rhsReplica1)
		}
		return nil
	})
	lhsLease, _ := lhsReplica0.GetLease()
	rhsLease, _ := rhsReplica0.GetLease()

	// Verify range info is not set if unrequested.
	getArgs := getArgs(splitKey.AsRawKey())
	reply, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], getArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if len(reply.Header().RangeInfos) > 0 {
		t.Errorf("expected empty range infos if unrequested; got %v", reply.Header().RangeInfos)
	}

	// Verify range info on a get request.
	h := roachpb.Header{
		ReturnRangeInfo: true,
	}
	reply, pErr = client.SendWrappedWith(context.Background(), mtc.distSenders[0], h, getArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	expRangeInfos := []roachpb.RangeInfo{
		{
			Desc:  *rhsReplica0.Desc(),
			Lease: *rhsLease,
		},
	}
	if !reflect.DeepEqual(reply.Header().RangeInfos, expRangeInfos) {
		t.Errorf("on get reply, expected %+v; got %+v", expRangeInfos, reply.Header().RangeInfos)
	}

	// Verify range info on a put request.
	putArgs := putArgs(splitKey.AsRawKey(), []byte("foo"))
	reply, pErr = client.SendWrappedWith(context.Background(), mtc.distSenders[0], h, putArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !reflect.DeepEqual(reply.Header().RangeInfos, expRangeInfos) {
		t.Errorf("on put reply, expected %+v; got %+v", expRangeInfos, reply.Header().RangeInfos)
	}

	// Verify multiple range infos on a scan request.
	scanArgs := roachpb.ScanRequest{
		Span: roachpb.Span{
			Key:    keys.SystemMax,
			EndKey: roachpb.KeyMax,
		},
	}
	h.Txn = roachpb.NewTransaction("test", roachpb.KeyMin, 1, enginepb.SERIALIZABLE, mtc.clock.Now(), 0)
	reply, pErr = client.SendWrappedWith(context.Background(), mtc.distSenders[0], h, &scanArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	expRangeInfos = []roachpb.RangeInfo{
		{
			Desc:  *lhsReplica0.Desc(),
			Lease: *lhsLease,
		},
		{
			Desc:  *rhsReplica0.Desc(),
			Lease: *rhsLease,
		},
	}
	if !reflect.DeepEqual(reply.Header().RangeInfos, expRangeInfos) {
		t.Errorf("on scan reply, expected %+v; got %+v", expRangeInfos, reply.Header().RangeInfos)
	}

	// Verify multiple range infos and order on a reverse scan request.
	revScanArgs := roachpb.ReverseScanRequest{
		Span: roachpb.Span{
			Key:    keys.SystemMax,
			EndKey: roachpb.KeyMax,
		},
	}
	reply, pErr = client.SendWrappedWith(context.Background(), mtc.distSenders[0], h, &revScanArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	expRangeInfos = []roachpb.RangeInfo{
		{
			Desc:  *rhsReplica0.Desc(),
			Lease: *rhsLease,
		},
		{
			Desc:  *lhsReplica0.Desc(),
			Lease: *lhsLease,
		},
	}
	if !reflect.DeepEqual(reply.Header().RangeInfos, expRangeInfos) {
		t.Errorf("on reverse scan reply, expected %+v; got %+v", expRangeInfos, reply.Header().RangeInfos)
	}

	// Change lease holders for both ranges and re-scan.
	for _, r := range []*storage.Replica{lhsReplica1, rhsReplica1} {
		replDesc, err := r.GetReplicaDescriptor()
		if err != nil {
			t.Fatal(err)
		}
		if err = mtc.dbs[0].AdminTransferLease(context.TODO(),
			r.Desc().StartKey.AsRawKey(), replDesc.StoreID); err != nil {
			t.Fatalf("unable to transfer lease to replica %s: %s", r, err)
		}
	}
	reply, pErr = client.SendWrappedWith(context.Background(), mtc.distSenders[0], h, &scanArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	lhsLease, _ = lhsReplica1.GetLease()
	rhsLease, _ = rhsReplica1.GetLease()
	expRangeInfos = []roachpb.RangeInfo{
		{
			Desc:  *lhsReplica1.Desc(),
			Lease: *lhsLease,
		},
		{
			Desc:  *rhsReplica1.Desc(),
			Lease: *rhsLease,
		},
	}
	if !reflect.DeepEqual(reply.Header().RangeInfos, expRangeInfos) {
		t.Errorf("on scan reply, expected %+v; got %+v", expRangeInfos, reply.Header().RangeInfos)
	}
}

// TestCampaignOnLazyRaftGroupInitialization verifies expected
// behavior for which replicas will campaign on lazy initialization.
func TestCampaignOnLazyRaftGroupInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("this test is flaky on the 5m initial stress due to errant Raft messages initializing the Raft group")
	splitKey := keys.MakeRowSentinelKey(keys.UserTableDataMin)
	testState := struct {
		syncutil.Mutex
		blockingCh chan struct{}
		campaigns  map[roachpb.ReplicaID]bool // map from ReplicaID -> whether the replica campaigned
	}{}
	sc := storage.TestStoreConfig(nil)
	sc.EnableEpochRangeLeases = false // simpler to test with
	sc.TestingKnobs.DontPreventUseOfOldLeaseOnStart = true
	sc.TestingKnobs.OnCampaign = func(r *storage.Replica) {
		if !r.DescLocked().StartKey.Equal(keys.UserTableDataMin) {
			return
		}
		testState.Lock()
		if testState.campaigns == nil {
			testState.Unlock()
			return
		}
		testState.campaigns[r.ReplicaIDLocked()] = true
		blocking := testState.blockingCh
		testState.Unlock()
		<-blocking
	}
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Split so we can rely on RHS range being quiescent after a restart.
	// We use UserTableDataMin to avoid having the range activated to
	// gossip system table data.
	splitArgs := adminSplitArgs(roachpb.KeyMin, splitKey)
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), splitArgs); err != nil {
		t.Fatal(err)
	}

	// Up-replicate to three replicas.
	repl := mtc.stores[0].LookupReplica(roachpb.RKey(splitKey), nil)
	if repl == nil {
		t.Fatal("replica should not be nil for RHS range")
	}
	mtc.replicateRange(repl.RangeID, 1, 2)

	testCases := []struct {
		desc         string
		prepFn       func(*testing.T)
		expCampaigns map[roachpb.ReplicaID]bool
	}{
		{
			desc:         "within idle replica campaign timeout",
			prepFn:       func(t *testing.T) {},
			expCampaigns: map[roachpb.ReplicaID]bool{},
		},
		{
			desc: "past idle replica campaign timeout",
			prepFn: func(t *testing.T) {
				for _, s := range mtc.stores {
					if err := s.GossipStore(context.TODO()); err != nil {
						t.Fatal(err)
					}
				}
				mtc.manualClock.Increment(
					storage.RaftElectionTimeout(sc.RaftTickInterval, sc.RaftElectionTimeoutTicks).Nanoseconds())
			},
			expCampaigns: map[roachpb.ReplicaID]bool{
				1: true,
			},
		},
		{
			desc: "lease expired all replicas should campaign",
			prepFn: func(t *testing.T) {
				for _, s := range mtc.stores {
					if err := s.GossipStore(context.TODO()); err != nil {
						t.Fatal(err)
					}
				}
				mtc.manualClock.Increment(mtc.storeConfig.LeaseExpiration())
			},
			expCampaigns: map[roachpb.ReplicaID]bool{
				1: true,
				2: true,
				3: true,
			},
		},
	}

	for i, test := range testCases {
		t.Run(test.desc, func(t *testing.T) {
			// Restart the cluster for lazy initialization of the raft group.
			mtc.restart()
			// Clear the campaign map.
			testState.Lock()
			testState.blockingCh = make(chan struct{})
			testState.campaigns = map[roachpb.ReplicaID]bool{}
			testState.Unlock()
			// Run whatever preparation is necessary for this test case.
			test.prepFn(t)

			// Send an increment to all three replicas in parallel.
			errCh := make(chan error, len(mtc.stores))
			for _, s := range mtc.stores {
				go func(s *storage.Store) {
					incArgs := incrementArgs(splitKey, 1)
					_, pErr := client.SendWrappedWith(
						context.Background(), s, roachpb.Header{RangeID: repl.RangeID}, incArgs,
					)
					errCh <- pErr.GoError()
				}(s)
			}

			// Allow parallel invocations to proceed.
			testutils.SucceedsSoon(t, func() error {
				testState.Lock()
				defer testState.Unlock()
				if c, ec := len(testState.campaigns), len(test.expCampaigns); c != ec {
					return errors.Errorf("have seen %d campaigns out of %d expected", c, ec)
				}
				return nil
			})
			close(testState.blockingCh)

			// We expect not lease holder errors on 2 out of the three replicas.
			var errCount int
			for range mtc.stores {
				if err := <-errCh; err != nil {
					errCount++
					if _, ok := err.(*roachpb.NotLeaseHolderError); !ok {
						t.Errorf("got unexpected error %s", err)
					}
				}
			}
			if errCount != 2 {
				t.Errorf("expected 2 errors; got %d", errCount)
			}

			mtc.waitForValues(splitKey, []int64{int64(i + 1), int64(i + 1), int64(i + 1)})

			testState.Lock()
			if !reflect.DeepEqual(test.expCampaigns, testState.campaigns) {
				t.Errorf("expected %+v; got %+v", test.expCampaigns, testState.campaigns)
			}
			testState.Unlock()

			// HACK: sleep to process raft leader wakeup proposals. It is
			// otherwise too difficult to guarantee that the next test cycle
			// will not have a stray raft request init the raft group after
			// restart.
			time.Sleep(100 * time.Millisecond)
		})
	}
}

// TestDrainRangeRejection verifies that an attempt to transfer a range to a
// draining store fails.
func TestDrainRangeRejection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 2)

	repl, err := mtc.stores[0].GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	drainingIdx := 1
	mtc.stores[drainingIdx].SetDraining(true)
	if err := repl.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicationTarget{
			NodeID:  mtc.idents[drainingIdx].NodeID,
			StoreID: mtc.idents[drainingIdx].StoreID,
		},
		repl.Desc(),
	); !testutils.IsError(err, "store is draining") {
		t.Fatalf("unexpected error: %v", err)
	}
}
