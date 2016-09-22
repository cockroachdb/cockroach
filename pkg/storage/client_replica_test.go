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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
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
		clocks = append(clocks, hlc.NewClock(manuals[i].UnixNano))
		clocks[i].SetMaxOffset(100 * time.Millisecond)
	}
	mtc := &multiTestContext{clocks: clocks}
	mtc.Start(t, numNodes)
	defer mtc.Stop()
	mtc.replicateRange(1, 1, 2)

	// Advance the lease holder's clock ahead of the followers (by more than
	// MaxOffset but less than the range lease) and execute a command.
	manuals[0].Increment(int64(500 * time.Millisecond))
	incArgs := incrementArgs([]byte("a"), 5)
	ts := clocks[0].Now()
	if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{Timestamp: ts}, &incArgs); err != nil {
		t.Fatal(err)
	}

	// Wait for that command to execute on all the followers.
	util.SucceedsSoon(t, func() error {
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

	const maxOffset = 100 * time.Millisecond
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxOffset)
	mtc := &multiTestContext{clock: clock}
	mtc.Start(t, 1)
	defer mtc.Stop()

	startTime := manual.UnixNano()

	// Commands with a future timestamp that is within the MaxOffset
	// bound will be accepted and will cause the clock to advance.
	for i := int64(0); i < 3; i++ {
		incArgs := incrementArgs([]byte("a"), 5)
		ts := hlc.ZeroTimestamp.Add(startTime+((i+1)*30)*int64(time.Millisecond), 0)
		if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{Timestamp: ts}, &incArgs); err != nil {
			t.Fatal(err)
		}
	}
	if now := clock.Now(); now.WallTime != int64(90*time.Millisecond) {
		t.Fatalf("expected clock to advance to 90ms; got %s", now)
	}

	// Once the accumulated offset reaches MaxOffset, commands will be rejected.
	incArgs := incrementArgs([]byte("a"), 11)
	ts := hlc.ZeroTimestamp.Add(int64((time.Duration(startTime)+maxOffset+1)*time.Millisecond), 0)
	if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{Timestamp: ts}, &incArgs); err == nil {
		t.Fatalf("expected clock offset error but got nil")
	}

	// The clock remained at 90ms and the final command was not executed.
	if now := clock.Now(); now.WallTime != int64(90*time.Millisecond) {
		t.Errorf("expected clock to stay at 90ms; got %s", now)
	}
	val, _, err := engine.MVCCGet(context.Background(), mtc.engines[0], roachpb.Key("a"), clock.Now(), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := mustGetInt(val); v != 15 {
		t.Errorf("expected 15, got %v", v)
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

	key := "key"
	// Set up a filter to so that the get operation at Step 3 will return an error.
	var numGets int32

	manualClock := hlc.NewManualClock(0)
	clock := hlc.NewClock(manualClock.UnixNano)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	cfg := storage.TestStoreConfig()
	cfg.TestingKnobs.TestingCommandFilter =
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
		clock,
		true,
		cfg,
		stopper)

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
	waitTxnComplete := make(chan struct{})

	// Start the Writer.
	go func() {
		epoch := -1
		// Start a txn that does read-after-write.
		// The txn will be restarted twice, and the out-of-order put
		// will happen in the second epoch.
		if err := store.DB().Txn(context.TODO(), func(txn *client.Txn) error {
			epoch++

			if epoch == 1 {
				// Wait until the second get operation is issued.
				close(waitTxnRestart)
				<-waitSecondGet
			}

			updatedVal := []byte("updatedVal")
			if err := txn.Put(key, updatedVal); err != nil {
				return err
			}

			// Make sure a get will return the value that was just written.
			actual, err := txn.Get(key)
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
			return txn.CommitInBatch(b)
		}); err != nil {
			t.Fatal(err)
		}

		if epoch != 2 {
			t.Fatalf("unexpected number of txn retries: %d", epoch)
		}

		close(waitTxnComplete)
	}()

	<-waitPut

	// Start the Reader.

	// Advance the clock and send a get operation with higher
	// priority to trigger the txn restart.
	manualClock.Increment(100)

	priority := roachpb.UserPriority(-math.MaxInt32)
	requestHeader := roachpb.Span{
		Key: roachpb.Key(key),
	}
	ts := clock.Now()
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Timestamp:    ts,
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
	manualClock.Increment(100)

	ts = clock.Now()
	if _, err := client.SendWrappedWith(context.Background(), rg1(store), roachpb.Header{
		Timestamp:    ts,
		UserPriority: priority,
	}, &roachpb.GetRequest{Span: requestHeader}); err == nil {
		t.Fatal("unexpected success of get")
	}

	close(waitSecondGet)
	<-waitTxnComplete
}

// TestRangeLookupUseReverse tests whether the results and the results count
// are correct when scanning in reverse order.
func TestRangeLookupUseReverse(t *testing.T) {
	defer leaktest.AfterTest(t)()
	storeCfg := storage.TestStoreConfig()
	storeCfg.TestingKnobs.DisableSplitQueue = true
	store, stopper, _ := createTestStoreWithConfig(t, storeCfg)
	defer stopper.Stop()

	// Init test ranges:
	// ["","a"), ["a","c"), ["c","e"), ["e","g") and ["g","\xff\xff").
	splits := []roachpb.AdminSplitRequest{
		adminSplitArgs(roachpb.Key("g"), roachpb.Key("g")),
		adminSplitArgs(roachpb.Key("e"), roachpb.Key("e")),
		adminSplitArgs(roachpb.Key("c"), roachpb.Key("c")),
		adminSplitArgs(roachpb.Key("a"), roachpb.Key("a")),
	}

	for _, split := range splits {
		_, pErr := client.SendWrapped(context.Background(), rg1(store), &split)
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
	util.SucceedsSoon(t, func() error {
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
	cfg := storage.TestStoreConfig()
	var filterMu syncutil.Mutex
	var filter func(filterArgs storagebase.FilterArgs) *roachpb.Error
	cfg.TestingKnobs.TestingCommandFilter =
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
	mtc.Start(t, 2)
	defer mtc.Stop()

	// First, do a write; we'll use it to determine when the dust has settled.
	leftKey := roachpb.Key("a")
	incArgs := incrementArgs(leftKey, 1)
	if _, pErr := client.SendWrapped(context.Background(), mtc.distSenders[0], &incArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get the left range's ID.
	rangeID := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil).RangeID

	// Replicate the left range onto node 1.
	mtc.replicateRange(rangeID, 1)

	replica0 := mtc.stores[0].LookupReplica(roachpb.RKey("a"), nil)
	replica1 := mtc.stores[1].LookupReplica(roachpb.RKey("a"), nil)
	gArgs := getArgs(leftKey)
	replica0Desc, err := replica0.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	// Check that replica0 can serve reads OK.
	if _, pErr := client.SendWrappedWith(
		context.Background(),
		mtc.senders[0],
		roachpb.Header{Replica: replica0Desc},
		&gArgs,
	); pErr != nil {
		t.Fatal(pErr)
	}

	{
		// Transferring the lease to ourself should be a no-op.
		origLeasePtr, _ := replica0.GetLease()
		origLease := *origLeasePtr
		if err := replica0.AdminTransferLease(replica0Desc.StoreID); err != nil {
			t.Fatal(err)
		}
		newLeasePtr, _ := replica0.GetLease()
		if origLeasePtr != newLeasePtr || origLease != *newLeasePtr {
			t.Fatalf("expected %+v, but found %+v", origLeasePtr, newLeasePtr)
		}
	}

	{
		// An invalid target should result in an error.
		const expected = "unable to find store .* in range"
		if err := replica0.AdminTransferLease(1000); !testutils.IsError(err, expected) {
			t.Fatalf("expected %s, but found %v", expected, err)
		}
	}

	// Move the lease to store 1.
	var newHolderDesc roachpb.ReplicaDescriptor
	util.SucceedsSoon(t, func() error {
		var err error
		newHolderDesc, err = replica1.GetReplicaDescriptor()
		return err
	})

	if err := replica0.AdminTransferLease(newHolderDesc.StoreID); err != nil {
		t.Fatal(err)
	}

	// Check that replica0 doesn't serve reads any more.
	replica0Desc, err = replica0.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}
	_, pErr := client.SendWrappedWith(
		context.Background(),
		mtc.senders[0],
		roachpb.Header{Replica: replica0Desc},
		&gArgs,
	)
	nlhe, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, pErr)
	}
	if *(nlhe.LeaseHolder) != newHolderDesc {
		t.Fatalf("expected lease holder %+v, got %+v",
			newHolderDesc, nlhe.LeaseHolder)
	}

	// Check that replica1 now has the lease (or gets it soon).
	util.SucceedsSoon(t, func() error {
		if _, pErr := client.SendWrappedWith(
			context.Background(),
			mtc.senders[1],
			roachpb.Header{Replica: replica0Desc},
			&gArgs,
		); pErr != nil {
			return pErr.GoError()
		}
		return nil
	})

	replica1Lease, _ := replica1.GetLease()

	// Verify the timestamp cache low water. Because we executed a transfer lease
	// request, the low water should be set to the new lease start time which is
	// less than the previous lease's expiration time.
	if lowWater := replica1.GetTimestampCacheLowWater(); lowWater != replica1Lease.Start {
		t.Fatalf("expected timestamp cache low water %s, but found %s",
			replica1Lease.Start, lowWater)
	}

	// Make replica1 extend its lease and transfer the lease immediately after
	// that. Test that the transfer still happens (it'll wait until the extension
	// is done).
	extensionSem := make(chan struct{})
	filterMu.Lock()
	filter = func(filterArgs storagebase.FilterArgs) *roachpb.Error {
		if filterArgs.Sid != mtc.stores[1].Ident.StoreID {
			return nil
		}
		llReq, ok := filterArgs.Req.(*roachpb.RequestLeaseRequest)
		if !ok {
			return nil
		}
		if llReq.Lease.Replica == newHolderDesc {
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
	filterMu.Unlock()
	// Initiate an extension.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		shouldRenewTS := replica1Lease.StartStasis.Add(-1, 0)
		mtc.manualClock.Set(shouldRenewTS.WallTime + 1)
		if _, pErr := client.SendWrappedWith(
			context.Background(),
			mtc.senders[1],
			roachpb.Header{Replica: replica0Desc},
			&gArgs,
		); pErr != nil {
			panic(pErr)
		}
	}()

	<-extensionSem
	waitForTransferBlocked.Store(true)
	// Initiate a transfer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Transfer back from replica1 to replica0.
		if err := replica1.AdminTransferLease(replica0Desc.StoreID); err != nil {
			panic(err)
		}
	}()
	// Wait for the transfer to be blocked by the extension.
	<-transferBlocked
	// Now unblock the extension.
	extensionSem <- struct{}{}
	// Check that the transfer to replica1 eventually happens.
	util.SucceedsSoon(t, func() error {
		if _, pErr := client.SendWrappedWith(
			context.Background(),
			mtc.senders[0],
			roachpb.Header{Replica: replica0Desc},
			&gArgs,
		); pErr != nil {
			return pErr.GoError()
		}
		return nil
	})
	filterMu.Lock()
	filter = nil
	filterMu.Unlock()
	wg.Wait()
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
					TestingCommandFilter: cmdFilter,
				},
			},
		})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop()

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
				Start:       s.Clock().Now(),
				StartStasis: s.Clock().Now().Add(time.Second.Nanoseconds(), 0),
				Expiration:  s.Clock().Now().Add(2*time.Second.Nanoseconds(), 0),
				Replica:     repDesc,
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
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop()

	kvDB0 := tc.Servers[0].DB()
	kvDB1 := tc.Servers[1].DB()

	key := []byte("a")
	rangeDesc := new(roachpb.RangeDescriptor)
	var err error
	*rangeDesc, err = tc.LookupRange(key)
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
	leaseHolderReplica := LeaseInfo(t, kvDB0, *rangeDesc, roachpb.INCONSISTENT).Lease.Replica
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
	leaseHolderReplica = LeaseInfo(t, kvDB0, *rangeDesc, roachpb.INCONSISTENT).Lease.Replica
	if leaseHolderReplica != replicas[1] {
		t.Fatalf("lease holder should be replica %+v, but is: %+v",
			replicas[1], leaseHolderReplica)
	}

	// A read on the new lease holder does not necessarily succeed immediately,
	// since it might take a while for it to apply the transfer.
	util.SucceedsSoon(t, func() error {
		// We can't reliably do a CONSISTENT read here, even though we're reading
		// from the supposed lease holder, because this node might initially be
		// unaware of the new lease and so the request might bounce around for a
		// while (see #8816).
		leaseHolderReplica = LeaseInfo(t, kvDB1, *rangeDesc, roachpb.INCONSISTENT).Lease.Replica
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
	leaseHolderReplica = LeaseInfo(t, kvDB1, *rangeDesc, roachpb.INCONSISTENT).Lease.Replica
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
					TestingCommandFilter: cmdFilter,
				},
			},
		})
	s := srv.(*server.TestServer)
	defer s.Stopper().Stop()

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
