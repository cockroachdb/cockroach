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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage_test

import (
	"bytes"
	"math"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestRangeCommandClockUpdate verifies that followers update their
// clocks when executing a command, even if the leader's clock is far
// in the future.
func TestRangeCommandClockUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)

	const numNodes = 3
	var manuals []*hlc.ManualClock
	var clocks []*hlc.Clock
	for i := 0; i < numNodes; i++ {
		manuals = append(manuals, hlc.NewManualClock(1))
		clocks = append(clocks, hlc.NewClock(manuals[i].UnixNano))
		clocks[i].SetMaxOffset(100 * time.Millisecond)
	}
	mtc := multiTestContext{
		clocks: clocks,
	}
	mtc.Start(t, numNodes)
	defer mtc.Stop()
	mtc.replicateRange(1, 0, 1, 2)

	// Advance the leader's clock ahead of the followers (by more than
	// MaxOffset but less than the leader lease) and execute a command.
	manuals[0].Increment(int64(500 * time.Millisecond))
	incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
	incArgs.Timestamp = clocks[0].Now()
	if err := mtc.stores[0].ExecuteCmd(context.Background(), proto.Call{Args: incArgs, Reply: incResp}); err != nil {
		t.Fatal(err)
	}

	// Wait for that command to execute on all the followers.
	util.SucceedsWithin(t, 50*time.Millisecond, func() error {
		values := []int64{}
		for _, eng := range mtc.engines {
			val, _, err := engine.MVCCGet(eng, proto.Key("a"), clocks[0].Now(), true, nil)
			if err != nil {
				return err
			}
			values = append(values, mustGetInteger(val))
		}
		if !reflect.DeepEqual(values, []int64{5, 5, 5}) {
			return util.Errorf("expected (5, 5, 5), got %v", values)
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

// TestRejectFutureCommand verifies that leaders reject commands that
// would cause a large time jump.
func TestRejectFutureCommand(t *testing.T) {
	defer leaktest.AfterTest(t)

	const maxOffset = 100 * time.Millisecond
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(maxOffset)
	mtc := multiTestContext{
		clock: clock,
	}
	mtc.Start(t, 1)
	defer mtc.Stop()

	// First do a write. The first write will advance the clock by MaxOffset
	// because of the read cache's low water mark.
	getArgs, getResp := putArgs([]byte("b"), []byte("b"), 1, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(context.Background(), proto.Call{Args: getArgs, Reply: getResp}); err != nil {
		t.Fatal(err)
	}
	if now := clock.Now(); now.WallTime != int64(maxOffset) {
		t.Fatalf("expected clock to advance to 100ms; got %s", now)
	}
	// The logical clock has advanced past the physical clock; increment
	// the "physical" clock to catch up.
	manual.Increment(int64(maxOffset))

	startTime := manual.UnixNano()

	// Commands with a future timestamp that is within the MaxOffset
	// bound will be accepted and will cause the clock to advance.
	for i := int64(0); i < 3; i++ {
		incArgs, incResp := incrementArgs([]byte("a"), 5, 1, mtc.stores[0].StoreID())
		incArgs.Timestamp.WallTime = startTime + ((i+1)*30)*int64(time.Millisecond)
		if err := mtc.stores[0].ExecuteCmd(context.Background(), proto.Call{Args: incArgs, Reply: incResp}); err != nil {
			t.Fatal(err)
		}
	}
	if now := clock.Now(); now.WallTime != int64(190*time.Millisecond) {
		t.Fatalf("expected clock to advance to 190ms; got %s", now)
	}

	// Once the accumulated offset reaches MaxOffset, commands will be rejected.
	incArgs, incResp := incrementArgs([]byte("a"), 11, 1, mtc.stores[0].StoreID())
	incArgs.Timestamp.WallTime = int64((time.Duration(startTime) + maxOffset + 1) * time.Millisecond)
	if err := mtc.stores[0].ExecuteCmd(context.Background(), proto.Call{Args: incArgs, Reply: incResp}); err == nil {
		t.Fatalf("expected clock offset error but got nil")
	}

	// The clock remained at 190ms and the final command was not executed.
	if now := clock.Now(); now.WallTime != int64(190*time.Millisecond) {
		t.Errorf("expected clock to advance to 190ms; got %s", now)
	}
	val, _, err := engine.MVCCGet(mtc.engines[0], proto.Key("a"), clock.Now(), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if v := mustGetInteger(val); v != 15 {
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
	defer leaktest.AfterTest(t)

	key := "key"
	// Set up a filter to so that the get operation at Step 3 will return an error.
	var numGets int32
	storage.TestingCommandFilter = func(args proto.Request) error {
		if _, ok := args.(*proto.GetRequest); ok &&
			args.Header().Key.Equal(proto.Key(key)) &&
			args.Header().Txn == nil {
			// The Reader executes two get operations, each of which triggers two get requests
			// (the first request fails and triggers txn push, and then the second request
			// succeeds). Returns an error for the fourth get request to avoid timestamp cache
			// update after the third get operation pushes the txn timestamp.
			if atomic.AddInt32(&numGets, 1) == 4 {
				return util.Errorf("Test")
			}
		}
		return nil
	}
	defer func() {
		storage.TestingCommandFilter = nil
	}()

	manualClock := hlc.NewManualClock(0)
	clock := hlc.NewClock(manualClock.UnixNano)
	store, stopper := createTestStoreWithEngine(t,
		engine.NewInMem(proto.Attributes{}, 10<<20),
		clock,
		true,
		nil)
	defer stopper.Stop()

	// Put an initial value.
	initVal := []byte("initVal")
	if err := store.DB().Put(key, initVal); err != nil {
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
		if err := store.DB().Txn(func(txn *client.Txn) error {
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

			b := &client.Batch{}
			err = txn.Commit(b)
			return err
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

	priority := int32(math.MaxInt32)
	requestHeader := proto.RequestHeader{
		Key:          proto.Key(key),
		RaftID:       1,
		Replica:      proto.Replica{StoreID: store.StoreID()},
		UserPriority: &priority,
		Timestamp:    clock.Now(),
	}
	getCall := proto.Call{
		Args: &proto.GetRequest{
			RequestHeader: requestHeader,
		},
		Reply: &proto.GetResponse{},
	}
	if err := store.ExecuteCmd(context.Background(), getCall); err != nil {
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

	requestHeader.Timestamp = clock.Now()
	getCall = proto.Call{
		Args: &proto.GetRequest{
			RequestHeader: requestHeader,
		},
		Reply: &proto.GetResponse{},
	}

	if err := store.ExecuteCmd(context.Background(), getCall); err == nil {
		t.Fatal("unexpected success of get")
	}

	close(waitSecondGet)
	<-waitTxnComplete
}
