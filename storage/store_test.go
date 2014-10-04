// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"testing"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

var testIdent = proto.StoreIdent{
	ClusterID: "cluster",
	NodeID:    1,
	StoreID:   1,
}

// TestStoreInitAndBootstrap verifies store initialization and
// bootstrap.
func TestStoreInitAndBootstrap(t *testing.T) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	store := NewStore(clock, eng, nil, nil)
	defer store.Close()

	// Can't init as haven't bootstrapped.
	if err := store.Init(); err == nil {
		t.Error("expected failure init'ing un-bootstrapped store")
	}

	// Bootstrap with a fake ident.
	if err := store.Bootstrap(testIdent); err != nil {
		t.Errorf("error bootstrapping store: %s", err)
	}

	// Try to get 1st range--non-existent.
	if _, err := store.GetRange(1); err == nil {
		t.Error("expected error fetching non-existent range")
	}

	// Create range and fetch.
	if _, err := store.CreateRange(store.BootstrapRangeMetadata()); err != nil {
		t.Errorf("failure to create first range: %s", err)
	}
	if rng, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %s", err)
	} else {
		rng.Start()
	}

	// Now, attempt to initialize a store with a now-bootstrapped engine.
	store = NewStore(clock, eng, nil, nil)
	if err := store.Init(); err != nil {
		t.Errorf("failure initializing bootstrapped store: %s", err)
	}
	// 1st range should be available.
	if _, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %s", err)
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)

	// Put some random garbage into the engine.
	if err := eng.Put(engine.Key("foo"), []byte("bar")); err != nil {
		t.Errorf("failure putting key foo into engine: %s", err)
	}
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	store := NewStore(clock, eng, nil, nil)
	defer store.Close()

	// Can't init as haven't bootstrapped.
	if err := store.Init(); err == nil {
		t.Error("expected failure init'ing un-bootstrapped store")
	}

	// Bootstrap should fail on non-empty engine.
	if err := store.Bootstrap(testIdent); err == nil {
		t.Error("expected bootstrap error on non-empty store")
	}
}

func TestRangeSliceSort(t *testing.T) {
	var rs RangeSlice
	for i := 4; i >= 0; i-- {
		key := engine.Key(fmt.Sprintf("foo%d", i))
		rs = append(rs, &Range{
			Meta: &proto.RangeMetadata{
				RangeDescriptor: proto.RangeDescriptor{StartKey: key},
			},
		})
	}

	sort.Sort(rs)
	for i := 0; i < 5; i++ {
		expectedKey := engine.Key(fmt.Sprintf("foo%d", i))
		if !bytes.Equal(rs[i].Meta.StartKey, expectedKey) {
			t.Errorf("Expected %s, got %s", expectedKey, rs[i].Meta.StartKey)
		}
	}
}

// createTestStore creates a test store using an in-memory
// engine. Returns the store clock's manual unix nanos time and the
// store. If createDefaultRange is true, creates a single range from
// key "a" to key "z" with a default replica descriptor (i.e. StoreID
// = 0, RangeID = 1, etc.). The caller is responsible for closing the
// store on exit.
func createTestStore(createDefaultRange bool, t *testing.T) (*Store, *hlc.ManualClock) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	store := NewStore(clock, eng, nil, nil)
	if err := store.Bootstrap(proto.StoreIdent{StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	db, _ := newTestDB(store)
	store.db = db
	replica := proto.Replica{StoreID: 1, RangeID: 1}
	// Create system key range for allocations.
	meta := store.BootstrapRangeMetadata()
	meta.StartKey = engine.KeySystemPrefix
	meta.EndKey = engine.KeySystemPrefix.PrefixEnd()
	rng, err := store.CreateRange(meta)
	if err != nil {
		t.Fatal(err)
	}
	rng.Start()
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	// Now that the system key range is available, initialize the store. set store DB so new
	// ranges can be allocated as needed for tests.
	// If requested, create a default range for tests from "a"-"z".
	if createDefaultRange {
		replica = proto.Replica{StoreID: 1}
		rng, err := store.CreateRange(store.NewRangeMetadata(engine.Key("a"), engine.Key("z"), []proto.Replica{replica}))
		rng.Start()
		if err != nil {
			t.Fatal(err)
		}
	}
	return store, &manual
}

// TestStoreExecuteCmd verifies straightforward command execution
// of both a read-only and a read-write command.
func TestStoreExecuteCmd(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()
	gArgs, gReply := getArgs([]byte("a"), 2)

	// Try a successful get request.
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("aaa"), 2)
	if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}

}

// TestStoreExecuteCmdUpdateTime verifies that the node clock is updated.
func TestStoreExecuteCmdUpdateTime(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()
	args, reply := getArgs([]byte("a"), 2)
	args.Timestamp = store.clock.Now()
	args.Timestamp.WallTime += (100 * time.Millisecond).Nanoseconds()
	err := store.ExecuteCmd(Get, args, reply)
	if err != nil {
		t.Fatal(err)
	}
	ts := store.clock.Timestamp()
	if ts.WallTime != args.Timestamp.WallTime || ts.Logical <= args.Timestamp.Logical {
		t.Errorf("expected store clock to advance to %s; got %s", args.Timestamp, ts)
	}
}

// TestStoreExecuteCmdWithZeroTime verifies that no timestamp causes
// the command to assume the node's wall time.
func TestStoreExecuteCmdWithZeroTime(t *testing.T) {
	store, mc := createTestStore(true, t)
	defer store.Close()
	args, reply := getArgs([]byte("a"), 2)

	// Set clock to time 1.
	*mc = hlc.ManualClock(1)
	err := store.ExecuteCmd(Get, args, reply)
	if err != nil {
		t.Fatal(err)
	}
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if reply.Timestamp.WallTime != store.clock.Timestamp().WallTime {
		t.Errorf("expected reply to have store clock time %s; got %s",
			store.clock.Timestamp(), reply.Timestamp)
	}
}

// TestStoreExecuteCmdWithClockDrift verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock drift, the cmd fails with an error.
func TestStoreExecuteCmdWithClockDrift(t *testing.T) {
	store, mc := createTestStore(true, t)
	defer store.Close()
	args, reply := getArgs([]byte("a"), 2)

	// Set clock to time 1.
	*mc = hlc.ManualClock(1)
	// Set clock max drift to 250ms.
	maxDrift := 250 * time.Millisecond
	store.clock.SetMaxDrift(maxDrift)
	// Set args timestamp to exceed max drift.
	args.Timestamp = store.clock.Now()
	args.Timestamp.WallTime += maxDrift.Nanoseconds() + 1
	err := store.ExecuteCmd(Get, args, reply)
	if err == nil {
		t.Error("expected max drift clock error")
	}
}

// TestStoreExecuteCmdBadRange passes a bad range.
func TestStoreExecuteCmdBadRange(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()
	// Range is from "a" to "z", so this value should fail.
	args, reply := getArgs([]byte("0"), 2)
	err := store.ExecuteCmd(Get, args, reply)
	if err == nil {
		t.Error("expected invalid range")
	}
}

// TestStoreExecuteCmdOutOfRange passes a key not contained
// within the range's key range.
func TestStoreExecuteCmdOutOfRange(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()
	// Range is from "a" to "z", so this value should fail.
	args, reply := getArgs([]byte("0"), 2)
	err := store.ExecuteCmd(Get, args, reply)
	if err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRaftIDAllocation verifies that raft IDs are
// allocated in successive blocks.
func TestStoreRaftIDAllocation(t *testing.T) {
	store, _ := createTestStore(false, t)
	defer store.Close()

	// Raft IDs should be allocated from ID 2 (first alloc'd range)
	// to raftIDAllocCount * 3 + 1.
	for i := 0; i < raftIDAllocCount*3; i++ {
		r := addTestRange(store, engine.Key(fmt.Sprintf("%03d", i)), engine.Key(fmt.Sprintf("%03d", i+1)), t)
		if r.Meta.RaftID != int64(2+i) {
			t.Errorf("expected Raft id %d; got %d", 2+i, r.Meta.RaftID)
		}
	}
}

func addTestRange(store *Store, start, end engine.Key, t *testing.T) *Range {
	replicas := []proto.Replica{
		proto.Replica{StoreID: store.Ident.StoreID},
	}
	r, err := store.CreateRange(store.NewRangeMetadata(start, end, replicas))
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	return r
}

// TestStoreRangesByKey verifies we can lookup ranges by key using
// the sorted rangesByKey slice.
func TestStoreRangesByKey(t *testing.T) {
	store, _ := createTestStore(false, t)
	defer store.Close()

	r1 := addTestRange(store, engine.Key("A"), engine.Key("C"), t)
	r2 := addTestRange(store, engine.Key("C"), engine.Key("X"), t)
	r3 := addTestRange(store, engine.Key("X"), engine.Key("ZZ"), t)

	if store.LookupRange(engine.Key("a"), nil) != nil {
		t.Errorf("expected \"a\" to not have an associated range")
	}
	if r := store.LookupRange(engine.Key("B"), nil); r != r1 {
		t.Errorf("mismatched range %+v != %+v", r, r1.Meta)
	}
	if r := store.LookupRange(engine.Key("C"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r, r2.Meta)
	}
	if r := store.LookupRange(engine.Key("M"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r, r2.Meta)
	}
	if r := store.LookupRange(engine.Key("X"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r, r3.Meta)
	}
	if r := store.LookupRange(engine.Key("Z"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r, r3.Meta)
	}
	if store.LookupRange(engine.KeyMax, nil) != nil {
		t.Errorf("expected engine.KeyMax to not have an associated range")
	}
}

// TestStoreResolveWriteIntent adds write intent and then verifies
// that a put returns a WriteIntentError with Resolved flag set to
// true in the event the pushee has lower priority or false otherwise.
// Retrying the put should succeed for the Resolved case and fail
// with another WriteIntentError otherwise.
func TestStoreResolveWriteIntent(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	for i, resolvable := range []bool{true, false} {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
		pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
		if resolvable {
			pushee.Priority = 1
			pusher.Priority = 2 // Pusher will win.
		} else {
			pushee.Priority = 2
			pusher.Priority = 1 // Pusher will lose.
		}

		// First lay down intent using the pushee's txn.
		pArgs, pReply := putArgs(key, []byte("value"), 2)
		pArgs.Timestamp = store.clock.Now()
		pArgs.Txn = pushee
		if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
			t.Fatal(err)
		}

		// Now, try a put using the pusher's txn.
		pArgs.Timestamp = store.clock.Now()
		pArgs.Txn = pusher
		err := store.ExecuteCmd(Put, pArgs, pReply)
		if err == nil {
			t.Errorf("resolvable? %t, expected write intent error", resolvable)
		}
		if resolvable {
			wiErr, ok := err.(*proto.WriteIntentError)
			if !ok {
				t.Fatalf("resolvable? %t, expected write intent error; got %s", resolvable, err)
			}
			if !bytes.Equal(wiErr.Key, key) || !bytes.Equal(wiErr.Txn.ID, pushee.ID) ||
				wiErr.Resolved != resolvable {
				t.Errorf("resolvable? %t, unexpected values in write intent error: %s", resolvable, wiErr)
			}
			// Trying again should succeed.
			if err = store.ExecuteCmd(Put, pArgs, pReply); err != nil {
				t.Errorf("resolvable? %t, expected write to succeed: %s", resolvable, err)
			}
		} else {
			rErr, ok := err.(*proto.TransactionRetryError)
			if !ok {
				t.Errorf("resolvable? %t, expected txn retry error; got %s", resolvable, err)
			}
			if !bytes.Equal(rErr.Txn.ID, pushee.ID) || !rErr.Backoff {
				t.Errorf("resolvable? %t, expected txn to match pushee %q, with !Backoff; got %s",
					resolvable, pushee.ID, rErr)
			}
			// Trying again should fail again.
			if err = store.ExecuteCmd(Put, pArgs, pReply); err == nil {
				t.Errorf("resolvable? %t, expected write intent error but succeeded", resolvable)
			}
		}
	}
}

// TestStoreResolveWriteIntentRollback verifies that resolving a write
// intent by aborting it yields the previous value.
func TestStoreResolveWriteIntentRollback(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	key := engine.Key("a")
	pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee.Priority = 1
	pusher.Priority = 2 // Pusher will win.

	// First lay down intent using the pushee's txn.
	args, reply := incrementArgs(key, 1, 2)
	args.Timestamp = store.clock.Now()
	args.Txn = pushee
	if err := store.ExecuteCmd(Increment, args, reply); err != nil {
		t.Fatal(err)
	}

	// Now, try a put using the pusher's txn.
	args.Timestamp = store.clock.Now()
	args.Txn = pusher
	args.Increment = 2
	err := store.ExecuteCmd(Increment, args, reply)
	if err == nil {
		t.Error("expected write intent error")
	}

	// Trying again should succeed.
	if err = store.ExecuteCmd(Increment, args, reply); err != nil {
		t.Errorf("expected write to succeed: %s", err)
	}
	if reply.NewValue != 2 {
		t.Errorf("expected rollback of earlier increment to yield increment value of 2; got %d", reply.NewValue)
	}
}

// TestStoreResolveWriteIntentPushOnRead verifies that resolving a
// write intent for a read will push the timestamp. On failure to
// push, verify a write intent error is returned with !Resolvable.
func TestStoreResolveWriteIntentPushOnRead(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	testCases := []struct {
		resolvable bool
		pusheeIso  proto.IsolationType
	}{
		// Resolvable is true, so we can read, but SERIALIZABLE means we can't commit.
		{true, proto.SERIALIZABLE},
		// Pushee is SNAPSHOT, meaning we can commit.
		{true, proto.SNAPSHOT},
		// Resolvable is false and SERIALIZABLE so can't read.
		{false, proto.SERIALIZABLE},
		// Resolvable is false, but SNAPSHOT means we can push it anyway, so can read.
		{false, proto.SNAPSHOT},
	}
	for i, test := range testCases {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
		pushee := NewTransaction("test", key, 1, test.pusheeIso, store.clock)
		if test.resolvable {
			pushee.Priority = 1
			pusher.Priority = 2 // Pusher will win.
		} else {
			pushee.Priority = 2
			pusher.Priority = 1 // Pusher will lose.
		}

		// First, write original value.
		args, reply := putArgs(key, []byte("value1"), 2)
		args.Timestamp = store.clock.Now()
		if err := store.ExecuteCmd(Put, args, reply); err != nil {
			t.Fatal(err)
		}

		// Second, lay down intent using the pushee's txn.
		args.Timestamp = store.clock.Now()
		args.Txn = pushee
		args.Value.Bytes = []byte("value2")
		if err := store.ExecuteCmd(Put, args, reply); err != nil {
			t.Fatal(err)
		}

		// Now, try to read value using the pusher's txn.
		gArgs, gReply := getArgs(key, 2)
		gArgs.Timestamp = store.clock.Now()
		gArgs.Txn = pusher
		err := store.ExecuteCmd(Get, gArgs, gReply)
		if test.resolvable {
			if err != nil {
				t.Errorf("expected read to succeed: %s", err)
			} else if !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
				t.Errorf("expected bytes to be %q, got %q", "value1", gReply.Value.Bytes)
			}

			// Finally, try to end the pushee's transaction; if we have
			// SNAPSHOT isolation, the commit should work: verify the txn
			// commit timestamp is equal to gArgs.Timestamp + 1. Otherwise,
			// verify commit fails with TransactionRetryError.
			etArgs, etReply := endTxnArgs(pushee, true, 2)
			etArgs.Timestamp = pushee.Timestamp
			err := store.ExecuteCmd(EndTransaction, etArgs, etReply)

			expTimestamp := gArgs.Timestamp
			expTimestamp.Logical++
			if test.pusheeIso == proto.SNAPSHOT {
				if err != nil {
					t.Errorf("unexpected error on commit: %s", err)
				}
				if etReply.Txn.Status != proto.COMMITTED || !etReply.Txn.Timestamp.Equal(expTimestamp) {
					t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
						expTimestamp, etReply.Txn)
				}
			} else {
				if retryErr, ok := err.(*proto.TransactionRetryError); !ok ||
					!retryErr.Txn.Timestamp.Equal(expTimestamp) {
					t.Errorf("expected transaction retry error with expTS=%s; got %s", expTimestamp, err)
				}
			}
		} else {
			// If isolation of pushee is SNAPSHOT, we can always push, so
			// even a non-resolvable read will succeed. Otherwise, verify we
			// receive a write intent error for iso=SERIALIZABLE.
			if test.pusheeIso == proto.SNAPSHOT {
				if err != nil {
					t.Errorf("expected read to succeed: %s", err)
				} else if !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
					t.Errorf("expected bytes to be %q, got %q", "value1", gReply.Value.Bytes)
				}
			} else {
				if err == nil {
					t.Errorf("expected read to fail")
				}
				if wiErr, ok := err.(*proto.WriteIntentError); !ok || wiErr.Resolved {
					t.Errorf("expected write intent error with !Resolved; got %s", err)
				}
			}
		}
	}
}

// TestStoreResolveWriteIntentSnapshotIsolation verifies that the
// timestamp can always be pushed if txn has snapshot isolation.
func TestStoreResolveWriteIntentSnapshotIsolation(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	key := engine.Key("a")
	pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee := NewTransaction("test", key, 1, proto.SNAPSHOT, store.clock)
	pushee.Priority = 2
	pusher.Priority = 1 // Pusher would lose based on priority.

	// First, write original value.
	args, reply := putArgs(key, []byte("value1"), 2)
	args.Timestamp = store.clock.Now()
	if err := store.ExecuteCmd(Put, args, reply); err != nil {
		t.Fatal(err)
	}

	// Lay down intent using the pushee's txn.
	args.Timestamp = store.clock.Now()
	args.Txn = pushee
	args.Value.Bytes = []byte("value2")
	if err := store.ExecuteCmd(Put, args, reply); err != nil {
		t.Fatal(err)
	}

	// Now, try to read value using the pusher's txn.
	gArgs, gReply := getArgs(key, 2)
	gArgs.Timestamp = store.clock.Now()
	gArgs.Txn = pusher
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil {
		t.Errorf("expected read to succeed: %s", err)
	} else if !bytes.Equal(gReply.Value.Bytes, []byte("value1")) {
		t.Errorf("expected bytes to be %q, got %q", "value1", gReply.Value.Bytes)
	}

	// Finally, try to end the pushee's transaction; since it's got
	// SNAPSHOT isolation, the end should work, but verify the txn
	// commit timestamp is equal to gArgs.Timestamp + 1.
	etArgs, etReply := endTxnArgs(pushee, true, 2)
	etArgs.Timestamp = pushee.Timestamp
	if err := store.ExecuteCmd(EndTransaction, etArgs, etReply); err != nil {
		t.Fatal(err)
	}
	expTimestamp := gArgs.Timestamp
	expTimestamp.Logical++
	if etReply.Txn.Status != proto.COMMITTED || !etReply.Txn.Timestamp.Equal(expTimestamp) {
		t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %s: %s",
			expTimestamp, etReply.Txn)
	}
}

// TestStoreResolveWriteIntentNoTxn verifies that reads and writes
// which are not part of a transaction can push intents.
func TestStoreResolveWriteIntentNoTxn(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	key := engine.Key("a")
	pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee.Priority = 0 // pushee should lose all conflicts

	// First, lay down intent from pushee.
	args, reply := putArgs(key, []byte("value1"), 2)
	args.Timestamp = pushee.Timestamp
	args.Txn = pushee
	if err := store.ExecuteCmd(Put, args, reply); err != nil {
		t.Fatal(err)
	}

	// Now, try to read outside a transaction. The non-transactional reader
	// will use a random priority, which is likely to be > 1, so should win.
	gArgs, gReply := getArgs(key, 2)
	gArgs.Timestamp = store.clock.Now()
	gArgs.UserPriority = gogoproto.Int32(math.MaxInt32)
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil {
		t.Errorf("expected read to succeed: %s", err)
	} else if gReply.Value != nil {
		t.Errorf("expected value to be nil, got %+v", gReply.Value)
	}

	// Next, try to write outside of a transaction. Same note about
	// priority applies here. We expect to abort the pushee txn.
	args.Timestamp = store.clock.Now()
	args.Value.Bytes = []byte("value2")
	args.Txn = nil
	args.UserPriority = gogoproto.Int32(math.MaxInt32)
	err := store.ExecuteCmd(Put, args, reply)
	if err == nil {
		t.Fatal("expected write intent error")
	}
	wiErr, ok := err.(*proto.WriteIntentError)
	if !ok {
		t.Errorf("expected write intent error; got %s", err)
	}
	if !bytes.Equal(wiErr.Key, key) || !bytes.Equal(wiErr.Txn.ID, pushee.ID) || !wiErr.Resolved {
		t.Errorf("unexpected values in write intent error: %s", wiErr)
	}
	// Verify that the pushee's timestamp was moved forward on
	// former read, since we have it available in write intent error.
	expTS := gArgs.Timestamp
	expTS.Logical++
	if !wiErr.Txn.Timestamp.Equal(expTS) {
		t.Errorf("expected pushee timestamp pushed to %s; got %s", expTS, wiErr.Txn.Timestamp)
	}
	// Similarly, verify that pushee's priority was moved from 0
	// to math.MaxInt32-1 during push.
	if wiErr.Txn.Priority != math.MaxInt32-1 {
		t.Errorf("expected pushee priority to be pushed to %d; got %d", math.MaxInt32-1, wiErr.Txn.Priority)
	}
	// Trying again should succeed.
	err = store.ExecuteCmd(Put, args, reply)
	if err != nil {
		t.Errorf("expected write to succeed: %s", err)
	}

	// Finally, try to end the pushee's transaction; it should have
	// been aborted.
	etArgs, etReply := endTxnArgs(pushee, true, 2)
	etArgs.Timestamp = pushee.Timestamp
	err = store.ExecuteCmd(EndTransaction, etArgs, etReply)
	if err == nil {
		t.Errorf("unexpected success committing transaction")
	}
	if _, ok := err.(*proto.TransactionAbortedError); !ok {
		t.Errorf("expected transaction aborted error; got %s", err)
	}
}

// TestStoreRangeSplit executes a split of a range and verifies that the
// resulting ranges respond to the right key ranges.
//
// TODO(Tobias, Spencer): More detailed testing of the transition into the new
// setup is required, and tests in which internal errors occur while splitting.
func TestStoreRangeSplit(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()
	rangeID := int64(2)
	splitKey := engine.Key("m")
	content := engine.Key("asdvb")

	// First, write some values left and right of the proposed split key.
	pArgs, pReply := putArgs([]byte("c"), content, rangeID)
	if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply = putArgs([]byte("x"), content, rangeID)
	if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}

	// Increments are a good way of testing the response cache. Up here, we
	// address them to the original range, then later to the one that contains
	// the key.
	rIncArgs := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     engine.Key("wobble"),
			Replica: proto.Replica{RangeID: rangeID},
			CmdID: proto.ClientCmdID{
				WallTime: 12,
				Random:   42,
			},
		},
		Increment: 10,
	}
	rIncReply := &proto.IncrementResponse{}
	if err := store.ExecuteCmd(Increment, rIncArgs, rIncReply); err != nil {
		t.Fatal(err)
	}

	lIncArgs := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     engine.Key("apoptosis"),
			Replica: proto.Replica{RangeID: rangeID},
			CmdID: proto.ClientCmdID{
				WallTime: 123,
				Random:   423,
			},
		},
		Increment: 100,
	}
	lIncReply := &proto.IncrementResponse{}
	if err := store.ExecuteCmd(Increment, lIncArgs, lIncReply); err != nil {
		t.Fatal(err)
	}

	args := &proto.InternalSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key:     engine.Key("b"), // Intentionally off.
			EndKey:  engine.Key("z"),
			Replica: proto.Replica{RangeID: rangeID},
		},
		SplitKey: splitKey,
	}
	reply := &proto.InternalSplitResponse{}
	err := store.ExecuteCmd(InternalSplit, args, reply)
	if err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}

	// Now fix the args and go again, this time expecting success.
	args.RequestHeader.Key = splitKey
	reply = &proto.InternalSplitResponse{}
	if err = store.ExecuteCmd(InternalSplit, args, reply); err != nil {
		t.Errorf("range split failed: %s", err)
	}
	rng, _ := store.GetRange(2)
	newRange, err := store.GetRange(3)
	if err != nil {
		t.Fatalf("no new range was created: %s", err)
	}
	nd := newRange.Meta.RangeDescriptor
	d := rng.Meta.RangeDescriptor
	if !bytes.Equal(nd.StartKey, splitKey) || !bytes.Equal(splitKey, d.EndKey) {
		t.Errorf("ranges mismatched, wanted %s=%s=%s", nd.StartKey, splitKey, d.EndKey)
	}
	if !bytes.Equal(nd.EndKey, engine.Key("z")) || !bytes.Equal(d.StartKey, engine.Key("a")) {
		t.Errorf("new ranges do not cover a-z, but only %s-%s", d.StartKey, nd.EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs, gReply := getArgs([]byte("c"), rangeID)
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("x"), newRange.Meta.RangeID)
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}

	// Send out an increment request copied from above (same ClientCmdID) which
	// remains in the old range.
	lIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(Increment, lIncArgs, lIncReply); err != nil {
		t.Fatal(err)
	}
	if lIncReply.NewValue != 100 {
		t.Errorf("response cache broken in old range, expected %d but got %d", lIncArgs.Increment, lIncReply.NewValue)
	}

	// Send out the same increment copied from above (same ClientCmdID), but
	// now to the newly created range (which should hold that key).
	rIncArgs.RequestHeader.Replica.RangeID = newRange.Meta.RangeID
	rIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(Increment, rIncArgs, rIncReply); err != nil {
		t.Fatal(err)
	}
	if rIncReply.NewValue != 10 {
		t.Errorf("response cache not copied correctly to new range, expected %d but got %d", rIncArgs.Increment, rIncReply.NewValue)
	}
}
