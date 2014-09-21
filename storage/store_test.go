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
	"sort"
	"testing"
	"time"

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
		t.Errorf("error bootstrapping store: %v", err)
	}

	// Try to get 1st range--non-existent.
	if _, err := store.GetRange(1); err == nil {
		t.Error("expected error fetching non-existent range")
	}

	// Create range and fetch.
	if _, err := store.CreateRange(store.BootstrapRangeMetadata()); err != nil {
		t.Errorf("failure to create first range: %v", err)
	}
	if _, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %v", err)
	}

	// Now, attempt to initialize a store with a now-bootstrapped engine.
	store = NewStore(clock, eng, nil, nil)
	if err := store.Init(); err != nil {
		t.Errorf("failure initializing bootstrapped store: %v", err)
	}
	// 1st range should be available.
	if _, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %v", err)
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)

	// Put some random garbage into the engine.
	if err := eng.Put(engine.Key("foo"), []byte("bar")); err != nil {
		t.Errorf("failure putting key foo into engine: %v", err)
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
	meta.EndKey = engine.PrefixEndKey(engine.KeySystemPrefix)
	_, err := store.CreateRange(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	// Now that the system key range is available, initialize the store. set store DB so new
	// ranges can be allocated as needed for tests.
	// If requested, create a default range for tests from "a"-"z".
	if createDefaultRange {
		replica = proto.Replica{StoreID: 1}
		_, err := store.CreateRange(store.NewRangeMetadata(engine.Key("a"), engine.Key("z"), []proto.Replica{replica}))
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
	args, reply := getArgs([]byte("a"), 2)

	// Try a successful get request.
	err := store.ExecuteCmd("Get", args, reply)
	if err != nil {
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
	err := store.ExecuteCmd("Get", args, reply)
	if err != nil {
		t.Fatal(err)
	}
	ts := store.clock.Timestamp()
	if ts.WallTime != args.Timestamp.WallTime || ts.Logical <= args.Timestamp.Logical {
		t.Errorf("expected store clock to advance to %+v; got %+v", args.Timestamp, ts)
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
	err := store.ExecuteCmd("Get", args, reply)
	if err != nil {
		t.Fatal(err)
	}
	// The Logical time will increase over the course of the command
	// execution so we can only rely on comparing the WallTime.
	if reply.Timestamp.WallTime != store.clock.Timestamp().WallTime {
		t.Errorf("expected reply to have store clock time %+v; got %+v",
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
	err := store.ExecuteCmd("Get", args, reply)
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
	err := store.ExecuteCmd("Get", args, reply)
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
	err := store.ExecuteCmd("Get", args, reply)
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
			t.Error("expected Raft id %d; got %d", 2+i, r.Meta.RaftID)
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
		pusher := NewTransaction(key, 1, proto.SERIALIZABLE, store.clock)
		pushee := NewTransaction(key, 1, proto.SERIALIZABLE, store.clock)
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
		if err := store.ExecuteCmd("Put", pArgs, pReply); err != nil {
			t.Fatal(err)
		}

		// Now, try a put using the pusher's txn.
		pArgs.Timestamp = store.clock.Now()
		pArgs.Txn = pusher
		err := store.ExecuteCmd("Put", pArgs, pReply)
		if err == nil {
			t.Errorf("resolvable? %t, expected write intent error", resolvable)
		}
		wiErr, ok := err.(*proto.WriteIntentError)
		if !ok {
			t.Errorf("resolvable? %t, expected write intent error; got %v", resolvable, err)
		}
		if !bytes.Equal(wiErr.Key, key) || !bytes.Equal(wiErr.Txn.ID, pushee.ID) ||
			wiErr.Resolved != resolvable {
			t.Errorf("resolvable? %t, unexpected values in write intent error: %+v", resolvable, wiErr)
		}

		// Trying again should succeed if wiErr.Resolved and fail otherwise.
		err = store.ExecuteCmd("Put", pArgs, pReply)
		if wiErr.Resolved && err != nil {
			t.Errorf("resolvable? %t, expected write to succeed: %v", resolvable, err)
		} else if !wiErr.Resolved && err == nil {
			t.Errorf("resolvable? %d, expected write intent error but succeeded", resolvable)
		}
	}
}

// TestStoreResolveWriteIntentRollback verifies that resolving a write
// intent by aborting it yields the previous value.
func TestStoreResolveWriteIntentRollback(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	key := engine.Key("a")
	pusher := NewTransaction(key, 1, proto.SERIALIZABLE, store.clock)
	pushee := NewTransaction(key, 1, proto.SERIALIZABLE, store.clock)
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
		t.Errorf("expected write to succeed: %v", err)
	}
	if reply.NewValue != 2 {
		t.Errorf("expected rollback of earlier increment to yield increment value of 2; got %d", reply.NewValue)
	}
}

// TestStoreResolveWriteIntentPushOnRead verifies that resolving a
// write intent for a read will push the timestamp.
func TestStoreResolveWriteIntentPushOnRead(t *testing.T) {
	store, _ := createTestStore(true, t)
	defer store.Close()

	key := engine.Key("a")
	pusher := NewTransaction(key, 1, proto.SERIALIZABLE, store.clock)
	pushee := NewTransaction(key, 1, proto.SNAPSHOT, store.clock)
	pushee.Priority = 1
	pusher.Priority = 2 // Pusher will win.

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
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil {
		t.Errorf("expected read %+v to succeed: %v", gArgs, err)
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
		t.Errorf("txn commit didn't yield expected status (COMMITTED) or timestamp %v: %+v",
			expTimestamp, etReply.Txn)
	}
}
