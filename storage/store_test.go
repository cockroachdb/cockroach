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
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

var testIdent = proto.StoreIdent{
	ClusterID: "cluster",
	NodeID:    1,
	StoreID:   1,
}

// createTestStore creates a test store using an in-memory
// engine. Returns the store clock's manual unix nanos time and the
// store. The caller is responsible for closing the store on exit.
func createTestStore(t *testing.T) (*Store, *hlc.ManualClock) {
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := gossip.New(rpcContext)
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	store := NewStore(clock, eng, nil, g)
	if err := store.Bootstrap(proto.StoreIdent{StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	db, _ := newTestDB(store)
	store.db = db
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	initConfigs(eng, t)
	if _, err := store.BootstrapRange(); err != nil {
		t.Fatal(err)
	}
	return store, &manual
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
	if rng, err := store.BootstrapRange(); rng == nil || err != nil {
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
			Desc: &proto.RangeDescriptor{StartKey: key},
		})
	}

	sort.Sort(rs)
	for i := 0; i < 5; i++ {
		expectedKey := engine.Key(fmt.Sprintf("foo%d", i))
		if !bytes.Equal(rs[i].Desc.StartKey, expectedKey) {
			t.Errorf("Expected %s, got %s", expectedKey, rs[i].Desc.StartKey)
		}
	}
}

// TestStoreExecuteCmd verifies straightforward command execution
// of both a read-only and a read-write command.
func TestStoreExecuteCmd(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()
	gArgs, gReply := getArgs([]byte("a"), 1)

	// Try a successful get request.
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil {
		t.Fatal(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("aaa"), 1)
	if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}
}

// TestStoreVerifyKeys checks that key length is enforced and
// that end keys must sort >= start.
func TestStoreVerifyKeys(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()
	tooLongKey := engine.MakeKey(engine.KeyMax, []byte{0})

	// Start with a too-long key on a get.
	gArgs, gReply := getArgs(tooLongKey, 1)
	if err := store.ExecuteCmd(Get, gArgs, gReply); err == nil {
		t.Fatal("expected error for key too long")
	}
	// Try a start key == KeyMax.
	gArgs.Key = engine.KeyMax
	if err := store.ExecuteCmd(Get, gArgs, gReply); err == nil {
		t.Fatal("expected error for start key == KeyMax")
	}
	// Try a scan with too-long EndKey.
	sArgs, sReply := scanArgs(engine.KeyMin, tooLongKey, 1)
	if err := store.ExecuteCmd(Scan, sArgs, sReply); err == nil {
		t.Fatal("expected error for end key too long")
	}
	// Try a scan with end key < start key.
	sArgs.Key = []byte("b")
	sArgs.EndKey = []byte("a")
	if err := store.ExecuteCmd(Scan, sArgs, sReply); err == nil {
		t.Fatal("expected error for end key < start")
	}
	// Try a put to meta2 key which would otherwise exceed maximum key
	// length, but is accepted because of the meta prefix.
	pArgs, pReply := putArgs(engine.MakeKey(engine.KeyMeta2Prefix, engine.KeyMax), []byte("value"), 1)
	if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", err)
	}
	// Try a put to txn record for a meta2 key.
	pArgs, pReply = putArgs(engine.MakeKey(engine.KeyLocalTransactionPrefix,
		engine.KeyMeta2Prefix, engine.KeyMax), []byte("value"), 1)
	if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
		t.Fatalf("unexpected error on put to meta2 value: %s", err)
	}
}

// TestStoreExecuteCmdUpdateTime verifies that the node clock is updated.
func TestStoreExecuteCmdUpdateTime(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()
	args, reply := getArgs([]byte("a"), 1)
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
	store, mc := createTestStore(t)
	defer store.Close()
	args, reply := getArgs([]byte("a"), 1)

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

// TestStoreExecuteCmdWithClockOffset verifies that if the request
// specifies a timestamp further into the future than the node's
// maximum allowed clock offset, the cmd fails with an error.
func TestStoreExecuteCmdWithClockOffset(t *testing.T) {
	store, mc := createTestStore(t)
	defer store.Close()
	args, reply := getArgs([]byte("a"), 1)

	// Set clock to time 1.
	*mc = hlc.ManualClock(1)
	// Set clock max offset to 250ms.
	maxOffset := 250 * time.Millisecond
	store.clock.SetMaxOffset(maxOffset)
	// Set args timestamp to exceed max offset.
	args.Timestamp = store.clock.Now()
	args.Timestamp.WallTime += maxOffset.Nanoseconds() + 1
	err := store.ExecuteCmd(Get, args, reply)
	if err == nil {
		t.Error("expected max offset clock error")
	}
}

// TestStoreExecuteCmdBadRange passes a bad range.
func TestStoreExecuteCmdBadRange(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()
	args, reply := getArgs([]byte("0"), 2) // no range ID 2
	err := store.ExecuteCmd(Get, args, reply)
	if err == nil {
		t.Error("expected invalid range")
	}
}

func splitTestRange(store *Store, key, splitKey engine.Key, t *testing.T) *Range {
	rng := store.LookupRange(key, key)
	if rng == nil {
		t.Fatalf("couldn't lookup range for key %q", key)
	}
	desc, err := store.NewRangeDescriptor(splitKey, rng.Desc.EndKey, rng.Desc.Replicas)
	if err != nil {
		t.Fatal(err)
	}
	newRng := NewRange(desc.FindReplica(store.StoreID()).RangeID, desc, store)
	if err := store.SplitRange(rng, newRng); err != nil {
		t.Fatal(err)
	}
	return newRng
}

// TestStoreExecuteCmdOutOfRange passes a key not contained
// within the range's key range.
func TestStoreExecuteCmdOutOfRange(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()
	// Split the range and then remove the second half to clear up some space.
	rng := splitTestRange(store, engine.KeyMin, engine.Key("a"), t)
	if err := store.RemoveRange(rng); err != nil {
		t.Fatal(err)
	}
	// Range is from KeyMin to "a", so reading "a" should fail because
	// it's just outside the range boundary.
	args, reply := getArgs([]byte("a"), 1)
	err := store.ExecuteCmd(Get, args, reply)
	if err == nil {
		t.Error("expected key to be out of range")
	}
}

// TestStoreRaftIDAllocation verifies that raft IDs are
// allocated in successive blocks.
func TestStoreRaftIDAllocation(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	// Raft IDs should be allocated from ID 2 (first alloc'd range)
	// to raftIDAllocCount * 3 + 1.
	for i := 0; i < raftIDAllocCount*3; i++ {
		replicas := []proto.Replica{{StoreID: store.StoreID()}}
		desc, err := store.NewRangeDescriptor(engine.Key(fmt.Sprintf("%03d", i)), engine.Key(fmt.Sprintf("%03d", i+1)), replicas)
		if err != nil {
			t.Fatal(err)
		}
		if desc.RaftID != int64(2+i) {
			t.Errorf("expected Raft id %d; got %d", 2+i, desc.RaftID)
		}
	}
}

// TestStoreRangesByKey verifies we can lookup ranges by key using
// the sorted rangesByKey slice.
func TestStoreRangesByKey(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	r0 := store.LookupRange(engine.KeyMin, engine.KeyMin)
	r1 := splitTestRange(store, engine.KeyMin, engine.Key("A"), t)
	r2 := splitTestRange(store, engine.Key("A"), engine.Key("C"), t)
	r3 := splitTestRange(store, engine.Key("C"), engine.Key("X"), t)
	r4 := splitTestRange(store, engine.Key("X"), engine.Key("ZZ"), t)

	if r := store.LookupRange(engine.Key("0"), nil); r != r0 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r0.Desc)
	}
	if r := store.LookupRange(engine.Key("B"), nil); r != r1 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r1.Desc)
	}
	if r := store.LookupRange(engine.Key("C"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r2.Desc)
	}
	if r := store.LookupRange(engine.Key("M"), nil); r != r2 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r2.Desc)
	}
	if r := store.LookupRange(engine.Key("X"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r3.Desc)
	}
	if r := store.LookupRange(engine.Key("Z"), nil); r != r3 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r3.Desc)
	}
	if r := store.LookupRange(engine.Key("ZZ"), nil); r != r4 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r4.Desc)
	}
	if r := store.LookupRange(engine.KeyMax[:engine.KeyMaxLength-1], nil); r != r4 {
		t.Errorf("mismatched range %+v != %+v", r.Desc, r4.Desc)
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
	store, _ := createTestStore(t)
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
		pArgs, pReply := putArgs(key, []byte("value"), 1)
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
	store, _ := createTestStore(t)
	defer store.Close()

	key := engine.Key("a")
	pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee.Priority = 1
	pusher.Priority = 2 // Pusher will win.

	// First lay down intent using the pushee's txn.
	args, reply := incrementArgs(key, 1, 1)
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
	store, _ := createTestStore(t)
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
		args, reply := putArgs(key, []byte("value1"), 1)
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
		gArgs, gReply := getArgs(key, 1)
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
			etArgs, etReply := endTxnArgs(pushee, true, 1)
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
	store, _ := createTestStore(t)
	defer store.Close()

	key := engine.Key("a")
	pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee := NewTransaction("test", key, 1, proto.SNAPSHOT, store.clock)
	pushee.Priority = 2
	pusher.Priority = 1 // Pusher would lose based on priority.

	// First, write original value.
	args, reply := putArgs(key, []byte("value1"), 1)
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
	gArgs, gReply := getArgs(key, 1)
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
	etArgs, etReply := endTxnArgs(pushee, true, 1)
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
	store, _ := createTestStore(t)
	defer store.Close()

	key := engine.Key("a")
	pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, store.clock)
	pushee.Priority = 0 // pushee should lose all conflicts

	// First, lay down intent from pushee.
	args, reply := putArgs(key, []byte("value1"), 1)
	args.Timestamp = pushee.Timestamp
	args.Txn = pushee
	if err := store.ExecuteCmd(Put, args, reply); err != nil {
		t.Fatal(err)
	}

	// Now, try to read outside a transaction. The non-transactional reader
	// will use a random priority, which is likely to be > 1, so should win.
	gArgs, gReply := getArgs(key, 1)
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
	etArgs, etReply := endTxnArgs(pushee, true, 1)
	etArgs.Timestamp = pushee.Timestamp
	err = store.ExecuteCmd(EndTransaction, etArgs, etReply)
	if err == nil {
		t.Errorf("unexpected success committing transaction")
	}
	if _, ok := err.(*proto.TransactionAbortedError); !ok {
		t.Errorf("expected transaction aborted error; got %s", err)
	}
}

func adminSplitArgs(key, splitKey []byte, rangeID int64) (*proto.AdminSplitRequest, *proto.AdminSplitResponse) {
	args := &proto.AdminSplitRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
		},
		SplitKey: splitKey,
	}
	reply := &proto.AdminSplitResponse{}
	return args, reply
}

// TestStoreRangeSplitAtMeta1 verifies a range cannot be split at
// a meta1 key.
func TestStoreRangeSplitAtMeta1(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	args, reply := adminSplitArgs(engine.KeyMin, engine.MakeKey(engine.KeyMeta1Prefix, engine.KeyMax), 1)
	err := store.ExecuteCmd(AdminSplit, args, reply)
	if err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
}

// TestStoreRangeSplitAtRangeBounds verifies a range cannot be split
// at its start or end keys (would create zero-length range!). This
// sort of thing might happen in the wild if two split requests
// arrived for same key.  first one succeeds and second would try to
// split at the start of the newly split range.
func TestStoreRangeSplitAtRangeBounds(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	args, reply := adminSplitArgs(engine.KeyMin, []byte("a"), 1)
	if err := store.ExecuteCmd(AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}
	// This second split will try to split at end of first split range.
	if err := store.ExecuteCmd(AdminSplit, args, reply); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
	// Now try to split at start of new range.
	args, reply = adminSplitArgs(engine.KeyMin, []byte("a"), 2)
	if err := store.ExecuteCmd(AdminSplit, args, reply); err == nil {
		t.Fatalf("split succeeded unexpectedly")
	}
}

// TestStoreRangeSplitConcurrent verifies that concurrent range splits
// of the same range are disallowed.
func TestStoreRangeSplitConcurrent(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	concurrentCount := int32(10)
	wg := sync.WaitGroup{}
	wg.Add(int(concurrentCount))
	failureCount := int32(0)
	for i := int32(0); i < concurrentCount; i++ {
		go func() {
			args, reply := adminSplitArgs(engine.KeyMin, []byte("a"), 1)
			err := store.ExecuteCmd(AdminSplit, args, reply)
			if err != nil {
				if matched, regexpErr := regexp.MatchString(".*already splitting range 1", err.Error()); !matched || regexpErr != nil {
					t.Errorf("error %s didn't match: %s", err, regexpErr)
				} else {
					atomic.AddInt32(&failureCount, 1)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
	if failureCount != concurrentCount-1 {
		t.Fatalf("concurrent splits succeeded unexpectedly")
	}
}

// TestStoreRangeSplit executes a split of a range and verifies that the
// resulting ranges respond to the right key ranges and that their stats
// and response caches have been properly accounted for.
func TestStoreRangeSplit(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()
	rangeID := int64(1)
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
	lIncArgs, lIncReply := incrementArgs([]byte("apoptosis"), 100, rangeID)
	lIncArgs.CmdID = proto.ClientCmdID{WallTime: 123, Random: 423}
	if err := store.ExecuteCmd(Increment, lIncArgs, lIncReply); err != nil {
		t.Fatal(err)
	}
	rIncArgs, rIncReply := incrementArgs([]byte("wobble"), 10, rangeID)
	rIncArgs.CmdID = proto.ClientCmdID{WallTime: 12, Random: 42}
	if err := store.ExecuteCmd(Increment, rIncArgs, rIncReply); err != nil {
		t.Fatal(err)
	}

	// Get the original stats for key and value bytes.
	keyBytes, err := engine.GetRangeStat(store.engine, rangeID, engine.StatKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	valBytes, err := engine.GetRangeStat(store.engine, rangeID, engine.StatValBytes)
	if err != nil {
		t.Fatal(err)
	}

	// Split the range.
	args, reply := adminSplitArgs(engine.KeyMin, splitKey, 1)
	if err := store.ExecuteCmd(AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}

	rng := store.LookupRange(engine.KeyMin, nil)
	newRng := store.LookupRange([]byte("m"), nil)
	if !bytes.Equal(newRng.Desc.StartKey, splitKey) || !bytes.Equal(splitKey, rng.Desc.EndKey) {
		t.Errorf("ranges mismatched, wanted %q=%q=%q", newRng.Desc.StartKey, splitKey, rng.Desc.EndKey)
	}
	if !bytes.Equal(newRng.Desc.EndKey, engine.KeyMax) || !bytes.Equal(rng.Desc.StartKey, engine.KeyMin) {
		t.Errorf("new ranges do not cover KeyMin-KeyMax, but only %q-%q", rng.Desc.StartKey, newRng.Desc.EndKey)
	}

	// Try to get values from both left and right of where the split happened.
	gArgs, gReply := getArgs([]byte("c"), rangeID)
	if err := store.ExecuteCmd(Get, gArgs, gReply); err != nil ||
		!bytes.Equal(gReply.Value.Bytes, content) {
		t.Fatal(err)
	}
	gArgs, gReply = getArgs([]byte("x"), newRng.RangeID)
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
	rIncArgs.RequestHeader.Replica.RangeID = newRng.RangeID
	rIncReply = &proto.IncrementResponse{}
	if err := store.ExecuteCmd(Increment, rIncArgs, rIncReply); err != nil {
		t.Fatal(err)
	}
	if rIncReply.NewValue != 10 {
		t.Errorf("response cache not copied correctly to new range, expected %d but got %d", rIncArgs.Increment, rIncReply.NewValue)
	}

	// Compare stats of split ranges to ensure they are non ero and
	// exceed the original range when summed.
	lKeyBytes, err := engine.GetRangeStat(store.engine, rangeID, engine.StatKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	lValBytes, err := engine.GetRangeStat(store.engine, rangeID, engine.StatValBytes)
	if err != nil {
		t.Fatal(err)
	}
	rKeyBytes, err := engine.GetRangeStat(store.engine, newRng.RangeID, engine.StatKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	rValBytes, err := engine.GetRangeStat(store.engine, newRng.RangeID, engine.StatValBytes)
	if err != nil {
		t.Fatal(err)
	}

	if lKeyBytes == 0 || rKeyBytes == 0 {
		t.Errorf("expected non-zero key bytes; got %d, %d", lKeyBytes, rKeyBytes)
	}
	if lValBytes == 0 || rValBytes == 0 {
		t.Errorf("expected non-zero val bytes; got %d, %d", lValBytes, rValBytes)
	}
	if lKeyBytes+rKeyBytes <= keyBytes {
		t.Errorf("left + right key bytes don't match; %d + %d <= %d", lKeyBytes, rKeyBytes, keyBytes)
	}
	if lValBytes+rValBytes <= valBytes {
		t.Errorf("left + right val bytes don't match; %d + %d <= %d", lValBytes, rValBytes, valBytes)
	}
}

// TestStoreRangeSplitStats starts by splitting the system keys from user-space
// keys and verifying that the user space side of the split (which is empty),
// has all zeros for stats. It then writes random data to the user space side,
// splits it halfway and verifies the two splits have stats exactly equaling
// the pre-split.
func TestStoreRangeSplitStats(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	// Split the range at the first user key.
	args, reply := adminSplitArgs(engine.KeyMin, engine.Key("\x01"), 1)
	if err := store.ExecuteCmd(AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}
	// Verify empty range has empty stats.
	rng := store.LookupRange(engine.Key("\x01"), nil)
	verifyRangeStats(store.Engine(), rng.RangeID, engine.MVCCStats{}, t)

	// Write random data.
	src := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		key := []byte(util.RandString(src, int(src.Int31n(1<<7))))
		val := []byte(util.RandString(src, int(src.Int31n(1<<8))))
		pArgs, pReply := putArgs(key, val, rng.RangeID)
		pArgs.Timestamp = store.Clock().Now()
		if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
			t.Fatal(err)
		}
	}
	// Get the range stats now that we have data.
	ms, err := engine.GetRangeMVCCStats(store.Engine(), rng.RangeID)
	if err != nil {
		t.Fatal(err)
	}

	// Split the range at approximate halfway point ("Z" in string "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz").
	args, reply = adminSplitArgs(engine.Key("\x01"), engine.Key("Z"), rng.RangeID)
	if err := store.ExecuteCmd(AdminSplit, args, reply); err != nil {
		t.Fatal(err)
	}

	msLeft, err := engine.GetRangeMVCCStats(store.Engine(), rng.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	rngRight := store.LookupRange(engine.Key("Z"), nil)
	msRight, err := engine.GetRangeMVCCStats(store.Engine(), rngRight.RangeID)
	if err != nil {
		t.Fatal(err)
	}

	// The stats should be exactly equal when added.
	expMS := engine.MVCCStats{
		LiveBytes:   msLeft.LiveBytes + msRight.LiveBytes,
		KeyBytes:    msLeft.KeyBytes + msRight.KeyBytes,
		ValBytes:    msLeft.ValBytes + msRight.ValBytes,
		IntentBytes: msLeft.IntentBytes + msRight.IntentBytes,
		LiveCount:   msLeft.LiveCount + msRight.LiveCount,
		KeyCount:    msLeft.KeyCount + msRight.KeyCount,
		ValCount:    msLeft.ValCount + msRight.ValCount,
		IntentCount: msLeft.IntentCount + msRight.IntentCount,
	}
	if !reflect.DeepEqual(expMS, *ms) {
		t.Errorf("expected left and right ranges to equal original: %+v + %+v != %+v", msLeft, msRight, ms)
	}
}

// fillRange writes keys with the given prefix and associated values
// until bytes bytes have been written.
func fillRange(store *Store, rangeID int64, prefix engine.Key, bytes int64, t *testing.T) {
	src := rand.New(rand.NewSource(0))
	for {
		keyBytes, err := engine.GetRangeStat(store.engine, rangeID, engine.StatKeyBytes)
		if err != nil {
			t.Fatal(err)
		}
		valBytes, err := engine.GetRangeStat(store.engine, rangeID, engine.StatValBytes)
		if err != nil {
			t.Fatal(err)
		}
		if keyBytes+valBytes >= bytes {
			return
		}
		key := append(append([]byte(nil), prefix...), []byte(util.RandString(src, 100))...)
		val := []byte(util.RandString(src, int(src.Int31n(1<<8))))
		pArgs, pReply := putArgs(key, val, rangeID)
		pArgs.Timestamp = store.Clock().Now()
		if err := store.ExecuteCmd(Put, pArgs, pReply); err != nil {
			t.Fatal(err)
		}
	}
}

// TestStoreShouldSplit verifies that shouldSplit() takes into account the
// zone configuration to figure out what the maximum size of a range is.
// It further verifies that the range is in fact split on exceeding
// zone's RangeMaxBytes.
func TestStoreShouldSplit(t *testing.T) {
	store, _ := createTestStore(t)
	defer store.Close()

	rng := store.LookupRange(engine.KeyMin, nil)
	if ok := rng.shouldSplit(); ok {
		t.Errorf("range should not split with no data in it")
	}

	maxBytes := testDefaultZoneConfig.RangeMaxBytes
	fillRange(store, rng.RangeID, engine.Key("test"), maxBytes, t)

	if ok := rng.shouldSplit(); !ok {
		t.Errorf("range should after writing %d bytes", maxBytes)
	}

	// Verify that the range is in fact split (give it a second for very slow test machines).
	if err := util.IsTrueWithin(func() bool {
		newRng := store.LookupRange(engine.KeyMax[:engine.KeyMaxLength-1], nil)
		return newRng != rng
	}, time.Second); err != nil {
		t.Errorf("expected range to split in 1s")
	}
}
