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
	replica := proto.Replica{StoreID: store.Ident.StoreID, RangeID: 1}
	if _, err := store.CreateRange(engine.KeyMin, engine.KeyMax, []proto.Replica{replica}); err != nil {
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
	store.Ident.StoreID = 1
	replica := proto.Replica{StoreID: 1, RangeID: 1}
	// Create system key range for allocations.
	_, err := store.CreateRange(engine.KeySystemPrefix, engine.PrefixEndKey(engine.KeySystemPrefix), []proto.Replica{replica})
	if err != nil {
		t.Fatal(err)
	}
	// Now that the system key range is available, set store DB so new
	// ranges can be allocated as needed for tests.
	db, _ := newTestDB(store)
	store.db = db
	// If requested, create a default range for tests from "a"-"z".
	if createDefaultRange {
		replica = proto.Replica{StoreID: 1, RangeID: 2}
		_, err := store.CreateRange(engine.Key("a"), engine.Key("z"), []proto.Replica{replica})
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
	r, err := store.CreateRange(start, end, replicas)
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
