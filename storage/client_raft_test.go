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
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// TestStoreRecoverFromEngine verifies that the store recovers all ranges and their contents
// after being stopped and recreated.
func TestStoreRecoverFromEngine(t *testing.T) {
	raftID := int64(1)
	splitKey := proto.Key("m")
	key1 := proto.Key("a")
	key2 := proto.Key("z")

	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	var raftID2 int64

	get := func(store *storage.Store, raftID int64, key proto.Key) int64 {
		args, resp := getArgs(key, raftID, store.StoreID())
		err := store.ExecuteCmd(proto.Get, args, resp)
		if err != nil {
			t.Fatal(err)
		}
		return resp.Value.GetInteger()
	}
	validate := func(store *storage.Store) {
		if val := get(store, raftID, key1); val != 13 {
			t.Errorf("key %q: expected 13 but got %v", key1, val)
		}
		if val := get(store, raftID2, key2); val != 28 {
			t.Errorf("key %q: expected 28 but got %v", key2, val)
		}
	}

	// First, populate the store with data across two ranges. Each range contains commands
	// that both predate and postdate the split.
	func() {
		store := createTestStoreWithEngine(t, eng, clock, true)
		defer store.Stop()

		increment := func(raftID int64, key proto.Key, value int64) (*proto.IncrementResponse, error) {
			args, resp := incrementArgs(key, value, raftID, store.StoreID())
			err := store.ExecuteCmd(proto.Increment, args, resp)
			return resp, err
		}

		if _, err := increment(raftID, key1, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(raftID, key2, 5); err != nil {
			t.Fatal(err)
		}
		splitArgs, splitResp := adminSplitArgs(engine.KeyMin, splitKey, raftID, store.StoreID())
		if err := store.ExecuteCmd(proto.AdminSplit, splitArgs, splitResp); err != nil {
			t.Fatal(err)
		}
		raftID2 = store.LookupRange(key2, nil).Desc.RaftID
		if raftID2 == raftID {
			t.Errorf("got same raft id after split")
		}
		if _, err := increment(raftID, key1, 11); err != nil {
			t.Fatal(err)
		}
		if _, err := increment(raftID2, key2, 23); err != nil {
			t.Fatal(err)
		}
		validate(store)
	}()

	// Now create a new store with the same engine and make sure the expected data is present.
	// We must use the same clock because a newly-created manual clock will be behind the one
	// we wrote with and so will see stale MVCC data.
	store := createTestStoreWithEngine(t, eng, clock, false)
	defer store.Stop()

	validate(store)
}
