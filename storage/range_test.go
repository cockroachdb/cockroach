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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/hlc"
)

var (
	testRangeDescriptor = RangeDescriptor{
		StartKey: KeyMin,
		Replicas: []Replica{
			{
				NodeID:  1,
				StoreID: 1,
				RangeID: 1,
				Attrs:   Attributes([]string{"dc1", "mem"}),
			},
			{
				NodeID:  2,
				StoreID: 1,
				RangeID: 1,
				Attrs:   Attributes([]string{"dc2", "mem"}),
			},
		},
	}
	testDefaultAcctConfig = AcctConfig{}
	testDefaultPermConfig = PermConfig{
		Read:  []string{"root"},
		Write: []string{"root"},
	}
	testDefaultZoneConfig = ZoneConfig{
		Replicas: []Attributes{
			Attributes([]string{"dc1", "mem"}),
			Attributes([]string{"dc2", "mem"}),
		},
	}
)

// createTestEngine creates an in-memory engine and initializes some
// default configuration settings.
func createTestEngine(t *testing.T) Engine {
	engine := NewInMem(Attributes([]string{"dc1", "mem"}), 1<<20)
	if err := putI(engine, KeyConfigAccountingPrefix, testDefaultAcctConfig); err != nil {
		t.Fatal(err)
	}
	if err := putI(engine, KeyConfigPermissionPrefix, testDefaultPermConfig); err != nil {
		t.Fatal(err)
	}
	if err := putI(engine, KeyConfigZonePrefix, testDefaultZoneConfig); err != nil {
		t.Fatal(err)
	}
	return engine
}

// createTestRange creates a new range initialized to the full extent
// of the keyspace. The gossip instance is also returned for testing.
func createTestRange(engine Engine, t *testing.T) (*Range, *gossip.Gossip) {
	rm := RangeMetadata{
		RangeID:  0,
		StartKey: KeyMin,
		EndKey:   KeyMax,
		Desc:     testRangeDescriptor,
	}
	g := gossip.New()
	clock := hlc.NewHLClock(hlc.UnixNano)
	r := NewRange(rm, clock, engine, nil, g)
	r.Start()
	return r, g
}

// TestRangeContains verifies methods to check whether a key or key range
// is contained within the range.
func TestRangeContains(t *testing.T) {
	r, _ := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	r.Meta.StartKey = Key("a")
	r.Meta.EndKey = Key("b")

	testData := []struct {
		start, end Key
		contains   bool
	}{
		// Single keys.
		{Key("a"), Key("a"), true},
		{Key("aa"), Key("aa"), true},
		{Key("`"), Key("`"), false},
		{Key("b"), Key("b"), false},
		{Key("c"), Key("c"), false},
		// Key ranges.
		{Key("a"), Key("b"), true},
		{Key("a"), Key("aa"), true},
		{Key("aa"), Key("b"), true},
		{Key("0"), Key("9"), false},
		{Key("`"), Key("a"), false},
		{Key("b"), Key("bb"), false},
		{Key("0"), Key("bb"), false},
		{Key("aa"), Key("bb"), false},
	}
	for _, test := range testData {
		if bytes.Compare(test.start, test.end) == 0 {
			if r.ContainsKey(test.start) != test.contains {
				t.Errorf("expected key %q within range", test.start)
			}
		} else {
			if r.ContainsKeyRange(test.start, test.end) != test.contains {
				t.Errorf("expected key range %q-%q within range", test.start, test.end)
			}
		}
	}
}

// TestRangeGossipFirstRange verifies that the first range gossips its location.
func TestRangeGossipFirstRange(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	info, err := g.GetInfo(gossip.KeyFirstRangeMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.(RangeDescriptor), testRangeDescriptor) {
		t.Errorf("expected gossipped range locations to be equal: %s vs %s", info.(RangeDescriptor), testRangeDescriptor)
	}
}

// TestRangeGossipAllConfigs verifies that all config types are
// gossipped.
func TestRangeGossipAllConfigs(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	testData := []struct {
		gossipKey string
		configs   []*PrefixConfig
	}{
		{gossip.KeyConfigAccounting, []*PrefixConfig{&PrefixConfig{KeyMin, nil, &testDefaultAcctConfig}}},
		{gossip.KeyConfigPermission, []*PrefixConfig{&PrefixConfig{KeyMin, nil, &testDefaultPermConfig}}},
		{gossip.KeyConfigZone, []*PrefixConfig{&PrefixConfig{KeyMin, nil, &testDefaultZoneConfig}}},
	}
	for _, test := range testData {
		info, err := g.GetInfo(test.gossipKey)
		if err != nil {
			t.Fatal(err)
		}
		configMap := info.(PrefixConfigMap)
		expConfigs := []*PrefixConfig{test.configs[0]}
		if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
			t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
		}
	}
}

// TestRangeGossipConfigWithMultipleKeyPrefixes verifies that multiple
// key prefixes for a config are gossipped.
func TestRangeGossipConfigWithMultipleKeyPrefixes(t *testing.T) {
	engine := createTestEngine(t)
	// Add a permission for a new key prefix.
	db1Perm := PermConfig{
		Read:  []string{"spencer", "foo", "bar", "baz"},
		Write: []string{"spencer"},
	}
	key := MakeKey(KeyConfigPermissionPrefix, Key("/db1"))
	if err := putI(engine, key, db1Perm); err != nil {
		t.Fatal(err)
	}
	r, g := createTestRange(engine, t)
	defer r.Stop()

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		&PrefixConfig{KeyMin, nil, &testDefaultPermConfig},
		&PrefixConfig{Key("/db1"), nil, &db1Perm},
		&PrefixConfig{Key("/db2"), KeyMin, &testDefaultPermConfig},
	}
	if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
	}
}

// TestRangeGossipConfigUpdates verifies that writes to the
// permissions cause the updated configs to be re-gossipped.
func TestRangeGossipConfigUpdates(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	// Add a permission for a new key prefix.
	db1Perm := PermConfig{
		Read:  []string{"spencer"},
		Write: []string{"spencer"},
	}
	key := MakeKey(KeyConfigPermissionPrefix, Key("/db1"))
	reply := &PutResponse{}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(db1Perm); err != nil {
		t.Fatal(err)
	}
	r.Put(&PutRequest{RequestHeader: RequestHeader{Key: key}, Value: Value{Bytes: buf.Bytes()}}, reply)
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		&PrefixConfig{KeyMin, nil, &testDefaultPermConfig},
		&PrefixConfig{Key("/db1"), nil, &db1Perm},
		&PrefixConfig{Key("/db2"), KeyMin, &testDefaultPermConfig},
	}
	if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
	}
}

func TestInternalRangeLookup(t *testing.T) {
	// TODO(Spencer): test, esp. for correct key range scanned
}

// A blockingEngine allows us to delay writes in order to test the
// pending read queue.
type blockingEngine struct {
	*InMem
	mu    sync.Mutex
	cvar  *sync.Cond
	block bool
}

func newBlockingEngine() *blockingEngine {
	be := &blockingEngine{
		InMem: NewInMem(Attributes{}, 1<<20),
	}
	be.cvar = sync.NewCond(&be.mu)
	return be
}

func (be *blockingEngine) setBlock(block bool) {
	be.mu.Lock()
	defer be.mu.Unlock()
	be.block = block
	if !be.block {
		be.cvar.Broadcast()
	}
}

func (be *blockingEngine) put(key Key, value []byte) error {
	be.mu.Lock()
	defer be.mu.Unlock()
	for be.block {
		be.cvar.Wait()
	}
	return be.InMem.put(key, value)
}

// createTestRange creates a range using a blocking engine. Returns
// the range clock's manual unix nanos time and the range.
func createTestRangeWithClock(t *testing.T) (*Range, *hlc.ManualClock, *blockingEngine) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewHLClock(manual.UnixNano)
	engine := newBlockingEngine()
	rng := NewRange(RangeMetadata{}, clock, engine, nil, nil)
	rng.Start()
	return rng, &manual, engine
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key string, rangeID int64) (*GetRequest, *GetResponse) {
	args := &GetRequest{
		RequestHeader: RequestHeader{
			Key:     []byte(key),
			Replica: Replica{RangeID: rangeID},
		},
	}
	reply := &GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value string, rangeID int64) (*PutRequest, *PutResponse) {
	args := &PutRequest{
		RequestHeader: RequestHeader{
			Key:     []byte(key),
			Replica: Replica{RangeID: rangeID},
		},
		Value: Value{
			Bytes: []byte(value),
		},
	}
	reply := &PutResponse{}
	return args, reply
}

// TestRangeUpdateTSCache verifies that reads update the read
// timestamp cache.
func TestRangeUpdateTSCache(t *testing.T) {
	rng, mc, _ := createTestRangeWithClock(t)
	defer rng.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	*mc = hlc.ManualClock(t0.Nanoseconds())
	args, reply := getArgs("a", 0)
	args.Timestamp = rng.tsCache.clock.Now()
	err := rng.ReadOnlyCmd("Get", &args.RequestHeader, args, reply)
	if err != nil {
		t.Error(err)
	}
	// Verify the read timestamp cache has 1sec for "a".
	ts := rng.tsCache.GetMax(Key("a"), nil)
	if ts.WallTime != t0.Nanoseconds() {
		t.Errorf("expected wall time to have 1s, but got %+v", ts)
	}
	// Verify another key ("b") has 0sec in timestamp cache.
	ts = rng.tsCache.GetMax(Key("b"), nil)
	if ts.WallTime != 0 {
		t.Errorf("expected wall time to have 0s, but got %+v", ts)
	}
}

// TestRangeReadQueue verifies that reads must wait for writes to
// complete through Raft before being executed on range.
func TestRangeReadQueue(t *testing.T) {
	rng, _, be := createTestRangeWithClock(t)
	defer rng.Stop()

	// Asynchronously put a value to the rng with blocking enabled.
	be.setBlock(true)
	writeDone := make(chan struct{})
	go func() {
		args, reply := putArgs("a", "value", 0)
		err := rng.ReadWriteCmd("Put", &args.RequestHeader, args, reply)
		if err != nil {
			t.Fatal(err)
		}
		close(writeDone)
	}()

	// First, try a read for a non-impacted key ("b").
	readBDone := make(chan struct{})
	go func() {
		args, reply := getArgs("b", 0)
		err := rng.ReadOnlyCmd("Get", &args.RequestHeader, args, reply)
		if err != nil {
			t.Error(err)
		}
		close(readBDone)
	}()

	// Next, try a read for same key being written to verify it blocks.
	readADone := make(chan struct{})
	go func() {
		args, reply := getArgs("a", 0)
		err := rng.ReadOnlyCmd("Get", &args.RequestHeader, args, reply)
		if err != nil {
			t.Error(err)
		}
		close(readADone)
	}()

	// Verify read of "b" finishes but not "a".
	select {
	case <-readADone:
		t.Fatal("should not have been able to read \"a\"")
	case <-readBDone:
		// success.
	case <-writeDone:
		t.Fatal("should not have been able to write \"a\" while blocked")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waited 500ms for read of \"b\"")
	}

	// Now, unblock write.
	be.setBlock(false)

	select {
	case <-readADone:
		// success.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waited 500ms for read of \"a\"")
	}
}

// TestRangeUseTSCache verifies that write timestamps are upgraded
// based on the read timestamp cache.
func TestRangeUseTSCache(t *testing.T) {
	rng, mc, _ := createTestRangeWithClock(t)
	defer rng.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	*mc = hlc.ManualClock(t0.Nanoseconds())
	args, reply := getArgs("a", 0)
	args.Timestamp = rng.tsCache.clock.Now()
	err := rng.ReadOnlyCmd("Get", &args.RequestHeader, args, reply)
	if err != nil {
		t.Error(err)
	}
	pArgs, pReply := putArgs("a", "value", 0)
	err = rng.ReadWriteCmd("Put", &pArgs.RequestHeader, pArgs, pReply)
	if err != nil {
		t.Fatal(err)
	}
	if pReply.Timestamp.WallTime != rng.tsCache.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %+v", pReply.Timestamp)
	}
}
