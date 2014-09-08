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

package storage

import (
	"bytes"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/hlc"
)

var (
	testRangeDescriptor = proto.RangeDescriptor{
		StartKey: engine.KeyMin,
		EndKey:   engine.KeyMax,
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
				RangeID: 1,
				Attrs:   proto.Attributes{Attrs: []string{"dc1", "mem"}},
			},
			{
				NodeID:  2,
				StoreID: 1,
				RangeID: 1,
				Attrs:   proto.Attributes{Attrs: []string{"dc2", "mem"}},
			},
		},
	}
	testDefaultAcctConfig = proto.AcctConfig{}
	testDefaultPermConfig = proto.PermConfig{
		Read:  []string{"root"},
		Write: []string{"root"},
	}
	testDefaultZoneConfig = proto.ZoneConfig{
		Replicas: []proto.Attributes{
			proto.Attributes{Attrs: []string{"dc1", "mem"}},
			proto.Attributes{Attrs: []string{"dc2", "mem"}},
		},
	}
)

// createTestEngine creates an in-memory engine and initializes some
// default configuration settings.
func createTestEngine(t *testing.T) engine.Engine {
	e := engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
	mvcc := engine.NewMVCC(e)
	if err := mvcc.PutProto(engine.KeyConfigAccountingPrefix, proto.MinTimestamp, nil, &testDefaultAcctConfig); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.PutProto(engine.KeyConfigPermissionPrefix, proto.MinTimestamp, nil, &testDefaultPermConfig); err != nil {
		t.Fatal(err)
	}
	if err := mvcc.PutProto(engine.KeyConfigZonePrefix, proto.MinTimestamp, nil, &testDefaultZoneConfig); err != nil {
		t.Fatal(err)
	}
	return e
}

// createTestRange creates a new range initialized to the full extent
// of the keyspace. The gossip instance is also returned for testing.
func createTestRange(engine engine.Engine, t *testing.T) (*Range, *gossip.Gossip) {
	rm := &proto.RangeMetadata{
		RangeID:         0,
		RangeDescriptor: testRangeDescriptor,
	}
	g := gossip.New(rpc.LoadInsecureTLSConfig())
	clock := hlc.NewClock(hlc.UnixNano)
	r := NewRange(rm, clock, engine, nil, g)
	r.Start()
	return r, g
}

// TestRangeGossipFirstRange verifies that the first range gossips its location.
func TestRangeGossipFirstRange(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	info, err := g.GetInfo(gossip.KeyFirstRangeMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.(proto.RangeDescriptor), testRangeDescriptor) {
		t.Errorf("expected gossipped range locations to be equal: %+v vs %+v", info.(proto.RangeDescriptor), testRangeDescriptor)
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
		{gossip.KeyConfigAccounting, []*PrefixConfig{&PrefixConfig{engine.KeyMin, nil, &testDefaultAcctConfig}}},
		{gossip.KeyConfigPermission, []*PrefixConfig{&PrefixConfig{engine.KeyMin, nil, &testDefaultPermConfig}}},
		{gossip.KeyConfigZone, []*PrefixConfig{&PrefixConfig{engine.KeyMin, nil, &testDefaultZoneConfig}}},
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
	e := createTestEngine(t)
	mvcc := engine.NewMVCC(e)
	// Add a permission for a new key prefix.
	db1Perm := proto.PermConfig{
		Read:  []string{"spencer", "foo", "bar", "baz"},
		Write: []string{"spencer"},
	}
	key := engine.MakeKey(engine.KeyConfigPermissionPrefix, engine.Key("/db1"))
	if err := mvcc.PutProto(key, proto.MinTimestamp, nil, &db1Perm); err != nil {
		t.Fatal(err)
	}
	r, g := createTestRange(e, t)
	defer r.Stop()

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		&PrefixConfig{engine.KeyMin, nil, &testDefaultPermConfig},
		&PrefixConfig{engine.Key("/db1"), nil, &db1Perm},
		&PrefixConfig{engine.Key("/db2"), engine.KeyMin, &testDefaultPermConfig},
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
	db1Perm := proto.PermConfig{
		Read:  []string{"spencer"},
		Write: []string{"spencer"},
	}
	key := engine.MakeKey(engine.KeyConfigPermissionPrefix, engine.Key("/db1"))
	reply := &proto.PutResponse{}

	data, err := gogoproto.Marshal(&db1Perm)
	if err != nil {
		t.Fatal(err)
	}
	r.Put(&proto.PutRequest{RequestHeader: proto.RequestHeader{Key: key}, Value: proto.Value{Bytes: data}}, reply)
	if reply.Error != nil {
		t.Fatal(reply.GoError())
	}

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		&PrefixConfig{engine.KeyMin, nil, &testDefaultPermConfig},
		&PrefixConfig{engine.Key("/db1"), nil, &db1Perm},
		&PrefixConfig{engine.Key("/db2"), engine.KeyMin, &testDefaultPermConfig},
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
	*engine.InMem
	mu    sync.Mutex
	cvar  *sync.Cond
	block bool
}

func newBlockingEngine() *blockingEngine {
	be := &blockingEngine{
		InMem: engine.NewInMem(proto.Attributes{}, 1<<20),
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

func (be *blockingEngine) put(key engine.Key, value []byte) error {
	be.mu.Lock()
	defer be.mu.Unlock()
	for be.block {
		be.cvar.Wait()
	}
	return be.InMem.Put(key, value)
}

// createTestRange creates a range using a blocking engine. Returns
// the range clock's manual unix nanos time and the range.
func createTestRangeWithClock(t *testing.T) (*Range, *hlc.ManualClock, *hlc.Clock, *blockingEngine) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	engine := newBlockingEngine()
	rng := NewRange(&proto.RangeMetadata{}, clock, engine, nil, nil)
	rng.Start()
	return rng, &manual, clock, engine
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key string, rangeID int64) (*proto.GetRequest, *proto.GetResponse) {
	args := &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     []byte(key),
			Replica: proto.Replica{RangeID: rangeID},
		},
	}
	reply := &proto.GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value string, rangeID int64) (*proto.PutRequest, *proto.PutResponse) {
	args := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     []byte(key),
			Replica: proto.Replica{RangeID: rangeID},
		},
		Value: proto.Value{
			Bytes: []byte(value),
		},
	}
	reply := &proto.PutResponse{}
	return args, reply
}

// incrementArgs returns a IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key string, inc int64, rangeID int64) (*proto.IncrementRequest, *proto.IncrementResponse) {
	args := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     []byte(key),
			Replica: proto.Replica{RangeID: rangeID},
		},
		Increment: inc,
	}
	reply := &proto.IncrementResponse{}
	return args, reply
}

// internalRangeScanArgs returns a InternalRangeScanRequest and
// InternalRangeScanResponse pair addressed to the default replica
// for the specified key and endKey.
func internalRangeScanArgs(key []byte, endKey []byte, maxResults int64, snapshotID string, rangeID int64) (*proto.InternalRangeScanRequest, *proto.InternalRangeScanResponse) {
	args := &proto.InternalRangeScanRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			EndKey:  endKey,
			Replica: proto.Replica{RangeID: rangeID},
		},
		SnapshotId: snapshotID,
		MaxResults: maxResults,
	}
	reply := &proto.InternalRangeScanResponse{}
	return args, reply
}

// internalReleaseSnapshotArgs returns a InternalReleaseSnapshotRequest and
// InternalReleaseSnapshotResponse pair addressed to the default replica
// for snapshotId.
func internalReleaseSnapshotArgs(snapshotID string, rangeID int64) (*proto.InternalReleaseSnapshotRequest, *proto.InternalReleaseSnapshotResponse) {
	args := &proto.InternalReleaseSnapshotRequest{
		RequestHeader: proto.RequestHeader{
			Replica: proto.Replica{RangeID: rangeID},
		},
		SnapshotId: snapshotID,
	}
	reply := &proto.InternalReleaseSnapshotResponse{}
	return args, reply
}

// TestRangeUpdateTSCache verifies that reads update the read
// timestamp cache.
func TestRangeUpdateTSCache(t *testing.T) {
	rng, mc, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	*mc = hlc.ManualClock(t0.Nanoseconds())
	args, reply := getArgs("a", 0)
	args.Timestamp = clock.Now()
	err := rng.ReadOnlyCmd("Get", args, reply)
	if err != nil {
		t.Error(err)
	}
	// Verify the read timestamp cache has 1sec for "a".
	ts := rng.tsCache.GetMax(engine.Key("a"), nil)
	if ts.WallTime != t0.Nanoseconds() {
		t.Errorf("expected wall time to have 1s, but got %+v", ts)
	}
	// Verify another key ("b") has 0sec in timestamp cache.
	ts = rng.tsCache.GetMax(engine.Key("b"), nil)
	if ts.WallTime != 0 {
		t.Errorf("expected wall time to have 0s, but got %+v", ts)
	}
}

// TestRangeReadQueue verifies that reads must wait for writes to
// complete through Raft before being executed on range.
func TestRangeReadQueue(t *testing.T) {
	rng, _, _, be := createTestRangeWithClock(t)
	defer rng.Stop()

	// Asynchronously put a value to the rng with blocking enabled.
	be.setBlock(true)
	writeDone := make(chan struct{})
	go func() {
		args, reply := putArgs("a", "value", 0)
		err := rng.ReadWriteCmd("Put", args, reply)
		if err != nil {
			t.Fatal(err)
		}
		close(writeDone)
	}()

	// First, try a read for a non-impacted key ("b").
	readBDone := make(chan struct{})
	go func() {
		args, reply := getArgs("b", 0)
		err := rng.ReadOnlyCmd("Get", args, reply)
		if err != nil {
			t.Error(err)
		}
		close(readBDone)
	}()

	// Next, try a read for same key being written to verify it blocks.
	readADone := make(chan struct{})
	go func() {
		args, reply := getArgs("a", 0)
		err := rng.ReadOnlyCmd("Get", args, reply)
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
	rng, mc, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	*mc = hlc.ManualClock(t0.Nanoseconds())
	args, reply := getArgs("a", 0)
	args.Timestamp = clock.Now()
	err := rng.ReadOnlyCmd("Get", args, reply)
	if err != nil {
		t.Error(err)
	}
	pArgs, pReply := putArgs("a", "value", 0)
	err = rng.ReadWriteCmd("Put", pArgs, pReply)
	if err != nil {
		t.Fatal(err)
	}
	if pReply.Timestamp.WallTime != clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %+v", pReply.Timestamp)
	}
}

// TestRangeIdempotence verifies that a retry increment with
// same client command ID receives same reply.
func TestRangeIdempotence(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	// Run the same increment 100 times, 50 with identical command ID,
	// interleaved with 50 using a sequence of different command IDs.
	goldenArgs, _ := incrementArgs("a", 1, 0)
	incDones := make([]chan struct{}, 100)
	var count int64
	for i := range incDones {
		incDones[i] = make(chan struct{})
		idx := i
		go func() {
			var args proto.IncrementRequest
			var reply proto.IncrementResponse
			args = *goldenArgs
			args.Header().Timestamp = clock.Now()
			if idx%2 == 0 {
				args.CmdID = proto.ClientCmdID{WallTime: 1, Random: 1}
			} else {
				args.CmdID = proto.ClientCmdID{WallTime: 1, Random: int64(idx + 100)}
			}
			err := rng.ReadWriteCmd("Increment", &args, &reply)
			if err != nil {
				t.Fatal(err)
			}
			if idx%2 == 0 && reply.NewValue != 1 {
				t.Errorf("expected all incremented values to be 1; got %d", reply.NewValue)
			} else if idx%2 == 1 {
				atomic.AddInt64(&count, reply.NewValue)
			}
			close(incDones[idx])
		}()
	}
	// Wait for all to complete.
	for _, done := range incDones {
		select {
		case <-done:
			// Success.
		case <-time.After(500 * time.Millisecond):
			t.Fatal("had to wait for increment to complete")
		}
	}
	// Verify that all non-repeated client commands incremented the
	// counter starting at 2 all the way to 51 (sum of sequence = 1325).
	if count != 1325 {
		t.Errorf("expected sum of all increments to be 1325; got %d", count)
	}
}

// TestRangeSnapshot.
func TestRangeSnapshot(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	key1 := "a"
	key2 := "b"
	val1 := "1"
	val2 := "2"
	val3 := "3"

	pArgs, pReply := putArgs(key1, val1, 0)
	pArgs.Timestamp = clock.Now()
	err := rng.ReadWriteCmd("Put", pArgs, pReply)

	pArgs, pReply = putArgs(key2, val2, 0)
	pArgs.Timestamp = clock.Now()
	err = rng.ReadWriteCmd("Put", pArgs, pReply)

	gArgs, gReply := getArgs(key1, 0)
	gArgs.Timestamp = clock.Now()
	err = rng.ReadOnlyCmd("Get", gArgs, gReply)

	if err != nil {
		t.Fatalf("error : %s", err)
	}
	if !bytes.Equal(gReply.Value.Bytes, []byte(val1)) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			gReply.Value.Bytes, []byte(val1))
	}

	irsArgs, irsReply := internalRangeScanArgs(engine.PrefixEndKey(engine.KeyLocalPrefix), engine.KeyMax, 50, "", 0)
	irsArgs.Timestamp = clock.Now()
	err = rng.ReadOnlyCmd("InternalRangeScan", irsArgs, irsReply)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	snapshotID := irsReply.SnapshotId
	var valueNormalPrefix = byte(0)
	expectedKey := encoding.EncodeBinary(nil, []byte(key1))
	expectedVal := bytes.Join([][]byte{[]byte{valueNormalPrefix}, []byte(val1)}, []byte(""))
	if len(irsReply.Rows) != 4 ||
		!bytes.Equal(irsReply.Rows[0].Key, expectedKey) ||
		!bytes.Equal(irsReply.Rows[1].Value, expectedVal) {
		t.Fatalf("the value %v of key %v in get result does not match the value %v of key %v in request",
			irsReply.Rows[1].Value, irsReply.Rows[0].Key, expectedVal, expectedKey)
	}

	pArgs, pReply = putArgs(key2, val3, 0)
	pArgs.Timestamp = clock.Now()
	err = rng.ReadWriteCmd("Put", pArgs, pReply)

	// Scan with the previous snapshot will get the old value val2 of key2.
	irsArgs, irsReply = internalRangeScanArgs(engine.PrefixEndKey(engine.KeyLocalPrefix), engine.KeyMax, 50, snapshotID, 0)
	irsArgs.Timestamp = clock.Now()
	err = rng.ReadOnlyCmd("InternalRangeScan", irsArgs, irsReply)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	expectedKey = encoding.EncodeBinary(nil, []byte(key2))
	expectedVal = bytes.Join([][]byte{[]byte{valueNormalPrefix}, []byte(val2)}, []byte(""))
	if len(irsReply.Rows) != 4 ||
		!bytes.Equal(irsReply.Rows[2].Key, expectedKey) ||
		!bytes.Equal(irsReply.Rows[3].Value, expectedVal) {
		t.Fatalf("the value %v of key %v in get result does not match the value %v of key %v in request",
			irsReply.Rows[3].Value, irsReply.Rows[2].Key, expectedVal, expectedKey)
	}

	// Create a new snapshot to cover the latest value.
	irsArgs, irsReply = internalRangeScanArgs(engine.PrefixEndKey(engine.KeyLocalPrefix), engine.KeyMax, 50, "", 0)
	irsArgs.Timestamp = clock.Now()
	err = rng.ReadOnlyCmd("InternalRangeScan", irsArgs, irsReply)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	snapshotID2 := irsReply.SnapshotId
	expectedKey = encoding.EncodeBinary(nil, []byte(key2))
	expectedVal = bytes.Join([][]byte{[]byte{valueNormalPrefix}, []byte(val3)}, []byte(""))
	// Expect one more mvcc version.
	if len(irsReply.Rows) != 5 ||
		!bytes.Equal(irsReply.Rows[2].Key, expectedKey) ||
		!bytes.Equal(irsReply.Rows[3].Value, expectedVal) {
		t.Fatalf("the value %v of key %v in get result does not match the value %v of key %v in request",
			irsReply.Rows[3].Value, irsReply.Rows[2].Key, expectedVal, expectedKey)
	}

	irArgs, irReply := internalReleaseSnapshotArgs(snapshotID, 0)
	irArgs.Timestamp = clock.Now()
	err = rng.ReadOnlyCmd("InternalReleaseSnapshot", irArgs, irReply)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	irArgs, irReply = internalReleaseSnapshotArgs(snapshotID2, 0)
	irArgs.Timestamp = clock.Now()
	err = rng.ReadOnlyCmd("InternalReleaseSnapshot", irArgs, irReply)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
}
