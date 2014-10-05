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
	"fmt"
	"reflect"
	"regexp"
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
		RaftID:   1,
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
		ReplicaAttrs: []proto.Attributes{
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
		ClusterID:       "cluster1",
		RangeDescriptor: testRangeDescriptor,
		RangeID:         0,
	}
	rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
	g := gossip.New(rpcContext)
	clock := hlc.NewClock(hlc.UnixNano)
	r := NewRange(rm, clock, engine, nil, g, nil)
	r.Start()
	return r, g
}

// TestRangeContains verifies that the range uses Key.Address() in
// order to properly resolve addresses for local keys.
func TestRangeContains(t *testing.T) {
	rm := &proto.RangeMetadata{
		ClusterID: "cluster1",
		RangeDescriptor: proto.RangeDescriptor{
			RaftID:   1,
			StartKey: engine.Key("a"),
			EndKey:   engine.Key("b"),
		},
		RangeID: 0,
	}
	clock := hlc.NewClock(hlc.UnixNano)
	r := NewRange(rm, clock, nil, nil, nil, nil)
	if !r.ContainsKey(engine.Key("aa")) {
		t.Errorf("expected range to contain key \"aa\"")
	}
	if !r.ContainsKey(engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, []byte("aa"))) {
		t.Errorf("expected range to contain key transaction key for \"aa\"")
	}
	if !r.ContainsKeyRange(engine.Key("aa"), engine.Key("b")) {
		t.Errorf("expected range to contain key range \"aa\"-\"b\"")
	}
	if !r.ContainsKeyRange(engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, []byte("aa")),
		engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, []byte("b"))) {
		t.Errorf("expected range to contain key transaction range \"aa\"-\"b\"")
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

// A blockingEngine allows us to delay get/put (but not other ops!).
// It works by allowing a single key to be primed for a delay. When
// a get/put ops arrives for that key, it's blocked via a mutex
// until unblock() is invoked.
type blockingEngine struct {
	blocker sync.Mutex // blocks Get() and Put()
	*engine.InMem
	sync.Mutex // protects key
	key        engine.Key
}

func newBlockingEngine() *blockingEngine {
	be := &blockingEngine{
		InMem: engine.NewInMem(proto.Attributes{}, 1<<20),
	}
	return be
}

func (be *blockingEngine) block(key engine.Key) {
	be.Lock()
	defer be.Unlock()
	// Need to binary encode the key so it matches when accessed through MVCC.
	be.key = key.Encode(nil)
	// Get() and Put() will try to get this lock, so they will wait.
	be.blocker.Lock()
}

func (be *blockingEngine) unblock() {
	be.blocker.Unlock()
}

func (be *blockingEngine) wait() {
	be.blocker.Lock()
	be.blocker.Unlock()
}

func (be *blockingEngine) Get(key engine.Key) ([]byte, error) {
	be.Lock()
	if bytes.Equal(key, be.key) {
		be.key = nil
		defer be.wait()
	}
	be.Unlock()
	return be.InMem.Get(key)
}

func (be *blockingEngine) Put(key engine.Key, value []byte) error {
	be.Lock()
	if bytes.Equal(key, be.key) {
		be.key = nil
		defer be.wait()
	}
	be.Unlock()
	return be.InMem.Put(key, value)
}

// createTestRangeWithClock creates a range using a blocking engine. Returns
// the range clock's manual unix nanos time and the range.
func createTestRangeWithClock(t *testing.T) (*Range, *hlc.ManualClock, *hlc.Clock, *blockingEngine) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	engine := newBlockingEngine()
	rm := &proto.RangeMetadata{
		ClusterID:       "cluster1",
		RangeDescriptor: testRangeDescriptor,
		RangeID:         0,
	}
	rng := NewRange(rm, clock, engine, nil, nil, nil)
	rng.Start()
	return rng, &manual, clock, engine
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, rangeID int64) (*proto.GetRequest, *proto.GetResponse) {
	args := &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
		},
	}
	reply := &proto.GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, rangeID int64) (*proto.PutRequest, *proto.PutResponse) {
	args := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
	reply := &proto.PutResponse{}
	return args, reply
}

// readOrWriteArgs returns either get or put arguments depending on
// value of "read". Get for true; Put for false. Returns method
// selected and args & reply.
func readOrWriteArgs(key engine.Key, read bool) (string, proto.Request, proto.Response) {
	if read {
		gArgs, gReply := getArgs(key, 0)
		return Get, gArgs, gReply
	}
	pArgs, pReply := putArgs(key, []byte("value"), 0)
	return Put, pArgs, pReply
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, rangeID int64) (*proto.IncrementRequest, *proto.IncrementResponse) {
	args := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			Replica: proto.Replica{RangeID: rangeID},
		},
		Increment: inc,
	}
	reply := &proto.IncrementResponse{}
	return args, reply
}

func scanArgs(start, end []byte, rangeID int64) (*proto.ScanRequest, *proto.ScanResponse) {
	args := &proto.ScanRequest{
		RequestHeader: proto.RequestHeader{
			Key:     start,
			EndKey:  end,
			Replica: proto.Replica{RangeID: rangeID},
		},
	}
	reply := &proto.ScanResponse{}
	return args, reply
}

// endTxnArgs returns request/response pair for EndTransaction RPC
// addressed to the default replica for the specified key.
func endTxnArgs(txn *proto.Transaction, commit bool, rangeID int64) (
	*proto.EndTransactionRequest, *proto.EndTransactionResponse) {
	args := &proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:     engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, txn.ID),
			Replica: proto.Replica{RangeID: rangeID},
			Txn:     txn,
		},
		Commit: commit,
	}
	reply := &proto.EndTransactionResponse{}
	return args, reply
}

// pushTxnArgs returns request/response pair for InternalPushTxn RPC
// addressed to the default replica for the specified key.
func pushTxnArgs(pusher, pushee *proto.Transaction, abort bool, rangeID int64) (
	*proto.InternalPushTxnRequest, *proto.InternalPushTxnResponse) {
	args := &proto.InternalPushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:       engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, pushee.ID),
			Timestamp: pusher.Timestamp,
			Replica:   proto.Replica{RangeID: rangeID},
			Txn:       pusher,
		},
		PusheeTxn: *pushee,
		Abort:     abort,
	}
	reply := &proto.InternalPushTxnResponse{}
	return args, reply
}

// heartbeatArgs returns request/response pair for InternalHeartbeatTxn RPC.
func heartbeatArgs(txn *proto.Transaction, rangeID int64) (
	*proto.InternalHeartbeatTxnRequest, *proto.InternalHeartbeatTxnResponse) {
	args := &proto.InternalHeartbeatTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:     engine.MakeLocalKey(engine.KeyLocalTransactionPrefix, txn.ID),
			Replica: proto.Replica{RangeID: rangeID},
			Txn:     txn,
		},
	}
	reply := &proto.InternalHeartbeatTxnResponse{}
	return args, reply
}

// internalSnapshotCopyArgs returns a InternalSnapshotCopyRequest and
// InternalSnapshotCopyResponse pair addressed to the default replica
// for the specified key and endKey.
func internalSnapshotCopyArgs(key []byte, endKey []byte, maxResults int64, snapshotID string, rangeID int64) (
	*proto.InternalSnapshotCopyRequest, *proto.InternalSnapshotCopyResponse) {
	args := &proto.InternalSnapshotCopyRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			EndKey:  endKey,
			Replica: proto.Replica{RangeID: rangeID},
		},
		SnapshotId: snapshotID,
		MaxResults: maxResults,
	}
	reply := &proto.InternalSnapshotCopyResponse{}
	return args, reply
}

// getSerializedMVCCValue produces a byte slice of the serialized
// mvcc value. If value is nil, MVCCValue.Deleted is set to true;
// otherwise MVCCValue.Value is set to value.
func getSerializedMVCCValue(value *proto.Value) []byte {
	mvccVal := &proto.MVCCValue{}
	if value != nil {
		mvccVal.Value = value
	} else {
		mvccVal.Deleted = true
	}
	data, err := gogoproto.Marshal(&proto.MVCCValue{Value: value})
	if err != nil {
		panic("unexpected marshal error")
	}
	return data
}

// verifyErrorMatches checks that the error is not nil and that its
// error string matches the provided regular expression.
func verifyErrorMatches(err error, regexpStr string, t *testing.T) {
	if err == nil {
		t.Errorf("command did not result in an error")
	} else {
		if matched, regexpErr := regexp.MatchString(regexpStr, err.Error()); !matched || regexpErr != nil {
			t.Errorf("expected error to match %q (%v): %v", regexpStr, regexpErr, err.Error())
		}
	}
}

// TestRangeUpdateTSCache verifies that reads and writes update the
// timestamp cache.
func TestRangeUpdateTSCache(t *testing.T) {
	rng, mc, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	*mc = hlc.ManualClock(t0.Nanoseconds())
	gArgs, gReply := getArgs([]byte("a"), 0)
	gArgs.Timestamp = clock.Now()
	err := rng.AddCmd(Get, gArgs, gReply, true)
	if err != nil {
		t.Error(err)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	*mc = hlc.ManualClock(t1.Nanoseconds())
	pArgs, pReply := putArgs([]byte("b"), []byte("1"), 0)
	pArgs.Timestamp = clock.Now()
	err = rng.AddCmd(Put, pArgs, pReply, true)
	if err != nil {
		t.Error(err)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	rTS, wTS := rng.tsCache.GetMax(engine.Key("a"), nil, proto.NoTxnMD5)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != 0 {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s", rTS, wTS)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, wTS = rng.tsCache.GetMax(engine.Key("b"), nil, proto.NoTxnMD5)
	if rTS.WallTime != 0 || wTS.WallTime != t1.Nanoseconds() {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s", rTS, wTS)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, wTS = rng.tsCache.GetMax(engine.Key("c"), nil, proto.NoTxnMD5)
	if rTS.WallTime != 0 || wTS.WallTime != 0 {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s", rTS, wTS)
	}
}

// TestRangeCommandQueue verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestRangeCommandQueue(t *testing.T) {
	rng, _, _, be := createTestRangeWithClock(t)
	defer rng.Stop()

	// Test all four combinations of reads & writes waiting.
	testCases := []struct {
		cmd1Read, cmd2Read bool
		expWait            bool
	}{
		// Read/read doesn't wait.
		{true, true, false},
		// All other combinations must wait.
		{true, false, true},
		{false, true, true},
		{false, false, true},
	}

	for i, test := range testCases {
		key1 := engine.Key(fmt.Sprintf("key1-%d", i))
		key2 := engine.Key(fmt.Sprintf("key2-%d", i))
		// Asynchronously put a value to the rng with blocking enabled.
		be.block(key1)
		cmd1Done := make(chan struct{})
		go func() {
			method, args, reply := readOrWriteArgs(key1, test.cmd1Read)
			err := rng.AddCmd(method, args, reply, true)
			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd1Done)
		}()

		// First, try a command for same key as cmd1 to verify it blocks.
		cmd2Done := make(chan struct{})
		go func() {
			method, args, reply := readOrWriteArgs(key1, test.cmd2Read)
			err := rng.AddCmd(method, args, reply, true)
			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd2Done)
		}()

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		go func() {
			method, args, reply := readOrWriteArgs(key2, true)
			err := rng.AddCmd(method, args, reply, true)
			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd3Done)
		}()

		if test.expWait {
			// Verify cmd3 finishes but not cmd2.
			select {
			case <-cmd2Done:
				t.Fatalf("test %d: should not have been able to execute cmd2", i)
			case <-cmd3Done:
				// success.
			case <-cmd1Done:
				t.Fatalf("test %d: should not have been able execute cmd1 while blocked", i)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("test %d: waited 500ms for cmd3 of key2", i)
			}
		} else {
			select {
			case <-cmd2Done:
				// success.
			case <-cmd1Done:
				t.Fatalf("test %d: should not have been able execute cmd1 while blocked", i)
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("test %d: waited 500ms for cmd2 of key1", i)
			}
			<-cmd3Done
		}

		be.unblock()
		select {
		case <-cmd2Done:
			// success.
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("test %d: waited 500ms for cmd2 of key1", i)
		}
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
	args, reply := getArgs([]byte("a"), 0)
	args.Timestamp = clock.Now()
	err := rng.AddCmd(Get, args, reply, true)
	if err != nil {
		t.Error(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("value"), 0)
	err = rng.AddCmd(Put, pArgs, pReply, true)
	if err != nil {
		t.Fatal(err)
	}
	if pReply.Timestamp.WallTime != clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %+v", pReply.Timestamp)
	}
}

// TestRangeNoTSCacheUpdateOnFailure verifies that read and write
// commands do not update the timestamp cache if they result in
// failure.
func TestRangeNoTSCacheUpdateOnFailure(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	// Test for both read & write attempts.
	for i, read := range []bool{true, false} {
		key := engine.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		pArgs, pReply := putArgs(key, []byte("value"), 0)
		pArgs.Txn = NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pArgs.Timestamp = pArgs.Txn.Timestamp
		if err := rng.AddCmd(Put, pArgs, pReply, true); err != nil {
			t.Fatalf("test %d: %s", i, err)
		}

		// Now attempt read or write.
		method, args, reply := readOrWriteArgs(key, read)
		args.Header().Timestamp = clock.Now() // later timestamp
		if err := rng.AddCmd(method, args, reply, true); err == nil {
			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		if err := rng.AddCmd(Put, pArgs, pReply, true); err != nil {
			t.Fatalf("test %d: %s", i, err)
		}
		if !pReply.Timestamp.Equal(pArgs.Timestamp) {
			t.Errorf("expected timestamp not to advance %s != %s", pReply.Timestamp, pArgs.Timestamp)
		}
	}
}

// TestRangeNoTimestampIncrementWithinTxn verifies that successive
// read the write commands within the same transaction do not cause
// the write to receive an incremented timestamp.
func TestRangeNoTimestampIncrementWithinTxn(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	// Test for both read & write attempts.
	key := engine.Key("a")
	txn := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)

	// Start with a read to warm the timestamp cache.
	gArgs, gReply := getArgs(key, 0)
	gArgs.Txn = txn
	gArgs.Timestamp = txn.Timestamp
	if err := rng.AddCmd(Get, gArgs, gReply, true); err != nil {
		t.Fatal(err)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs, pReply := putArgs(key, []byte("value"), 0)
	pArgs.Txn = txn
	pArgs.Timestamp = pArgs.Txn.Timestamp
	if err := rng.AddCmd(Put, pArgs, pReply, true); err != nil {
		t.Fatal(err)
	}
	if !pReply.Timestamp.Equal(pArgs.Timestamp) {
		t.Errorf("expected timestamp to remain %s; got %s", pArgs.Timestamp, pReply.Timestamp)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	pArgs.Txn = nil
	expTS := pArgs.Timestamp
	expTS.Logical++
	if err := rng.AddCmd(Put, pArgs, pReply, true); err == nil {
		t.Errorf("expected write intent error")
	}
	if !pReply.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, pReply.Timestamp)
	}
}

// TestRangeIdempotence verifies that a retry increment with
// same client command ID receives same reply.
func TestRangeIdempotence(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	// Run the same increment 100 times, 50 with identical command ID,
	// interleaved with 50 using a sequence of different command IDs.
	goldenArgs, _ := incrementArgs([]byte("a"), 1, 0)
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
			err := rng.AddCmd(Increment, &args, &reply, true)
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

	key1 := []byte("a")
	key2 := []byte("b")
	val1 := []byte("1")
	val2 := []byte("2")
	val3 := []byte("3")

	pArgs, pReply := putArgs(key1, val1, 0)
	pArgs.Timestamp = clock.Now()
	err := rng.AddCmd(Put, pArgs, pReply, true)

	pArgs, pReply = putArgs(key2, val2, 0)
	pArgs.Timestamp = clock.Now()
	err = rng.AddCmd(Put, pArgs, pReply, true)

	gArgs, gReply := getArgs(key1, 0)
	gArgs.Timestamp = clock.Now()
	err = rng.AddCmd(Get, gArgs, gReply, true)

	if err != nil {
		t.Fatalf("error : %s", err)
	}
	if !bytes.Equal(gReply.Value.Bytes, val1) {
		t.Fatalf("the value %s in get result does not match the value %s in request",
			gReply.Value.Bytes, val1)
	}

	iscArgs, iscReply := internalSnapshotCopyArgs(engine.KeyLocalPrefix.PrefixEnd(), engine.KeyMax, 50, "", 0)
	iscArgs.Timestamp = clock.Now()
	err = rng.AddCmd(InternalSnapshotCopy, iscArgs, iscReply, true)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	snapshotID := iscReply.SnapshotId
	expectedKey := encoding.EncodeBinary(nil, key1)
	expectedVal := getSerializedMVCCValue(&proto.Value{Bytes: val1})
	if len(iscReply.Rows) != 4 ||
		!bytes.Equal(iscReply.Rows[0].Key, expectedKey) ||
		!bytes.Equal(iscReply.Rows[1].Value, expectedVal) {
		t.Fatalf("the value %v of key %v in get result does not match the value %v of key %v in request",
			iscReply.Rows[1].Value, iscReply.Rows[0].Key, expectedVal, expectedKey)
	}

	pArgs, pReply = putArgs(key2, val3, 0)
	pArgs.Timestamp = clock.Now()
	err = rng.AddCmd(Put, pArgs, pReply, true)

	// Scan with the previous snapshot will get the old value val2 of key2.
	iscArgs, iscReply = internalSnapshotCopyArgs(engine.KeyLocalPrefix.PrefixEnd(), engine.KeyMax, 50, snapshotID, 0)
	iscArgs.Timestamp = clock.Now()
	err = rng.AddCmd(InternalSnapshotCopy, iscArgs, iscReply, true)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	expectedKey = encoding.EncodeBinary(nil, key2)
	expectedVal = getSerializedMVCCValue(&proto.Value{Bytes: val2})
	if len(iscReply.Rows) != 4 ||
		!bytes.Equal(iscReply.Rows[2].Key, expectedKey) ||
		!bytes.Equal(iscReply.Rows[3].Value, expectedVal) {
		t.Fatalf("the value %v of key %v in get result does not match the value %v of key %v in request",
			iscReply.Rows[3].Value, iscReply.Rows[2].Key, expectedVal, expectedKey)
	}
	snapshotLastKey := engine.Key(iscReply.Rows[3].Key)

	// Create a new snapshot to cover the latest value.
	iscArgs, iscReply = internalSnapshotCopyArgs(engine.KeyLocalPrefix.PrefixEnd(), engine.KeyMax, 50, "", 0)
	iscArgs.Timestamp = clock.Now()
	err = rng.AddCmd(InternalSnapshotCopy, iscArgs, iscReply, true)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	snapshotID2 := iscReply.SnapshotId
	expectedKey = encoding.EncodeBinary(nil, key2)
	expectedVal = getSerializedMVCCValue(&proto.Value{Bytes: val3})
	// Expect one more mvcc version.
	if len(iscReply.Rows) != 5 ||
		!bytes.Equal(iscReply.Rows[2].Key, expectedKey) ||
		!bytes.Equal(iscReply.Rows[3].Value, expectedVal) {
		t.Fatalf("the value %v of key %v in get result does not match the value %v of key %v in request",
			iscReply.Rows[3].Value, iscReply.Rows[2].Key, expectedVal, expectedKey)
	}
	snapshot2LastKey := engine.Key(iscReply.Rows[4].Key)

	iscArgs, iscReply = internalSnapshotCopyArgs(snapshotLastKey.PrefixEnd(), engine.KeyMax, 50, snapshotID, 0)
	iscArgs.Timestamp = clock.Now()
	err = rng.AddCmd(InternalSnapshotCopy, iscArgs, iscReply, true)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	if len(iscReply.Rows) != 0 {
		t.Fatalf("error : %d", len(iscReply.Rows))
	}
	iscArgs, iscReply = internalSnapshotCopyArgs(snapshot2LastKey.PrefixEnd(), engine.KeyMax, 50, snapshotID2, 0)
	iscArgs.Timestamp = clock.Now()
	err = rng.AddCmd(InternalSnapshotCopy, iscArgs, iscReply, true)
	if err != nil {
		t.Fatalf("error : %s", err)
	}
	if len(iscReply.Rows) != 0 {
		t.Fatalf("error : %d", len(iscReply.Rows))
	}
}

// TestEndTransactionBeforeHeartbeat verifies that a transaction
// can be committed/aborted before being heartbeat.
func TestEndTransactionBeforeHeartbeat(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		args, reply := endTxnArgs(txn, commit, 0)
		args.Timestamp = txn.Timestamp
		if err := rng.AddCmd(EndTransaction, args, reply, true); err != nil {
			t.Error(err)
		}
		expStatus := proto.COMMITTED
		if !commit {
			expStatus = proto.ABORTED
		}
		if reply.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
		}

		// Try a heartbeat to the already-committed transaction; should get
		// committed txn back, but without last heartbeat timestamp set.
		hbArgs, hbReply := heartbeatArgs(txn, 0)
		if err := rng.AddCmd(InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Error(err)
		}
		if hbReply.Txn.Status != expStatus || hbReply.Txn.LastHeartbeat != nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hbReply)
		}
	}
}

// TestEndTransactionAfterHeartbeat verifies that a transaction
// can be committed/aborted after being heartbeat.
func TestEndTransactionAfterHeartbeat(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)

		// Start out with a heartbeat to the transaction.
		hbArgs, hbReply := heartbeatArgs(txn, 0)
		hbArgs.Timestamp = txn.Timestamp
		if err := rng.AddCmd(InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Error(err)
		}
		if hbReply.Txn.Status != proto.PENDING || hbReply.Txn.LastHeartbeat == nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hbReply)
		}

		args, reply := endTxnArgs(txn, commit, 0)
		args.Timestamp = txn.Timestamp
		if err := rng.AddCmd(EndTransaction, args, reply, true); err != nil {
			t.Error(err)
		}
		expStatus := proto.COMMITTED
		if !commit {
			expStatus = proto.ABORTED
		}
		if reply.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
		}
		if reply.Txn.LastHeartbeat == nil || !reply.Txn.LastHeartbeat.Equal(*hbReply.Txn.LastHeartbeat) {
			t.Errorf("expected heartbeats to remain equal: %+v != %+v",
				reply.Txn.LastHeartbeat, hbReply.Txn.LastHeartbeat)
		}
	}
}

// TestEndTransactionWithPushedTimestamp verifies that txn can be
// ended (both commit or abort) correctly when the commit timestamp is
// greater than the transaction timestamp, depending on the isolation
// level.
func TestEndTransactionWithPushedTimestamp(t *testing.T) {
	rng, mc, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	testCases := []struct {
		commit    bool
		isolation proto.IsolationType
		expErr    bool
	}{
		{true, proto.SERIALIZABLE, true},
		{true, proto.SNAPSHOT, false},
		{false, proto.SERIALIZABLE, false},
		{false, proto.SNAPSHOT, false},
	}
	key := []byte("a")
	for _, test := range testCases {
		txn := NewTransaction("test", key, 1, test.isolation, clock)
		// End the transaction with args timestamp moved forward in time.
		args, reply := endTxnArgs(txn, test.commit, 0)
		*mc = hlc.ManualClock(1)
		args.Timestamp = clock.Now()
		err := rng.AddCmd(EndTransaction, args, reply, true)
		if test.expErr {
			if err == nil {
				t.Errorf("expected error")
			}
			if rErr, ok := err.(*proto.TransactionRetryError); !ok || rErr.Backoff {
				t.Errorf("expected retry error with !Backoff; got %s", err)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			expStatus := proto.COMMITTED
			if !test.commit {
				expStatus = proto.ABORTED
			}
			if reply.Txn.Status != expStatus {
				t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
			}
		}
	}
}

// TestEndTransactionWithIncrementedEpoch verifies that txn ended with
// a higher epoch (and priority) correctly assumes the higher epoch.
func TestEndTransactionWithIncrementedEpoch(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	key := []byte("a")
	txn := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)

	// Start out with a heartbeat to the transaction.
	hbArgs, hbReply := heartbeatArgs(txn, 0)
	hbArgs.Timestamp = txn.Timestamp
	if err := rng.AddCmd(InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
		t.Error(err)
	}

	// Now end the txn with increased epoch and priority.
	args, reply := endTxnArgs(txn, true, 0)
	args.Timestamp = txn.Timestamp
	args.Txn.Epoch = txn.Epoch + 1
	args.Txn.Priority = txn.Priority + 1
	if err := rng.AddCmd(EndTransaction, args, reply, true); err != nil {
		t.Error(err)
	}
	if reply.Txn.Status != proto.COMMITTED {
		t.Errorf("expected transaction status to be COMMITTED; got %s", reply.Txn.Status)
	}
	if reply.Txn.Epoch != txn.Epoch {
		t.Errorf("expected epoch to equal %d; got %d", txn.Epoch, reply.Txn.Epoch)
	}
	if reply.Txn.Priority != txn.Priority {
		t.Errorf("expected priority to equal %d; got %d", txn.Priority, reply.Txn.Priority)
	}
}

// TestEndTransactionWithErrors verifies various error conditions
// are checked such as transaction already being committed or
// aborted, or timestamp or epoch regression.
func TestEndTransactionWithErrors(t *testing.T) {
	rng, mc, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	regressTS := clock.Now()
	*mc = hlc.ManualClock(1)
	txn := NewTransaction("test", engine.Key(""), 1, proto.SERIALIZABLE, clock)

	testCases := []struct {
		key          engine.Key
		existStatus  proto.TransactionStatus
		existEpoch   int32
		existTS      proto.Timestamp
		expErrRegexp string
	}{
		{engine.Key("a"), proto.COMMITTED, txn.Epoch, txn.Timestamp, "txn \"test\" {.*}: already committed"},
		{engine.Key("b"), proto.ABORTED, txn.Epoch, txn.Timestamp, "txn \"test\" {.*}: aborted"},
		{engine.Key("c"), proto.PENDING, txn.Epoch + 1, txn.Timestamp, "txn \"test\" {.*}: epoch regression: 0"},
		{engine.Key("d"), proto.PENDING, txn.Epoch, regressTS, "txn \"test\" {.*}: timestamp regression: 0.000000001,0"},
	}
	for _, test := range testCases {
		// Establish existing txn state by writing directly to range engine.
		var existTxn proto.Transaction
		gogoproto.Merge(&existTxn, txn)
		existTxn.ID = test.key
		existTxn.Status = test.existStatus
		existTxn.Epoch = test.existEpoch
		existTxn.Timestamp = test.existTS
		txnKey := engine.MakeKey(engine.KeyLocalTransactionPrefix, test.key).Encode(nil)
		if err := engine.PutProto(rng.engine, txnKey, &existTxn); err != nil {
			t.Fatal(err)
		}

		// End the transaction, verify expected error.
		txn.ID = test.key
		args, reply := endTxnArgs(txn, true, 0)
		args.Timestamp = txn.Timestamp
		verifyErrorMatches(rng.AddCmd(EndTransaction, args, reply, true), test.expErrRegexp, t)
	}
}

// TestInternalPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestInternalPushTxnBadKey(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	pusher := NewTransaction("test", engine.Key("a"), 1, proto.SERIALIZABLE, clock)
	pushee := NewTransaction("test", engine.Key("b"), 1, proto.SERIALIZABLE, clock)

	args, reply := pushTxnArgs(pusher, pushee, true, 0)
	args.Key = pusher.ID
	verifyErrorMatches(rng.AddCmd(InternalPushTxn, args, reply, true), ".*should match pushee.*", t)
}

// TestInternalPushTxnAlreadyCommittedOrAborted verifies success
// (noop) in event that pushee is already committed or aborted.
func TestInternalPushTxnAlreadyCommittedOrAborted(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	for i, status := range []proto.TransactionStatus{proto.COMMITTED, proto.ABORTED} {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pusher.Priority = 1
		pushee.Priority = 2 // pusher will lose, meaning we shouldn't push unless pushee is already ended.

		// End the pushee's transaction.
		etArgs, etReply := endTxnArgs(pushee, status == proto.COMMITTED, 0)
		etArgs.Timestamp = pushee.Timestamp
		if err := rng.AddCmd(EndTransaction, etArgs, etReply, true); err != nil {
			t.Fatal(err)
		}

		// Now try to push what's already committed or aborted.
		args, reply := pushTxnArgs(pusher, pushee, true, 0)
		if err := rng.AddCmd(InternalPushTxn, args, reply, true); err != nil {
			t.Fatal(err)
		}
		if reply.PusheeTxn.Status != status {
			t.Errorf("expected push txn to return with status == %s; got %+v", status, reply.PusheeTxn)
		}
	}
}

// TestInternalPushTxnUpgradeExistingTxn verifies that pushing
// a transaction record with a new epoch upgrades the pushee's
// epoch and timestamp if greater. In all test cases, the
// priorities are set such that the push will succeed.
func TestInternalPushTxnUpgradeExistingTxn(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	ts1 := proto.Timestamp{WallTime: 1}
	ts2 := proto.Timestamp{WallTime: 2}
	testCases := []struct {
		startEpoch, epoch, expEpoch int32
		startTS, ts, expTS          proto.Timestamp
	}{
		// Move epoch forward.
		{0, 1, 1, ts1, ts1, ts1},
		// Move timestamp forward.
		{0, 0, 0, ts1, ts2, ts2},
		// Move epoch backwards (has no effect).
		{1, 0, 1, ts1, ts1, ts1},
		// Move timestamp backwards (has no effect).
		{0, 0, 0, ts2, ts1, ts2},
		// Move both epoch & timestamp forward.
		{0, 1, 1, ts1, ts2, ts2},
		// Move both epoch & timestamp backward (has no effect).
		{1, 0, 1, ts2, ts1, ts2},
	}

	for i, test := range testCases {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee.Priority = 1
		pusher.Priority = 2 // Pusher will win.

		// First, establish "start" of existing pushee's txn via heartbeat.
		pushee.Epoch = test.startEpoch
		pushee.Timestamp = test.startTS
		hbArgs, hbReply := heartbeatArgs(pushee, 0)
		hbArgs.Timestamp = pushee.Timestamp
		if err := rng.AddCmd(InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction using updated values for epoch & timestamp.
		pushee.Epoch = test.epoch
		pushee.Timestamp = test.ts
		args, reply := pushTxnArgs(pusher, pushee, true, 0)
		if err := rng.AddCmd(InternalPushTxn, args, reply, true); err != nil {
			t.Fatal(err)
		}
		expTxn := gogoproto.Clone(pushee).(*proto.Transaction)
		expTxn.Epoch = test.expEpoch
		expTxn.Timestamp = test.expTS
		expTxn.Status = proto.ABORTED
		expTxn.LastHeartbeat = &test.startTS

		if !reflect.DeepEqual(expTxn, reply.PusheeTxn) {
			t.Errorf("unexpected push txn in trial %d; expected %+v, got %+v", i, expTxn, reply.PusheeTxn)
		}
	}
}

// TestInternalPushTxnHeartbeatTimeout verifies that a txn which
// hasn't been heartbeat within 2x the heartbeat interval can be
// aborted.
func TestInternalPushTxnHeartbeatTimeout(t *testing.T) {
	rng, mc, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	ts := proto.Timestamp{WallTime: 1}
	ns := DefaultHeartbeatInterval.Nanoseconds()
	testCases := []struct {
		heartbeat   *proto.Timestamp // nil indicates no heartbeat
		currentTime int64            // nanoseconds
		expSuccess  bool
	}{
		{nil, 0, false},
		{nil, ns, false},
		{nil, ns*2 - 1, false},
		{nil, ns * 2, false},
		{&ts, ns*2 + 1, false},
		{&ts, ns*2 + 2, true},
	}

	for i, test := range testCases {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via heartbeat.
		if test.heartbeat != nil {
			hbArgs, hbReply := heartbeatArgs(pushee, 0)
			hbArgs.Timestamp = *test.heartbeat
			if err := rng.AddCmd(InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
				t.Fatal(err)
			}
		}

		// Now, attempt to push the transaction with clock set to "currentTime".
		*mc = hlc.ManualClock(test.currentTime)
		args, reply := pushTxnArgs(pusher, pushee, true, 0)
		err := rng.AddCmd(InternalPushTxn, args, reply, true)
		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %v", i, test.expSuccess, err)
		}
		if err != nil {
			if rErr, ok := err.(*proto.TransactionRetryError); !ok || !rErr.Backoff {
				t.Errorf("expected txn retry error with Backoff=true: %v", err)
			}
		}
	}
}

// TestInternalPushTxnOldEpoch verifies that a txn intent from an
// older epoch may be pushed.
func TestInternalPushTxnOldEpoch(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	testCases := []struct {
		curEpoch, intentEpoch int32
		expSuccess            bool
	}{
		// Same epoch; can't push based on epoch.
		{0, 0, false},
		// The intent is newer; definitely can't push.
		{0, 1, false},
		// The intent is old; can push.
		{1, 0, true},
	}

	for i, test := range testCases {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via heartbeat.
		pushee.Epoch = test.curEpoch
		hbArgs, hbReply := heartbeatArgs(pushee, 0)
		hbArgs.Timestamp = pushee.Timestamp
		if err := rng.AddCmd(InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction with intent epoch set appropriately.
		pushee.Epoch = test.intentEpoch
		args, reply := pushTxnArgs(pusher, pushee, true, 0)
		err := rng.AddCmd(InternalPushTxn, args, reply, true)
		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %v", i, test.expSuccess, err)
		}
		if err != nil {
			if rErr, ok := err.(*proto.TransactionRetryError); !ok || !rErr.Backoff {
				t.Errorf("expected txn retry error with Backoff=true: %v", err)
			}
		}
	}
}

// TestInternalPushTxnPriorities verifies that txns with lower
// priority are pushed; if priorities are equal, then the txns
// are ordered by txn timestamp, with the more recent timestamp
// being pushable.
func TestInternalPushTxnPriorities(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	ts1 := proto.Timestamp{WallTime: 1}
	ts2 := proto.Timestamp{WallTime: 2}
	testCases := []struct {
		pusherPriority, pusheePriority int32
		pusherTS, pusheeTS             proto.Timestamp
		abort                          bool
		expSuccess                     bool
	}{
		// Pusher has higher priority succeeds.
		{2, 1, ts1, ts1, true, true},
		// Pusher has lower priority fails.
		{1, 2, ts1, ts1, true, false},
		{1, 2, ts1, ts1, false, false},
		// Pusher has lower priority fails, even with older txn timestamp.
		{1, 2, ts1, ts2, true, false},
		// Pusher has lower priority, but older txn timestamp allows success if !abort.
		{1, 2, ts1, ts2, false, true},
		// With same priorities, older txn timestamp succeeds.
		{1, 1, ts1, ts2, true, true},
		// With same priorities, same txn timestamp fails.
		{1, 1, ts1, ts1, true, false},
		{1, 1, ts1, ts1, false, false},
		// With same priorities, newer txn timestamp fails.
		{1, 1, ts2, ts1, true, false},
		{1, 1, ts2, ts1, false, false},
	}

	for i, test := range testCases {
		key := engine.Key(fmt.Sprintf("key-%d", i))
		pusher := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pushee := NewTransaction("test", key, 1, proto.SERIALIZABLE, clock)
		pusher.Priority = test.pusherPriority
		pushee.Priority = test.pusheePriority
		pusher.Timestamp = test.pusherTS
		pushee.Timestamp = test.pusheeTS

		// Now, attempt to push the transaction with intent epoch set appropriately.
		args, reply := pushTxnArgs(pusher, pushee, test.abort, 0)
		err := rng.AddCmd(InternalPushTxn, args, reply, true)
		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %v", i, test.expSuccess, err)
		}
		if err != nil {
			if rErr, ok := err.(*proto.TransactionRetryError); !ok || !rErr.Backoff {
				t.Errorf("expected txn retry error with Backoff=true: %v", err)
			}
		}
	}
}

// TestInternalPushTxnPushTimestamp verifies that with args.Abort is
// false (i.e. for read/write conflict), the pushed txn keeps status
// PENDING, but has its txn Timestamp moved forward to the pusher's
// txn Timestamp + 1.
func TestInternalPushTxnPushTimestamp(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	pusher := NewTransaction("test", engine.Key("a"), 1, proto.SERIALIZABLE, clock)
	pushee := NewTransaction("test", engine.Key("b"), 1, proto.SERIALIZABLE, clock)
	pusher.Priority = 2
	pushee.Priority = 1 // pusher will win
	pusher.Timestamp = proto.Timestamp{WallTime: 50, Logical: 25}
	pushee.Timestamp = proto.Timestamp{WallTime: 5, Logical: 1}

	// Now, push the transaction with args.Abort=false.
	args, reply := pushTxnArgs(pusher, pushee, false /* abort */, 0)
	if err := rng.AddCmd(InternalPushTxn, args, reply, true); err != nil {
		t.Errorf("unexpected error on push: %v", err)
	}
	expTS := pusher.Timestamp
	expTS.Logical++
	if !reply.PusheeTxn.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to be pushed to %+v; got %+v", expTS, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != proto.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}

// TestInternalPushTxnPushTimestampAlreadyPushed verifies that pushing
// a timestamp forward which is already far enough forward is a simple
// noop. We do this by ensuring that priorities would otherwise make
// pushing impossible.
func TestInternalPushTxnPushTimestampAlreadyPushed(t *testing.T) {
	rng, _, clock, _ := createTestRangeWithClock(t)
	defer rng.Stop()

	pusher := NewTransaction("test", engine.Key("a"), 1, proto.SERIALIZABLE, clock)
	pushee := NewTransaction("test", engine.Key("b"), 1, proto.SERIALIZABLE, clock)
	pusher.Priority = 1
	pushee.Priority = 2 // pusher will lose
	pusher.Timestamp = proto.Timestamp{WallTime: 50, Logical: 0}
	pushee.Timestamp = proto.Timestamp{WallTime: 50, Logical: 1}

	// Now, push the transaction with args.Abort=false.
	args, reply := pushTxnArgs(pusher, pushee, false /* abort */, 0)
	if err := rng.AddCmd(InternalPushTxn, args, reply, true); err != nil {
		t.Errorf("unexpected error on push: %v", err)
	}
	if !reply.PusheeTxn.Timestamp.Equal(pushee.Timestamp) {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.Timestamp, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != proto.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}
