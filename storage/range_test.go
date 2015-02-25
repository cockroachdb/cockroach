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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/multiraft/storagetest"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft"
	gogoproto "github.com/gogo/protobuf/proto"
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
				Attrs:   proto.Attributes{Attrs: []string{"dc1", "mem"}},
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
			{Attrs: []string{"dc1", "mem"}},
			{Attrs: []string{"dc2", "mem"}},
		},
		RangeMinBytes: 1 << 10, // 1k
		RangeMaxBytes: 1 << 18, // 256k
		GC: &proto.GCPolicy{
			TTLSeconds: 24 * 60 * 60, // 1 day
		},
	}
)

// boostrapMode controls how the first range is created in testContext.
type bootstrapMode int

const (
	// Use Store.BootstrapRange, which writes the range descriptor and other metadata.
	// Most tests should use this mode because it more closely resembles the real world.
	bootstrapRangeWithMetadata bootstrapMode = iota
	// Create a range with NewRange and Store.AddRange. The store's data will
	// be persisted but metadata will not. This mode is provided for backwards compatiblity
	// for tests that expect the store to initially be empty.
	bootstrapRangeOnly
)

// testContext contains all the objects necessary to test a Range.
// In most cases, simply call Start(t) (and later Stop()) on a zero-initialized
// testContext{}. Any fields which are initialized to non-nil values
// will be used as-is.
type testContext struct {
	transport     multiraft.Transport
	store         *Store
	rng           *Range
	gossip        *gossip.Gossip
	engine        engine.Engine
	manualClock   *hlc.ManualClock
	clock         *hlc.Clock
	bootstrapMode bootstrapMode
}

// testContext.Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(t *testing.T) {
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(hlc.NewClock(hlc.UnixNano), rpc.LoadInsecureTLSConfig())
		tc.gossip = gossip.New(rpcContext)
	}
	if tc.manualClock == nil {
		tc.manualClock = hlc.NewManualClock(0)
	}
	if tc.clock == nil {
		tc.clock = hlc.NewClock(tc.manualClock.UnixNano)
	}
	if tc.engine == nil {
		tc.engine = engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
	}

	if tc.transport == nil {
		tc.transport = multiraft.NewLocalRPCTransport()
	}

	if tc.store == nil {
		tc.store = NewStore(tc.clock, tc.engine, nil, tc.gossip, tc.transport)
		if err := tc.store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}); err != nil {
			t.Fatal(err)
		}

		if tc.rng == nil && tc.bootstrapMode == bootstrapRangeWithMetadata {
			if err := tc.store.BootstrapRange(); err != nil {
				t.Fatal(err)
			}
		}
		tc.store.db = client.NewKV(&testSender{store: tc.store}, nil)
		if err := tc.store.Start(); err != nil {
			t.Fatal(err)
		}
	}

	initConfigs(tc.engine, t)

	if tc.rng == nil {
		if tc.bootstrapMode == bootstrapRangeOnly {
			rng, err := NewRange(&testRangeDescriptor, tc.store)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AddRange(rng); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		tc.rng, err = tc.store.GetRange(1)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func (tc *testContext) Stop() {
	tc.store.Stop()
}

// initConfigs creates default configuration entries.
func initConfigs(e engine.Engine, t *testing.T) {
	if err := engine.MVCCPutProto(e, nil, engine.KeyConfigAccountingPrefix, proto.MinTimestamp, nil, &testDefaultAcctConfig); err != nil {
		t.Fatal(err)
	}
	if err := engine.MVCCPutProto(e, nil, engine.KeyConfigPermissionPrefix, proto.MinTimestamp, nil, &testDefaultPermConfig); err != nil {
		t.Fatal(err)
	}
	if err := engine.MVCCPutProto(e, nil, engine.KeyConfigZonePrefix, proto.MinTimestamp, nil, &testDefaultZoneConfig); err != nil {
		t.Fatal(err)
	}
}

func newTransaction(name string, baseKey proto.Key, userPriority int32,
	isolation proto.IsolationType, clock *hlc.Clock) *proto.Transaction {
	return proto.NewTransaction(name, engine.KeyAddress(baseKey), userPriority,
		isolation, clock.Now(), clock.MaxOffset().Nanoseconds())
}

// CreateReplicaSets creates new proto.Replica protos based on an array of integers
// to aid in testing. Note that this does not actualy produce any actual replicas, it
// just creates the proto.
func createReplicaSets(replicaNumbers []proto.StoreID) []proto.Replica {
	result := []proto.Replica{}
	for _, replicaNumber := range replicaNumbers {
		result = append(result, proto.Replica{
			StoreID: replicaNumber,
		})
	}
	return result
}

// TestRangeContains verifies that the range uses Key.Address() in
// order to properly resolve addresses for local keys.
func TestRangeContains(t *testing.T) {
	desc := &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: proto.Key("a"),
		EndKey:   proto.Key("b"),
	}

	e := engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
	clock := hlc.NewClock(hlc.UnixNano)
	r, err := NewRange(desc, NewStore(clock, e, nil, nil, multiraft.NewLocalRPCTransport()))
	if err != nil {
		t.Fatal(err)
	}
	if !r.ContainsKey(proto.Key("aa")) {
		t.Errorf("expected range to contain key \"aa\"")
	}
	if !r.ContainsKey(engine.RangeDescriptorKey([]byte("aa"))) {
		t.Errorf("expected range to contain range descriptor key for \"aa\"")
	}
	if !r.ContainsKeyRange(proto.Key("aa"), proto.Key("b")) {
		t.Errorf("expected range to contain key range \"aa\"-\"b\"")
	}
	if !r.ContainsKeyRange(engine.RangeDescriptorKey([]byte("aa")),
		engine.RangeDescriptorKey([]byte("b"))) {
		t.Errorf("expected range to contain key transaction range \"aa\"-\"b\"")
	}
}

// TestRangeGossipFirstRange verifies that the first range gossips its
// location and the cluster ID.
func TestRangeGossipFirstRange(t *testing.T) {
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()
	if err := util.IsTrueWithin(func() bool {
		for _, key := range []string{gossip.KeyClusterID, gossip.KeyFirstRangeDescriptor} {
			info, err := tc.gossip.GetInfo(key)
			if err != nil {
				log.Warningf("still waiting for first range gossip of key %s...", key)
				return false
			}
			if key == gossip.KeyFirstRangeDescriptor &&
				!reflect.DeepEqual(info.(proto.RangeDescriptor), testRangeDescriptor) {
				t.Errorf("expected gossiped range locations to be equal: %+v vs %+v", info.(proto.RangeDescriptor), testRangeDescriptor)
			}
			if key == gossip.KeyClusterID && info.(string) != tc.store.Ident.ClusterID {
				t.Errorf("expected gossiped cluster ID %s; got %s", tc.store.Ident.ClusterID, info.(string))
			}
		}
		return true
	}, 500*time.Millisecond); err != nil {
		t.Error(err)
	}
}

// TestRangeGossipAllConfigs verifies that all config types are
// gossiped.
func TestRangeGossipAllConfigs(t *testing.T) {
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()
	testData := []struct {
		gossipKey string
		configs   []*PrefixConfig
	}{
		{gossip.KeyConfigAccounting, []*PrefixConfig{{engine.KeyMin, nil, &testDefaultAcctConfig}}},
		{gossip.KeyConfigPermission, []*PrefixConfig{{engine.KeyMin, nil, &testDefaultPermConfig}}},
		{gossip.KeyConfigZone, []*PrefixConfig{{engine.KeyMin, nil, &testDefaultZoneConfig}}},
	}
	for _, test := range testData {
		info, err := tc.gossip.GetInfo(test.gossipKey)
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
// key prefixes for a config are gossiped.
func TestRangeGossipConfigWithMultipleKeyPrefixes(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Add a permission for a new key prefix.
	db1Perm := &proto.PermConfig{
		Read:  []string{"spencer", "foo", "bar", "baz"},
		Write: []string{"spencer"},
	}
	key := engine.MakeKey(engine.KeyConfigPermissionPrefix, proto.Key("/db1"))
	data, err := gogoproto.Marshal(db1Perm)
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key, Timestamp: proto.MinTimestamp},
		Value:         proto.Value{Bytes: data},
	}
	reply := &proto.PutResponse{}

	if err := tc.rng.executeCmd(proto.Put, req, reply); err != nil {
		t.Fatal(err)
	}

	info, err := tc.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		{engine.KeyMin, nil, &testDefaultPermConfig},
		{proto.Key("/db1"), nil, db1Perm},
		{proto.Key("/db2"), engine.KeyMin, &testDefaultPermConfig},
	}
	if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
	}
}

// TestRangeGossipConfigUpdates verifies that writes to the
// permissions cause the updated configs to be re-gossiped.
func TestRangeGossipConfigUpdates(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Add a permission for a new key prefix.
	db1Perm := &proto.PermConfig{
		Read:  []string{"spencer"},
		Write: []string{"spencer"},
	}
	key := engine.MakeKey(engine.KeyConfigPermissionPrefix, proto.Key("/db1"))
	data, err := gogoproto.Marshal(db1Perm)
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key, Timestamp: proto.MinTimestamp},
		Value:         proto.Value{Bytes: data},
	}
	reply := &proto.PutResponse{}

	if err := tc.rng.executeCmd(proto.Put, req, reply); err != nil {
		t.Fatal(err)
	}

	info, err := tc.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		{engine.KeyMin, nil, &testDefaultPermConfig},
		{proto.Key("/db1"), nil, db1Perm},
		{proto.Key("/db2"), engine.KeyMin, &testDefaultPermConfig},
	}
	if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
	}
}

// A blockingEngine allows us to delay get/put (but not other ops!).
// It works by allowing a single key to be primed for a delay. When
// a get/put ops arrives for that key, it's blocked via a mutex
// until unblock() is invoked.
type blockingEngine struct {
	blocker sync.Mutex // blocks Get() and Put()
	*engine.InMem
	sync.Mutex // protects key
	key        proto.EncodedKey
}

func newBlockingEngine() *blockingEngine {
	be := &blockingEngine{
		InMem: engine.NewInMem(proto.Attributes{}, 1<<20),
	}
	return be
}

func (be *blockingEngine) block(key proto.Key) {
	be.Lock()
	defer be.Unlock()
	// Need to binary encode the key so it matches when accessed through MVCC.
	be.key = engine.MVCCEncodeKey(key)
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

func (be *blockingEngine) Get(key proto.EncodedKey) ([]byte, error) {
	be.Lock()
	if bytes.Equal(key, be.key) {
		be.key = nil
		defer be.wait()
	}
	be.Unlock()
	return be.InMem.Get(key)
}

func (be *blockingEngine) Put(key proto.EncodedKey, value []byte) error {
	be.Lock()
	if bytes.Equal(key, be.key) {
		be.key = nil
		defer be.wait()
	}
	be.Unlock()
	return be.InMem.Put(key, value)
}

func (be *blockingEngine) NewBatch() engine.Engine {
	return engine.NewBatch(be)
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, raftID int64, storeID proto.StoreID) (*proto.GetRequest, *proto.GetResponse) {
	args := &proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
	reply := &proto.GetResponse{}
	return args, reply
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, raftID int64, storeID proto.StoreID) (*proto.PutRequest, *proto.PutResponse) {
	args := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			Timestamp: proto.MinTimestamp,
			RaftID:    raftID,
			Replica:   proto.Replica{StoreID: storeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
	reply := &proto.PutResponse{}
	return args, reply
}

// deleteArgs returns a DeleteRequest and DeleteResponse pair.
func deleteArgs(key proto.Key, raftID int64, storeID proto.StoreID) (*proto.DeleteRequest, *proto.DeleteResponse) {
	args := &proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
	reply := &proto.DeleteResponse{}
	return args, reply
}

// readOrWriteArgs returns either get or put arguments depending on
// value of "read". Get for true; Put for false. Returns method
// selected and args & reply.
func readOrWriteArgs(key proto.Key, read bool, raftID int64, storeID proto.StoreID) (string, proto.Request, proto.Response) {
	if read {
		gArgs, gReply := getArgs(key, raftID, storeID)
		return proto.Get, gArgs, gReply
	}
	pArgs, pReply := putArgs(key, []byte("value"), raftID, storeID)
	return proto.Put, pArgs, pReply
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, raftID int64, storeID proto.StoreID) (*proto.IncrementRequest, *proto.IncrementResponse) {
	args := &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Increment: inc,
	}
	reply := &proto.IncrementResponse{}
	return args, reply
}

func scanArgs(start, end []byte, raftID int64, storeID proto.StoreID) (*proto.ScanRequest, *proto.ScanResponse) {
	args := &proto.ScanRequest{
		RequestHeader: proto.RequestHeader{
			Key:     start,
			EndKey:  end,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
	reply := &proto.ScanResponse{}
	return args, reply
}

// endTxnArgs returns request/response pair for EndTransaction RPC
// addressed to the default replica for the specified key.
func endTxnArgs(txn *proto.Transaction, commit bool, raftID int64, storeID proto.StoreID) (
	*proto.EndTransactionRequest, *proto.EndTransactionResponse) {
	args := &proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:     txn.Key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
			Txn:     txn,
		},
		Commit: commit,
	}
	reply := &proto.EndTransactionResponse{}
	return args, reply
}

// pushTxnArgs returns request/response pair for InternalPushTxn RPC
// addressed to the default replica for the specified key.
func pushTxnArgs(pusher, pushee *proto.Transaction, abort bool, raftID int64, storeID proto.StoreID) (
	*proto.InternalPushTxnRequest, *proto.InternalPushTxnResponse) {
	args := &proto.InternalPushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:       pushee.Key,
			Timestamp: pusher.Timestamp,
			RaftID:    raftID,
			Replica:   proto.Replica{StoreID: storeID},
			Txn:       pusher,
		},
		PusheeTxn: *pushee,
		Abort:     abort,
	}
	reply := &proto.InternalPushTxnResponse{}
	return args, reply
}

// heartbeatArgs returns request/response pair for InternalHeartbeatTxn RPC.
func heartbeatArgs(txn *proto.Transaction, raftID int64, storeID proto.StoreID) (
	*proto.InternalHeartbeatTxnRequest, *proto.InternalHeartbeatTxnResponse) {
	args := &proto.InternalHeartbeatTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:     txn.Key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
			Txn:     txn,
		},
	}
	reply := &proto.InternalHeartbeatTxnResponse{}
	return args, reply
}

// internalMergeArgs returns a InternalMergeRequest and InternalMergeResponse
// pair addressed to the default replica for the specified key. The request will
// contain the given proto.Value.
func internalMergeArgs(key []byte, value proto.Value, raftID int64, storeID proto.StoreID) (
	*proto.InternalMergeRequest, *proto.InternalMergeResponse) {
	args := &proto.InternalMergeRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Value: value,
	}
	reply := &proto.InternalMergeResponse{}
	return args, reply
}

func internalTruncateLogArgs(index uint64, raftID int64, storeID proto.StoreID) (
	*proto.InternalTruncateLogRequest, *proto.InternalTruncateLogResponse) {
	args := &proto.InternalTruncateLogRequest{
		RequestHeader: proto.RequestHeader{
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Index: index,
	}
	reply := &proto.InternalTruncateLogResponse{}
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
			t.Errorf("expected error to match %q (%s): %s", regexpStr, regexpErr, err.Error())
		}
	}
}

// TestRangeUpdateTSCache verifies that reads and writes update the
// timestamp cache.
func TestRangeUpdateTSCache(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	gArgs, gReply := getArgs([]byte("a"), 1, tc.store.StoreID())
	gArgs.Timestamp = tc.clock.Now()
	err := tc.rng.AddCmd(proto.Get, gArgs, gReply, true)
	if err != nil {
		t.Error(err)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	tc.manualClock.Set(t1.Nanoseconds())
	pArgs, pReply := putArgs([]byte("b"), []byte("1"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()
	err = tc.rng.AddCmd(proto.Put, pArgs, pReply, true)
	if err != nil {
		t.Error(err)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	rTS, wTS := tc.rng.tsCache.GetMax(proto.Key("a"), nil, proto.NoTxnMD5)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != 0 {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s", rTS, wTS)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, wTS = tc.rng.tsCache.GetMax(proto.Key("b"), nil, proto.NoTxnMD5)
	if rTS.WallTime != 0 || wTS.WallTime != t1.Nanoseconds() {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s", rTS, wTS)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, wTS = tc.rng.tsCache.GetMax(proto.Key("c"), nil, proto.NoTxnMD5)
	if rTS.WallTime != 0 || wTS.WallTime != 0 {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s", rTS, wTS)
	}
}

// TestRangeCommandQueue verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestRangeCommandQueue(t *testing.T) {
	be := newBlockingEngine()
	tc := testContext{
		engine: be,
	}
	tc.Start(t)
	defer tc.Stop()

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
		key1 := proto.Key(fmt.Sprintf("key1-%d", i))
		key2 := proto.Key(fmt.Sprintf("key2-%d", i))
		// Asynchronously put a value to the rng with blocking enabled.
		be.block(key1)
		cmd1Done := make(chan struct{})
		go func() {
			method, args, reply := readOrWriteArgs(key1, test.cmd1Read, tc.rng.Desc.RaftID,
				tc.store.StoreID())
			err := tc.rng.AddCmd(method, args, reply, true)
			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd1Done)
		}()

		// First, try a command for same key as cmd1 to verify it blocks.
		cmd2Done := make(chan struct{})
		go func() {
			method, args, reply := readOrWriteArgs(key1, test.cmd2Read, tc.rng.Desc.RaftID,
				tc.store.StoreID())
			err := tc.rng.AddCmd(method, args, reply, true)
			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd2Done)
		}()

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		go func() {
			method, args, reply := readOrWriteArgs(key2, true, tc.rng.Desc.RaftID, tc.store.StoreID())
			err := tc.rng.AddCmd(method, args, reply, true)
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
				t.Fatalf("test %d: should not have been able to execute cmd1 while blocked", i)
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args, reply := getArgs([]byte("a"), 1, tc.store.StoreID())
	args.Timestamp = tc.clock.Now()
	err := tc.rng.AddCmd(proto.Get, args, reply, true)
	if err != nil {
		t.Error(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("value"), 1, tc.store.StoreID())
	err = tc.rng.AddCmd(proto.Put, pArgs, pReply, true)
	if err != nil {
		t.Fatal(err)
	}
	if pReply.Timestamp.WallTime != tc.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %+v", pReply.Timestamp)
	}
}

// TestRangeNoTSCacheUpdateOnFailure verifies that read and write
// commands do not update the timestamp cache if they result in
// failure.
func TestRangeNoTSCacheUpdateOnFailure(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Test for both read & write attempts.
	for i, read := range []bool{true, false} {
		key := proto.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		pArgs, pReply := putArgs(key, []byte("value"), 1, tc.store.StoreID())
		pArgs.Txn = newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pArgs.Timestamp = pArgs.Txn.Timestamp
		if err := tc.rng.AddCmd(proto.Put, pArgs, pReply, true); err != nil {
			t.Fatalf("test %d: %s", i, err)
		}

		// Now attempt read or write.
		method, args, reply := readOrWriteArgs(key, read, tc.rng.Desc.RaftID, tc.store.StoreID())
		args.Header().Timestamp = tc.clock.Now() // later timestamp
		if err := tc.rng.AddCmd(method, args, reply, true); err == nil {
			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		if err := tc.rng.AddCmd(proto.Put, pArgs, pReply, true); err != nil {
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Test for both read & write attempts.
	key := proto.Key("a")
	txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)

	// Start with a read to warm the timestamp cache.
	gArgs, gReply := getArgs(key, 1, tc.store.StoreID())
	gArgs.Txn = txn
	gArgs.Timestamp = txn.Timestamp
	if err := tc.rng.AddCmd(proto.Get, gArgs, gReply, true); err != nil {
		t.Fatal(err)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs, pReply := putArgs(key, []byte("value"), 1, tc.store.StoreID())
	pArgs.Txn = txn
	pArgs.Timestamp = pArgs.Txn.Timestamp
	if err := tc.rng.AddCmd(proto.Put, pArgs, pReply, true); err != nil {
		t.Fatal(err)
	}
	if !pReply.Timestamp.Equal(pArgs.Timestamp) {
		t.Errorf("expected timestamp to remain %s; got %s", pArgs.Timestamp, pReply.Timestamp)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	pArgs.Txn = nil
	expTS := pArgs.Timestamp
	expTS.Logical++
	if err := tc.rng.AddCmd(proto.Put, pArgs, pReply, true); err == nil {
		t.Errorf("expected write intent error")
	}
	if !pReply.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, pReply.Timestamp)
	}
}

// TestRangeIdempotence verifies that a retry increment with
// same client command ID receives same reply.
func TestRangeIdempotence(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Run the same increment 100 times, 50 with identical command ID,
	// interleaved with 50 using a sequence of different command IDs.
	goldenArgs, _ := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
	incDones := make([]chan struct{}, 100)
	var count int64
	for i := range incDones {
		incDones[i] = make(chan struct{})
		idx := i
		go func() {
			var args proto.IncrementRequest
			var reply proto.IncrementResponse
			args = *goldenArgs
			args.Header().Timestamp = tc.clock.Now()
			if idx%2 == 0 {
				args.CmdID = proto.ClientCmdID{WallTime: 1, Random: 1}
			} else {
				args.CmdID = proto.ClientCmdID{WallTime: 1, Random: int64(idx + 100)}
			}
			err := tc.rng.AddCmd(proto.Increment, &args, &reply, true)
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
		case <-time.After(2000 * time.Millisecond):
			t.Fatal("had to wait for increment to complete")
		}
	}
	// Verify that all non-repeated client commands incremented the
	// counter starting at 2 all the way to 51 (sum of sequence = 1325).
	if count != 1325 {
		t.Errorf("expected sum of all increments to be 1325; got %d", count)
	}
}

// TestEndTransactionBeforeHeartbeat verifies that a transaction
// can be committed/aborted before being heartbeat.
func TestEndTransactionBeforeHeartbeat(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		args, reply := endTxnArgs(txn, commit, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp
		if err := tc.rng.AddCmd(proto.EndTransaction, args, reply, true); err != nil {
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
		hbArgs, hbReply := heartbeatArgs(txn, 1, tc.store.StoreID())
		if err := tc.rng.AddCmd(proto.InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)

		// Start out with a heartbeat to the transaction.
		hbArgs, hbReply := heartbeatArgs(txn, 1, tc.store.StoreID())
		hbArgs.Timestamp = txn.Timestamp
		if err := tc.rng.AddCmd(proto.InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Error(err)
		}
		if hbReply.Txn.Status != proto.PENDING || hbReply.Txn.LastHeartbeat == nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hbReply)
		}

		args, reply := endTxnArgs(txn, commit, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp
		if err := tc.rng.AddCmd(proto.EndTransaction, args, reply, true); err != nil {
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

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
		txn := newTransaction("test", key, 1, test.isolation, tc.clock)
		// End the transaction with args timestamp moved forward in time.
		args, reply := endTxnArgs(txn, test.commit, 1, tc.store.StoreID())
		tc.manualClock.Set(1)
		args.Timestamp = tc.clock.Now()
		err := tc.rng.AddCmd(proto.EndTransaction, args, reply, true)
		if test.expErr {
			if err == nil {
				t.Errorf("expected error")
			}
			if _, ok := err.(*proto.TransactionRetryError); !ok {
				t.Errorf("expected retry error; got %s", err)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error: %s", err)
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)

	// Start out with a heartbeat to the transaction.
	hbArgs, hbReply := heartbeatArgs(txn, 1, tc.store.StoreID())
	hbArgs.Timestamp = txn.Timestamp
	if err := tc.rng.AddCmd(proto.InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
		t.Error(err)
	}

	// Now end the txn with increased epoch and priority.
	args, reply := endTxnArgs(txn, true, 1, tc.store.StoreID())
	args.Timestamp = txn.Timestamp
	args.Txn.Epoch = txn.Epoch + 1
	args.Txn.Priority = txn.Priority + 1
	if err := tc.rng.AddCmd(proto.EndTransaction, args, reply, true); err != nil {
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	regressTS := tc.clock.Now()
	tc.manualClock.Set(1)
	txn := newTransaction("test", proto.Key(""), 1, proto.SERIALIZABLE, tc.clock)

	testCases := []struct {
		key          proto.Key
		existStatus  proto.TransactionStatus
		existEpoch   int32
		existTS      proto.Timestamp
		expErrRegexp string
	}{
		{proto.Key("a"), proto.COMMITTED, txn.Epoch, txn.Timestamp, "txn \"test\" {.*}: already committed"},
		{proto.Key("b"), proto.ABORTED, txn.Epoch, txn.Timestamp, "txn aborted \"test\" {.*}"},
		{proto.Key("c"), proto.PENDING, txn.Epoch + 1, txn.Timestamp, "txn \"test\" {.*}: epoch regression: 0"},
		{proto.Key("d"), proto.PENDING, txn.Epoch, regressTS, "txn \"test\" {.*}: timestamp regression: 0.000000001,0"},
	}
	for _, test := range testCases {
		// Establish existing txn state by writing directly to range engine.
		var existTxn proto.Transaction
		gogoproto.Merge(&existTxn, txn)
		existTxn.Key = test.key
		existTxn.Status = test.existStatus
		existTxn.Epoch = test.existEpoch
		existTxn.Timestamp = test.existTS
		txnKey := engine.TransactionKey(test.key, txn.ID)
		if err := engine.MVCCPutProto(tc.rng.rm.Engine(), nil, txnKey, proto.ZeroTimestamp,
			nil, &existTxn); err != nil {
			t.Fatal(err)
		}

		// End the transaction, verify expected error.
		txn.Key = test.key
		args, reply := endTxnArgs(txn, true, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp
		verifyErrorMatches(tc.rng.AddCmd(proto.EndTransaction, args, reply, true), test.expErrRegexp, t)
	}
}

// TestInternalPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestInternalPushTxnBadKey(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", proto.Key("b"), 1, proto.SERIALIZABLE, tc.clock)

	args, reply := pushTxnArgs(pusher, pushee, true, 1, tc.store.StoreID())
	args.Key = pusher.Key
	verifyErrorMatches(tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true), ".*should match pushee.*", t)
}

// TestInternalPushTxnAlreadyCommittedOrAborted verifies success
// (noop) in event that pushee is already committed or aborted.
func TestInternalPushTxnAlreadyCommittedOrAborted(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	for i, status := range []proto.TransactionStatus{proto.COMMITTED, proto.ABORTED} {
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pusher.Priority = 1
		pushee.Priority = 2 // pusher will lose, meaning we shouldn't push unless pushee is already ended.

		// End the pushee's transaction.
		etArgs, etReply := endTxnArgs(pushee, status == proto.COMMITTED, 1, tc.store.StoreID())
		etArgs.Timestamp = pushee.Timestamp
		if err := tc.rng.AddCmd(proto.EndTransaction, etArgs, etReply, true); err != nil {
			t.Fatal(err)
		}

		// Now try to push what's already committed or aborted.
		args, reply := pushTxnArgs(pusher, pushee, true, 1, tc.store.StoreID())
		if err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true); err != nil {
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

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
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee.Priority = 1
		pusher.Priority = 2 // Pusher will win.

		// First, establish "start" of existing pushee's txn via heartbeat.
		pushee.Epoch = test.startEpoch
		pushee.Timestamp = test.startTS
		hbArgs, hbReply := heartbeatArgs(pushee, 1, tc.store.StoreID())
		hbArgs.Timestamp = pushee.Timestamp
		if err := tc.rng.AddCmd(proto.InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction using updated values for epoch & timestamp.
		pushee.Epoch = test.epoch
		pushee.Timestamp = test.ts
		args, reply := pushTxnArgs(pusher, pushee, true, 1, tc.store.StoreID())
		if err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true); err != nil {
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
// pushed/aborted.
func TestInternalPushTxnHeartbeatTimeout(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

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
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via heartbeat.
		if test.heartbeat != nil {
			hbArgs, hbReply := heartbeatArgs(pushee, 1, tc.store.StoreID())
			hbArgs.Timestamp = *test.heartbeat
			if err := tc.rng.AddCmd(proto.InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
				t.Fatal(err)
			}
		}

		// Now, attempt to push the transaction with clock set to "currentTime".
		tc.manualClock.Set(test.currentTime)
		args, reply := pushTxnArgs(pusher, pushee, true, 1, tc.store.StoreID())
		err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true)
		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %s", i, test.expSuccess, err)
		}
		if err != nil {
			if _, ok := err.(*proto.TransactionPushError); !ok {
				t.Errorf("expected txn push error: %s", err)
			}
		}
	}
}

// TestInternalPushTxnOldEpoch verifies that a txn intent from an
// older epoch may be pushed.
func TestInternalPushTxnOldEpoch(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

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
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via heartbeat.
		pushee.Epoch = test.curEpoch
		hbArgs, hbReply := heartbeatArgs(pushee, 1, tc.store.StoreID())
		hbArgs.Timestamp = pushee.Timestamp
		if err := tc.rng.AddCmd(proto.InternalHeartbeatTxn, hbArgs, hbReply, true); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction with intent epoch set appropriately.
		pushee.Epoch = test.intentEpoch
		args, reply := pushTxnArgs(pusher, pushee, true, 1, tc.store.StoreID())
		err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true)
		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %s", i, test.expSuccess, err)
		}
		if err != nil {
			if _, ok := err.(*proto.TransactionPushError); !ok {
				t.Errorf("expected txn push error; got %s", err)
			}
		}
	}
}

// TestInternalPushTxnPriorities verifies that txns with lower
// priority are pushed; if priorities are equal, then the txns
// are ordered by txn timestamp, with the more recent timestamp
// being pushable.
func TestInternalPushTxnPriorities(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

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
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pusher.Priority = test.pusherPriority
		pushee.Priority = test.pusheePriority
		pusher.Timestamp = test.pusherTS
		pushee.Timestamp = test.pusheeTS

		// Now, attempt to push the transaction with intent epoch set appropriately.
		args, reply := pushTxnArgs(pusher, pushee, test.abort, 1, tc.store.StoreID())
		err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true)
		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %s", i, test.expSuccess, err)
		}
		if err != nil {
			if _, ok := err.(*proto.TransactionPushError); !ok {
				t.Errorf("expected txn push error: %s", err)
			}
		}
	}
}

// TestInternalPushTxnPushTimestamp verifies that with args.Abort is
// false (i.e. for read/write conflict), the pushed txn keeps status
// PENDING, but has its txn Timestamp moved forward to the pusher's
// txn Timestamp + 1.
func TestInternalPushTxnPushTimestamp(t *testing.T) {
	tc := testContext{}
	tc.Start(t)

	pusher := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", proto.Key("b"), 1, proto.SERIALIZABLE, tc.clock)
	pusher.Priority = 2
	pushee.Priority = 1 // pusher will win
	pusher.Timestamp = proto.Timestamp{WallTime: 50, Logical: 25}
	pushee.Timestamp = proto.Timestamp{WallTime: 5, Logical: 1}

	// Now, push the transaction with args.Abort=false.
	args, reply := pushTxnArgs(pusher, pushee, false /* abort */, 1, tc.store.StoreID())
	if err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true); err != nil {
		t.Errorf("unexpected error on push: %s", err)
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
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", proto.Key("b"), 1, proto.SERIALIZABLE, tc.clock)
	pusher.Priority = 1
	pushee.Priority = 2 // pusher will lose
	pusher.Timestamp = proto.Timestamp{WallTime: 50, Logical: 0}
	pushee.Timestamp = proto.Timestamp{WallTime: 50, Logical: 1}

	// Now, push the transaction with args.Abort=false.
	args, reply := pushTxnArgs(pusher, pushee, false /* abort */, 1, tc.store.StoreID())
	if err := tc.rng.AddCmd(proto.InternalPushTxn, args, reply, true); err != nil {
		t.Errorf("unexpected error on push: %s", err)
	}
	if !reply.PusheeTxn.Timestamp.Equal(pushee.Timestamp) {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.Timestamp, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != proto.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}

func verifyRangeStats(eng engine.Engine, raftID int64, expMS engine.MVCCStats, t *testing.T) {
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, raftID, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expMS, ms) {
		t.Errorf("expected stats %+v; got %+v", expMS, ms)
	}
	// Also verify the GetRangeSize method.
	rangeSize, err := engine.MVCCGetRangeSize(eng, raftID)
	if err != nil {
		t.Fatal(err)
	}
	if expSize := expMS.KeyBytes + expMS.ValBytes; expSize != rangeSize {
		t.Errorf("expected range size %d; got %d", expSize, rangeSize)
	}
}

// TestRangeStatsComputation verifies that commands executed against a
// range update the range stat counters. The stat values are
// empirically derived; we're really just testing that they increment
// in the right ways, not the exact amounts. If the encodings change,
// will need to update this test.
func TestRangeStatsComputation(t *testing.T) {
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	// Put a value.
	pArgs, pReply := putArgs([]byte("a"), []byte("value1"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()
	if err := tc.rng.AddCmd(proto.Put, pArgs, pReply, true); err != nil {
		t.Fatal(err)
	}
	expMS := engine.MVCCStats{LiveBytes: 44, KeyBytes: 20, ValBytes: 24, IntentBytes: 0, LiveCount: 1, KeyCount: 1, ValCount: 1, IntentCount: 0}
	verifyRangeStats(tc.engine, tc.rng.Desc.RaftID, expMS, t)

	// Put a 2nd value transactionally.
	pArgs, pReply = putArgs([]byte("b"), []byte("value2"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()
	pArgs.Txn = &proto.Transaction{ID: []byte("txn1"), Timestamp: pArgs.Timestamp}
	if err := tc.rng.AddCmd(proto.Put, pArgs, pReply, true); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 124 + 2, KeyBytes: 40, ValBytes: 84 + 2, IntentBytes: 28, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc.RaftID, expMS, t)

	// Resolve the 2nd value.
	rArgs := &proto.InternalResolveIntentRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp: pArgs.Txn.Timestamp,
			Key:       pArgs.Key,
			RaftID:    tc.rng.Desc.RaftID,
			Replica:   proto.Replica{StoreID: tc.store.StoreID()},
			Txn:       pArgs.Txn,
		},
	}
	rArgs.Txn.Status = proto.COMMITTED
	rReply := &proto.InternalResolveIntentResponse{}
	if err := tc.rng.AddCmd(proto.InternalResolveIntent, rArgs, rReply, true); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 88, KeyBytes: 40, ValBytes: 48, IntentBytes: 0, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 0}
	verifyRangeStats(tc.engine, tc.rng.Desc.RaftID, expMS, t)

	// Delete the 1st value.
	dArgs, dReply := deleteArgs([]byte("a"), 1, tc.store.StoreID())
	dArgs.Timestamp = tc.clock.Now()
	if err := tc.rng.AddCmd(proto.Delete, dArgs, dReply, true); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 44, KeyBytes: 56, ValBytes: 50, IntentBytes: 0, LiveCount: 1, KeyCount: 2, ValCount: 3, IntentCount: 0}
	verifyRangeStats(tc.engine, tc.rng.Desc.RaftID, expMS, t)
}

// TestInternalMerge verifies that the InternalMerge command is behaving as
// expected. Merge semantics for different data types are tested more robustly
// at the engine level; this test is intended only to show that values passed to
// InternalMerge are being merged.
func TestInternalMerge(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("mergedkey")
	stringArgs := []string{"a", "b", "c", "d"}
	stringExpected := "abcd"

	for _, str := range stringArgs {
		mergeArgs, resp := internalMergeArgs(key, proto.Value{Bytes: []byte(str)}, 1,
			tc.store.StoreID())
		if err := tc.rng.AddCmd(proto.InternalMerge, mergeArgs, resp, true); err != nil {
			t.Fatalf("unexpected error from InternalMerge: %s", err.Error())
		}
	}

	getArgs, resp := getArgs(key, 1, tc.store.StoreID())
	if err := tc.rng.AddCmd(proto.Get, getArgs, resp, true); err != nil {
		t.Fatalf("unexpected error from Get: %s", err.Error())
	}
	if resp.Value == nil {
		t.Fatal("GetResponse had nil value")
	}
	if a, e := resp.Value.Bytes, []byte(stringExpected); !bytes.Equal(a, e) {
		t.Errorf("Get did not return expected value: %s != %s", string(a), e)
	}
}

// TestInternalTruncateLog verifies that the InternalTruncateLog command
// removes a prefix of the raft logs (modifying FirstIndex() and making them
// inaccessible via Entries()).
func TestInternalTruncateLog(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args, resp := incrementArgs([]byte("a"), int64(i), 1, tc.store.StoreID())
		if err := tc.rng.AddCmd(proto.Increment, args, resp, true); err != nil {
			t.Fatal(err)
		}
		idx, err := tc.rng.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	// Discard the first half of the log
	truncateArgs, truncateResp := internalTruncateLogArgs(indexes[5], 1, tc.store.StoreID())
	if err := tc.rng.AddCmd(proto.InternalTruncateLog, truncateArgs, truncateResp, true); err != nil {
		t.Fatal(err)
	}

	// FirstIndex has changed.
	firstIndex, err := tc.rng.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != indexes[5] {
		t.Errorf("expected firstIndex == %d, got %d", indexes[5], firstIndex)
	}

	// We can still get what remains of the log.
	entries, err := tc.rng.Entries(indexes[5], indexes[9])
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != int(indexes[9]-indexes[5]) {
		t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
	}

	// But any range that includes the truncated entries returns an error.
	_, err = tc.rng.Entries(indexes[4], indexes[9])
	if err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
}

func TestRaftStorage(t *testing.T) {
	var tc testContext
	storagetest.RunTests(t,
		func(t *testing.T) storagetest.WriteableStorage {
			tc = testContext{}
			tc.Start(t)
			return tc.rng
		},
		func(t *testing.T, r storagetest.WriteableStorage) {
			tc.Stop()
		})
}

// TestConditionFailedError tests that a ConditionFailedError correctly
// bubbles up from MVCC to Range.
func TestConditionFailedError(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("k")
	value := []byte("quack")
	pArgs, pReply := putArgs(key, value, 1, tc.store.StoreID())
	if err := tc.rng.executeCmd(proto.Put, pArgs, pReply); err != nil {
		t.Fatal(err)
	}
	args := &proto.ConditionalPutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			Timestamp: proto.MinTimestamp,
			RaftID:    1,
			Replica:   proto.Replica{StoreID: tc.store.StoreID()},
		},
		Value: proto.Value{
			Bytes: value,
		},
		ExpValue: &proto.Value{
			Bytes: []byte("moo"),
		},
	}
	reply := &proto.ConditionalPutResponse{}
	err := tc.rng.executeCmd(proto.ConditionalPut, args, reply)
	if cErr, ok := err.(*proto.ConditionFailedError); err == nil || !ok {
		t.Fatalf("expected ConditionFailedError, got %T with content %+v",
			err, err)
	} else if v := cErr.ActualValue; v == nil || !bytes.Equal(v.Bytes, value) {
		t.Errorf("ConditionFailedError with bytes %q expected, but got %+v",
			value, v)
	}
}

// TestReplicaSetsEqual tests to ensure that intersectReplicaSets
// returns the correct responses.
func TestReplicaSetsEqual(t *testing.T) {
	testData := []struct {
		expected bool
		a        []proto.Replica
		b        []proto.Replica
	}{
		{true, []proto.Replica{}, []proto.Replica{}},
		{true, createReplicaSets([]proto.StoreID{1}), createReplicaSets([]proto.StoreID{1})},
		{true, createReplicaSets([]proto.StoreID{1, 2}), createReplicaSets([]proto.StoreID{1, 2})},
		{true, createReplicaSets([]proto.StoreID{1, 2}), createReplicaSets([]proto.StoreID{2, 1})},
		{false, createReplicaSets([]proto.StoreID{1}), createReplicaSets([]proto.StoreID{2})},
		{false, createReplicaSets([]proto.StoreID{1, 2}), createReplicaSets([]proto.StoreID{2})},
		{false, createReplicaSets([]proto.StoreID{1, 2}), createReplicaSets([]proto.StoreID{1})},
		{false, createReplicaSets([]proto.StoreID{}), createReplicaSets([]proto.StoreID{1})},
		{true, createReplicaSets([]proto.StoreID{1, 2, 3}), createReplicaSets([]proto.StoreID{2, 3, 1})},
		{true, createReplicaSets([]proto.StoreID{1, 1}), createReplicaSets([]proto.StoreID{1, 1})},
		{false, createReplicaSets([]proto.StoreID{1, 1}), createReplicaSets([]proto.StoreID{1, 1, 1})},
		{true, createReplicaSets([]proto.StoreID{1, 2, 3, 1, 2, 3}), createReplicaSets([]proto.StoreID{1, 1, 2, 2, 3, 3})},
	}
	for _, test := range testData {
		if ReplicaSetsEqual(test.a, test.b) != test.expected {
			t.Fatalf("unexpected replica intersection: %+v", test)
		}
	}
}
