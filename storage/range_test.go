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
	"math"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/multiraft/storagetest"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/coreos/etcd/raft"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
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

func testRangeDescriptor() *proto.RangeDescriptor {
	return &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: proto.KeyMin,
		EndKey:   proto.KeyMax,
		Replicas: []proto.Replica{
			{
				NodeID:  1,
				StoreID: 1,
			},
		},
	}
}

// boostrapMode controls how the first range is created in testContext.
type bootstrapMode int

const (
	// Use Store.BootstrapRange, which writes the range descriptor and
	// other metadata. Most tests should use this mode because it more
	// closely resembles the real world.
	bootstrapRangeWithMetadata bootstrapMode = iota
	// Create a range with NewRange and Store.AddRangeTest. The store's data
	// will be persisted but metadata will not. This mode is provided
	// for backwards compatibility for tests that expect the store to
	// initially be empty.
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
	rangeID       proto.RaftID
	gossip        *gossip.Gossip
	engine        engine.Engine
	manualClock   *hlc.ManualClock
	clock         *hlc.Clock
	stopper       *util.Stopper
	bootstrapMode bootstrapMode
	feed          *util.Feed
}

// testContext.Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(t testing.TB) {
	if tc.stopper == nil {
		tc.stopper = util.NewStopper()
	}
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(testBaseContext, hlc.NewClock(hlc.UnixNano), tc.stopper)
		tc.gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
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
	tc.stopper.AddCloser(tc.transport)
	if tc.feed != nil {
		tc.stopper.AddCloser(tc.feed)
	}

	if tc.store == nil {
		ctx := TestStoreContext
		ctx.Clock = tc.clock
		ctx.Gossip = tc.gossip
		ctx.Transport = tc.transport
		ctx.EventFeed = tc.feed
		// Create a test sender without setting a store. This is to deal with the
		// circular dependency between the test sender and the store. The actual
		// store will be passed to the sender after it is created and bootstrapped.
		sender := &testSender{}
		var err error
		if ctx.DB, err = client.Open("//root@", client.SenderOpt(sender)); err != nil {
			t.Fatal(err)
		}
		tc.store = NewStore(ctx, tc.engine, &proto.NodeDescriptor{NodeID: 1})
		if err := tc.store.Bootstrap(proto.StoreIdent{
			ClusterID: "test",
			NodeID:    1,
			StoreID:   1,
		}, tc.stopper); err != nil {
			t.Fatal(err)
		}
		// Now that we have our actual store, monkey patch the sender used in ctx.DB.
		sender.store = tc.store
		// We created the store without a real KV client, so it can't perform splits.
		tc.store._splitQueue.disabled = true

		if tc.rng == nil && tc.bootstrapMode == bootstrapRangeWithMetadata {
			if err := tc.store.BootstrapRange(); err != nil {
				t.Fatal(err)
			}
		}
		if err := tc.store.Start(tc.stopper); err != nil {
			t.Fatal(err)
		}
		tc.store.WaitForInit()
	}

	initConfigs(tc.engine, t)

	if tc.rng == nil {
		if tc.bootstrapMode == bootstrapRangeOnly {
			rng, err := NewRange(testRangeDescriptor(), tc.store)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AddRangeTest(rng); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		tc.rng, err = tc.store.GetRange(1)
		if err != nil {
			t.Fatal(err)
		}
		tc.rangeID = tc.rng.Desc().RaftID
	}
}

func (tc *testContext) Stop() {
	tc.stopper.Stop()
}

// initConfigs creates default configuration entries.
func initConfigs(e engine.Engine, t testing.TB) {
	timestamp := proto.MinTimestamp.Next()
	if err := engine.MVCCPutProto(e, nil, keys.ConfigAccountingPrefix, timestamp, nil, &testDefaultAcctConfig); err != nil {
		t.Fatal(err)
	}
	if err := engine.MVCCPutProto(e, nil, keys.ConfigPermissionPrefix, timestamp, nil, &testDefaultPermConfig); err != nil {
		t.Fatal(err)
	}
	if err := engine.MVCCPutProto(e, nil, keys.ConfigZonePrefix, timestamp, nil, &testDefaultZoneConfig); err != nil {
		t.Fatal(err)
	}
}

func newTransaction(name string, baseKey proto.Key, userPriority int32,
	isolation proto.IsolationType, clock *hlc.Clock) *proto.Transaction {
	return proto.NewTransaction(name, keys.KeyAddress(baseKey), userPriority,
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
	defer leaktest.AfterTest(t)
	desc := &proto.RangeDescriptor{
		RaftID:   1,
		StartKey: proto.Key("a"),
		EndKey:   proto.Key("b"),
	}

	e := engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
	clock := hlc.NewClock(hlc.UnixNano)
	transport := multiraft.NewLocalRPCTransport()
	defer transport.Close()
	ctx := TestStoreContext
	ctx.Clock = clock
	ctx.Transport = multiraft.NewLocalRPCTransport()
	store := NewStore(ctx, e, &proto.NodeDescriptor{NodeID: 1})
	r, err := NewRange(desc, store)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ContainsKey(proto.Key("aa")) {
		t.Errorf("expected range to contain key \"aa\"")
	}
	if !r.ContainsKey(keys.RangeDescriptorKey([]byte("aa"))) {
		t.Errorf("expected range to contain range descriptor key for \"aa\"")
	}
	if !r.ContainsKeyRange(proto.Key("aa"), proto.Key("b")) {
		t.Errorf("expected range to contain key range \"aa\"-\"b\"")
	}
	if !r.ContainsKeyRange(keys.RangeDescriptorKey([]byte("aa")),
		keys.RangeDescriptorKey([]byte("b"))) {
		t.Errorf("expected range to contain key transaction range \"aa\"-\"b\"")
	}
}

func setLeaderLease(t *testing.T, r *Range, l *proto.Lease) {
	args := &proto.InternalLeaderLeaseRequest{Lease: *l}
	reply := &proto.InternalLeaderLeaseResponse{}
	errChan, pendingCmd := r.proposeRaftCommand(r.context(), args, reply)
	var err error
	if err = <-errChan; err == nil {
		// Next if the command was committed, wait for the range to apply it.
		err = <-pendingCmd.done
	}
	if err != nil {
		t.Errorf("failed to set lease: %s", err)
	}
}

// TestRangeReadConsistency verifies behavior of the range under
// different read consistencies. Note that this unittest plays
// fast and loose with granting leader leases.
func TestRangeReadConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	gArgs, gReply := getArgs(proto.Key("a"), 1, tc.store.StoreID())
	gArgs.Timestamp = tc.clock.Now()

	// Try consistent read and verify success.

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true); err != nil {

		t.Errorf("expected success on consistent read: %s", err)
	}

	// Try a consensus read and verify error.
	gArgs.ReadConsistency = proto.CONSENSUS

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true); err == nil {

		t.Errorf("expected error on consensus read")
	}

	// Try an inconsistent read within a transaction.
	gArgs.ReadConsistency = proto.INCONSISTENT
	gArgs.Txn = newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true); err == nil {

		t.Errorf("expected error on inconsistent read within a txn")
	}

	// Lose the lease and verify CONSISTENT reads receive NotLeaderError
	// and INCONSISTENT reads work as expected.
	start := tc.rng.getLease().Expiration.Add(1, 0)
	tc.manualClock.Set(start.WallTime)
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      start,
		Expiration: start.Add(10, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2), // a different node
	})
	gArgs.ReadConsistency = proto.CONSISTENT
	gArgs.Txn = nil

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true)

	if _, ok := err.(*proto.NotLeaderError); !ok {
		t.Errorf("expected not leader error; got %s", err)
	}

	gArgs.ReadConsistency = proto.INCONSISTENT

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true); err != nil {

		t.Errorf("expected success reading with inconsistent: %s", err)
	}
}

func TestRangeRangeBoundsChecking(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	splitTestRange(tc.store, proto.Key("a"), proto.Key("a"), t)
	gArgs, gReply := getArgs(proto.Key("b"), 1, tc.store.StoreID())

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true)

	if _, ok := err.(*proto.RangeKeyMismatchError); !ok {
		t.Errorf("expected range key mismatch error: %s", err)
	}
}

func TestRangeHasLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.clock.SetMaxOffset(maxClockOffset)

	if held, _ := tc.rng.HasLeaderLease(tc.clock.Now()); !held {
		t.Errorf("expected lease on range start")
	}
	tc.manualClock.Set(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      now.Add(10, 0),
		Expiration: now.Add(20, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2),
	})
	if held, expired := tc.rng.HasLeaderLease(tc.clock.Now().Add(15, 0)); held || expired {
		t.Errorf("expected another replica to have leader lease")
	}

	err := tc.rng.redirectOnOrAcquireLeaderLease(tc.clock.Now())
	if lErr, ok := err.(*proto.NotLeaderError); !ok || lErr == nil {
		t.Fatalf("wanted NotLeaderError, got %s", err)
	}

	// Advance clock past expiration and verify that another has
	// leader lease will not be true.
	tc.manualClock.Increment(21) // 21ns pass
	if held, expired := tc.rng.HasLeaderLease(tc.clock.Now()); held || !expired {
		t.Errorf("expected another replica to have expired lease")
	}
}

func TestRangeNotLeaderError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2),
	})

	header := proto.RequestHeader{
		Key:       proto.Key("a"),
		RaftID:    tc.rng.Desc().RaftID,
		Replica:   proto.Replica{StoreID: tc.store.StoreID()},
		Timestamp: now,
	}
	testCases := []struct {
		args  proto.Request
		reply proto.Response
	}{
		// Admin split covers admin commands.
		{&proto.AdminSplitRequest{
			RequestHeader: header,
			SplitKey:      proto.Key("a"),
		},
			&proto.AdminSplitResponse{},
		},
		// Get covers read-only commands.
		{&proto.GetRequest{
			RequestHeader: header,
		},
			&proto.GetResponse{},
		},
		// Put covers read-write commands.
		{&proto.PutRequest{
			RequestHeader: header,
			Value: proto.Value{
				Bytes: []byte("value"),
			},
		},
			&proto.PutResponse{},
		},
	}

	for i, test := range testCases {

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: test.args, Reply: test.reply}, true)

		if _, ok := err.(*proto.NotLeaderError); !ok {
			t.Errorf("%d: expected not leader error: %s", i, err)
		}
	}
}

// TestRangeGossipConfigsOnLease verifies that config info is gossiped
// upon acquisition of the leader lease.
func TestRangeGossipConfigsOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Add a permission for a new key prefix.
	db1Perm := proto.PermConfig{
		Read:  []string{"spencer", "foo", "bar", "baz"},
		Write: []string{"spencer"},
	}
	key := keys.MakeKey(keys.ConfigPermissionPrefix, proto.Key("/db1"))
	if err := engine.MVCCPutProto(tc.engine, nil, key, proto.MinTimestamp, nil, &db1Perm); err != nil {
		t.Fatal(err)
	}

	verifyPerm := func() bool {
		info, err := tc.gossip.GetInfo(gossip.KeyConfigPermission)
		if err != nil {
			t.Fatal(err)
		}
		configMap := info.(PrefixConfigMap)
		expConfigs := []*PrefixConfig{
			{proto.KeyMin, nil, &testDefaultPermConfig},
			{proto.Key("/db1"), nil, &db1Perm},
			{proto.Key("/db2"), proto.KeyMin, &testDefaultPermConfig},
		}
		return reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs)
	}

	// If this actually failed, we would have gossiped from MVCCPutProto.
	// Unlikely, but why not check.
	if verifyPerm() {
		t.Errorf("not expecting gossip of new config until new lease is acquired")
	}

	// Expire our own lease which we automagically acquired due to being
	// first range and config holder.
	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()

	// Give lease to someone else.
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2),
	})

	// Expire that lease.
	tc.manualClock.Increment(11 + int64(tc.clock.MaxOffset())) // advance time
	now = tc.clock.Now()

	// Give lease to this range.
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      now.Add(11, 0),
		Expiration: now.Add(20, 0),
		RaftNodeID: tc.store.RaftNodeID(),
	})
	if !verifyPerm() {
		t.Errorf("expected gossip of new config")
	}
}

// TestRangeTSCacheLowWaterOnLease verifies that the low water mark is
// set on the timestamp cache when the node is granted the leader
// lease after not holding it and it is not set when the node is
// granted the leader lease when it was the last holder.
func TestRangeTSCacheLowWaterOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.clock.SetMaxOffset(maxClockOffset)

	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := proto.Timestamp{WallTime: tc.manualClock.UnixNano()}

	baseRTS, _ := tc.rng.tsCache.GetMax(proto.Key("a"), nil /* end */, nil /* txn */)
	baseLowWater := baseRTS.WallTime

	testCases := []struct {
		nodeID      proto.RaftNodeID
		start       proto.Timestamp
		expiration  proto.Timestamp
		expLowWater int64
	}{
		// Grant the lease fresh.
		{tc.store.RaftNodeID(), now, now.Add(10, 0), baseLowWater},
		// Renew the lease.
		{tc.store.RaftNodeID(), now.Add(15, 0), now.Add(30, 0), baseLowWater},
		// Renew the lease but shorten expiration.
		{tc.store.RaftNodeID(), now.Add(16, 0), now.Add(25, 0), baseLowWater},
		// Lease is held by another.
		{proto.MakeRaftNodeID(2, 2), now.Add(29, 0), now.Add(50, 0), baseLowWater},
		// Lease is regranted to this replica.
		{tc.store.RaftNodeID(), now.Add(60, 0), now.Add(70, 0), now.Add(50, 0).WallTime + int64(maxClockOffset) + baseLowWater},
	}

	for i, test := range testCases {
		setLeaderLease(t, tc.rng, &proto.Lease{
			Start:      test.start,
			Expiration: test.expiration,
			RaftNodeID: test.nodeID,
		})
		// Verify expected low water mark.
		rTS, wTS := tc.rng.tsCache.GetMax(proto.Key("a"), nil, nil)
		if rTS.WallTime != test.expLowWater || wTS.WallTime != test.expLowWater {
			t.Errorf("%d: expected low water %d; got %d, %d", i, test.expLowWater, rTS.WallTime, wTS.WallTime)
		}
	}
}

// TestRangeGossipFirstRange verifies that the first range gossips its
// location and the cluster ID.
func TestRangeGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	for _, key := range []string{gossip.KeyClusterID, gossip.KeyFirstRangeDescriptor, gossip.KeySentinel} {
		info, err := tc.gossip.GetInfo(key)
		if err != nil {
			t.Errorf("missing first range gossip of key %s", key)
		}
		if key == gossip.KeyFirstRangeDescriptor &&
			info.(proto.RangeDescriptor).RaftID == 0 {
			t.Errorf("expected gossiped range location, got %+v", info.(proto.RangeDescriptor))
		}
		if key == gossip.KeyClusterID && info.(string) == "" {
			t.Errorf("expected non-empty gossiped cluster ID, got %+v", info)
		}
		if key == gossip.KeySentinel && info.(string) == "" {
			t.Errorf("expected non-empty gossiped sentinel, got %+v", info)
		}
	}
}

// TestRangeGossipAllConfigs verifies that all config types are gossiped.
func TestRangeGossipAllConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	testData := []struct {
		gossipKey string
		configs   []*PrefixConfig
	}{
		{gossip.KeyConfigAccounting, []*PrefixConfig{{proto.KeyMin, nil, &testDefaultAcctConfig}}},
		{gossip.KeyConfigPermission, []*PrefixConfig{{proto.KeyMin, nil, &testDefaultPermConfig}}},
		{gossip.KeyConfigZone, []*PrefixConfig{{proto.KeyMin, nil, &testDefaultZoneConfig}}},
	}
	for _, test := range testData {
		_, err := tc.gossip.GetInfo(test.gossipKey)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestRangeGossipConfigWithMultipleKeyPrefixes verifies that multiple
// key prefixes for a config are gossiped.
func TestRangeGossipConfigWithMultipleKeyPrefixes(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Add a permission for a new key prefix.
	db1Perm := &proto.PermConfig{
		Read:  []string{"spencer", "foo", "bar", "baz"},
		Write: []string{"spencer"},
	}
	key := keys.MakeKey(keys.ConfigPermissionPrefix, proto.Key("/db1"))
	data, err := gogoproto.Marshal(db1Perm)
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key, Timestamp: proto.MinTimestamp},
		Value:         proto.Value{Bytes: data},
	}
	reply := &proto.PutResponse{}

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: req, Reply: reply}, true); err != nil {

		t.Fatal(err)
	}

	info, err := tc.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		{proto.KeyMin, nil, &testDefaultPermConfig},
		{proto.Key("/db1"), nil, db1Perm},
		{proto.Key("/db2"), proto.KeyMin, &testDefaultPermConfig},
	}
	if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
	}
}

// TestRangeGossipConfigUpdates verifies that writes to the
// permissions cause the updated configs to be re-gossiped.
func TestRangeGossipConfigUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Add a permission for a new key prefix.
	db1Perm := &proto.PermConfig{
		Read:  []string{"spencer"},
		Write: []string{"spencer"},
	}
	key := keys.MakeKey(keys.ConfigPermissionPrefix, proto.Key("/db1"))
	data, err := gogoproto.Marshal(db1Perm)
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: key, Timestamp: proto.MinTimestamp},
		Value:         proto.Value{Bytes: data},
	}
	reply := &proto.PutResponse{}

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: req, Reply: reply}, true); err != nil {

		t.Fatal(err)
	}

	info, err := tc.gossip.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(PrefixConfigMap)
	expConfigs := []*PrefixConfig{
		{proto.KeyMin, nil, &testDefaultPermConfig},
		{proto.Key("/db1"), nil, db1Perm},
		{proto.Key("/db2"), proto.KeyMin, &testDefaultPermConfig},
	}
	if !reflect.DeepEqual([]*PrefixConfig(configMap), expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap, expConfigs)
	}
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, raftID proto.RaftID, storeID proto.StoreID) (*proto.GetRequest, *proto.GetResponse) {
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
func putArgs(key, value []byte, raftID proto.RaftID, storeID proto.StoreID) (*proto.PutRequest, *proto.PutResponse) {
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
func deleteArgs(key proto.Key, raftID proto.RaftID, storeID proto.StoreID) (*proto.DeleteRequest, *proto.DeleteResponse) {
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
func readOrWriteArgs(key proto.Key, read bool, raftID proto.RaftID, storeID proto.StoreID) (proto.Request, proto.Response) {
	if read {
		gArgs, gReply := getArgs(key, raftID, storeID)
		return gArgs, gReply
	}
	pArgs, pReply := putArgs(key, []byte("value"), raftID, storeID)
	return pArgs, pReply
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, raftID proto.RaftID, storeID proto.StoreID) (*proto.IncrementRequest, *proto.IncrementResponse) {
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

func scanArgs(start, end []byte, raftID proto.RaftID, storeID proto.StoreID) (*proto.ScanRequest, *proto.ScanResponse) {
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
func endTxnArgs(txn *proto.Transaction, commit bool, raftID proto.RaftID, storeID proto.StoreID) (
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
func pushTxnArgs(pusher, pushee *proto.Transaction, pushType proto.PushTxnType, raftID proto.RaftID, storeID proto.StoreID) (
	*proto.InternalPushTxnRequest, *proto.InternalPushTxnResponse) {
	args := &proto.InternalPushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:       pushee.Key,
			Timestamp: pusher.Timestamp,
			RaftID:    raftID,
			Replica:   proto.Replica{StoreID: storeID},
			Txn:       pusher,
		},
		Now:       pusher.Timestamp,
		PusheeTxn: *pushee,
		PushType:  pushType,
	}
	reply := &proto.InternalPushTxnResponse{}
	return args, reply
}

// heartbeatArgs returns request/response pair for InternalHeartbeatTxn RPC.
func heartbeatArgs(txn *proto.Transaction, raftID proto.RaftID, storeID proto.StoreID) (
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
func internalMergeArgs(key []byte, value proto.Value, raftID proto.RaftID, storeID proto.StoreID) (
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

func internalTruncateLogArgs(index uint64, raftID proto.RaftID, storeID proto.StoreID) (
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

// TestAcquireLeaderLease verifies that the leader lease is acquired
// for read and write methods.
func TestAcquireLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)

	gArgs, gReply := getArgs([]byte("a"), 1, 0)
	pArgs, pReply := putArgs([]byte("b"), []byte("1"), 1, 0)

	testCases := []struct {
		args  proto.Request
		reply proto.Response
	}{
		{gArgs, gReply},
		{pArgs, pReply},
	}
	for i, test := range testCases {
		tc := testContext{}
		tc.Start(t)
		// This is a single-replica test; since we're automatically pushing back
		// the start of a lease as far as possible, and since there is an auto-
		// matic lease for us at the beginning, we'll basically create a lease from
		// then on.
		expStart := tc.rng.getLease().Expiration
		tc.manualClock.Set(int64(DefaultLeaderLeaseDuration + 1000))

		test.args.Header().Timestamp = tc.clock.Now()

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: test.args, Reply: test.reply}, true); err != nil {

			t.Fatal(err)
		}
		if held, expired := tc.rng.HasLeaderLease(test.args.Header().Timestamp); !held || expired {
			t.Fatalf("%d: expected lease acquisition", i)
		}
		lease := tc.rng.getLease()
		// The lease may start earlier than our request timestamp, but the
		// expiration will still be measured relative to it.
		expExpiration := test.args.Header().Timestamp.Add(int64(DefaultLeaderLeaseDuration), 0)
		if !lease.Start.Equal(expStart) || !lease.Expiration.Equal(expExpiration) {
			t.Errorf("%d: unexpected lease timing %s, %s; expected %s, %s", i,
				lease.Start, lease.Expiration, expStart, expExpiration)
		}
		tc.Stop()
	}
}

// TestRangeUpdateTSCache verifies that reads and writes update the
// timestamp cache.
func TestRangeUpdateTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	gArgs, gReply := getArgs([]byte("a"), 1, tc.store.StoreID())
	gArgs.Timestamp = tc.clock.Now()

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true)

	if err != nil {
		t.Error(err)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	tc.manualClock.Set(t1.Nanoseconds())
	pArgs, pReply := putArgs([]byte("b"), []byte("1"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()

	err = tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true)

	if err != nil {
		t.Error(err)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	rTS, wTS := tc.rng.tsCache.GetMax(proto.Key("a"), nil, nil)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != 0 {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s", rTS, wTS)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, wTS = tc.rng.tsCache.GetMax(proto.Key("b"), nil, nil)
	if rTS.WallTime != 0 || wTS.WallTime != t1.Nanoseconds() {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s", rTS, wTS)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, wTS = tc.rng.tsCache.GetMax(proto.Key("c"), nil, nil)
	if rTS.WallTime != 0 || wTS.WallTime != 0 {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s", rTS, wTS)
	}
}

// TestRangeCommandQueue verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestRangeCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	defer func() { TestingCommandFilter = nil }()

	// Intercept commands with matching command IDs and block them.
	blockingDone := make(chan struct{}, 1)
	TestingCommandFilter = func(args proto.Request, reply proto.Response) bool {
		if args.Header().User == "Foo" {
			<-blockingDone
		}
		return false
	}

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
		cmd1Done := make(chan struct{})
		go func() {
			args, reply := readOrWriteArgs(key1, test.cmd1Read, tc.rng.Desc().RaftID, tc.store.StoreID())
			args.Header().User = "Foo"

			err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd1Done)
		}()

		// First, try a command for same key as cmd1 to verify it blocks.
		cmd2Done := make(chan struct{})
		go func() {
			args, reply := readOrWriteArgs(key1, test.cmd2Read, tc.rng.Desc().RaftID, tc.store.StoreID())

			err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd2Done)
		}()

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		go func() {
			args, reply := readOrWriteArgs(key2, true, tc.rng.Desc().RaftID, tc.store.StoreID())

			err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

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

		blockingDone <- struct{}{}
		select {
		case <-cmd2Done:
			// success.
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("test %d: waited 500ms for cmd2 of key1", i)
		}
	}
}

// TestRangeCommandQueueInconsistent verifies that inconsistent reads need
// not wait for pending commands to complete through Raft.
func TestRangeCommandQueueInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer func() { TestingCommandFilter = nil }()
	defer tc.Stop()

	key := proto.Key("key1")
	blockingDone := make(chan struct{})
	TestingCommandFilter = func(args proto.Request, reply proto.Response) bool {
		if args.Header().CmdID.Random == 1 {
			<-blockingDone
		}
		return false
	}
	cmd1Done := make(chan struct{})
	go func() {
		args, reply := putArgs(key, []byte("value"), tc.rng.Desc().RaftID, tc.store.StoreID())
		args.CmdID.Random = 1

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

		if err != nil {
			t.Fatal(err)
		}
		close(cmd1Done)
	}()

	// An inconsistent read to the key won't wait.
	cmd2Done := make(chan struct{})
	go func() {
		args, reply := getArgs(key, tc.rng.Desc().RaftID, tc.store.StoreID())
		args.ReadConsistency = proto.INCONSISTENT

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

		if err != nil {
			t.Fatal(err)
		}
		close(cmd2Done)
	}()

	select {
	case <-cmd2Done:
		// success.
	case <-cmd1Done:
		t.Fatalf("cmd1 should have been blocked")
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waited 500ms for cmd2 of key")
	}

	close(blockingDone)
	select {
	case <-cmd1Done:
		// success.
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("waited 500ms for cmd2 of key")
	}
}

// TestRangeUseTSCache verifies that write timestamps are upgraded
// based on the read timestamp cache.
func TestRangeUseTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args, reply := getArgs([]byte("a"), 1, tc.store.StoreID())
	args.Timestamp = tc.clock.Now()

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

	if err != nil {
		t.Error(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("value"), 1, tc.store.StoreID())

	err = tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true)

	if err != nil {
		t.Fatal(err)
	}
	if pReply.Timestamp.WallTime != tc.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %s", pReply.Timestamp)
	}
}

// TestRangeNoTSCacheInconsistent verifies that the timestamp cache
// is no affected by inconsistent reads.
func TestRangeNoTSCacheInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args, reply := getArgs([]byte("a"), 1, tc.store.StoreID())
	args.Timestamp = tc.clock.Now()
	args.ReadConsistency = proto.INCONSISTENT

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

	if err != nil {
		t.Error(err)
	}
	pArgs, pReply := putArgs([]byte("a"), []byte("value"), 1, tc.store.StoreID())

	err = tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true)

	if err != nil {
		t.Fatal(err)
	}
	if pReply.Timestamp.WallTime == tc.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp not to upgrade to 1s; got %s", pReply.Timestamp)
	}
}

// TestRangeNoTSCacheUpdateOnFailure verifies that read and write
// commands do not update the timestamp cache if they result in
// failure.
func TestRangeNoTSCacheUpdateOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)
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

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

			t.Fatalf("test %d: %s", i, err)
		}

		// Now attempt read or write.
		args, reply := readOrWriteArgs(key, read, tc.rng.Desc().RaftID, tc.store.StoreID())
		args.Header().Timestamp = tc.clock.Now() // later timestamp

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err == nil {

			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
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

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: gArgs, Reply: gReply}, true); err != nil {

		t.Fatal(err)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs, pReply := putArgs(key, []byte("value"), 1, tc.store.StoreID())
	pArgs.Txn = txn
	pArgs.Timestamp = pArgs.Txn.Timestamp

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

		t.Fatal(err)
	}
	if !pReply.Timestamp.Equal(pArgs.Timestamp) {
		t.Errorf("expected timestamp to remain %s; got %s", pArgs.Timestamp, pReply.Timestamp)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	pArgs.Txn = nil
	expTS := pArgs.Timestamp
	expTS.Logical++

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err == nil {

		t.Errorf("expected write intent error")
	}
	if !pReply.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, pReply.Timestamp)
	}
}

// TestRangeIdempotence verifies that a retry increment with
// same client command ID receives same reply.
func TestRangeIdempotence(t *testing.T) {
	defer leaktest.AfterTest(t)
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

			err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: &args, Reply: &reply}, true)

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

// TestRangeResponseCacheError verifies that an error is returned to the
// client in the event that a response cache entry is found but is not
// decodable.
func TestRangeResponseCacheError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	args, reply := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
	args.CmdID = proto.ClientCmdID{WallTime: 1, Random: 1}

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

	if err != nil {
		t.Fatal(err)
	}

	// Overwrite repsonse cache entry with garbage for the last op.
	key := keys.ResponseCacheKey(tc.rng.Desc().RaftID, &args.CmdID)
	err = engine.MVCCPut(tc.engine, nil, key, proto.ZeroTimestamp, proto.Value{Bytes: []byte("\xff")}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now try increment again and verify error.
	reply.Reset()

	err = tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

	if err == nil {
		t.Fatal(err)
	}
}

// TestEndTransactionBeforeHeartbeat verifies that a transaction
// can be committed/aborted before being heartbeat.
func TestEndTransactionBeforeHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		args, reply := endTxnArgs(txn, commit, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

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

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: hbArgs, Reply: hbReply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)

		// Start out with a heartbeat to the transaction.
		hbArgs, hbReply := heartbeatArgs(txn, 1, tc.store.StoreID())
		hbArgs.Timestamp = txn.Timestamp

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: hbArgs, Reply: hbReply}, true); err != nil {

			t.Error(err)
		}
		if hbReply.Txn.Status != proto.PENDING || hbReply.Txn.LastHeartbeat == nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hbReply)
		}

		args, reply := endTxnArgs(txn, commit, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
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

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

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
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)

	// Start out with a heartbeat to the transaction.
	hbArgs, hbReply := heartbeatArgs(txn, 1, tc.store.StoreID())
	hbArgs.Timestamp = txn.Timestamp

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: hbArgs, Reply: hbReply}, true); err != nil {

		t.Error(err)
	}

	// Now end the txn with increased epoch and priority.
	args, reply := endTxnArgs(txn, true, 1, tc.store.StoreID())
	args.Timestamp = txn.Timestamp
	args.Txn.Epoch = txn.Epoch + 1
	args.Txn.Priority = txn.Priority + 1

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
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
		{proto.Key("a"), proto.COMMITTED, txn.Epoch, txn.Timestamp, "txn \"test\" id=.*: already committed"},
		{proto.Key("b"), proto.ABORTED, txn.Epoch, txn.Timestamp, "txn aborted \"test\" id=.*"},
		{proto.Key("c"), proto.PENDING, txn.Epoch + 1, txn.Timestamp, "txn \"test\" id=.*: epoch regression: 0"},
		{proto.Key("d"), proto.PENDING, txn.Epoch, regressTS, "txn \"test\" id=.*: timestamp regression: 0.000000001,0"},
	}
	for _, test := range testCases {
		// Establish existing txn state by writing directly to range engine.
		var existTxn proto.Transaction
		gogoproto.Merge(&existTxn, txn)
		existTxn.Key = test.key
		existTxn.Status = test.existStatus
		existTxn.Epoch = test.existEpoch
		existTxn.Timestamp = test.existTS
		txnKey := keys.TransactionKey(test.key, txn.ID)
		if err := engine.MVCCPutProto(tc.rng.rm.Engine(), nil, txnKey, proto.ZeroTimestamp,
			nil, &existTxn); err != nil {
			t.Fatal(err)
		}

		// End the transaction, verify expected error.
		txn.Key = test.key
		args, reply := endTxnArgs(txn, true, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp

		verifyErrorMatches(tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true), test.expErrRegexp, t)

	}
}

// TestInternalPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestInternalPushTxnBadKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", proto.Key("b"), 1, proto.SERIALIZABLE, tc.clock)

	args, reply := pushTxnArgs(pusher, pushee, proto.ABORT_TXN, 1, tc.store.StoreID())
	args.Key = pusher.Key

	verifyErrorMatches(tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true), ".*should match pushee.*", t)

}

// TestInternalPushTxnAlreadyCommittedOrAborted verifies success
// (noop) in event that pushee is already committed or aborted.
func TestInternalPushTxnAlreadyCommittedOrAborted(t *testing.T) {
	defer leaktest.AfterTest(t)
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

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: etArgs, Reply: etReply}, true); err != nil {

			t.Fatal(err)
		}

		// Now try to push what's already committed or aborted.
		args, reply := pushTxnArgs(pusher, pushee, proto.ABORT_TXN, 1, tc.store.StoreID())

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
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

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: hbArgs, Reply: hbReply}, true); err != nil {

			t.Fatal(err)
		}

		// Now, attempt to push the transaction using updated values for epoch & timestamp.
		pushee.Epoch = test.epoch
		pushee.Timestamp = test.ts
		args, reply := pushTxnArgs(pusher, pushee, proto.ABORT_TXN, 1, tc.store.StoreID())

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	ts := proto.Timestamp{WallTime: 1}
	ns := DefaultHeartbeatInterval.Nanoseconds()
	testCases := []struct {
		heartbeat   *proto.Timestamp // nil indicates no heartbeat
		currentTime int64            // nanoseconds
		pushType    proto.PushTxnType
		expSuccess  bool
	}{
		{nil, 1, proto.PUSH_TIMESTAMP, false}, // using 0 as time is awkward
		{nil, 1, proto.ABORT_TXN, false},
		{nil, 1, proto.CLEANUP_TXN, false},
		{nil, ns, proto.PUSH_TIMESTAMP, false},
		{nil, ns, proto.ABORT_TXN, false},
		{nil, ns, proto.CLEANUP_TXN, false},
		{nil, ns*2 - 1, proto.PUSH_TIMESTAMP, false},
		{nil, ns*2 - 1, proto.ABORT_TXN, false},
		{nil, ns*2 - 1, proto.CLEANUP_TXN, false},
		{nil, ns * 2, proto.PUSH_TIMESTAMP, false},
		{nil, ns * 2, proto.ABORT_TXN, false},
		{nil, ns * 2, proto.CLEANUP_TXN, false},
		{&ts, ns*2 + 1, proto.PUSH_TIMESTAMP, false},
		{&ts, ns*2 + 1, proto.ABORT_TXN, false},
		{&ts, ns*2 + 1, proto.CLEANUP_TXN, false},
		{&ts, ns*2 + 2, proto.PUSH_TIMESTAMP, true},
		{&ts, ns*2 + 2, proto.ABORT_TXN, true},
		{&ts, ns*2 + 2, proto.CLEANUP_TXN, true},
	}

	for i, test := range testCases {
		key := proto.Key(fmt.Sprintf("key-%d", i))
		pushee := newTransaction(fmt.Sprintf("test-%d", i), key, 1, proto.SERIALIZABLE, tc.clock)
		pusher := newTransaction("pusher", key, 1, proto.SERIALIZABLE, tc.clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via heartbeat.
		if test.heartbeat != nil {
			hbArgs, hbReply := heartbeatArgs(pushee, 1, tc.store.StoreID())
			hbArgs.Timestamp = *test.heartbeat

			if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: hbArgs, Reply: hbReply}, true); err != nil {

				t.Fatal(err)
			}
		}

		// Now, attempt to push the transaction with clock set to "currentTime".
		tc.manualClock.Set(test.currentTime)
		args, reply := pushTxnArgs(pusher, pushee, test.pushType, 1, tc.store.StoreID())
		// Avoid logical ticks here, they make the borderline cases hard to
		// test.
		args.Timestamp = proto.Timestamp{WallTime: test.currentTime}
		args.Now = args.Timestamp
		args.Timestamp.Logical = 0

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

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

// TestInternalPushTxnPriorities verifies that txns with lower
// priority are pushed; if priorities are equal, then the txns
// are ordered by txn timestamp, with the more recent timestamp
// being pushable.
func TestInternalPushTxnPriorities(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	ts1 := proto.Timestamp{WallTime: 1}
	ts2 := proto.Timestamp{WallTime: 2}
	testCases := []struct {
		pusherPriority, pusheePriority int32
		pusherTS, pusheeTS             proto.Timestamp
		pushType                       proto.PushTxnType
		expSuccess                     bool
	}{
		// Pusher has higher priority succeeds.
		{2, 1, ts1, ts1, proto.ABORT_TXN, true},
		// Pusher has lower priority fails.
		{1, 2, ts1, ts1, proto.ABORT_TXN, false},
		{1, 2, ts1, ts1, proto.PUSH_TIMESTAMP, false},
		// Pusher has lower priority fails, even with older txn timestamp.
		{1, 2, ts1, ts2, proto.ABORT_TXN, false},
		// Pusher has lower priority, but older txn timestamp allows success if !abort.
		{1, 2, ts1, ts2, proto.PUSH_TIMESTAMP, true},
		// With same priorities, older txn timestamp succeeds.
		{1, 1, ts1, ts2, proto.ABORT_TXN, true},
		// With same priorities, same txn timestamp fails.
		{1, 1, ts1, ts1, proto.ABORT_TXN, false},
		{1, 1, ts1, ts1, proto.PUSH_TIMESTAMP, false},
		// With same priorities, newer txn timestamp fails.
		{1, 1, ts2, ts1, proto.ABORT_TXN, false},
		{1, 1, ts2, ts1, proto.PUSH_TIMESTAMP, false},
		// When confirming, priority never wins.
		{2, 1, ts1, ts1, proto.CLEANUP_TXN, false},
		{1, 2, ts1, ts1, proto.CLEANUP_TXN, false},
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
		args, reply := pushTxnArgs(pusher, pushee, test.pushType, 1, tc.store.StoreID())

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

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
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", proto.Key("b"), 1, proto.SERIALIZABLE, tc.clock)
	pusher.Priority = 2
	pushee.Priority = 1 // pusher will win
	pusher.Timestamp = proto.Timestamp{WallTime: 50, Logical: 25}
	pushee.Timestamp = proto.Timestamp{WallTime: 5, Logical: 1}

	// Now, push the transaction with args.Abort=false.
	args, reply := pushTxnArgs(pusher, pushee, proto.PUSH_TIMESTAMP, 1, tc.store.StoreID())

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

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
	defer leaktest.AfterTest(t)
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
	args, reply := pushTxnArgs(pusher, pushee, proto.PUSH_TIMESTAMP, 1, tc.store.StoreID())

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true); err != nil {

		t.Errorf("unexpected error on push: %s", err)
	}
	if !reply.PusheeTxn.Timestamp.Equal(pushee.Timestamp) {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.Timestamp, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != proto.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}

func verifyRangeStats(eng engine.Engine, raftID proto.RaftID, expMS proto.MVCCStats, t *testing.T) {
	var ms proto.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, raftID, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expMS, ms) {
		t.Errorf("expected stats %+v; got %+v", expMS, ms)
	}
}

// TestRangeStatsComputation verifies that commands executed against a
// range update the range stat counters. The stat values are
// empirically derived; we're really just testing that they increment
// in the right ways, not the exact amounts. If the encodings change,
// will need to update this test.
func TestRangeStatsComputation(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	// Put a value.
	pArgs, pReply := putArgs([]byte("a"), []byte("value1"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

		t.Fatal(err)
	}
	expMS := proto.MVCCStats{LiveBytes: 39, KeyBytes: 15, ValBytes: 24, IntentBytes: 0, LiveCount: 1, KeyCount: 1, ValCount: 1, IntentCount: 0, SysBytes: 58, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RaftID, expMS, t)

	// Put a 2nd value transactionally.
	pArgs, pReply = putArgs([]byte("b"), []byte("value2"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()
	pArgs.Txn = &proto.Transaction{ID: util.NewUUID4(), Timestamp: pArgs.Timestamp}

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

		t.Fatal(err)
	}
	expMS = proto.MVCCStats{LiveBytes: 128, KeyBytes: 30, ValBytes: 98, IntentBytes: 24, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 1, SysBytes: 58, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RaftID, expMS, t)

	// Resolve the 2nd value.
	rArgs := &proto.InternalResolveIntentRequest{
		RequestHeader: proto.RequestHeader{
			Timestamp: pArgs.Txn.Timestamp,
			Key:       pArgs.Key,
			RaftID:    tc.rng.Desc().RaftID,
			Replica:   proto.Replica{StoreID: tc.store.StoreID()},
			Txn:       pArgs.Txn,
		},
	}
	rArgs.Txn.Status = proto.COMMITTED
	rReply := &proto.InternalResolveIntentResponse{}

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: rArgs, Reply: rReply}, true); err != nil {

		t.Fatal(err)
	}
	expMS = proto.MVCCStats{LiveBytes: 78, KeyBytes: 30, ValBytes: 48, IntentBytes: 0, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 0, SysBytes: 58, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RaftID, expMS, t)

	// Delete the 1st value.
	dArgs, dReply := deleteArgs([]byte("a"), 1, tc.store.StoreID())
	dArgs.Timestamp = tc.clock.Now()

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: dArgs, Reply: dReply}, true); err != nil {

		t.Fatal(err)
	}
	expMS = proto.MVCCStats{LiveBytes: 39, KeyBytes: 42, ValBytes: 50, IntentBytes: 0, LiveCount: 1, KeyCount: 2, ValCount: 3, IntentCount: 0, SysBytes: 58, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RaftID, expMS, t)
}

// TestInternalMerge verifies that the InternalMerge command is behaving as
// expected. Merge semantics for different data types are tested more robustly
// at the engine level; this test is intended only to show that values passed to
// InternalMerge are being merged.
func TestInternalMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("mergedkey")
	stringArgs := []string{"a", "b", "c", "d"}
	stringExpected := "abcd"

	for _, str := range stringArgs {
		mergeArgs, resp := internalMergeArgs(key, proto.Value{Bytes: []byte(str)}, 1,
			tc.store.StoreID())

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: mergeArgs, Reply: resp}, true); err != nil {

			t.Fatalf("unexpected error from InternalMerge: %s", err.Error())
		}
	}

	getArgs, resp := getArgs(key, 1, tc.store.StoreID())

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: getArgs, Reply: resp}, true); err != nil {

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
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args, resp := incrementArgs([]byte("a"), int64(i), 1, tc.store.StoreID())

		if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: resp}, true); err != nil {

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

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: truncateArgs, Reply: truncateResp}, true); err != nil {

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
	entries, err := tc.rng.Entries(indexes[5], indexes[9], math.MaxUint64)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != int(indexes[9]-indexes[5]) {
		t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
	}

	// But any range that includes the truncated entries returns an error.
	_, err = tc.rng.Entries(indexes[4], indexes[9], math.MaxUint64)
	if err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}

	// The term of the last truncated entry is still available.
	term, err := tc.rng.Term(indexes[4])
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 {
		t.Errorf("invalid term 0 for truncated entry")
	}

	// The terms of older entries are gone.
	_, err = tc.rng.Term(indexes[3])
	if err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
}

func TestRaftStorage(t *testing.T) {
	defer leaktest.AfterTest(t)
	var eng engine.Engine
	storagetest.RunTests(t,
		func(t *testing.T) storagetest.WriteableStorage {
			eng = engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
			// Fake store to house the engine.
			store := &Store{
				ctx: StoreContext{
					Clock: hlc.NewClock(hlc.UnixNano),
				},
				engine: eng,
			}
			rng, err := NewRange(&proto.RangeDescriptor{
				RaftID:   1,
				StartKey: proto.KeyMin,
				EndKey:   proto.KeyMax,
			}, store)
			if err != nil {
				t.Fatal(err)
			}
			return rng
		},
		func(t *testing.T, r storagetest.WriteableStorage) {
			eng.Close()
		})
}

// TestConditionFailedError tests that a ConditionFailedError correctly
// bubbles up from MVCC to Range.
func TestConditionFailedError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("k")
	value := []byte("quack")
	pArgs, pReply := putArgs(key, value, 1, tc.store.StoreID())

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

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

	err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

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
	defer leaktest.AfterTest(t)
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
		if replicaSetsEqual(test.a, test.b) != test.expected {
			t.Fatalf("unexpected replica intersection: %+v", test)
		}
	}
}

func TestAppliedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	var appliedIndex uint64
	var sum int64
	for i := int64(1); i <= 10; i++ {
		args, reply := incrementArgs([]byte("a"), i, 1, tc.store.StoreID())

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

		if err != nil {
			t.Fatal(err)
		}
		sum += i

		if reply.NewValue != sum {
			t.Errorf("expected %d, got %d", sum, reply.NewValue)
		}

		newAppliedIndex := atomic.LoadUint64(&tc.rng.appliedIndex)
		if newAppliedIndex <= appliedIndex {
			t.Errorf("appliedIndex did not advance. Was %d, now %d", appliedIndex, newAppliedIndex)
		}
		appliedIndex = newAppliedIndex
	}
}

// TestChangeReplicasDuplicateError tests that a replica change that would
// use a NodeID twice in the replica configuration fails.
func TestChangeReplicasDuplicateError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	if err := tc.rng.ChangeReplicas(proto.ADD_REPLICA, proto.Replica{
		NodeID:  tc.store.Ident.NodeID,
		StoreID: 9999,
	}); err == nil || !strings.Contains(err.Error(),
		"already present") {
		t.Fatalf("must not be able to add second replica to same node (err=%s)",
			err)
	}
}

// TestRangeDanglingMetaIntent creates a dangling intent on a
// meta2 record and verifies that InternalRangeLookup requests
// behave appropriately. Normally, the old value and a write intent
// error should be returned. If IgnoreIntents is specified, then
// a random choice of old or new is returned with no error.
func TestRangeDanglingMetaIntent(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := proto.Key("a")

	// Get original meta2 descriptor.
	rlArgs := &proto.InternalRangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:     keys.RangeMetaKey(key),
			RaftID:  tc.rng.Desc().RaftID,
			Replica: proto.Replica{StoreID: tc.store.StoreID()},
		},
		MaxRanges: 1,
	}
	rlReply := &proto.InternalRangeLookupResponse{}

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: rlArgs, Reply: rlReply}, true); err != nil {

		t.Fatal(err)
	}

	origDesc := rlReply.Ranges[0]
	newDesc := origDesc
	newDesc.EndKey = key

	// Write the new descriptor as an intent.
	data, err := gogoproto.Marshal(&newDesc)
	if err != nil {
		t.Fatal(err)
	}
	pArgs, pReply := putArgs(keys.RangeMetaKey(key), data, 1, tc.store.StoreID())
	pArgs.Txn = newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
	pArgs.Timestamp = pArgs.Txn.Timestamp

	if err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: pArgs, Reply: pReply}, true); err != nil {

		t.Fatal(err)
	}

	// Now lookup the range; should get old value + write intent error.
	rlArgs.Key = keys.RangeMetaKey(proto.Key("A"))
	rlArgs.Timestamp = proto.ZeroTimestamp

	err = tc.rng.AddCmd(tc.rng.context(), client.Call{Args: rlArgs, Reply: rlReply}, true)

	if wiErr, ok := err.(*proto.WriteIntentError); !ok || len(wiErr.Intents) != 1 {
		t.Errorf("expected write intent error with 1 key; got %s", err)
	}
	if !reflect.DeepEqual(rlReply.Ranges[0], origDesc) {
		t.Errorf("expected original descriptor %s; got %s", &origDesc, &rlReply.Ranges[0])
	}

	// Try 100 lookups with IgnoreIntents. Expect roughly 50/50.
	rlArgs.IgnoreIntents = true
	var origCount, newCount int
	for i := 0; i < 100; i++ {
		rlArgs.Timestamp = proto.ZeroTimestamp
		rlReply.Reset()

		if err = tc.rng.AddCmd(tc.rng.context(), client.Call{Args: rlArgs, Reply: rlReply}, true); err != nil {

			t.Fatal(err)
		}
		if reflect.DeepEqual(rlReply.Ranges[0], origDesc) {
			origCount++
		} else if reflect.DeepEqual(rlReply.Ranges[0], newDesc) {
			newCount++
		} else {
			t.Errorf("expected orig/new descriptor %s/%s; got %s", &origDesc, &newDesc, &rlReply.Ranges[0])
		}
	}
	if origCount > newCount+20 || origCount < newCount-20 {
		t.Errorf("expected close to 50/50 counts; got %d/%d", origCount, newCount)
	}
}

func TestInternalRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	for _, key := range []proto.Key{
		// Test with the first range (StartKey==KeyMin). Normally we look up this
		// range in gossip instead of executing the RPC, but InternalRangeLookup
		// is still used when up-to-date information is required.
		proto.KeyMin,
		// Test with the last key in a meta prefix. This is an edge case in the
		// implementation.
		proto.MakeKey(keys.Meta1Prefix, proto.KeyMax),
	} {
		reply := proto.InternalRangeLookupResponse{}
		if err := tc.store.ExecuteCmd(context.Background(), client.Call{
			Args: &proto.InternalRangeLookupRequest{
				RequestHeader: proto.RequestHeader{
					RaftID: 1,
					Key:    key,
				},
				MaxRanges: 1,
			},
			Reply: &reply,
		}); err != nil {
			t.Fatal(err)
		}
		expected := []proto.RangeDescriptor{*tc.rng.Desc()}
		if !reflect.DeepEqual(reply.Ranges, expected) {
			t.Fatalf("expected %+v, got %+v", expected, reply.Ranges)
		}
	}
}

// benchmarkEvents is designed to determine the impact of sending events on the
// performance of write commands. This benchmark can be run with or without
// events, and with or without a consumer reading the events.
func benchmarkEvents(b *testing.B, sendEvents, consumeEvents bool) {
	defer leaktest.AfterTest(b)
	tc := testContext{}

	if sendEvents {
		tc.feed = &util.Feed{}
	}
	eventC := 0
	consumeStopper := util.NewStopper()
	if consumeEvents {
		sub := tc.feed.Subscribe()
		consumeStopper.RunWorker(func() {
			for range sub.Events() {
				eventC++
			}
		})
	}

	tc.Start(b)
	defer tc.Stop()

	args, reply := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		err := tc.rng.AddCmd(tc.rng.context(), client.Call{Args: args, Reply: reply}, true)

		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if consumeEvents {
		// Close feed and wait for consumer to finish.
		tc.feed.Close()
		consumeStopper.Stop()
		<-consumeStopper.IsStopped()

		// The test expects exactly b.N update events to be sent on the feed.
		// The '+5' is a constant number of non-update events sent when the
		// store is first started.
		if a, e := eventC, b.N+5; a != e {
			b.Errorf("Unexpected number of events received %d, expected %d", a, e)
		}
	}
}

// BenchmarkWriteCmdNoEvents benchmarks write commands on a store that does not
// publish events.
func BenchmarkWriteCmdNoEvents(b *testing.B) {
	benchmarkEvents(b, false, false)
}

// BenchmarkWriteCmdNoEvents benchmarks write commands on a store that publishes
// events. There are no subscribers to the events, but they are still produced.
func BenchmarkWriteCmdWithEvents(b *testing.B) {
	benchmarkEvents(b, true, false)
}

// BenchmarkWriteConsumeEvents benchmarks write commands on a store that publishes
// events. There is a single subscriber reading the events.
func BenchmarkWriteCmdWithEventsAndConsumer(b *testing.B) {
	benchmarkEvents(b, true, true)
}
