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
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/multiraft/storagetest"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/coreos/etcd/raft"
	gogoproto "github.com/gogo/protobuf/proto"
)

func testRangeDescriptor() *proto.RangeDescriptor {
	return &proto.RangeDescriptor{
		RangeID:  1,
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
	rng           *Replica
	rangeID       proto.RangeID
	gossip        *gossip.Gossip
	engine        engine.Engine
	manualClock   *hlc.ManualClock
	clock         *hlc.Clock
	stopper       *stop.Stopper
	bootstrapMode bootstrapMode
	feed          *util.Feed
}

// testContext.Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(t testing.TB) {
	if tc.stopper == nil {
		tc.stopper = stop.NewStopper()
	}
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(tc.stopper)
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(&base.Context{}, hlc.NewClock(hlc.UnixNano), tc.stopper)
		tc.gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	}
	if tc.manualClock == nil {
		tc.manualClock = hlc.NewManualClock(0)
	}
	if tc.clock == nil {
		tc.clock = hlc.NewClock(tc.manualClock.UnixNano)
	}
	if tc.engine == nil {
		tc.engine = engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20, tc.stopper)
	}
	if tc.transport == nil {
		tc.transport = multiraft.NewLocalRPCTransport(tc.stopper)
	}
	tc.stopper.AddCloser(tc.transport)

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
		ctx.DB = client.NewDB(sender)
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
		tc.store.splitQueue().SetDisabled(true)

		if tc.rng == nil && tc.bootstrapMode == bootstrapRangeWithMetadata {
			if err := tc.store.BootstrapRange(nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := tc.store.Start(tc.stopper); err != nil {
			t.Fatal(err)
		}
		tc.store.WaitForInit()
	}

	realRange := tc.rng == nil

	if realRange {
		if tc.bootstrapMode == bootstrapRangeOnly {
			rng, err := NewReplica(testRangeDescriptor(), tc.store)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AddReplicaTest(rng); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		tc.rng, err = tc.store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		tc.rangeID = tc.rng.Desc().RangeID
	}

	if err := tc.initConfigs(realRange); err != nil {
		t.Fatal(err)
	}
}

func (tc *testContext) Stop() {
	tc.stopper.Stop()
}

// initConfigs creates default configuration entries.
func (tc *testContext) initConfigs(realRange bool) error {
	// Put an empty system config into gossip so that gossip callbacks get
	// run. We're using a fake config, but it's hooked into SystemConfig.
	if err := tc.gossip.AddInfoProto(gossip.KeySystemConfig,
		&config.SystemConfig{}, 0); err != nil {
		return err
	}

	// Wait for the unmarshalling callback to run.
	if err := util.IsTrueWithin(func() bool {
		return tc.gossip.GetSystemConfig() != nil
	}, 100*time.Millisecond); err != nil {
		return err
	}

	return nil
}

func newTransaction(name string, baseKey proto.Key, userPriority int32,
	isolation proto.IsolationType, clock *hlc.Clock) *proto.Transaction {
	return proto.NewTransaction(name, keys.KeyAddress(baseKey), userPriority,
		isolation, clock.Now(), clock.MaxOffset().Nanoseconds())
}

// CreateReplicaSets creates new proto.Replica protos based on an array of
// StoreIDs to aid in testing. Note that this does not actually produce any
// replicas, it just creates the proto.
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
		RangeID:  1,
		StartKey: proto.Key("a"),
		EndKey:   proto.Key("b"),
	}

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20, stopper)
	clock := hlc.NewClock(hlc.UnixNano)
	ctx := TestStoreContext
	ctx.Clock = clock
	ctx.Transport = multiraft.NewLocalRPCTransport(stopper)
	defer ctx.Transport.Close()
	store := NewStore(ctx, e, &proto.NodeDescriptor{NodeID: 1})
	r, err := NewReplica(desc, store)
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

func setLeaderLease(t *testing.T, r *Replica, l *proto.Lease) {
	args := &proto.BatchRequest{}
	args.Add(&proto.LeaderLeaseRequest{Lease: *l})
	errChan, pendingCmd := r.proposeRaftCommand(r.context(), args)
	var err error
	if err = <-errChan; err == nil {
		// Next if the command was committed, wait for the range to apply it.
		err = (<-pendingCmd.done).Err
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

	// Modify range descriptor to include a second replica; leader lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := proto.Replica{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	gArgs := getArgs(proto.Key("a"), 1, tc.store.StoreID())
	gArgs.Timestamp = tc.clock.Now()

	// Try consistent read and verify success.

	if _, err := tc.rng.AddCmd(tc.rng.context(), &gArgs); err != nil {
		t.Errorf("expected success on consistent read: %s", err)
	}

	// Try a consensus read and verify error.
	gArgs.ReadConsistency = proto.CONSENSUS

	if _, err := tc.rng.AddCmd(tc.rng.context(), &gArgs); err == nil {
		t.Errorf("expected error on consensus read")
	}

	// Try an inconsistent read within a transaction.
	gArgs.ReadConsistency = proto.INCONSISTENT
	gArgs.Txn = newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)

	if _, err := tc.rng.AddCmd(tc.rng.context(), &gArgs); err == nil {
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

	_, err := tc.rng.AddCmd(tc.rng.context(), &gArgs)
	if _, ok := err.(*proto.NotLeaderError); !ok {
		t.Errorf("expected not leader error; got %s", err)
	}

	gArgs.ReadConsistency = proto.INCONSISTENT

	if _, err := tc.rng.AddCmd(tc.rng.context(), &gArgs); err != nil {
		t.Errorf("expected success reading with inconsistent: %s", err)
	}
}

// TestApplyCmdLeaseError verifies that when during application of a Raft
// command the proposing node no longer holds the leader lease, an error is
// returned. This prevents regression of #1483.
func TestApplyCmdLeaseError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Modify range descriptor to include a second replica; leader lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := proto.Replica{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	ba := proto.BatchRequest{}
	ba.Timestamp = tc.clock.Now()
	pArgs := putArgs(proto.Key("a"), []byte("asd"),
		tc.rng.Desc().RangeID, tc.store.StoreID())
	pArgs.Timestamp = ba.Timestamp
	ba.Add(&pArgs)

	// Lose the lease.
	start := tc.rng.getLease().Expiration.Add(1, 0)
	tc.manualClock.Set(start.WallTime)
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      start,
		Expiration: start.Add(10, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2), // a different node
	})

	// Submit a proposal to Raft.
	errChan, pendingCmd := tc.rng.proposeRaftCommand(tc.rng.context(), &ba)
	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
	if err := (<-pendingCmd.done).Err; err == nil {
		t.Fatalf("expected an error")
	} else if _, ok := err.(*proto.NotLeaderError); !ok {
		t.Fatalf("expected not leader error in return, got %s", err)
	}
}

func TestRangeRangeBoundsChecking(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	splitTestRange(tc.store, proto.Key("a"), proto.Key("a"), t)
	gArgs := getArgs(proto.Key("b"), 1, tc.store.StoreID())

	_, err := tc.rng.AddCmd(tc.rng.context(), &gArgs)

	if _, ok := err.(*proto.RangeKeyMismatchError); !ok {
		t.Errorf("expected range key mismatch error: %s", err)
	}
}

// hasLease returns whether the most recent leader lease was held by the given
// range replica and whether it's expired for the given timestamp.
func hasLease(rng *Replica, timestamp proto.Timestamp) (bool, bool) {
	l := rng.getLease()
	return l.OwnedBy(rng.rm.RaftNodeID()), !l.Covers(timestamp)
}

func TestRangeLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.clock.SetMaxOffset(maxClockOffset)

	// Modify range descriptor to include a second replica; leader lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := proto.Replica{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	if held, _ := hasLease(tc.rng, tc.clock.Now()); !held {
		t.Errorf("expected lease on range start")
	}
	tc.manualClock.Set(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      now.Add(10, 0),
		Expiration: now.Add(20, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2),
	})
	if held, expired := hasLease(tc.rng, tc.clock.Now().Add(15, 0)); held || expired {
		t.Errorf("expected another replica to have leader lease")
	}

	err := tc.rng.redirectOnOrAcquireLeaderLease(nil, tc.clock.Now())
	if lErr, ok := err.(*proto.NotLeaderError); !ok || lErr == nil {
		t.Fatalf("wanted NotLeaderError, got %s", err)
	}

	// Advance clock past expiration and verify that another has
	// leader lease will not be true.
	tc.manualClock.Increment(21) // 21ns pass
	if held, expired := hasLease(tc.rng, tc.clock.Now()); held || !expired {
		t.Errorf("expected another replica to have expired lease")
	}
}

func TestRangeNotLeaderError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Modify range descriptor to include a second replica; leader lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := proto.Replica{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()
	setLeaderLease(t, tc.rng, &proto.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2),
	})

	header := proto.RequestHeader{
		Key:       proto.Key("a"),
		RangeID:   tc.rng.Desc().RangeID,
		Replica:   proto.Replica{StoreID: tc.store.StoreID()},
		Timestamp: now,
	}
	testCases := []proto.Request{
		// Admin split covers admin commands.
		&proto.AdminSplitRequest{
			RequestHeader: header,
			SplitKey:      proto.Key("a"),
		},
		// Get covers read-only commands.
		&proto.GetRequest{
			RequestHeader: header,
		},
		// Put covers read-write commands.
		&proto.PutRequest{
			RequestHeader: header,
			Value: proto.Value{
				Bytes: []byte("value"),
			},
		},
	}

	for i, test := range testCases {
		_, err := tc.rng.AddCmd(tc.rng.context(), test)

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

	// Modify range descriptor to include a second replica; leader lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := proto.Replica{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)
	var val proto.Value
	val.SetInt(42)
	if err := engine.MVCCPut(tc.engine, nil, key, proto.MinTimestamp, val, nil); err != nil {
		t.Fatal(err)
	}

	verifySystem := func() bool {
		return util.IsTrueWithin(func() bool {
			cfg := tc.gossip.GetSystemConfig()
			if cfg == nil {
				return false
			}
			numValues := len(cfg.Values)
			return numValues == 1 && cfg.Values[numValues-1].Key.Equal(key)
		}, 100*time.Millisecond) == nil
	}

	// If this actually failed, we would have gossiped from MVCCPutProto.
	// Unlikely, but why not check.
	if verifySystem() {
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
	if !verifySystem() {
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

	// Modify range descriptor to include a second replica; leader lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := proto.Replica{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

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

// TestRangeLeaderLeaseRejectUnknownRaftNodeID ensures that a replica cannot
// obtain the leader lease if it is not part of the current range descriptor.
// TODO(mrtracy): This should probably be tested in client_raft_test package,
// using a real second store.
func TestRangeLeaderLeaseRejectUnknownRaftNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()
	lease := &proto.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		RaftNodeID: proto.MakeRaftNodeID(2, 2),
	}
	args := &proto.BatchRequest{}
	args.Add(&proto.LeaderLeaseRequest{Lease: *lease})
	errChan, pendingCmd := tc.rng.proposeRaftCommand(tc.rng.context(), args)
	var err error
	if err = <-errChan; err == nil {
		// Next if the command was committed, wait for the range to apply it.
		err = (<-pendingCmd.done).Err
	}
	if err == nil {
		t.Error("error: successfully obtained lease for a store that was not in the RangeDescriptor", err)
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
		bytes, err := tc.gossip.GetInfo(key)
		if err != nil {
			t.Errorf("missing first range gossip of key %s", key)
		}
		if key == gossip.KeyFirstRangeDescriptor {
			var rangeDesc proto.RangeDescriptor
			if err := gogoproto.Unmarshal(bytes, &rangeDesc); err != nil {
				t.Fatal(err)
			}
		}
		if key == gossip.KeyClusterID && len(bytes) == 0 {
			t.Errorf("expected non-empty gossiped cluster ID, got %q", bytes)
		}
		if key == gossip.KeySentinel && len(bytes) == 0 {
			t.Errorf("expected non-empty gossiped sentinel, got %q", bytes)
		}
	}
}

// TestRangeGossipAllConfigs verifies that all config types are gossiped.
func TestRangeGossipAllConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	cfg := tc.gossip.GetSystemConfig()
	if cfg == nil {
		t.Fatal("nil config")
	}
}

// TestRangeNoGossipConfig verifies that certain commands (e.g.,
// reads, writes in uncommitted transactions) do not trigger gossip.
func TestRangeNoGossipConfig(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)
	rangeID := proto.RangeID(1)

	txn := newTransaction("test", key, 1 /* userPriority */, proto.SERIALIZABLE, tc.clock)
	req1 := putArgs(key, []byte("foo"), rangeID, tc.store.StoreID())
	req1.Txn = txn
	req1.Timestamp = txn.Timestamp

	req2 := endTxnArgs(txn, true /* commit */, rangeID, tc.store.StoreID())
	req2.Timestamp = txn.Timestamp
	req2.Intents = []proto.Intent{{Key: key}}

	req3 := getArgs(key, rangeID, tc.store.StoreID())
	req3.Timestamp = txn.Timestamp

	for i, req := range []proto.Request{&req1, &req2, &req3} {
		if _, err := tc.store.ExecuteCmd(tc.rng.context(), req); err != nil {
			t.Fatal(err)
		}

		// System config is not gossiped.
		cfg := tc.gossip.GetSystemConfig()
		if cfg == nil {
			t.Fatal("nil config")
		}
		if len(cfg.Values) != 0 {
			t.Errorf("System config was gossiped at #%d", i)
		}
	}
}

// TestRangeNoGossipFromNonLeader verifies that a non-leader replica
// does not gossip configurations.
func TestRangeNoGossipFromNonLeader(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)
	rangeID := proto.RangeID(1)

	txn := newTransaction("test", key, 1 /* userPriority */, proto.SERIALIZABLE, tc.clock)
	req1 := putArgs(key, nil, rangeID, tc.store.StoreID())
	req1.Txn = txn
	req1.Timestamp = txn.Timestamp
	if _, err := tc.store.ExecuteCmd(tc.rng.context(), &req1); err != nil {
		t.Fatal(err)
	}
	req2 := endTxnArgs(txn, true /* commit */, rangeID, tc.store.StoreID())
	req2.Timestamp = txn.Timestamp
	req2.Intents = []proto.Intent{{Key: key}}
	if _, err := tc.store.ExecuteCmd(tc.rng.context(), &req2); err != nil {
		t.Fatal(err)
	}
	// Execute a get to resolve the intent.
	req3 := getArgs(key, rangeID, tc.store.StoreID())
	req3.Timestamp = txn.Timestamp
	if _, err := tc.store.ExecuteCmd(tc.rng.context(), &req3); err != nil {
		t.Fatal(err)
	}

	// Increment the clock's timestamp to expire the leader lease.
	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration) + 1)
	if lease := tc.rng.getLease(); lease.Covers(tc.clock.Now()) {
		t.Fatal("leader lease should have been expired")
	}

	// Make sure the information for db1 is not gossiped.
	tc.rng.maybeGossipSystemConfig()
	// Fetch the raw gossip info. GetSystemConfig is based on callbacks at
	// modification time. But we're checking for _not_ gossiped, so there should
	// be no callbacks. Easier to check the raw info.
	var cfg config.SystemConfig
	err := tc.gossip.GetInfoProto(gossip.KeySystemConfig, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Values) != 0 {
		t.Fatalf("non-lease holder gossiped the system config")
	}
}

// getArgs returns a GetRequest and GetResponse pair addressed to
// the default replica for the specified key.
func getArgs(key []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.GetRequest {
	return proto.GetRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key, value []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.PutRequest {
	return proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			Timestamp: proto.MinTimestamp,
			RangeID:   rangeID,
			Replica:   proto.Replica{StoreID: storeID},
		},
		Value: proto.Value{
			Bytes: value,
		},
	}
}

// deleteArgs returns a DeleteRequest and DeleteResponse pair.
func deleteArgs(key proto.Key, rangeID proto.RangeID, storeID proto.StoreID) proto.DeleteRequest {
	return proto.DeleteRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
}

// readOrWriteArgs returns either get or put arguments depending on
// value of "read". Get for true; Put for false. Returns method
// selected and args & reply.
func readOrWriteArgs(key proto.Key, read bool, rangeID proto.RangeID, storeID proto.StoreID) proto.Request {
	if read {
		gArgs := getArgs(key, rangeID, storeID)
		return &gArgs
	}
	pArgs := putArgs(key, []byte("value"), rangeID, storeID)
	return &pArgs
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64, rangeID proto.RangeID, storeID proto.StoreID) proto.IncrementRequest {
	return proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Increment: inc,
	}
}

func scanArgs(start, end []byte, rangeID proto.RangeID, storeID proto.StoreID) proto.ScanRequest {
	return proto.ScanRequest{
		RequestHeader: proto.RequestHeader{
			Key:     start,
			EndKey:  end,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
	}
}

// endTxnArgs returns request/response pair for EndTransaction RPC
// addressed to the default replica for the specified key.
func endTxnArgs(txn *proto.Transaction, commit bool, rangeID proto.RangeID, storeID proto.StoreID) proto.EndTransactionRequest {
	return proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:     txn.Key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
			Txn:     txn,
		},
		Commit: commit,
	}
}

// pushTxnArgs returns a request/response pair for PushTxn RPC
// addressed to the default replica for the specified key.
func pushTxnArgs(pusher, pushee *proto.Transaction, pushType proto.PushTxnType, rangeID proto.RangeID, storeID proto.StoreID) proto.PushTxnRequest {
	return proto.PushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:       pushee.Key,
			Timestamp: pusher.Timestamp,
			RangeID:   rangeID,
			Replica:   proto.Replica{StoreID: storeID},
		},
		Now:       pusher.Timestamp,
		PusherTxn: pusher,
		PusheeTxn: *pushee,
		PushType:  pushType,
	}
}

// heartbeatArgs returns request/response pair for HeartbeatTxn RPC.
func heartbeatArgs(txn *proto.Transaction, rangeID proto.RangeID, storeID proto.StoreID) proto.HeartbeatTxnRequest {
	return proto.HeartbeatTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:     txn.Key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
			Txn:     txn,
		},
	}
}

// internalMergeArgs returns a MergeRequest and MergeResponse
// pair addressed to the default replica for the specified key. The request will
// contain the given proto.Value.
func internalMergeArgs(key []byte, value proto.Value, rangeID proto.RangeID, storeID proto.StoreID) proto.MergeRequest {
	return proto.MergeRequest{
		RequestHeader: proto.RequestHeader{
			Key:     key,
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Value: value,
	}
}

func truncateLogArgs(index uint64, rangeID proto.RangeID, storeID proto.StoreID) proto.TruncateLogRequest {
	return proto.TruncateLogRequest{
		RequestHeader: proto.RequestHeader{
			RangeID: rangeID,
			Replica: proto.Replica{StoreID: storeID},
		},
		Index: index,
	}
}

// getSerializedMVCCValue produces a byte slice of the serialized
// mvcc value. If value is nil, MVCCValue.Deleted is set to true;
// otherwise MVCCValue.Value is set to value.
func getSerializedMVCCValue(value *proto.Value) []byte {
	mvccVal := &engine.MVCCValue{}
	if value != nil {
		mvccVal.Value = value
	} else {
		mvccVal.Deleted = true
	}
	data, err := gogoproto.Marshal(&engine.MVCCValue{Value: value})
	if err != nil {
		panic("unexpected marshal error")
	}
	return data
}

// TestAcquireLeaderLease verifies that the leader lease is acquired
// for read and write methods.
func TestAcquireLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)

	gArgs := getArgs([]byte("a"), 1, 0)
	pArgs := putArgs([]byte("b"), []byte("1"), 1, 0)

	testCases := []proto.Request{&gArgs, &pArgs}

	for i, test := range testCases {
		tc := testContext{}
		tc.Start(t)
		// This is a single-replica test; since we're automatically pushing back
		// the start of a lease as far as possible, and since there is an auto-
		// matic lease for us at the beginning, we'll basically create a lease from
		// then on.
		expStart := tc.rng.getLease().Expiration
		tc.manualClock.Set(int64(DefaultLeaderLeaseDuration + 1000))

		test.Header().Timestamp = tc.clock.Now()

		if _, err := tc.rng.AddCmd(tc.rng.context(), test); err != nil {
			t.Fatal(err)
		}
		if held, expired := hasLease(tc.rng, test.Header().Timestamp); !held || expired {
			t.Fatalf("%d: expected lease acquisition", i)
		}
		lease := tc.rng.getLease()
		// The lease may start earlier than our request timestamp, but the
		// expiration will still be measured relative to it.
		expExpiration := test.Header().Timestamp.Add(int64(DefaultLeaderLeaseDuration), 0)
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
	gArgs := getArgs([]byte("a"), 1, tc.store.StoreID())
	gArgs.Timestamp = tc.clock.Now()

	_, err := tc.rng.AddCmd(tc.rng.context(), &gArgs)

	if err != nil {
		t.Error(err)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	tc.manualClock.Set(t1.Nanoseconds())
	pArgs := putArgs([]byte("b"), []byte("1"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()

	_, err = tc.rng.AddCmd(tc.rng.context(), &pArgs)

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
	defer func() { TestingCommandFilter = nil }()

	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Intercept commands with matching command IDs and block them.
	blockingStart := make(chan struct{})
	blockingDone := make(chan struct{})
	defer close(blockingDone) // make sure teardown can happen
	TestingCommandFilter = func(args proto.Request) error {
		if args.Header().GetUserPriority() == 42 {
			blockingStart <- struct{}{}
			<-blockingDone
		}
		return nil
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

	tooLong := 5 * time.Second

	for i, test := range testCases {
		key1 := proto.Key(fmt.Sprintf("key1-%d", i))
		key2 := proto.Key(fmt.Sprintf("key2-%d", i))
		// Asynchronously put a value to the rng with blocking enabled.
		cmd1Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key1, test.cmd1Read, tc.rng.Desc().RangeID, tc.store.StoreID())
			args.Header().UserPriority = gogoproto.Int32(42)

			_, err := tc.rng.AddCmd(tc.rng.context(), args)

			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd1Done)
		})
		// Wait for cmd1 to get into the command queue.
		<-blockingStart

		// First, try a command for same key as cmd1 to verify it blocks.
		cmd2Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key1, test.cmd2Read, tc.rng.Desc().RangeID, tc.store.StoreID())

			_, err := tc.rng.AddCmd(tc.rng.context(), args)

			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd2Done)
		})

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key2, true, tc.rng.Desc().RangeID, tc.store.StoreID())

			_, err := tc.rng.AddCmd(tc.rng.context(), args)

			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd3Done)
		})

		if test.expWait {
			// Verify cmd3 finishes but not cmd2.
			select {
			case <-cmd2Done:
				t.Fatalf("test %d: should not have been able to execute cmd2", i)
			case <-cmd3Done:
				// success.
			case <-cmd1Done:
				t.Fatalf("test %d: should not have been able execute cmd1 while blocked", i)
			case <-time.After(tooLong):
				t.Fatalf("test %d: waited %s for cmd3 of key2", i, tooLong)
			}
		} else {
			select {
			case <-cmd2Done:
				// success.
			case <-cmd1Done:
				t.Fatalf("test %d: should not have been able to execute cmd1 while blocked", i)
			case <-time.After(tooLong):
				t.Fatalf("test %d: waited %s for cmd2 of key1", i, tooLong)
			}
			<-cmd3Done
		}

		blockingDone <- struct{}{}
		select {
		case <-cmd2Done:
			// success.
		case <-time.After(tooLong):
			t.Fatalf("test %d: waited %s for cmd2 of key1", i, tooLong)
		}
	}
}

// TestRangeCommandQueueInconsistent verifies that inconsistent reads need
// not wait for pending commands to complete through Raft.
func TestRangeCommandQueueInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer func() { TestingCommandFilter = nil }()

	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := proto.Key("key1")
	blockingStart := make(chan struct{})
	blockingDone := make(chan struct{})
	TestingCommandFilter = func(args proto.Request) error {
		if args.Header().CmdID.Random == 1 {
			blockingStart <- struct{}{}
			<-blockingDone
		}
		return nil
	}
	cmd1Done := make(chan struct{})
	go func() {
		args := putArgs(key, []byte("value"), tc.rng.Desc().RangeID, tc.store.StoreID())
		args.CmdID.Random = 1

		_, err := tc.rng.AddCmd(tc.rng.context(), &args)

		if err != nil {
			t.Fatal(err)
		}
		close(cmd1Done)
	}()
	// Wait for cmd1 to get into the command queue.
	<-blockingStart

	// An inconsistent read to the key won't wait.
	cmd2Done := make(chan struct{})
	go func() {
		args := getArgs(key, tc.rng.Desc().RangeID, tc.store.StoreID())
		args.ReadConsistency = proto.INCONSISTENT

		_, err := tc.rng.AddCmd(tc.rng.context(), &args)

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

	blockingDone <- struct{}{}
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
	args := getArgs([]byte("a"), 1, tc.store.StoreID())
	args.Timestamp = tc.clock.Now()

	_, err := tc.rng.AddCmd(tc.rng.context(), &args)

	if err != nil {
		t.Error(err)
	}
	pArgs := putArgs([]byte("a"), []byte("value"), 1, tc.store.StoreID())

	reply, err := tc.rng.AddCmd(tc.rng.context(), &pArgs)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*proto.PutResponse)
	if pReply.Timestamp.WallTime != tc.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %s", pReply.Timestamp)
	}
}

// TestRangeNoTSCacheInconsistent verifies that the timestamp cache
// is not affected by inconsistent reads.
func TestRangeNoTSCacheInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args := getArgs([]byte("a"), 1, tc.store.StoreID())
	args.Timestamp = tc.clock.Now()
	args.ReadConsistency = proto.INCONSISTENT

	_, err := tc.rng.AddCmd(tc.rng.context(), &args)

	if err != nil {
		t.Error(err)
	}
	pArgs := putArgs([]byte("a"), []byte("value"), 1, tc.store.StoreID())

	reply, err := tc.rng.AddCmd(tc.rng.context(), &pArgs)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*proto.PutResponse)
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
		pArgs := putArgs(key, []byte("value"), 1, tc.store.StoreID())
		pArgs.Txn = newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		pArgs.Timestamp = pArgs.Txn.Timestamp

		reply, err := tc.rng.AddCmd(tc.rng.context(), &pArgs)
		if err != nil {
			t.Fatalf("test %d: %s", i, err)
		}
		pReply := reply.(*proto.PutResponse)

		// Now attempt read or write.
		args := readOrWriteArgs(key, read, tc.rng.Desc().RangeID, tc.store.StoreID())
		args.Header().Timestamp = tc.clock.Now() // later timestamp

		if _, err := tc.rng.AddCmd(tc.rng.context(), args); err == nil {
			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!

		if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
			t.Fatalf("test %d: %s", i, err)
		}
		if !pReply.Timestamp.Equal(pArgs.Timestamp) {
			t.Errorf("expected timestamp not to advance %s != %s", pReply.Timestamp, pArgs.Timestamp)
		}
	}
}

// TestRangeNoTimestampIncrementWithinTxn verifies that successive
// read and write commands within the same transaction do not cause
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
	gArgs := getArgs(key, 1, tc.store.StoreID())
	gArgs.Txn = txn
	gArgs.Timestamp = txn.Timestamp

	if _, err := tc.rng.AddCmd(tc.rng.context(), &gArgs); err != nil {
		t.Fatal(err)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs := putArgs(key, []byte("value"), 1, tc.store.StoreID())
	pArgs.Txn = txn
	pArgs.Timestamp = pArgs.Txn.Timestamp

	reply, err := tc.rng.AddCmd(tc.rng.context(), &pArgs)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*proto.PutResponse)
	if !pReply.Timestamp.Equal(pArgs.Timestamp) {
		t.Errorf("expected timestamp to remain %s; got %s", pArgs.Timestamp, pReply.Timestamp)
	}

	// Resolve the intent.
	rArgs := &proto.ResolveIntentRequest{
		RequestHeader: *pArgs.Header(),
		IntentTxn:     *pArgs.Txn,
	}
	rArgs.IntentTxn.Status = proto.COMMITTED
	if _, err := tc.rng.AddCmd(tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	pArgs.Txn = nil
	expTS := pArgs.Timestamp
	expTS.Logical++

	if reply, err = tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	pReply = reply.(*proto.PutResponse)
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

	// Run the same increment 100 times, 50 with identical command IDs,
	// interleaved with 50 using a sequence of different command IDs.
	const numIncs = 100
	var wg sync.WaitGroup
	var count int64
	incFunc := func(idx int) {
		defer wg.Done()
		args := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
		args.Header().Timestamp = tc.clock.Now()
		if idx%2 == 0 {
			args.CmdID = proto.ClientCmdID{WallTime: 1, Random: 1}
		} else {
			args.CmdID = proto.ClientCmdID{WallTime: 1, Random: int64(idx + 100)}
		}
		resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
		reply := resp.(*proto.IncrementResponse)
		if err != nil {
			t.Fatal(err)
		}
		if idx%2 == 0 && reply.NewValue != 1 {
			t.Errorf("expected all incremented values to be 1; got %d", reply.NewValue)
		} else if idx%2 == 1 {
			atomic.AddInt64(&count, reply.NewValue)
		}
	}

	wg.Add(numIncs)
	// The assertions for the even-numbered half of this test look for a
	// hardcoded value of 1, so we have to make sure the first iteration
	// that actually runs is even-numbered so that the cached response
	// for that command ID has a value of 1. Otherwise, N odd-numbered
	// operations may get scheduled first, run with a different command
	// ID, and cause even-numbered operations to report something other
	// than 1, depending on the value of N.
	incFunc(0)
	for i := 1; i < numIncs; i++ {
		go incFunc(i)
	}
	// Wait for all to complete.
	wg.Wait()

	// Verify that all non-repeated client commands incremented the
	// counter starting at 2 all the way to 51 (sum of sequence = 1325).
	if count != 1325 {
		t.Errorf("expected sum of all increments to be 1325; got %d", count)
	}
}

// TestRangeResponseCacheReadError verifies that an error is returned to the
// client in the event that a response cache entry is found but is not
// decodable.
func TestRangeResponseCacheReadError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	args := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
	args.CmdID = proto.ClientCmdID{WallTime: 1, Random: 1}

	_, err := tc.rng.AddCmd(tc.rng.context(), &args)

	if err != nil {
		t.Fatal(err)
	}

	// Overwrite repsonse cache entry with garbage for the last op.
	key := keys.ResponseCacheKey(tc.rng.Desc().RangeID, &args.CmdID)
	err = engine.MVCCPut(tc.engine, nil, key, proto.ZeroTimestamp, proto.Value{Bytes: []byte("\xff")}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now try increment again and verify error.
	_, err = tc.rng.AddCmd(tc.rng.context(), &args)
	if err == nil {
		t.Fatal(err)
	}
}

// TestRangeResponseCacheStoredError verifies that if a cached entry contains
// an error, that error is returned.
func TestRangeResponseCacheStoredError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	cmdID := proto.ClientCmdID{WallTime: 1, Random: 1}
	// Write an error into the response cache.
	incReply := proto.IncrementResponse{}
	br := proto.BatchResponse{}
	br.Add(&incReply)
	pastError := errors.New("boom")
	_ = tc.rng.respCache.PutResponse(tc.engine, cmdID,
		proto.ResponseWithError{Reply: &br, Err: pastError})

	args := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
	args.CmdID = cmdID
	_, err := tc.rng.AddCmd(tc.rng.context(), &args)
	if err == nil {
		t.Fatal("expected to see cached error but got nil")
	} else if !testutils.IsError(err, pastError.Error()) {
		t.Fatalf("expected '%s', but got %s", pastError, err)
	}
}

// TestEndTransactionWithMalformedSplitTrigger verifies an
// EndTransaction call with a malformed commit trigger fails.
func TestEndTransactionWithMalformedSplitTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := proto.Key("foo")
	txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
	pArgs := putArgs(key, []byte("only here to make this a rw transaction"), tc.rng.Desc().RangeID, tc.store.StoreID())
	pArgs.Txn = txn
	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}

	args := endTxnArgs(txn, true /* commit */, 1, tc.store.StoreID())
	args.Timestamp = txn.Timestamp
	// Make an EndTransaction request which would fail if not
	// stripped. In this case, we set the start key to "bar" for a
	// split of the default range; start key must be "" in this case.
	args.InternalCommitTrigger = &proto.InternalCommitTrigger{
		SplitTrigger: &proto.SplitTrigger{
			UpdatedDesc: proto.RangeDescriptor{StartKey: proto.Key("bar")},
		},
	}

	if _, err := tc.rng.AddCmd(tc.rng.context(), &args); !testutils.IsError(err, "range does not match splits") {
		t.Errorf("expected range does not match splits error; got %s", err)
	}
}

// TestEndTransactionBeforeHeartbeat verifies that a transaction
// can be committed/aborted before being heartbeat.
func TestEndTransactionBeforeHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Don't automatically GC the Txn record: We want to heartbeat the
	// committed Transaction and compare it against our expectations.
	// When it's removed, the heartbeat would recreate it.
	defer setTxnAutoGC(false)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
		args := endTxnArgs(txn, commit, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp

		resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
		if err != nil {
			t.Error(err)
		}
		reply := resp.(*proto.EndTransactionResponse)
		expStatus := proto.COMMITTED
		if !commit {
			expStatus = proto.ABORTED
		}
		if reply.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
		}

		// Try a heartbeat to the already-committed transaction; should get
		// committed txn back, but without last heartbeat timestamp set.
		hBA := heartbeatArgs(txn, 1, tc.store.StoreID())

		resp, err = tc.rng.AddCmd(tc.rng.context(), &hBA)
		if err != nil {
			t.Error(err)
		}
		hBR := resp.(*proto.HeartbeatTxnResponse)
		if hBR.Txn.Status != expStatus || hBR.Txn.LastHeartbeat != nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hBR)
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
		hBA := heartbeatArgs(txn, 1, tc.store.StoreID())
		hBA.Timestamp = txn.Timestamp

		resp, err := tc.rng.AddCmd(tc.rng.context(), &hBA)
		if err != nil {
			t.Error(err)
		}
		hBR := resp.(*proto.HeartbeatTxnResponse)
		if hBR.Txn.Status != proto.PENDING || hBR.Txn.LastHeartbeat == nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hBR)
		}

		args := endTxnArgs(txn, commit, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp

		resp, err = tc.rng.AddCmd(tc.rng.context(), &args)
		if err != nil {
			t.Error(err)
		}
		reply := resp.(*proto.EndTransactionResponse)
		expStatus := proto.COMMITTED
		if !commit {
			expStatus = proto.ABORTED
		}
		if reply.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
		}
		if reply.Txn.LastHeartbeat == nil || !reply.Txn.LastHeartbeat.Equal(*hBR.Txn.LastHeartbeat) {
			t.Errorf("expected heartbeats to remain equal: %+v != %+v",
				reply.Txn.LastHeartbeat, hBR.Txn.LastHeartbeat)
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
		args := endTxnArgs(txn, test.commit, 1, tc.store.StoreID())
		tc.manualClock.Set(1)
		args.Timestamp = tc.clock.Now()

		resp, err := tc.rng.AddCmd(tc.rng.context(), &args)

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
			reply := resp.(*proto.EndTransactionResponse)
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
	hBA := heartbeatArgs(txn, 1, tc.store.StoreID())
	hBA.Timestamp = txn.Timestamp

	_, err := tc.rng.AddCmd(tc.rng.context(), &hBA)
	if err != nil {
		t.Error(err)
	}

	// Now end the txn with increased epoch and priority.
	args := endTxnArgs(txn, true, 1, tc.store.StoreID())
	args.Timestamp = txn.Timestamp
	args.Txn.Epoch = txn.Epoch + 1
	args.Txn.Priority = txn.Priority + 1

	resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
	if err != nil {
		t.Error(err)
	}
	reply := resp.(*proto.EndTransactionResponse)
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
		args := endTxnArgs(txn, true, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp

		if _, err := tc.rng.AddCmd(tc.rng.context(), &args); !testutils.IsError(err, test.expErrRegexp) {
			t.Errorf("expected %s to match %s", err, test.expErrRegexp)
		}
	}
}

// TestEndTransactionGC verifies that a transaction record is immediately
// garbage-collected upon EndTransaction iff all of the supplied intents are
// local relative to the transaction record's location.
func TestEndTransactionGC(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer setTxnAutoGC(true)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	splitKey := proto.Key("c")
	splitTestRange(tc.store, splitKey, splitKey, t)
	for i, test := range []struct {
		intents []proto.Intent
		expGC   bool
	}{
		// Range inside.
		{[]proto.Intent{{Key: proto.Key("a"), EndKey: proto.Key("b")}}, true},
		// Two intents inside.
		{[]proto.Intent{{Key: proto.Key("a")}, {Key: proto.Key("b")}}, true},
		// Intent range spilling over right endpoint.
		{[]proto.Intent{{Key: proto.Key("a"), EndKey: splitKey.Next()}}, false},
		// Intent range completely outside.
		{[]proto.Intent{{Key: splitKey, EndKey: proto.Key("q")}}, false},
		// Intent inside and outside.
		{[]proto.Intent{{Key: proto.Key("a")}, {Key: splitKey}}, false},
	} {
		txn := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
		args := endTxnArgs(txn, true, 1, tc.store.StoreID())
		args.Timestamp = txn.Timestamp
		args.Intents = test.intents
		if _, err := tc.rng.AddCmd(tc.rng.context(), &args); err != nil {
			t.Fatal(err)
		}
		var readTxn proto.Transaction
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		ok, err := engine.MVCCGetProto(tc.rng.rm.Engine(), txnKey, proto.ZeroTimestamp,
			true /* consistent */, nil /* txn */, &readTxn)
		if err != nil {
			t.Fatal(err)
		}
		if !ok != test.expGC {
			t.Errorf("%d: unexpected gc'ed: %t", i, !ok)
		}
	}
}

// TestEndTransactionResolveOnlyLocalIntents verifies that an end transaction
// request resolves only local intents within the same batch.
func TestEndTransactionResolveOnlyLocalIntents(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	key := proto.Key("a")
	splitKey := key.Next()
	TestingCommandFilter = func(args proto.Request) error {
		if args.Method() == proto.ResolveIntentRange && args.Header().Key.Equal(splitKey) {
			return util.Errorf("boom")
		}
		return nil
	}
	tc.Start(t)
	defer tc.Stop()

	// Split the range and create an intent in each range.
	// The keys of the two intents are next to each other.
	newRng := splitTestRange(tc.store, splitKey, splitKey, t)

	txn := newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
	pArgs := putArgs(key, []byte("value"), tc.rng.Desc().RangeID, tc.store.StoreID())
	pArgs.Txn = txn
	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}

	pArgs = putArgs(splitKey, []byte("value"), newRng.Desc().RangeID, tc.store.StoreID())
	pArgs.Txn = txn
	if _, err := newRng.AddCmd(newRng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}

	// End the transaction and resolve the intents.
	args := endTxnArgs(txn, true /* commit */, 1, tc.store.StoreID())
	args.Timestamp = txn.Timestamp
	args.Intents = []proto.Intent{{Key: key, EndKey: splitKey.Next()}}
	if _, err := tc.rng.AddCmd(tc.rng.context(), &args); err != nil {
		t.Fatal(err)
	}

	// Check if the intent in the other range has not yet been resolved.
	gArgs := getArgs(splitKey, newRng.Desc().RangeID, tc.store.StoreID())
	_, err := newRng.AddCmd(newRng.context(), &gArgs)
	if _, ok := err.(*proto.WriteIntentError); !ok {
		t.Errorf("expected write intent error, but got %s", err)
	}
}

// TestPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestPushTxnBadKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", proto.Key("a"), 1, proto.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", proto.Key("b"), 1, proto.SERIALIZABLE, tc.clock)

	args := pushTxnArgs(pusher, pushee, proto.ABORT_TXN, 1, tc.store.StoreID())
	args.Key = pusher.Key

	if _, err := tc.rng.AddCmd(tc.rng.context(), &args); !testutils.IsError(err, ".*should match pushee.*") {
		t.Errorf("unexpected error %s", err)
	}
}

// TestPushTxnAlreadyCommittedOrAborted verifies success
// (noop) in event that pushee is already committed or aborted.
func TestPushTxnAlreadyCommittedOrAborted(t *testing.T) {
	defer leaktest.AfterTest(t)
	// This test simulates running into an open intent and resolving it
	// using the Txn table. If we auto-gc'ed entries here, the entry would
	// be deleted and the intents resolved instantaneously on successful
	// commit (since they're on the same Range). Could split the range and have
	// non-local intents if we ever wanted to get rid of this.
	defer setTxnAutoGC(false)()
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
		etArgs := endTxnArgs(pushee, status == proto.COMMITTED, 1, tc.store.StoreID())
		etArgs.Timestamp = pushee.Timestamp

		if _, err := tc.rng.AddCmd(tc.rng.context(), &etArgs); err != nil {
			t.Fatal(err)
		}

		// Now try to push what's already committed or aborted.
		args := pushTxnArgs(pusher, pushee, proto.ABORT_TXN, 1, tc.store.StoreID())

		resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
		if err != nil {
			t.Fatal(err)
		}
		reply := resp.(*proto.PushTxnResponse)
		if reply.PusheeTxn.Status != status {
			t.Errorf("expected push txn to return with status == %s; got %+v", status, reply.PusheeTxn)
		}
	}
}

// TestPushTxnUpgradeExistingTxn verifies that pushing
// a transaction record with a new epoch upgrades the pushee's
// epoch and timestamp if greater. In all test cases, the
// priorities are set such that the push will succeed.
func TestPushTxnUpgradeExistingTxn(t *testing.T) {
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
		hBA := heartbeatArgs(pushee, 1, tc.store.StoreID())
		hBA.Timestamp = pushee.Timestamp

		if _, err := tc.rng.AddCmd(tc.rng.context(), &hBA); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction using updated values for epoch & timestamp.
		pushee.Epoch = test.epoch
		pushee.Timestamp = test.ts
		args := pushTxnArgs(pusher, pushee, proto.ABORT_TXN, 1, tc.store.StoreID())

		resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
		if err != nil {
			t.Fatal(err)
		}
		reply := resp.(*proto.PushTxnResponse)
		expTxn := gogoproto.Clone(pushee).(*proto.Transaction)
		expTxn.Epoch = test.expEpoch
		expTxn.Timestamp = test.expTS
		expTxn.Status = proto.ABORTED
		expTxn.LastHeartbeat = &test.startTS
		expTxn.Writing = true // always set for physical txn table entries

		if !reflect.DeepEqual(expTxn, reply.PusheeTxn) {
			t.Errorf("unexpected push txn in trial %d; expected %+v, got %+v", i, expTxn, reply.PusheeTxn)
		}
	}
}

// TestPushTxnHeartbeatTimeout verifies that a txn which
// hasn't been heartbeat within 2x the heartbeat interval can be
// pushed/aborted.
func TestPushTxnHeartbeatTimeout(t *testing.T) {
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
			hBA := heartbeatArgs(pushee, 1, tc.store.StoreID())
			hBA.Timestamp = *test.heartbeat

			if _, err := tc.rng.AddCmd(tc.rng.context(), &hBA); err != nil {
				t.Fatal(err)
			}
		}

		// Now, attempt to push the transaction with clock set to "currentTime".
		tc.manualClock.Set(test.currentTime)
		args := pushTxnArgs(pusher, pushee, test.pushType, 1, tc.store.StoreID())
		// Avoid logical ticks here, they make the borderline cases hard to
		// test.
		args.Timestamp = proto.Timestamp{WallTime: test.currentTime}
		args.Now = args.Timestamp
		args.Timestamp.Logical = 0

		_, err := tc.rng.AddCmd(tc.rng.context(), &args)

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

// TestPushTxnPriorities verifies that txns with lower
// priority are pushed; if priorities are equal, then the txns
// are ordered by txn timestamp, with the more recent timestamp
// being pushable.
func TestPushTxnPriorities(t *testing.T) {
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
		args := pushTxnArgs(pusher, pushee, test.pushType, 1, tc.store.StoreID())

		_, err := tc.rng.AddCmd(tc.rng.context(), &args)

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

// TestPushTxnPushTimestamp verifies that with args.Abort is
// false (i.e. for read/write conflict), the pushed txn keeps status
// PENDING, but has its txn Timestamp moved forward to the pusher's
// txn Timestamp + 1.
func TestPushTxnPushTimestamp(t *testing.T) {
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
	args := pushTxnArgs(pusher, pushee, proto.PUSH_TIMESTAMP, 1, tc.store.StoreID())

	resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
	if err != nil {
		t.Errorf("unexpected error on push: %s", err)
	}
	expTS := pusher.Timestamp
	expTS.Logical++
	reply := resp.(*proto.PushTxnResponse)
	if !reply.PusheeTxn.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to be pushed to %+v; got %+v", expTS, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != proto.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}

// TestPushTxnPushTimestampAlreadyPushed verifies that pushing
// a timestamp forward which is already far enough forward is a simple
// noop. We do this by ensuring that priorities would otherwise make
// pushing impossible.
func TestPushTxnPushTimestampAlreadyPushed(t *testing.T) {
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
	args := pushTxnArgs(pusher, pushee, proto.PUSH_TIMESTAMP, 1, tc.store.StoreID())

	resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
	if err != nil {
		t.Errorf("unexpected error on push: %s", err)
	}
	reply := resp.(*proto.PushTxnResponse)
	if !reply.PusheeTxn.Timestamp.Equal(pushee.Timestamp) {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.Timestamp, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != proto.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}

// TestRangeResolveIntentRange verifies resolving a range of intents.
func TestRangeResolveIntentRange(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Put two values transactionally.
	txn := &proto.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now()}
	for _, key := range []proto.Key{proto.Key("a"), proto.Key("b")} {
		pArgs := putArgs(key, []byte("value1"), 1, tc.store.StoreID())
		pArgs.Txn = txn
		if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Resolve the intents.
	rArgs := &proto.ResolveIntentRangeRequest{
		RequestHeader: proto.RequestHeader{
			Key:     proto.Key("a"),
			EndKey:  proto.Key("c"),
			RangeID: tc.rng.Desc().RangeID,
			Replica: proto.Replica{StoreID: tc.store.StoreID()},
		},
		IntentTxn: *txn,
	}
	rArgs.IntentTxn.Status = proto.COMMITTED
	if _, err := tc.rng.AddCmd(tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}

	// Do a consistent scan to verify intents have been cleared.
	sArgs := scanArgs(proto.Key("a"), proto.Key("c"), 1, tc.store.StoreID())
	reply, err := tc.store.ExecuteCmd(context.Background(), &sArgs)
	if err != nil {
		t.Fatalf("unexpected error on scan: %s", err)
	}
	sReply := reply.(*proto.ScanResponse)
	if len(sReply.Rows) != 2 {
		t.Errorf("expected 2 rows; got %v", sReply.Rows)
	}
}

func verifyRangeStats(eng engine.Engine, rangeID proto.RangeID, expMS engine.MVCCStats, t *testing.T) {
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, rangeID, &ms); err != nil {
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
	pArgs := putArgs([]byte("a"), []byte("value1"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()

	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}
	expMS := engine.MVCCStats{LiveBytes: 41, KeyBytes: 15, ValBytes: 26, IntentBytes: 0, LiveCount: 1, KeyCount: 1, ValCount: 1, IntentCount: 0, SysBytes: 60, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)

	// Put a 2nd value transactionally.
	pArgs = putArgs([]byte("b"), []byte("value2"), 1, tc.store.StoreID())
	pArgs.Timestamp = tc.clock.Now()
	pArgs.Txn = &proto.Transaction{ID: uuid.NewUUID4(), Timestamp: pArgs.Timestamp}

	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 134, KeyBytes: 30, ValBytes: 104, IntentBytes: 26, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 1, SysBytes: 60, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)

	// Resolve the 2nd value.
	rArgs := &proto.ResolveIntentRequest{
		RequestHeader: proto.RequestHeader{
			Key:     pArgs.Key,
			RangeID: tc.rng.Desc().RangeID,
			Replica: proto.Replica{StoreID: tc.store.StoreID()},
		},
		IntentTxn: *pArgs.Txn,
	}
	rArgs.IntentTxn.Status = proto.COMMITTED

	if _, err := tc.rng.AddCmd(tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 82, KeyBytes: 30, ValBytes: 52, IntentBytes: 0, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 0, SysBytes: 60, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)

	// Delete the 1st value.
	dArgs := deleteArgs([]byte("a"), 1, tc.store.StoreID())
	dArgs.Timestamp = tc.clock.Now()

	if _, err := tc.rng.AddCmd(tc.rng.context(), &dArgs); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 41, KeyBytes: 42, ValBytes: 54, IntentBytes: 0, LiveCount: 1, KeyCount: 2, ValCount: 3, IntentCount: 0, SysBytes: 60, SysCount: 1}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)
}

// TestMerge verifies that the Merge command is behaving as
// expected. Merge semantics for different data types are tested more
// robustly at the engine level; this test is intended only to show
// that values passed to Merge are being merged.
func TestMerge(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("mergedkey")
	stringArgs := []string{"a", "b", "c", "d"}
	stringExpected := "abcd"

	for _, str := range stringArgs {
		mergeArgs := internalMergeArgs(key, proto.Value{Bytes: []byte(str)}, 1, tc.store.StoreID())

		if _, err := tc.rng.AddCmd(tc.rng.context(), &mergeArgs); err != nil {
			t.Fatalf("unexpected error from Merge: %s", err.Error())
		}
	}

	getArgs := getArgs(key, 1, tc.store.StoreID())

	reply, err := tc.rng.AddCmd(tc.rng.context(), &getArgs)
	if err != nil {
		t.Fatalf("unexpected error from Get: %s", err)
	}
	resp := reply.(*proto.GetResponse)
	if resp.Value == nil {
		t.Fatal("GetResponse had nil value")
	}
	if a, e := resp.Value.Bytes, []byte(stringExpected); !bytes.Equal(a, e) {
		t.Errorf("Get did not return expected value: %s != %s", string(a), e)
	}
}

// TestTruncateLog verifies that the TruncateLog command removes a
// prefix of the raft logs (modifying FirstIndex() and making them
// inaccessible via Entries()).
func TestTruncateLog(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i), 1, tc.store.StoreID())

		if _, err := tc.rng.AddCmd(tc.rng.context(), &args); err != nil {
			t.Fatal(err)
		}
		idx, err := tc.rng.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	// Discard the first half of the log
	truncateArgs := truncateLogArgs(indexes[5], 1, tc.store.StoreID())

	if _, err := tc.rng.AddCmd(tc.rng.context(), &truncateArgs); err != nil {
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
	stopper := stop.NewStopper()
	defer stopper.Stop()
	storagetest.RunTests(t,
		func(t *testing.T) storagetest.WriteableStorage {
			eng = engine.NewInMem(proto.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20, stopper)
			// Fake store to house the engine.
			store := &Store{
				ctx: StoreContext{
					Clock: hlc.NewClock(hlc.UnixNano),
				},
				engine: eng,
			}
			rng, err := NewReplica(&proto.RangeDescriptor{
				RangeID:  1,
				StartKey: proto.KeyMin,
				EndKey:   proto.KeyMax,
			}, store)
			if err != nil {
				t.Fatal(err)
			}
			return rng
		},
		func(t *testing.T, r storagetest.WriteableStorage) {
			// Do nothing
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
	pArgs := putArgs(key, value, 1, tc.store.StoreID())

	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}
	args := proto.ConditionalPutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			Timestamp: proto.MinTimestamp,
			RangeID:   1,
			Replica:   proto.Replica{StoreID: tc.store.StoreID()},
		},
		Value: proto.Value{
			Bytes: value,
		},
		ExpValue: &proto.Value{
			Bytes: []byte("moo"),
		},
	}

	_, err := tc.rng.AddCmd(tc.rng.context(), &args)

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
		args := incrementArgs([]byte("a"), i, 1, tc.store.StoreID())

		resp, err := tc.rng.AddCmd(tc.rng.context(), &args)
		if err != nil {
			t.Fatal(err)
		}
		reply := resp.(*proto.IncrementResponse)
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

// TestReplicaCorruption verifies that a replicaCorruptionError correctly marks
// the range as corrupt.
func TestReplicaCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	args := putArgs(proto.Key("test"), []byte("value"), tc.rng.Desc().RangeID, tc.store.StoreID())
	_, err := tc.rng.AddCmd(tc.rng.context(), &args)
	if err != nil {
		t.Fatal(err)
	}
	// Set the stored applied index sky high.
	newIndex := 2*atomic.LoadUint64(&tc.rng.appliedIndex) + 1
	atomic.StoreUint64(&tc.rng.appliedIndex, newIndex)
	// Not really needed, but let's be thorough.
	err = setAppliedIndex(tc.rng.rm.Engine(), tc.rng.Desc().RangeID, newIndex)
	if err != nil {
		t.Fatal(err)
	}
	// Should mark replica corrupt (and panic as a result) since we messed
	// with the applied index.
	_, err = tc.rng.AddCmd(tc.rng.context(), &args)

	if err == nil || !strings.Contains(err.Error(), "replica corruption (processed=true)") {
		t.Fatalf("unexpected error: %s", err)
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
	}, tc.rng.Desc()); err == nil || !strings.Contains(err.Error(),
		"already present") {
		t.Fatalf("must not be able to add second replica to same node (err=%s)",
			err)
	}
}

// TestRangeDanglingMetaIntent creates a dangling intent on a meta2
// record and verifies that RangeLookup requests behave
// appropriately. Normally, the old value and a write intent error
// should be returned. If IgnoreIntents is specified, then a random
// choice of old or new is returned with no error.
// TODO(tschottdorf): add a test in which there is a dangling intent on a
// descriptor we would've otherwise discarded in a reverse scan; verify that
// we don't erroneously return that descriptor (recently fixed bug) if the
func TestRangeDanglingMetaIntent(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(tschottdorf): disabled; see comment in RangeLookup")
	// Test RangeLookup with Scan.
	testRangeDanglingMetaIntent(t, false)
	// Test RangeLookup with ReverseScan.
	testRangeDanglingMetaIntent(t, true)
}

func testRangeDanglingMetaIntent(t *testing.T, isReverse bool) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := proto.Key("a")

	// Get original meta2 descriptor.
	rlArgs := &proto.RangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			Key:             keys.RangeMetaKey(key),
			RangeID:         tc.rng.Desc().RangeID,
			Replica:         proto.Replica{StoreID: tc.store.StoreID()},
			ReadConsistency: proto.INCONSISTENT,
		},
		MaxRanges: 1,
		Reverse:   isReverse,
	}

	var rlReply *proto.RangeLookupResponse

	reply, err := tc.rng.AddCmd(tc.rng.context(), rlArgs)
	if err != nil {
		t.Fatal(err)
	}
	rlReply = reply.(*proto.RangeLookupResponse)

	origDesc := rlReply.Ranges[0]
	newDesc := origDesc
	newDesc.EndKey = key

	// Write the new descriptor as an intent.
	data, err := gogoproto.Marshal(&newDesc)
	if err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs(keys.RangeMetaKey(key), data, 1, tc.store.StoreID())
	pArgs.Txn = newTransaction("test", key, 1, proto.SERIALIZABLE, tc.clock)
	pArgs.Timestamp = pArgs.Txn.Timestamp

	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}

	// Now lookup the range; should get the value. Since the lookup is
	// inconsistent, there's no WriteIntentErorr.
	rlArgs.Key = keys.RangeMetaKey(proto.Key("A"))
	rlArgs.Timestamp = proto.ZeroTimestamp

	reply, err = tc.rng.AddCmd(tc.rng.context(), rlArgs)
	if err != nil {
		t.Errorf("unexpected lookup error: %s", err)
	}
	rlReply = reply.(*proto.RangeLookupResponse)
	if !reflect.DeepEqual(rlReply.Ranges[0], origDesc) {
		t.Errorf("expected original descriptor %s; got %s", &origDesc, &rlReply.Ranges[0])
	}

	// Switch to consistent lookups, which should run into the intent.
	rlArgs.ReadConsistency = proto.CONSISTENT
	_, err = tc.rng.AddCmd(tc.rng.context(), rlArgs)
	if _, ok := err.(*proto.WriteIntentError); !ok {
		t.Fatalf("expected WriteIntentError, not %s", err)
	}

	// Try 100 lookups with IgnoreIntents. Expect to see each descriptor at least once.
	// First, try this consistently, which should not be allowed.
	rlArgs.ConsiderIntents = true
	_, err = tc.rng.AddCmd(tc.rng.context(), rlArgs)
	if !testutils.IsError(err, "can not read consistently and skip intents") {
		t.Fatalf("wanted specific error, not %s", err)
	}
	// After changing back to inconsistent lookups, should be good to go.
	rlArgs.ReadConsistency = proto.INCONSISTENT

	var origSeen, newSeen bool
	const count = 100

	for i := 0; i < count && !(origSeen && newSeen); i++ {
		clonedRLArgs := gogoproto.Clone(rlArgs).(*proto.RangeLookupRequest)
		clonedRLArgs.Timestamp = proto.ZeroTimestamp

		reply, err = tc.rng.AddCmd(tc.rng.context(), clonedRLArgs)
		if err != nil {
			t.Fatal(err)
		}
		rlReply = reply.(*proto.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if reflect.DeepEqual(seen, origDesc) {
			origSeen = true
		} else if reflect.DeepEqual(seen, newDesc) {
			newSeen = true
		} else {
			t.Errorf("expected orig/new descriptor %s/%s; got %s", &origDesc, &newDesc, &seen)
		}
	}
	if !(origSeen && newSeen) {
		t.Errorf("didn't see both descriptors after %d attempts", count)
	}
}

// TestRangeLookupUseReverseScan verifies the correctness of the results which are retrieved
// from RangeLookup by using ReverseScan.
func TestRangeLookupUseReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(tschottdorf): disabled; see comment in RangeLookup")
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Test ranges: ["a","c"), ["c","f"), ["f","h") and ["h","y").
	testRanges := []proto.RangeDescriptor{
		{RangeID: 2, StartKey: proto.Key("a"), EndKey: proto.Key("c")},
		{RangeID: 3, StartKey: proto.Key("c"), EndKey: proto.Key("f")},
		{RangeID: 4, StartKey: proto.Key("f"), EndKey: proto.Key("h")},
		{RangeID: 5, StartKey: proto.Key("h"), EndKey: proto.Key("y")},
	}
	// The range ["f","h") has dangling intent in meta2.
	withIntentRangeIndex := 2

	testCases := []struct {
		key      string
		expected proto.RangeDescriptor
	}{
		// For testRanges[0|1|3] there is no intent. A key in the middle
		// and the end key should both give us the range itself.
		{key: "b", expected: testRanges[0]},
		{key: "c", expected: testRanges[0]},
		{key: "d", expected: testRanges[1]},
		{key: "f", expected: testRanges[1]},
		{key: "j", expected: testRanges[3]},
		// testRanges[2] has an intent, so the inconsistent scan will read
		// an old value (nil). Since we're in reverse mode, testRanges[1]
		// is the result.
		{key: "g", expected: testRanges[1]},
		{key: "h", expected: testRanges[1]},
	}

	txn := &proto.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now()}
	for i, r := range testRanges {
		if i != withIntentRangeIndex {
			// Write the new descriptor as an intent.
			data, err := gogoproto.Marshal(&r)
			if err != nil {
				t.Fatal(err)
			}
			pArgs := putArgs(keys.RangeMetaKey(r.EndKey), data,
				tc.rng.Desc().RangeID, tc.store.StoreID())
			pArgs.Txn = txn

			if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Resolve the intents.
	rArgs := &proto.ResolveIntentRangeRequest{
		RequestHeader: proto.RequestHeader{
			Key:     keys.RangeMetaKey(proto.Key("a")),
			EndKey:  keys.RangeMetaKey(proto.Key("z")),
			RangeID: tc.rng.Desc().RangeID,
			Replica: proto.Replica{StoreID: tc.store.StoreID()},
		},
		IntentTxn: *txn,
	}
	rArgs.IntentTxn.Status = proto.COMMITTED
	if _, err := tc.rng.AddCmd(tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}

	// Get original meta2 descriptor.
	rlArgs := &proto.RangeLookupRequest{
		RequestHeader: proto.RequestHeader{
			RangeID:         tc.rng.Desc().RangeID,
			Replica:         proto.Replica{StoreID: tc.store.StoreID()},
			ReadConsistency: proto.INCONSISTENT,
		},
		MaxRanges: 1,
		Reverse:   true,
	}
	var rlReply *proto.RangeLookupResponse

	// Test ReverseScan without intents.
	for _, c := range testCases {
		clonedRLArgs := gogoproto.Clone(rlArgs).(*proto.RangeLookupRequest)
		clonedRLArgs.Timestamp = proto.ZeroTimestamp
		clonedRLArgs.Key = keys.RangeMetaKey(proto.Key(c.key))
		reply, err := tc.rng.AddCmd(tc.rng.context(), clonedRLArgs)
		if err != nil {
			t.Fatal(err)
		}
		rlReply = reply.(*proto.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if !(seen.StartKey.Equal(c.expected.StartKey) && seen.EndKey.Equal(c.expected.EndKey)) {
			t.Errorf("expected descriptor %s; got %s", &c.expected, &seen)
		}
	}

	// Write the new descriptor as an intent.
	intentRange := testRanges[withIntentRangeIndex]
	data, err := gogoproto.Marshal(&intentRange)
	if err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs(keys.RangeMetaKey(intentRange.EndKey), data, 1, tc.store.StoreID())
	pArgs.Txn = &proto.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now()}
	pArgs.Timestamp = pArgs.Txn.Timestamp
	if _, err := tc.rng.AddCmd(tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}

	// Test ReverseScan with intents.
	for _, c := range testCases {
		clonedRLArgs := gogoproto.Clone(rlArgs).(*proto.RangeLookupRequest)
		clonedRLArgs.Timestamp = proto.ZeroTimestamp
		clonedRLArgs.Key = keys.RangeMetaKey(proto.Key(c.key))
		reply, err := tc.rng.AddCmd(tc.rng.context(), clonedRLArgs)
		if err != nil {
			t.Fatal(err)
		}
		rlReply = reply.(*proto.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if !(seen.StartKey.Equal(c.expected.StartKey) && seen.EndKey.Equal(c.expected.EndKey)) {
			t.Errorf("expected descriptor %s; got %s", &c.expected, &seen)
		}
	}
}

func TestRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	expected := []proto.RangeDescriptor{*tc.rng.Desc()}
	testCases := []struct {
		key      proto.Key
		reverse  bool
		expected []proto.RangeDescriptor
	}{

		// Test with the first range (StartKey==KeyMin). Normally we look
		// up this range in gossip instead of executing the RPC, but
		// RangeLookup is still used when up-to-date information is
		// required.
		{key: proto.KeyMin, reverse: false, expected: expected},
		// Test with the last key in a meta prefix. This is an edge case in the
		// implementation.
		{key: keys.Meta1KeyMax, reverse: false, expected: expected},
		{key: keys.Meta2KeyMax, reverse: false, expected: nil},
		{key: keys.Meta1KeyMax, reverse: true, expected: expected},
		{key: keys.Meta2KeyMax, reverse: true, expected: expected},
	}

	for _, c := range testCases {
		resp, err := tc.store.ExecuteCmd(context.Background(), &proto.RangeLookupRequest{
			RequestHeader: proto.RequestHeader{
				RangeID: 1,
				Key:     c.key,
			},
			MaxRanges: 1,
			Reverse:   c.reverse,
		})
		if err != nil {
			if c.expected != nil {
				t.Fatal(err)
			}
		} else {
			reply := resp.(*proto.RangeLookupResponse)
			if !reflect.DeepEqual(reply.Ranges, c.expected) {
				t.Fatalf("expected %+v, got %+v", c.expected, reply.Ranges)
			}
		}
	}
}

// benchmarkEvents is designed to determine the impact of sending events on the
// performance of write commands. This benchmark can be run with or without
// events, and with or without a consumer reading the events.
func benchmarkEvents(b *testing.B, sendEvents, consumeEvents bool) {
	defer leaktest.AfterTest(b)
	tc := testContext{}

	stopper := stop.NewStopper()
	if sendEvents {
		tc.feed = util.NewFeed(stopper)
	}
	eventC := 0
	if consumeEvents {
		tc.feed.Subscribe(func(_ interface{}) {
			eventC++
		})
	}

	tc.Start(b)
	defer tc.Stop()

	args := incrementArgs([]byte("a"), 1, 1, tc.store.StoreID())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := tc.rng.AddCmd(tc.rng.context(), &args)

		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	stopper.Stop()

	if consumeEvents {
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

type mockRangeManager struct {
	*Store
	mockProposeRaftCommand func(cmdIDKey, proto.RaftCommand) <-chan error
}

// ProposeRaftCommand mocks out the corresponding method on the Store.
func (mrm *mockRangeManager) ProposeRaftCommand(idKey cmdIDKey, cmd proto.RaftCommand) <-chan error {
	if mrm.mockProposeRaftCommand == nil {
		return mrm.Store.ProposeRaftCommand(idKey, cmd)
	}
	return mrm.mockProposeRaftCommand(idKey, cmd)
}

// TestRequestLeaderEncounterGroupDeleteError verifies that a request leader proposal which fails with
// multiraft.ErrGroupDeleted is converted to a RangeNotFoundError in the Store.
func TestRequestLeaderEncounterGroupDeleteError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{
		rng: &Replica{},
	}
	tc.Start(t)
	defer tc.Stop()

	// Mock the RangeManager, simulate proposeRaftCommand return an ErrGroupDeleted error.
	proposeRaftCommandFn := func(cmdIDKey, proto.RaftCommand) <-chan error {
		ch := make(chan error, 1)
		ch <- multiraft.ErrGroupDeleted
		return ch
	}
	testRangeManager := &mockRangeManager{
		Store: tc.store,
		mockProposeRaftCommand: proposeRaftCommandFn,
	}
	rng, err := NewReplica(testRangeDescriptor(), testRangeManager)
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.store.AddReplicaTest(rng); err != nil {
		t.Fatal(err)
	}

	gArgs := getArgs(proto.Key("a"), 1, tc.store.StoreID())
	// Force the read command request a new lease.
	clock := tc.clock
	gArgs.Header().Timestamp = clock.Update(clock.Now().Add(int64(DefaultLeaderLeaseDuration), 0))
	{
		_, pErr := tc.store.ExecuteCmd(context.Background(), &gArgs)
		err = pErr.GoError()
	}
	if _, ok := err.(*proto.RangeNotFoundError); !ok {
		t.Fatalf("expected a RangeNotFoundError, get %s", err)
	}
}

func TestIntentIntersect(t *testing.T) {
	defer leaktest.AfterTest(t)
	iPt := proto.Intent{
		Key:    proto.Key("asd"),
		EndKey: nil,
	}
	iRn := proto.Intent{
		Key:    proto.Key("c"),
		EndKey: proto.Key("x"),
	}

	suffix := proto.Key("abcd")
	iLc := proto.Intent{
		Key:    keys.MakeRangeKey(proto.Key("c"), suffix, nil),
		EndKey: keys.MakeRangeKey(proto.Key("x"), suffix, nil),
	}
	kl1 := string(iLc.Key)
	kl2 := string(iLc.EndKey)

	for i, tc := range []struct {
		intent   proto.Intent
		from, to string
		exp      []string
	}{
		{intent: iPt, from: "", to: "z", exp: []string{"", ""}},

		{intent: iRn, from: "", to: "a", exp: []string{"", "", "c", "x"}},
		{intent: iRn, from: "", to: "c", exp: []string{"", "", "c", "x"}},
		{intent: iRn, from: "a", to: "z", exp: []string{"c", "x"}},
		{intent: iRn, from: "c", to: "d", exp: []string{"c", "d", "d", "x"}},
		{intent: iRn, from: "c", to: "x", exp: []string{"c", "x"}},
		{intent: iRn, from: "d", to: "x", exp: []string{"d", "x", "c", "d"}},
		{intent: iRn, from: "d", to: "w", exp: []string{"d", "w", "c", "d", "w", "x"}},
		{intent: iRn, from: "c", to: "w", exp: []string{"c", "w", "w", "x"}},
		{intent: iRn, from: "w", to: "x", exp: []string{"w", "x", "c", "w"}},
		{intent: iRn, from: "x", to: "z", exp: []string{"", "", "c", "x"}},
		{intent: iRn, from: "y", to: "z", exp: []string{"", "", "c", "x"}},

		// A local intent range always comes back in one piece, either inside
		// or outside of the Range.
		{intent: iLc, from: "a", to: "b", exp: []string{"", "", kl1, kl2}},
		{intent: iLc, from: "d", to: "z", exp: []string{"", "", kl1, kl2}},
		{intent: iLc, from: "f", to: "g", exp: []string{"", "", kl1, kl2}},
		{intent: iLc, from: "c", to: "x", exp: []string{kl1, kl2}},
		{intent: iLc, from: "a", to: "z", exp: []string{kl1, kl2}},
	} {
		var all []string
		in, out := intersectIntent(tc.intent, proto.RangeDescriptor{
			StartKey: proto.Key(tc.from),
			EndKey:   proto.Key(tc.to),
		})
		if in != nil {
			all = append(all, string(in.Key), string(in.EndKey))
		} else {
			all = append(all, "", "")
		}
		for _, o := range out {
			all = append(all, string(o.Key), string(o.EndKey))
		}
		if !reflect.DeepEqual(all, tc.exp) {
			t.Errorf("%d: wanted %v, got %v", i, tc.exp, all)
		}
	}
}

// TestBatchErrorWithIndex tests that when an individual entry in a batch
// results in an error which implements proto.IndexedError, the index of this
// command is stored into the error.
func TestBatchErrorWithIndex(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	ba := &proto.BatchRequest{}
	ba.RangeID = tc.rng.Desc().RangeID
	ba.Replica.StoreID = tc.store.StoreID()
	ba.Add(&proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: proto.Key("k")},
		Value:         proto.Value{Bytes: []byte("not nil")},
	})
	// This one fails with a ConditionalPutError, which implements
	// proto.IndexedError.
	ba.Add(&proto.ConditionalPutRequest{
		RequestHeader: proto.RequestHeader{Key: proto.Key("k")},
		Value:         proto.Value{Bytes: []byte("irrelevant")},
		ExpValue:      nil, // not true after above Put
	})
	// This one is never executed.
	ba.Add(&proto.GetRequest{
		RequestHeader: proto.RequestHeader{Key: proto.Key("k")},
	})

	if _, err := tc.rng.AddCmd(tc.rng.context(), ba); err == nil {
		t.Fatal("expected an error")
	} else if iErr, ok := err.(proto.IndexedError); !ok {
		t.Fatalf("expected indexed error, got %s", err)
	} else if index, ok := iErr.ErrorIndex(); !ok || index != 1 || !testutils.IsError(err, "unexpected value") {
		t.Fatalf("invalid index or error type: %s", iErr)
	}

}
