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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/coreos/etcd/raft"
	"github.com/gogo/protobuf/proto"
)

const runUnmarshalCallbackTimeout = 100 * time.Millisecond

func testRangeDescriptor() *roachpb.RangeDescriptor {
	return &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		Replicas: []roachpb.ReplicaDescriptor{
			{
				ReplicaID: 1,
				NodeID:    1,
				StoreID:   1,
			},
		},
		NextReplicaID: 2,
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
	transport     RaftTransport
	store         *Store
	rng           *Replica
	rangeID       roachpb.RangeID
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
		tc.gossip = gossip.New(rpcContext, gossip.TestBootstrap)
		tc.gossip.SetNodeID(1)
	}
	if tc.manualClock == nil {
		tc.manualClock = hlc.NewManualClock(0)
	}
	if tc.clock == nil {
		tc.clock = hlc.NewClock(tc.manualClock.UnixNano)
	}
	if tc.engine == nil {
		tc.engine = engine.NewInMem(roachpb.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20, tc.stopper)
	}
	if tc.transport == nil {
		tc.transport = NewLocalRPCTransport(tc.stopper)
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
		tc.store = NewStore(ctx, tc.engine, &roachpb.NodeDescriptor{NodeID: 1})
		if err := tc.store.Bootstrap(roachpb.StoreIdent{
			ClusterID: "test",
			NodeID:    1,
			StoreID:   1,
		}, tc.stopper); err != nil {
			t.Fatal(err)
		}
		// Now that we have our actual store, monkey patch the sender used in ctx.DB.
		sender.store = tc.store
		// We created the store without a real KV client, so it can't perform splits.
		tc.store.splitQueue.SetDisabled(true)

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

func (tc *testContext) Sender() client.Sender {
	return client.Wrap(tc.rng, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
		if ba.RangeID != 0 {
			ba.RangeID = 1
		}
		return ba
	})
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

	if err := util.IsTrueWithin(func() bool {
		return tc.gossip.GetSystemConfig() != nil
	}, runUnmarshalCallbackTimeout); err != nil {
		return err
	}

	return nil
}

func newTransaction(name string, baseKey roachpb.Key, userPriority int32,
	isolation roachpb.IsolationType, clock *hlc.Clock) *roachpb.Transaction {
	var offset int64
	var now roachpb.Timestamp
	if clock != nil {
		offset = clock.MaxOffset().Nanoseconds()
		now = clock.Now()
	}
	return roachpb.NewTransaction(name, baseKey, userPriority,
		isolation, now, offset)
}

// CreateReplicaSets creates new roachpb.ReplicaDescriptor protos based on an array of
// StoreIDs to aid in testing. Note that this does not actually produce any
// replicas, it just creates the descriptors.
func createReplicaSets(replicaNumbers []roachpb.StoreID) []roachpb.ReplicaDescriptor {
	result := []roachpb.ReplicaDescriptor{}
	for _, replicaNumber := range replicaNumbers {
		result = append(result, roachpb.ReplicaDescriptor{
			StoreID: replicaNumber,
		})
	}
	return result
}

// TestRangeContains verifies that the range uses Key.Address() in
// order to properly resolve addresses for local keys.
func TestRangeContains(t *testing.T) {
	defer leaktest.AfterTest(t)
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := engine.NewInMem(roachpb.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20, stopper)
	clock := hlc.NewClock(hlc.UnixNano)
	ctx := TestStoreContext
	ctx.Clock = clock
	ctx.Transport = NewLocalRPCTransport(stopper)
	defer ctx.Transport.Close()
	store := NewStore(ctx, e, &roachpb.NodeDescriptor{NodeID: 1})
	r, err := NewReplica(desc, store)
	if err != nil {
		t.Fatal(err)
	}
	if !r.ContainsKey(roachpb.Key("aa")) {
		t.Errorf("expected range to contain key \"aa\"")
	}
	if !r.ContainsKey(keys.RangeDescriptorKey([]byte("aa"))) {
		t.Errorf("expected range to contain range descriptor key for \"aa\"")
	}
	if !r.ContainsKeyRange(roachpb.Key("aa"), roachpb.Key("b")) {
		t.Errorf("expected range to contain key range \"aa\"-\"b\"")
	}
	if !r.ContainsKeyRange(keys.RangeDescriptorKey([]byte("aa")),
		keys.RangeDescriptorKey([]byte("b"))) {
		t.Errorf("expected range to contain key transaction range \"aa\"-\"b\"")
	}
}

func setLeaderLease(t *testing.T, r *Replica, l *roachpb.Lease) {
	ba := roachpb.BatchRequest{}
	ba.Add(&roachpb.LeaderLeaseRequest{Lease: *l})
	pendingCmd, err := r.proposeRaftCommand(r.context(), ba)
	if err == nil {
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
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	gArgs := getArgs(roachpb.Key("a"))

	// Try consistent read and verify success.

	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &gArgs); err != nil {
		t.Errorf("expected success on consistent read: %s", err)
	}

	// Try a consensus read and verify error.

	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		ReadConsistency: roachpb.CONSENSUS,
	}, &gArgs); err == nil {
		t.Errorf("expected error on consensus read")
	}

	// Try an inconsistent read within a transaction.
	txn := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)

	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Txn:             txn,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); err == nil {
		t.Errorf("expected error on inconsistent read within a txn")
	}

	// Lose the lease and verify CONSISTENT reads receive NotLeaderError
	// and INCONSISTENT reads work as expected.
	start := tc.rng.getLease().Expiration.Add(1, 0)
	tc.manualClock.Set(start.WallTime)
	setLeaderLease(t, tc.rng, &roachpb.Lease{
		Start:      start,
		Expiration: start.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{ // a different node
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	})

	// Send without Txn.
	_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		ReadConsistency: roachpb.CONSISTENT,
	}, &gArgs)
	if _, ok := err.(*roachpb.NotLeaderError); !ok {
		t.Errorf("expected not leader error; got %s", err)
	}

	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); err != nil {
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
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	pArgs := putArgs(roachpb.Key("a"), []byte("asd"))

	// Lose the lease.
	start := tc.rng.getLease().Expiration.Add(1, 0)
	tc.manualClock.Set(start.WallTime)
	setLeaderLease(t, tc.rng, &roachpb.Lease{
		Start:      start,
		Expiration: start.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{ // a different node
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	})

	_, err := client.SendWrappedWith(tc.Sender(), nil, roachpb.Header{
		Timestamp: tc.clock.Now().Add(-100, 0),
	}, &pArgs)
	if _, ok := err.(*roachpb.NotLeaderError); !ok {
		t.Fatalf("expected not leader error in return, got %v", err)
	}
}

func TestRangeRangeBoundsChecking(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	splitTestRange(tc.store, roachpb.RKey("a"), roachpb.RKey("a"), t)
	gArgs := getArgs(roachpb.Key("b"))

	_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &gArgs)

	if _, ok := err.(*roachpb.RangeKeyMismatchError); !ok {
		t.Errorf("expected range key mismatch error: %s", err)
	}
}

// hasLease returns whether the most recent leader lease was held by the given
// range replica and whether it's expired for the given timestamp.
func hasLease(rng *Replica, timestamp roachpb.Timestamp) (bool, bool) {
	l := rng.getLease()
	return l.OwnedBy(rng.store.StoreID()), !l.Covers(timestamp)
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
	secondReplica := roachpb.ReplicaDescriptor{
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
	setLeaderLease(t, tc.rng, &roachpb.Lease{
		Start:      now.Add(10, 0),
		Expiration: now.Add(20, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	})
	if held, expired := hasLease(tc.rng, tc.clock.Now().Add(15, 0)); held || expired {
		t.Errorf("expected another replica to have leader lease")
	}

	err := tc.rng.redirectOnOrAcquireLeaderLease(nil, tc.clock.Now())
	if lErr, ok := err.(*roachpb.NotLeaderError); !ok || lErr == nil {
		t.Fatalf("wanted NotLeaderError, got %s", err)
	}

	// Advance clock past expiration and verify that another has
	// leader lease will not be true.
	tc.manualClock.Increment(21) // 21ns pass
	if held, expired := hasLease(tc.rng, tc.clock.Now()); held || !expired {
		t.Errorf("expected another replica to have expired lease")
	}

	// Verify that command returns NotLeaderError when lease is rejected.
	rng, err := NewReplica(testRangeDescriptor(), tc.store)
	if err != nil {
		t.Fatal(err)
	}
	rng.proposeRaftCommandFn = func(id cmdIDKey, cmd roachpb.RaftCommand) error {
		return &roachpb.LeaseRejectedError{
			Message: "replica not found",
		}
	}

	if _, ok := rng.redirectOnOrAcquireLeaderLease(nil, tc.clock.Now()).(*roachpb.NotLeaderError); !ok {
		t.Fatalf("expected %T, got %s", &roachpb.NotLeaderError{}, err)
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
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := tc.clock.Now()
	setLeaderLease(t, tc.rng, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	})

	header := roachpb.Span{
		Key: roachpb.Key("a"),
	}
	testCases := []roachpb.Request{
		// Admin split covers admin commands.
		&roachpb.AdminSplitRequest{
			Span:     header,
			SplitKey: roachpb.Key("a"),
		},
		// Get covers read-only commands.
		&roachpb.GetRequest{
			Span: header,
		},
		// Put covers read-write commands.
		&roachpb.PutRequest{
			Span:  header,
			Value: roachpb.MakeValueFromString("value"),
		},
	}

	for i, test := range testCases {
		_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: now}, test)

		if _, ok := err.(*roachpb.NotLeaderError); !ok {
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
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxSystemDescID)
	var val roachpb.Value
	val.SetInt(42)
	if err := engine.MVCCPut(tc.engine, nil, key, roachpb.MinTimestamp, val, nil); err != nil {
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
		}, runUnmarshalCallbackTimeout) == nil
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
	setLeaderLease(t, tc.rng, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	})

	// Expire that lease.
	tc.manualClock.Increment(11 + int64(tc.clock.MaxOffset())) // advance time
	now = tc.clock.Now()

	// Give lease to this range.
	setLeaderLease(t, tc.rng, &roachpb.Lease{
		Start:      now.Add(11, 0),
		Expiration: now.Add(20, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
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
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(rngDesc)

	tc.manualClock.Increment(int64(DefaultLeaderLeaseDuration + 1))
	now := roachpb.Timestamp{WallTime: tc.manualClock.UnixNano()}

	baseRTS, _ := tc.rng.tsCache.GetMax(roachpb.Key("a"), nil /* end */, nil /* txn */)
	baseLowWater := baseRTS.WallTime

	testCases := []struct {
		storeID     roachpb.StoreID
		start       roachpb.Timestamp
		expiration  roachpb.Timestamp
		expLowWater int64
	}{
		// Grant the lease fresh.
		{tc.store.StoreID(), now, now.Add(10, 0), baseLowWater},
		// Renew the lease.
		{tc.store.StoreID(), now.Add(15, 0), now.Add(30, 0), baseLowWater},
		// Renew the lease but shorten expiration.
		{tc.store.StoreID(), now.Add(16, 0), now.Add(25, 0), baseLowWater},
		// Lease is held by another.
		{tc.store.StoreID() + 1, now.Add(29, 0), now.Add(50, 0), baseLowWater},
		// Lease is regranted to this replica.
		{tc.store.StoreID(), now.Add(60, 0), now.Add(70, 0), now.Add(50, 0).WallTime + int64(maxClockOffset) + baseLowWater},
	}

	for i, test := range testCases {
		setLeaderLease(t, tc.rng, &roachpb.Lease{
			Start:      test.start,
			Expiration: test.expiration,
			Replica: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(test.storeID),
				NodeID:    roachpb.NodeID(test.storeID),
				StoreID:   test.storeID,
			},
		})
		// Verify expected low water mark.
		rTS, wTS := tc.rng.tsCache.GetMax(roachpb.Key("a"), nil, nil)
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
	lease := &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}
	ba := roachpb.BatchRequest{}
	ba.Add(&roachpb.LeaderLeaseRequest{Lease: *lease})
	pendingCmd, err := tc.rng.proposeRaftCommand(tc.rng.context(), ba)
	if err == nil {
		// Next if the command was committed, wait for the range to apply it.
		err = (<-pendingCmd.done).Err
	}
	if !testutils.IsError(err, "replica not found") {
		t.Errorf("unexpected error obtaining lease for invalid store: %v", err)
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
			var rangeDesc roachpb.RangeDescriptor
			if err := proto.Unmarshal(bytes, &rangeDesc); err != nil {
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

	txn := newTransaction("test", key, 1 /* userPriority */, roachpb.SERIALIZABLE, tc.clock)
	h := roachpb.Header{Txn: txn}
	bt, _ := beginTxnArgs(key, txn)
	req1 := putArgs(key, []byte("foo"))
	req2, _ := endTxnArgs(txn, true /* commit */)
	req2.IntentSpans = []roachpb.Span{{Key: key}}
	req3 := getArgs(key)

	testCases := []struct {
		req roachpb.Request
		h   roachpb.Header
	}{
		{&bt, h},
		{&req1, h},
		{&req2, h},
		{&req3, roachpb.Header{}},
	}

	for i, test := range testCases {
		txn.Sequence++
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), test.h, test.req); err != nil {
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

	txn := newTransaction("test", key, 1 /* userPriority */, roachpb.SERIALIZABLE, tc.clock)
	bt, h := beginTxnArgs(key, txn)
	if _, err := client.SendWrappedWith(tc.Sender(), nil, h, &bt); err != nil {
		t.Fatal(err)
	}
	req1 := putArgs(key, nil)
	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), nil, roachpb.Header{
		Txn: txn,
	}, &req1); err != nil {
		t.Fatal(err)
	}
	req2, h := endTxnArgs(txn, true /* commit */)
	req2.IntentSpans = []roachpb.Span{{Key: key}}
	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), nil, h, &req2); err != nil {
		t.Fatal(err)
	}
	// Execute a get to resolve the intent.
	req3 := getArgs(key)
	if _, err := client.SendWrappedWith(tc.Sender(), nil, roachpb.Header{Timestamp: txn.Timestamp}, &req3); err != nil {
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
func getArgs(key []byte) roachpb.GetRequest {
	return roachpb.GetRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

// putArgs returns a PutRequest and PutResponse pair addressed to
// the default replica for the specified key / value.
func putArgs(key roachpb.Key, value []byte) roachpb.PutRequest {
	return roachpb.PutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

// deleteArgs returns a DeleteRequest and DeleteResponse pair.
func deleteArgs(key roachpb.Key) roachpb.DeleteRequest {
	return roachpb.DeleteRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

// readOrWriteArgs returns either get or put arguments depending on
// value of "read". Get for true; Put for false. Returns method
// selected and args & reply.
func readOrWriteArgs(key roachpb.Key, read bool) roachpb.Request {
	if read {
		gArgs := getArgs(key)
		return &gArgs
	}
	pArgs := putArgs(key, []byte("value"))
	return &pArgs
}

// incrementArgs returns an IncrementRequest and IncrementResponse pair
// addressed to the default replica for the specified key / value.
func incrementArgs(key []byte, inc int64) roachpb.IncrementRequest {
	return roachpb.IncrementRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Increment: inc,
	}
}

func scanArgs(start, end []byte) roachpb.ScanRequest {
	return roachpb.ScanRequest{
		Span: roachpb.Span{
			Key:    start,
			EndKey: end,
		},
	}
}

func beginTxnArgs(key []byte, txn *roachpb.Transaction) (_ roachpb.BeginTransactionRequest, h roachpb.Header) {
	h.Txn = txn
	return roachpb.BeginTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}, h
}

// endTxnArgs returns a request and header for an EndTransaction RPC for the
// specified key.
func endTxnArgs(txn *roachpb.Transaction, commit bool) (_ roachpb.EndTransactionRequest, h roachpb.Header) {
	h.Txn = txn
	return roachpb.EndTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key, // not allowed when going through TxnCoordSender, but we're not
		},
		Commit: commit,
	}, h
}

// endTxnArgs returns a request and header for a PushTxn RPC for the
// specified key.
func pushTxnArgs(pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType) roachpb.PushTxnRequest {
	return roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: pushee.Key,
		},
		Now:       pusher.Timestamp,
		PushTo:    pusher.Timestamp,
		PusherTxn: *pusher,
		PusheeTxn: *pushee,
		PushType:  pushType,
	}
}

// heartbeatArgs returns request/response pair for HeartbeatTxn RPC.
func heartbeatArgs(txn *roachpb.Transaction) (_ roachpb.HeartbeatTxnRequest, h roachpb.Header) {
	h.Txn = txn
	return roachpb.HeartbeatTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}, h
}

// internalMergeArgs returns a MergeRequest and MergeResponse
// pair addressed to the default replica for the specified key. The request will
// contain the given roachpb.Value.
func internalMergeArgs(key []byte, value roachpb.Value) roachpb.MergeRequest {
	return roachpb.MergeRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value: value,
	}
}

func truncateLogArgs(index uint64, rangeID roachpb.RangeID) roachpb.TruncateLogRequest {
	return roachpb.TruncateLogRequest{
		Index:   index,
		RangeID: rangeID,
	}
}

// TestAcquireLeaderLease verifies that the leader lease is acquired
// for read and write methods.
func TestAcquireLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)

	gArgs := getArgs([]byte("a"))
	pArgs := putArgs([]byte("b"), []byte("1"))

	testCases := []roachpb.Request{&gArgs, &pArgs}

	for i, test := range testCases {
		tc := testContext{}
		tc.Start(t)
		// This is a single-replica test; since we're automatically pushing back
		// the start of a lease as far as possible, and since there is an auto-
		// matic lease for us at the beginning, we'll basically create a lease from
		// then on.
		expStart := tc.rng.getLease().Expiration
		tc.manualClock.Set(int64(DefaultLeaderLeaseDuration + 1000))

		ts := tc.clock.Now()
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: ts}, test); err != nil {
			t.Fatal(err)
		}
		if held, expired := hasLease(tc.rng, ts); !held || expired {
			t.Fatalf("%d: expected lease acquisition", i)
		}
		lease := tc.rng.getLease()
		// The lease may start earlier than our request timestamp, but the
		// expiration will still be measured relative to it.
		expExpiration := ts.Add(int64(DefaultLeaderLeaseDuration), 0)
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
	gArgs := getArgs([]byte("a"))
	ts := tc.clock.Now()

	_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: ts}, &gArgs)

	if err != nil {
		t.Error(err)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	tc.manualClock.Set(t1.Nanoseconds())
	pArgs := putArgs([]byte("b"), []byte("1"))
	ts = tc.clock.Now()

	_, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: ts}, &pArgs)

	if err != nil {
		t.Error(err)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	rTS, wTS := tc.rng.tsCache.GetMax(roachpb.Key("a"), nil, nil)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != 0 {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s", rTS, wTS)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, wTS = tc.rng.tsCache.GetMax(roachpb.Key("b"), nil, nil)
	if rTS.WallTime != 0 || wTS.WallTime != t1.Nanoseconds() {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s", rTS, wTS)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, wTS = tc.rng.tsCache.GetMax(roachpb.Key("c"), nil, nil)
	if rTS.WallTime != 0 || wTS.WallTime != 0 {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s", rTS, wTS)
	}
}

// TestRangeCommandQueue verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestRangeCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)
	// Intercept commands with matching command IDs and block them.
	blockingStart := make(chan struct{})
	blockingDone := make(chan struct{})
	defer func() { TestingCommandFilter = nil }()
	TestingCommandFilter = func(_ roachpb.StoreID, _ roachpb.Request, h roachpb.Header) error {
		if h.GetUserPriority() == 42 {
			blockingStart <- struct{}{}
			<-blockingDone
		}
		return nil
	}

	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	defer close(blockingDone) // make sure teardown can happen
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
		key1 := roachpb.Key(fmt.Sprintf("key1-%d", i))
		key2 := roachpb.Key(fmt.Sprintf("key2-%d", i))
		// Asynchronously put a value to the rng with blocking enabled.
		cmd1Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key1, test.cmd1Read)

			_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
				UserPriority: proto.Int32(42),
			}, args)

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
			args := readOrWriteArgs(key1, test.cmd2Read)

			_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), args)

			if err != nil {
				t.Fatalf("test %d: %s", i, err)
			}
			close(cmd2Done)
		})

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key2, true)

			_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), args)

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
	key := roachpb.Key("key1")
	blockingStart := make(chan struct{})
	blockingDone := make(chan struct{})
	TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if put, ok := args.(*roachpb.PutRequest); ok {
			putBytes, err := put.Value.GetBytes()
			if err != nil {
				return err
			}
			if bytes.Equal(put.Key, key) && bytes.Equal(putBytes, []byte{1}) {
				blockingStart <- struct{}{}
				<-blockingDone
			}
		}

		return nil
	}

	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	cmd1Done := make(chan struct{})
	go func() {
		args := putArgs(key, []byte{1})

		_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)

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
		args := getArgs(key)

		_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &args)

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
	args := getArgs([]byte("a"))

	_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)

	if err != nil {
		t.Error(err)
	}
	pArgs := putArgs([]byte("a"), []byte("value"))

	reply, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &pArgs)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*roachpb.PutResponse)
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
	args := getArgs([]byte("a"))
	ts := tc.clock.Now()

	_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Timestamp:       ts,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &args)

	if err != nil {
		t.Error(err)
	}
	pArgs := putArgs([]byte("a"), []byte("value"))

	reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: roachpb.ZeroTimestamp.Add(0, 1)}, &pArgs)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*roachpb.PutResponse)
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
		key := roachpb.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		pArgs := putArgs(key, []byte("value"))
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)

		reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			Txn: txn,
		}, &pArgs)
		if err != nil {
			t.Fatalf("test %d: %s", i, err)
		}
		pReply := reply.(*roachpb.PutResponse)

		// Now attempt read or write.
		args := readOrWriteArgs(key, read)
		ts := tc.clock.Now() // later timestamp

		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			Timestamp: ts,
		}, args); err == nil {
			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		txn.Sequence++
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			Txn: txn,
		}, &pArgs); err != nil {
			t.Fatalf("test %d: %s", i, err)
		}
		if !pReply.Timestamp.Equal(txn.Timestamp) {
			t.Errorf("expected timestamp not to advance %s != %s", pReply.Timestamp, txn.Timestamp)
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
	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)

	// Start with a read to warm the timestamp cache.
	gArgs := getArgs(key)

	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Txn: txn,
	}, &gArgs); err != nil {
		t.Fatal(err)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs := putArgs(key, []byte("value"))

	txn.Sequence++
	reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Txn: txn,
	}, &pArgs)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*roachpb.PutResponse)
	if !pReply.Timestamp.Equal(txn.Timestamp) {
		t.Errorf("expected timestamp to remain %s; got %s", txn.Timestamp, pReply.Timestamp)
	}

	// Resolve the intent.
	rArgs := &roachpb.ResolveIntentRequest{
		Span:      *pArgs.Header(),
		IntentTxn: *txn,
	}
	txn.Sequence++
	rArgs.IntentTxn.Status = roachpb.COMMITTED
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Txn: txn, Timestamp: txn.Timestamp}, rArgs); err != nil {
		t.Fatal(err)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	ts := txn.Timestamp
	expTS := ts
	expTS.Logical++

	if reply, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: ts}, &pArgs); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	pReply = reply.(*roachpb.PutResponse)
	if !pReply.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, pReply.Timestamp)
	}
}

// TestRangeSequenceCacheReadError verifies that an error is returned to the
// client in the event that a sequence cache entry is found but is not
// decodable.
func TestRangeSequenceCacheReadError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	k := []byte("a")
	txn := newTransaction("test", k, 10, roachpb.SERIALIZABLE, tc.clock)
	args := incrementArgs(k, 1)
	txn.Sequence = 1

	if _, pErr := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Txn: txn,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite sequence cache entry with garbage for the last op.
	key := keys.SequenceCacheKey(tc.rng.Desc().RangeID, txn.ID, uint32(txn.Epoch), txn.Sequence)
	// Make garbageKey sort before key (we've chosen Sequence=1 above,
	// the last byte of which isn't \x00); add an extra byte of garbage.
	garbageKey := append(roachpb.Key(nil), key[:len(key)-1]...)
	garbageKey = append(garbageKey, '\x00', '!')
	err := engine.MVCCPut(tc.engine, nil, garbageKey, roachpb.ZeroTimestamp, roachpb.MakeValueFromString("never read in this test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now try increment again and verify error.
	_, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Txn: txn,
	}, &args)
	if !testutils.IsError(err, "replica corruption") {
		t.Fatal(err)
	}
}

// TestRangeSequenceCacheStoredError verifies that if a cached entry is present,
// a transaction restart error is returned.
func TestRangeSequenceCacheStoredTxnRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	for i, pastError := range []error{errors.New("boom"), nil} {
		txn := newTransaction("test", key, 10, roachpb.SERIALIZABLE, tc.clock)
		txn.Sequence = uint32(1 + i)
		_ = tc.rng.sequence.Put(tc.engine, txn.ID, uint32(txn.Epoch), txn.Sequence, txn.Key, txn.Timestamp, pastError)

		args := incrementArgs(key, 1)
		_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			Txn: txn,
		}, &args)
		if _, ok := err.(*roachpb.TransactionRetryError); !ok {
			t.Fatalf("%d: unexpected error %v", i, err)
		}
	}

	// Try the same again, this time verifying that the Put will actually
	// populate the cache appropriately.
	txn := newTransaction("test", key, 10, roachpb.SERIALIZABLE, tc.clock)
	txn.Sequence = 321
	args := incrementArgs(key, 1)
	try := func() error {
		_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			Txn: txn,
		}, &args)
		return err
	}

	if err := try(); err != nil {
		t.Fatal(err)
	}
	txn.Timestamp.Forward(txn.Timestamp.Add(10, 10)) // can't hurt
	{
		err := try()
		if _, ok := err.(*roachpb.TransactionRetryError); !ok {
			t.Fatal(err)
		}
	}

	//  Pretend we restarted by increasing the epoch. That's all that's needed.
	txn.Epoch++
	if err := try(); err != nil {
		t.Fatal(err)
	}

	// Now increase the sequence as well. Still good to go.
	txn.Sequence++
	if err := try(); err != nil {
		t.Fatal(err)
	}
}

// TestEndTransactionDeadline verifies that EndTransaction respects the
// transaction deadline.
func TestEndTransactionDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := newTransaction("txn: "+strconv.Itoa(i), key, 1, roachpb.SERIALIZABLE, tc.clock)

		btArgs, btHeader := beginTxnArgs(key, txn)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btHeader, &btArgs); err != nil {
			t.Fatal(err)
		}

		etArgs, etHeader := endTxnArgs(txn, true /* commit */)
		switch i {
		case 0:
			// No deadline.
		case 1:
			// Past deadline.
			ts := txn.Timestamp.Prev()
			etArgs.Deadline = &ts
		case 2:
			// Equal deadline.
			etArgs.Deadline = &txn.Timestamp
		case 3:
			// Future deadline.
			ts := txn.Timestamp.Next()
			etArgs.Deadline = &ts
		}

		{
			txn.Sequence++
			_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), etHeader, &etArgs)
			switch i {
			case 0:
				// No deadline.
				if err != nil {
					t.Error(err)
				}
			case 1:
				// Past deadline.
				if _, ok := err.(*roachpb.TransactionAbortedError); !ok {
					t.Errorf("expected TransactionAbortedError but got %T: %s", err, err)
				}
			case 2:
				// Equal deadline.
				if err != nil {
					t.Error(err)
				}
			case 3:
				// Future deadline.
				if err != nil {
					t.Error(err)
				}
			}
		}
	}
}

// TestEndTransactionWithMalformedSplitTrigger verifies an
// EndTransaction call with a malformed commit trigger fails.
func TestEndTransactionWithMalformedSplitTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("foo")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	bt, h := beginTxnArgs(key, txn)
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &bt); err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs(key, []byte("only here to make this a rw transaction"))
	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Txn: txn,
	}, &pArgs); err != nil {
		t.Fatal(err)
	}

	args, h := endTxnArgs(txn, true /* commit */)
	// Make an EndTransaction request which would fail if not
	// stripped. In this case, we set the start key to "bar" for a
	// split of the default range; start key must be "" in this case.
	args.InternalCommitTrigger = &roachpb.InternalCommitTrigger{
		SplitTrigger: &roachpb.SplitTrigger{
			UpdatedDesc: roachpb.RangeDescriptor{StartKey: roachpb.RKey("bar")},
		},
	}

	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args); !testutils.IsError(err, "range does not match splits") {
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
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		bt, btH := beginTxnArgs(key, txn)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}
		txn.Sequence++
		args, h := endTxnArgs(txn, commit)
		resp, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args)
		if err != nil {
			t.Error(err)
		}
		reply := resp.(*roachpb.EndTransactionResponse)
		expStatus := roachpb.COMMITTED
		if !commit {
			expStatus = roachpb.ABORTED
		}
		if reply.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
		}

		// Try a heartbeat to the already-committed transaction; should get
		// committed txn back, but without last heartbeat timestamp set.
		txn.Sequence++
		hBA, h := heartbeatArgs(txn)

		resp, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &hBA)
		if err != nil {
			t.Error(err)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
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
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		bt, btH := beginTxnArgs(key, txn)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}

		// Start out with a heartbeat to the transaction.
		hBA, h := heartbeatArgs(txn)
		txn.Sequence++

		resp, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &hBA)
		if err != nil {
			t.Fatal(err)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != roachpb.PENDING || hBR.Txn.LastHeartbeat == nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hBR)
		}

		args, h := endTxnArgs(txn, commit)
		txn.Sequence++

		resp, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args)
		if err != nil {
			t.Error(err)
		}
		reply := resp.(*roachpb.EndTransactionResponse)
		expStatus := roachpb.COMMITTED
		if !commit {
			expStatus = roachpb.ABORTED
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
		isolation roachpb.IsolationType
		expErr    bool
	}{
		{true, roachpb.SERIALIZABLE, true},
		{true, roachpb.SNAPSHOT, false},
		{false, roachpb.SERIALIZABLE, false},
		{false, roachpb.SNAPSHOT, false},
	}
	key := []byte("a")
	for _, test := range testCases {
		txn := newTransaction("test", key, 1, test.isolation, tc.clock)
		bt, btH := beginTxnArgs(key, txn)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}
		// End the transaction with args timestamp moved forward in time.
		args, h := endTxnArgs(txn, test.commit)
		// TODO(tschottdorf): this test is pretty dirty. It should really
		// write a txn entry and then try to commit that; the way it works
		// now is by supplying its txn (which has its own timestamp) at
		// another timestamp. This constrains changes we want to make on
		// how timestamps work.
		tc.manualClock.Set(1)
		ts := tc.clock.Now()
		h.Timestamp = ts
		txn.Sequence++

		resp, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args)

		if test.expErr {
			if err == nil {
				t.Errorf("expected error")
			}
			if _, ok := err.(*roachpb.TransactionRetryError); !ok {
				t.Errorf("expected retry error; got %s", err)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}
			expStatus := roachpb.COMMITTED
			if !test.commit {
				expStatus = roachpb.ABORTED
			}
			reply := resp.(*roachpb.EndTransactionResponse)
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
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	bt, btH := beginTxnArgs(key, txn)
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
		t.Fatal(err)
	}

	// Start out with a heartbeat to the transaction.
	hBA, h := heartbeatArgs(txn)
	txn.Sequence++

	_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &hBA)
	if err != nil {
		t.Error(err)
	}

	// Now end the txn with increased epoch and priority.
	args, h := endTxnArgs(txn, true)
	h.Txn.Epoch = txn.Epoch + 1
	h.Txn.Priority = txn.Priority + 1

	txn.Sequence++
	resp, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args)
	if err != nil {
		t.Error(err)
	}
	reply := resp.(*roachpb.EndTransactionResponse)
	if reply.Txn.Status != roachpb.COMMITTED {
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
	txn := newTransaction("test", roachpb.Key(""), 1, roachpb.SERIALIZABLE, tc.clock)

	testCases := []struct {
		key          roachpb.Key
		existStatus  roachpb.TransactionStatus
		existEpoch   uint32
		existTS      roachpb.Timestamp
		expErrRegexp string
	}{
		{roachpb.Key("a"), roachpb.COMMITTED, txn.Epoch, txn.Timestamp, "txn \"test\" id=.*: already committed"},
		{roachpb.Key("b"), roachpb.ABORTED, txn.Epoch, txn.Timestamp, "txn aborted \"test\" id=.*"},
		{roachpb.Key("c"), roachpb.PENDING, txn.Epoch + 1, txn.Timestamp, "txn \"test\" id=.*: epoch regression: 0"},
		{roachpb.Key("d"), roachpb.PENDING, txn.Epoch, regressTS, `txn "test" id=.*: timestamp regression: 0.000000001,\d+`},
	}
	for _, test := range testCases {
		// Establish existing txn state by writing directly to range engine.
		var existTxn roachpb.Transaction
		proto.Merge(&existTxn, txn)
		existTxn.Key = test.key
		existTxn.Status = test.existStatus
		existTxn.Epoch = test.existEpoch
		existTxn.Timestamp = test.existTS
		txnKey := keys.TransactionKey(test.key, txn.ID)
		if err := engine.MVCCPutProto(tc.rng.store.Engine(), nil, txnKey, roachpb.ZeroTimestamp,
			nil, &existTxn); err != nil {
			t.Fatal(err)
		}

		// End the transaction, verify expected error.
		txn.Key = test.key
		args, h := endTxnArgs(txn, true)
		txn.Sequence++

		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args); !testutils.IsError(err, test.expErrRegexp) {
			t.Errorf("expected error:\n%s\nto match:\n%s", err, test.expErrRegexp)
		}
	}
}

// TestEndTransactionGC verifies that a transaction record is immediately
// garbage-collected upon EndTransaction iff all of the supplied intents are
// local relative to the transaction record's location.
func TestEndTransactionLocalGC(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer setTxnAutoGC(true)()
	defer func() { TestingCommandFilter = nil }()
	TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		// Make sure the direct GC path doesn't interfere with this test.
		if args.Method() == roachpb.GC {
			return util.Errorf("boom")
		}
		return nil
	}
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	splitKey := roachpb.RKey("c")
	splitTestRange(tc.store, splitKey, splitKey, t)
	for i, test := range []struct {
		intents []roachpb.Span
		expGC   bool
	}{
		// Range inside.
		{[]roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}}, true},
		// Two intents inside.
		{[]roachpb.Span{{Key: roachpb.Key("a")}, {Key: roachpb.Key("b")}}, true},
		// Intent range spilling over right endpoint.
		{[]roachpb.Span{{Key: roachpb.Key("a"), EndKey: splitKey.Next().AsRawKey()}}, false},
		// Intent range completely outside.
		{[]roachpb.Span{{Key: splitKey.AsRawKey(), EndKey: roachpb.Key("q")}}, false},
		// Intent inside and outside.
		{[]roachpb.Span{{Key: roachpb.Key("a")}, {Key: splitKey.AsRawKey()}}, false},
	} {
		key := roachpb.Key("a")
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		bt, btH := beginTxnArgs(key, txn)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}
		args, h := endTxnArgs(txn, true)
		args.IntentSpans = test.intents
		txn.Sequence++
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args); err != nil {
			t.Fatal(err)
		}
		var readTxn roachpb.Transaction
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		ok, err := engine.MVCCGetProto(tc.rng.store.Engine(), txnKey, roachpb.ZeroTimestamp,
			true /* consistent */, nil /* txn */, &readTxn)
		if err != nil {
			t.Fatal(err)
		}
		if !ok != test.expGC {
			t.Errorf("%d: unexpected gc'ed: %t", i, !ok)
		}
	}
}

func setupResolutionTest(t *testing.T, tc testContext, key roachpb.Key, splitKey roachpb.RKey) (*Replica, *roachpb.Transaction) {
	// Split the range and create an intent in each range.
	// The keys of the two intents are next to each other.
	newRng := splitTestRange(tc.store, splitKey, splitKey, t)

	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	bt, btH := beginTxnArgs(key, txn)
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs(key, []byte("value"))
	h := roachpb.Header{Txn: txn}
	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &pArgs); err != nil {
		t.Fatal(err)
	}

	pArgs = putArgs(splitKey.AsRawKey(), []byte("value"))
	txn.Sequence++
	if _, err := client.SendWrappedWith(newRng, newRng.context(), h, &pArgs); err != nil {
		t.Fatal(err)
	}

	// End the transaction and resolve the intents.
	args, h := endTxnArgs(txn, true /* commit */)
	args.IntentSpans = []roachpb.Span{{Key: key, EndKey: splitKey.Next().AsRawKey()}}
	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &args); err != nil {
		t.Fatal(err)
	}
	return newRng, txn
}

// TestEndTransactionResolveOnlyLocalIntents verifies that an end transaction
// request resolves only local intents within the same batch.
func TestEndTransactionResolveOnlyLocalIntents(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	defer func() { TestingCommandFilter = nil }()
	TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if args.Method() == roachpb.ResolveIntentRange && args.Header().Key.Equal(splitKey.AsRawKey()) {
			return util.Errorf("boom")
		}
		return nil
	}
	tc.Start(t)
	defer tc.Stop()

	newRng, txn := setupResolutionTest(t, tc, key, splitKey)

	// Check if the intent in the other range has not yet been resolved.
	gArgs := getArgs(splitKey)
	_, err := client.SendWrapped(newRng, newRng.context(), &gArgs)
	if _, ok := err.(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected write intent error, but got %s", err)
	}

	txn.Sequence++
	hbArgs, h := heartbeatArgs(txn)
	reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &hbArgs)
	if err != nil {
		t.Fatal(err)
	}
	hbResp := reply.(*roachpb.HeartbeatTxnResponse)
	expIntents := []roachpb.Span{{Key: splitKey.AsRawKey(), EndKey: splitKey.AsRawKey().Next()}}
	if !reflect.DeepEqual(hbResp.Txn.Intents, expIntents) {
		t.Fatalf("expected persisted intents %v, got %v",
			expIntents, hbResp.Txn.Intents)
	}
}

// TestEndTransactionDirectGC verifies that after successfully resolving the
// external intents of a transaction after EndTransaction, the transaction and
// sequence cache records are purged.
func TestEndTransactionDirectGC(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tc.Start(t)
	defer tc.Stop()

	_, txn := setupResolutionTest(t, tc, key, splitKey)

	util.SucceedsWithin(t, 5*time.Second, func() error {
		if gr, _, err := tc.rng.Get(tc.engine, roachpb.Header{}, roachpb.GetRequest{Span: roachpb.Span{Key: keys.TransactionKey(txn.Key, txn.ID)}}); err != nil {
			return err
		} else if gr.Value != nil {
			return util.Errorf("txn entry still there: %+v", gr)
		}

		var entry roachpb.SequenceCacheEntry
		if seq, _, err := tc.rng.sequence.Get(tc.engine, txn.ID, &entry); err != nil {
			return err
		} else if seq > 0 {
			return util.Errorf("sequence cache still populated: %v", entry)
		}

		return nil
	})
}

// TestEndTransactionDirectGCFailure verifies that no immediate GC takes place
// if external intents can't be resolved (see also TestEndTransactionDirectGC).
func TestEndTransactionDirectGCFailure(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	var count int64
	defer func() { TestingCommandFilter = nil }()
	TestingCommandFilter = func(_ roachpb.StoreID, args roachpb.Request, _ roachpb.Header) error {
		if args.Method() == roachpb.ResolveIntentRange && args.Header().Key.Equal(splitKey.AsRawKey()) {
			atomic.AddInt64(&count, 1)
			return util.Errorf("boom")
		} else if args.Method() == roachpb.GC {
			t.Fatalf("unexpected GCRequest: %+v", args)
		}
		return nil
	}
	tc.Start(t)
	defer tc.Stop()

	setupResolutionTest(t, tc, key, splitKey)

	// Now test that no GCRequest is issued. We can't test that directly (since
	// it's completely asynchronous), so we first make sure ResolveIntent
	// happened and subsequently issue a bogus Put which is likely to make it
	// into Raft only after a rogue GCRequest (at least sporadically), which
	// would trigger a Fatal from the command filter.
	util.SucceedsWithin(t, 5*time.Second, func() error {
		if atomic.LoadInt64(&count) == 0 {
			return util.Errorf("intent resolution not attempted yet")
		} else if err := tc.store.DB().Put("panama", "banana"); err != nil {
			return err
		}
		return nil
	})
}

// TestSequenceCachePoisonOnResolve verifies that when an intent is pushed into
// the future or aborted, the sequence cache on the respective Range is
// poisoned and the pushee is presented with a txn retry or abort on its next
// contact with the Range in the same epoch.
func TestSequenceCachePoisonOnResolve(t *testing.T) {
	defer leaktest.AfterTest(t)
	key := roachpb.Key("a")

	// Isolation of the pushee and whether we're going to abort it.
	// Run the actual meat of the test, which pushes the pushee and
	// checks whether we get the correct behaviour as it touches the
	// Range again.
	run := func(abort bool, iso roachpb.IsolationType) {
		tc := testContext{}
		tc.Start(t)
		defer tc.Stop()

		pushee := newTransaction("test", key, 1, iso, tc.clock)
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pusher.Priority = 2
		pushee.Priority = 1 // pusher will win

		inc := func(actor *roachpb.Transaction, k roachpb.Key) (*roachpb.IncrementResponse, error) {
			actor.Sequence++
			reply, err := client.SendWrappedWith(tc.store, nil, roachpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, &roachpb.IncrementRequest{Span: roachpb.Span{Key: k}, Increment: 123})
			if err != nil {
				return nil, err
			}
			return reply.(*roachpb.IncrementResponse), err
		}

		get := func(actor *roachpb.Transaction, k roachpb.Key) error {
			actor.Sequence++
			_, err := client.SendWrappedWith(tc.store, nil, roachpb.Header{
				Txn:       actor,
				Timestamp: actor.Timestamp,
				RangeID:   1,
			}, &roachpb.GetRequest{Span: roachpb.Span{Key: k}})
			return err
		}

		// Begin the pushee's transaction and write an intent.
		btArgs, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(tc.Sender(), nil,
			btH, &btArgs); err != nil {
			t.Fatal(err)
		}
		if _, err := inc(pushee, key); err != nil {
			t.Fatal(err)
		}

		// Have the pusher run into the intent. That pushes our pushee and resolves
		// the intent, which in turn should poison the sequence cache.
		var assert func(error)
		if abort {
			// Write/Write conflict will abort pushee.
			if _, err := inc(pusher, key); err != nil {
				t.Fatal(err)
			}
			assert = func(err error) {
				if _, ok := err.(*roachpb.TransactionAbortedError); !ok {
					t.Fatalf("abort=%t, iso=%s: expected txn abort, got %s", abort, iso, err)
				}
			}
		} else if iso == roachpb.SNAPSHOT {
			// At SNAPSHOT, we shouldn't be restart-poisoned.
			assert = func(err error) {
				if err != nil {
					t.Fatalf("abort=%t, iso=%s: unexpected: %s", abort, iso, err)
				}
			}
		} else {
			// Trigger a Read/Write conflict which pushes pushee's timestamp.
			if err := get(pusher, key); err != nil {
				t.Fatal(err)
			}
			assert = func(err error) {
				if _, ok := err.(*roachpb.TransactionRetryError); !ok {
					t.Fatalf("abort=%t, iso=%s: expected txn retry, got %s",
						abort, iso, err)
				}
			}
		}

		// We shouldn't be able to read or write within the transaction on this
		// Range.
		err := get(pushee, key)
		assert(err)
		_, err = inc(pushee, key)
		assert(err)
		// Still poisoned (on any key on the Range).
		err = get(pushee, key.Next())
		assert(err)
		_, err = inc(pushee, key.Next())
		assert(err)

		// Pretend we're coming back. This works regardless of retry or restart,
		// but obviously in practice only on a retry would the Txn actually come
		// back with an increased epoch.
		pushee.Epoch++
		if _, err := inc(pushee, roachpb.Key("b")); err != nil {
			t.Fatal(err)
		}
	}

	for _, abort := range []bool{false, true} {
		run(abort, roachpb.SERIALIZABLE)
		run(abort, roachpb.SNAPSHOT)
	}
}

// TestPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestPushTxnBadKey(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, roachpb.SERIALIZABLE, tc.clock)

	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)
	args.Key = pusher.Key

	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args); !testutils.IsError(err, ".*should match pushee.*") {
		t.Errorf("unexpected error %s", err)
	}
}

// TestPushTxnAlreadyCommittedOrAborted verifies success
// (noop) in event that pushee is already committed or aborted.
func TestPushTxnAlreadyCommittedOrAborted(t *testing.T) {
	defer leaktest.AfterTest(t)
	// This test simulates running into an open intent and resolving it using
	// the transaction record. If we auto-gc'ed entries here, the entry would
	// be deleted and the intents resolved instantaneously on successful commit
	// (since they're on the same Range). Could split the range and have
	// non-local intents if we ever wanted to get rid of this.
	defer setTxnAutoGC(false)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	for i, status := range []roachpb.TransactionStatus{roachpb.COMMITTED, roachpb.ABORTED} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pusher.Priority = 1
		pushee.Priority = 2 // pusher will lose, meaning we shouldn't push unless pushee is already ended.

		// Begin the pushee's transaction.
		btArgs, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &btArgs); err != nil {
			t.Fatal(err)
		}
		// End the pushee's transaction.
		etArgs, h := endTxnArgs(pushee, status == roachpb.COMMITTED)
		pushee.Sequence++
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &etArgs); err != nil {
			t.Fatal(err)
		}

		// Now try to push what's already committed or aborted.
		args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)
		resp, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)
		if err != nil {
			t.Fatal(err)
		}
		reply := resp.(*roachpb.PushTxnResponse)
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

	ts1 := roachpb.Timestamp{WallTime: 1}
	ts2 := roachpb.Timestamp{WallTime: 2}
	testCases := []struct {
		startEpoch, epoch, expEpoch uint32
		startTS, ts, expTS          roachpb.Timestamp
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
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee.Priority = 1
		pusher.Priority = 2   // Pusher will win
		pusher.Writing = true // expected when a txn is heartbeat

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		pushee.Epoch = test.startEpoch
		pushee.Timestamp = test.startTS
		pushee.LastHeartbeat = &test.startTS
		bt, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction using updated values for epoch & timestamp.
		pushee.Epoch = test.epoch
		pushee.Timestamp = test.ts
		args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)

		resp, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)
		if err != nil {
			t.Fatal(err)
		}
		reply := resp.(*roachpb.PushTxnResponse)
		expTxn := proto.Clone(pushee).(*roachpb.Transaction)
		expTxn.Epoch = test.expEpoch
		expTxn.Timestamp = test.expTS
		expTxn.Status = roachpb.ABORTED
		expTxn.LastHeartbeat = &test.startTS

		if !reflect.DeepEqual(expTxn, &reply.PusheeTxn) {
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

	ts := roachpb.Timestamp{WallTime: 1}
	ns := DefaultHeartbeatInterval.Nanoseconds()
	testCases := []struct {
		heartbeat   roachpb.Timestamp // zero value indicates no heartbeat
		currentTime int64             // nanoseconds
		pushType    roachpb.PushTxnType
		expSuccess  bool
	}{
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_TIMESTAMP, false}, // using 0 as time is awkward
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_TOUCH, false},
		{ts, ns*2 + 1, roachpb.PUSH_TIMESTAMP, false},
		{ts, ns*2 + 1, roachpb.PUSH_ABORT, false},
		{ts, ns*2 + 1, roachpb.PUSH_TOUCH, false},
		{ts, ns*2 + 2, roachpb.PUSH_TIMESTAMP, true},
		{ts, ns*2 + 2, roachpb.PUSH_ABORT, true},
		{ts, ns*2 + 2, roachpb.PUSH_TOUCH, true},
	}

	for i, test := range testCases {
		tc.manualClock.Set(0)
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pushee := newTransaction(fmt.Sprintf("test-%d", i), key, 1, roachpb.SERIALIZABLE, nil /* clock */)
		pusher := newTransaction("pusher", key, 1, roachpb.SERIALIZABLE, nil /* clock */)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		if !test.heartbeat.Equal(roachpb.ZeroTimestamp) {
			pushee.LastHeartbeat = &test.heartbeat
		}
		bt, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}

		// Now, attempt to push the transaction with Now set to our current time.
		args := pushTxnArgs(pusher, pushee, test.pushType)
		args.Now = roachpb.Timestamp{WallTime: test.currentTime}

		reply, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)

		if test.expSuccess != (err == nil) {
			t.Errorf("%d: expSuccess=%t; got err %s", i, test.expSuccess, err)
			continue
		}
		if err != nil {
			if _, ok := err.(*roachpb.TransactionPushError); !ok {
				t.Errorf("%d: expected txn push error: %s", i, err)
			}
		} else if txn := reply.(*roachpb.PushTxnResponse).PusheeTxn; txn.Status != roachpb.ABORTED {
			t.Errorf("%d: expected aborted transaction, got %s", i, txn)
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

	ts1 := roachpb.Timestamp{WallTime: 1}
	ts2 := roachpb.Timestamp{WallTime: 2}
	testCases := []struct {
		pusherPriority, pusheePriority int32
		pusherTS, pusheeTS             roachpb.Timestamp
		pushType                       roachpb.PushTxnType
		expSuccess                     bool
	}{
		// Pusher has higher priority succeeds.
		{2, 1, ts1, ts1, roachpb.PUSH_ABORT, true},
		// Pusher has lower priority fails.
		{1, 2, ts1, ts1, roachpb.PUSH_ABORT, false},
		{1, 2, ts1, ts1, roachpb.PUSH_TIMESTAMP, false},
		// Pusher has lower priority fails, even with older txn timestamp.
		{1, 2, ts1, ts2, roachpb.PUSH_ABORT, false},
		// Pusher has lower priority, but older txn timestamp allows success if !abort.
		{1, 2, ts1, ts2, roachpb.PUSH_TIMESTAMP, true},
		// With same priorities, older txn timestamp succeeds.
		{1, 1, ts1, ts2, roachpb.PUSH_ABORT, true},
		// With same priorities, same txn timestamp fails.
		{1, 1, ts1, ts1, roachpb.PUSH_ABORT, false},
		{1, 1, ts1, ts1, roachpb.PUSH_TIMESTAMP, false},
		// With same priorities, newer txn timestamp fails.
		{1, 1, ts2, ts1, roachpb.PUSH_ABORT, false},
		{1, 1, ts2, ts1, roachpb.PUSH_TIMESTAMP, false},
		// When confirming, priority never wins.
		{2, 1, ts1, ts1, roachpb.PUSH_TOUCH, false},
		{1, 2, ts1, ts1, roachpb.PUSH_TOUCH, false},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pusher.Priority = test.pusherPriority
		pushee.Priority = test.pusheePriority
		pusher.Timestamp = test.pusherTS
		pushee.Timestamp = test.pusheeTS

		bt, btH := beginTxnArgs(key, pushee)
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
			t.Fatal(err)
		}
		// Now, attempt to push the transaction with intent epoch set appropriately.
		args := pushTxnArgs(pusher, pushee, test.pushType)

		_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)

		if test.expSuccess != (err == nil) {
			t.Errorf("expected success on trial %d? %t; got err %s", i, test.expSuccess, err)
		}
		if err != nil {
			if _, ok := err.(*roachpb.TransactionPushError); !ok {
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

	pusher := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, roachpb.SERIALIZABLE, tc.clock)
	pusher.Priority = 2
	pushee.Priority = 1 // pusher will win
	pusher.Timestamp = roachpb.Timestamp{WallTime: 50, Logical: 25}
	pushee.Timestamp = roachpb.Timestamp{WallTime: 5, Logical: 1}

	bt, btH := beginTxnArgs(roachpb.Key("a"), pushee)
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
		t.Fatal(err)
	}

	// Now, push the transaction with args.Abort=false.
	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)

	resp, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)
	if err != nil {
		t.Errorf("unexpected error on push: %s", err)
	}
	expTS := pusher.Timestamp
	expTS.Logical++
	reply := resp.(*roachpb.PushTxnResponse)
	if !reply.PusheeTxn.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to be pushed to %+v; got %+v", expTS, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != roachpb.PENDING {
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

	pusher := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, roachpb.SERIALIZABLE, tc.clock)
	pusher.Priority = 1
	pushee.Priority = 2 // pusher will lose
	pusher.Timestamp = roachpb.Timestamp{WallTime: 50, Logical: 0}
	pushee.Timestamp = roachpb.Timestamp{WallTime: 50, Logical: 1}

	bt, btH := beginTxnArgs(roachpb.Key("a"), pushee)
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), btH, &bt); err != nil {
		t.Fatal(err)
	}

	// Now, push the transaction with args.Abort=false.
	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)

	resp, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)
	if err != nil {
		t.Errorf("unexpected error on push: %s", err)
	}
	reply := resp.(*roachpb.PushTxnResponse)
	if !reply.PusheeTxn.Timestamp.Equal(pushee.Timestamp) {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.Timestamp, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != roachpb.PENDING {
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
	txn := &roachpb.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now(), Sequence: 1}
	for _, key := range []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")} {
		pArgs := putArgs(key, []byte("value1"))
		txn.Sequence++
		if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Txn: txn}, &pArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Resolve the intents.
	rArgs := &roachpb.ResolveIntentRangeRequest{
		Span: roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		},
		IntentTxn: *txn,
	}
	rArgs.IntentTxn.Status = roachpb.COMMITTED
	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}

	// Do a consistent scan to verify intents have been cleared.
	sArgs := scanArgs(roachpb.Key("a"), roachpb.Key("c"))
	reply, err := client.SendWrapped(tc.Sender(), nil, &sArgs)
	if err != nil {
		t.Fatalf("unexpected error on scan: %s", err)
	}
	sReply := reply.(*roachpb.ScanResponse)
	if len(sReply.Rows) != 2 {
		t.Errorf("expected 2 rows; got %v", sReply.Rows)
	}
}

func verifyRangeStats(eng engine.Engine, rangeID roachpb.RangeID, expMS engine.MVCCStats, t *testing.T) {
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(eng, rangeID, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expMS, ms) {
		t.Errorf("expected stats \n  %+v;\ngot \n  %+v", expMS, ms)
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
	pArgs := putArgs([]byte("a"), []byte("value1"))

	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}
	expMS := engine.MVCCStats{LiveBytes: 25, KeyBytes: 14, ValBytes: 11, IntentBytes: 0, LiveCount: 1, KeyCount: 1, ValCount: 1, IntentCount: 0, IntentAge: 0, GCBytesAge: 0, SysBytes: 51, SysCount: 1, LastUpdateNanos: 0}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)

	// Put a 2nd value transactionally.
	pArgs = putArgs([]byte("b"), []byte("value2"))
	txn := &roachpb.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now(), Sequence: 1}

	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Txn: txn}, &pArgs); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 116, KeyBytes: 28, ValBytes: 88, IntentBytes: 23, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 1, IntentAge: 0, GCBytesAge: 0, SysBytes: 51, SysCount: 1, LastUpdateNanos: 0}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)

	// Resolve the 2nd value.
	rArgs := &roachpb.ResolveIntentRequest{
		Span: roachpb.Span{
			Key: pArgs.Key,
		},
		IntentTxn: *txn,
	}
	rArgs.IntentTxn.Status = roachpb.COMMITTED

	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 50, KeyBytes: 28, ValBytes: 22, IntentBytes: 0, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 0, IntentAge: 0, GCBytesAge: 0, SysBytes: 51, SysCount: 1, LastUpdateNanos: 0}
	verifyRangeStats(tc.engine, tc.rng.Desc().RangeID, expMS, t)

	// Delete the 1st value.
	dArgs := deleteArgs([]byte("a"))

	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &dArgs); err != nil {
		t.Fatal(err)
	}
	expMS = engine.MVCCStats{LiveBytes: 25, KeyBytes: 40, ValBytes: 22, IntentBytes: 0, LiveCount: 1, KeyCount: 2, ValCount: 3, IntentCount: 0, IntentAge: 0, GCBytesAge: 0, SysBytes: 51, SysCount: 1, LastUpdateNanos: 0}
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
		mergeArgs := internalMergeArgs(key, roachpb.MakeValueFromString(str))

		if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &mergeArgs); err != nil {
			t.Fatalf("unexpected error from Merge: %s", err.Error())
		}
	}

	getArgs := getArgs(key)

	reply, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &getArgs)
	if err != nil {
		t.Fatalf("unexpected error from Get: %s", err)
	}
	resp := reply.(*roachpb.GetResponse)
	if resp.Value == nil {
		t.Fatal("GetResponse had nil value")
	}
	a, err := resp.Value.GetBytes()
	if err != nil {
		t.Fatal(err)
	}
	if e := []byte(stringExpected); !bytes.Equal(a, e) {
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
	tc.rng.store.DisableRaftLogQueue(true)

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args); err != nil {
			t.Fatal(err)
		}
		idx, err := tc.rng.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	rangeID := tc.rng.Desc().RangeID

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &truncateArgs); err != nil {
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
	if err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
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
	if err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// Truncating logs that have already been truncated should not return an
	// error.
	truncateArgs = truncateLogArgs(indexes[3], rangeID)
	if _, err = client.SendWrapped(tc.Sender(), tc.rng.context(), &truncateArgs); err != nil {
		t.Fatal(err)
	}

	// Truncating logs that have the wrong rangeID included should not return
	// an error but should not truncate any logs.
	truncateArgs = truncateLogArgs(indexes[9], rangeID+1)
	if _, err = client.SendWrapped(tc.Sender(), tc.rng.context(), &truncateArgs); err != nil {
		t.Fatal(err)
	}

	// The term of the last truncated entry is still available.
	term, err = tc.rng.Term(indexes[4])
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 {
		t.Errorf("invalid term 0 for truncated entry")
	}
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
	pArgs := putArgs(key, value)

	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &pArgs); err != nil {
		t.Fatal(err)
	}
	val := roachpb.MakeValueFromString("moo")
	args := roachpb.ConditionalPutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpValue: &val,
	}

	_, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Timestamp: roachpb.MinTimestamp}, &args)

	if cErr, ok := err.(*roachpb.ConditionFailedError); err == nil || !ok {
		t.Fatalf("expected ConditionFailedError, got %T with content %+v",
			err, err)
	} else if valueBytes, err := cErr.ActualValue.GetBytes(); err != nil {
		t.Fatal(err)
	} else if cErr.ActualValue == nil || !bytes.Equal(valueBytes, value) {
		t.Errorf("ConditionFailedError with bytes %q expected, but got %+v",
			value, cErr.ActualValue)
	}
}

// TestReplicaSetsEqual tests to ensure that intersectReplicaSets
// returns the correct responses.
func TestReplicaSetsEqual(t *testing.T) {
	defer leaktest.AfterTest(t)
	testData := []struct {
		expected bool
		a        []roachpb.ReplicaDescriptor
		b        []roachpb.ReplicaDescriptor
	}{
		{true, []roachpb.ReplicaDescriptor{}, []roachpb.ReplicaDescriptor{}},
		{true, createReplicaSets([]roachpb.StoreID{1}), createReplicaSets([]roachpb.StoreID{1})},
		{true, createReplicaSets([]roachpb.StoreID{1, 2}), createReplicaSets([]roachpb.StoreID{1, 2})},
		{true, createReplicaSets([]roachpb.StoreID{1, 2}), createReplicaSets([]roachpb.StoreID{2, 1})},
		{false, createReplicaSets([]roachpb.StoreID{1}), createReplicaSets([]roachpb.StoreID{2})},
		{false, createReplicaSets([]roachpb.StoreID{1, 2}), createReplicaSets([]roachpb.StoreID{2})},
		{false, createReplicaSets([]roachpb.StoreID{1, 2}), createReplicaSets([]roachpb.StoreID{1})},
		{false, createReplicaSets([]roachpb.StoreID{}), createReplicaSets([]roachpb.StoreID{1})},
		{true, createReplicaSets([]roachpb.StoreID{1, 2, 3}), createReplicaSets([]roachpb.StoreID{2, 3, 1})},
		{true, createReplicaSets([]roachpb.StoreID{1, 1}), createReplicaSets([]roachpb.StoreID{1, 1})},
		{false, createReplicaSets([]roachpb.StoreID{1, 1}), createReplicaSets([]roachpb.StoreID{1, 1, 1})},
		{true, createReplicaSets([]roachpb.StoreID{1, 2, 3, 1, 2, 3}), createReplicaSets([]roachpb.StoreID{1, 1, 2, 2, 3, 3})},
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
		args := incrementArgs([]byte("a"), i)

		resp, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)
		if err != nil {
			t.Fatal(err)
		}
		reply := resp.(*roachpb.IncrementResponse)
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

	args := putArgs(roachpb.Key("test"), []byte("value"))
	_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)
	if err != nil {
		t.Fatal(err)
	}
	// Set the stored applied index sky high.
	newIndex := 2*atomic.LoadUint64(&tc.rng.appliedIndex) + 1
	atomic.StoreUint64(&tc.rng.appliedIndex, newIndex)
	// Not really needed, but let's be thorough.
	err = setAppliedIndex(tc.rng.store.Engine(), tc.rng.Desc().RangeID, newIndex)
	if err != nil {
		t.Fatal(err)
	}
	// Should mark replica corrupt (and panic as a result) since we messed
	// with the applied index.
	_, err = client.SendWrapped(tc.Sender(), tc.rng.context(), &args)

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

	if err := tc.rng.ChangeReplicas(roachpb.ADD_REPLICA, roachpb.ReplicaDescriptor{
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
	// Test RangeLookup with Scan.
	testRangeDanglingMetaIntent(t, false)
	// Test RangeLookup with ReverseScan.
	testRangeDanglingMetaIntent(t, true)
}

func testRangeDanglingMetaIntent(t *testing.T, isReverse bool) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")

	// Get original meta2 descriptor.
	rlArgs := &roachpb.RangeLookupRequest{
		Span: roachpb.Span{
			Key: keys.RangeMetaKey(roachpb.RKey(key)),
		},
		MaxRanges: 1,
		Reverse:   isReverse,
	}

	var rlReply *roachpb.RangeLookupResponse

	reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, rlArgs)
	if err != nil {
		t.Fatal(err)
	}
	rlReply = reply.(*roachpb.RangeLookupResponse)

	origDesc := rlReply.Ranges[0]
	newDesc := origDesc
	newDesc.EndKey = keys.Addr(key)

	// Write the new descriptor as an intent.
	data, err := proto.Marshal(&newDesc)
	if err != nil {
		t.Fatal(err)
	}
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	// Officially begin the transaction. If not for this, the intent resolution
	// machinery would simply remove the intent we write below, see #3020.
	// We send directly to Replica throughout this test, so there's no danger
	// of the Store aborting this transaction (i.e. we don't have to set a high
	// priority).
	bArgs, h := beginTxnArgs(key, txn)
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), h, &bArgs); err != nil {
		t.Fatal(err)
	}

	pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(key)), data)
	txn.Sequence++
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Txn: txn}, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Now lookup the range; should get the value. Since the lookup is
	// inconsistent, there's no WriteIntentError.
	// Note that 'A' < 'a'.
	rlArgs.Key = keys.RangeMetaKey(roachpb.RKey{'A'})

	reply, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		Timestamp:       roachpb.MinTimestamp,
		ReadConsistency: roachpb.INCONSISTENT,
	}, rlArgs)
	if err != nil {
		t.Errorf("unexpected lookup error: %s", err)
	}
	rlReply = reply.(*roachpb.RangeLookupResponse)
	if !reflect.DeepEqual(rlReply.Ranges[0], origDesc) {
		t.Errorf("expected original descriptor %s; got %s", &origDesc, &rlReply.Ranges[0])
	}

	// Switch to consistent lookups, which should run into the intent.
	_, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
		ReadConsistency: roachpb.CONSISTENT,
	}, rlArgs)
	if _, ok := err.(*roachpb.WriteIntentError); !ok {
		t.Fatalf("expected WriteIntentError, not %s", err)
	}

	// Try 100 lookups with IgnoreIntents. Expect to see each descriptor at least once.
	// First, try this consistently, which should not be allowed.
	rlArgs.ConsiderIntents = true
	_, err = client.SendWrapped(tc.Sender(), tc.rng.context(), rlArgs)
	if !testutils.IsError(err, "can not read consistently and special-case intents") {
		t.Fatalf("wanted specific error, not %s", err)
	}
	// After changing back to inconsistent lookups, should be good to go.
	var origSeen, newSeen bool

	for !(origSeen && newSeen) {
		clonedRLArgs := proto.Clone(rlArgs).(*roachpb.RangeLookupRequest)

		reply, err = client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, clonedRLArgs)
		if err != nil {
			t.Fatal(err)
		}
		rlReply = reply.(*roachpb.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if reflect.DeepEqual(seen, origDesc) {
			origSeen = true
		} else if reflect.DeepEqual(seen, newDesc) {
			newSeen = true
		} else {
			t.Errorf("expected orig/new descriptor %s/%s; got %s", &origDesc, &newDesc, &seen)
		}
	}
}

// TestRangeLookupUseReverseScan verifies the correctness of the results which are retrieved
// from RangeLookup by using ReverseScan.
func TestRangeLookupUseReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Test ranges: ["a","c"), ["c","f"), ["f","h") and ["h","y").
	testRanges := []roachpb.RangeDescriptor{
		{RangeID: 2, StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		{RangeID: 3, StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("f")},
		{RangeID: 4, StartKey: roachpb.RKey("f"), EndKey: roachpb.RKey("h")},
		{RangeID: 5, StartKey: roachpb.RKey("h"), EndKey: roachpb.RKey("y")},
	}
	// The range ["f","h") has dangling intent in meta2.
	withIntentRangeIndex := 2

	testCases := []struct {
		key      string
		expected roachpb.RangeDescriptor
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

	txn := &roachpb.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now(), Sequence: 1}
	for i, r := range testRanges {
		if i != withIntentRangeIndex {
			// Write the new descriptor as an intent.
			data, err := proto.Marshal(&r)
			if err != nil {
				t.Fatal(err)
			}
			pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(r.EndKey)), data)

			txn.Sequence++
			if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Txn: txn}, &pArgs); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Resolve the intents.
	rArgs := &roachpb.ResolveIntentRangeRequest{
		Span: roachpb.Span{
			Key:    keys.RangeMetaKey(roachpb.RKey("a")),
			EndKey: keys.RangeMetaKey(roachpb.RKey("z")),
		},
		IntentTxn: *txn,
	}
	rArgs.IntentTxn.Status = roachpb.COMMITTED
	if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), rArgs); err != nil {
		t.Fatal(err)
	}

	// Get original meta2 descriptor.
	rlArgs := &roachpb.RangeLookupRequest{
		MaxRanges: 1,
		Reverse:   true,
	}
	var rlReply *roachpb.RangeLookupResponse

	// Test ReverseScan without intents.
	for _, c := range testCases {
		clonedRLArgs := proto.Clone(rlArgs).(*roachpb.RangeLookupRequest)
		clonedRLArgs.Key = keys.RangeMetaKey(roachpb.RKey(c.key))
		reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, clonedRLArgs)
		if err != nil {
			t.Fatal(err)
		}
		rlReply = reply.(*roachpb.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if !(seen.StartKey.Equal(c.expected.StartKey) && seen.EndKey.Equal(c.expected.EndKey)) {
			t.Errorf("expected descriptor %s; got %s", &c.expected, &seen)
		}
	}

	// Write the new descriptor as an intent.
	intentRange := testRanges[withIntentRangeIndex]
	data, err := proto.Marshal(&intentRange)
	if err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(intentRange.EndKey)), data)
	txn2 := &roachpb.Transaction{ID: uuid.NewUUID4(), Timestamp: tc.clock.Now(), Sequence: 1}
	if _, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{Txn: txn2}, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Test ReverseScan with intents.
	for _, c := range testCases {
		clonedRLArgs := proto.Clone(rlArgs).(*roachpb.RangeLookupRequest)
		clonedRLArgs.Key = keys.RangeMetaKey(roachpb.RKey(c.key))
		reply, err := client.SendWrappedWith(tc.Sender(), tc.rng.context(), roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, clonedRLArgs)
		if err != nil {
			t.Fatal(err)
		}
		rlReply = reply.(*roachpb.RangeLookupResponse)
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

	expected := []roachpb.RangeDescriptor{*tc.rng.Desc()}
	testCases := []struct {
		key      roachpb.RKey
		reverse  bool
		expected []roachpb.RangeDescriptor
	}{

		// Test with the first range (StartKey==KeyMin). Normally we look
		// up this range in gossip instead of executing the RPC, but
		// RangeLookup is still used when up-to-date information is
		// required.
		{key: roachpb.RKeyMin, reverse: false, expected: expected},
		// Test with the last key in a meta prefix. This is an edge case in the
		// implementation.
		{key: keys.Addr(keys.Meta1KeyMax), reverse: false, expected: expected},
		{key: keys.Addr(keys.Meta2KeyMax), reverse: false, expected: nil},
		{key: keys.Addr(keys.Meta1KeyMax), reverse: true, expected: expected},
		{key: keys.Addr(keys.Meta2KeyMax), reverse: true, expected: expected},
	}

	for _, c := range testCases {
		resp, err := client.SendWrapped(tc.Sender(), nil, &roachpb.RangeLookupRequest{
			Span: roachpb.Span{
				Key: c.key.AsRawKey(),
			},
			MaxRanges: 1,
			Reverse:   c.reverse,
		})
		if err != nil {
			if c.expected != nil {
				t.Fatal(err)
			}
		} else {
			reply := resp.(*roachpb.RangeLookupResponse)
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

	args := incrementArgs([]byte("a"), 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args)

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

// TestRequestLeaderEncounterGroupDeleteError verifies that a request leader proposal which fails with
// errRaftGroupDeleted is converted to a RangeNotFoundError in the Store.
func TestRequestLeaderEncounterGroupDeleteError(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{
		rng: &Replica{},
	}
	tc.Start(t)
	defer tc.Stop()

	// Mock proposeRaftCommand to return an errRaftGroupDeleted error.
	proposeRaftCommandFn := func(cmdIDKey, roachpb.RaftCommand) error {
		return errRaftGroupDeleted
	}
	rng, err := NewReplica(testRangeDescriptor(), tc.store)
	rng.proposeRaftCommandFn = proposeRaftCommandFn
	if err != nil {
		t.Fatal(err)
	}
	if err := tc.store.AddReplicaTest(rng); err != nil {
		t.Fatal(err)
	}

	gArgs := getArgs(roachpb.Key("a"))
	// Force the read command request a new lease.
	clock := tc.clock
	ts := clock.Update(clock.Now().Add(int64(DefaultLeaderLeaseDuration), 0))
	_, err = client.SendWrappedWith(tc.store, nil, roachpb.Header{
		Timestamp: ts,
		RangeID:   1,
	}, &gArgs)
	if _, ok := err.(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expected a RangeNotFoundError, get %s", err)
	}
}

func TestIntentIntersect(t *testing.T) {
	defer leaktest.AfterTest(t)
	iPt := roachpb.Span{
		Key:    roachpb.Key("asd"),
		EndKey: nil,
	}
	iRn := roachpb.Span{
		Key:    roachpb.Key("c"),
		EndKey: roachpb.Key("x"),
	}

	suffix := roachpb.RKey("abcd")
	iLc := roachpb.Span{
		Key:    keys.MakeRangeKey(roachpb.RKey("c"), suffix, nil),
		EndKey: keys.MakeRangeKey(roachpb.RKey("x"), suffix, nil),
	}
	kl1 := string(iLc.Key)
	kl2 := string(iLc.EndKey)

	for i, tc := range []struct {
		intent   roachpb.Span
		from, to string
		exp      []string
	}{
		{intent: iPt, from: "", to: "z", exp: []string{"", "", "asd", ""}},

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
		in, out := intersectSpan(tc.intent, roachpb.RangeDescriptor{
			StartKey: roachpb.RKey(tc.from),
			EndKey:   roachpb.RKey(tc.to),
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
// results in an error which implements roachpb.IndexedError, the index of this
// command is stored into the error.
func TestBatchErrorWithIndex(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	ba := roachpb.BatchRequest{}
	ba.Add(&roachpb.PutRequest{
		Span:  roachpb.Span{Key: roachpb.Key("k")},
		Value: roachpb.MakeValueFromString("not nil"),
	})
	// This one fails with a ConditionalPutError, which implements
	// roachpb.IndexedError.
	ba.Add(&roachpb.ConditionalPutRequest{
		Span:     roachpb.Span{Key: roachpb.Key("k")},
		Value:    roachpb.MakeValueFromString("irrelevant"),
		ExpValue: nil, // not true after above Put
	})
	// This one is never executed.
	ba.Add(&roachpb.GetRequest{
		Span: roachpb.Span{Key: roachpb.Key("k")},
	})

	if _, pErr := tc.Sender().Send(tc.rng.context(), ba); pErr == nil {
		t.Fatal("expected an error")
	} else if iErr, ok := pErr.GoError().(roachpb.IndexedError); !ok {
		t.Fatalf("expected indexed error, got %s", pErr)
	} else if index, ok := iErr.ErrorIndex(); !ok || index != 1 || !testutils.IsError(pErr.GoError(), "unexpected value") {
		t.Fatalf("invalid index or error type: %s", iErr)
	}

}

// TestReplicaLoadSystemDBSpanIntent verifies that intents on the SystemDBSpan
// cause an error, but trigger asynchronous cleanup.
func TestReplicaLoadSystemDBSpanIntent(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	rng := tc.store.LookupReplica(keys.Addr(keys.SystemDBSpan.Key), nil)
	if rng == nil {
		t.Fatalf("no replica contains the SystemDB span")
	}
	v := roachpb.MakeValueFromString("foo")
	// Create a transaction. We don't write a txn record, so pushing this will
	// succeed and abort the intent we write here.
	txn := newTransaction("test", []byte("a"), 1, roachpb.SERIALIZABLE, rng.store.Clock())
	if err := engine.MVCCPut(rng.store.Engine(), &engine.MVCCStats{},
		keys.SystemDBSpan.Key, rng.store.Clock().Now(), v, txn); err != nil {
		t.Fatal(err)
	}
	// Verify that this trips up loading the SystemDB data.
	if _, _, err := rng.loadSystemDBSpan(); err != errSystemDBIntent {
		t.Fatal(err)
	}

	// In the loop, wait until the intent is aborted. Then write a "real" value
	// there and verify that we can now load the data as expected.
	util.SucceedsWithin(t, time.Second, func() error {
		if err := engine.MVCCPut(rng.store.Engine(), &engine.MVCCStats{},
			keys.SystemDBSpan.Key, rng.store.Clock().Now(), v, nil); err != nil {
			return err
		}

		kvs, _, err := rng.loadSystemDBSpan()
		if err != nil {
			return err
		}

		if len(kvs) != 1 || !bytes.Equal(kvs[0].Key, keys.SystemDBSpan.Key) {
			return util.Errorf("expected only key %q in SystemDB map: %+v", keys.SystemDBSpan.Key, kvs)
		}
		return nil
	})
}

func TestReplicaDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	rep, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// First try and fail with an outdated descriptor.
	origDesc := rep.Desc()
	newDesc := proto.Clone(origDesc).(*roachpb.RangeDescriptor)
	_, newRep := newDesc.FindReplica(tc.store.StoreID())
	newRep.ReplicaID++
	newDesc.NextReplicaID++
	if err := rep.setDesc(newDesc); err != nil {
		t.Fatal(err)
	}
	if err := rep.Destroy(*origDesc); !testutils.IsError(err, "replica ID has changed") {
		t.Fatalf("expected error 'replica ID has changed' but got %s", err)
	}

	// Now try a fresh descriptor and succeed.
	if err := rep.Destroy(*rep.Desc()); err != nil {
		t.Fatal(err)
	}
}

func TestEntries(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.DisableRaftLogQueue(true)

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args); err != nil {
			t.Fatal(err)
		}
		idx, err := tc.rng.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	rng := tc.rng
	rangeID := rng.Desc().RangeID

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, err := client.SendWrapped(tc.Sender(), rng.context(), &truncateArgs); err != nil {
		t.Fatal(err)
	}

	for i, tc := range []struct {
		lo             uint64
		hi             uint64
		maxBytes       uint64
		expResultCount int
		expError       error
	}{
		// Case 0: Just most of the entries.
		{lo: indexes[5], hi: indexes[9], expResultCount: 4},
		// Case 1: Get a single entry.
		{lo: indexes[5], hi: indexes[6], expResultCount: 1},
		// Case 2: Use MaxUint64 instead of 0 for maxBytes.
		{lo: indexes[5], hi: indexes[9], maxBytes: math.MaxUint64, expResultCount: 4},
		// Case 3: maxBytes is set low so only a single value should be
		// returned.
		{lo: indexes[5], hi: indexes[9], maxBytes: 1, expResultCount: 1},
		// Case 4: hi value is past the last index, should return all available
		// entries
		{lo: indexes[5], hi: indexes[9] + 1, expResultCount: 5},
		// Case 5: all values have been truncated.
		{lo: indexes[1], hi: indexes[2], expError: raft.ErrCompacted},
		// Case 6: hi has just been truncated.
		{lo: indexes[1], hi: indexes[4], expError: raft.ErrCompacted},
		// Case 7: another case where hi has just been truncated.
		{lo: indexes[3], hi: indexes[4], expError: raft.ErrCompacted},
		// Case 8: lo has been truncated and hi is the truncation point.
		{lo: indexes[4], hi: indexes[5], expError: raft.ErrCompacted},
		// Case 9: lo has been truncated but hi is available.
		{lo: indexes[4], hi: indexes[9], expError: raft.ErrCompacted},
		// Case 10: lo has been truncated and hi is not available.
		{lo: indexes[4], hi: indexes[9] + 100, expError: raft.ErrCompacted},
		// Case 11: lo has been truncated but hi is available, and maxBytes is
		// set low.
		{lo: indexes[4], hi: indexes[9], maxBytes: 1, expError: raft.ErrCompacted},
		// Case 12: lo is available but hi isn't.
		{lo: indexes[5], hi: indexes[9] + 100, expError: raft.ErrUnavailable},
		// Case 13: both lo and hi are not available.
		{lo: indexes[9] + 100, hi: indexes[9] + 1000, expError: raft.ErrUnavailable},
		// Case 14: lo is available, hi is not, but it was cut off by maxBytes.
		{lo: indexes[5], hi: indexes[9] + 1000, maxBytes: 1, expResultCount: 1},
	} {
		ents, err := rng.Entries(tc.lo, tc.hi, tc.maxBytes)
		if tc.expError == nil && err != nil {
			t.Errorf("%d: expected no error, got %s", i, err)
			continue
		} else if err != tc.expError {
			t.Errorf("%d: expected error %s, got %s", i, tc.expError, err)
			continue
		}
		if len(ents) != tc.expResultCount {
			t.Errorf("%d: expected %d entires, got %d", i, tc.expResultCount, len(ents))
		}
	}

	// Case 15: Lo must be less than or equal to hi.
	if _, err := rng.Entries(indexes[9], indexes[5], 0); err == nil {
		t.Errorf("15: error expected, got none")
	}

	// Case 16: add a gap to the indexes.
	if err := engine.MVCCDelete(tc.store.Engine(), nil, keys.RaftLogKey(rangeID, indexes[6]), roachpb.ZeroTimestamp,
		nil); err != nil {
		t.Fatal(err)
	}
	if _, err := rng.Entries(indexes[5], indexes[9], 0); err == nil {
		t.Errorf("16: error expected, got none")
	}

	// Case 17: don't hit the gap due to maxBytes.
	ents, err := rng.Entries(indexes[5], indexes[9], 1)
	if err != nil {
		t.Errorf("17: expected no error, got %s", err)
	}
	if len(ents) != 1 {
		t.Errorf("17: expected 1 entry, got %d", len(ents))
	}

	// Case 18: don't hit the gap due to truncation.
	if _, err := rng.Entries(indexes[4], indexes[9], 0); err != raft.ErrCompacted {
		t.Errorf("18: expected error %s , got %s", raft.ErrCompacted, err)
	}
}

func TestTerm(t *testing.T) {
	defer leaktest.AfterTest(t)
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.DisableRaftLogQueue(true)

	rng := tc.rng
	rangeID := rng.Desc().RangeID

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, err := client.SendWrapped(tc.Sender(), tc.rng.context(), &args); err != nil {
			t.Fatal(err)
		}
		idx, err := tc.rng.LastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, err := client.SendWrapped(tc.Sender(), rng.context(), &truncateArgs); err != nil {
		t.Fatal(err)
	}

	firstIndex, err := rng.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != indexes[5] {
		t.Fatalf("expected fristIndex %d to be %d", firstIndex, indexes[4])
	}

	// Truncated logs should return an ErrCompacted error.
	if _, err := tc.rng.Term(indexes[1]); err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}
	if _, err := tc.rng.Term(indexes[3]); err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// FirstIndex-1 should return the term of firstIndex.
	firstIndexTerm, err := tc.rng.Term(firstIndex)
	if err != nil {
		t.Errorf("expect no error, got %s", err)
	}

	term, err := tc.rng.Term(indexes[4])
	if err != nil {
		t.Errorf("expect no error, got %s", err)
	}
	if term != firstIndexTerm {
		t.Errorf("expected firstIndex-1's term:%d to equal that of firstIndex:%d", term, firstIndexTerm)
	}

	lastIndex, err := rng.LastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Last index should return correctly.
	if _, err := tc.rng.Term(lastIndex); err != nil {
		t.Errorf("expected no error, got %s", err)
	}

	// Terms for after the last index should return ErrUnavailable.
	if _, err := tc.rng.Term(lastIndex + 1); err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
	if _, err := tc.rng.Term(indexes[9] + 1000); err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
}
