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

	"golang.org/x/net/context"

	"github.com/coreos/etcd/raft"
	"github.com/gogo/protobuf/proto"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
)

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

// LeaderLeaseExpiration returns an int64 to increment a manual clock with to
// make sure that all active leader leases expire.
func LeaderLeaseExpiration(clock *hlc.Clock) int64 {
	// Due to lease extensions, the remaining interval can be longer than just
	// the sum of the offset (=length of stasis period) and the active
	// duration, but definitely not by 2x.
	return 2 * int64(LeaderLeaseActiveDuration+clock.MaxOffset())
}

// leaseExpiry returns a duration in nanos after which any leader lease the
// Replica may hold is expired. It is more precise than LeaderLeaseExpiration
// in that it returns the minimal duration necessary.
func leaseExpiry(rng *Replica) int64 {
	if l, _ := rng.getLeaderLease(); l != nil {
		return l.Expiration.WallTime + 1
	}
	return 0
}

// testContext contains all the objects necessary to test a Range.
// In most cases, simply call Start(t) (and later Stop()) on a zero-initialized
// testContext{}. Any fields which are initialized to non-nil values
// will be used as-is.
type testContext struct {
	testing.TB
	transport     *RaftTransport
	store         *Store
	rng           *Replica
	rangeID       roachpb.RangeID
	gossip        *gossip.Gossip
	engine        engine.Engine
	manualClock   *hlc.ManualClock
	clock         *hlc.Clock
	stopper       *stop.Stopper
	bootstrapMode bootstrapMode
}

// Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(t testing.TB) {
	ctx := TestStoreContext()
	tc.StartWithStoreContext(t, ctx)
}

// StartWithStoreContext initializes the test context with a single
// range covering the entire keyspace.
func (tc *testContext) StartWithStoreContext(t testing.TB, ctx StoreContext) {
	tc.TB = t
	if tc.stopper == nil {
		tc.stopper = stop.NewStopper()
	}
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(tc.stopper)
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(nil, nil, tc.stopper)
		tc.gossip = gossip.New(rpcContext, nil, tc.stopper)
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
		tc.transport = NewDummyRaftTransport()
	}

	if tc.store == nil {
		ctx.Clock = tc.clock
		ctx.Gossip = tc.gossip
		ctx.Transport = tc.transport
		// Create a test sender without setting a store. This is to deal with the
		// circular dependency between the test sender and the store. The actual
		// store will be passed to the sender after it is created and bootstrapped.
		sender := &testSender{}
		ctx.DB = client.NewDB(sender)
		tc.store = NewStore(ctx, tc.engine, &roachpb.NodeDescriptor{NodeID: 1})
		if err := tc.store.Bootstrap(roachpb.StoreIdent{
			ClusterID: uuid.MakeV4(),
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
			rng, err := NewReplica(testRangeDescriptor(), tc.store, 0)
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
		tc.rangeID = tc.rng.RangeID
	}

	if err := tc.initConfigs(realRange, t); err != nil {
		t.Fatal(err)
	}
}

func (tc *testContext) Sender() client.Sender {
	return client.Wrap(tc.rng, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
		if ba.RangeID != 0 {
			ba.RangeID = 1
		}
		if ba.Timestamp == roachpb.ZeroTimestamp {
			if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
				tc.Fatal(err)
			}
		}
		return ba
	})
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it
func (tc *testContext) SendWrappedWith(h roachpb.Header, args roachpb.Request) (roachpb.Response, *roachpb.Error) {
	return client.SendWrappedWith(tc.Sender(), tc.rng.context(context.Background()), h, args)
}

// SendWrapped is identical to SendWrappedWith with a zero header.
func (tc *testContext) SendWrapped(args roachpb.Request) (roachpb.Response, *roachpb.Error) {
	return tc.SendWrappedWith(roachpb.Header{}, args)
}

func (tc *testContext) Stop() {
	tc.stopper.Stop()
}

// initConfigs creates default configuration entries.
func (tc *testContext) initConfigs(realRange bool, t testing.TB) error {
	// Put an empty system config into gossip so that gossip callbacks get
	// run. We're using a fake config, but it's hooked into SystemConfig.
	if err := tc.gossip.AddInfoProto(gossip.KeySystemConfig,
		&config.SystemConfig{}, 0); err != nil {
		return err
	}

	util.SucceedsSoon(t, func() error {
		if _, ok := tc.gossip.GetSystemConfig(); !ok {
			return util.Errorf("expected system config to be set")
		}
		return nil
	})

	return nil
}

func newTransaction(name string, baseKey roachpb.Key, userPriority roachpb.UserPriority,
	isolation roachpb.IsolationType, clock *hlc.Clock) *roachpb.Transaction {
	var offset int64
	var now roachpb.Timestamp
	if clock != nil {
		offset = clock.MaxOffset().Nanoseconds()
		now = clock.Now()
	}
	return roachpb.NewTransaction(name, baseKey, userPriority, isolation, now, offset)
}

// createReplicaSets creates new roachpb.ReplicaDescriptor protos based on an array of
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

// TestIsOnePhaseCommit verifies the circumstances where a
// transactional batch can be committed as an atomic write.
func TestIsOnePhaseCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	txnReqs := []roachpb.RequestUnion{
		{BeginTransaction: &roachpb.BeginTransactionRequest{}},
		{Put: &roachpb.PutRequest{}},
		{EndTransaction: &roachpb.EndTransactionRequest{}},
	}
	testCases := []struct {
		bu      []roachpb.RequestUnion
		isTxn   bool
		isWTO   bool
		isTSOff bool
		exp1PC  bool
	}{
		{[]roachpb.RequestUnion{}, false, false, false, false},
		{[]roachpb.RequestUnion{}, true, false, false, false},
		{[]roachpb.RequestUnion{{Get: &roachpb.GetRequest{}}}, true, false, false, false},
		{[]roachpb.RequestUnion{{Put: &roachpb.PutRequest{}}}, true, false, false, false},
		{txnReqs[0 : len(txnReqs)-1], true, false, false, false},
		{txnReqs[1:], true, false, false, false},
		{txnReqs, true, false, false, true},
		{txnReqs, true, true, false, false},
		{txnReqs, true, false, true, false},
		{txnReqs, true, true, true, false},
	}

	clock := hlc.NewClock(hlc.UnixNano)
	for i, c := range testCases {
		ba := roachpb.BatchRequest{Requests: c.bu}
		if c.isTxn {
			ba.Txn = newTransaction("txn", roachpb.Key("a"), 1, roachpb.SNAPSHOT, clock)
			if c.isWTO {
				ba.Txn.WriteTooOld = true
			}
			ba.Txn.Timestamp = ba.Txn.OrigTimestamp.Add(1, 0)
			if c.isTSOff {
				ba.Txn.Isolation = roachpb.SERIALIZABLE
			}
		}
		if is1PC := isOnePhaseCommit(ba); is1PC != c.exp1PC {
			t.Errorf("%d: expected 1pc=%t; got %t", i, c.exp1PC, is1PC)
		}
	}
}

// TestReplicaContains verifies that the range uses Key.Address() in
// order to properly resolve addresses for local keys.
func TestReplicaContains(t *testing.T) {
	defer leaktest.AfterTest(t)()
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
		Replicas: []roachpb.ReplicaDescriptor{{
			// This test is a little weird since it doesn't call Store.Start
			// which would assign IDs.
			StoreID:   0,
			ReplicaID: 1,
		}},
	}

	stopper := stop.NewStopper()
	defer stopper.Stop()
	e := engine.NewInMem(roachpb.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20, stopper)
	clock := hlc.NewClock(hlc.UnixNano)
	ctx := TestStoreContext()
	ctx.Clock = clock
	ctx.Transport = NewDummyRaftTransport()
	store := NewStore(ctx, e, &roachpb.NodeDescriptor{NodeID: 1})
	r, err := NewReplica(desc, store, 0)
	if err != nil {
		t.Fatal(err)
	}
	if statsKey := keys.RangeStatsKey(desc.RangeID); !r.ContainsKey(statsKey) {
		t.Errorf("expected range to contain range stats key %q", statsKey)
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

func setLeaderLease(r *Replica, l *roachpb.Lease) error {
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.LeaderLeaseRequest{Lease: *l})
	pendingCmd, err := r.proposeRaftCommand(r.context(context.Background()), ba)
	if err == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor this to a more conventional error-handling pattern.
		err = (<-pendingCmd.done).Err.GoError()
	}
	return err
}

// TestReplicaReadConsistency verifies behavior of the range under
// different read consistencies. Note that this unittest plays
// fast and loose with granting leader leases.
func TestReplicaReadConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	if _, err := tc.SendWrapped(&gArgs); err != nil {
		t.Errorf("expected success on consistent read: %s", err)
	}

	// Try a consensus read and verify error.

	if _, err := tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.CONSENSUS,
	}, &gArgs); err == nil {
		t.Errorf("expected error on consensus read")
	}

	// Try an inconsistent read within a transaction.
	txn := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)

	if _, err := tc.SendWrappedWith(roachpb.Header{
		Txn:             txn,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); err == nil {
		t.Errorf("expected error on inconsistent read within a txn")
	}

	// Lose the lease and verify CONSISTENT reads receive NotLeaderError
	// and INCONSISTENT reads work as expected.
	start := roachpb.ZeroTimestamp.Add(leaseExpiry(tc.rng), 0)
	tc.manualClock.Set(start.WallTime)
	if err := setLeaderLease(tc.rng, &roachpb.Lease{
		Start:       start,
		StartStasis: start.Add(10, 0),
		Expiration:  start.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{ // a different node
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Send without Txn.
	_, pErr := tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.CONSISTENT,
	}, &gArgs)
	if _, ok := pErr.GetDetail().(*roachpb.NotLeaderError); !ok {
		t.Errorf("expected not leader error; got %s", pErr)
	}

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); pErr != nil {
		t.Errorf("expected success reading with inconsistent: %s", pErr)
	}
}

// TestApplyCmdLeaseError verifies that when during application of a Raft
// command the proposing node no longer holds the leader lease, an error is
// returned. This prevents regression of #1483.
func TestApplyCmdLeaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	start := roachpb.ZeroTimestamp.Add(leaseExpiry(tc.rng), 0)
	tc.manualClock.Set(start.WallTime)
	if err := setLeaderLease(tc.rng, &roachpb.Lease{
		Start:       start,
		StartStasis: start.Add(10, 0),
		Expiration:  start.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{ // a different node
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}); err != nil {
		t.Fatal(err)
	}

	_, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: tc.clock.Now().Add(-100, 0),
	}, &pArgs)
	if _, ok := pErr.GetDetail().(*roachpb.NotLeaderError); !ok {
		t.Fatalf("expected not leader error in return, got %v", pErr)
	}
}

func TestReplicaRangeBoundsChecking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.RKey("a")
	firstRng := tc.store.LookupReplica(key, nil)
	newRng := splitTestRange(tc.store, key, key, t)
	if pErr := newRng.redirectOnOrAcquireLeaderLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	gArgs := getArgs(roachpb.Key("b"))
	_, pErr := tc.SendWrapped(&gArgs)

	if mismatchErr, ok := pErr.GetDetail().(*roachpb.RangeKeyMismatchError); !ok {
		t.Errorf("expected range key mismatch error: %s", pErr)
	} else {
		if mismatchedDesc := mismatchErr.MismatchedRange; mismatchedDesc == nil || mismatchedDesc.RangeID != firstRng.RangeID {
			t.Errorf("expected mismatched range to be %d, found %v", firstRng.RangeID, mismatchedDesc)
		}
		if suggestedDesc := mismatchErr.SuggestedRange; suggestedDesc == nil || suggestedDesc.RangeID != newRng.RangeID {
			t.Errorf("expected suggested range to be %d, found %v", newRng.RangeID, suggestedDesc)
		}
	}
}

// hasLease returns whether the most recent leader lease was held by the given
// range replica and whether it's expired for the given timestamp.
func hasLease(rng *Replica, timestamp roachpb.Timestamp) (owned bool, expired bool) {
	l, _ := rng.getLeaderLease()
	return l.OwnedBy(rng.store.StoreID()), !l.Covers(timestamp)
}

func TestReplicaLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	one := roachpb.ZeroTimestamp.Add(1, 0)

	for _, lease := range []roachpb.Lease{
		{StartStasis: one},
		{Start: one, StartStasis: one},
		{Expiration: one, StartStasis: one.Next()},
	} {
		if _, err := tc.rng.LeaderLease(context.Background(), tc.store.Engine(), nil,
			roachpb.Header{}, roachpb.LeaderLeaseRequest{
				Lease: lease,
			}); !testutils.IsError(err, "illegal lease interval") {
			t.Fatalf("unexpected error: %v", err)
		}
	}

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
	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := tc.clock.Now()
	if err := setLeaderLease(tc.rng, &roachpb.Lease{
		Start:       now.Add(10, 0),
		StartStasis: now.Add(20, 0),
		Expiration:  now.Add(20, 0),
		Replica:     secondReplica,
	}); err != nil {
		t.Fatal(err)
	}
	if held, expired := hasLease(tc.rng, tc.clock.Now().Add(15, 0)); held || expired {
		t.Errorf("expected second replica to have leader lease")
	}

	{
		pErr := tc.rng.redirectOnOrAcquireLeaderLease(tc.rng.context(context.Background()))
		if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaderError); !ok || lErr == nil {
			t.Fatalf("wanted NotLeaderError, got %s", pErr)
		}
	}
	// Advance clock past expiration and verify that another has
	// leader lease will not be true.
	tc.manualClock.Increment(21) // 21ns have passed
	if held, expired := hasLease(tc.rng, tc.clock.Now()); held || !expired {
		t.Errorf("expected another replica to have expired lease")
	}

	// Verify that command returns NotLeaderError when lease is rejected.
	rng, err := NewReplica(testRangeDescriptor(), tc.store, 0)
	if err != nil {
		t.Fatal(err)
	}

	rng.mu.Lock()
	rng.mu.proposeRaftCommandFn = func(*pendingCmd) error {
		return &roachpb.LeaseRejectedError{
			Message: "replica not found",
		}
	}
	rng.mu.Unlock()

	{
		if _, ok := rng.redirectOnOrAcquireLeaderLease(tc.rng.context(context.Background())).GetDetail().(*roachpb.NotLeaderError); !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaderError{}, err)
		}
	}
}

// TestReplicaNotLeaderError verifies NotLeaderError when lease is rejected.
func TestReplicaNotLeaderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := tc.clock.Now()
	if err := setLeaderLease(tc.rng, &roachpb.Lease{
		Start:       now,
		StartStasis: now.Add(10, 0),
		Expiration:  now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}); err != nil {
		t.Fatal(err)
	}

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
		_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: now}, test)

		if _, ok := pErr.GetDetail().(*roachpb.NotLeaderError); !ok {
			t.Errorf("%d: expected not leader error: %s", i, pErr)
		}
	}
}

// TestReplicaGossipConfigsOnLease verifies that config info is gossiped
// upon acquisition of the leader lease.
func TestReplicaGossipConfigsOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	// Write some arbitrary data in the system config span.
	key := keys.MakeTablePrefix(keys.MaxSystemConfigDescID)
	var val roachpb.Value
	val.SetInt(42)
	if err := engine.MVCCPut(context.Background(), tc.engine, nil, key, roachpb.MinTimestamp, val, nil); err != nil {
		t.Fatal(err)
	}

	// If this actually failed, we would have gossiped from MVCCPutProto.
	// Unlikely, but why not check.
	if cfg, ok := tc.gossip.GetSystemConfig(); ok {
		if nv := len(cfg.Values); nv == 1 && cfg.Values[nv-1].Key.Equal(key) {
			t.Errorf("unexpected gossip of system config: %s", cfg)
		}
	}

	// Expire our own lease which we automagically acquired due to being
	// first range and config holder.
	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := tc.clock.Now()

	// Give lease to someone else.
	if err := setLeaderLease(tc.rng, &roachpb.Lease{
		Start:       now,
		StartStasis: now.Add(10, 0),
		Expiration:  now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}); err != nil {
		t.Fatal(err)
	}

	// Expire that lease.
	tc.manualClock.Increment(11 + int64(tc.clock.MaxOffset())) // advance time
	now = tc.clock.Now()

	// Give lease to this range.
	if err := setLeaderLease(tc.rng, &roachpb.Lease{
		Start:       now.Add(11, 0),
		StartStasis: now.Add(20, 0),
		Expiration:  now.Add(20, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
	}); err != nil {
		t.Fatal(err)
	}

	util.SucceedsSoon(t, func() error {
		cfg, ok := tc.gossip.GetSystemConfig()
		if !ok {
			return util.Errorf("expected system config to be set")
		}
		numValues := len(cfg.Values)
		if numValues != 1 {
			return util.Errorf("num config values != 1; got %d", numValues)
		}
		if k := cfg.Values[numValues-1].Key; !k.Equal(key) {
			return util.Errorf("invalid key for config value (%q != %q)", k, key)
		}
		return nil
	})
}

// TestReplicaTSCacheLowWaterOnLease verifies that the low water mark
// is set on the timestamp cache when the node is granted the leader
// lease after not holding it and it is not set when the node is
// granted the leader lease when it was the last holder.
func TestReplicaTSCacheLowWaterOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := roachpb.Timestamp{WallTime: tc.manualClock.UnixNano()}

	tc.rng.mu.Lock()
	baseRTS, _ := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil /* end */, nil /* txn */)
	tc.rng.mu.Unlock()
	baseLowWater := baseRTS.WallTime

	newLowWater := now.Add(50, 0).WallTime + baseLowWater

	testCases := []struct {
		storeID     roachpb.StoreID
		start       roachpb.Timestamp
		expiration  roachpb.Timestamp
		expLowWater int64
		expErr      string
	}{
		// Grant the lease fresh.
		{storeID: tc.store.StoreID(),
			start: now, expiration: now.Add(10, 0),
			expLowWater: baseLowWater},
		// Renew the lease.
		{storeID: tc.store.StoreID(),
			start: now.Add(15, 0), expiration: now.Add(30, 0),
			expLowWater: baseLowWater},
		// Renew the lease but shorten expiration. This errors out.
		{storeID: tc.store.StoreID(),
			start: now.Add(16, 0), expiration: now.Add(25, 0),
			expErr: "lease shortening currently unsupported",
		},
		// Another Store attempts to get the lease, but overlaps. If the
		// previous lease expiration had worked, this would have too.
		{storeID: tc.store.StoreID() + 1,
			start: now.Add(29, 0), expiration: now.Add(50, 0),
			expLowWater: baseLowWater,
			expErr:      "overlaps previous",
		},
		// The other store tries again, this time without the overlap.
		{storeID: tc.store.StoreID() + 1,
			start: now.Add(31, 0), expiration: now.Add(50, 0),
			expLowWater: baseLowWater},
		// Lease is regranted to this replica. Store clock moves forward avoid
		// influencing the result.
		{storeID: tc.store.StoreID(),
			start: now.Add(60, 0), expiration: now.Add(70, 0),
			expLowWater: newLowWater},
		// Lease is held by another once more.
		{storeID: tc.store.StoreID() + 1,
			start: now.Add(70, 0), expiration: now.Add(90, 0),
			expLowWater: newLowWater},
	}

	for i, test := range testCases {
		if err := setLeaderLease(tc.rng, &roachpb.Lease{
			Start:       test.start,
			StartStasis: test.expiration.Add(-1, 0), // smaller than durations used
			Expiration:  test.expiration,
			Replica: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(test.storeID),
				NodeID:    roachpb.NodeID(test.storeID),
				StoreID:   test.storeID,
			},
		}); err != nil {
			if test.expErr == "" || !testutils.IsError(err, test.expErr) {
				t.Fatalf("%d: unexpected error %s", i, err)
			}
		}
		// Verify expected low water mark.
		tc.rng.mu.Lock()
		rTS, rOK := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil, nil)
		wTS, wOK := tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("a"), nil, nil)
		tc.rng.mu.Unlock()
		if rTS.WallTime != test.expLowWater || wTS.WallTime != test.expLowWater || rOK || wOK {
			t.Errorf("%d: expected low water %d; got %d, %d; rOK=%t, wOK=%t", i, test.expLowWater, rTS.WallTime, wTS.WallTime, rOK, wOK)
		}
	}
}

// TestReplicaLeaderLeaseRejectUnknownRaftNodeID ensures that a replica cannot
// obtain the leader lease if it is not part of the current range descriptor.
// TODO(mrtracy): This should probably be tested in client_raft_test package,
// using a real second store.
func TestReplicaLeaderLeaseRejectUnknownRaftNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := tc.clock.Now()
	lease := &roachpb.Lease{
		Start:       now,
		StartStasis: now.Add(10, 0),
		Expiration:  now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}
	ba := roachpb.BatchRequest{}
	ba.Timestamp = tc.rng.store.Clock().Now()
	ba.Add(&roachpb.LeaderLeaseRequest{Lease: *lease})
	pendingCmd, err := tc.rng.proposeRaftCommand(tc.rng.context(context.Background()), ba)
	if err == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor to a more conventional error-handling pattern.
		// Remove ambiguity about where the "replica not found" error comes from.
		err = (<-pendingCmd.done).Err.GoError()
	}
	if !testutils.IsError(err, "replica not found") {
		t.Errorf("unexpected error obtaining lease for invalid store: %v", err)
	}
}

// TestReplicaGossipFirstRange verifies that the first range gossips its
// location and the cluster ID.
func TestReplicaGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

// TestReplicaGossipAllConfigs verifies that all config types are gossiped.
func TestReplicaGossipAllConfigs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	if _, ok := tc.gossip.GetSystemConfig(); !ok {
		t.Fatal("config not set")
	}
}

func maybeWrapWithBeginTransaction(sender client.Sender, ctx context.Context, header roachpb.Header, req roachpb.Request) (roachpb.Response, *roachpb.Error) {
	if header.Txn == nil || header.Txn.Writing {
		return client.SendWrappedWith(sender, ctx, header, req)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var ba roachpb.BatchRequest
	bt, _ := beginTxnArgs(req.Header().Key, header.Txn)
	ba.Header = header
	ba.Add(&bt)
	ba.Add(req)
	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	unwrappedReply := br.Responses[1].GetInner()
	unwrappedHeader := unwrappedReply.Header()
	unwrappedHeader.Txn = br.Txn
	unwrappedReply.SetHeader(unwrappedHeader)
	return unwrappedReply, nil

}

// TestReplicaNoGossipConfig verifies that certain commands (e.g.,
// reads, writes in uncommitted transactions) do not trigger gossip.
func TestReplicaNoGossipConfig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)

	txn := newTransaction("test", key, 1 /* userPriority */, roachpb.SERIALIZABLE, tc.clock)
	h := roachpb.Header{Txn: txn}
	req1 := putArgs(key, []byte("foo"))
	req2, _ := endTxnArgs(txn, true /* commit */)
	req2.IntentSpans = []roachpb.Span{{Key: key}}
	req3 := getArgs(key)

	testCases := []struct {
		req roachpb.Request
		h   roachpb.Header
	}{
		{&req1, h},
		{&req2, h},
		{&req3, roachpb.Header{}},
	}

	for i, test := range testCases {
		txn.Sequence++
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), test.h, test.req); pErr != nil {
			t.Fatal(pErr)
		}
		txn.Writing = true

		// System config is not gossiped.
		cfg, ok := tc.gossip.GetSystemConfig()
		if !ok {
			t.Fatal("config not set")
		}
		if len(cfg.Values) != 0 {
			t.Errorf("System config was gossiped at #%d", i)
		}
	}
}

// TestReplicaNoGossipFromNonLeader verifies that a non-leader replica
// does not gossip configurations.
func TestReplicaNoGossipFromNonLeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)

	txn := newTransaction("test", key, 1 /* userPriority */, roachpb.SERIALIZABLE, tc.clock)
	req1 := putArgs(key, nil)
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), nil, roachpb.Header{
		Txn: txn,
	}, &req1); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Writing = true
	txn.Sequence++

	req2, h := endTxnArgs(txn, true /* commit */)
	req2.IntentSpans = []roachpb.Span{{Key: key}}
	txn.Sequence++
	if _, pErr := tc.SendWrappedWith(h, &req2); pErr != nil {
		t.Fatal(pErr)
	}
	// Execute a get to resolve the intent.
	req3 := getArgs(key)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: txn.Timestamp}, &req3); pErr != nil {
		t.Fatal(pErr)
	}

	// Increment the clock's timestamp to expire the leader lease.
	tc.manualClock.Set(leaseExpiry(tc.rng))
	if lease, _ := tc.rng.getLeaderLease(); lease.Covers(tc.clock.Now()) {
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

// pushTxnArgs returns a request and header for a PushTxn RPC for the
// specified key.
func pushTxnArgs(pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType) roachpb.PushTxnRequest {
	return roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: pushee.Key,
		},
		Now:       pusher.Timestamp,
		PushTo:    pusher.Timestamp,
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
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

func gcKey(key roachpb.Key, timestamp roachpb.Timestamp) roachpb.GCRequest_GCKey {
	return roachpb.GCRequest_GCKey{
		Key:       key,
		Timestamp: timestamp,
	}
}

func gcArgs(startKey []byte, endKey []byte, keys ...roachpb.GCRequest_GCKey) roachpb.GCRequest {
	return roachpb.GCRequest{
		Span: roachpb.Span{
			Key:    startKey,
			EndKey: endKey,
		},
		Keys: keys,
	}
}

// TestAcquireLeaderLease verifies that the leader lease is acquired
// for read and write methods, and eagerly renewed.
func TestAcquireLeaderLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
		lease, _ := tc.rng.getLeaderLease()
		expStart := lease.Start
		tc.manualClock.Set(leaseExpiry(tc.rng))

		ts := tc.clock.Now().Next()
		if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, test); pErr != nil {
			t.Error(pErr)
		}
		if held, expired := hasLease(tc.rng, ts); !held || expired {
			t.Errorf("%d: expected lease acquisition", i)
		}
		lease, _ = tc.rng.getLeaderLease()
		if !lease.Start.Equal(expStart) {
			t.Errorf("%d: unexpected lease start: %s; expected %s", i, lease.Start, expStart)
		}

		if !ts.Less(lease.StartStasis) {
			t.Errorf("%d: %s already in stasis (or beyond): %+v", i, ts, lease)
		}

		shouldRenewTS := lease.StartStasis.Add(-1, 0)
		tc.manualClock.Set(shouldRenewTS.WallTime + 1)
		if _, pErr := tc.SendWrapped(test); pErr != nil {
			t.Error(pErr)
		}
		// Since the command we sent above does not get blocked on the lease
		// extension, we need to wait for it to go through.
		util.SucceedsSoon(t, func() error {
			newLease, _ := tc.rng.getLeaderLease()
			if !lease.StartStasis.Less(newLease.StartStasis) {
				return util.Errorf("%d: lease did not get extended: %+v to %+v", i, lease, newLease)
			}
			return nil
		})
		tc.Stop()
		if t.Failed() {
			return
		}
	}
}

// TestReplicaUpdateTSCache verifies that reads and writes update the
// timestamp cache.
func TestReplicaUpdateTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	gArgs := getArgs([]byte("a"))
	ts := tc.clock.Now()

	_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, &gArgs)

	if pErr != nil {
		t.Error(pErr)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	key := roachpb.Key([]byte("b"))
	tc.manualClock.Set(t1.Nanoseconds())
	drArgs := roachpb.NewDeleteRange(key, key.Next(), false)
	ts = tc.clock.Now()

	_, pErr = tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, drArgs)

	if pErr != nil {
		t.Error(pErr)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	tc.rng.mu.Lock()
	defer tc.rng.mu.Unlock()
	rTS, rOK := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil, nil)
	wTS, wOK := tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("a"), nil, nil)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != 0 || !rOK || wOK {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, rOK = tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("b"), nil, nil)
	wTS, wOK = tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("b"), nil, nil)
	if rTS.WallTime != 0 || wTS.WallTime != t1.Nanoseconds() || rOK || !wOK {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, rOK = tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("c"), nil, nil)
	wTS, wOK = tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("c"), nil, nil)
	if rTS.WallTime != 0 || wTS.WallTime != 0 || rOK || wOK {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
}

// TestReplicaCommandQueue verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestReplicaCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Intercept commands with matching command IDs and block them.
	blockingStart := make(chan struct{})
	blockingDone := make(chan struct{})

	tc := testContext{}
	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if filterArgs.Hdr.UserPriority == 42 {
				blockingStart <- struct{}{}
				<-blockingDone
			}
			return nil
		}
	tc.StartWithStoreContext(t, tsc)
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

			_, pErr := tc.SendWrappedWith(roachpb.Header{
				UserPriority: 42,
			}, args)

			if pErr != nil {
				t.Fatalf("test %d: %s", i, pErr)
			}
			close(cmd1Done)
		})
		// Wait for cmd1 to get into the command queue.
		<-blockingStart

		// First, try a command for same key as cmd1 to verify it blocks.
		cmd2Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key1, test.cmd2Read)

			_, pErr := tc.SendWrapped(args)

			if pErr != nil {
				t.Fatalf("test %d: %s", i, pErr)
			}
			close(cmd2Done)
		})

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		tc.stopper.RunAsyncTask(func() {
			args := readOrWriteArgs(key2, true)

			_, pErr := tc.SendWrapped(args)

			if pErr != nil {
				t.Fatalf("test %d: %s", i, pErr)
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

// TestReplicaCommandQueueInconsistent verifies that inconsistent reads need
// not wait for pending commands to complete through Raft.
func TestReplicaCommandQueueInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := roachpb.Key("key1")
	blockingStart := make(chan struct{}, 1)
	blockingDone := make(chan struct{})

	tc := testContext{}
	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if put, ok := filterArgs.Req.(*roachpb.PutRequest); ok {
				putBytes, err := put.Value.GetBytes()
				if err != nil {
					return roachpb.NewErrorWithTxn(err, filterArgs.Hdr.Txn)
				}
				if bytes.Equal(put.Key, key) && bytes.Equal(putBytes, []byte{1}) {
					// Absence of replay protection can mean that we end up here
					// more often than we expect, hence the select (#3669).
					select {
					case blockingStart <- struct{}{}:
					default:
					}
					<-blockingDone
				}
			}

			return nil
		}
	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()
	cmd1Done := make(chan struct{})
	go func() {
		args := putArgs(key, []byte{1})

		_, pErr := tc.SendWrapped(&args)

		if pErr != nil {
			t.Fatal(pErr)
		}
		close(cmd1Done)
	}()
	// Wait for cmd1 to get into the command queue.
	<-blockingStart

	// An inconsistent read to the key won't wait.
	cmd2Done := make(chan struct{})
	go func() {
		args := getArgs(key)

		_, pErr := tc.SendWrappedWith(roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &args)

		if pErr != nil {
			t.Fatal(pErr)
		}
		close(cmd2Done)
	}()

	select {
	case <-cmd2Done:
		// success.
	case <-cmd1Done:
		t.Fatalf("cmd1 should have been blocked")
	}

	close(blockingDone)
	<-cmd1Done
	// Success.
}

func SendWrapped(sender client.Sender, ctx context.Context, header roachpb.Header, args roachpb.Request) (roachpb.Response, roachpb.BatchResponse_Header, *roachpb.Error) {
	var ba roachpb.BatchRequest
	ba.Add(args)
	ba.Header = header
	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, roachpb.BatchResponse_Header{}, pErr
	}
	return br.Responses[0].GetInner(), br.BatchResponse_Header, pErr
}

// TestReplicaUseTSCache verifies that write timestamps are upgraded
// based on the read timestamp cache.
func TestReplicaUseTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args := getArgs([]byte("a"))

	_, pErr := tc.SendWrapped(&args)

	if pErr != nil {
		t.Error(pErr)
	}
	pArgs := putArgs([]byte("a"), []byte("value"))

	_, respH, pErr := SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{}, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if respH.Timestamp.WallTime != tc.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %s", respH.Timestamp)
	}
}

// TestReplicaNoTSCacheInconsistent verifies that the timestamp cache
// is not affected by inconsistent reads.
func TestReplicaNoTSCacheInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args := getArgs([]byte("a"))
	ts := tc.clock.Now()

	_, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp:       ts,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &args)

	if pErr != nil {
		t.Error(pErr)
	}
	pArgs := putArgs([]byte("a"), []byte("value"))

	_, respH, pErr := SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Timestamp: roachpb.ZeroTimestamp.Add(0, 1)}, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if respH.Timestamp.WallTime == tc.clock.Timestamp().WallTime {
		t.Errorf("expected write timestamp not to upgrade to 1s; got %s", respH.Timestamp)
	}
}

// TestReplicaNoTSCacheUpdateOnFailure verifies that read and write
// commands do not update the timestamp cache if they result in
// failure.
func TestReplicaNoTSCacheUpdateOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Test for both read & write attempts.
	for i, read := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		pArgs := putArgs(key, []byte("value"))
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)

		_, pErr := tc.SendWrappedWith(roachpb.Header{
			Txn: txn,
		}, &pArgs)
		if pErr != nil {
			t.Fatalf("test %d: %s", i, pErr)
		}

		// Now attempt read or write.
		args := readOrWriteArgs(key, read)
		ts := tc.clock.Now() // later timestamp

		if _, pErr := tc.SendWrappedWith(roachpb.Header{
			Timestamp: ts,
		}, args); pErr == nil {
			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		txn.Sequence++
		if _, respH, pErr := SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatalf("test %d: %s", i, pErr)
		} else if !respH.Txn.Timestamp.Equal(txn.Timestamp) {
			t.Errorf("expected timestamp not to advance %s != %s", respH.Timestamp, txn.Timestamp)
		}
	}
}

// TestReplicaNoTimestampIncrementWithinTxn verifies that successive
// read and write commands within the same transaction do not cause
// the write to receive an incremented timestamp.
func TestReplicaNoTimestampIncrementWithinTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Test for both read & write attempts.
	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)

	// Start with a read to warm the timestamp cache.
	gArgs := getArgs(key)

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs := putArgs(key, []byte("value"))

	txn.Sequence++
	_, respH, pErr := SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Txn: txn}, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !respH.Txn.Timestamp.Equal(txn.Timestamp) {
		t.Errorf("expected timestamp to remain %s; got %s", txn.Timestamp, respH.Timestamp)
	}

	// Resolve the intent.
	rArgs := &roachpb.ResolveIntentRequest{
		Span:      pArgs.Header(),
		IntentTxn: txn.TxnMeta,
		Status:    roachpb.COMMITTED,
	}
	txn.Sequence++
	if _, pErr = tc.SendWrappedWith(roachpb.Header{Timestamp: txn.Timestamp}, rArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	ts := txn.Timestamp
	expTS := ts
	expTS.Logical++

	_, respH, pErr = SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Timestamp: ts}, &pArgs)
	if pErr != nil {
		t.Errorf("unexpected pError: %s", pErr)
	}
	if !respH.Timestamp.Equal(expTS) {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, respH.Timestamp)
	}
}

// TestReplicaAbortCacheReadError verifies that an error is returned
// to the client in the event that a abort cache entry is found but is
// not decodable.
func TestReplicaAbortCacheReadError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	k := []byte("a")
	txn := newTransaction("test", k, 10, roachpb.SERIALIZABLE, tc.clock)
	args := incrementArgs(k, 1)
	txn.Sequence = 1

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite Abort cache entry with garbage for the last op.
	key := keys.AbortCacheKey(tc.rng.RangeID, txn.ID)
	err := engine.MVCCPut(context.Background(), tc.engine, nil, key, roachpb.ZeroTimestamp, roachpb.MakeValueFromString("never read in this test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now try increment again and verify error.
	_, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &args)
	if !testutils.IsPError(pErr, "replica corruption") {
		t.Fatal(pErr)
	}
}

// TestReplicaAbortCacheStoredTxnRetryError verifies that if a cached
// entry is present, a transaction restart error is returned.
func TestReplicaAbortCacheStoredTxnRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	{
		txn := newTransaction("test", key, 10, roachpb.SERIALIZABLE, tc.clock)
		txn.Sequence = int32(1)
		entry := roachpb.AbortCacheEntry{
			Key:       txn.Key,
			Timestamp: txn.Timestamp,
			Priority:  0,
		}
		if err := tc.rng.abortCache.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
			t.Fatal(err)
		}

		args := incrementArgs(key, 1)
		_, pErr := tc.SendWrappedWith(roachpb.Header{
			Txn: txn,
		}, &args)
		if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
			t.Fatalf("unexpected error %v", pErr)
		}
	}

	// Try the same again, this time verifying that the Put will actually
	// populate the cache appropriately.
	txn := newTransaction("test", key, 10, roachpb.SERIALIZABLE, tc.clock)
	txn.Sequence = 321
	args := incrementArgs(key, 1)
	try := func() *roachpb.Error {
		_, pErr := tc.SendWrappedWith(roachpb.Header{
			Txn: txn,
		}, &args)
		return pErr
	}

	if pErr := try(); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Timestamp.Forward(txn.Timestamp.Add(10, 10)) // can't hurt
	{
		pErr := try()
		if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
			t.Fatal(pErr)
		}
	}

	//  Pretend we restarted by increasing the epoch. That's all that's needed.
	txn.Epoch++
	txn.Sequence++
	if pErr := try(); pErr != nil {
		t.Fatal(pErr)
	}

	// Now increase the sequence as well. Still good to go.
	txn.Sequence++
	if pErr := try(); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTransactionRetryLeavesIntents sets up a transaction retry event
// and verifies that the intents which were written as part of a
// single batch are left in place despite the failed end transaction.
func TestTransactionRetryLeavesIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	pushee.Priority = 1
	pusher.Priority = 2 // pusher will win

	// Read from the key to increment the timestamp cache.
	gArgs := getArgs(key)
	if _, pErr := tc.SendWrapped(&gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Begin txn, write to key (with now-higher timestamp), and attempt to
	// commit the txn, which should result in a retryable error.
	btArgs, _ := beginTxnArgs(key, pushee)
	pArgs := putArgs(key, []byte("foo"))
	etArgs, _ := endTxnArgs(pushee, true /* commit */)
	var ba roachpb.BatchRequest
	ba.Header.Txn = pushee
	ba.Add(&btArgs)
	ba.Add(&pArgs)
	ba.Add(&etArgs)
	_, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("expected retry error; got %s", pErr)
	}

	// Now verify that the intent was still written for key.
	_, pErr = tc.SendWrapped(&gArgs)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Fatalf("expected write intent error; got %s", pErr)
	}
}

// TestReplicaAbortCacheOnlyWithIntent verifies that a transactional command
// which goes through Raft but is not a transactional write (i.e. does not
// leave intents) passes the abort cache unhindered.
func TestReplicaAbortCacheOnlyWithIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	txn := newTransaction("test", []byte("test"), 10, roachpb.SERIALIZABLE, tc.clock)
	txn.Sequence = 100
	entry := roachpb.AbortCacheEntry{
		Key:       txn.Key,
		Timestamp: txn.Timestamp,
		Priority:  0,
	}
	if err := tc.rng.abortCache.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	args, h := heartbeatArgs(txn)
	// If the abort cache were active for this request, we'd catch a txn retry.
	// Instead, we expect the error from heartbeating a nonexistent txn.
	if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, "record not present") {
		t.Fatal(pErr)
	}
}

// TestEndTransactionDeadline verifies that EndTransaction respects the
// transaction deadline.
func TestEndTransactionDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := newTransaction("txn: "+strconv.Itoa(i), key, 1, roachpb.SERIALIZABLE, tc.clock)
		put := putArgs(key, key)

		_, header := beginTxnArgs(key, txn)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), header, &put); pErr != nil {
			t.Fatal(pErr)
		}
		txn.Writing = true

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
			_, pErr := tc.SendWrappedWith(etHeader, &etArgs)
			switch i {
			case 0:
				// No deadline.
				if pErr != nil {
					t.Error(pErr)
				}
			case 1:
				// Past deadline.
				if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
					t.Errorf("expected TransactionAbortedError but got %T: %s", pErr, pErr)
				}
			case 2:
				// Equal deadline.
				if pErr != nil {
					t.Error(pErr)
				}
			case 3:
				// Future deadline.
				if pErr != nil {
					t.Error(pErr)
				}
			}
		}
	}
}

// TestEndTransactionDeadline_1PC verifies that a transaction that
// exceeded its deadline will be aborted even when one phase commit is
// applicable.
func TestEndTransactionDeadline_1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	bt, _ := beginTxnArgs(key, txn)
	put := putArgs(key, []byte("value"))
	et, etH := endTxnArgs(txn, true)
	// Past deadline.
	ts := txn.Timestamp.Prev()
	et.Deadline = &ts

	var ba roachpb.BatchRequest
	ba.Header = etH
	ba.Add(&bt, &put, &et)
	_, pErr := tc.Sender().Send(context.Background(), ba)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
		t.Errorf("expected TransactionAbortedError but got %T: %s", pErr, pErr)
	}
}

// TestEndTransactionWithMalformedSplitTrigger verifies an
// EndTransaction call with a malformed commit trigger fails.
func TestEndTransactionWithMalformedSplitTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("foo")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	pArgs := putArgs(key, []byte("only here to make this a rw transaction"))
	txn.Sequence++
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{
		Txn: txn,
	}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Writing = true

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
	if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, "range does not match splits") {
		t.Errorf("expected range does not match splits error; got %s", pErr)
	}
}

// TestEndTransactionBeforeHeartbeat verifies that a transaction
// can be committed/aborted before being heartbeat.
func TestEndTransactionBeforeHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}
		txn.Sequence++
		txn.Writing = true
		args, h := endTxnArgs(txn, commit)
		resp, pErr := tc.SendWrappedWith(h, &args)
		if pErr != nil {
			t.Error(pErr)
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
		txn.Epoch++ // need to fake a higher epoch to sneak past sequence cache
		txn.Sequence++
		hBA, h := heartbeatArgs(txn)

		resp, pErr = tc.SendWrappedWith(h, &hBA)
		if pErr != nil {
			t.Error(pErr)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != expStatus || hBR.Txn.LastHeartbeat != nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hBR)
		}
		key = roachpb.Key(key).Next()
	}
}

// TestEndTransactionAfterHeartbeat verifies that a transaction
// can be committed/aborted after being heartbeat.
func TestEndTransactionAfterHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}

		// Start out with a heartbeat to the transaction.
		hBA, h := heartbeatArgs(txn)
		txn.Sequence++

		resp, pErr := tc.SendWrappedWith(h, &hBA)
		if pErr != nil {
			t.Fatal(pErr)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != roachpb.PENDING || hBR.Txn.LastHeartbeat == nil {
			t.Errorf("unexpected heartbeat reply contents: %+v", hBR)
		}

		args, h := endTxnArgs(txn, commit)
		txn.Sequence++

		resp, pErr = tc.SendWrappedWith(h, &args)
		if pErr != nil {
			t.Error(pErr)
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
		key = key.Next()
	}
}

// TestEndTransactionWithPushedTimestamp verifies that txn can be
// ended (both commit or abort) correctly when the commit timestamp is
// greater than the transaction timestamp, depending on the isolation
// level.
func TestEndTransactionWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	key := roachpb.Key("a")
	for i, test := range testCases {
		pushee := newTransaction("pushee", key, 1, test.isolation, tc.clock)
		pusher := newTransaction("pusher", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee.Priority = 1
		pusher.Priority = 2 // pusher will win
		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, []byte("value"))
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}

		// Push pushee txn.
		pushTxn := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)
		pushTxn.Key = pusher.Key
		if _, pErr := tc.SendWrapped(&pushTxn); pErr != nil {
			t.Error(pErr)
		}

		// End the transaction with args timestamp moved forward in time.
		endTxn, h := endTxnArgs(pushee, test.commit)
		pushee.Sequence++
		resp, pErr := tc.SendWrappedWith(h, &endTxn)

		if test.expErr {
			if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
				t.Errorf("%d: expected retry error; got %s", i, pErr)
			}
		} else {
			if pErr != nil {
				t.Errorf("%d: unexpected error: %s", i, pErr)
			}
			expStatus := roachpb.COMMITTED
			if !test.commit {
				expStatus = roachpb.ABORTED
			}
			reply := resp.(*roachpb.EndTransactionResponse)
			if reply.Txn.Status != expStatus {
				t.Errorf("%d: expected transaction status to be %s; got %s", i, expStatus, reply.Txn.Status)
			}
		}
		key = key.Next()
	}
}

// TestEndTransactionWithIncrementedEpoch verifies that txn ended with
// a higher epoch (and priority) correctly assumes the higher epoch.
func TestEndTransactionWithIncrementedEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	_, btH := beginTxnArgs(key, txn)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Writing = true

	// Start out with a heartbeat to the transaction.
	hBA, h := heartbeatArgs(txn)
	txn.Sequence++

	_, pErr := tc.SendWrappedWith(h, &hBA)
	if pErr != nil {
		t.Error(pErr)
	}

	// Now end the txn with increased epoch and priority.
	args, h := endTxnArgs(txn, true)
	h.Txn.Epoch = txn.Epoch + 1
	h.Txn.Priority = txn.Priority + 1

	txn.Sequence++
	resp, pErr := tc.SendWrappedWith(h, &args)
	if pErr != nil {
		t.Error(pErr)
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	regressTS := tc.clock.Now()
	tc.manualClock.Set(1)
	txn := newTransaction("test", roachpb.Key(""), 1, roachpb.SERIALIZABLE, tc.clock)

	doesNotExist := roachpb.TransactionStatus(-1)

	testCases := []struct {
		key          roachpb.Key
		existStatus  roachpb.TransactionStatus
		existEpoch   uint32
		existTS      roachpb.Timestamp
		expErrRegexp string
	}{
		{roachpb.Key("a"), doesNotExist, txn.Epoch, txn.Timestamp, "does not exist"},
		{roachpb.Key("a"), roachpb.COMMITTED, txn.Epoch, txn.Timestamp, "txn \"test\" id=.*: already committed"},
		{roachpb.Key("b"), roachpb.ABORTED, txn.Epoch, txn.Timestamp, "txn aborted \"test\" id=.*"},
		{roachpb.Key("c"), roachpb.PENDING, txn.Epoch + 1, txn.Timestamp, "txn \"test\" id=.*: epoch regression: 0"},
		{roachpb.Key("d"), roachpb.PENDING, txn.Epoch, regressTS, `txn "test" id=.*: timestamp regression: 0.000000001,\d+`},
	}
	for i, test := range testCases {
		// Establish existing txn state by writing directly to range engine.
		existTxn := txn.Clone()
		existTxn.Key = test.key
		existTxn.Status = test.existStatus
		existTxn.Epoch = test.existEpoch
		existTxn.Timestamp = test.existTS
		txnKey := keys.TransactionKey(test.key, txn.ID)

		if test.existStatus != doesNotExist {
			if err := engine.MVCCPutProto(context.Background(), tc.rng.store.Engine(), nil, txnKey, roachpb.ZeroTimestamp,
				nil, &existTxn); err != nil {
				t.Fatal(err)
			}
		}

		// End the transaction, verify expected error.
		txn.Key = test.key
		args, h := endTxnArgs(txn, true)
		txn.Sequence++

		if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, test.expErrRegexp) {
			t.Errorf("%d: expected error:\n%s\nto match:\n%s", i, pErr, test.expErrRegexp)
		} else if txn := pErr.GetTxn(); txn != nil && txn.ID == nil {
			// Prevent regression of #5591.
			t.Fatalf("%d: received empty Transaction proto in error", i)
		}
	}
}

// TestEndTransactionRollbackAbortedTransaction verifies that no error
// is returned when a transaction that has already been aborted is
// rolled back by an EndTransactionRequest.
func TestEndTransactionRollbackAbortedTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(false)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("a")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	_, btH := beginTxnArgs(key, txn)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction by pushing it with a higher priority.
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	pusher.Priority = txn.Priority + 1 // will push successfully
	pushArgs := pushTxnArgs(pusher, btH.Txn, roachpb.PUSH_ABORT)
	if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Check if the intent has not yet been resolved.
	var ba roachpb.BatchRequest
	gArgs := getArgs(key)
	ba.Add(&gArgs)
	if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
		t.Fatal(err)
	}
	_, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected write intent error, but got %s", pErr)
	}

	// Abort the transaction again. No error is returned.
	args, h := endTxnArgs(txn, false)
	args.IntentSpans = []roachpb.Span{{Key: key}}
	resp, pErr := tc.SendWrappedWith(h, &args)
	if pErr != nil {
		t.Error(pErr)
	}
	reply := resp.(*roachpb.EndTransactionResponse)
	if reply.Txn.Status != roachpb.ABORTED {
		t.Errorf("expected transaction status to be ABORTED; got %s", reply.Txn.Status)
	}

	// Verify that the intent has been resolved.
	if _, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba); pErr != nil {
		t.Errorf("expected resolved intent, but got %s", pErr)
	}
}

// TestRaftReplayProtection verifies that non-transactional batches
// enjoy some protection from raft replays, but highlights an example
// where they won't.
func TestRaftReplayProtection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	incs := []int64{1, 3, 7}
	sum := 2 * incs[0]
	for _, n := range incs[1:] {
		sum += n
	}

	{
		// Start with an increment for key.
		incArgs := incrementArgs(key, incs[0])
		_, respH, pErr := SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{}, &incArgs)
		if pErr != nil {
			t.Fatal(pErr)
		}

		// Do an increment with timestamp to an earlier timestamp, but same key.
		// This will bump up to a higher timestamp than the original increment
		// and not surface a WriteTooOldError.
		h := roachpb.Header{Timestamp: respH.Timestamp.Prev()}
		_, respH, pErr = SendWrapped(tc.Sender(), tc.rng.context(context.Background()), h, &incArgs)
		if pErr != nil {
			t.Fatalf("unexpected error: %s", respH)
		}
		if expTS := h.Timestamp.Next().Next(); !respH.Timestamp.Equal(expTS) {
			t.Fatalf("expected too-old increment to advance two logical ticks to %s; got %s", expTS, respH.Timestamp)
		}

		// Do an increment with exact timestamp; should propagate write too
		// old error. This is assumed to be a replay because the timestamp
		// encountered is an exact duplicate and nothing came before the
		// increment in the batch.
		h.Timestamp = respH.Timestamp
		_, _, pErr = SendWrapped(tc.Sender(), tc.rng.context(context.Background()), h, &incArgs)
		if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
			t.Fatalf("expected WriteTooOldError; got %s", pErr)
		}
	}

	// Send a double increment in a batch. This should increment twice,
	// as the same key is being incremented in the same batch.
	var ba roachpb.BatchRequest
	for _, inc := range incs[1:] {
		incArgs := incrementArgs(key, inc)
		ba.Add(&incArgs)
	}
	br, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	if latest := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.IncrementResponse).NewValue; latest != sum {
		t.Fatalf("expected %d, got %d", sum, latest)
	}

	// Now resend the batch with the same timestamp; this should look
	// like the replay it is and surface a WriteTooOldError.
	ba.Timestamp = br.Timestamp
	_, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("expected WriteTooOldError; got %s", pErr)
	}

	// Send a DeleteRange & increment.
	incArgs := incrementArgs(key, 1)
	ba = roachpb.BatchRequest{}
	ba.Add(roachpb.NewDeleteRange(key, key.Next(), false))
	ba.Add(&incArgs)
	br, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Send exact same batch; the DeleteRange should trip up and
	// we'll get a replay error.
	ba.Timestamp = br.Timestamp
	_, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("expected WriteTooOldError; got %s", pErr)
	}

	// Send just a DeleteRange batch.
	ba = roachpb.BatchRequest{}
	ba.Add(roachpb.NewDeleteRange(key, key.Next(), false))
	br, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Now send it again; will not look like a replay because the
	// previous DeleteRange didn't leave any tombstones at this
	// timestamp for the replay to "trip" over.
	ba.Timestamp = br.Timestamp
	_, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}
}

// TestRaftReplayProtectionInTxn verifies that transactional batches
// enjoy protection from raft replays.
func TestRaftReplayProtectionInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	ctx := TestStoreContext()
	tc := testContext{}
	tc.StartWithStoreContext(t, ctx)
	defer tc.Stop()

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)

	// Send a batch with begin txn, put & end txn.
	var ba roachpb.BatchRequest
	bt, btH := beginTxnArgs(key, txn)
	put := putArgs(key, []byte("value"))
	et, _ := endTxnArgs(txn, true)
	et.IntentSpans = []roachpb.Span{{Key: key, EndKey: nil}}
	ba.Header = btH
	ba.Add(&bt)
	ba.Add(&put)
	ba.Add(&et)
	_, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	for i := 0; i < 2; i++ {
		// Reach in and manually send to raft (to simulate Raft replay) and
		// also avoid updating the timestamp cache; verify WriteTooOldError.
		ba.Timestamp = txn.OrigTimestamp
		pendingCmd, err := tc.rng.proposeRaftCommand(tc.rng.context(context.Background()), ba)
		if err != nil {
			t.Fatalf("%d: unexpected error: %s", i, err)
		}
		respWithErr := <-pendingCmd.done
		if _, ok := respWithErr.Err.GetDetail().(*roachpb.WriteTooOldError); !ok {
			t.Fatalf("%d: expected WriteTooOldError; got %s", i, respWithErr.Err)
		}
	}
}

// TestReplayProtection verifies that transactional replays cannot
// commit intents. The replay consists of an initial BeginTxn/Write
// batch and ends with an EndTxn batch.
func TestReplayProtection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	for i, iso := range []roachpb.IsolationType{roachpb.SERIALIZABLE, roachpb.SNAPSHOT} {
		key := roachpb.Key(fmt.Sprintf("a-%d", i))
		keyB := roachpb.Key(fmt.Sprintf("b-%d", i))
		txn := newTransaction("test", key, 1, iso, tc.clock)

		// Send a batch with put to key.
		var ba roachpb.BatchRequest
		bt, btH := beginTxnArgs(key, txn)
		put := putArgs(key, []byte("value"))
		ba.Header = btH
		ba.Add(&bt)
		ba.Add(&put)
		if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
			t.Fatal(err)
		}
		br, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba)
		if pErr != nil {
			t.Fatalf("%d: unexpected error: %s", i, pErr)
		}

		// Send a put for keyB.
		putB := putArgs(keyB, []byte("value"))
		putTxn := br.Txn.Clone()
		putTxn.Sequence++
		_, respH, pErr := SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Txn: &putTxn}, &putB)
		if pErr != nil {
			t.Fatal(pErr)
		}

		// EndTransaction.
		etTxn := respH.Txn.Clone()
		etTxn.Sequence++
		et, etH := endTxnArgs(&etTxn, true)
		et.IntentSpans = []roachpb.Span{{Key: key, EndKey: nil}, {Key: keyB, EndKey: nil}}
		if _, pErr := tc.SendWrappedWith(etH, &et); pErr != nil {
			t.Fatalf("%d: unexpected error: %s", i, pErr)
		}

		// Verify txn record is cleaned.
		var readTxn roachpb.Transaction
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		ok, err := engine.MVCCGetProto(context.Background(), tc.rng.store.Engine(), txnKey, roachpb.ZeroTimestamp, true /* consistent */, nil /* txn */, &readTxn)
		if err != nil || ok {
			t.Errorf("%d: expected transaction record to be cleared (%t): %s", i, ok, err)
		}

		// Now replay begin & put. BeginTransaction should fail with a replay error.
		_, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
		if _, ok := pErr.GetDetail().(*roachpb.TransactionReplayError); !ok {
			t.Errorf("%d: expected transaction replay for iso=%s; got %s", i, iso, pErr)
		}

		// Intent should not have been created.
		gArgs := getArgs(key)
		if _, pErr = tc.SendWrapped(&gArgs); pErr != nil {
			t.Errorf("%d: unexpected error reading key: %s", i, pErr)
		}

		// Send a put for keyB; should fail with a WriteTooOldError as this
		// will look like an obvious replay.
		_, _, pErr = SendWrapped(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Txn: &putTxn}, &putB)
		if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
			t.Errorf("%d: expected write too old error for iso=%s; got %s", i, iso, pErr)
		}

		// EndTransaction should also fail, but with a status error (does not exist).
		_, pErr = tc.SendWrappedWith(etH, &et)
		if _, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); !ok {
			t.Errorf("%d: expected transaction aborted for iso=%s; got %s", i, iso, pErr)
		}

		// Expect that keyB intent did not get written!
		gArgs = getArgs(keyB)
		if _, pErr = tc.SendWrapped(&gArgs); pErr != nil {
			t.Errorf("%d: unexpected error reading keyB: %s", i, pErr)
		}
	}
}

// TestEndTransactionGC verifies that a transaction record is immediately
// garbage-collected upon EndTransaction iff all of the supplied intents are
// local relative to the transaction record's location.
func TestEndTransactionLocalGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	tc := testContext{}
	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if filterArgs.Req.Method() == roachpb.GC {
				return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()

	splitKey := roachpb.RKey("c")
	splitTestRange(tc.store, splitKey, splitKey, t)
	key := roachpb.Key("a")
	putKey := key
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
		txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(putKey, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}
		putKey = putKey.Next() // for the next iteration
		args, h := endTxnArgs(txn, true)
		args.IntentSpans = test.intents
		txn.Sequence++
		if _, pErr := tc.SendWrappedWith(h, &args); pErr != nil {
			t.Fatal(pErr)
		}
		var readTxn roachpb.Transaction
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		ok, err := engine.MVCCGetProto(context.Background(), tc.rng.store.Engine(), txnKey, roachpb.ZeroTimestamp,
			true /* consistent */, nil /* txn */, &readTxn)
		if err != nil {
			t.Fatal(err)
		}
		if !ok != test.expGC {
			t.Errorf("%d: unexpected gc'ed: %t", i, !ok)
		}
	}
}

func setupResolutionTest(t *testing.T, tc testContext, key roachpb.Key,
	splitKey roachpb.RKey, commit bool) (*Replica, *roachpb.Transaction) {
	// Split the range and create an intent at splitKey and key.
	newRng := splitTestRange(tc.store, splitKey, splitKey, t)

	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	// These increments are not required, but testing feels safer when zero
	// values are unexpected.
	txn.Sequence++
	txn.Epoch++
	pArgs := putArgs(key, []byte("value"))
	h := roachpb.Header{Txn: txn}
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), h, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	{
		var ba roachpb.BatchRequest
		ba.Header = h
		if err := ba.SetActiveTimestamp(newRng.store.Clock().Now); err != nil {
			t.Fatal(err)
		}
		pArgs := putArgs(splitKey.AsRawKey(), []byte("value"))
		ba.Add(&pArgs)
		txn.Sequence++
		if _, pErr := newRng.Send(newRng.context(context.Background()), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// End the transaction and resolve the intents.
	args, h := endTxnArgs(txn, commit)
	args.IntentSpans = []roachpb.Span{{Key: key, EndKey: splitKey.Next().AsRawKey()}}
	txn.Sequence++
	if _, pErr := tc.SendWrappedWith(h, &args); pErr != nil {
		t.Fatal(pErr)
	}
	return newRng, txn
}

// TestEndTransactionResolveOnlyLocalIntents verifies that an end transaction
// request resolves only local intents within the same batch.
func TestEndTransactionResolveOnlyLocalIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tsc := TestStoreContext()
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntentRange &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}

	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()

	newRng, txn := setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Check if the intent in the other range has not yet been resolved.
	{
		var ba roachpb.BatchRequest
		gArgs := getArgs(splitKey)
		ba.Add(&gArgs)
		if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
			t.Fatal(err)
		}
		_, pErr := newRng.Send(newRng.context(context.Background()), ba)
		if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
			t.Errorf("expected write intent error, but got %s", pErr)
		}
	}

	txn.Sequence++
	hbArgs, h := heartbeatArgs(txn)
	reply, pErr := tc.SendWrappedWith(h, &hbArgs)
	if pErr != nil {
		t.Fatal(pErr)
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
// abort cache records are purged on both the local range and non-local range.
func TestEndTransactionDirectGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tc.Start(t)
	defer tc.Stop()

	rightRng, txn := setupResolutionTest(t, tc, key, splitKey, false /* generate abort cache entry */)

	util.SucceedsSoon(t, func() error {
		if gr, _, err := tc.rng.Get(context.Background(), tc.engine, roachpb.Header{}, roachpb.GetRequest{Span: roachpb.Span{Key: keys.TransactionKey(txn.Key, txn.ID)}}); err != nil {
			return err
		} else if gr.Value != nil {
			return util.Errorf("txn entry still there: %+v", gr)
		}

		var entry roachpb.AbortCacheEntry
		if aborted, err := tc.rng.abortCache.Get(context.Background(), tc.engine, txn.ID, &entry); err != nil {
			t.Fatal(err)
		} else if aborted {
			return util.Errorf("abort cache still populated: %v", entry)
		}
		if aborted, err := rightRng.abortCache.Get(context.Background(), tc.engine, txn.ID, &entry); err != nil {
			t.Fatal(err)
		} else if aborted {
			t.Fatalf("right-hand side abort cache still populated: %v", entry)
		}

		return nil
	})
}

// TestEndTransactionDirectGCFailure verifies that no immediate GC takes place
// if external intents can't be resolved (see also TestEndTransactionDirectGC).
func TestEndTransactionDirectGCFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	var count int64
	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntentRange &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				atomic.AddInt64(&count, 1)
				return roachpb.NewErrorWithTxn(util.Errorf("boom"), filterArgs.Hdr.Txn)
			} else if filterArgs.Req.Method() == roachpb.GC {
				t.Fatalf("unexpected GCRequest: %+v", filterArgs.Req)
			}
			return nil
		}
	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()

	setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Now test that no GCRequest is issued. We can't test that directly (since
	// it's completely asynchronous), so we first make sure ResolveIntent
	// happened and subsequently issue a bogus Put which is likely to make it
	// into Raft only after a rogue GCRequest (at least sporadically), which
	// would trigger a Fatal from the command filter.
	util.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&count) == 0 {
			return util.Errorf("intent resolution not attempted yet")
		} else if pErr := tc.store.DB().Put("panama", "banana"); pErr != nil {
			return pErr.GoError()
		}
		return nil
	})
}

// TestEndTransactionDirectGC_1PC runs a test similar to TestEndTransactionDirectGC
// for the case of a transaction which is contained in a single batch.
func TestEndTransactionDirectGC_1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, commit := range []bool{true, false} {
		func() {
			tc := testContext{}
			tc.Start(t)
			defer tc.Stop()

			key := roachpb.Key("a")
			txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
			bt, _ := beginTxnArgs(key, txn)
			put := putArgs(key, []byte("value"))
			et, etH := endTxnArgs(txn, commit)
			et.IntentSpans = []roachpb.Span{{Key: key}}

			var ba roachpb.BatchRequest
			ba.Header = etH
			ba.Add(&bt, &put, &et)
			br, err := tc.Sender().Send(context.Background(), ba)
			if err != nil {
				t.Fatalf("commit=%t: %s", commit, err)
			}
			etArgs, ok := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.EndTransactionResponse)
			if !ok || !etArgs.OnePhaseCommit {
				t.Errorf("commit=%t: expected one phase commit", commit)
			}

			var entry roachpb.AbortCacheEntry
			if aborted, err := tc.rng.abortCache.Get(context.Background(), tc.engine, txn.ID, &entry); err != nil {
				t.Fatal(err)
			} else if aborted {
				t.Fatalf("commit=%t: abort cache still populated: %v", commit, entry)
			}
		}()
	}
}

func TestReplicaResolveIntentNoWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var seen int32
	key := roachpb.Key("zresolveme")
	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(key) {
				atomic.StoreInt32(&seen, 1)
			}
			return nil
		}

	tc := testContext{}
	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()
	splitKey := roachpb.RKey("aa")
	setupResolutionTest(t, tc, roachpb.Key("a") /* irrelevant */, splitKey, true /* commit */)
	txn := newTransaction("name", key, 1, roachpb.SERIALIZABLE, tc.clock)
	txn.Status = roachpb.COMMITTED
	if pErr := tc.store.intentResolver.resolveIntents(context.Background(), tc.rng,
		[]roachpb.Intent{{
			Span:   roachpb.Span{Key: key},
			Txn:    txn.TxnMeta,
			Status: txn.Status,
		}}, false /* !wait */, false /* !poison; irrelevant */); pErr != nil {
		t.Fatal(pErr)
	}
	util.SucceedsSoon(t, func() error {
		if atomic.LoadInt32(&seen) > 0 {
			return nil
		}
		return fmt.Errorf("no intent resolution on %q so far", key)
	})
}

// TestAbortCachePoisonOnResolve verifies that when an intent is
// aborted, the abort cache on the respective Range is poisoned and
// the pushee is presented with a txn abort on its next contact with
// the Range in the same epoch.
func TestSequenceCachePoisonOnResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

		inc := func(actor *roachpb.Transaction, k roachpb.Key) (*roachpb.IncrementResponse, *roachpb.Error) {
			reply, pErr := maybeWrapWithBeginTransaction(tc.store, nil, roachpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, &roachpb.IncrementRequest{Span: roachpb.Span{Key: k}, Increment: 123})
			if pErr != nil {
				return nil, pErr
			}
			actor.Writing = true
			actor.Sequence++
			return reply.(*roachpb.IncrementResponse), nil
		}

		get := func(actor *roachpb.Transaction, k roachpb.Key) *roachpb.Error {
			actor.Sequence++
			_, pErr := client.SendWrappedWith(tc.store, nil, roachpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, &roachpb.GetRequest{Span: roachpb.Span{Key: k}})
			return pErr
		}

		// Write an intent (this also begins the pushee's transaction).
		if _, pErr := inc(pushee, key); pErr != nil {
			t.Fatal(pErr)
		}

		// Have the pusher run into the intent. That pushes our pushee and
		// resolves the intent, which in turn should poison the abort cache.
		var assert func(*roachpb.Error)
		if abort {
			// Write/Write conflict will abort pushee.
			if _, pErr := inc(pusher, key); pErr != nil {
				t.Fatal(pErr)
			}
			assert = func(pErr *roachpb.Error) {
				if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
					t.Fatalf("abort=%t, iso=%s: expected txn abort, got %s", abort, iso, pErr)
				}
			}
		} else {
			// Verify we're not poisoned.
			assert = func(pErr *roachpb.Error) {
				if pErr != nil {
					t.Fatalf("abort=%t, iso=%s: unexpected: %s", abort, iso, pErr)
				}
			}
		}

		// Our assert should be true for any reads or writes.
		pErr := get(pushee, key)
		assert(pErr)
		_, pErr = inc(pushee, key)
		assert(pErr)
		// Still poisoned (on any key on the Range).
		pErr = get(pushee, key.Next())
		assert(pErr)
		_, pErr = inc(pushee, key.Next())
		assert(pErr)

		// Pretend we're coming back. Increasing the epoch on an abort should
		// still fail obviously, while on no abort will succeed.
		pushee.Epoch++
		_, pErr = inc(pushee, roachpb.Key("b"))
		assert(pErr)
	}

	for _, abort := range []bool{false, true} {
		run(abort, roachpb.SERIALIZABLE)
		run(abort, roachpb.SNAPSHOT)
	}
}

// TestAbortCacheError verifies that roachpb.Errors returned by checkIfTxnAborted
// have txns that are identical to txns stored in Transaction{Retry,Aborted}Error.
func TestAbortCacheError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	txn := roachpb.Transaction{}
	txn.ID = uuid.NewV4()
	txn.Priority = 1
	txn.Sequence = 1
	txn.Timestamp = roachpb.Timestamp{WallTime: 1}

	key := roachpb.Key("k")
	ts := txn.Timestamp.Next()
	priority := int32(10)
	entry := roachpb.AbortCacheEntry{
		Key:       key,
		Timestamp: ts,
		Priority:  priority,
	}
	if err := tc.rng.abortCache.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	pErr := tc.rng.checkIfTxnAborted(context.Background(), tc.engine, txn)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); ok {
		expected := txn.Clone()
		expected.Timestamp = txn.Timestamp
		expected.Priority = priority
		if pErr.GetTxn() == nil || !reflect.DeepEqual(pErr.GetTxn(), &expected) {
			t.Errorf("txn does not match: %s vs. %s", pErr.GetTxn(), expected)
		}
	} else {
		t.Errorf("unexpected error: %s", pErr)
	}
}

// TestPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestPushTxnBadKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, roachpb.SERIALIZABLE, tc.clock)

	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)
	args.Key = pusher.Key

	if _, pErr := tc.SendWrapped(&args); !testutils.IsPError(pErr, ".*should match pushee.*") {
		t.Errorf("unexpected error %s", pErr)
	}
}

// TestPushTxnAlreadyCommittedOrAborted verifies success
// (noop) in event that pushee is already committed or aborted.
func TestPushTxnAlreadyCommittedOrAborted(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}
		// End the pushee's transaction.
		etArgs, h := endTxnArgs(pushee, status == roachpb.COMMITTED)
		pushee.Sequence++
		if _, pErr := tc.SendWrappedWith(h, &etArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Now try to push what's already committed or aborted.
		args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)
		resp, pErr := tc.SendWrapped(&args)
		if pErr != nil {
			t.Fatal(pErr)
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	ts1 := roachpb.Timestamp{WallTime: 1}
	ts2 := roachpb.Timestamp{WallTime: 2}
	testCases := []struct {
		startTS, ts, expTS roachpb.Timestamp
	}{
		// Noop.
		{ts1, ts1, ts1},
		// Move timestamp forward.
		{ts1, ts2, ts2},
		// Move timestamp backwards (has no effect).
		{ts2, ts1, ts2},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee.Priority = 1
		pushee.Epoch = 12345
		pusher.Priority = 2   // Pusher will win
		pusher.Writing = true // expected when a txn is heartbeat

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		pushee.Timestamp = test.startTS
		pushee.LastHeartbeat = &test.startTS
		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}

		// Now, attempt to push the transaction using updated timestamp.
		pushee.Timestamp = test.ts
		args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)

		resp, pErr := tc.SendWrapped(&args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*roachpb.PushTxnResponse)
		expTxn := pushee.Clone()
		expTxn.Epoch = pushee.Epoch // no change
		expTxn.Timestamp = test.expTS
		expTxn.Status = roachpb.ABORTED
		expTxn.LastHeartbeat = &test.startTS
		expTxn.Writing = true

		if !reflect.DeepEqual(expTxn, reply.PusheeTxn) {
			t.Fatalf("unexpected push txn in trial %d; expected:\n%+v\ngot:\n%+v", i, expTxn, reply.PusheeTxn)
		}
	}
}

// TestPushTxnHeartbeatTimeout verifies that a txn which
// hasn't been heartbeat within 2x the heartbeat interval can be
// pushed/aborted.
func TestPushTxnHeartbeatTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		// Avoid using 0 as currentTime since our manualClock is at 0 and we
		// don't want to have outcomes depend on random logical ticks.
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, 1, roachpb.PUSH_QUERY, true},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, ns, roachpb.PUSH_QUERY, true},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_QUERY, true},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_TIMESTAMP, false},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_ABORT, false},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_TOUCH, false},
		{roachpb.ZeroTimestamp, ns * 2, roachpb.PUSH_QUERY, true},
		{ts, ns*2 + 1, roachpb.PUSH_TIMESTAMP, false},
		{ts, ns*2 + 1, roachpb.PUSH_ABORT, false},
		{ts, ns*2 + 1, roachpb.PUSH_TOUCH, false},
		{ts, ns*2 + 1, roachpb.PUSH_QUERY, true},
		{ts, ns*2 + 2, roachpb.PUSH_TIMESTAMP, true},
		{ts, ns*2 + 2, roachpb.PUSH_ABORT, true},
		{ts, ns*2 + 2, roachpb.PUSH_TOUCH, true},
		{ts, ns*2 + 2, roachpb.PUSH_QUERY, true},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pushee := newTransaction(fmt.Sprintf("test-%d", i), key, 1, roachpb.SERIALIZABLE, tc.clock)
		pusher := newTransaction("pusher", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		if !test.heartbeat.Equal(roachpb.ZeroTimestamp) {
			pushee.LastHeartbeat = &test.heartbeat
		}
		_, btH := beginTxnArgs(key, pushee)
		btH.Timestamp = tc.rng.store.Clock().Now()
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatalf("%d: %s", i, pErr)
		}

		// Now, attempt to push the transaction with Now set to our current time.
		args := pushTxnArgs(pusher, pushee, test.pushType)
		args.Now = roachpb.Timestamp{WallTime: test.currentTime}
		args.PushTo = args.Now

		reply, pErr := tc.SendWrapped(&args)

		if test.expSuccess != (pErr == nil) {
			t.Fatalf("%d: expSuccess=%t; got pErr %s, reply %+v", i,
				test.expSuccess, pErr, reply)
		}
		if pErr != nil {
			if _, ok := pErr.GetDetail().(*roachpb.TransactionPushError); !ok {
				t.Errorf("%d: expected txn push error: %s", i, pErr)
			}
		} else if test.pushType != roachpb.PUSH_QUERY {
			if txn := reply.(*roachpb.PushTxnResponse).PusheeTxn; txn.Status != roachpb.ABORTED {
				t.Errorf("%d: expected aborted transaction, got %s", i, txn)
			}
		}
	}
}

// TestPushTxnNoTxn makes sure that no Txn is returned from PushTxn and that
// it and ResolveIntent{,Range} can not be carried out in a transaction.
func TestResolveIntentPushTxnReplyTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	b := tc.engine.NewBatch()
	defer b.Close()

	txn := newTransaction("test", roachpb.Key("test"), 1, roachpb.SERIALIZABLE, tc.clock)
	txnPushee := txn.Clone()
	txnPushee.Priority--
	pa := pushTxnArgs(txn, &txnPushee, roachpb.PUSH_ABORT)
	var ms engine.MVCCStats
	var ra roachpb.ResolveIntentRequest
	var rra roachpb.ResolveIntentRangeRequest

	ctx := context.Background()
	// Should not be able to push or resolve in a transaction.
	if _, err := tc.rng.PushTxn(ctx, b, &ms, roachpb.Header{Txn: txn}, pa); !testutils.IsError(err, errTransactionUnsupported.Error()) {
		t.Fatalf("transactional PushTxn returned unexpected error: %v", err)
	}
	if _, err := tc.rng.ResolveIntent(ctx, b, &ms, roachpb.Header{Txn: txn}, ra); !testutils.IsError(err, errTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntent returned unexpected error: %v", err)
	}
	if _, err := tc.rng.ResolveIntentRange(ctx, b, &ms, roachpb.Header{Txn: txn}, rra); !testutils.IsError(err, errTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntentRange returned unexpected error: %v", err)
	}

	// Should not get a transaction back from PushTxn. It used to erroneously
	// return args.PusherTxn.
	if reply, err := tc.rng.PushTxn(ctx, b, &ms, roachpb.Header{}, pa); err != nil {
		t.Fatal(err)
	} else if reply.Txn != nil {
		t.Fatalf("expected nil response txn, but got %s", reply.Txn)
	}
}

// TestPushTxnPriorities verifies that txns with lower
// priority are pushed; if priorities are equal, then the txns
// are ordered by txn timestamp, with the more recent timestamp
// being pushable.
// TODO(tschottdorf): we should have a randomized version of this test which
// also simulates the client proto and persisted record diverging. For example,
// clients may be using a higher timestamp for their push or the persisted
// record (which they are not using) might have a higher timestamp, and even
// in the presence of such skewed information, conflicts between two (or more)
// conflicting transactions must not deadlock (see #5685 for an example of this
// happening with older code).
func TestPushTxnPriorities(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		// Pusher with higher priority succeeds.
		{2, 1, ts1, ts1, roachpb.PUSH_TIMESTAMP, true},
		{2, 1, ts1, ts1, roachpb.PUSH_ABORT, true},
		// Pusher with lower priority fails.
		{1, 2, ts1, ts1, roachpb.PUSH_ABORT, false},
		{1, 2, ts1, ts1, roachpb.PUSH_TIMESTAMP, false},
		// Pusher with lower priority fails, even with older txn timestamp.
		{1, 2, ts1, ts2, roachpb.PUSH_ABORT, false},
		// Pusher has lower priority, but older txn timestamp allows success if
		// !abort since there's nothing to do.
		{1, 2, ts1, ts2, roachpb.PUSH_TIMESTAMP, true},
		// With same priorities, larger Txn ID wins. Timestamp does not matter
		// (unless it implies that nothing needs to be pushed in the first
		// place; see above).
		// Note: in this test, the pusher has the larger ID.
		{1, 1, ts1, ts1, roachpb.PUSH_ABORT, true},
		{1, 1, ts1, ts1, roachpb.PUSH_TIMESTAMP, true},
		{1, 1, ts2, ts1, roachpb.PUSH_ABORT, true},
		{1, 1, ts2, ts1, roachpb.PUSH_TIMESTAMP, true},
		// When touching, priority never wins.
		{2, 1, ts1, ts1, roachpb.PUSH_TOUCH, false},
		{1, 2, ts1, ts1, roachpb.PUSH_TOUCH, false},
		// When updating, priority always succeeds.
		{2, 1, ts1, ts1, roachpb.PUSH_QUERY, true},
		{1, 2, ts1, ts1, roachpb.PUSH_QUERY, true},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
		pusher.Priority = test.pusherPriority
		pushee.Priority = test.pusheePriority
		pusher.Timestamp = test.pusherTS
		pushee.Timestamp = test.pusheeTS
		// Make sure pusher ID is greater; if priorities and timestamps are the same,
		// the greater ID succeeds with push.
		if bytes.Compare(pusher.ID.GetBytes(), pushee.ID.GetBytes()) < 0 {
			pusher.ID, pushee.ID = pushee.ID, pusher.ID
		}

		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}
		// Now, attempt to push the transaction with intent epoch set appropriately.
		args := pushTxnArgs(pusher, pushee, test.pushType)

		_, pErr := tc.SendWrapped(&args)

		if test.expSuccess != (pErr == nil) {
			t.Errorf("expected success on trial %d? %t; got err %s", i, test.expSuccess, pErr)
		}
		if pErr != nil {
			if _, ok := pErr.GetDetail().(*roachpb.TransactionPushError); !ok {
				t.Errorf("expected txn push error: %s", pErr)
			}
		}
	}
}

// TestPushTxnPushTimestamp verifies that with args.Abort is
// false (i.e. for read/write conflict), the pushed txn keeps status
// PENDING, but has its txn Timestamp moved forward to the pusher's
// txn Timestamp + 1.
func TestPushTxnPushTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, roachpb.SERIALIZABLE, tc.clock)
	pusher.Priority = 2
	pushee.Priority = 1 // pusher will win
	pusher.Timestamp = roachpb.Timestamp{WallTime: 50, Logical: 25}
	pushee.Timestamp = roachpb.Timestamp{WallTime: 5, Logical: 1}

	key := roachpb.Key("a")
	_, btH := beginTxnArgs(key, pushee)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}
	pushee.Writing = true

	// Now, push the transaction with args.Abort=false.
	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)

	resp, pErr := tc.SendWrapped(&args)
	if pErr != nil {
		t.Errorf("unexpected error on push: %s", pErr)
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pusher := newTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, roachpb.SERIALIZABLE, tc.clock)
	pusher.Priority = 1
	pushee.Priority = 2 // pusher will lose
	pusher.Timestamp = roachpb.Timestamp{WallTime: 50, Logical: 0}
	pushee.Timestamp = roachpb.Timestamp{WallTime: 50, Logical: 1}

	key := roachpb.Key("a")
	_, btH := beginTxnArgs(key, pushee)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, push the transaction with args.Abort=false.
	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)

	resp, pErr := tc.SendWrapped(&args)
	if pErr != nil {
		t.Errorf("unexpected pError on push: %s", pErr)
	}
	reply := resp.(*roachpb.PushTxnResponse)
	if !reply.PusheeTxn.Timestamp.Equal(pushee.Timestamp) {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.Timestamp, reply.PusheeTxn.Timestamp)
	}
	if reply.PusheeTxn.Status != roachpb.PENDING {
		t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
	}
}

// TestPushTxnSerializableRestart simulates a transaction which is
// started at t=0, fails serializable commit due to a read at a key
// being written at t=1, is then restarted at the updated timestamp,
// but before the txn can be retried, it's pushed to t=2, an even
// higher timestamp. The test verifies that the serializable commit
// fails yet again, preventing regression of a bug in which we blindly
// overwrote the transaction record on BeginTransaction..
func TestPushTxnSerializableRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	pushee.Priority = 1
	pusher.Priority = 2 // pusher will win

	// Read from the key to increment the timestamp cache.
	gArgs := getArgs(key)
	if _, pErr := tc.SendWrapped(&gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Begin the pushee's transaction & write to key.
	btArgs, btH := beginTxnArgs(key, pushee)
	put := putArgs(key, []byte("foo"))
	resp, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put)
	if pErr != nil {
		t.Fatal(pErr)
	}
	pushee.Update(resp.Header().Txn)

	// Try to end the pushee's transaction; should get a retry failure.
	etArgs, h := endTxnArgs(pushee, true /* commit */)
	pushee.Sequence++
	_, pErr = tc.SendWrappedWith(h, &etArgs)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("expected retry error; got %s", pErr)
	}
	pusheeCopy := *pushee
	pushee.Restart(1, 1, pusher.Timestamp)

	// Next push pushee to advance timestamp of txn record.
	pusher.Timestamp = tc.rng.store.Clock().Now()
	args := pushTxnArgs(pusher, &pusheeCopy, roachpb.PUSH_TIMESTAMP)
	if _, pErr := tc.SendWrapped(&args); pErr != nil {
		t.Fatal(pErr)
	}

	// Try to end pushed transaction at restart timestamp, which is
	// earlier than its now-pushed timestamp. Should fail.
	var ba roachpb.BatchRequest
	pushee.Sequence++
	ba.Header.Txn = pushee
	ba.Add(&btArgs)
	ba.Add(&put)
	ba.Add(&etArgs)
	_, pErr = tc.Sender().Send(tc.rng.context(context.Background()), ba)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("expected retry error; got %s", pErr)
	}
	// Verify that the returned transaction has timestamp equal to the
	// pushed timestamp. This verifies that the BeginTransaction found
	// the pushed record and propagated it.
	if txn := pErr.GetTxn(); !txn.Timestamp.Equal(pusher.Timestamp.Next()) {
		t.Errorf("expected retry error txn timestamp %s; got %s", pusher.Timestamp, txn.Timestamp)
	}
}

// TestReplicaResolveIntentRange verifies resolving a range of intents.
func TestReplicaResolveIntentRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")}
	txn := newTransaction("test", keys[0], 1, roachpb.SERIALIZABLE, tc.clock)

	// Put two values transactionally.
	for _, key := range keys {
		pArgs := putArgs(key, []byte("value1"))
		txn.Sequence++
		if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Resolve the intents.
	rArgs := &roachpb.ResolveIntentRangeRequest{
		Span: roachpb.Span{
			Key:    roachpb.Key("a"),
			EndKey: roachpb.Key("c"),
		},
		IntentTxn: txn.TxnMeta,
		Status:    roachpb.COMMITTED,
	}
	if _, pErr := tc.SendWrapped(rArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Do a consistent scan to verify intents have been cleared.
	sArgs := scanArgs(roachpb.Key("a"), roachpb.Key("c"))
	reply, pErr := tc.SendWrapped(&sArgs)
	if pErr != nil {
		t.Fatalf("unexpected error on scan: %s", pErr)
	}
	sReply := reply.(*roachpb.ScanResponse)
	if len(sReply.Rows) != 2 {
		t.Errorf("expected 2 rows; got %v", sReply.Rows)
	}
}

func verifyRangeStats(eng engine.Engine, rangeID roachpb.RangeID, expMS engine.MVCCStats, t *testing.T) {
	var ms engine.MVCCStats
	if err := engine.MVCCGetRangeStats(context.Background(), eng, rangeID, &ms); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expMS, ms) {
		f, l, _ := caller.Lookup(1)
		t.Errorf("%s:%d: expected stats \n  %+v;\ngot \n  %+v", f, l, expMS, ms)
	}
}

// TestReplicaStatsComputation verifies that commands executed against a
// range update the range stat counters. The stat values are
// empirically derived; we're really just testing that they increment
// in the right ways, not the exact amounts. If the encodings change,
// will need to update this test.
func TestReplicaStatsComputation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	tc.Start(t)
	defer tc.Stop()

	// Put a value.
	pArgs := putArgs([]byte("a"), []byte("value1"))

	if _, pErr := tc.SendWrapped(&pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS := engine.MVCCStats{LiveBytes: 25, KeyBytes: 14, ValBytes: 11, IntentBytes: 0, LiveCount: 1, KeyCount: 1, ValCount: 1, IntentCount: 0, IntentAge: 0, GCBytesAge: 0, SysBytes: 83, SysCount: 2, LastUpdateNanos: 0}

	// Put a 2nd value transactionally.
	pArgs = putArgs([]byte("b"), []byte("value2"))

	// Consistent UUID needed for a deterministic SysBytes value. This is because
	// a random UUID could have a 0x00 byte that would be escaped by the encoding,
	// increasing the encoded size and throwing off statistics verification.
	uuid, err := uuid.FromString("ea5b9590-a157-421b-8b93-a4caa2c41137")
	if err != nil {
		t.Fatal(err)
	}
	txn := newTransaction("test", pArgs.Key, 1, roachpb.SERIALIZABLE, tc.clock)
	txn.Priority = 123 // So we don't have random values messing with the byte counts on encoding
	txn.ID = uuid

	if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = engine.MVCCStats{LiveBytes: 101, KeyBytes: 28, ValBytes: 73, IntentBytes: 23, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 1, IntentAge: 0, GCBytesAge: 0, SysBytes: 91, SysCount: 2, LastUpdateNanos: 0}
	verifyRangeStats(tc.engine, tc.rng.RangeID, expMS, t)

	// Resolve the 2nd value.
	rArgs := &roachpb.ResolveIntentRequest{
		Span: roachpb.Span{
			Key: pArgs.Key,
		},
		IntentTxn: txn.TxnMeta,
		Status:    roachpb.COMMITTED,
	}

	if _, pErr := tc.SendWrapped(rArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = engine.MVCCStats{LiveBytes: 50, KeyBytes: 28, ValBytes: 22, IntentBytes: 0, LiveCount: 2, KeyCount: 2, ValCount: 2, IntentCount: 0, IntentAge: 0, GCBytesAge: 0, SysBytes: 91, SysCount: 2, LastUpdateNanos: 0}
	verifyRangeStats(tc.engine, tc.rng.RangeID, expMS, t)

	// Delete the 1st value.
	dArgs := deleteArgs([]byte("a"))

	if _, pErr := tc.SendWrapped(&dArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = engine.MVCCStats{LiveBytes: 25, KeyBytes: 40, ValBytes: 22, IntentBytes: 0, LiveCount: 1, KeyCount: 2, ValCount: 3, IntentCount: 0, IntentAge: 0, GCBytesAge: 0, SysBytes: 91, SysCount: 2, LastUpdateNanos: 0}
	verifyRangeStats(tc.engine, tc.rng.RangeID, expMS, t)
}

// TestMerge verifies that the Merge command is behaving as
// expected. Merge semantics for different data types are tested more
// robustly at the engine level; this test is intended only to show
// that values passed to Merge are being merged.
func TestMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("mergedkey")
	stringArgs := []string{"a", "b", "c", "d"}
	stringExpected := "abcd"

	for _, str := range stringArgs {
		mergeArgs := internalMergeArgs(key, roachpb.MakeValueFromString(str))

		if _, pErr := tc.SendWrapped(&mergeArgs); pErr != nil {
			t.Fatalf("unexpected error from Merge: %s", pErr)
		}
	}

	getArgs := getArgs(key)

	reply, pErr := tc.SendWrapped(&getArgs)
	if pErr != nil {
		t.Fatalf("unexpected error from Get: %s", pErr)
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.DisableRaftLogQueue(true)

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, pErr := tc.SendWrapped(&args); pErr != nil {
			t.Fatal(pErr)
		}
		idx, err := tc.rng.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	rangeID := tc.rng.RangeID

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// FirstIndex has changed.
	firstIndex, err := tc.rng.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != indexes[5] {
		t.Errorf("expected firstIndex == %d, got %d", indexes[5], firstIndex)
	}

	// We can still get what remains of the log.
	tc.rng.mu.Lock()
	entries, err := tc.rng.Entries(indexes[5], indexes[9], math.MaxUint64)
	tc.rng.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != int(indexes[9]-indexes[5]) {
		t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
	}

	// But any range that includes the truncated entries returns an error.
	tc.rng.mu.Lock()
	_, err = tc.rng.Entries(indexes[4], indexes[9], math.MaxUint64)
	tc.rng.mu.Unlock()
	if err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// The term of the last truncated entry is still available.
	tc.rng.mu.Lock()
	term, err := tc.rng.Term(indexes[4])
	tc.rng.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 {
		t.Errorf("invalid term 0 for truncated entry")
	}

	// The terms of older entries are gone.
	tc.rng.mu.Lock()
	_, err = tc.rng.Term(indexes[3])
	tc.rng.mu.Unlock()
	if err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// Truncating logs that have already been truncated should not return an
	// error.
	truncateArgs = truncateLogArgs(indexes[3], rangeID)
	if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Truncating logs that have the wrong rangeID included should not return
	// an error but should not truncate any logs.
	truncateArgs = truncateLogArgs(indexes[9], rangeID+1)
	if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
		t.Fatal(pErr)
	}

	tc.rng.mu.Lock()
	// The term of the last truncated entry is still available.
	term, err = tc.rng.Term(indexes[4])
	tc.rng.mu.Unlock()
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("k")
	value := []byte("quack")
	pArgs := putArgs(key, value)

	if _, pErr := tc.SendWrapped(&pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	val := roachpb.MakeValueFromString("moo")
	args := roachpb.ConditionalPutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpValue: &val,
	}

	_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: roachpb.MinTimestamp}, &args)

	if cErr, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); pErr == nil || !ok {
		t.Fatalf("expected ConditionFailedError, got %T with content %+v",
			pErr, pErr)
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
	defer leaktest.AfterTest(t)()
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	var appliedIndex uint64
	var sum int64
	for i := int64(1); i <= 10; i++ {
		args := incrementArgs([]byte("a"), i)

		resp, pErr := tc.SendWrapped(&args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*roachpb.IncrementResponse)
		sum += i

		if reply.NewValue != sum {
			t.Errorf("expected %d, got %d", sum, reply.NewValue)
		}

		tc.rng.mu.Lock()
		newAppliedIndex := tc.rng.mu.appliedIndex
		tc.rng.mu.Unlock()
		if newAppliedIndex <= appliedIndex {
			t.Errorf("appliedIndex did not advance. Was %d, now %d", appliedIndex, newAppliedIndex)
		}
		appliedIndex = newAppliedIndex
	}
}

// TestReplicaCorruption verifies that a replicaCorruptionError correctly marks
// the range as corrupt.
func TestReplicaCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsc := TestStoreContext()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storageutils.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Header().Key.Equal(roachpb.Key("boom")) {
				return roachpb.NewError(newReplicaCorruptionError())
			}
			return nil
		}

	tc := testContext{}
	tc.StartWithStoreContext(t, tsc)
	defer tc.Stop()

	// First send a regular command.
	args := putArgs(roachpb.Key("test1"), []byte("value"))
	if _, pErr := tc.SendWrapped(&args); pErr != nil {
		t.Fatal(pErr)
	}

	// maybeSetCorrupt should have been called.
	args = putArgs(roachpb.Key("boom"), []byte("value"))
	_, pErr := tc.SendWrapped(&args)
	if !testutils.IsPError(pErr, "replica corruption \\(processed=true\\)") {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// TODO(bdarnell): when maybeSetCorrupt is finished verify that future commands fail too.
}

// TestChangeReplicasDuplicateError tests that a replica change that would
// use a NodeID twice in the replica configuration fails.
func TestChangeReplicasDuplicateError(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

// TestReplicaDanglingMetaIntent creates a dangling intent on a meta2
// record and verifies that RangeLookup requests behave
// appropriately. Normally, the old value and a write intent error
// should be returned. If IgnoreIntents is specified, then a random
// choice of old or new is returned with no error.
// TODO(tschottdorf): add a test in which there is a dangling intent on a
// descriptor we would've otherwise discarded in a reverse scan; verify that
// we don't erroneously return that descriptor (recently fixed bug).
func TestReplicaDanglingMetaIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	reply, pErr := tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, rlArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	rlReply = reply.(*roachpb.RangeLookupResponse)

	origDesc := rlReply.Ranges[0]
	newDesc := origDesc
	var err error
	newDesc.EndKey, err = keys.Addr(key)
	if err != nil {
		t.Fatal(err)
	}

	// Write the new descriptor as an intent.
	data, err := protoutil.Marshal(&newDesc)
	if err != nil {
		t.Fatal(err)
	}
	txn := newTransaction("test", key, 1, roachpb.SERIALIZABLE, tc.clock)
	// Officially begin the transaction. If not for this, the intent resolution
	// machinery would simply remove the intent we write below, see #3020.
	// We send directly to Replica throughout this test, so there's no danger
	// of the Store aborting this transaction (i.e. we don't have to set a high
	// priority).
	pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(key)), data)
	txn.Sequence++
	if _, pErr = maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now lookup the range; should get the value. Since the lookup is
	// inconsistent, there's no WriteIntentError.
	// Note that 'A' < 'a'.
	rlArgs.Key = keys.RangeMetaKey(roachpb.RKey{'A'})

	reply, pErr = tc.SendWrappedWith(roachpb.Header{
		Timestamp:       roachpb.MinTimestamp,
		ReadConsistency: roachpb.INCONSISTENT,
	}, rlArgs)
	if pErr != nil {
		t.Errorf("unexpected lookup error: %s", pErr)
	}
	rlReply = reply.(*roachpb.RangeLookupResponse)
	if !reflect.DeepEqual(rlReply.Ranges[0], origDesc) {
		t.Errorf("expected original descriptor %s; got %s", &origDesc, &rlReply.Ranges[0])
	}

	// Switch to consistent lookups, which should run into the intent.
	_, pErr = tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.CONSISTENT,
	}, rlArgs)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Fatalf("expected WriteIntentError, not %s", pErr)
	}

	// Try a single lookup with ConsiderIntents. Expect to see both descriptors.
	// First, try this consistently, which should not be allowed.
	rlArgs.ConsiderIntents = true
	_, pErr = tc.SendWrapped(rlArgs)
	if !testutils.IsPError(pErr, "can not read consistently and special-case intents") {
		t.Fatalf("wanted specific error, not %s", pErr)
	}

	// After changing back to inconsistent lookups, should be good to go.
	var origSeen, newSeen bool
	clonedRLArgs := *rlArgs
	reply, pErr = tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, &clonedRLArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	rlReply = reply.(*roachpb.RangeLookupResponse)
	for _, seen := range rlReply.Ranges {
		if reflect.DeepEqual(seen, origDesc) {
			origSeen = true
		} else if reflect.DeepEqual(seen, newDesc) {
			newSeen = true
		} else {
			t.Errorf("expected orig/new descriptor %s/%s; got %s", &origDesc, &newDesc, &seen)
		}
	}
	if !origSeen || !newSeen {
		t.Errorf("expected to see both original and new descriptor; saw original = %t, saw new = %t", origSeen, newSeen)
	}
}

// TestReplicaLookupUseReverseScan verifies the correctness of the results which are retrieved
// from RangeLookup by using ReverseScan.
func TestReplicaLookupUseReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	txn := newTransaction("test", roachpb.Key{}, 1, roachpb.SERIALIZABLE, tc.clock)
	for i, r := range testRanges {
		if i != withIntentRangeIndex {
			// Write the new descriptor as an intent.
			data, err := protoutil.Marshal(&r)
			if err != nil {
				t.Fatal(err)
			}
			pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(r.EndKey)), data)

			txn.Sequence++
			if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}
		}
	}

	// Resolve the intents.
	rArgs := &roachpb.ResolveIntentRangeRequest{
		Span: roachpb.Span{
			Key:    keys.RangeMetaKey(roachpb.RKey("a")),
			EndKey: keys.RangeMetaKey(roachpb.RKey("z")),
		},
		IntentTxn: txn.TxnMeta,
		Status:    roachpb.COMMITTED,
	}
	if _, pErr := tc.SendWrapped(rArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Get original meta2 descriptor.
	rlArgs := &roachpb.RangeLookupRequest{
		MaxRanges: 1,
		Reverse:   true,
	}
	var rlReply *roachpb.RangeLookupResponse

	// Test ReverseScan without intents.
	for _, c := range testCases {
		clonedRLArgs := *rlArgs
		clonedRLArgs.Key = keys.RangeMetaKey(roachpb.RKey(c.key))
		reply, pErr := tc.SendWrappedWith(roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &clonedRLArgs)
		if pErr != nil {
			t.Fatal(pErr)
		}
		rlReply = reply.(*roachpb.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if !(seen.StartKey.Equal(c.expected.StartKey) && seen.EndKey.Equal(c.expected.EndKey)) {
			t.Errorf("expected descriptor %s; got %s", &c.expected, &seen)
		}
	}

	// Write the new descriptor as an intent.
	intentRange := testRanges[withIntentRangeIndex]
	data, err := protoutil.Marshal(&intentRange)
	if err != nil {
		t.Fatal(err)
	}
	pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(intentRange.EndKey)), data)
	txn2 := newTransaction("test", roachpb.Key{}, 1, roachpb.SERIALIZABLE, tc.clock)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn2}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Test ReverseScan with intents.
	for _, c := range testCases {
		clonedRLArgs := *rlArgs
		clonedRLArgs.Key = keys.RangeMetaKey(roachpb.RKey(c.key))
		reply, pErr := tc.SendWrappedWith(roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &clonedRLArgs)
		if pErr != nil {
			t.Fatal(pErr)
		}
		rlReply = reply.(*roachpb.RangeLookupResponse)
		seen := rlReply.Ranges[0]
		if !(seen.StartKey.Equal(c.expected.StartKey) && seen.EndKey.Equal(c.expected.EndKey)) {
			t.Errorf("expected descriptor %s; got %s", &c.expected, &seen)
		}
	}
}

func TestReplicaLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	mustAddr := func(k roachpb.Key) roachpb.RKey {
		rk, err := keys.Addr(k)
		if err != nil {
			panic(err)
		}
		return rk
	}

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
		{key: mustAddr(keys.Meta1KeyMax), reverse: false, expected: expected},
		{key: mustAddr(keys.Meta2KeyMax), reverse: false, expected: nil},
		{key: mustAddr(keys.Meta1KeyMax), reverse: true, expected: expected},
		{key: mustAddr(keys.Meta2KeyMax), reverse: true, expected: expected},
	}

	for _, c := range testCases {
		resp, pErr := tc.SendWrapped(&roachpb.RangeLookupRequest{
			Span: roachpb.Span{
				Key: c.key.AsRawKey(),
			},
			MaxRanges: 1,
			Reverse:   c.reverse,
		})
		if pErr != nil {
			if c.expected != nil {
				t.Fatal(pErr)
			}
		} else {
			reply := resp.(*roachpb.RangeLookupResponse)
			if !reflect.DeepEqual(reply.Ranges, c.expected) {
				t.Fatalf("expected %+v, got %+v", c.expected, reply.Ranges)
			}
		}
	}
}

// TestRequestLeaderEncounterGroupDeleteError verifies that a request leader proposal which fails with
// RaftGroupDeletedError is converted to a RangeNotFoundError in the Store.
func TestRequestLeaderEncounterGroupDeleteError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{
		rng: &Replica{},
	}
	tc.Start(t)
	defer tc.Stop()

	// Mock proposeRaftCommand to return an roachpb.RaftGroupDeletedError.
	proposeRaftCommandFn := func(*pendingCmd) error {
		return &roachpb.RaftGroupDeletedError{}
	}
	rng, err := NewReplica(testRangeDescriptor(), tc.store, 0)

	rng.mu.Lock()
	rng.mu.proposeRaftCommandFn = proposeRaftCommandFn
	rng.mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}
	if err := tc.store.AddReplicaTest(rng); err != nil {
		t.Fatal(err)
	}

	gArgs := getArgs(roachpb.Key("a"))
	// Force the read command request a new lease.
	clock := tc.clock
	ts := clock.Update(clock.Now().Add(leaseExpiry(tc.rng), 0))
	_, pErr := client.SendWrappedWith(tc.store, nil, roachpb.Header{
		Timestamp: ts,
		RangeID:   1,
	}, &gArgs)
	if _, ok := pErr.GetDetail().(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expected a RangeNotFoundError, get %s", pErr)
	}
}

func TestIntentIntersect(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

// TestBatchErrorWithIndex tests that when an individual entry in a
// batch results in an error with an index, the index of this command
// is stored into the error.
func TestBatchErrorWithIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	if _, pErr := tc.Sender().Send(tc.rng.context(context.Background()), ba); pErr == nil {
		t.Fatal("expected an error")
	} else if pErr.Index == nil || pErr.Index.Index != 1 || !testutils.IsPError(pErr, "unexpected value") {
		t.Fatalf("invalid index or error type: %s", pErr)
	}

}

// TestReplicaLoadSystemConfigSpanIntent verifies that intents on the SystemConfigSpan
// cause an error, but trigger asynchronous cleanup.
func TestReplicaLoadSystemConfigSpanIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	scStartSddr, err := keys.Addr(keys.SystemConfigSpan.Key)
	if err != nil {
		t.Fatal(err)
	}
	rng := tc.store.LookupReplica(scStartSddr, nil)
	if rng == nil {
		t.Fatalf("no replica contains the SystemConfig span")
	}

	// Create a transaction and write an intent to the system
	// config span.
	key := keys.SystemConfigSpan.Key
	_, btH := beginTxnArgs(key, newTransaction("test", key, 1, roachpb.SERIALIZABLE, rng.store.Clock()))
	btH.Txn.Priority = 1 // low so it can be pushed
	put := putArgs(key, []byte("foo"))
	if _, pErr := maybeWrapWithBeginTransaction(tc.Sender(), tc.rng.context(context.Background()), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction so that the async intent resolution caused
	// by loading the system config span doesn't waste any time in
	// clearing the intent.
	pusher := newTransaction("test", key, 1, roachpb.SERIALIZABLE, rng.store.Clock())
	pusher.Priority = 2 // will push successfully
	pushArgs := pushTxnArgs(pusher, btH.Txn, roachpb.PUSH_ABORT)
	if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify that the intent trips up loading the SystemConfig data.
	if _, _, err := rng.loadSystemConfigSpan(); err != errSystemConfigIntent {
		t.Fatal(err)
	}

	// In the loop, wait until the intent is aborted. Then write a "real" value
	// there and verify that we can now load the data as expected.
	v := roachpb.MakeValueFromString("foo")
	util.SucceedsSoon(t, func() error {
		if err := engine.MVCCPut(context.Background(), rng.store.Engine(), &engine.MVCCStats{},
			keys.SystemConfigSpan.Key, rng.store.Clock().Now(), v, nil); err != nil {
			return err
		}

		kvs, _, err := rng.loadSystemConfigSpan()
		if err != nil {
			return err
		}

		if len(kvs) != 1 || !bytes.Equal(kvs[0].Key, keys.SystemConfigSpan.Key) {
			return util.Errorf("expected only key %s in SystemConfigSpan map: %+v", keys.SystemConfigSpan.Key, kvs)
		}
		return nil
	})
}

func TestReplicaDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	rep, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// First try and fail with an outdated descriptor.
	origDesc := rep.Desc()
	newDesc := protoutil.Clone(origDesc).(*roachpb.RangeDescriptor)
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.DisableRaftLogQueue(true)

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, pErr := tc.SendWrapped(&args); pErr != nil {
			t.Fatal(pErr)
		}
		idx, err := tc.rng.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	rng := tc.rng
	rangeID := rng.RangeID

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
		t.Fatal(pErr)
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
		rng.mu.Lock()
		ents, err := rng.Entries(tc.lo, tc.hi, tc.maxBytes)
		rng.mu.Unlock()
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
	rng.mu.Lock()
	if _, err := rng.Entries(indexes[9], indexes[5], 0); err == nil {
		t.Errorf("15: error expected, got none")
	}
	rng.mu.Unlock()

	// Case 16: add a gap to the indexes.
	if err := engine.MVCCDelete(context.Background(), tc.store.Engine(), nil, keys.RaftLogKey(rangeID, indexes[6]), roachpb.ZeroTimestamp,
		nil); err != nil {
		t.Fatal(err)
	}

	rng.mu.Lock()
	defer rng.mu.Unlock()
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
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.DisableRaftLogQueue(true)

	rng := tc.rng
	rangeID := rng.RangeID

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, pErr := tc.SendWrapped(&args); pErr != nil {
			t.Fatal(pErr)
		}
		idx, err := tc.rng.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
		t.Fatal(pErr)
	}

	rng.mu.Lock()
	defer rng.mu.Unlock()

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

func TestGCIncorrectRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Split range into two ranges.
	splitKey := roachpb.RKey("c")
	rng1 := tc.rng
	rng2 := splitTestRange(tc.store, splitKey, splitKey, t)

	// Write a key to range 2 at two different timestamps so we can
	// GC the earlier timestamp without needing to delete it.
	key := splitKey.PrefixEnd().AsRawKey()
	val := []byte("value")
	putReq := putArgs(key, val)
	ts1 := makeTS(1, 0)
	ts2 := makeTS(2, 0)
	ts1Header := roachpb.Header{Timestamp: ts1}
	ts2Header := roachpb.Header{Timestamp: ts2}
	if _, pErr := client.SendWrappedWith(rng2, rng2.context(context.Background()), ts1Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}
	if _, pErr := client.SendWrappedWith(rng2, rng2.context(context.Background()), ts2Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}

	// Send GC request to range 1 for the key on range 2, which
	// should succeed even though it doesn't contain the key, because
	// the request for the incorrect key will be silently dropped.
	gKey := gcKey(key, ts1)
	gcReq := gcArgs(rng1.Desc().StartKey, rng1.Desc().EndKey, gKey)
	if _, pErr := client.SendWrappedWith(rng1, rng1.context(context.Background()), roachpb.Header{Timestamp: tc.clock.Now()}, &gcReq); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to incorrect range: %s", pErr)
	}

	// Make sure the key still exists on range 2.
	getReq := getArgs(key)
	if res, pErr := client.SendWrappedWith(rng2, rng2.context(context.Background()), ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal == nil {
		t.Errorf("expected value %s to exists after GC to incorrect range but before GC to correct range, found %v", val, resVal)
	}

	// Send GC request to range 2 for the same key.
	gcReq = gcArgs(rng2.Desc().StartKey, rng2.Desc().EndKey, gKey)
	if _, pErr := client.SendWrappedWith(rng2, rng2.context(context.Background()), roachpb.Header{Timestamp: tc.clock.Now()}, &gcReq); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to correct range: %s", pErr)
	}

	// Make sure the key no longer exists on range 2.
	if res, pErr := client.SendWrappedWith(rng2, rng2.context(context.Background()), ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal != nil {
		t.Errorf("expected value at key %s to no longer exist after GC to correct range, found value %v", key, resVal)
	}
}

// TestReplicaCancelRaft checks that it is possible to safely abandon Raft
// commands via a canceable context.Context.
func TestReplicaCancelRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, cancelEarly := range []bool{true, false} {
		func() {
			// Pick a key unlikely to be used by background processes.
			key := []byte("acdfg")
			ctx, cancel := context.WithCancel(context.Background())
			tsc := TestStoreContext()
			if !cancelEarly {
				tsc.TestingKnobs.TestingCommandFilter =
					func(filterArgs storageutils.FilterArgs) *roachpb.Error {
						if !filterArgs.Req.Header().Key.Equal(key) {
							return nil
						}
						cancel()
						return nil
					}

			}
			tc := testContext{}
			tc.StartWithStoreContext(t, tsc)
			defer tc.Stop()
			if cancelEarly {
				cancel()
				tc.rng.mu.Lock()
				tc.rng.mu.proposeRaftCommandFn = func(*pendingCmd) error {
					return nil
				}
				tc.rng.mu.Unlock()
			}
			var ba roachpb.BatchRequest
			ba.Add(&roachpb.GetRequest{
				Span: roachpb.Span{Key: key},
			})
			if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
				t.Fatal(err)
			}
			br, pErr := tc.rng.addWriteCmd(ctx, ba, nil /* wg */)
			if pErr == nil {
				if !cancelEarly {
					// We cancelled the context while the command was already
					// being processed, so the client had to wait for successful
					// execution.
					return
				}
				t.Fatalf("expected an error, but got successful response %+v", br)
			}
			// If we cancelled the context early enough, we expect to receive a
			// corresponding error and not wait for the command.
			if !testutils.IsPError(pErr, context.Canceled.Error()) {
				t.Fatalf("unexpected error: %s", pErr)
			}
		}()
	}
}

// verify the checksum for the range and returrn it.
func verifyChecksum(t *testing.T, rng *Replica) []byte {
	id := uuid.MakeV4()
	args := roachpb.ComputeChecksumRequest{
		ChecksumID: id,
		Version:    replicaChecksumVersion,
	}
	_, err := rng.ComputeChecksum(context.Background(), nil, nil, roachpb.Header{}, args)
	if err != nil {
		t.Fatal(err)
	}
	c, ok := rng.getChecksum(id)
	if !ok {
		t.Fatalf("checksum for id = %v not found", id)
	}
	if c.checksum == nil {
		t.Fatal("couldn't compute checksum")
	}
	verifyArgs := roachpb.VerifyChecksumRequest{
		ChecksumID: id,
		Version:    replicaChecksumVersion,
		Checksum:   c.checksum,
	}
	_, err = rng.VerifyChecksum(context.Background(), nil, nil, roachpb.Header{}, verifyArgs)
	if err != nil {
		t.Fatal(err)
	}
	return c.checksum
}

func TestComputeVerifyChecksum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	rng := tc.rng

	incArgs := incrementArgs([]byte("a"), 23)
	if _, err := tc.SendWrapped(&incArgs); err != nil {
		t.Fatal(err)
	}
	initialChecksum := verifyChecksum(t, rng)

	// Getting a value will not affect the snapshot checksum
	gArgs := getArgs(roachpb.Key("a"))
	if _, err := tc.SendWrapped(&gArgs); err != nil {
		t.Fatal(err)
	}
	checksum := verifyChecksum(t, rng)

	if !bytes.Equal(initialChecksum, checksum) {
		t.Fatalf("changed checksum: e = %v, c = %v", initialChecksum, checksum)
	}

	// Modifying the range will change the checksum.
	incArgs = incrementArgs([]byte("a"), 5)
	if _, err := tc.SendWrapped(&incArgs); err != nil {
		t.Fatal(err)
	}
	checksum = verifyChecksum(t, rng)
	if bytes.Equal(initialChecksum, checksum) {
		t.Fatalf("same checksum: e = %v, c = %v", initialChecksum, checksum)
	}

	// Verify that a bad version/checksum sent will result in an error.
	id := uuid.MakeV4()
	args := roachpb.ComputeChecksumRequest{
		ChecksumID: id,
		Version:    replicaChecksumVersion,
	}
	_, err := rng.ComputeChecksum(context.Background(), nil, nil, roachpb.Header{}, args)
	if err != nil {
		t.Fatal(err)
	}
	// Set a callback for checksum mismatch panics.
	var panicked bool
	rng.store.ctx.TestingKnobs.BadChecksumPanic = func(diff []ReplicaSnapshotDiff) { panicked = true }

	// First test that sending a Verification request with a bad version and
	// bad checksum will return without panicking because of a bad checksum.
	verifyArgs := roachpb.VerifyChecksumRequest{
		ChecksumID: id,
		Version:    10000001,
		Checksum:   []byte("bad checksum"),
	}
	_, err = rng.VerifyChecksum(context.Background(), nil, nil, roachpb.Header{}, verifyArgs)
	if err != nil {
		t.Fatal(err)
	}
	if panicked {
		t.Fatal("VerifyChecksum panicked")
	}
	// Setting the correct version will verify the checksum see a
	// checksum mismatch and trigger a rerun of the consistency check,
	// but the second consistency check will succeed because the checksum
	// provided in the second consistency check is the correct one.
	verifyArgs.Version = replicaChecksumVersion
	_, err = rng.VerifyChecksum(context.Background(), nil, nil, roachpb.Header{}, verifyArgs)
	if err != nil {
		t.Fatal(err)
	}
	if panicked {
		t.Fatal("VerifyChecksum panicked")
	}

	// Repeat the same but provide a snapshot this time. This will
	// result in the checksum failure not running the second consistency
	// check; it will panic.
	verifyArgs.Snapshot = &roachpb.RaftSnapshotData{}
	_, err = rng.VerifyChecksum(context.Background(), nil, nil, roachpb.Header{}, verifyArgs)
	if err != nil {
		t.Fatal(err)
	}
	if !panicked {
		t.Fatal("VerifyChecksum didn't panic")
	}
	panicked = false

	id = uuid.MakeV4()
	// send a ComputeChecksum with a bad version doesn't result in a
	// computed checksum.
	args = roachpb.ComputeChecksumRequest{
		ChecksumID: id,
		Version:    23343434,
	}
	_, err = rng.ComputeChecksum(context.Background(), nil, nil, roachpb.Header{}, args)
	if err != nil {
		t.Fatal(err)
	}
	// Sending a VerifyChecksum with a bad checksum is a noop.
	verifyArgs = roachpb.VerifyChecksumRequest{
		ChecksumID: id,
		Version:    replicaChecksumVersion,
		Checksum:   []byte("bad checksum"),
		Snapshot:   &roachpb.RaftSnapshotData{},
	}
	_, err = rng.VerifyChecksum(context.Background(), nil, nil, roachpb.Header{}, verifyArgs)
	if err != nil {
		t.Fatal(err)
	}
	if panicked {
		t.Fatal("VerifyChecksum panicked")
	}
}

func TestNewReplicaCorruptionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i, tc := range []struct {
		errStruct *roachpb.ReplicaCorruptionError
		expErr    string
	}{
		{newReplicaCorruptionError(errors.New("foo"), nil, errors.New("bar"), nil), "replica corruption (processed=false): foo (caused by bar)"},
		{newReplicaCorruptionError(nil, nil, nil), "replica corruption (processed=false)"},
	} {
		// This uses fmt.Sprint because that ends up calling Error() and is the
		// intended use. A previous version of this test called String() directly
		// which called the wrong (reflection-based) implementation.
		if errStr := fmt.Sprint(tc.errStruct); errStr != tc.expErr {
			t.Errorf("%d: expected '%s' but got '%s'", i, tc.expErr, errStr)
		}
	}
}

func TestDiffRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if diff := diffRange(nil, nil); diff != nil {
		t.Fatalf("diff of nils =  %v", diff)
	}

	timestamp := roachpb.Timestamp{WallTime: 1729, Logical: 1}
	value := []byte("foo")

	// Construct the two snapshots.
	leaderSnapshot := &roachpb.RaftSnapshotData{
		KV: []roachpb.RaftSnapshotData_KeyValue{
			{Key: []byte("a"), Timestamp: timestamp, Value: value},
			{Key: []byte("abc"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcd"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcde"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, -1), Value: value},
			{Key: []byte("abcdefgh"), Timestamp: timestamp, Value: value},
			{Key: []byte("x"), Timestamp: timestamp, Value: value},
			{Key: []byte("y"), Timestamp: timestamp, Value: value},
		},
	}

	// No diff works.
	if diff := diffRange(leaderSnapshot, leaderSnapshot); diff != nil {
		t.Fatalf("diff of similar snapshots = %v", diff)
	}

	replicaSnapshot := &roachpb.RaftSnapshotData{
		KV: []roachpb.RaftSnapshotData_KeyValue{
			{Key: []byte("ab"), Timestamp: timestamp, Value: value},
			{Key: []byte("abc"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcde"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdef"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, 1), Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefgh"), Timestamp: timestamp, Value: value},
			{Key: []byte("x"), Timestamp: timestamp, Value: []byte("bar")},
			{Key: []byte("z"), Timestamp: timestamp, Value: value},
		},
	}

	// The expected diff.
	eDiff := []ReplicaSnapshotDiff{
		{Leader: true, Key: []byte("a"), Timestamp: timestamp, Value: value},
		{Leader: false, Key: []byte("ab"), Timestamp: timestamp, Value: value},
		{Leader: true, Key: []byte("abcd"), Timestamp: timestamp, Value: value},
		{Leader: false, Key: []byte("abcdef"), Timestamp: timestamp, Value: value},
		{Leader: false, Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, 1), Value: value},
		{Leader: true, Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, -1), Value: value},
		{Leader: true, Key: []byte("x"), Timestamp: timestamp, Value: value},
		{Leader: false, Key: []byte("x"), Timestamp: timestamp, Value: []byte("bar")},
		{Leader: true, Key: []byte("y"), Timestamp: timestamp, Value: value},
		{Leader: false, Key: []byte("z"), Timestamp: timestamp, Value: value},
	}

	diff := diffRange(leaderSnapshot, replicaSnapshot)
	if diff == nil {
		t.Fatalf("differing snapshots didn't reveal diff %v", diff)
	}
	if len(eDiff) != len(diff) {
		t.Fatalf("expected diff length different from diff (%d vs %d) , %v vs %v", len(eDiff), len(diff), eDiff, diff)
	}

	for i, e := range eDiff {
		v := diff[i]
		if e.Leader != v.Leader || !bytes.Equal(e.Key, v.Key) || !e.Timestamp.Equal(v.Timestamp) || !bytes.Equal(e.Value, v.Value) {
			t.Fatalf("diff varies at row %d, %v vs %v", i, e, v)
		}
	}
}
