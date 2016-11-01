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
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	// will be persisted but metadata will not.
	//
	// Tests which run in this mode play fast and loose; they want
	// a Replica which doesn't have too many moving parts, but then
	// may still exercise a sizable amount of code, be it by accident
	// or design. We bootstrap them here with what's absolutely
	// necessary to not immediately crash on a Raft command, but
	// nothing more.
	// If you read this and you're writing a new test, try not to
	// use this mode - it's deprecated and tends to get in the way
	// of new development.
	bootstrapRangeOnly
)

// leaseExpiry returns a duration in nanos after which any range lease the
// Replica may hold is expired. It is more precise than LeaseExpiration
// in that it returns the minimal duration necessary.
func leaseExpiry(rng *Replica) int64 {
	if l, _ := rng.getLease(); l != nil {
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
	cfg := TestStoreConfig()
	tc.StartWithStoreConfig(t, cfg)
}

// StartWithStoreConfig initializes the test context with a single
// range covering the entire keyspace.
func (tc *testContext) StartWithStoreConfig(t testing.TB, cfg StoreConfig) {
	tc.TB = t
	if tc.stopper == nil {
		tc.stopper = stop.NewStopper()
	}
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(tc.stopper)
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(cfg.AmbientCtx, &base.Config{Insecure: true}, nil, tc.stopper)
		server := rpc.NewServer(rpcContext) // never started
		tc.gossip = gossip.NewTest(1, rpcContext, server, nil, tc.stopper, metric.NewRegistry())
	}
	if tc.manualClock == nil {
		tc.manualClock = hlc.NewManualClock(0)
	}
	if tc.clock == nil {
		tc.clock = hlc.NewClock(tc.manualClock.UnixNano)
	}
	if tc.engine == nil {
		tc.engine = engine.NewInMem(roachpb.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
		tc.stopper.AddCloser(tc.engine)
	}
	if tc.transport == nil {
		tc.transport = NewDummyRaftTransport()
	}

	if tc.store == nil {
		cfg.Clock = tc.clock
		cfg.Gossip = tc.gossip
		cfg.Transport = tc.transport
		// Create a test sender without setting a store. This is to deal with the
		// circular dependency between the test sender and the store. The actual
		// store will be passed to the sender after it is created and bootstrapped.
		sender := &testSender{}
		cfg.DB = client.NewDB(sender)
		tc.store = NewStore(cfg, tc.engine, &roachpb.NodeDescriptor{NodeID: 1})
		if err := tc.store.Bootstrap(roachpb.StoreIdent{
			ClusterID: uuid.MakeV4(),
			NodeID:    1,
			StoreID:   1,
		}); err != nil {
			t.Fatal(err)
		}
		// Now that we have our actual store, monkey patch the sender used in cfg.DB.
		sender.store = tc.store
		// We created the store without a real KV client, so it can't perform splits.
		tc.store.splitQueue.SetDisabled(true)

		if tc.rng == nil && tc.bootstrapMode == bootstrapRangeWithMetadata {
			if err := tc.store.BootstrapRange(nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := tc.store.Start(context.Background(), tc.stopper); err != nil {
			t.Fatal(err)
		}
		tc.store.WaitForInit()
	}

	realRange := tc.rng == nil

	if realRange {
		if tc.bootstrapMode == bootstrapRangeOnly {
			testDesc := testRangeDescriptor()
			if _, err := writeInitialState(
				context.Background(),
				tc.store.Engine(),
				enginepb.MVCCStats{},
				*testDesc,
				raftpb.HardState{},
				&roachpb.Lease{},
			); err != nil {
				t.Fatal(err)
			}
			rng, err := NewReplica(testDesc, tc.store, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AddReplica(rng); err != nil {
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
		if ba.Timestamp == hlc.ZeroTimestamp {
			if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
				tc.Fatal(err)
			}
		}
		return ba
	})
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it
func (tc *testContext) SendWrappedWith(
	h roachpb.Header, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	return client.SendWrappedWith(context.Background(), tc.Sender(), h, args)
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
			return errors.Errorf("expected system config to be set")
		}
		return nil
	})

	return nil
}

func newTransaction(
	name string,
	baseKey roachpb.Key,
	userPriority roachpb.UserPriority,
	isolation enginepb.IsolationType,
	clock *hlc.Clock,
) *roachpb.Transaction {
	var offset int64
	var now hlc.Timestamp
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
			ba.Txn = newTransaction("txn", roachpb.Key("a"), 1, enginepb.SNAPSHOT, clock)
			if c.isWTO {
				ba.Txn.WriteTooOld = true
			}
			ba.Txn.Timestamp = ba.Txn.OrigTimestamp.Add(1, 0)
			if c.isTSOff {
				ba.Txn.Isolation = enginepb.SERIALIZABLE
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
	}

	// This test really only needs a hollow shell of a Replica.
	r := &Replica{}
	r.mu.TimedMutex = syncutil.MakeTimedMutex(defaultMuLogger)
	r.cmdQMu.TimedMutex = syncutil.MakeTimedMutex(defaultMuLogger)
	r.mu.state.Desc = desc
	r.rangeStr.store(0, desc)

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

func sendLeaseRequest(r *Replica, l *roachpb.Lease) error {
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.Add(&roachpb.RequestLeaseRequest{Lease: *l})
	ch, _, err := r.propose(context.TODO(), ba)
	if err == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor this to a more conventional error-handling pattern.
		err = (<-ch).Err.GoError()
	}
	return err
}

// TestReplicaReadConsistency verifies behavior of the range under
// different read consistencies. Note that this unittest plays
// fast and loose with granting range leases.
func TestReplicaReadConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Modify range descriptor to include a second replica; range lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := *tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(&rngDesc)

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
	txn := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.clock)

	if _, err := tc.SendWrappedWith(roachpb.Header{
		Txn:             txn,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); err == nil {
		t.Errorf("expected error on inconsistent read within a txn")
	}

	// Lose the lease and verify CONSISTENT reads receive NotLeaseHolderError
	// and INCONSISTENT reads work as expected.
	start := hlc.ZeroTimestamp.Add(leaseExpiry(tc.rng), 0)
	tc.manualClock.Set(start.WallTime)
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
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
	if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
		t.Errorf("expected not lease holder error; got %s", pErr)
	}

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); pErr != nil {
		t.Errorf("expected success reading with inconsistent: %s", pErr)
	}
}

// TestApplyCmdLeaseError verifies that when during application of a Raft
// command the proposing node no longer holds the range lease, an error is
// returned. This prevents regression of #1483.
func TestApplyCmdLeaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Modify range descriptor to include a second replica; range lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := *tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(&rngDesc)

	pArgs := putArgs(roachpb.Key("a"), []byte("asd"))

	// Lose the lease.
	start := hlc.ZeroTimestamp.Add(leaseExpiry(tc.rng), 0)
	tc.manualClock.Set(start.WallTime)
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
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
	if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
		t.Fatalf("expected not lease holder error in return, got %v", pErr)
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
	if pErr := newRng.redirectOnOrAcquireLease(context.Background()); pErr != nil {
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

// hasLease returns whether the most recent range lease was held by the given
// range replica and whether it's expired for the given timestamp.
func hasLease(rng *Replica, timestamp hlc.Timestamp) (owned bool, expired bool) {
	l, _ := rng.getLease()
	return l.OwnedBy(rng.store.StoreID()), !l.Covers(timestamp)
}

func TestReplicaLease(t *testing.T) {
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
	rngDesc := *tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(&rngDesc)

	// Test that leases with invalid times are rejected.
	// Start leases at a point that avoids overlapping with the existing lease.
	one := hlc.ZeroTimestamp.Add(time.Second.Nanoseconds(), 0)
	for _, lease := range []roachpb.Lease{
		{Start: one, StartStasis: one},
		{Start: one, StartStasis: one.Next(), Expiration: one},
	} {
		if _, _, err := tc.rng.RequestLease(context.Background(), tc.store.Engine(), nil,
			roachpb.Header{}, roachpb.RequestLeaseRequest{
				Lease: lease,
			}); !testutils.IsError(err, "illegal lease interval") {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if held, _ := hasLease(tc.rng, tc.clock.Now()); !held {
		t.Errorf("expected lease on range start")
	}
	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := tc.clock.Now()
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
		Start:       now.Add(10, 0),
		StartStasis: now.Add(20, 0),
		Expiration:  now.Add(20, 0),
		Replica:     secondReplica,
	}); err != nil {
		t.Fatal(err)
	}
	if held, expired := hasLease(tc.rng, tc.clock.Now().Add(15, 0)); held || expired {
		t.Errorf("expected second replica to have range lease")
	}

	{
		pErr := tc.rng.redirectOnOrAcquireLease(context.Background())
		if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok || lErr == nil {
			t.Fatalf("wanted NotLeaseHolderError, got %s", pErr)
		}
	}
	// Advance clock past expiration and verify that another has
	// range lease will not be true.
	tc.manualClock.Increment(21) // 21ns have passed
	if held, expired := hasLease(tc.rng, tc.clock.Now()); held || !expired {
		t.Errorf("expected another replica to have expired lease")
	}

	// Verify that command returns NotLeaseHolderError when lease is rejected.
	rng, err := NewReplica(testRangeDescriptor(), tc.store, 0)
	if err != nil {
		t.Fatal(err)
	}

	rng.mu.Lock()
	rng.mu.submitProposalFn = func(*ProposalData) error {
		return &roachpb.LeaseRejectedError{
			Message: "replica not found",
		}
	}
	rng.mu.Unlock()

	{
		if _, ok := rng.redirectOnOrAcquireLease(context.Background()).GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, err)
		}
	}
}

// TestReplicaNotLeaseHolderError verifies NotLeaderError when lease is rejected.
func TestReplicaNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Modify range descriptor to include a second replica; range lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := *tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(&rngDesc)

	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := tc.clock.Now()
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
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

		if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Errorf("%d: expected not lease holder error: %s", i, pErr)
		}
	}
}

// TestReplicaLeaseCounters verifies leaseRequest metrics counters are updated
// correctly after a lease request.
func TestReplicaLeaseCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(tschottdorf): need to fix leases for proposer-evaluated KV before fixing this test")
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	assert := func(actual, min, max int64) {
		if actual < min || actual > max {
			t.Fatal(errors.Errorf(
				"metrics counters actual=%d, expected=[%d,%d]", actual, min, max))
		}
	}
	metrics := tc.rng.store.metrics
	assert(metrics.LeaseRequestSuccessCount.Count(), 1, 1000)
	assert(metrics.LeaseRequestErrorCount.Count(), 0, 0)

	now := tc.clock.Now()
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
		Start:       now,
		StartStasis: now.Add(10, 0),
		Expiration:  now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	assert(metrics.LeaseRequestSuccessCount.Count(), 2, 1000)
	assert(metrics.LeaseRequestErrorCount.Count(), 0, 0)

	// Make lease request fail by providing an invalid ReplicaDescriptor.
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
		Start:       now,
		StartStasis: now.Add(10, 0),
		Expiration:  now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    99,
			StoreID:   99,
		},
	}); err == nil {
		t.Fatal("lease request did not fail on invalid ReplicaDescriptor")
	}

	assert(metrics.LeaseRequestSuccessCount.Count(), 2, 1000)
	assert(metrics.LeaseRequestErrorCount.Count(), 1, 1000)
}

// TestReplicaGossipConfigsOnLease verifies that config info is gossiped
// upon acquisition of the range lease.
func TestReplicaGossipConfigsOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Modify range descriptor to include a second replica; range lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := *tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(&rngDesc)

	// Write some arbitrary data in the system config span.
	key := keys.MakeTablePrefix(keys.MaxSystemConfigDescID)
	var val roachpb.Value
	val.SetInt(42)
	if err := engine.MVCCPut(context.Background(), tc.engine, nil, key, hlc.MinTimestamp, val, nil); err != nil {
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
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
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
	if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
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
			return errors.Errorf("expected system config to be set")
		}
		numValues := len(cfg.Values)
		if numValues != 1 {
			return errors.Errorf("num config values != 1; got %d", numValues)
		}
		if k := cfg.Values[numValues-1].Key; !k.Equal(key) {
			return errors.Errorf("invalid key for config value (%q != %q)", k, key)
		}
		return nil
	})
}

// TestReplicaTSCacheLowWaterOnLease verifies that the low water mark
// is set on the timestamp cache when the node is granted the lease holder
// lease after not holding it and it is not set when the node is
// granted the range lease when it was the last holder.
// TODO(andrei): rewrite this test to use a TestCluster so we can test that the
// cache gets the correct timestamp on all the replicas that get the lease at
// some point; now we're just testing the cache on the first replica.
func TestReplicaTSCacheLowWaterOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	// Disable raft log truncation which confuses this test.
	tc.store.SetRaftLogQueueActive(false)

	// Modify range descriptor to include a second replica; range lease can
	// only be obtained by Replicas which are part of the range descriptor. This
	// workaround is sufficient for the purpose of this test.
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	rngDesc := *tc.rng.Desc()
	rngDesc.Replicas = append(rngDesc.Replicas, secondReplica)
	tc.rng.setDescWithoutProcessUpdate(&rngDesc)

	tc.manualClock.Set(leaseExpiry(tc.rng))
	now := hlc.Timestamp{WallTime: tc.manualClock.UnixNano()}

	tc.rng.mu.Lock()
	baseRTS, _, _ := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil /* end */)
	tc.rng.mu.Unlock()
	baseLowWater := baseRTS.WallTime

	testCases := []struct {
		storeID     roachpb.StoreID
		start       hlc.Timestamp
		expiration  hlc.Timestamp
		expLowWater int64 // 0 for not expecting anything
		expErr      string
	}{
		// Grant the lease fresh.
		{storeID: tc.store.StoreID(),
			start: now, expiration: now.Add(10, 0)},
		// Renew the lease.
		{storeID: tc.store.StoreID(),
			start: now.Add(15, 0), expiration: now.Add(30, 0)},
		// Renew the lease but shorten expiration. This is silently ignored.
		{storeID: tc.store.StoreID(),
			start: now.Add(16, 0), expiration: now.Add(25, 0)},
		// Another Store attempts to get the lease, but overlaps. If the
		// previous lease expiration had worked, this would have too.
		{storeID: tc.store.StoreID() + 1,
			start: now.Add(29, 0), expiration: now.Add(50, 0),
			expErr: "overlaps previous"},
		// The other store tries again, this time without the overlap.
		{storeID: tc.store.StoreID() + 1,
			start: now.Add(31, 0), expiration: now.Add(50, 0),
			// The cache now moves to this other store, and we can't query that.
			expLowWater: 0},
		// Lease is regranted to this replica. The low-water mark is updated to the
		// beginning of the lease.
		{storeID: tc.store.StoreID(),
			start: now.Add(60, 0), expiration: now.Add(70, 0),
			// We expect 50, not 60, because the new lease is wound back to the end
			// of the previous lease.
			expLowWater: now.Add(50, 0).WallTime + baseLowWater},
	}

	for i, test := range testCases {
		if err := sendLeaseRequest(tc.rng, &roachpb.Lease{
			Start:       test.start,
			StartStasis: test.expiration.Add(-1, 0), // smaller than durations used
			Expiration:  test.expiration,
			Replica: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(test.storeID),
				NodeID:    roachpb.NodeID(test.storeID),
				StoreID:   test.storeID,
			},
		}); !(test.expErr == "" && err == nil || testutils.IsError(err, test.expErr)) {
			t.Fatalf("%d: unexpected error %v", i, err)
		}
		// Verify expected low water mark.
		tc.rng.mu.Lock()
		rTS, _, _ := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil)
		wTS, _, _ := tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("a"), nil)
		tc.rng.mu.Unlock()

		if test.expLowWater == 0 {
			continue
		}
		if rTS.WallTime != test.expLowWater || wTS.WallTime != test.expLowWater {
			t.Errorf("%d: expected low water %d; got maxRead=%d, maxWrite=%d", i, test.expLowWater, rTS.WallTime, wTS.WallTime)
		}
	}
}

// TestReplicaLeaseRejectUnknownRaftNodeID ensures that a replica cannot
// obtain the range lease if it is not part of the current range descriptor.
// TODO(mrtracy): This should probably be tested in client_raft_test package,
// using a real second store.
func TestReplicaLeaseRejectUnknownRaftNodeID(t *testing.T) {
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
	ba.Add(&roachpb.RequestLeaseRequest{Lease: *lease})
	ch, _, err := tc.rng.propose(context.Background(), ba)
	if err == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor to a more conventional error-handling pattern.
		// Remove ambiguity about where the "replica not found" error comes from.
		err = (<-ch).Err.GoError()
	}
	if !testutils.IsError(err, "replica not found") {
		t.Errorf("unexpected error obtaining lease for invalid store: %v", err)
	}
}

// TestReplicaDrainLease makes sure that no new leases are granted when
// the Store is in DrainLeases mode.
func TestReplicaDrainLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Acquire initial lease.
	if pErr := tc.rng.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}
	var slept atomic.Value
	slept.Store(false)
	if err := tc.stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
		// Wait just a bit so that the main thread can check that
		// DrainLeases blocks (false negatives are possible, but 10ms is
		// plenty to make this fail 99.999% of the time in practice).
		time.Sleep(10 * time.Millisecond)
		slept.Store(true)
		// Expire the lease (and any others that may race in before we drain).
		for {
			tc.manualClock.Increment(leaseExpiry(tc.rng))
			select {
			case <-time.After(10 * time.Millisecond): // real code would use Ticker
			case <-tc.stopper.ShouldQuiesce():
				return
			}
		}
	}); err != nil {
		t.Fatal(err)
	}

	if err := tc.store.DrainLeases(true); err != nil {
		t.Fatal(err)
	}
	if !slept.Load().(bool) {
		t.Fatal("DrainLeases returned with active lease")
	}
	tc.rng.mu.Lock()
	pErr := <-tc.rng.requestLeaseLocked(tc.clock.Now())
	tc.rng.mu.Unlock()
	_, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected NotLeaseHolderError, not %v", pErr)
	}
	if err := tc.store.DrainLeases(false); err != nil {
		t.Fatal(err)
	}
	// Newly unfrozen, leases work again.
	if pErr := tc.rng.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
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

func maybeWrapWithBeginTransaction(
	ctx context.Context, sender client.Sender, header roachpb.Header, req roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	if header.Txn == nil || header.Txn.Writing {
		return client.SendWrappedWith(ctx, sender, header, req)
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

	txn := newTransaction("test", key, 1 /* userPriority */, enginepb.SERIALIZABLE, tc.clock)
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
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), test.h, test.req); pErr != nil {
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

// TestReplicaNoGossipFromNonLeader verifies that a non-lease holder replica
// does not gossip configurations.
func TestReplicaNoGossipFromNonLeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)

	txn := newTransaction("test", key, 1 /* userPriority */, enginepb.SERIALIZABLE, tc.clock)
	req1 := putArgs(key, nil)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), roachpb.Header{
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

	// Increment the clock's timestamp to expire the range lease.
	tc.manualClock.Set(leaseExpiry(tc.rng))
	if lease, _ := tc.rng.getLease(); lease.Covers(tc.clock.Now()) {
		t.Fatal("range lease should have been expired")
	}

	// Make sure the information for db1 is not gossiped. Since obtaining
	// a lease updates the gossiped information, we do that.
	if pErr := tc.rng.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}
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

func getArgs(key []byte) roachpb.GetRequest {
	return roachpb.GetRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

func putArgs(key roachpb.Key, value []byte) roachpb.PutRequest {
	return roachpb.PutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

func cPutArgs(key roachpb.Key, value, expValue []byte) roachpb.ConditionalPutRequest {
	expV := roachpb.MakeValueFromBytes(expValue)
	return roachpb.ConditionalPutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpValue: &expV,
	}
}

func deleteArgs(key roachpb.Key) roachpb.DeleteRequest {
	return roachpb.DeleteRequest{
		Span: roachpb.Span{
			Key: key,
		},
	}
}

// readOrWriteArgs returns either get or put arguments depending on
// value of "read". Get for true; Put for false.
func readOrWriteArgs(key roachpb.Key, read bool) roachpb.Request {
	if read {
		gArgs := getArgs(key)
		return &gArgs
	}
	pArgs := putArgs(key, []byte("value"))
	return &pArgs
}

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

func beginTxnArgs(
	key []byte, txn *roachpb.Transaction,
) (_ roachpb.BeginTransactionRequest, h roachpb.Header) {
	h.Txn = txn
	return roachpb.BeginTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}, h
}

func endTxnArgs(
	txn *roachpb.Transaction, commit bool,
) (_ roachpb.EndTransactionRequest, h roachpb.Header) {
	h.Txn = txn
	return roachpb.EndTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key, // not allowed when going through TxnCoordSender, but we're not
		},
		Commit: commit,
	}, h
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType,
) roachpb.PushTxnRequest {
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

func heartbeatArgs(txn *roachpb.Transaction) (_ roachpb.HeartbeatTxnRequest, h roachpb.Header) {
	h.Txn = txn
	return roachpb.HeartbeatTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}, h
}

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

func gcKey(key roachpb.Key, timestamp hlc.Timestamp) roachpb.GCRequest_GCKey {
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

// TestOptimizePuts verifies that contiguous runs of puts and
// conditional puts are marked as "blind" if they're written
// to a virgin keyspace.
func TestOptimizePuts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	pArgs := make([]roachpb.PutRequest, optimizePutThreshold)
	cpArgs := make([]roachpb.ConditionalPutRequest, optimizePutThreshold)
	for i := 0; i < optimizePutThreshold; i++ {
		pArgs[i] = putArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"))
		cpArgs[i] = cPutArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"), []byte("0"))
	}
	incArgs := incrementArgs([]byte("inc"), 1)

	testCases := []struct {
		exKey    roachpb.Key
		reqs     []roachpb.Request
		expBlind []bool
	}{
		// No existing keys, single put.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0],
			},
			[]bool{
				false,
			},
		},
		// No existing keys, nine puts.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false,
			},
		},
		// No existing keys, ten puts.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at "0", ten conditional puts.
		{
			roachpb.Key("0"),
			[]roachpb.Request{
				&cpArgs[0], &cpArgs[1], &cpArgs[2], &cpArgs[3], &cpArgs[4], &cpArgs[5], &cpArgs[6], &cpArgs[7], &cpArgs[8], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at 11, mixed puts and conditional puts.
		{
			roachpb.Key("11"),
			[]roachpb.Request{
				&pArgs[0], &cpArgs[1], &pArgs[2], &cpArgs[3], &pArgs[4], &cpArgs[5], &pArgs[6], &cpArgs[7], &pArgs[8], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at 00, ten puts, expect nothing blind.
		{
			roachpb.Key("00"),
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false, false,
			},
		},
		// Existing key at 00, ten puts in reverse order, expect nothing blind.
		{
			roachpb.Key("00"),
			[]roachpb.Request{
				&pArgs[9], &pArgs[8], &pArgs[7], &pArgs[6], &pArgs[5], &pArgs[4], &pArgs[3], &pArgs[2], &pArgs[1], &pArgs[0],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false, false,
			},
		},
		// Existing key at 05, ten puts, expect first five puts are blind.
		{
			roachpb.Key("05"),
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, false, false, false, false, false,
			},
		},
		// No existing key, ten puts + inc + ten cputs.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
				&incArgs, &cpArgs[0], &cpArgs[1], &cpArgs[2], &cpArgs[3], &cpArgs[4], &cpArgs[5], &cpArgs[6], &cpArgs[7], &cpArgs[8], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
				false, false, false, false, false, false, false, false, false, false, false,
			},
		},
		// Duplicate put at 11th key; should see ten puts.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Duplicate cput at 11th key; should see ten puts.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Duplicate cput at 6th key; should see ten cputs.
		{
			nil,
			[]roachpb.Request{
				&cpArgs[0], &cpArgs[1], &cpArgs[2], &cpArgs[3], &cpArgs[4], &cpArgs[5], &cpArgs[6], &cpArgs[7], &cpArgs[8], &cpArgs[9], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
	}

	for i, c := range testCases {
		if c.exKey != nil {
			if err := engine.MVCCPut(context.Background(), tc.engine, nil, c.exKey,
				hlc.ZeroTimestamp, roachpb.MakeValueFromString("foo"), nil); err != nil {
				t.Fatal(err)
			}
		}
		batch := roachpb.BatchRequest{}
		for _, r := range c.reqs {
			batch.Add(r)
		}
		optimizePuts(tc.engine, batch.Requests, false)
		blind := []bool{}
		for _, r := range batch.Requests {
			switch t := r.GetInner().(type) {
			case *roachpb.PutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			case *roachpb.ConditionalPutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			default:
				blind = append(blind, false)
			}
		}
		if !reflect.DeepEqual(blind, c.expBlind) {
			t.Errorf("%d: expected %+v; got %+v", i, c.expBlind, blind)
		}
		if c.exKey != nil {
			if err := tc.engine.Clear(engine.MakeMVCCMetadataKey(c.exKey)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestAcquireLease verifies that the range lease is acquired
// for read and write methods, and eagerly renewed.
func TestAcquireLease(t *testing.T) {
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
		lease, _ := tc.rng.getLease()
		expStart := lease.Start
		tc.manualClock.Set(leaseExpiry(tc.rng))

		ts := tc.clock.Now().Next()
		if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, test); pErr != nil {
			t.Error(pErr)
		}
		if held, expired := hasLease(tc.rng, ts); !held || expired {
			t.Errorf("%d: expected lease acquisition", i)
		}
		lease, _ = tc.rng.getLease()
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
			newLease, _ := tc.rng.getLease()
			if !lease.StartStasis.Less(newLease.StartStasis) {
				return errors.Errorf("%d: lease did not get extended: %+v to %+v", i, lease, newLease)
			}
			return nil
		})
		tc.Stop()
		if t.Failed() {
			return
		}
	}
}

func TestLeaseConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const num = 5

	// The test was written to test this individual block below. The worry
	// was that this would NPE (it does not; proto.Clone is unusual in that it
	// returns the input value instead).
	{
		if protoutil.Clone((*roachpb.Error)(nil)).(*roachpb.Error) != nil {
			t.Fatal("could not clone nil *Error")
		}
	}

	// Testing concurrent range lease requests is still a good idea. We check
	// that they work and clone *Error, which prevents regression of #6111.
	const origMsg = "boom"
	for _, withError := range []bool{false, true} {
		func(withError bool) {
			tc := testContext{}
			tc.Start(t)
			defer tc.Stop()

			var wg sync.WaitGroup
			wg.Add(num)
			var active atomic.Value
			active.Store(false)

			var seen int32
			tc.rng.mu.Lock()
			tc.rng.mu.submitProposalFn = func(cmd *ProposalData) error {
				ll, ok := cmd.Cmd.Requests[0].
					GetInner().(*roachpb.RequestLeaseRequest)
				if !ok || !active.Load().(bool) {
					return defaultSubmitProposalLocked(tc.rng, cmd)
				}
				if c := atomic.AddInt32(&seen, 1); c > 1 {
					// Morally speaking, this is an error, but reproposals can
					// happen and so we warn (in case this trips the test up
					// in more unexpected ways).
					log.Infof(context.Background(), "reproposal of %+v", ll)
				}
				go func() {
					wg.Wait()
					tc.rng.mu.Lock()
					defer tc.rng.mu.Unlock()
					if withError {
						// When we complete the command, we have to remove it from the map;
						// otherwise its context (and tracing span) may be used after the
						// client cleaned up.
						delete(tc.rng.mu.proposals, cmd.idKey)
						cmd.done <- proposalResult{
							Err: roachpb.NewErrorf(origMsg),
						}
						return
					}
					if err := defaultSubmitProposalLocked(tc.rng, cmd); err != nil {
						panic(err) // unlikely, so punt on proper handling
					}
				}()
				return nil
			}
			tc.rng.mu.Unlock()

			active.Store(true)
			tc.manualClock.Increment(leaseExpiry(tc.rng))
			ts := tc.clock.Now()
			pErrCh := make(chan *roachpb.Error, num)
			for i := 0; i < num; i++ {
				if err := tc.stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
					tc.rng.mu.Lock()
					leaseCh := tc.rng.requestLeaseLocked(ts)
					tc.rng.mu.Unlock()
					wg.Done()
					pErr := <-leaseCh
					// Mutate the errors as we receive them to expose races.
					if pErr != nil {
						pErr.OriginNode = 0
					}
					pErrCh <- pErr
				}); err != nil {
					t.Fatal(err)
				}
			}

			pErrs := make([]*roachpb.Error, num)
			for i := range pErrs {
				// Make sure all of the responses are in (just so that we can
				// mess with the "original" error knowing that all of the
				// cloning must have happened by now).
				pErrs[i] = <-pErrCh
			}

			newMsg := "moob"
			for i, pErr := range pErrs {
				if withError != (pErr != nil) {
					t.Errorf("%d: wanted error: %t, got error %v", i, withError, pErr)
				}
				if testutils.IsPError(pErr, newMsg) {
					t.Errorf("%d: errors shared memory: %v", i, pErr)
				} else if testutils.IsPError(pErr, origMsg) {
					// Mess with anyone holding the same reference.
					pErr.Message = newMsg
				} else if pErr != nil {
					t.Errorf("%d: unexpected error: %s", i, pErr)
				}
			}
		}(withError)
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
	_, _, rOK := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil)
	_, _, wOK := tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("a"), nil)
	if rOK || wOK {
		t.Errorf("expected rOK=false and wOK=false; rOK=%t, wOK=%t", rOK, wOK)
	}
	tc.rng.mu.tsCache.ExpandRequests(hlc.ZeroTimestamp)
	rTS, _, rOK := tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("a"), nil)
	wTS, _, wOK := tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("a"), nil)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != 0 || !rOK || wOK {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, _, rOK = tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("b"), nil)
	wTS, _, wOK = tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("b"), nil)
	if rTS.WallTime != 0 || wTS.WallTime != t1.Nanoseconds() || rOK || !wOK {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, _, rOK = tc.rng.mu.tsCache.GetMaxRead(roachpb.Key("c"), nil)
	wTS, _, wOK = tc.rng.mu.tsCache.GetMaxWrite(roachpb.Key("c"), nil)
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
	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Hdr.UserPriority == 42 {
				blockingStart <- struct{}{}
				<-blockingDone
			}
			return nil
		}
	tc.StartWithStoreConfig(t, tsc)
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
		if err := tc.stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
			args := readOrWriteArgs(key1, test.cmd1Read)

			_, pErr := tc.SendWrappedWith(roachpb.Header{
				UserPriority: 42,
			}, args)

			if pErr != nil {
				t.Fatalf("test %d: %s", i, pErr)
			}
			close(cmd1Done)
		}); err != nil {
			t.Fatal(err)
		}
		// Wait for cmd1 to get into the command queue.
		<-blockingStart

		// First, try a command for same key as cmd1 to verify it blocks.
		cmd2Done := make(chan struct{})
		if err := tc.stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
			args := readOrWriteArgs(key1, test.cmd2Read)

			_, pErr := tc.SendWrapped(args)

			if pErr != nil {
				t.Fatalf("test %d: %s", i, pErr)
			}
			close(cmd2Done)
		}); err != nil {
			t.Fatal(err)
		}

		// Next, try read for a non-impacted key--should go through immediately.
		cmd3Done := make(chan struct{})
		if err := tc.stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
			args := readOrWriteArgs(key2, true)

			_, pErr := tc.SendWrapped(args)

			if pErr != nil {
				t.Fatalf("test %d: %s", i, pErr)
			}
			close(cmd3Done)
		}); err != nil {
			t.Fatal(err)
		}

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
	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
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
	tc.StartWithStoreConfig(t, tsc)
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

// TestReplicaCommandQueueCancellation verifies that commands which are
// waiting on the command queue do not execute if their context is cancelled.
func TestReplicaCommandQueueCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Intercept commands with matching command IDs and block them.
	blockingStart := make(chan struct{})
	blockingDone := make(chan struct{})

	tc := testContext{}
	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Hdr.UserPriority == 42 {
				blockingStart <- struct{}{}
				<-blockingDone
			}
			return nil
		}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	defer close(blockingDone) // make sure teardown can happen

	startBlockingCmd := func(ctx context.Context, keys ...roachpb.Key) <-chan *roachpb.Error {
		done := make(chan *roachpb.Error)

		if err := tc.stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
			ba := roachpb.BatchRequest{
				Header: roachpb.Header{
					UserPriority: 42,
				},
			}
			for _, key := range keys {
				args := putArgs(key, nil)
				ba.Add(&args)
			}

			_, pErr := tc.Sender().Send(ctx, ba)
			done <- pErr
		}); err != nil {
			t.Fatal(err)
		}

		return done
	}

	key1 := roachpb.Key("one")
	key2 := roachpb.Key("two")

	// Wait until the command queue is blocked.
	cmd1Done := startBlockingCmd(context.Background(), key1)
	<-blockingStart

	// Put a cancelled blocking command in the command queue. This command will
	// block on the previous command, but will not itself reach the filter since
	// its context is cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd2Done := startBlockingCmd(ctx, key1, key2)

	// Wait until both commands are in the command queue.
	util.SucceedsSoon(t, func() error {
		tc.rng.cmdQMu.Lock()
		chans := tc.rng.cmdQMu.global.getWait(false, roachpb.Span{Key: key1}, roachpb.Span{Key: key2})
		tc.rng.cmdQMu.Unlock()
		if a, e := len(chans), 2; a < e {
			return errors.Errorf("%d of %d commands in the command queue", a, e)
		}
		return nil
	})

	// If this deadlocks, the command has unexpectedly begun executing and was
	// trapped in the command filter. Indeed, the absence of such a deadlock is
	// what's being tested here.
	if pErr := <-cmd2Done; !testutils.IsPError(pErr, context.Canceled.Error()) {
		t.Fatal(pErr)
	}

	// Finish the previous command, allowing the test to shut down.
	blockingDone <- struct{}{}
	if pErr := <-cmd1Done; pErr != nil {
		t.Fatal(pErr)
	}
}

func SendWrapped(
	ctx context.Context, sender client.Sender, header roachpb.Header, args roachpb.Request,
) (roachpb.Response, roachpb.BatchResponse_Header, *roachpb.Error) {
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

	_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{}, &pArgs)
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

	_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Timestamp: hlc.ZeroTimestamp.Add(0, 1)}, &pArgs)
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
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)

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
		if _, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)

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
	_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: txn}, &pArgs)
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

	_, respH, pErr = SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Timestamp: ts}, &pArgs)
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
	txn := newTransaction("test", k, 10, enginepb.SERIALIZABLE, tc.clock)
	args := incrementArgs(k, 1)
	txn.Sequence = 1

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite Abort cache entry with garbage for the last op.
	key := keys.AbortCacheKey(tc.rng.RangeID, txn.ID)
	err := engine.MVCCPut(context.Background(), tc.engine, nil, key, hlc.ZeroTimestamp, roachpb.MakeValueFromString("never read in this test"), nil)
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
		txn := newTransaction("test", key, 10, enginepb.SERIALIZABLE, tc.clock)
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
	txn := newTransaction("test", key, 10, enginepb.SERIALIZABLE, tc.clock)
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
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
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
	_, pErr := tc.Sender().Send(context.Background(), ba)
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

	txn := newTransaction("test", []byte("test"), 10, enginepb.SERIALIZABLE, tc.clock)
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
		txn := newTransaction("txn: "+strconv.Itoa(i), key, 1, enginepb.SERIALIZABLE, tc.clock)
		put := putArgs(key, key)

		_, header := beginTxnArgs(key, txn)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), header, &put); pErr != nil {
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

// TestTxnSpanGCThreshold verifies that aborting transactions which haven't
// written their initial txn record yet does not lead to anomalies. Precisely,
// verify that if the GC queue could potentially have removed a txn record
// created through a successful push (by a concurrent actor), the original
// transaction's subsequent attempt to create its initial record fails.
//
// See #9265 for context.
func TestEndTransactionTxnSpanGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	desc := tc.rng.Desc()
	// This test avoids a zero-timestamp regression (see LastActive() below),
	// so avoid zero timestamps.
	tc.manualClock.Increment(123)
	pusher := &roachpb.Transaction{} // non-transactional pusher is enough

	// This pushee should never be allowed to write a txn record because it
	// will be aborted before it even tries.
	pushee := newTransaction("foo", key, 1, enginepb.SERIALIZABLE, tc.clock)
	pushReq := pushTxnArgs(pusher, pushee, roachpb.PUSH_TOUCH)
	pushReq.Now = tc.clock.Now()
	resp, pErr := tc.SendWrapped(&pushReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	abortedPushee := resp.(*roachpb.PushTxnResponse).PusheeTxn
	if abortedPushee.Status != roachpb.ABORTED {
		t.Fatalf("expected push to abort pushee, got %+v", abortedPushee)
	}
	if lastActive := abortedPushee.LastActive(); lastActive.Less(pushReq.Now) {
		t.Fatalf("pushee has no recent activity: %s (expected >= %s): %+v",
			lastActive, pushReq.Now, abortedPushee)
	}

	gcSpan := roachpb.Span{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	}

	// Pretend that the GC queue removes the aborted transaction entry, as it
	// would after a period of inactivity, while our pushee txn is unaware and
	// may have written intents elsewhere.
	{
		gcReq := roachpb.GCRequest{
			Span: gcSpan,
			Keys: []roachpb.GCRequest_GCKey{
				{Key: keys.TransactionKey(pushee.Key, pushee.ID)},
			},
			TxnSpanGCThreshold: tc.clock.Now(),
		}
		if _, pErr := tc.SendWrapped(&gcReq); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Try to let our transaction write its initial record. If this succeeds,
	// we're in trouble because other written intents may have been aborted,
	// i.e. the transaction might commit but lose some of its writes.
	//
	// It should not succeed because BeginTransaction checks the transaction's
	// last activity against the persisted TxnSpanGCThreshold.
	{
		beginArgs, header := beginTxnArgs(key, pushee)
		resp, pErr := tc.SendWrappedWith(header, &beginArgs)
		if pErr == nil {
			t.Fatalf("unexpected success: %+v", resp)
		} else if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
			t.Fatalf("expected txn aborted error, got %v and response %+v", pErr, resp)
		}
	}

	// A transaction which starts later (i.e. at a higher timestamp) should not
	// be prevented from writing its record.
	// See #9522.
	{
		txn := newTransaction("foo", key, 1, enginepb.SERIALIZABLE, tc.clock)
		beginArgs, header := beginTxnArgs(key, txn)
		if _, pErr := tc.SendWrappedWith(header, &beginArgs); pErr != nil {
			t.Fatal(pErr)
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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	pArgs := putArgs(key, []byte("only here to make this a rw transaction"))
	txn.Sequence++
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), roachpb.Header{
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
			LeftDesc: roachpb.RangeDescriptor{StartKey: roachpb.RKey("bar")},
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
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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
		isolation enginepb.IsolationType
		expErr    bool
	}{
		{true, enginepb.SERIALIZABLE, true},
		{true, enginepb.SNAPSHOT, false},
		{false, enginepb.SERIALIZABLE, false},
		{false, enginepb.SNAPSHOT, false},
	}
	key := roachpb.Key("a")
	for i, test := range testCases {
		pushee := newTransaction("pushee", key, 1, test.isolation, tc.clock)
		pusher := newTransaction("pusher", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pushee.Priority = 1
		pusher.Priority = 2 // pusher will win
		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, []byte("value"))
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	_, btH := beginTxnArgs(key, txn)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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
	txn := newTransaction("test", roachpb.Key(""), 1, enginepb.SERIALIZABLE, tc.clock)

	doesNotExist := roachpb.TransactionStatus(-1)

	testCases := []struct {
		key          roachpb.Key
		existStatus  roachpb.TransactionStatus
		existEpoch   uint32
		existTS      hlc.Timestamp
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
			if err := engine.MVCCPutProto(context.Background(), tc.rng.store.Engine(), nil, txnKey, hlc.ZeroTimestamp,
				nil, &existTxn); err != nil {
				t.Fatal(err)
			}
		}

		// End the transaction, verify expected error.
		txn.Key = test.key
		args, h := endTxnArgs(txn, true)
		txn.Sequence++

		if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, test.expErrRegexp) {
			t.Errorf("%d: expected error:\n%s\not match:\n%s", i, pErr, test.expErrRegexp)
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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	_, btH := beginTxnArgs(key, txn)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction by pushing it with a higher priority.
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
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
	_, pErr := tc.Sender().Send(context.Background(), ba)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected write intent error, but got %s", pErr)
	}

	// Abort the transaction again. No error is returned.
	args, h := endTxnArgs(txn, false)
	args.IntentSpans = []roachpb.Span{{Key: key}}
	resp, pErr := tc.SendWrappedWith(h, &args)
	if pErr != nil {
		t.Fatal(pErr)
	}
	reply := resp.(*roachpb.EndTransactionResponse)
	if reply.Txn.Status != roachpb.ABORTED {
		t.Errorf("expected transaction status to be ABORTED; got %s", reply.Txn.Status)
	}

	// Verify that the intent has been resolved.
	if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
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
		_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{}, &incArgs)
		if pErr != nil {
			t.Fatal(pErr)
		}

		// Do an increment with timestamp to an earlier timestamp, but same key.
		// This will bump up to a higher timestamp than the original increment
		// and not surface a WriteTooOldError.
		h := roachpb.Header{Timestamp: respH.Timestamp.Prev()}
		_, respH, pErr = SendWrapped(context.Background(), tc.Sender(), h, &incArgs)
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
		_, _, pErr = SendWrapped(context.Background(), tc.Sender(), h, &incArgs)
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
	br, pErr := tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	if latest := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.IncrementResponse).NewValue; latest != sum {
		t.Fatalf("expected %d, got %d", sum, latest)
	}

	// Now resend the batch with the same timestamp; this should look
	// like the replay it is and surface a WriteTooOldError.
	ba.Timestamp = br.Timestamp
	_, pErr = tc.Sender().Send(context.Background(), ba)
	if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("expected WriteTooOldError; got %s", pErr)
	}

	// Send a DeleteRange & increment.
	incArgs := incrementArgs(key, 1)
	ba = roachpb.BatchRequest{}
	ba.Add(roachpb.NewDeleteRange(key, key.Next(), false))
	ba.Add(&incArgs)
	br, pErr = tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Send exact same batch; the DeleteRange should trip up and
	// we'll get a replay error.
	ba.Timestamp = br.Timestamp
	_, pErr = tc.Sender().Send(context.Background(), ba)
	if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
		t.Fatalf("expected WriteTooOldError; got %s", pErr)
	}

	// Send just a DeleteRange batch.
	ba = roachpb.BatchRequest{}
	ba.Add(roachpb.NewDeleteRange(key, key.Next(), false))
	br, pErr = tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Now send it again; will not look like a replay because the
	// previous DeleteRange didn't leave any tombstones at this
	// timestamp for the replay to "trip" over.
	ba.Timestamp = br.Timestamp
	_, pErr = tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}
}

// TestRaftReplayProtectionInTxn verifies that transactional batches
// enjoy protection from raft replays.
func TestRaftReplayProtectionInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	cfg := TestStoreConfig()
	tc := testContext{}
	tc.StartWithStoreConfig(t, cfg)
	defer tc.Stop()

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)

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
	_, pErr := tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	for i := 0; i < 2; i++ {
		// Reach in and manually send to raft (to simulate Raft replay) and
		// also avoid updating the timestamp cache; verify WriteTooOldError.
		ba.Timestamp = txn.OrigTimestamp
		ch, _, err := tc.rng.propose(context.Background(), ba)
		if err != nil {
			t.Fatalf("%d: unexpected error: %s", i, err)
		}
		respWithErr := <-ch
		if _, ok := respWithErr.Err.GetDetail().(*roachpb.WriteTooOldError); !ok {
			t.Fatalf("%d: expected WriteTooOldError; got %s", i, respWithErr.Err)
		}
	}
}

// TestReplicaLaziness verifies that Raft Groups are brought up lazily.
func TestReplicaLaziness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// testWithAction is a function that creates an uninitialized Raft group,
	// calls the supplied function, and then tests that the Raft group is
	// initialized.
	testWithAction := func(action func() roachpb.Request) {
		tc := testContext{bootstrapMode: bootstrapRangeOnly}
		tc.Start(t)
		defer tc.Stop()

		if status := tc.rng.RaftStatus(); status != nil {
			t.Fatalf("expected raft group to not be initialized, got RaftStatus() of %v", status)
		}
		var ba roachpb.BatchRequest
		request := action()
		ba.Add(request)
		if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
			t.Fatalf("unexpected error: %s", pErr)
		}

		if tc.rng.RaftStatus() == nil {
			t.Fatalf("expected raft group to be initialized")
		}
	}

	testWithAction(func() roachpb.Request {
		put := putArgs(roachpb.Key("a"), []byte("value"))
		return &put
	})

	testWithAction(func() roachpb.Request {
		get := getArgs(roachpb.Key("a"))
		return &get
	})

	testWithAction(func() roachpb.Request {
		scan := scanArgs(roachpb.KeyMin, roachpb.KeyMax)
		return &scan
	})
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

	for i, iso := range []enginepb.IsolationType{enginepb.SERIALIZABLE, enginepb.SNAPSHOT} {
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
		br, pErr := tc.Sender().Send(context.Background(), ba)
		if pErr != nil {
			t.Fatalf("%d: unexpected error: %s", i, pErr)
		}

		// Send a put for keyB.
		putB := putArgs(keyB, []byte("value"))
		putTxn := br.Txn.Clone()
		putTxn.Sequence++
		_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: &putTxn}, &putB)
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
		ok, err := engine.MVCCGetProto(context.Background(), tc.rng.store.Engine(), txnKey, hlc.ZeroTimestamp, true /* consistent */, nil /* txn */, &readTxn)
		if err != nil || ok {
			t.Errorf("%d: expected transaction record to be cleared (%t): %s", i, ok, err)
		}

		// Now replay begin & put. BeginTransaction should fail with a replay error.
		_, pErr = tc.Sender().Send(context.Background(), ba)
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
		_, _, pErr = SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: &putTxn}, &putB)
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

// Test that a duplicate BeginTransaction results in a TransactionRetryError, as
// such recognizing that it's likely the result of the batch being retried by
// DistSender.
func TestDuplicateBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	bt, btH := beginTxnArgs(key, txn)
	var ba roachpb.BatchRequest
	ba.Header = btH
	ba.Add(&bt)
	_, pErr := tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	// Send the batch again.
	_, pErr = tc.Sender().Send(context.Background(), ba)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("expected retry error; got %v", pErr)
	}
}

// TestEndTransactionGC verifies that a transaction record is immediately
// garbage-collected upon EndTransaction iff all of the supplied intents are
// local relative to the transaction record's location.
func TestEndTransactionLocalGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	tc := testContext{}
	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if filterArgs.Req.Method() == roachpb.GC {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	tc.StartWithStoreConfig(t, tsc)
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
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(putKey, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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
		ok, err := engine.MVCCGetProto(context.Background(), tc.rng.store.Engine(), txnKey, hlc.ZeroTimestamp,
			true /* consistent */, nil /* txn */, &readTxn)
		if err != nil {
			t.Fatal(err)
		}
		if !ok != test.expGC {
			t.Errorf("%d: unexpected gc'ed: %t", i, !ok)
		}
	}
}

// setupResolutionTest splits the range at the specified splitKey and completes
// a transaction which creates intents at key and splitKey.
func setupResolutionTest(
	t *testing.T, tc testContext, key roachpb.Key, splitKey roachpb.RKey, commit bool,
) (*Replica, *roachpb.Transaction) {
	// Split the range and create an intent at splitKey and key.
	newRng := splitTestRange(tc.store, splitKey, splitKey, t)

	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	// These increments are not required, but testing feels safer when zero
	// values are unexpected.
	txn.Sequence++
	txn.Epoch++
	pArgs := putArgs(key, []byte("value"))
	h := roachpb.Header{Txn: txn}
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), h, &pArgs); pErr != nil {
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
		if _, pErr := newRng.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// End the transaction and resolve the intents.
	args, h := endTxnArgs(txn, commit)
	args.IntentSpans = []roachpb.Span{{Key: key}, {Key: splitKey.AsRawKey()}}
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
	tsc := TestStoreConfig()
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}

	tc.StartWithStoreConfig(t, tsc)
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
		_, pErr := newRng.Send(context.Background(), ba)
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
	expIntents := []roachpb.Span{{Key: splitKey.AsRawKey()}}
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
	a := roachpb.Key("a")
	splitKey := keys.MustAddr(a).Next()

	for i, testKey := range []roachpb.Key{
		a,
		keys.RangeDescriptorKey(keys.MustAddr(a)),
		keys.RangeDescriptorKey(keys.MustAddr(roachpb.KeyMin)),
	} {
		func() {
			tc := testContext{}
			tc.Start(t)
			defer tc.Stop()

			ctx := log.WithLogTag(context.Background(), "testcase", i)

			rightRng, txn := setupResolutionTest(t, tc, testKey, splitKey, false /* generate abort cache entry */)

			util.SucceedsSoon(t, func() error {
				if gr, _, err := tc.rng.Get(
					ctx, tc.engine, roachpb.Header{},
					roachpb.GetRequest{Span: roachpb.Span{
						Key: keys.TransactionKey(txn.Key, txn.ID),
					}},
				); err != nil {
					return err
				} else if gr.Value != nil {
					return errors.Errorf("%d: txn entry still there: %+v", i, gr)
				}

				var entry roachpb.AbortCacheEntry
				if aborted, err := tc.rng.abortCache.Get(ctx, tc.engine, txn.ID, &entry); err != nil {
					t.Fatal(err)
				} else if aborted {
					return errors.Errorf("%d: abort cache still populated: %v", i, entry)
				}
				if aborted, err := rightRng.abortCache.Get(ctx, tc.engine, txn.ID, &entry); err != nil {
					t.Fatal(err)
				} else if aborted {
					t.Fatalf("%d: right-hand side abort cache still populated: %v", i, entry)
				}

				return nil
			})
		}()
	}
}

// TestEndTransactionDirectGCFailure verifies that no immediate GC takes place
// if external intents can't be resolved (see also TestEndTransactionDirectGC).
func TestEndTransactionDirectGCFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	var count int64
	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				atomic.AddInt64(&count, 1)
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			} else if filterArgs.Req.Method() == roachpb.GC {
				t.Fatalf("unexpected GCRequest: %+v", filterArgs.Req)
			}
			return nil
		}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Now test that no GCRequest is issued. We can't test that directly (since
	// it's completely asynchronous), so we first make sure ResolveIntent
	// happened and subsequently issue a bogus Put which is likely to make it
	// into Raft only after a rogue GCRequest (at least sporadically), which
	// would trigger a Fatal from the command filter.
	util.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&count) == 0 {
			return errors.Errorf("intent resolution not attempted yet")
		} else if err := tc.store.DB().Put(context.TODO(), "panama", "banana"); err != nil {
			return err
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
			txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
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
	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(key) {
				atomic.StoreInt32(&seen, 1)
			}
			return nil
		}

	tc := testContext{}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()
	splitKey := roachpb.RKey("aa")
	setupResolutionTest(t, tc, roachpb.Key("a") /* irrelevant */, splitKey, true /* commit */)
	txn := newTransaction("name", key, 1, enginepb.SERIALIZABLE, tc.clock)
	txn.Status = roachpb.COMMITTED
	if pErr := tc.store.intentResolver.resolveIntents(context.Background(),
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
	run := func(abort bool, iso enginepb.IsolationType) {
		tc := testContext{}
		tc.Start(t)
		defer tc.Stop()

		pushee := newTransaction("test", key, 1, iso, tc.clock)
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pusher.Priority = 2
		pushee.Priority = 1 // pusher will win

		inc := func(actor *roachpb.Transaction, k roachpb.Key) (*roachpb.IncrementResponse, *roachpb.Error) {
			reply, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.store, roachpb.Header{
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
			_, pErr := client.SendWrappedWith(context.Background(), tc.store, roachpb.Header{
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
		run(abort, enginepb.SERIALIZABLE)
		run(abort, enginepb.SNAPSHOT)
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
	txn.Timestamp = hlc.Timestamp{WallTime: 1}

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

	pusher := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, enginepb.SERIALIZABLE, tc.clock)

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
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pusher.Priority = 1
		pushee.Priority = 2 // pusher will lose, meaning we shouldn't push unless pushee is already ended.

		// Begin the pushee's transaction.
		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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

	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	testCases := []struct {
		startTS, ts, expTS hlc.Timestamp
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
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pushee.Priority = 1
		pushee.Epoch = 12345
		pusher.Priority = 2   // Pusher will win
		pusher.Writing = true // expected when a txn is heartbeat

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		pushee.Timestamp = test.startTS
		pushee.LastHeartbeat = &test.startTS
		_, btH := beginTxnArgs(key, pushee)
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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

		// TODO(tschottdorf): with proposer-evaluated KV, we are sharing memory
		// where the other code takes a copy, resulting in this adjustment
		// being necessary.
		if propEvalKV {
			expTxn.BatchIndex = 0
		}

		if !reflect.DeepEqual(expTxn, reply.PusheeTxn) {
			t.Fatalf("unexpected push txn in trial %d: %s", i, pretty.Diff(expTxn, reply.PusheeTxn))
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

	ts := hlc.Timestamp{WallTime: 1}
	ns := base.DefaultHeartbeatInterval.Nanoseconds()
	testCases := []struct {
		heartbeat   hlc.Timestamp // zero value indicates no heartbeat
		currentTime int64         // nanoseconds
		pushType    roachpb.PushTxnType
		expSuccess  bool
	}{
		// Avoid using 0 as currentTime since our manualClock is at 0 and we
		// don't want to have outcomes depend on random logical ticks.
		{hlc.ZeroTimestamp, 1, roachpb.PUSH_TIMESTAMP, false},
		{hlc.ZeroTimestamp, 1, roachpb.PUSH_ABORT, false},
		{hlc.ZeroTimestamp, 1, roachpb.PUSH_TOUCH, false},
		{hlc.ZeroTimestamp, 1, roachpb.PUSH_QUERY, true},
		{hlc.ZeroTimestamp, ns, roachpb.PUSH_TIMESTAMP, false},
		{hlc.ZeroTimestamp, ns, roachpb.PUSH_ABORT, false},
		{hlc.ZeroTimestamp, ns, roachpb.PUSH_TOUCH, false},
		{hlc.ZeroTimestamp, ns, roachpb.PUSH_QUERY, true},
		{hlc.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_TIMESTAMP, false},
		{hlc.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_ABORT, false},
		{hlc.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_TOUCH, false},
		{hlc.ZeroTimestamp, ns*2 - 1, roachpb.PUSH_QUERY, true},
		{hlc.ZeroTimestamp, ns * 2, roachpb.PUSH_TIMESTAMP, false},
		{hlc.ZeroTimestamp, ns * 2, roachpb.PUSH_ABORT, false},
		{hlc.ZeroTimestamp, ns * 2, roachpb.PUSH_TOUCH, false},
		{hlc.ZeroTimestamp, ns * 2, roachpb.PUSH_QUERY, true},
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
		pushee := newTransaction(fmt.Sprintf("test-%d", i), key, 1, enginepb.SERIALIZABLE, tc.clock)
		pusher := newTransaction("pusher", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pushee.Priority = 2
		pusher.Priority = 1 // Pusher won't win based on priority.

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		if !test.heartbeat.Equal(hlc.ZeroTimestamp) {
			pushee.LastHeartbeat = &test.heartbeat
		}
		_, btH := beginTxnArgs(key, pushee)
		btH.Timestamp = tc.rng.store.Clock().Now()
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
			t.Fatalf("%d: %s", i, pErr)
		}

		// Now, attempt to push the transaction with Now set to our current time.
		args := pushTxnArgs(pusher, pushee, test.pushType)
		args.Now = hlc.Timestamp{WallTime: test.currentTime}
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

	txn := newTransaction("test", roachpb.Key("test"), 1, enginepb.SERIALIZABLE, tc.clock)
	txnPushee := txn.Clone()
	txnPushee.Priority--
	pa := pushTxnArgs(txn, &txnPushee, roachpb.PUSH_ABORT)
	var ms enginepb.MVCCStats
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

	ts1 := hlc.Timestamp{WallTime: 1}
	ts2 := hlc.Timestamp{WallTime: 2}
	testCases := []struct {
		pusherPriority, pusheePriority int32
		pusherTS, pusheeTS             hlc.Timestamp
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
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
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
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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

	pusher := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, enginepb.SERIALIZABLE, tc.clock)
	pusher.Priority = 2
	pushee.Priority = 1 // pusher will win
	pusher.Timestamp = hlc.Timestamp{WallTime: 50, Logical: 25}
	pushee.Timestamp = hlc.Timestamp{WallTime: 5, Logical: 1}

	key := roachpb.Key("a")
	_, btH := beginTxnArgs(key, pushee)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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

	pusher := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.clock)
	pushee := newTransaction("test", roachpb.Key("b"), 1, enginepb.SERIALIZABLE, tc.clock)
	pusher.Priority = 1
	pushee.Priority = 2 // pusher will lose
	pusher.Timestamp = hlc.Timestamp{WallTime: 50, Logical: 0}
	pushee.Timestamp = hlc.Timestamp{WallTime: 50, Logical: 1}

	key := roachpb.Key("a")
	_, btH := beginTxnArgs(key, pushee)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
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
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
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
	resp, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put)
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
	_, pErr = tc.Sender().Send(context.Background(), ba)
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
	txn := newTransaction("test", keys[0], 1, enginepb.SERIALIZABLE, tc.clock)

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

func verifyRangeStats(eng engine.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats) error {
	ms, err := engine.MVCCGetRangeStats(context.Background(), eng, rangeID)
	if err != nil {
		return err
	}
	if ms != expMS {
		return errors.Errorf("expected and actual stats differ:\n%s", pretty.Diff(expMS, ms))
	}
	return nil
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

	baseStats := initialStats()
	// The initial stats contain an empty lease, but there will be an initial
	// nontrivial lease requested with the first write below.
	baseStats.Add(enginepb.MVCCStats{
		SysBytes: 8,
	})

	// Put a value.
	pArgs := putArgs([]byte("a"), []byte("value1"))

	if _, pErr := tc.SendWrapped(&pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS := baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes: 25,
		KeyBytes:  14,
		ValBytes:  11,
		LiveCount: 1,
		KeyCount:  1,
		ValCount:  1,
	})

	if err := verifyRangeStats(tc.engine, tc.rng.RangeID, expMS); err != nil {
		t.Fatal(err)
	}

	// Put a 2nd value transactionally.
	pArgs = putArgs([]byte("b"), []byte("value2"))

	// Consistent UUID needed for a deterministic SysBytes value. This is because
	// a random UUID could have a 0x00 byte that would be escaped by the encoding,
	// increasing the encoded size and throwing off statistics verification.
	uuid, err := uuid.FromString("ea5b9590-a157-421b-8b93-a4caa2c41137")
	if err != nil {
		t.Fatal(err)
	}
	txn := newTransaction("test", pArgs.Key, 1, enginepb.SERIALIZABLE, tc.clock)
	txn.Priority = 123 // So we don't have random values messing with the byte counts on encoding
	txn.ID = uuid

	if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes:   101,
		KeyBytes:    28,
		ValBytes:    73,
		IntentBytes: 23,
		LiveCount:   2,
		KeyCount:    2,
		ValCount:    2,
		IntentCount: 1,
	})
	if err := verifyRangeStats(tc.engine, tc.rng.RangeID, expMS); err != nil {
		t.Fatal(err)
	}

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
	expMS = baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes: 50,
		KeyBytes:  28,
		ValBytes:  22,
		LiveCount: 2,
		KeyCount:  2,
		ValCount:  2,
	})
	if err := verifyRangeStats(tc.engine, tc.rng.RangeID, expMS); err != nil {
		t.Fatal(err)
	}

	// Delete the 1st value.
	dArgs := deleteArgs([]byte("a"))

	if _, pErr := tc.SendWrapped(&dArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes: 25,
		KeyBytes:  40,
		ValBytes:  22,
		LiveCount: 1,
		KeyCount:  2,
		ValCount:  3,
	})
	if err := verifyRangeStats(tc.engine, tc.rng.RangeID, expMS); err != nil {
		t.Fatal(err)
	}
}

// TestMerge verifies that the Merge command is behaving as expected. Time
// series data is used, as it is the only data type currently fully supported by
// the merge command.
func TestMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	key := []byte("mergedkey")
	args := make([]roachpb.InternalTimeSeriesData, 3)
	expected := roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 0,
		SampleDurationNanos: 1000,
		Samples:             make([]roachpb.InternalTimeSeriesSample, len(args)),
	}

	for i := 0; i < len(args); i++ {
		sample := roachpb.InternalTimeSeriesSample{
			Offset: int32(i),
			Count:  1,
			Sum:    float64(i),
		}
		args[i] = roachpb.InternalTimeSeriesData{
			StartTimestampNanos: expected.StartTimestampNanos,
			SampleDurationNanos: expected.SampleDurationNanos,
			Samples:             []roachpb.InternalTimeSeriesSample{sample},
		}
		expected.Samples[i] = sample
	}

	for _, arg := range args {
		var v roachpb.Value
		if err := v.SetProto(&arg); err != nil {
			t.Fatal(err)
		}
		mergeArgs := internalMergeArgs(key, v)
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

	var actual roachpb.InternalTimeSeriesData
	if err := resp.Value.GetProto(&actual); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&actual, &expected) {
		t.Errorf("Get did not return expected value: %v != %v", actual, expected)
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
	tc.rng.store.SetRaftLogQueueActive(false)

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

	_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: hlc.MinTimestamp}, &args)

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
		newAppliedIndex := tc.rng.mu.state.RaftAppliedIndex
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

	tsc := TestStoreConfig()
	tsc.TestingKnobs.TestingCommandFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Header().Key.Equal(roachpb.Key("boom")) {
				return roachpb.NewError(NewReplicaCorruptionError(errors.New("boom")))
			}
			return nil
		}

	tc := testContext{}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	// First send a regular command.
	args := putArgs(roachpb.Key("test1"), []byte("value"))
	if _, pErr := tc.SendWrapped(&args); pErr != nil {
		t.Fatal(pErr)
	}

	key := roachpb.Key("boom")

	// maybeSetCorrupt should have been called.
	args = putArgs(key, []byte("value"))
	_, pErr := tc.SendWrapped(&args)
	if !testutils.IsPError(pErr, "replica corruption \\(processed=true\\)") {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Verify replica destroyed was set.
	rkey, err := keys.Addr(key)
	if err != nil {
		t.Fatal(err)
	}
	r := tc.store.LookupReplica(rkey, rkey)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.destroyed.Error() != pErr.GetDetail().Error() {
		t.Fatalf("expected r.mu.destroyed == pErr.GetDetail(), instead %q != %q", r.mu.destroyed, pErr.GetDetail())
	}

	// Verify destroyed error was persisted.
	pErr, err = loadReplicaDestroyedError(context.Background(), r.store.Engine(), r.RangeID)
	if err != nil {
		t.Fatal(err)
	}
	if r.mu.destroyed.Error() != pErr.GetDetail().Error() {
		t.Fatalf("expected r.mu.destroyed == pErr.GetDetail(), instead %q != %q", r.mu.destroyed, pErr.GetDetail())
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

	if err := tc.rng.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicaDescriptor{
			NodeID:  tc.store.Ident.NodeID,
			StoreID: 9999,
		},
		tc.rng.Desc(),
	); err == nil || !strings.Contains(err.Error(), "already present") {
		t.Fatalf("must not be able to add second replica to same node (err=%s)", err)
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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.clock)
	// Officially begin the transaction. If not for this, the intent resolution
	// machinery would simply remove the intent we write below, see #3020.
	// We send directly to Replica throughout this test, so there's no danger
	// of the Store aborting this transaction (i.e. we don't have to set a high
	// priority).
	pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(key)), data)
	txn.Sequence++
	if _, pErr = maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now lookup the range; should get the value. Since the lookup is
	// inconsistent, there's no WriteIntentError.
	// Note that 'A' < 'a'.
	rlArgs.Key = keys.RangeMetaKey(roachpb.RKey{'A'})

	reply, pErr = tc.SendWrappedWith(roachpb.Header{
		Timestamp:       hlc.MinTimestamp,
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

	// Try a single inconsistent lookup. Expect to see both descriptors.
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

	splitRangeBefore := roachpb.RangeDescriptor{RangeID: 3, StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("h")}
	splitRangeLHS := roachpb.RangeDescriptor{RangeID: 3, StartKey: roachpb.RKey("c"), EndKey: roachpb.RKey("f")}
	splitRangeRHS := roachpb.RangeDescriptor{RangeID: 5, StartKey: roachpb.RKey("f"), EndKey: roachpb.RKey("h")}

	// Test ranges: ["a","c"), ["c","f"), ["f","h") and ["h","y").
	testRanges := []roachpb.RangeDescriptor{
		{RangeID: 2, StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("c")},
		splitRangeBefore,
		{RangeID: 4, StartKey: roachpb.RKey("h"), EndKey: roachpb.RKey("y")},
	}

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
		{key: "j", expected: testRanges[2]},
		// testRanges[2] has an intent, so the inconsistent scan will read
		// an old value (nil). Since we're in reverse mode, testRanges[1]
		// is the result.
		{key: "g", expected: testRanges[1]},
		{key: "h", expected: testRanges[1]},
	}

	{
		txn := newTransaction("test", roachpb.Key{}, 1, enginepb.SERIALIZABLE, tc.clock)
		for _, r := range testRanges {
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

	// Write the new descriptors as intents.
	txn := newTransaction("test", roachpb.Key{}, 1, enginepb.SERIALIZABLE, tc.clock)
	for _, r := range []roachpb.RangeDescriptor{splitRangeLHS, splitRangeRHS} {
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

func TestRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		{key: keys.MustAddr(keys.Meta1KeyMax), reverse: false, expected: expected},
		{key: keys.MustAddr(keys.Meta2KeyMax), reverse: false, expected: nil},
		{key: keys.MustAddr(keys.Meta1KeyMax), reverse: true, expected: expected},
		{key: keys.MustAddr(keys.Meta2KeyMax), reverse: true, expected: expected},
	}

	for i, c := range testCases {
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
				t.Errorf("%d: expected %+v, got %+v", i, c.expected, reply.Ranges)
			}
		}
	}
}

// TestRequestLeaderEncounterGroupDeleteError verifies that a lease request which fails with
// RaftGroupDeletedError is converted to a RangeNotFoundError in the Store.
func TestRequestLeaderEncounterGroupDeleteError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	// Mock propose to return an roachpb.RaftGroupDeletedError.
	submitProposalFn := func(*ProposalData) error {
		return &roachpb.RaftGroupDeletedError{}
	}

	rng := tc.rng

	rng.mu.Lock()
	rng.mu.submitProposalFn = submitProposalFn
	rng.mu.Unlock()

	gArgs := getArgs(roachpb.Key("a"))
	// Force the read command request a new lease.
	clock := tc.clock
	ts := clock.Update(clock.Now().Add(leaseExpiry(tc.rng), 0))
	_, pErr := client.SendWrappedWith(context.Background(), tc.store, roachpb.Header{
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
	// This one succeeds.
	ba.Add(&roachpb.PutRequest{
		Span:  roachpb.Span{Key: roachpb.Key("k")},
		Value: roachpb.MakeValueFromString("not nil"),
	})
	// This one fails with a ConditionalPutError, which will populate the
	// returned error's index.
	ba.Add(&roachpb.ConditionalPutRequest{
		Span:     roachpb.Span{Key: roachpb.Key("k")},
		Value:    roachpb.MakeValueFromString("irrelevant"),
		ExpValue: nil, // not true after above Put
	})
	// This one is never executed.
	ba.Add(&roachpb.GetRequest{
		Span: roachpb.Span{Key: roachpb.Key("k")},
	})

	if _, pErr := tc.Sender().Send(context.Background(), ba); pErr == nil {
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
	_, btH := beginTxnArgs(key, newTransaction("test", key, 1, enginepb.SERIALIZABLE, rng.store.Clock()))
	btH.Txn.Priority = 1 // low so it can be pushed
	put := putArgs(key, []byte("foo"))
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction so that the async intent resolution caused
	// by loading the system config span doesn't waste any time in
	// clearing the intent.
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, rng.store.Clock())
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
		if err := engine.MVCCPut(context.Background(), rng.store.Engine(), &enginepb.MVCCStats{},
			keys.SystemConfigSpan.Key, rng.store.Clock().Now(), v, nil); err != nil {
			return err
		}

		kvs, _, err := rng.loadSystemConfigSpan()
		if err != nil {
			return err
		}

		if len(kvs) != 1 || !bytes.Equal(kvs[0].Key, keys.SystemConfigSpan.Key) {
			return errors.Errorf("expected only key %s in SystemConfigSpan map: %+v", keys.SystemConfigSpan.Key, kvs)
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

	// First try and fail with a stale descriptor.
	origDesc := rep.Desc()
	newDesc := protoutil.Clone(origDesc).(*roachpb.RangeDescriptor)
	for i := range newDesc.Replicas {
		if newDesc.Replicas[i].StoreID == tc.store.StoreID() {
			newDesc.Replicas[i].ReplicaID++
			newDesc.NextReplicaID++
			break
		}
	}

	if err := rep.setDesc(newDesc); err != nil {
		t.Fatal(err)
	}
	if err := tc.store.removeReplicaImpl(tc.rng, *origDesc, true); !testutils.IsError(err, "replica ID has changed") {
		t.Fatalf("expected error 'replica ID has changed' but got %v", err)
	}

	// Now try a fresh descriptor and succeed.
	if err := tc.store.removeReplicaImpl(tc.rng, *rep.Desc(), true); err != nil {
		t.Fatal(err)
	}
}

func TestEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.SetRaftLogQueueActive(false)

	rng := tc.rng
	rangeID := rng.RangeID
	var indexes []uint64

	populateLogs := func(from, to int) []uint64 {
		var newIndexes []uint64
		for i := from; i < to; i++ {
			args := incrementArgs([]byte("a"), int64(i))
			if _, pErr := tc.SendWrapped(&args); pErr != nil {
				t.Fatal(pErr)
			}
			idx, err := rng.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			newIndexes = append(newIndexes, idx)
		}
		return newIndexes
	}

	truncateLogs := func(index int) {
		truncateArgs := truncateLogArgs(indexes[index], rangeID)
		if _, err := client.SendWrapped(context.Background(), tc.Sender(), &truncateArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Populate the log with 10 entries. Save the LastIndex after each write.
	indexes = append(indexes, populateLogs(0, 10)...)

	for i, tc := range []struct {
		lo             uint64
		hi             uint64
		maxBytes       uint64
		expResultCount int
		expCacheCount  int
		expError       error
		// Setup, if not nil, is called before running the test case.
		setup func()
	}{
		// Case 0: All of the entries from cache.
		{lo: indexes[0], hi: indexes[9] + 1, expResultCount: 10, expCacheCount: 10, setup: nil},
		// Case 1: Get the first entry from cache.
		{lo: indexes[0], hi: indexes[1], expResultCount: 1, expCacheCount: 1, setup: nil},
		// Case 2: Get the last entry from cache.
		{lo: indexes[9], hi: indexes[9] + 1, expResultCount: 1, expCacheCount: 1, setup: nil},
		// Case 3: lo is available, but hi is not, cache miss.
		{lo: indexes[9], hi: indexes[9] + 2, expCacheCount: 1, expError: raft.ErrUnavailable, setup: nil},

		// Case 4: Just most of the entries from cache.
		{lo: indexes[5], hi: indexes[9], expResultCount: 4, expCacheCount: 4, setup: func() {
			// Discard the first half of the log.
			truncateLogs(5)
		}},
		// Case 5: Get a single entry from cache.
		{lo: indexes[5], hi: indexes[6], expResultCount: 1, expCacheCount: 1, setup: nil},
		// Case 6: Use MaxUint64 instead of 0 for maxBytes.
		{lo: indexes[5], hi: indexes[9], maxBytes: math.MaxUint64, expResultCount: 4, expCacheCount: 4, setup: nil},
		// Case 7: maxBytes is set low so only a single value should be
		// returned.
		{lo: indexes[5], hi: indexes[9], maxBytes: 1, expResultCount: 1, expCacheCount: 1, setup: nil},
		// Case 8: hi value is just past the last index, should return all
		// available entries.
		{lo: indexes[5], hi: indexes[9] + 1, expResultCount: 5, expCacheCount: 5, setup: nil},
		// Case 9: all values have been truncated from cache and storage.
		{lo: indexes[1], hi: indexes[2], expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 10: hi has just been truncated from cache and storage.
		{lo: indexes[1], hi: indexes[4], expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 11: another case where hi has just been truncated from
		// cache and storage.
		{lo: indexes[3], hi: indexes[4], expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 12: lo has been truncated and hi is the truncation point.
		{lo: indexes[4], hi: indexes[5], expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 13: lo has been truncated but hi is available.
		{lo: indexes[4], hi: indexes[9], expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 14: lo has been truncated and hi is not available.
		{lo: indexes[4], hi: indexes[9] + 100, expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 15: lo has been truncated but hi is available, and maxBytes is
		// set low.
		{lo: indexes[4], hi: indexes[9], maxBytes: 1, expCacheCount: 0, expError: raft.ErrCompacted, setup: nil},
		// Case 16: lo is available but hi is not.
		{lo: indexes[5], hi: indexes[9] + 100, expCacheCount: 6, expError: raft.ErrUnavailable, setup: nil},
		// Case 17: both lo and hi are not available, cache miss.
		{lo: indexes[9] + 100, hi: indexes[9] + 1000, expCacheCount: 0, expError: raft.ErrUnavailable, setup: nil},
		// Case 18: lo is available, hi is not, but it was cut off by maxBytes.
		{lo: indexes[5], hi: indexes[9] + 1000, maxBytes: 1, expResultCount: 1, expCacheCount: 1, setup: nil},

		// Case 19: lo and hi are available, but entry cache evicted.
		{lo: indexes[5], hi: indexes[9], expResultCount: 4, expCacheCount: 0, setup: func() {
			// Manually evict cache for the first 10 log entries.
			rng.store.raftEntryCache.delEntries(rangeID, indexes[0], indexes[9]+1)
			indexes = append(indexes, populateLogs(10, 40)...)
		}},
		// Case 20: lo and hi are available, entry cache evicted and hi available in cache.
		{lo: indexes[5], hi: indexes[9] + 5, expResultCount: 9, expCacheCount: 4, setup: nil},
		// Case 21: lo and hi are available and in entry cache.
		{lo: indexes[9] + 2, hi: indexes[9] + 32, expResultCount: 30, expCacheCount: 30, setup: nil},
		// Case 22: lo is available and hi is not.
		{lo: indexes[9] + 2, hi: indexes[9] + 33, expCacheCount: 30, expError: raft.ErrUnavailable, setup: nil},
	} {
		if tc.setup != nil {
			tc.setup()
		}
		cacheEntries, _, _ := rng.store.raftEntryCache.getEntries(rangeID, tc.lo, tc.hi, tc.maxBytes)
		if len(cacheEntries) != tc.expCacheCount {
			t.Errorf("%d: expected cache count %d, got %d", i, tc.expCacheCount, len(cacheEntries))
		}
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
			t.Errorf("%d: expected %d entries, got %d", i, tc.expResultCount, len(ents))
		}
	}

	// Case 23: Lo must be less than or equal to hi.
	rng.mu.Lock()
	if _, err := rng.Entries(indexes[9], indexes[5], 0); err == nil {
		t.Errorf("23: error expected, got none")
	}
	rng.mu.Unlock()

	// Case 24: add a gap to the indexes.
	if err := engine.MVCCDelete(context.Background(), tc.store.Engine(), nil, keys.RaftLogKey(rangeID, indexes[6]), hlc.ZeroTimestamp, nil); err != nil {
		t.Fatal(err)
	}
	rng.store.raftEntryCache.delEntries(rangeID, indexes[6], indexes[6]+1)

	rng.mu.Lock()
	defer rng.mu.Unlock()
	if _, err := rng.Entries(indexes[5], indexes[9], 0); err == nil {
		t.Errorf("24: error expected, got none")
	}

	// Case 25: don't hit the gap due to maxBytes.
	ents, err := rng.Entries(indexes[5], indexes[9], 1)
	if err != nil {
		t.Errorf("25: expected no error, got %s", err)
	}
	if len(ents) != 1 {
		t.Errorf("25: expected 1 entry, got %d", len(ents))
	}

	// Case 26: don't hit the gap due to truncation.
	if _, err := rng.Entries(indexes[4], indexes[9], 0); err != raft.ErrCompacted {
		t.Errorf("26: expected error %s , got %s", raft.ErrCompacted, err)
	}
}

func TestTerm(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	tc.rng.store.SetRaftLogQueueActive(false)

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
		t.Fatalf("expected firstIndex %d to be %d", firstIndex, indexes[4])
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
	if _, pErr := client.SendWrappedWith(context.Background(), rng2, ts1Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}
	if _, pErr := client.SendWrappedWith(context.Background(), rng2, ts2Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}

	// Send GC request to range 1 for the key on range 2, which
	// should succeed even though it doesn't contain the key, because
	// the request for the incorrect key will be silently dropped.
	gKey := gcKey(key, ts1)
	gcReq := gcArgs(rng1.Desc().StartKey, rng1.Desc().EndKey, gKey)
	if _, pErr := client.SendWrappedWith(context.Background(), rng1, roachpb.Header{Timestamp: tc.clock.Now()}, &gcReq); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to incorrect range: %s", pErr)
	}

	// Make sure the key still exists on range 2.
	getReq := getArgs(key)
	if res, pErr := client.SendWrappedWith(context.Background(), rng2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal == nil {
		t.Errorf("expected value %s to exists after GC to incorrect range but before GC to correct range, found %v", val, resVal)
	}

	// Send GC request to range 2 for the same key.
	gcReq = gcArgs(rng2.Desc().StartKey, rng2.Desc().EndKey, gKey)
	if _, pErr := client.SendWrappedWith(context.Background(), rng2, roachpb.Header{Timestamp: tc.clock.Now()}, &gcReq); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to correct range: %s", pErr)
	}

	// Make sure the key no longer exists on range 2.
	if res, pErr := client.SendWrappedWith(context.Background(), rng2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal != nil {
		t.Errorf("expected value at key %s to no longer exist after GC to correct range, found value %v", key, resVal)
	}
}

// TestReplicaCancelRaft checks that it is possible to safely abandon Raft
// commands via a cancelable context.Context.
func TestReplicaCancelRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Pick a key unlikely to be used by background processes.
	key := []byte("acdfg")
	tc := testContext{}
	tc.Start(t)
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.GetRequest{
		Span: roachpb.Span{Key: key},
	})
	if err := ba.SetActiveTimestamp(tc.clock.Now); err != nil {
		t.Fatal(err)
	}
	tc.Stop()
	_, pErr := tc.rng.addWriteCmd(context.Background(), ba)
	if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); !ok {
		t.Fatalf("expected an ambiguous result error; got %v", pErr)
	}
}

// TestComputeChecksumVersioning checks that the ComputeChecksum post-commit
// trigger is called if and only if the checksum version is right.
func TestComputeChecksumVersioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()
	rng := tc.rng

	if _, pct, _ := rng.ComputeChecksum(context.TODO(), nil, nil, roachpb.Header{},
		roachpb.ComputeChecksumRequest{ChecksumID: uuid.MakeV4(), Version: replicaChecksumVersion},
	); pct.ComputeChecksum == nil {
		t.Error("right checksum version: expected post-commit trigger")
	}

	if _, pct, _ := rng.ComputeChecksum(context.TODO(), nil, nil, roachpb.Header{},
		roachpb.ComputeChecksumRequest{ChecksumID: uuid.MakeV4(), Version: replicaChecksumVersion + 1},
	); pct.ComputeChecksum != nil {
		t.Errorf("wrong checksum version: expected no post-commit trigger: %s", pct.ComputeChecksum)
	}
}

func TestNewReplicaCorruptionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for i, tc := range []struct {
		errStruct *roachpb.ReplicaCorruptionError
		expErr    string
	}{
		{NewReplicaCorruptionError(errors.New("")), "replica corruption (processed=false)"},
		{NewReplicaCorruptionError(errors.New("foo")), "replica corruption (processed=false): foo"},
		{NewReplicaCorruptionError(errors.Wrap(errors.New("bar"), "foo")), "replica corruption (processed=false): foo: bar"},
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

	// TODO(tschottdorf): this test should really pass the data through a
	// RocksDB engine to verify that the original snapshots sort correctly.

	if diff := diffRange(nil, nil); diff != nil {
		t.Fatalf("diff of nils =  %v", diff)
	}

	timestamp := hlc.Timestamp{WallTime: 1729, Logical: 1}
	value := []byte("foo")

	// Construct the two snapshots.
	leaderSnapshot := &roachpb.RaftSnapshotData{
		KV: []roachpb.RaftSnapshotData_KeyValue{
			{Key: []byte("a"), Timestamp: timestamp, Value: value},
			{Key: []byte("abc"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcd"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcde"), Timestamp: timestamp, Value: value},
			// Timestamps sort in descending order, with the notable exception
			// of the zero timestamp, which sorts first.
			{Key: []byte("abcdefg"), Timestamp: hlc.ZeroTimestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, -1), Value: value},
			{Key: []byte("abcdefgh"), Timestamp: timestamp, Value: value},
			{Key: []byte("x"), Timestamp: timestamp, Value: value},
			{Key: []byte("y"), Timestamp: timestamp, Value: value},
			// Both 'zeroleft' and 'zeroright' share the version at (1,1), but
			// a zero timestamp (=meta) key pair exists on the leader or
			// follower, respectively.
			{Key: []byte("zeroleft"), Timestamp: hlc.ZeroTimestamp, Value: value},
			{Key: []byte("zeroleft"), Timestamp: hlc.ZeroTimestamp.Add(1, 1), Value: value},
			{Key: []byte("zeroright"), Timestamp: hlc.ZeroTimestamp.Add(1, 1), Value: value},
		},
	}

	// No diff works.
	if diff := diffRange(leaderSnapshot, leaderSnapshot); diff != nil {
		t.Fatalf("diff of equal snapshots = %v", diff)
	}

	replicaSnapshot := &roachpb.RaftSnapshotData{
		KV: []roachpb.RaftSnapshotData_KeyValue{
			{Key: []byte("ab"), Timestamp: timestamp, Value: value},
			{Key: []byte("abc"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcde"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdef"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: hlc.ZeroTimestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, 1), Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefgh"), Timestamp: timestamp, Value: value},
			{Key: []byte("x"), Timestamp: timestamp, Value: []byte("bar")},
			{Key: []byte("z"), Timestamp: timestamp, Value: value},
			{Key: []byte("zeroleft"), Timestamp: hlc.ZeroTimestamp.Add(1, 1), Value: value},
			{Key: []byte("zeroright"), Timestamp: hlc.ZeroTimestamp, Value: value},
			{Key: []byte("zeroright"), Timestamp: hlc.ZeroTimestamp.Add(1, 1), Value: value},
		},
	}

	// The expected diff.
	eDiff := ReplicaSnapshotDiffSlice{
		{LeaseHolder: true, Key: []byte("a"), Timestamp: timestamp, Value: value},
		{LeaseHolder: false, Key: []byte("ab"), Timestamp: timestamp, Value: value},
		{LeaseHolder: true, Key: []byte("abcd"), Timestamp: timestamp, Value: value},
		{LeaseHolder: false, Key: []byte("abcdef"), Timestamp: timestamp, Value: value},
		{LeaseHolder: false, Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, 1), Value: value},
		{LeaseHolder: true, Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, -1), Value: value},
		{LeaseHolder: true, Key: []byte("x"), Timestamp: timestamp, Value: value},
		{LeaseHolder: false, Key: []byte("x"), Timestamp: timestamp, Value: []byte("bar")},
		{LeaseHolder: true, Key: []byte("y"), Timestamp: timestamp, Value: value},
		{LeaseHolder: false, Key: []byte("z"), Timestamp: timestamp, Value: value},
		{LeaseHolder: true, Key: []byte("zeroleft"), Timestamp: hlc.ZeroTimestamp, Value: value},
		{LeaseHolder: false, Key: []byte("zeroright"), Timestamp: hlc.ZeroTimestamp, Value: value},
	}

	diff := diffRange(leaderSnapshot, replicaSnapshot)

	for i, e := range eDiff {
		v := diff[i]
		if e.LeaseHolder != v.LeaseHolder || !bytes.Equal(e.Key, v.Key) || !e.Timestamp.Equal(v.Timestamp) || !bytes.Equal(e.Value, v.Value) {
			t.Fatalf("diff varies at row %d, want %v and got %v\n\ngot:\n%s\nexpected:\n%s", i, e, v, diff, eDiff)
		}
	}

	// Document the stringifed output. This is what the consistency checker
	// will actually print.
	stringDiff := append(eDiff[:4],
		ReplicaSnapshotDiff{Key: []byte("foo"), Value: value},
	)

	const expDiff = `--- leaseholder
+++ follower
-0.000001729,1 "a"
-  ts:1970-01-01 00:00:00.000001729 +0000 UTC
-  value:foo
-  raw_key:"a" raw_value:666f6f
+0.000001729,1 "ab"
+  ts:1970-01-01 00:00:00.000001729 +0000 UTC
+  value:foo
+  raw_key:"ab" raw_value:666f6f
-0.000001729,1 "abcd"
-  ts:1970-01-01 00:00:00.000001729 +0000 UTC
-  value:foo
-  raw_key:"abcd" raw_value:666f6f
+0.000001729,1 "abcdef"
+  ts:1970-01-01 00:00:00.000001729 +0000 UTC
+  value:foo
+  raw_key:"abcdef" raw_value:666f6f
+0.000000000,0 "foo"
+  ts:<zero>
+  value:foo
+  raw_key:"foo" raw_value:666f6f
`

	if diff := stringDiff.String(); diff != expDiff {
		t.Fatalf("expected:\n%s\ngot:\n%s", expDiff, diff)
	}
}

func TestSyncSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsc := TestStoreConfig()
	tc := testContext{}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	// With enough time in BlockingSnapshotDuration, we succeed on the
	// first try.
	tc.rng.mu.Lock()
	snap, err := tc.rng.Snapshot()
	tc.rng.mu.Unlock()
	tc.rng.CloseOutSnap()

	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Data) == 0 {
		t.Fatal("snapshot is empty")
	}
}

// TestReplicaIDChangePending verifies that on a replica ID change, pending
// commands are re-proposed on the new raft group.
func TestReplicaIDChangePending(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	cfg := TestStoreConfig()
	// Disable ticks to avoid automatic reproposals after a timeout, which
	// would pass this test.
	cfg.RaftTickInterval = math.MaxInt32
	tc.StartWithStoreConfig(t, cfg)
	defer tc.Stop()
	rng := tc.rng

	// Stop the command from being proposed to the raft group and being removed.
	rng.mu.Lock()
	rng.mu.submitProposalFn = func(p *ProposalData) error { return nil }
	rng.mu.Unlock()

	// Add a command to the pending list.
	magicTS := tc.clock.Now()
	ba := roachpb.BatchRequest{}
	ba.Timestamp = magicTS
	ba.Add(&roachpb.GetRequest{
		Span: roachpb.Span{
			Key: roachpb.Key("a"),
		},
	})
	_, _, err := rng.propose(context.Background(), ba)
	if err != nil {
		t.Fatal(err)
	}

	// Set the raft command handler so we can tell if the command has been
	// re-proposed.
	commandProposed := make(chan struct{}, 1)
	rng.mu.Lock()
	rng.mu.submitProposalFn = func(p *ProposalData) error {
		if p.Cmd.Timestamp.Equal(magicTS) {
			commandProposed <- struct{}{}
		}
		return nil
	}
	rng.mu.Unlock()

	// Set the ReplicaID on the replica.
	if err := rng.setReplicaID(2); err != nil {
		t.Fatal(err)
	}

	<-commandProposed
}

func TestReplicaRetryRaftProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	var tc testContext
	tc.Start(t)
	defer tc.Stop()

	type magicKey struct{}

	var c int32                // updated atomically
	var wrongLeaseIndex uint64 // populated below

	tc.rng.mu.Lock()
	tc.rng.mu.submitProposalFn = func(cmd *ProposalData) error {
		if v := cmd.ctx.Value(magicKey{}); v != nil {
			if curAttempt := atomic.AddInt32(&c, 1); curAttempt == 1 {
				cmd.MaxLeaseIndex = wrongLeaseIndex
			}
		}
		return defaultSubmitProposalLocked(tc.rng, cmd)
	}
	tc.rng.mu.Unlock()

	pArg := putArgs(roachpb.Key("a"), []byte("asd"))
	{
		var ba roachpb.BatchRequest
		ba.Add(&pArg)
		ba.Timestamp = tc.clock.Now()
		if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	tc.rng.mu.Lock()
	ai := tc.rng.mu.state.LeaseAppliedIndex
	tc.rng.mu.Unlock()

	if ai < 1 {
		t.Fatal("committed a batch, but still at lease index zero")
	}

	wrongLeaseIndex = ai - 1 // used by submitProposalFn above

	log.Infof(ctx, "test begins")

	var ba roachpb.BatchRequest
	ba.Timestamp = tc.clock.Now()
	const expInc = 123
	iArg := incrementArgs(roachpb.Key("b"), expInc)
	ba.Add(&iArg)
	{
		br, pErr, shouldRetry := tc.rng.tryAddWriteCmd(
			context.WithValue(ctx, magicKey{}, "foo"),
			ba,
		)
		if !shouldRetry {
			t.Fatalf("expected retry, but got (%v, %v)", br, pErr)
		}
		if exp, act := int32(1), atomic.LoadInt32(&c); exp != act {
			t.Fatalf("expected %d proposals, got %d", exp, act)
		}
	}

	atomic.StoreInt32(&c, 0)
	{
		br, pErr := tc.rng.addWriteCmd(
			context.WithValue(ctx, magicKey{}, "foo"),
			ba,
		)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if exp, act := int32(2), atomic.LoadInt32(&c); exp != act {
			t.Fatalf("expected %d proposals, got %d", exp, act)
		}
		if resp, ok := br.Responses[0].GetInner().(*roachpb.IncrementResponse); !ok || resp.NewValue != expInc {
			t.Fatalf("expected new value %d, got (%t, %+v)", expInc, ok, resp)
		}
	}
}

// TestReplicaCancelRaftCommandProgress creates a number of Raft commands and
// immediately abandons some of them, while proposing the remaining ones. It
// then verifies that all the non-abandoned commands get applied (which would
// not be the case if gaps in the applied index posed an issue).
func TestReplicaCancelRaftCommandProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var tc testContext
	tc.Start(t)
	defer tc.Stop()
	rng := tc.rng
	repDesc, err := rng.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	const num = 10

	var chs []chan proposalResult // protected by rng.mu

	func() {
		for i := 0; i < num; i++ {
			var ba roachpb.BatchRequest
			ba.Timestamp = tc.clock.Now()
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{
				Key: roachpb.Key(fmt.Sprintf("k%d", i))}})
			cmd, pErr := rng.evaluateProposal(
				context.Background(), propEvalKV, makeIDKey(), repDesc, ba,
			)
			if pErr != nil {
				t.Fatal(pErr)
			}
			rng.mu.Lock()
			rng.insertProposalLocked(cmd)
			// We actually propose the command only if we don't
			// cancel it to simulate the case in which Raft loses
			// the command and it isn't reproposed due to the
			// client abandoning it.
			if rand.Intn(2) == 0 {
				log.Infof(context.Background(), "abandoning command %d", i)
				delete(rng.mu.proposals, cmd.idKey)
			} else if err := rng.submitProposalLocked(cmd); err != nil {
				t.Error(err)
			} else {
				chs = append(chs, cmd.done)
			}
			rng.mu.Unlock()
		}
	}()

	for _, ch := range chs {
		if rwe := <-ch; rwe.Err != nil {
			t.Fatal(rwe.Err)
		}
	}
}

// TestReplicaBurstPendingCommandsAndRepropose verifies that a burst of
// proposed commands assigns a correct sequence of required indexes,
// and then goes and checks that a reproposal (without prior proposal) results
// in these commands applying at the computed indexes.
func TestReplicaBurstPendingCommandsAndRepropose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(bdarnell): https://github.com/cockroachdb/cockroach/issues/8422")
	var tc testContext
	tc.Start(t)
	defer tc.Stop()

	const num = 10
	repDesc, err := tc.rng.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	type magicKey struct{}

	var seenCmds []int
	tc.rng.mu.Lock()
	tc.rng.mu.submitProposalFn = func(cmd *ProposalData) error {
		if v := cmd.ctx.Value(magicKey{}); v != nil {
			seenCmds = append(seenCmds, int(cmd.MaxLeaseIndex))
		}
		return defaultSubmitProposalLocked(tc.rng, cmd)
	}
	tc.rng.mu.Unlock()

	if pErr := tc.rng.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	expIndexes := make([]int, 0, num)
	chs := func() []chan proposalResult {
		tc.rng.mu.Lock()
		defer tc.rng.mu.Unlock()
		chs := make([]chan proposalResult, 0, num)

		origIndexes := make([]int, 0, num)
		for i := 0; i < num; i++ {
			expIndexes = append(expIndexes, i+1)
			ctx := context.WithValue(context.Background(), magicKey{}, "foo")
			var ba roachpb.BatchRequest
			ba.Timestamp = tc.clock.Now()
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{
				Key: roachpb.Key(fmt.Sprintf("k%d", i))}})
			cmd, pErr := tc.rng.evaluateProposal(ctx, propEvalKV, makeIDKey(), repDesc, ba)
			if pErr != nil {
				t.Fatal(pErr)
			}

			tc.rng.mu.Lock()
			tc.rng.insertProposalLocked(cmd)
			chs = append(chs, cmd.done)
			tc.rng.mu.Unlock()
		}

		tc.rng.mu.Lock()
		for _, p := range tc.rng.mu.proposals {
			if v := p.ctx.Value(magicKey{}); v != nil {
				origIndexes = append(origIndexes, int(p.MaxLeaseIndex))
			}
		}
		tc.rng.mu.Unlock()

		sort.Ints(origIndexes)

		if !reflect.DeepEqual(expIndexes, origIndexes) {
			t.Fatalf("wanted required indexes %v, got %v", expIndexes, origIndexes)
		}

		tc.rng.refreshProposalsLocked(0, reasonTicks)
		return chs
	}()
	for _, ch := range chs {
		if pErr := (<-ch).Err; pErr != nil {
			t.Fatal(pErr)
		}
	}

	if !reflect.DeepEqual(seenCmds, expIndexes) {
		t.Fatalf("expected indexes %v, got %v", expIndexes, seenCmds)
	}

	tc.rng.mu.Lock()
	defer tc.rng.mu.Unlock()
	nonePending := len(tc.rng.mu.proposals) == 0
	c := int(tc.rng.mu.lastAssignedLeaseIndex) - int(tc.rng.mu.state.LeaseAppliedIndex)
	if nonePending && c > 0 {
		t.Errorf("no pending cmds, but have required index offset %d", c)
	}
	if !nonePending {
		t.Fatalf("still pending commands: %+v", tc.rng.mu.proposals)
	}
}

func TestReplicaRefreshPendingCommandsTicks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var tc testContext
	cfg := TestStoreConfig()
	// Disable ticks which would interfere with the manual ticking in this test.
	cfg.RaftTickInterval = math.MaxInt32
	tc.StartWithStoreConfig(t, cfg)
	defer tc.Stop()

	// Grab Replica.raftMu in order to block normal raft replica processing. This
	// test is ticking the replica manually and doesn't want the store to be
	// doing so concurrently.
	r := tc.rng

	repDesc, err := r.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	electionTicks := tc.store.cfg.RaftElectionTimeoutTicks

	{
		// The verifications of the reproposal counts below rely on r.mu.ticks
		// starting with a value of 0 (modulo electionTicks). Move the replica into
		// that state in case the replica was ticked before we grabbed
		// processRaftMu.
		r.mu.Lock()
		ticks := r.mu.ticks
		r.mu.Unlock()
		for ; (ticks % electionTicks) != 0; ticks++ {
			if _, err := r.tick(); err != nil {
				t.Fatal(err)
			}
		}
	}

	var dropProposals struct {
		syncutil.Mutex
		m map[*ProposalData]struct{}
	}
	dropProposals.m = make(map[*ProposalData]struct{})

	r.mu.Lock()
	r.mu.submitProposalFn = func(pd *ProposalData) error {
		dropProposals.Lock()
		defer dropProposals.Unlock()
		if _, ok := dropProposals.m[pd]; !ok {
			return defaultSubmitProposalLocked(r, pd)
		}
		return nil // pretend we proposed though we didn't
	}
	r.mu.Unlock()

	// We tick the replica 2*RaftElectionTimeoutTicks. RaftElectionTimeoutTicks
	// is special in that it controls how often pending commands are reproposed.
	for i := 0; i < 2*electionTicks; i++ {
		// Add another pending command on each iteration.
		id := fmt.Sprintf("%08d", i)
		var ba roachpb.BatchRequest
		ba.Timestamp = tc.clock.Now()
		ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: roachpb.Key(id)}})
		cmd, pErr := r.evaluateProposal(context.Background(), propEvalKV,
			storagebase.CmdIDKey(id), repDesc, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}

		dropProposals.Lock()
		dropProposals.m[cmd] = struct{}{} // silently drop proposals
		dropProposals.Unlock()

		r.mu.Lock()
		r.insertProposalLocked(cmd)
		if err := r.submitProposalLocked(cmd); err != nil {
			t.Error(err)
		}
		// Build the map of expected reproposals at this stage.
		m := map[storagebase.CmdIDKey]int{}
		for id, p := range r.mu.proposals {
			m[id] = p.proposedAtTicks
		}
		r.mu.Unlock()

		// Tick raft.
		if _, err := r.tick(); err != nil {
			t.Fatal(err)
		}

		r.mu.Lock()
		ticks := r.mu.ticks
		r.mu.Unlock()

		var reproposed []*ProposalData
		r.mu.Lock() // avoid data race - proposals belong to the Replica
		dropProposals.Lock()
		for p := range dropProposals.m {
			if p.proposedAtTicks >= ticks {
				reproposed = append(reproposed, p)
			}
		}
		dropProposals.Unlock()
		r.mu.Unlock()

		// Reproposals are only performed every electionTicks. We'll need
		// to fix this test if that changes.
		if (ticks % electionTicks) == 0 {
			if len(reproposed) != i-1 {
				t.Fatalf("%d: expected %d reproposed commands, but found %d", i, i-1, len(reproposed))
			}
		} else {
			if len(reproposed) != 0 {
				t.Fatalf("%d: expected no reproposed commands, but found %+v", i, reproposed)
			}
		}
	}
}

// TestCommandTimeThreshold verifies that commands outside the replica GC
// threshold fail.
func TestCommandTimeThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	ts := makeTS(1, 0)
	ts2 := makeTS(2, 0)
	ts3 := makeTS(3, 0)

	key := roachpb.Key("a")
	keycp := roachpb.Key("c")

	va := []byte("a")
	vb := []byte("b")

	// Verify a Get works.
	gArgs := getArgs(key)
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts,
	}, &gArgs); err != nil {
		t.Fatalf("could not get data: %s", err)
	}
	// Verify a later Get works.
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &gArgs); err != nil {
		t.Fatalf("could not get data: %s", err)
	}

	// Put some data for use with CP later on.
	pArgs := putArgs(keycp, va)
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts,
	}, &pArgs); err != nil {
		t.Fatalf("could not put data: %s", err)
	}

	// Do a GC.
	gcr := roachpb.GCRequest{
		Threshold: ts2,
	}
	if _, err := tc.SendWrapped(&gcr); err != nil {
		t.Fatal(err)
	}

	// Do the same Get, which should now fail.
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts,
	}, &gArgs); err == nil {
		t.Fatal("expected failure")
	} else if err.String() != "batch timestamp 0.000000001,0 must be after replica GC threshold 0.000000002,0" {
		t.Fatalf("unexpected error: %s", err)
	}

	// Verify a later Get works.
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &gArgs); err != nil {
		t.Fatalf("could not get data: %s", err)
	}

	// Verify an early CPut fails.
	cpArgs := cPutArgs(keycp, vb, va)
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts2,
	}, &cpArgs); err == nil {
		t.Fatal("expected failure")
	} else if err.String() != "batch timestamp 0.000000002,0 must be after replica GC threshold 0.000000002,0" {
		t.Fatalf("unexpected error: %s", err)
	}
	// Verify a later CPut works.
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &cpArgs); err != nil {
		t.Fatalf("could not cput data: %s", err)
	}
}

// TestReserveAndApplySnapshot checks to see if a snapshot is correctly applied
// and that its reservation is removed.
func TestReserveAndApplySnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsc := TestStoreConfig()
	tc := testContext{}
	tc.StartWithStoreConfig(t, tsc)
	defer tc.Stop()

	checkReservations := func(t *testing.T, expected int) {
		tc.store.bookie.mu.Lock()
		defer tc.store.bookie.mu.Unlock()
		if e, a := expected, len(tc.store.bookie.mu.reservationsByRangeID); e != a {
			t.Fatalf("wrong number of reservations - expected:%d, actual:%d", e, a)
		}
	}

	key := roachpb.RKey("a")
	firstRng := tc.store.LookupReplica(key, nil)
	snap, err := firstRng.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tc.store.metrics.Available.Update(tc.store.bookie.maxReservedBytes)

	// Note that this is an artificial scenario in which we're adding a
	// reservation for a replica that is already on the range. This test is
	// designed to test the filling of the reservation specifically and in
	// normal operation there should not be a reservation for an existing
	// replica.
	req := ReservationRequest{
		StoreRequestHeader: StoreRequestHeader{
			StoreID: tc.store.StoreID(),
			NodeID:  tc.store.nodeDesc.NodeID,
		},
		RangeID:   firstRng.RangeID,
		RangeSize: 10,
	}

	if !tc.store.Reserve(context.Background(), req).Reserved {
		t.Fatalf("Can't reserve the replica")
	}
	checkReservations(t, 1)

	b := firstRng.store.Engine().NewBatch()
	var alloc bufalloc.ByteAllocator
	for ; snap.Iter.Valid(); snap.Iter.Next() {
		var key engine.MVCCKey
		var value []byte
		alloc, key, value = snap.Iter.allocIterKeyValue(alloc)
		mvccKey := engine.MVCCKey{
			Key:       key.Key,
			Timestamp: key.Timestamp,
		}
		if err := b.Put(mvccKey, value); err != nil {
			t.Fatal(err)
		}
	}

	// Apply a snapshot and check the reservation was filled. Note that this
	// out-of-band application could be a root cause if this test ever crashes.
	if err := firstRng.applySnapshot(context.Background(), IncomingSnapshot{
		SnapUUID:        snap.SnapUUID,
		RangeDescriptor: *firstRng.Desc(),
		Batches:         [][]byte{b.Repr()},
	},
		snap.RaftSnap, raftpb.HardState{}); err != nil {
		t.Fatal(err)
	}
	firstRng.CloseOutSnap()
	checkReservations(t, 0)
}

func TestDeprecatedRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	if reply, err := tc.SendWrapped(&roachpb.DeprecatedVerifyChecksumRequest{}); err != nil {
		t.Fatal(err)
	} else if _, ok := reply.(*roachpb.DeprecatedVerifyChecksumResponse); !ok {
		t.Fatalf("expected %T but got %T", &roachpb.DeprecatedVerifyChecksumResponse{}, reply)
	}
}
