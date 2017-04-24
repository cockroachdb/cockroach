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

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

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
func leaseExpiry(repl *Replica) int64 {
	if l, _ := repl.getLease(); l != nil {
		if l.Type() != roachpb.LeaseExpiration {
			panic("leaseExpiry only valid for expiration-based leases")
		}
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
	repl          *Replica
	rangeID       roachpb.RangeID
	gossip        *gossip.Gossip
	engine        engine.Engine
	manualClock   *hlc.ManualClock
	bootstrapMode bootstrapMode
}

func (tc *testContext) Clock() *hlc.Clock {
	return tc.store.cfg.Clock
}

// Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(t testing.TB, stopper *stop.Stopper) {
	tc.manualClock = hlc.NewManualClock(123)
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	tc.StartWithStoreConfig(t, stopper, cfg)
}

// StartWithStoreConfig initializes the test context with a single
// range covering the entire keyspace.
func (tc *testContext) StartWithStoreConfig(t testing.TB, stopper *stop.Stopper, cfg StoreConfig) {
	tc.TB = t
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(stopper)
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(cfg.AmbientCtx, &base.Config{Insecure: true}, cfg.Clock, stopper)
		server := rpc.NewServer(rpcContext) // never started
		tc.gossip = gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())
	}
	if tc.engine == nil {
		tc.engine = engine.NewInMem(roachpb.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
		stopper.AddCloser(tc.engine)
	}
	if tc.transport == nil {
		tc.transport = NewDummyRaftTransport()
	}

	if tc.store == nil {
		cfg.Gossip = tc.gossip
		cfg.Transport = tc.transport
		cfg.StorePool = NewTestStorePool(cfg)
		// Create a test sender without setting a store. This is to deal with the
		// circular dependency between the test sender and the store. The actual
		// store will be passed to the sender after it is created and bootstrapped.
		sender := &testSender{}
		cfg.DB = client.NewDB(sender, cfg.Clock)
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

		if tc.repl == nil && tc.bootstrapMode == bootstrapRangeWithMetadata {
			if err := tc.store.BootstrapRange(nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := tc.store.Start(context.Background(), stopper); err != nil {
			t.Fatal(err)
		}
		tc.store.WaitForInit()
	}

	realRange := tc.repl == nil

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
			repl, err := NewReplica(testDesc, tc.store, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.store.AddReplica(repl); err != nil {
				t.Fatal(err)
			}
		}
		var err error
		tc.repl, err = tc.store.GetReplica(1)
		if err != nil {
			t.Fatal(err)
		}
		tc.rangeID = tc.repl.RangeID
	}

	if err := tc.initConfigs(realRange, t); err != nil {
		t.Fatal(err)
	}
}

func (tc *testContext) Sender() client.Sender {
	return client.Wrap(tc.repl, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
		if ba.RangeID == 0 {
			ba.RangeID = 1
		}
		if ba.Timestamp == (hlc.Timestamp{}) {
			if err := ba.SetActiveTimestamp(tc.Clock().Now); err != nil {
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

// initConfigs creates default configuration entries.
func (tc *testContext) initConfigs(realRange bool, t testing.TB) error {
	// Put an empty system config into gossip so that gossip callbacks get
	// run. We're using a fake config, but it's hooked into SystemConfig.
	if err := tc.gossip.AddInfoProto(gossip.KeySystemConfig,
		&config.SystemConfig{}, 0); err != nil {
		return err
	}

	testutils.SucceedsSoon(t, func() error {
		if _, ok := tc.gossip.GetSystemConfig(); !ok {
			return errors.Errorf("expected system config to be set")
		}
		return nil
	})

	return nil
}

// addBogusReplicaToRangeDesc modifies the range descriptor to include a second
// replica. This is useful for tests that want to pretend they're transferring
// the range lease away, as the lease can only be obtained by Replicas which are
// part of the range descriptor.
// This is a workaround, but it's sufficient for the purposes of several tests.
func (tc *testContext) addBogusReplicaToRangeDesc(
	ctx context.Context,
) (roachpb.ReplicaDescriptor, error) {
	secondReplica := roachpb.ReplicaDescriptor{
		NodeID:    2,
		StoreID:   2,
		ReplicaID: 2,
	}
	oldDesc := *tc.repl.Desc()
	newDesc := oldDesc
	newDesc.Replicas = append(newDesc.Replicas, secondReplica)
	newDesc.NextReplicaID = 3

	// Update the "on-disk" replica state, so that it doesn't diverge from what we
	// have in memory. At the time of this writing, this is not actually required
	// by the tests using this functionality, but it seems sane to do.
	ba := client.Batch{
		Header: roachpb.Header{Timestamp: tc.Clock().Now()},
	}
	descKey := keys.RangeDescriptorKey(oldDesc.StartKey)
	if err := updateRangeDescriptor(&ba, descKey, &oldDesc, newDesc); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	if err := tc.store.DB().Run(ctx, &ba); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}

	tc.repl.setDescWithoutProcessUpdate(&newDesc)
	tc.repl.assertState(tc.engine)
	return secondReplica, nil
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

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
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
	exLease, _ := r.getLease()
	ch, _, err := r.propose(context.TODO(), exLease, ba, nil, nil)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

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
	txn := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())

	if _, err := tc.SendWrappedWith(roachpb.Header{
		Txn:             txn,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &gArgs); err == nil {
		t.Errorf("expected error on inconsistent read within a txn")
	}

	// Lose the lease and verify CONSISTENT reads receive NotLeaseHolderError
	// and INCONSISTENT reads work as expected.
	tc.manualClock.Set(leaseExpiry(tc.repl))
	start := tc.Clock().Now()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      start,
		Expiration: start.Add(10, 0),
		Replica:    secondReplica,
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

// Test the behavior of a replica while a range lease transfer is in progress:
// - while the transfer is in progress, reads should return errors pointing to
// the transfer target.
// - if a transfer fails, the pre-existing lease does not start being used
// again. Instead, a new lease needs to be obtained. This is because, even
// though the transfer got an error, that error is considered ambiguous as the
// transfer might still apply.
func TestBehaviorDuringLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, 100*time.Millisecond)
	tc := testContext{manualClock: manual}
	tsc := TestStoreConfig(clock)
	var leaseAcquisitionTrap atomic.Value
	tsc.TestingKnobs.LeaseRequestEvent = func(ts hlc.Timestamp) {
		val := leaseAcquisitionTrap.Load()
		if val == nil {
			return
		}
		trapCallback := val.(func(ts hlc.Timestamp))
		if trapCallback != nil {
			trapCallback(ts)
		}
	}
	transferSem := make(chan struct{})
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if _, ok := filterArgs.Req.(*roachpb.TransferLeaseRequest); ok {
				// Notify the test that the transfer has been trapped.
				transferSem <- struct{}{}
				// Wait for the test to unblock the transfer.
				<-transferSem
				// Return an error, so that the pendingLeaseRequest considers the
				// transfer failed.
				return roachpb.NewErrorf("injected transfer error")
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	// Do a read to acquire the lease.
	gArgs := getArgs(roachpb.Key("a"))
	if _, err := tc.SendWrapped(&gArgs); err != nil {
		t.Fatal(err)
	}

	// Advance the clock so that the transfer we're going to perform sets a higher
	// minLeaseProposedTS.
	tc.manualClock.Increment((500 * time.Nanosecond).Nanoseconds())

	// Initiate a transfer (async) and wait for it to be blocked.
	transferResChan := make(chan error)
	go func() {
		err := tc.repl.AdminTransferLease(context.Background(), secondReplica.StoreID)
		if !testutils.IsError(err, "injected") {
			transferResChan <- err
		} else {
			transferResChan <- nil
		}
	}()
	<-transferSem
	// Check that a transfer is indeed on-going.
	tc.repl.mu.Lock()
	repDesc, err := tc.repl.getReplicaDescriptorRLocked()
	if err != nil {
		tc.repl.mu.Unlock()
		t.Fatal(err)
	}
	_, pending := tc.repl.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID)
	tc.repl.mu.Unlock()
	if !pending {
		t.Fatalf("expected transfer to be in progress, and it wasn't")
	}

	// Check that, while the transfer is on-going, the replica redirects to the
	// transfer target.
	_, pErr := tc.SendWrapped(&gArgs)
	nlhe, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok || nlhe.LeaseHolder.StoreID != secondReplica.StoreID {
		t.Fatalf("expected not lease holder error pointing to store %d, got %v",
			secondReplica.StoreID, pErr)
	}

	// Unblock the transfer and wait for the pendingLeaseRequest to clear the
	// transfer state.
	transferSem <- struct{}{}
	if err := <-transferResChan; err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		tc.repl.mu.Lock()
		defer tc.repl.mu.Unlock()
		_, pending := tc.repl.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID)
		if pending {
			return errors.New("transfer pending")
		}
		return nil
	})

	// Check that the replica doesn't use its lease, even though there's no longer
	// a transfer in progress. This is because, even though the transfer got an
	// error, that error is considered ambiguous as the transfer might still
	// apply.
	// Concretely, we're going to check that a read triggers a new lease
	// acquisition.
	tc.repl.mu.Lock()
	minLeaseProposedTS := tc.repl.mu.minLeaseProposedTS
	leaseStartTS := tc.repl.mu.state.Lease.Start
	tc.repl.mu.Unlock()
	if !leaseStartTS.Less(minLeaseProposedTS) {
		t.Fatalf("expected minLeaseProposedTS > lease start. minLeaseProposedTS: %s, "+
			"leas start: %s", minLeaseProposedTS, leaseStartTS)
	}
	expectedLeaseStartTS := tc.manualClock.UnixNano()
	leaseAcquisitionCh := make(chan error)
	leaseAcquisitionTrap.Store(func(ts hlc.Timestamp) {
		if ts.WallTime == expectedLeaseStartTS {
			close(leaseAcquisitionCh)
		} else {
			leaseAcquisitionCh <- errors.Errorf(
				"expected acquisition of lease with start: %d but got start: %s",
				expectedLeaseStartTS, ts)
		}
	})
	// We expect this call to succeed, but after acquiring a new lease.
	if _, err := tc.SendWrapped(&gArgs); err != nil {
		t.Fatal(err)
	}
	// Check that the Send above triggered a lease acquisition.
	select {
	case <-leaseAcquisitionCh:
	case <-time.After(time.Second):
		t.Fatalf("read did not acquire a new lease")
	}
}

// TestApplyCmdLeaseError verifies that when during application of a Raft
// command the proposing node no longer holds the range lease, an error is
// returned. This prevents regression of #1483.
func TestApplyCmdLeaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	pArgs := putArgs(roachpb.Key("a"), []byte("asd"))

	// Lose the lease.
	tc.manualClock.Set(leaseExpiry(tc.repl))
	start := tc.Clock().Now()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      start,
		Expiration: start.Add(10, 0),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	_, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: tc.Clock().Now().Add(-100, 0),
	}, &pArgs)
	if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
		t.Fatalf("expected not lease holder error in return, got %v", pErr)
	}
}

func TestReplicaRangeBoundsChecking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.RKey("a")
	firstRepl := tc.store.LookupReplica(key, nil)
	newRepl := splitTestRange(tc.store, key, key, t)
	if _, pErr := newRepl.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	gArgs := getArgs(roachpb.Key("b"))
	_, pErr := tc.SendWrapped(&gArgs)

	if mismatchErr, ok := pErr.GetDetail().(*roachpb.RangeKeyMismatchError); !ok {
		t.Errorf("expected range key mismatch error: %s", pErr)
	} else {
		if mismatchedDesc := mismatchErr.MismatchedRange; mismatchedDesc == nil || mismatchedDesc.RangeID != firstRepl.RangeID {
			t.Errorf("expected mismatched range to be %d, found %v", firstRepl.RangeID, mismatchedDesc)
		}
		if suggestedDesc := mismatchErr.SuggestedRange; suggestedDesc == nil || suggestedDesc.RangeID != newRepl.RangeID {
			t.Errorf("expected suggested range to be %d, found %v", newRepl.RangeID, suggestedDesc)
		}
	}
}

// hasLease returns whether the most recent range lease was held by the given
// range replica and whether it's expired for the given timestamp.
func hasLease(repl *Replica, timestamp hlc.Timestamp) (owned bool, expired bool) {
	repl.mu.Lock()
	defer repl.mu.Unlock()
	status := repl.leaseStatus(repl.mu.state.Lease, timestamp, repl.mu.minLeaseProposedTS)
	return repl.mu.state.Lease.OwnedBy(repl.store.StoreID()), status.state != leaseValid
}

func TestReplicaLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	// Test that leases with invalid times are rejected.
	// Start leases at a point that avoids overlapping with the existing lease.
	one := hlc.Timestamp{WallTime: time.Second.Nanoseconds(), Logical: 0}
	for _, lease := range []roachpb.Lease{
		{Start: one, Expiration: hlc.Timestamp{}},
	} {
		if _, err := evalRequestLease(context.Background(), tc.store.Engine(),
			CommandArgs{
				EvalCtx: ReplicaEvalContext{tc.repl, nil},
				Args: &roachpb.RequestLeaseRequest{
					Lease: lease,
				},
			}, &roachpb.RequestLeaseResponse{}); !testutils.IsError(err, "illegal lease") {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if held, _ := hasLease(tc.repl, tc.Clock().Now()); !held {
		t.Errorf("expected lease on range start")
	}
	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now.Add(10, 0),
		Expiration: now.Add(20, 0),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}
	if held, expired := hasLease(tc.repl, tc.Clock().Now().Add(15, 0)); held || expired {
		t.Errorf("expected second replica to have range lease")
	}

	{
		_, pErr := tc.repl.redirectOnOrAcquireLease(context.Background())
		if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok || lErr == nil {
			t.Fatalf("wanted NotLeaseHolderError, got %s", pErr)
		}
	}
	// Advance clock past expiration and verify that another has
	// range lease will not be true.
	tc.manualClock.Increment(21) // 21ns have passed
	if held, expired := hasLease(tc.repl, tc.Clock().Now()); held || !expired {
		t.Errorf("expected another replica to have expired lease; %t, %t", held, expired)
	}

	// Verify that command returns NotLeaseHolderError when lease is rejected.
	repl, err := NewReplica(testRangeDescriptor(), tc.store, 0)
	if err != nil {
		t.Fatal(err)
	}

	repl.mu.Lock()
	repl.mu.submitProposalFn = func(*ProposalData) error {
		return &roachpb.LeaseRejectedError{
			Message: "replica not found",
		}
	}
	repl.mu.Unlock()

	{
		_, err := repl.redirectOnOrAcquireLease(context.Background())
		if _, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, err)
		}
	}
}

// TestReplicaNotLeaseHolderError verifies NotLeaderError when lease is rejected.
func TestReplicaNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica:    secondReplica,
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
	defer EnableLeaseHistory(100)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	assert := func(actual, min, max int64) error {
		if actual < min || actual > max {
			return errors.Errorf(
				"metrics counters actual=%d, expected=[%d,%d]",
				actual, min, max,
			)
		}
		return nil
	}
	metrics := tc.repl.store.metrics
	if err := assert(metrics.LeaseRequestSuccessCount.Count(), 1, 1000); err != nil {
		t.Fatal(err)
	}
	if err := assert(metrics.LeaseRequestErrorCount.Count(), 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := tc.repl.store.updateReplicationGauges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a, e := metrics.LeaseExpirationCount.Value(), int64(1); a != e {
		t.Fatalf("expected expiration lease count of %d; got %d", e, a)
	}
	// Check the lease history to ensure it contains the first lease.
	if e, a := 1, len(tc.repl.leaseHistory.get()); e != a {
		t.Fatalf("expected lease history count to be %d, got %d", e, a)
	}

	now := tc.Clock().Now()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := assert(metrics.LeaseRequestSuccessCount.Count(), 2, 1000); err != nil {
		t.Fatal(err)
	}
	if err := assert(metrics.LeaseRequestErrorCount.Count(), 0, 0); err != nil {
		t.Fatal(err)
	}
	// The expiration count should still be 1, as this is a gauge.
	if err := tc.repl.store.updateReplicationGauges(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a, e := metrics.LeaseExpirationCount.Value(), int64(1); a != e {
		t.Fatalf("expected expiration lease count of %d; got %d", e, a)
	}
	// Check the lease history to ensure it recorded the new lease.
	if e, a := 2, len(tc.repl.leaseHistory.get()); e != a {
		t.Fatalf("expected lease history count to be %d, got %d", e, a)
	}

	// Make lease request fail by requesting overlapping lease from bogus Replica.
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    99,
			StoreID:   99,
		},
	}); !testutils.IsError(err, "cannot replace lease") {
		t.Fatal(err)
	}

	if err := assert(metrics.LeaseRequestSuccessCount.Count(), 2, 1000); err != nil {
		t.Fatal(err)
	}
	if err := assert(metrics.LeaseRequestErrorCount.Count(), 1, 1000); err != nil {
		t.Fatal(err)
	}
	// Check the lease history to ensure it did not record the failed lease.
	if e, a := 2, len(tc.repl.leaseHistory.get()); e != a {
		t.Fatalf("expected lease history count to be %d, got %d", e, a)
	}
}

// TestReplicaGossipConfigsOnLease verifies that config info is gossiped
// upon acquisition of the range lease.
func TestReplicaGossipConfigsOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

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
	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()

	// Give lease to someone else.
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	// Expire that lease.
	tc.manualClock.Increment(11 + int64(tc.Clock().MaxOffset())) // advance time
	now = tc.Clock().Now()

	// Give lease to this range.
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now.Add(11, 0),
		Expiration: now.Add(20, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
	}); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	// Disable raft log truncation which confuses this test.
	tc.store.SetRaftLogQueueActive(false)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()

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
		{storeID: secondReplica.StoreID,
			start: now.Add(29, 0), expiration: now.Add(50, 0),
			expErr: "overlaps previous"},
		// The other store tries again, this time without the overlap.
		{storeID: secondReplica.StoreID,
			start: now.Add(31, 0), expiration: now.Add(50, 0),
			// The cache now moves to this other store, and we can't query that.
			expLowWater: 0},
		// Lease is regranted to this store. The low-water mark is updated to the
		// beginning of the lease.
		{storeID: tc.store.StoreID(),
			start: now.Add(60, 0), expiration: now.Add(70, 0),
			// We expect 50, not 60, because the new lease is wound back to the end
			// of the previous lease.
			expLowWater: now.Add(50, 0).WallTime},
	}

	for i, test := range testCases {
		if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
			Start:      test.start,
			Expiration: test.expiration,
			Replica: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(test.storeID),
				NodeID:    roachpb.NodeID(test.storeID),
				StoreID:   test.storeID,
			},
		}); !testutils.IsError(err, test.expErr) {
			t.Fatalf("%d: unexpected error %v", i, err)
		}
		// Verify expected low water mark.
		tc.repl.store.tsCacheMu.Lock()
		rTS, _, _ := tc.repl.store.tsCacheMu.cache.GetMaxRead(roachpb.Key("a"), nil)
		wTS, _, _ := tc.repl.store.tsCacheMu.cache.GetMaxWrite(roachpb.Key("a"), nil)
		tc.repl.store.tsCacheMu.Unlock()

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()
	lease := &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}
	exLease, _ := tc.repl.getLease()
	ba := roachpb.BatchRequest{}
	ba.Timestamp = tc.repl.store.Clock().Now()
	ba.Add(&roachpb.RequestLeaseRequest{Lease: *lease})
	ch, _, err := tc.repl.propose(context.Background(), exLease, ba, nil, nil)
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
// the Store is draining.
func TestReplicaDrainLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Acquire initial lease.
	status, pErr := tc.repl.redirectOnOrAcquireLease(context.Background())
	if pErr != nil {
		t.Fatal(pErr)
	}

	tc.store.SetDraining(true)
	tc.repl.mu.Lock()
	pErr = <-tc.repl.requestLeaseLocked(context.Background(), status)
	tc.repl.mu.Unlock()
	_, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected NotLeaseHolderError, not %v", pErr)
	}
	tc.store.SetDraining(false)
	// Newly undrained, leases work again.
	if _, pErr := tc.repl.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestReplicaGossipFirstRange verifies that the first range gossips its
// location and the cluster ID.
func TestReplicaGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)

	txn := newTransaction("test", key, 1 /* userPriority */, enginepb.SERIALIZABLE, tc.Clock())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Write some arbitrary data in the system span (up to, but not including MaxReservedID+1)
	key := keys.MakeTablePrefix(keys.MaxReservedDescID)

	txn := newTransaction("test", key, 1 /* userPriority */, enginepb.SERIALIZABLE, tc.Clock())
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
	tc.manualClock.Set(leaseExpiry(tc.repl))
	lease, _ := tc.repl.getLease()
	if tc.repl.leaseStatus(lease, tc.Clock().Now(), hlc.Timestamp{}).state != leaseExpired {
		t.Fatal("range lease should have been expired")
	}

	// Make sure the information for db1 is not gossiped. Since obtaining
	// a lease updates the gossiped information, we do that.
	if _, pErr := tc.repl.redirectOnOrAcquireLease(context.Background()); pErr != nil {
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

func iPutArgs(key roachpb.Key, value []byte) roachpb.InitPutRequest {
	return roachpb.InitPutRequest{
		Span: roachpb.Span{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
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
) (roachpb.BeginTransactionRequest, roachpb.Header) {
	return roachpb.BeginTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}, roachpb.Header{Txn: txn}
}

func endTxnArgs(
	txn *roachpb.Transaction, commit bool,
) (roachpb.EndTransactionRequest, roachpb.Header) {
	return roachpb.EndTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key, // not allowed when going through TxnCoordSender, but we're not
		},
		Commit: commit,
	}, roachpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType,
) roachpb.PushTxnRequest {
	return roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: pushee.Key,
		},
		Now:           pusher.Timestamp,
		PushTo:        pusher.Timestamp,
		PusherTxn:     *pusher,
		PusheeTxn:     pushee.TxnMeta,
		PushType:      pushType,
		NewPriorities: true,
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (roachpb.HeartbeatTxnRequest, roachpb.Header) {
	return roachpb.HeartbeatTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Now: now,
	}, roachpb.Header{Txn: txn}
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	pArgs := make([]roachpb.PutRequest, optimizePutThreshold)
	cpArgs := make([]roachpb.ConditionalPutRequest, optimizePutThreshold)
	ipArgs := make([]roachpb.InitPutRequest, optimizePutThreshold)
	for i := 0; i < optimizePutThreshold; i++ {
		pArgs[i] = putArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"))
		cpArgs[i] = cPutArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"), []byte("0"))
		ipArgs[i] = iPutArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"))
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
		// Existing key at "0", ten init puts.
		{
			roachpb.Key("0"),
			[]roachpb.Request{
				&ipArgs[0], &ipArgs[1], &ipArgs[2], &ipArgs[3], &ipArgs[4], &ipArgs[5], &ipArgs[6], &ipArgs[7], &ipArgs[8], &ipArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at 11, mixed put types.
		{
			roachpb.Key("11"),
			[]roachpb.Request{
				&pArgs[0], &cpArgs[1], &pArgs[2], &cpArgs[3], &ipArgs[4], &ipArgs[5], &pArgs[6], &cpArgs[7], &pArgs[8], &ipArgs[9],
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
		// Duplicate iput at 11th key; should see ten puts.
		{
			nil,
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9], &ipArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Duplicate cput at 10th key; should see ten cputs.
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
				hlc.Timestamp{}, roachpb.MakeValueFromString("foo"), nil); err != nil {
				t.Fatal(err)
			}
		}
		batch := roachpb.BatchRequest{}
		for _, r := range c.reqs {
			batch.Add(r)
		}
		// Make a deep clone of the requests slice. We need a deep clone
		// because the regression which is prevented here changed data on the
		// individual requests, and not the slice.
		goldenRequests := append([]roachpb.RequestUnion(nil), batch.Requests...)
		for i := range goldenRequests {
			clone := protoutil.Clone(goldenRequests[i].GetInner()).(roachpb.Request)
			goldenRequests[i].MustSetInner(clone)
		}
		// Save the original slice, allowing us to assert that it doesn't
		// change when it is passed to optimizePuts.
		oldRequests := batch.Requests
		batch.Requests = optimizePuts(tc.engine, batch.Requests, false)
		if !reflect.DeepEqual(goldenRequests, oldRequests) {
			t.Fatalf("%d: optimizePuts mutated the original request slice: %s",
				i, pretty.Diff(goldenRequests, oldRequests),
			)
		}

		blind := []bool{}
		for _, r := range batch.Requests {
			switch t := r.GetInner().(type) {
			case *roachpb.PutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			case *roachpb.ConditionalPutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			case *roachpb.InitPutRequest:
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

	for _, test := range []roachpb.Request{
		&gArgs,
		&pArgs,
	} {
		t.Run("", func(t *testing.T) {
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			tc.Start(t, stopper)
			// This is a single-replica test; since we're automatically pushing back
			// the start of a lease as far as possible, and since there is an auto-
			// matic lease for us at the beginning, we'll basically create a lease from
			// then on.
			lease, _ := tc.repl.getLease()
			expStart := lease.Start
			tc.manualClock.Set(leaseExpiry(tc.repl))

			ts := tc.Clock().Now().Next()
			if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, test); pErr != nil {
				t.Error(pErr)
			}
			if held, expired := hasLease(tc.repl, ts); !held || expired {
				t.Errorf("expected lease acquisition")
			}
			lease, _ = tc.repl.getLease()
			if lease.Start != expStart {
				t.Errorf("unexpected lease start: %s; expected %s", lease.Start, expStart)
			}

			if lease.DeprecatedStartStasis != lease.Expiration {
				t.Errorf("%s already in stasis (or beyond): %+v", ts, lease)
			}
			if !ts.Less(lease.Expiration) {
				t.Errorf("%s already expired: %+v", ts, lease)
			}

			shouldRenewTS := lease.Expiration.Add(-1, 0)
			tc.manualClock.Set(shouldRenewTS.WallTime + 1)
			if _, pErr := tc.SendWrapped(test); pErr != nil {
				t.Error(pErr)
			}
			// Since the command we sent above does not get blocked on the lease
			// extension, we need to wait for it to go through.
			testutils.SucceedsSoon(t, func() error {
				newLease, _ := tc.repl.getLease()
				if !lease.Expiration.Less(newLease.Expiration) {
					return errors.Errorf("lease did not get extended: %+v to %+v", lease, newLease)
				}
				return nil
			})
		})
	}
}

// TestLeaseConcurrent requests the lease multiple times, all of which
// will join the same LeaseRequest command. This exercises the cloning of
// the *roachpb.Error to ensure that each requestor gets a distinct
// error object (which prevents regression of #6111)
func TestLeaseConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const num = 5

	const origMsg = "boom"
	for _, withError := range []bool{false, true} {
		func(withError bool) {
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			tc.Start(t, stopper)

			var wg sync.WaitGroup
			wg.Add(num)
			var active atomic.Value
			active.Store(false)

			var seen int32
			tc.repl.mu.Lock()
			tc.repl.mu.submitProposalFn = func(proposal *ProposalData) error {
				ll, ok := proposal.Request.Requests[0].
					GetInner().(*roachpb.RequestLeaseRequest)
				if !ok || !active.Load().(bool) {
					return defaultSubmitProposalLocked(tc.repl, proposal)
				}
				if c := atomic.AddInt32(&seen, 1); c > 1 {
					// Morally speaking, this is an error, but reproposals can
					// happen and so we warn (in case this trips the test up
					// in more unexpected ways).
					log.Infof(context.Background(), "reproposal of %+v", ll)
				}
				go func() {
					wg.Wait()
					tc.repl.mu.Lock()
					defer tc.repl.mu.Unlock()
					if withError {
						// When we complete the command, we have to remove it from the map;
						// otherwise its context (and tracing span) may be used after the
						// client cleaned up.
						delete(tc.repl.mu.proposals, proposal.idKey)
						proposal.finish(proposalResult{Err: roachpb.NewErrorf(origMsg)})
						return
					}
					if err := defaultSubmitProposalLocked(tc.repl, proposal); err != nil {
						panic(err) // unlikely, so punt on proper handling
					}
				}()
				return nil
			}
			tc.repl.mu.Unlock()

			active.Store(true)
			tc.manualClock.Increment(leaseExpiry(tc.repl))
			ts := tc.Clock().Now()
			pErrCh := make(chan *roachpb.Error, num)
			for i := 0; i < num; i++ {
				if err := stopper.RunAsyncTask(context.Background(), func(ctx context.Context) {
					tc.repl.mu.Lock()
					status := tc.repl.leaseStatus(tc.repl.mu.state.Lease, ts, hlc.Timestamp{})
					leaseCh := tc.repl.requestLeaseLocked(ctx, status)
					tc.repl.mu.Unlock()
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	startNanos := tc.Clock().Now().WallTime

	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	gArgs := getArgs([]byte("a"))

	if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: tc.Clock().Now()}, &gArgs); pErr != nil {
		t.Error(pErr)
	}
	// Set clock to time 2s for write.
	t1 := 2 * time.Second
	key := roachpb.Key([]byte("b"))
	tc.manualClock.Set(t1.Nanoseconds())
	drArgs := roachpb.NewDeleteRange(key, key.Next(), false)

	if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: tc.Clock().Now()}, drArgs); pErr != nil {
		t.Error(pErr)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	tc.repl.store.tsCacheMu.Lock()
	defer tc.repl.store.tsCacheMu.Unlock()
	_, _, rOK := tc.repl.store.tsCacheMu.cache.GetMaxRead(roachpb.Key("a"), nil)
	_, _, wOK := tc.repl.store.tsCacheMu.cache.GetMaxWrite(roachpb.Key("a"), nil)
	if rOK || wOK {
		t.Errorf("expected rOK=false and wOK=false; rOK=%t, wOK=%t", rOK, wOK)
	}
	tc.repl.store.tsCacheMu.cache.ExpandRequests(hlc.Timestamp{}, tc.repl.Desc().RSpan())
	rTS, _, rOK := tc.repl.store.tsCacheMu.cache.GetMaxRead(roachpb.Key("a"), nil)
	wTS, _, wOK := tc.repl.store.tsCacheMu.cache.GetMaxWrite(roachpb.Key("a"), nil)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != startNanos || !rOK || wOK {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, _, rOK = tc.repl.store.tsCacheMu.cache.GetMaxRead(roachpb.Key("b"), nil)
	wTS, _, wOK = tc.repl.store.tsCacheMu.cache.GetMaxWrite(roachpb.Key("b"), nil)
	if rTS.WallTime != startNanos || wTS.WallTime != t1.Nanoseconds() || rOK || !wOK {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, _, rOK = tc.repl.store.tsCacheMu.cache.GetMaxRead(roachpb.Key("c"), nil)
	wTS, _, wOK = tc.repl.store.tsCacheMu.cache.GetMaxWrite(roachpb.Key("c"), nil)
	if rTS.WallTime != startNanos || wTS.WallTime != startNanos || rOK || wOK {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s; rOK=%t, wOK=%t", rTS, wTS, rOK, wOK)
	}
}

// TestReplicaCommandQueue verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestReplicaCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Test all four combinations of reads & writes waiting.
	testCases := []struct {
		cmd1Read, cmd2Read    bool
		expWait, expLocalWait bool
	}{
		// Read/read doesn't wait.
		{true, true, false, false},
		// A write doesn't wait for an earlier read (except for local keys).
		{true, false, false, true},
		// A read must wait for an earlier write.
		{false, true, true, true},
		// Writes always wait for other writes.
		{false, false, true, true},
	}

	tooLong := 5 * time.Second

	uniqueKeyCounter := int32(0)

	for _, test := range testCases {
		var addReqs []string
		if test.cmd1Read {
			addReqs = []string{"", "noop", "read"}
		} else {
			addReqs = []string{"", "noop", "write"}
		}
		for _, addReq := range addReqs {
			for _, localKey := range []bool{false, true} {
				expWait := test.expWait
				if localKey {
					expWait = test.expLocalWait
				}
				readWriteLabels := map[bool]string{true: "read", false: "write"}
				testName := fmt.Sprintf(
					"%s-%s", readWriteLabels[test.cmd1Read], readWriteLabels[test.cmd2Read],
				)
				switch addReq {
				case "noop":
					testName += "-noop"
				case "read":
					testName += "-addRead"
				case "write":
					testName += "-addWrite"
				}
				if localKey {
					testName += "-local"
				}
				key1 := roachpb.Key(fmt.Sprintf("key1-%s", testName))
				key2 := roachpb.Key(fmt.Sprintf("key2-%s", testName))
				if localKey {
					key1 = keys.MakeRangeKeyPrefix(roachpb.RKey(key1))
					key2 = keys.MakeRangeKeyPrefix(roachpb.RKey(key2))
				}
				t.Run(testName,
					func(t *testing.T) {
						// Intercept commands matching a specific priority and block them.
						const blockingPriority = 42
						blockingStart := make(chan struct{})
						blockingDone := make(chan struct{})

						tc := testContext{}
						tsc := TestStoreConfig(nil)
						tsc.TestingKnobs.TestingEvalFilter =
							func(filterArgs storagebase.FilterArgs) *roachpb.Error {
								if filterArgs.Hdr.UserPriority == blockingPriority && filterArgs.Index == 0 {
									blockingStart <- struct{}{}
									<-blockingDone
								}
								return nil
							}
						stopper := stop.NewStopper()
						defer stopper.Stop(context.TODO())
						tc.StartWithStoreConfig(t, stopper, tsc)

						defer close(blockingDone) // make sure teardown can happen

						sendWithHeader := func(header roachpb.Header, args roachpb.Request) *roachpb.Error {
							ba := roachpb.BatchRequest{}
							ba.Header = header
							ba.Add(args)

							if header.UserPriority == blockingPriority {
								switch addReq {
								case "noop":
									// Add a noop request to make sure that its empty key
									// doesn't cause additional blocking.
									ba.Add(&roachpb.NoopRequest{})

								case "read", "write":
									// Additional reads and writes to unique keys do not
									// cause additional blocking; the read/write nature of
									// the keys in the command queue is determined on a
									// per-request basis.
									key := roachpb.Key(fmt.Sprintf("unique-key-%s-%d", testName, atomic.AddInt32(&uniqueKeyCounter, 1)))
									if addReq == "read" {
										req := getArgs(key)
										ba.Add(&req)
									} else {
										req := putArgs(key, []byte{})
										ba.Add(&req)
									}
								}
							}

							_, pErr := tc.Sender().Send(context.Background(), ba)
							return pErr
						}

						// Asynchronously put a value to the range with blocking enabled.
						cmd1Done := make(chan *roachpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
							args := readOrWriteArgs(key1, test.cmd1Read)
							cmd1Done <- sendWithHeader(roachpb.Header{
								UserPriority: blockingPriority,
							}, args)
						}); err != nil {
							t.Fatal(err)
						}
						// Wait for cmd1 to get into the command queue.
						select {
						case <-blockingStart:
						case <-time.After(tooLong):
							t.Fatalf("waited %s for cmd1 to enter command queue", tooLong)
						}

						// First, try a command for same key as cmd1 to verify whether it blocks.
						cmd2Done := make(chan *roachpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
							args := readOrWriteArgs(key1, test.cmd2Read)
							cmd2Done <- sendWithHeader(roachpb.Header{}, args)
						}); err != nil {
							t.Fatal(err)
						}

						// Next, try read for a non-impacted key--should go through immediately.
						cmd3Done := make(chan *roachpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
							args := readOrWriteArgs(key2, true)
							cmd3Done <- sendWithHeader(roachpb.Header{}, args)
						}); err != nil {
							t.Fatal(err)
						}

						// Verify that cmd3 finishes quickly no matter what cmds 1 and 2 were.
						select {
						case pErr := <-cmd3Done:
							if pErr != nil {
								t.Fatalf("cmd3 failed: %s", pErr)
							}
							// success.
						case pErr := <-cmd1Done:
							t.Fatalf("should not have been able execute cmd1 while blocked (pErr: %v)", pErr)
						case <-time.After(tooLong):
							t.Fatalf("waited %s for cmd3 of key2", tooLong)
						}

						if expWait {
							// Ensure that cmd2 didn't finish while cmd1 is still blocked.
							select {
							case pErr := <-cmd2Done:
								t.Fatalf("should not have been able to execute cmd2 (pErr: %v)", pErr)
							case pErr := <-cmd1Done:
								t.Fatalf("should not have been able to execute cmd1 while blocked (pErr: %v)", pErr)
							default:
								// success
							}
						} else {
							// Ensure that cmd2 finished if we didn't expect to have to wait.
							select {
							case pErr := <-cmd2Done:
								if pErr != nil {
									t.Fatalf("cmd2 failed: %s", pErr)
								}
								// success.
							case pErr := <-cmd1Done:
								t.Fatalf("should not have been able to execute cmd1 while blocked (pErr: %v)", pErr)
							case <-time.After(tooLong):
								t.Fatalf("waited %s for cmd2 of key1", tooLong)
							}
						}

						// Wait for cmd1 to finish.
						blockingDone <- struct{}{}
						select {
						case pErr := <-cmd1Done:
							if pErr != nil {
								t.Fatalf("cmd1 failed: %s", pErr)
							}
							// success.
						case <-time.After(tooLong):
							t.Fatalf("waited %s for cmd1 of key1", tooLong)
						}

						// Wait for cmd2 now if it didn't finish above.
						if test.expWait {
							select {
							case pErr := <-cmd2Done:
								if pErr != nil {
									t.Fatalf("cmd2 failed: %s", pErr)
								}
								// success.
							case <-time.After(tooLong):
								t.Fatalf("waited %s for cmd2 of key1", tooLong)
							}
						}
					})
			}
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
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)
	cmd1Done := make(chan *roachpb.Error)
	go func() {
		args := putArgs(key, []byte{1})

		_, pErr := tc.SendWrapped(&args)
		cmd1Done <- pErr
	}()
	// Wait for cmd1 to get into the command queue.
	<-blockingStart

	// An inconsistent read to the key won't wait.
	cmd2Done := make(chan *roachpb.Error)
	go func() {
		args := getArgs(key)

		_, pErr := tc.SendWrappedWith(roachpb.Header{
			ReadConsistency: roachpb.INCONSISTENT,
		}, &args)
		cmd2Done <- pErr
	}()

	select {
	case pErr := <-cmd2Done:
		if pErr != nil {
			t.Fatal(pErr)
		}
		// success.
	case pErr := <-cmd1Done:
		t.Fatalf("cmd1 should have been blocked, got %v", pErr)
	}

	close(blockingDone)
	if pErr := <-cmd1Done; pErr != nil {
		t.Fatal(pErr)
	}
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
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Hdr.UserPriority == 42 {
				log.Infof(context.Background(), "starting to block %s", filterArgs.Req)
				blockingStart <- struct{}{}
				<-blockingDone
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	defer close(blockingDone) // make sure teardown can happen

	startBlockingCmd := func(ctx context.Context, keys ...roachpb.Key) <-chan *roachpb.Error {
		done := make(chan *roachpb.Error)

		if err := stopper.RunAsyncTask(context.Background(), func(_ context.Context) {
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

	// Start a command that is already cancelled. It will return immediately.
	{
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		done := startBlockingCmd(ctx, key1, key2)
		if pErr := <-done; !testutils.IsPError(pErr, context.Canceled.Error()) {
			t.Fatalf("unexpected error %v", pErr)
		}
	}

	// Start a third command which will wait for the first.
	ctx, cancel := context.WithCancel(context.Background())
	cmd3Done := startBlockingCmd(ctx, key1, key2)

	// Wait until both commands are in the command queue.
	testutils.SucceedsSoon(t, func() error {
		tc.repl.cmdQMu.Lock()
		chans := tc.repl.cmdQMu.global.getWait(false, hlc.Timestamp{}, []roachpb.Span{{Key: key1}, {Key: key2}})
		tc.repl.cmdQMu.Unlock()
		if a, e := len(chans), 2; a < e {
			return errors.Errorf("%d of %d commands in the command queue", a, e)
		}
		return nil
	})

	// Cancel the third command; it will finish even though it is still
	// blocked by the command queue.
	cancel()
	if pErr := <-cmd3Done; !testutils.IsPError(pErr, context.Canceled.Error()) {
		t.Fatal(pErr)
	}

	// Now unblock the first command to allow the test to clean up.
	blockingDone <- struct{}{}
	if pErr := <-cmd1Done; pErr != nil {
		t.Fatal(pErr)
	}
}

// TestReplicaCommandQueueSelfOverlap verifies that self-overlapping
// batches are allowed, and in particular do not deadlock by
// introducing command-queue dependencies between the parts of the
// batch.
func TestReplicaCommandQueueSelfOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	for _, cmd1Read := range []bool{false, true} {
		for _, cmd2Read := range []bool{false, true} {
			name := fmt.Sprintf("%v,%v", cmd1Read, cmd2Read)
			t.Run(name, func(t *testing.T) {
				ba := roachpb.BatchRequest{}
				ba.Add(readOrWriteArgs(roachpb.Key(name), cmd1Read))
				ba.Add(readOrWriteArgs(roachpb.Key(name), cmd2Read))

				// Set a deadline for nicer error behavior on deadlock.
				ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
				defer cancel()
				_, pErr := tc.Sender().Send(ctx, ba)
				if pErr != nil {
					if _, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); ok && !cmd1Read && !cmd2Read {
						// WriteTooOldError is expected in the write/write case because we don't
						// allow self-overlapping non-transactional batches.
					} else {
						t.Fatal(pErr)
					}
				}
			})
		}
	}
}

// TestReplicaCommandQueueTimestampNonInterference verifies that
// reads with earlier timestamps do not interfere with writes.
func TestReplicaCommandQueueTimestampNonInterference(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var blockKey, blockReader, blockWriter atomic.Value
	blockKey.Store(roachpb.Key("a"))
	blockReader.Store(false)
	blockWriter.Store(false)
	blockCh := make(chan struct{}, 1)
	blockedCh := make(chan struct{}, 1)

	tc := testContext{}
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if !filterArgs.Req.Header().Key.Equal(blockKey.Load().(roachpb.Key)) {
				return nil
			}
			if filterArgs.Req.Method() == roachpb.Get && blockReader.Load().(bool) {
				blockedCh <- struct{}{}
				<-blockCh
			} else if filterArgs.Req.Method() == roachpb.Put && blockWriter.Load().(bool) {
				blockedCh <- struct{}{}
				<-blockCh
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	testCases := []struct {
		readerTS    hlc.Timestamp
		writerTS    hlc.Timestamp
		key         roachpb.Key
		readerFirst bool
		interferes  bool
	}{
		// Reader & writer have same timestamps.
		{makeTS(1, 0), makeTS(1, 0), roachpb.Key("a"), true, true},
		{makeTS(1, 0), makeTS(1, 0), roachpb.Key("b"), false, true},
		// Reader has earlier timestamp.
		{makeTS(1, 0), makeTS(1, 1), roachpb.Key("c"), true, false},
		{makeTS(1, 0), makeTS(1, 1), roachpb.Key("d"), false, false},
		// Writer has earlier timestamp.
		{makeTS(1, 1), makeTS(1, 0), roachpb.Key("e"), true, true},
		{makeTS(1, 1), makeTS(1, 0), roachpb.Key("f"), false, true},
		// Local keys always interfere.
		{makeTS(1, 0), makeTS(1, 1), keys.RangeDescriptorKey(roachpb.RKey("a")), true, true},
		{makeTS(1, 0), makeTS(1, 1), keys.RangeDescriptorKey(roachpb.RKey("b")), false, true},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			blockReader.Store(false)
			blockWriter.Store(false)
			blockKey.Store(test.key)
			errCh := make(chan *roachpb.Error, 2)

			baR := roachpb.BatchRequest{}
			baR.Timestamp = test.readerTS
			gArgs := getArgs(test.key)
			baR.Add(&gArgs)
			baW := roachpb.BatchRequest{}
			baW.Timestamp = test.writerTS
			pArgs := putArgs(test.key, []byte("value"))
			baW.Add(&pArgs)

			if test.readerFirst {
				blockReader.Store(true)
				go func() {
					_, pErr := tc.Sender().Send(context.Background(), baR)
					errCh <- pErr
				}()
				<-blockedCh
				go func() {
					_, pErr := tc.Sender().Send(context.Background(), baW)
					errCh <- pErr
				}()
			} else {
				blockWriter.Store(true)
				go func() {
					_, pErr := tc.Sender().Send(context.Background(), baW)
					errCh <- pErr
				}()
				<-blockedCh
				go func() {
					_, pErr := tc.Sender().Send(context.Background(), baR)
					errCh <- pErr
				}()
			}

			if test.interferes {
				select {
				case <-time.After(10 * time.Millisecond):
					// Expected.
				case pErr := <-errCh:
					t.Fatalf("expected interference: got error %s", pErr)
				}
			}
			// Verify no errors on waiting read and write.
			blockCh <- struct{}{}
			for j := 0; j < 2; j++ {
				if pErr := <-errCh; pErr != nil {
					t.Errorf("error %d: unexpected error: %s", j, pErr)
				}
			}
		})
	}
}

// TestReplicaCommandQueueSplitDeclaresWrites verifies that split
// operations declare write access to their entire span. This is
// necessary to avoid conflicting changes to the range's stats, even
// though splits do not actually write to their data span (and
// therefore a failure to declare writes are not caught directly by
// any other test).
func TestReplicaCommandQueueSplitDeclaresWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var spans SpanSet
	commands[roachpb.EndTransaction].DeclareKeys(
		roachpb.RangeDescriptor{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("d")},
		roachpb.Header{},
		&roachpb.EndTransactionRequest{
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				SplitTrigger: &roachpb.SplitTrigger{
					LeftDesc: roachpb.RangeDescriptor{
						StartKey: roachpb.RKey("a"),
						EndKey:   roachpb.RKey("c"),
					},
					RightDesc: roachpb.RangeDescriptor{
						StartKey: roachpb.RKey("c"),
						EndKey:   roachpb.RKey("d"),
					},
				},
			},
		},
		&spans)
	if err := spans.checkAllowed(SpanReadWrite, roachpb.Span{Key: roachpb.Key("b")}); err != nil {
		t.Fatalf("expected declaration of write access, err=%s", err)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
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
	if respH.Timestamp.WallTime != tc.Clock().Now().WallTime {
		t.Errorf("expected write timestamp to upgrade to 1s; got %s", respH.Timestamp)
	}
}

// TestReplicaNoTSCacheInconsistent verifies that the timestamp cache
// is not affected by inconsistent reads.
func TestReplicaNoTSCacheInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	// Set clock to time 1s and do the read.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())
	args := getArgs([]byte("a"))
	ts := tc.Clock().Now()

	_, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp:       ts,
		ReadConsistency: roachpb.INCONSISTENT,
	}, &args)

	if pErr != nil {
		t.Error(pErr)
	}
	pArgs := putArgs([]byte("a"), []byte("value"))

	_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Timestamp: hlc.Timestamp{WallTime: 0, Logical: 1}}, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if respH.Timestamp.WallTime == tc.Clock().Now().WallTime {
		t.Errorf("expected write timestamp not to upgrade to 1s; got %s", respH.Timestamp)
	}
}

// TestReplicaNoTSCacheUpdateOnFailure verifies that read and write
// commands do not update the timestamp cache if they result in
// failure.
func TestReplicaNoTSCacheUpdateOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Test for both read & write attempts.
	for i, read := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		pArgs := putArgs(key, []byte("value"))
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())

		_, pErr := tc.SendWrappedWith(roachpb.Header{
			Txn: txn,
		}, &pArgs)
		if pErr != nil {
			t.Fatalf("test %d: %s", i, pErr)
		}

		// Now attempt read or write.
		args := readOrWriteArgs(key, read)
		ts := tc.Clock().Now() // later timestamp

		if _, pErr := tc.SendWrappedWith(roachpb.Header{
			Timestamp: ts,
		}, args); pErr == nil {
			t.Errorf("test %d: expected failure", i)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		txn.Sequence++
		if _, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatalf("test %d: %s", i, pErr)
		} else if respH.Txn.Timestamp != txn.Timestamp {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Test for both read & write attempts.
	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())

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
	if respH.Txn.Timestamp != txn.Timestamp {
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
	if respH.Timestamp != expTS {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, respH.Timestamp)
	}
}

// TestReplicaAbortCacheReadError verifies that an error is returned
// to the client in the event that a abort cache entry is found but is
// not decodable.
func TestReplicaAbortCacheReadError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	k := []byte("a")
	txn := newTransaction("test", k, 10, enginepb.SERIALIZABLE, tc.Clock())
	args := incrementArgs(k, 1)
	txn.Sequence = 1

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite Abort cache entry with garbage for the last op.
	key := keys.AbortCacheKey(tc.repl.RangeID, *txn.ID)
	err := engine.MVCCPut(context.Background(), tc.engine, nil, key, hlc.Timestamp{}, roachpb.MakeValueFromString("never read in this test"), nil)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := []byte("a")
	{
		txn := newTransaction("test", key, 10, enginepb.SERIALIZABLE, tc.Clock())
		txn.Sequence = int32(1)
		entry := roachpb.AbortCacheEntry{
			Key:       txn.Key,
			Timestamp: txn.Timestamp,
			Priority:  0,
		}
		if err := tc.repl.abortCache.Put(context.Background(), tc.engine, nil, *txn.ID, &entry); err != nil {
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
	txn := newTransaction("test", key, 10, enginepb.SERIALIZABLE, tc.Clock())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn := newTransaction("test", []byte("test"), 10, enginepb.SERIALIZABLE, tc.Clock())
	txn.Sequence = 100
	entry := roachpb.AbortCacheEntry{
		Key:       txn.Key,
		Timestamp: txn.Timestamp,
		Priority:  0,
	}
	if err := tc.repl.abortCache.Put(context.Background(), tc.engine, nil, *txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	args, h := heartbeatArgs(txn, tc.Clock().Now())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := newTransaction("txn: "+strconv.Itoa(i), key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
				if statusError, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); !ok {
					t.Errorf("expected TransactionStatusError but got %T: %s", pErr, pErr)
				} else if e := "transaction deadline exceeded"; statusError.Msg != e {
					t.Errorf("expected %s, got %s", e, statusError.Msg)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	desc := tc.repl.Desc()
	// This test avoids a zero-timestamp regression (see LastActive() below),
	// so avoid zero timestamps.
	tc.manualClock.Increment(123)
	pusher := &roachpb.Transaction{} // non-transactional pusher is enough

	// This pushee should never be allowed to write a txn record because it
	// will be aborted before it even tries.
	pushee := newTransaction("foo", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pushReq := pushTxnArgs(pusher, pushee, roachpb.PUSH_TOUCH)
	pushReq.Now = tc.Clock().Now()
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
				{Key: keys.TransactionKey(pushee.Key, *pushee.ID)},
			},
			TxnSpanGCThreshold: tc.Clock().Now(),
		}
		if _, pErr := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &gcReq); pErr != nil {
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
		txn := newTransaction("foo", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	if statusError, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); !ok {
		t.Errorf("expected TransactionStatusError but got %T: %s", pErr, pErr)
	} else if e := "transaction deadline exceeded"; statusError.Msg != e {
		t.Errorf("expected %s, got %s", e, statusError.Msg)
	}
}

// Test1PCTransactionWriteTimestamp verifies that the transaction's
// timestamp is used when writing values in a 1PC transaction. We
// verify this by updating the timestamp cache for the key being
// written so that the timestamp there is greater than the txn's
// OrigTimestamp.
func Test1PCTransactionWriteTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	for _, iso := range []enginepb.IsolationType{enginepb.SNAPSHOT, enginepb.SERIALIZABLE} {
		t.Run(iso.String(), func(t *testing.T) {
			key := roachpb.Key(iso.String())
			txn := newTransaction("test", key, 1, iso, tc.Clock())
			bt, _ := beginTxnArgs(key, txn)
			put := putArgs(key, []byte("value"))
			et, etH := endTxnArgs(txn, true)

			// Update the timestamp cache for the key being written.
			gArgs := getArgs(key)
			if _, pErr := tc.SendWrapped(&gArgs); pErr != nil {
				t.Fatal(pErr)
			}

			// Now verify that the write triggers a retry for SERIALIZABLE and
			// writes at the higher timestamp for SNAPSHOT.
			var ba roachpb.BatchRequest
			ba.Header = etH
			ba.Add(&bt, &put, &et)
			br, pErr := tc.Sender().Send(context.Background(), ba)
			if iso == enginepb.SNAPSHOT {
				if pErr != nil {
					t.Fatal(pErr)
				}
				if !txn.OrigTimestamp.Less(br.Txn.Timestamp) {
					t.Errorf(
						"expected result timestamp %s > txn orig timestamp %s", br.Txn.Timestamp, txn.OrigTimestamp,
					)
				}
			} else {
				if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
					t.Errorf("expected retry error; got %s", pErr)
				}
			}
		})
	}
}

// TestEndTransactionWithMalformedSplitTrigger verifies an
// EndTransaction call with a malformed commit trigger fails.
func TestEndTransactionWithMalformedSplitTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("foo")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
			LeftDesc: roachpb.RangeDescriptor{
				StartKey: roachpb.RKey("bar"),
				EndKey:   roachpb.RKey("foo"),
			},
			RightDesc: roachpb.RangeDescriptor{
				StartKey: roachpb.RKey("foo"),
				EndKey:   roachpb.RKeyMax,
			},
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := []byte("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		beginReply, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put)
		if pErr != nil {
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
		hBA, h := heartbeatArgs(txn, tc.Clock().Now())

		resp, pErr = tc.SendWrappedWith(h, &hBA)
		if pErr != nil {
			t.Error(pErr)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s, but got %s", hBR.Txn.Status, expStatus)
		}
		if initHeartbeat := beginReply.Header().Txn.LastHeartbeat; hBR.Txn.LastHeartbeat != initHeartbeat {
			t.Errorf("expected transaction last heartbeat to be %s, but got %s", reply.Txn.LastHeartbeat, initHeartbeat)
		}
		key = roachpb.Key(key).Next()
	}
}

// TestEndTransactionAfterHeartbeat verifies that a transaction
// can be committed/aborted after being heartbeat.
func TestEndTransactionAfterHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	for _, commit := range []bool{true, false} {
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		beginReply, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put)
		if pErr != nil {
			t.Fatal(pErr)
		}

		// Start out with a heartbeat to the transaction.
		hBA, h := heartbeatArgs(txn, tc.Clock().Now())
		txn.Sequence++

		resp, pErr := tc.SendWrappedWith(h, &hBA)
		if pErr != nil {
			t.Fatal(pErr)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != roachpb.PENDING {
			t.Errorf("expected transaction status to be %s, but got %s", hBR.Txn.Status, roachpb.PENDING)
		}
		if initHeartbeat := beginReply.Header().Txn.LastHeartbeat; hBR.Txn.LastHeartbeat == initHeartbeat {
			t.Errorf("expected transaction last heartbeat to advance, but it remained at %s", initHeartbeat)
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
		if reply.Txn.LastHeartbeat != hBR.Txn.LastHeartbeat {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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
		pushee := newTransaction("pushee", key, 1, test.isolation, tc.Clock())
		pusher := newTransaction("pusher", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pushee.Priority = roachpb.MinTxnPriority
		pusher.Priority = roachpb.MaxTxnPriority // pusher will win
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := []byte("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	_, btH := beginTxnArgs(key, txn)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Writing = true

	// Start out with a heartbeat to the transaction.
	hBA, h := heartbeatArgs(txn, tc.Clock().Now())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	regressTS := tc.Clock().Now()
	txn := newTransaction("test", roachpb.Key(""), 1, enginepb.SERIALIZABLE, tc.Clock())

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
		{roachpb.Key("d"), roachpb.PENDING, txn.Epoch, regressTS, `txn "test" id=.*: timestamp regression: 0.\d+,\d+`},
	}
	for i, test := range testCases {
		// Establish existing txn state by writing directly to range engine.
		existTxn := txn.Clone()
		existTxn.Key = test.key
		existTxn.Status = test.existStatus
		existTxn.Epoch = test.existEpoch
		existTxn.Timestamp = test.existTS
		txnKey := keys.TransactionKey(test.key, *txn.ID)

		if test.existStatus != doesNotExist {
			if err := engine.MVCCPutProto(context.Background(), tc.repl.store.Engine(), nil, txnKey, hlc.Timestamp{},
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := []byte("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	_, btH := beginTxnArgs(key, txn)
	put := putArgs(key, key)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction by pushing it with maximum priority.
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher.Priority = roachpb.MaxTxnPriority
	pushArgs := pushTxnArgs(pusher, btH.Txn, roachpb.PUSH_ABORT)
	if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Check if the intent has not yet been resolved.
	var ba roachpb.BatchRequest
	gArgs := getArgs(key)
	ba.Add(&gArgs)
	if err := ba.SetActiveTimestamp(tc.Clock().Now); err != nil {
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

// TestRaftReplayProtectionInTxn verifies that transactional batches
// enjoy protection from "Raft retries".
func TestRaftRetryProtectionInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	cfg := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, cfg)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())

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

	// We're going to attempt two retries:
	// - the first one will fail because of a WriteTooOldError that pushes the
	// transaction, which fails the 1PC path and forces the txn to execute
	// normally at which point the WriteTooOld gets indirectly turned into a
	// TransactionRetryError.
	// - the second one fails because the BeginTxn is detected to be a duplicate.
	for i := 0; i < 2; i++ {
		// Reach in and manually send to raft (to simulate Raft retry) and
		// also avoid updating the timestamp cache.
		ba.Timestamp = txn.OrigTimestamp
		lease, _ := tc.repl.getLease()
		ch, _, err := tc.repl.propose(context.Background(), lease, ba, nil, nil)
		if err != nil {
			t.Fatalf("%d: unexpected error: %s", i, err)
		}
		respWithErr := <-ch
		if _, ok := respWithErr.Err.GetDetail().(*roachpb.TransactionRetryError); !ok {
			t.Fatalf("%d: expected TransactionRetryError; got %s", i, respWithErr.Err)
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
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())
		tc.Start(t, stopper)

		if status := tc.repl.RaftStatus(); status != nil {
			t.Fatalf("expected raft group to not be initialized, got RaftStatus() of %v", status)
		}
		var ba roachpb.BatchRequest
		request := action()
		ba.Add(request)
		if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
			t.Fatalf("unexpected error: %s", pErr)
		}

		if tc.repl.RaftStatus() == nil {
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

// TestRaftRetryCantCommitIntents tests that transactional Raft retries cannot
// commit intents.
// It also tests current behavior - that a retried transactional batch can lay
// down an intent that will never be committed. We don't necessarily like this
// behavior, though. Note that normally intents are not left hanging by retries
// like they are in this low-level test. There are two cases:
// - in case of Raft *reproposals*, the MaxLeaseIndex mechanism will make
// the reproposal not execute if the original proposal had already been
// applied.
// - in case of Raft *retries*, in most cases we know that the original proposal
// has been dropped and will never be applied. In some cases, the retry is
// "ambiguous" - we don't know if the original proposal was applied. In those
// cases, the retry does not leave intents around because of the
// batch.WillNotBeRetried bit.
func TestRaftRetryCantCommitIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer setTxnAutoGC(true)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	testCases := []enginepb.IsolationType{enginepb.SERIALIZABLE, enginepb.SNAPSHOT}

	for i, iso := range testCases {
		t.Run(iso.String(), func(t *testing.T) {
			key := roachpb.Key(fmt.Sprintf("a-%d", i))
			keyB := roachpb.Key(fmt.Sprintf("b-%d", i))
			txn := newTransaction("test", key, 1, iso, tc.Clock())

			// Send a batch with put to key.
			var ba roachpb.BatchRequest
			bt, btH := beginTxnArgs(key, txn)
			put := putArgs(key, []byte("value"))
			ba.Header = btH
			ba.Add(&bt)
			ba.Add(&put)
			if err := ba.SetActiveTimestamp(tc.Clock().Now); err != nil {
				t.Fatal(err)
			}
			br, pErr := tc.Sender().Send(context.Background(), ba)
			if pErr != nil {
				t.Fatalf("unexpected error: %s", pErr)
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
				t.Fatalf("unexpected error: %s", pErr)
			}

			// Verify txn record is cleaned.
			var readTxn roachpb.Transaction
			txnKey := keys.TransactionKey(txn.Key, *txn.ID)
			ok, err := engine.MVCCGetProto(context.Background(), tc.repl.store.Engine(), txnKey, hlc.Timestamp{}, true /* consistent */, nil /* txn */, &readTxn)
			if err != nil || ok {
				t.Errorf("expected transaction record to be cleared (%t): %s", ok, err)
			}

			// Now replay begin & put. BeginTransaction should fail with a replay error.
			_, pErr = tc.Sender().Send(context.Background(), ba)
			if _, ok := pErr.GetDetail().(*roachpb.TransactionReplayError); !ok {
				t.Errorf("expected transaction replay for iso=%s; got %s", iso, pErr)
			}

			// Intent should not have been created.
			gArgs := getArgs(key)
			if _, pErr = tc.SendWrapped(&gArgs); pErr != nil {
				t.Errorf("unexpected error reading key: %s", pErr)
			}

			// Send a put for keyB; this currently succeeds as there's nothing to detect
			// the retry.
			if _, _, pErr = SendWrapped(
				context.Background(), tc.Sender(),
				roachpb.Header{Txn: &putTxn}, &putB); pErr != nil {
				t.Error(pErr)
			}

			// EndTransaction should fail with a status error (does not exist).
			_, pErr = tc.SendWrappedWith(etH, &et)
			if _, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); !ok {
				t.Errorf("expected transaction aborted for iso=%s; got %s", iso, pErr)
			}

			// Expect that keyB intent did not get written!
			gArgs = getArgs(keyB)
			_, pErr = tc.SendWrapped(&gArgs)
			if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
				t.Errorf("expected WriteIntentError, got: %v", pErr)
			}
		})
	}
}

// Test that a duplicate BeginTransaction results in a TransactionRetryError, as
// such recognizing that it's likely the result of the batch being retried by
// DistSender.
func TestDuplicateBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if filterArgs.Req.Method() == roachpb.GC {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

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
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
		txnKey := keys.TransactionKey(txn.Key, *txn.ID)
		ok, err := engine.MVCCGetProto(context.Background(), tc.repl.store.Engine(), txnKey, hlc.Timestamp{},
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
	newRepl := splitTestRange(tc.store, splitKey, splitKey, t)

	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
		ba.RangeID = newRepl.RangeID
		if err := ba.SetActiveTimestamp(newRepl.store.Clock().Now); err != nil {
			t.Fatal(err)
		}
		pArgs := putArgs(splitKey.AsRawKey(), []byte("value"))
		ba.Add(&pArgs)
		txn.Sequence++
		if _, pErr := newRepl.Send(context.Background(), ba); pErr != nil {
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
	return newRepl, txn
}

// TestEndTransactionResolveOnlyLocalIntents verifies that an end transaction
// request resolves only local intents within the same batch.
func TestEndTransactionResolveOnlyLocalIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tsc := TestStoreConfig(nil)
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	newRepl, txn := setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Check if the intent in the other range has not yet been resolved.
	{
		var ba roachpb.BatchRequest
		ba.Header.RangeID = newRepl.RangeID
		gArgs := getArgs(splitKey)
		ba.Add(&gArgs)
		if err := ba.SetActiveTimestamp(tc.Clock().Now); err != nil {
			t.Fatal(err)
		}
		_, pErr := newRepl.Send(context.Background(), ba)
		if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
			t.Errorf("expected write intent error, but got %s", pErr)
		}
	}

	txn.Sequence++
	hbArgs, h := heartbeatArgs(txn, tc.Clock().Now())
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
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			tc.Start(t, stopper)

			ctx := log.WithLogTag(context.Background(), "testcase", i)

			rightRepl, txn := setupResolutionTest(t, tc, testKey, splitKey, false /* generate abort cache entry */)

			testutils.SucceedsSoon(t, func() error {
				var gr roachpb.GetResponse
				if _, err := evalGet(
					ctx, tc.engine, CommandArgs{
						Args: &roachpb.GetRequest{Span: roachpb.Span{
							Key: keys.TransactionKey(txn.Key, *txn.ID),
						}},
					},
					&gr,
				); err != nil {
					return err
				} else if gr.Value != nil {
					return errors.Errorf("%d: txn entry still there: %+v", i, gr)
				}

				var entry roachpb.AbortCacheEntry
				if aborted, err := tc.repl.abortCache.Get(ctx, tc.engine, *txn.ID, &entry); err != nil {
					t.Fatal(err)
				} else if aborted {
					return errors.Errorf("%d: abort cache still populated: %v", i, entry)
				}
				if aborted, err := rightRepl.abortCache.Get(ctx, tc.engine, *txn.ID, &entry); err != nil {
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
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Now test that no GCRequest is issued. We can't test that directly (since
	// it's completely asynchronous), so we first make sure ResolveIntent
	// happened and subsequently issue a bogus Put which is likely to make it
	// into Raft only after a rogue GCRequest (at least sporadically), which
	// would trigger a Fatal from the command filter.
	testutils.SucceedsSoon(t, func() error {
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
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			tc.Start(t, stopper)

			key := roachpb.Key("a")
			txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
			if aborted, err := tc.repl.abortCache.Get(context.Background(), tc.engine, *txn.ID, &entry); err != nil {
				t.Fatal(err)
			} else if aborted {
				t.Fatalf("commit=%t: abort cache still populated: %v", commit, entry)
			}
		}()
	}
}

// TestReplicaTransactionRequires1PC verifies that a transaction which
// sets Requires1PC on EndTransaction request will never leave intents
// in the event that it experiences an error or the timestamp is
// advanced.
func TestReplicaTransactionRequires1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsc := TestStoreConfig(nil)
	var injectErrorOnKey atomic.Value
	injectErrorOnKey.Store(roachpb.Key(""))

	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.Put &&
				injectErrorOnKey.Load().(roachpb.Key).Equal(filterArgs.Req.Header().Key) {
				return roachpb.NewErrorf("injected error")
			}
			return nil
		}
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	testCases := []struct {
		setupFn     func(roachpb.Key)
		expErrorPat string
	}{
		// Case 1: verify error if we augment the timestamp cache in order
		// to cause the response timestamp to move forward.
		{
			setupFn: func(key roachpb.Key) {
				gArgs := getArgs(key)
				if _, pErr := tc.SendWrapped(&gArgs); pErr != nil {
					t.Fatal(pErr)
				}
			},
			expErrorPat: "could not commit in one phase as requested",
		},
		// Case 2: inject an error on the put.
		{
			setupFn: func(key roachpb.Key) {
				injectErrorOnKey.Store(key)
			},
			expErrorPat: "injected error",
		},
	}

	for i, test := range testCases {
		t.Run("", func(t *testing.T) {
			key := roachpb.Key(fmt.Sprintf("%d", i))

			// Create the 1PC batch.
			var ba roachpb.BatchRequest
			txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
			bt, _ := beginTxnArgs(key, txn)
			put := putArgs(key, []byte("value"))
			et, etH := endTxnArgs(txn, true)
			et.Require1PC = true
			ba.Header = etH
			ba.Add(&bt, &put, &et)

			// Run the setup method.
			test.setupFn(key)

			// Send the batch command.
			_, pErr := tc.Sender().Send(context.Background(), ba)
			if !testutils.IsPError(pErr, test.expErrorPat) {
				t.Errorf("expected error=%q running required 1PC txn; got %s", test.expErrorPat, pErr)
			}

			// Do a consistent scan to verify no intents were created.
			sArgs := scanArgs(key, key.Next())
			_, pErr = tc.SendWrapped(&sArgs)
			if pErr != nil {
				t.Fatalf("error scanning to verify no intent present: %s", pErr)
			}
		})
	}
}

// TestReplicaEndTransactionWithRequire1PC verifies an error if an EndTransaction
// request is received with the Requires1PC flag set to true.
func TestReplicaEndTransactionWithRequire1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	bt, btH := beginTxnArgs(key, txn)
	put := putArgs(key, []byte("value"))
	var ba roachpb.BatchRequest
	ba.Header = btH
	ba.Add(&bt, &put)
	if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
		t.Fatalf("unexpected error beginning txn: %s", pErr)
	}

	et, etH := endTxnArgs(txn, true)
	et.Require1PC = true
	ba = roachpb.BatchRequest{}
	ba.Header = etH
	ba.Add(&et)
	_, pErr := tc.Sender().Send(context.Background(), ba)
	if !testutils.IsPError(pErr, "could not commit in one phase as requested") {
		t.Fatalf("expected requires 1PC error; fgot %v", pErr)
	}
}

func TestReplicaResolveIntentNoWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var seen int32
	key := roachpb.Key("zresolveme")
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(key) {
				atomic.StoreInt32(&seen, 1)
			}
			return nil
		}

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)
	splitKey := roachpb.RKey("aa")
	setupResolutionTest(t, tc, roachpb.Key("a") /* irrelevant */, splitKey, true /* commit */)
	txn := newTransaction("name", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	txn.Status = roachpb.COMMITTED
	if pErr := tc.store.intentResolver.resolveIntents(context.Background(),
		[]roachpb.Intent{{
			Span:   roachpb.Span{Key: key},
			Txn:    txn.TxnMeta,
			Status: txn.Status,
		}}, false /* !wait */, false /* !poison; irrelevant */); pErr != nil {
		t.Fatal(pErr)
	}
	testutils.SucceedsSoon(t, func() error {
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
func TestAbortCachePoisonOnResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := roachpb.Key("a")

	// Isolation of the pushee and whether we're going to abort it.
	// Run the actual meat of the test, which pushes the pushee and
	// checks whether we get the correct behaviour as it touches the
	// Range again.
	run := func(abort bool, iso enginepb.IsolationType) {
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())
		tc.Start(t, stopper)

		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pushee := newTransaction("test", key, 1, iso, tc.Clock())
		pusher.Priority = roachpb.MaxTxnPriority
		pushee.Priority = roachpb.MinTxnPriority // pusher will win

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
		var assert func(*roachpb.Error) error
		if abort {
			// Write/Write conflict will abort pushee.
			if _, pErr := inc(pusher, key); pErr != nil {
				t.Fatal(pErr)
			}
			assert = func(pErr *roachpb.Error) error {
				if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
					return errors.Errorf("abort=%t, iso=%s: expected txn abort, got %s", abort, iso, pErr)
				}
				return nil
			}
		} else {
			// Verify we're not poisoned.
			assert = func(pErr *roachpb.Error) error {
				if pErr != nil {
					return errors.Errorf("abort=%t, iso=%s: unexpected: %s", abort, iso, pErr)
				}
				return nil
			}
		}

		{
			// Our assert should be true for any reads or writes.
			pErr := get(pushee, key)
			if err := assert(pErr); err != nil {
				t.Fatal(err)
			}
		}
		{
			_, pErr := inc(pushee, key)
			if err := assert(pErr); err != nil {
				t.Fatal(err)
			}
		}
		{
			// Still poisoned (on any key on the Range).
			pErr := get(pushee, key.Next())
			if err := assert(pErr); err != nil {
				t.Fatal(err)
			}
		}
		{
			_, pErr := inc(pushee, key.Next())
			if err := assert(pErr); err != nil {
				t.Fatal(err)
			}
		}
		{
			// Pretend we're coming back. Increasing the epoch on an abort should
			// still fail obviously, while on no abort will succeed.
			pushee.Epoch++
			_, pErr := inc(pushee, roachpb.Key("b"))
			if err := assert(pErr); err != nil {
				t.Fatal(err)
			}
		}
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	u := uuid.MakeV4()
	txn := roachpb.Transaction{}
	txn.ID = &u
	txn.Priority = 1
	txn.Sequence = 1
	txn.Timestamp = tc.Clock().Now().Add(1, 0)

	key := roachpb.Key("k")
	ts := txn.Timestamp.Next()
	priority := int32(10)
	entry := roachpb.AbortCacheEntry{
		Key:       key,
		Timestamp: ts,
		Priority:  priority,
	}
	if err := tc.repl.abortCache.Put(context.Background(), tc.engine, nil, *txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	rec := ReplicaEvalContext{tc.repl, nil}
	pErr := checkIfTxnAborted(context.Background(), rec, tc.engine, txn)
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	pusher := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	pushee := newTransaction("test", roachpb.Key("b"), 1, enginepb.SERIALIZABLE, tc.Clock())

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	for i, status := range []roachpb.TransactionStatus{roachpb.COMMITTED, roachpb.ABORTED} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	now := tc.Clock().Now()
	ts1 := now.Add(1, 0)
	ts2 := now.Add(2, 0)
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
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pushee.Epoch = 12345
		pusher.Priority = roachpb.MaxTxnPriority // Pusher will win
		pusher.Writing = true                    // expected when a txn is heartbeat

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		pushee.Timestamp = test.startTS
		pushee.LastHeartbeat = test.startTS
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
		expTxn.Priority = roachpb.MaxTxnPriority - 1
		expTxn.Epoch = pushee.Epoch // no change
		expTxn.Timestamp = test.expTS
		expTxn.Status = roachpb.ABORTED
		expTxn.LastHeartbeat = test.startTS
		expTxn.Writing = true

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	now := tc.Clock().Now()
	ts := now.Add(1, 0)
	ns := base.DefaultHeartbeatInterval.Nanoseconds()
	testCases := []struct {
		heartbeat  hlc.Timestamp // zero value indicates no heartbeat
		timeOffset int64         // nanoseconds
		pushType   roachpb.PushTxnType
		expSuccess bool
	}{
		// Avoid using 0 as timeOffset to avoid having outcomes depend on random
		// logical ticks.
		{hlc.Timestamp{}, 1, roachpb.PUSH_TIMESTAMP, false},
		{hlc.Timestamp{}, 1, roachpb.PUSH_ABORT, false},
		{hlc.Timestamp{}, 1, roachpb.PUSH_TOUCH, false},
		{hlc.Timestamp{}, ns, roachpb.PUSH_TIMESTAMP, false},
		{hlc.Timestamp{}, ns, roachpb.PUSH_ABORT, false},
		{hlc.Timestamp{}, ns, roachpb.PUSH_TOUCH, false},
		{hlc.Timestamp{}, ns*2 - 1, roachpb.PUSH_TIMESTAMP, false},
		{hlc.Timestamp{}, ns*2 - 1, roachpb.PUSH_ABORT, false},
		{hlc.Timestamp{}, ns*2 - 1, roachpb.PUSH_TOUCH, false},
		{hlc.Timestamp{}, ns * 2, roachpb.PUSH_TIMESTAMP, false},
		{hlc.Timestamp{}, ns * 2, roachpb.PUSH_ABORT, false},
		{hlc.Timestamp{}, ns * 2, roachpb.PUSH_TOUCH, false},
		{ts, ns*2 + 1, roachpb.PUSH_TIMESTAMP, false},
		{ts, ns*2 + 1, roachpb.PUSH_ABORT, false},
		{ts, ns*2 + 1, roachpb.PUSH_TOUCH, false},
		{ts, ns*2 + 2, roachpb.PUSH_TIMESTAMP, true},
		{ts, ns*2 + 2, roachpb.PUSH_ABORT, true},
		{ts, ns*2 + 2, roachpb.PUSH_TOUCH, true},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pushee := newTransaction(fmt.Sprintf("test-%d", i), key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pusher := newTransaction("pusher", key, 1, enginepb.SERIALIZABLE, tc.Clock())

		// First, establish "start" of existing pushee's txn via BeginTransaction.
		if test.heartbeat != (hlc.Timestamp{}) {
			pushee.LastHeartbeat = test.heartbeat
		}
		_, btH := beginTxnArgs(key, pushee)
		btH.Timestamp = tc.repl.store.Clock().Now()
		put := putArgs(key, key)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
			t.Fatalf("%d: %s", i, pErr)
		}

		// Now, attempt to push the transaction with Now set to our current time.
		args := pushTxnArgs(pusher, pushee, test.pushType)
		args.Now = now.Add(test.timeOffset, 0)
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
		} else {
			if txn := reply.(*roachpb.PushTxnResponse).PusheeTxn; txn.Status != roachpb.ABORTED {
				t.Errorf("%d: expected aborted transaction, got %s", i, txn)
			}
		}
	}
}

// TestResolveIntentPushTxnReplyTxn makes sure that no Txn is returned from PushTxn and that
// it and ResolveIntent{,Range} can not be carried out in a transaction.
func TestResolveIntentPushTxnReplyTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	b := tc.engine.NewBatch()
	defer b.Close()

	txn := newTransaction("test", roachpb.Key("test"), 1, enginepb.SERIALIZABLE, tc.Clock())
	txnPushee := txn.Clone()
	pa := pushTxnArgs(txn, &txnPushee, roachpb.PUSH_ABORT)
	var ms enginepb.MVCCStats
	var ra roachpb.ResolveIntentRequest
	var rra roachpb.ResolveIntentRangeRequest

	ctx := context.Background()
	// Should not be able to push or resolve in a transaction.
	if _, err := evalPushTxn(ctx, b, CommandArgs{Stats: &ms, Header: roachpb.Header{Txn: txn}, Args: &pa}, &roachpb.PushTxnResponse{}); !testutils.IsError(err, errTransactionUnsupported.Error()) {
		t.Fatalf("transactional PushTxn returned unexpected error: %v", err)
	}
	if _, err := evalResolveIntent(ctx, b, CommandArgs{Stats: &ms, Header: roachpb.Header{Txn: txn}, Args: &ra}, &roachpb.ResolveIntentResponse{}); !testutils.IsError(err, errTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntent returned unexpected error: %v", err)
	}
	if _, err := evalResolveIntentRange(ctx, b, CommandArgs{Stats: &ms, Header: roachpb.Header{Txn: txn}, Args: &rra}, &roachpb.ResolveIntentRangeResponse{}); !testutils.IsError(err, errTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntentRange returned unexpected error: %v", err)
	}

	// Should not get a transaction back from PushTxn. It used to erroneously
	// return args.PusherTxn.
	var reply roachpb.PushTxnResponse
	if _, err := evalPushTxn(ctx, b, CommandArgs{Stats: &ms, Args: &pa}, &reply); err != nil {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	now := tc.Clock().Now()
	ts1 := now.Add(1, 0)
	ts2 := now.Add(2, 0)
	testCases := []struct {
		pusherPriority, pusheePriority int32
		pusherTS, pusheeTS             hlc.Timestamp
		pushType                       roachpb.PushTxnType
		expSuccess                     bool
	}{
		// Pusher with higher priority succeeds.
		{roachpb.MaxTxnPriority, roachpb.MinTxnPriority, ts1, ts1, roachpb.PUSH_TIMESTAMP, true},
		{roachpb.MaxTxnPriority, roachpb.MinTxnPriority, ts1, ts1, roachpb.PUSH_ABORT, true},
		// Pusher with lower priority fails.
		{roachpb.MinTxnPriority, roachpb.MaxTxnPriority, ts1, ts1, roachpb.PUSH_ABORT, false},
		{roachpb.MinTxnPriority, roachpb.MaxTxnPriority, ts1, ts1, roachpb.PUSH_TIMESTAMP, false},
		// Pusher with lower priority fails, even with older txn timestamp.
		{roachpb.MinTxnPriority, roachpb.MaxTxnPriority, ts1, ts2, roachpb.PUSH_ABORT, false},
		// Pusher has lower priority, but older txn timestamp allows success if
		// !abort since there's nothing to do.
		{roachpb.MinTxnPriority, roachpb.MaxTxnPriority, ts1, ts2, roachpb.PUSH_TIMESTAMP, true},
		// When touching, priority never wins.
		{roachpb.MaxTxnPriority, roachpb.MinTxnPriority, ts1, ts1, roachpb.PUSH_TOUCH, false},
		{roachpb.MinTxnPriority, roachpb.MaxTxnPriority, ts1, ts1, roachpb.PUSH_TOUCH, false},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	pusher := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	pushee := newTransaction("test", roachpb.Key("b"), 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher.Priority = roachpb.MaxTxnPriority
	pushee.Priority = roachpb.MinTxnPriority // pusher will win
	now := tc.Clock().Now()
	pusher.Timestamp = now.Add(50, 25)
	pushee.Timestamp = now.Add(5, 1)

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
	if reply.PusheeTxn.Timestamp != expTS {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	pusher := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	pushee := newTransaction("test", roachpb.Key("b"), 1, enginepb.SERIALIZABLE, tc.Clock())
	now := tc.Clock().Now()
	pusher.Timestamp = now.Add(50, 0)
	pushee.Timestamp = now.Add(50, 1)

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
	if reply.PusheeTxn.Timestamp != pushee.Timestamp {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pushee.Priority = roachpb.MinTxnPriority
	pusher.Priority = roachpb.MaxTxnPriority // pusher will win

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
	pusher.Timestamp = tc.repl.store.Clock().Now()
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
	if txn := pErr.GetTxn(); txn.Timestamp != pusher.Timestamp.Next() {
		t.Errorf("expected retry error txn timestamp %s; got %s", pusher.Timestamp, txn.Timestamp)
	}
}

// TestReplicaResolveIntentRange verifies resolving a range of intents.
func TestReplicaResolveIntentRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")}
	txn := newTransaction("test", keys[0], 1, enginepb.SERIALIZABLE, tc.Clock())

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

// TestRangeStatsComputation verifies that commands executed against a
// range update the range stat counters. The stat values are
// empirically derived; we're really just testing that they increment
// in the right ways, not the exact amounts. If the encodings change,
// will need to update this test.
func TestRangeStatsComputation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{
		bootstrapMode: bootstrapRangeOnly,
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	baseStats := initialStats()
	// The initial stats contain no lease, but there will be an initial
	// nontrivial lease requested with the first write below.
	baseStats.Add(enginepb.MVCCStats{
		SysBytes: 14,
	})

	// Our clock might not be set to zero.
	baseStats.LastUpdateNanos = tc.manualClock.UnixNano()

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

	if err := verifyRangeStats(tc.engine, tc.repl.RangeID, expMS); err != nil {
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
	txn := newTransaction("test", pArgs.Key, 1, enginepb.SERIALIZABLE, tc.Clock())
	txn.Priority = 123 // So we don't have random values messing with the byte counts on encoding
	txn.ID = &uuid

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
	if err := verifyRangeStats(tc.engine, tc.repl.RangeID, expMS); err != nil {
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
	if err := verifyRangeStats(tc.engine, tc.repl.RangeID, expMS); err != nil {
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
	if err := verifyRangeStats(tc.engine, tc.repl.RangeID, expMS); err != nil {
		t.Fatal(err)
	}
}

// TestMerge verifies that the Merge command is behaving as expected. Time
// series data is used, as it is the only data type currently fully supported by
// the merge command.
func TestMerge(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	tc.repl.store.SetRaftLogQueueActive(false)

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, pErr := tc.SendWrapped(&args); pErr != nil {
			t.Fatal(pErr)
		}
		idx, err := tc.repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	rangeID := tc.repl.RangeID

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &truncateArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// FirstIndex has changed.
	firstIndex, err := tc.repl.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != indexes[5] {
		t.Errorf("expected firstIndex == %d, got %d", indexes[5], firstIndex)
	}

	// We can still get what remains of the log.
	tc.repl.mu.Lock()
	entries, err := tc.repl.Entries(indexes[5], indexes[9], math.MaxUint64)
	tc.repl.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != int(indexes[9]-indexes[5]) {
		t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
	}

	// But any range that includes the truncated entries returns an error.
	tc.repl.mu.Lock()
	_, err = tc.repl.Entries(indexes[4], indexes[9], math.MaxUint64)
	tc.repl.mu.Unlock()
	if err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// The term of the last truncated entry is still available.
	tc.repl.mu.Lock()
	term, err := tc.repl.Term(indexes[4])
	tc.repl.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 {
		t.Errorf("invalid term 0 for truncated entry")
	}

	// The terms of older entries are gone.
	tc.repl.mu.Lock()
	_, err = tc.repl.Term(indexes[3])
	tc.repl.mu.Unlock()
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

	tc.repl.mu.Lock()
	// The term of the last truncated entry is still available.
	term, err = tc.repl.Term(indexes[4])
	tc.repl.mu.Unlock()
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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

		tc.repl.mu.Lock()
		newAppliedIndex := tc.repl.mu.state.RaftAppliedIndex
		tc.repl.mu.Unlock()
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

	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Header().Key.Equal(roachpb.Key("boom")) {
				return roachpb.NewError(NewReplicaCorruptionError(errors.New("boom")))
			}
			return nil
		}

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

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
	pErr, err = r.stateLoader.loadReplicaDestroyedError(context.Background(), r.store.Engine())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	if err := tc.repl.ChangeReplicas(
		context.Background(),
		roachpb.ADD_REPLICA,
		roachpb.ReplicationTarget{
			NodeID:  tc.store.Ident.NodeID,
			StoreID: 9999,
		},
		tc.repl.Desc(),
	); err == nil || !strings.Contains(err.Error(), "node already has a replica") {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
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
		Timestamp:       tc.Clock().Now(),
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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
		txn := newTransaction("test", roachpb.Key{}, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	txn := newTransaction("test", roachpb.Key{}, 1, enginepb.SERIALIZABLE, tc.Clock())
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	expected := []roachpb.RangeDescriptor{*tc.repl.Desc()}
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Mock propose to return an roachpb.RaftGroupDeletedError.
	submitProposalFn := func(*ProposalData) error {
		return &roachpb.RaftGroupDeletedError{}
	}

	repl := tc.repl

	repl.mu.Lock()
	repl.mu.submitProposalFn = submitProposalFn
	repl.mu.Unlock()

	gArgs := getArgs(roachpb.Key("a"))
	// Force the read command request a new lease.
	tc.manualClock.Set(leaseExpiry(repl))
	_, pErr := client.SendWrappedWith(context.Background(), tc.store, roachpb.Header{
		Timestamp: tc.Clock().Now(),
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	scStartSddr, err := keys.Addr(keys.SystemConfigSpan.Key)
	if err != nil {
		t.Fatal(err)
	}
	repl := tc.store.LookupReplica(scStartSddr, nil)
	if repl == nil {
		t.Fatalf("no replica contains the SystemConfig span")
	}

	// Create a transaction and write an intent to the system
	// config span.
	key := keys.SystemConfigSpan.Key
	_, btH := beginTxnArgs(key, newTransaction("test", key, 1, enginepb.SERIALIZABLE, repl.store.Clock()))
	btH.Txn.Priority = roachpb.MinTxnPriority // low so it can be pushed
	put := putArgs(key, []byte("foo"))
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction so that the async intent resolution caused
	// by loading the system config span doesn't waste any time in
	// clearing the intent.
	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, repl.store.Clock())
	pusher.Priority = roachpb.MaxTxnPriority // will push successfully
	pushArgs := pushTxnArgs(pusher, btH.Txn, roachpb.PUSH_ABORT)
	if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify that the intent trips up loading the SystemConfig data.
	if _, err := repl.loadSystemConfig(context.Background()); err != errSystemConfigIntent {
		t.Fatal(err)
	}

	// In the loop, wait until the intent is aborted. Then write a "real" value
	// there and verify that we can now load the data as expected.
	v := roachpb.MakeValueFromString("foo")
	testutils.SucceedsSoon(t, func() error {
		if err := engine.MVCCPut(context.Background(), repl.store.Engine(), &enginepb.MVCCStats{},
			keys.SystemConfigSpan.Key, repl.store.Clock().Now(), v, nil); err != nil {
			return err
		}

		cfg, err := repl.loadSystemConfig(context.Background())
		if err != nil {
			return err
		}

		if len(cfg.Values) != 1 || !bytes.Equal(cfg.Values[0].Key, keys.SystemConfigSpan.Key) {
			return errors.Errorf("expected only key %s in SystemConfigSpan map: %+v", keys.SystemConfigSpan.Key, cfg)
		}
		return nil
	})
}

func TestReplicaDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	repl, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	// First try and fail with a stale descriptor.
	origDesc := repl.Desc()
	newDesc := protoutil.Clone(origDesc).(*roachpb.RangeDescriptor)
	for i := range newDesc.Replicas {
		if newDesc.Replicas[i].StoreID == tc.store.StoreID() {
			newDesc.Replicas[i].ReplicaID++
			newDesc.NextReplicaID++
			break
		}
	}

	if err := repl.setDesc(newDesc); err != nil {
		t.Fatal(err)
	}
	expectedErr := "replica descriptor's ID has changed"
	if err := tc.store.removeReplicaImpl(context.Background(), tc.repl, *origDesc, true); !testutils.IsError(err, expectedErr) {
		t.Fatalf("expected error %q but got %v", expectedErr, err)
	}

	// Now try a fresh descriptor and succeed.
	if err := tc.store.removeReplicaImpl(context.Background(), tc.repl, *repl.Desc(), true); err != nil {
		t.Fatal(err)
	}
}

func TestEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	tc.repl.store.SetRaftLogQueueActive(false)

	repl := tc.repl
	rangeID := repl.RangeID
	var indexes []uint64

	populateLogs := func(from, to int) []uint64 {
		var newIndexes []uint64
		for i := from; i < to; i++ {
			args := incrementArgs([]byte("a"), int64(i))
			if _, pErr := tc.SendWrapped(&args); pErr != nil {
				t.Fatal(pErr)
			}
			idx, err := repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			newIndexes = append(newIndexes, idx)
		}
		return newIndexes
	}

	truncateLogs := func(index int) {
		truncateArgs := truncateLogArgs(indexes[index], rangeID)
		if _, err := client.SendWrappedWith(
			context.Background(),
			tc.Sender(),
			roachpb.Header{RangeID: 1},
			&truncateArgs,
		); err != nil {
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
			repl.store.raftEntryCache.delEntries(rangeID, indexes[0], indexes[9]+1)
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
		cacheEntries, _, _ := repl.store.raftEntryCache.getEntries(nil, rangeID, tc.lo, tc.hi, tc.maxBytes)
		if len(cacheEntries) != tc.expCacheCount {
			t.Errorf("%d: expected cache count %d, got %d", i, tc.expCacheCount, len(cacheEntries))
		}
		repl.mu.Lock()
		ents, err := repl.Entries(tc.lo, tc.hi, tc.maxBytes)
		repl.mu.Unlock()
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
	repl.mu.Lock()
	if _, err := repl.Entries(indexes[9], indexes[5], 0); err == nil {
		t.Errorf("23: error expected, got none")
	}
	repl.mu.Unlock()

	// Case 24: add a gap to the indexes.
	if err := engine.MVCCDelete(context.Background(), tc.store.Engine(), nil, keys.RaftLogKey(rangeID, indexes[6]), hlc.Timestamp{}, nil); err != nil {
		t.Fatal(err)
	}
	repl.store.raftEntryCache.delEntries(rangeID, indexes[6], indexes[6]+1)

	repl.mu.Lock()
	defer repl.mu.Unlock()
	if _, err := repl.Entries(indexes[5], indexes[9], 0); err == nil {
		t.Errorf("24: error expected, got none")
	}

	// Case 25: don't hit the gap due to maxBytes.
	ents, err := repl.Entries(indexes[5], indexes[9], 1)
	if err != nil {
		t.Errorf("25: expected no error, got %s", err)
	}
	if len(ents) != 1 {
		t.Errorf("25: expected 1 entry, got %d", len(ents))
	}

	// Case 26: don't hit the gap due to truncation.
	if _, err := repl.Entries(indexes[4], indexes[9], 0); err != raft.ErrCompacted {
		t.Errorf("26: expected error %s , got %s", raft.ErrCompacted, err)
	}
}

func TestTerm(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	tc.repl.store.SetRaftLogQueueActive(false)

	repl := tc.repl
	rangeID := repl.RangeID

	// Populate the log with 10 entries. Save the LastIndex after each write.
	var indexes []uint64
	for i := 0; i < 10; i++ {
		args := incrementArgs([]byte("a"), int64(i))

		if _, pErr := tc.SendWrapped(&args); pErr != nil {
			t.Fatal(pErr)
		}
		idx, err := tc.repl.GetLastIndex()
		if err != nil {
			t.Fatal(err)
		}
		indexes = append(indexes, idx)
	}

	// Discard the first half of the log.
	truncateArgs := truncateLogArgs(indexes[5], rangeID)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &truncateArgs); pErr != nil {
		t.Fatal(pErr)
	}

	repl.mu.Lock()
	defer repl.mu.Unlock()

	firstIndex, err := repl.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != indexes[5] {
		t.Fatalf("expected firstIndex %d to be %d", firstIndex, indexes[4])
	}

	// Truncated logs should return an ErrCompacted error.
	if _, err := tc.repl.Term(indexes[1]); err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}
	if _, err := tc.repl.Term(indexes[3]); err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// FirstIndex-1 should return the term of firstIndex.
	firstIndexTerm, err := tc.repl.Term(firstIndex)
	if err != nil {
		t.Errorf("expect no error, got %s", err)
	}

	term, err := tc.repl.Term(indexes[4])
	if err != nil {
		t.Errorf("expect no error, got %s", err)
	}
	if term != firstIndexTerm {
		t.Errorf("expected firstIndex-1's term:%d to equal that of firstIndex:%d", term, firstIndexTerm)
	}

	lastIndex, err := repl.LastIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Last index should return correctly.
	if _, err := tc.repl.Term(lastIndex); err != nil {
		t.Errorf("expected no error, got %s", err)
	}

	// Terms for after the last index should return ErrUnavailable.
	if _, err := tc.repl.Term(lastIndex + 1); err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
	if _, err := tc.repl.Term(indexes[9] + 1000); err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
}

func TestGCIncorrectRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Split range into two ranges.
	splitKey := roachpb.RKey("c")
	repl1 := tc.repl
	repl2 := splitTestRange(tc.store, splitKey, splitKey, t)

	// Write a key to range 2 at two different timestamps so we can
	// GC the earlier timestamp without needing to delete it.
	key := splitKey.PrefixEnd().AsRawKey()
	val := []byte("value")
	putReq := putArgs(key, val)
	now := tc.Clock().Now()
	ts1 := now.Add(1, 0)
	ts2 := now.Add(2, 0)
	ts1Header := roachpb.Header{RangeID: repl2.RangeID, Timestamp: ts1}
	ts2Header := roachpb.Header{RangeID: repl2.RangeID, Timestamp: ts2}
	if _, pErr := client.SendWrappedWith(context.Background(), repl2, ts1Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}
	if _, pErr := client.SendWrappedWith(context.Background(), repl2, ts2Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}

	// Send GC request to range 1 for the key on range 2, which
	// should succeed even though it doesn't contain the key, because
	// the request for the incorrect key will be silently dropped.
	gKey := gcKey(key, ts1)
	gcReq := gcArgs(repl1.Desc().StartKey, repl1.Desc().EndKey, gKey)
	if _, pErr := client.SendWrappedWith(
		context.Background(),
		repl1,
		roachpb.Header{RangeID: 1, Timestamp: tc.Clock().Now()},
		&gcReq,
	); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to incorrect range: %s", pErr)
	}

	// Make sure the key still exists on range 2.
	getReq := getArgs(key)
	if res, pErr := client.SendWrappedWith(context.Background(), repl2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal == nil {
		t.Errorf("expected value %s to exists after GC to incorrect range but before GC to correct range, found %v", val, resVal)
	}

	// Send GC request to range 2 for the same key.
	gcReq = gcArgs(repl2.Desc().StartKey, repl2.Desc().EndKey, gKey)
	if _, pErr := client.SendWrappedWith(
		context.Background(),
		repl2,
		roachpb.Header{RangeID: repl2.RangeID, Timestamp: tc.Clock().Now()},
		&gcReq,
	); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to correct range: %s", pErr)
	}

	// Make sure the key no longer exists on range 2.
	if res, pErr := client.SendWrappedWith(context.Background(), repl2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal != nil {
		t.Errorf("expected value at key %s to no longer exist after GC to correct range, found value %v", key, resVal)
	}
}

// TestReplicaCancelRaft checks that it is possible to safely abandon Raft
// commands via a cancelable context.Context.
func TestReplicaCancelRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, cancelEarly := range []bool{true, false} {
		func() {
			// Pick a key unlikely to be used by background processes.
			key := []byte("acdfg")
			ctx, cancel := context.WithCancel(context.Background())
			cfg := TestStoreConfig(nil)
			if !cancelEarly {
				cfg.TestingKnobs.TestingEvalFilter =
					func(filterArgs storagebase.FilterArgs) *roachpb.Error {
						if filterArgs.Req.Header().Key.Equal(key) {
							cancel()
						}
						return nil
					}
			}
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			tc.StartWithStoreConfig(t, stopper, cfg)
			if cancelEarly {
				cancel()
			}
			var ba roachpb.BatchRequest
			ba.RangeID = 1
			ba.Add(&roachpb.GetRequest{
				Span: roachpb.Span{Key: key},
			})
			if err := ba.SetActiveTimestamp(tc.Clock().Now); err != nil {
				t.Fatal(err)
			}
			_, pErr := tc.repl.executeWriteBatch(ctx, ba)
			if cancelEarly {
				if !testutils.IsPError(pErr, context.Canceled.Error()) {
					t.Fatalf("expected canceled error; got %v", pErr)
				}
			} else {
				if pErr == nil {
					// We cancelled the context while the command was already
					// being processed, so the client had to wait for successful
					// execution.
					return
				}
				detail := pErr.GetDetail()
				if _, ok := detail.(*roachpb.AmbiguousResultError); !ok {
					t.Fatalf("expected AmbiguousResultError error; got %s (%T)", detail, detail)
				}
			}
		}()
	}
}

// TestReplicaTryAbandon checks that cancelling a request that has been
// proposed to Raft but before it has been executed correctly cleans up the
// command queue. See #11986.
func TestReplicaTryAbandon(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc := testContext{}
	tc.Start(t, stopper)

	ctx, cancel := context.WithCancel(context.Background())
	proposalCh := make(chan struct{})
	proposalErrCh := make(chan error)

	// Cancel the request before it is proposed to Raft.
	tc.repl.mu.Lock()
	tc.repl.mu.submitProposalFn = func(result *ProposalData) error {
		cancel()
		go func() {
			<-proposalCh
			tc.repl.mu.Lock()
			proposalErrCh <- defaultSubmitProposalLocked(tc.repl, result)
			tc.repl.mu.Unlock()
		}()
		return nil
	}
	tc.repl.mu.Unlock()

	var ba roachpb.BatchRequest
	ba.RangeID = 1
	ba.Add(&roachpb.PutRequest{
		Span: roachpb.Span{Key: []byte("acdfg")},
	})
	if err := ba.SetActiveTimestamp(tc.Clock().Now); err != nil {
		t.Fatal(err)
	}
	_, pErr := tc.repl.executeWriteBatch(ctx, ba)
	if pErr == nil {
		t.Fatalf("expected failure, but found success")
	}
	detail := pErr.GetDetail()
	if _, ok := detail.(*roachpb.AmbiguousResultError); !ok {
		t.Fatalf("expected AmbiguousResultError error; got %s (%T)", detail, detail)
	}

	// Despite the cancellation the request should still be occupying the
	// proposals map and command queue.
	func() {
		tc.repl.mu.Lock()
		defer tc.repl.mu.Unlock()
		if len(tc.repl.mu.proposals) == 0 {
			t.Fatal("expected non-empty proposals map")
		}
	}()

	func() {
		tc.repl.cmdQMu.Lock()
		defer tc.repl.cmdQMu.Unlock()
		if s := tc.repl.cmdQMu.global.String(); s == "" {
			t.Fatal("expected non-empty command queue")
		}
	}()

	// Allow the proposal to go through.
	close(proposalCh)
	if err := <-proposalErrCh; err != nil {
		t.Fatal(err)
	}

	// Even though we cancelled the command it will still get executed and the
	// command queue cleaned up.
	testutils.SucceedsSoon(t, func() error {
		tc.repl.cmdQMu.Lock()
		defer tc.repl.cmdQMu.Unlock()
		if s := tc.repl.cmdQMu.global.String(); s != "" {
			return errors.Errorf("expected empty command queue, but found\n%s", s)
		}
		return nil
	})
}

// TestComputeChecksumVersioning checks that the ComputeChecksum post-commit
// trigger is called if and only if the checksum version is right.
func TestComputeChecksumVersioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	if pct, _ := evalComputeChecksum(context.TODO(), nil,
		CommandArgs{Args: &roachpb.ComputeChecksumRequest{
			ChecksumID: uuid.MakeV4(),
			Version:    replicaChecksumVersion,
		}}, &roachpb.ComputeChecksumResponse{},
	); pct.Replicated.ComputeChecksum == nil {
		t.Error("right checksum version: expected post-commit trigger")
	}

	if pct, _ := evalComputeChecksum(context.TODO(), nil,
		CommandArgs{Args: &roachpb.ComputeChecksumRequest{
			ChecksumID: uuid.MakeV4(),
			Version:    replicaChecksumVersion + 1,
		}}, &roachpb.ComputeChecksumResponse{},
	); pct.Replicated.ComputeChecksum != nil {
		t.Errorf("wrong checksum version: expected no post-commit trigger: %s", pct.Replicated.ComputeChecksum)
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
			{Key: []byte("abcdefg"), Timestamp: hlc.Timestamp{}, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, -1), Value: value},
			{Key: []byte("abcdefgh"), Timestamp: timestamp, Value: value},
			{Key: []byte("x"), Timestamp: timestamp, Value: value},
			{Key: []byte("y"), Timestamp: timestamp, Value: value},
			// Both 'zeroleft' and 'zeroright' share the version at (1,1), but
			// a zero timestamp (=meta) key pair exists on the leader or
			// follower, respectively.
			{Key: []byte("zeroleft"), Timestamp: hlc.Timestamp{}, Value: value},
			{Key: []byte("zeroleft"), Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1}, Value: value},
			{Key: []byte("zeroright"), Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1}, Value: value},
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
			{Key: []byte("abcdefg"), Timestamp: hlc.Timestamp{}, Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp.Add(0, 1), Value: value},
			{Key: []byte("abcdefg"), Timestamp: timestamp, Value: value},
			{Key: []byte("abcdefgh"), Timestamp: timestamp, Value: value},
			{Key: []byte("x"), Timestamp: timestamp, Value: []byte("bar")},
			{Key: []byte("z"), Timestamp: timestamp, Value: value},
			{Key: []byte("zeroleft"), Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1}, Value: value},
			{Key: []byte("zeroright"), Timestamp: hlc.Timestamp{}, Value: value},
			{Key: []byte("zeroright"), Timestamp: hlc.Timestamp{WallTime: 1, Logical: 1}, Value: value},
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
		{LeaseHolder: true, Key: []byte("zeroleft"), Timestamp: hlc.Timestamp{}, Value: value},
		{LeaseHolder: false, Key: []byte("zeroright"), Timestamp: hlc.Timestamp{}, Value: value},
	}

	diff := diffRange(leaderSnapshot, replicaSnapshot)

	for i, e := range eDiff {
		v := diff[i]
		if e.LeaseHolder != v.LeaseHolder || !bytes.Equal(e.Key, v.Key) || e.Timestamp != v.Timestamp || !bytes.Equal(e.Value, v.Value) {
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

	tsc := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	// With enough time in BlockingSnapshotDuration, we succeed on the
	// first try.
	tc.repl.mu.Lock()
	snap, err := tc.repl.Snapshot()
	tc.repl.mu.Unlock()

	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Data) != 0 {
		t.Fatal("snapshot is not empty")
	}
}

// TestReplicaIDChangePending verifies that on a replica ID change, pending
// commands are re-proposed on the new raft group.
func TestReplicaIDChangePending(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	cfg := TestStoreConfig(nil)
	// Disable ticks to avoid automatic reproposals after a timeout, which
	// would pass this test.
	cfg.RaftTickInterval = math.MaxInt32
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, cfg)
	repl := tc.repl

	// Stop the command from being proposed to the raft group and being removed.
	repl.mu.Lock()
	repl.mu.submitProposalFn = func(p *ProposalData) error { return nil }
	lease := repl.mu.state.Lease
	repl.mu.Unlock()

	// Add a command to the pending list.
	magicTS := tc.Clock().Now()
	ba := roachpb.BatchRequest{}
	ba.Timestamp = magicTS
	ba.Add(&roachpb.GetRequest{
		Span: roachpb.Span{
			Key: roachpb.Key("a"),
		},
	})
	_, _, err := repl.propose(context.Background(), lease, ba, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Set the raft command handler so we can tell if the command has been
	// re-proposed.
	commandProposed := make(chan struct{}, 1)
	repl.mu.Lock()
	repl.mu.submitProposalFn = func(p *ProposalData) error {
		if p.Request.Timestamp == magicTS {
			commandProposed <- struct{}{}
		}
		return nil
	}
	repl.mu.Unlock()

	// Set the ReplicaID on the replica.
	if err := repl.setReplicaID(2); err != nil {
		t.Fatal(err)
	}

	<-commandProposed
}

func TestSetReplicaID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tsc := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)

	repl := tc.repl

	testCases := []struct {
		replicaID            roachpb.ReplicaID
		minReplicaID         roachpb.ReplicaID
		newReplicaID         roachpb.ReplicaID
		expectedMinReplicaID roachpb.ReplicaID
		expectedErr          string
	}{
		{0, 0, 1, 2, ""},
		{0, 1, 1, 2, ""},
		{0, 2, 1, 2, "raft group deleted"},
		{1, 2, 1, 2, ""}, // not an error; replicaID == newReplicaID is checked first
		{2, 0, 1, 0, "replicaID cannot move backwards"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			repl.mu.Lock()
			repl.mu.replicaID = c.replicaID
			repl.mu.minReplicaID = c.minReplicaID
			repl.mu.Unlock()

			err := repl.setReplicaID(c.newReplicaID)
			repl.mu.Lock()
			if repl.mu.minReplicaID != c.expectedMinReplicaID {
				t.Errorf("expected minReplicaID=%d, but found %d", c.expectedMinReplicaID, repl.mu.minReplicaID)
			}
			repl.mu.Unlock()
			if !testutils.IsError(err, c.expectedErr) {
				t.Fatalf("expected %q, but found %v", c.expectedErr, err)
			}
		})
	}
}

func TestReplicaRetryRaftProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	type magicKey struct{}

	var c int32                // updated atomically
	var wrongLeaseIndex uint64 // populated below

	tc.repl.mu.Lock()
	tc.repl.mu.submitProposalFn = func(proposal *ProposalData) error {
		if v := proposal.ctx.Value(magicKey{}); v != nil {
			if curAttempt := atomic.AddInt32(&c, 1); curAttempt == 1 {
				proposal.command.MaxLeaseIndex = wrongLeaseIndex
			}
		}
		return defaultSubmitProposalLocked(tc.repl, proposal)
	}
	tc.repl.mu.Unlock()

	pArg := putArgs(roachpb.Key("a"), []byte("asd"))
	{
		var ba roachpb.BatchRequest
		ba.Add(&pArg)
		ba.Timestamp = tc.Clock().Now()
		if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	tc.repl.mu.Lock()
	ai := tc.repl.mu.state.LeaseAppliedIndex
	tc.repl.mu.Unlock()

	if ai < 1 {
		t.Fatal("committed a batch, but still at lease index zero")
	}

	wrongLeaseIndex = ai - 1 // used by submitProposalFn above

	log.Infof(ctx, "test begins")

	var ba roachpb.BatchRequest
	ba.RangeID = 1
	ba.Timestamp = tc.Clock().Now()
	const expInc = 123
	iArg := incrementArgs(roachpb.Key("b"), expInc)
	ba.Add(&iArg)
	{
		br, pErr, retry := tc.repl.tryExecuteWriteBatch(
			context.WithValue(ctx, magicKey{}, "foo"),
			ba,
		)
		if retry != proposalIllegalLeaseIndex {
			t.Fatalf("expected retry from illegal lease index, but got (%v, %v, %d)", br, pErr, retry)
		}
		if exp, act := int32(1), atomic.LoadInt32(&c); exp != act {
			t.Fatalf("expected %d proposals, got %d", exp, act)
		}
	}

	atomic.StoreInt32(&c, 0)
	{
		br, pErr := tc.repl.executeWriteBatch(
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	repl := tc.repl

	repDesc, err := repl.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	const num = 10

	var chs []chan proposalResult // protected by repl.mu

	func() {
		for i := 0; i < num; i++ {
			var ba roachpb.BatchRequest
			ba.Timestamp = tc.Clock().Now()
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{
				Key: roachpb.Key(fmt.Sprintf("k%d", i))}})
			lease, _ := repl.getLease()
			cmd, pErr := repl.requestToProposal(context.Background(), makeIDKey(), ba, nil, nil)
			if pErr != nil {
				t.Fatal(pErr)
			}
			repl.mu.Lock()
			repl.insertProposalLocked(cmd, repDesc, lease)
			// We actually propose the command only if we don't
			// cancel it to simulate the case in which Raft loses
			// the command and it isn't reproposed due to the
			// client abandoning it.
			if rand.Intn(2) == 0 {
				log.Infof(context.Background(), "abandoning command %d", i)
				delete(repl.mu.proposals, cmd.idKey)
			} else if err := repl.submitProposalLocked(cmd); err != nil {
				t.Error(err)
			} else {
				chs = append(chs, cmd.doneCh)
			}
			repl.mu.Unlock()
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

	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	const num = 10
	repDesc, err := tc.repl.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	type magicKey struct{}

	var seenCmds []int
	tc.repl.mu.Lock()
	tc.repl.mu.submitProposalFn = func(proposal *ProposalData) error {
		if v := proposal.ctx.Value(magicKey{}); v != nil {
			seenCmds = append(seenCmds, int(proposal.command.MaxLeaseIndex))
		}
		return defaultSubmitProposalLocked(tc.repl, proposal)
	}
	tc.repl.mu.Unlock()

	status, pErr := tc.repl.redirectOnOrAcquireLease(context.Background())
	if pErr != nil {
		t.Fatal(pErr)
	}

	expIndexes := make([]int, 0, num)
	chs := func() []chan proposalResult {
		chs := make([]chan proposalResult, 0, num)

		origIndexes := make([]int, 0, num)
		for i := 0; i < num; i++ {
			expIndexes = append(expIndexes, i+1)
			ctx := context.WithValue(context.Background(), magicKey{}, "foo")
			var ba roachpb.BatchRequest
			ba.Timestamp = tc.Clock().Now()
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{
				Key: roachpb.Key(fmt.Sprintf("k%d", i))}})
			cmd, pErr := tc.repl.requestToProposal(ctx, makeIDKey(), ba, nil, nil)
			if pErr != nil {
				t.Fatal(pErr)
			}

			tc.repl.raftMu.Lock()
			tc.repl.mu.Lock()
			tc.repl.insertProposalLocked(cmd, repDesc, status.lease)
			chs = append(chs, cmd.doneCh)
			tc.repl.mu.Unlock()
			tc.repl.raftMu.Unlock()
		}

		tc.repl.mu.Lock()
		for _, p := range tc.repl.mu.proposals {
			if v := p.ctx.Value(magicKey{}); v != nil {
				origIndexes = append(origIndexes, int(p.command.MaxLeaseIndex))
			}
		}
		tc.repl.mu.Unlock()

		sort.Ints(origIndexes)

		if !reflect.DeepEqual(expIndexes, origIndexes) {
			t.Fatalf("wanted required indexes %v, got %v", expIndexes, origIndexes)
		}

		tc.repl.raftMu.Lock()
		tc.repl.mu.Lock()
		tc.repl.refreshProposalsLocked(0, reasonTicks)
		tc.repl.mu.Unlock()
		tc.repl.raftMu.Unlock()
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

	tc.repl.mu.Lock()
	defer tc.repl.mu.Unlock()
	nonePending := len(tc.repl.mu.proposals) == 0
	c := int(tc.repl.mu.lastAssignedLeaseIndex) - int(tc.repl.mu.state.LeaseAppliedIndex)
	if nonePending && c > 0 {
		t.Errorf("no pending cmds, but have required index offset %d", c)
	}
	if !nonePending {
		t.Fatalf("still pending commands: %+v", tc.repl.mu.proposals)
	}
}

func TestReplicaRefreshPendingCommandsTicks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var tc testContext
	cfg := TestStoreConfig(nil)
	// Disable ticks which would interfere with the manual ticking in this test.
	cfg.RaftTickInterval = math.MaxInt32
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, cfg)

	// Grab Replica.raftMu in order to block normal raft replica processing. This
	// test is ticking the replica manually and doesn't want the store to be
	// doing so concurrently.
	r := tc.repl

	repDesc, err := tc.repl.GetReplicaDescriptor()
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
		ba.Timestamp = tc.Clock().Now()
		ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: roachpb.Key(id)}})
		lease, _ := r.getLease()
		cmd, pErr := r.requestToProposal(context.Background(), storagebase.CmdIDKey(id), ba, nil, nil)
		if pErr != nil {
			t.Fatal(pErr)
		}

		dropProposals.Lock()
		dropProposals.m[cmd] = struct{}{} // silently drop proposals
		dropProposals.Unlock()

		r.mu.Lock()
		r.insertProposalLocked(cmd, repDesc, lease)
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

// checkValue asserts that the value for a key is the expected one.
// The function will attempt to resolve the intent present on the key, if any.
func checkValue(ctx context.Context, tc *testContext, key []byte, expectedVal []byte) error {
	gArgs := getArgs(key)
	// Note: sending through the store, not directly through the replica, for
	// intent resolution to kick in. Use max user priority to ensure we push
	// any residual intent.
	resp, pErr := client.SendWrappedWith(ctx, tc.store.testSender(), roachpb.Header{
		UserPriority: roachpb.MaxUserPriority,
	}, &gArgs)
	if pErr != nil {
		return errors.Errorf("could not get data: %s", pErr)
	}
	v := resp.(*roachpb.GetResponse).Value
	if v == nil {
		return errors.Errorf("no value")
	}
	val, err := v.GetBytes()
	if err != nil {
		return err
	}
	if !bytes.Equal(val, expectedVal) {
		return errors.Errorf("expected %q got: %s", expectedVal, val)
	}
	return nil
}

// TestAmbiguousResultErrorOnRetry verifies that when a batch with
// EndTransaction(commit=true) is retried, it will return an
// AmbiguousResultError if the retry fails.
func TestAmbiguousResultErrorOnRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if testing.Short() {
		t.Skip("short flag")
	}

	cfg := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, cfg)

	var baPut roachpb.BatchRequest
	{
		baPut.RangeID = 1
		key := roachpb.Key("put1")
		put1 := putArgs(key, []byte("value"))
		baPut.Add(&put1)
	}
	var ba1PCTxn roachpb.BatchRequest
	{
		ba1PCTxn.RangeID = 1
		key := roachpb.Key("1pc")
		txn := newTransaction("1pc", key, -1, enginepb.SERIALIZABLE, tc.Clock())
		bt, _ := beginTxnArgs(key, txn)
		put := putArgs(key, []byte("value"))
		et, etH := endTxnArgs(txn, true)
		et.IntentSpans = []roachpb.Span{{Key: key}}
		ba1PCTxn.Header = etH
		ba1PCTxn.Add(&bt, &put, &et)
	}

	testCases := []struct {
		name    string
		ba      roachpb.BatchRequest
		checkFn func(*roachpb.Error) error
	}{
		{
			name: "non-txn-put",
			ba:   baPut,
			checkFn: func(pErr *roachpb.Error) error {
				detail := pErr.GetDetail()
				are, ok := detail.(*roachpb.AmbiguousResultError)
				if !ok {
					return errors.Wrapf(detail, "expected AmbiguousResultError, got error %T", detail)
				}
				detail = are.WrappedErr.GetDetail()
				if _, ok := detail.(*roachpb.WriteTooOldError); !ok {
					return errors.Wrapf(detail,
						"expected the AmbiguousResultError to be caused by a "+
							"WriteTooOldError, got %T", detail)
				}
				// Test that the original proposal succeeded by checking the effects of
				// the transaction.
				return checkValue(context.Background(), &tc, roachpb.Key("put1"), []byte("value"))
			},
		},
		{
			// This test checks two things:
			// - that we can do retries of 1pc batches
			// - that they fail ambiguously if the original command was applied
			name: "1PC-txn",
			ba:   ba1PCTxn,
			checkFn: func(pErr *roachpb.Error) error {
				// This unittest verifies that the timestamp cache isn't updated before
				// sending the retry. Otherwise, the retry would receive an
				// AmbiguousResultError, but it would be caused by a
				// TransactionReplayError instead of a TransactionRetryError. This is
				// because the EndTxn updates the timestamp cache and the subsequent
				// begin (part of the retried) checks it. Before the fix in #10639, this
				// retry of 1pc batches would always get TransactionReplayError (wrapped
				// in an AmbiguousResultError), which would have been too pessimistic -
				// unnecessary in case the original command was never actually applied.
				//
				// The one phase transaction will succeed because the original command
				// executes first. However, the response the client gets corresponds to
				// the retried one, and that one fails because of MVCC protections.
				detail := pErr.GetDetail()
				are, ok := detail.(*roachpb.AmbiguousResultError)
				if !ok {
					return errors.Wrapf(detail, "expected AmbiguousResultError, got error %T", detail)
				}
				detail = are.WrappedErr.GetDetail()
				if _, ok := detail.(*roachpb.TransactionRetryError); !ok {
					return errors.Wrapf(detail,
						"expected the AmbiguousResultError to be caused by a "+
							"TransactionRetryError, got %T", detail)
				}

				// Test that the original proposal succeeded by checking the effects of
				// the transaction.
				return checkValue(context.Background(), &tc, roachpb.Key("1pc"), []byte("value"))
			},
		},
	}

	originalProposalErrChan := make(chan error, 1)

	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			// Install a proposal function which starts a retry before doing the
			// proposal. The batch will ultimately be executed twice, and the result
			// of the second execution is returned to the client.
			tc.repl.mu.Lock()
			tc.repl.mu.submitProposalFn = func(p *ProposalData) error {
				go func() {
					// Manually refresh proposals.

					// Holding on to this mutex until after the
					// defaultSubmitProposalLocked below guarantees that the "original"
					// command will be proposed and applied before the retry.
					tc.repl.mu.Lock()
					// We are going to cheat here and increase the lease applied
					// index to force the sort of re-proposal we're after (we want the
					// command to be re-evaluated and ambiguously re-proposed).
					origLeaseAppliedIndex := tc.repl.mu.state.LeaseAppliedIndex
					tc.repl.mu.state.LeaseAppliedIndex += 10
					// We need to use reasonSnapshotApplied here because that's the reason
					// that causes retries instead of reproposals, and moreover makes the
					// errors of retries ambiguous. It also allows the original command to
					// still be applied, whereas other reasons assume that won't be the
					// case.
					tc.repl.refreshProposalsLocked(0, reasonSnapshotApplied)
					tc.repl.mu.state.LeaseAppliedIndex = origLeaseAppliedIndex
					// Unset the custom proposal function so re-proposal proceeds
					// without blocking.
					tc.repl.mu.submitProposalFn = nil
					// We've just told addWriteCmd to retry the request, so it will no
					// longer listen for the result of this original proposal. However, we
					// still want the original proposal to succeed (before the retry is
					// processed), so submit it before releasing the lock.
					originalProposalErrChan <- defaultSubmitProposalLocked(tc.repl, p)
					tc.repl.mu.Unlock()
				}()
				return nil // pretend we proposed though we haven't yet.
			}
			tc.repl.mu.Unlock()

			_, pErr := tc.Sender().Send(context.Background(), c.ba)
			if err := <-originalProposalErrChan; err != nil {
				t.Fatal(err)
			}
			if err := c.checkFn(pErr); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestCommandTimeThreshold verifies that commands outside the replica GC
// threshold fail.
func TestCommandTimeThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	now := tc.Clock().Now()
	ts1 := now.Add(1, 0)
	ts2 := now.Add(2, 0)
	ts3 := now.Add(3, 0)

	key := roachpb.Key("a")
	keycp := roachpb.Key("c")

	va := []byte("a")
	vb := []byte("b")

	// Verify a Get works.
	gArgs := getArgs(key)
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts1,
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
		Timestamp: ts1,
	}, &pArgs); err != nil {
		t.Fatalf("could not put data: %s", err)
	}

	// Do a GC.
	gcr := roachpb.GCRequest{
		Threshold: ts2,
	}
	if _, err := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &gcr); err != nil {
		t.Fatal(err)
	}

	// Do the same Get, which should now fail.
	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts1,
	}, &gArgs); !testutils.IsPError(pErr, `batch timestamp 0.\d+,\d+ must be after GC threshold 0.\d+,\d+`) {
		t.Fatalf("unexpected error: %v", pErr)
	}

	// Verify a later Get works.
	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify an early CPut fails.
	cpArgs := cPutArgs(keycp, vb, va)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts2,
	}, &cpArgs); !testutils.IsPError(pErr, `batch timestamp 0.\d+,\d+ must be after GC threshold 0.\d+,\d+`) {
		t.Fatalf("unexpected error: %v", pErr)
	}
	// Verify a later CPut works.
	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &cpArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

func TestDeprecatedRequests(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	if reply, err := tc.SendWrapped(&roachpb.DeprecatedVerifyChecksumRequest{}); err != nil {
		t.Fatal(err)
	} else if _, ok := reply.(*roachpb.DeprecatedVerifyChecksumResponse); !ok {
		t.Fatalf("expected %T but got %T", &roachpb.DeprecatedVerifyChecksumResponse{}, reply)
	}
}

func TestReplicaTimestampCacheBumpNotLost(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	ctx := tc.store.AnnotateCtx(context.TODO())
	key := keys.LocalMax

	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	origTxn := txn.Clone()

	minNewTS := func() hlc.Timestamp {
		var ba roachpb.BatchRequest
		scan := scanArgs(key, tc.repl.Desc().EndKey.AsRawKey())
		ba.Add(&scan)

		resp, pErr := tc.Sender().Send(ctx, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if !txn.Timestamp.Less(resp.Timestamp) {
			t.Fatalf("expected txn ts %s < scan TS %s", txn.Timestamp, resp.Timestamp)
		}
		return resp.Timestamp
	}()

	var ba roachpb.BatchRequest
	ba.Txn = txn
	txnPut := putArgs(key, []byte("timestamp should be bumped"))
	ba.Add(&txnPut)

	resp, pErr := tc.Sender().Send(ctx, ba)
	if pErr != nil {
		t.Fatal(pErr)
	}

	if !reflect.DeepEqual(&origTxn, txn) {
		t.Fatalf(
			"original transaction proto was mutated: %s",
			pretty.Diff(&origTxn, txn),
		)
	}
	if resp.Txn == nil {
		t.Fatal("no transaction in response")
	} else if resp.Txn.Timestamp.Less(minNewTS) {
		t.Fatalf(
			"expected txn ts bumped at least to %s, but got %s",
			minNewTS, txn.Timestamp,
		)
	}
}

func TestReplicaEvaluationNotTxnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	ctx := tc.repl.AnnotateCtx(context.TODO())
	key := keys.LocalMax

	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	origTxn := txn.Clone()

	var ba roachpb.BatchRequest
	ba.Txn = txn
	ba.Timestamp = txn.Timestamp
	txnPut := putArgs(key, []byte("foo"))
	// Add two puts (the second one gets BatchIndex 1, which was a failure mode
	// observed when this test was written and the failure fixed). Originally
	// observed in #10137, where this became relevant (before that, evaluation
	// happened downstream of Raft, so a serialization pass always took place).
	ba.Add(&txnPut)
	ba.Add(&txnPut)

	batch, _, _, _, pErr := tc.repl.evaluateTxnWriteBatch(ctx, makeIDKey(), ba, nil)
	defer batch.Close()
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !reflect.DeepEqual(&origTxn, txn) {
		t.Fatalf("transaction was mutated during evaluation: %s", pretty.Diff(&origTxn, txn))
	}
}

// TODO(peter): Test replicaMetrics.leaseholder.
func TestReplicaMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	progress := func(vals ...uint64) map[uint64]raft.Progress {
		m := make(map[uint64]raft.Progress)
		for i, v := range vals {
			m[uint64(i+1)] = raft.Progress{Match: v}
		}
		return m
	}
	status := func(lead uint64, progress map[uint64]raft.Progress) *raft.Status {
		status := &raft.Status{
			Progress: progress,
		}
		// The commit index is set so that a progress.Match value of 1 is behind
		// and 2 is ok.
		status.HardState.Commit = 12
		if lead == 1 {
			status.SoftState.RaftState = raft.StateLeader
		} else {
			status.SoftState.RaftState = raft.StateFollower
		}
		status.SoftState.Lead = lead
		return status
	}
	desc := func(ids ...int) roachpb.RangeDescriptor {
		var d roachpb.RangeDescriptor
		for i, id := range ids {
			d.Replicas = append(d.Replicas, roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(i + 1),
				StoreID:   roachpb.StoreID(id),
				NodeID:    roachpb.NodeID(id),
			})
		}
		return d
	}
	live := func(ids ...roachpb.NodeID) map[roachpb.NodeID]bool {
		m := make(map[roachpb.NodeID]bool)
		for _, id := range ids {
			m[id] = true
		}
		return m
	}

	testCases := []struct {
		replicas   int32
		storeID    roachpb.StoreID
		desc       roachpb.RangeDescriptor
		raftStatus *raft.Status
		liveness   map[roachpb.NodeID]bool
		expected   ReplicaMetrics
	}{
		// The leader of a 1-replica range is up.
		{1, 1, desc(1), status(1, progress(2)), live(1),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     10,
			}},
		// The leader of a 2-replica range is up (only 1 replica present).
		{2, 1, desc(1), status(1, progress(2)), live(1),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: true,
				BehindCount:     10,
			}},
		// The leader of a 2-replica range is up.
		{2, 1, desc(1, 2), status(1, progress(2)), live(1),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     10,
			}},
		// Both replicas of a 2-replica range are up to date.
		{2, 1, desc(1, 2), status(1, progress(2, 2)), live(1, 2),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     20,
			}},
		// Both replicas of a 2-replica range are up to date (local replica is not leader)
		{2, 2, desc(1, 2), status(2, progress(2, 2)), live(1, 2),
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    false,
				Unavailable:     false,
				Underreplicated: false,
				SelfBehindCount: 5,
			}},
		// Both replicas of a 2-replica range are live, but follower is behind.
		{2, 1, desc(1, 2), status(1, progress(2, 1)), live(1, 2),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     21,
			}},
		// Both replicas of a 2-replica range are up to date, but follower is dead.
		{2, 1, desc(1, 2), status(1, progress(2, 2)), live(1),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     20,
			}},
		// The leader of a 3-replica range is up.
		{3, 1, desc(1, 2, 3), status(1, progress(1)), live(1),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     11,
			}},
		// All replicas of a 3-replica range are up to date.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 2)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     30,
			}},
		// All replicas of a 3-replica range are up to date (match = 0 is
		// considered up to date).
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 0)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     20,
			}},
		// All replicas of a 3-replica range are live but one replica is behind.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 1)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: true,
				BehindCount:     31,
			}},
		// All replicas of a 3-replica range are live but two replicas are behind.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 1, 1)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     32,
			}},
		// All replicas of a 3-replica range are up to date, but one replica is dead.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 2)), live(1, 2),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: true,
				BehindCount:     30,
			}},
		// All replicas of a 3-replica range are up to date, but two replicas are dead.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 2)), live(1),
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     30,
			}},
		// Range has no leader, local replica is the range counter.
		{3, 1, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				SelfBehindCount: 15,
			}},
		// Range has no leader, local replica is the range counter.
		{3, 3, desc(3, 2, 1), status(0, progress(2, 2, 2)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				SelfBehindCount: 16,
			}},
		// Range has no leader, local replica is not the range counter.
		{3, 2, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    false,
				Unavailable:     false,
				Underreplicated: false,
				SelfBehindCount: 17,
			}},
		// Range has no leader, local replica is not the range counter.
		{3, 3, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(1, 2, 3),
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    false,
				Unavailable:     false,
				Underreplicated: false,
				SelfBehindCount: 18,
			}},
	}
	for i, c := range testCases {
		t.Run("", func(t *testing.T) {
			zoneConfig := config.ZoneConfig{NumReplicas: c.replicas}
			defer config.TestingSetDefaultZoneConfig(zoneConfig)()

			// Alternate between quiescent and non-quiescent replicas to test the
			// quiescent metric.
			c.expected.Quiescent = i%2 == 0
			metrics := calcReplicaMetrics(
				context.Background(), hlc.Timestamp{}, config.SystemConfig{},
				c.liveness, &c.desc, c.raftStatus, LeaseStatus{},
				c.storeID, c.expected.Quiescent, int64(i+1))
			if c.expected != metrics {
				t.Fatalf("unexpected metrics:\n%s", pretty.Diff(c.expected, metrics))
			}
		})
	}
}

// TestCancelPendingCommands verifies that cancelPendingCommands sends
// an error to each command awaiting execution.
func TestCancelPendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Install a proposal function which drops all increment commands on
	// the floor (so the command remains "pending" until we cancel it).
	proposalDroppedCh := make(chan struct{})
	proposalDropped := false
	tc.repl.mu.Lock()
	tc.repl.mu.submitProposalFn = func(p *ProposalData) error {
		if _, ok := p.Request.GetArg(roachpb.Increment); ok {
			if !proposalDropped {
				// Notify the main thread the first time we drop a proposal.
				close(proposalDroppedCh)
				proposalDropped = true
			}
			return nil
		}
		return defaultSubmitProposalLocked(tc.repl, p)
	}
	tc.repl.mu.Unlock()

	errChan := make(chan *roachpb.Error, 1)
	go func() {
		incArgs := incrementArgs(roachpb.Key("a"), 1)
		_, pErr := client.SendWrapped(context.Background(), tc.Sender(), &incArgs)
		errChan <- pErr
	}()

	<-proposalDroppedCh

	select {
	case pErr := <-errChan:
		t.Fatalf("command finished earlier than expected with error %v", pErr)
	default:
	}

	tc.repl.mu.Lock()
	tc.repl.cancelPendingCommandsLocked()
	tc.repl.mu.Unlock()

	pErr := <-errChan
	if _, ok := pErr.GetDetail().(*roachpb.AmbiguousResultError); !ok {
		t.Errorf("expected AmbiguousResultError, got %v", pErr)
	}
}

func TestMakeTimestampCacheRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := roachpb.Key("a")
	b := roachpb.Key("b")
	c := roachpb.Key("c")
	ac := roachpb.Span{Key: a, EndKey: c}
	testCases := []struct {
		maxKeys  int64
		req      roachpb.Request
		resp     roachpb.Response
		expected cacheRequest
	}{
		{
			0,
			&roachpb.ScanRequest{Span: ac},
			&roachpb.ScanResponse{},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			0,
			&roachpb.ScanRequest{Span: ac},
			&roachpb.ScanResponse{Rows: []roachpb.KeyValue{{Key: a}}},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			2,
			&roachpb.ScanRequest{Span: ac},
			&roachpb.ScanResponse{},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			2,
			&roachpb.ScanRequest{Span: ac},
			&roachpb.ScanResponse{Rows: []roachpb.KeyValue{{Key: a}}},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			2,
			&roachpb.ScanRequest{Span: ac},
			&roachpb.ScanResponse{Rows: []roachpb.KeyValue{{Key: a}, {Key: b}}},
			cacheRequest{reads: []roachpb.Span{{Key: a, EndKey: b.Next()}}},
		},
		{
			0,
			&roachpb.ReverseScanRequest{Span: ac},
			&roachpb.ReverseScanResponse{},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			0,
			&roachpb.ReverseScanRequest{Span: ac},
			&roachpb.ReverseScanResponse{Rows: []roachpb.KeyValue{{Key: a}}},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			2,
			&roachpb.ReverseScanRequest{Span: ac},
			&roachpb.ReverseScanResponse{},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			2,
			&roachpb.ReverseScanRequest{Span: ac},
			&roachpb.ReverseScanResponse{Rows: []roachpb.KeyValue{{Key: a}}},
			cacheRequest{reads: []roachpb.Span{ac}},
		},
		{
			2,
			&roachpb.ReverseScanRequest{Span: ac},
			&roachpb.ReverseScanResponse{Rows: []roachpb.KeyValue{{Key: c}, {Key: b}}},
			cacheRequest{reads: []roachpb.Span{{Key: b, EndKey: c}}},
		},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var ba roachpb.BatchRequest
			var br roachpb.BatchResponse
			ba.Header.MaxSpanRequestKeys = c.maxKeys
			ba.Add(c.req)
			br.Add(c.resp)
			cr := makeCacheRequest(&ba, &br, roachpb.RSpan{})
			if !reflect.DeepEqual(c.expected, cr) {
				t.Fatalf("%s", pretty.Diff(c.expected, cr))
			}
		})
	}
}
