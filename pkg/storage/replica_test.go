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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// allSpans is a SpanSet that covers *everything* for use in tests that don't
// care about properly declaring their spans.
var allSpans = func() spanset.SpanSet {
	var ss spanset.SpanSet
	ss.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    roachpb.KeyMin,
		EndKey: roachpb.KeyMax,
	})
	// Local keys (see `keys.localPrefix`).
	ss.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    append([]byte("\x01"), roachpb.KeyMin...),
		EndKey: append([]byte("\x01"), roachpb.KeyMax...),
	})
	return ss
}()

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
	l, _ := repl.GetLease()
	if l.Type() != roachpb.LeaseExpiration {
		panic("leaseExpiry only valid for expiration-based leases")
	}
	return l.Expiration.WallTime + 1
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
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	tc.StartWithStoreConfig(t, stopper, cfg)
}

// StartWithStoreConfig initializes the test context with a single
// range covering the entire keyspace.
func (tc *testContext) StartWithStoreConfig(t testing.TB, stopper *stop.Stopper, cfg StoreConfig) {
	tc.TB = t
	// Setup fake zone config handler.
	config.TestingSetupZoneConfigHook(stopper)
	if tc.gossip == nil {
		rpcContext := rpc.NewContext(
			cfg.AmbientCtx, &base.Config{Insecure: true}, cfg.Clock, stopper, &cfg.Settings.Version)
		server := rpc.NewServer(rpcContext) // never started
		tc.gossip = gossip.NewTest(1, rpcContext, server, stopper, metric.NewRegistry())
	}
	if tc.engine == nil {
		tc.engine = engine.NewInMem(roachpb.Attributes{Attrs: []string{"dc1", "mem"}}, 1<<20)
		stopper.AddCloser(tc.engine)
	}
	if tc.transport == nil {
		tc.transport = NewDummyRaftTransport(cfg.Settings)
	}
	ctx := context.TODO()
	if tc.store == nil {
		cfg.Gossip = tc.gossip
		cfg.Transport = tc.transport
		cfg.StorePool = NewTestStorePool(cfg)
		// Create a test sender without setting a store. This is to deal with the
		// circular dependency between the test sender and the store. The actual
		// store will be passed to the sender after it is created and bootstrapped.
		factory := &testSenderFactory{}
		cfg.DB = client.NewDB(cfg.AmbientCtx, factory, cfg.Clock)
		if err := Bootstrap(ctx, tc.engine, roachpb.StoreIdent{
			ClusterID: uuid.MakeV4(),
			NodeID:    1,
			StoreID:   1,
		}, cfg.Settings.Version.BootstrapVersion()); err != nil {
			t.Fatal(err)
		}
		tc.store = NewStore(cfg, tc.engine, &roachpb.NodeDescriptor{NodeID: 1})
		// Now that we have our actual store, monkey patch the factory used in cfg.DB.
		factory.setStore(tc.store)
		// We created the store without a real KV client, so it can't perform splits
		// or merges.
		tc.store.splitQueue.SetDisabled(true)
		tc.store.mergeQueue.SetDisabled(true)

		if tc.repl == nil && tc.bootstrapMode == bootstrapRangeWithMetadata {
			if err := tc.store.BootstrapRange(nil, cfg.Settings.Version.ServerVersion); err != nil {
				t.Fatal(err)
			}
		}
		if err := tc.store.Start(ctx, stopper); err != nil {
			t.Fatal(err)
		}
		tc.store.WaitForInit()
	}

	realRange := tc.repl == nil

	if realRange {
		if tc.bootstrapMode == bootstrapRangeOnly {
			testDesc := testRangeDescriptor()
			if _, err := stateloader.WriteInitialState(
				ctx,
				tc.store.ClusterSettings(),
				tc.store.Engine(),
				enginepb.MVCCStats{},
				*testDesc,
				roachpb.BootstrapLease(),
				hlc.Timestamp{},
				hlc.Timestamp{},
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
		&config.SystemConfigEntries{}, 0); err != nil {
		return err
	}

	testutils.SucceedsSoon(t, func() error {
		if cfg := tc.gossip.GetSystemConfig(); cfg == nil {
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
	if err := updateRangeDescriptor(&ba, descKey, &oldDesc, &newDesc); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	if err := tc.store.DB().Run(ctx, &ba); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}

	tc.repl.setDesc(ctx, &newDesc)
	tc.repl.raftMu.Lock()
	tc.repl.mu.Lock()
	tc.repl.assertStateLocked(ctx, tc.engine)
	tc.repl.mu.Unlock()
	tc.repl.raftMu.Unlock()
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
	txn := roachpb.MakeTransaction(name, baseKey, userPriority, isolation, now, offset)
	return &txn
}

// assignSeqNumsForReqs sets sequence numbers for each of the provided requests
// given a transaction proto. It also updates the proto to reflect the incremented
// sequence number.
func assignSeqNumsForReqs(txn *roachpb.Transaction, reqs ...roachpb.Request) {
	for _, ru := range reqs {
		txn.Sequence++
		oldHeader := ru.Header()
		oldHeader.Sequence = txn.Sequence
		ru.SetHeader(oldHeader)
	}
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
	txnReqs := make([]roachpb.RequestUnion, 3)
	txnReqs[0].MustSetInner(&roachpb.BeginTransactionRequest{})
	txnReqs[1].MustSetInner(&roachpb.PutRequest{})
	txnReqs[2].MustSetInner(&roachpb.EndTransactionRequest{})
	txnReqsNoRefresh := make([]roachpb.RequestUnion, 3)
	txnReqsNoRefresh[0].MustSetInner(&roachpb.BeginTransactionRequest{})
	txnReqsNoRefresh[1].MustSetInner(&roachpb.PutRequest{})
	txnReqsNoRefresh[2].MustSetInner(&roachpb.EndTransactionRequest{NoRefreshSpans: true})
	testCases := []struct {
		bu      []roachpb.RequestUnion
		isTxn   bool
		isWTO   bool
		isTSOff bool
		iso     enginepb.IsolationType
		exp1PC  bool
	}{
		{[]roachpb.RequestUnion{}, false, false, false, enginepb.SERIALIZABLE, false},
		{[]roachpb.RequestUnion{}, true, false, false, enginepb.SERIALIZABLE, false},
		{[]roachpb.RequestUnion{{Value: &roachpb.RequestUnion_Get{Get: &roachpb.GetRequest{}}}}, true, false, false, enginepb.SERIALIZABLE, false},
		{[]roachpb.RequestUnion{{Value: &roachpb.RequestUnion_Put{Put: &roachpb.PutRequest{}}}}, true, false, false, enginepb.SERIALIZABLE, false},
		{txnReqs[0 : len(txnReqs)-1], true, false, false, enginepb.SERIALIZABLE, false},
		{txnReqs[1:], true, false, false, enginepb.SERIALIZABLE, false},
		{txnReqs, true, false, false, enginepb.SERIALIZABLE, true},
		{txnReqs, true, false, false, enginepb.SNAPSHOT, true},
		{txnReqs, true, true, false, enginepb.SERIALIZABLE, false},
		{txnReqs, true, true, false, enginepb.SNAPSHOT, false},
		{txnReqs, true, false, true, enginepb.SERIALIZABLE, false},
		{txnReqs, true, false, true, enginepb.SNAPSHOT, false},
		{txnReqs, true, true, true, enginepb.SERIALIZABLE, false},
		{txnReqs, true, true, true, enginepb.SNAPSHOT, false},
		{txnReqsNoRefresh, true, false, false, enginepb.SERIALIZABLE, true},
		{txnReqsNoRefresh, true, false, false, enginepb.SNAPSHOT, true},
		{txnReqsNoRefresh, true, true, false, enginepb.SERIALIZABLE, true},
		{txnReqsNoRefresh, true, true, false, enginepb.SNAPSHOT, false},
		{txnReqsNoRefresh, true, false, true, enginepb.SERIALIZABLE, true},
		{txnReqsNoRefresh, true, false, true, enginepb.SNAPSHOT, false},
		{txnReqsNoRefresh, true, true, true, enginepb.SERIALIZABLE, true},
		{txnReqsNoRefresh, true, true, true, enginepb.SNAPSHOT, false},
	}

	clock := hlc.NewClock(log.Logger, hlc.UnixNano, time.Nanosecond)
	for i, c := range testCases {
		ba := roachpb.BatchRequest{Requests: c.bu}
		if c.isTxn {
			ba.Txn = newTransaction("txn", roachpb.Key("a"), 1, c.iso, clock)
			if c.isWTO {
				ba.Txn.WriteTooOld = true
			}
			if c.isTSOff {
				ba.Txn.Timestamp = ba.Txn.OrigTimestamp.Add(1, 0)
			}
		}
		if is1PC := isOnePhaseCommit(ba, &storagebase.StoreTestingKnobs{}); is1PC != c.exp1PC {
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

	if statsKey := keys.RangeStatsLegacyKey(desc.RangeID); !r.ContainsKey(statsKey) {
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
	exLease, _ := r.GetLease()
	ch, _, _, pErr := r.propose(context.TODO(), exLease, ba, nil, &allSpans)
	if pErr == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor this to a more conventional error-handling pattern.
		pErr = (<-ch).Err
	}
	return pErr.GoError()
}

// TestReplicaReadConsistency verifies behavior of the range under
// different read consistencies. Note that this unittest plays
// fast and loose with granting range leases.
func TestReplicaReadConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	gArgs := getArgs(roachpb.Key("a"))

	// Try consistent read and verify success.

	if _, err := tc.SendWrapped(&gArgs); err != nil {
		t.Errorf("expected success on consistent read: %s", err)
	}

	// Try a read commmitted read and an inconsistent read, both within a
	// transaction.
	txn := newTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE, tc.Clock())
	assignSeqNumsForReqs(txn, &gArgs)

	if _, err := tc.SendWrappedWith(roachpb.Header{
		Txn:             txn,
		ReadConsistency: roachpb.READ_UNCOMMITTED,
	}, &gArgs); err == nil {
		t.Errorf("expected error on read uncommitted read within a txn")
	}

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
		Expiration: start.Add(10, 0).Clone(),
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

	_, pErr = tc.SendWrappedWith(roachpb.Header{
		ReadConsistency: roachpb.READ_UNCOMMITTED,
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
	clock := hlc.NewClock(log.Logger, manual.UnixNano, 100*time.Millisecond)
	tc := testContext{manualClock: manual}
	tsc := TestStoreConfig(clock)
	var leaseAcquisitionTrap atomic.Value
	tsc.TestingKnobs.DisableAutomaticLeaseRenewal = true
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, cfg)

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
		Expiration: start.Add(10, 0).Clone(),
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

func TestLeaseReplicaNotInDesc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	lease, _ := tc.repl.GetLease()
	invalidLease := lease
	invalidLease.Sequence++
	invalidLease.Replica.StoreID += 12345

	raftCmd := storagepb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		ProposerReplica:       invalidLease.Replica,
		ReplicatedEvalResult: storagepb.ReplicatedEvalResult{
			IsLeaseRequest: true,
			State: &storagepb.ReplicaState{
				Lease: &invalidLease,
			},
		},
	}
	tc.repl.mu.Lock()
	_, _, pErr := tc.repl.checkForcedErrLocked(
		context.Background(), makeIDKey(), raftCmd, nil /* proposal */, false, /* proposedLocally */
	)
	tc.repl.mu.Unlock()
	if _, isErr := pErr.GetDetail().(*roachpb.LeaseRejectedError); !isErr {
		t.Fatal(pErr)
	} else if !testutils.IsPError(pErr, "replica not part of range") {
		t.Fatal(pErr)
	}
}

func TestReplicaRangeBoundsChecking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.RKey("a")
	firstRepl := tc.store.LookupReplica(key)
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
	status := repl.leaseStatus(*repl.mu.state.Lease, timestamp, repl.mu.minLeaseProposedTS)
	return repl.mu.state.Lease.OwnedBy(repl.store.StoreID()), status.State != storagepb.LeaseState_VALID
}

func TestReplicaLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.manualClock = hlc.NewManualClock(123)
	tsc := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	tsc.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, tsc)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	// Test that leases with invalid times are rejected.
	// Start leases at a point that avoids overlapping with the existing lease.
	leaseDuration := tc.store.cfg.RangeLeaseActiveDuration()
	start := hlc.Timestamp{WallTime: (time.Second + leaseDuration).Nanoseconds(), Logical: 0}
	for _, lease := range []roachpb.Lease{
		{Start: start, Expiration: &hlc.Timestamp{}},
	} {
		if _, err := batcheval.RequestLease(context.Background(), tc.store.Engine(),
			batcheval.CommandArgs{
				EvalCtx: NewReplicaEvalContext(tc.repl, &allSpans),
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
		Expiration: now.Add(20, 0).Clone(),
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
	tc.repl.mu.Lock()
	tc.repl.mu.submitProposalFn = func(*ProposalData) error {
		return &roachpb.LeaseRejectedError{
			Message: "replica not found",
		}
	}
	tc.repl.mu.Unlock()

	{
		_, err := tc.repl.redirectOnOrAcquireLease(context.Background())
		if _, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, err)
		}
	}
}

func TestReplicaNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0).Clone(),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	header := roachpb.RequestHeader{
		Key: roachpb.Key("a"),
	}
	testCases := []roachpb.Request{
		// Admin split covers admin commands.
		&roachpb.AdminSplitRequest{
			RequestHeader: header,
			SplitKey:      roachpb.Key("a"),
		},
		// Get covers read-only commands.
		&roachpb.GetRequest{
			RequestHeader: header,
		},
		// Put covers read-write commands.
		&roachpb.PutRequest{
			RequestHeader: header,
			Value:         roachpb.MakeValueFromString("value"),
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	var tc testContext
	cfg := TestStoreConfig(nil)
	// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
	// refreshes so that our lease proposal does not risk being rejected
	// with an AmbiguousResultError.
	cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
	cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
	tc.StartWithStoreConfig(t, stopper, cfg)

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
		Expiration: now.Add(10, 0).Clone(),
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
		Expiration: now.Add(10, 0).Clone(),
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

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, cfg)

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
	if cfg := tc.gossip.GetSystemConfig(); cfg != nil {
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
		Expiration: now.Add(10, 0).Clone(),
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
		Expiration: now.Add(20, 0).Clone(),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
	}); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		cfg := tc.gossip.GetSystemConfig()
		if cfg == nil {
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

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, cfg)

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
			Expiration: test.expiration.Clone(),
			Replica: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(test.storeID),
				NodeID:    roachpb.NodeID(test.storeID),
				StoreID:   test.storeID,
			},
		}); !testutils.IsError(err, test.expErr) {
			t.Fatalf("%d: unexpected error %v", i, err)
		}
		// Verify expected low water mark.
		rTS, _ := tc.repl.store.tsCache.GetMaxRead(roachpb.Key("a"), nil)
		wTS, _ := tc.repl.store.tsCache.GetMaxWrite(roachpb.Key("a"), nil)

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

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(t, stopper, cfg)

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().Now()
	lease := &roachpb.Lease{
		Start:      now,
		Expiration: now.Add(10, 0).Clone(),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}
	exLease, _ := tc.repl.GetLease()
	ba := roachpb.BatchRequest{}
	ba.Timestamp = tc.repl.store.Clock().Now()
	ba.Add(&roachpb.RequestLeaseRequest{Lease: *lease})
	ch, _, _, pErr := tc.repl.propose(context.Background(), exLease, ba, nil, &allSpans)
	if pErr == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor to a more conventional error-handling pattern.
		// Remove ambiguity about where the "replica not found" error comes from.
		pErr = (<-ch).Err
	}
	if !testutils.IsPError(pErr, "replica not found") {
		t.Errorf("unexpected error obtaining lease for invalid store: %v", pErr)
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
	ctx := context.Background()
	status, pErr := tc.repl.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		t.Fatal(pErr)
	}

	tc.store.SetDraining(true)
	tc.repl.mu.Lock()
	pErr = <-tc.repl.requestLeaseLocked(ctx, status).C()
	tc.repl.mu.Unlock()
	_, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError)
	if !ok {
		t.Fatalf("expected NotLeaseHolderError, not %v", pErr)
	}
	tc.store.SetDraining(false)
	// Newly undrained, leases work again.
	if _, pErr := tc.repl.redirectOnOrAcquireLease(ctx); pErr != nil {
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
			if err := protoutil.Unmarshal(bytes, &rangeDesc); err != nil {
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
	if cfg := tc.gossip.GetSystemConfig(); cfg == nil {
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
		assignSeqNumsForReqs(txn, test.req)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), test.h, test.req); pErr != nil {
			t.Fatal(pErr)
		}
		txn.Writing = true

		// System config is not gossiped.
		cfg := tc.gossip.GetSystemConfig()
		if cfg == nil {
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

	assignSeqNumsForReqs(txn, &req1)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), roachpb.Header{
		Txn: txn,
	}, &req1); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Writing = true

	req2, h := endTxnArgs(txn, true /* commit */)
	req2.IntentSpans = []roachpb.Span{{Key: key}}
	assignSeqNumsForReqs(txn, &req2)
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
	lease, _ := tc.repl.GetLease()
	if tc.repl.leaseStatus(lease, tc.Clock().Now(), hlc.Timestamp{}).State != storagepb.LeaseState_EXPIRED {
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
	var cfg config.SystemConfigEntries
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
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	}
}

func putArgs(key roachpb.Key, value []byte) roachpb.PutRequest {
	return roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

func cPutArgs(key roachpb.Key, value, expValue []byte) roachpb.ConditionalPutRequest {
	expV := roachpb.MakeValueFromBytes(expValue)
	return roachpb.ConditionalPutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value:    roachpb.MakeValueFromBytes(value),
		ExpValue: &expV,
	}
}

func iPutArgs(key roachpb.Key, value []byte) roachpb.InitPutRequest {
	return roachpb.InitPutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

func deleteArgs(key roachpb.Key) roachpb.DeleteRequest {
	return roachpb.DeleteRequest{
		RequestHeader: roachpb.RequestHeader{
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
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

func scanArgs(start, end []byte) roachpb.ScanRequest {
	return roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
	}
}

func reverseScanArgs(start, end []byte) roachpb.ReverseScanRequest {
	return roachpb.ReverseScanRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
	}
}

func beginTxnArgs(
	key []byte, txn *roachpb.Transaction,
) (roachpb.BeginTransactionRequest, roachpb.Header) {
	return roachpb.BeginTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
	}, roachpb.Header{Txn: txn}
}

func endTxnArgs(
	txn *roachpb.Transaction, commit bool,
) (roachpb.EndTransactionRequest, roachpb.Header) {
	return roachpb.EndTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key, // not allowed when going through TxnCoordSender, but we're not
		},
		Commit: commit,
	}, roachpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType roachpb.PushTxnType,
) roachpb.PushTxnRequest {
	return roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: pushee.Key,
		},
		Now:       pusher.Timestamp,
		PushTo:    pusher.Timestamp,
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (roachpb.HeartbeatTxnRequest, roachpb.Header) {
	return roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	}, roachpb.Header{Txn: txn}
}

func queryIntentArgs(
	key []byte, txn enginepb.TxnMeta, behavior roachpb.QueryIntentRequest_IfMissingBehavior,
) roachpb.QueryIntentRequest {
	return roachpb.QueryIntentRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Txn:       txn,
		IfMissing: behavior,
	}
}

func internalMergeArgs(key []byte, value roachpb.Value) roachpb.MergeRequest {
	return roachpb.MergeRequest{
		RequestHeader: roachpb.RequestHeader{
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
		RequestHeader: roachpb.RequestHeader{
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
		// Existing key at 09, ten puts, expect first nine puts are blind.
		{
			roachpb.Key("09"),
			[]roachpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, false,
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
			testutils.RunTrueAndFalse(t, "withMinLeaseProposedTS", func(t *testing.T, withMinLeaseProposedTS bool) {
				tc := testContext{}
				stopper := stop.NewStopper()
				defer stopper.Stop(context.TODO())
				tc.Start(t, stopper)

				lease, _ := tc.repl.GetLease()

				// This is a single-replica test; since we're automatically pushing back
				// the start of a lease as far as possible, and since there is an auto-
				// matic lease for us at the beginning, we'll basically create a lease
				// from then on. That is, unless the minLeaseProposedTS which gets set
				// automatically at server start forces us to get a new lease. We
				// simulate both cases.
				var expStart hlc.Timestamp

				tc.repl.mu.Lock()
				if !withMinLeaseProposedTS {
					tc.repl.mu.minLeaseProposedTS = hlc.Timestamp{}
					expStart = lease.Start
				} else {
					expStart = tc.repl.mu.minLeaseProposedTS
				}
				tc.repl.mu.Unlock()

				tc.manualClock.Set(leaseExpiry(tc.repl))

				ts := tc.Clock().Now().Next()
				if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, test); pErr != nil {
					t.Error(pErr)
				}
				if held, expired := hasLease(tc.repl, ts); !held || expired {
					t.Errorf("expected lease acquisition")
				}
				lease, _ = tc.repl.GetLease()
				if lease.Start != expStart {
					t.Errorf("unexpected lease start: %s; expected %s", lease.Start, expStart)
				}

				if *lease.DeprecatedStartStasis != *lease.Expiration {
					t.Errorf("%s already in stasis (or beyond): %+v", ts, lease)
				}
				if !ts.Less(*lease.Expiration) {
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
					newLease, _ := tc.repl.GetLease()
					if !lease.Expiration.Less(*newLease.Expiration) {
						return errors.Errorf("lease did not get extended: %+v to %+v", lease, newLease)
					}
					return nil
				})
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
	testutils.RunTrueAndFalse(t, "withError", func(t *testing.T, withError bool) {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())

		tc := testContext{manualClock: hlc.NewManualClock(123)}
		cfg := TestStoreConfig(hlc.NewClock(log.Logger, tc.manualClock.UnixNano, time.Nanosecond))
		// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
		// refreshes so that our lease proposal does not risk being rejected
		// with an AmbiguousResultError.
		cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
		cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
		tc.StartWithStoreConfig(t, stopper, cfg)

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
					tc.repl.cleanupFailedProposalLocked(proposal)
					proposal.finishApplication(proposalResult{Err: roachpb.NewErrorf(origMsg)})
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
			if err := stopper.RunAsyncTask(context.Background(), "test", func(ctx context.Context) {
				tc.repl.mu.Lock()
				status := tc.repl.leaseStatus(*tc.repl.mu.state.Lease, ts, hlc.Timestamp{})
				llHandle := tc.repl.requestLeaseLocked(ctx, status)
				tc.repl.mu.Unlock()
				wg.Done()
				pErr := <-llHandle.C()
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
	})
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
	noID := uuid.UUID{}
	rTS, rTxnID := tc.repl.store.tsCache.GetMaxRead(roachpb.Key("a"), nil)
	wTS, wTxnID := tc.repl.store.tsCache.GetMaxWrite(roachpb.Key("a"), nil)
	if rTS.WallTime != t0.Nanoseconds() || wTS.WallTime != startNanos || rTxnID != noID || wTxnID != noID {
		t.Errorf("expected rTS=1s and wTS=0s, but got %s, %s; rTxnID=%s, wTxnID=%s", rTS, wTS, rTxnID, wTxnID)
	}
	// Verify the timestamp cache has rTS=0s and wTS=2s for "b".
	rTS, rTxnID = tc.repl.store.tsCache.GetMaxRead(roachpb.Key("b"), nil)
	wTS, wTxnID = tc.repl.store.tsCache.GetMaxWrite(roachpb.Key("b"), nil)
	if rTS.WallTime != startNanos || wTS.WallTime != t1.Nanoseconds() || rTxnID != noID || wTxnID != noID {
		t.Errorf("expected rTS=0s and wTS=2s, but got %s, %s; rTxnID=%s, wTxnID=%s", rTS, wTS, rTxnID, wTxnID)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, rTxnID = tc.repl.store.tsCache.GetMaxRead(roachpb.Key("c"), nil)
	wTS, wTxnID = tc.repl.store.tsCache.GetMaxWrite(roachpb.Key("c"), nil)
	if rTS.WallTime != startNanos || wTS.WallTime != startNanos || rTxnID != noID || wTxnID != noID {
		t.Errorf("expected rTS=0s and wTS=0s, but got %s %s; rTxnID=%s, wTxnID=%s", rTS, wTS, rTxnID, wTxnID)
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
			addReqs = []string{"", "read"}
		} else {
			addReqs = []string{"", "write"}
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
						tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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
						if err := stopper.RunAsyncTask(context.Background(), "test", func(_ context.Context) {
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
						if err := stopper.RunAsyncTask(context.Background(), "", func(_ context.Context) {
							args := readOrWriteArgs(key1, test.cmd2Read)
							cmd2Done <- sendWithHeader(roachpb.Header{}, args)
						}); err != nil {
							t.Fatal(err)
						}

						// Next, try read for a non-impacted key--should go through immediately.
						cmd3Done := make(chan *roachpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), "", func(_ context.Context) {
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

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			key := roachpb.Key("key1")
			blockingStart := make(chan struct{}, 1)
			blockingDone := make(chan struct{})

			tc := testContext{}
			tsc := TestStoreConfig(nil)
			tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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
					ReadConsistency: rc,
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
		})
	}
}

// TestReplicaCommandQueueCancellation verifies that commands which are
// waiting on the command queue do not execute or deadlock if their context
// is canceled, and that commands dependent on canceled commands execute
// correctly.
func TestReplicaCommandQueueCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	keyC := roachpb.Key("c")
	keyD := roachpb.Key("d")
	keyE := roachpb.Key("e")
	keyF := roachpb.Key("e")

	makeSpan := func(start, end roachpb.Key) roachpb.Span {
		return roachpb.Span{Key: start, EndKey: end.Next()}
	}
	spanAB := makeSpan(keyA, keyB)
	spanAC := makeSpan(keyA, keyC)
	spanAF := makeSpan(keyA, keyF)
	spanBE := makeSpan(keyB, keyE)
	spanCD := makeSpan(keyC, keyD)
	spanDF := makeSpan(keyD, keyF)
	spanEF := makeSpan(keyE, keyF)

	testCases := []struct {
		name   string
		instrs []cancelInstr
	}{
		//  -----      -----     -----      xxxxx
		//    |    ->    |    &    |    ->    |    etc.
		//  -----      xxxxx     -----      -----
		{name: "SingleDependency", instrs: []cancelInstr{
			{span: spanAF},
			{span: spanAF},
		}},
		//  --------      xxxxxxxx      --------      xxxxxxxx     --------      --------
		//   |    |   ->   |    |    &   |    |   ->   |    |   &   |    |   ->   |    |   etc.
		//  ---  ---      ---  ---      ---  ---      xxx  xxx     ---  ---      xxx  xxx
		{name: "MultipleDependencies", instrs: []cancelInstr{
			{span: spanAF},
			{span: spanAC},
			{span: spanDF},
		}},
		//  -----      -----       -----      xxxxx       -----      xxxxx
		//    |          |           |          |           |          |
		//  -----  ->  xxxxx   &   -----  ->  xxxxx   &   -----  ->  xxxxx  etc.
		//    |          |           |          |           |          |
		//  -----      -----       -----      -----       -----      xxxxx
		{name: "DependencyChain", instrs: []cancelInstr{
			{span: spanAF},
			{span: spanAF},
			{span: spanAF},
		}},
		//  ---     ---      ---     ---     ---     ---      xxx     xxx
		//   |       |        |       |       |       |        |       |
		//  -----------  ->  xxxxxxxxxxx  &  -----------  ->  -----------  etc.
		//       |                |               |                |
		//      ---              ---             ---              xxx
		{name: "SplitDependencyChain", instrs: []cancelInstr{
			{span: spanAB},
			{span: spanEF},
			{span: spanAF},
			{span: spanCD},
		}},
		//  -------------      -------------     -------------      -------------
		//            |                  |                 |                  |
		//          -----              -----             -----              xxxxx
		//           |     ->           |     &           |     ->           |     etc.
		//    ---------          xxxxxxxxx         ---------          ---------
		//     |                  |                 |                  |
		//  -----              -----             -----              xxxxx
		{name: "NonOverlappingDependencyChain", instrs: []cancelInstr{
			{span: spanAF},
			{span: spanDF},
			{span: spanBE},
			{span: spanAC},
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Run every permutation of command cancellation as a separate subtest
			// for the current instruction configuration.
			var permuteCancellation func(cancelCmds []bool, left int)
			permuteCancellation = func(cancelCmds []bool, left int) {
				if left == 0 {
					var cancelOrder []int

					// Create a name for this permutation.
					// C = cancel
					// R = run
					var permName bytes.Buffer
					for i, cancel := range cancelCmds {
						if cancel {
							permName.WriteByte('C')
							cancelOrder = append(cancelOrder, i)
						} else {
							permName.WriteByte('R')
						}
					}

					t.Run(permName.String(), func(t *testing.T) {
						t.Run("Forward", func(t *testing.T) {
							ct := newCmdQCancelTest(t)
							ct.Run(tc.instrs, cancelOrder)
						})
						if len(cancelOrder) > 1 {
							t.Run("Reverse", func(t *testing.T) {
								ct := newCmdQCancelTest(t)
								reverse := make([]int, len(cancelOrder))
								for i, cancel := range cancelOrder {
									reverse[len(reverse)-1-i] = cancel
								}
								ct.Run(tc.instrs, reverse)
							})
						}
					})
					return
				}

				permuteCancellation(append(cancelCmds, false), left-1)
				permuteCancellation(append(cancelCmds, true), left-1)
			}
			permuteCancellation(nil, len(tc.instrs))
		})
	}
}

// TestReplicaCommandQueueCancellationCascade verifies that commands which are
// waiting on the command queue properly handle cascading cancellations without
// resulting in quadratic memory growth.
func TestReplicaCommandQueueCancellationCascade(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a dependency graph that looks like:
	//
	//   --- --- --- ---
	//    |   |   |   |
	//   ---------------
	//    |   |   |   |
	//   --- --- --- ---  ...
	//    |   |   |   |
	//   ---------------
	//    |   |   |   |
	//
	//         ...
	//
	// The test then creates a cascade cancellation scenario
	// where most commands are canceled:
	//
	//   --- xxx xxx xxx
	//    |   |   |   |
	//   xxxxxxxxxxxxxxx
	//    |   |   |   |
	//   --- xxx xxx xxx  ...
	//    |   |   |   |
	//   xxxxxxxxxxxxxxx
	//    |   |   |   |
	//
	//         ...
	//
	// Canceling commands in this pattern stresses two operations that
	// are necessary to avoid quadratic prerequisite count growth:
	// 1. Canceling all but the first command in each of the multi-command
	//    levels stresses OptimisticallyResolvePrereqs. Without scanning
	//    through all of a command's prereqs and ignoring already completed
	//    commands, commands that were finished would be transferred to
	//    dependent commands.
	// 2. Alternating multi-command layers and single command layers creates
	//    a scenario where dependencies would add duplicate prereqs to their
	//    prereq slice if not careful. This duplication of commands would
	//    result in quadratic growth.
	//
	// Without these operations, the test fails.
	const spansPerLevel = 25
	const levels = 25

	var levelSpans [spansPerLevel]roachpb.Span
	keyBuf := bytes.Repeat([]byte("a"), spansPerLevel+1)
	for i := 0; i < len(levelSpans); i++ {
		levelSpans[i] = roachpb.Span{
			Key:    keyBuf[:i+1],
			EndKey: keyBuf[:i+2],
		}
	}
	globalSpan := roachpb.Span{
		Key:    levelSpans[0].Key,
		EndKey: levelSpans[len(levelSpans)-1].EndKey,
	}

	var instrs []cancelInstr
	var cancelOrder []int
	for level := 0; level < levels; level++ {
		for i, span := range levelSpans {
			if i != 0 {
				cancelOrder = append(cancelOrder, len(instrs))
			}
			instrs = append(instrs, cancelInstr{span: span})
		}

		cancelOrder = append(cancelOrder, len(instrs))
		instrs = append(instrs, cancelInstr{span: globalSpan})
	}

	// Fails with "command never left CommandQueue after cancellation"
	// timeout without proper handling of cascade cancellation.
	ct := newCmdQCancelTest(t)
	ct.Run(instrs, cancelOrder)
}

// TestReplicaCommandQueueCancellationRandom verifies that commands in a
// random dependency graph which are waiting on the command queue do not
// execute or deadlock if their context is canceled, and that commands
// dependent on canceled commands execute correctly.
func TestReplicaCommandQueueCancellationRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rng, seed := randutil.NewPseudoRand()
	randKey := func() roachpb.Key {
		return roachpb.Key(randutil.RandBytes(rng, 1))
	}
	randBool := func() bool {
		return rng.Int31n(2) == 0
	}
	t.Logf("running with seed %d", seed)

	const trials = 25
	for i := 0; i < trials; i++ {
		commandCount := randutil.RandIntInRange(rng, 0, 25)
		instrs := make([]cancelInstr, commandCount)
		var cancelOrder []int
		for j := range instrs {
			startKey := randKey()
			endKey := randKey()
			if startKey.Compare(endKey) >= 0 {
				endKey = startKey.Next()
			}
			instrs[j] = cancelInstr{
				span: roachpb.Span{Key: startKey, EndKey: endKey},
			}
			if randBool() {
				cancelOrder = append(cancelOrder, j)
			}
		}
		shuffle.Shuffle(sort.IntSlice(cancelOrder))
		ct := newCmdQCancelTest(t)
		ct.Run(instrs, cancelOrder)
	}
}

// TestReplicaCommandQueueCancellationLocal tests various cancellation scenarios
// surrounding EndTxnReqs, PushTxnReqs, and HeartbeatTxnReqs. One of these
// scenarios was the cause of #16266.
func TestReplicaCommandQueueCancellationLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	now := hlc.Timestamp{WallTime: cmdQCancelTestTimestamp}
	txn := roachpb.MakeTransaction("txn", roachpb.Key("foobar"), 0, enginepb.SERIALIZABLE, now, 0)
	intentKey := roachpb.Key("baz")

	newBa := func(withTxn bool) *roachpb.BatchRequest {
		ba := roachpb.BatchRequest{}
		ba.Timestamp = now
		if withTxn {
			ba.Txn = &txn
		}
		return &ba
	}

	// Used only to block the other requests. Not a necessary part of the
	// scenario. See the comment on RunWithoutInitialSpan.
	heartbeatBa := newBa(true /* withTxn */)
	heartbeatBa.Add(&roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	})

	endTxnBa := newBa(true /* withTxn */)
	endTxnBa.Add(&roachpb.EndTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Commit: true,
		IntentSpans: []roachpb.Span{
			{Key: intentKey},
		},
	})

	splitBa := newBa(true /* withTxn */)
	splitBa.Add(&roachpb.EndTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Commit: true,
		IntentSpans: []roachpb.Span{
			{Key: intentKey},
		},
		InternalCommitTrigger: &roachpb.InternalCommitTrigger{
			SplitTrigger: &roachpb.SplitTrigger{
				LeftDesc: roachpb.RangeDescriptor{
					RangeID:  100,
					StartKey: roachpb.RKey("a"),
					EndKey:   roachpb.RKey("m"),
				},
				RightDesc: roachpb.RangeDescriptor{
					RangeID:  101,
					StartKey: roachpb.RKey("n"),
					EndKey:   roachpb.RKey("z"),
				},
			},
		},
	})

	pushBa := newBa(false /* withTxn */)
	pushBa.Add(&roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		PusheeTxn: txn.TxnMeta,
		Now:       now,
		PushType:  roachpb.PUSH_ABORT,
		Force:     true,
	})

	resolveIntentBa := newBa(false /* withTxn */)
	resolveIntentBa.Add(&roachpb.ResolveIntentRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: intentKey,
		},
		IntentTxn: txn.TxnMeta,
		// NB: This test originally used PENDING but the test requires the abort
		// cache key to be declared here, and this is no more the case due to an
		// optimization. Use an ABORTED resolve instead...
		Status: roachpb.ABORTED,
		// ... but with Poison=false which accesses the AbortSpan by *clearing*
		// it. Poison=true would fail the test since the transaction would not be
		// able to issue further commands.
		Poison: false,
	})

	getKeyBa := newBa(true /* withTxn */)
	getKeyBa.Add(&roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: intentKey,
		},
	})

	putKeyBa := newBa(true /* withTxn */)
	putKeyBa.Add(&roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: intentKey,
		},
		Value: roachpb.MakeValueFromBytes([]byte("val")),
	})

	t.Run("16266", func(t *testing.T) {
		instrs := []cancelInstr{
			{reqOverride: heartbeatBa, expErr: "txn record not found"},
			{reqOverride: endTxnBa, expErr: "txn record not found"},
			{reqOverride: pushBa},
			{reqOverride: pushBa},
		}

		ct := newCmdQCancelTest(t)
		ct.RunWithoutInitialSpan(instrs, []int{2})
	})
	t.Run("CancelEndTxn", func(t *testing.T) {
		instrs := []cancelInstr{
			{reqOverride: endTxnBa, expErr: "txn record not found"},
			{reqOverride: heartbeatBa, expErr: "txn record not found"},
			{reqOverride: pushBa},
			{reqOverride: heartbeatBa, expErr: "txn record not found"},
			{reqOverride: resolveIntentBa},
			{reqOverride: pushBa},
		}

		for _, cancelOrder := range [][]int{
			{1, 2},
			{2, 1},
		} {
			t.Run(fmt.Sprint(cancelOrder), func(t *testing.T) {
				ct := newCmdQCancelTest(t)
				ct.RunWithoutInitialSpan(instrs, cancelOrder)
			})
		}
	})
	t.Run("CancelResolveIntent", func(t *testing.T) {
		instrs := []cancelInstr{
			{reqOverride: resolveIntentBa},
			{reqOverride: getKeyBa},
			{reqOverride: putKeyBa},
			{reqOverride: resolveIntentBa},
			{reqOverride: putKeyBa, expErr: "retry txn"},
			{reqOverride: getKeyBa},
			{reqOverride: putKeyBa, expErr: "retry txn"},
			{reqOverride: resolveIntentBa},
			{reqOverride: pushBa},
		}

		for _, cancelOrder := range [][]int{
			{3, 4},
			{4, 3},
			{6, 3},
			{6, 5, 4, 3, 2},
			{2, 3, 4, 5, 6},
			{2, 6, 3, 5, 4},
		} {
			t.Run(fmt.Sprint(cancelOrder), func(t *testing.T) {
				ct := newCmdQCancelTest(t)
				ct.RunWithoutInitialSpan(instrs, cancelOrder)
			})
		}
	})
	t.Run("CancelSplit", func(t *testing.T) {
		instrs := []cancelInstr{
			{reqOverride: resolveIntentBa},
			{reqOverride: getKeyBa},
			{reqOverride: putKeyBa},
			{reqOverride: splitBa},
			{reqOverride: putKeyBa, expErr: "retry txn"},
			{reqOverride: getKeyBa},
			{reqOverride: endTxnBa, expErr: "txn record not found"},
			{reqOverride: resolveIntentBa},
		}

		for _, cancelOrder := range [][]int{
			{3},
			{3, 6},
			{6, 3},
			{1, 3, 5, 6},
			{5, 1, 6, 3},
		} {
			t.Run(fmt.Sprint(cancelOrder), func(t *testing.T) {
				ct := newCmdQCancelTest(t)
				ct.RunWithoutInitialSpan(instrs, cancelOrder)
			})
		}
	})
}

type cancelInstr struct {
	span        roachpb.Span
	reqOverride *roachpb.BatchRequest // overrides span when present
	expErr      string
}

func (ci cancelInstr) req() *roachpb.BatchRequest {
	if req := ci.reqOverride; req != nil {
		if s := ci.span; s.Key != nil || s.EndKey != nil {
			panic(fmt.Sprintf("if overriding instr request, span must be empty; found %v", ci))
		}
		if req.Timestamp.WallTime != cmdQCancelTestTimestamp {
			panic(fmt.Sprintf("BatchRequest should have WallTime %v; found %v",
				cmdQCancelTestTimestamp, req.Timestamp.WallTime))
		}
		if req.Txn != nil {
			for _, ru := range req.Requests {
				assignSeqNumsForReqs(req.Txn, ru.GetInner())
			}
		}
		return req
	}
	// Default to a DeleteRangeRequest because it performs a single write over its Span.
	ba := roachpb.BatchRequest{}
	ba.Timestamp = hlc.Timestamp{WallTime: cmdQCancelTestTimestamp}
	ba.Add(&roachpb.DeleteRangeRequest{RequestHeader: roachpb.RequestHeaderFromSpan(ci.span)})
	return &ba
}

type cmdQCancelTest struct {
	testing.TB
	s   *stop.Stopper
	tc  testContext
	tsc StoreConfig

	// the following channels intercept requests and block them.
	blockCmdBegin, blockCmdFinish chan struct{}
	// the following channels track CommandQueueActions.
	enteredCmdQ, canceledCmd, startingCmd chan struct{}

	cmds map[int]*testCmd
}

type testCmd struct {
	id      int
	spanSet *spanset.SpanSet
	prereqs map[int]struct{}
	cancel  context.CancelFunc
	done    <-chan *roachpb.Error
	expErr  string
}

func newCmdQCancelTest(t *testing.T) *cmdQCancelTest {
	ct := &cmdQCancelTest{
		TB:            t,
		s:             stop.NewStopper(),
		tsc:           TestStoreConfig(nil),
		blockCmdBegin: make(chan struct{}),
		enteredCmdQ:   make(chan struct{}),
		canceledCmd:   make(chan struct{}),
		startingCmd:   make(chan struct{}),
		cmds:          make(map[int]*testCmd),
	}
	ct.tsc.TestingKnobs.OnCommandQueueAction = ct.onCmdQAction
	return ct
}

// letCmdsRun unblocks commands in the CommandQueue that are ready to run. They
// will then be blocked when they try to exit the CommandQueue.
func (ct *cmdQCancelTest) letCmdsRun() {
	ct.blockCmdFinish = make(chan struct{})
	close(ct.blockCmdBegin)
}

// letCmdsFinish unblocks commands that are ready to exit the CommandQueue and
// finish. It resets the block on cmds in the CommandQueue so that they don't
// immediately start running.
func (ct *cmdQCancelTest) letCmdsFinish() {
	ct.blockCmdBegin = make(chan struct{})
	close(ct.blockCmdFinish)
}

const (
	// cmdQCancelTestTimestamp is the WallTime all BatchRequests in
	// cmdQCancelTest should use. This allows them to be identified and
	// intercepted by the onCmdQAction handler.
	cmdQCancelTestTimestamp = 12345
	// cmdQCancelTestDeadlockTimeout is the timeout used in cmdQCancelTest to
	// determine that a deadlock has occurred.
	cmdQCancelTestDeadlockTimeout = 500 * time.Millisecond
	// cmdQCancelTestProgressTimeout is the timeout used in cmdQCancelTest to
	// wait for any unexpected progress, before determining that progress has
	// properly halted. Once this timeout expires, the cmdQCancelTest unblocks
	// other commands to allow for further progress.
	cmdQCancelTestProgressTimeout = 10 * time.Millisecond
)

// onCmdQAction instruments CommandQueueActions, sending to different channels
// depending on the action. The instrumentation is also used to block commands
// at different stages of execution.
func (ct *cmdQCancelTest) onCmdQAction(
	ba *roachpb.BatchRequest, action storagebase.CommandQueueAction,
) {
	if ba.Header.Timestamp.WallTime == cmdQCancelTestTimestamp {
		quiesce := ct.s.ShouldQuiesce()
		switch action {
		case storagebase.CommandQueueWaitForPrereqs:
			select {
			case ct.enteredCmdQ <- struct{}{}:
			case <-quiesce:
			}
		case storagebase.CommandQueueCancellation:
			select {
			case ct.canceledCmd <- struct{}{}:
			case <-quiesce:
			}
		case storagebase.CommandQueueBeginExecuting:
			select {
			case <-ct.blockCmdBegin:
			case <-quiesce:
			}
			select {
			case ct.startingCmd <- struct{}{}:
			case <-quiesce:
			}
		case storagebase.CommandQueueFinishExecuting:
			select {
			case <-ct.blockCmdFinish:
			case <-quiesce:
			}
		}
	}
}

// startInstr will create a goroutine to send the BatchRequest. It returns a
// channel that will be given the result of the request when it completes.
func (ct *cmdQCancelTest) startInstr(
	ctx context.Context, ba *roachpb.BatchRequest,
) <-chan *roachpb.Error {
	done := make(chan *roachpb.Error)
	if err := ct.s.RunAsyncTask(context.Background(), "test", func(_ context.Context) {
		_, pErr := ct.tc.Sender().Send(ctx, *ba)
		select {
		case done <- pErr:
		case <-ct.s.ShouldQuiesce():
		}
	}); err != nil {
		ct.Fatal(err)
	}
	return done
}

func spanSetsOverlap(ss, ss2 *spanset.SpanSet) bool {
	for ac1 := spanset.SpanAccess(0); ac1 < spanset.NumSpanAccess; ac1++ {
		for ac2 := spanset.SpanAccess(0); ac2 < spanset.NumSpanAccess; ac2++ {
			if ac1 == spanset.SpanReadOnly && ac2 == spanset.SpanReadOnly {
				// Reads ignore other reads.
				continue
			}
			for sc := spanset.SpanScope(0); sc < spanset.NumSpanScope; sc++ {
				for _, s := range ss.GetSpans(ac1, sc) {
					for _, s2 := range ss2.GetSpans(ac2, sc) {
						if s.Overlaps(s2) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// insertCmds inserts each command into the CommandQueue, populating the cmds map
// while doing so.
func (ct *cmdQCancelTest) insertCmds(instrs []cancelInstr) {
	for i, instr := range instrs {
		// Create a new context for each instruction with a cancel function.
		ctx, cancel := context.WithCancel(context.Background())

		ba := instr.req()
		spanSet, err := collectSpans(roachpb.RangeDescriptor{}, ba)
		if err != nil {
			ct.Fatal(err)
		}

		// Start command and wait until the new command is in the command queue.
		cmdDone := ct.startInstr(ctx, ba)
		select {
		case <-ct.enteredCmdQ:
		case <-time.After(cmdQCancelTestDeadlockTimeout):
			ct.Fatalf("request %v never entered CommandQueue", ba)
		}

		// Determine the prereq set of each command, including all transitive
		// dependencies. Add the new command to the cmds map.
		prereqs := make(map[int]struct{})
		for j, prevCmd := range ct.cmds {
			if spanSetsOverlap(spanSet, prevCmd.spanSet) {
				prereqs[j] = struct{}{}
				for trans := range prevCmd.prereqs {
					prereqs[trans] = struct{}{}
				}
			} else if j == 0 {
				ct.Fatalf("all instruction spans should overlap the initial span; found %v",
					instr.span)
			}
		}
		ct.cmds[i] = &testCmd{
			id:      i,
			spanSet: spanSet,
			prereqs: prereqs,
			cancel:  cancel,
			done:    cmdDone,
			expErr:  instr.expErr,
		}
	}
}

// cancelCmds cancels all testCmds that need to be canceled, in the provided
// order.
func (ct *cmdQCancelTest) cancelCmds(cancelOrder []int) {
	for _, cancelID := range cancelOrder {
		if cancelID == 0 {
			ct.Fatalf("the first cancelInstr cannot be canceled")
		}

		cmd, ok := ct.cmds[cancelID]
		if !ok {
			ct.Fatalf("command %d not found", cancelID)
		}

		// Cancel the context on each cmd that should be canceled.
		cmd.cancel()

		// If either of these deadlocks, the command was never canceled and may have
		// unexpectedly begun executing. Indeed, the absence of such a deadlock is
		// what's being tested here.
		select {
		case <-ct.canceledCmd:
		case <-time.After(cmdQCancelTestDeadlockTimeout):
			ct.Fatalf("command %d never left CommandQueue after cancellation", cancelID)
		}
		select {
		case pErr := <-cmd.done:
			if ctxCancelErr := context.Canceled; !testutils.IsPError(pErr, ctxCancelErr.Error()) {
				ct.Fatalf("expected error %v for cmd %d, found %v", ctxCancelErr, cancelID, pErr)
			}
		case <-time.After(cmdQCancelTestDeadlockTimeout):
			ct.Fatalf("command %d never returned to client after cancellation", cancelID)
		}

		// Remove the command from the cmds map and delete it from the
		// prereq sets of all pending cmds.
		delete(ct.cmds, cancelID)
		for _, cmd := range ct.cmds {
			delete(cmd.prereqs, cancelID)
		}
	}
}

// runCmds runs all testCmds that were not canceled.
func (ct *cmdQCancelTest) runCmds() {
	for len(ct.cmds) > 0 {
		// Determine the commands we expect to be ready to run. Remove these
		// from the cmds map and from the prereq sets of all cmds still pending.
		var readyToRun []*testCmd
		for i, cmd := range ct.cmds {
			if len(cmd.prereqs) == 0 {
				readyToRun = append(readyToRun, cmd)
				delete(ct.cmds, i)
			}
		}

		readyLen := len(readyToRun)
		if readyLen == 0 {
			ct.Fatal("found no commands ready to run")
		}
		for _, cmd := range ct.cmds {
			for _, ready := range readyToRun {
				delete(cmd.prereqs, ready.id)
			}
		}

		// We should see exactly this many commands begin executing. If we see
		// fewer or more we'll deadlock, which is what we're testing for.
		ct.letCmdsRun()
		for i := range readyToRun {
			select {
			case <-ct.startingCmd:
			case <-time.After(cmdQCancelTestDeadlockTimeout):
				ct.Fatalf("expected %d commands to begin running together, saw %d", readyLen, i)
			}
		}
		select {
		case <-ct.startingCmd:
			ct.Fatalf("expected %d commands to begin running together, saw extra", readyLen)
		case <-time.After(cmdQCancelTestProgressTimeout):
		}

		// We should see exactly this many command finish. Again, the absence
		// of a deadlock is what we're testing for.
		ct.letCmdsFinish()
		for _, running := range readyToRun {
			select {
			case pErr := <-running.done:
				if running.expErr == "" {
					if pErr != nil {
						ct.Fatalf("expected no error for cmd %d, found %v", running.id, pErr)
					}
				} else {
					if !testutils.IsPError(pErr, running.expErr) {
						ct.Fatalf("expected error %q for cmd %d, found %v",
							running.expErr, running.id, pErr)
					}
				}
			case <-time.After(cmdQCancelTestDeadlockTimeout):
				ct.Fatalf("command %d never returned to client", running.id)
			}
		}
	}
}

// Run runs the cmdQCancelTest with the provided cancelInstrs. Commands will be
// canceled in the order provided.
func (ct *cmdQCancelTest) Run(instrs []cancelInstr, cancelOrder []int) {
	// Create an initial span that will block all others until we're ready to
	// begin monitoring. This initial span cannot be canceled before exiting
	// the prereq wait period.
	initial := cancelInstr{
		span: roachpb.Span{Key: keys.SystemMax, EndKey: keys.TableDataMin},
	}

	// Update the cancel order to account for the new instr.
	newCancelOrder := make([]int, len(cancelOrder))
	for i, cancelID := range cancelOrder {
		newCancelOrder[i] = cancelID + 1
	}

	ct.RunWithoutInitialSpan(append([]cancelInstr{initial}, instrs...), newCancelOrder)
}

// RunWithoutInitialSpan runs the cmdQCancelTest with the provided cancelInstrs.
// Commands will be canceled in the order provided. The first cancelInstrs
// should be a prereq of all other instructions and cannot be canceled. Use
// ct.Run to assure this.
func (ct *cmdQCancelTest) RunWithoutInitialSpan(instrs []cancelInstr, cancelOrder []int) {
	defer ct.s.Stop(context.Background())
	ct.tc.StartWithStoreConfig(ct, ct.s, ct.tsc)

	ct.insertCmds(instrs)
	ct.cancelCmds(cancelOrder)
	ct.runCmds()
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

	testutils.RunTrueAndFalse(t, "cmd1Read", func(t *testing.T, cmd1Read bool) {
		testutils.RunTrueAndFalse(t, "cmd2Read", func(t *testing.T, cmd2Read bool) {
			key := fmt.Sprintf("%v,%v", cmd1Read, cmd2Read)
			ba := roachpb.BatchRequest{}
			ba.Add(readOrWriteArgs(roachpb.Key(key), cmd1Read))
			ba.Add(readOrWriteArgs(roachpb.Key(key), cmd2Read))

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
	})
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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

	var spans spanset.SpanSet
	cmd, _ := batcheval.LookupCommand(roachpb.EndTransaction)
	cmd.DeclareKeys(
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
	if err := spans.CheckAllowed(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("b")}); err != nil {
		t.Fatalf("expected declaration of write access, err=%s", err)
	}
}

// TestReplicaCommandQueuePrereqDebugSummary tests the debug summary logged
// about a request's prerequisites when entering the command queue.
func TestReplicaCommandQueuePrereqDebugSummary(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var ba1, ba2, ba3 roachpb.BatchRequest
	txn := newTransaction("test", []byte("k"), 1, enginepb.SERIALIZABLE, nil)
	bt, _ := beginTxnArgs([]byte("k"), txn)
	put := putArgs([]byte("k"), []byte("v"))
	et, _ := endTxnArgs(txn, true)
	ba1.Add(&bt)
	ba2.Add(&put, &put, &put)
	ba3.Add(&bt, &put, &et)
	cmdForBatch := func(ba *roachpb.BatchRequest) *cmd {
		return &cmd{debugInfo: ba}
	}

	testCases := []struct {
		prereqs prereqCmdSet
		expect  string
	}{
		{
			prereqs: prereqCmdSet{
				/* SpanReadOnly */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {},
				},
				/* SpanReadWrite */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {},
				},
			},
			expect: "no prereqs",
		},
		{
			prereqs: prereqCmdSet{
				/* SpanReadOnly */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {},
				},
				/* SpanReadWrite */ {
					/* SpanGlobal */ {cmdForBatch(&ba3)},
					/* SpanLocal */ {},
				},
			},
			expect: "{write/global: [1 Put, 1 BeginTxn, 1 EndTxn]}",
		},
		{
			prereqs: prereqCmdSet{
				/* SpanReadOnly */ {
					/* SpanGlobal */ {cmdForBatch(&ba1)},
					/* SpanLocal */ {cmdForBatch(&ba1)},
				},
				/* SpanReadWrite */ {
					/* SpanGlobal */ {cmdForBatch(&ba2)},
					/* SpanLocal */ {cmdForBatch(&ba3)},
				},
			},
			expect: "{read/global: [1 BeginTxn]} {read/local: [1 BeginTxn]} {write/global: [3 Put]} {write/local: [1 Put, 1 BeginTxn, 1 EndTxn]}",
		},
		{
			prereqs: prereqCmdSet{
				/* SpanReadOnly */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {cmdForBatch(&ba1)},
				},
				/* SpanReadWrite */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {cmdForBatch(&ba1), cmdForBatch(&ba2), cmdForBatch(&ba3), cmdForBatch(&ba1)},
				},
			},
			expect: "{read/local: [1 BeginTxn]} {write/local: [1 BeginTxn], [3 Put], [1 Put, 1 BeginTxn, 1 EndTxn], [1 BeginTxn]}",
		},
		{
			prereqs: prereqCmdSet{
				/* SpanReadOnly */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {cmdForBatch(&ba1)},
				},
				/* SpanReadWrite */ {
					/* SpanGlobal */ {},
					/* SpanLocal */ {cmdForBatch(&ba1), cmdForBatch(&ba2), cmdForBatch(&ba3), cmdForBatch(&ba1), cmdForBatch(&ba2), cmdForBatch(&ba3)},
				},
			},
			expect: "{read/local: [1 BeginTxn]} {write/local: [1 BeginTxn], [3 Put], [1 Put, 1 BeginTxn, 1 EndTxn], [1 BeginTxn], [3 Put], ...}",
		},
	}
	for _, test := range testCases {
		t.Run(test.expect, func(t *testing.T) {
			s := prereqDebugSummary(test.prereqs)
			if s != test.expect {
				t.Errorf("expected %q for %+v, found %q", test.expect, test.prereqs, s)
			}
		})
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

func TestConditionalPutUpdatesTSCacheOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	// Set clock to time 1s and do the conditional put.
	t0 := 1 * time.Second
	tc.manualClock.Set(t0.Nanoseconds())

	// CPut args which expect value "1" to write "0".
	key := []byte("a")
	cpArgs := cPutArgs(key, []byte("1"), []byte("0"))
	_, pErr := tc.SendWrapped(&cpArgs)
	cfErr, ok := pErr.GetDetail().(*roachpb.ConditionFailedError)
	if !ok {
		t.Errorf("expected ConditionFailedError; got %v", pErr)
	}
	if cfErr.ActualValue != nil {
		t.Errorf("expected empty actual value; got %s", cfErr.ActualValue)
	}

	// Try a transactional put at a lower timestamp and ensure it is pushed.
	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	txn.OrigTimestamp, txn.Timestamp = makeTS(1, 0), makeTS(1, 0)
	pArgs := putArgs(key, []byte("value"))
	assignSeqNumsForReqs(txn, &pArgs)
	h := roachpb.Header{Txn: txn}
	_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), h, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if respH.Txn.Timestamp.WallTime != t0.Nanoseconds() {
		t.Errorf("expected write timestamp to upgrade to 1s; got %s", respH.Txn.Timestamp)
	}

	// Try a conditional put at a later timestamp which will fail
	// because there's now a transaction intent. This failure will
	// not update the timestamp cache.
	t1 := 2 * time.Second
	tc.manualClock.Set(t1.Nanoseconds())
	if pErr != nil {
		t.Fatal(pErr)
	}
	_, pErr = tc.SendWrapped(&cpArgs)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected WriteIntentError; got %v", pErr)
	}

	// Abort the intent and try to write again to ensure the timestamp
	// cache wasn't updated by the second failed conditional put.
	rArgs := &roachpb.ResolveIntentRequest{
		RequestHeader: pArgs.Header(),
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.ABORTED,
	}
	h = roachpb.Header{Timestamp: txn.Timestamp}
	if _, pErr = tc.SendWrappedWith(h, rArgs); pErr != nil {
		t.Fatal(pErr)
	}
	_, respH, pErr = SendWrapped(context.Background(), tc.Sender(), h, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if respH.Timestamp.WallTime != t0.Nanoseconds() {
		t.Errorf("expected write timestamp to succeed at 1s; got %s", respH.Timestamp)
	}
}

// TestReplicaNoTSCacheInconsistent verifies that the timestamp cache
// is not affected by inconsistent reads.
func TestReplicaNoTSCacheInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
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
				ReadConsistency: rc,
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
		})
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
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pArgs := putArgs(key, []byte("value"))
		assignSeqNumsForReqs(txn, &pArgs)

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
		assignSeqNumsForReqs(txn, &pArgs)
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
	assignSeqNumsForReqs(txn, &gArgs)

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now try a write and verify timestamp isn't incremented.
	pArgs := putArgs(key, []byte("value"))
	assignSeqNumsForReqs(txn, &pArgs)

	_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: txn}, &pArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if respH.Txn.Timestamp != txn.Timestamp {
		t.Errorf("expected timestamp to remain %s; got %s", txn.Timestamp, respH.Timestamp)
	}

	// Resolve the intent.
	rArgs := &roachpb.ResolveIntentRequest{
		RequestHeader: pArgs.Header(),
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.COMMITTED,
	}
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

// TestReplicaAbortSpanReadError verifies that an error is returned
// to the client in the event that a AbortSpan entry is found but is
// not decodable.
func TestReplicaAbortSpanReadError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	k := []byte("a")
	txn := newTransaction("test", k, 10, enginepb.SERIALIZABLE, tc.Clock())
	args := incrementArgs(k, 1)
	assignSeqNumsForReqs(txn, &args)

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite Abort span entry with garbage for the last op.
	key := keys.AbortSpanKey(tc.repl.RangeID, txn.ID)
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

// TestReplicaAbortSpanStoredTxnRetryError verifies that if a cached
// entry is present, a transaction restart error is returned.
func TestReplicaAbortSpanStoredTxnRetryError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := []byte("a")
	{
		txn := newTransaction("test", key, 10, enginepb.SERIALIZABLE, tc.Clock())
		txn.Sequence = int32(1)
		entry := roachpb.AbortSpanEntry{
			Key:       txn.Key,
			Timestamp: txn.Timestamp,
			Priority:  0,
		}
		if err := tc.repl.abortSpan.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
			t.Fatal(err)
		}

		args := incrementArgs(key, 1)
		assignSeqNumsForReqs(txn, &args)
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

	assignSeqNumsForReqs(txn, &args)
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

	// Pretend we restarted by increasing the epoch. That's all that's needed.
	txn.Epoch++
	assignSeqNumsForReqs(txn, &args)
	if pErr := try(); pErr != nil {
		t.Fatal(pErr)
	}

	// Try again with just an incremented sequence. Still good to go.
	assignSeqNumsForReqs(txn, &args)
	if pErr := try(); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestReplicaAbortSpanOnlyWithIntent verifies that a transactional command
// which goes through Raft but is not a transactional write (i.e. does not
// leave intents) passes the AbortSpan unhindered.
func TestReplicaAbortSpanOnlyWithIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn := newTransaction("test", []byte("test"), 10, enginepb.SERIALIZABLE, tc.Clock())
	txn.Sequence = 100
	entry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.Timestamp,
		Priority:  0,
	}
	if err := tc.repl.abortSpan.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	args, h := heartbeatArgs(txn, tc.Clock().Now())
	// If the AbortSpan were active for this request, we'd catch a txn retry.
	// Instead, we expect the error from heartbeating a nonexistent txn.
	_, pErr := tc.SendWrappedWith(h, &args)
	if tse, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); !ok ||
		tse.Reason != roachpb.TransactionStatusError_REASON_TXN_NOT_FOUND {
		t.Fatalf("expected TransactionStatusError with REASON_TXN_NOT_FOUND, found %v", pErr)
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
		assignSeqNumsForReqs(txn, &put)

		if _, pErr := maybeWrapWithBeginTransaction(
			context.Background(), tc.Sender(), roachpb.Header{Txn: txn}, &put,
		); pErr != nil {
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
			assignSeqNumsForReqs(txn, &etArgs)
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

// Test that, when a pushed Snapshot txn would get a "deadline exceeded" error,
// a Serializable one would get a serializable restart instead. In other words,
// for Serializable transactions, regular push retriable errors take precedence
// over the deadline check.
func TestSerializableDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	const expectedTransactionStatusError int = 1
	const expectedTransactionRestartError int = 2

	tests := []struct {
		isoLevel        enginepb.IsolationType
		expectedErrType int
		expectedErrMsg  string
	}{{
		isoLevel:        enginepb.SNAPSHOT,
		expectedErrType: expectedTransactionStatusError,
		expectedErrMsg:  "TransactionStatusError: transaction deadline exceeded",
	}, {
		isoLevel:        enginepb.SERIALIZABLE,
		expectedErrType: expectedTransactionRestartError,
		expectedErrMsg:  "TransactionRetryError: retry txn \\(RETRY_SERIALIZABLE\\)",
	}}
	for i, test := range tests {
		t.Run(test.isoLevel.String(), func(t *testing.T) {
			key := roachpb.Key("key: " + strconv.Itoa(i))
			// Start our txn. It will be pushed next.
			txn := newTransaction("test txn", key, roachpb.MinUserPriority,
				test.isoLevel, tc.Clock())
			beginTxn, header := beginTxnArgs(key, txn)
			if _, pErr := tc.SendWrappedWith(header, &beginTxn); pErr != nil {
				t.Fatal(pErr.GoError())
			}

			tc.manualClock.Increment(100)
			pusher := newTransaction(
				"test pusher", key, roachpb.MaxUserPriority,
				enginepb.SERIALIZABLE, tc.Clock())
			pushReq := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
			pushReq.Now = tc.Clock().Now()
			resp, pErr := tc.SendWrapped(&pushReq)
			if pErr != nil {
				t.Fatal(pErr)
			}
			updatedPushee := resp.(*roachpb.PushTxnResponse).PusheeTxn
			if updatedPushee.Status != roachpb.PENDING {
				t.Fatalf("expected pushee to still be alive, but got %+v", updatedPushee)
			}

			// Send an EndTransaction with a deadline below the point where the txn
			// has been pushed.
			etArgs, etHeader := endTxnArgs(txn, true /* commit */)
			deadline := updatedPushee.Timestamp
			deadline.Logical--
			etArgs.Deadline = &deadline
			_, pErr = tc.SendWrappedWith(etHeader, &etArgs)
			if pErr == nil {
				t.Fatalf("expected %q, got: nil", test.expectedErrMsg)
			}
			err := pErr.GoError()
			if test.expectedErrType == expectedTransactionStatusError {
				if _, ok := err.(*roachpb.TransactionStatusError); !ok ||
					!testutils.IsError(err, test.expectedErrMsg) {
					t.Fatalf("expected %q, got: %s (%T)", test.expectedErrMsg,
						err, err)
				}
			} else {
				if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok ||
					!testutils.IsError(err, test.expectedErrMsg) {
					t.Fatalf("expected %q, got: %s (%T)", test.expectedErrMsg,
						err, pErr.GetDetail())
				}
			}
		})
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

	gcHeader := roachpb.RequestHeader{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	}

	// Pretend that the GC queue removes the aborted transaction entry, as it
	// would after a period of inactivity, while our pushee txn is unaware and
	// may have written intents elsewhere.
	{
		gcReq := roachpb.GCRequest{
			RequestHeader: gcHeader,
			Keys: []roachpb.GCRequest_GCKey{
				{Key: keys.TransactionKey(pushee.Key, pushee.ID)},
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
	assignSeqNumsForReqs(txn, &bt, &put, &et)
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
			assignSeqNumsForReqs(txn, &bt, &put, &et)
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
	assignSeqNumsForReqs(txn, &pArgs)
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

	assignSeqNumsForReqs(txn, &args)
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
		assignSeqNumsForReqs(txn, &put)
		beginReply, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put)
		if pErr != nil {
			t.Fatal(pErr)
		}
		txn.Writing = true
		args, h := endTxnArgs(txn, commit)
		assignSeqNumsForReqs(txn, &args)
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
		txn.Epoch++ // need to fake a higher epoch to sneak past abort span
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
		assignSeqNumsForReqs(txn, &put)
		beginReply, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put)
		if pErr != nil {
			t.Fatal(pErr)
		}

		// Start out with a heartbeat to the transaction.
		hBA, h := heartbeatArgs(txn, tc.Clock().Now())

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
		assignSeqNumsForReqs(txn, &args)

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
		assignSeqNumsForReqs(pushee, &put)
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
		assignSeqNumsForReqs(pushee, &endTxn)
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
	assignSeqNumsForReqs(txn, &put)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}
	txn.Writing = true

	// Start out with a heartbeat to the transaction.
	hBA, h := heartbeatArgs(txn, tc.Clock().Now())

	_, pErr := tc.SendWrappedWith(h, &hBA)
	if pErr != nil {
		t.Error(pErr)
	}

	// Now end the txn with increased epoch and priority.
	args, h := endTxnArgs(txn, true)
	h.Txn.Epoch = txn.Epoch + 1
	h.Txn.Priority = txn.Priority + 1
	assignSeqNumsForReqs(txn, &args)

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
		{roachpb.Key("a"), doesNotExist, txn.Epoch, txn.Timestamp, "txn record not found"},
		{roachpb.Key("a"), roachpb.COMMITTED, txn.Epoch, txn.Timestamp, "already committed"},
		{roachpb.Key("b"), roachpb.ABORTED, txn.Epoch, txn.Timestamp,
			regexp.QuoteMeta("TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)")},
		{roachpb.Key("c"), roachpb.PENDING, txn.Epoch + 1, txn.Timestamp, "epoch regression: 0"},
		{roachpb.Key("d"), roachpb.PENDING, txn.Epoch, regressTS, `timestamp regression: 0`},
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
			if err := engine.MVCCPutProto(context.Background(), tc.repl.store.Engine(), nil, txnKey, hlc.Timestamp{},
				nil, &existTxn); err != nil {
				t.Fatal(err)
			}
		}

		// End the transaction, verify expected error.
		txn.Key = test.key
		args, h := endTxnArgs(txn, true)
		assignSeqNumsForReqs(txn, &args)

		if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, test.expErrRegexp) {
			t.Errorf("%d: expected error:\n%s\nto match:\n%s", i, pErr, test.expErrRegexp)
		} else if txn := pErr.GetTxn(); txn != nil && txn.ID == (uuid.UUID{}) {
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

	testutils.RunTrueAndFalse(t, "populateAbortSpan", func(t *testing.T, populateAbortSpan bool) {
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())
		tc.Start(t, stopper)

		key := []byte("a")
		txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		_, btH := beginTxnArgs(key, txn)
		put := putArgs(key, key)
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := maybeWrapWithBeginTransaction(
			context.TODO(), tc.Sender(), btH, &put,
		); pErr != nil {
			t.Fatal(pErr)
		}
		// Simulate what the client is supposed to do (update the transaction
		// based on the response). The Writing field is needed by this test.
		txn.Writing = true

		// Abort the transaction by pushing it with maximum priority.
		pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
		pusher.Priority = roachpb.MaxTxnPriority
		pushArgs := pushTxnArgs(pusher, btH.Txn, roachpb.PUSH_ABORT)
		if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Check that the intent has not yet been resolved.
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

		if populateAbortSpan {
			var txnRecord roachpb.Transaction
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			ok, err := engine.MVCCGetProto(
				context.TODO(), tc.repl.store.Engine(),
				txnKey, hlc.Timestamp{}, true /* consistent */, nil, /* txn */
				&txnRecord,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("txn record missing")
			}

			if pErr := tc.store.intentResolver.resolveIntents(context.TODO(),
				[]roachpb.Intent{{
					Span:   roachpb.Span{Key: key},
					Txn:    txnRecord.TxnMeta,
					Status: txnRecord.Status,
				}}, ResolveOptions{Wait: true, Poison: true}); pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Abort the transaction again. No error is returned.
		args, h := endTxnArgs(txn, false /* commit */)
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
	})
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
	assignSeqNumsForReqs(txn, &bt, &put, &et)
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
		lease, _ := tc.repl.GetLease()
		ch, _, _, err := tc.repl.propose(context.Background(), lease, ba, nil, &allSpans)
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
			assignSeqNumsForReqs(txn, &bt, &put)
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
			assignSeqNumsForReqs(&putTxn, &putB)
			_, respH, pErr := SendWrapped(context.Background(), tc.Sender(), roachpb.Header{Txn: &putTxn}, &putB)
			if pErr != nil {
				t.Fatal(pErr)
			}

			// EndTransaction.
			etTxn := respH.Txn.Clone()
			et, etH := endTxnArgs(&etTxn, true)
			et.IntentSpans = []roachpb.Span{{Key: key, EndKey: nil}, {Key: keyB, EndKey: nil}}
			assignSeqNumsForReqs(&etTxn, &et)
			if _, pErr := tc.SendWrappedWith(etH, &et); pErr != nil {
				t.Fatalf("unexpected error: %s", pErr)
			}

			// Verify txn record is cleaned.
			var readTxn roachpb.Transaction
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			ok, err := engine.MVCCGetProto(context.Background(), tc.repl.store.Engine(), txnKey, hlc.Timestamp{}, true /* consistent */, nil /* txn */, &readTxn)
			if err != nil || ok {
				t.Errorf("expected transaction record to be cleared (%t): %s", ok, err)
			}

			// Now replay begin & put. BeginTransaction should fail with a
			// TransactionAbortedError.
			_, pErr = tc.Sender().Send(context.Background(), ba)
			expErr := "TransactionAbortedError(ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY)"
			if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
				t.Errorf("expected %s; got %v", expErr, pErr)
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

			// EndTransaction should fail with a txn not found error.
			_, pErr = tc.SendWrappedWith(etH, &et)
			if tse, ok := pErr.GetDetail().(*roachpb.TransactionStatusError); !ok ||
				tse.Reason != roachpb.TransactionStatusError_REASON_TXN_NOT_FOUND {
				t.Fatalf("expected TransactionStatusError with REASON_TXN_NOT_FOUND, found %v", pErr)
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}
		putKey = putKey.Next() // for the next iteration
		args, h := endTxnArgs(txn, true)
		args.IntentSpans = test.intents
		assignSeqNumsForReqs(txn, &args)
		if _, pErr := tc.SendWrappedWith(h, &args); pErr != nil {
			t.Fatal(pErr)
		}
		var readTxn roachpb.Transaction
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
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
	// This increment is not required, but testing feels safer when zero
	// values are unexpected.
	txn.Epoch++
	pArgs := putArgs(key, []byte("value"))
	h := roachpb.Header{Txn: txn}
	assignSeqNumsForReqs(txn, &pArgs)
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
		assignSeqNumsForReqs(txn, &pArgs)
		if _, pErr := newRepl.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// End the transaction and resolve the intents.
	args, h := endTxnArgs(txn, commit)
	args.IntentSpans = []roachpb.Span{{Key: key}, {Key: splitKey.AsRawKey()}}
	assignSeqNumsForReqs(txn, &args)
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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
// AbortSpan records are purged on both the local range and non-local range.
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

			ctx := logtags.AddTag(context.Background(), "testcase", i)

			rightRepl, txn := setupResolutionTest(t, tc, testKey, splitKey, false /* generate AbortSpan entry */)

			testutils.SucceedsSoon(t, func() error {
				var gr roachpb.GetResponse
				if _, err := batcheval.Get(
					ctx, tc.engine, batcheval.CommandArgs{
						Args: &roachpb.GetRequest{RequestHeader: roachpb.RequestHeader{
							Key: keys.TransactionKey(txn.Key, txn.ID),
						}},
					},
					&gr,
				); err != nil {
					return err
				} else if gr.Value != nil {
					return errors.Errorf("%d: txn entry still there: %+v", i, gr)
				}

				var entry roachpb.AbortSpanEntry
				if aborted, err := tc.repl.abortSpan.Get(ctx, tc.engine, txn.ID, &entry); err != nil {
					t.Fatal(err)
				} else if aborted {
					return errors.Errorf("%d: AbortSpan still populated: %v", i, entry)
				}
				if aborted, err := rightRepl.abortSpan.Get(ctx, tc.engine, txn.ID, &entry); err != nil {
					t.Fatal(err)
				} else if aborted {
					t.Fatalf("%d: right-hand side AbortSpan still populated: %v", i, entry)
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				atomic.AddInt64(&count, 1)
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			} else if filterArgs.Req.Method() == roachpb.GC {
				// Can't fatal since we're on a goroutine. This'll do it.
				t.Error(errors.Errorf("unexpected GCRequest: %+v", filterArgs.Req))
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

			var entry roachpb.AbortSpanEntry
			if aborted, err := tc.repl.abortSpan.Get(context.Background(), tc.engine, txn.ID, &entry); err != nil {
				t.Fatal(err)
			} else if aborted {
				t.Fatalf("commit=%t: AbortSpan still populated: %v", commit, entry)
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

	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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
			assignSeqNumsForReqs(txn, &bt, &put, &et)

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
	assignSeqNumsForReqs(txn, &bt, &put)
	if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
		t.Fatalf("unexpected error beginning txn: %s", pErr)
	}

	et, etH := endTxnArgs(txn, true)
	et.Require1PC = true
	ba = roachpb.BatchRequest{}
	ba.Header = etH
	ba.Add(&et)
	assignSeqNumsForReqs(txn, &et)
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
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
		}}, ResolveOptions{Wait: false, Poison: true /* irrelevant */}); pErr != nil {
		t.Fatal(pErr)
	}
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt32(&seen) > 0 {
			return nil
		}
		return fmt.Errorf("no intent resolution on %q so far", key)
	})
}

// TestAbortSpanPoisonOnResolve verifies that when an intent is
// aborted, the AbortSpan on the respective Range is poisoned and
// the pushee is presented with a txn abort on its next contact with
// the Range in the same epoch.
func TestAbortSpanPoisonOnResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := roachpb.Key("a")

	// Isolation of the pushee and whether we're going to abort it.
	// Run the actual meat of the test, which pushes the pushee and
	// checks whether we get the correct behavior as it touches the
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
			incArgs := &roachpb.IncrementRequest{
				RequestHeader: roachpb.RequestHeader{Key: k}, Increment: 123,
			}
			assignSeqNumsForReqs(actor, incArgs)
			reply, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.store, roachpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, incArgs)
			if pErr != nil {
				return nil, pErr
			}
			actor.Writing = true
			return reply.(*roachpb.IncrementResponse), nil
		}

		get := func(actor *roachpb.Transaction, k roachpb.Key) *roachpb.Error {
			gArgs := getArgs(k)
			assignSeqNumsForReqs(actor, &gArgs)
			_, pErr := client.SendWrappedWith(context.Background(), tc.store, roachpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, &gArgs)
			return pErr
		}

		// Write an intent (this also begins the pushee's transaction).
		if _, pErr := inc(pushee, key); pErr != nil {
			t.Fatal(pErr)
		}

		// Have the pusher run into the intent. That pushes our pushee and
		// resolves the intent, which in turn should poison the AbortSpan.
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

// TestAbortSpanError verifies that roachpb.Errors returned by checkIfTxnAborted
// have txns that are identical to txns stored in Transaction{Retry,Aborted}Error.
func TestAbortSpanError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	txn := roachpb.Transaction{}
	txn.ID = uuid.MakeV4()
	txn.Priority = 1
	txn.Sequence = 1
	txn.Timestamp = tc.Clock().Now().Add(1, 0)

	key := roachpb.Key("k")
	ts := txn.Timestamp.Next()
	priority := int32(10)
	entry := roachpb.AbortSpanEntry{
		Key:       key,
		Timestamp: ts,
		Priority:  priority,
	}
	if err := tc.repl.abortSpan.Put(context.Background(), tc.engine, nil, txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	rec := &SpanSetReplicaEvalContext{tc.repl, allSpans}
	pErr := checkIfTxnAborted(context.Background(), rec, tc.engine, txn)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); ok {
		expected := txn.Clone()
		expected.Timestamp = txn.Timestamp
		expected.Priority = priority
		expected.Status = roachpb.ABORTED
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
		assignSeqNumsForReqs(pushee, &put)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
			t.Fatal(pErr)
		}
		// End the pushee's transaction.
		etArgs, h := endTxnArgs(pushee, status == roachpb.COMMITTED)
		assignSeqNumsForReqs(pushee, &etArgs)
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
		assignSeqNumsForReqs(pushee, &put)
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

		if !reflect.DeepEqual(expTxn, reply.PusheeTxn) {
			t.Fatalf("unexpected push txn in trial %d: %s", i, pretty.Diff(expTxn, reply.PusheeTxn))
		}
	}
}

// TestPushTxnQueryPusheerHasNewerVersion verifies that PushTxn
// uses the newer version of the pushee in a push request.
func TestPushTxnQueryPusheeHasNewerVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.Key("key")
	pushee := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pushee.Priority = 1
	pushee.Epoch = 12345
	pushee.Sequence = 2
	ts := tc.Clock().Now()
	pushee.Timestamp = ts
	pushee.LastHeartbeat = ts

	pusher := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())
	pusher.Priority = 2

	_, btH := beginTxnArgs(key, pushee)
	put := putArgs(key, key)
	assignSeqNumsForReqs(pushee, &put)
	if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Make sure the pushee in the request has updated information on the pushee.
	// Since the pushee has higher priority than the pusher, the push should fail.
	pushee.Priority = 4
	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)

	_, pErr := tc.SendWrapped(&args)
	if pErr == nil {
		t.Fatalf("unexpected push success")
	}
	if _, ok := pErr.GetDetail().(*roachpb.TransactionPushError); !ok {
		t.Errorf("expected txn push error: %s", pErr)
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
		{ts, ns*2 + 1, roachpb.PUSH_TIMESTAMP, true},
		{ts, ns*2 + 1, roachpb.PUSH_ABORT, true},
		{ts, ns*2 + 1, roachpb.PUSH_TOUCH, true},
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
		btH.Timestamp = pushee.Timestamp
		put := putArgs(key, key)
		assignSeqNumsForReqs(pushee, &put)
		if _, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put); pErr != nil {
			t.Fatalf("%d: %s", i, pErr)
		}

		// Now, attempt to push the transaction with Now set to the txn start time + offset.
		args := pushTxnArgs(pusher, pushee, test.pushType)
		args.Now = pushee.Timestamp.Add(test.timeOffset, 0)
		args.PushTo = args.Now

		reply, pErr := tc.SendWrapped(&args)

		if test.expSuccess != (pErr == nil) {
			t.Fatalf("%d: expSuccess=%t; got pErr %s, args=%+v, reply=%+v", i,
				test.expSuccess, pErr, args, reply)
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
	if _, err := batcheval.PushTxn(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: roachpb.Header{Txn: txn}, Args: &pa}, &roachpb.PushTxnResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional PushTxn returned unexpected error: %v", err)
	}
	if _, err := batcheval.ResolveIntent(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: roachpb.Header{Txn: txn}, Args: &ra}, &roachpb.ResolveIntentResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntent returned unexpected error: %v", err)
	}
	if _, err := batcheval.ResolveIntentRange(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: roachpb.Header{Txn: txn}, Args: &rra}, &roachpb.ResolveIntentRangeResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntentRange returned unexpected error: %v", err)
	}

	// Should not get a transaction back from PushTxn. It used to erroneously
	// return args.PusherTxn.
	var reply roachpb.PushTxnResponse
	if _, err := batcheval.PushTxn(ctx, b, batcheval.CommandArgs{Stats: &ms, Args: &pa}, &reply); err != nil {
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
		assignSeqNumsForReqs(pushee, &put)
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
	assignSeqNumsForReqs(pushee, &put)
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
	assignSeqNumsForReqs(pushee, &put)
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
	assignSeqNumsForReqs(pushee, &put)
	resp, pErr := maybeWrapWithBeginTransaction(context.Background(), tc.Sender(), btH, &put)
	if pErr != nil {
		t.Fatal(pErr)
	}
	pushee.Update(resp.Header().Txn)

	// Try to end the pushee's transaction; should get a retry failure.
	etArgs, h := endTxnArgs(pushee, true /* commit */)
	assignSeqNumsForReqs(pushee, &etArgs)
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
	ba.Add(&btArgs)
	ba.Add(&put)
	ba.Add(&etArgs)
	ba.Header.Txn = pushee
	assignSeqNumsForReqs(pushee, &btArgs, &put, &etArgs)
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

// TestQueryIntentRequest tests the different behaviors of QueryIntent requests.
func TestQueryIntentRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, iso := range []enginepb.IsolationType{enginepb.SNAPSHOT, enginepb.SERIALIZABLE} {
		t.Run(iso.String(), func(t *testing.T) {
			for _, behavior := range []roachpb.QueryIntentRequest_IfMissingBehavior{
				roachpb.QueryIntentRequest_DO_NOTHING,
				roachpb.QueryIntentRequest_RETURN_ERROR,
				roachpb.QueryIntentRequest_PREVENT,
			} {
				if iso == enginepb.SNAPSHOT && behavior == roachpb.QueryIntentRequest_PREVENT {
					// Cannot prevent SNAPSHOT transaction with QueryIntent.
					continue
				}
				t.Run(fmt.Sprintf("behavior=%s", behavior), func(t *testing.T) {

					tc := testContext{}
					stopper := stop.NewStopper()
					defer stopper.Stop(context.TODO())
					tc.Start(t, stopper)

					key1 := roachpb.Key("a")
					key2 := roachpb.Key("b")
					txn := newTransaction("test", key1, 1, iso, tc.Clock())
					txn2 := newTransaction("test2", key2, 1, iso, tc.Clock())

					pArgs := putArgs(key1, []byte("value1"))
					assignSeqNumsForReqs(txn, &pArgs)
					if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
						t.Fatal(pErr)
					}

					queryIntent := func(
						key []byte,
						txnMeta enginepb.TxnMeta,
						baTxn *roachpb.Transaction,
						expectIntent bool,
					) {
						t.Helper()
						qiArgs := queryIntentArgs(key, txnMeta, behavior)
						qiRes, pErr := tc.SendWrappedWith(roachpb.Header{Txn: baTxn}, &qiArgs)
						if behavior == roachpb.QueryIntentRequest_RETURN_ERROR && !expectIntent {
							ownIntent := baTxn != nil && baTxn.ID == txnMeta.ID
							if ownIntent && txnMeta.Timestamp.Less(txn.Timestamp) {
								if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
									t.Fatalf("expected TransactionRetryError, found %v %v", txnMeta, pErr)
								}
							} else {
								if _, ok := pErr.GetDetail().(*roachpb.IntentMissingError); !ok {
									t.Fatalf("expected IntentMissingError, found %v", pErr)
								}
							}
						} else {
							if pErr != nil {
								t.Fatal(pErr)
							}
							if e, a := expectIntent, qiRes.(*roachpb.QueryIntentResponse).FoundIntent; e != a {
								t.Fatalf("expected FoundIntent=%t but FoundIntent=%t", e, a)
							}
						}
					}

					for _, baTxn := range []*roachpb.Transaction{nil, txn, txn2} {
						// Query the intent with the correct txn meta. Should see intent regardless
						// of whether we're inside the txn or not.
						queryIntent(key1, txn.TxnMeta, baTxn, true)

						// Query an intent on a different key for the same transaction. Should not
						// see an intent.
						queryIntent(key2, txn.TxnMeta, baTxn, false)

						// Query an intent on the same key for a different transaction. Should not
						// see an intent.
						diffIDMeta := txn.TxnMeta
						diffIDMeta.ID = txn2.ID
						queryIntent(key1, diffIDMeta, baTxn, false)

						// Query the intent with a larger epoch. Should not see an intent.
						largerEpochMeta := txn.TxnMeta
						largerEpochMeta.Epoch++
						queryIntent(key1, largerEpochMeta, baTxn, false)

						// Query the intent with a smaller epoch. Should not see an intent.
						smallerEpochMeta := txn.TxnMeta
						smallerEpochMeta.Epoch--
						queryIntent(key1, smallerEpochMeta, baTxn, false)

						// Query the intent with a larger timestamp. Should see an intent.
						// See the comment on QueryIntentRequest.Txn for an explanation of why
						// the request behaves like this.
						largerTSMeta := txn.TxnMeta
						largerTSMeta.Timestamp = largerTSMeta.Timestamp.Next()
						queryIntent(key1, largerTSMeta, baTxn, true)

						// Query the intent with a smaller timestamp. Should not see an intent
						// if transaction is serializable. Should see an intent if the transaction
						// is SNAPSHOT.
						smallerTSMeta := txn.TxnMeta
						smallerTSMeta.Timestamp = smallerTSMeta.Timestamp.Prev()
						queryIntent(key1, smallerTSMeta, baTxn, iso == enginepb.SNAPSHOT)

						// Query the intent with a larger sequence number. Should not see an intent.
						largerSeqMeta := txn.TxnMeta
						largerSeqMeta.Sequence++
						queryIntent(key1, largerSeqMeta, baTxn, false)

						// Query the intent with a smaller sequence number. Should see an intent.
						// See the comment on QueryIntentRequest.Txn for an explanation of why
						// the request behaves like this.
						smallerSeqMeta := txn.TxnMeta
						smallerSeqMeta.Sequence--
						queryIntent(key1, smallerSeqMeta, baTxn, true)
					}

					// Perform a write at key2. Depending on the behavior of the queryIntent
					// that queried that key, this write should have different results.
					pArgs2 := putArgs(key2, []byte("value2"))
					assignSeqNumsForReqs(txn, &pArgs2)
					ba := roachpb.BatchRequest{}
					ba.Header = roachpb.Header{Txn: txn}
					ba.Add(&pArgs2)
					br, pErr := tc.Sender().Send(context.Background(), ba)
					if pErr != nil {
						t.Fatal(pErr)
					}
					tsBumped := br.Txn.Timestamp != br.Txn.OrigTimestamp
					if behavior == roachpb.QueryIntentRequest_PREVENT {
						if !tsBumped {
							t.Fatalf("transaction timestamp not bumped: %v", br.Txn)
						}
					} else {
						if tsBumped {
							t.Fatalf("unexpected transaction timestamp bumped: %v", br.Txn)
						}
					}
				})
			}
		})
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
		assignSeqNumsForReqs(txn, &pArgs)
		if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Resolve the intents.
	rArgs := &roachpb.ResolveIntentRangeRequest{
		RequestHeader: roachpb.RequestHeader{
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
	ms, err := stateloader.Make(nil /* st */, rangeID).LoadMVCCStats(context.Background(), eng)
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
		SysBytes: 24,
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
	txn.ID = uuid

	assignSeqNumsForReqs(txn, &pArgs)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes:   95,
		KeyBytes:    28,
		ValBytes:    67,
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
		RequestHeader: roachpb.RequestHeader{
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
	entries, err := tc.repl.raftEntriesLocked(indexes[5], indexes[9], math.MaxUint64)
	tc.repl.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != int(indexes[9]-indexes[5]) {
		t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
	}

	// But any range that includes the truncated entries returns an error.
	tc.repl.mu.Lock()
	_, err = tc.repl.raftEntriesLocked(indexes[4], indexes[9], math.MaxUint64)
	tc.repl.mu.Unlock()
	if err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// The term of the last truncated entry is still available.
	tc.repl.mu.Lock()
	term, err := tc.repl.raftTermRLocked(indexes[4])
	tc.repl.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if term == 0 {
		t.Errorf("invalid term 0 for truncated entry")
	}

	// The terms of older entries are gone.
	tc.repl.mu.Lock()
	_, err = tc.repl.raftTermRLocked(indexes[3])
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
	term, err = tc.repl.raftTermRLocked(indexes[4])
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
		RequestHeader: roachpb.RequestHeader{
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
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs storagebase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Header().Key.Equal(roachpb.Key("boom")) {
				return roachpb.NewError(roachpb.NewReplicaCorruptionError(errors.New("boom")))
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
	r := tc.store.LookupReplica(rkey)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.destroyStatus.err.Error() != pErr.GetDetail().Error() {
		t.Fatalf("expected r.mu.destroyed == pErr.GetDetail(), instead %q != %q", r.mu.destroyStatus.err, pErr.GetDetail())
	}

	// Verify destroyed error was persisted.
	pErr, err = r.mu.stateLoader.LoadReplicaDestroyedError(context.Background(), r.store.Engine())
	if err != nil {
		t.Fatal(err)
	}
	if r.mu.destroyStatus.err.Error() != pErr.GetDetail().Error() {
		t.Fatalf("expected r.mu.destroyed == pErr.GetDetail(), instead %q != %q", r.mu.destroyStatus.err, pErr.GetDetail())
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
		storagepb.ReasonRebalance,
		"",
	); err == nil || !strings.Contains(err.Error(), "node already has a replica") {
		t.Fatalf("must not be able to add second replica to same node (err=%s)", err)
	}
}

// TestReplicaDanglingMetaIntent creates a dangling intent on a meta2
// record and verifies that RangeLookup scans behave
// appropriately. Normally, the old value and a write intent error
// should be returned. If IgnoreIntents is specified, then a random
// choice of old or new is returned with no error.
// TODO(tschottdorf): add a test in which there is a dangling intent on a
// descriptor we would've otherwise discarded in a reverse scan; verify that
// we don't erroneously return that descriptor (recently fixed bug).
func TestReplicaDanglingMetaIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		tc := testContext{}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(t, stopper)

		key := roachpb.Key("a")

		// Get original meta2 descriptor.
		rs, _, err := client.RangeLookup(ctx, tc.Sender(), key, roachpb.READ_UNCOMMITTED, 0, reverse)
		if err != nil {
			t.Fatal(err)
		}
		origDesc := rs[0]

		newDesc := origDesc
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
		pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(key)).AsRawKey(), data)
		assignSeqNumsForReqs(txn, &pArgs)
		if _, pErr := maybeWrapWithBeginTransaction(ctx, tc.Sender(), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Now lookup the range; should get the value. Since the lookup is
		// not consistent, there's no WriteIntentError. It should return both
		// the committed descriptor and the intent descriptor.
		//
		// Note that 'A' < 'a'.
		newKey := roachpb.Key{'A'}
		rs, _, err = client.RangeLookup(ctx, tc.Sender(), newKey, roachpb.READ_UNCOMMITTED, 0, reverse)
		if err != nil {
			t.Fatal(err)
		}
		if len(rs) != 2 {
			t.Fatalf("expected 2 matching range descriptors, found %v", rs)
		}
		if desc := rs[0]; !reflect.DeepEqual(desc, origDesc) {
			t.Errorf("expected original descriptor %s; got %s", &origDesc, &desc)
		}
		if intentDesc := rs[1]; !reflect.DeepEqual(intentDesc, newDesc) {
			t.Errorf("expected original descriptor %s; got %s", &newDesc, &intentDesc)
		}

		// Switch to consistent lookups, which should run into the intent.
		_, _, err = client.RangeLookup(ctx, tc.Sender(), newKey, roachpb.CONSISTENT, 0, reverse)
		if _, ok := err.(*roachpb.WriteIntentError); !ok {
			t.Fatalf("expected WriteIntentError, not %s", err)
		}
	})
}

// TestReplicaLookupUseReverseScan verifies the correctness of the results which are retrieved
// from RangeLookup by using ReverseScan.
func TestReplicaLookupUseReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
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
			pArgs := putArgs(keys.RangeMetaKey(r.EndKey).AsRawKey(), data)
			assignSeqNumsForReqs(txn, &pArgs)

			if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Resolve the intents.
		rArgs := &roachpb.ResolveIntentRangeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key:    keys.RangeMetaKey(roachpb.RKey("a")).AsRawKey(),
				EndKey: keys.RangeMetaKey(roachpb.RKey("z")).AsRawKey(),
			},
			IntentTxn: txn.TxnMeta,
			Status:    roachpb.COMMITTED,
		}
		if _, pErr := tc.SendWrapped(rArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Test reverse RangeLookup scan without intents.
	for _, c := range testCases {
		rs, _, err := client.RangeLookup(ctx, tc.Sender(), roachpb.Key(c.key),
			roachpb.READ_UNCOMMITTED, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		seen := rs[0]
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
		pArgs := putArgs(keys.RangeMetaKey(r.EndKey).AsRawKey(), data)
		assignSeqNumsForReqs(txn, &pArgs)

		if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Test reverse RangeLookup scan with intents.
	for _, c := range testCases {
		rs, _, err := client.RangeLookup(ctx, tc.Sender(), roachpb.Key(c.key),
			roachpb.READ_UNCOMMITTED, 0, true)
		if err != nil {
			t.Fatal(err)
		}
		seen := rs[0]
		if !(seen.StartKey.Equal(c.expected.StartKey) && seen.EndKey.Equal(c.expected.EndKey)) {
			t.Errorf("expected descriptor %s; got %s", &c.expected, &seen)
		}
	}
}

func TestRangeLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
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
		{key: roachpb.RKey(keys.Meta1Prefix), reverse: false, expected: expected},
		// Test with the last key in a meta prefix. This is an edge case in the
		// implementation.
		{key: roachpb.RKey(keys.Meta2KeyMax), reverse: false, expected: expected},
		{key: roachpb.RKey(roachpb.KeyMax), reverse: false, expected: nil},
		{key: roachpb.RKey(keys.Meta2KeyMax), reverse: true, expected: expected},
		{key: roachpb.RKey(roachpb.KeyMax), reverse: true, expected: expected},
	}

	for i, c := range testCases {
		rs, _, err := client.RangeLookup(ctx, tc.Sender(), c.key.AsRawKey(),
			roachpb.CONSISTENT, 0, c.reverse)
		if err != nil {
			if c.expected != nil {
				t.Fatal(err)
			}
		} else {
			if !reflect.DeepEqual(rs, c.expected) {
				t.Errorf("%d: expected %+v, got %+v", i, c.expected, rs)
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
		in, out := storagebase.IntersectSpan(tc.intent, roachpb.RangeDescriptor{
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
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("k")},
		Value:         roachpb.MakeValueFromString("not nil"),
	})
	// This one fails with a ConditionalPutError, which will populate the
	// returned error's index.
	ba.Add(&roachpb.ConditionalPutRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("k")},
		Value:         roachpb.MakeValueFromString("irrelevant"),
		ExpValue:      nil, // not true after above Put
	})
	// This one is never executed.
	ba.Add(&roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("k")},
	})

	if _, pErr := tc.Sender().Send(context.Background(), ba); pErr == nil {
		t.Fatal("expected an error")
	} else if pErr.Index == nil || pErr.Index.Index != 1 || !testutils.IsPError(pErr, "unexpected value") {
		t.Fatalf("invalid index or error type: %s", pErr)
	}
}

func TestProposalOverhead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var overhead uint32

	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.TestingProposalFilter =
		func(args storagebase.ProposalFilterArgs) *roachpb.Error {
			for _, union := range args.Req.Requests {
				if union.GetInner().Method() == roachpb.Put {
					atomic.StoreUint32(&overhead, uint32(args.Cmd.Size()-args.Cmd.WriteBatch.Size()))
					break
				}
			}
			return nil
		}
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, cfg)

	ba := roachpb.BatchRequest{}
	ba.Add(&roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("k")},
		Value:         roachpb.MakeValueFromString("v"),
	})
	if _, pErr := tc.Sender().Send(context.Background(), ba); pErr != nil {
		t.Fatal(pErr)
	}

	// NB: the expected overhead reflects the space overhead currently present in
	// Raft commands. This test will fail if that overhead changes. Try to make
	// this number go down and not up.
	const expectedOverhead = 52
	if v := atomic.LoadUint32(&overhead); expectedOverhead != v {
		t.Fatalf("expected overhead of %d, but found %d", expectedOverhead, v)
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
	repl := tc.store.LookupReplica(scStartSddr)
	if repl == nil {
		t.Fatalf("no replica contains the SystemConfig span")
	}

	// Create a transaction and write an intent to the system
	// config span.
	key := keys.SystemConfigSpan.Key
	_, btH := beginTxnArgs(key, newTransaction("test", key, 1, enginepb.SERIALIZABLE, repl.store.Clock()))
	btH.Txn.Priority = roachpb.MinTxnPriority // low so it can be pushed
	put := putArgs(key, []byte("foo"))
	assignSeqNumsForReqs(btH.Txn, &put)
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
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
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

	repl.setDesc(ctx, newDesc)
	expectedErr := "replica descriptor's ID has changed"
	if err := tc.store.removeReplicaImpl(ctx, tc.repl, origDesc.NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); !testutils.IsError(err, expectedErr) {
		t.Fatalf("expected error %q but got %v", expectedErr, err)
	}

	// Now try a fresh descriptor and succeed.
	if err := tc.store.removeReplicaImpl(ctx, tc.repl, repl.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Fatal(err)
	}

	iter := rditer.NewReplicaDataIterator(tc.repl.Desc(), tc.repl.store.Engine(), false /* replicatedOnly */)
	defer iter.Close()
	if ok, err := iter.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		// If the range is destroyed, only a tombstone key should be there.
		k1 := iter.Key().Key
		if tombstoneKey := keys.RaftTombstoneKey(tc.repl.RangeID); !bytes.Equal(k1, tombstoneKey) {
			t.Errorf("expected a tombstone key %q, but found %q", tombstoneKey, k1)
		}

		iter.Next()
		if ok, err := iter.Valid(); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Errorf("expected a destroyed replica to have only a tombstone key, but found more")
		}
	} else {
		t.Errorf("expected a tombstone key, but got an empty iteration")
	}
}

// TestQuotaPoolAccessOnDestroyedReplica tests the occurrence of #17303 where
// following a leader replica getting destroyed, the scheduling of
// handleRaftReady twice on the replica would cause a panic when
// finding a nil/closed quota pool.
func TestQuotaPoolAccessOnDestroyedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	repl, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	if err := tc.store.removeReplicaImpl(context.TODO(), repl, repl.Desc().NextReplicaID, RemoveOptions{
		DestroyData: true,
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := repl.handleRaftReady(noSnap); err != nil {
		t.Fatal(err)
	}

	if _, _, err := repl.handleRaftReady(noSnap); err != nil {
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
		// Case 6: Get range without size limitation. (Like case 4, without truncating).
		{lo: indexes[5], hi: indexes[9], expResultCount: 4, expCacheCount: 4, setup: nil},
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
		if tc.maxBytes == 0 {
			tc.maxBytes = math.MaxUint64
		}
		cacheEntries, _, _, hitLimit := repl.store.raftEntryCache.getEntries(nil, rangeID, tc.lo, tc.hi, tc.maxBytes)
		if len(cacheEntries) != tc.expCacheCount {
			t.Errorf("%d: expected cache count %d, got %d", i, tc.expCacheCount, len(cacheEntries))
		}
		repl.mu.Lock()
		ents, err := repl.raftEntriesLocked(tc.lo, tc.hi, tc.maxBytes)
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
		} else if tc.expResultCount > 0 {
			expHitLimit := ents[len(ents)-1].Index < tc.hi-1
			if hitLimit != expHitLimit {
				t.Errorf("%d: unexpected hit limit: %t", i, hitLimit)
			}
		}
	}

	// Case 23: Lo must be less than or equal to hi.
	repl.mu.Lock()
	if _, err := repl.raftEntriesLocked(indexes[9], indexes[5], math.MaxUint64); err == nil {
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
	if _, err := repl.raftEntriesLocked(indexes[5], indexes[9], math.MaxUint64); err == nil {
		t.Errorf("24: error expected, got none")
	}

	// Case 25a: don't hit the gap due to maxBytes, cache populated.
	{
		ents, err := repl.raftEntriesLocked(indexes[5], indexes[9], 1)
		if err != nil {
			t.Errorf("25: expected no error, got %s", err)
		}
		if len(ents) != 1 {
			t.Errorf("25: expected 1 entry, got %d", len(ents))
		}
	}
	// Case 25b: don't hit the gap due to maxBytes, cache cleared.
	{
		repl.store.raftEntryCache.delEntries(rangeID, indexes[5], indexes[5]+1)
		ents, err := repl.raftEntriesLocked(indexes[5], indexes[9], 1)
		if err != nil {
			t.Errorf("25: expected no error, got %s", err)
		}
		if len(ents) != 1 {
			t.Errorf("25: expected 1 entry, got %d", len(ents))
		}
	}

	// Case 26: don't hit the gap due to truncation.
	if _, err := repl.raftEntriesLocked(indexes[4], indexes[9], math.MaxUint64); err != raft.ErrCompacted {
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

	firstIndex, err := repl.raftFirstIndexLocked()
	if err != nil {
		t.Fatal(err)
	}
	if firstIndex != indexes[5] {
		t.Fatalf("expected firstIndex %d to be %d", firstIndex, indexes[4])
	}

	// Truncated logs should return an ErrCompacted error.
	if _, err := tc.repl.raftTermRLocked(indexes[1]); err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}
	if _, err := tc.repl.raftTermRLocked(indexes[3]); err != raft.ErrCompacted {
		t.Errorf("expected ErrCompacted, got %s", err)
	}

	// FirstIndex-1 should return the term of firstIndex.
	firstIndexTerm, err := tc.repl.raftTermRLocked(firstIndex)
	if err != nil {
		t.Errorf("expect no error, got %s", err)
	}

	term, err := tc.repl.raftTermRLocked(indexes[4])
	if err != nil {
		t.Errorf("expect no error, got %s", err)
	}
	if term != firstIndexTerm {
		t.Errorf("expected firstIndex-1's term:%d to equal that of firstIndex:%d", term, firstIndexTerm)
	}

	lastIndex, err := repl.raftLastIndexLocked()
	if err != nil {
		t.Fatal(err)
	}

	// Last index should return correctly.
	if _, err := tc.repl.raftTermRLocked(lastIndex); err != nil {
		t.Errorf("expected no error, got %s", err)
	}

	// Terms for after the last index should return ErrUnavailable.
	if _, err := tc.repl.raftTermRLocked(lastIndex + 1); err != raft.ErrUnavailable {
		t.Errorf("expected ErrUnavailable, got %s", err)
	}
	if _, err := tc.repl.raftTermRLocked(indexes[9] + 1000); err != raft.ErrUnavailable {
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
			defer cancel()
			cfg := TestStoreConfig(nil)
			if !cancelEarly {
				cfg.TestingKnobs.TestingProposalFilter =
					func(args storagebase.ProposalFilterArgs) *roachpb.Error {
						for _, union := range args.Req.Requests {
							if union.GetInner().Header().Key.Equal(key) {
								cancel()
								break
							}
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
				RequestHeader: roachpb.RequestHeader{Key: key},
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
					// We canceled the context while the command was already
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

// TestReplicaTryAbandon checks that canceling a request that has been
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
	var proposed int32
	tc.repl.mu.Lock()
	tc.repl.mu.submitProposalFn = func(result *ProposalData) error {
		if atomic.AddInt32(&proposed, 1) != 1 {
			// We only need to propose the command once for this test. Worse, a
			// second proposal will cause the test to deadlock. How do we get a
			// second proposal? reasonNewLeaderOrConfigChange ->
			// refreshProposalsLocked(). Note that this form of refresh cannot
			// currently be disabled via a testing knob.
			return nil
		}

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
		RequestHeader: roachpb.RequestHeader{Key: []byte("acdfg")},
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
		if len(tc.repl.mu.localProposals) == 0 {
			t.Fatal("expected non-empty proposals map")
		}
	}()

	func() {
		tc.repl.cmdQMu.Lock()
		defer tc.repl.cmdQMu.Unlock()
		if s := tc.repl.cmdQMu.queues[spanset.SpanGlobal].String(); s == "" {
			t.Fatal("expected non-empty command queue")
		}
	}()

	// Allow the proposal to go through.
	close(proposalCh)
	if err := <-proposalErrCh; err != nil {
		t.Fatal(err)
	}

	// Even though we canceled the command it will still get executed and the
	// command queue cleaned up.
	testutils.SucceedsSoon(t, func() error {
		tc.repl.cmdQMu.Lock()
		defer tc.repl.cmdQMu.Unlock()
		if s := tc.repl.cmdQMu.queues[spanset.SpanGlobal].String(); s != "" {
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

	if pct, _ := batcheval.ComputeChecksum(context.TODO(), nil,
		batcheval.CommandArgs{Args: &roachpb.ComputeChecksumRequest{
			Version: batcheval.ReplicaChecksumVersion,
		}}, &roachpb.ComputeChecksumResponse{},
	); pct.Replicated.ComputeChecksum == nil {
		t.Error("right checksum version: expected post-commit trigger")
	}

	if pct, _ := batcheval.ComputeChecksum(context.TODO(), nil,
		batcheval.CommandArgs{Args: &roachpb.ComputeChecksumRequest{
			Version: batcheval.ReplicaChecksumVersion + 1,
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
		{roachpb.NewReplicaCorruptionError(errors.New("")), "replica corruption (processed=false)"},
		{roachpb.NewReplicaCorruptionError(errors.New("foo")), "replica corruption (processed=false): foo"},
		{roachpb.NewReplicaCorruptionError(errors.Wrap(errors.New("bar"), "foo")), "replica corruption (processed=false): foo: bar"},
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
-    ts:1970-01-01 00:00:00.000001729 +0000 UTC
-    value:"foo"
-    raw mvcc_key/value: 610000000000000006c1000000010d 666f6f
+0.000001729,1 "ab"
+    ts:1970-01-01 00:00:00.000001729 +0000 UTC
+    value:"foo"
+    raw mvcc_key/value: 61620000000000000006c1000000010d 666f6f
-0.000001729,1 "abcd"
-    ts:1970-01-01 00:00:00.000001729 +0000 UTC
-    value:"foo"
-    raw mvcc_key/value: 616263640000000000000006c1000000010d 666f6f
+0.000001729,1 "abcdef"
+    ts:1970-01-01 00:00:00.000001729 +0000 UTC
+    value:"foo"
+    raw mvcc_key/value: 6162636465660000000000000006c1000000010d 666f6f
+0.000000000,0 "foo"
+    ts:<zero>
+    value:"foo"
+    raw mvcc_key/value: 666f6f00 666f6f
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
	snap, err := tc.repl.raftSnapshotLocked()
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
	lease := *repl.mu.state.Lease
	repl.mu.Unlock()

	// Add a command to the pending list.
	magicTS := tc.Clock().Now()
	ba := roachpb.BatchRequest{}
	ba.Timestamp = magicTS
	ba.Add(&roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: roachpb.Key("a"),
		},
		Value: roachpb.MakeValueFromBytes([]byte("val")),
	})
	_, _, _, err := repl.propose(context.Background(), lease, ba, nil, &allSpans)
	if err != nil {
		t.Fatal(err)
	}

	// Set the raft command handler so we can tell if the command has been
	// re-proposed.
	commandProposed := make(chan struct{}, 1)
	repl.mu.Lock()
	repl.mu.submitProposalFn = func(p *ProposalData) error {
		if p.Request.Timestamp == magicTS {
			select {
			case commandProposed <- struct{}{}:
			default:
			}
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

	// Test LeaseRequest since it's special: MaxLeaseIndex plays no role and so
	// there is no re-evaluation of the request.
	atomic.StoreInt32(&c, 0)
	{
		prevLease, _ := tc.repl.GetLease()
		ba := ba
		ba.Requests = nil

		lease := prevLease
		lease.Sequence = 0

		ba.Add(&roachpb.RequestLeaseRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: tc.repl.Desc().StartKey.AsRawKey(),
			},
			Lease:     lease,
			PrevLease: prevLease,
		})
		_, pErr := tc.repl.executeWriteBatch(
			context.WithValue(ctx, magicKey{}, "foo"),
			ba,
		)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if exp, act := int32(1), atomic.LoadInt32(&c); exp != act {
			t.Fatalf("expected %d proposals, got %d", exp, act)
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

	{
		for i := 0; i < num; i++ {
			var ba roachpb.BatchRequest
			ba.Timestamp = tc.Clock().Now()
			ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(fmt.Sprintf("k%d", i))}})
			lease, _ := repl.GetLease()
			proposal, pErr := repl.requestToProposal(context.Background(), makeIDKey(), ba, nil, &allSpans)
			if pErr != nil {
				t.Fatal(pErr)
			}
			repl.mu.Lock()

			repl.insertProposalLocked(proposal, repDesc, lease)
			// We actually propose the command only if we don't
			// cancel it to simulate the case in which Raft loses
			// the command and it isn't reproposed due to the
			// client abandoning it.
			if rand.Intn(2) == 0 {
				log.Infof(context.Background(), "abandoning command %d", i)
				repl.cleanupFailedProposalLocked(proposal)
			} else if err := repl.submitProposalLocked(proposal); err != nil {
				t.Error(err)
			} else {
				chs = append(chs, proposal.doneCh)
			}
			repl.mu.Unlock()
		}
	}

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
			ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(fmt.Sprintf("k%d", i))}})
			cmd, pErr := tc.repl.requestToProposal(ctx, makeIDKey(), ba, nil, &allSpans)
			if pErr != nil {
				t.Fatal(pErr)
			}

			tc.repl.raftMu.Lock()
			tc.repl.mu.Lock()
			tc.repl.insertProposalLocked(cmd, repDesc, status.Lease)
			chs = append(chs, cmd.doneCh)
			tc.repl.mu.Unlock()
			tc.repl.raftMu.Unlock()
		}

		tc.repl.mu.Lock()
		for _, p := range tc.repl.mu.localProposals {
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
	nonePending := len(tc.repl.mu.localProposals) == 0
	c := int(tc.repl.mu.lastAssignedLeaseIndex) - int(tc.repl.mu.state.LeaseAppliedIndex)
	if nonePending && c > 0 {
		t.Errorf("no pending cmds, but have required index offset %d", c)
	}
	if !nonePending {
		t.Fatalf("still pending commands: %+v", tc.repl.mu.localProposals)
	}
}

// TestReplicaLeaseReproposal verifies that the reproposal logic is aware of the
// fact that for lease requests, MaxLeaseIndex plays no role.
func TestReplicaLeaseReproposal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)
	repl := tc.repl

	repDesc, err := repl.GetReplicaDescriptor()
	if err != nil {
		t.Fatal(err)
	}

	var ba roachpb.BatchRequest
	ba.Requests = nil

	prevLease, _ := tc.repl.GetLease()
	lease := prevLease
	lease.Sequence = 0

	key := roachpb.Key("a")

	for i := 1; i < 10; i++ {
		inc := incrementArgs(key, 1)
		if _, pErr := client.SendWrapped(ctx, tc.Sender(), &inc); pErr != nil {
			t.Fatal(pErr)
		}
	}

	ba.Add(&roachpb.RequestLeaseRequest{
		RequestHeader: roachpb.RequestHeader{Key: key},
		Lease:         lease,
		PrevLease:     prevLease,
	})

	ba.Timestamp = tc.Clock().Now()
	proposal, pErr := repl.requestToProposal(context.Background(), makeIDKey(), ba, nil, &allSpans)
	if pErr != nil {
		t.Fatal(pErr)
	}

	repl.mu.Lock()
	defer repl.mu.Unlock()
	ai := repl.mu.state.LeaseAppliedIndex
	if ai <= 1 {
		// Lease index zero is special. If this assertion ever fires, just add some
		// earlier proposals to the test.
		t.Fatalf("test requires LeaseAppliedIndex >= 2 at this point, have %d", ai)
	}
	// Decrement the MaxLeaseIndex. If refreshProposalsLocked didn't know that
	// MaxLeaseIndex doesn't matter for lease requests, it might be tempted to
	// end the proposal early (since it would assume that it couldn't commit).
	repl.insertProposalLocked(proposal, repDesc, lease)
	proposal.command.MaxLeaseIndex = ai - 1
	repl.refreshProposalsLocked(1 /* delta */, reasonTicks)
	select {
	case res := <-proposal.doneCh:
		t.Fatalf("proposal unexpectedly terminated: %+v", res)
	default:
		// Happy case. The proposal might have been submitted to Raft again (well,
		// for the first time in this test actually), but that's fine.
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

	// Only followers refresh pending commands during tick events. Change the
	// replica that the range thinks is the leader so that the replica thinks
	// it's a follower.
	r.mu.Lock()
	r.mu.leaderID = 2
	r.mu.Unlock()

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
			if _, err := r.tick(nil); err != nil {
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
		ba.Add(&roachpb.PutRequest{RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(id)}})
		lease, _ := r.GetLease()
		cmd, pErr := r.requestToProposal(context.Background(), storagebase.CmdIDKey(id), ba, nil, &allSpans)
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
		for id, p := range r.mu.localProposals {
			m[id] = p.proposedAtTicks
		}
		r.mu.Unlock()

		// Tick raft.
		if _, err := r.tick(nil); err != nil {
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

func TestReplicaShouldDropForwardedProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cmdSeen, cmdNotSeen := makeIDKey(), makeIDKey()
	data, noData := []byte("data"), []byte("")

	testCases := []struct {
		name                string
		leader              bool
		msg                 raftpb.Message
		expDrop             bool
		expRemotePropsAfter int
	}{
		{
			name:   "new proposal",
			leader: true,
			msg: raftpb.Message{
				Type: raftpb.MsgProp,
				Entries: []raftpb.Entry{
					{Type: raftpb.EntryNormal, Data: encodeRaftCommandV1(cmdNotSeen, data)},
				},
			},
			expDrop:             false,
			expRemotePropsAfter: 2,
		},
		{
			name:   "duplicate proposal",
			leader: true,
			msg: raftpb.Message{
				Type: raftpb.MsgProp,
				Entries: []raftpb.Entry{
					{Type: raftpb.EntryNormal, Data: encodeRaftCommandV1(cmdSeen, data)},
				},
			},
			expDrop:             true,
			expRemotePropsAfter: 1,
		},
		{
			name:   "partially new proposal",
			leader: true,
			msg: raftpb.Message{
				Type: raftpb.MsgProp,
				Entries: []raftpb.Entry{
					{Type: raftpb.EntryNormal, Data: encodeRaftCommandV1(cmdNotSeen, data)},
					{Type: raftpb.EntryNormal, Data: encodeRaftCommandV1(cmdSeen, data)},
				},
			},
			expDrop:             false,
			expRemotePropsAfter: 2,
		},
		{
			name:   "empty proposal",
			leader: true,
			msg: raftpb.Message{
				Type: raftpb.MsgProp,
				Entries: []raftpb.Entry{
					{Type: raftpb.EntryNormal, Data: encodeRaftCommandV1(cmdNotSeen, noData)},
				},
			},
			expDrop:             false,
			expRemotePropsAfter: 1,
		},
		{
			name:   "conf change",
			leader: true,
			msg: raftpb.Message{
				Type: raftpb.MsgProp,
				Entries: []raftpb.Entry{
					{Type: raftpb.EntryConfChange, Data: encodeRaftCommandV1(cmdNotSeen, data)},
				},
			},
			expDrop:             false,
			expRemotePropsAfter: 1,
		},
		{
			name:   "non proposal",
			leader: true,
			msg: raftpb.Message{
				Type: raftpb.MsgApp,
			},
			expDrop:             false,
			expRemotePropsAfter: 1,
		},
		{
			name:   "not leader",
			leader: false,
			msg: raftpb.Message{
				Type: raftpb.MsgProp,
				Entries: []raftpb.Entry{
					{Type: raftpb.EntryNormal, Data: encodeRaftCommandV1(cmdNotSeen, data)},
				},
			},
			expDrop:             false,
			expRemotePropsAfter: 0,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			var tc testContext
			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			tc.Start(t, stopper)
			tc.repl.mu.Lock()
			defer tc.repl.mu.Unlock()

			rg := tc.repl.mu.internalRaftGroup
			if c.leader {
				// Set the remoteProposals map to only contain cmdSeen.
				tc.repl.mu.remoteProposals = map[storagebase.CmdIDKey]struct{}{
					cmdSeen: {},
				}
				// Make sure the replica is the leader.
				if s := rg.Status(); s.RaftState != raft.StateLeader {
					t.Errorf("Replica not leader: %v", s)
				}
			} else {
				// Clear the remoteProposals map.
				tc.repl.mu.remoteProposals = nil
				// Force the replica to step down as the leader by sending it a
				// heartbeat at a high term.
				if err := rg.Step(raftpb.Message{
					Type: raftpb.MsgHeartbeat,
					Term: 999,
				}); err != nil {
					t.Error(err)
				}
				if s := rg.Status(); s.RaftState != raft.StateFollower {
					t.Errorf("Replica not follower: %v", s)
				}
			}

			req := &RaftMessageRequest{Message: c.msg}
			drop := tc.repl.shouldDropForwardedProposalLocked(req)
			if c.expDrop != drop {
				t.Errorf("expected drop=%t, found %t", c.expDrop, drop)
			}

			tc.repl.maybeTrackForwardedProposalLocked(rg, req)
			if l := len(tc.repl.mu.remoteProposals); c.expRemotePropsAfter != l {
				t.Errorf("expected %d tracked remote proposals, found %d", c.expRemotePropsAfter, l)
			}
		})
	}
}

// checkValue asserts that the value for a key is the expected one.
// The function will attempt to resolve the intent present on the key, if any.
func checkValue(ctx context.Context, tc *testContext, key []byte, expectedVal []byte) error {
	gArgs := getArgs(key)
	// Note: sending through the store, not directly through the replica, for
	// intent resolution to kick in. Use max user priority to ensure we push
	// any residual intent.
	resp, pErr := client.SendWrappedWith(ctx, tc.store.TestSender(), roachpb.Header{
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
		assignSeqNumsForReqs(txn, &bt, &put, &et)
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
				//
				// TODO(andrei): update the comment above and remove references to
				// TransactionReplayError once 28985 is fixed.
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

// TestGCWithoutThreshold validates that GCRequest only declares the threshold
// keys which are subject to change, and that it does not access these keys if
// it does not declare them.
func TestGCWithoutThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := roachpb.RangeDescriptor{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("z")}
	ctx := context.Background()

	tc := &testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	options := []hlc.Timestamp{{}, hlc.Timestamp{}.Add(1, 0)}

	for i, keyThresh := range options {
		for j, txnThresh := range options {
			func() {
				var gc roachpb.GCRequest
				var spans spanset.SpanSet

				gc.Threshold = keyThresh
				gc.TxnSpanGCThreshold = txnThresh
				cmd, _ := batcheval.LookupCommand(roachpb.GC)
				cmd.DeclareKeys(desc, roachpb.Header{RangeID: tc.repl.RangeID}, &gc, &spans)

				if num, exp := spans.Len(), i+j+1; num != exp {
					t.Fatalf("(%s,%s): expected %d declared keys, found %d",
						keyThresh, txnThresh, exp, num)
				}

				eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
				defer eng.Close()

				batch := eng.NewBatch()
				defer batch.Close()
				rw := spanset.NewBatch(batch, &spans)

				var resp roachpb.GCResponse

				if _, err := batcheval.GC(ctx, rw, batcheval.CommandArgs{
					Args:    &gc,
					EvalCtx: NewReplicaEvalContext(tc.repl, &spans),
				}, &resp); err != nil {
					t.Fatalf("at (%s,%s): %s", keyThresh, txnThresh, err)
				}
			}()
		}
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

func TestReplicaTimestampCacheBumpNotLost(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	ctx := tc.store.AnnotateCtx(context.TODO())
	key := keys.LocalMax

	txn := newTransaction("test", key, 1, enginepb.SERIALIZABLE, tc.Clock())

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

	assignSeqNumsForReqs(txn, &txnPut)
	origTxn := txn.Clone()

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

	var ba roachpb.BatchRequest
	ba.Txn = txn
	ba.Timestamp = txn.Timestamp
	txnPut := putArgs(key, []byte("foo"))
	txnPut2 := txnPut
	// Add two puts (the second one gets Sequence 2, which was a failure mode
	// observed when this test was written and the failure fixed). Originally
	// observed in #10137, where this became relevant (before that, evaluation
	// happened downstream of Raft, so a serialization pass always took place).
	ba.Add(&txnPut)
	ba.Add(&txnPut2)
	assignSeqNumsForReqs(txn, &txnPut, &txnPut2)
	origTxn := txn.Clone()

	batch, _, _, _, pErr := tc.repl.evaluateWriteBatch(ctx, makeIDKey(), ba, &allSpans)
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
	live := func(ids ...roachpb.NodeID) IsLiveMap {
		m := IsLiveMap{}
		for _, id := range ids {
			m[id] = IsLiveMapEntry{IsLive: true}
		}
		return m
	}

	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	cfg := TestStoreConfig(nil)
	tc.StartWithStoreConfig(t, stopper, cfg)

	testCases := []struct {
		replicas    int32
		storeID     roachpb.StoreID
		desc        roachpb.RangeDescriptor
		raftStatus  *raft.Status
		liveness    IsLiveMap
		raftLogSize int64
		expected    ReplicaMetrics
	}{
		// The leader of a 1-replica range is up.
		{1, 1, desc(1), status(1, progress(2)), live(1), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     10,
			}},
		// The leader of a 2-replica range is up (only 1 replica present).
		{2, 1, desc(1), status(1, progress(2)), live(1), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: true,
				BehindCount:     10,
			}},
		// The leader of a 2-replica range is up.
		{2, 1, desc(1, 2), status(1, progress(2)), live(1), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     10,
			}},
		// Both replicas of a 2-replica range are up to date.
		{2, 1, desc(1, 2), status(1, progress(2, 2)), live(1, 2), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     20,
			}},
		// Both replicas of a 2-replica range are up to date (local replica is not leader)
		{2, 2, desc(1, 2), status(2, progress(2, 2)), live(1, 2), 0,
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    false,
				Unavailable:     false,
				Underreplicated: false,
			}},
		// Both replicas of a 2-replica range are live, but follower is behind.
		{2, 1, desc(1, 2), status(1, progress(2, 1)), live(1, 2), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     21,
			}},
		// Both replicas of a 2-replica range are up to date, but follower is dead.
		{2, 1, desc(1, 2), status(1, progress(2, 2)), live(1), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     20,
			}},
		// The leader of a 3-replica range is up.
		{3, 1, desc(1, 2, 3), status(1, progress(1)), live(1), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     11,
			}},
		// All replicas of a 3-replica range are up to date.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 2)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     30,
			}},
		// All replicas of a 3-replica range are up to date (match = 0 is
		// considered up to date).
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 0)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     20,
			}},
		// All replicas of a 3-replica range are live but one replica is behind.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 1)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     31,
			}},
		// All replicas of a 3-replica range are live but two replicas are behind.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 1, 1)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     32,
			}},
		// All replicas of a 3-replica range are up to date, but one replica is dead.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 2)), live(1, 2), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: true,
				BehindCount:     30,
			}},
		// All replicas of a 3-replica range are up to date, but two replicas are dead.
		{3, 1, desc(1, 2, 3), status(1, progress(2, 2, 2)), live(1), 0,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     30,
			}},
		// All replicas of a 3-replica range are up to date, but two replicas are
		// dead, including the leader.
		{3, 2, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(2), 0,
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    true,
				Unavailable:     true,
				Underreplicated: true,
				BehindCount:     0,
			}},
		// Range has no leader, local replica is the range counter.
		{3, 1, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
			}},
		// Range has no leader, local replica is the range counter.
		{3, 3, desc(3, 2, 1), status(0, progress(2, 2, 2)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
			}},
		// Range has no leader, local replica is not the range counter.
		{3, 2, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    false,
				Unavailable:     false,
				Underreplicated: false,
			}},
		// Range has no leader, local replica is not the range counter.
		{3, 3, desc(1, 2, 3), status(0, progress(2, 2, 2)), live(1, 2, 3), 0,
			ReplicaMetrics{
				Leader:          false,
				RangeCounter:    false,
				Unavailable:     false,
				Underreplicated: false,
			}},
		// The leader of a 1-replica range is up and raft log is too large.
		{1, 1, desc(1), status(1, progress(2)), live(1), 5 * raftLogMaxSize,
			ReplicaMetrics{
				Leader:          true,
				RangeCounter:    true,
				Unavailable:     false,
				Underreplicated: false,
				BehindCount:     10,
				RaftLogTooLarge: true,
			}},
	}

	for i, c := range testCases {
		t.Run("", func(t *testing.T) {
			zoneConfig := config.DefaultZoneConfig()
			zoneConfig.NumReplicas = proto.Int32(c.replicas)
			defer config.TestingSetDefaultZoneConfig(zoneConfig)()

			// Alternate between quiescent and non-quiescent replicas to test the
			// quiescent metric.
			c.expected.Quiescent = i%2 == 0
			c.expected.Ticking = !c.expected.Quiescent
			metrics := calcReplicaMetrics(
				context.Background(), hlc.Timestamp{}, &zoneConfig,
				c.liveness, 0, &c.desc, c.raftStatus, storagepb.LeaseStatus{},
				c.storeID, c.expected.Quiescent, c.expected.Ticking,
				CommandQueueMetrics{}, CommandQueueMetrics{}, c.raftLogSize)
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

// TestProposalNoop verifies that batches that result in no-ops do not
// get proposed through Raft and wait for consensus before returning to
// the client.
func TestNoopRequestsNotProposed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cfg := TestStoreConfig(nil)
	rh := roachpb.RequestHeader{Key: roachpb.Key("a")}
	txn := newTransaction(
		"name",
		rh.Key,
		roachpb.NormalUserPriority,
		enginepb.SERIALIZABLE,
		cfg.Clock,
	)

	getReq := &roachpb.GetRequest{
		RequestHeader: rh,
	}
	putReq := &roachpb.PutRequest{
		RequestHeader: rh,
		Value:         roachpb.MakeValueFromBytes([]byte("val")),
	}
	deleteReq := &roachpb.DeleteRequest{
		RequestHeader: rh,
	}
	commitTxnReq := &roachpb.EndTransactionRequest{
		RequestHeader: rh,
		Commit:        true,
	}
	pushTxnReq := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.TxnMeta.Key,
		},
		PusheeTxn: txn.TxnMeta,
		Now:       cfg.Clock.Now(),
		PushType:  roachpb.PUSH_ABORT,
	}
	resolveCommittedIntentReq := &roachpb.ResolveIntentRequest{
		RequestHeader: rh,
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.COMMITTED,
		Poison:        false,
	}
	resolveAbortedIntentReq := &roachpb.ResolveIntentRequest{
		RequestHeader: rh,
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.ABORTED,
		Poison:        true,
	}

	testCases := []struct {
		name        string
		setup       func(context.Context, *Replica) *roachpb.Error // optional
		useTxn      bool
		req         roachpb.Request
		expFailure  string // regexp pattern to match on error if not empty
		expProposal bool
	}{
		{
			name:        "get req",
			req:         getReq,
			expProposal: false,
		},
		{
			name:        "put req",
			req:         putReq,
			expProposal: true,
		},
		{
			name: "delete req",
			req:  deleteReq,
			// NB: a tombstone is written even if no value exists at the key.
			expProposal: true,
		},
		{
			name:        "get req in txn",
			useTxn:      true,
			req:         getReq,
			expProposal: false,
		},
		{
			name:        "put req in txn",
			useTxn:      true,
			req:         putReq,
			expProposal: true,
		},
		{
			name:   "delete req in txn",
			useTxn: true,
			req:    deleteReq,
			// NB: a tombstone intent is written even if no value exists at the key.
			expProposal: true,
		},
		{
			name:       "failed commit txn req",
			useTxn:     true,
			req:        commitTxnReq,
			expFailure: "txn record not found",
			// No-op - the request fails.
			expProposal: false,
		},
		{
			name:        "push txn req",
			req:         pushTxnReq,
			expProposal: true,
		},
		{
			name: "redundant push txn req",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				_, pErr := client.SendWrappedWith(ctx, repl, roachpb.Header{
					RangeID: repl.RangeID,
				}, pushTxnReq)
				return pErr
			},
			req: pushTxnReq,
			// No-op - the transaction has already been pushed successfully.
			expProposal: false,
		},
		{
			name: "resolve committed intent req, with intent",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				_, pErr := client.SendWrappedWith(ctx, repl, roachpb.Header{
					RangeID: repl.RangeID,
					Txn:     txn,
				}, putReq)
				return pErr
			},
			req:         resolveCommittedIntentReq,
			expProposal: true,
		},
		{
			name: "resolve committed intent req, without intent",
			req:  resolveCommittedIntentReq,
			// No-op - the intent is missing.
			expProposal: false,
		},
		{
			name: "resolve aborted intent req",
			req:  resolveAbortedIntentReq,
			// Not a no-op - the request needs to poison the abort span.
			expProposal: true,
		},
		{
			name: "redundant resolve aborted intent req",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				_, pErr := client.SendWrappedWith(ctx, repl, roachpb.Header{
					RangeID: repl.RangeID,
				}, resolveAbortedIntentReq)
				return pErr
			},
			req: resolveAbortedIntentReq,
			// No-op - the abort span has already been poisoned.
			expProposal: false,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			tc := testContext{}
			tc.StartWithStoreConfig(t, stopper, cfg)
			repl := tc.repl

			if c.setup != nil {
				if pErr := c.setup(ctx, repl); pErr != nil {
					t.Fatalf("test setup failed: %v", pErr)
				}
			}

			var propCount int32
			markerTS := tc.Clock().Now()
			repl.mu.Lock()
			repl.store.TestingKnobs().TestingProposalFilter =
				func(args storagebase.ProposalFilterArgs) *roachpb.Error {
					if args.Req.Timestamp == markerTS {
						atomic.AddInt32(&propCount, 1)
					}
					return nil
				}
			repl.mu.Unlock()

			ba := roachpb.BatchRequest{}
			ba.Timestamp = markerTS
			ba.RangeID = repl.RangeID
			if c.useTxn {
				ba.Txn = txn
				assignSeqNumsForReqs(txn, c.req)
			}
			ba.Add(c.req)
			_, pErr := repl.Send(ctx, ba)

			// Check return error.
			if c.expFailure == "" {
				if pErr != nil {
					t.Fatalf("unexpected error: %v", pErr)
				}
			} else {
				if !testutils.IsPError(pErr, c.expFailure) {
					t.Fatalf("expected error %q, found %v", c.expFailure, pErr)
				}
			}

			// Check proposal status.
			if sawProp := (propCount > 0); sawProp != c.expProposal {
				t.Errorf("expected proposal=%t, found %t", c.expProposal, sawProp)
			}
		})
	}
}

func TestCommandTooLarge(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	st := tc.store.cfg.Settings
	st.Manual.Store(true)
	MaxCommandSize.Override(&st.SV, 1024)

	args := putArgs(roachpb.Key("k"),
		[]byte(strings.Repeat("a", int(MaxCommandSize.Get(&st.SV)))))
	if _, pErr := tc.SendWrapped(&args); !testutils.IsPError(pErr, "command is too large") {
		t.Fatalf("did not get expected error: %v", pErr)
	}
}

// Test that, if the application of a Raft command fails, intents are not
// resolved. This is because we don't want intent resolution to take place if an
// EndTransaction fails.
func TestErrorInRaftApplicationClearsIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var storeKnobs storagebase.StoreTestingKnobs
	var filterActive int32
	key := roachpb.Key("a")
	rkey, err := keys.Addr(key)
	if err != nil {
		t.Fatal(err)
	}
	storeKnobs.TestingApplyFilter = func(filterArgs storagebase.ApplyFilterArgs) *roachpb.Error {
		if atomic.LoadInt32(&filterActive) == 1 {
			return roachpb.NewErrorf("boom")
		}
		return nil
	}
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storeKnobs}})
	defer s.Stopper().Stop(context.TODO())

	splitKey := roachpb.Key("b")
	if err := kvDB.AdminSplit(context.TODO(), splitKey, splitKey); err != nil {
		t.Fatal(err)
	}

	txn := newTransaction("test", key, roachpb.NormalUserPriority,
		enginepb.SERIALIZABLE, s.Clock(),
	)
	btArgs, _ := beginTxnArgs(key, txn)
	var ba roachpb.BatchRequest
	ba.Header.Txn = txn
	ba.Add(&btArgs)
	assignSeqNumsForReqs(txn, &btArgs)
	if _, pErr := s.DB().GetFactory().NonTransactionalSender().Send(context.TODO(), ba); pErr != nil {
		t.Fatal(pErr.GoError())
	}

	// Fail future command applications.
	atomic.StoreInt32(&filterActive, 1)

	// Propose an EndTransaction with a remote intent. The _remote_ part is
	// important because intents local to the txn's range are resolved inline with
	// the EndTransaction execution.
	// We do this by using replica.propose() directly, as opposed to going through
	// the DistSender, because we want to inspect the proposal's result after the
	// injected error.
	txnCpy := *txn
	etArgs, _ := endTxnArgs(&txnCpy, true /* commit */)
	etArgs.IntentSpans = []roachpb.Span{{Key: roachpb.Key("bb")}}
	ba = roachpb.BatchRequest{}
	ba.Timestamp = s.Clock().Now()
	ba.Header.Txn = &txnCpy
	ba.Add(&etArgs)
	assignSeqNumsForReqs(&txnCpy, &etArgs)
	// Get a reference to the txn's replica.
	stores := s.GetStores().(*Stores)
	store, err := stores.GetStore(s.GetFirstStoreID())
	if err != nil {
		t.Fatal(err)
	}
	repl := store.LookupReplica(rkey) /* end */
	if repl == nil {
		t.Fatalf("replica for key %s not found", rkey)
	}

	exLease, _ := repl.GetLease()
	ch, _, _, pErr := repl.propose(
		context.Background(), exLease, ba, nil /* endCmds */, &allSpans,
	)
	if pErr != nil {
		t.Fatal(pErr)
	}
	propRes := <-ch
	if !testutils.IsPError(propRes.Err, "boom") {
		t.Fatalf("expected injected error, got: %v", propRes.Err)
	}
	if len(propRes.Intents) != 0 {
		t.Fatal("expected intents to have been cleared")
	}
}

// TestProposeWithAsyncConsensus tests that the proposal of a batch with
// AsyncConsensus set to true will return its evaluation result before Raft
// command has completed consensus and applied.
func TestProposeWithAsyncConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	tsc := TestStoreConfig(nil)

	var filterActive int32
	blockRaftApplication := make(chan struct{})
	tsc.TestingKnobs.TestingApplyFilter =
		func(filterArgs storagebase.ApplyFilterArgs) *roachpb.Error {
			if atomic.LoadInt32(&filterActive) == 1 {
				<-blockRaftApplication
			}
			return nil
		}

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.StartWithStoreConfig(t, stopper, tsc)
	repl := tc.repl

	var ba roachpb.BatchRequest
	key := roachpb.Key("a")
	put := putArgs(key, []byte("val"))
	ba.Add(&put)
	ba.Timestamp = tc.Clock().Now()
	ba.AsyncConsensus = true

	atomic.StoreInt32(&filterActive, 1)
	exLease, _ := repl.GetLease()
	ch, _, _, pErr := repl.propose(
		context.Background(), exLease, ba, nil /* endCmds */, &allSpans,
	)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// The result should be signaled before consensus.
	propRes := <-ch
	if propRes.Err != nil {
		t.Fatalf("unexpected proposal result error: %v", propRes.Err)
	}
	if propRes.Reply == nil || len(propRes.Reply.Responses) != 1 {
		t.Fatalf("expected proposal result with 1 response, found: %v", propRes.Reply)
	}

	// Stop blocking Raft application to allow everything to shut down cleanly.
	close(blockRaftApplication)
}

func TestSplitMsgApps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	msgApp := func(idx uint64) raftpb.Message {
		return raftpb.Message{Index: idx, Type: raftpb.MsgApp}
	}
	otherMsg := func(idx uint64) raftpb.Message {
		return raftpb.Message{Index: idx, Type: raftpb.MsgVote}
	}
	formatMsgs := func(msgs []raftpb.Message) string {
		strs := make([]string, len(msgs))
		for i, msg := range msgs {
			strs[i] = fmt.Sprintf("{%s:%d}", msg.Type, msg.Index)
		}
		return fmt.Sprint(strs)
	}

	testCases := []struct {
		msgsIn, msgAppsOut, otherMsgsOut []raftpb.Message
	}{
		// No msgs.
		{
			msgsIn:       []raftpb.Message{},
			msgAppsOut:   []raftpb.Message{},
			otherMsgsOut: []raftpb.Message{},
		},
		// Only msgApps.
		{
			msgsIn:       []raftpb.Message{msgApp(1)},
			msgAppsOut:   []raftpb.Message{msgApp(1)},
			otherMsgsOut: []raftpb.Message{},
		},
		{
			msgsIn:       []raftpb.Message{msgApp(1), msgApp(2)},
			msgAppsOut:   []raftpb.Message{msgApp(1), msgApp(2)},
			otherMsgsOut: []raftpb.Message{},
		},
		{
			msgsIn:       []raftpb.Message{msgApp(2), msgApp(1)},
			msgAppsOut:   []raftpb.Message{msgApp(2), msgApp(1)},
			otherMsgsOut: []raftpb.Message{},
		},
		// Only otherMsgs.
		{
			msgsIn:       []raftpb.Message{otherMsg(1)},
			msgAppsOut:   []raftpb.Message{},
			otherMsgsOut: []raftpb.Message{otherMsg(1)},
		},
		{
			msgsIn:       []raftpb.Message{otherMsg(1), otherMsg(2)},
			msgAppsOut:   []raftpb.Message{},
			otherMsgsOut: []raftpb.Message{otherMsg(1), otherMsg(2)},
		},
		{
			msgsIn:       []raftpb.Message{otherMsg(2), otherMsg(1)},
			msgAppsOut:   []raftpb.Message{},
			otherMsgsOut: []raftpb.Message{otherMsg(2), otherMsg(1)},
		},
		// Mixed msgApps and otherMsgs.
		{
			msgsIn:       []raftpb.Message{msgApp(1), otherMsg(2)},
			msgAppsOut:   []raftpb.Message{msgApp(1)},
			otherMsgsOut: []raftpb.Message{otherMsg(2)},
		},
		{
			msgsIn:       []raftpb.Message{otherMsg(1), msgApp(2)},
			msgAppsOut:   []raftpb.Message{msgApp(2)},
			otherMsgsOut: []raftpb.Message{otherMsg(1)},
		},
		{
			msgsIn:       []raftpb.Message{msgApp(1), otherMsg(2), msgApp(3)},
			msgAppsOut:   []raftpb.Message{msgApp(1), msgApp(3)},
			otherMsgsOut: []raftpb.Message{otherMsg(2)},
		},
		{
			msgsIn:       []raftpb.Message{otherMsg(1), msgApp(2), otherMsg(3)},
			msgAppsOut:   []raftpb.Message{msgApp(2)},
			otherMsgsOut: []raftpb.Message{otherMsg(1), otherMsg(3)},
		},
	}
	for _, c := range testCases {
		inStr := formatMsgs(c.msgsIn)
		t.Run(inStr, func(t *testing.T) {
			msgAppsRes, otherMsgsRes := splitMsgApps(c.msgsIn)
			if !reflect.DeepEqual(msgAppsRes, c.msgAppsOut) || !reflect.DeepEqual(otherMsgsRes, c.otherMsgsOut) {
				t.Errorf("expected splitMsgApps(%s)=%s/%s, found %s/%s", inStr, formatMsgs(c.msgAppsOut),
					formatMsgs(c.otherMsgsOut), formatMsgs(msgAppsRes), formatMsgs(otherMsgsRes))
			}
		})
	}
}

type testQuiescer struct {
	desc           roachpb.RangeDescriptor
	numProposals   int
	status         *raft.Status
	lastIndex      uint64
	raftReady      bool
	ownsValidLease bool
	livenessMap    IsLiveMap
}

func (q *testQuiescer) descRLocked() *roachpb.RangeDescriptor {
	return &q.desc
}

func (q *testQuiescer) raftStatusRLocked() *raft.Status {
	return q.status
}

func (q *testQuiescer) raftLastIndexLocked() (uint64, error) {
	return q.lastIndex, nil
}

func (q *testQuiescer) hasRaftReadyRLocked() bool {
	return q.raftReady
}

func (q *testQuiescer) ownsValidLeaseRLocked(ts hlc.Timestamp) bool {
	return q.ownsValidLease
}

func (q *testQuiescer) maybeTransferRaftLeader(
	ctx context.Context, status *raft.Status, ts hlc.Timestamp,
) {
	// Nothing to do here. We test Raft leadership transfer in
	// TestTransferRaftLeadership.
}

func TestShouldReplicaQuiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const logIndex = 10
	const invalidIndex = 11
	test := func(expected bool, transform func(q *testQuiescer) *testQuiescer) {
		t.Run("", func(t *testing.T) {
			// A testQuiescer initialized so that shouldReplicaQuiesce will return
			// true. The transform function is intended to perform one mutation to
			// this quiescer so that shouldReplicaQuiesce will return false.
			q := &testQuiescer{
				desc: roachpb.RangeDescriptor{
					Replicas: []roachpb.ReplicaDescriptor{
						{NodeID: 1, ReplicaID: 1},
						{NodeID: 2, ReplicaID: 2},
						{NodeID: 3, ReplicaID: 3},
					},
				},
				status: &raft.Status{
					ID: 1,
					HardState: raftpb.HardState{
						Commit: logIndex,
					},
					SoftState: raft.SoftState{
						RaftState: raft.StateLeader,
					},
					Applied: logIndex,
					Progress: map[uint64]raft.Progress{
						1: {Match: logIndex},
						2: {Match: logIndex},
						3: {Match: logIndex},
					},
					LeadTransferee: 0,
				},
				lastIndex:      logIndex,
				raftReady:      false,
				ownsValidLease: true,
				livenessMap: IsLiveMap{
					1: {IsLive: true},
					2: {IsLive: true},
					3: {IsLive: true},
				},
			}
			q = transform(q)
			_, ok := shouldReplicaQuiesce(context.Background(), q, hlc.Timestamp{}, q.numProposals, q.livenessMap)
			if expected != ok {
				t.Fatalf("expected %v, but found %v", expected, ok)
			}
		})
	}

	test(true, func(q *testQuiescer) *testQuiescer {
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.numProposals = 1
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.status = nil
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.status.RaftState = raft.StateFollower
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.status.RaftState = raft.StateCandidate
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.status.LeadTransferee = 1
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.status.Commit = invalidIndex
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.status.Applied = invalidIndex
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.lastIndex = invalidIndex
		return q
	})
	for _, i := range []uint64{1, 2, 3} {
		test(false, func(q *testQuiescer) *testQuiescer {
			q.status.Progress[i] = raft.Progress{Match: invalidIndex}
			return q
		})
	}
	test(false, func(q *testQuiescer) *testQuiescer {
		delete(q.status.Progress, q.status.ID)
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.ownsValidLease = false
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.raftReady = true
		return q
	})
	// Create a mismatch between the raft progress replica IDs and the
	// replica IDs in the range descriptor.
	for i := 0; i < 3; i++ {
		test(false, func(q *testQuiescer) *testQuiescer {
			q.desc.Replicas[i].ReplicaID = roachpb.ReplicaID(4 + i)
			return q
		})
	}
	// Pass a nil liveness map.
	test(true, func(q *testQuiescer) *testQuiescer {
		q.livenessMap = nil
		return q
	})
	// Verify quiesce even when replica progress doesn't match, if
	// the replica is on a non-live node.
	for _, i := range []uint64{1, 2, 3} {
		test(true, func(q *testQuiescer) *testQuiescer {
			q.livenessMap[roachpb.NodeID(i)] = IsLiveMapEntry{IsLive: false}
			q.status.Progress[i] = raft.Progress{Match: invalidIndex}
			return q
		})
	}
}

func TestReplicaRecomputeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	key := roachpb.RKey("a")
	repl := tc.store.LookupReplica(key)
	desc := repl.Desc()
	sKey := desc.StartKey.AsRawKey()

	const errMismatch = "descriptor mismatch; range likely merged"

	type testCase struct {
		name     string
		key      roachpb.Key
		expDelta enginepb.MVCCStats
		expErr   string
	}

	runTest := func(test testCase) {
		t.Run(test.name, func(t *testing.T) {
			args := &roachpb.RecomputeStatsRequest{
				RequestHeader: roachpb.RequestHeader{
					Key: test.key,
				},
			}

			resp, pErr := tc.SendWrapped(args)
			if !testutils.IsPError(pErr, test.expErr) {
				t.Fatalf("got:\n%s\nexpected: %s", pErr, test.expErr)
			}
			if test.expErr != "" {
				return
			}

			delta := enginepb.MVCCStats(resp.(*roachpb.RecomputeStatsResponse).AddedDelta)
			delta.AgeTo(test.expDelta.LastUpdateNanos)

			if delta != test.expDelta {
				t.Fatal("diff(wanted, actual) = ", strings.Join(pretty.Diff(test.expDelta, delta), "\n"))
			}
		})
	}

	for _, test := range []testCase{
		// Non-matching endpoints.
		{"leftmismatch", roachpb.Key("a"), enginepb.MVCCStats{}, errMismatch},
		// Recomputation that shouldn't find anything.
		{"noop", sKey, enginepb.MVCCStats{}, ""},
	} {
		runTest(test)
	}

	ctx := context.Background()
	seed := randutil.NewPseudoSeed()
	t.Logf("seed is %d", seed)
	rnd := rand.New(rand.NewSource(seed))

	repl.raftMu.Lock()
	repl.mu.Lock()
	ms := repl.mu.state.Stats // intentionally mutated below
	disturbMS := enginepb.NewPopulatedMVCCStats(rnd, false)
	disturbMS.ContainsEstimates = false
	ms.Add(*disturbMS)
	err := repl.raftMu.stateLoader.SetMVCCStats(ctx, tc.engine, ms)
	repl.assertStateLocked(ctx, tc.engine)
	repl.mu.Unlock()
	repl.raftMu.Unlock()

	if err != nil {
		t.Fatal(err)
	}

	// We have `stored ms = recomputable ms + disturbMS`, and so the returned delta
	// should be `recomputable ms - stored ms = -disturbMS`.
	var expDelta enginepb.MVCCStats
	expDelta.Subtract(*disturbMS)

	runTest(testCase{"randdelta", sKey, expDelta, ""})
	if !t.Failed() {
		runTest(testCase{"noopagain", sKey, enginepb.MVCCStats{}, ""})
	}
}

// TestConsistencyQueueErrorFromCheckConsistency exercises the case in which
// the queue receives an error from CheckConsistency.
func TestConsistenctQueueErrorFromCheckConsistency(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs = storagebase.StoreTestingKnobs{
		TestingRequestFilter: func(ba roachpb.BatchRequest) *roachpb.Error {
			if _, ok := ba.GetArg(roachpb.ComputeChecksum); ok {
				return roachpb.NewErrorf("boom")
			}
			return nil
		},
	}
	tc := testContext{}
	tc.StartWithStoreConfig(t, stopper, cfg)

	for i := 0; i < 2; i++ {
		// Do this twice because it used to deadlock. See #25456.
		sysCfg := tc.store.Gossip().GetSystemConfig()
		if err := tc.store.consistencyQueue.process(ctx, tc.repl, sysCfg); !testutils.IsError(err, "boom") {
			t.Fatal(err)
		}
	}
}

// TestReplicaLocalRetries verifies local retry logic for transactional
// and non transactional batches. Verifies the timestamp cache is updated
// to reflect the timestamp at which retried batches are executed.
func TestReplicaLocalRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	newTxn := func(key string, ts hlc.Timestamp) *roachpb.Transaction {
		txn := roachpb.MakeTransaction(
			"test", roachpb.Key(key), roachpb.NormalUserPriority, enginepb.SERIALIZABLE, ts, 0,
		)
		return &txn
	}
	send := func(ba roachpb.BatchRequest) (hlc.Timestamp, error) {
		br, pErr := tc.Sender().Send(context.Background(), ba)
		if pErr != nil {
			return hlc.Timestamp{}, pErr.GetDetail()
		}
		return br.Timestamp, nil
	}
	get := func(key string) (hlc.Timestamp, error) {
		var ba roachpb.BatchRequest
		get := getArgs(roachpb.Key(key))
		ba.Add(&get)
		return send(ba)
	}
	put := func(key, val string) (hlc.Timestamp, error) {
		var ba roachpb.BatchRequest
		put := putArgs(roachpb.Key(key), []byte(val))
		ba.Add(&put)
		return send(ba)
	}

	testCases := []struct {
		name             string
		setupFn          func() (hlc.Timestamp, error) // returns expected batch execution timestamp
		batchFn          func(hlc.Timestamp) (roachpb.BatchRequest, hlc.Timestamp)
		expErr           string
		expTSCUpdateKeys []string
	}{
		{
			name: "local retry of write too old on put",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				expTS = ts.Next()
				put := putArgs(roachpb.Key("a"), []byte("put2"))
				ba.Add(&put)
				return
			},
		},
		{
			name: "local retry of write too old on cput",
			setupFn: func() (hlc.Timestamp, error) {
				// Note there are two different version of the value, but a
				// non-txnal cput will evaluate the most recent version and
				// avoid a condition failed error.
				_, _ = put("b", "put1")
				return put("b", "put2")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				expTS = ts.Next()
				cput := cPutArgs(roachpb.Key("b"), []byte("cput"), []byte("put2"))
				ba.Add(&cput)
				return
			},
			expTSCUpdateKeys: []string{"b"},
		},
		{
			name: "local retry of write too old on initput",
			setupFn: func() (hlc.Timestamp, error) {
				// Note there are two different version of the value, but a
				// non-txnal cput will evaluate the most recent version and
				// avoid a condition failed error.
				_, _ = put("b-iput", "put1")
				return put("b-iput", "put2")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				expTS = ts.Next()
				iput := iPutArgs(roachpb.Key("b-iput"), []byte("put2"))
				ba.Add(&iput)
				return
			},
			expTSCUpdateKeys: []string{"b-iput"},
		},
		{
			name: "serializable push without retry",
			setupFn: func() (hlc.Timestamp, error) {
				return get("a")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				expTS = ts.Next()
				put := putArgs(roachpb.Key("a"), []byte("put2"))
				ba.Add(&put)
				return
			},
		},
		// Non-1PC serializable txn cput will fail with write too old error.
		{
			name: "no local retry of write too old on non-1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("c", "put")
				return put("c", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("c", ts.Prev())
				cput := cPutArgs(roachpb.Key("c"), []byte("iput"), []byte("put"))
				ba.Add(&cput)
				assignSeqNumsForReqs(ba.Txn, &cput)
				return
			},
			expErr: "write at timestamp .* too old",
		},
		// Non-1PC serializable txn initput will fail with write too old error.
		{
			name: "no local retry of write too old on non-1PC txn initput",
			setupFn: func() (hlc.Timestamp, error) {
				return put("c-iput", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("c-iput", ts.Prev())
				iput := iPutArgs(roachpb.Key("c-iput"), []byte("iput"))
				ba.Add(&iput)
				assignSeqNumsForReqs(ba.Txn, &iput)
				return
			},
			expErr: "write at timestamp .* too old",
		},
		// 1PC serializable transaction will fail instead of retrying if
		// EndTransactionRequest.NoRefreshSpans is not true.
		{
			name: "no local retry of write too old on 1PC txn and refresh spans",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("d", "put")
				return put("d", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("d", ts.Prev())
				bt, _ := beginTxnArgs(ba.Txn.Key, ba.Txn)
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&bt, &cput, &et)
				assignSeqNumsForReqs(ba.Txn, &bt, &cput, &et)
				return
			},
			expErr: "RETRY_WRITE_TOO_OLD",
		},
		// 1PC serializable transaction will retry locally.
		{
			name: "local retry of write too old on 1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("e", "put")
				return put("e", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("e", ts.Prev())
				expTS = ts.Next()
				bt, _ := beginTxnArgs(ba.Txn.Key, ba.Txn)
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				et.NoRefreshSpans = true // necessary to indicate local retry is possible
				ba.Add(&bt, &cput, &et)
				assignSeqNumsForReqs(ba.Txn, &bt, &cput, &et)
				return
			},
			expTSCUpdateKeys: []string{"e"},
		},
		// Handle multiple write too old errors.
		{
			name: "local retry with multiple write too old errors",
			setupFn: func() (hlc.Timestamp, error) {
				if _, err := put("f1", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				if _, err := put("f2", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				return put("f3", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				expTS = ts.Next()
				for i := 1; i <= 3; i++ {
					cput := cPutArgs(roachpb.Key(fmt.Sprintf("f%d", i)), []byte("cput"), []byte("put"))
					ba.Add(&cput)
				}
				return
			},
			expTSCUpdateKeys: []string{"f1", "f2", "f3"},
		},
		// Handle multiple write too old errors in 1PC transaction.
		{
			name: "local retry with multiple write too old errors",
			setupFn: func() (hlc.Timestamp, error) {
				if _, err := put("g1", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				if _, err := put("g2", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				return put("g3", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("g1", ts.Prev())
				expTS = ts.Next()
				bt, _ := beginTxnArgs(ba.Txn.Key, ba.Txn)
				ba.Add(&bt)
				assignSeqNumsForReqs(ba.Txn, &bt)
				for i := 1; i <= 3; i++ {
					cput := cPutArgs(roachpb.Key(fmt.Sprintf("g%d", i)), []byte("cput"), []byte("put"))
					ba.Add(&cput)
					assignSeqNumsForReqs(ba.Txn, &cput)
				}
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				et.NoRefreshSpans = true // necessary to indicate local retry is possible
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &et)
				return
			},
			expTSCUpdateKeys: []string{"g1", "g2", "g3"},
		},
		// Serializable transaction will commit with forwarded timestamp if no refresh spans.
		{
			name: "serializable commit with forwarded timestamp",
			setupFn: func() (hlc.Timestamp, error) {
				if _, err := put("h", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				return get("h")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				txn := newTxn("h", ts.Prev())
				// Send begin transaction first.
				ba.Txn = txn
				bt, _ := beginTxnArgs(ba.Txn.Key, ba.Txn)
				ba.Add(&bt)
				assignSeqNumsForReqs(ba.Txn, &bt)
				if _, err := send(ba); err != nil {
					panic(err)
				}
				// Send the remainder of the transaction in another batch.
				expTS = ts.Next()
				ba = roachpb.BatchRequest{}
				ba.Txn = txn
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				ba.Add(&cput)
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				et.NoRefreshSpans = true // necessary to indicate local retry is possible
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
			expTSCUpdateKeys: []string{"h"},
		},
		// Serializable 1PC transaction will commit with forwarded timestamp
		// using the 1PC path if no refresh spans.
		{
			name: "serializable commit with forwarded timestamp on 1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				return get("a")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("a", ts.Prev())
				expTS = ts.Next()
				bt, _ := beginTxnArgs(ba.Txn.Key, ba.Txn)
				cput := putArgs(ba.Txn.Key, []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				et.Require1PC = true     // don't allow this to bypass the 1PC optimization
				et.NoRefreshSpans = true // necessary to indicate local retry is possible
				ba.Add(&bt, &cput, &et)
				assignSeqNumsForReqs(ba.Txn, &bt, &cput, &et)
				return
			},
		},
		// Serializable transaction will commit with WriteTooOld flag if no refresh spans.
		{
			name: "serializable commit with write-too-old flag",
			setupFn: func() (hlc.Timestamp, error) {
				return put("i", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				txn := newTxn("i", ts.Prev())
				// Send begin transaction first.
				ba.Txn = txn
				bt, _ := beginTxnArgs(ba.Txn.Key, ba.Txn)
				ba.Add(&bt)
				assignSeqNumsForReqs(ba.Txn, &bt)
				if _, err := send(ba); err != nil {
					panic(err)
				}
				// Send the remainder of the transaction in another batch.
				expTS = ts.Next()
				ba = roachpb.BatchRequest{}
				ba.Txn = txn
				put := putArgs(ba.Txn.Key, []byte("newput"))
				ba.Add(&put)
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				et.NoRefreshSpans = true // necessary to indicate local retry is possible
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &put, &et)
				return
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ts, err := test.setupFn()
			if err != nil {
				t.Fatal(err)
			}
			ba, expTS := test.batchFn(ts)
			actualTS, err := send(ba)
			if !testutils.IsError(err, test.expErr) {
				t.Fatalf("expected error %q; got \"%v\"", test.expErr, err)
			}
			if actualTS != expTS {
				t.Fatalf("expected ts=%s; got %s", expTS, actualTS)
			}
			for _, k := range test.expTSCUpdateKeys {
				rTS, _ := tc.repl.store.tsCache.GetMaxRead(roachpb.Key(k), nil)
				if rTS != expTS {
					t.Fatalf("expected timestamp cache update for %s to %s; got %s", k, expTS, rTS)
				}
			}
		})
	}
}

// TestReplicaPushed1PC verifies that a transaction that has its
// timestamp pushed while reading and then sends all its writes in a
// 1PC batch correctly detects conflicts with writes between its
// original and pushed timestamps. This was hypothesized as a possible
// cause of https://github.com/cockroachdb/cockroach/issues/23176
// but we were already guarding against this case. This test ensures
// it stays that way.
func TestReplicaPushed1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	tc.Start(t, stopper)

	for _, isolation := range []enginepb.IsolationType{enginepb.SERIALIZABLE, enginepb.SNAPSHOT} {
		t.Run(isolation.String(), func(t *testing.T) {
			ctx := context.Background()
			k := roachpb.Key(isolation.String())

			// Start a transaction and assign its OrigTimestamp.
			ts1 := tc.Clock().Now()
			txn := roachpb.MakeTransaction("test", k, roachpb.NormalUserPriority, isolation, ts1, 0)

			// Write a value outside the transaction.
			tc.manualClock.Increment(10)
			ts2 := tc.Clock().Now()
			if err := engine.MVCCPut(ctx, tc.engine, nil, k, ts2, roachpb.MakeValueFromString("one"), nil); err != nil {
				t.Fatalf("writing interfering value: %s", err)
			}

			// Push the transaction's timestamp. In real-world situations,
			// the only thing that can push a read-only transaction's
			// timestamp is ReadWithinUncertaintyIntervalError, but
			// synthesizing one of those in this single-node test harness is
			// tricky.
			tc.manualClock.Increment(10)
			ts3 := tc.Clock().Now()
			txn.Timestamp.Forward(ts3)

			// Execute the write phase of the transaction as a single batch,
			// which must return a WRITE_TOO_OLD TransactionRetryError.
			//
			// TODO(bdarnell): When this test was written, in SNAPSHOT
			// isolation we would attempt to execute the transaction on the
			// 1PC path, see a timestamp mismatch, and then then throw the
			// 1PC results away and re-execute it on the regular path (which
			// would generate the WRITE_TOO_OLD error). We have added earlier
			// timestamp checks for a small performance improvement, but
			// this difference is difficult to observe in a test. If we had
			// more detailed metrics we could assert that the 1PC path was
			// not even attempted here.
			var ba roachpb.BatchRequest
			bt, h := beginTxnArgs(txn.Key, &txn)
			ba.Header = h
			put := putArgs(k, []byte("two"))
			et, _ := endTxnArgs(&txn, true)
			ba.Add(&bt, &put, &et)
			assignSeqNumsForReqs(&txn, &bt, &put, &et)
			if br, pErr := tc.Sender().Send(ctx, ba); pErr == nil {
				t.Errorf("did not get expected error. resp=%s", br)
			} else if trErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
				t.Errorf("expected TransactionRetryError, got %s", pErr)
			} else if trErr.Reason != roachpb.RETRY_WRITE_TOO_OLD {
				t.Errorf("expected RETRY_WRITE_TOO_OLD, got %s", trErr)
			}
		})
	}
}

// assertUsingRangeAppliedState asserts that the value of
// ReplicaState.UsingAppliedStateKey is equal to the expected value.
func assertUsingRangeAppliedState(t *testing.T, repl *Replica, expSet bool) {
	t.Helper()
	repl.raftMu.Lock()
	defer repl.raftMu.Unlock()
	repl.mu.Lock()
	defer repl.mu.Unlock()

	usingAppliedStateKey := repl.mu.state.UsingAppliedStateKey
	if usingAppliedStateKey != expSet {
		t.Errorf("expected ReplicaState.UsingAppliedStateKey=%t, found %t",
			expSet, usingAppliedStateKey)
	}
}

// assertRangeAppliedStateRelatedKeysExist performs a series of assertions
// that each key related to the RangeAppliedState key migration is either
// present or missing, depending on the expRASK flag.
func assertRangeAppliedStateRelatedKeysExist(
	ctx context.Context, t *testing.T, repl *Replica, expRASK bool,
) {
	t.Helper()
	repl.raftMu.Lock()
	defer repl.raftMu.Unlock()
	repl.mu.Lock()
	defer repl.mu.Unlock()

	assertHasKey := func(key roachpb.Key, expect bool) {
		t.Helper()
		val, _, err := engine.MVCCGet(ctx, repl.store.Engine(), key, hlc.Timestamp{}, true, nil)
		if err != nil {
			t.Fatal(err)
		}

		exists := val != nil
		if exists != expect {
			t.Errorf("expected key %s to exist=%t, found %t", key, expect, exists)
		}
	}

	rsl := repl.mu.stateLoader
	assertHasKey(rsl.RangeAppliedStateKey(), expRASK)
	assertHasKey(rsl.RaftAppliedIndexLegacyKey(), !expRASK)
	assertHasKey(rsl.LeaseAppliedIndexLegacyKey(), !expRASK)
	assertHasKey(rsl.RangeStatsLegacyKey(), !expRASK)
}

// TestReplicaBootstrapRangeAppliedStateKey verifies that a bootstrapped range
// is only created with a RangeAppliedStateKey if the cluster version is high
// enough to permit it.
func TestReplicaBootstrapRangeAppliedStateKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		version                 roachpb.Version
		expRangeAppliedStateKey bool
	}{
		{
			version:                 cluster.VersionByKey(cluster.Version2_0),
			expRangeAppliedStateKey: false,
		},
		{
			version:                 cluster.VersionByKey(cluster.VersionRangeAppliedStateKey),
			expRangeAppliedStateKey: true,
		},
		{
			version:                 cluster.BinaryServerVersion,
			expRangeAppliedStateKey: true,
		},
	}
	for _, c := range testCases {
		t.Run(fmt.Sprintf("version=%s", c.version), func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			cfg := TestStoreConfig(nil)
			cfg.Settings = cluster.MakeTestingClusterSettingsWithVersion(
				c.version /* minVersion */, cluster.BinaryServerVersion /* serverVersion */)
			tc := testContext{}
			tc.StartWithStoreConfig(t, stopper, cfg)
			repl := tc.repl

			// Check that that UsingAppliedStateKey flag in ReplicaState is set
			// as expected.
			assertInMemState := func(t *testing.T) {
				t.Helper()
				assertUsingRangeAppliedState(t, repl, c.expRangeAppliedStateKey)
			}

			// Check that persisted keys agree with the UsingAppliedStateKey flag.
			assertPersistentState := func(t *testing.T) {
				t.Helper()
				assertRangeAppliedStateRelatedKeysExist(ctx, t, repl, c.expRangeAppliedStateKey)
			}

			// Check that in-mem and persistent state agree.
			assertInMemAndPersistentStateAgree := func(t *testing.T) {
				t.Helper()
				repl.AssertState(ctx, tc.engine)
			}

			// Check that the MVCCStats are correct.
			computeStatsDelta := func(db *client.DB) (enginepb.MVCCStats, error) {
				var b client.Batch
				b.AddRawRequest(&roachpb.RecomputeStatsRequest{
					RequestHeader: roachpb.RequestHeader{Key: roachpb.KeyMin},
					DryRun:        true,
				})
				if err := db.Run(ctx, &b); err != nil {
					return enginepb.MVCCStats{}, err
				}
				resp := b.RawResponse().Responses[0].GetInner().(*roachpb.RecomputeStatsResponse)
				delta := enginepb.MVCCStats(resp.AddedDelta)
				delta.AgeTo(0)
				return delta, nil
			}
			assertEmptyStatsDelta := func(t *testing.T) {
				t.Helper()
				delta, err := computeStatsDelta(repl.DB())
				if err != nil {
					t.Fatal(err)
				}
				if delta != (enginepb.MVCCStats{}) {
					t.Errorf("unexpected stats adjustment of %+v", delta)
				}
			}

			// Perform initial series of assertions.
			assertInMemState(t)
			assertPersistentState(t)
			assertInMemAndPersistentStateAgree(t)
			assertEmptyStatsDelta(t)

			// Save the ReplicaState and perform persistent assertions again.
			repl.raftMu.Lock()
			repl.mu.Lock()
			if _, err := repl.mu.stateLoader.Save(ctx, tc.engine, repl.mu.state); err != nil {
				t.Fatalf("could not save ReplicaState: %v", err)
			}
			repl.mu.Unlock()
			repl.raftMu.Unlock()
			assertPersistentState(t)
			assertInMemAndPersistentStateAgree(t)
			assertEmptyStatsDelta(t)

			// Load the ReplicaState and perform in-memory assertions again.
			repl.raftMu.Lock()
			repl.mu.Lock()
			state, err := repl.mu.stateLoader.Load(ctx, tc.engine, repl.DescLocked())
			if err != nil {
				t.Fatalf("could not load ReplicaState: %v", err)
			}
			repl.mu.state = state
			repl.mu.Unlock()
			repl.raftMu.Unlock()
			assertInMemState(t)
			assertInMemAndPersistentStateAgree(t)
		})
	}
}

// TestReplicaMigrateRangeAppliedStateKey verifies that a range which is not yet
// using the RangeAppliedStateKey will eventually trigger a migration to begin
// using it once the cluster version is high enough to permit its use.
func TestReplicaMigrateRangeAppliedStateKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	cfg := TestStoreConfig(nil)
	cfg.Settings = cluster.MakeTestingClusterSettingsWithVersion(
		cluster.VersionByKey(cluster.Version2_0), /* minVersion */
		cluster.BinaryServerVersion /* serverVersion */)
	tc := testContext{}
	tc.StartWithStoreConfig(t, stopper, cfg)
	repl := tc.repl

	assertMigrationComplete := func(t *testing.T, exp bool) {
		t.Helper()
		assertUsingRangeAppliedState(t, repl, exp)
		assertRangeAppliedStateRelatedKeysExist(ctx, t, repl, exp)
	}

	// We should not be using the AppliedStateKey to begin with.
	assertMigrationComplete(t, false)

	// Begin performing some writes regularly.
	errCh := make(chan *roachpb.Error, 1)
	pArgs := putArgs(roachpb.Key("a"), []byte("val"))
	stopper.RunWorker(ctx, func(ctx context.Context) {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if _, pErr := tc.SendWrapped(&pArgs); pErr != nil {
					errCh <- pErr
					return
				}
			case <-stopper.ShouldQuiesce():
				return
			}
		}
	})

	migrateCh := make(chan struct{}, 1)
	repl.mu.Lock()
	repl.store.TestingKnobs().TestingApplyFilter =
		func(args storagebase.ApplyFilterArgs) *roachpb.Error {
			if args.State != nil && args.State.UsingAppliedStateKey {
				select {
				case migrateCh <- struct{}{}:
				default:
				}
			}
			return nil
		}
	repl.mu.Unlock()

	// No attempt should be made to migrate to the AppliedStateKey
	select {
	case <-time.After(20 * time.Millisecond):
		// Expected. Assert again that we're not yet using the AppliedStateKey.
		assertMigrationComplete(t, false)
	case <-migrateCh:
		t.Fatalf("unexpected migration attempt")
	case pErr := <-errCh:
		t.Fatalf("unexpected error %s", pErr)
	}

	// Bump the cluster version to trigger the ReplicaAppliedState key migration.
	cv := cluster.ClusterVersion{
		MinimumVersion: cluster.VersionByKey(cluster.VersionRangeAppliedStateKey),
		UseVersion:     cluster.BinaryServerVersion,
	}
	if err := cfg.Settings.InitializeVersion(cv); err != nil {
		t.Fatal(err)
	}

	// Wait until we see a proposal attempt a migration.
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("expected to see a migration attempt")
	case <-migrateCh:
		// Expected.
	case pErr := <-errCh:
		t.Fatalf("unexpected error %s", pErr)
	}

	// Assert that the replica is now using the ReplicaAppliedState key. The
	// migration will also trigger assertStateLocked, so we're sure that the
	// in-memory and on-disk ReplicaStates are not diverging.
	assertMigrationComplete(t, true)
}

func TestReplicaShouldCampaignOnWake(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const storeID = roachpb.StoreID(1)

	myLease := roachpb.Lease{
		Replica: roachpb.ReplicaDescriptor{
			StoreID: storeID,
		},
	}
	otherLease := roachpb.Lease{
		Replica: roachpb.ReplicaDescriptor{
			StoreID: roachpb.StoreID(2),
		},
	}

	followerWithoutLeader := raft.Status{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      0,
		},
	}
	followerWithLeader := raft.Status{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      1,
		},
	}
	candidate := raft.Status{
		SoftState: raft.SoftState{
			RaftState: raft.StateCandidate,
			Lead:      0,
		},
	}
	leader := raft.Status{
		SoftState: raft.SoftState{
			RaftState: raft.StateLeader,
			Lead:      1,
		},
	}

	tests := []struct {
		leaseStatus storagepb.LeaseStatus
		lease       roachpb.Lease
		raftStatus  raft.Status
		exp         bool
	}{
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, myLease, followerWithoutLeader, true},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, otherLease, followerWithoutLeader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, myLease, followerWithLeader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, otherLease, followerWithLeader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, myLease, candidate, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, otherLease, candidate, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, myLease, leader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_VALID}, otherLease, leader, false},

		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, myLease, followerWithoutLeader, true},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, otherLease, followerWithoutLeader, true},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, myLease, followerWithLeader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, otherLease, followerWithLeader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, myLease, candidate, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, otherLease, candidate, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, myLease, leader, false},
		{storagepb.LeaseStatus{State: storagepb.LeaseState_EXPIRED}, otherLease, leader, false},
	}

	for i, test := range tests {
		v := shouldCampaignOnWake(test.leaseStatus, test.lease, storeID, test.raftStatus)
		if v != test.exp {
			t.Errorf("%d: expected %v but got %v", i, test.exp, v)
		}
	}
}

func TestRangeStatsRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	keyPrefix := roachpb.RKey("dummy-prefix")

	// Write some random data to the range and verify that a RangeStatsRequest
	// returns the same MVCC stats as the replica's in-memory state.
	WriteRandomDataToRange(t, tc.store, tc.repl.RangeID, keyPrefix)
	expMS := tc.repl.GetMVCCStats()
	res, pErr := client.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		RangeID: tc.repl.RangeID,
	}, &roachpb.RangeStatsRequest{})
	if pErr != nil {
		t.Fatal(pErr)
	}
	resMS := res.(*roachpb.RangeStatsResponse).MVCCStats
	require.Equal(t, expMS, resMS)

	// Write another key to the range and verify that the MVCC stats returned
	// by a RangeStatsRequest reflect the additional key.
	key := append(keyPrefix, roachpb.RKey("123")...)
	if err := tc.store.DB().Put(ctx, key, "123"); err != nil {
		t.Fatal(err)
	}
	res, pErr = client.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		RangeID: tc.repl.RangeID,
	}, &roachpb.RangeStatsRequest{})
	if pErr != nil {
		t.Fatal(pErr)
	}
	resMS = res.(*roachpb.RangeStatsResponse).MVCCStats
	// Only verify the update is reflected in the key/value counts. Verifying
	// the byte count would couple this test too tightly to our encoding scheme.
	require.Equal(t, expMS.KeyCount+1, resMS.KeyCount)
	require.Equal(t, expMS.ValCount+1, resMS.ValCount)
	require.Equal(t, expMS.LiveCount+1, resMS.LiveCount)
}

// Test that an EndTransaction(commit=false) request that doesn't find its
// transaction record doesn't return an error.
// This is relied upon by the client which liberally sends rollbacks even when
// it's unclear whether the txn record has been written.
func TestRollbackMissingTxnRecordNoError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)

	key := roachpb.Key("bogus key")
	txn := roachpb.MakeTransaction("test", key,
		roachpb.NormalUserPriority, enginepb.SERIALIZABLE,
		tc.Clock().Now(), tc.Clock().MaxOffset().Nanoseconds())

	res, pErr := client.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		RangeID: tc.repl.RangeID,
		Txn:     &txn,
	}, &roachpb.EndTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Commit: false,
	})
	if pErr != nil {
		t.Fatal(pErr)
	}
	if res.Header().Txn == nil {
		t.Fatal("expected Txn to be filled on the response")
	}

	// For good measure, let's take the opportunity to check replay protection for
	// a BeginTransaction arriving after the rollback.
	_, pErr = client.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		RangeID: tc.repl.RangeID,
		Txn:     &txn,
	}, &roachpb.BeginTransactionRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	})
	// Note that, as explained in the abort reason comments, the server generates
	// a retryable TransactionAbortedError, but if there's actually a sort of
	// replay at work and a client is still waiting for the error, the error would
	// be transformed into something more ambiguous on the way.
	expErr := "TransactionAbortedError(ABORT_REASON_ALREADY_COMMITTED_OR_ROLLED_BACK_POSSIBLE_REPLAY)"
	if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
		t.Errorf("expected %s; got %v", expErr, pErr)
	}
}
