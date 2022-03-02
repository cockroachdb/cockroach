// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

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
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	raft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"golang.org/x/net/trace"
)

// allSpans is a SpanSet that covers *everything* for use in tests that don't
// care about properly declaring their spans.
func allSpans() *spanset.SpanSet {
	ss := spanset.New()
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key:    roachpb.KeyMin,
		EndKey: roachpb.KeyMax,
	})
	// Local keys (see `keys.LocalPrefix`).
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key:    append([]byte("\x01"), roachpb.KeyMin...),
		EndKey: append([]byte("\x01"), roachpb.KeyMax...),
	})
	return ss
}

// allSpansGuard returns a concurrency guard that indicates that it provides
// isolation across all key spans for use in tests that don't care about
// properly declaring their spans or sequencing with the concurrency manager.
func allSpansGuard() *concurrency.Guard {
	return &concurrency.Guard{
		Req: concurrency.Request{
			LatchSpans: allSpans(),
			LockSpans:  spanset.New(),
		},
	}
}

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

// Create a Raft status that shows everyone fully up to date.
func upToDateRaftStatus(repls []roachpb.ReplicaDescriptor) *raft.Status {
	prs := make(map[uint64]tracker.Progress)
	for _, repl := range repls {
		prs[uint64(repl.ReplicaID)] = tracker.Progress{
			State: tracker.StateReplicate,
			Match: 100,
		}
	}
	return &raft.Status{
		BasicStatus: raft.BasicStatus{
			HardState: raftpb.HardState{Commit: 100},
			SoftState: raft.SoftState{Lead: 1, RaftState: raft.StateLeader},
		},
		Progress: prs,
	}
}

// testContext contains all the objects necessary to test a Range.
// In most cases, simply call Start(t) (and later Stop()) on a zero-initialized
// testContext{}. Any fields which are initialized to non-nil values
// will be used as-is.
type testContext struct {
	testing.TB
	transport   *RaftTransport
	store       *Store
	repl        *Replica
	rangeID     roachpb.RangeID
	gossip      *gossip.Gossip
	engine      storage.Engine
	manualClock *hlc.ManualClock
}

func (tc *testContext) Clock() *hlc.Clock {
	return tc.store.cfg.Clock
}

// Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(ctx context.Context, t testing.TB, stopper *stop.Stopper) {
	tc.manualClock = hlc.NewManualClock(123)
	cfg := TestStoreConfig(
		hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	// testContext tests like to move the manual clock around and assume that they can write at past
	// timestamps.
	cfg.TestingKnobs.DontCloseTimestamps = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)
}

// StartWithStoreConfig initializes the test context with a single
// range covering the entire keyspace.
func (tc *testContext) StartWithStoreConfig(
	ctx context.Context, t testing.TB, stopper *stop.Stopper, cfg StoreConfig,
) {
	tc.StartWithStoreConfigAndVersion(ctx, t, stopper, cfg, cfg.Settings.Version.BinaryVersion())
}

// StartWithStoreConfigAndVersion is like StartWithStoreConfig but additionally
// allows control over the bootstrap version.
func (tc *testContext) StartWithStoreConfigAndVersion(
	ctx context.Context,
	t testing.TB,
	stopper *stop.Stopper,
	cfg StoreConfig,
	bootstrapVersion roachpb.Version,
) {
	tc.TB = t
	require.Nil(t, tc.gossip)
	require.Nil(t, tc.transport)
	require.Nil(t, tc.engine)
	require.Nil(t, tc.store)
	require.Nil(t, tc.repl)

	// testContext doesn't make use of the span configs infra (uses gossip
	// in fact); it's not able to craft ranges that know that they're part
	// of system tables and therefore can opt out of strict GC enforcement.
	cfg.TestingKnobs.IgnoreStrictGCEnforcement = true

	// NB: this also sets up fake zone config handlers via TestingSetupZoneConfigHook.
	//
	// TODO(tbg): the above is not good, figure out which tests need this and make them
	// call it directly.
	//
	// NB: split queue, merge queue, and scanner are also disabled.
	store := createTestStoreWithoutStart(
		ctx, t, stopper, testStoreOpts{
			createSystemRanges: false,
			bootstrapVersion:   bootstrapVersion,
		}, &cfg,
	)
	if err := store.Start(ctx, stopper); err != nil {
		t.Fatal(err)
	}
	store.WaitForInit()
	repl, err := store.GetReplica(1)
	require.NoError(t, err)
	tc.repl = repl
	tc.rangeID = repl.RangeID
	tc.gossip = store.cfg.Gossip
	tc.transport = store.cfg.Transport
	tc.engine = store.engine
	tc.store = store
	// TODO(tbg): see if this is needed. Would like to remove it.
	require.NoError(t, tc.initConfigs(t))
}

func (tc *testContext) Sender() kv.Sender {
	return kv.Wrap(tc.repl, func(ba roachpb.BatchRequest) roachpb.BatchRequest {
		if ba.RangeID == 0 {
			ba.RangeID = 1
		}
		if ba.Timestamp.IsEmpty() {
			if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
				tc.Fatal(err)
			}
		}
		if baClockTS, ok := ba.Timestamp.TryToClockTimestamp(); ok {
			tc.Clock().Update(baClockTS)
		}
		return ba
	})
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it
func (tc *testContext) SendWrappedWith(
	h roachpb.Header, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	return kv.SendWrappedWith(context.Background(), tc.Sender(), h, args)
}

// SendWrapped is identical to SendWrappedWith with a zero header.
func (tc *testContext) SendWrapped(args roachpb.Request) (roachpb.Response, *roachpb.Error) {
	return tc.SendWrappedWith(roachpb.Header{}, args)
}

// initConfigs creates default configuration entries.
//
// TODO(ajwerner): Remove this in 22.2.
func (tc *testContext) initConfigs(t testing.TB) error {
	// Put an empty system config into gossip so that gossip callbacks get
	// run. We're using a fake config, but it's hooked into SystemConfig.
	if err := tc.gossip.AddInfoProto(gossip.KeyDeprecatedSystemConfig,
		&config.SystemConfigEntries{}, 0); err != nil {
		return err
	}

	testutils.SucceedsSoon(t, func() error {
		if cfg := tc.gossip.DeprecatedGetSystemConfig(); cfg == nil {
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
	newDesc.InternalReplicas = append(newDesc.InternalReplicas, secondReplica)
	newDesc.NextReplicaID = 3

	dbDescKV, err := tc.store.DB().Get(ctx, keys.RangeDescriptorKey(oldDesc.StartKey))
	if err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	var dbDesc roachpb.RangeDescriptor
	if err := dbDescKV.Value.GetProto(&dbDesc); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	if !oldDesc.Equal(&dbDesc) {
		return roachpb.ReplicaDescriptor{}, errors.Errorf(`descs didn't match: %v vs %v`, oldDesc, dbDesc)
	}

	// Update the "on-disk" replica state, so that it doesn't diverge from what we
	// have in memory. At the time of this writing, this is not actually required
	// by the tests using this functionality, but it seems sane to do.
	ba := kv.Batch{
		Header: roachpb.Header{Timestamp: tc.Clock().Now()},
	}
	descKey := keys.RangeDescriptorKey(oldDesc.StartKey)
	if err := updateRangeDescriptor(ctx, &ba, descKey, dbDescKV.Value.TagAndDataBytes(), &newDesc); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	if err := tc.store.DB().Run(ctx, &ba); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}

	tc.repl.setDescRaftMuLocked(ctx, &newDesc)
	tc.repl.raftMu.Lock()
	tc.repl.mu.RLock()
	tc.repl.assertStateRaftMuLockedReplicaMuRLocked(ctx, tc.engine)
	tc.repl.mu.RUnlock()
	tc.repl.raftMu.Unlock()
	return secondReplica, nil
}

func newTransaction(
	name string, baseKey roachpb.Key, userPriority roachpb.UserPriority, clock *hlc.Clock,
) *roachpb.Transaction {
	var offset int64
	var now hlc.Timestamp
	if clock != nil {
		offset = clock.MaxOffset().Nanoseconds()
		now = clock.Now()
	}
	txn := roachpb.MakeTransaction(name, baseKey, userPriority, now, offset, 0)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	withSeq := func(req roachpb.Request, seq enginepb.TxnSeq) roachpb.Request {
		h := req.Header()
		h.Sequence = seq
		req.SetHeader(h)
		return req
	}
	makeReqs := func(reqs ...roachpb.Request) []roachpb.RequestUnion {
		ru := make([]roachpb.RequestUnion, len(reqs))
		for i, r := range reqs {
			ru[i].MustSetInner(r)
		}
		return ru
	}

	noReqs := makeReqs()
	getReq := makeReqs(withSeq(&roachpb.GetRequest{}, 0))
	putReq := makeReqs(withSeq(&roachpb.PutRequest{}, 1))
	etReq := makeReqs(withSeq(&roachpb.EndTxnRequest{Commit: true}, 1))
	txnReqs := makeReqs(
		withSeq(&roachpb.PutRequest{}, 1),
		withSeq(&roachpb.EndTxnRequest{Commit: true}, 2),
	)
	txnReqsRequire1PC := makeReqs(
		withSeq(&roachpb.PutRequest{}, 1),
		withSeq(&roachpb.EndTxnRequest{Commit: true, Require1PC: true}, 2),
	)

	testCases := []struct {
		ru           []roachpb.RequestUnion
		isNonTxn     bool
		canForwardTS bool
		isRestarted  bool
		isWTO        bool // isWTO implies isTSOff
		isTSOff      bool
		exp1PC       bool
	}{
		{ru: noReqs, isNonTxn: true, exp1PC: false},
		{ru: noReqs, exp1PC: false},
		{ru: getReq, exp1PC: false},
		{ru: putReq, exp1PC: false},
		{ru: etReq, exp1PC: true},
		{ru: etReq, isTSOff: true, exp1PC: false},
		{ru: etReq, isWTO: true, exp1PC: false},
		{ru: etReq, isRestarted: true, exp1PC: false},
		{ru: etReq, isRestarted: true, isTSOff: true, exp1PC: false},
		{ru: etReq, isRestarted: true, isWTO: true, isTSOff: true, exp1PC: false},
		{ru: etReq, canForwardTS: true, exp1PC: true},
		{ru: etReq, canForwardTS: true, isTSOff: true, exp1PC: true},
		{ru: etReq, canForwardTS: true, isWTO: true, exp1PC: true},
		{ru: etReq, canForwardTS: true, isRestarted: true, exp1PC: false},
		{ru: etReq, canForwardTS: true, isRestarted: true, isTSOff: true, exp1PC: false},
		{ru: etReq, canForwardTS: true, isRestarted: true, isWTO: true, isTSOff: true, exp1PC: false},
		{ru: txnReqs[:1], exp1PC: false},
		{ru: txnReqs[1:], exp1PC: false},
		{ru: txnReqs, exp1PC: true},
		{ru: txnReqs, isTSOff: true, exp1PC: false},
		{ru: txnReqs, isWTO: true, exp1PC: false},
		{ru: txnReqs, isRestarted: true, exp1PC: false},
		{ru: txnReqs, isRestarted: true, isTSOff: true, exp1PC: false},
		{ru: txnReqs, isRestarted: true, isWTO: true, exp1PC: false},
		{ru: txnReqs[:1], canForwardTS: true, exp1PC: false},
		{ru: txnReqs[1:], canForwardTS: true, exp1PC: false},
		{ru: txnReqs, canForwardTS: true, exp1PC: true},
		{ru: txnReqs, canForwardTS: true, isTSOff: true, exp1PC: true},
		{ru: txnReqs, canForwardTS: true, isWTO: true, exp1PC: true},
		{ru: txnReqs, canForwardTS: true, isRestarted: true, exp1PC: false},
		{ru: txnReqs, canForwardTS: true, isRestarted: true, isTSOff: true, exp1PC: false},
		{ru: txnReqs, canForwardTS: true, isRestarted: true, isWTO: true, exp1PC: false},
		{ru: txnReqsRequire1PC[:1], exp1PC: false},
		{ru: txnReqsRequire1PC[1:], exp1PC: false},
		{ru: txnReqsRequire1PC, exp1PC: true},
		{ru: txnReqsRequire1PC, isTSOff: true, exp1PC: false},
		{ru: txnReqsRequire1PC, isWTO: true, exp1PC: false},
		{ru: txnReqsRequire1PC, isRestarted: true, exp1PC: true},
		{ru: txnReqsRequire1PC, isRestarted: true, isTSOff: true, exp1PC: false},
		{ru: txnReqsRequire1PC, isRestarted: true, isWTO: true, exp1PC: false},
	}

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	for i, c := range testCases {
		t.Run(
			fmt.Sprintf("%d:isNonTxn:%t,canForwardTS:%t,isRestarted:%t,isWTO:%t,isTSOff:%t",
				i, c.isNonTxn, c.canForwardTS, c.isRestarted, c.isWTO, c.isTSOff),
			func(t *testing.T) {
				ba := roachpb.BatchRequest{Requests: c.ru}
				if !c.isNonTxn {
					ba.Txn = newTransaction("txn", roachpb.Key("a"), 1, clock)
					if c.canForwardTS {
						ba.CanForwardReadTimestamp = true
					}
					if c.isRestarted {
						ba.Txn.Restart(-1, 0, clock.Now())
					}
					if c.isWTO {
						ba.Txn.WriteTooOld = true
						c.isTSOff = true
					}
					if c.isTSOff {
						ba.Txn.WriteTimestamp = ba.Txn.ReadTimestamp.Add(1, 0)
					}
				} else {
					require.False(t, c.isRestarted)
					require.False(t, c.isWTO)
					require.False(t, c.isTSOff)
				}

				// Emulate what a server actually does and bump the write timestamp when
				// possible. This makes some batches with diverged read and write
				// timestamps pass isOnePhaseCommit().
				maybeBumpReadTimestampToWriteTimestamp(ctx, &ba, allSpansGuard())

				if is1PC := isOnePhaseCommit(&ba); is1PC != c.exp1PC {
					t.Errorf("expected 1pc=%t; got %t", c.exp1PC, is1PC)
				}
			})
	}
}

// TestReplicaContains verifies that the range uses Key.Address() in
// order to properly resolve addresses for local keys.
func TestReplicaContains(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	desc := &roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	}

	// This test really only needs a hollow shell of a Replica.
	r := &Replica{}
	r.mu.state.Desc = desc
	r.rangeStr.store(0, desc)

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
	ctx := context.Background()
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	st := r.CurrentLeaseStatus(ctx)
	leaseReq := &roachpb.RequestLeaseRequest{
		Lease:     *l,
		PrevLease: st.Lease,
	}
	ba.Add(leaseReq)
	_, tok := r.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, pErr := r.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	gArgs := getArgs(roachpb.Key("a"))

	// Try consistent read and verify success.

	if _, err := tc.SendWrapped(&gArgs); err != nil {
		t.Errorf("expected success on consistent read: %+v", err)
	}

	// Try a read commmitted read and an inconsistent read, both within a
	// transaction.
	txn := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())
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
	start := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
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
// - while the transfer is in progress, reads should block while attempting to
// acquire latches.
// - if a transfer fails, the pre-existing lease does not start being used
// again. Instead, a new lease needs to be obtained. This is because, even
// though the transfer got an error, that error is considered ambiguous as the
// transfer might still apply.
func TestBehaviorDuringLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "transferSucceeds", func(t *testing.T, transferSucceeds bool) {
		manual := hlc.NewManualClock(123)
		clock := hlc.NewClock(manual.UnixNano, 100*time.Millisecond)
		tc := testContext{manualClock: manual}
		tsc := TestStoreConfig(clock)
		var leaseAcquisitionTrap atomic.Value
		tsc.TestingKnobs.DisableAutomaticLeaseRenewal = true
		tsc.TestingKnobs.LeaseRequestEvent = func(ts hlc.Timestamp, _ roachpb.StoreID, _ roachpb.RangeID) *roachpb.Error {
			val := leaseAcquisitionTrap.Load()
			if val == nil {
				return nil
			}
			trapCallback := val.(func(ts hlc.Timestamp))
			if trapCallback != nil {
				trapCallback(ts)
			}
			return nil
		}
		transferSem := make(chan struct{})
		tsc.TestingKnobs.EvalKnobs.TestingPostEvalFilter =
			func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
				if _, ok := filterArgs.Req.(*roachpb.TransferLeaseRequest); ok {
					// Notify the test that the transfer has been trapped.
					transferSem <- struct{}{}
					// Wait for the test to unblock the transfer.
					<-transferSem
					if !transferSucceeds {
						// Return an error, so that the pendingLeaseRequest
						// considers the transfer failed.
						return roachpb.NewErrorf("injected transfer error")
					}
					return nil
				}
				return nil
			}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.StartWithStoreConfig(ctx, t, stopper, tsc)
		secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
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
			err := tc.repl.AdminTransferLease(ctx, secondReplica.StoreID)
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
		pending := tc.repl.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID)
		tc.repl.mu.Unlock()
		if !pending {
			t.Fatalf("expected transfer to be in progress, and it wasn't")
		}

		// Check that, while the transfer is on-going, reads are blocked.
		readResChan := make(chan error)
		go func() {
			_, pErr := tc.SendWrapped(&gArgs)
			readResChan <- pErr.GoError()
		}()
		select {
		case err := <-readResChan:
			t.Fatalf("expected read to block, completed with err=%v", err)
		case <-time.After(10 * time.Millisecond):
		}

		// Set up a hook to detect lease requests, in case this transfer fails.
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

		// Unblock the transfer and wait for the pendingLeaseRequest to clear
		// the transfer state.
		transferSem <- struct{}{}
		if err := <-transferResChan; err != nil {
			t.Fatal(err)
		}

		testutils.SucceedsSoon(t, func() error {
			tc.repl.mu.Lock()
			defer tc.repl.mu.Unlock()
			pending := tc.repl.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID)
			if pending {
				return errors.New("transfer pending")
			}
			return nil
		})

		if transferSucceeds {
			// Check that the read is rejected and redirected to the transfer
			// target.
			err = <-readResChan
			require.Error(t, err)
			var lErr *roachpb.NotLeaseHolderError
			require.True(t, errors.As(err, &lErr))
			require.Equal(t, secondReplica.StoreID, lErr.LeaseHolder.StoreID)
		} else {
			// Check that the replica doesn't use its lease, even though there's
			// no longer a transfer in progress. This is because, even though
			// the transfer got an error, that error is considered ambiguous as
			// the transfer might still apply.
			//
			// Concretely, we're going to check that a read triggered a new
			// lease acquisition and that is then succeeds.
			select {
			case <-leaseAcquisitionCh:
			case <-time.After(time.Second):
				t.Fatalf("read did not acquire a new lease")
			}

			err = <-readResChan
			require.NoError(t, err)
		}
	})
}

// TestApplyCmdLeaseError verifies that when during application of a Raft
// command the proposing node no longer holds the range lease, an error is
// returned. This prevents regression of #1483.
func TestApplyCmdLeaseError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pArgs := putArgs(roachpb.Key("a"), []byte("asd"))

	// Lose the lease.
	tc.manualClock.Set(leaseExpiry(tc.repl))
	start := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
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
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	lease, _ := tc.repl.GetLease()
	invalidLease := lease
	invalidLease.Sequence++
	invalidLease.Replica.StoreID += 12345

	raftCmd := kvserverpb.RaftCommand{
		ProposerLeaseSequence: lease.Sequence,
		ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
			IsLeaseRequest: true,
			State: &kvserverpb.ReplicaState{
				Lease: &invalidLease,
			},
		},
	}
	tc.repl.mu.Lock()
	_, _, pErr := checkForcedErr(
		ctx, makeIDKey(), &raftCmd, false, /* isLocal */
		&tc.repl.mu.state,
	)
	tc.repl.mu.Unlock()
	if _, isErr := pErr.GetDetail().(*roachpb.LeaseRejectedError); !isErr {
		t.Fatal(pErr)
	} else if !testutils.IsPError(pErr, "replica not part of range") {
		t.Fatal(pErr)
	}
}

// TestReplicaRangeMismatchRedirect tests two behaviors that should occur.
// - Following a Range split, the client may send BatchRequests based on stale
//   cache data targeting the wrong range. Internally this triggers a
//   RangeKeyMismatchError, but in the cases where the RHS of the range is still
//   present on the local store, we opportunistically retry server-side by
//   re-routing the request to the right range. No error is bubbled up to the
//   client.
// - This test also ensures that after a successful server-side retry attempt we
//   bubble up the most up-to-date RangeInfos for the client to update its range
//   cache.
func TestReplicaRangeMismatchRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.RKey("a")
	firstRepl := tc.store.LookupReplica(key)
	newRepl := splitTestRange(tc.store, key, t)
	if _, pErr := newRepl.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	gArgs := getArgs(roachpb.Key("b"))
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{
		RangeID: 1,
	}
	ba.Add(&gArgs)

	br, pErr := tc.store.Send(context.Background(), ba)

	require.Nil(t, pErr)
	rangeInfos := br.RangeInfos

	// Here we expect 2 ranges, where [A1, A2] is returned
	// where A represents the RangeInfos aggregated by send(),
	// before redirecting the query to the correct range.
	require.Len(t, rangeInfos, 2)
	mismatchedDesc := rangeInfos[0].Desc
	suggestedDesc := rangeInfos[1].Desc
	if mismatchedDesc.RangeID != firstRepl.RangeID {
		t.Errorf("expected mismatched range to be %d, found %v", firstRepl.RangeID, mismatchedDesc)
	}
	if suggestedDesc.RangeID != newRepl.RangeID {
		t.Errorf("expected suggested range to be %d, found %v", newRepl.RangeID, suggestedDesc)
	}
}

func TestReplicaLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var filterErr atomic.Value
	applyFilter := func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		if pErr := filterErr.Load(); pErr != nil {
			return 0, pErr.(*roachpb.Error)
		}
		return 0, nil
	}

	tc.manualClock = hlc.NewManualClock(123)
	tsc := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	tsc.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tsc.TestingKnobs.TestingApplyFilter = applyFilter
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Test that leases with invalid times are rejected.
	// Start leases at a point that avoids overlapping with the existing lease.
	leaseDuration := tc.store.cfg.RangeLeaseActiveDuration()
	start := hlc.ClockTimestamp{WallTime: (time.Second + leaseDuration).Nanoseconds(), Logical: 0}
	for _, lease := range []roachpb.Lease{
		{Start: start, Expiration: &hlc.Timestamp{}},
	} {
		if _, err := batcheval.RequestLease(ctx, tc.store.Engine(),
			batcheval.CommandArgs{
				EvalCtx: NewReplicaEvalContext(tc.repl, allSpans()),
				Args: &roachpb.RequestLeaseRequest{
					Lease: lease,
				},
			}, &roachpb.RequestLeaseResponse{}); !testutils.IsError(err, "replica not found") {
			t.Fatalf("unexpected error: %+v", err)
		}
	}

	if !tc.repl.OwnsValidLease(ctx, tc.Clock().NowAsClockTimestamp()) {
		t.Errorf("expected lease on range start")
	}
	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now.ToTimestamp().Add(10, 0).UnsafeToClockTimestamp(),
		Expiration: now.ToTimestamp().Add(20, 0).Clone(),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}
	if tc.repl.OwnsValidLease(ctx, tc.Clock().Now().Add(15, 0).UnsafeToClockTimestamp()) {
		t.Errorf("expected second replica to have range lease")
	}

	{
		_, pErr := tc.repl.redirectOnOrAcquireLease(ctx)
		if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok || lErr == nil {
			t.Fatalf("wanted NotLeaseHolderError, got %s", pErr)
		}
	}
	// Advance clock past expiration and verify that another has
	// range lease will not be true.
	tc.manualClock.Increment(21) // 21ns have passed
	if tc.repl.OwnsValidLease(ctx, tc.Clock().NowAsClockTimestamp()) {
		t.Errorf("expected another replica to have expired lease")
	}

	// Verify that command returns NotLeaseHolderError when lease is rejected.
	filterErr.Store(roachpb.NewError(&roachpb.LeaseRejectedError{Message: "replica not found"}))

	{
		_, err := tc.repl.redirectOnOrAcquireLease(ctx)
		if _, ok := err.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected %T, got %s", &roachpb.NotLeaseHolderError{}, err)
		}
	}
}

func TestReplicaNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
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
		_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: now.ToTimestamp()}, test)

		if _, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); !ok {
			t.Errorf("%d: expected not lease holder error: %s", i, pErr)
		}
	}
}

// TestReplicaLeaseCounters verifies leaseRequest metrics counters are updated
// correctly after a lease request.
func TestReplicaLeaseCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer EnableLeaseHistory(100)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var tc testContext
	cfg := TestStoreConfig(nil)
	// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
	// refreshes so that our lease proposal does not risk being rejected
	// with an AmbiguousResultError.
	cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
	cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

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

	now := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
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
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
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
//
// TODO(ajwerner): Delete this test in 22.2.
func TestReplicaGossipConfigsOnLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	// Use the TestingBinaryMinSupportedVersion for bootstrap because we won't
	// gossip the system config once the current version is finalized.
	cfg.Settings = cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false,
	)
	require.NoError(t, cfg.Settings.Version.SetActiveVersion(ctx, clusterversion.ClusterVersion{
		Version: clusterversion.TestingBinaryMinSupportedVersion,
	}))
	tc.StartWithStoreConfigAndVersion(ctx, t, stopper, cfg,
		clusterversion.TestingBinaryMinSupportedVersion)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Write some arbitrary data in the system config span.
	key := keys.SystemSQLCodec.TablePrefix(keys.MaxSystemConfigDescID)
	var val roachpb.Value
	val.SetInt(42)
	if err := storage.MVCCPut(context.Background(), tc.engine, nil, key, hlc.Timestamp{}, val, nil); err != nil {
		t.Fatal(err)
	}

	// If this actually failed, we would have gossiped from MVCCPutProto.
	// Unlikely, but why not check.
	if cfg := tc.gossip.DeprecatedGetSystemConfig(); cfg != nil {
		if nv := len(cfg.Values); nv == 1 && cfg.Values[nv-1].Key.Equal(key) {
			t.Errorf("unexpected gossip of system config: %s", cfg)
		}
	}

	// Expire our own lease which we automagically acquired due to being
	// first range and config holder.
	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()

	// Give lease to someone else.
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now,
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	// Expire that lease.
	tc.manualClock.Increment(11 + int64(tc.Clock().MaxOffset())) // advance time
	now = tc.Clock().NowAsClockTimestamp()

	ch := tc.gossip.DeprecatedRegisterSystemConfigChannel()
	select {
	case <-ch:
	default:
	}

	// Give lease to this range.
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		Start:      now.ToTimestamp().Add(11, 0).UnsafeToClockTimestamp(),
		Expiration: now.ToTimestamp().Add(20, 0).Clone(),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 1,
			NodeID:    1,
			StoreID:   1,
		},
	}); err != nil {
		t.Fatal(err)
	}

	testutils.SucceedsSoon(t, func() error {
		sysCfg := tc.gossip.DeprecatedGetSystemConfig()
		if sysCfg == nil {
			return errors.Errorf("no system config yet")
		}
		var found bool
		for _, cur := range sysCfg.Values {
			if key.Equal(cur.Key) {
				found = true
				break
			}
		}
		if !found {
			return errors.Errorf("key %s not found in SystemConfig", key)
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	// Disable raft log truncation which confuses this test.
	cfg.TestingKnobs.DisableRaftLogQueue = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.Background())
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
		// Make a unique ProposedTS. Without this, we don't have replay protection.
		// This can bite us at i=2, where the lease stays entirely identical and
		// thus matches the predecessor lease even when replayed, meaning that the
		// replay will also apply. This wouldn't be an issue (after all, the lease
		// stays the same) but it does mean that the corresponding pending inflight
		// proposal gets finished twice, which tickles an assertion in
		// ApplySideEffects.
		propTS := test.start.UnsafeToClockTimestamp()
		propTS.Logical = int32(i)
		if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
			ProposedTS: &propTS,
			Start:      test.start.UnsafeToClockTimestamp(),
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
		rTS, _ := tc.repl.store.tsCache.GetMax(roachpb.Key("a"), nil /* end */)

		if test.expLowWater == 0 {
			continue
		}
		if rTS.WallTime != test.expLowWater {
			t.Errorf("%d: expected low water %d; got max=%d", i, test.expLowWater, rTS.WallTime)
		}
	}
}

// TestReplicaLeaseRejectUnknownRaftNodeID ensures that a replica cannot
// obtain the range lease if it is not part of the current range descriptor.
// TODO(mrtracy): This should probably be tested in client_raft_test package,
// using a real second store.
func TestReplicaLeaseRejectUnknownRaftNodeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: hlc.NewManualClock(123)}
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	tc.manualClock.Set(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	lease := &roachpb.Lease{
		Start:      now,
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
	}
	st := tc.repl.CurrentLeaseStatus(ctx)
	ba := roachpb.BatchRequest{}
	ba.Timestamp = tc.repl.store.Clock().Now()
	ba.Add(&roachpb.RequestLeaseRequest{Lease: *lease})
	_, tok := tc.repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, pErr := tc.repl.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
	if pErr == nil {
		// Next if the command was committed, wait for the range to apply it.
		// TODO(bdarnell): refactor to a more conventional error-handling pattern.
		// Remove ambiguity about where the "replica not found" error comes from.
		pErr = (<-ch).Err
	}
	if !testutils.IsPError(pErr, "replica.*not found") {
		t.Errorf("unexpected error obtaining lease for invalid store: %v", pErr)
	}
}

// Test that draining nodes only take the lease if they're the leader.
func TestReplicaDrainLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	clusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				NodeLiveness: NodeLivenessTestingKnobs{
					// This test waits for an epoch-based lease to expire, so we're setting the
					// liveness duration as low as possible while still keeping the test stable.
					LivenessDuration: 2000 * time.Millisecond,
					RenewalDuration:  1000 * time.Millisecond,
				},
				Store: &StoreTestingKnobs{
					// We eliminate clock offsets in order to eliminate the stasis period of
					// leases. Otherwise we'd need to make leases longer.
					MaxOffset: time.Nanosecond,
				},
			},
		},
	}
	tc := serverutils.StartNewTestCluster(t, 2, clusterArgs)
	defer tc.Stopper().Stop(ctx)
	rngKey := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, rngKey, tc.Target(1))

	s1 := tc.Server(0)
	s2 := tc.Server(1)
	store1, err := s1.GetStores().(*Stores).GetStore(s1.GetFirstStoreID())
	require.NoError(t, err)
	store2, err := s2.GetStores().(*Stores).GetStore(s2.GetFirstStoreID())
	require.NoError(t, err)

	rd := tc.LookupRangeOrFatal(t, rngKey)
	r1, err := store1.GetReplica(rd.RangeID)
	require.NoError(t, err)
	status := r1.CurrentLeaseStatus(ctx)
	require.True(t, status.Lease.OwnedBy(store1.StoreID()), "someone else got the lease: %s", status)
	// We expect the lease to be valid, but don't check that because, under race, it might have
	// expired already.

	// Stop n1's heartbeats and wait for the lease to expire.
	log.Infof(ctx, "test: suspending heartbeats for n1")
	cleanup := s1.NodeLiveness().(*liveness.NodeLiveness).PauseAllHeartbeatsForTest()
	defer cleanup()

	testutils.SucceedsSoon(t, func() error {
		status := r1.CurrentLeaseStatus(ctx)
		require.True(t, status.Lease.OwnedBy(store1.StoreID()), "someone else got the lease: %s", status)
		if status.State == kvserverpb.LeaseState_VALID {
			return errors.New("lease still valid")
		}
		// We need to wait for the stasis state to pass too; during stasis other
		// replicas can't take the lease.
		if status.State == kvserverpb.LeaseState_UNUSABLE {
			return errors.New("lease still in stasis")
		}
		return nil
	})

	require.Equal(t, r1.RaftStatus().Lead, uint64(r1.ReplicaID()),
		"expected leadership to still be on the first replica")

	// Wait until n1 has heartbeat its liveness record (epoch >= 1) and n2
	// knows about it. Otherwise, the following could occur:
	//
	// - n1's heartbeats to epoch 1 and acquires lease
	// - n2 doesn't receive this yet (gossip)
	// - when n2 is asked to acquire the lease, it uses a lease with epoch 1
	//   but the liveness record with epoch zero
	// - lease status is ERROR, lease acquisition (and thus test) fails.
	testutils.SucceedsSoon(t, func() error {
		nl, ok := s2.NodeLiveness().(*liveness.NodeLiveness).GetLiveness(s1.NodeID())
		if !ok {
			return errors.New("no liveness record for n1")
		}
		if nl.Epoch < 1 {
			return errors.New("epoch for n1 still zero")
		}
		return nil
	})

	// Mark the stores as draining. We'll then start checking how acquiring leases
	// behaves while draining.
	store1.draining.Store(true)
	store2.draining.Store(true)

	r2, err := store2.GetReplica(rd.RangeID)
	require.NoError(t, err)
	// Check that a draining replica that's not the leader does NOT take the
	// lease.
	_, pErr := r2.redirectOnOrAcquireLease(ctx)
	require.NotNil(t, pErr)
	require.IsType(t, &roachpb.NotLeaseHolderError{}, pErr.GetDetail())

	// Now transfer the leadership from r1 to r2 and check that r1 can now acquire
	// the lease.

	// Initiate the leadership transfer.
	r1.mu.Lock()
	r1.mu.internalRaftGroup.TransferLeader(uint64(r2.ReplicaID()))
	r1.mu.Unlock()
	// Run the range through the Raft scheduler, otherwise the leadership messages
	// doesn't get sent because the range is quiesced.
	store1.EnqueueRaftUpdateCheck(r1.RangeID)

	// Wait for the leadership transfer to happen.
	testutils.SucceedsSoon(t, func() error {
		if r2.RaftStatus().SoftState.RaftState != raft.StateLeader {
			return errors.Newf("r1 not yet leader")
		}
		return nil
	})

	// Check that r2 can now acquire the lease.
	_, pErr = r2.redirectOnOrAcquireLease(ctx)
	require.NoError(t, pErr.GoError())
}

// TestReplicaGossipFirstRange verifies that the first range gossips its
// location and the cluster ID.
func TestReplicaGossipFirstRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	if cfg := tc.gossip.DeprecatedGetSystemConfig(); cfg == nil {
		t.Fatal("config not set")
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
	if expValue != nil {
		expValue = roachpb.MakeValueFromBytes(expValue).TagAndDataBytes()
	}
	req := roachpb.NewConditionalPut(key, roachpb.MakeValueFromBytes(value), expValue, false /* allowNotExist */)
	return *req.(*roachpb.ConditionalPutRequest)
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

func deleteRangeArgs(key, endKey roachpb.Key) roachpb.DeleteRangeRequest {
	return roachpb.DeleteRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    key,
			EndKey: endKey,
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

func incrementArgs(key []byte, inc int64) *roachpb.IncrementRequest {
	return &roachpb.IncrementRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

func scanArgsString(s, e string) *roachpb.ScanRequest {
	return &roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(s), EndKey: roachpb.Key(e)},
	}
}

func getArgsString(k string) *roachpb.GetRequest {
	return &roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(k)},
	}
}

func scanArgs(start, end []byte) *roachpb.ScanRequest {
	return &roachpb.ScanRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
	}
}

func revScanArgsString(s, e string) *roachpb.ReverseScanRequest {
	return &roachpb.ReverseScanRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(s), EndKey: roachpb.Key(e)},
	}
}

func revScanArgs(start, end []byte) *roachpb.ReverseScanRequest {
	return &roachpb.ReverseScanRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
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

// endTxnArgs creates a EndTxnRequest. By leaving the Sequence field 0, the
// request will not qualify for 1PC.
func endTxnArgs(txn *roachpb.Transaction, commit bool) (roachpb.EndTxnRequest, roachpb.Header) {
	return roachpb.EndTxnRequest{
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
		PushTo:    pusher.WriteTimestamp.Next(),
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func recoverTxnArgs(txn *roachpb.Transaction, implicitlyCommitted bool) roachpb.RecoverTxnRequest {
	return roachpb.RecoverTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Txn:                 txn.TxnMeta,
		ImplicitlyCommitted: implicitlyCommitted,
	}
}

func queryTxnArgs(txn enginepb.TxnMeta, waitForUpdate bool) roachpb.QueryTxnRequest {
	return roachpb.QueryTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Txn:           txn,
		WaitForUpdate: waitForUpdate,
	}
}

func queryIntentArgs(
	key []byte, txn enginepb.TxnMeta, errIfMissing bool,
) roachpb.QueryIntentRequest {
	return roachpb.QueryIntentRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Txn:            txn,
		ErrorIfMissing: errIfMissing,
	}
}

func resolveIntentRangeArgsString(
	s, e string, txn enginepb.TxnMeta, status roachpb.TransactionStatus,
) *roachpb.ResolveIntentRangeRequest {
	return &roachpb.ResolveIntentRangeRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key(s), EndKey: roachpb.Key(e)},
		IntentTxn:     txn,
		Status:        status,
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

func clearRangeArgs(startKey, endKey roachpb.Key) roachpb.ClearRangeRequest {
	return roachpb.ClearRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
	}
}

// TestOptimizePuts verifies that contiguous runs of puts and
// conditional puts are marked as "blind" if they're written
// to a virgin keyspace.
func TestOptimizePuts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
				incArgs, &cpArgs[0], &cpArgs[1], &cpArgs[2], &cpArgs[3], &cpArgs[4], &cpArgs[5], &cpArgs[6], &cpArgs[7], &cpArgs[8], &cpArgs[9],
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
			if err := storage.MVCCPut(context.Background(), tc.engine, nil, c.exKey,
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
			if err := tc.engine.ClearUnversioned(c.exKey); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestAcquireLease verifies that the range lease is acquired
// for read and write methods, and eagerly renewed.
func TestAcquireLease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	gArgs := getArgs([]byte("a"))
	pArgs := putArgs([]byte("b"), []byte("1"))

	for _, test := range []roachpb.Request{
		&gArgs,
		&pArgs,
	} {
		t.Run("", func(t *testing.T) {
			testutils.RunTrueAndFalse(t, "withMinLeaseProposedTS", func(t *testing.T, withMinLeaseProposedTS bool) {
				ctx := context.Background()
				tc := testContext{}
				stopper := stop.NewStopper()
				defer stopper.Stop(ctx)
				tc.Start(ctx, t, stopper)

				lease, _ := tc.repl.GetLease()

				// This is a single-replica test; since we're automatically pushing back
				// the start of a lease as far as possible, and since there is an auto-
				// matic lease for us at the beginning, we'll basically create a lease
				// from then on. That is, unless the minLeaseProposedTS which gets set
				// automatically at server start forces us to get a new lease. We
				// simulate both cases.
				var expStart hlc.ClockTimestamp

				tc.repl.mu.Lock()
				if !withMinLeaseProposedTS {
					tc.repl.mu.minLeaseProposedTS = hlc.ClockTimestamp{}
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
				if !tc.repl.OwnsValidLease(ctx, ts.UnsafeToClockTimestamp()) {
					t.Errorf("expected lease acquisition")
				}
				lease, _ = tc.repl.GetLease()
				if lease.Start != expStart {
					t.Errorf("unexpected lease start: %s; expected %s", lease.Start, expStart)
				}

				if *lease.DeprecatedStartStasis != *lease.Expiration {
					t.Errorf("%s already in stasis (or beyond): %+v", ts, lease)
				}
				if lease.Expiration.LessEq(ts) {
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
					if newLease.Expiration.LessEq(*lease.Expiration) {
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
	defer log.Scope(t).Close(t)
	const num = 5

	const origMsg = "boom"
	testutils.RunTrueAndFalse(t, "withError", func(t *testing.T, withError bool) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		var seen int32
		var active int32
		var wg sync.WaitGroup
		wg.Add(num)

		tc := testContext{manualClock: hlc.NewManualClock(123)}
		cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
		// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
		// refreshes so that our lease proposal does not risk being rejected
		// with an AmbiguousResultError.
		cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
		cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
		cfg.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
			ll, ok := args.Req.Requests[0].GetInner().(*roachpb.RequestLeaseRequest)
			if !ok || atomic.LoadInt32(&active) == 0 {
				return nil
			}
			if c := atomic.AddInt32(&seen, 1); c > 1 {
				// Morally speaking, this is an error, but reproposals can
				// happen and so we warn (in case this trips the test up
				// in more unexpected ways).
				log.Infof(ctx, "reproposal of %+v", ll)
			}
			// Wait for all lease requests to join the same LeaseRequest command.
			wg.Wait()
			if withError {
				return roachpb.NewErrorf(origMsg)
			}
			return nil
		}
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)

		atomic.StoreInt32(&active, 1)
		tc.manualClock.Increment(leaseExpiry(tc.repl))
		now := tc.Clock().NowAsClockTimestamp()
		pErrCh := make(chan *roachpb.Error, num)
		for i := 0; i < num; i++ {
			if err := stopper.RunAsyncTask(ctx, "test", func(ctx context.Context) {
				tc.repl.mu.Lock()
				status := tc.repl.leaseStatusAtRLocked(ctx, now)
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

		newErr := errors.New("moob")
		for i, pErr := range pErrs {
			if withError != (pErr != nil) {
				t.Errorf("%d: wanted error: %t, got error %v", i, withError, pErr)
			}
			if testutils.IsPError(pErr, newErr.Error()) {
				t.Errorf("%d: errors shared memory: %v", i, pErr)
			} else if testutils.IsPError(pErr, origMsg) {
				// Mess with anyone holding the same reference.
				pErr.EncodedError = errors.EncodeError(ctx, newErr)
			} else if pErr != nil {
				t.Errorf("%d: unexpected error: %s", i, pErr)
			}
		}
	})
}

// TestReplicaUpdateTSCache verifies that reads and ranged writes update the
// timestamp cache. The test performs the operations with and without the use
// of synthetic timestamps.
func TestReplicaUpdateTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		startNanos := tc.Clock().Now().WallTime

		// Set clock to time 1s and do the read.
		tc.manualClock.Set(1 * time.Second.Nanoseconds())
		ts1 := tc.Clock().Now().WithSynthetic(synthetic)
		gArgs := getArgs([]byte("a"))

		if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts1}, &gArgs); pErr != nil {
			t.Error(pErr)
		}
		// Set clock to time 2s for write.
		tc.manualClock.Set(2 * time.Second.Nanoseconds())
		ts2 := tc.Clock().Now().WithSynthetic(synthetic)
		key := roachpb.Key([]byte("b"))
		drArgs := roachpb.NewDeleteRange(key, key.Next(), false /* returnKeys */)

		if _, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: ts2}, drArgs); pErr != nil {
			t.Error(pErr)
		}
		// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
		noID := uuid.UUID{}
		rTS, rTxnID := tc.repl.store.tsCache.GetMax(roachpb.Key("a"), nil)
		if rTS != ts1 || rTxnID != noID {
			t.Errorf("expected rTS=%s but got %s; rTxnID=%s", ts1, rTS, rTxnID)
		}
		// Verify the timestamp cache has rTS=2s for "b".
		rTS, rTxnID = tc.repl.store.tsCache.GetMax(roachpb.Key("b"), nil)
		if rTS != ts2 || rTxnID != noID {
			t.Errorf("expected rTS=%s but got %s; rTxnID=%s", ts2, rTS, rTxnID)
		}
		// Verify another key ("c") has 0sec in timestamp cache.
		rTS, rTxnID = tc.repl.store.tsCache.GetMax(roachpb.Key("c"), nil)
		if rTS.WallTime != startNanos || rTxnID != noID {
			t.Errorf("expected rTS=0s but got %s; rTxnID=%s", rTS, rTxnID)
		}
	})
}

// TestReplicaLatching verifies that reads/writes must wait for
// pending commands to complete through Raft before being executed on
// range.
func TestReplicaLatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
							func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
								if filterArgs.Hdr.UserPriority == blockingPriority && filterArgs.Index == 0 {
									blockingStart <- struct{}{}
									<-blockingDone
								}
								return nil
							}
						ctx := context.Background()
						stopper := stop.NewStopper()
						defer stopper.Stop(ctx)
						tc.StartWithStoreConfig(ctx, t, stopper, tsc)

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
									// the keys for latching purposes is determined on a
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
						// Wait for cmd1 to get acquire latches.
						select {
						case <-blockingStart:
						case <-time.After(tooLong):
							t.Fatalf("waited %s for cmd1 to acquire latches", tooLong)
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

// TestReplicaLatchingInconsistent verifies that inconsistent reads need
// not wait for pending commands to complete through Raft.
func TestReplicaLatchingInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
				func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
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
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.StartWithStoreConfig(ctx, t, stopper, tsc)
			cmd1Done := make(chan *roachpb.Error)
			go func() {
				args := putArgs(key, []byte{1})

				_, pErr := tc.SendWrapped(&args)
				cmd1Done <- pErr
			}()
			// Wait for cmd1 to get acquire latches.
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

// TestReplicaLatchingSelfOverlap verifies that self-overlapping batches are
// allowed, and in particular do not deadlock by introducing latch dependencies
// between the parts of the batch.
func TestReplicaLatchingSelfOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	testutils.RunTrueAndFalse(t, "cmd1Read", func(t *testing.T, cmd1Read bool) {
		testutils.RunTrueAndFalse(t, "cmd2Read", func(t *testing.T, cmd2Read bool) {
			key := fmt.Sprintf("%v,%v", cmd1Read, cmd2Read)
			ba := roachpb.BatchRequest{}
			ba.Add(readOrWriteArgs(roachpb.Key(key), cmd1Read))
			ba.Add(readOrWriteArgs(roachpb.Key(key), cmd2Read))

			// Set a deadline for nicer error behavior on deadlock.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

// TestReplicaLatchingTimestampNonInterference verifies that
// reads with earlier timestamps do not interfere with writes.
func TestReplicaLatchingTimestampNonInterference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var blockKey, blockReader, blockWriter atomic.Value
	blockKey.Store(roachpb.Key("a"))
	blockReader.Store(false)
	blockWriter.Store(false)
	blockCh := make(chan struct{}, 1)
	blockedCh := make(chan struct{}, 1)

	tc := testContext{}
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
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
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

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

// TestReplicaLatchingSplitDeclaresWrites verifies that split operations declare
// non-MVCC read access to the LHS and non-MVCC write access to the RHS of the
// split. This is necessary to avoid conflicting changes to the range's stats,
// even though splits do not actually write to their data span (and therefore a
// failure to declare writes are not caught directly by any other test).
func TestReplicaLatchingSplitDeclaresWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var spans spanset.SpanSet
	cmd, _ := batcheval.LookupCommand(roachpb.EndTxn)
	cmd.DeclareKeys(
		&roachpb.RangeDescriptor{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("e")},
		&roachpb.Header{},
		&roachpb.EndTxnRequest{
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				SplitTrigger: &roachpb.SplitTrigger{
					LeftDesc: roachpb.RangeDescriptor{
						StartKey: roachpb.RKey("a"),
						EndKey:   roachpb.RKey("c"),
					},
					RightDesc: roachpb.RangeDescriptor{
						StartKey: roachpb.RKey("c"),
						EndKey:   roachpb.RKey("e"),
					},
				},
			},
		},
		&spans,
		nil,
		0,
	)
	for _, tc := range []struct {
		access       spanset.SpanAccess
		key          roachpb.Key
		expectAccess bool
	}{
		{spanset.SpanReadOnly, roachpb.Key("b"), true},
		{spanset.SpanReadOnly, roachpb.Key("d"), true},
		{spanset.SpanReadWrite, roachpb.Key("b"), false},
		{spanset.SpanReadWrite, roachpb.Key("d"), true},
	} {
		err := spans.CheckAllowed(tc.access, roachpb.Span{Key: tc.key})
		if tc.expectAccess {
			require.NoError(t, err)
		} else {
			require.NotNil(t, err)
			require.Regexp(t, "undeclared span", err)
		}
	}
}

// TestReplicaLatchingOptimisticEvaluation verifies that limited scans
// evaluate optimistically without waiting for latches to be acquired. In some
// cases, this allows them to avoid waiting on writes that their
// over-estimated declared spans overlapped with.
func TestReplicaLatchingOptimisticEvaluation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Split into two back-to-back scans for better test coverage.
	sArgs1 := scanArgs([]byte("a"), []byte("c"))
	sArgs2 := scanArgs([]byte("c"), []byte("e"))
	baScan := roachpb.BatchRequest{}
	baScan.Add(sArgs1, sArgs2)
	// The state that will block a write while holding latches.
	var blockKey, blockWriter atomic.Value
	blockKey.Store(roachpb.Key("a"))
	blockWriter.Store(false)
	blockCh := make(chan struct{}, 1)
	blockedCh := make(chan struct{}, 1)
	// Setup filter to block the write.
	tc := testContext{}
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if !filterArgs.Req.Header().Key.Equal(blockKey.Load().(roachpb.Key)) {
				return nil
			}
			if filterArgs.Req.Method() == roachpb.Put && blockWriter.Load().(bool) {
				blockedCh <- struct{}{}
				<-blockCh
			}
			return nil
		}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	// Write initial keys.
	for _, k := range []string{"a", "b", "c", "d"} {
		pArgs := putArgs([]byte(k), []byte("value"))
		if _, pErr := tc.SendWrapped(&pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
	testCases := []struct {
		writeKey   string
		limit      int64
		interferes bool
	}{
		// No limit, so pessimistic latching.
		{"a", 0, true},
		{"b", 0, true},
		{"c", 0, true},
		{"d", 0, true},
		{"e", 0, false}, // Only scanning from [a,e)
		// Limited, with optimistic latching.
		{"a", 1, true},
		{"b", 1, false},
		{"b", 2, true},
		{"c", 2, false},
		{"c", 3, true},
		{"d", 3, false},
		// Limited, with pessimistic latching since limit count is equal to number
		// of keys in range.
		{"d", 4, true},
		{"e", 4, false}, // Only scanning from [a,e)
		{"e", 5, false}, // Only scanning from [a,e)
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			errCh := make(chan *roachpb.Error, 2)
			pArgs := putArgs([]byte(test.writeKey), []byte("value"))
			blockKey.Store(roachpb.Key(test.writeKey))
			blockWriter.Store(true)
			go func() {
				_, pErr := tc.SendWrapped(&pArgs)
				errCh <- pErr
			}()
			<-blockedCh
			// Write is now blocked while holding latches.
			blockWriter.Store(false)
			baScanCopy := baScan
			baScanCopy.MaxSpanRequestKeys = test.limit
			go func() {
				_, pErr := tc.Sender().Send(context.Background(), baScanCopy)
				errCh <- pErr
			}()
			if test.interferes {
				// Neither request should complete until the write is unblocked.
				select {
				case <-time.After(10 * time.Millisecond):
					// Expected.
				case pErr := <-errCh:
					t.Fatalf("expected interference: got error %s", pErr)
				}
				// Unblock the write.
				blockCh <- struct{}{}
				// Both read and write should complete with no errors.
				for j := 0; j < 2; j++ {
					if pErr := <-errCh; pErr != nil {
						t.Errorf("error %d: unexpected error: %s", j, pErr)
					}
				}
			} else {
				// The read should complete first.
				if pErr := <-errCh; pErr != nil {
					t.Errorf("unexpected error: %s", pErr)
				}
				// The write should complete next, after it is unblocked.
				blockCh <- struct{}{}
				if pErr := <-errCh; pErr != nil {
					t.Errorf("unexpected error: %s", pErr)
				}
			}
		})
	}
}

// TestReplicaUseTSCache verifies that write timestamps are upgraded based on
// the timestamp cache. The test performs the operations with and without the
// use of synthetic timestamps.
func TestReplicaUseTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)
		startTS := tc.Clock().Now()

		// Set clock to time 1s and do the read.
		tc.manualClock.Increment(1)
		readTS := tc.Clock().Now().WithSynthetic(synthetic)
		args := getArgs([]byte("a"))

		_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: readTS}, &args)
		require.Nil(t, pErr)

		// Perform a conflicting write. Should get bumped.
		pArgs := putArgs([]byte("a"), []byte("value"))
		var ba roachpb.BatchRequest
		ba.Add(&pArgs)
		ba.Timestamp = startTS

		br, pErr := tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)
		require.NotEqual(t, startTS, br.Timestamp)
		require.Equal(t, readTS.Next(), br.Timestamp)
	})
}

// TestReplicaTSCacheForwardsIntentTS verifies that the timestamp cache affects
// the timestamps at which intents are written. That is, if a transactional
// write is forwarded by the timestamp cache due to a more recent read, the
// written intents must be left at the forwarded timestamp. See the comment on
// the enginepb.TxnMeta.Timestamp field for rationale.
//
// The test performs the operations with and without the use of synthetic
// timestamps.
func TestReplicaTSCacheForwardsIntentTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		tsOld := tc.Clock().Now()
		tsNew := tsOld.Add(time.Millisecond.Nanoseconds(), 0).WithSynthetic(synthetic)

		// Read at tNew to populate the timestamp cache.
		// DeleteRange at tNew to populate the timestamp cache.
		txnNew := newTransaction("new", roachpb.Key("txn-anchor"), roachpb.NormalUserPriority, tc.Clock())
		txnNew.ReadTimestamp = tsNew
		txnNew.WriteTimestamp = tsNew
		keyGet := roachpb.Key("get")
		keyDeleteRange := roachpb.Key("delete-range")
		gArgs := getArgs(keyGet)
		drArgs := deleteRangeArgs(keyDeleteRange, keyDeleteRange.Next())
		assignSeqNumsForReqs(txnNew, &gArgs, &drArgs)
		var ba roachpb.BatchRequest
		ba.Header.Txn = txnNew
		ba.Add(&gArgs, &drArgs)
		if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}

		// Write under the timestamp cache within the transaction, and verify that
		// the intents are written above the timestamp cache.
		txnOld := newTransaction("old", roachpb.Key("txn-anchor"), roachpb.NormalUserPriority, tc.Clock())
		txnOld.ReadTimestamp = tsOld
		txnOld.WriteTimestamp = tsOld
		for _, key := range []roachpb.Key{keyGet, keyDeleteRange} {
			t.Run(string(key), func(t *testing.T) {
				pArgs := putArgs(key, []byte("foo"))
				assignSeqNumsForReqs(txnOld, &pArgs)
				if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txnOld}, &pArgs); pErr != nil {
					t.Fatal(pErr)
				}
				iter := tc.engine.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{Prefix: true})
				defer iter.Close()
				mvccKey := storage.MakeMVCCMetadataKey(key)
				iter.SeekGE(mvccKey)
				var keyMeta enginepb.MVCCMetadata
				if ok, err := iter.Valid(); !ok || !iter.UnsafeKey().Equal(mvccKey) {
					t.Fatalf("missing mvcc metadata for %q: %+v", mvccKey, err)
				} else if err := iter.ValueProto(&keyMeta); err != nil {
					t.Fatalf("failed to unmarshal metadata for %q", mvccKey)
				}
				if tsNext := tsNew.Next(); keyMeta.Timestamp.ToTimestamp() != tsNext {
					t.Errorf("timestamp not forwarded for %q intent: expected %s but got %s",
						key, tsNext, keyMeta.Timestamp)
				}
			})
		}
	})
}

func TestConditionalPutUpdatesTSCacheOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{manualClock: hlc.NewManualClock(123)}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DontPushOnWriteIntentError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Set clock to time 2s and do the conditional put.
	t1 := makeTS(1*time.Second.Nanoseconds(), 0)
	t2 := makeTS(2*time.Second.Nanoseconds(), 0)
	t2Next := t2.Next()
	tc.manualClock.Set(t2.WallTime)

	// CPut args which expect value "1" to write "0".
	key := []byte("a")
	cpArgs1 := cPutArgs(key, []byte("1"), []byte("0"))
	_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: t2}, &cpArgs1)
	if cfErr, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); !ok {
		t.Errorf("expected ConditionFailedError; got %v", pErr)
	} else if cfErr.ActualValue != nil {
		t.Errorf("expected empty actual value; got %s", cfErr.ActualValue)
	}

	// Try a transactional conditional put at a lower timestamp and
	// ensure it is pushed.
	txnEarly := newTransaction("test", key, 1, tc.Clock())
	txnEarly.ReadTimestamp, txnEarly.WriteTimestamp = t1, t1
	cpArgs2 := cPutArgs(key, []byte("value"), nil)
	resp, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txnEarly}, &cpArgs2)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t2Next {
		t.Errorf("expected write timestamp to upgrade to %s; got %s", t2Next, respTS)
	}

	// Try a conditional put at a later timestamp which will fail
	// because there's now a transaction intent. This failure will
	// not update the timestamp cache.
	t3 := makeTS(3*time.Second.Nanoseconds(), 0)
	tc.manualClock.Set(t3.WallTime)
	_, pErr = tc.SendWrapped(&cpArgs1)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected WriteIntentError; got %v", pErr)
	}

	// Abort the intent and try a transactional conditional put at
	// a later timestamp. This should succeed and should not update
	// the timestamp cache.
	abortIntent := func(s roachpb.Span, abortTxn *roachpb.Transaction) {
		if _, pErr = tc.SendWrapped(&roachpb.ResolveIntentRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(s),
			IntentTxn:     abortTxn.TxnMeta,
			Status:        roachpb.ABORTED,
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
	abortIntent(cpArgs2.Span(), txnEarly)
	txnLater := *txnEarly
	txnLater.ReadTimestamp, txnLater.WriteTimestamp = t3, t3
	resp, pErr = tc.SendWrappedWith(roachpb.Header{Txn: &txnLater}, &cpArgs2)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t3 {
		t.Errorf("expected write timestamp to be %s; got %s", t3, respTS)
	}

	// Abort the intent again and try to write again to ensure the timestamp
	// cache wasn't updated by the second (successful), third (unsuccessful),
	// or fourth (successful) conditional put. Only the conditional put that
	// hit a ConditionFailedError should update the timestamp cache.
	abortIntent(cpArgs2.Span(), &txnLater)
	resp, pErr = tc.SendWrappedWith(roachpb.Header{Txn: txnEarly}, &cpArgs2)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t2Next {
		t.Errorf("expected write timestamp to upgrade to %s; got %s", t2Next, respTS)
	}
}

func TestInitPutUpdatesTSCacheOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{manualClock: hlc.NewManualClock(123)}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DontPushOnWriteIntentError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// InitPut args to write "0". Should succeed.
	key := []byte("a")
	value := []byte("0")
	ipArgs1 := iPutArgs(key, value)
	_, pErr := tc.SendWrapped(&ipArgs1)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Set clock to time 2s and do other init puts.
	t1 := makeTS(1*time.Second.Nanoseconds(), 0)
	t2 := makeTS(2*time.Second.Nanoseconds(), 0)
	t2Next := t2.Next()
	tc.manualClock.Set(t2.WallTime)

	// InitPut args to write "1" to same key. Should fail.
	ipArgs2 := iPutArgs(key, []byte("1"))
	_, pErr = tc.SendWrappedWith(roachpb.Header{Timestamp: t2}, &ipArgs2)
	if cfErr, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); !ok {
		t.Errorf("expected ConditionFailedError; got %v", pErr)
	} else if valueBytes, err := cfErr.ActualValue.GetBytes(); err != nil {
		t.Fatal(err)
	} else if cfErr.ActualValue == nil || !bytes.Equal(valueBytes, value) {
		t.Errorf("expected value %q; got %+v", value, valueBytes)
	}

	// Try a transactional init put at a lower timestamp and
	// ensure it is pushed.
	txnEarly := newTransaction("test", key, 1, tc.Clock())
	txnEarly.ReadTimestamp, txnEarly.WriteTimestamp = t1, t1
	resp, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txnEarly}, &ipArgs1)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t2Next {
		t.Errorf("expected write timestamp to upgrade to %s; got %s", t2Next, respTS)
	}

	// Try an init put at a later timestamp which will fail
	// because there's now a transaction intent. This failure
	// will not update the timestamp cache.
	t3 := makeTS(3*time.Second.Nanoseconds(), 0)
	tc.manualClock.Set(t3.WallTime)
	_, pErr = tc.SendWrapped(&ipArgs2)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected WriteIntentError; got %v", pErr)
	}

	// Abort the intent and try a transactional init put at a later
	// timestamp. This should succeed and should not update the
	// timestamp cache.
	abortIntent := func(s roachpb.Span, abortTxn *roachpb.Transaction) {
		if _, pErr = tc.SendWrapped(&roachpb.ResolveIntentRequest{
			RequestHeader: roachpb.RequestHeaderFromSpan(s),
			IntentTxn:     abortTxn.TxnMeta,
			Status:        roachpb.ABORTED,
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
	abortIntent(ipArgs1.Span(), txnEarly)
	txnLater := *txnEarly
	txnLater.ReadTimestamp, txnLater.WriteTimestamp = t3, t3
	resp, pErr = tc.SendWrappedWith(roachpb.Header{Txn: &txnLater}, &ipArgs1)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t3 {
		t.Errorf("expected write timestamp to be %s; got %s", t3, respTS)
	}

	// Abort the intent again and try to write again to ensure the timestamp
	// cache wasn't updated by the second (successful), third (unsuccessful),
	// or fourth (successful) init put. Only the init put that hit a
	// ConditionFailedError should update the timestamp cache.
	abortIntent(ipArgs1.Span(), &txnLater)
	resp, pErr = tc.SendWrappedWith(roachpb.Header{Txn: txnEarly}, &ipArgs1)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t2Next {
		t.Errorf("expected write timestamp to upgrade to %s; got %s", t2Next, respTS)
	}
}

// TestReplicaNoTSCacheInconsistent verifies that the timestamp cache
// is not affected by inconsistent reads.
func TestReplicaNoTSCacheInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, rc := range []roachpb.ReadConsistencyType{
		roachpb.READ_UNCOMMITTED,
		roachpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			tc := testContext{}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(ctx, t, stopper)
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

			var ba roachpb.BatchRequest
			ba.Header = roachpb.Header{Timestamp: hlc.Timestamp{WallTime: 0, Logical: 1}}
			ba.Add(&pArgs)
			br, pErr := tc.Sender().Send(context.Background(), ba)
			if pErr != nil {
				t.Fatal(pErr)
			}
			if br.Timestamp.WallTime == tc.Clock().Now().WallTime {
				t.Errorf("expected write timestamp not to upgrade to 1s; got %s", br.Timestamp)
			}
		})
	}
}

// TestReplicaNoTSCacheUpdateOnFailure verifies that read and write
// commands do not update the timestamp cache if they result in
// failure.
func TestReplicaNoTSCacheUpdateOnFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DontPushOnWriteIntentError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Test for both read & write attempts.
	for i, read := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		txn := newTransaction("test", key, 1, tc.Clock())
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

		_, pErr = tc.SendWrappedWith(roachpb.Header{Timestamp: ts}, args)
		if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
			t.Errorf("expected WriteIntentError; got %v", pErr)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		var ba roachpb.BatchRequest
		ba.Header = roachpb.Header{Txn: txn}
		ba.Add(&pArgs)
		assignSeqNumsForReqs(txn, &pArgs)
		br, pErr := tc.Sender().Send(context.Background(), ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if br.Txn.WriteTimestamp != txn.WriteTimestamp {
			t.Errorf("expected timestamp not to advance %s != %s", br.Timestamp, txn.WriteTimestamp)
		}
	}
}

// TestReplicaNoTSCacheIncrementWithinTxn verifies that successive
// read and write commands within the same transaction do not cause
// the write to receive an incremented timestamp.
func TestReplicaNoTSCacheIncrementWithinTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Test for both read & write attempts.
	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, tc.Clock())

	// Start with a read to warm the timestamp cache.
	gArgs := getArgs(key)
	assignSeqNumsForReqs(txn, &gArgs)

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now try a write and verify timestamp isn't incremented.
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	pArgs := putArgs(key, []byte("value"))
	ba.Add(&pArgs)
	assignSeqNumsForReqs(txn, &pArgs)
	br, pErr := tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if br.Txn.WriteTimestamp != txn.WriteTimestamp {
		t.Errorf("expected timestamp to remain %s; got %s", txn.WriteTimestamp, br.Timestamp)
	}

	// Resolve the intent.
	rArgs := &roachpb.ResolveIntentRequest{
		RequestHeader: pArgs.Header(),
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.COMMITTED,
	}
	if _, pErr = tc.SendWrappedWith(roachpb.Header{Timestamp: txn.WriteTimestamp}, rArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	ts := txn.WriteTimestamp
	expTS := ts
	expTS.Logical++

	ba = roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Timestamp: ts}
	ba.Add(&pArgs)
	assignSeqNumsForReqs(txn, &pArgs)
	br, pErr = tc.Sender().Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if br.Timestamp != expTS {
		t.Errorf("expected timestamp to increment to %s; got %s", expTS, br.Timestamp)
	}
}

// TestReplicaAbortSpanReadError verifies that an error is returned
// to the client in the event that a AbortSpan entry is found but is
// not decodable.
//
// This doubles as a test that replica corruption errors are propagated
// and handled correctly.
func TestReplicaAbortSpanReadError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var exitStatus exit.Code
	log.SetExitFunc(true /* hideStack */, func(i exit.Code) {
		exitStatus = i
	})
	defer log.ResetExitFunc()

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	k := []byte("a")
	txn := newTransaction("test", k, 10, tc.Clock())
	args := incrementArgs(k, 1)
	assignSeqNumsForReqs(txn, args)

	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite Abort span entry with garbage for the last op.
	key := keys.AbortSpanKey(tc.repl.RangeID, txn.ID)
	err := storage.MVCCPut(ctx, tc.engine, nil, key, hlc.Timestamp{}, roachpb.MakeValueFromString("never read in this test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	// Now try increment again and verify error.
	_, pErr := tc.SendWrappedWith(roachpb.Header{
		Txn: txn,
	}, args)
	if !testutils.IsPError(pErr, "replica corruption") {
		t.Fatal(pErr)
	}
	if exitStatus != exit.FatalError() {
		t.Fatalf("did not fatal (exit status %d)", exitStatus)
	}
}

// TestReplicaAbortSpanOnlyWithIntent verifies that a transactional command
// which goes through Raft but is not a transactional write (i.e. does not
// leave intents) passes the AbortSpan unhindered.
func TestReplicaAbortSpanOnlyWithIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	txn := newTransaction("test", []byte("test"), 10, tc.Clock())
	txn.Sequence = 100
	entry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.WriteTimestamp,
		Priority:  0,
	}
	if err := tc.repl.abortSpan.Put(ctx, tc.engine, nil, txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	args, h := heartbeatArgs(txn, tc.Clock().Now())
	// If the AbortSpan were active for this request, we'd catch a txn retry.
	// Instead, we expect no error and a successfully created transaction record.
	if _, pErr := tc.SendWrappedWith(h, &args); pErr != nil {
		t.Fatalf("unexpected error: %v", pErr)
	}
}

// TestReplicaTxnIdempotency verifies that transactions run successfully and in
// an idempotent manner when replaying the same requests.
func TestReplicaTxnIdempotency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	runWithTxn := func(txn *roachpb.Transaction, reqs ...roachpb.Request) error {
		ba := roachpb.BatchRequest{}
		ba.Header.Txn = txn
		ba.Add(reqs...)
		_, pErr := tc.Sender().Send(ctx, ba)
		return pErr.GoError()
	}
	keyAtSeqHasVal := func(txn *roachpb.Transaction, key []byte, seq enginepb.TxnSeq, val *roachpb.Value) error {
		args := getArgs(key)
		args.Sequence = seq
		resp, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &args)
		if pErr != nil {
			return pErr.GoError()
		}
		foundVal := resp.(*roachpb.GetResponse).Value
		if (foundVal == nil) == (val == nil) {
			if foundVal == nil {
				return nil
			}
			if foundVal.EqualTagAndData(*val) {
				return nil
			}
		}
		return errors.Errorf("expected val %v at seq %d, found %v", val, seq, foundVal)
	}
	firstErr := func(errs ...error) error {
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
		return nil
	}

	val1 := []byte("value")
	val2 := []byte("value2")
	byteVal := func(b []byte) *roachpb.Value {
		var v roachpb.Value
		v.SetBytes(b)
		return &v
	}
	intVal := func(i int64) *roachpb.Value {
		var v roachpb.Value
		v.SetInt(i)
		return &v
	}

	testCases := []struct {
		name           string
		beforeTxnStart func(key []byte) error
		afterTxnStart  func(txn *roachpb.Transaction, key []byte) error
		run            func(txn *roachpb.Transaction, key []byte) error
		validate       func(txn *roachpb.Transaction, key []byte) error
		expError       string // regexp pattern to match on run error, if not empty
	}{
		{
			// Requests are meant to be idempotent, so identical requests at the
			// same sequence should always succeed without changing state.
			name: "reissued put",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			name: "reissued cput",
			beforeTxnStart: func(key []byte) error {
				// Write an initial key for the CPuts to expect.
				args := putArgs(key, val2)
				return runWithTxn(nil, &args)
			},
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := cPutArgs(key, val1, val2)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := cPutArgs(key, val1, val2)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, byteVal(val2)),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			name: "reissued initput",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := iPutArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := iPutArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			name: "reissued increment",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := incrementArgs(key, 3)
				args.Sequence = 2
				return runWithTxn(txn, args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := incrementArgs(key, 3)
				args.Sequence = 2
				return runWithTxn(txn, args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, intVal(3)),
					keyAtSeqHasVal(txn, key, 3, intVal(3)),
				)
			},
		},
		{
			name: "reissued delete",
			beforeTxnStart: func(key []byte) error {
				// Write an initial key to delete.
				args := putArgs(key, val2)
				return runWithTxn(nil, &args)
			},
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := deleteArgs(key)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := deleteArgs(key)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, byteVal(val2)),
					keyAtSeqHasVal(txn, key, 2, nil),
					keyAtSeqHasVal(txn, key, 3, nil),
				)
			},
		},
		{
			name: "reissued delete range",
			beforeTxnStart: func(key []byte) error {
				// Write an initial key to delete.
				args := putArgs(key, val2)
				return runWithTxn(nil, &args)
			},
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := deleteRangeArgs(key, append(key, 0))
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := deleteRangeArgs(key, append(key, 0))
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, byteVal(val2)),
					keyAtSeqHasVal(txn, key, 2, nil),
					keyAtSeqHasVal(txn, key, 3, nil),
				)
			},
		},
		{
			// A request reissued from an earlier txn epoch will be rejected.
			name: "reissued write at lower epoch",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				txnEpochBump := txn.Clone()
				txnEpochBump.Epoch++

				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txnEpochBump, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val2)
				args.Sequence = 3
				return runWithTxn(txn, &args)
			},
			expError: "put with epoch 0 came after put with epoch 1",
			validate: func(txn *roachpb.Transaction, key []byte) error {
				txnEpochBump := txn.Clone()
				txnEpochBump.Epoch++

				return firstErr(
					keyAtSeqHasVal(txnEpochBump, key, 1, nil),
					keyAtSeqHasVal(txnEpochBump, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txnEpochBump, key, 3, byteVal(val1)),
				)
			},
		},
		{
			// A request issued after a request with a larger sequence has
			// already laid down an intent on the same key will be rejected.
			// Unlike the next case, seq two was not issued before seq three,
			// which would indicate a faulty client.
			name: "reordered write at lower sequence",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 3
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			expError: "sequence 3 missing an intent with lower sequence 2",
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, nil),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			// Unlike the previous case, here a request is reissued after a
			// request with an identical sequence is issued AND a request with a
			// larger sequence is issued. Because the replay isn't rewriting
			// history, it can succeed. This does not indicate a faulty client.
			// It is possible if a batch that writes to a key twice is reissued.
			name: "reissued write at lower sequence",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				if err := runWithTxn(txn, &args); err != nil {
					return err
				}

				args = putArgs(key, val2)
				args.Sequence = 3
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val2)),
				)
			},
		},
		{
			// A request at the same sequence as another but that produces a
			// different result will be rejected.
			name: "different write at same sequence",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val2)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			expError: "sequence 2 has a different value",
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			// A request is issued again, but with a lower timestamp than the
			// timestamp in the intent. This is possible if an intent is pushed
			// and then the request that wrote it is reissued. We allow this
			// without issue because timestamps on intents are moved to the
			// commit timestamp on commit.
			name: "reissued write at lower timestamp",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				txnHighTS := txn.Clone()
				txnHighTS.WriteTimestamp = txnHighTS.WriteTimestamp.Add(1, 0)

				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txnHighTS, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			// A request is issued again, but with a higher timestamp than the
			// timestamp in the intent. This is possible if the txn coordinator
			// increased its timestamp between the two requests (for instance,
			// after a refresh). We allow this without issue because timestamps
			// on intents are moved to the commit timestamp on commit.
			name: "reissued write at higher timestamp",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				txnHighTS := txn.Clone()
				txnHighTS.WriteTimestamp = txnHighTS.WriteTimestamp.Add(1, 0)

				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txnHighTS, &args)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, byteVal(val1)),
				)
			},
		},
		{
			// If part of a batch has already succeeded and another part hasn't
			// then the previously successful portion will be evaluated as a
			// no-op while the rest will evaluate as normal. This isn't common,
			// but it could happen if a partially successful batch is reissued
			// after a range merge.
			name: "reissued write in partially successful batch",
			afterTxnStart: func(txn *roachpb.Transaction, key []byte) error {
				args := putArgs(key, val1)
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				pArgs := putArgs(key, val1)
				pArgs.Sequence = 2
				dArgs := deleteArgs(key)
				dArgs.Sequence = 3
				return runWithTxn(txn, &pArgs, &dArgs)
			},
			validate: func(txn *roachpb.Transaction, key []byte) error {
				return firstErr(
					keyAtSeqHasVal(txn, key, 1, nil),
					keyAtSeqHasVal(txn, key, 2, byteVal(val1)),
					keyAtSeqHasVal(txn, key, 3, nil),
				)
			},
		},
	}
	for i, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			key := []byte(strconv.Itoa(i))
			if c.beforeTxnStart != nil {
				if err := c.beforeTxnStart(key); err != nil {
					t.Fatalf("failed beforeTxnStart: %v", err)
				}
			}

			txn := newTransaction(c.name, roachpb.Key(c.name), 1, tc.Clock())
			if c.afterTxnStart != nil {
				if err := c.afterTxnStart(txn, key); err != nil {
					t.Fatalf("failed afterTxnStart: %v", err)
				}
			}

			if err := c.run(txn, key); err != nil {
				if len(c.expError) == 0 {
					t.Fatalf("expected no failure, found %q", err.Error())
				}
				if !testutils.IsError(err, regexp.QuoteMeta(c.expError)) {
					t.Fatalf("expected failure %q, found %q", c.expError, err.Error())
				}
			} else {
				if len(c.expError) > 0 {
					t.Fatalf("expected failure %q", c.expError)
				}
			}

			if c.validate != nil {
				if err := c.validate(txn, key); err != nil {
					t.Fatalf("failed during validation: %v", err)
				}
			}
		})
	}
}

// TestEndTxnDeadline verifies that EndTxn respects the transaction deadline.
func TestEndTxnDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := newTransaction("txn: "+strconv.Itoa(i), key, 1, tc.Clock())
		put := putArgs(key, key)
		assignSeqNumsForReqs(txn, &put)

		if _, pErr := kv.SendWrappedWith(
			ctx, tc.Sender(), roachpb.Header{Txn: txn}, &put,
		); pErr != nil {
			t.Fatal(pErr)
		}

		etArgs, etHeader := endTxnArgs(txn, true /* commit */)
		switch i {
		case 0:
			// No deadline.
		case 1:
			// Past deadline.
			ts := txn.WriteTimestamp.Prev()
			etArgs.Deadline = &ts
		case 2:
			// Equal deadline.
			etArgs.Deadline = &txn.WriteTimestamp
		case 3:
			// Future deadline.
			ts := txn.WriteTimestamp.Next()
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
				fallthrough
			case 2:
				// Past deadline.
				retErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError)
				if !ok || retErr.Reason != roachpb.RETRY_COMMIT_DEADLINE_EXCEEDED {
					t.Fatalf("expected deadline exceeded, got: %v", pErr)
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

// Test that regular push retriable errors take precedence over the deadline
// check.
func TestSerializableDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Create our txn. It will be pushed next.
	key := roachpb.Key("key")
	txn := newTransaction("test txn", key, roachpb.MinUserPriority, tc.Clock())

	tc.manualClock.Increment(100)
	pusher := newTransaction(
		"test pusher", key, roachpb.MaxUserPriority, tc.Clock())
	pushReq := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
	resp, pErr := tc.SendWrapped(&pushReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	updatedPushee := resp.(*roachpb.PushTxnResponse).PusheeTxn
	if updatedPushee.Status != roachpb.PENDING {
		t.Fatalf("expected pushee to still be alive, but got %+v", updatedPushee)
	}

	// Send an EndTxn with a deadline below the point where the txn has been
	// pushed.
	etArgs, etHeader := endTxnArgs(txn, true /* commit */)
	deadline := updatedPushee.WriteTimestamp
	deadline.Logical--
	etArgs.Deadline = &deadline
	_, pErr = tc.SendWrappedWith(etHeader, &etArgs)
	const expectedErrMsg = "TransactionRetryError: retry txn \\(RETRY_SERIALIZABLE\\)"
	if pErr == nil {
		t.Fatalf("expected %q, got: nil", expectedErrMsg)
	}
	err := pErr.GoError()
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok ||
		!testutils.IsError(err, expectedErrMsg) {
		t.Fatalf("expected %q, got: %s (%T)", expectedErrMsg,
			err, pErr.GoError())
	}
}

// TestCreateTxnRecordAfterPushAndGC verifies that aborting transactions does
// not lead to anomalies even after the aborted transaction record is cleaned
// up. Precisely, verify that if the GC queue could potentially have removed a
// txn record created through a successful push (by a concurrent actor), the
// original transaction's subsequent attempt to create its initial record fails.
//
// See #9265 for context.
func TestCreateTxnRecordAfterPushAndGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("a")
	desc := tc.repl.Desc()
	// This test avoids a zero-timestamp regression (see LastActive() below),
	// so avoid zero timestamps.
	tc.manualClock.Increment(123)
	pusher := newTransaction("pusher", key, 1, tc.Clock())

	// This pushee should never be allowed to write a txn record because it
	// will be aborted before it even tries.
	pushee := newTransaction("pushee", key, 1, tc.Clock())
	pushReq := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)
	pushReq.Force = true
	resp, pErr := tc.SendWrapped(&pushReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	abortedPushee := resp.(*roachpb.PushTxnResponse).PusheeTxn
	if abortedPushee.Status != roachpb.ABORTED {
		t.Fatalf("expected push to abort pushee, got %+v", abortedPushee)
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
		}
		if _, pErr := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &gcReq); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Try to let our transaction write its initial record. If this succeeds,
	// we're in trouble because other written intents may have been aborted,
	// i.e. the transaction might commit but lose some of its writes. It should
	// not succeed because the abort is reflected in the timestamp cache,
	// which is consulted when attempting to create the transaction record.
	{
		expErr := "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)"

		// HeartbeatTxn.
		hb, hbH := heartbeatArgs(pushee, tc.Clock().Now())
		resp, pErr = tc.SendWrappedWith(hbH, &hb)
		if pErr == nil {
			t.Fatalf("unexpected success: %+v", resp)
		} else if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
			t.Fatalf("expected %s, got %v and response %+v", expErr, pErr, resp)
		}

		// EndTxn.
		et, etH := endTxnArgs(pushee, true)
		resp, pErr = tc.SendWrappedWith(etH, &et)
		if pErr == nil {
			t.Fatalf("unexpected success: %+v", resp)
		} else if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
			t.Fatalf("expected %s, got %v and response %+v", expErr, pErr, resp)
		}
	}

	// A transaction which starts later (i.e. at a higher timestamp) should not
	// be prevented from writing its record.
	// See #9522.
	{
		// HeartbeatTxn.
		newTxn1 := newTransaction("foo", key, 1, tc.Clock())
		hb, hbH := heartbeatArgs(newTxn1, tc.Clock().Now())
		if _, pErr := tc.SendWrappedWith(hbH, &hb); pErr != nil {
			t.Fatal(pErr)
		}

		// EndTxn.
		newTxn2 := newTransaction("foo", key, 1, tc.Clock())
		et, etH := endTxnArgs(newTxn2, true)
		if _, pErr := tc.SendWrappedWith(etH, &et); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestEndTxnDeadline_1PC verifies that a transaction that exceeded its deadline
// will be aborted even when one phase commit is applicable.
func TestEndTxnDeadline_1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, tc.Clock())
	put := putArgs(key, []byte("value"))
	et, etH := endTxnArgs(txn, true)
	// Past deadline.
	ts := txn.WriteTimestamp.Prev()
	et.Deadline = &ts

	var ba roachpb.BatchRequest
	ba.Header = etH
	ba.Add(&put, &et)
	assignSeqNumsForReqs(txn, &put, &et)
	_, pErr := tc.Sender().Send(ctx, ba)
	retErr, ok := pErr.GetDetail().(*roachpb.TransactionRetryError)
	if !ok || retErr.Reason != roachpb.RETRY_COMMIT_DEADLINE_EXCEEDED {
		t.Fatalf("expected deadline exceeded, got: %v", pErr)
	}
}

// Test1PCTransactionWriteTimestamp verifies that the transaction's
// timestamp is used when writing values in a 1PC transaction. We
// verify this by updating the timestamp cache for the key being
// written so that the timestamp there is greater than the txn's
// ReadTimestamp.
func Test1PCTransactionWriteTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("key")
	txn := newTransaction("test", key, 1, tc.Clock())
	put := putArgs(key, []byte("value"))
	et, etH := endTxnArgs(txn, true)

	// Update the timestamp cache for the key being written.
	gArgs := getArgs(key)
	if _, pErr := tc.SendWrapped(&gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now verify that the write triggers a retry.
	var ba roachpb.BatchRequest
	ba.Header = etH
	ba.Add(&put, &et)
	assignSeqNumsForReqs(txn, &put, &et)
	_, pErr := tc.Sender().Send(ctx, ba)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Errorf("expected retry error; got %s", pErr)
	}
}

// TestEndTxnWithMalformedSplitTrigger verifies an EndTxn call with a malformed
// commit trigger fails.
func TestEndTxnWithMalformedSplitTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var exitStatus exit.Code
	log.SetExitFunc(true /* hideStack */, func(i exit.Code) {
		exitStatus = i
	})
	defer log.ResetExitFunc()

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("foo")
	txn := newTransaction("test", key, 1, tc.Clock())
	pArgs := putArgs(key, []byte("only here to make this a rw transaction"))
	assignSeqNumsForReqs(txn, &pArgs)
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		Txn: txn,
	}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	args, h := endTxnArgs(txn, true /* commit */)
	// Make an EndTxn request which would fail if not stripped. In this case, we
	// set the start key to "bar" for a split of the default range; start key
	// must be "" in this case.
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
	expErr := regexp.QuoteMeta("replica corruption (processed=true): range does not match splits")
	if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, expErr) {
		t.Errorf("unexpected error: %s", pErr)
	}

	if exitStatus != exit.FatalError() {
		t.Fatalf("unexpected exit status %d", exitStatus)
	}
}

// TestEndTxnBeforeHeartbeat verifies that a transaction can be
// committed/aborted before being heartbeat.
func TestEndTxnBeforeHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Don't automatically GC the Txn record: We want to heartbeat the
	// committed Transaction and compare it against our expectations.
	// When it's removed, the heartbeat would recreate it.
	defer setTxnAutoGC(false)()
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := []byte("a")
	testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
		key = roachpb.Key(key).Next()
		txn := newTransaction("test", key, 1, tc.Clock())
		h := roachpb.Header{Txn: txn}

		put := putArgs(key, key)
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := tc.SendWrappedWith(h, &put); pErr != nil {
			t.Fatal(pErr)
		}

		et, _ := endTxnArgs(txn, commit)
		assignSeqNumsForReqs(txn, &et)
		resp, pErr := tc.SendWrappedWith(h, &et)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*roachpb.EndTxnResponse)
		expStatus := roachpb.COMMITTED
		if !commit {
			expStatus = roachpb.ABORTED
		}
		if reply.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s; got %s", expStatus, reply.Txn.Status)
		}

		// Try a heartbeat to the already-committed transaction; should get
		// committed txn back, but without last heartbeat timestamp set.
		hBA, h := heartbeatArgs(txn, tc.Clock().Now())
		resp, pErr = tc.SendWrappedWith(h, &hBA)
		if pErr != nil {
			t.Error(pErr)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != expStatus {
			t.Errorf("expected transaction status to be %s, but got %s", expStatus, hBR.Txn.Status)
		}
	})
}

// TestEndTxnAfterHeartbeat verifies that a transaction can be committed/aborted
// after being heartbeat.
func TestEndTxnAfterHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("a")
	testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
		txn := newTransaction("test", key, 1, tc.Clock())
		h := roachpb.Header{Txn: txn}

		put := putArgs(key, key)
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := tc.SendWrappedWith(h, &put); pErr != nil {
			t.Fatal(pErr)
		}

		// Start out with a heartbeat to the transaction.
		hBA, _ := heartbeatArgs(txn, tc.Clock().Now())
		resp, pErr := tc.SendWrappedWith(h, &hBA)
		if pErr != nil {
			t.Fatal(pErr)
		}
		hBR := resp.(*roachpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != roachpb.PENDING {
			t.Errorf("expected transaction status to be %s, but got %s", hBR.Txn.Status, roachpb.PENDING)
		}

		et, h := endTxnArgs(txn, commit)
		assignSeqNumsForReqs(txn, &et)
		resp, pErr = tc.SendWrappedWith(h, &et)
		if pErr != nil {
			t.Error(pErr)
		}
		reply := resp.(*roachpb.EndTxnResponse)
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
	})
}

// TestEndTxnWithPushedTimestamp verifies that txn can be ended (both commit or
// abort) correctly when the commit timestamp is greater than the transaction
// timestamp, depending on the isolation level.
func TestEndTxnWithPushedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	testCases := []struct {
		commit bool
		expErr bool
	}{
		{true, true},
		{false, false},
	}
	key := roachpb.Key("a")
	for i, test := range testCases {
		pushee := newTransaction("pushee", key, 1, tc.Clock())
		pusher := newTransaction("pusher", key, 1, tc.Clock())
		pushee.Priority = enginepb.MinTxnPriority
		pusher.Priority = enginepb.MaxTxnPriority // pusher will win
		put := putArgs(key, []byte("value"))
		assignSeqNumsForReqs(pushee, &put)
		if _, pErr := kv.SendWrappedWith(
			ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put,
		); pErr != nil {
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
			reply := resp.(*roachpb.EndTxnResponse)
			if reply.Txn.Status != expStatus {
				t.Errorf("%d: expected transaction status to be %s; got %s", i, expStatus, reply.Txn.Status)
			}
		}
		key = key.Next()
	}
}

// TestEndTxnWithIncrementedEpoch verifies that txn ended with a higher epoch
// (and priority) correctly assumes the higher epoch.
func TestEndTxnWithIncrementedEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := []byte("a")
	txn := newTransaction("test", key, 1, tc.Clock())
	put := putArgs(key, key)
	assignSeqNumsForReqs(txn, &put)
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: txn}, &put); pErr != nil {
		t.Fatal(pErr)
	}

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
	reply := resp.(*roachpb.EndTxnResponse)
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

// TestEndTxnWithErrors verifies various error conditions are checked such as
// transaction already being committed or aborted, or timestamp or epoch
// regression.
func TestEndTxnWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	txn := newTransaction("test", roachpb.Key(""), 1, tc.Clock())

	testCases := []struct {
		key          roachpb.Key
		existStatus  roachpb.TransactionStatus
		existEpoch   enginepb.TxnEpoch
		expErrRegexp string
	}{
		{roachpb.Key("a"), roachpb.COMMITTED, txn.Epoch, "already committed"},
		{roachpb.Key("b"), roachpb.ABORTED, txn.Epoch,
			regexp.QuoteMeta("TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)")},
		{roachpb.Key("c"), roachpb.PENDING, txn.Epoch + 1, "epoch regression: 0"},
	}
	for _, test := range testCases {
		t.Run("", func(t *testing.T) {
			// Establish existing txn state by writing directly to range engine.
			existTxn := txn.Clone()
			existTxn.Key = test.key
			existTxn.Status = test.existStatus
			existTxn.Epoch = test.existEpoch
			existTxnRecord := existTxn.AsRecord()
			txnKey := keys.TransactionKey(test.key, txn.ID)
			if err := storage.MVCCPutProto(
				ctx, tc.repl.store.Engine(), nil, txnKey, hlc.Timestamp{}, nil, &existTxnRecord,
			); err != nil {
				t.Fatal(err)
			}

			// End the transaction, verify expected error.
			txn.Key = test.key
			args, h := endTxnArgs(txn, true)
			args.LockSpans = []roachpb.Span{{Key: txn.Key}}
			args.Sequence = 2

			if _, pErr := tc.SendWrappedWith(h, &args); !testutils.IsPError(pErr, test.expErrRegexp) {
				t.Fatalf("expected error:\n%s\nto match:\n%s", pErr, test.expErrRegexp)
			} else if txn := pErr.GetTxn(); txn != nil && txn.ID == (uuid.UUID{}) {
				// Prevent regression of #5591.
				t.Fatalf("received empty Transaction proto in error")
			}
		})
	}
}

// TestEndTxnWithErrorAndSyncIntentResolution verifies that an EndTransaction
// request that hits an error and then is forced to perform intent resolution
// synchronously does not deadlock on itself. This is a regression test against
// #47187.
func TestEndTxnWithErrorAndSyncIntentResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.IntentResolverKnobs.ForceSyncIntentResolution = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	txn := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())

	// Establish existing txn state by writing directly to range engine.
	existTxn := txn.Clone()
	existTxn.Status = roachpb.ABORTED
	existTxnRec := existTxn.AsRecord()
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	err := storage.MVCCPutProto(ctx, tc.repl.store.Engine(), nil, txnKey, hlc.Timestamp{}, nil, &existTxnRec)
	require.NoError(t, err)

	// End the transaction, verify expected error, shouldn't deadlock.
	args, h := endTxnArgs(txn, true)
	args.LockSpans = []roachpb.Span{{Key: txn.Key}}
	args.Sequence = 2

	_, pErr := tc.SendWrappedWith(h, &args)
	require.Regexp(t, `TransactionAbortedError\(ABORT_REASON_ABORTED_RECORD_FOUND\)`, pErr)
	require.NotNil(t, pErr.GetTxn())
	require.Equal(t, txn.ID, pErr.GetTxn().ID)
}

// TestEndTxnRollbackAbortedTransaction verifies that no error is returned when
// a transaction that has already been aborted is rolled back by an EndTxn
// request.
func TestEndTxnRollbackAbortedTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "populateAbortSpan", func(t *testing.T, populateAbortSpan bool) {
		tc := testContext{}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		cfg := TestStoreConfig(nil)
		cfg.TestingKnobs.DontPushOnWriteIntentError = true
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)

		key := []byte("a")
		txn := newTransaction("test", key, 1, tc.Clock())
		put := putArgs(key, key)
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := kv.SendWrappedWith(
			ctx, tc.Sender(), roachpb.Header{Txn: txn}, &put,
		); pErr != nil {
			t.Fatal(pErr)
		}
		// Simulate what the client is supposed to do (update the transaction
		// based on the response). The Writing field is needed by this test.

		// Abort the transaction by pushing it with maximum priority.
		pusher := newTransaction("test", key, 1, tc.Clock())
		pusher.Priority = enginepb.MaxTxnPriority
		pushArgs := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
		if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Check that the intent has not yet been resolved.
		var ba roachpb.BatchRequest
		gArgs := getArgs(key)
		ba.Add(&gArgs)
		if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
			t.Fatal(err)
		}
		_, pErr := tc.Sender().Send(ctx, ba)
		if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
			t.Errorf("expected write intent error, but got %s", pErr)
		}

		if populateAbortSpan {
			var txnRecord roachpb.Transaction
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			if ok, err := storage.MVCCGetProto(
				ctx, tc.repl.store.Engine(),
				txnKey, hlc.Timestamp{}, &txnRecord, storage.MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Fatalf("unexpected txn record %v", txnRecord)
			}

			if pErr := tc.store.intentResolver.ResolveIntents(ctx,
				[]roachpb.LockUpdate{
					roachpb.MakeLockUpdate(&txnRecord, roachpb.Span{Key: key}),
				}, intentresolver.ResolveOptions{Poison: true}); pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Abort the transaction again. No error is returned.
		args, h := endTxnArgs(txn, false /* commit */)
		args.LockSpans = []roachpb.Span{{Key: key}}
		resp, pErr := tc.SendWrappedWith(h, &args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*roachpb.EndTxnResponse)
		if reply.Txn.Status != roachpb.ABORTED {
			t.Errorf("expected transaction status to be ABORTED; got %s", reply.Txn.Status)
		}

		// Verify that the intent has been resolved.
		if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
			t.Errorf("expected resolved intent, but got %s", pErr)
		}
	})
}

// TestRPCRetryProtectionInTxn verifies that transactional batches
// enjoy protection from RPC replays.
func TestRPCRetryProtectionInTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	testutils.RunTrueAndFalse(t, "CanForwardReadTimestamp", func(t *testing.T, noPriorReads bool) {
		key := roachpb.Key("a")
		txn := newTransaction("test", key, 1, tc.Clock())

		// Send a batch with put & end txn.
		var ba roachpb.BatchRequest
		ba.CanForwardReadTimestamp = noPriorReads
		put := putArgs(key, []byte("value"))
		et, _ := endTxnArgs(txn, true)
		et.LockSpans = []roachpb.Span{{Key: key, EndKey: nil}}
		ba.Header = roachpb.Header{Txn: txn}
		ba.Add(&put)
		ba.Add(&et)
		assignSeqNumsForReqs(txn, &put, &et)
		_, pErr := tc.Sender().Send(ctx, ba)
		if pErr != nil {
			t.Fatalf("unexpected error: %s", pErr)
		}

		// Replay the request. It initially tries to execute as a 1PC transaction,
		// but will fail because of a WriteTooOldError that pushes the transaction.
		// This forces the txn to execute normally, at which point it fails because
		// the EndTxn is detected to be a duplicate.
		_, pErr = tc.Sender().Send(ctx, ba)
		if pErr == nil {
			t.Fatalf("expected error, got nil")
		}
		require.Regexp(t,
			`TransactionAbortedError\(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY\)`,
			pErr)
	})
}

// Test that errors from batch evaluation never have the WriteTooOld flag set.
// The WriteTooOld flag is supposed to only be set on successful responses.
//
// The test will construct a batch with a write that would normally cause the
// WriteTooOld flag to be set on the response, and another CPut which causes an
// error to be returned.
func TestErrorsDontCarryWriteTooOldFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	cfg := TestStoreConfig(nil /* clock */)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	// Start a transaction early to get a low timestamp.
	txn := roachpb.MakeTransaction("test", keyA, roachpb.NormalUserPriority,
		tc.Clock().Now(), 0 /* maxOffsetNs */, 0 /* coordinatorNodeID */)

	// Write a value outside of the txn to cause a WriteTooOldError later.
	put := putArgs(keyA, []byte("val1"))
	var ba roachpb.BatchRequest
	ba.Add(&put)
	_, pErr := tc.Sender().Send(ctx, ba)
	require.Nil(t, pErr)

	// This put will cause the WriteTooOld flag to be set.
	put = putArgs(keyA, []byte("val2"))
	// This will cause a ConditionFailedError.
	cput := cPutArgs(keyB, []byte("missing"), []byte("newVal"))
	ba.Header = roachpb.Header{Txn: &txn}
	ba.Add(&put)
	ba.Add(&cput)
	assignSeqNumsForReqs(&txn, &put, &cput)
	_, pErr = tc.Sender().Send(ctx, ba)
	require.IsType(t, pErr.GetDetail(), &roachpb.ConditionFailedError{})
	require.False(t, pErr.GetTxn().WriteTooOld)
}

// TestBatchRetryCantCommitIntents tests that transactional retries cannot
// commit intents.
// It also tests current behavior - that a retried transactional batch can lay
// down an intent that will never be committed. We don't necessarily like this
// behavior, though. Note that intents are not always left hanging by retries
// like they are in this low-level test. For example:
// - in case of Raft *reproposals*, the MaxLeaseIndex mechanism will make
// the reproposal not execute if the original proposal had already been
// applied.
// - in case of request *re-evaluations*, we know that the original proposal
// will not apply.
func TestBatchRetryCantCommitIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DontPushOnWriteIntentError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	key := roachpb.Key("a")
	keyB := roachpb.Key("b")
	txn := newTransaction("test", key, 1, tc.Clock())

	// Send a put for keyA.
	var ba roachpb.BatchRequest
	put := putArgs(key, []byte("value"))
	ba.Header = roachpb.Header{Txn: txn}
	ba.Add(&put)
	assignSeqNumsForReqs(txn, &put)
	if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
		t.Fatal(err)
	}
	br, pErr := tc.Sender().Send(ctx, ba)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Send a put for keyB.
	var ba2 roachpb.BatchRequest
	putB := putArgs(keyB, []byte("value"))
	putTxn := br.Txn.Clone()
	ba2.Header = roachpb.Header{Txn: putTxn}
	ba2.Add(&putB)
	assignSeqNumsForReqs(putTxn, &putB)
	br, pErr = tc.Sender().Send(ctx, ba2)
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// HeartbeatTxn.
	hbTxn := br.Txn.Clone()
	hb, hbH := heartbeatArgs(hbTxn, tc.Clock().Now())
	if _, pErr := tc.SendWrappedWith(hbH, &hb); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// EndTxn.
	etTxn := br.Txn.Clone()
	et, etH := endTxnArgs(etTxn, true)
	et.LockSpans = []roachpb.Span{{Key: key, EndKey: nil}, {Key: keyB, EndKey: nil}}
	assignSeqNumsForReqs(etTxn, &et)
	if _, pErr := tc.SendWrappedWith(etH, &et); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Verify txn record is cleaned.
	var readTxn roachpb.Transaction
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	ok, err := storage.MVCCGetProto(ctx, tc.repl.store.Engine(), txnKey,
		hlc.Timestamp{}, &readTxn, storage.MVCCGetOptions{})
	if err != nil || ok {
		t.Errorf("expected transaction record to be cleared (%t): %+v", ok, err)
	}

	// Now replay put for key A; this succeeds as there's nothing to detect
	// the replay. The WriteTooOld flag will be set though.
	br, pErr = tc.Sender().Send(ctx, ba)
	require.NoError(t, pErr.GoError())
	require.True(t, br.Txn.WriteTooOld)

	// Intent should have been created.
	gArgs := getArgs(key)
	_, pErr = tc.SendWrapped(&gArgs)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected WriteIntentError, got: %v", pErr)
	}

	// Heartbeat should fail with a TransactionAbortedError.
	_, pErr = tc.SendWrappedWith(hbH, &hb)
	expErr := "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)"
	if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
		t.Errorf("expected %s; got %v", expErr, pErr)
	}

	// EndTxn should fail with a TransactionAbortedError.
	_, pErr = tc.SendWrappedWith(etH, &et)
	if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
		t.Errorf("expected %s; got %v", expErr, pErr)
	}

	// Expect that the txn left behind an intent on key A.
	gArgs = getArgs(key)
	_, pErr = tc.SendWrapped(&gArgs)
	if _, ok := pErr.GetDetail().(*roachpb.WriteIntentError); !ok {
		t.Errorf("expected WriteIntentError, got: %v", pErr)
	}
}

// TestEndTxnGC verifies that a transaction record is immediately
// garbage-collected upon EndTxn iff all of the supplied intents are local
// relative to the transaction record's location.
func TestEndTxnLocalGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if filterArgs.Req.Method() == roachpb.GC {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	splitKey := roachpb.RKey("c")
	splitTestRange(tc.store, splitKey, t)
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
		txn := newTransaction("test", key, 1, tc.Clock())
		put := putArgs(putKey, key)
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: txn}, &put); pErr != nil {
			t.Fatal(pErr)
		}
		putKey = putKey.Next() // for the next iteration
		args, h := endTxnArgs(txn, true)
		args.LockSpans = test.intents
		assignSeqNumsForReqs(txn, &args)
		if _, pErr := tc.SendWrappedWith(h, &args); pErr != nil {
			t.Fatal(pErr)
		}
		var readTxn roachpb.Transaction
		txnKey := keys.TransactionKey(txn.Key, txn.ID)
		ok, err := storage.MVCCGetProto(ctx, tc.repl.store.Engine(), txnKey, hlc.Timestamp{},
			&readTxn, storage.MVCCGetOptions{})
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
	newRepl := splitTestRange(tc.store, splitKey, t)

	txn := newTransaction("test", key, 1, tc.Clock())
	// This increment is not required, but testing feels safer when zero
	// values are unexpected.
	txn.Epoch++
	pArgs := putArgs(key, []byte("value"))
	h := roachpb.Header{Txn: txn}
	assignSeqNumsForReqs(txn, &pArgs)
	if _, pErr := kv.SendWrappedWith(context.Background(), tc.Sender(), h, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	{
		var ba roachpb.BatchRequest
		ba.Header = h
		ba.RangeID = newRepl.RangeID
		if err := ba.SetActiveTimestamp(newRepl.store.Clock()); err != nil {
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
	args.LockSpans = []roachpb.Span{{Key: key}, {Key: splitKey.AsRawKey()}}
	assignSeqNumsForReqs(txn, &args)
	if _, pErr := tc.SendWrappedWith(h, &args); pErr != nil {
		t.Fatal(pErr)
	}
	return newRepl, txn
}

// TestEndTxnResolveOnlyLocalIntents verifies that an end transaction request
// resolves only local intents within the same batch.
func TestEndTxnResolveOnlyLocalIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.DontPushOnWriteIntentError = true
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				return roachpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			}
			return nil
		}

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	newRepl, txn := setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Check if the intent in the other range has not yet been resolved.
	{
		var ba roachpb.BatchRequest
		ba.Header.RangeID = newRepl.RangeID
		gArgs := getArgs(splitKey)
		ba.Add(&gArgs)
		if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
			t.Fatal(err)
		}
		_, pErr := newRepl.Send(ctx, ba)
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
	if !reflect.DeepEqual(hbResp.Txn.LockSpans, expIntents) {
		t.Fatalf("expected persisted intents %v, got %v",
			expIntents, hbResp.Txn.LockSpans)
	}
}

// TestEndTxnDirectGC verifies that after successfully resolving the external
// intents of a transaction after EndTxn, the transaction and AbortSpan records
// are purged on both the local range and non-local range.
func TestEndTxnDirectGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	a := roachpb.Key("a")
	splitKey := keys.MustAddr(a).Next()

	for i, testKey := range []roachpb.Key{
		a,
		keys.RangeDescriptorKey(keys.MustAddr(a)),
		keys.RangeDescriptorKey(keys.MustAddr(roachpb.KeyMin)),
	} {
		func() {
			ctx := context.Background()
			ctx = logtags.AddTag(ctx, "testcase", i)
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(ctx, t, stopper)

			rightRepl, txn := setupResolutionTest(t, tc, testKey, splitKey, false /* generate AbortSpan entry */)

			testutils.SucceedsSoon(t, func() error {
				var gr roachpb.GetResponse
				if _, err := batcheval.Get(
					ctx, tc.engine, batcheval.CommandArgs{
						EvalCtx: NewReplicaEvalContext(tc.repl, allSpans()),
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

// TestEndTxnDirectGCFailure verifies that no immediate GC takes place
// if external intents can't be resolved (see also TestEndTxnDirectGC).
func TestEndTxnDirectGCFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	tc := testContext{}
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	var count int64
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
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
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	setupResolutionTest(t, tc, key, splitKey, true /* commit */)

	// Now test that no GCRequest is issued. We can't test that directly (since
	// it's completely asynchronous), so we first make sure ResolveIntent
	// happened and subsequently issue a bogus Put which is likely to make it
	// into Raft only after a rogue GCRequest (at least sporadically), which
	// would trigger a Fatal from the command filter.
	testutils.SucceedsSoon(t, func() error {
		if atomic.LoadInt64(&count) == 0 {
			return errors.Errorf("intent resolution not attempted yet")
		} else if err := tc.store.DB().Put(ctx, "panama", "banana"); err != nil {
			return err
		}
		return nil
	})
}

// TestEndTxnDirectGC_1PC runs a test similar to TestEndTxnDirectGC
// for the case of a transaction which is contained in a single batch.
func TestEndTxnDirectGC_1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, commit := range []bool{true, false} {
		func() {
			ctx := context.Background()
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(ctx, t, stopper)

			key := roachpb.Key("a")
			txn := newTransaction("test", key, 1, tc.Clock())
			put := putArgs(key, []byte("value"))
			et, etH := endTxnArgs(txn, commit)
			et.LockSpans = []roachpb.Span{{Key: key}}
			assignSeqNumsForReqs(txn, &put, &et)

			var ba roachpb.BatchRequest
			ba.Header = etH
			ba.Add(&put, &et)
			br, err := tc.Sender().Send(ctx, ba)
			if err != nil {
				t.Fatalf("commit=%t: %+v", commit, err)
			}
			etArgs, ok := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.EndTxnResponse)
			if !ok || (!etArgs.OnePhaseCommit && commit) {
				t.Errorf("commit=%t: expected one phase commit", commit)
			}

			var entry roachpb.AbortSpanEntry
			if aborted, err := tc.repl.abortSpan.Get(ctx, tc.engine, txn.ID, &entry); err != nil {
				t.Fatal(err)
			} else if aborted {
				t.Fatalf("commit=%t: AbortSpan still populated: %v", commit, entry)
			}
		}()
	}
}

// TestReplicaTransactionRequires1PC verifies that a transaction which sets
// Requires1PC on EndTxn request will never leave intents in the event that it
// experiences an error or the timestamp is advanced.
func TestReplicaTransactionRequires1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tsc := TestStoreConfig(nil)
	var injectErrorOnKey atomic.Value
	injectErrorOnKey.Store(roachpb.Key(""))

	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Method() == roachpb.Put &&
				injectErrorOnKey.Load().(roachpb.Key).Equal(filterArgs.Req.Header().Key) {
				return roachpb.NewErrorf("injected error")
			}
			return nil
		}
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

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
			txn := newTransaction("test", key, 1, tc.Clock())
			put := putArgs(key, []byte("value"))
			et, etH := endTxnArgs(txn, true)
			et.Require1PC = true
			ba.Header = etH
			ba.Add(&put, &et)
			assignSeqNumsForReqs(txn, &put, &et)

			// Run the setup method.
			test.setupFn(key)

			// Send the batch command.
			_, pErr := tc.Sender().Send(ctx, ba)
			if !testutils.IsPError(pErr, test.expErrorPat) {
				t.Errorf("expected error=%q running required 1PC txn; got %s", test.expErrorPat, pErr)
			}

			// Do a consistent scan to verify no intents were created.
			sArgs := scanArgs(key, key.Next())
			_, pErr = tc.SendWrapped(sArgs)
			if pErr != nil {
				t.Fatalf("error scanning to verify no intent present: %s", pErr)
			}
		})
	}
}

// TestReplicaEndTxnWithRequire1PC verifies an error if an EndTxn request is
// received with the Requires1PC flag set to true.
func TestReplicaEndTxnWithRequire1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, tc.Clock())
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	ba.Add(&put)
	assignSeqNumsForReqs(txn, &put)
	if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	et, etH := endTxnArgs(txn, true)
	et.Require1PC = true
	ba = roachpb.BatchRequest{}
	ba.Header = etH
	ba.Add(&et)
	assignSeqNumsForReqs(txn, &et)
	_, pErr := tc.Sender().Send(ctx, ba)
	if !testutils.IsPError(pErr, "could not commit in one phase as requested") {
		t.Fatalf("expected requires 1PC error; fgot %v", pErr)
	}
}

// TestAbortSpanPoisonOnResolve verifies that when an intent is
// aborted, the AbortSpan on the respective Range is poisoned and
// the pushee is presented with a txn abort on its next contact with
// the Range in the same epoch.
func TestAbortSpanPoisonOnResolve(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	key := roachpb.Key("a")

	// Whether we're going to abort the pushee.
	// Run the actual meat of the test, which pushes the pushee and
	// checks whether we get the correct behavior as it touches the
	// Range again.
	run := func(abort bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		pusher := newTransaction("test", key, 1, tc.Clock())
		pushee := newTransaction("test", key, 1, tc.Clock())
		pusher.Priority = enginepb.MaxTxnPriority
		pushee.Priority = enginepb.MinTxnPriority // pusher will win

		inc := func(actor *roachpb.Transaction, k roachpb.Key) (*roachpb.IncrementResponse, *roachpb.Error) {
			incArgs := &roachpb.IncrementRequest{
				RequestHeader: roachpb.RequestHeader{Key: k}, Increment: 123,
			}
			assignSeqNumsForReqs(actor, incArgs)
			reply, pErr := kv.SendWrappedWith(ctx, tc.store, roachpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, incArgs)
			if pErr != nil {
				return nil, pErr
			}
			return reply.(*roachpb.IncrementResponse), nil
		}

		get := func(actor *roachpb.Transaction, k roachpb.Key) *roachpb.Error {
			gArgs := getArgs(k)
			assignSeqNumsForReqs(actor, &gArgs)
			_, pErr := kv.SendWrappedWith(ctx, tc.store, roachpb.Header{
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
					return errors.Errorf("abort=%t: expected txn abort, got %s", abort, pErr)
				}
				return nil
			}
		} else {
			// Verify we're not poisoned.
			assert = func(pErr *roachpb.Error) error {
				if pErr != nil {
					return errors.Errorf("abort=%t: unexpected: %s", abort, pErr)
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
		run(abort)
	}
}

// TestAbortSpanError verifies that roachpb.Errors returned by checkIfTxnAborted
// have txns that are identical to txns stored in Transaction{Retry,Aborted}Error.
func TestAbortSpanError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	txn := roachpb.Transaction{}
	txn.ID = uuid.MakeV4()
	txn.Priority = 1
	txn.Sequence = 1
	txn.WriteTimestamp = tc.Clock().Now().Add(1, 0)

	key := roachpb.Key("k")
	ts := txn.WriteTimestamp.Next()
	priority := enginepb.TxnPriority(10)
	entry := roachpb.AbortSpanEntry{
		Key:       key,
		Timestamp: ts,
		Priority:  priority,
	}
	if err := tc.repl.abortSpan.Put(ctx, tc.engine, nil, txn.ID, &entry); err != nil {
		t.Fatal(err)
	}

	rec := &SpanSetReplicaEvalContext{tc.repl, *allSpans()}
	pErr := checkIfTxnAborted(ctx, rec, tc.engine, txn)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); ok {
		expected := txn.Clone()
		expected.WriteTimestamp = txn.WriteTimestamp
		expected.Priority = priority
		expected.Status = roachpb.ABORTED
		if pErr.GetTxn() == nil || !reflect.DeepEqual(pErr.GetTxn(), expected) {
			t.Errorf("txn does not match: %s vs. %s", pErr.GetTxn(), expected)
		}
	} else {
		t.Errorf("unexpected error: %s", pErr)
	}
}

// TestPushTxnBadKey verifies that args.Key equals args.PusheeTxn.ID.
func TestPushTxnBadKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	pusher := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())
	pushee := newTransaction("test", roachpb.Key("b"), 1, tc.Clock())

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
	defer log.Scope(t).Close(t)

	// This test simulates running into an open intent and resolving it using
	// the transaction record. We test this in two ways:
	// 1. The first prevents the transaction record from being GCed by the
	// EndTxn request. The effect of this is that the pusher finds the
	// transaction record in a finalized status and returns it directly.
	// 2. The second allows the transaction record to be GCed by the EndTxn
	// request. The effect of this is that the pusher finds no transaction
	// record but discovers that the transaction has already been finalized
	// using the timestamp cache. It doesn't know whether the transaction
	// was COMMITTED or ABORTED, so it is forced to be conservative and return
	// an ABORTED transaction.
	testutils.RunTrueAndFalse(t, "auto-gc", func(t *testing.T, autoGC bool) {
		defer setTxnAutoGC(autoGC)()

		// Test for COMMITTED and ABORTED transactions.
		testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
			ctx := context.Background()
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(ctx, t, stopper)

			key := roachpb.Key(fmt.Sprintf("key-%t-%t", autoGC, commit))
			pusher := newTransaction("test", key, 1, tc.Clock())
			pushee := newTransaction("test", key, 1, tc.Clock())

			// Begin the pushee's transaction.
			put := putArgs(key, key)
			assignSeqNumsForReqs(pushee, &put)
			if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put); pErr != nil {
				t.Fatal(pErr)
			}
			// End the pushee's transaction.
			etArgs, h := endTxnArgs(pushee, commit)
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

			// We expect the push to return an ABORTED transaction record for all
			// cases except when the transaction is COMMITTED and its record is not
			// GCed. The case where it is COMMITTED and its record is GCed can be
			// surprising, but doesn't result in problems because a transaction must
			// resolve all of its intents before garbage collecting its record, so
			// the pusher won't end up removing a still-pending intent for a
			// COMMITTED transaction.
			expStatus := roachpb.ABORTED
			if commit && !autoGC {
				expStatus = roachpb.COMMITTED
			}
			if reply.PusheeTxn.Status != expStatus {
				t.Errorf("expected push txn to return with status == %s; got %+v", expStatus, reply.PusheeTxn)
			}
		})
	})
}

// TestPushTxnUpgradeExistingTxn verifies that pushing a transaction record
// with a new timestamp upgrades the pushee's timestamp if greater. In all
// test cases, the priorities are set such that the push will succeed.
func TestPushTxnUpgradeExistingTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	testCases := []struct {
		startOffset, pushOffset, expOffset int64
	}{
		// Noop.
		{1, 1, 1},
		// Move timestamp forward.
		{1, 2, 2},
		// Move timestamp backwards (has no effect).
		{2, 1, 2},
	}

	for i, test := range testCases {
		now := tc.Clock().Now()
		startTS := now.Add(test.startOffset, 0)
		pushTS := now.Add(test.pushOffset, 0)
		expTS := now.Add(test.expOffset, 0)

		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("pusher", key, 1, tc.Clock())
		pushee := newTransaction("pushee", key, 1, tc.Clock())
		pushee.Epoch = 12345
		pusher.Priority = enginepb.MaxTxnPriority // Pusher will win

		// First, establish "start" of existing pushee's txn via HeartbeatTxn.
		pushee.WriteTimestamp = startTS
		pushee.LastHeartbeat = startTS
		pushee.ReadTimestamp = startTS
		hb, hbH := heartbeatArgs(pushee, pushee.WriteTimestamp)
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), hbH, &hb); pErr != nil {
			t.Fatal(pErr)
		}

		// Now, attempt to push the transaction using updated timestamp.
		pushee.WriteTimestamp = pushTS
		args := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)

		// Set header timestamp to the maximum of the pusher and pushee timestamps.
		h := roachpb.Header{Timestamp: args.PushTo}
		h.Timestamp.Forward(pushee.WriteTimestamp)
		resp, pErr := tc.SendWrappedWith(h, &args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*roachpb.PushTxnResponse)
		expTxnRecord := pushee.AsRecord()
		expTxn := expTxnRecord.AsTransaction()
		expTxn.Priority = enginepb.MaxTxnPriority - 1
		expTxn.Epoch = pushee.Epoch // no change
		expTxn.WriteTimestamp = expTS
		expTxn.Status = roachpb.ABORTED

		if !reflect.DeepEqual(expTxn, reply.PusheeTxn) {
			t.Fatalf("unexpected push txn in trial %d: %s", i, pretty.Diff(expTxn, reply.PusheeTxn))
		}
	}
}

// TestPushTxnQueryPusheerHasNewerVersion verifies that PushTxn
// uses the newer version of the pushee in a push request.
func TestPushTxnQueryPusheeHasNewerVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DontRetryPushTxnFailures = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	key := roachpb.Key("key")
	pushee := newTransaction("test", key, 1, tc.Clock())
	pushee.Priority = 1
	pushee.Epoch = 12345
	pushee.Sequence = 2
	ts := tc.Clock().Now()
	pushee.WriteTimestamp = ts
	pushee.LastHeartbeat = ts

	pusher := newTransaction("test", key, 1, tc.Clock())
	pusher.Priority = 2

	put := putArgs(key, key)
	assignSeqNumsForReqs(pushee, &put)
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put); pErr != nil {
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

// TestPushTxnHeartbeatTimeout verifies that a txn which hasn't been
// heartbeat within its transaction liveness threshold can be pushed/aborted.
func TestPushTxnHeartbeatTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{manualClock: hlc.NewManualClock(123)}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.DontRetryPushTxnFailures = true
	cfg.TestingKnobs.DontRecoverIndeterminateCommits = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	const noError = ""
	const txnPushError = "failed to push"
	const indetCommitError = "txn in indeterminate STAGING state"

	m := int64(txnwait.TxnLivenessHeartbeatMultiplier)
	ns := base.DefaultTxnHeartbeatInterval.Nanoseconds()
	testCases := []struct {
		status          roachpb.TransactionStatus // -1 for no record
		heartbeatOffset int64                     // nanoseconds from original timestamp, 0 for no heartbeat
		timeOffset      int64                     // nanoseconds from original timestamp
		pushType        roachpb.PushTxnType
		expErr          string
	}{
		// Avoid using offsets that result in outcomes that depend on logical
		// ticks.
		{roachpb.PENDING, 0, 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, 0, 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, 0, 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, 0, ns, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, 0, ns, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, 0, ns, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, 0, m*ns - 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, 0, m*ns - 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, 0, m*ns - 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, 0, m*ns + 1, roachpb.PUSH_TIMESTAMP, noError},
		{roachpb.PENDING, 0, m*ns + 1, roachpb.PUSH_ABORT, noError},
		{roachpb.PENDING, 0, m*ns + 1, roachpb.PUSH_TOUCH, noError},
		{roachpb.PENDING, ns, m*ns + 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, ns, m*ns + 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, ns, m*ns + 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns - 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns - 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns - 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns + 1, roachpb.PUSH_TIMESTAMP, noError},
		{roachpb.PENDING, ns, (m+1)*ns + 1, roachpb.PUSH_ABORT, noError},
		{roachpb.PENDING, ns, (m+1)*ns + 1, roachpb.PUSH_TOUCH, noError},
		// If the transaction record is STAGING then any case that previously
		// returned a TransactionPushError will continue to return that error,
		// but any case that previously succeeded in pushing the transaction
		// will now return an IndeterminateCommitError.
		{roachpb.STAGING, 0, 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, 0, 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, 0, 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, 0, ns, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, 0, ns, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, 0, ns, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, 0, m*ns - 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, 0, m*ns - 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, 0, m*ns - 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, 0, m*ns + 1, roachpb.PUSH_TIMESTAMP, indetCommitError},
		{roachpb.STAGING, 0, m*ns + 1, roachpb.PUSH_ABORT, indetCommitError},
		{roachpb.STAGING, 0, m*ns + 1, roachpb.PUSH_TOUCH, indetCommitError},
		{roachpb.STAGING, ns, m*ns + 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, ns, m*ns + 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, ns, m*ns + 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns - 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns - 1, roachpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns - 1, roachpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns + 1, roachpb.PUSH_TIMESTAMP, indetCommitError},
		{roachpb.STAGING, ns, (m+1)*ns + 1, roachpb.PUSH_ABORT, indetCommitError},
		{roachpb.STAGING, ns, (m+1)*ns + 1, roachpb.PUSH_TOUCH, indetCommitError},
		// Even when a transaction record doesn't exist, if the timestamp
		// from the PushTxn request indicates sufficiently recent client
		// activity, the push will fail.
		{-1, 0, 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{-1, 0, 1, roachpb.PUSH_ABORT, txnPushError},
		{-1, 0, 1, roachpb.PUSH_TOUCH, txnPushError},
		{-1, 0, ns, roachpb.PUSH_TIMESTAMP, txnPushError},
		{-1, 0, ns, roachpb.PUSH_ABORT, txnPushError},
		{-1, 0, ns, roachpb.PUSH_TOUCH, txnPushError},
		{-1, 0, m*ns - 1, roachpb.PUSH_TIMESTAMP, txnPushError},
		{-1, 0, m*ns - 1, roachpb.PUSH_ABORT, txnPushError},
		{-1, 0, m*ns - 1, roachpb.PUSH_TOUCH, txnPushError},
		{-1, 0, m*ns + 1, roachpb.PUSH_TIMESTAMP, noError},
		{-1, 0, m*ns + 1, roachpb.PUSH_ABORT, noError},
		{-1, 0, m*ns + 1, roachpb.PUSH_TOUCH, noError},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pushee := newTransaction(fmt.Sprintf("test-%d", i), key, 1, tc.Clock())
		pusher := newTransaction("pusher", key, 1, tc.Clock())

		// Add the pushee's heartbeat offset.
		if test.heartbeatOffset != 0 {
			if test.status == -1 {
				t.Fatal("cannot heartbeat transaction record if it doesn't exist")
			}
			pushee.LastHeartbeat = pushee.ReadTimestamp.Add(test.heartbeatOffset, 0)
		}

		switch test.status {
		case -1:
			// Do nothing.
		case roachpb.PENDING:
			// Establish "start" of existing pushee's txn via HeartbeatTxn request
			// if the test case wants an existing transaction record.
			hb, hbH := heartbeatArgs(pushee, pushee.WriteTimestamp)
			if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), hbH, &hb); pErr != nil {
				t.Fatalf("%d: %s", i, pErr)
			}
		case roachpb.STAGING:
			et, etH := endTxnArgs(pushee, true)
			et.InFlightWrites = []roachpb.SequencedWrite{
				{Key: key, Sequence: 1},
			}
			if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), etH, &et); pErr != nil {
				t.Fatalf("%d: %s", i, pErr)
			}
		default:
			t.Fatalf("unexpected status: %v", test.status)
		}

		// Now, attempt to push the transaction.
		args := pushTxnArgs(pusher, pushee, test.pushType)
		args.PushTo = pushee.ReadTimestamp.Add(0, 1)
		h := roachpb.Header{Timestamp: args.PushTo}

		// Set the manual clock to the txn start time + offset. This is the time
		// source used to detect transaction expiration. We make sure to set it
		// above h.Timestamp to avoid it being updated by the request.
		now := pushee.ReadTimestamp.Add(test.timeOffset, 0)
		tc.manualClock.Set(now.WallTime)

		reply, pErr := tc.SendWrappedWith(h, &args)
		if !testutils.IsPError(pErr, test.expErr) {
			t.Fatalf("%d: expected error %q; got %v, args=%+v, reply=%+v", i, test.expErr, pErr, args, reply)
		}
		if reply != nil {
			if txn := reply.(*roachpb.PushTxnResponse).PusheeTxn; txn.Status != roachpb.ABORTED {
				t.Errorf("%d: expected aborted transaction, got %s", i, txn)
			}
		}
	}
}

// TestResolveIntentPushTxnReplyTxn makes sure that no Txn is returned from
// PushTxn and that it and ResolveIntent{,Range} can not be carried out in a
// transaction.
func TestResolveIntentPushTxnReplyTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	b := tc.engine.NewBatch()
	defer b.Close()

	txn := newTransaction("test", roachpb.Key("test"), 1, tc.Clock())
	txnPushee := txn.Clone()
	pa := pushTxnArgs(txn, txnPushee, roachpb.PUSH_ABORT)
	pa.Force = true
	var ms enginepb.MVCCStats
	var ra roachpb.ResolveIntentRequest
	var rra roachpb.ResolveIntentRangeRequest

	h := roachpb.Header{Txn: txn, Timestamp: tc.Clock().Now()}
	// Should not be able to push or resolve in a transaction.
	if _, err := batcheval.PushTxn(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: h, Args: &pa}, &roachpb.PushTxnResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional PushTxn returned unexpected error: %+v", err)
	}
	if _, err := batcheval.ResolveIntent(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: h, Args: &ra}, &roachpb.ResolveIntentResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntent returned unexpected error: %+v", err)
	}
	if _, err := batcheval.ResolveIntentRange(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: h, Args: &rra}, &roachpb.ResolveIntentRangeResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntentRange returned unexpected error: %+v", err)
	}

	// Should not get a transaction back from PushTxn. It used to erroneously
	// return args.PusherTxn.
	h = roachpb.Header{Timestamp: tc.Clock().Now()}
	var reply roachpb.PushTxnResponse
	if _, err := batcheval.PushTxn(ctx, b, batcheval.CommandArgs{EvalCtx: tc.repl, Stats: &ms, Header: h, Args: &pa}, &reply); err != nil {
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DontRetryPushTxnFailures = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	now := tc.Clock().Now()
	ts1 := now.Add(1, 0)
	ts2 := now.Add(2, 0)
	testCases := []struct {
		pusherPriority, pusheePriority enginepb.TxnPriority
		pusherTS, pusheeTS             hlc.Timestamp
		pushType                       roachpb.PushTxnType
		expSuccess                     bool
	}{
		// Pusher with higher priority succeeds.
		{enginepb.MaxTxnPriority, enginepb.MinTxnPriority, ts1, ts1, roachpb.PUSH_TIMESTAMP, true},
		{enginepb.MaxTxnPriority, enginepb.MinTxnPriority, ts1, ts1, roachpb.PUSH_ABORT, true},
		// Pusher with lower priority fails.
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts1, roachpb.PUSH_ABORT, false},
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts1, roachpb.PUSH_TIMESTAMP, false},
		// Pusher with lower priority fails, even with older txn timestamp.
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts2, roachpb.PUSH_ABORT, false},
		// Pusher has lower priority, but older txn timestamp allows success if
		// !abort since there's nothing to do.
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts2, roachpb.PUSH_TIMESTAMP, true},
		// When touching, priority never wins.
		{enginepb.MaxTxnPriority, enginepb.MinTxnPriority, ts1, ts1, roachpb.PUSH_TOUCH, false},
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts1, roachpb.PUSH_TOUCH, false},
	}

	for i, test := range testCases {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))
		pusher := newTransaction("test", key, 1, tc.Clock())
		pushee := newTransaction("test", key, 1, tc.Clock())
		pusher.Priority = test.pusherPriority
		pushee.Priority = test.pusheePriority
		pusher.MinTimestamp = test.pusherTS
		pushee.MinTimestamp = test.pusheeTS
		pusher.WriteTimestamp = test.pusherTS
		pushee.WriteTimestamp = test.pusheeTS
		// Make sure pusher ID is greater; if priorities and timestamps are the same,
		// the greater ID succeeds with push.
		if bytes.Compare(pusher.ID.GetBytes(), pushee.ID.GetBytes()) < 0 {
			pusher.ID, pushee.ID = pushee.ID, pusher.ID
		}

		put := putArgs(key, key)
		assignSeqNumsForReqs(pushee, &put)
		if _, pErr := kv.SendWrappedWith(context.Background(), tc.Sender(), roachpb.Header{Txn: pushee}, &put); pErr != nil {
			t.Fatal(pErr)
		}
		// Now, attempt to push the transaction with intent epoch set appropriately.
		args := pushTxnArgs(pusher, pushee, test.pushType)

		// Set header timestamp to the maximum of the pusher and pushee timestamps.
		h := roachpb.Header{Timestamp: args.PushTo}
		h.Timestamp.Forward(pushee.MinTimestamp)
		_, pErr := tc.SendWrappedWith(h, &args)

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

// TestPushTxnPushTimestamp verifies that with PUSH_TIMESTAMP pushes (i.e. for
// read/write conflict), the pushed txn keeps status PENDING, but has its txn
// Timestamp moved forward to the pusher's txn Timestamp + 1.
func TestPushTxnPushTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "synthetic", func(t *testing.T, synthetic bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		pusher := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())
		pushee := newTransaction("test", roachpb.Key("b"), 1, tc.Clock())
		pusher.Priority = enginepb.MaxTxnPriority
		pushee.Priority = enginepb.MinTxnPriority // pusher will win
		now := tc.Clock().Now()
		pusher.WriteTimestamp = now.Add(50, 25).WithSynthetic(synthetic)
		pushee.WriteTimestamp = now.Add(5, 1)

		key := roachpb.Key("a")
		put := putArgs(key, key)
		assignSeqNumsForReqs(pushee, &put)
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put); pErr != nil {
			t.Fatal(pErr)
		}

		// Now, push the transaction using a PUSH_TIMESTAMP push request.
		args := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)

		resp, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: args.PushTo}, &args)
		if pErr != nil {
			t.Fatalf("unexpected error on push: %s", pErr)
		}
		expTS := pusher.WriteTimestamp
		expTS.Logical++
		reply := resp.(*roachpb.PushTxnResponse)
		if reply.PusheeTxn.WriteTimestamp != expTS {
			t.Errorf("expected timestamp to be pushed to %+v; got %+v", expTS, reply.PusheeTxn.WriteTimestamp)
		}
		if reply.PusheeTxn.Status != roachpb.PENDING {
			t.Errorf("expected pushed txn to have status PENDING; got %s", reply.PusheeTxn.Status)
		}

		// Sanity check clock update, or lack thereof.
		after := tc.Clock().Now()
		require.Equal(t, synthetic, after.Less(expTS))
	})
}

// TestPushTxnPushTimestampAlreadyPushed verifies that pushing
// a timestamp forward which is already far enough forward is a simple
// noop. We do this by ensuring that priorities would otherwise make
// pushing impossible.
func TestPushTxnPushTimestampAlreadyPushed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	pusher := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())
	pushee := newTransaction("test", roachpb.Key("b"), 1, tc.Clock())
	now := tc.Clock().Now()
	pusher.WriteTimestamp = now.Add(50, 0)
	pushee.WriteTimestamp = now.Add(50, 1)

	key := roachpb.Key("a")
	put := putArgs(key, key)
	assignSeqNumsForReqs(pushee, &put)
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, push the transaction using a PUSH_TIMESTAMP push request.
	args := pushTxnArgs(pusher, pushee, roachpb.PUSH_TIMESTAMP)

	resp, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: args.PushTo}, &args)
	if pErr != nil {
		t.Fatalf("unexpected pError on push: %s", pErr)
	}
	reply := resp.(*roachpb.PushTxnResponse)
	if reply.PusheeTxn.WriteTimestamp != pushee.WriteTimestamp {
		t.Errorf("expected timestamp to be equal to original %+v; got %+v", pushee.WriteTimestamp, reply.PusheeTxn.WriteTimestamp)
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
// overwrote the transaction record on the second epoch.
func TestPushTxnSerializableRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("a")
	pushee := newTransaction("test", key, 1, tc.Clock())
	pusher := newTransaction("test", key, 1, tc.Clock())
	pushee.Priority = enginepb.MinTxnPriority
	pusher.Priority = enginepb.MaxTxnPriority // pusher will win

	// Read from the key to increment the timestamp cache.
	gArgs := getArgs(key)
	if _, pErr := tc.SendWrapped(&gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Write to a key.
	put := putArgs(key, []byte("foo"))
	assignSeqNumsForReqs(pushee, &put)
	resp, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put)
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
	pushee.Restart(1, 1, pusher.WriteTimestamp)

	// Next push pushee to advance timestamp of txn record.
	pusher.WriteTimestamp = tc.repl.store.Clock().Now()
	args := pushTxnArgs(pusher, &pusheeCopy, roachpb.PUSH_TIMESTAMP)
	if _, pErr := tc.SendWrapped(&args); pErr != nil {
		t.Fatal(pErr)
	}

	// Try to end pushed transaction at restart timestamp, which is
	// earlier than its now-pushed timestamp. Should fail.
	var ba roachpb.BatchRequest
	ba.Add(&put)
	ba.Add(&etArgs)
	ba.Header.Txn = pushee
	assignSeqNumsForReqs(pushee, &put, &etArgs)
	_, pErr = tc.Sender().Send(ctx, ba)
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Fatalf("expected retry error; got %s", pErr)
	}
	// Verify that the returned transaction has timestamp equal to the pushed
	// timestamp. This verifies that the EndTxn found the pushed record and
	// propagated it.
	if txn := pErr.GetTxn(); txn.WriteTimestamp != pusher.WriteTimestamp.Next() {
		t.Errorf("expected retry error txn timestamp %s; got %s", pusher.WriteTimestamp, txn.WriteTimestamp)
	}
}

// TestQueryIntentRequest tests QueryIntent requests in a number of scenarios,
// both with and without the ErrorIfMissing option set to true.
func TestQueryIntentRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "errIfMissing", func(t *testing.T, errIfMissing bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		key1 := roachpb.Key("a")
		key2 := roachpb.Key("b")
		txn := newTransaction("test", key1, 1, tc.Clock())

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
			var h roachpb.Header
			if baTxn != nil {
				h.Txn = baTxn
			} else {
				h.Timestamp = txnMeta.WriteTimestamp
			}
			qiArgs := queryIntentArgs(key, txnMeta, errIfMissing)
			qiRes, pErr := tc.SendWrappedWith(h, &qiArgs)
			if errIfMissing && !expectIntent {
				ownIntent := baTxn != nil
				if ownIntent && txnMeta.WriteTimestamp.Less(txn.WriteTimestamp) {
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

		for i, baTxn := range []*roachpb.Transaction{nil, txn} {
			// Query the intent with the correct txn meta. Should see intent regardless
			// of whether we're inside the txn or not.
			queryIntent(key1, txn.TxnMeta, baTxn, true)

			// Query an intent on a different key for the same transaction. Should not
			// see an intent.
			keyPrevent := roachpb.Key(fmt.Sprintf("%s-%t-%d", key2, errIfMissing, i))
			queryIntent(keyPrevent, txn.TxnMeta, baTxn, false)

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
			largerTSMeta.WriteTimestamp = largerTSMeta.WriteTimestamp.Next()
			largerBATxn := baTxn
			if largerBATxn != nil {
				largerBATxn = largerBATxn.Clone()
				largerBATxn.WriteTimestamp = largerTSMeta.WriteTimestamp
			}
			queryIntent(key1, largerTSMeta, largerBATxn, true)

			// Query the intent with a smaller timestamp. Should not see an
			// intent unless we're querying our own intent, in which case
			// the smaller timestamp will be forwarded to the batch header
			// transaction's timestamp.
			smallerTSMeta := txn.TxnMeta
			smallerTSMeta.WriteTimestamp = smallerTSMeta.WriteTimestamp.Prev()
			queryIntent(key1, smallerTSMeta, baTxn, baTxn == txn)

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

			// Perform a write at keyPrevent. The associated intent at this key
			// was queried and found to be missing, so this write should be
			// prevented and pushed to a higher timestamp.
			txnCopy := *txn
			pArgs2 := putArgs(keyPrevent, []byte("value2"))
			assignSeqNumsForReqs(&txnCopy, &pArgs2)
			ba := roachpb.BatchRequest{}
			ba.Header = roachpb.Header{Txn: &txnCopy}
			ba.Add(&pArgs2)
			br, pErr := tc.Sender().Send(context.Background(), ba)
			if pErr != nil {
				t.Fatal(pErr)
			}
			if br.Txn.WriteTimestamp == br.Txn.ReadTimestamp {
				t.Fatalf("transaction timestamp not bumped: %v", br.Txn)
			}
		}
	})
}

// TestReplicaResolveIntentRange verifies resolving a range of intents.
func TestReplicaResolveIntentRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	keys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")}
	txn := newTransaction("test", keys[0], 1, tc.Clock())

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
		IntentTxn:      txn.TxnMeta,
		Status:         roachpb.COMMITTED,
		IgnoredSeqNums: txn.IgnoredSeqNums,
	}
	if _, pErr := tc.SendWrapped(rArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Do a consistent scan to verify intents have been cleared.
	sArgs := scanArgs(roachpb.Key("a"), roachpb.Key("c"))
	reply, pErr := tc.SendWrapped(sArgs)
	if pErr != nil {
		t.Fatalf("unexpected error on scan: %s", pErr)
	}
	sReply := reply.(*roachpb.ScanResponse)
	if len(sReply.Rows) != 2 {
		t.Errorf("expected 2 rows; got %v", sReply.Rows)
	}
}

func verifyRangeStats(
	reader storage.Reader, rangeID roachpb.RangeID, expMS enginepb.MVCCStats,
) error {
	ms, err := stateloader.Make(rangeID).LoadMVCCStats(context.Background(), reader)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	baseStats := tc.repl.GetMVCCStats()

	require.NoError(t, verifyRangeStats(tc.engine, tc.repl.RangeID, baseStats))

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
	txn := newTransaction("test", pArgs.Key, 1, tc.Clock())
	txn.Priority = 123 // So we don't have random values messing with the byte counts on encoding
	txn.ID = uuid

	assignSeqNumsForReqs(txn, &pArgs)
	if _, pErr := tc.SendWrappedWith(roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes:            103,
		KeyBytes:             28,
		ValBytes:             75,
		IntentBytes:          23,
		LiveCount:            2,
		KeyCount:             2,
		ValCount:             2,
		IntentCount:          1,
		SeparatedIntentCount: 1,
	})
	if err := verifyRangeStats(tc.engine, tc.repl.RangeID, expMS); err != nil {
		t.Fatal(err)
	}

	// Resolve the 2nd value.
	rArgs := &roachpb.ResolveIntentRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: pArgs.Key,
		},
		IntentTxn:      txn.TxnMeta,
		Status:         roachpb.COMMITTED,
		IgnoredSeqNums: txn.IgnoredSeqNums,
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
	if !actual.Equal(&expected) {
		t.Errorf("Get did not return expected value: %v != %v", actual, expected)
	}
}

// TestConditionFailedError tests that a ConditionFailedError correctly
// bubbles up from MVCC to Range.
func TestConditionFailedError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := []byte("k")
	value := []byte("quack")
	pArgs := putArgs(key, value)
	if _, pErr := tc.SendWrapped(&pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	cpArgs := cPutArgs(key, value, []byte("moo"))
	_, pErr := tc.SendWrappedWith(roachpb.Header{Timestamp: hlc.MinTimestamp}, &cpArgs)
	if cErr, ok := pErr.GetDetail().(*roachpb.ConditionFailedError); pErr == nil || !ok {
		t.Fatalf("expected ConditionFailedError, got %T with content %+v", pErr, pErr)
	} else if valueBytes, err := cErr.ActualValue.GetBytes(); err != nil {
		t.Fatal(err)
	} else if cErr.ActualValue == nil || !bytes.Equal(valueBytes, value) {
		t.Errorf("ConditionFailedError with bytes %q expected, but got %+v", value, cErr.ActualValue)
	}
}

// TestReplicaSetsEqual tests to ensure that intersectReplicaSets
// returns the correct responses.
func TestReplicaSetsEqual(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		if replicasCollocated(test.a, test.b) != test.expected {
			t.Fatalf("unexpected replica intersection: %+v", test)
		}
	}
}

func TestAppliedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	var appliedIndex uint64
	var sum int64
	for i := int64(1); i <= 10; i++ {
		args := incrementArgs([]byte("a"), i)

		resp, pErr := tc.SendWrapped(args)
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
	defer log.Scope(t).Close(t)

	var exitStatus exit.Code
	log.SetExitFunc(true /* hideStack */, func(i exit.Code) {
		exitStatus = i
	})
	defer log.ResetExitFunc()

	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *roachpb.Error {
			if filterArgs.Req.Header().Key.Equal(roachpb.Key("boom")) {
				return roachpb.NewError(roachpb.NewReplicaCorruptionError(errors.New("boom")))
			}
			return nil
		}

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	// First send a regular command.
	args := putArgs(roachpb.Key("test1"), []byte("value"))
	if _, pErr := tc.SendWrapped(&args); pErr != nil {
		t.Fatal(pErr)
	}

	key := roachpb.Key("boom")

	args = putArgs(key, []byte("value"))
	_, pErr := tc.SendWrapped(&args)
	if !testutils.IsPError(pErr, "replica corruption \\(processed=true\\)") {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Should have laid down marker file to prevent startup.
	_, err := tc.engine.Stat(base.PreventedStartupFile(tc.engine.GetAuxiliaryDir()))
	require.NoError(t, err)

	// Should have triggered fatal error.
	if exitStatus != exit.FatalError() {
		t.Fatalf("unexpected exit status %d", exitStatus)
	}
}

// TestChangeReplicasDuplicateError tests that a replica change that would
// use a NodeID twice in the replica configuration fails.
func TestChangeReplicasDuplicateError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []roachpb.ReplicaChangeType{
		roachpb.ADD_VOTER,
		roachpb.ADD_NON_VOTER,
	}
	for _, typ := range tests {
		t.Run(typ.String(), func(t *testing.T) {
			ctx := context.Background()
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			cfg := TestStoreConfig(nil)
			cfg.TestingKnobs.DisableReplicateQueue = true
			tc.StartWithStoreConfig(ctx, t, stopper, cfg)
			// We now allow adding a replica to the same node, to support rebalances
			// within the same node when replication is 1x, so add another replica to the
			// range descriptor to avoid this case.
			if _, err := tc.addBogusReplicaToRangeDesc(context.Background()); err != nil {
				t.Fatalf("Unexpected error %v", err)
			}
			chgs := roachpb.MakeReplicationChanges(typ, roachpb.ReplicationTarget{
				NodeID:  tc.store.Ident.NodeID,
				StoreID: 9999,
			})
			if _, err := tc.repl.ChangeReplicas(
				context.Background(),
				tc.repl.Desc(),
				kvserverpb.SnapshotRequest_REBALANCE,
				kvserverpb.ReasonRebalance,
				"",
				chgs,
			); err == nil || !strings.Contains(err.Error(),
				"only valid actions are a removal or a rebalance") {
				t.Fatalf("must not be able to add second replica to same node (err=%+v)", err)
			}
		})
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
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		tc := testContext{}
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		cfg := TestStoreConfig(nil)
		cfg.TestingKnobs.DontPushOnWriteIntentError = true
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)

		key := roachpb.Key("a")

		// Get original meta2 descriptor.
		rs, _, err := kv.RangeLookup(ctx, tc.Sender(), key, roachpb.READ_UNCOMMITTED, 0, reverse)
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
		txn := newTransaction("test", key, 1, tc.Clock())
		// Officially begin the transaction. If not for this, the intent resolution
		// machinery would simply remove the intent we write below, see #3020.
		// We send directly to Replica throughout this test, so there's no danger
		// of the Store aborting this transaction (i.e. we don't have to set a high
		// priority).
		pArgs := putArgs(keys.RangeMetaKey(roachpb.RKey(key)).AsRawKey(), data)
		assignSeqNumsForReqs(txn, &pArgs)
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Now lookup the range; should get the value. Since the lookup is
		// not consistent, there's no WriteIntentError. It should return both
		// the committed descriptor and the intent descriptor.
		//
		// Note that 'A' < 'a'.
		newKey := roachpb.Key{'A'}
		rs, _, err = kv.RangeLookup(ctx, tc.Sender(), newKey, roachpb.READ_UNCOMMITTED, 0, reverse)
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
		_, _, err = kv.RangeLookup(ctx, tc.Sender(), newKey, roachpb.CONSISTENT, 0, reverse)
		if !errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
			t.Fatalf("expected WriteIntentError, not %s", err)
		}
	})
}

// TestReplicaLookupUseReverseScan verifies the correctness of the results which are retrieved
// from RangeLookup by using ReverseScan.
func TestReplicaLookupUseReverseScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
		txn := newTransaction("test", roachpb.Key{}, 1, tc.Clock())
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
			IntentTxn:      txn.TxnMeta,
			Status:         roachpb.COMMITTED,
			IgnoredSeqNums: txn.IgnoredSeqNums,
		}
		if _, pErr := tc.SendWrapped(rArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Test reverse RangeLookup scan without intents.
	for _, c := range testCases {
		rs, _, err := kv.RangeLookup(ctx, tc.Sender(), roachpb.Key(c.key),
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
	txn := newTransaction("test", roachpb.Key{}, 1, tc.Clock())
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
		rs, _, err := kv.RangeLookup(ctx, tc.Sender(), roachpb.Key(c.key),
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
	defer log.Scope(t).Close(t)
	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
		rs, _, err := kv.RangeLookup(ctx, tc.Sender(), c.key.AsRawKey(),
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// Mock propose to return a roachpb.RaftGroupDeletedError.
	var active int32
	proposeFn := func(fArgs kvserverbase.ProposalFilterArgs) *roachpb.Error {
		if atomic.LoadInt32(&active) == 1 {
			return roachpb.NewError(&roachpb.RaftGroupDeletedError{})
		}
		return nil
	}

	manual := hlc.NewManualClock(123)
	tc := testContext{manualClock: manual}
	cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	cfg.TestingKnobs.TestingProposalFilter = proposeFn
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	atomic.StoreInt32(&active, 1)
	gArgs := getArgs(roachpb.Key("a"))
	// Force the read command request a new lease.
	manual.Set(leaseExpiry(tc.repl))
	_, pErr := kv.SendWrappedWith(ctx, tc.store, roachpb.Header{
		Timestamp: tc.Clock().Now(),
		RangeID:   1,
	}, &gArgs)
	if _, ok := pErr.GetDetail().(*roachpb.RangeNotFoundError); !ok {
		t.Fatalf("expected a RangeNotFoundError, get %s", pErr)
	}
}

func TestIntentIntersect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
		in, out := kvserverbase.IntersectSpan(tc.intent, &roachpb.RangeDescriptor{
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
		ExpBytes:      nil, // not true after above Put
	})
	// This one is never executed.
	ba.Add(&roachpb.GetRequest{
		RequestHeader: roachpb.RequestHeader{Key: roachpb.Key("k")},
	})

	if _, pErr := tc.Sender().Send(ctx, ba); pErr == nil {
		t.Fatal("expected an error")
	} else if pErr.Index == nil || pErr.Index.Index != 1 || !testutils.IsPError(pErr, "unexpected value") {
		t.Fatalf("invalid index or error type: %s", pErr)
	}
}

// TestReplicaLoadSystemConfigSpanIntent verifies that intents on the SystemConfigSpan
// cause an error, but trigger asynchronous cleanup.
func TestReplicaLoadSystemConfigSpanIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
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
	pushee := newTransaction("test", key, 1, repl.store.Clock())
	pushee.Priority = enginepb.MinTxnPriority // low so it can be pushed
	put := putArgs(key, []byte("foo"))
	assignSeqNumsForReqs(pushee, &put)
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{Txn: pushee}, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Abort the transaction so that the async intent resolution caused
	// by loading the system config span doesn't waste any time in
	// clearing the intent.
	pusher := newTransaction("test", key, 1, repl.store.Clock())
	pusher.Priority = enginepb.MaxTxnPriority // will push successfully
	pushArgs := pushTxnArgs(pusher, pushee, roachpb.PUSH_ABORT)
	if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify that the intent trips up loading the SystemConfig data.
	if _, err := repl.loadSystemConfig(ctx); !errors.Is(err, errSystemConfigIntent) {
		t.Fatal(err)
	}

	// In the loop, wait until the intent is aborted. Then write a "real" value
	// there and verify that we can now load the data as expected.
	v := roachpb.MakeValueFromString("foo")
	testutils.SucceedsSoon(t, func() error {
		if err := storage.MVCCPut(ctx, repl.store.Engine(), &enginepb.MVCCStats{},
			keys.SystemConfigSpan.Key, repl.store.Clock().Now(), v, nil); err != nil {
			return err
		}

		cfg, err := repl.loadSystemConfig(ctx)
		if err != nil {
			return err
		}

		var found bool
		for _, cur := range cfg.Values {
			if !cur.Key.Equal(keys.SystemConfigSpan.Key) {
				continue
			}
			if !v.EqualTagAndData(cur.Value) {
				continue
			}
			found = true
			break
		}
		if found {
			return nil
		}
		return errors.New("recent write not found in gossiped SystemConfig")
	})
}

func TestReplicaDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	repl, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	func() {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()
		if _, err := tc.store.removeInitializedReplicaRaftMuLocked(ctx, tc.repl, repl.Desc().NextReplicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			t.Fatal(err)
		}
	}()

	iter := rditer.NewReplicaEngineDataIterator(tc.repl.Desc(), tc.repl.store.Engine(),
		false /* replicatedOnly */)
	defer iter.Close()
	if ok, err := iter.Valid(); err != nil {
		t.Fatal(err)
	} else if ok {
		// If the range is destroyed, only a tombstone key should be there.
		k1 := iter.UnsafeKey().Key
		if tombstoneKey := keys.RangeTombstoneKey(tc.repl.RangeID); !bytes.Equal(k1, tombstoneKey) {
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

// TestQuotaPoolReleasedOnFailedProposal tests that the quota acquired by
// proposals is released back into the quota pool if the proposal fails before
// being submitted to Raft.
func TestQuotaPoolReleasedOnFailedProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	type magicKey struct{}
	var minQuotaSize uint64
	propErr := errors.New("proposal error")

	tsc := TestStoreConfig(nil /* clock */)
	tsc.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
		if v := args.Ctx.Value(magicKey{}); v != nil {
			minQuotaSize = tc.repl.mu.proposalQuota.ApproximateQuota() + args.QuotaAlloc.Acquired()
			return roachpb.NewError(propErr)
		}
		return nil
	}
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	// Flush a write all the way through the Raft proposal pipeline to ensure
	// that the replica becomes the Raft leader and sets up its quota pool.
	iArgs := incrementArgs([]byte("a"), 1)
	if _, pErr := tc.SendWrapped(iArgs); pErr != nil {
		t.Fatal(pErr)
	}

	var ba roachpb.BatchRequest
	pArg := putArgs(roachpb.Key("a"), make([]byte, 1<<10))
	ba.Add(&pArg)
	ctx = context.WithValue(ctx, magicKey{}, "foo")
	if _, pErr := tc.Sender().Send(ctx, ba); !testutils.IsPError(pErr, propErr.Error()) {
		t.Fatalf("expected error %v, found %v", propErr, pErr)
	}
	if curQuota := tc.repl.QuotaAvailable(); curQuota < minQuotaSize {
		t.Fatalf("proposal quota not released: found=%d, want=%d", curQuota, minQuotaSize)
	}
}

// TestQuotaPoolAccessOnDestroyedReplica tests the occurrence of #17303 where
// following a leader replica getting destroyed, the scheduling of
// handleRaftReady twice on the replica would cause a panic when
// finding a nil/closed quota pool.
func TestQuotaPoolAccessOnDestroyedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	repl, err := tc.store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	ctx = repl.AnnotateCtx(ctx)
	func() {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()
		if _, err := tc.store.removeInitializedReplicaRaftMuLocked(ctx, repl, repl.Desc().NextReplicaID, RemoveOptions{
			DestroyData: true,
		}); err != nil {
			t.Fatal(err)
		}
	}()

	if _, _, err := repl.handleRaftReady(ctx, noSnap); err != nil {
		t.Fatal(err)
	}

	if _, _, err := repl.handleRaftReady(ctx, noSnap); err != nil {
		t.Fatal(err)
	}
}

func TestEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
		ctx := context.Background()
		tc := testContext{}
		cfg := TestStoreConfig(nil)
		// Disable ticks to avoid quiescence, which can result in empty
		// entries being proposed and causing the test to flake.
		cfg.RaftTickInterval = math.MaxInt32
		cfg.TestingKnobs.DisableRaftLogQueue = true
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)
		st := tc.store.ClusterSettings()
		looselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)

		repl := tc.repl
		rangeID := repl.RangeID
		var indexes []uint64

		populateLogs := func(from, to int) []uint64 {
			var newIndexes []uint64
			for i := from; i < to; i++ {
				args := incrementArgs([]byte("a"), int64(i))
				if _, pErr := tc.SendWrapped(args); pErr != nil {
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
			if _, err := kv.SendWrappedWith(
				ctx,
				tc.Sender(),
				roachpb.Header{RangeID: 1},
				&truncateArgs,
			); err != nil {
				t.Fatal(err)
			}
			waitForTruncationForTesting(t, repl, indexes[index], looselyCoupled)
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
				repl.store.raftEntryCache.Clear(rangeID, indexes[9]+1)
				indexes = append(indexes, populateLogs(10, 40)...)
			}},
			// Case 20: lo and hi are available, entry cache evicted and hi available in cache.
			{lo: indexes[5], hi: indexes[9] + 5, expResultCount: 9, expCacheCount: 0, setup: nil},
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
			cacheEntries, _, _, hitLimit := repl.store.raftEntryCache.Scan(nil, rangeID, tc.lo, tc.hi, tc.maxBytes)
			if len(cacheEntries) != tc.expCacheCount {
				t.Errorf("%d: expected cache count %d, got %d", i, tc.expCacheCount, len(cacheEntries))
			}
			repl.mu.Lock()
			ents, err := repl.raftEntriesLocked(tc.lo, tc.hi, tc.maxBytes)
			repl.mu.Unlock()
			if tc.expError == nil && err != nil {
				t.Errorf("%d: expected no error, got %s", i, err)
				continue
			} else if !errors.Is(err, tc.expError) {
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
	})
}

func TestTerm(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
		ctx := context.Background()
		tc := testContext{}
		tsc := TestStoreConfig(nil)
		tsc.TestingKnobs.DisableRaftLogQueue = true
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.StartWithStoreConfig(ctx, t, stopper, tsc)
		st := tc.store.ClusterSettings()
		looselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)

		repl := tc.repl
		rangeID := repl.RangeID

		// Populate the log with 10 entries. Save the LastIndex after each write.
		var indexes []uint64
		for i := 0; i < 10; i++ {
			args := incrementArgs([]byte("a"), int64(i))

			if _, pErr := tc.SendWrapped(args); pErr != nil {
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
		waitForTruncationForTesting(t, repl, indexes[5], looselyCoupled)

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
		if _, err := tc.repl.raftTermRLocked(indexes[1]); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}
		if _, err := tc.repl.raftTermRLocked(indexes[3]); !errors.Is(err, raft.ErrCompacted) {
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
		if _, err := tc.repl.raftTermRLocked(lastIndex + 1); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("expected ErrUnavailable, got %s", err)
		}
		if _, err := tc.repl.raftTermRLocked(indexes[9] + 1000); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("expected ErrUnavailable, got %s", err)
		}
	})
}

func TestGCIncorrectRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Split range into two ranges.
	splitKey := roachpb.RKey("c")
	repl1 := tc.repl
	repl2 := splitTestRange(tc.store, splitKey, t)

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
	if _, pErr := kv.SendWrappedWith(ctx, repl2, ts1Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}
	if _, pErr := kv.SendWrappedWith(ctx, repl2, ts2Header, &putReq); pErr != nil {
		t.Errorf("unexpected pError on put key request: %s", pErr)
	}

	// Send GC request to range 1 for the key on range 2, which
	// should succeed even though it doesn't contain the key, because
	// the request for the incorrect key will be silently dropped.
	gKey := gcKey(key, ts1)
	gcReq := gcArgs(repl1.Desc().StartKey, repl1.Desc().EndKey, gKey)
	if _, pErr := kv.SendWrappedWith(
		ctx,
		repl1,
		roachpb.Header{RangeID: 1, Timestamp: tc.Clock().Now()},
		&gcReq,
	); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to incorrect range: %s", pErr)
	}

	// Make sure the key still exists on range 2.
	getReq := getArgs(key)
	if res, pErr := kv.SendWrappedWith(ctx, repl2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal == nil {
		t.Errorf("expected value %s to exists after GC to incorrect range but before GC to correct range, found %v", val, resVal)
	}

	// Send GC request to range 2 for the same key.
	gcReq = gcArgs(repl2.Desc().StartKey, repl2.Desc().EndKey, gKey)
	if _, pErr := kv.SendWrappedWith(
		ctx,
		repl2,
		roachpb.Header{RangeID: repl2.RangeID, Timestamp: tc.Clock().Now()},
		&gcReq,
	); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to correct range: %s", pErr)
	}

	// Make sure the key no longer exists on range 2.
	if res, pErr := kv.SendWrappedWith(ctx, repl2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*roachpb.GetResponse).Value; resVal != nil {
		t.Errorf("expected value at key %s to no longer exist after GC to correct range, found value %v", key, resVal)
	}
}

// TestReplicaCancelRaft checks that it is possible to safely abandon Raft
// commands via a cancelable context.Context.
func TestReplicaCancelRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	for _, cancelEarly := range []bool{true, false} {
		func() {
			// Pick a key unlikely to be used by background processes.
			key := []byte("acdfg")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cfg := TestStoreConfig(nil)
			if !cancelEarly {
				cfg.TestingKnobs.TestingProposalFilter =
					func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
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
			defer stopper.Stop(ctx)
			tc.StartWithStoreConfig(ctx, t, stopper, cfg)
			if cancelEarly {
				cancel()
			}
			var ba roachpb.BatchRequest
			ba.RangeID = 1
			ba.Add(&roachpb.GetRequest{
				RequestHeader: roachpb.RequestHeader{Key: key},
			})
			if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
				t.Fatal(err)
			}
			_, pErr := tc.repl.executeBatchWithConcurrencyRetries(ctx, &ba, (*Replica).executeWriteBatch)
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

// TestReplicaAbandonProposal checks that canceling a request that has been
// proposed to Raft but before it has been executed correctly releases its
// latches. See #11986.
func TestReplicaAbandonProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)

	type magicKey struct{}
	var cancel func()
	ctx, cancel = context.WithCancel(ctx)
	ctx = context.WithValue(ctx, magicKey{}, "foo")

	// Cancel the request before it is proposed to Raft.
	dropProp := int32(1)
	tc.repl.mu.Lock()
	tc.repl.mu.proposalBuf.testing.submitProposalFilter = func(p *ProposalData) (drop bool, _ error) {
		if v := p.ctx.Value(magicKey{}); v != nil {
			cancel()
			return atomic.LoadInt32(&dropProp) == 1, nil
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	var ba roachpb.BatchRequest
	ba.RangeID = 1
	ba.Timestamp = tc.Clock().Now()
	ba.Add(&roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{Key: []byte("acdfg")},
	})
	_, pErr := tc.repl.executeBatchWithConcurrencyRetries(ctx, &ba, (*Replica).executeWriteBatch)
	if pErr == nil {
		t.Fatal("expected failure, but found success")
	}
	detail := pErr.GetDetail()
	if _, ok := detail.(*roachpb.AmbiguousResultError); !ok {
		t.Fatalf("expected AmbiguousResultError error; got %s (%T)", detail, detail)
	}

	// The request should still be holding its latches.
	latchMetrics := tc.repl.concMgr.LatchMetrics()
	if w := latchMetrics.WriteCount; w == 0 {
		t.Fatal("expected non-empty latch manager")
	}

	// Let the proposal be reproposed and go through.
	atomic.StoreInt32(&dropProp, 0)

	// Even though we canceled the command it will still get executed and its
	// latches cleaned up.
	testutils.SucceedsSoon(t, func() error {
		latchMetrics := tc.repl.concMgr.LatchMetrics()
		if w := latchMetrics.WriteCount; w != 0 {
			return errors.Errorf("expected empty latch manager")
		}
		return nil
	})
}

func TestNewReplicaCorruptionError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)

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
+0,0 "foo"
+    ts:1970-01-01 00:00:00 +0000 UTC
+    value:"foo"
+    raw mvcc_key/value: 666f6f00 666f6f
`

	require.Equal(t, expDiff, stringDiff.String())
}

func TestSyncSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tsc := TestStoreConfig(nil)
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

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

func TestReplicaRetryRaftProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	type magicKey struct{}

	var c int32                // updated atomically
	var wrongLeaseIndex uint64 // populated below

	tc.repl.mu.Lock()
	tc.repl.mu.proposalBuf.testing.leaseIndexFilter = func(p *ProposalData) (indexOverride uint64) {
		if v := p.ctx.Value(magicKey{}); v != nil {
			if curAttempt := atomic.AddInt32(&c, 1); curAttempt == 1 {
				return wrongLeaseIndex
			}
		}
		return 0
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

	// Set the max lease index to that of the recently applied write.
	// Two requests can't have the same lease applied index.
	tc.repl.mu.RLock()
	wrongLeaseIndex = tc.repl.mu.state.LeaseAppliedIndex
	if wrongLeaseIndex < 1 {
		t.Fatal("committed a few batches, but still at lease index zero")
	}
	tc.repl.mu.RUnlock()

	log.Infof(ctx, "test begins")

	var ba roachpb.BatchRequest
	ba.RangeID = 1
	ba.Timestamp = tc.Clock().Now()
	const expInc = 123
	iArg := incrementArgs(roachpb.Key("b"), expInc)
	ba.Add(iArg)
	{
		_, pErr := tc.repl.executeBatchWithConcurrencyRetries(
			context.WithValue(ctx, magicKey{}, "foo"),
			&ba,
			(*Replica).executeWriteBatch,
		)
		if pErr != nil {
			t.Fatalf("write batch returned error: %s", pErr)
		}
		// The command was reproposed internally, for two total proposals.
		if exp, act := int32(2), atomic.LoadInt32(&c); exp != act {
			t.Fatalf("expected %d proposals, got %d", exp, act)
		}
	}

	// Test LeaseRequest since it's special: MaxLeaseIndex plays no role and so
	// there is no re-evaluation of the request. Replay protection is conferred
	// by the lease sequence.
	atomic.StoreInt32(&c, 0)
	{
		prevLease, _ := tc.repl.GetLease()
		ba := ba
		ba.Requests = nil

		lease := prevLease
		lease.Sequence = 0
		now := tc.Clock().Now().UnsafeToClockTimestamp()
		lease.ProposedTS = &now

		ba.Add(&roachpb.RequestLeaseRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: tc.repl.Desc().StartKey.AsRawKey(),
			},
			Lease:     lease,
			PrevLease: prevLease,
		})
		_, pErr := tc.repl.executeBatchWithConcurrencyRetries(
			context.WithValue(ctx, magicKey{}, "foo"),
			&ba,
			(*Replica).executeWriteBatch,
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	var tc testContext
	tc.Start(ctx, t, stopper)
	repl := tc.repl

	tc.repl.mu.Lock()
	abandoned := make(map[kvserverbase.CmdIDKey]struct{}) // protected by repl.mu
	tc.repl.mu.proposalBuf.testing.submitProposalFilter = func(p *ProposalData) (drop bool, _ error) {
		if _, ok := abandoned[p.idKey]; ok {
			log.Infof(p.ctx, "abandoning command")
			return true, nil
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	var chs []chan proposalResult
	const num = 10
	for i := 0; i < num; i++ {
		var ba roachpb.BatchRequest
		ba.Timestamp = tc.Clock().Now()
		ba.Add(&roachpb.PutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(fmt.Sprintf("k%d", i)),
			},
		})
		st := repl.CurrentLeaseStatus(ctx)
		_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		ch, _, id, err := repl.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
		if err != nil {
			t.Fatal(err)
		}

		repl.mu.Lock()
		if rand.Intn(2) == 0 {
			abandoned[id] = struct{}{}
		} else {
			chs = append(chs, ch)
		}
		repl.mu.Unlock()
	}

	log.Infof(ctx, "waiting on %d chans", len(chs))
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
	defer log.Scope(t).Close(t)

	var tc testContext
	cfg := TestStoreConfig(nil)
	// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
	// refreshes so that our proposals don't risk being reproposed due to
	// Raft leadership instability.
	cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
	cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	type magicKey struct{}
	ctx = context.WithValue(ctx, magicKey{}, "foo")

	var seenCmds []int
	dropAll := int32(1)
	tc.repl.mu.Lock()
	tc.repl.mu.proposalBuf.testing.submitProposalFilter = func(p *ProposalData) (drop bool, _ error) {
		if atomic.LoadInt32(&dropAll) == 1 {
			return true, nil
		}
		if v := p.ctx.Value(magicKey{}); v != nil {
			seenCmds = append(seenCmds, int(p.command.MaxLeaseIndex))
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	const num = 10
	chs := make([]chan proposalResult, 0, num)
	for i := 0; i < num; i++ {
		var ba roachpb.BatchRequest
		ba.Timestamp = tc.Clock().Now()
		ba.Add(&roachpb.PutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key(fmt.Sprintf("k%d", i)),
			},
		})
		_, tok := tc.repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		st := tc.repl.CurrentLeaseStatus(ctx)
		ch, _, _, err := tc.repl.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
		if err != nil {
			t.Fatal(err)
		}
		chs = append(chs, ch)
	}

	tc.repl.mu.Lock()
	if err := tc.repl.mu.proposalBuf.flushLocked(ctx); err != nil {
		t.Fatal(err)
	}
	origIndexes := make([]int, 0, num)
	for _, p := range tc.repl.mu.proposals {
		if v := p.ctx.Value(magicKey{}); v != nil {
			origIndexes = append(origIndexes, int(p.command.MaxLeaseIndex))
		}
	}
	sort.Ints(origIndexes)
	tc.repl.mu.Unlock()

	tc.repl.raftMu.Lock()
	tc.repl.mu.Lock()
	atomic.StoreInt32(&dropAll, 0)
	tc.repl.refreshProposalsLocked(ctx, 0 /* refreshAtDelta */, reasonTicks)
	if err := tc.repl.mu.proposalBuf.flushLocked(ctx); err != nil {
		t.Fatal(err)
	}
	tc.repl.mu.Unlock()
	tc.repl.raftMu.Unlock()

	for _, ch := range chs {
		if pErr := (<-ch).Err; pErr != nil {
			t.Fatal(pErr)
		}
	}

	if !reflect.DeepEqual(seenCmds, origIndexes) {
		t.Fatalf("expected indexes %v, got %v", origIndexes, seenCmds)
	}

	tc.repl.mu.RLock()
	defer tc.repl.mu.RUnlock()
	if tc.repl.hasPendingProposalsRLocked() {
		t.Fatal("still pending commands")
	}
	lastAssignedIdx := tc.repl.mu.proposalBuf.LastAssignedLeaseIndexRLocked()
	curIdx := tc.repl.mu.state.LeaseAppliedIndex
	if c := lastAssignedIdx - curIdx; c > 0 {
		t.Errorf("no pending cmds, but have required index offset %d", c)
	}
}

func TestReplicaRefreshPendingCommandsTicks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var tc testContext
	cfg := TestStoreConfig(nil)
	// Disable ticks which would interfere with the manual ticking in this test.
	cfg.RaftTickInterval = math.MaxInt32
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Flush a write all the way through the Raft proposal pipeline. This
	// ensures that leadership settles down before we start manually submitting
	// proposals and that we don't see any unexpected proposal refreshes due to
	// reasons like reasonNewLeaderOrConfigChange.
	args := incrementArgs([]byte("a"), 1)
	if _, pErr := tc.SendWrapped(args); pErr != nil {
		t.Fatal(pErr)
	}

	r := tc.repl
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
			if _, err := r.tick(ctx, nil); err != nil {
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
	r.mu.proposalBuf.testing.submitProposalFilter = func(p *ProposalData) (drop bool, _ error) {
		dropProposals.Lock()
		defer dropProposals.Unlock()
		_, ok := dropProposals.m[p]
		return ok, nil
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
		st := r.CurrentLeaseStatus(ctx)
		cmd, pErr := r.requestToProposal(ctx, kvserverbase.CmdIDKey(id), &ba, st, uncertainty.Interval{}, allSpansGuard())
		if pErr != nil {
			t.Fatal(pErr)
		}

		dropProposals.Lock()
		dropProposals.m[cmd] = struct{}{} // silently drop proposals
		dropProposals.Unlock()

		cmd.command.ProposerLeaseSequence = st.Lease.Sequence
		_, tok := r.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		if pErr := r.propose(ctx, cmd, tok); pErr != nil {
			t.Error(pErr)
		}
		r.mu.Lock()
		if err := tc.repl.mu.proposalBuf.flushLocked(ctx); err != nil {
			t.Fatal(err)
		}
		r.mu.Unlock()

		// Tick raft.
		if _, err := r.tick(ctx, nil); err != nil {
			t.Fatal(err)
		}

		r.mu.Lock()
		ticks := r.mu.ticks
		r.mu.Unlock()

		var reproposed []*ProposalData
		r.mu.Lock() // avoid data race - proposals belong to the Replica
		if err := tc.repl.mu.proposalBuf.flushLocked(ctx); err != nil {
			t.Fatal(err)
		}
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

// TestReplicaRefreshMultiple tests an interaction between refreshing
// proposals after a new leader or ticks (which results in multiple
// copies in the log with the same lease index) and refreshing after
// an illegal lease index error (with a new lease index assigned).
//
// The setup here is rather artificial, but it represents something
// that can happen (very rarely) in the real world with multiple raft
// leadership transfers.
func TestReplicaRefreshMultiple(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	var filterActive int32
	var incCmdID kvserverbase.CmdIDKey
	var incApplyCount int64
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.TestingApplyFilter = func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		if atomic.LoadInt32(&filterActive) != 0 && filterArgs.CmdID == incCmdID {
			atomic.AddInt64(&incApplyCount, 1)
		}
		return 0, nil
	}
	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	repl := tc.repl

	key := roachpb.Key("a")

	// Run a few commands first: This advances the lease index, which is
	// necessary for the tricks we're going to play to induce failures
	// (we need to be able to subtract from the current lease index
	// without going below 0).
	for i := 0; i < 3; i++ {
		inc := incrementArgs(key, 1)
		if _, pErr := kv.SendWrapped(ctx, tc.Sender(), inc); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// Sanity check the resulting value.
	get := getArgs(key)
	if resp, pErr := kv.SendWrapped(ctx, tc.Sender(), &get); pErr != nil {
		t.Fatal(pErr)
	} else if x, err := resp.(*roachpb.GetResponse).Value.GetInt(); err != nil {
		t.Fatalf("returned non-int: %+v", err)
	} else if x != 3 {
		t.Fatalf("expected 3, got %d", x)
	}

	// Manually propose another increment. This is the one we'll
	// manipulate into failing. (the use of increment here is not
	// significant. I originally wrote it this way because I thought the
	// non-idempotence of increment would make it easier to test, but
	// since the reproposals we're concerned with don't result in
	// reevaluation it doesn't matter)
	inc := incrementArgs(key, 1)
	var ba roachpb.BatchRequest
	ba.Add(inc)
	ba.Timestamp = tc.Clock().Now()

	incCmdID = makeIDKey()
	atomic.StoreInt32(&filterActive, 1)
	proposal, pErr := repl.requestToProposal(ctx, incCmdID, &ba, repl.CurrentLeaseStatus(ctx), uncertainty.Interval{}, allSpansGuard())
	if pErr != nil {
		t.Fatal(pErr)
	}
	// Save this channel; it may get reset to nil before we read from it.
	proposalDoneCh := proposal.doneCh

	repl.mu.Lock()
	ai := repl.mu.state.LeaseAppliedIndex
	if ai <= 1 {
		// Lease index zero is special in this test because we subtract
		// from it below, so we need enough previous proposals in the
		// log to ensure it doesn't go negative.
		t.Fatalf("test requires LeaseAppliedIndex >= 2 at this point, have %d", ai)
	}
	assigned := false
	repl.mu.proposalBuf.testing.leaseIndexFilter = func(p *ProposalData) (indexOverride uint64) {
		if p == proposal && !assigned {
			assigned = true
			return ai - 1
		}
		return 0
	}
	repl.mu.Unlock()

	// Propose the command manually with errors induced. The first time it is
	// proposed it will be given the incorrect max lease index which ensures
	// that it will generate a retry when it fails. Then call refreshProposals
	// twice to repropose it and put it in the logs twice more.
	proposal.command.ProposerLeaseSequence = repl.mu.state.Lease.Sequence
	_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	if pErr := repl.propose(ctx, proposal, tok); pErr != nil {
		t.Fatal(pErr)
	}
	repl.mu.Lock()
	if err := tc.repl.mu.proposalBuf.flushLocked(ctx); err != nil {
		t.Fatal(err)
	}
	repl.refreshProposalsLocked(ctx, 0 /* refreshAtDelta */, reasonNewLeader)
	repl.refreshProposalsLocked(ctx, 0 /* refreshAtDelta */, reasonNewLeader)
	repl.mu.Unlock()
	require.Zero(t, tc.repl.mu.proposalBuf.EvaluatingRequestsCount())

	// Wait for our proposal to apply. The two refreshed proposals above
	// will fail due to their illegal lease index. Then they'll generate
	// a reproposal (in the bug that we're testing against, they'd
	// *each* generate a reproposal). When this reproposal succeeds, the
	// doneCh is signaled.
	select {
	case resp := <-proposalDoneCh:
		if resp.Err != nil {
			t.Fatal(resp.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
	}
	// In the buggy case, there's a second reproposal that we don't have
	// a good way to observe, so just sleep to let it apply if it's in
	// the system.
	time.Sleep(10 * time.Millisecond)

	// The command applied exactly once. Note that this check would pass
	// even in the buggy case, since illegal lease index proposals do
	// not generate reevaluations (and increment is handled upstream of
	// raft).
	if resp, pErr := kv.SendWrapped(ctx, tc.Sender(), &get); pErr != nil {
		t.Fatal(pErr)
	} else if x, err := resp.(*roachpb.GetResponse).Value.GetInt(); err != nil {
		t.Fatalf("returned non-int: %+v", err)
	} else if x != 4 {
		t.Fatalf("expected 4, got %d", x)
	}

	// The real test: our apply filter can tell us whether there was a
	// duplicate reproposal. (A reproposed increment isn't harmful, but
	// some other commands could be)
	if x := atomic.LoadInt64(&incApplyCount); x != 1 {
		t.Fatalf("expected 1, got %d", x)
	}
}

// TestReplicaReproposalWithNewLeaseIndexError tests an interaction where a
// proposal is rejected beneath raft due an illegal lease index error and then
// hits an error when being reproposed. The expectation is that this error
// manages to make its way back to the client.
func TestReplicaReproposalWithNewLeaseIndexError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	type magicKey struct{}
	magicCtx := context.WithValue(ctx, magicKey{}, "foo")

	var curFlushAttempt, curInsertAttempt int32 // updated atomically
	tc.repl.mu.Lock()
	tc.repl.mu.proposalBuf.testing.leaseIndexFilter = func(p *ProposalData) (indexOverride uint64) {
		if v := p.ctx.Value(magicKey{}); v != nil {
			flushAttempts := atomic.AddInt32(&curFlushAttempt, 1)
			switch flushAttempts {
			case 1:
				// This is the first time the command is being given a max lease
				// applied index. Set the index to that of the recently applied
				// write. Two requests can't have the same lease applied index,
				// so this will cause it to be rejected beneath raft with an
				// illegal lease index error.
				wrongLeaseIndex := uint64(1)
				return wrongLeaseIndex
			default:
				// Unexpected. Asserted against below.
				return 0
			}
		}
		return 0
	}
	tc.repl.mu.proposalBuf.testing.insertFilter = func(p *ProposalData) error {
		if v := p.ctx.Value(magicKey{}); v != nil {
			curAttempt := atomic.AddInt32(&curInsertAttempt, 1)
			switch curAttempt {
			case 2:
				// This is the second time the command is being given a max
				// lease applied index, which should be after the command was
				// rejected beneath raft. Return an error. We expect this error
				// to propagate up through tryReproposeWithNewLeaseIndex and
				// make it back to the client.
				return errors.New("boom")
			default:
				// Unexpected. Asserted against below.
				return nil
			}
		}
		return nil
	}
	tc.repl.mu.Unlock()

	// Perform a few writes to advance the lease applied index.
	const initCount = 3
	key := roachpb.Key("a")
	for i := 0; i < initCount; i++ {
		iArg := incrementArgs(key, 1)
		if _, pErr := tc.SendWrapped(iArg); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Perform a write that will first hit an illegal lease index error and
	// will then hit the injected error when we attempt to repropose it.
	var ba roachpb.BatchRequest
	iArg := incrementArgs(key, 10)
	ba.Add(iArg)
	if _, pErr := tc.Sender().Send(magicCtx, ba); pErr == nil {
		t.Fatal("expected a non-nil error")
	} else if !testutils.IsPError(pErr, "boom") {
		t.Fatalf("unexpected error: %v", pErr)
	}
	// The command should have been inserted in the buffer exactly twice.
	if exp, act := int32(2), atomic.LoadInt32(&curInsertAttempt); exp != act {
		t.Fatalf("expected %d proposals, got %d", exp, act)
	}

	// The command should not have applied.
	gArgs := getArgs(key)
	if reply, pErr := tc.SendWrapped(&gArgs); pErr != nil {
		t.Fatal(pErr)
	} else if v, err := reply.(*roachpb.GetResponse).Value.GetInt(); err != nil {
		t.Fatal(err)
	} else if v != initCount {
		t.Fatalf("expected value of %d, found %d", initCount, v)
	}
}

// TestGCWithoutThreshold validates that GCRequest only declares the threshold
// key if it is subject to change, and that it does not access this key if it
// does not declare them.
func TestGCWithoutThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := &testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	for _, keyThresh := range []hlc.Timestamp{{}, {Logical: 1}} {
		t.Run(fmt.Sprintf("thresh=%s", keyThresh), func(t *testing.T) {
			var gc roachpb.GCRequest
			var spans spanset.SpanSet

			gc.Threshold = keyThresh
			cmd, _ := batcheval.LookupCommand(roachpb.GC)
			cmd.DeclareKeys(tc.repl.Desc(), &roachpb.Header{RangeID: tc.repl.RangeID}, &gc, &spans, nil, 0)

			expSpans := 1
			if !keyThresh.IsEmpty() {
				expSpans++
			}
			if numSpans := spans.Len(); numSpans != expSpans {
				t.Fatalf("expected %d declared keys, found %d", expSpans, numSpans)
			}

			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			batch := eng.NewBatch()
			defer batch.Close()
			rw := spanset.NewBatch(batch, &spans)

			var resp roachpb.GCResponse

			if _, err := batcheval.GC(ctx, rw, batcheval.CommandArgs{
				Args:    &gc,
				EvalCtx: NewReplicaEvalContext(tc.repl, &spans),
			}, &resp); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Test that, if the Raft command resulting from EndTxn request fails to be
// processed/apply, then the LocalResult associated with that command is
// cleared.
func TestFailureToProcessCommandClearsLocalResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var tc testContext
	cfg := TestStoreConfig(nil)
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	key := roachpb.Key("a")
	txn := newTransaction("test", key, 1, tc.Clock())

	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	assignSeqNumsForReqs(txn, &put)
	ba.Add(&put)
	if _, err := tc.Sender().Send(ctx, ba); err != nil {
		t.Fatal(err)
	}

	var proposalRecognized int64 // accessed atomically

	r := tc.repl
	r.mu.Lock()
	r.mu.proposalBuf.testing.leaseIndexFilter = func(p *ProposalData) (indexOverride uint64) {
		// We're going to recognize the first time the commnand for the EndTxn is
		// proposed and we're going to hackily force a low MaxLeaseIndex, so that
		// the processing gets rejected further on.
		ut := p.Local.UpdatedTxns
		if atomic.LoadInt64(&proposalRecognized) == 0 && ut != nil && len(ut) == 1 && ut[0].ID == txn.ID {
			atomic.StoreInt64(&proposalRecognized, 1)
			return 1
		}
		return 0
	}
	r.mu.Unlock()

	tr := tc.store.cfg.AmbientCtx.Tracer
	opCtx, getRecAndFinish := tracing.ContextWithRecordingSpan(ctx, tr, "test-recording")
	defer getRecAndFinish()

	ba = roachpb.BatchRequest{}
	et, etH := endTxnArgs(txn, true /* commit */)
	et.LockSpans = []roachpb.Span{{Key: key}}
	assignSeqNumsForReqs(txn, &et)
	ba.Header = etH
	ba.Add(&et)
	if _, err := tc.Sender().Send(opCtx, ba); err != nil {
		t.Fatal(err)
	}
	formatted := getRecAndFinish().String()
	if err := testutils.MatchInOrder(formatted,
		// The first proposal is rejected.
		"retry proposal.*applied at lease index.*but required",
		// The request will be re-evaluated.
		"retry: proposalIllegalLeaseIndex",
		// The LocalResult is nil. This is the important part for this test.
		"LocalResult: nil",
		// Re-evaluation succeeds and one txn is to be updated.
		"LocalResult \\(reply.*#updated txns: 1",
	); err != nil {
		t.Fatal(err)
	}
}

// TestBatchTimestampBelowGCThreshold verifies that commands below the replica
// GC threshold fail.
func TestBatchTimestampBelowGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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
		t.Fatalf("could not get data: %+v", err)
	}
	// Verify a later Get works.
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &gArgs); err != nil {
		t.Fatalf("could not get data: %+v", err)
	}

	// Put some data for use with CP later on.
	pArgs := putArgs(keycp, va)
	if _, err := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts1,
	}, &pArgs); err != nil {
		t.Fatalf("could not put data: %+v", err)
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
	}, &gArgs); !testutils.IsPError(pErr, `batch timestamp 0.\d+,\d+ must be after replica GC threshold 0.\d+,\d+`) {
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
	}, &cpArgs); !testutils.IsPError(pErr, `batch timestamp 0.\d+,\d+ must be after replica GC threshold 0.\d+,\d+`) {
		t.Fatalf("unexpected error: %v", pErr)
	}
	// Verify a later CPut works.
	if _, pErr := tc.SendWrappedWith(roachpb.Header{
		Timestamp: ts3,
	}, &cpArgs); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestRefreshFromBelowGCThreshold verifies that refresh requests that need to
// see MVCC history below the replica GC threshold fail.
func TestRefreshFromBelowGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "ranged", func(t *testing.T, ranged bool) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.Start(ctx, t, stopper)

		now := tc.Clock().Now()
		ts1 := now.Add(1, 0)
		ts2 := now.Add(2, 0)
		ts3 := now.Add(3, 0)
		ts4 := now.Add(4, 0)
		ts5 := now.Add(5, 0)

		keyA := roachpb.Key("a")
		keyB := roachpb.Key("b")

		// Construct a Refresh{Range} request for a transaction that refreshes the
		// time interval (ts2, ts4].
		var refresh roachpb.Request
		if ranged {
			refresh = &roachpb.RefreshRangeRequest{
				RequestHeader: roachpb.RequestHeader{Key: keyA, EndKey: keyB},
				RefreshFrom:   ts2,
			}
		} else {
			refresh = &roachpb.RefreshRequest{
				RequestHeader: roachpb.RequestHeader{Key: keyA},
				RefreshFrom:   ts2,
			}
		}
		txn := roachpb.MakeTransaction("test", keyA, 0, ts2, 0, 0)
		txn.Refresh(ts4)

		for _, testCase := range []struct {
			gc     hlc.Timestamp
			expErr bool
		}{
			{hlc.Timestamp{}, false},
			{ts1, false},
			{ts2, false},
			{ts3, true},
			{ts4, true},
			{ts5, true},
		} {
			t.Run(fmt.Sprintf("gcThreshold=%s", testCase.gc), func(t *testing.T) {
				if !testCase.gc.IsEmpty() {
					gcr := roachpb.GCRequest{Threshold: testCase.gc}
					_, pErr := tc.SendWrapped(&gcr)
					require.Nil(t, pErr)
				}

				_, pErr := tc.SendWrappedWith(roachpb.Header{Txn: &txn}, refresh)
				if testCase.expErr {
					require.NotNil(t, pErr)
					require.Regexp(t, `batch timestamp .* must be after replica GC threshold .*`, pErr)
				} else {
					require.Nil(t, pErr)
				}
			})
		}
	})
}

// TestGCThresholdRacesWithRead performs a GC and a read concurrently on the
// same key. It ensures that either the read wins the race and observes the
// correct result or the GC wins the race and the read returns an error. Either
// result is ok. However, it should never be possible for the read to return
// without error and without the correct result. This would indicate a bug in
// the synchronization between reads and GC operations.
//
// The test contains a subtest for each of the combinations of the following
// boolean options:
//
// - followerRead: configures whether the read should be served from the
//     leaseholder replica or from a follower replica.
//
// - thresholdFirst: configures whether the GC operation should be split into
//     two requests, with the first bumping the GC threshold and the second
//     GCing the expired version. This is how the real MVCC GC queue works.
//
func TestGCThresholdRacesWithRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 55293)

	testutils.RunTrueAndFalse(t, "followerRead", func(t *testing.T, followerRead bool) {
		testutils.RunTrueAndFalse(t, "thresholdFirst", func(t *testing.T, thresholdFirst bool) {
			if !thresholdFirst {
				skip.IgnoreLint(t, "the test fails, revealing that it is not safe "+
					"to bump the GC threshold and to GC individual keys at the same time")
			}

			ctx := context.Background()
			tc := serverutils.StartNewTestCluster(t, 2, base.TestClusterArgs{
				ReplicationMode: base.ReplicationManual,
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						Store: &StoreTestingKnobs{
							// Disable the GC queue so the test is the only one issuing GC
							// requests.
							DisableGCQueue: true,
							EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
								// The thresholdFirst = false variants will perform the unsafe
								// action of bumping the GC threshold at the same time that it
								// GCs individual keys.
								AllowGCWithNewThresholdAndKeys: true,
							},
						},
					},
				},
			})
			defer tc.Stopper().Stop(ctx)
			key := tc.ScratchRange(t)
			desc := tc.LookupRangeOrFatal(t, key)
			tc.AddVotersOrFatal(t, key, tc.Target(1))

			var stores []*Store
			for i := 0; i < tc.NumServers(); i++ {
				server := tc.Server(i)
				store, err := server.GetStores().(*Stores).GetStore(server.GetFirstStoreID())
				require.NoError(t, err)
				stores = append(stores, store)
			}
			writer := stores[0]
			reader := stores[0]
			if followerRead {
				reader = stores[1]
			}

			now := tc.Server(0).Clock().Now()
			ts1 := now.Add(1, 0)
			ts2 := now.Add(2, 0)
			ts3 := now.Add(3, 0)
			h1 := roachpb.Header{RangeID: desc.RangeID, Timestamp: ts1}
			h2 := roachpb.Header{RangeID: desc.RangeID, Timestamp: ts2}
			h3 := roachpb.Header{RangeID: desc.RangeID, Timestamp: ts3}
			va := []byte("a")
			vb := []byte("b")

			// Write two versions of the key:
			//  k@ts1 -> a
			//  k@ts2 -> b
			pArgs := putArgs(key, va)
			_, pErr := kv.SendWrappedWith(ctx, writer, h1, &pArgs)
			require.Nil(t, pErr)

			pArgs = putArgs(key, vb)
			_, pErr = kv.SendWrappedWith(ctx, writer, h2, &pArgs)
			require.Nil(t, pErr)

			// If the test wants to read from a follower, drop the closed timestamp
			// duration and then wait until the follower can serve requests at ts1.
			if followerRead {
				_, err := tc.ServerConn(0).Exec(
					`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
				require.NoError(t, err)

				testutils.SucceedsSoon(t, func() error {
					var ba roachpb.BatchRequest
					ba.RangeID = desc.RangeID
					ba.ReadConsistency = roachpb.INCONSISTENT
					ba.Add(&roachpb.QueryResolvedTimestampRequest{
						RequestHeader: roachpb.RequestHeader{Key: key, EndKey: key.Next()},
					})
					br, pErr := reader.Send(ctx, ba)
					require.Nil(t, pErr)
					rts := br.Responses[0].GetQueryResolvedTimestamp().ResolvedTS
					if rts.Less(ts1) {
						return errors.Errorf("resolved timestamp %s < %s", rts, ts1)
					}
					return nil
				})
			}

			// Verify that a read @ ts1 returns "a".
			gArgs := getArgs(key)
			resp, pErr := kv.SendWrappedWith(ctx, reader, h1, &gArgs)
			require.Nil(t, pErr)
			require.NotNil(t, resp)
			require.NotNil(t, resp.(*roachpb.GetResponse).Value)
			b, err := resp.(*roachpb.GetResponse).Value.GetBytes()
			require.Nil(t, err)
			require.Equal(t, va, b)

			// Perform two actions concurrently:
			//  1. GC up to ts2. This should remove the k@ts1 version.
			//  2. Read @ ts1.
			//
			// There are two valid results for the read. If it wins the race, it
			// should succeed and return "a". If it loses the race, it should fail
			// with a BatchTimestampBeforeGCError error. It should not be possible
			// for the read to return without error and also without the value "a".
			// This would indicate a bug in the synchronization between reads and GC
			// operations.
			gc := func() {
				if thresholdFirst {
					gcReq := gcArgs(key, key.Next())
					gcReq.Threshold = ts2
					_, pErr = kv.SendWrappedWith(ctx, writer, h3, &gcReq)
					require.Nil(t, pErr)
				}

				gcReq := gcArgs(key, key.Next(), gcKey(key, ts1))
				if !thresholdFirst {
					gcReq.Threshold = ts2
				}
				_, pErr = kv.SendWrappedWith(ctx, writer, h3, &gcReq)
				require.Nil(t, pErr)
			}
			read := func() {
				resp, pErr := kv.SendWrappedWith(ctx, reader, h1, &gArgs)
				if pErr == nil {
					t.Logf("read won race: %v", resp)
					require.NotNil(t, resp)
					require.NotNil(t, resp.(*roachpb.GetResponse).Value)
					b, err := resp.(*roachpb.GetResponse).Value.GetBytes()
					require.Nil(t, err)
					require.Equal(t, va, b)
				} else {
					t.Logf("read lost race: %v", pErr)
					gcErr := &roachpb.BatchTimestampBeforeGCError{}
					require.ErrorAs(t, pErr.GoError(), &gcErr)
				}
			}

			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); gc() }()
			go func() { defer wg.Done(); read() }()
			wg.Wait()
		})
	})
}

func TestReplicaTimestampCacheBumpNotLost(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	ctx = tc.store.AnnotateCtx(ctx)
	key := keys.LocalMax

	txn := newTransaction("test", key, 1, tc.Clock())

	minNewTS := func() hlc.Timestamp {
		var ba roachpb.BatchRequest
		scan := scanArgs(key, tc.repl.Desc().EndKey.AsRawKey())
		ba.Add(scan)

		resp, pErr := tc.Sender().Send(ctx, ba)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if resp.Timestamp.LessEq(txn.WriteTimestamp) {
			t.Fatalf("expected txn ts %s < scan TS %s", txn.WriteTimestamp, resp.Timestamp)
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

	if !reflect.DeepEqual(origTxn, txn) {
		t.Fatalf(
			"original transaction proto was mutated: %s",
			pretty.Diff(origTxn, txn),
		)
	}
	if resp.Txn == nil {
		t.Fatal("no transaction in response")
	} else if resp.Txn.WriteTimestamp.Less(minNewTS) {
		t.Fatalf(
			"expected txn ts bumped at least to %s, but got %s",
			minNewTS, txn.WriteTimestamp,
		)
	}
}

func TestReplicaEvaluationNotTxnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	ctx = tc.repl.AnnotateCtx(ctx)
	key := keys.LocalMax

	txn := newTransaction("test", key, 1, tc.Clock())

	var ba roachpb.BatchRequest
	ba.Txn = txn
	ba.Timestamp = txn.WriteTimestamp
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

	batch, _, _, _, pErr := tc.repl.evaluateWriteBatch(ctx, makeIDKey(), &ba, uncertainty.Interval{}, allSpansGuard())
	defer batch.Close()
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !reflect.DeepEqual(origTxn, txn) {
		t.Fatalf("transaction was mutated during evaluation: %s", pretty.Diff(origTxn, txn))
	}
}

// TODO(peter): Test replicaMetrics.leaseholder.
func TestReplicaMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	progress := func(vals ...uint64) map[uint64]tracker.Progress {
		m := make(map[uint64]tracker.Progress)
		for i, v := range vals {
			m[uint64(i+1)] = tracker.Progress{Match: v}
		}
		return m
	}
	status := func(lead uint64, progress map[uint64]tracker.Progress) *raft.Status {
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
			d.InternalReplicas = append(d.InternalReplicas, roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(i + 1),
				StoreID:   roachpb.StoreID(id),
				NodeID:    roachpb.NodeID(id),
			})
		}
		return d
	}
	live := func(ids ...roachpb.NodeID) liveness.IsLiveMap {
		m := liveness.IsLiveMap{}
		for _, id := range ids {
			m[id] = liveness.IsLiveMapEntry{IsLive: true}
		}
		return m
	}

	ctx := context.Background()
	var tc testContext
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(nil)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	testCases := []struct {
		replicas    int32
		storeID     roachpb.StoreID
		desc        roachpb.RangeDescriptor
		raftStatus  *raft.Status
		liveness    liveness.IsLiveMap
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
		{1, 1, desc(1), status(1, progress(2)), live(1), 5 * cfg.RaftLogTruncationThreshold,
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
			spanConfig := cfg.DefaultSpanConfig
			spanConfig.NumReplicas = c.replicas

			// Alternate between quiescent and non-quiescent replicas to test the
			// quiescent metric.
			c.expected.Quiescent = i%2 == 0
			c.expected.Ticking = !c.expected.Quiescent
			metrics := calcReplicaMetrics(
				ctx, hlc.Timestamp{}, &cfg.RaftConfig, spanConfig,
				c.liveness, 0, &c.desc, c.raftStatus, kvserverpb.LeaseStatus{},
				c.storeID, c.expected.Quiescent, c.expected.Ticking,
				concurrency.LatchMetrics{}, concurrency.LockTableMetrics{}, c.raftLogSize, true)
			require.Equal(t, c.expected, metrics)
		})
	}
}

// TestCancelPendingCommands verifies that cancelPendingCommands sends
// an error to each command awaiting execution.
func TestCancelPendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Install a proposal function which drops all increment commands on
	// the floor (so the command remains "pending" until we cancel it).
	proposalDroppedCh := make(chan struct{})
	proposalDropped := false
	tc.repl.mu.Lock()
	tc.repl.mu.proposalBuf.testing.submitProposalFilter = func(p *ProposalData) (drop bool, _ error) {
		if _, ok := p.Request.GetArg(roachpb.Increment); ok {
			if !proposalDropped {
				// Notify the main thread the first time we drop a proposal.
				close(proposalDroppedCh)
				proposalDropped = true
			}
			return true, nil
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	errChan := make(chan *roachpb.Error, 1)
	go func() {
		incArgs := incrementArgs(roachpb.Key("a"), 1)
		_, pErr := kv.SendWrapped(ctx, tc.Sender(), incArgs)
		errChan <- pErr
	}()

	<-proposalDroppedCh

	select {
	case pErr := <-errChan:
		t.Fatalf("command finished earlier than expected with error %v", pErr)
	default:
	}
	tc.repl.raftMu.Lock()
	tc.repl.disconnectReplicationRaftMuLocked(ctx)
	tc.repl.raftMu.Unlock()
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
	defer log.Scope(t).Close(t)

	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DontRetryPushTxnFailures = true
	rh := roachpb.RequestHeader{Key: roachpb.Key("a")}
	txn := newTransaction(
		"name",
		rh.Key,
		roachpb.NormalUserPriority,
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
	endTxnCommitReq := &roachpb.EndTxnRequest{
		RequestHeader: rh,
		Commit:        true,
	}
	endTxnAbortReq := &roachpb.EndTxnRequest{
		RequestHeader: rh,
		Commit:        true,
	}
	hbTxnReq := &roachpb.HeartbeatTxnRequest{
		RequestHeader: rh,
		Now:           cfg.Clock.Now(),
	}
	pushTxnReq := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.TxnMeta.Key,
		},
		PusheeTxn: txn.TxnMeta,
		PushType:  roachpb.PUSH_ABORT,
		Force:     true,
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
	barrierReq := &roachpb.BarrierRequest{
		RequestHeader: rh,
	}

	sendReq := func(
		ctx context.Context, repl *Replica, req roachpb.Request, txn *roachpb.Transaction,
	) *roachpb.Error {
		var ba roachpb.BatchRequest
		ba.Header.RangeID = repl.RangeID
		ba.Add(req)
		ba.Txn = txn
		if err := ba.SetActiveTimestamp(repl.Clock()); err != nil {
			t.Fatal(err)
		}
		_, pErr := repl.Send(ctx, ba)
		return pErr
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
			name:        "end txn (commit) with auto-gc, without existing record",
			useTxn:      true,
			req:         endTxnCommitReq,
			expProposal: false,
		},
		{
			name:        "end txn (abort) with auto-gc, without existing record",
			useTxn:      true,
			req:         endTxnAbortReq,
			expProposal: false,
		},
		{
			name: "end txn (commit) with auto-gc, with existing record",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, hbTxnReq, txn)
			},
			useTxn:      true,
			req:         endTxnCommitReq,
			expProposal: true,
		},
		{
			name: "end txn (abort) with auto-gc, with existing record",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, hbTxnReq, txn)
			},
			useTxn:      true,
			req:         endTxnAbortReq,
			expProposal: true,
		},
		{
			name:        "heartbeat txn",
			useTxn:      true,
			req:         hbTxnReq,
			expProposal: true,
		},
		{
			name: "push txn req",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, hbTxnReq, txn)
			},
			req:         pushTxnReq,
			expProposal: true,
		},
		{
			name: "redundant push txn req",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				if pErr := sendReq(ctx, repl, hbTxnReq, txn); pErr != nil {
					return pErr
				}
				return sendReq(ctx, repl, pushTxnReq, nil /* txn */)
			},
			req: pushTxnReq,
			// No-op - the transaction has already been pushed successfully.
			expProposal: false,
		},
		{
			name: "resolve committed intent req, with intent",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, putReq, txn)
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
			name: "resolve aborted intent req, with intent",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, putReq, txn)
			},
			req: resolveAbortedIntentReq,
			// Not a no-op - the request needs to poison the abort span.
			expProposal: true,
		},
		{
			name: "resolve aborted intent req, without intent",
			req:  resolveAbortedIntentReq,
			// No-op - the intent is missing, so there's nothing to resolve.
			// This also means that the abort span isn't written.
			expProposal: false,
		},
		{
			name: "redundant resolve aborted intent req",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, resolveAbortedIntentReq, nil /* txn */)
			},
			req: resolveAbortedIntentReq,
			// No-op - the abort span has already been poisoned.
			expProposal: false,
		},
		{
			name: "barrier",
			setup: func(ctx context.Context, repl *Replica) *roachpb.Error {
				return sendReq(ctx, repl, barrierReq, nil /* txn */)
			},
			req: barrierReq,
			// Barrier requests are always proposed on the leaseholder.
			expProposal: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)

			tc := testContext{}
			tc.StartWithStoreConfig(ctx, t, stopper, cfg)
			repl := tc.repl

			// Update the transaction's timestamps so that it
			// doesn't run into issues with the new cluster.
			now := tc.Clock().Now()
			txn.WriteTimestamp = now
			txn.MinTimestamp = now
			txn.ReadTimestamp = now

			if c.setup != nil {
				if pErr := c.setup(ctx, repl); pErr != nil {
					t.Fatalf("test setup failed: %v", pErr)
				}
			}

			var propCount int32
			markerTS := tc.Clock().Now()
			repl.mu.Lock()
			repl.store.TestingKnobs().TestingProposalFilter =
				func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
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
				ba.Txn.ReadTimestamp = markerTS
				ba.Txn.WriteTimestamp = markerTS
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	st := tc.store.cfg.Settings
	st.Manual.Store(true)
	MaxCommandSize.Override(ctx, &st.SV, 1024)

	args := putArgs(roachpb.Key("k"),
		[]byte(strings.Repeat("a", int(MaxCommandSize.Get(&st.SV)))))
	if _, pErr := tc.SendWrapped(&args); !testutils.IsPError(pErr, "command is too large") {
		t.Fatalf("did not get expected error: %v", pErr)
	}
}

// Test that, if the application of a Raft command fails, intents are not
// resolved. This is because we don't want intent resolution to take place if an
// EndTxn fails.
func TestErrorInRaftApplicationClearsIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var storeKnobs StoreTestingKnobs
	var filterActive int32
	key := roachpb.Key("a")
	rkey, err := keys.Addr(key)
	if err != nil {
		t.Fatal(err)
	}
	storeKnobs.TestingApplyFilter = func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		if atomic.LoadInt32(&filterActive) == 1 {
			return 0, roachpb.NewErrorf("boom")
		}
		return 0, nil
	}
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storeKnobs}})
	defer s.Stopper().Stop(context.Background())

	splitKey := roachpb.Key("b")
	if err := kvDB.AdminSplit(ctx, splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// Fail future command applications.
	atomic.StoreInt32(&filterActive, 1)

	// Propose an EndTxn with a remote intent. The _remote_ part is important
	// because intents local to the txn's range are resolved inline with the
	// EndTxn execution.
	// We do this by using replica.propose() directly, as opposed to going through
	// the DistSender, because we want to inspect the proposal's result after the
	// injected error.
	txn := newTransaction("test", key, roachpb.NormalUserPriority, s.Clock())
	// Increase the sequence to make it look like there have been some writes.
	// This fits with the LockSpans that we're going to set on the EndTxn.
	// Without properly setting the sequence number, the EndTxn batch would
	// erroneously execute as a 1PC.
	txn.Sequence++
	etArgs, _ := endTxnArgs(txn, true /* commit */)
	etArgs.LockSpans = []roachpb.Span{{Key: roachpb.Key("bb")}}
	var ba roachpb.BatchRequest
	ba.Header.Txn = txn
	ba.Add(&etArgs)
	assignSeqNumsForReqs(txn, &etArgs)
	require.NoError(t, ba.SetActiveTimestamp(s.Clock()))
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
	st := kvserverpb.LeaseStatus{Lease: exLease, State: kvserverpb.LeaseState_VALID}
	_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, pErr := repl.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
	if pErr != nil {
		t.Fatal(pErr)
	}
	propRes := <-ch
	if !testutils.IsPError(propRes.Err, "boom") {
		t.Fatalf("expected injected error, got: %v", propRes.Err)
	}
	if len(propRes.EncounteredIntents) != 0 {
		t.Fatal("expected encountered intents to have been cleared")
	}
}

// TestProposeWithAsyncConsensus tests that the proposal of a batch with
// AsyncConsensus set to true will return its evaluation result before Raft
// command has completed consensus and applied.
func TestProposeWithAsyncConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	tsc := TestStoreConfig(nil)

	var filterActive int32
	blockRaftApplication := make(chan struct{})
	tsc.TestingKnobs.TestingApplyFilter =
		func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
			if atomic.LoadInt32(&filterActive) == 1 {
				<-blockRaftApplication
			}
			return 0, nil
		}

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	repl := tc.repl

	var ba roachpb.BatchRequest
	key := roachpb.Key("a")
	put := putArgs(key, []byte("val"))
	ba.Add(&put)
	ba.Timestamp = tc.Clock().Now()
	ba.AsyncConsensus = true

	atomic.StoreInt32(&filterActive, 1)
	st := tc.repl.CurrentLeaseStatus(ctx)
	_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, pErr := repl.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
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

// TestApplyPaginatedCommittedEntries tests that a Raft group's committed
// entries are quickly applied, even if their application is paginated due to
// the RaftMaxSizePerMsg configuration. This is a regression test for #31330.
func TestApplyPaginatedCommittedEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	tsc := TestStoreConfig(nil)

	// Drop the RaftMaxCommittedSizePerReady so that even small Raft entries
	// trigger pagination during entry application.
	tsc.RaftMaxCommittedSizePerReady = 128
	// Slow down the tick interval dramatically so that Raft groups can't rely
	// on ticks to trigger Raft ready iterations.
	tsc.RaftTickInterval = 5 * time.Second

	var filterActive int32
	blockRaftApplication := make(chan struct{})
	blockingRaftApplication := make(chan struct{}, 1)
	tsc.TestingKnobs.TestingApplyFilter =
		func(filterArgs kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
			if atomic.LoadInt32(&filterActive) == 1 {
				select {
				case blockingRaftApplication <- struct{}{}:
				default:
				}
				<-blockRaftApplication
			}
			return 0, nil
		}

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	repl := tc.repl

	// Block command application then propose a command to Raft.
	var ba roachpb.BatchRequest
	key := roachpb.Key("a")
	put := putArgs(key, []byte("val"))
	ba.Add(&put)
	ba.Timestamp = tc.Clock().Now()

	atomic.StoreInt32(&filterActive, 1)
	st := repl.CurrentLeaseStatus(ctx)
	_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	_, _, _, pErr := repl.evalAndPropose(ctx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Once that command is stuck applying, propose a number of large commands.
	// This will allow them to all build up without any being applied so that
	// their application will require pagination.
	<-blockingRaftApplication
	var ch chan proposalResult
	for i := 0; i < 50; i++ {
		var ba2 roachpb.BatchRequest
		key := roachpb.Key("a")
		put := putArgs(key, make([]byte, 2*tsc.RaftMaxCommittedSizePerReady))
		ba2.Add(&put)
		ba2.Timestamp = tc.Clock().Now()

		var pErr *roachpb.Error
		_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		ch, _, _, pErr = repl.evalAndPropose(ctx, &ba2, allSpansGuard(), st, uncertainty.Interval{}, tok.Move(ctx))
		if pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Stop blocking Raft application. All of the proposals should quickly
	// commit and apply, even if their application is paginated due to the
	// small RaftMaxCommittedSizePerReady.
	close(blockRaftApplication)
	const maxWait = 10 * time.Second
	select {
	case propRes := <-ch:
		if propRes.Err != nil {
			t.Fatalf("unexpected proposal result error: %v", propRes.Err)
		}
		if propRes.Reply == nil || len(propRes.Reply.Responses) != 1 {
			t.Fatalf("expected proposal result with 1 response, found: %v", propRes.Reply)
		}
	case <-time.After(maxWait):
		// If we don't re-enqueue Raft groups for another round of processing
		// when their committed entries are paginated and not all immediately
		// applied, this test will take more than three minutes to finish.
		t.Fatalf("stall detected, proposal did not finish within %s", maxWait)
	}
}

func TestSplitMsgApps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	desc            roachpb.RangeDescriptor
	numProposals    int
	pendingQuota    bool
	status          *raft.Status
	lastIndex       uint64
	raftReady       bool
	ownsValidLease  bool
	mergeInProgress bool
	isDestroyed     bool

	// Not used to implement quiescer, but used by tests.
	livenessMap liveness.IsLiveMap
}

func (q *testQuiescer) descRLocked() *roachpb.RangeDescriptor {
	return &q.desc
}

func (q *testQuiescer) raftStatusRLocked() *raft.Status {
	return q.status
}

func (q *testQuiescer) raftBasicStatusRLocked() raft.BasicStatus {
	return q.status.BasicStatus
}

func (q *testQuiescer) raftLastIndexLocked() (uint64, error) {
	return q.lastIndex, nil
}

func (q *testQuiescer) hasRaftReadyRLocked() bool {
	return q.raftReady
}

func (q *testQuiescer) hasPendingProposalsRLocked() bool {
	return q.numProposals > 0
}

func (q *testQuiescer) hasPendingProposalQuotaRLocked() bool {
	return q.pendingQuota
}

func (q *testQuiescer) ownsValidLeaseRLocked(context.Context, hlc.ClockTimestamp) bool {
	return q.ownsValidLease
}

func (q *testQuiescer) mergeInProgressRLocked() bool {
	return q.mergeInProgress
}

func (q *testQuiescer) isDestroyedRLocked() (DestroyReason, error) {
	if q.isDestroyed {
		return destroyReasonRemoved, errors.New("testQuiescer: replica destroyed")
	}
	return 0, nil
}

func TestShouldReplicaQuiesce(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const logIndex = 10
	const invalidIndex = 11
	test := func(expected bool, transform func(q *testQuiescer) *testQuiescer) {
		t.Run("", func(t *testing.T) {
			// A testQuiescer initialized so that shouldReplicaQuiesce will return
			// true. The transform function is intended to perform one mutation to
			// this quiescer so that shouldReplicaQuiesce will return false.
			q := &testQuiescer{
				desc: roachpb.RangeDescriptor{
					InternalReplicas: []roachpb.ReplicaDescriptor{
						{NodeID: 1, ReplicaID: 1},
						{NodeID: 2, ReplicaID: 2},
						{NodeID: 3, ReplicaID: 3},
					},
				},
				status: &raft.Status{
					BasicStatus: raft.BasicStatus{
						ID: 1,
						HardState: raftpb.HardState{
							Commit: logIndex,
						},
						SoftState: raft.SoftState{
							RaftState: raft.StateLeader,
						},
						Applied:        logIndex,
						LeadTransferee: 0,
					},
					Progress: map[uint64]tracker.Progress{
						1: {Match: logIndex},
						2: {Match: logIndex},
						3: {Match: logIndex},
					},
				},
				lastIndex:      logIndex,
				raftReady:      false,
				ownsValidLease: true,
				livenessMap: liveness.IsLiveMap{
					1: {IsLive: true},
					2: {IsLive: true},
					3: {IsLive: true},
				},
			}
			q = transform(q)
			_, lagging, ok := shouldReplicaQuiesce(context.Background(), q, hlc.ClockTimestamp{}, q.livenessMap)
			require.Equal(t, expected, ok)
			if ok {
				// Any non-live replicas should be in the laggingReplicaSet.
				var expLagging laggingReplicaSet
				for _, rep := range q.descRLocked().Replicas().Descriptors() {
					if l, ok := q.livenessMap[rep.NodeID]; ok && !l.IsLive {
						expLagging = append(expLagging, l.Liveness)
					}
				}
				sort.Sort(expLagging)
				require.Equal(t, expLagging, lagging)
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
		q.pendingQuota = true
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.mergeInProgress = true
		return q
	})
	test(false, func(q *testQuiescer) *testQuiescer {
		q.isDestroyed = true
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
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
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
			q.desc.InternalReplicas[i].ReplicaID = roachpb.ReplicaID(4 + i)
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
			nodeID := roachpb.NodeID(i)
			q.livenessMap[nodeID] = liveness.IsLiveMapEntry{
				Liveness: livenesspb.Liveness{NodeID: nodeID},
				IsLive:   false,
			}
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
			return q
		})
	}
	// Verify no quiescence when replica progress doesn't match, if
	// given a nil liveness map.
	for _, i := range []uint64{1, 2, 3} {
		test(false, func(q *testQuiescer) *testQuiescer {
			q.livenessMap = nil
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
			return q
		})
	}
	// Verify no quiescence when replica progress doesn't match, if
	// liveness map does not contain the lagging replica.
	for _, i := range []uint64{1, 2, 3} {
		test(false, func(q *testQuiescer) *testQuiescer {
			delete(q.livenessMap, roachpb.NodeID(i))
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
			return q
		})
	}
}

func TestFollowerQuiesceOnNotify(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	test := func(
		expected bool,
		transform func(*testQuiescer, kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest),
	) {
		t.Run("", func(t *testing.T) {
			q := &testQuiescer{
				status: &raft.Status{
					BasicStatus: raft.BasicStatus{
						ID: 2,
						HardState: raftpb.HardState{
							Term:   5,
							Commit: 10,
						},
						SoftState: raft.SoftState{
							Lead: 1,
						},
					},
				},
				livenessMap: liveness.IsLiveMap{
					1: {IsLive: true},
					2: {IsLive: true},
					3: {IsLive: true},
				},
			}
			req := kvserverpb.RaftMessageRequest{
				Message: raftpb.Message{
					Type:   raftpb.MsgHeartbeat,
					From:   1,
					Term:   5,
					Commit: 10,
				},
				Quiesce:                   true,
				LaggingFollowersOnQuiesce: nil,
			}
			q, req = transform(q, req)

			ok := shouldFollowerQuiesceOnNotify(
				context.Background(),
				q,
				req.Message,
				laggingReplicaSet(req.LaggingFollowersOnQuiesce),
				q.livenessMap,
			)
			require.Equal(t, expected, ok)
		})
	}

	test(true, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		return q, req
	})
	test(false, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		req.Message.Term = 4
		return q, req
	})
	test(false, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		req.Message.Commit = 9
		return q, req
	})
	test(false, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		q.numProposals = 1
		return q, req
	})
	// Lagging replica with same liveness information.
	test(true, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		l := livenesspb.Liveness{
			NodeID:     3,
			Epoch:      7,
			Expiration: hlc.LegacyTimestamp{WallTime: 8},
		}
		q.livenessMap[l.NodeID] = liveness.IsLiveMapEntry{
			Liveness: l,
			IsLive:   false,
		}
		req.LaggingFollowersOnQuiesce = []livenesspb.Liveness{l}
		return q, req
	})
	// Lagging replica with older liveness information.
	test(false, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		l := livenesspb.Liveness{
			NodeID:     3,
			Epoch:      7,
			Expiration: hlc.LegacyTimestamp{WallTime: 8},
		}
		q.livenessMap[l.NodeID] = liveness.IsLiveMapEntry{
			Liveness: l,
			IsLive:   false,
		}
		lOld := l
		lOld.Epoch--
		req.LaggingFollowersOnQuiesce = []livenesspb.Liveness{lOld}
		return q, req
	})
	test(false, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		l := livenesspb.Liveness{
			NodeID:     3,
			Epoch:      7,
			Expiration: hlc.LegacyTimestamp{WallTime: 8},
		}
		q.livenessMap[l.NodeID] = liveness.IsLiveMapEntry{
			Liveness: l,
			IsLive:   false,
		}
		lOld := l
		lOld.Expiration.WallTime--
		req.LaggingFollowersOnQuiesce = []livenesspb.Liveness{lOld}
		return q, req
	})
	// Lagging replica with newer liveness information.
	test(true, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		l := livenesspb.Liveness{
			NodeID:     3,
			Epoch:      7,
			Expiration: hlc.LegacyTimestamp{WallTime: 8},
		}
		q.livenessMap[l.NodeID] = liveness.IsLiveMapEntry{
			Liveness: l,
			IsLive:   false,
		}
		lNew := l
		lNew.Epoch++
		req.LaggingFollowersOnQuiesce = []livenesspb.Liveness{lNew}
		return q, req
	})
	test(true, func(q *testQuiescer, req kvserverpb.RaftMessageRequest) (*testQuiescer, kvserverpb.RaftMessageRequest) {
		l := livenesspb.Liveness{
			NodeID:     3,
			Epoch:      7,
			Expiration: hlc.LegacyTimestamp{WallTime: 8},
		}
		q.livenessMap[l.NodeID] = liveness.IsLiveMapEntry{
			Liveness: l,
			IsLive:   false,
		}
		lNew := l
		lNew.Expiration.WallTime++
		req.LaggingFollowersOnQuiesce = []livenesspb.Liveness{lNew}
		return q, req
	})
}

func TestReplicaRecomputeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

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

	seed := randutil.NewPseudoSeed()
	t.Logf("seed is %d", seed)
	rnd := rand.New(rand.NewSource(seed))

	repl.raftMu.Lock()
	repl.mu.Lock()
	ms := repl.mu.state.Stats // intentionally mutated below
	disturbMS := enginepb.NewPopulatedMVCCStats(rnd, false)
	disturbMS.ContainsEstimates = 0
	ms.Add(*disturbMS)
	err := repl.raftMu.stateLoader.SetMVCCStats(ctx, tc.engine, ms)
	repl.assertStateRaftMuLockedReplicaMuRLocked(ctx, tc.engine)
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs = StoreTestingKnobs{
		TestingRequestFilter: func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			if _, ok := ba.GetArg(roachpb.ComputeChecksum); ok {
				return roachpb.NewErrorf("boom")
			}
			return nil
		},
	}
	tc := testContext{}
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	for i := 0; i < 2; i++ {
		// Do this twice because it used to deadlock. See #25456.
		sysCfg := tc.store.Gossip().DeprecatedGetSystemConfig()
		processed, err := tc.store.consistencyQueue.process(ctx, tc.repl, sysCfg)
		if !testutils.IsError(err, "boom") {
			t.Fatal(err)
		}
		assert.False(t, processed)
	}
}

// TestReplicaServersideRefreshes verifies local retry logic for transactional
// and non transactional batches. Verifies the timestamp cache is updated to
// reflect the timestamp at which retried batches are executed.
func TestReplicaServersideRefreshes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// TODO(andrei): make each subtest use its own testContext so that they don't
	// have to use distinct keys.
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Increment the clock so that all the transactions in the tests run at a
	// different physical timestamp than the one used to initialize the replica's
	// timestamp cache. This allows one of the tests to reset the logical part of
	// the timestamp it's operating and not run into the timestamp cache.
	tc.manualClock.Increment(1)

	newTxn := func(key string, ts hlc.Timestamp) *roachpb.Transaction {
		txn := roachpb.MakeTransaction(
			"test", roachpb.Key(key), roachpb.NormalUserPriority, ts, 0, 0,
		)
		return &txn
	}
	send := func(ba roachpb.BatchRequest) (hlc.Timestamp, error) {
		br, pErr := tc.Sender().Send(ctx, ba)
		if pErr != nil {
			return hlc.Timestamp{}, pErr.GoError()
		}

		// Check that we didn't mess up the stats.
		// Regression test for #31870.
		snap := tc.engine.NewSnapshot()
		defer snap.Close()
		res, err := tc.repl.sha512(ctx, *tc.repl.Desc(), tc.engine,
			nil /* diff */, roachpb.ChecksumMode_CHECK_FULL,
			quotapool.NewRateLimiter("ConsistencyQueue", quotapool.Limit(math.MaxFloat64), math.MaxInt64))
		if err != nil {
			return hlc.Timestamp{}, err
		}
		if res.PersistedMS != res.RecomputedMS {
			return hlc.Timestamp{}, errors.Errorf("stats are inconsistent:\npersisted:\n%+v\nrecomputed:\n%+v", res.PersistedMS, res.RecomputedMS)
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
		name    string
		setupFn func() (hlc.Timestamp, error) // returns expected batch execution timestamp
		batchFn func(hlc.Timestamp) (roachpb.BatchRequest, hlc.Timestamp)
		expErr  string
	}{
		{
			name: "serverside-refresh of write too old on put",
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
			name: "serverside-refresh of write too old on cput",
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
		},
		{
			name: "serverside-refresh of write too old on initput",
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
		},
		// Serverside-refresh will not be allowed because the request contains
		// a read-only request that acquires read-latches. We cannot bump the
		// request's timestamp without re-acquiring latches, so we don't even
		// try to.
		// NOTE: this is an unusual batch because DistSender usually splits
		// reads and writes. Still, we should handle it correctly.
		{
			name: "no serverside-refresh of write too old on get and put",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				get := getArgs(roachpb.Key("a"))
				put := putArgs(roachpb.Key("a"), []byte("put2"))
				ba.Add(&get, &put)
				return
			},
			expErr: "write for key .* at timestamp .* too old",
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
			name: "no serverside-refresh of write too old on non-1PC txn cput",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("c-cput", "put")
				return put("c-cput", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("c-cput", ts.Prev())
				cput := cPutArgs(roachpb.Key("c-cput"), []byte("iput"), []byte("put"))
				ba.Add(&cput)
				assignSeqNumsForReqs(ba.Txn, &cput)
				return
			},
			expErr: "write for key .* at timestamp .* too old",
		},
		// Non-1PC serializable txn initput will fail with write too old error.
		{
			name: "no serverside-refresh of write too old on non-1PC txn initput",
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
			expErr: "write for key .* at timestamp .* too old",
		},
		// Non-1PC serializable txn locking scan will fail with write too old error.
		{
			name: "no serverside-refresh of write too old on non-1PC txn locking scan",
			setupFn: func() (hlc.Timestamp, error) {
				return put("c-scan", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("c-scan", ts.Prev())
				scan := scanArgs(roachpb.Key("c-scan"), roachpb.Key("c-scan\x00"))
				scan.KeyLocking = lock.Exclusive
				ba.Add(scan)
				return
			},
			expErr: "write for key .* at timestamp .* too old",
		},
		// Non-1PC serializable txn cput with CanForwardReadTimestamp set to
		// true will succeed with write too old error.
		{
			name: "serverside-refresh of write too old on non-1PC txn cput without prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("c-cput", "put")
				return put("c-cput", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba.Txn = newTxn("c-cput", ts.Prev())
				ba.CanForwardReadTimestamp = true
				cput := cPutArgs(roachpb.Key("c-cput"), []byte("iput"), []byte("put"))
				ba.Add(&cput)
				assignSeqNumsForReqs(ba.Txn, &cput)
				return
			},
		},
		// This test tests a scenario where an InitPut is failing at its timestamp,
		// but it would succeed if it'd evaluate at a bumped timestamp. The request
		// is not retried at the bumped timestamp. We don't necessarily like this
		// current behavior; for example since there's nothing to refresh, the
		// request could be retried.
		{
			name: "serverside-refresh of write too old on non-1PC txn initput without prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				// Note there are two different version of the value, but a
				// non-txnal cput will evaluate the most recent version and
				// avoid a condition failed error.
				_, _ = put("c-iput", "put1")
				return put("c-iput", "put2")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("c-iput", ts.Prev())
				ba.CanForwardReadTimestamp = true
				iput := iPutArgs(roachpb.Key("c-iput"), []byte("put2"))
				ba.Add(&iput)
				assignSeqNumsForReqs(ba.Txn, &iput)
				return
			},
			expErr: "unexpected value: .*",
		},
		// Non-1PC serializable txn locking scan with CanForwardReadTimestamp
		// set to true will succeed with write too old error.
		{
			name: "serverside-refresh of write too old on non-1PC txn locking scan without prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				return put("c-scan", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba.Txn = newTxn("c-scan", ts.Prev())
				ba.CanForwardReadTimestamp = true
				scan := scanArgs(roachpb.Key("c-scan"), roachpb.Key("c-scan\x00"))
				scan.KeyLocking = lock.Exclusive
				ba.Add(scan)
				return
			},
		},
		// 1PC serializable transaction will fail instead of retrying if
		// BatchRequest.CanForwardReadTimestamp is not true.
		{
			name: "no serverside-refresh of write too old on 1PC txn and refresh spans",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("d", "put")
				return put("d", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Txn = newTxn("d", ts.Prev())
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&cput, &et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
			expErr: "WriteTooOldError",
		},
		// 1PC serializable transaction will retry locally.
		{
			name: "serverside-refresh of write too old on 1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("e", "put")
				return put("e", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba.Txn = newTxn("e", ts.Prev())
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&cput, &et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
		},
		// 1PC serializable transaction will retry locally.
		{
			name: "serverside-refresh of write too old on 1PC txn without prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				_, _ = put("e", "put")
				return put("e", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba.Txn = newTxn("e", ts.Prev())
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&cput, &et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
		},
		// This test tests a scenario where a CPut is failing at its timestamp, but it would
		// succeed if it'd evaluate at a bumped timestamp. The request is not retried at the
		// bumped timestamp. We don't necessarily like this current behavior; for example if
		// there's nothing to refresh, the request could be retried.
		// The previous test shows different behavior for a non-transactional
		// request or a 1PC one.
		{
			name: "no serverside-refresh with failed cput despite write too old errors on txn",
			setupFn: func() (hlc.Timestamp, error) {
				return put("e1", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				txn := newTxn("e1", ts.Prev())

				// Send write to another key first to avoid 1PC.
				ba.Txn = txn
				put := putArgs([]byte("e1-other-key"), []byte("otherput"))
				ba.Add(&put)
				assignSeqNumsForReqs(ba.Txn, &put)
				if _, err := send(ba); err != nil {
					panic(err)
				}

				ba = roachpb.BatchRequest{}
				ba.Txn = txn
				// Indicate local retry is possible, even though we don't currently take
				// advantage of this.
				ba.CanForwardReadTimestamp = true
				cput := cPutArgs(roachpb.Key("e1"), []byte("cput"), []byte("put"))
				ba.Add(&cput)
				assignSeqNumsForReqs(ba.Txn, &cput)
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &et)
				return
			},
			expErr: "unexpected value: <nil>",
		},
		// Handle multiple write too old errors on a non-transactional request.
		//
		// Note that in this test's scenario if the request was transactional, it
		// generally would receive a ConditionFailedError from the CPuts.
		{
			name: "serverside-refresh with multiple write too old errors on non-txn request",
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
				expTS = ts.Next()
				// We're going to execute before any of the writes in setupFn.
				ts.Logical = 0
				ba.Timestamp = ts
				for i := 1; i <= 3; i++ {
					cput := cPutArgs(roachpb.Key(fmt.Sprintf("f%d", i)), []byte("cput"), []byte("put"))
					ba.Add(&cput)
				}
				return
			},
		},
		// Handle multiple write too old errors in 1PC transaction.
		{
			name: "serverside-refresh with multiple write too old errors on 1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				// Do a couple of writes. Their timestamps are going to differ in their
				// logical component. The batch that we're going to run in batchFn will
				// run at a lower timestamp than all of them.
				if _, err := put("ga1", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				if _, err := put("ga2", "put"); err != nil {
					return hlc.Timestamp{}, err
				}
				return put("ga3", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				// We're going to execute before any of the writes in setupFn.
				ts.Logical = 0
				ba.Txn = newTxn("ga1", ts)
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				for i := 1; i <= 3; i++ {
					cput := cPutArgs(roachpb.Key(fmt.Sprintf("ga%d", i)), []byte("cput"), []byte("put"))
					ba.Add(&cput)
					assignSeqNumsForReqs(ba.Txn, &cput)
				}
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &et)
				return
			},
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
				// Send write to another key first to avoid 1PC.
				ba.Txn = txn
				put := putArgs([]byte("h2"), []byte("otherput"))
				ba.Add(&put)
				assignSeqNumsForReqs(ba.Txn, &put)
				if _, err := send(ba); err != nil {
					panic(err)
				}
				// Send the remainder of the transaction in another batch.
				expTS = ts.Next()
				ba = roachpb.BatchRequest{}
				ba.Txn = txn
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				ba.Add(&cput)
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
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
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				expTS = ts.Next()
				cput := putArgs(ba.Txn.Key, []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				et.Require1PC = true // don't allow this to bypass the 1PC optimization
				ba.Add(&cput, &et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
		},
		// Regression test for #43273. When locking scans run into write too old
		// errors, the refreshed timestamp should not be below the txn's
		// existing write timestamp.
		{
			name: "serverside-refresh with write too old errors during locking scan",
			setupFn: func() (hlc.Timestamp, error) {
				return put("lscan", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				// Txn with (read_ts, write_ts) = (1, 4) finds a value with
				// `ts = 2`. Final timestamp should be `ts = 4`.
				ba.Txn = newTxn("lscan", ts.Prev())
				ba.Txn.WriteTimestamp = ts.Next().Next()
				ba.CanForwardReadTimestamp = true

				expTS = ba.Txn.WriteTimestamp

				scan := scanArgs(roachpb.Key("lscan"), roachpb.Key("lscan\x00"))
				scan.KeyLocking = lock.Upgrade
				ba.Add(scan)
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
				// Send write to another key first to avoid 1PC.
				ba.Txn = txn
				put1 := putArgs([]byte("i2"), []byte("otherput"))
				ba.Add(&put1)
				assignSeqNumsForReqs(ba.Txn, &put1)
				if _, err := send(ba); err != nil {
					panic(err)
				}
				// Send the remainder of the transaction in another batch.
				expTS = ts.Next()
				ba = roachpb.BatchRequest{}
				ba.Txn = txn
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				put2 := putArgs(ba.Txn.Key, []byte("newput"))
				ba.Add(&put2)
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&et)
				assignSeqNumsForReqs(ba.Txn, &put2, &et)
				return
			},
		},
		// TODO(andrei): We should also have a test similar to the one above, but
		// with the WriteTooOld flag set by a different batch than the one with the
		// EndTransaction. This is hard to do at the moment, though, because we
		// never defer the handling of the write too old conditions to the end of
		// the transaction (but we might in the future).
		{
			name: "serverside-refresh of read within uncertainty interval error on get in non-txn",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ba.Timestamp = ts.Prev()
				// NOTE: set the TimestampFromServerClock field manually. This is
				// usually set on the server for non-transactional requests without
				// client-assigned timestamps. It is also usually set to the same
				// value as the server-assigned timestamp. But to have more control
				// over the uncertainty interval that this request receives, we set
				// it here to a value above the request timestamp.
				serverTS := ts.Next()
				ba.TimestampFromServerClock = (*hlc.ClockTimestamp)(&serverTS)
				expTS = ts.Next()
				get := getArgs(roachpb.Key("a"))
				ba.Add(&get)
				return
			},
		},
		{
			name: "serverside-refresh of read within uncertainty interval error on get in non-1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ts = ts.Prev()
				ba.Txn = newTxn("a", ts)
				ba.Txn.GlobalUncertaintyLimit = expTS
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				get := getArgs(roachpb.Key("a"))
				ba.Add(&get)
				return
			},
		},
		{
			name: "serverside-refresh of read within uncertainty interval error on get in non-1PC txn with prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ts = ts.Prev()
				ba.Txn = newTxn("a", ts)
				ba.Txn.GlobalUncertaintyLimit = ts.Next()
				get := getArgs(roachpb.Key("a"))
				ba.Add(&get)
				return
			},
			expErr: "ReadWithinUncertaintyIntervalError",
		},
		{
			name: "serverside-refresh of read within uncertainty interval error on get in 1PC txn",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ts = ts.Prev()
				ba.Txn = newTxn("a", ts)
				ba.Txn.GlobalUncertaintyLimit = expTS
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				get := getArgs(roachpb.Key("a"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&get, &et)
				assignSeqNumsForReqs(ba.Txn, &get, &et)
				return
			},
		},
		{
			name: "serverside-refresh of read within uncertainty interval error on get in 1PC txn with prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba roachpb.BatchRequest, expTS hlc.Timestamp) {
				ts = ts.Prev()
				ba.Txn = newTxn("a", ts)
				ba.Txn.GlobalUncertaintyLimit = ts.Next()
				get := getArgs(roachpb.Key("a"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&get, &et)
				assignSeqNumsForReqs(ba.Txn, &get, &et)
				return
			},
			expErr: "ReadWithinUncertaintyIntervalError",
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
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	k := roachpb.Key("key")

	// Start a transaction and assign its ReadTimestamp.
	ts1 := tc.Clock().Now()
	txn := roachpb.MakeTransaction("test", k, roachpb.NormalUserPriority, ts1, 0, 0)

	// Write a value outside the transaction.
	tc.manualClock.Increment(10)
	ts2 := tc.Clock().Now()
	if err := storage.MVCCPut(ctx, tc.engine, nil, k, ts2, roachpb.MakeValueFromString("one"), nil); err != nil {
		t.Fatalf("writing interfering value: %+v", err)
	}

	// Push the transaction's timestamp. In real-world situations,
	// the only thing that can push a read-only transaction's
	// timestamp is ReadWithinUncertaintyIntervalError, but
	// synthesizing one of those in this single-node test harness is
	// tricky.
	tc.manualClock.Increment(10)
	ts3 := tc.Clock().Now()
	txn.WriteTimestamp.Forward(ts3)

	// Execute the write phase of the transaction as a single batch,
	// which must return a WriteTooOldError.
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
	ba.Header = roachpb.Header{Txn: &txn}
	put := putArgs(k, []byte("two"))
	et, _ := endTxnArgs(&txn, true)
	ba.Add(&put, &et)
	assignSeqNumsForReqs(&txn, &put, &et)
	if br, pErr := tc.Sender().Send(ctx, ba); pErr == nil {
		t.Errorf("did not get expected error. resp=%s", br)
	} else if wtoe, ok := pErr.GetDetail().(*roachpb.WriteTooOldError); !ok {
		t.Errorf("expected WriteTooOldError, got %s", wtoe)
	}
}

// TestReplicaNotifyLockTableOn1PC verifies that a 1-phase commit transaction
// notifies the concurrency manager's lock-table that the transaction has been
// committed. This is necessary even though the transaction, by virtue of
// performing a 1PC commit, could not have written any intents. It still could
// have acquired read locks.
func TestReplicaNotifyLockTableOn1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	// Disable txn liveness pushes. See below for why.
	st := tc.store.cfg.Settings
	st.Manual.Store(true)
	concurrency.LockTableLivenessPushDelay.Override(ctx, &st.SV, 24*time.Hour)

	// Write a value to a key A.
	key := roachpb.Key("a")
	initVal := incrementArgs(key, 1)
	if _, pErr := tc.SendWrapped(initVal); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Create a new transaction and perform a "for update" scan. This should
	// acquire unreplicated, exclusive locks on the key.
	txn := newTransaction("test", key, 1, tc.Clock())
	var ba roachpb.BatchRequest
	ba.Header = roachpb.Header{Txn: txn}
	ba.Add(roachpb.NewScan(key, key.Next(), true /* forUpdate */))
	if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Try to write to the key outside of this transaction. Should wait on the
	// "for update" lock in a lock wait-queue in the concurrency manager until
	// the lock is released. If we don't notify the lock-table when the first
	// txn eventually commits, this will wait for much longer than it needs to.
	// It will eventually push the first txn and notice that it has committed.
	// However, we've disabled liveness pushes in this test, so the test will
	// block forever without the lock-table notification. We didn't need to
	// disable deadlock detection pushes because this is a non-transactional
	// write, so it never performs them.
	pErrC := make(chan *roachpb.Error, 1)
	go func() {
		otherWrite := incrementArgs(key, 1)
		_, pErr := tc.SendWrapped(otherWrite)
		pErrC <- pErr
	}()

	// The second write should not complete.
	select {
	case pErr := <-pErrC:
		t.Fatalf("write unexpectedly finished with error: %v", pErr)
	case <-time.After(5 * time.Millisecond):
	}

	// Update the locked value and commit in a single batch. This should release
	// the "for update" lock.
	ba = roachpb.BatchRequest{}
	incArgs := incrementArgs(key, 1)
	et, etH := endTxnArgs(txn, true /* commit */)
	et.Require1PC = true
	et.LockSpans = []roachpb.Span{{Key: key, EndKey: key.Next()}}
	ba.Header = etH
	ba.Add(incArgs, &et)
	assignSeqNumsForReqs(txn, incArgs, &et)
	if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// The second write should complete.
	pErr := <-pErrC
	if pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}
}

func TestReplicaShouldCampaignOnWake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const storeID = roachpb.StoreID(1)

	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKeyMin,
		EndKey:   roachpb.RKeyMax,
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{
				ReplicaID: 1,
				NodeID:    1,
				StoreID:   1,
			},
			{
				ReplicaID: 2,
				NodeID:    2,
				StoreID:   2,
			},
			{
				ReplicaID: 3,
				NodeID:    3,
				StoreID:   3,
			},
		},
		NextReplicaID: 4,
	}
	livenessMap := liveness.IsLiveMap{
		1: liveness.IsLiveMapEntry{IsLive: true},
		2: liveness.IsLiveMapEntry{IsLive: false},
		4: liveness.IsLiveMapEntry{IsLive: false},
	}

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

	followerWithoutLeader := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      0,
		},
	}
	followerWithLeader := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      1,
		},
	}
	candidate := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateCandidate,
			Lead:      0,
		},
	}
	leader := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateLeader,
			Lead:      1,
		},
	}
	followerDeadLeader := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      2,
		},
	}
	candidateDeadLeader := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateCandidate,
			Lead:      2,
		},
	}
	followerMissingLiveness := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      3,
		},
	}
	followerMissingDesc := raft.BasicStatus{
		SoftState: raft.SoftState{
			RaftState: raft.StateFollower,
			Lead:      4,
		},
	}

	tests := []struct {
		leaseStatus           kvserverpb.LeaseStatus
		raftStatus            raft.BasicStatus
		livenessMap           liveness.IsLiveMap
		desc                  *roachpb.RangeDescriptor
		requiresExpiringLease bool
		exp                   bool
	}{
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: myLease}, followerWithoutLeader, livenessMap, &desc, false, true},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: otherLease}, followerWithoutLeader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: myLease}, followerWithLeader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: otherLease}, followerWithLeader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: myLease}, candidate, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: otherLease}, candidate, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: myLease}, leader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_VALID, Lease: otherLease}, leader, livenessMap, &desc, false, false},

		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: myLease}, followerWithoutLeader, livenessMap, &desc, false, true},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerWithoutLeader, livenessMap, &desc, false, true},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: myLease}, followerWithoutLeader, livenessMap, &desc, true, true},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerWithoutLeader, livenessMap, &desc, true, true},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: myLease}, followerWithLeader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerWithLeader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: myLease}, candidate, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, candidate, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: myLease}, leader, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, leader, livenessMap, &desc, false, false},

		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerDeadLeader, livenessMap, &desc, false, true},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerDeadLeader, livenessMap, &desc, true, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerMissingLiveness, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, followerMissingDesc, livenessMap, &desc, false, false},
		{kvserverpb.LeaseStatus{State: kvserverpb.LeaseState_EXPIRED, Lease: otherLease}, candidateDeadLeader, livenessMap, &desc, false, false},
	}

	for i, test := range tests {
		v := shouldCampaignOnWake(test.leaseStatus, storeID, test.raftStatus, test.livenessMap, test.desc, test.requiresExpiringLease)
		if v != test.exp {
			t.Errorf("%d: expected %v but got %v", i, test.exp, v)
		}
	}
}

func TestRangeStatsRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	keyPrefix := roachpb.RKey("dummy-prefix")

	// Write some random data to the range and verify that a RangeStatsRequest
	// returns the same MVCC stats as the replica's in-memory state.
	WriteRandomDataToRange(t, tc.store, tc.repl.RangeID, keyPrefix)
	expMS := tc.repl.GetMVCCStats()
	res, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
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
	res, pErr = kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
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

// TestTxnRecordLifecycleTransitions tests various scenarios where a transaction
// attempts to create or modify its transaction record. It verifies that
// finalized transaction records can never be recreated, even after they have
// been GCed. It also verifies that the effect of transaction pushes is not lost
// even when the push occurred before the transaction record was created.
func TestTxnRecordLifecycleTransitions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manual := hlc.NewManualClock(123)
	tc := testContext{manualClock: manual}
	tsc := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	tsc.TestingKnobs.DisableGCQueue = true
	tsc.TestingKnobs.DontRetryPushTxnFailures = true
	tsc.TestingKnobs.DontRecoverIndeterminateCommits = true
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	pusher := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())
	pusher.Priority = enginepb.MaxTxnPriority

	type runFunc func(*roachpb.Transaction, hlc.Timestamp) error
	sendWrappedWithErr := func(h roachpb.Header, args roachpb.Request) error {
		_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), h, args)
		return pErr.GoError()
	}

	intents := []roachpb.Span{{Key: roachpb.Key("a")}}
	inFlightWrites := []roachpb.SequencedWrite{{Key: roachpb.Key("a"), Sequence: 1}}
	otherInFlightWrites := []roachpb.SequencedWrite{{Key: roachpb.Key("b"), Sequence: 2}}

	type verifyFunc func(*roachpb.Transaction, hlc.Timestamp) roachpb.TransactionRecord
	noTxnRecord := verifyFunc(nil)
	txnWithoutChanges := func(txn *roachpb.Transaction, _ hlc.Timestamp) roachpb.TransactionRecord {
		return txn.AsRecord()
	}
	txnWithStatus := func(status roachpb.TransactionStatus) verifyFunc {
		return func(txn *roachpb.Transaction, _ hlc.Timestamp) roachpb.TransactionRecord {
			record := txn.AsRecord()
			record.Status = status
			return record
		}
	}
	txnWithStagingStatusAndInFlightWrites := func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
		record := txnWithStatus(roachpb.STAGING)(txn, now)
		record.InFlightWrites = inFlightWrites
		return record
	}

	testCases := []struct {
		name             string
		setup            runFunc // all three functions are provided the same txn and timestamp
		run              runFunc
		expTxn           verifyFunc
		expError         string // regexp pattern to match on run error, if not empty
		disableTxnAutoGC bool   // disables auto txn record GC
	}{
		{
			name: "heartbeat transaction",
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expTxn: func(txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.LastHeartbeat.Forward(hbTs)
				return record
			},
		},
		{
			name: "end transaction (stage)",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: txnWithStagingStatusAndInFlightWrites,
		},
		{
			name: "end transaction (abort)",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit)",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (abort) without eager gc",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			name: "end transaction (commit) without eager gc",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (timestamp)",
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			// If no transaction record exists, the push (timestamp) request does
			// not create one. It only records its push in the tscache.
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (abort)",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			// If no transaction record exists, the push (abort) request does
			// not create one. It only records its push in the tscache.
			expTxn: noTxnRecord,
		},
		{
			// Should not happen because RecoverTxn requests are only
			// sent after observing a STAGING transaction record.
			name: "recover transaction (implicitly committed)",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError: "txn record synthesized with non-ABORTED status",
			expTxn:   noTxnRecord,
		},
		{
			// Should not happen because RecoverTxn requests are only
			// sent after observing a STAGING transaction record.
			name: "recover transaction (not implicitly committed)",
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError: "txn record synthesized with non-ABORTED status",
			expTxn:   noTxnRecord,
		},
		{
			name: "heartbeat transaction after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now.Add(0, 5))
				return sendWrappedWithErr(hbH, &hb)
			},
			expTxn: func(txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.LastHeartbeat.Forward(hbTs.Add(0, 5))
				return record
			},
		},
		{
			name: "heartbeat transaction with epoch bump after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, now.Add(0, 5))
				return sendWrappedWithErr(hbH, &hb)
			},
			expTxn: func(txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				// NOTE: the HeartbeatTxnRequest with the larger epoch does not
				// update any fields other than LastHeartbeat. This is fine,
				// although it's arguably not optimal.
				//  record.Epoch = txn.Epoch + 1
				//  record.WriteTimestamp.Forward(hbTs)
				record.LastHeartbeat.Forward(hbTs.Add(0, 5))
				return record
			},
		},
		{
			name: "end transaction (stage) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: txnWithStagingStatusAndInFlightWrites,
		},
		{
			name: "end transaction (abort) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (abort) without eager gc after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			name: "end transaction (commit) without eager gc after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (timestamp) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: func(txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.WriteTimestamp.Forward(pushTs)
				record.Priority = pusher.Priority - 1
				return record
			},
		},
		{
			name: "push transaction (abort) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(txn, now)
				record.Priority = pusher.Priority - 1
				return record
			},
		},
		{
			// Staging transaction records can still be heartbeat.
			name: "heartbeat transaction after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expTxn: func(txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(txn, hbTs)
				record.LastHeartbeat.Forward(hbTs)
				return record
			},
		},
		{
			// Should not be possible outside of replays or re-issues of the
			// same request, but also not prevented. If not a re-issue, the
			// second stage will always either bump the commit timestamp or
			// bump the epoch.
			name: "end transaction (stage) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: txnWithStagingStatusAndInFlightWrites,
		},
		{
			// Case of a transaction that refreshed after an unsuccessful
			// implicit commit. If the refresh is successful then the
			// transaction coordinator can attempt the implicit commit again.
			name: "end transaction (stage) with timestamp increase after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.ReadTimestamp.Forward(now)
				clone.WriteTimestamp.Forward(now)
				et, etH := endTxnArgs(clone, true /* commit */)
				// Add different in-flight writes to test whether they are
				// replaced by the second EndTxn request.
				et.InFlightWrites = otherInFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(txn, now)
				record.InFlightWrites = otherInFlightWrites
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Case of a transaction that restarted after an unsuccessful
			// implicit commit. The transaction coordinator can attempt an
			// implicit commit in the next epoch.
			name: "end transaction (stage) with epoch bump after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, true /* commit */)
				// Add different in-flight writes to test whether they are
				// replaced by the second EndTxn request.
				et.InFlightWrites = otherInFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(txn, now)
				record.InFlightWrites = otherInFlightWrites
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Case of a rollback after an unsuccessful implicit commit.
			// The rollback request is not considered an authoritative indication that
			// the transaction is not implicitly committed (i.e. the txn coordinator
			// may have given up on the txn before it heard the result of a commit one
			// way or another), so an IndeterminateCommitError is returned to force
			// transaction recovery to be performed.
			name: "end transaction (abort) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError: "found txn in indeterminate STAGING state",
			expTxn:   txnWithStagingStatusAndInFlightWrites,
		},
		{
			// Case of a rollback after an unsuccessful implicit commit and txn
			// restart. Because the rollback request uses a newer epoch than the
			// staging record, it is considered an authoritative indication that the
			// transaction was never implicitly committed and was later restarted, so
			// the rollback succeeds and the record is immediately aborted.
			name: "end transaction (abort) with epoch bump after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			// Case of making a commit "explicit" after a successful implicit commit.
			name: "end transaction (commit) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			// Case of a rollback after an unsuccessful implicit commit.
			name: "end transaction (abort) with epoch bump, without eager gc after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(txn, now)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
			disableTxnAutoGC: true,
		},
		{
			name: "end transaction (commit) without eager gc after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (timestamp) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expError: "found txn in indeterminate STAGING state",
			expTxn:   txnWithStagingStatusAndInFlightWrites,
		},
		{
			name: "push transaction (abort) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expError: "found txn in indeterminate STAGING state",
			expTxn:   txnWithStagingStatusAndInFlightWrites,
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// re-writing new intents at higher timestamps. The push should not
			// consider the pushee to be staging.
			name: "push transaction (timestamp) after end transaction (stage) with outdated timestamp",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.WriteTimestamp = clone.WriteTimestamp.Add(0, 1)
				pt := pushTxnArgs(pusher, clone, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: func(txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(txn, pushTs)
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = pusher.Priority - 1
				return record
			},
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// re-writing new intents at higher timestamps. The push should not
			// consider the pushee to be staging.
			name: "push transaction (abort) after end transaction (stage) with outdated timestamp",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.WriteTimestamp = clone.WriteTimestamp.Add(0, 1)
				pt := pushTxnArgs(pusher, clone, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: func(txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(txn, pushTs)
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = pusher.Priority - 1
				return record
			},
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// writing new intents in a new epoch. The push should not consider
			// the pushee to be staging.
			name: "push transaction (timestamp) after end transaction (stage) with outdated epoch",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, clone.WriteTimestamp.Add(0, 1))
				pt := pushTxnArgs(pusher, clone, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: func(txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(pushTs)
				record.Priority = pusher.Priority - 1
				return record
			},
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// writing new intents in a new epoch. The push should not consider
			// the pushee to be staging.
			name: "push transaction (abort) after end transaction (stage) with outdated epoch",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, clone.WriteTimestamp.Add(0, 1))
				pt := pushTxnArgs(pusher, clone, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: func(txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(txn, pushTs)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = pusher.Priority - 1
				return record
			},
		},
		{
			name: "heartbeat transaction after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			name: "heartbeat transaction with epoch bump after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				// Restart the transaction at a higher timestamp. This will
				// increment its ReadTimestamp as well. We used to check the GC
				// threshold against this timestamp instead of its minimum
				// timestamp.
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (abort) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: noTxnRecord,
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (commit) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			name: "push transaction (timestamp) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (abort) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "heartbeat transaction after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (abort) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (commit) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			name: "push transaction (timestamp) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (abort) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "heartbeat transaction after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			// The heartbeat request won't throw an error, but also won't update the
			// transaction record. It will simply return the updated transaction state.
			// This is kind of strange, but also doesn't cause any issues.
			expError:         "",
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expError:         "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (abort) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (commit) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError:         "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (timestamp) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (abort) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			name: "heartbeat transaction after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			// The heartbeat request won't throw an error, but also won't update the
			// transaction record. It will simply return the updated transaction state.
			// This is kind of strange, but also doesn't cause any issues.
			expError:         "",
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expError:         "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (abort) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError:         "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (commit) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError:         "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (timestamp) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			name: "push transaction (abort) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			name: "heartbeat transaction after push transaction (timestamp)",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now.Add(0, 5))
				return sendWrappedWithErr(hbH, &hb)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.WriteTimestamp.Forward(now)
				record.LastHeartbeat.Forward(now.Add(0, 5))
				return record
			},
		},
		{
			name: "end transaction (stage) after push transaction (timestamp)",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
			// The end transaction (stage) does not write a transaction record
			// if it hits a serializable retry error.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (abort) after push transaction (timestamp)",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The end transaction (abort) request succeeds and cleans up the
			// transaction record.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit) after push transaction (timestamp)",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
			// The end transaction (commit) does not write a transaction record
			// if it hits a serializable retry error.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (one-phase commit) after push transaction (timestamp)",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
			// The end transaction (commit) does not write a transaction record
			// if it hits a serializable retry error.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (one-phase commit) after push transaction (abort)",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				pt.PushTo = now
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			// 1PC is disabled if the transaction already has a record to ensure
			// that the record is properly cleaned up by the EndTxn request. If
			// we did not disable 1PC then the test would need txnWithoutChanges
			// as the expTxn.
			name: "end transaction (one-phase commit) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				return sendWrappedWithErr(etH, &et)
			},
			expTxn: noTxnRecord,
		},
		{
			// 1PC is disabled if the transaction already has a record to ensure
			// that the record is properly cleaned up by the EndTxn request. If
			// we did not disable 1PC then the test would not throw an error.
			name: "end transaction (one-phase commit required) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				et.Require1PC = true
				return sendWrappedWithErr(etH, &et)
			},
			expError: "could not commit in one phase as requested",
			expTxn:   txnWithoutChanges,
		},
		{
			name: "heartbeat transaction after push transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			name: "heartbeat transaction with epoch bump after push transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				// Restart the transaction at a higher timestamp. This will
				// increment its ReadTimestamp as well. We used to check the GC
				// threshold against this timestamp instead of its minimum
				// timestamp.
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, now)
				return sendWrappedWithErr(hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			name: "end transaction (stage) after push transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			name: "end transaction (abort) after push transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			// The end transaction (abort) request succeeds and cleans up the
			// transaction record.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit) after push transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(pusher, txn, roachpb.PUSH_ABORT)
				return sendWrappedWithErr(roachpb.Header{}, &pt)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError: "found PENDING record for implicitly committed transaction",
			expTxn:   txnWithoutChanges,
		},
		{
			// Typical case of transaction recovery from a STAGING status after
			// a successful implicit commit.
			name: "recover transaction (implicitly committed) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.COMMITTED)(txn, now)
				// RecoverTxn does not synchronously resolve local intents.
				record.LockSpans = intents
				return record
			},
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (stage) with timestamp increase",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.ReadTimestamp.Forward(now)
				clone.WriteTimestamp.Forward(now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError: "timestamp change by implicitly committed transaction",
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(txn, now)
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (stage) with epoch bump",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError: "epoch change by implicitly committed transaction",
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(txn, now)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			// The transaction record was cleaned up, so RecoverTxn can't perform
			// the same assertion that it does in the case without eager gc.
			expTxn: noTxnRecord,
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator made its commit explicit.
			name: "recover transaction (implicitly committed) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: noTxnRecord,
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError:         "found ABORTED record for implicitly committed transaction",
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator made its commit explicit.
			name: "recover transaction (implicitly committed) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
		{
			// Should not be possible.
			name: "recover transaction (not implicitly committed) after heartbeat transaction",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expError: "cannot recover PENDING transaction",
			expTxn:   txnWithoutChanges,
		},
		{
			// Transaction coordinator restarted after failing to perform a
			// implicit commit. Common case.
			name: "recover transaction (not implicitly committed) after heartbeat transaction with epoch bump",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, clone.MinTimestamp)
				return sendWrappedWithErr(hbH, &hb)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Typical case of transaction recovery from a STAGING status after
			// an unsuccessful implicit commit.
			name: "recover transaction (not implicitly committed) after end transaction (stage)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(txn, now)
				// RecoverTxn does not synchronously resolve local intents.
				record.LockSpans = intents
				return record
			},
		},
		{
			// Typical case of transaction recovery from a STAGING status after
			// an unsuccessful implicit commit. Transaction coordinator bumped
			// timestamp in same epoch to attempt implicit commit again. The
			// RecoverTxn request should not modify the transaction record.
			name: "recover transaction (not implicitly committed) after end transaction (stage) with timestamp increase",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.ReadTimestamp.Forward(now)
				clone.WriteTimestamp.Forward(now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				// Unchanged by the RecoverTxn request.
				record := txnWithStagingStatusAndInFlightWrites(txn, now)
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Typical case of transaction recovery from a STAGING status after
			// an unsuccessful implicit commit. Transaction coordinator bumped
			// epoch after a restart and is attempting implicit commit again.
			// The RecoverTxn request should not modify the transaction record.
			name: "recover transaction (not implicitly committed) after end transaction (stage) with epoch bump",
			setup: func(txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: func(txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(txn, now)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator rolled back its transaction record after an
			// unsuccessful implicit commit.
			name: "recover transaction (not implicitly committed) after end transaction (abort)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn: noTxnRecord,
		},
		{
			// Should not be possible.
			name: "recover transaction (not implicitly committed) after end transaction (commit)",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			// The transaction record was cleaned up, so RecoverTxn can't perform
			// the same assertion that it does in the case without eager gc.
			expTxn: noTxnRecord,
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator rolled back its transaction record after an
			// unsuccessful implicit commit.
			name: "recover transaction (not implicitly committed) after end transaction (abort) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn:           txnWithStatus(roachpb.ABORTED),
			disableTxnAutoGC: true,
		},
		{
			// A transaction committed while a recovery process was running
			// concurrently. The recovery process attempted to prevent an intent
			// write after the intent write already succeeded (allowing the
			// transaction to commit) and was resolved. The recovery process
			// thinks that it prevented an intent write because the intent has
			// already been resolved, but later find that the transaction record
			// is committed, so it does nothing.
			name: "recover transaction (not implicitly committed) after end transaction (commit) without eager gc",
			setup: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(etH, &et)
			},
			run: func(txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(roachpb.Header{}, &rt)
			},
			expTxn:           txnWithStatus(roachpb.COMMITTED),
			disableTxnAutoGC: true,
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			defer setTxnAutoGC(!c.disableTxnAutoGC)()

			txn := newTransaction(c.name, roachpb.Key(c.name), 1, tc.Clock())
			manual.Increment(99)
			runTs := tc.Clock().Now()

			if c.setup != nil {
				if err := c.setup(txn, runTs); err != nil {
					t.Fatalf("failed during test setup: %+v", err)
				}
			}

			if err := c.run(txn, runTs); err != nil {
				if len(c.expError) == 0 {
					t.Fatalf("expected no failure, found %q", err.Error())
				}
				if !testutils.IsError(err, regexp.QuoteMeta(c.expError)) {
					t.Fatalf("expected failure %q, found %q", c.expError, err.Error())
				}
			} else {
				if len(c.expError) > 0 {
					t.Fatalf("expected failure %q", c.expError)
				}
			}

			var foundRecord roachpb.TransactionRecord
			if found, err := storage.MVCCGetProto(
				ctx, tc.repl.store.Engine(), keys.TransactionKey(txn.Key, txn.ID),
				hlc.Timestamp{}, &foundRecord, storage.MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			} else if found {
				if c.expTxn == nil {
					t.Fatalf("expected no txn record, found %v", found)
				}
				expRecord := c.expTxn(txn, runTs)
				if !reflect.DeepEqual(expRecord, foundRecord) {
					t.Fatalf("txn record does not match expectations:\n%s",
						strings.Join(pretty.Diff(foundRecord, expRecord), "\n"))
				}
			} else {
				if c.expTxn != nil {
					t.Fatalf("expected txn record, found no txn record")
				}
			}
		})
	}
}

// Test that an EndTxn(commit=false) request that doesn't find its transaction
// record doesn't return an error.
// This is relied upon by the client which liberally sends rollbacks even when
// it's unclear whether the txn record has been written.
func TestRollbackMissingTxnRecordNoError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("bogus key")
	txn := newTransaction("test", key, roachpb.NormalUserPriority, tc.Clock())

	res, pErr := kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		RangeID: tc.repl.RangeID,
		Txn:     txn,
	}, &roachpb.EndTxnRequest{
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
	// a HeartbeatTxn arriving after the rollback.
	_, pErr = kv.SendWrappedWith(ctx, tc.Sender(), roachpb.Header{
		RangeID: tc.repl.RangeID,
		Txn:     txn,
	}, &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Now: tc.Clock().Now(),
	})
	// Note that, as explained in the abort reason comments, the server generates
	// a retryable TransactionAbortedError, but if there's actually a sort of
	// replay at work and a client is still waiting for the error, the error would
	// be transformed into something more ambiguous on the way.
	expErr := "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)"
	if !testutils.IsPError(pErr, regexp.QuoteMeta(expErr)) {
		t.Errorf("expected %s; got %v", expErr, pErr)
	}
}

func TestSplitSnapshotWarningStr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	status := upToDateRaftStatus(replicas(1, 3, 5))
	assert.Equal(t, "", splitSnapshotWarningStr(12, status))

	pr := status.Progress[2]
	pr.State = tracker.StateProbe
	status.Progress[2] = pr

	assert.Equal(
		t,
		"; r12/2 is being probed (may or may not need a Raft snapshot)",
		splitSnapshotWarningStr(12, status),
	)

	pr.State = tracker.StateSnapshot

	assert.Equal(
		t,
		"; r12/2 is being probed (may or may not need a Raft snapshot)",
		splitSnapshotWarningStr(12, status),
	)
}

// TestProposalNotAcknowledgedOrReproposedAfterApplication exercises a case
// where a command is reproposed twice at different MaxLeaseIndex values to
// ultimately fail with an error which cannot be reproposed (say due to a lease
// transfer or change to the gc threshold). This test works to exercise the
// invariant that when a proposal has been reproposed at different MaxLeaseIndex
// values are not additionally reproposed or acknowledged after applying
// locally. The test verfies this condition by asserting that the
// span used to trace the execution of the proposal is not used after the
// proposal has been finished as it would be if the proposal were reproposed
// after applying locally.
//
// The test does the following things:
//
//  * Propose cmd at an initial MaxLeaseIndex.
//  * Refresh that cmd immediately.
//  * Fail the initial command with an injected error which will lead to a
//    reproposal at a higher MaxLeaseIndex.
//  * Simultaneously update the lease sequence number on the replica so all
//    future commands will fail with NotLeaseHolderError.
//  * Enable unconditional refreshes of commands after a raft ready so that
//    higher MaxLeaseIndex commands are refreshed.
//
// This order of events ensures that there will be a committed command which
// experiences the lease mismatch error but does not carry the highest
// MaxLeaseIndex for the proposal. The test attempts to verify that once a
// proposal has been acknowledged it will not be reproposed or acknowledged
// again by asserting that the proposal's context is not reused after it is
// finished by the waiting client.
func TestProposalNotAcknowledgedOrReproposedAfterApplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 71148, "the test is fooling itself")

	// Set the trace infrastructure to log if a span is used after being finished.
	defer enableTraceDebugUseAfterFree()()

	tc := testContext{}
	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.manualClock = hlc.NewManualClock(123)
	cfg := TestStoreConfig(hlc.NewClock(tc.manualClock.UnixNano, time.Nanosecond))
	// Set the RaftMaxCommittedSizePerReady so that only a single raft entry is
	// applied at a time, which makes it easier to line up the timing of reproposals.
	cfg.RaftMaxCommittedSizePerReady = 1
	// Set up tracing.
	tracer := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&cfg.Settings.SV))
	cfg.AmbientCtx.Tracer = tracer

	// Below we set txnID to the value of the transaction we're going to force to
	// be proposed multiple times.
	var txnID uuid.UUID
	// In the TestingProposalFilter we populater cmdID with the id of the proposal
	// which corresponds to txnID.
	var cmdID kvserverbase.CmdIDKey
	// seen is used to detect the first application of our proposal.
	var seen bool
	cfg.TestingKnobs = StoreTestingKnobs{
		// Constant reproposals are the worst case which this test is trying to
		// examine.
		EnableUnconditionalRefreshesInRaftReady: true,
		// Set the TestingProposalFilter in order to know the CmdIDKey for our
		// request by detecting its txnID.
		TestingProposalFilter: func(args kvserverbase.ProposalFilterArgs) *roachpb.Error {
			if args.Req.Header.Txn != nil && args.Req.Header.Txn.ID == txnID {
				cmdID = args.CmdID
			}
			return nil
		},
		// Detect the application of the proposal to repropose it and also
		// invalidate the lease.
		TestingApplyFilter: func(args kvserverbase.ApplyFilterArgs) (retry int, pErr *roachpb.Error) {
			if seen || args.CmdID != cmdID {
				return 0, nil
			}
			seen = true
			tc.repl.mu.Lock()
			defer tc.repl.mu.Unlock()

			// Increase the lease sequence so that future reproposals will fail with
			// NotLeaseHolderError. This mimics the outcome of a leaseholder change
			// slipping in between the application of the first proposal and the
			// reproposals.
			tc.repl.mu.state.Lease.Sequence++
			// This return value will force another retry which will carry a yet
			// higher MaxLeaseIndex. The first reproposal will fail and return to the
			// client but the second (which hasn't been applied due to the
			// MaxCommittedSizePerReady setting) will be reproposed again. This test
			// ensure that it does not reuse the original proposal's context for that
			// reproposal by ensuring that no event is recorded after the original
			// proposal has been finished.
			return int(proposalIllegalLeaseIndex),
				roachpb.NewErrorf("forced error that can be reproposed at a higher index")
		},
	}
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)
	key := roachpb.Key("a")
	st := tc.repl.CurrentLeaseStatus(ctx)
	txn := newTransaction("test", key, roachpb.NormalUserPriority, tc.Clock())
	txnID = txn.ID
	ba := roachpb.BatchRequest{
		Header: roachpb.Header{
			RangeID: tc.repl.RangeID,
			Txn:     txn,
		},
	}
	ba.Timestamp = txn.ReadTimestamp
	ba.Add(&roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes([]byte("val")),
	})

	// Hold the RaftLock to ensure that after evalAndPropose our proposal is in
	// the proposal map. Entries are only removed from that map underneath raft.
	tc.repl.RaftLock()
	_, tok := tc.repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	sp := cfg.AmbientCtx.Tracer.StartSpan("replica send", tracing.WithForceRealSpan())
	tracedCtx := tracing.ContextWithSpan(ctx, sp)
	ch, _, _, pErr := tc.repl.evalAndPropose(tracedCtx, &ba, allSpansGuard(), st, uncertainty.Interval{}, tok)
	if pErr != nil {
		t.Fatal(pErr)
	}
	errCh := make(chan *roachpb.Error)
	go func() {
		res := <-ch
		sp.Finish()
		errCh <- res.Err
	}()

	// While still holding the raftMu, repropose the initial proposal so we know
	// that there will be two instances
	func() {
		tc.repl.mu.Lock()
		defer tc.repl.mu.Unlock()
		if err := tc.repl.mu.proposalBuf.flushLocked(ctx); err != nil {
			t.Fatal(err)
		}
		tc.repl.refreshProposalsLocked(ctx, 0 /* refreshAtDelta */, reasonNewLeaderOrConfigChange)
	}()
	tc.repl.RaftUnlock()

	if pErr = <-errCh; !testutils.IsPError(pErr, "NotLeaseHolder") {
		t.Fatal(pErr)
	}

	// Round trip another proposal through the replica to ensure that previously
	// committed entries have been applied.
	_, pErr = tc.repl.sendWithoutRangeID(ctx, &ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
	log.Flush()

	stopper.Quiesce(ctx)
	entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 1,
		regexp.MustCompile("net/trace"), log.WithFlattenedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) > 0 {
		t.Fatalf("reused span after free: %v", entries)
	}
}

// This test ensures that pushes due to closed timestamps are properly recorded
// into the associated telemetry counter.
func TestReplicaTelemetryCounterForPushesDueToClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	keyA := append(keys.SystemSQLCodec.TablePrefix(math.MaxUint32), 'a')
	keyAA := append(keyA[:len(keyA):len(keyA)], 'a')
	rKeyA, err := keys.Addr(keyA)
	putReq := func(key roachpb.Key) *roachpb.PutRequest {
		r := putArgs(key, []byte("foo"))
		return &r
	}
	require.NoError(t, err)
	type testCase struct {
		name string
		f    func(*testing.T, *Replica)
	}
	runTestCase := func(c testCase) {
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		_ = telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)
		tc.Start(ctx, t, stopper)
		r := tc.store.LookupReplica(rKeyA)
		t.Run(c.name, func(t *testing.T) {
			c.f(t, r)
		})
	}
	for _, c := range []testCase{
		{
			// Test the case where no bump occurs.
			name: "no bump", f: func(t *testing.T, r *Replica) {
				ba := roachpb.BatchRequest{}
				ba.Add(putReq(keyA))
				minReadTS := r.store.Clock().Now()
				ba.Timestamp = minReadTS.Next()
				require.False(t, r.applyTimestampCache(ctx, &ba, minReadTS))
				require.Equal(t, int32(0), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
		{
			// Test the case where the bump occurs due to minReadTS.
			name: "bump due to minTS", f: func(t *testing.T, r *Replica) {
				ba := roachpb.BatchRequest{}
				ba.Add(putReq(keyA))
				ba.Timestamp = r.store.Clock().Now()
				minReadTS := ba.Timestamp.Next()
				require.True(t, r.applyTimestampCache(ctx, &ba, minReadTS))
				require.Equal(t, int32(1), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
		{
			// Test the case where we bump due to the read ts cache rather than the minReadTS.
			name: "bump due to later read ts cache entry", f: func(t *testing.T, r *Replica) {
				ba := roachpb.BatchRequest{}
				ba.Add(putReq(keyA))
				ba.Timestamp = r.store.Clock().Now()
				minReadTS := ba.Timestamp.Next()
				r.store.tsCache.Add(keyA, keyA, minReadTS.Next(), uuid.MakeV4())
				require.True(t, r.applyTimestampCache(ctx, &ba, minReadTS))
				require.Equal(t, int32(0), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
		{
			// Test the case where we do initially bump due to the minReadTS but then
			// bump again to a higher ts due to the read ts cache.
			name: "higher bump due to read ts cache entry", f: func(t *testing.T, r *Replica) {
				ba := roachpb.BatchRequest{}
				ba.Add(putReq(keyA))
				ba.Add(putReq(keyAA))
				ba.Timestamp = r.store.Clock().Now()
				minReadTS := ba.Timestamp.Next()
				t.Log(ba.Timestamp, minReadTS, minReadTS.Next())
				r.store.tsCache.Add(keyAA, keyAA, minReadTS.Next(), uuid.MakeV4())
				require.True(t, r.applyTimestampCache(ctx, &ba, minReadTS))
				require.Equal(t, int32(0), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
	} {
		runTestCase(c)
	}
}

func TestReplicateQueueProcessOne(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	errBoom := errors.New("boom")
	tc.repl.mu.Lock()
	tc.repl.mu.destroyStatus.Set(errBoom, destroyReasonMergePending)
	tc.repl.mu.Unlock()

	requeue, err := tc.store.replicateQueue.processOneChange(
		ctx,
		tc.repl,
		func(ctx context.Context, repl *Replica) bool { return false },
		false, /* scatter */
		true,  /* dryRun */
	)
	require.Equal(t, errBoom, err)
	require.False(t, requeue)
}

// TestContainsEstimatesClamp tests the massaging of ContainsEstimates
// before proposing a raft command. It should always be >1 and an even number.
// See the comment on ContainEstimates to understand why.
func TestContainsEstimatesClampProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	someRequestToProposal := func(tc *testContext, ctx context.Context) *ProposalData {
		cmdIDKey := kvserverbase.CmdIDKey("some-cmdid-key")
		var ba roachpb.BatchRequest
		ba.Timestamp = tc.Clock().Now()
		req := putArgs(roachpb.Key("some-key"), []byte("some-value"))
		ba.Add(&req)
		proposal, err := tc.repl.requestToProposal(ctx, cmdIDKey, &ba, tc.repl.CurrentLeaseStatus(ctx), uncertainty.Interval{}, allSpansGuard())
		if err != nil {
			t.Error(err)
		}
		return proposal
	}

	// Mock Put command so that it always adds 2 to ContainsEstimates. Could be
	// any number >1.
	defer setMockPutWithEstimates(2)()

	t.Run("VersionContainsEstimatesCounter", func(t *testing.T) {
		ctx := context.Background()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		var tc testContext
		tc.Start(ctx, t, stopper)

		proposal := someRequestToProposal(&tc, ctx)

		if proposal.command.ReplicatedEvalResult.Delta.ContainsEstimates != 4 {
			t.Error("Expected ContainsEstimates to be 4, was", proposal.command.ReplicatedEvalResult.Delta.ContainsEstimates)
		}
	})

}

// setMockPutWithEstimates mocks the Put command (could be any) to simulate a command
// that touches ContainsEstimates, in order to test request proposal behavior.
func setMockPutWithEstimates(containsEstimatesDelta int64) (undo func()) {
	prev, _ := batcheval.LookupCommand(roachpb.Put)

	mockPut := func(
		ctx context.Context, readWriter storage.ReadWriter, cArgs batcheval.CommandArgs, _ roachpb.Response,
	) (result.Result, error) {
		args := cArgs.Args.(*roachpb.PutRequest)
		ms := cArgs.Stats
		ms.ContainsEstimates += containsEstimatesDelta
		ts := cArgs.Header.Timestamp
		return result.Result{}, storage.MVCCBlindPut(ctx, readWriter, ms, args.Key, ts, args.Value, cArgs.Header.Txn)
	}

	batcheval.UnregisterCommand(roachpb.Put)
	batcheval.RegisterReadWriteCommand(roachpb.Put, batcheval.DefaultDeclareIsolatedKeys, mockPut)
	return func() {
		batcheval.UnregisterCommand(roachpb.Put)
		batcheval.RegisterReadWriteCommand(roachpb.Put, prev.DeclareKeys, prev.EvalRW)
	}
}

func TestPrepareChangeReplicasTrigger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	type typOp struct {
		roachpb.ReplicaType
		internalChangeType // 0 for noop
	}

	type testCase struct {
		desc       *roachpb.RangeDescriptor
		chgs       internalReplicationChanges
		expTrigger string
	}

	const noop = internalChangeType(0)
	const none = roachpb.ReplicaType(-1)

	mk := func(expTrigger string, typs ...typOp) testCase {
		chgs := make([]internalReplicationChange, 0, len(typs))
		rDescs := make([]roachpb.ReplicaDescriptor, 0, len(typs))
		for i, typ := range typs {
			typ := typ // local copy - we take addr below
			rDesc := roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(i + 1),
				NodeID:    roachpb.NodeID(100 * (1 + i)),
				StoreID:   roachpb.StoreID(100 * (1 + i)),
				Type:      &(typ.ReplicaType),
			}
			if typ.ReplicaType != none {
				rDescs = append(rDescs, rDesc)
			}
			if typ.internalChangeType != noop {
				chgs = append(chgs, internalReplicationChange{
					target: roachpb.ReplicationTarget{NodeID: rDesc.NodeID, StoreID: rDesc.StoreID},
					typ:    typ.internalChangeType,
				})
			}
		}
		desc := roachpb.NewRangeDescriptor(roachpb.RangeID(10), roachpb.RKeyMin, roachpb.RKeyMax, roachpb.MakeReplicaSet(rDescs))
		return testCase{
			desc:       desc,
			chgs:       chgs,
			expTrigger: expTrigger,
		}
	}

	tcs := []testCase{
		// Simple addition of learner.
		mk(
			"SIMPLE(l2) [(n200,s200):2LEARNER]: after=[(n100,s100):1 (n200,s200):2LEARNER] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{none, internalChangeTypeAddLearner},
		),
		// Simple addition of voter (necessarily via learner).
		mk(
			"SIMPLE(v2) [(n200,s200):2]: after=[(n100,s100):1 (n200,s200):2] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
		),
		// Simple removal of voter.
		mk(
			"SIMPLE(r2) [(n200,s200):2]: after=[(n100,s100):1] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemove},
		),
		// Simple removal of learner.
		mk(
			"SIMPLE(r2) [(n200,s200):2LEARNER]: after=[(n100,s100):1] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.LEARNER, internalChangeTypeRemove},
		),

		// All other cases below need to go through joint quorums (though some
		// of them only due to limitations in etcd/raft).

		// Addition of learner and removal of voter at same time.
		mk(
			"ENTER_JOINT(r2 l3) [(n200,s200):3LEARNER], [(n300,s300):2VOTER_OUTGOING]: after=[(n100,s100):1 (n300,s300):2VOTER_OUTGOING (n200,s200):3LEARNER] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{none, internalChangeTypeAddLearner},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemove},
		),

		// Promotion of two voters.
		mk(
			"ENTER_JOINT(v2 v3) [(n200,s200):2VOTER_INCOMING (n300,s300):3VOTER_INCOMING]: after=[(n100,s100):1 (n200,s200):2VOTER_INCOMING (n300,s300):3VOTER_INCOMING] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
		),

		// Removal of two voters.
		mk(
			"ENTER_JOINT(r2 r3) [(n200,s200):2VOTER_OUTGOING (n300,s300):3VOTER_OUTGOING]: after=[(n100,s100):1 (n200,s200):2VOTER_OUTGOING (n300,s300):3VOTER_OUTGOING] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemove},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemove},
		),

		// Demoting two voters.
		mk(
			"ENTER_JOINT(r2 l2 r3 l3) [(n200,s200):2VOTER_DEMOTING_LEARNER (n300,s300):3VOTER_DEMOTING_LEARNER]: after=[(n100,s100):1 (n200,s200):2VOTER_DEMOTING_LEARNER (n300,s300):3VOTER_DEMOTING_LEARNER] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_FULL, internalChangeTypeDemoteVoterToLearner},
			typOp{roachpb.VOTER_FULL, internalChangeTypeDemoteVoterToLearner},
		),
		// Leave joint config entered via demotion.
		mk(
			"LEAVE_JOINT: after=[(n100,s100):1 (n200,s200):2LEARNER (n300,s300):3LEARNER] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_DEMOTING_LEARNER, noop},
			typOp{roachpb.VOTER_DEMOTING_LEARNER, noop},
		),
	}

	for _, tc := range tcs {
		t.Run("", func(t *testing.T) {
			trigger, err := prepareChangeReplicasTrigger(
				ctx,
				tc.desc,
				tc.chgs,
				nil, /* testingForceJointConfig */
			)
			require.NoError(t, err)
			assert.Equal(t, tc.expTrigger, trigger.String())
		})
	}
}

func enableTraceDebugUseAfterFree() (restore func()) {
	prev := trace.DebugUseAfterFinish
	trace.DebugUseAfterFinish = true
	return func() { trace.DebugUseAfterFinish = prev }
}

// Test that, depending on the request's ClientRangeInfo, descriptor and lease
// updates are returned.
func TestRangeInfoReturned(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	var tc testContext
	tc.Start(ctx, t, stopper)

	key := roachpb.Key("a")
	gArgs := getArgs(key)

	ri := tc.repl.GetRangeInfo(ctx)
	require.False(t, ri.Lease.Empty())
	require.Equal(t, roachpb.LAG_BY_CLUSTER_SETTING, ri.ClosedTimestampPolicy)
	staleDescGen := ri.Desc.Generation - 1
	staleLeaseSeq := ri.Lease.Sequence - 1
	wrongCTPolicy := roachpb.LEAD_FOR_GLOBAL_READS

	for _, test := range []struct {
		req roachpb.ClientRangeInfo
		exp *roachpb.RangeInfo
	}{
		{
			// Empty client info. This case shouldn't happen.
			req: roachpb.ClientRangeInfo{},
			exp: &ri,
		},
		{
			// Correct descriptor, missing lease, correct closedts policy.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, stale lease, correct closedts policy.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				LeaseSequence:         staleLeaseSeq,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, correct lease, incorrect closedts policy.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				LeaseSequence:         ri.Lease.Sequence,
				ClosedTimestampPolicy: wrongCTPolicy,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, correct lease, correct closedts policy.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				LeaseSequence:         ri.Lease.Sequence,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: nil, // No update should be returned.
		},
		{
			// Stale descriptor, no lease, correct closedts policy.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  staleDescGen,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
		{
			// Stale descriptor, stale lease, incorrect closedts policy.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  staleDescGen,
				LeaseSequence:         staleLeaseSeq,
				ClosedTimestampPolicy: wrongCTPolicy,
			},
			exp: &ri,
		},
		{
			// Stale desc, good lease, correct closedts policy. This case
			// shouldn't happen.
			req: roachpb.ClientRangeInfo{
				DescriptorGeneration:  staleDescGen,
				LeaseSequence:         staleLeaseSeq,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
	} {
		t.Run("", func(t *testing.T) {
			ba := roachpb.BatchRequest{}
			ba.Add(&gArgs)
			ba.Header.ClientRangeInfo = test.req
			br, pErr := tc.Sender().Send(ctx, ba)
			require.Nil(t, pErr)
			if test.exp == nil {
				require.Empty(t, br.RangeInfos)
			} else {
				require.Len(t, br.RangeInfos, 1)
				require.Equal(t, br.RangeInfos[0], *test.exp)
			}
		})
	}
}

func tenantsWithMetrics(m *StoreMetrics) map[roachpb.TenantID]struct{} {
	metricsTenants := map[roachpb.TenantID]struct{}{}
	m.tenants.Range(func(tenID int64, _ unsafe.Pointer) bool {
		metricsTenants[roachpb.MakeTenantID(uint64(tenID))] = struct{}{}
		return true // more
	})
	return metricsTenants
}

func isSystemTenantRepl(t *testing.T, repl *Replica) {
	t.Log(repl)
	// Even system tenant has a metrics ref.
	require.NotNil(t, repl.tenantMetricsRef)
	// System tenant has no rate limiter.
	require.Nil(t, repl.tenantLimiter)
	tenID, ok := repl.TenantID()
	require.True(t, ok) // repl is initialized
	require.Equal(t, roachpb.SystemTenantID, tenID)
	// Even though the system tenant is not populated with a rate limiter and
	// there is no refcounting for the system tenant, there is a system rate
	// limiter (which would exist and be used for some requests even if the store
	// had no replica for the system tenant).
	require.NotNil(t, repl.store.tenantRateLimiters.GetTenant(context.Background(), tenID, nil /* closer */))
}

// TestStoreTenantMetricsAndRateLimiterRefcount verifies that the refcounting
// for replicas owned by tenants works: a tenant metrics or tenant rate limiter
// reference is only retained as long as a replica owned by that tenant exists
// on the store. This does not exhaustively test all of the ways in which a
// Replica can be destroyed (but uses a Merge). There are always-on assertions
// (see calls to tenantMetricsRef.assert) that fire on use-after-release, but
// if we leaked references on the non-merge code paths, this would not be
// obviously caught in testing.
func TestStoreTenantMetricsAndRateLimiterRefcount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc := testContext{}
	tc.Start(ctx, t, stopper)

	// Initially all replicas are system tenant replicas.
	tc.store.VisitReplicas(func(repl *Replica) (wantMore bool) {
		isSystemTenantRepl(t, repl)
		return true // wantMore
	})

	// The metrics only know the system tenant.
	require.Equal(t,
		map[roachpb.TenantID]struct{}{roachpb.SystemTenantID: {}},
		tenantsWithMetrics(tc.store.metrics),
	)

	// A range for tenant 123 appears via a split.
	ten123 := roachpb.MakeTenantID(123)
	splitKey := keys.MustAddr(keys.MakeSQLCodec(ten123).TenantPrefix())
	leftRepl := tc.store.LookupReplica(splitKey)
	require.NotNil(t, leftRepl)
	splitTestRange(tc.store, splitKey, t)
	tenRepl := tc.store.LookupReplica(splitKey)
	require.NotNil(t, tenRepl)
	require.NotNil(t, tenRepl.tenantMetricsRef)
	require.NotNil(t, tenRepl.tenantLimiter)

	// The store metrics correspondingly track the system tenant and tenant 123
	// and the rate limiter registry has an entry for it as well.
	require.Equal(t,
		map[roachpb.TenantID]struct{}{
			roachpb.SystemTenantID: {},
			ten123:                 {},
		},
		tenantsWithMetrics(tc.store.metrics),
	)
	tenLimiter := tenRepl.tenantLimiter
	secondLimiter := tenRepl.store.tenantRateLimiters.GetTenant(context.Background(), ten123, nil /* closer */)
	tenRepl.store.tenantRateLimiters.Release(secondLimiter)
	require.Equal(t,
		tenLimiter,
		secondLimiter,
	)

	// The sole range owned by tenant 123 gets merged away again.
	_, pErr := leftRepl.AdminMerge(context.Background(), roachpb.AdminMergeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: leftRepl.Desc().StartKey.AsRawKey(),
		},
	}, "testing")
	require.Nil(t, pErr)

	// The store metrics no longer track tenant 123.
	require.Equal(t,
		map[roachpb.TenantID]struct{}{
			roachpb.SystemTenantID: {},
		},
		tenantsWithMetrics(tc.store.metrics),
	)
	// The rate limiter is similarly gone. We can't test this directly
	// but we can check that the limiter we had has been released, which
	// we can tell from a panic with an assertion failure if we release
	// again.
	func() {
		defer func() {
			r := recover()
			err, ok := r.(error)
			if !ok || !errors.HasAssertionFailure(err) {
				t.Errorf("unxpected recover() after double-Release: %+v", r)
			}
		}()
		tc.store.tenantRateLimiters.Release(tenLimiter)
	}()
}
