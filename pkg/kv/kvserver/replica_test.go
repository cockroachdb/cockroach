// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/intentresolver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tenantrate"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			LockSpans:  lockspanset.New(),
		},
	}
}

// leaseExpiry the time after which any range lease the
// Replica may hold is expired. It is more precise than LeaseExpiration
// in that it returns the minimal duration necessary.
func leaseExpiry(repl *Replica) time.Time {
	l, _ := repl.GetLease()
	if l.Type() != roachpb.LeaseExpiration {
		panic("leaseExpiry only valid for expiration-based leases")
	}
	return l.Expiration.GoTime().Add(1)
}

// Create a Raft status that shows everyone fully up to date.
func upToDateRaftStatus(repls []roachpb.ReplicaDescriptor) *raft.Status {
	prs := make(map[raftpb.PeerID]tracker.Progress)
	for _, repl := range repls {
		prs[raftpb.PeerID(repl.ReplicaID)] = tracker.Progress{
			State: tracker.StateReplicate,
			Match: 100,
		}
	}
	return &raft.Status{
		BasicStatus: raft.BasicStatus{
			HardState: raftpb.HardState{Commit: 100, Lead: 1},
			SoftState: raft.SoftState{RaftState: raftpb.StateLeader},
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
	manualClock *timeutil.ManualTime
}

func (tc *testContext) Clock() *hlc.Clock {
	return tc.store.cfg.Clock
}

// Start initializes the test context with a single range covering the
// entire keyspace.
func (tc *testContext) Start(ctx context.Context, t testing.TB, stopper *stop.Stopper) {
	tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123))
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	// testContext tests like to move the manual clock around and assume that they can write at past
	// timestamps.
	cfg.TestingKnobs.DontCloseTimestamps = true
	cfg.TestingKnobs.DisableMergeWaitForReplicasInit = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)
}

// StartWithStoreConfig initializes the test context with a single
// range covering the entire keyspace.
func (tc *testContext) StartWithStoreConfig(
	ctx context.Context, t testing.TB, stopper *stop.Stopper, cfg StoreConfig,
) {
	tc.StartWithStoreConfigAndVersion(ctx, t, stopper, cfg, cfg.Settings.Version.LatestVersion())
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
	// TODO(sep-raft-log): may need to update our various test harnesses to
	// support two engines, do metamorphic stuff, etc.
	tc.engine = store.TODOEngine()
	tc.store = store
}

func (tc *testContext) Sender() kv.Sender {
	return kv.Wrap(tc.repl, func(ba *kvpb.BatchRequest) *kvpb.BatchRequest {
		if ba.RangeID == 0 {
			ba.RangeID = 1
		}
		if ba.Timestamp.IsEmpty() {
			if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
				tc.Fatal(err)
			}
		}
		tc.Clock().Update(ba.Now)
		return ba
	})
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it
func (tc *testContext) SendWrappedWith(
	h kvpb.Header, args kvpb.Request,
) (kvpb.Response, *kvpb.Error) {
	return kv.SendWrappedWith(context.Background(), tc.Sender(), h, args)
}

// SendWrapped is identical to SendWrappedWith with a zero header.
func (tc *testContext) SendWrapped(args kvpb.Request) (kvpb.Response, *kvpb.Error) {
	return tc.SendWrappedWith(kvpb.Header{}, args)
}

// addBogusReplicaToRangeDesc modifies the range descriptor to include an additional
// replica. This is useful for tests that want to pretend they're transferring
// the range lease away, as the lease can only be obtained by Replicas which are
// part of the range descriptor.
// This is a workaround, but it's sufficient for the purposes of several tests.
func (tc *testContext) addBogusReplicaToRangeDesc(
	ctx context.Context,
) (roachpb.ReplicaDescriptor, error) {
	oldDesc := *tc.repl.Desc()
	newID := oldDesc.NextReplicaID
	newReplica := roachpb.ReplicaDescriptor{
		NodeID:    roachpb.NodeID(newID),
		StoreID:   roachpb.StoreID(newID),
		ReplicaID: newID,
	}
	newDesc := oldDesc
	newDesc.InternalReplicas = append(newDesc.InternalReplicas, newReplica)
	newDesc.NextReplicaID++
	newDesc.IncrementGeneration()

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
		Header: kvpb.Header{Timestamp: tc.Clock().Now()},
	}
	descKey := keys.RangeDescriptorKey(oldDesc.StartKey)
	if err := updateRangeDescriptor(&ba, descKey, dbDescKV.Value.TagAndDataBytes(), &newDesc); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}
	if err := tc.store.DB().Run(ctx, &ba); err != nil {
		return roachpb.ReplicaDescriptor{}, err
	}

	tc.repl.raftMu.Lock()
	tc.repl.setDescRaftMuLocked(ctx, &newDesc)
	tc.repl.mu.RLock()
	tc.repl.assertStateRaftMuLockedReplicaMuRLocked(ctx, tc.engine)
	tc.repl.mu.RUnlock()
	tc.repl.raftMu.Unlock()
	return newReplica, nil
}

func newTransaction(
	name string, baseKey roachpb.Key, userPriority roachpb.UserPriority, clock *hlc.Clock,
) *roachpb.Transaction {
	isoLevel := isolation.Serializable
	var offset int64
	var now hlc.Timestamp
	if clock != nil {
		offset = clock.MaxOffset().Nanoseconds()
		now = clock.Now()
	}
	txn := roachpb.MakeTransaction(name, baseKey, isoLevel, userPriority, now, offset, 0, 0, false /* omitInRangefeeds */)
	return &txn
}

// assignSeqNumsForReqs sets sequence numbers for each of the provided requests
// given a transaction proto. It also updates the proto to reflect the incremented
// sequence number.
func assignSeqNumsForReqs(txn *roachpb.Transaction, reqs ...kvpb.Request) {
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
	withSeq := func(req kvpb.Request, seq enginepb.TxnSeq) kvpb.Request {
		h := req.Header()
		h.Sequence = seq
		req.SetHeader(h)
		return req
	}
	makeReqs := func(reqs ...kvpb.Request) []kvpb.RequestUnion {
		ru := make([]kvpb.RequestUnion, len(reqs))
		for i, r := range reqs {
			ru[i].MustSetInner(r)
		}
		return ru
	}

	noReqs := makeReqs()
	getReq := makeReqs(withSeq(&kvpb.GetRequest{}, 0))
	putReq := makeReqs(withSeq(&kvpb.PutRequest{}, 1))
	etReq := makeReqs(withSeq(&kvpb.EndTxnRequest{Commit: true}, 1))
	txnReqs := makeReqs(
		withSeq(&kvpb.PutRequest{}, 1),
		withSeq(&kvpb.EndTxnRequest{Commit: true}, 2),
	)
	txnReqsRequire1PC := makeReqs(
		withSeq(&kvpb.PutRequest{}, 1),
		withSeq(&kvpb.EndTxnRequest{Commit: true, Require1PC: true}, 2),
	)

	testCases := []struct {
		ru           []kvpb.RequestUnion
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

	clock := hlc.NewClockForTesting(nil)
	for i, c := range testCases {
		t.Run(
			fmt.Sprintf("%d:isNonTxn:%t,canForwardTS:%t,isRestarted:%t,isWTO:%t,isTSOff:%t",
				i, c.isNonTxn, c.canForwardTS, c.isRestarted, c.isWTO, c.isTSOff),
			func(t *testing.T) {
				ba := &kvpb.BatchRequest{Requests: c.ru}
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
				ba, _ = maybeBumpReadTimestampToWriteTimestamp(ctx, ba, allSpansGuard())

				if is1PC := isOnePhaseCommit(ba); is1PC != c.exp1PC {
					t.Errorf("expected 1pc=%t; got %t", c.exp1PC, is1PC)
				}
			})
	}
}

func TestReplicaStringAndSafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test really only needs a hollow shell of a Store and Replica.
	s := &Store{}
	s.Ident = &roachpb.StoreIdent{NodeID: 1, StoreID: 2}
	r := &Replica{}
	r.store = s
	r.rangeStr.store(4, &roachpb.RangeDescriptor{
		RangeID:  3,
		StartKey: roachpb.RKey("a"),
		EndKey:   roachpb.RKey("b"),
	})

	// String.
	assert.Equal(t, "[n1,s2,r3/4:{a-b}]", r.String())
	// Redactable string.
	assert.EqualValues(t, "[n1,s2,r3/4:‹{a-b}›]", redact.Sprint(r))
	// Redacted string.
	assert.EqualValues(t, "[n1,s2,r3/4:‹×›]", redact.Sprint(r).Redact())
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
	r.shMu.state.Desc = desc
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
	ba := &kvpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	st := r.CurrentLeaseStatus(ctx)
	lease := *l
	prevLease := st.Lease
	lease.Sequence = prevLease.Sequence + 1
	leaseReq := &kvpb.RequestLeaseRequest{
		Lease:     lease,
		PrevLease: prevLease,
	}
	ba.Add(leaseReq)
	_, tok := r.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, _, pErr := r.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
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

	if _, err := tc.SendWrappedWith(kvpb.Header{
		Txn:             txn,
		ReadConsistency: kvpb.READ_UNCOMMITTED,
	}, &gArgs); err == nil {
		t.Errorf("expected error on read uncommitted read within a txn")
	}

	if _, err := tc.SendWrappedWith(kvpb.Header{
		Txn:             txn,
		ReadConsistency: kvpb.INCONSISTENT,
	}, &gArgs); err == nil {
		t.Errorf("expected error on inconsistent read within a txn")
	}

	// Lose the lease and verify CONSISTENT reads receive NotLeaseHolderError
	// and INCONSISTENT reads work as expected.
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	start := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: start,
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	// Send without Txn.
	_, pErr := tc.SendWrappedWith(kvpb.Header{
		ReadConsistency: kvpb.CONSISTENT,
	}, &gArgs)
	if _, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError); !ok {
		t.Errorf("expected not lease holder error; got %s", pErr)
	}

	_, pErr = tc.SendWrappedWith(kvpb.Header{
		ReadConsistency: kvpb.READ_UNCOMMITTED,
	}, &gArgs)
	if _, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError); !ok {
		t.Errorf("expected not lease holder error; got %s", pErr)
	}

	if _, pErr := tc.SendWrappedWith(kvpb.Header{
		ReadConsistency: kvpb.INCONSISTENT,
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
		manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
		clock := hlc.NewClockForTesting(manual)
		tc := testContext{manualClock: manual}
		tsc := TestStoreConfig(clock)
		var leaseAcquisitionTrap atomic.Value
		tsc.TestingKnobs.DisableAutomaticLeaseRenewal = true
		tsc.TestingKnobs.LeaseRequestEvent = func(ts hlc.Timestamp, _ roachpb.StoreID, _ roachpb.RangeID) *kvpb.Error {
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
			func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
				if _, ok := filterArgs.Req.(*kvpb.TransferLeaseRequest); ok {
					// Notify the test that the transfer has been trapped.
					transferSem <- struct{}{}
					// Wait for the test to unblock the transfer.
					<-transferSem
					if !transferSucceeds {
						// Return an error, so that the pendingLeaseRequest
						// considers the transfer failed.
						return kvpb.NewErrorf("injected transfer error")
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
		tc.manualClock.Advance(500 * time.Nanosecond)

		// Initiate a transfer (async) and wait for it to be blocked.
		transferResChan := make(chan error)
		go func() {
			// We're transferring the lease to a bogus replica, so disable protection
			// which would otherwise notice this and reject the lease transfer.
			err := tc.repl.AdminTransferLease(ctx, secondReplica.StoreID, true /* bypassSafetyChecks */)
			if !testutils.IsError(err, "injected") {
				transferResChan <- err
			} else {
				transferResChan <- nil
			}
		}()
		<-transferSem
		// Check that a transfer is indeed on-going.
		tc.repl.mu.Lock()
		pending := tc.repl.mu.pendingLeaseRequest.TransferInProgress()
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
		expectedLeaseStartTS := tc.manualClock.Now().UnixNano()
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
			pending := tc.repl.mu.pendingLeaseRequest.TransferInProgress()
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
			var lErr *kvpb.NotLeaseHolderError
			require.True(t, errors.As(err, &lErr))
			require.Equal(t, secondReplica.StoreID, lErr.Lease.Replica.StoreID)
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

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	pArgs := putArgs(roachpb.Key("a"), []byte("asd"))

	// Lose the lease.
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	start := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: start,
		Start:      start,
		Expiration: start.ToTimestamp().Add(10, 0).Clone(),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	_, pErr := tc.SendWrappedWith(kvpb.Header{
		Timestamp: tc.Clock().Now().Add(-100, 0),
	}, &pArgs)
	if _, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError); !ok {
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
	tc.repl.mu.RLock()
	fr := kvserverbase.CheckForcedErr(
		ctx, raftlog.MakeCmdIDKey(), &raftCmd, false, /* isLocal */
		&tc.repl.shMu.state,
	)
	pErr := fr.ForcedError
	tc.repl.mu.RUnlock()
	if _, isErr := pErr.GetDetail().(*kvpb.LeaseRejectedError); !isErr {
		t.Fatal(pErr)
	} else if !testutils.IsPError(pErr, "replica not part of range") {
		t.Fatal(pErr)
	}
}

// TestReplicaRangeMismatchRedirect tests two behaviors that should occur.
//   - Following a Range split, the client may send BatchRequests based on stale
//     cache data targeting the wrong range. Internally this triggers a
//     RangeKeyMismatchError, but in the cases where the RHS of the range is still
//     present on the local store, we opportunistically retry server-side by
//     re-routing the request to the right range. No error is bubbled up to the
//     client.
//   - This test also ensures that after a successful server-side retry attempt we
//     bubble up the most up-to-date RangeInfos for the client to update its range
//     cache.
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
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{
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
	applyFilter := func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		if pErr := filterErr.Load(); pErr != nil {
			return 0, pErr.(*kvpb.Error)
		}
		return 0, nil
	}

	tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123))
	tsc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	tsc.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tsc.TestingKnobs.TestingApplyCalledTwiceFilter = applyFilter
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Test that leases with invalid times are rejected.
	// Start leases at a point that avoids overlapping with the existing lease.
	leaseDuration := tc.store.cfg.RangeLeaseDuration
	start := hlc.ClockTimestamp{WallTime: (time.Second + leaseDuration).Nanoseconds(), Logical: 0}
	for _, lease := range []roachpb.Lease{
		{Start: start, Expiration: &hlc.Timestamp{}},
	} {
		if _, err := batcheval.RequestLease(ctx, tc.store.TODOEngine(),
			batcheval.CommandArgs{
				EvalCtx: NewReplicaEvalContext(
					ctx, tc.repl, allSpans(), false, /* requiresClosedTSOlderThanStorageSnap */
					kvpb.AdmissionHeader{},
				),
				Args: &kvpb.RequestLeaseRequest{
					Lease:     lease,
					PrevLease: tc.repl.CurrentLeaseStatus(ctx).Lease,
				},
			}, &kvpb.RequestLeaseResponse{}); !testutils.IsError(err, "replica not found") {
			t.Fatalf("unexpected error: %+v", err)
		}
	}

	if !tc.repl.OwnsValidLease(ctx, tc.Clock().NowAsClockTimestamp()) {
		t.Errorf("expected lease on range start")
	}
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: now,
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
		if lErr, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError); !ok || lErr == nil {
			t.Fatalf("wanted NotLeaseHolderError, got %s", pErr)
		}
	}
	// Advance clock past expiration and verify that another has
	// range lease will not be true.
	tc.manualClock.Advance(21) // 21ns have passed
	if tc.repl.OwnsValidLease(ctx, tc.Clock().NowAsClockTimestamp()) {
		t.Errorf("expected another replica to have expired lease")
	}

	// Verify that command returns NotLeaseHolderError when lease is rejected.
	filterErr.Store(kvpb.NewError(&kvpb.LeaseRejectedError{Message: "replica not found"}))

	{
		_, err := tc.repl.redirectOnOrAcquireLease(ctx)
		if _, ok := err.GetDetail().(*kvpb.NotLeaseHolderError); !ok {
			t.Fatalf("expected %T, got %s", &kvpb.NotLeaseHolderError{}, err)
		}
	}
}

func TestReplicaNotLeaseHolderError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(ctx)
	if err != nil {
		t.Fatal(err)
	}

	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	if err := sendLeaseRequest(tc.repl, &roachpb.Lease{
		ProposedTS: now,
		Start:      now,
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
		Replica:    secondReplica,
	}); err != nil {
		t.Fatal(err)
	}

	header := kvpb.RequestHeader{
		Key: roachpb.Key("a"),
	}
	testCases := []kvpb.Request{
		// Admin split covers admin commands.
		&kvpb.AdminSplitRequest{
			RequestHeader: header,
			SplitKey:      roachpb.Key("a"),
		},
		// Get covers read-only commands.
		&kvpb.GetRequest{
			RequestHeader: header,
		},
		// Put covers read-write commands.
		&kvpb.PutRequest{
			RequestHeader: header,
			Value:         roachpb.MakeValueFromString("value"),
		},
	}

	for i, test := range testCases {
		_, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: now.ToTimestamp()}, test)

		if _, ok := pErr.GetDetail().(*kvpb.NotLeaseHolderError); !ok {
			t.Errorf("%d: expected not lease holder error: %s", i, pErr)
		}
	}
}

// TestReplicaLeaseCounters verifies leaseRequest metrics counters are updated
// correctly after a lease request.
func TestReplicaLeaseCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer EnableLeaseHistoryForTesting(100)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var tc testContext
	cfg := TestStoreConfig(nil)
	nlActive, nlRenewal := cfg.NodeLivenessDurations()
	cache := liveness.NewCache(
		gossip.NewTest(roachpb.NodeID(1), stopper, metric.NewRegistry()),
		cfg.Clock,
		cfg.Settings,
		cfg.NodeDialer,
	)

	cfg.NodeLiveness = liveness.NewNodeLiveness(liveness.NodeLivenessOptions{
		AmbientCtx:        log.AmbientContext{},
		Stopper:           stopper,
		Clock:             cfg.Clock,
		Cache:             cache,
		LivenessThreshold: nlActive,
		RenewalDuration:   nlRenewal,
		Engines:           []storage.Engine{},
	})
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
		ProposedTS: now,
		Start:      now,
		Expiration: now.ToTimestamp().Add(cfg.RangeLeaseDuration.Nanoseconds(), 0).Clone(),
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
		ProposedTS: now,
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

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	// Disable raft log truncation which confuses this test.
	cfg.TestingKnobs.DisableRaftLogQueue = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	secondReplica, err := tc.addBogusReplicaToRangeDesc(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	now := tc.Clock().Now()

	testCases := []struct {
		storeID     roachpb.StoreID
		start       hlc.Timestamp
		expiration  hlc.Timestamp
		expLowWater int64 // 0 for not expecting anything
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
		// Another Store grabs the lease.
		{storeID: secondReplica.StoreID,
			start: now.Add(31, 0), expiration: now.Add(50, 0),
			// The cache now moves to this other store, and we can't query that.
			expLowWater: 0},
		// Lease is regranted to this store. The low-water mark is updated to the
		// beginning of the lease.
		{storeID: tc.store.StoreID(),
			start: now.Add(50, 0), expiration: now.Add(70, 0),
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
			ProposedTS: propTS,
			Start:      test.start.UnsafeToClockTimestamp(),
			Expiration: test.expiration.Clone(),
			Replica: roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(test.storeID),
				NodeID:    roachpb.NodeID(test.storeID),
				StoreID:   test.storeID,
			},
		}); err != nil {
			t.Fatalf("%d: unexpected error %v", i, err)
		}
		// Verify expected low water mark.
		rTS, _ := tc.repl.store.tsCache.GetMax(ctx, roachpb.Key("a"), nil /* end */)

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

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	st := tc.repl.CurrentLeaseStatus(ctx)
	lease := &roachpb.Lease{
		Start:      now,
		Expiration: now.ToTimestamp().Add(10, 0).Clone(),
		Replica: roachpb.ReplicaDescriptor{
			ReplicaID: 2,
			NodeID:    2,
			StoreID:   2,
		},
		Sequence: st.Lease.Sequence + 1,
	}
	ba := &kvpb.BatchRequest{}
	ba.Timestamp = tc.repl.store.Clock().Now()
	ba.Add(&kvpb.RequestLeaseRequest{Lease: *lease, PrevLease: st.Lease})
	_, tok := tc.repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, _, pErr := tc.repl.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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

func getArgs(key []byte) kvpb.GetRequest {
	return kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	}
}

func putArgs(key roachpb.Key, value []byte) kvpb.PutRequest {
	return kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

func cPutArgs(key roachpb.Key, value, expValue []byte) kvpb.ConditionalPutRequest {
	if expValue != nil {
		expValue = roachpb.MakeValueFromBytes(expValue).TagAndDataBytes()
	}
	req := kvpb.NewConditionalPut(key, roachpb.MakeValueFromBytes(value), expValue, false /* allowNotExist */)
	return *req.(*kvpb.ConditionalPutRequest)
}

func iPutArgs(key roachpb.Key, value []byte) kvpb.InitPutRequest {
	return kvpb.InitPutRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.MakeValueFromBytes(value),
	}
}

func deleteArgs(key roachpb.Key) kvpb.DeleteRequest {
	return kvpb.DeleteRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	}
}

func deleteRangeArgs(key, endKey roachpb.Key) kvpb.DeleteRangeRequest {
	return kvpb.DeleteRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
	}
}

// readOrWriteArgs returns either get or put arguments depending on
// value of "read". Get for true; Put for false.
func readOrWriteArgs(key roachpb.Key, read bool) kvpb.Request {
	if read {
		gArgs := getArgs(key)
		return &gArgs
	}
	pArgs := putArgs(key, []byte("value"))
	return &pArgs
}

func incrementArgs(key []byte, inc int64) *kvpb.IncrementRequest {
	return &kvpb.IncrementRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Increment: inc,
	}
}

func scanArgsString(s, e string) *kvpb.ScanRequest {
	return &kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(s), EndKey: roachpb.Key(e)},
	}
}

func getArgsString(k string) *kvpb.GetRequest {
	return &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(k)},
	}
}

func scanArgs(start, end []byte) *kvpb.ScanRequest {
	return &kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
	}
}

func revScanArgsString(s, e string) *kvpb.ReverseScanRequest {
	return &kvpb.ReverseScanRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(s), EndKey: roachpb.Key(e)},
	}
}

func revScanArgs(start, end []byte) *kvpb.ReverseScanRequest {
	return &kvpb.ReverseScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    start,
			EndKey: end,
		},
	}
}

func heartbeatArgs(
	txn *roachpb.Transaction, now hlc.Timestamp,
) (kvpb.HeartbeatTxnRequest, kvpb.Header) {
	return kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key,
		},
		Now: now,
	}, kvpb.Header{Txn: txn}
}

// endTxnArgs creates a EndTxnRequest. By leaving the Sequence field 0, the
// request will not qualify for 1PC.
func endTxnArgs(txn *roachpb.Transaction, commit bool) (kvpb.EndTxnRequest, kvpb.Header) {
	return kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key, // not allowed when going through TxnCoordSender, but we're not
		},
		Commit: commit,
	}, kvpb.Header{Txn: txn}
}

func pushTxnArgs(
	pusher, pushee *roachpb.Transaction, pushType kvpb.PushTxnType,
) kvpb.PushTxnRequest {
	return kvpb.PushTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: pushee.Key,
		},
		PushTo:    pusher.WriteTimestamp.Next(),
		PusherTxn: *pusher,
		PusheeTxn: pushee.TxnMeta,
		PushType:  pushType,
	}
}

func recoverTxnArgs(txn *roachpb.Transaction, implicitlyCommitted bool) kvpb.RecoverTxnRequest {
	return kvpb.RecoverTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key,
		},
		Txn:                 txn.TxnMeta,
		ImplicitlyCommitted: implicitlyCommitted,
	}
}

func queryTxnArgs(txn enginepb.TxnMeta, waitForUpdate bool) kvpb.QueryTxnRequest {
	return kvpb.QueryTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key,
		},
		Txn:           txn,
		WaitForUpdate: waitForUpdate,
	}
}

func queryIntentArgs(key []byte, txn enginepb.TxnMeta, errIfMissing bool) kvpb.QueryIntentRequest {
	return kvpb.QueryIntentRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Txn:            txn,
		ErrorIfMissing: errIfMissing,
	}
}

func queryLocksArgs(key, endKey []byte, includeUncontended bool) kvpb.QueryLocksRequest {
	return kvpb.QueryLocksRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
		IncludeUncontended: includeUncontended,
	}
}

func resolveIntentArgsString(
	s string, txn enginepb.TxnMeta, status roachpb.TransactionStatus,
) kvpb.ResolveIntentRequest {
	return kvpb.ResolveIntentRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(s)},
		IntentTxn:     txn,
		Status:        status,
	}
}

func resolveIntentRangeArgsString(
	s, e string, txn enginepb.TxnMeta, status roachpb.TransactionStatus,
) *kvpb.ResolveIntentRangeRequest {
	return &kvpb.ResolveIntentRangeRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(s), EndKey: roachpb.Key(e)},
		IntentTxn:     txn,
		Status:        status,
	}
}

func internalMergeArgs(key []byte, value roachpb.Value) kvpb.MergeRequest {
	return kvpb.MergeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
		Value: value,
	}
}

func truncateLogArgs(index kvpb.RaftIndex, rangeID roachpb.RangeID) kvpb.TruncateLogRequest {
	return kvpb.TruncateLogRequest{
		Index:   index,
		RangeID: rangeID,
	}
}

func gcKey(key roachpb.Key, timestamp hlc.Timestamp) kvpb.GCRequest_GCKey {
	return kvpb.GCRequest_GCKey{
		Key:       key,
		Timestamp: timestamp,
	}
}

func recomputeStatsArgs(key roachpb.Key) kvpb.RecomputeStatsRequest {
	return kvpb.RecomputeStatsRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: key,
		},
	}
}

func gcArgs(startKey []byte, endKey []byte, keys ...kvpb.GCRequest_GCKey) kvpb.GCRequest {
	return kvpb.GCRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		Keys: keys,
	}
}

func clearRangeArgs(startKey, endKey roachpb.Key) kvpb.ClearRangeRequest {
	return kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{
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

	pArgs := make([]kvpb.PutRequest, optimizePutThreshold)
	cpArgs := make([]kvpb.ConditionalPutRequest, optimizePutThreshold)
	ipArgs := make([]kvpb.InitPutRequest, optimizePutThreshold)
	for i := 0; i < optimizePutThreshold; i++ {
		pArgs[i] = putArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"))
		cpArgs[i] = cPutArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"), []byte("0"))
		ipArgs[i] = iPutArgs([]byte(fmt.Sprintf("%02d", i)), []byte("1"))
	}
	incArgs := incrementArgs([]byte("inc"), 1)

	testCases := []struct {
		exKey    roachpb.Key
		exEndKey roachpb.Key // MVCC range key
		reqs     []kvpb.Request
		expBlind []bool
	}{
		// No existing keys, single put.
		{
			nil, nil,
			[]kvpb.Request{
				&pArgs[0],
			},
			[]bool{
				false,
			},
		},
		// No existing keys, nine puts.
		{
			nil, nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false,
			},
		},
		// No existing keys, ten puts.
		{
			nil, nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at "0", ten conditional puts.
		{
			roachpb.Key("0"), nil,
			[]kvpb.Request{
				&cpArgs[0], &cpArgs[1], &cpArgs[2], &cpArgs[3], &cpArgs[4], &cpArgs[5], &cpArgs[6], &cpArgs[7], &cpArgs[8], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at "0", ten init puts.
		{
			roachpb.Key("0"), nil,
			[]kvpb.Request{
				&ipArgs[0], &ipArgs[1], &ipArgs[2], &ipArgs[3], &ipArgs[4], &ipArgs[5], &ipArgs[6], &ipArgs[7], &ipArgs[8], &ipArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at 11, mixed put types.
		{
			roachpb.Key("11"), nil,
			[]kvpb.Request{
				&pArgs[0], &cpArgs[1], &pArgs[2], &cpArgs[3], &ipArgs[4], &ipArgs[5], &pArgs[6], &cpArgs[7], &pArgs[8], &ipArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
		// Existing key at 00, ten puts, expect nothing blind.
		{
			roachpb.Key("00"), nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false, false,
			},
		},
		// Existing key at 00, ten puts in reverse order, expect nothing blind.
		{
			roachpb.Key("00"), nil,
			[]kvpb.Request{
				&pArgs[9], &pArgs[8], &pArgs[7], &pArgs[6], &pArgs[5], &pArgs[4], &pArgs[3], &pArgs[2], &pArgs[1], &pArgs[0],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false, false,
			},
		},
		// Existing key at 05, ten puts, expect first five puts are blind.
		{
			roachpb.Key("05"), nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, false, false, false, false, false,
			},
		},
		// Existing key at 09, ten puts, expect first nine puts are blind.
		{
			roachpb.Key("09"), nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, false,
			},
		},
		// No existing key, ten puts + inc + ten cputs.
		{
			nil, nil,
			[]kvpb.Request{
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
			nil, nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Duplicate cput at 11th key; should see ten puts.
		{
			nil, nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Duplicate iput at 11th key; should see ten puts.
		{
			nil, nil,
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9], &ipArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Duplicate cput at 10th key; should see ten cputs.
		{
			nil, nil,
			[]kvpb.Request{
				&cpArgs[0], &cpArgs[1], &cpArgs[2], &cpArgs[3], &cpArgs[4], &cpArgs[5], &cpArgs[6], &cpArgs[7], &cpArgs[8], &cpArgs[9], &cpArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true, false,
			},
		},
		// Existing range key at 00-20, ten puts, expect no blind.
		{
			roachpb.Key("00"), roachpb.Key("20"),
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				false, false, false, false, false, false, false, false, false, false,
			},
		},
		// Existing range key at 05-08, ten puts, expect first five puts are blind.
		{
			roachpb.Key("05"), roachpb.Key("08"),
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, false, false, false, false, false,
			},
		},
		// Existing range key at 20-21, ten puts, expect all blind.
		{
			roachpb.Key("20"), roachpb.Key("21"),
			[]kvpb.Request{
				&pArgs[0], &pArgs[1], &pArgs[2], &pArgs[3], &pArgs[4], &pArgs[5], &pArgs[6], &pArgs[7], &pArgs[8], &pArgs[9],
			},
			[]bool{
				true, true, true, true, true, true, true, true, true, true,
			},
		},
	}

	for i, c := range testCases {
		if c.exEndKey != nil {
			require.NoError(t, storage.MVCCDeleteRangeUsingTombstone(ctx, tc.engine, nil,
				c.exKey, c.exEndKey, hlc.MinTimestamp, hlc.ClockTimestamp{}, nil, nil, false, 0, 0, nil))
		} else if c.exKey != nil {
			_, err := storage.MVCCPut(ctx, tc.engine, c.exKey,
				hlc.Timestamp{}, roachpb.MakeValueFromString("foo"), storage.MVCCWriteOptions{})
			require.NoError(t, err)
		}
		batch := kvpb.BatchRequest{}
		for _, r := range c.reqs {
			batch.Add(r)
		}
		// Make a deep clone of the requests slice. We need a deep clone
		// because the regression which is prevented here changed data on the
		// individual requests, and not the slice.
		goldenRequests := append([]kvpb.RequestUnion(nil), batch.Requests...)
		for i := range goldenRequests {
			clone := protoutil.Clone(goldenRequests[i].GetInner()).(kvpb.Request)
			goldenRequests[i].MustSetInner(clone)
		}
		// Save the original slice, allowing us to assert that it doesn't
		// change when it is passed to optimizePuts.
		oldRequests := batch.Requests
		var err error
		batch.Requests, err = optimizePuts(ctx, tc.engine, batch.Requests, false)
		require.NoError(t, err)
		if !reflect.DeepEqual(goldenRequests, oldRequests) {
			t.Fatalf("%d: optimizePuts mutated the original request slice: %s",
				i, pretty.Diff(goldenRequests, oldRequests),
			)
		}

		blind := []bool{}
		for _, r := range batch.Requests {
			switch t := r.GetInner().(type) {
			case *kvpb.PutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			case *kvpb.ConditionalPutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			case *kvpb.InitPutRequest:
				blind = append(blind, t.Blind)
				t.Blind = false
			default:
				blind = append(blind, false)
			}
		}
		if !reflect.DeepEqual(blind, c.expBlind) {
			t.Errorf("%d: expected %+v; got %+v", i, c.expBlind, blind)
		}
		if c.exEndKey != nil {
			require.NoError(t, tc.engine.ClearMVCCRangeKey(storage.MVCCRangeKey{
				StartKey: c.exKey, EndKey: c.exEndKey, Timestamp: hlc.MinTimestamp}))
		} else if c.exKey != nil {
			require.NoError(t, tc.engine.ClearUnversioned(c.exKey, storage.ClearOptions{}))
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

	for _, test := range []kvpb.Request{
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

				tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))

				ts := tc.Clock().Now().Next()
				if _, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: ts}, test); pErr != nil {
					t.Error(pErr)
				}
				if !tc.repl.OwnsValidLease(ctx, ts.UnsafeToClockTimestamp()) {
					t.Errorf("expected lease acquisition")
				}
				lease, _ = tc.repl.GetLease()
				if lease.Start != expStart {
					t.Errorf("unexpected lease start: %s; expected %s", lease.Start, expStart)
				}

				if lease.Expiration.LessEq(ts) {
					t.Errorf("%s already expired: %+v", ts, lease)
				}

				shouldRenewTS := lease.Expiration.Add(-1, 0)
				tc.manualClock.MustAdvanceTo(shouldRenewTS.GoTime().Add(1))
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
// the *kvpb.Error to ensure that each requestor gets a distinct
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

		tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
		cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
		// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
		// refreshes so that our lease proposal does not risk being rejected
		// with an AmbiguousResultError.
		cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
		cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
		cfg.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
			ll, ok := args.Req.Requests[0].GetInner().(*kvpb.RequestLeaseRequest)
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
				return kvpb.NewErrorf(origMsg)
			}
			return nil
		}
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)

		atomic.StoreInt32(&active, 1)
		tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
		now := tc.Clock().NowAsClockTimestamp()
		pErrCh := make(chan *kvpb.Error, num)
		for i := 0; i < num; i++ {
			if err := stopper.RunAsyncTask(ctx, "test", func(ctx context.Context) {
				tc.repl.mu.Lock()
				status := tc.repl.leaseStatusAtRLocked(ctx, now)
				llHandle := tc.repl.requestLeaseLocked(ctx, status, nil)
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

		pErrs := make([]*kvpb.Error, num)
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

// TestLeaseCallerCancelled tests that lease requests continue to completion
// even when all callers have cancelled.
func TestLeaseCallerCancelled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const num = 5

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var active, seen int32
	var wg sync.WaitGroup
	errC := make(chan error, 1)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	// Disable reasonNewLeader and reasonNewLeaderOrConfigChange proposal
	// refreshes so that our lease proposal does not risk being rejected
	// with an AmbiguousResultError.
	cfg.TestingKnobs.DisableRefreshReasonNewLeader = true
	cfg.TestingKnobs.DisableRefreshReasonNewLeaderOrConfigChange = true
	cfg.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		ll, ok := args.Req.Requests[0].GetInner().(*kvpb.RequestLeaseRequest)
		if !ok || atomic.LoadInt32(&active) == 0 {
			return nil
		}
		if c := atomic.AddInt32(&seen, 1); c > 1 {
			// Morally speaking, this is an error, but reproposals can happen and so
			// we warn (in case this trips the test up in more unexpected ways).
			t.Logf("reproposal of %+v", ll)
		}
		// Wait for all lease requests to join the same LeaseRequest command
		// and cancel.
		wg.Wait()

		// The lease request's context should not be cancelled. Propagate it up to
		// the main test.
		select {
		case <-args.Ctx.Done():
		case <-time.After(time.Second):
		}
		select {
		case errC <- args.Ctx.Err():
		default:
		}
		return nil
	}
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	atomic.StoreInt32(&active, 1)
	tc.manualClock.MustAdvanceTo(leaseExpiry(tc.repl))
	now := tc.Clock().NowAsClockTimestamp()
	var llHandles []*leaseRequestHandle
	for i := 0; i < num; i++ {
		wg.Add(1)
		tc.repl.mu.Lock()
		status := tc.repl.leaseStatusAtRLocked(ctx, now)
		llHandles = append(llHandles, tc.repl.requestLeaseLocked(ctx, status, nil))
		tc.repl.mu.Unlock()
	}
	for _, llHandle := range llHandles {
		select {
		case <-llHandle.C():
			t.Fatal("lease request unexpectedly completed")
		default:
		}
		llHandle.Cancel()
		wg.Done()
	}

	select {
	case err := <-errC:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lease request")
	}
}

// TestRequestLeaseLimit tests that lease requests respect the limiter
func TestRequestLeaseLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var tc testContext
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DisableAutomaticLeaseRenewal = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	requestLease := func(ctx context.Context, limiter *quotapool.IntPool) error {
		now := tc.Clock().NowAsClockTimestamp()
		tc.repl.mu.Lock()
		status := tc.repl.leaseStatusAtRLocked(ctx, now)
		llHandle := tc.repl.requestLeaseLocked(ctx, status, limiter)
		tc.repl.mu.Unlock()
		pErr := <-llHandle.C()
		return pErr.GoError()
	}

	// A 0 limit should immediately error.
	limiter := quotapool.NewIntPool("test", 0)
	err := requestLease(ctx, limiter)
	require.Error(t, err)
	require.True(t, errors.Is(err, stop.ErrThrottled), "%v", err)

	// A limit of 1 should work. Wait for the quota to get released.
	limiter.UpdateCapacity(1)
	err = requestLease(ctx, limiter)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return limiter.ApproximateQuota() > 0
	}, 3*time.Second, 100*time.Millisecond)

	// Acquire the slot, and watch the lease request error again.
	_, err = limiter.TryAcquire(ctx, 1)
	require.NoError(t, err)
	err = requestLease(ctx, limiter)
	require.Error(t, err)
	require.True(t, errors.Is(err, stop.ErrThrottled), "%v", err)
}

// TestReplicaUpdateTSCache verifies that reads and ranged writes update the
// timestamp cache.
func TestReplicaUpdateTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)

	startNanos := tc.Clock().Now().WallTime

	// Set clock to time 1s and do the read.
	tc.manualClock.MustAdvanceTo(timeutil.Unix(1, 0))
	ts1 := tc.Clock().Now()
	gArgs := getArgs([]byte("a"))

	if _, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: ts1}, &gArgs); pErr != nil {
		t.Error(pErr)
	}
	// Set clock to time 2s for write.
	tc.manualClock.MustAdvanceTo(timeutil.Unix(2, 0))
	ts2 := tc.Clock().Now()
	key := roachpb.Key("b")
	drArgs := kvpb.NewDeleteRange(key, key.Next(), false /* returnKeys */)

	if _, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: ts2}, drArgs); pErr != nil {
		t.Error(pErr)
	}
	// Verify the timestamp cache has rTS=1s and wTS=0s for "a".
	noID := uuid.UUID{}
	rTS, rTxnID := tc.repl.store.tsCache.GetMax(ctx, roachpb.Key("a"), nil)
	if rTS != ts1 || rTxnID != noID {
		t.Errorf("expected rTS=%s but got %s; rTxnID=%s", ts1, rTS, rTxnID)
	}
	// Verify the timestamp cache has rTS=2s for "b".
	rTS, rTxnID = tc.repl.store.tsCache.GetMax(ctx, roachpb.Key("b"), nil)
	if rTS != ts2 || rTxnID != noID {
		t.Errorf("expected rTS=%s but got %s; rTxnID=%s", ts2, rTS, rTxnID)
	}
	// Verify another key ("c") has 0sec in timestamp cache.
	rTS, rTxnID = tc.repl.store.tsCache.GetMax(ctx, roachpb.Key("c"), nil)
	if rTS.WallTime != startNanos || rTxnID != noID {
		t.Errorf("expected rTS=0s but got %s; rTxnID=%s", rTS, rTxnID)
	}
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
		// A write doesn't wait for an earlier read.
		{true, false, false, false},
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
							func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
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

						sendWithHeader := func(header kvpb.Header, args kvpb.Request) *kvpb.Error {
							ba := &kvpb.BatchRequest{}
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
						cmd1Done := make(chan *kvpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), "test", func(_ context.Context) {
							args := readOrWriteArgs(key1, test.cmd1Read)
							cmd1Done <- sendWithHeader(kvpb.Header{
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
						cmd2Done := make(chan *kvpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), "", func(_ context.Context) {
							args := readOrWriteArgs(key1, test.cmd2Read)
							cmd2Done <- sendWithHeader(kvpb.Header{}, args)
						}); err != nil {
							t.Fatal(err)
						}

						// Next, try read for a non-impacted key--should go through immediately.
						cmd3Done := make(chan *kvpb.Error, 1)
						if err := stopper.RunAsyncTask(context.Background(), "", func(_ context.Context) {
							args := readOrWriteArgs(key2, true)
							cmd3Done <- sendWithHeader(kvpb.Header{}, args)
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

	for _, rc := range []kvpb.ReadConsistencyType{
		kvpb.READ_UNCOMMITTED,
		kvpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			key := roachpb.Key("key1")
			blockingStart := make(chan struct{}, 1)
			blockingDone := make(chan struct{})

			tc := testContext{}
			tsc := TestStoreConfig(nil)
			tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
				func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
					if put, ok := filterArgs.Req.(*kvpb.PutRequest); ok {
						putBytes, err := put.Value.GetBytes()
						if err != nil {
							return kvpb.NewErrorWithTxn(err, filterArgs.Hdr.Txn)
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
			cmd1Done := make(chan *kvpb.Error)
			go func() {
				args := putArgs(key, []byte{1})

				_, pErr := tc.SendWrapped(&args)
				cmd1Done <- pErr
			}()
			// Wait for cmd1 to get acquire latches.
			<-blockingStart

			// An inconsistent read to the key won't wait.
			cmd2Done := make(chan *kvpb.Error)
			go func(rc kvpb.ReadConsistencyType) {
				args := getArgs(key)

				_, pErr := tc.SendWrappedWith(kvpb.Header{
					ReadConsistency: rc,
				}, &args)
				cmd2Done <- pErr
			}(rc)

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
			ba := &kvpb.BatchRequest{}
			ba.Add(readOrWriteArgs(roachpb.Key(key), cmd1Read))
			ba.Add(readOrWriteArgs(roachpb.Key(key), cmd2Read))

			// Set a deadline for nicer error behavior on deadlock.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, pErr := tc.Sender().Send(ctx, ba)
			if pErr != nil {
				if _, ok := pErr.GetDetail().(*kvpb.WriteTooOldError); ok && !cmd1Read && !cmd2Read {
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
	waitForRequestBlocked := make(chan struct{}, 1)

	tc := testContext{}
	tsc := TestStoreConfig(nil)
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if !filterArgs.Req.Header().Key.Equal(blockKey.Load().(roachpb.Key)) {
				return nil
			}
			if filterArgs.Req.Method() == kvpb.Get && blockReader.Load().(bool) {
				waitForRequestBlocked <- struct{}{}
				<-blockCh
			} else if filterArgs.Req.Method() == kvpb.Put && blockWriter.Load().(bool) {
				waitForRequestBlocked <- struct{}{}
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
		//
		// Reader goes first, but the reader does not need to hold latches during
		// evaluation, so we expect no interference.
		{
			readerTS:    makeTS(1, 0),
			writerTS:    makeTS(1, 0),
			key:         roachpb.Key("a"),
			readerFirst: true,
			interferes:  false,
		},
		// Writer goes first, but the writer does need to hold latches during
		// evaluation, so it should block the reader.
		{
			readerTS:    makeTS(1, 0),
			writerTS:    makeTS(1, 0),
			key:         roachpb.Key("b"),
			readerFirst: false,
			interferes:  true,
		},
		// Reader has earlier timestamp, so it doesn't interfere with the write
		// that's in its future.
		{
			readerTS:    makeTS(1, 0),
			writerTS:    makeTS(1, 1),
			key:         roachpb.Key("c"),
			readerFirst: true,
			interferes:  false,
		},
		{
			readerTS:    makeTS(1, 0),
			writerTS:    makeTS(1, 1),
			key:         roachpb.Key("d"),
			readerFirst: false,
			interferes:  false,
		},
		// Writer has an earlier timestamp. We expect no interference for the writer
		// as the reader will be evaluating over a pebble snapshot. We'd expect the
		// writer to be able to continue without interference but to get bumped by
		// the timestamp cache.
		{
			readerTS:    makeTS(1, 1),
			writerTS:    makeTS(1, 0),
			key:         roachpb.Key("e"),
			readerFirst: true,
			interferes:  false,
		},
		// We expect the reader to block for the writer that's writing in the
		// reader's past.
		{
			readerTS:    makeTS(1, 1),
			writerTS:    makeTS(1, 0),
			key:         roachpb.Key("f"),
			readerFirst: false,
			interferes:  true,
		},
		// Even though local key accesses are NonMVCC, the reader should not block
		// the writer because it should not need to hold its latches during
		// evaluation.
		{
			readerTS:    makeTS(1, 0),
			writerTS:    makeTS(1, 1),
			key:         keys.RangeDescriptorKey(roachpb.RKey("a")),
			readerFirst: true,
			interferes:  false,
		},
		// The writer will block the reader since it holds NonMVCC latches during
		// evaluation.
		{
			readerTS:   makeTS(1, 0),
			writerTS:   makeTS(1, 1),
			key:        keys.RangeDescriptorKey(roachpb.RKey("b")),
			interferes: true,
		},
	}
	for _, test := range testCases {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			blockReader.Store(false)
			blockWriter.Store(false)
			blockKey.Store(test.key)
			errCh := make(chan *kvpb.Error, 2)

			baR := &kvpb.BatchRequest{}
			baR.Timestamp = test.readerTS
			gArgs := getArgs(test.key)
			baR.Add(&gArgs)
			baW := &kvpb.BatchRequest{}
			baW.Timestamp = test.writerTS
			pArgs := putArgs(test.key, []byte("value"))
			baW.Add(&pArgs)

			if test.readerFirst {
				blockReader.Store(true)
				go func() {
					_, pErr := tc.Sender().Send(context.Background(), baR)
					errCh <- pErr
				}()
				// Wait for the above read to get blocked on blockCh.
				<-waitForRequestBlocked
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
				// Wait for the above write to get blocked on blockCh while it's holding
				// latches.
				<-waitForRequestBlocked
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
	cmd, _ := batcheval.LookupCommand(kvpb.EndTxn)
	err := cmd.DeclareKeys(
		&roachpb.RangeDescriptor{StartKey: roachpb.RKey("a"), EndKey: roachpb.RKey("e")},
		&kvpb.Header{},
		&kvpb.EndTxnRequest{
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
	require.NoError(t, err)
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

// TestReplicaLatchingOptimisticEvaluationKeyLimit verifies that limited scans
// evaluate optimistically without waiting for latches to be acquired. In some
// cases, this allows them to avoid waiting on writes that their
// over-estimated declared spans overlapped with.
func TestReplicaLatchingOptimisticEvaluationKeyLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "point-reads", func(t *testing.T, pointReads bool) {
		baRead := &kvpb.BatchRequest{}
		if pointReads {
			gArgs1, gArgs2 := getArgsString("a"), getArgsString("b")
			gArgs3, gArgs4 := getArgsString("c"), getArgsString("d")
			baRead.Add(gArgs1, gArgs2, gArgs3, gArgs4)
		} else {
			// Split into two back-to-back scans for better test coverage.
			sArgs1 := scanArgsString("a", "c")
			sArgs2 := scanArgsString("c", "e")
			baRead.Add(sArgs1, sArgs2)
		}
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
			func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
				// Make sure the direct GC path doesn't interfere with this test.
				if !filterArgs.Req.Header().Key.Equal(blockKey.Load().(roachpb.Key)) {
					return nil
				}
				if filterArgs.Req.Method() == kvpb.Put && blockWriter.Load().(bool) {
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
				errCh := make(chan *kvpb.Error, 2)
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
				baReadCopy := baRead.ShallowCopy()
				baReadCopy.MaxSpanRequestKeys = test.limit
				go func() {
					_, pErr := tc.Sender().Send(context.Background(), baReadCopy)
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
	})
}

// TestReplicaLatchingOptimisticEvaluationSkipLocked verifies that reads using
// the SkipLocked wait policy evaluate optimistically without waiting for
// latches to be acquired. In some cases, this allows them to avoid waiting on
// latches that are touching the same keys but that the weaker isolation level
// of SkipLocked allows the read to skip.
func TestReplicaLatchingOptimisticEvaluationSkipLocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "point-reads", func(t *testing.T, pointReads bool) {
		testutils.RunTrueAndFalse(t, "locking-reads", func(t *testing.T, lockingReads bool) {
			baRead := &kvpb.BatchRequest{}
			baRead.WaitPolicy = lock.WaitPolicy_SkipLocked
			if pointReads {
				gArgs1, gArgs2 := getArgsString("a"), getArgsString("b")
				gArgs3, gArgs4 := getArgsString("c"), getArgsString("d")
				if lockingReads {
					gArgs1.KeyLockingStrength = lock.Exclusive
					gArgs2.KeyLockingStrength = lock.Exclusive
					gArgs3.KeyLockingStrength = lock.Exclusive
					gArgs4.KeyLockingStrength = lock.Exclusive
				}
				baRead.Add(gArgs1, gArgs2, gArgs3, gArgs4)
			} else {
				// Split into two back-to-back scans for better test coverage.
				sArgs1 := scanArgsString("a", "c")
				sArgs2 := scanArgsString("c", "e")
				if lockingReads {
					sArgs1.KeyLockingStrength = lock.Exclusive
					sArgs2.KeyLockingStrength = lock.Exclusive
				}
				baRead.Add(sArgs1, sArgs2)
			}
			// The state that will block two writes while holding latches.
			var blockWriters atomic.Value
			blockKey1, blockKey2 := roachpb.Key("c"), roachpb.Key("d")
			blockWriters.Store(false)
			blockCh := make(chan struct{})
			blockedCh := make(chan struct{}, 1)
			// Setup filter to block the writes.
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
				func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
					// Make sure the direct GC path doesn't interfere with this test.
					reqKey := filterArgs.Req.Header().Key
					if !reqKey.Equal(blockKey1) && !reqKey.Equal(blockKey2) {
						return nil
					}
					if filterArgs.Req.Method() == kvpb.Put && blockWriters.Load().(bool) {
						blockedCh <- struct{}{}
						<-blockCh
					}
					return nil
				}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.StartWithStoreConfig(ctx, t, stopper, tsc)
			// Write initial keys on some, but not all keys.
			for _, k := range []string{"a", "b", "c"} {
				pArgs := putArgs([]byte(k), []byte("value"))
				_, pErr := tc.SendWrapped(&pArgs)
				require.Nil(t, pErr)
			}

			// Write #1: lock key "c" and then write to it again in the same txn. Since
			// the key is locked at the time the write is blocked and the SkipLocked
			// read evaluates, the read skips over the key and does not conflict with
			// the write's latches.
			txn := newTransaction("locker", blockKey1, 0, tc.Clock())
			txnH := kvpb.Header{Txn: txn}
			putArgs1 := putArgs(blockKey1, []byte("value"))
			_, pErr := tc.SendWrappedWith(txnH, &putArgs1)
			require.Nil(t, pErr)
			// Write to the blocked key again, and this time stall. Note that this could
			// also be the ResolveIntent request that's removing the lock, which is
			// likely an even more common cause of blocking today. However, we use a Put
			// here because we may stop acquiring latches during intent resolution in
			// the future and don't want this test to break when we do.
			errCh := make(chan *kvpb.Error, 3)
			blockWriters.Store(true)
			go func() {
				_, pErr := tc.SendWrappedWith(txnH, &putArgs1)
				errCh <- pErr
			}()
			<-blockedCh

			// Write #2: perform an initial write on key "d". Since the key is missing
			// at the time the write is blocked and the SkipLocked read evaluates, the
			// read skips over the key and does not conflict with the write's latches.
			putArgs2 := putArgs(blockKey2, []byte("value"))
			go func() {
				_, pErr := tc.SendWrappedWith(txnH, &putArgs2)
				errCh <- pErr
			}()
			<-blockedCh

			// The writes are now blocked while holding latches. Issue the read.
			blockWriters.Store(false)
			var respKeys []roachpb.Key
			go func() {
				errCh <- func() *kvpb.Error {
					br, pErr := tc.Sender().Send(ctx, baRead)
					if pErr != nil {
						return pErr
					}
					for i, req := range baRead.Requests {
						resp := br.Responses[i]
						if err := kvpb.ResponseKeyIterate(req.GetInner(), resp.GetInner(), func(k roachpb.Key) {
							respKeys = append(respKeys, k)
						}); err != nil {
							return kvpb.NewError(err)
						}
					}
					return nil
				}()
			}()

			// The read should complete first.
			require.Nil(t, <-errCh)
			// The writes should complete next, after they are unblocked.
			close(blockCh)
			require.Nil(t, <-errCh)
			require.Nil(t, <-errCh)

			// The read should have only returned the unlocked keys.
			expRespKeys := []roachpb.Key{roachpb.Key("a"), roachpb.Key("b")}
			require.Equal(t, expRespKeys, respKeys)
		})
	})
}

// TestReplicaUseTSCache verifies that write timestamps are upgraded based on
// the timestamp cache.
func TestReplicaUseTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.Start(ctx, t, stopper)
	startTS := tc.Clock().Now()

	// Set clock to time 1s and do the read.
	tc.manualClock.Advance(1)
	readTS := tc.Clock().Now()
	args := getArgs([]byte("a"))

	_, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: readTS}, &args)
	require.Nil(t, pErr)

	// Perform a conflicting write. Should get bumped.
	pArgs := putArgs([]byte("a"), []byte("value"))
	ba := &kvpb.BatchRequest{}
	ba.Add(&pArgs)
	ba.Timestamp = startTS

	br, pErr := tc.Sender().Send(ctx, ba)
	require.Nil(t, pErr)
	require.NotEqual(t, startTS, br.Timestamp)
	require.Equal(t, readTS.Next(), br.Timestamp)
}

// TestReplicaTSCacheForwardsIntentTS verifies that the timestamp cache affects
// the timestamps at which intents are written. That is, if a transactional
// write is forwarded by the timestamp cache due to a more recent read, the
// written intents must be left at the forwarded timestamp. See the comment on
// the enginepb.TxnMeta.Timestamp field for rationale.
func TestReplicaTSCacheForwardsIntentTS(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

	tsOld := tc.Clock().Now()
	tsNew := tsOld.Add(time.Millisecond.Nanoseconds(), 0)

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
	ba := &kvpb.BatchRequest{}
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
			if _, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txnOld}, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}
			iter, err := tc.engine.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{Prefix: true})
			if err != nil {
				t.Fatal(err)
			}
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
}

func TestConditionalPutUpdatesTSCacheOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DontPushOnLockConflictError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Set clock to time 2s and do the conditional put.
	t1 := makeTS(1*time.Second.Nanoseconds(), 0)
	t2 := makeTS(2*time.Second.Nanoseconds(), 0)
	t2Next := t2.Next()
	tc.manualClock.MustAdvanceTo(t2.GoTime())

	// CPut args which expect value "1" to write "0".
	key := []byte("a")
	cpArgs1 := cPutArgs(key, []byte("1"), []byte("0"))
	_, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: t2}, &cpArgs1)
	if cfErr, ok := pErr.GetDetail().(*kvpb.ConditionFailedError); !ok {
		t.Errorf("expected ConditionFailedError; got %v", pErr)
	} else if cfErr.ActualValue != nil {
		t.Errorf("expected empty actual value; got %s", cfErr.ActualValue)
	}

	// Try a transactional conditional put at a lower timestamp and
	// ensure it is pushed.
	txnEarly := newTransaction("test", key, 1, tc.Clock())
	txnEarly.ReadTimestamp, txnEarly.WriteTimestamp = t1, t1
	cpArgs2 := cPutArgs(key, []byte("value"), nil)
	resp, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txnEarly}, &cpArgs2)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t2Next {
		t.Errorf("expected write timestamp to upgrade to %s; got %s", t2Next, respTS)
	}

	// Try a conditional put at a later timestamp which will fail
	// because there's now a transaction intent. This failure will
	// not update the timestamp cache.
	t3 := makeTS(3*time.Second.Nanoseconds(), 0)
	tc.manualClock.MustAdvanceTo(t3.GoTime())
	_, pErr = tc.SendWrapped(&cpArgs1)
	if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
		t.Errorf("expected LockConflictError; got %v", pErr)
	}

	// Abort the intent and try a transactional conditional put at
	// a later timestamp. This should succeed and should not update
	// the timestamp cache.
	abortIntent := func(s roachpb.Span, abortTxn *roachpb.Transaction) {
		if _, pErr = tc.SendWrapped(&kvpb.ResolveIntentRequest{
			RequestHeader: kvpb.RequestHeaderFromSpan(s),
			IntentTxn:     abortTxn.TxnMeta,
			Status:        roachpb.ABORTED,
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
	abortIntent(cpArgs2.Span(), txnEarly)
	txnLater := *txnEarly
	txnLater.ReadTimestamp, txnLater.WriteTimestamp = t3, t3
	resp, pErr = tc.SendWrappedWith(kvpb.Header{Txn: &txnLater}, &cpArgs2)
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
	resp, pErr = tc.SendWrappedWith(kvpb.Header{Txn: txnEarly}, &cpArgs2)
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
	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	cfg.TestingKnobs.DontPushOnLockConflictError = true
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
	tc.manualClock.MustAdvanceTo(t2.GoTime())

	// InitPut args to write "1" to same key. Should fail.
	ipArgs2 := iPutArgs(key, []byte("1"))
	_, pErr = tc.SendWrappedWith(kvpb.Header{Timestamp: t2}, &ipArgs2)
	if cfErr, ok := pErr.GetDetail().(*kvpb.ConditionFailedError); !ok {
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
	resp, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txnEarly}, &ipArgs1)
	if pErr != nil {
		t.Fatal(pErr)
	} else if respTS := resp.Header().Txn.WriteTimestamp; respTS != t2Next {
		t.Errorf("expected write timestamp to upgrade to %s; got %s", t2Next, respTS)
	}

	// Try an init put at a later timestamp which will fail
	// because there's now a transaction intent. This failure
	// will not update the timestamp cache.
	t3 := makeTS(3*time.Second.Nanoseconds(), 0)
	tc.manualClock.MustAdvanceTo(t3.GoTime())
	_, pErr = tc.SendWrapped(&ipArgs2)
	if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
		t.Errorf("expected LockConflictError; got %v", pErr)
	}

	// Abort the intent and try a transactional init put at a later
	// timestamp. This should succeed and should not update the
	// timestamp cache.
	abortIntent := func(s roachpb.Span, abortTxn *roachpb.Transaction) {
		if _, pErr = tc.SendWrapped(&kvpb.ResolveIntentRequest{
			RequestHeader: kvpb.RequestHeaderFromSpan(s),
			IntentTxn:     abortTxn.TxnMeta,
			Status:        roachpb.ABORTED,
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
	abortIntent(ipArgs1.Span(), txnEarly)
	txnLater := *txnEarly
	txnLater.ReadTimestamp, txnLater.WriteTimestamp = t3, t3
	resp, pErr = tc.SendWrappedWith(kvpb.Header{Txn: &txnLater}, &ipArgs1)
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
	resp, pErr = tc.SendWrappedWith(kvpb.Header{Txn: txnEarly}, &ipArgs1)
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

	for _, rc := range []kvpb.ReadConsistencyType{
		kvpb.READ_UNCOMMITTED,
		kvpb.INCONSISTENT,
	} {
		t.Run(rc.String(), func(t *testing.T) {
			tc := testContext{}
			ctx := context.Background()
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(ctx, t, stopper)
			// Set clock to time 1s and do the read.
			t0 := 1 * time.Second
			tc.manualClock.MustAdvanceTo(timeutil.Unix(0, t0.Nanoseconds()))
			args := getArgs([]byte("a"))
			ts := tc.Clock().Now()

			_, pErr := tc.SendWrappedWith(kvpb.Header{
				Timestamp:       ts,
				ReadConsistency: rc,
			}, &args)

			if pErr != nil {
				t.Error(pErr)
			}
			pArgs := putArgs([]byte("a"), []byte("value"))

			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Timestamp: hlc.Timestamp{WallTime: 0, Logical: 1}}
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
	cfg.TestingKnobs.DontPushOnLockConflictError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// Test for both read & write attempts.
	for i, read := range []bool{true, false} {
		key := roachpb.Key(fmt.Sprintf("key-%d", i))

		// Start by laying down an intent to trip up future read or write to same key.
		txn := newTransaction("test", key, 1, tc.Clock())
		pArgs := putArgs(key, []byte("value"))
		assignSeqNumsForReqs(txn, &pArgs)

		_, pErr := tc.SendWrappedWith(kvpb.Header{
			Txn: txn,
		}, &pArgs)
		if pErr != nil {
			t.Fatalf("test %d: %s", i, pErr)
		}

		// Now attempt read or write.
		args := readOrWriteArgs(key, read)
		ts := tc.Clock().Now() // later timestamp

		_, pErr = tc.SendWrappedWith(kvpb.Header{Timestamp: ts}, args)
		if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
			t.Errorf("expected LockConflictError; got %v", pErr)
		}

		// Write the intent again -- should not have its timestamp upgraded!
		ba := &kvpb.BatchRequest{}
		ba.Header = kvpb.Header{Txn: txn}
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

	if _, pErr := tc.SendWrappedWith(kvpb.Header{
		Txn: txn,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Now try a write and verify timestamp isn't incremented.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
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
	rArgs := &kvpb.ResolveIntentRequest{
		RequestHeader: pArgs.Header(),
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.COMMITTED,
	}
	if _, pErr = tc.SendWrappedWith(kvpb.Header{Timestamp: txn.WriteTimestamp}, rArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Finally, try a non-transactional write and verify timestamp is incremented.
	ts := txn.WriteTimestamp
	expTS := ts
	expTS.Logical++

	ba = &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Timestamp: ts}
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

	if _, pErr := tc.SendWrappedWith(kvpb.Header{
		Txn: txn,
	}, args); pErr != nil {
		t.Fatal(pErr)
	}

	// Overwrite Abort span entry with garbage for the last op.
	key := keys.AbortSpanKey(tc.repl.RangeID, txn.ID)
	_, err := storage.MVCCPut(ctx, tc.engine, key, hlc.Timestamp{}, roachpb.MakeValueFromString("never read in this test"), storage.MVCCWriteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// Now try increment again and verify error.
	_, pErr := tc.SendWrappedWith(kvpb.Header{
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

	runWithTxn := func(txn *roachpb.Transaction, reqs ...kvpb.Request) error {
		ba := &kvpb.BatchRequest{}
		ba.Header.Txn = txn
		ba.Add(reqs...)
		_, pErr := tc.Sender().Send(ctx, ba)
		return pErr.GoError()
	}
	keyAtSeqHasVal := func(txn *roachpb.Transaction, key []byte, seq enginepb.TxnSeq, val *roachpb.Value) error {
		args := getArgs(key)
		args.Sequence = seq
		resp, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txn}, &args)
		if pErr != nil {
			return pErr.GoError()
		}
		foundVal := resp.(*kvpb.GetResponse).Value
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
				args := deleteRangeArgs(key, roachpb.Key(key).Clone().Next())
				args.Sequence = 2
				return runWithTxn(txn, &args)
			},
			run: func(txn *roachpb.Transaction, key []byte) error {
				args := deleteRangeArgs(key, roachpb.Key(key).Clone().Next())
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
			ctx, tc.Sender(), kvpb.Header{Txn: txn}, &put,
		); pErr != nil {
			t.Fatal(pErr)
		}

		etArgs, etHeader := endTxnArgs(txn, true /* commit */)
		switch i {
		case 0:
			// No deadline.
		case 1:
			// Past deadline.
			etArgs.Deadline = txn.WriteTimestamp.Prev()
		case 2:
			// Equal deadline.
			etArgs.Deadline = txn.WriteTimestamp
		case 3:
			// Future deadline.
			etArgs.Deadline = txn.WriteTimestamp.Next()
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
				retErr, ok := pErr.GetDetail().(*kvpb.TransactionRetryError)
				if !ok || retErr.Reason != kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED {
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

	tc.manualClock.Advance(100)
	pusher := newTransaction(
		"test pusher", key, roachpb.MaxUserPriority, tc.Clock())
	pushReq := pushTxnArgs(pusher, txn, kvpb.PUSH_TIMESTAMP)
	resp, pErr := tc.SendWrapped(&pushReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	updatedPushee := resp.(*kvpb.PushTxnResponse).PusheeTxn
	if updatedPushee.Status != roachpb.PENDING {
		t.Fatalf("expected pushee to still be alive, but got %+v", updatedPushee)
	}

	// Send an EndTxn with a deadline below the point where the txn has been
	// pushed.
	etArgs, etHeader := endTxnArgs(txn, true /* commit */)
	etArgs.Deadline = updatedPushee.WriteTimestamp
	etArgs.Deadline.Logical--
	_, pErr = tc.SendWrappedWith(etHeader, &etArgs)
	const expectedErrMsg = "TransactionRetryError: retry txn \\(RETRY_SERIALIZABLE\\)"
	if pErr == nil {
		t.Fatalf("expected %q, got: nil", expectedErrMsg)
	}
	err := pErr.GoError()
	if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok ||
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
	tc.manualClock.Advance(123)
	pusher := newTransaction("pusher", key, 1, tc.Clock())

	// This pushee should never be allowed to write a txn record because it
	// will be aborted before it even tries.
	pushee := newTransaction("pushee", key, 1, tc.Clock())
	pushReq := pushTxnArgs(pusher, pushee, kvpb.PUSH_ABORT)
	pushReq.Force = true
	resp, pErr := tc.SendWrapped(&pushReq)
	if pErr != nil {
		t.Fatal(pErr)
	}
	abortedPushee := resp.(*kvpb.PushTxnResponse).PusheeTxn
	if abortedPushee.Status != roachpb.ABORTED {
		t.Fatalf("expected push to abort pushee, got %+v", abortedPushee)
	}

	gcHeader := kvpb.RequestHeader{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	}

	// Pretend that the GC queue removes the aborted transaction entry, as it
	// would after a period of inactivity, while our pushee txn is unaware and
	// may have written intents elsewhere.
	{
		gcReq := kvpb.GCRequest{
			RequestHeader: gcHeader,
			Keys: []kvpb.GCRequest_GCKey{
				{Key: keys.TransactionKey(pushee.Key, pushee.ID)},
			},
		}
		if _, pErr := tc.SendWrappedWith(kvpb.Header{RangeID: 1}, &gcReq); pErr != nil {
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
	et.Deadline = txn.WriteTimestamp.Prev()

	ba := &kvpb.BatchRequest{}
	ba.Header = etH
	ba.Add(&put, &et)
	assignSeqNumsForReqs(txn, &put, &et)
	_, pErr := tc.Sender().Send(ctx, ba)
	retErr, ok := pErr.GetDetail().(*kvpb.TransactionRetryError)
	if !ok || retErr.Reason != kvpb.RETRY_COMMIT_DEADLINE_EXCEEDED {
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
	ba := &kvpb.BatchRequest{}
	ba.Header = etH
	ba.Add(&put, &et)
	assignSeqNumsForReqs(txn, &put, &et)
	_, pErr := tc.Sender().Send(ctx, ba)
	if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok {
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
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{
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
	ctx := context.Background()
	tc := testContext{}
	tsc := TestStoreConfig(nil)
	// Don't automatically GC the Txn record: We want to heartbeat the
	// committed Transaction and compare it against our expectations.
	// When it's removed, the heartbeat would recreate it.
	tsc.TestingKnobs.EvalKnobs.DisableTxnAutoGC = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

	key := []byte("a")
	testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
		key = roachpb.Key(key).Next()
		txn := newTransaction("test", key, 1, tc.Clock())
		h := kvpb.Header{Txn: txn}

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
		reply := resp.(*kvpb.EndTxnResponse)
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
		hBR := resp.(*kvpb.HeartbeatTxnResponse)
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
		h := kvpb.Header{Txn: txn}

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
		hBR := resp.(*kvpb.HeartbeatTxnResponse)
		if hBR.Txn.Status != roachpb.PENDING {
			t.Errorf("expected transaction status to be %s, but got %s", hBR.Txn.Status, roachpb.PENDING)
		}

		et, h := endTxnArgs(txn, commit)
		assignSeqNumsForReqs(txn, &et)
		resp, pErr = tc.SendWrappedWith(h, &et)
		if pErr != nil {
			t.Error(pErr)
		}
		reply := resp.(*kvpb.EndTxnResponse)
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
			ctx, tc.Sender(), kvpb.Header{Txn: pushee}, &put,
		); pErr != nil {
			t.Fatal(pErr)
		}

		// Push pushee txn.
		pushTxn := pushTxnArgs(pusher, pushee, kvpb.PUSH_TIMESTAMP)
		pushTxn.Key = pusher.Key
		if _, pErr := tc.SendWrapped(&pushTxn); pErr != nil {
			t.Error(pErr)
		}

		// End the transaction with args timestamp moved forward in time.
		endTxn, h := endTxnArgs(pushee, test.commit)
		assignSeqNumsForReqs(pushee, &endTxn)
		resp, pErr := tc.SendWrappedWith(h, &endTxn)

		if test.expErr {
			if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok {
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
			reply := resp.(*kvpb.EndTxnResponse)
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
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: txn}, &put); pErr != nil {
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
	reply := resp.(*kvpb.EndTxnResponse)
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
				ctx, tc.repl.store.TODOEngine(), txnKey, hlc.Timestamp{}, &existTxnRecord, storage.MVCCWriteOptions{},
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
	err := storage.MVCCPutProto(ctx, tc.repl.store.TODOEngine(), txnKey, hlc.Timestamp{}, &existTxnRec, storage.MVCCWriteOptions{})
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
		cfg.TestingKnobs.DontPushOnLockConflictError = true
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)

		key := []byte("a")
		txn := newTransaction("test", key, 1, tc.Clock())
		put := putArgs(key, key)
		assignSeqNumsForReqs(txn, &put)
		if _, pErr := kv.SendWrappedWith(
			ctx, tc.Sender(), kvpb.Header{Txn: txn}, &put,
		); pErr != nil {
			t.Fatal(pErr)
		}
		// Simulate what the client is supposed to do (update the transaction
		// based on the response). The Writing field is needed by this test.

		// Abort the transaction by pushing it with maximum priority.
		pusher := newTransaction("test", key, 1, tc.Clock())
		pusher.Priority = enginepb.MaxTxnPriority
		pushArgs := pushTxnArgs(pusher, txn, kvpb.PUSH_ABORT)
		if _, pErr := tc.SendWrapped(&pushArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Check that the intent has not yet been resolved.
		ba := &kvpb.BatchRequest{}
		gArgs := getArgs(key)
		ba.Add(&gArgs)
		if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
			t.Fatal(err)
		}
		_, pErr := tc.Sender().Send(ctx, ba)
		if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
			t.Errorf("expected lock conflict error, but got %s", pErr)
		}

		if populateAbortSpan {
			var txnRecord roachpb.Transaction
			txnKey := keys.TransactionKey(txn.Key, txn.ID)
			if ok, err := storage.MVCCGetProto(
				ctx, tc.repl.store.TODOEngine(),
				txnKey, hlc.Timestamp{}, &txnRecord, storage.MVCCGetOptions{},
			); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Fatalf("unexpected txn record %v", txnRecord)
			}

			if pErr := tc.store.intentResolver.ResolveIntents(ctx,
				[]roachpb.LockUpdate{
					roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key}),
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
		reply := resp.(*kvpb.EndTxnResponse)
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
		ba := &kvpb.BatchRequest{}
		ba.Txn = txn
		ba.CanForwardReadTimestamp = noPriorReads
		put := putArgs(key, []byte("value"))
		et, _ := endTxnArgs(txn, true)
		et.LockSpans = []roachpb.Span{{Key: key, EndKey: nil}}
		ba.Add(&put, &et)
		assignSeqNumsForReqs(txn, &put, &et)
		_, pErr := tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		// Replay the request. It initially tries to execute as a 1PC transaction,
		// but will fail because of a WriteTooOldError that pushes the transaction.
		// This forces the txn to execute normally, at which point it fails, either
		// because of a WriteTooOld error, if it cannot server-side refresh, or
		// because the EndTxn is detected to be a duplicate, if it can server-side
		// refresh to avoid the WriteTooOld error. Either way, it fails.
		_, pErr = tc.Sender().Send(ctx, ba)
		require.NotNil(t, pErr)
		var expRx string
		if noPriorReads {
			expRx = `TransactionAbortedError\(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY\)`
		} else {
			expRx = `WriteTooOldError`
		}
		require.Regexp(t, expRx, pErr)
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
	txn := roachpb.MakeTransaction("test", keyA, isolation.Serializable, roachpb.NormalUserPriority,
		tc.Clock().Now(), 0 /* maxOffsetNs */, 0 /* coordinatorNodeID */, 0, false /* omitInRangefeeds */)

	// Write a value outside of the txn to cause a WriteTooOldError later.
	put := putArgs(keyA, []byte("val1"))
	{
		ba := &kvpb.BatchRequest{}
		ba.Add(&put)
		_, pErr := tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)
	}

	ba := &kvpb.BatchRequest{}
	// This put will cause the WriteTooOld flag to be set.
	put = putArgs(keyA, []byte("val2"))
	// This will cause a ConditionFailedError.
	cput := cPutArgs(keyB, []byte("missing"), []byte("newVal"))
	ba.Header = kvpb.Header{Txn: &txn}
	ba.Add(&put)
	ba.Add(&cput)
	assignSeqNumsForReqs(&txn, &put, &cput)
	_, pErr := tc.Sender().Send(ctx, ba)
	require.IsType(t, pErr.GetDetail(), &kvpb.ConditionFailedError{})
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
	cfg.TestingKnobs.DontPushOnLockConflictError = true
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	key := roachpb.Key("a")
	keyB := roachpb.Key("b")
	txn := newTransaction("test", key, 1, tc.Clock())

	// Send a put for keyA.
	ba := &kvpb.BatchRequest{}
	put := putArgs(key, []byte("value"))
	ba.Header = kvpb.Header{Txn: txn, CanForwardReadTimestamp: true}
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
	ba2 := &kvpb.BatchRequest{}
	putB := putArgs(keyB, []byte("value"))
	putTxn := br.Txn.Clone()
	ba2.Header = kvpb.Header{Txn: putTxn}
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
	ok, err := storage.MVCCGetProto(ctx, tc.repl.store.TODOEngine(), txnKey,
		hlc.Timestamp{}, &readTxn, storage.MVCCGetOptions{})
	if err != nil || ok {
		t.Errorf("expected transaction record to be cleared (%t): %+v", ok, err)
	}

	// Now replay put for key A; this succeeds as there's nothing to detect
	// the replay and server-side refreshes were permitted. The transaction's
	// timestamp will be pushed due to the server-side refresh.
	preReplayTxn := ba.Txn.Clone()
	br, pErr = tc.Sender().Send(ctx, ba)
	require.NoError(t, pErr.GoError())
	require.True(t, preReplayTxn.ReadTimestamp.Less(br.Txn.ReadTimestamp))

	// Intent should have been created.
	gArgs := getArgs(key)
	_, pErr = tc.SendWrapped(&gArgs)
	if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
		t.Errorf("expected LockConflictError, got: %v", pErr)
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
	if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
		t.Errorf("expected LockConflictError, got: %v", pErr)
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
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			// Make sure the direct GC path doesn't interfere with this test.
			if filterArgs.Req.Method() == kvpb.GC {
				return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
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
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: txn}, &put); pErr != nil {
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
		ok, err := storage.MVCCGetProto(ctx, tc.repl.store.TODOEngine(), txnKey, hlc.Timestamp{},
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
	h := kvpb.Header{Txn: txn}
	assignSeqNumsForReqs(txn, &pArgs)
	if _, pErr := kv.SendWrappedWith(context.Background(), tc.Sender(), h, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}

	{
		ba := &kvpb.BatchRequest{}
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
	tsc.TestingKnobs.DontPushOnLockConflictError = true
	key := roachpb.Key("a")
	splitKey := roachpb.RKey(key).Next()
	tsc.TestingKnobs.EvalKnobs.TestingEvalFilter =
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if filterArgs.Req.Method() == kvpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
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
		ba := &kvpb.BatchRequest{}
		ba.Header.RangeID = newRepl.RangeID
		gArgs := getArgs(splitKey)
		ba.Add(&gArgs)
		if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
			t.Fatal(err)
		}
		_, pErr := newRepl.Send(ctx, ba)
		if _, ok := pErr.GetDetail().(*kvpb.LockConflictError); !ok {
			t.Errorf("expected lock conflict error, but got %s", pErr)
		}
	}

	hbArgs, h := heartbeatArgs(txn, tc.Clock().Now())
	reply, pErr := tc.SendWrappedWith(h, &hbArgs)
	if pErr != nil {
		t.Fatal(pErr)
	}
	hbResp := reply.(*kvpb.HeartbeatTxnResponse)
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
				var gr kvpb.GetResponse
				if _, err := batcheval.Get(
					ctx, tc.engine, batcheval.CommandArgs{
						EvalCtx: NewReplicaEvalContext(
							ctx, tc.repl, allSpans(), false, /* requiresClosedTSOlderThanStorageSnap */
							kvpb.AdmissionHeader{},
						),
						Args: &kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{
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
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if filterArgs.Req.Method() == kvpb.ResolveIntent &&
				filterArgs.Req.Header().Key.Equal(splitKey.AsRawKey()) {
				atomic.AddInt64(&count, 1)
				return kvpb.NewErrorWithTxn(errors.Errorf("boom"), filterArgs.Hdr.Txn)
			} else if filterArgs.Req.Method() == kvpb.GC {
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

			ba := &kvpb.BatchRequest{}
			ba.Header = etH
			ba.Add(&put, &et)
			br, err := tc.Sender().Send(ctx, ba)
			if err != nil {
				t.Fatalf("commit=%t: %+v", commit, err)
			}
			etArgs, ok := br.Responses[len(br.Responses)-1].GetInner().(*kvpb.EndTxnResponse)
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
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if filterArgs.Req.Method() == kvpb.Put &&
				injectErrorOnKey.Load().(roachpb.Key).Equal(filterArgs.Req.Header().Key) {
				return kvpb.NewErrorf("injected error")
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
			ba := &kvpb.BatchRequest{}
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
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	ba.Add(&put)
	assignSeqNumsForReqs(txn, &put)
	if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	et, etH := endTxnArgs(txn, true)
	et.Require1PC = true
	ba = &kvpb.BatchRequest{}
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

		inc := func(actor *roachpb.Transaction, k roachpb.Key) (*kvpb.IncrementResponse, *kvpb.Error) {
			incArgs := &kvpb.IncrementRequest{
				RequestHeader: kvpb.RequestHeader{Key: k}, Increment: 123,
			}
			assignSeqNumsForReqs(actor, incArgs)
			reply, pErr := kv.SendWrappedWith(ctx, tc.store, kvpb.Header{
				Txn:     actor,
				RangeID: 1,
			}, incArgs)
			if pErr != nil {
				return nil, pErr
			}
			return reply.(*kvpb.IncrementResponse), nil
		}

		get := func(actor *roachpb.Transaction, k roachpb.Key) *kvpb.Error {
			gArgs := getArgs(k)
			assignSeqNumsForReqs(actor, &gArgs)
			_, pErr := kv.SendWrappedWith(ctx, tc.store, kvpb.Header{
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
		var assert func(*kvpb.Error) error
		if abort {
			// Write/Write conflict will abort pushee.
			if _, pErr := inc(pusher, key); pErr != nil {
				t.Fatal(pErr)
			}
			assert = func(pErr *kvpb.Error) error {
				if _, ok := pErr.GetDetail().(*kvpb.TransactionAbortedError); !ok {
					return errors.Errorf("abort=%t: expected txn abort, got %s", abort, pErr)
				}
				return nil
			}
		} else {
			// Verify we're not poisoned.
			assert = func(pErr *kvpb.Error) error {
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

// TestAbortSpanError verifies that kvpb.Errors returned by checkIfTxnAborted
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

	ec := newEvalContextImpl(ctx, tc.repl, false /* requireClosedTS */, kvpb.AdmissionHeader{})
	rec := &SpanSetReplicaEvalContext{ec, *allSpans()}
	pErr := checkIfTxnAborted(ctx, rec, tc.engine, txn)
	if _, ok := pErr.GetDetail().(*kvpb.TransactionAbortedError); ok {
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

	args := pushTxnArgs(pusher, pushee, kvpb.PUSH_ABORT)
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
	testutils.RunTrueAndFalse(t, "disable-auto-gc", func(t *testing.T, disableAutoGC bool) {
		// Test for COMMITTED and ABORTED transactions.
		testutils.RunTrueAndFalse(t, "commit", func(t *testing.T, commit bool) {
			ctx := context.Background()
			tc := testContext{}
			tsc := TestStoreConfig(nil)
			tsc.TestingKnobs.EvalKnobs.DisableTxnAutoGC = disableAutoGC
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.StartWithStoreConfig(ctx, t, stopper, tsc)

			key := roachpb.Key(fmt.Sprintf("key-%t-%t", disableAutoGC, commit))
			pusher := newTransaction("test", key, 1, tc.Clock())
			pushee := newTransaction("test", key, 1, tc.Clock())

			// Begin the pushee's transaction.
			put := putArgs(key, key)
			assignSeqNumsForReqs(pushee, &put)
			if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: pushee}, &put); pErr != nil {
				t.Fatal(pErr)
			}
			// End the pushee's transaction.
			etArgs, h := endTxnArgs(pushee, commit)
			assignSeqNumsForReqs(pushee, &etArgs)
			if _, pErr := tc.SendWrappedWith(h, &etArgs); pErr != nil {
				t.Fatal(pErr)
			}

			// Now try to push what's already committed or aborted.
			args := pushTxnArgs(pusher, pushee, kvpb.PUSH_ABORT)
			resp, pErr := tc.SendWrapped(&args)
			if pErr != nil {
				t.Fatal(pErr)
			}
			reply := resp.(*kvpb.PushTxnResponse)

			// We expect the push to return an ABORTED transaction record for all
			// cases except when the transaction is COMMITTED and its record is not
			// GCed. The case where it is COMMITTED and its record is GCed can be
			// surprising, but doesn't result in problems because a transaction must
			// resolve all of its intents before garbage collecting its record, so
			// the pusher won't end up removing a still-pending intent for a
			// COMMITTED transaction.
			expStatus := roachpb.ABORTED
			if commit && disableAutoGC {
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
		args := pushTxnArgs(pusher, pushee, kvpb.PUSH_ABORT)

		// Set header timestamp to the maximum of the pusher and pushee timestamps.
		h := kvpb.Header{Timestamp: args.PushTo}
		h.Timestamp.Forward(pushee.WriteTimestamp)
		resp, pErr := tc.SendWrappedWith(h, &args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*kvpb.PushTxnResponse)
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
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: pushee}, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Make sure the pushee in the request has updated information on the pushee.
	// Since the pushee has higher priority than the pusher, the push should fail.
	pushee.Priority = 4
	args := pushTxnArgs(pusher, pushee, kvpb.PUSH_ABORT)

	_, pErr := tc.SendWrapped(&args)
	if pErr == nil {
		t.Fatalf("unexpected push success")
	}
	if _, ok := pErr.GetDetail().(*kvpb.TransactionPushError); !ok {
		t.Errorf("expected txn push error: %s", pErr)
	}
}

// TestPushTxnHeartbeatTimeout verifies that a txn which hasn't been
// heartbeat within its transaction liveness threshold can be pushed/aborted.
func TestPushTxnHeartbeatTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
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
		pushType        kvpb.PushTxnType
		expErr          string
	}{
		// Avoid using offsets that result in outcomes that depend on logical
		// ticks.
		{roachpb.PENDING, 0, 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, 0, 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, 0, 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, 0, ns, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, 0, ns, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, 0, ns, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, 0, m*ns - 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, 0, m*ns - 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, 0, m*ns - 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, 0, m*ns + 1, kvpb.PUSH_TIMESTAMP, noError},
		{roachpb.PENDING, 0, m*ns + 1, kvpb.PUSH_ABORT, noError},
		{roachpb.PENDING, 0, m*ns + 1, kvpb.PUSH_TOUCH, noError},
		{roachpb.PENDING, ns, m*ns + 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, ns, m*ns + 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, ns, m*ns + 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns - 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns - 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns - 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.PENDING, ns, (m+1)*ns + 1, kvpb.PUSH_TIMESTAMP, noError},
		{roachpb.PENDING, ns, (m+1)*ns + 1, kvpb.PUSH_ABORT, noError},
		{roachpb.PENDING, ns, (m+1)*ns + 1, kvpb.PUSH_TOUCH, noError},
		// If the transaction record is STAGING then any case that previously
		// returned a TransactionPushError will continue to return that error,
		// but any case that previously succeeded in pushing the transaction
		// will now return an IndeterminateCommitError.
		{roachpb.STAGING, 0, 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, 0, 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, 0, 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, 0, ns, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, 0, ns, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, 0, ns, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, 0, m*ns - 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, 0, m*ns - 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, 0, m*ns - 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, 0, m*ns + 1, kvpb.PUSH_TIMESTAMP, indetCommitError},
		{roachpb.STAGING, 0, m*ns + 1, kvpb.PUSH_ABORT, indetCommitError},
		{roachpb.STAGING, 0, m*ns + 1, kvpb.PUSH_TOUCH, indetCommitError},
		{roachpb.STAGING, ns, m*ns + 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, ns, m*ns + 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, ns, m*ns + 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns - 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns - 1, kvpb.PUSH_ABORT, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns - 1, kvpb.PUSH_TOUCH, txnPushError},
		{roachpb.STAGING, ns, (m+1)*ns + 1, kvpb.PUSH_TIMESTAMP, indetCommitError},
		{roachpb.STAGING, ns, (m+1)*ns + 1, kvpb.PUSH_ABORT, indetCommitError},
		{roachpb.STAGING, ns, (m+1)*ns + 1, kvpb.PUSH_TOUCH, indetCommitError},
		// Even when a transaction record doesn't exist, if the timestamp
		// from the PushTxn request indicates sufficiently recent client
		// activity, the push will fail.
		{-1, 0, 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{-1, 0, 1, kvpb.PUSH_ABORT, txnPushError},
		{-1, 0, 1, kvpb.PUSH_TOUCH, txnPushError},
		{-1, 0, ns, kvpb.PUSH_TIMESTAMP, txnPushError},
		{-1, 0, ns, kvpb.PUSH_ABORT, txnPushError},
		{-1, 0, ns, kvpb.PUSH_TOUCH, txnPushError},
		{-1, 0, m*ns - 1, kvpb.PUSH_TIMESTAMP, txnPushError},
		{-1, 0, m*ns - 1, kvpb.PUSH_ABORT, txnPushError},
		{-1, 0, m*ns - 1, kvpb.PUSH_TOUCH, txnPushError},
		{-1, 0, m*ns + 1, kvpb.PUSH_TIMESTAMP, noError},
		{-1, 0, m*ns + 1, kvpb.PUSH_ABORT, noError},
		{-1, 0, m*ns + 1, kvpb.PUSH_TOUCH, noError},
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
		h := kvpb.Header{Timestamp: args.PushTo}

		// Set the manual clock to the txn start time + offset. This is the time
		// source used to detect transaction expiration. We make sure to set it
		// above h.Timestamp to avoid it being updated by the request.
		now := pushee.ReadTimestamp.Add(test.timeOffset, 0)
		tc.manualClock.MustAdvanceTo(now.GoTime())

		reply, pErr := tc.SendWrappedWith(h, &args)
		if !testutils.IsPError(pErr, test.expErr) {
			t.Fatalf("%d: expected error %q; got %v, args=%+v, reply=%+v", i, test.expErr, pErr, args, reply)
		}
		if reply != nil {
			if txn := reply.(*kvpb.PushTxnResponse).PusheeTxn; txn.Status != roachpb.ABORTED {
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
	pa := pushTxnArgs(txn, txnPushee, kvpb.PUSH_ABORT)
	pa.Force = true
	var ms enginepb.MVCCStats
	var ra kvpb.ResolveIntentRequest
	var rra kvpb.ResolveIntentRangeRequest

	h := kvpb.Header{Txn: txn, Timestamp: tc.Clock().Now()}
	// Should not be able to push or resolve in a transaction.
	if _, err := batcheval.PushTxn(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: h, Args: &pa}, &kvpb.PushTxnResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional PushTxn returned unexpected error: %+v", err)
	}
	if _, err := batcheval.ResolveIntent(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: h, Args: &ra}, &kvpb.ResolveIntentResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntent returned unexpected error: %+v", err)
	}
	if _, err := batcheval.ResolveIntentRange(ctx, b, batcheval.CommandArgs{Stats: &ms, Header: h, Args: &rra}, &kvpb.ResolveIntentRangeResponse{}); !testutils.IsError(err, batcheval.ErrTransactionUnsupported.Error()) {
		t.Fatalf("transactional ResolveIntentRange returned unexpected error: %+v", err)
	}

	// Should not get a transaction back from PushTxn. It used to erroneously
	// return args.PusherTxn.
	h = kvpb.Header{Timestamp: tc.Clock().Now()}
	var reply kvpb.PushTxnResponse
	ec := newEvalContextImpl(
		ctx,
		tc.repl,
		false, /* requireClosedTS */
		kvpb.AdmissionHeader{},
	)
	if _, err := batcheval.PushTxn(ctx, b, batcheval.CommandArgs{EvalCtx: ec, Stats: &ms, Header: h, Args: &pa}, &reply); err != nil {
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
		pushType                       kvpb.PushTxnType
		expSuccess                     bool
	}{
		// Pusher with higher priority succeeds.
		{enginepb.MaxTxnPriority, enginepb.MinTxnPriority, ts1, ts1, kvpb.PUSH_TIMESTAMP, true},
		{enginepb.MaxTxnPriority, enginepb.MinTxnPriority, ts1, ts1, kvpb.PUSH_ABORT, true},
		// Pusher with lower priority fails.
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts1, kvpb.PUSH_ABORT, false},
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts1, kvpb.PUSH_TIMESTAMP, false},
		// Pusher with lower priority fails, even with older txn timestamp.
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts2, kvpb.PUSH_ABORT, false},
		// Pusher has lower priority, but older txn timestamp allows success if
		// !abort since there's nothing to do.
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts2, kvpb.PUSH_TIMESTAMP, true},
		// When touching, priority never wins.
		{enginepb.MaxTxnPriority, enginepb.MinTxnPriority, ts1, ts1, kvpb.PUSH_TOUCH, false},
		{enginepb.MinTxnPriority, enginepb.MaxTxnPriority, ts1, ts1, kvpb.PUSH_TOUCH, false},
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
		if _, pErr := kv.SendWrappedWith(context.Background(), tc.Sender(), kvpb.Header{Txn: pushee}, &put); pErr != nil {
			t.Fatal(pErr)
		}
		// Now, attempt to push the transaction with intent epoch set appropriately.
		args := pushTxnArgs(pusher, pushee, test.pushType)

		// Set header timestamp to the maximum of the pusher and pushee timestamps.
		h := kvpb.Header{Timestamp: args.PushTo}
		h.Timestamp.Forward(pushee.MinTimestamp)
		_, pErr := tc.SendWrappedWith(h, &args)

		if test.expSuccess != (pErr == nil) {
			t.Errorf("expected success on trial %d? %t; got err %s", i, test.expSuccess, pErr)
		}
		if pErr != nil {
			if _, ok := pErr.GetDetail().(*kvpb.TransactionPushError); !ok {
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
	pusher.WriteTimestamp = now.Add(50, 25)
	pushee.WriteTimestamp = now.Add(5, 1)

	key := roachpb.Key("a")
	put := putArgs(key, key)
	assignSeqNumsForReqs(pushee, &put)
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: pushee}, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, push the transaction using a PUSH_TIMESTAMP push request.
	args := pushTxnArgs(pusher, pushee, kvpb.PUSH_TIMESTAMP)

	resp, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: args.PushTo}, &args)
	if pErr != nil {
		t.Fatalf("unexpected error on push: %s", pErr)
	}
	expTS := pusher.WriteTimestamp
	expTS.Logical++
	reply := resp.(*kvpb.PushTxnResponse)
	if reply.PusheeTxn.WriteTimestamp != expTS {
		t.Errorf("expected timestamp to be pushed to %+v; got %+v", expTS, reply.PusheeTxn.WriteTimestamp)
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
	if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: pushee}, &put); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, push the transaction using a PUSH_TIMESTAMP push request.
	args := pushTxnArgs(pusher, pushee, kvpb.PUSH_TIMESTAMP)

	resp, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: args.PushTo}, &args)
	if pErr != nil {
		t.Fatalf("unexpected pError on push: %s", pErr)
	}
	reply := resp.(*kvpb.PushTxnResponse)
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
	resp, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: pushee}, &put)
	if pErr != nil {
		t.Fatal(pErr)
	}
	pushee.Update(resp.Header().Txn)

	// Try to end the pushee's transaction; should get a retry failure.
	etArgs, h := endTxnArgs(pushee, true /* commit */)
	assignSeqNumsForReqs(pushee, &etArgs)
	_, pErr = tc.SendWrappedWith(h, &etArgs)
	if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok {
		t.Fatalf("expected retry error; got %s", pErr)
	}
	pusheeCopy := *pushee
	pushee.Restart(1, 1, pusher.WriteTimestamp)

	// Next push pushee to advance timestamp of txn record.
	pusher.WriteTimestamp = tc.repl.store.Clock().Now()
	args := pushTxnArgs(pusher, &pusheeCopy, kvpb.PUSH_TIMESTAMP)
	if _, pErr := tc.SendWrapped(&args); pErr != nil {
		t.Fatal(pErr)
	}

	// Try to end pushed transaction at restart timestamp, which is
	// earlier than its now-pushed timestamp. Should fail.
	ba := &kvpb.BatchRequest{}
	ba.Add(&put)
	ba.Add(&etArgs)
	ba.Header.Txn = pushee
	assignSeqNumsForReqs(pushee, &put, &etArgs)
	_, pErr = tc.Sender().Send(ctx, ba)
	if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok {
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
		if _, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		queryIntent := func(
			key []byte,
			txnMeta enginepb.TxnMeta,
			baTxn *roachpb.Transaction,
			expectMatchingTxn bool,
			expectMatchingTxnAndTimestamp bool,
		) {
			t.Helper()
			var h kvpb.Header
			if baTxn != nil {
				h.Txn = baTxn
			} else {
				h.Timestamp = txnMeta.WriteTimestamp
			}
			qiArgs := queryIntentArgs(key, txnMeta, errIfMissing)
			res, pErr := tc.SendWrappedWith(h, &qiArgs)
			if errIfMissing && !expectMatchingTxn {
				ownIntent := baTxn != nil
				if ownIntent && txnMeta.WriteTimestamp.Less(txn.WriteTimestamp) {
					if _, ok := pErr.GetDetail().(*kvpb.TransactionRetryError); !ok {
						t.Fatalf("expected TransactionRetryError, found %v %v", txnMeta, pErr)
					}
				} else {
					if _, ok := pErr.GetDetail().(*kvpb.IntentMissingError); !ok {
						t.Fatalf("expected IntentMissingError, found %v", pErr)
					}
				}
			} else {
				require.Nil(t, pErr)
				qiRes := res.(*kvpb.QueryIntentResponse)
				require.Equal(t, expectMatchingTxn, qiRes.FoundIntent)
				require.Equal(t, expectMatchingTxnAndTimestamp, qiRes.FoundUnpushedIntent)
			}
		}

		for i, baTxn := range []*roachpb.Transaction{nil, txn} {
			// Query the intent with the correct txn meta. Should see intent regardless
			// of whether we're inside the txn or not.
			queryIntent(key1, txn.TxnMeta, baTxn, true, true)

			// Query an intent on a different key for the same transaction. Should not
			// see an intent.
			keyPrevent := roachpb.Key(fmt.Sprintf("%s-%t-%d", key2, errIfMissing, i))
			queryIntent(keyPrevent, txn.TxnMeta, baTxn, false, false)

			// Query the intent with a larger epoch. Should not see an intent.
			largerEpochMeta := txn.TxnMeta
			largerEpochMeta.Epoch++
			queryIntent(key1, largerEpochMeta, baTxn, false, false)

			// Query the intent with a smaller epoch. Should not see an intent.
			smallerEpochMeta := txn.TxnMeta
			smallerEpochMeta.Epoch--
			queryIntent(key1, smallerEpochMeta, baTxn, false, false)

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
			queryIntent(key1, largerTSMeta, largerBATxn, true, true)

			// Query the intent with a smaller timestamp. Should be considered a
			// pushed intent unless we're querying our own intent, in which case the
			// smaller timestamp will be forwarded to the batch header transaction's
			// timestamp and the intent will be considered an unpushed intent.
			smallerTSMeta := txn.TxnMeta
			smallerTSMeta.WriteTimestamp = smallerTSMeta.WriteTimestamp.Prev()
			queryIntent(key1, smallerTSMeta, baTxn, true, baTxn == txn)

			// Query the intent with a larger sequence number. Should not see an intent.
			largerSeqMeta := txn.TxnMeta
			largerSeqMeta.Sequence++
			queryIntent(key1, largerSeqMeta, baTxn, false, false)

			// Query the intent with a smaller sequence number. Should see an intent.
			// See the comment on QueryIntentRequest.Txn for an explanation of why
			// the request behaves like this.
			smallerSeqMeta := txn.TxnMeta
			smallerSeqMeta.Sequence--
			queryIntent(key1, smallerSeqMeta, baTxn, true, true)

			// Perform a write at keyPrevent. The associated intent at this key
			// was queried and found to be missing, so this write should be
			// prevented and pushed to a higher timestamp.
			txnCopy := *txn
			pArgs2 := putArgs(keyPrevent, []byte("value2"))
			assignSeqNumsForReqs(&txnCopy, &pArgs2)
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: &txnCopy}
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
		if _, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Resolve the intents.
	rArgs := &kvpb.ResolveIntentRangeRequest{
		RequestHeader: kvpb.RequestHeader{
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
	sReply := reply.(*kvpb.ScanResponse)
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
	storage.DisableMetamorphicSimpleValueEncoding(t)
	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123))
	sc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))

	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

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
	if _, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txn}, &pArgs); pErr != nil {
		t.Fatal(pErr)
	}
	expMS = baseStats
	expMS.Add(enginepb.MVCCStats{
		LiveBytes:   103,
		KeyBytes:    28,
		ValBytes:    75,
		IntentBytes: 23,
		LiveCount:   2,
		KeyCount:    2,
		ValCount:    2,
		IntentCount: 1,
		LockCount:   1,
	})
	if err := verifyRangeStats(tc.engine, tc.repl.RangeID, expMS); err != nil {
		t.Fatal(err)
	}

	// Resolve the 2nd value.
	rArgs := &kvpb.ResolveIntentRequest{
		RequestHeader: kvpb.RequestHeader{
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
	resp := reply.(*kvpb.GetResponse)
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
	_, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: hlc.MinTimestamp}, &cpArgs)
	if cErr, ok := pErr.GetDetail().(*kvpb.ConditionFailedError); pErr == nil || !ok {
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
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

	var appliedIndex kvpb.RaftIndex
	var sum int64
	for i := int64(1); i <= 10; i++ {
		args := incrementArgs([]byte("a"), i)

		resp, pErr := tc.SendWrapped(args)
		if pErr != nil {
			t.Fatal(pErr)
		}
		reply := resp.(*kvpb.IncrementResponse)
		sum += i

		if reply.NewValue != sum {
			t.Errorf("expected %d, got %d", sum, reply.NewValue)
		}

		tc.repl.mu.RLock()
		newAppliedIndex := tc.repl.shMu.state.RaftAppliedIndex
		tc.repl.mu.RUnlock()
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
		func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
			if filterArgs.Req.Header().Key.Equal(roachpb.Key("boom")) {
				return kvpb.NewError(kvpb.NewReplicaCorruptionError(errors.New("boom")))
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
	_, err := tc.engine.Env().Stat(base.PreventedStartupFile(tc.engine.GetAuxiliaryDir()))
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
			chgs := kvpb.MakeReplicationChanges(typ, roachpb.ReplicationTarget{
				NodeID:  tc.store.Ident.NodeID,
				StoreID: 9999,
			})
			if _, err := tc.repl.ChangeReplicas(
				context.Background(),
				tc.repl.Desc(),
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
// appropriately. Normally, the old value and a lock conflict error
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
		cfg.TestingKnobs.DontPushOnLockConflictError = true
		cfg.TestingKnobs.DisableCanAckBeforeApplication = true
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)

		key := roachpb.Key("a")

		// Get original meta2 descriptor.
		rs, _, err := kv.RangeLookup(ctx, tc.Sender(), key, kvpb.READ_UNCOMMITTED, 0, reverse)
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
		if _, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Now lookup the range; should get the value. Since the lookup is
		// not consistent, there's no LockConflictError. It should return both
		// the committed descriptor and the intent descriptor.
		//
		// Note that 'A' < 'a'.
		newKey := roachpb.Key{'A'}
		rs, _, err = kv.RangeLookup(ctx, tc.Sender(), newKey, kvpb.READ_UNCOMMITTED, 0, reverse)
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
		_, _, err = kv.RangeLookup(ctx, tc.Sender(), newKey, kvpb.CONSISTENT, 0, reverse)
		if !errors.HasType(err, (*kvpb.LockConflictError)(nil)) {
			t.Fatalf("expected LockConflictError, not %s", err)
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

			if _, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txn}, &pArgs); pErr != nil {
				t.Fatal(pErr)
			}
		}

		// Resolve the intents.
		rArgs := &kvpb.ResolveIntentRangeRequest{
			RequestHeader: kvpb.RequestHeader{
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
			kvpb.READ_UNCOMMITTED, 0, true)
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

		if _, pErr := tc.SendWrappedWith(kvpb.Header{Txn: txn}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Test reverse RangeLookup scan with intents.
	for _, c := range testCases {
		rs, _, err := kv.RangeLookup(ctx, tc.Sender(), roachpb.Key(c.key),
			kvpb.READ_UNCOMMITTED, 0, true)
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
			kvpb.CONSISTENT, 0, c.reverse)
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

	// Mock propose to return a kvpb.RaftGroupDeletedError.
	var active int32
	proposeFn := func(fArgs kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if atomic.LoadInt32(&active) == 1 {
			return kvpb.NewError(&kvpb.RaftGroupDeletedError{})
		}
		return nil
	}

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	tc := testContext{manualClock: manual}
	cfg := TestStoreConfig(hlc.NewClockForTesting(manual))
	cfg.TestingKnobs.TestingProposalFilter = proposeFn
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	atomic.StoreInt32(&active, 1)
	gArgs := getArgs(roachpb.Key("a"))
	// Force the read command request a new lease.
	manual.MustAdvanceTo(leaseExpiry(tc.repl))
	_, pErr := kv.SendWrappedWith(ctx, tc.store, kvpb.Header{
		Timestamp: tc.Clock().Now(),
		RangeID:   1,
	}, &gArgs)
	if _, ok := pErr.GetDetail().(*kvpb.RangeNotFoundError); !ok {
		t.Fatalf("expected a RangeNotFoundError, get %s", pErr)
	}
}

func TestIntersectSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	spPt := roachpb.Span{
		Key:    roachpb.Key("asd"),
		EndKey: nil,
	}
	spRn := roachpb.Span{
		Key:    roachpb.Key("c"),
		EndKey: roachpb.Key("x"),
	}

	suffix := roachpb.RKey("abcd")
	spLc := roachpb.Span{
		Key:    keys.MakeRangeKey(roachpb.RKey("c"), suffix, nil),
		EndKey: keys.MakeRangeKey(roachpb.RKey("x"), suffix, nil),
	}
	kl1 := string(spLc.Key)
	kl2 := string(spLc.EndKey)

	for _, tc := range []struct {
		span     roachpb.Span
		from, to string
		exp      []string
	}{
		{span: spRn, from: "", to: "a", exp: []string{"", "", "c", "x"}},
		{span: spRn, from: "", to: "c", exp: []string{"", "", "c", "x"}},
		{span: spRn, from: "a", to: "z", exp: []string{"c", "x"}},
		{span: spRn, from: "c", to: "d", exp: []string{"c", "d", "d", "x"}},
		{span: spRn, from: "c", to: "x", exp: []string{"c", "x"}},
		{span: spRn, from: "d", to: "x", exp: []string{"d", "x", "c", "d"}},
		{span: spRn, from: "d", to: "w", exp: []string{"d", "w", "c", "d", "w", "x"}},
		{span: spRn, from: "c", to: "w", exp: []string{"c", "w", "w", "x"}},
		{span: spRn, from: "w", to: "x", exp: []string{"w", "x", "c", "w"}},
		{span: spRn, from: "x", to: "z", exp: []string{"", "", "c", "x"}},
		{span: spRn, from: "y", to: "z", exp: []string{"", "", "c", "x"}},

		// A local span range always comes back in one piece, either inside
		// or outside of the Range.
		{span: spLc, from: "a", to: "b", exp: []string{"", "", kl1, kl2}},
		{span: spLc, from: "d", to: "z", exp: []string{"", "", kl1, kl2}},
		{span: spLc, from: "f", to: "g", exp: []string{"", "", kl1, kl2}},
		{span: spLc, from: "c", to: "x", exp: []string{kl1, kl2}},
		{span: spLc, from: "a", to: "z", exp: []string{kl1, kl2}},
	} {
		desc := &roachpb.RangeDescriptor{
			StartKey: roachpb.RKey(tc.from),
			EndKey:   roachpb.RKey(tc.to),
		}
		name := fmt.Sprintf("span=%v,desc=%v", tc.span, desc.RSpan())
		t.Run(name, func(t *testing.T) {
			var all []string
			in, out := kvserverbase.IntersectSpan(tc.span, desc)
			if in != nil {
				all = append(all, string(in.Key), string(in.EndKey))
			} else {
				all = append(all, "", "")
			}
			for _, o := range out {
				all = append(all, string(o.Key), string(o.EndKey))
			}
			require.Equal(t, tc.exp, all)
		})
	}

	t.Run("point", func(t *testing.T) {
		desc := &roachpb.RangeDescriptor{
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("z"),
		}
		require.Panics(t, func() {
			_, _ = kvserverbase.IntersectSpan(spPt, desc)
		})
	})
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

	ba := &kvpb.BatchRequest{}
	ba.Txn = newTransaction("test", roachpb.Key("k"), 1, tc.Clock())
	// This one succeeds.
	put := &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k")},
		Value:         roachpb.MakeValueFromString("not nil"),
	}
	// This one fails with a ConditionalPutError, which will populate the
	// returned error's index.
	cput := &kvpb.ConditionalPutRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k")},
		Value:         roachpb.MakeValueFromString("irrelevant"),
		ExpBytes:      nil, // not true after above Put
	}
	// This one is never executed.
	get := &kvpb.GetRequest{
		RequestHeader: kvpb.RequestHeader{Key: roachpb.Key("k")},
	}
	assignSeqNumsForReqs(ba.Txn, put, cput, get)
	ba.Add(put, cput, get)

	_, pErr := tc.Sender().Send(ctx, ba)
	require.NotNil(t, pErr)
	require.NotNil(t, pErr.Index)
	require.Equal(t, int32(1), pErr.Index.Index)
	require.Regexp(t, "unexpected value", pErr)
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
	require.NoError(t, err)

	func() {
		tc.repl.raftMu.Lock()
		defer tc.repl.raftMu.Unlock()
		_, err := tc.store.removeInitializedReplicaRaftMuLocked(ctx, tc.repl, repl.Desc().NextReplicaID, RemoveOptions{
			DestroyData: true,
		})
		require.NoError(t, err)
	}()

	engSnapshot := tc.repl.store.TODOEngine().NewSnapshot()
	defer engSnapshot.Close()

	// If the range is destroyed, only a tombstone key should be there.
	expectedKeys := []roachpb.Key{keys.RangeTombstoneKey(tc.repl.RangeID)}
	actualKeys := []roachpb.Key{}

	require.NoError(t, rditer.IterateReplicaKeySpans(
		ctx, tc.repl.Desc(), engSnapshot, false /* replicatedOnly */, rditer.ReplicatedSpansAll,
		func(iter storage.EngineIterator, _ roachpb.Span) error {
			var err error
			for ok := true; ok && err == nil; ok, err = iter.NextEngineKey() {
				key, err := iter.UnsafeEngineKey()
				require.NoError(t, err)
				actualKeys = append(actualKeys, key.Key.Clone())
			}
			return err
		}))
	require.Equal(t, expectedKeys, actualKeys)
}

// TestQuotaPoolDisabled tests that the no quota is acquired by proposals when
// the quota pool enablement setting is disabled or the flow control mode is
// set to kvflowcontrol.ApplyToAll and kvflowcontrol is enabled.
func TestQuotaPoolDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "enableRaftProposalQuota", func(t *testing.T, quotaPoolSettingEnabled bool) {
		testutils.RunValues(t, "flowControlMode",
			[]kvflowcontrol.ModeT{
				kvflowcontrol.ApplyToElastic,
				kvflowcontrol.ApplyToAll,
			}, func(t *testing.T, flowControlMode kvflowcontrol.ModeT) {
				testutils.RunTrueAndFalse(t, "flowControlEnabled", func(t *testing.T, flowControlEnabled bool) {
					ctx := context.Background()
					propErr := errors.New("proposal error")
					type magicKey struct{}

					// We expect the quota pool to be enabled when both the quota pool
					// setting is enabled and the flow control mode is set to
					// ApplyToElastic or flow control is disabled.
					expectEnabled := quotaPoolSettingEnabled &&
						(flowControlMode == kvflowcontrol.ApplyToElastic || !flowControlEnabled)

					tc := testContext{}
					stopper := stop.NewStopper()
					defer stopper.Stop(ctx)

					tsc := TestStoreConfig(nil /* clock */)
					enableRaftProposalQuota.Override(ctx, &tsc.Settings.SV, quotaPoolSettingEnabled)
					kvflowcontrol.Mode.Override(ctx, &tsc.Settings.SV, flowControlMode)
					kvflowcontrol.Enabled.Override(ctx, &tsc.Settings.SV, flowControlEnabled)
					tsc.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
						// Expect no quota allocation when the quota pool is disabled,
						// otherwise expect some quota for our requests only (some ranges are
						// also disabled selectively).
						if v := args.Ctx.Value(magicKey{}); v != nil && expectEnabled {
							require.NotNil(t, args.QuotaAlloc)
							return kvpb.NewError(propErr)
						} else if !expectEnabled {
							require.Nil(t, args.QuotaAlloc)
						}
						return nil
					}
					tc.StartWithStoreConfig(ctx, t, stopper, tsc)

					// Flush a write all the way through the Raft proposal pipeline to ensure
					// that the replica becomes the Raft leader and sets up its quota pool.
					iArgs := incrementArgs([]byte("a"), 1)
					_, pErr := tc.SendWrapped(iArgs)
					require.Nil(t, pErr)

					// The quota pool shouldn't be initialized if the pool is disabled via
					// either setting.
					if expectEnabled {
						require.NotNil(t, tc.repl.mu.proposalQuota)
					} else {
						require.Nil(t, tc.repl.mu.proposalQuota)
					}

					for i := 0; i < 10; i++ {
						ctx = context.WithValue(ctx, magicKey{}, "foo")
						ba := &kvpb.BatchRequest{}
						pArg := putArgs(roachpb.Key("a"), make([]byte, 1<<10))
						ba.Add(&pArg)
						_, pErr := tc.Sender().Send(ctx, ba)
						if expectEnabled {
							if !testutils.IsPError(pErr, propErr.Error()) {
								t.Fatalf("expected error %v, found %v", propErr, pErr)
							}
						} else {
							require.Nil(t, pErr)
						}
					}
				})
			})
	})
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
	// Override the kvflowcontrol.Mode setting to apply_to_elastic, as when
	// apply_to_all is set (metamorphically), the quota pool will be disabled.
	// See getQuotaPoolEnabledRLocked.
	kvflowcontrol.Mode.Override(ctx, &tsc.Settings.SV, kvflowcontrol.ApplyToElastic)
	tsc.TestingKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if v := args.Ctx.Value(magicKey{}); v != nil {
			minQuotaSize = tc.repl.mu.proposalQuota.ApproximateQuota() + args.QuotaAlloc.Acquired()
			return kvpb.NewError(propErr)
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

	ba := &kvpb.BatchRequest{}
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

	// Override the kvflowcontrol.Mode setting to apply_to_elastic, as when
	// apply_to_all is set (metamorphically), the quota pool will be disabled.
	// See getQuotaPoolEnabledRLocked.
	tsc := TestStoreConfig(nil /* clock */)
	kvflowcontrol.Mode.Override(ctx, &tsc.Settings.SV, kvflowcontrol.ApplyToElastic)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)

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

	if _, err := repl.handleRaftReady(ctx, noSnap); err != nil {
		t.Fatal(err)
	}

	if _, err := repl.handleRaftReady(ctx, noSnap); err != nil {
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
		var indexes []kvpb.RaftIndex

		populateLogs := func(from, to int) []kvpb.RaftIndex {
			var newIndexes []kvpb.RaftIndex
			for i := from; i < to; i++ {
				args := incrementArgs([]byte("a"), int64(i))
				if _, pErr := tc.SendWrapped(args); pErr != nil {
					t.Fatal(pErr)
				}
				idx := repl.GetLastIndex()
				newIndexes = append(newIndexes, idx)
			}
			return newIndexes
		}

		truncateLogs := func(index int) {
			truncateArgs := truncateLogArgs(indexes[index], rangeID)
			if _, err := kv.SendWrappedWith(
				ctx,
				tc.Sender(),
				kvpb.Header{RangeID: 1},
				&truncateArgs,
			); err != nil {
				t.Fatal(err)
			}
			waitForTruncationForTesting(t, repl, indexes[index], looselyCoupled)
		}

		// Populate the log with 10 entries. Save the LastIndex after each write.
		indexes = append(indexes, populateLogs(0, 10)...)

		for i, tc := range []struct {
			lo             kvpb.RaftIndex
			hi             kvpb.RaftIndex
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
				expHitLimit := kvpb.RaftIndex(ents[len(ents)-1].Index) < tc.hi-1
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
		var indexes []kvpb.RaftIndex
		for i := 0; i < 10; i++ {
			args := incrementArgs([]byte("a"), int64(i))

			if _, pErr := tc.SendWrapped(args); pErr != nil {
				t.Fatal(pErr)
			}
			idx := tc.repl.GetLastIndex()
			indexes = append(indexes, idx)
		}

		// Discard the first half of the log.
		truncateArgs := truncateLogArgs(indexes[5], rangeID)
		if _, pErr := tc.SendWrappedWith(kvpb.Header{RangeID: 1}, &truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}
		waitForTruncationForTesting(t, repl, indexes[5], looselyCoupled)

		repl.mu.Lock()
		defer repl.mu.Unlock()

		firstIndex := repl.raftFirstIndexRLocked()
		if firstIndex != indexes[5] {
			t.Fatalf("expected firstIndex %d to be %d", firstIndex, indexes[4])
		}

		// Truncated logs should return an ErrCompacted error.
		if _, err := tc.repl.raftTermLocked(indexes[1]); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}
		if _, err := tc.repl.raftTermLocked(indexes[3]); !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}

		// FirstIndex-1 should return the term of firstIndex.
		firstIndexTerm, err := tc.repl.raftTermLocked(firstIndex)
		if err != nil {
			t.Errorf("expect no error, got %s", err)
		}

		term, err := tc.repl.raftTermLocked(indexes[4])
		if err != nil {
			t.Errorf("expect no error, got %s", err)
		}
		if term != firstIndexTerm {
			t.Errorf("expected firstIndex-1's term:%d to equal that of firstIndex:%d", term, firstIndexTerm)
		}

		lastIndex := repl.raftLastIndexRLocked()

		// Last index should return correctly.
		if _, err := tc.repl.raftTermLocked(lastIndex); err != nil {
			t.Errorf("expected no error, got %s", err)
		}

		// Terms for after the last index should return ErrUnavailable.
		if _, err := tc.repl.raftTermLocked(lastIndex + 1); !errors.Is(err, raft.ErrUnavailable) {
			t.Errorf("expected ErrUnavailable, got %s", err)
		}
		if _, err := tc.repl.raftTermLocked(indexes[9] + 1000); !errors.Is(err, raft.ErrUnavailable) {
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
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

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
	ts1Header := kvpb.Header{RangeID: repl2.RangeID, Timestamp: ts1}
	ts2Header := kvpb.Header{RangeID: repl2.RangeID, Timestamp: ts2}
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
		kvpb.Header{RangeID: 1, Timestamp: tc.Clock().Now()},
		&gcReq,
	); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to incorrect range: %s", pErr)
	}

	// Make sure the key still exists on range 2.
	getReq := getArgs(key)
	if res, pErr := kv.SendWrappedWith(ctx, repl2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*kvpb.GetResponse).Value; resVal == nil {
		t.Errorf("expected value %s to exists after GC to incorrect range but before GC to correct range, found %v", val, resVal)
	}

	// Send GC request to range 2 for the same key.
	gcReq = gcArgs(repl2.Desc().StartKey, repl2.Desc().EndKey, gKey)
	if _, pErr := kv.SendWrappedWith(
		ctx,
		repl2,
		kvpb.Header{RangeID: repl2.RangeID, Timestamp: tc.Clock().Now()},
		&gcReq,
	); pErr != nil {
		t.Errorf("unexpected pError on garbage collection request to correct range: %s", pErr)
	}

	// Make sure the key no longer exists on range 2.
	if res, pErr := kv.SendWrappedWith(ctx, repl2, ts1Header, &getReq); pErr != nil {
		t.Errorf("unexpected pError on get request to correct range: %s", pErr)
	} else if resVal := res.(*kvpb.GetResponse).Value; resVal != nil {
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
					func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
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
			ba := &kvpb.BatchRequest{}
			ba.RangeID = 1
			ba.Add(&kvpb.GetRequest{
				RequestHeader: kvpb.RequestHeader{Key: key},
			})
			if err := ba.SetActiveTimestamp(tc.Clock()); err != nil {
				t.Fatal(err)
			}
			_, _, pErr := tc.repl.executeBatchWithConcurrencyRetries(ctx, ba, (*Replica).executeWriteBatch)
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
				if _, ok := detail.(*kvpb.AmbiguousResultError); !ok {
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
		if v := p.Context().Value(magicKey{}); v != nil {
			cancel()
			return atomic.LoadInt32(&dropProp) == 1, nil
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	ba := &kvpb.BatchRequest{}
	ba.RangeID = 1
	ba.Timestamp = tc.Clock().Now()
	ba.Add(&kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{Key: []byte("acdfg")},
	})
	_, _, pErr := tc.repl.executeBatchWithConcurrencyRetries(ctx, ba, (*Replica).executeWriteBatch)
	if pErr == nil {
		t.Fatal("expected failure, but found success")
	}
	detail := pErr.GetDetail()
	if _, ok := detail.(*kvpb.AmbiguousResultError); !ok {
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
		errStruct *kvpb.ReplicaCorruptionError
		expErr    string
	}{
		{kvpb.NewReplicaCorruptionError(errors.New("")), "replica corruption (processed=false)"},
		{kvpb.NewReplicaCorruptionError(errors.New("foo")), "replica corruption (processed=false): foo"},
		{kvpb.NewReplicaCorruptionError(errors.Wrap(errors.New("bar"), "foo")), "replica corruption (processed=false): foo: bar"},
	} {
		// This uses fmt.Sprint because that ends up calling Error() and is the
		// intended use. A previous version of this test called String() directly
		// which called the wrong (reflection-based) implementation.
		if errStr := fmt.Sprint(tc.errStruct); errStr != tc.expErr {
			t.Errorf("%d: expected '%s' but got '%s'", i, tc.expErr, errStr)
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	if !errors.HasType(kvpb.MaybeWrapReplicaCorruptionError(ctx, errors.New("foo")), &kvpb.ReplicaCorruptionError{}) {
		t.Fatal("MaybeWrapReplicaCorruptionError should wrap a non-ctx err")
	}

	cancel(errors.New("we're done here"))
	if errors.HasType(kvpb.MaybeWrapReplicaCorruptionError(ctx, ctx.Err()), &kvpb.ReplicaCorruptionError{}) {
		t.Fatal("MaybeWrapReplicaCorruptionError should not wrap a ctx err")
	}
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
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

	type magicKey struct{}

	var c int32                                // updated atomically
	var wrongLeaseIndex kvpb.LeaseAppliedIndex // populated below

	tc.repl.mu.Lock()
	tc.repl.mu.proposalBuf.testing.leaseIndexFilter = func(p *ProposalData) (indexOverride kvpb.LeaseAppliedIndex) {
		if v := p.Context().Value(magicKey{}); v != nil {
			if curAttempt := atomic.AddInt32(&c, 1); curAttempt == 1 {
				return wrongLeaseIndex
			}
		}
		return 0
	}
	tc.repl.mu.Unlock()

	pArg := putArgs(roachpb.Key("a"), []byte("asd"))
	{
		ba := &kvpb.BatchRequest{}
		ba.Add(&pArg)
		ba.Timestamp = tc.Clock().Now()
		if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Set the max lease index to that of the recently applied write.
	// Two requests can't have the same lease applied index.
	tc.repl.mu.RLock()
	wrongLeaseIndex = tc.repl.shMu.state.LeaseAppliedIndex
	if wrongLeaseIndex < 1 {
		t.Fatal("committed a few batches, but still at lease index zero")
	}
	tc.repl.mu.RUnlock()

	log.Infof(ctx, "test begins")

	ba := &kvpb.BatchRequest{}
	ba.RangeID = 1
	ba.Timestamp = tc.Clock().Now()
	const expInc = 123
	iArg := incrementArgs(roachpb.Key("b"), expInc)
	ba.Add(iArg)
	{
		_, _, pErr := tc.repl.executeBatchWithConcurrencyRetries(
			context.WithValue(ctx, magicKey{}, "foo"),
			ba,
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
		lease.Sequence++
		lease.ProposedTS = tc.Clock().Now().UnsafeToClockTimestamp()

		ba.Add(&kvpb.RequestLeaseRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: tc.repl.Desc().StartKey.AsRawKey(),
			},
			Lease:     lease,
			PrevLease: prevLease,
		})
		_, _, pErr := tc.repl.executeBatchWithConcurrencyRetries(
			context.WithValue(ctx, magicKey{}, "foo"),
			ba,
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
			log.Infof(p.Context(), "abandoning command")
			return true, nil
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	var chs []chan proposalResult
	const num = 10
	for i := 0; i < num; i++ {
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = tc.Clock().Now()
		ba.Add(&kvpb.PutRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: roachpb.Key(fmt.Sprintf("k%d", i)),
			},
		})
		st := repl.CurrentLeaseStatus(ctx)
		_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		ch, _, id, _, err := repl.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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
	cfg.TestingKnobs.DisableCanAckBeforeApplication = true
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
		if v := p.Context().Value(magicKey{}); v != nil {
			seenCmds = append(seenCmds, int(p.command.MaxLeaseIndex))
		}
		return false, nil
	}
	tc.repl.mu.Unlock()

	const num = 10
	chs := make([]chan proposalResult, 0, num)
	for i := 0; i < num; i++ {
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = tc.Clock().Now()
		ba.Add(&kvpb.PutRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: roachpb.Key(fmt.Sprintf("k%d", i)),
			},
		})
		_, tok := tc.repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		st := tc.repl.CurrentLeaseStatus(ctx)
		ch, _, _, _, err := tc.repl.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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
		if v := p.Context().Value(magicKey{}); v != nil {
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
	curIdx := tc.repl.shMu.state.LeaseAppliedIndex
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
	cfg.RaftTickInterval = time.Hour
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
	reproposalTicks := tc.store.cfg.RaftReproposalTimeoutTicks
	{
		// The verifications of the reproposal counts below rely on r.mu.ticks
		// starting with a value of 0 modulo reproposalTicks.
		r.mu.Lock()
		ticks := r.mu.ticks
		r.mu.Unlock()
		for ; (ticks % reproposalTicks) != 0; ticks++ {
			if _, err := r.tick(ctx, nil, nil); err != nil {
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

	// We tick the replica 3*RaftReproposalTimeoutTicks.
	for i := 0; i < 3*reproposalTicks; i++ {
		// Add another pending command on each iteration.
		id := fmt.Sprintf("%08d", i)
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = tc.Clock().Now()
		ba.Add(&kvpb.PutRequest{RequestHeader: kvpb.RequestHeader{Key: roachpb.Key(id)}})
		st := r.CurrentLeaseStatus(ctx)
		cmd, pErr := r.requestToProposal(ctx, kvserverbase.CmdIDKey(id), ba, allSpansGuard(), &st, uncertainty.Interval{})
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
		iot := ioThresholdMap{m: map[roachpb.StoreID]*admissionpb.IOThreshold{}}
		if _, err := r.tick(ctx, nil, &iot); err != nil {
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

		// Reproposals are only performed every reproposalTicks, and only for
		// commands proposed at least reproposalTicks ago (inclusive). The first
		// time, this will be 1 reproposal (the one at ticks=0 for the reproposal at
		// ticks=reproposalTicks), then +reproposalTicks reproposals each time.
		if (ticks % reproposalTicks) == 0 {
			if exp := i + 2 - reproposalTicks; len(reproposed) != exp { // +1 to offset i, +1 for inclusive
				t.Fatalf("%d: expected %d reproposed commands, but found %d", i, exp, len(reproposed))
			}
		} else {
			if len(reproposed) != 0 {
				t.Fatalf("%d: expected no reproposed commands, but found %+v", i, reproposed)
			}
		}
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

	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
	put := putArgs(key, []byte("value"))
	assignSeqNumsForReqs(txn, &put)
	ba.Add(&put)
	if _, err := tc.Sender().Send(ctx, ba); err != nil {
		t.Fatal(err)
	}

	var proposalRecognized int64 // accessed atomically

	r := tc.repl
	r.mu.Lock()
	r.mu.proposalBuf.testing.leaseIndexFilter = func(p *ProposalData) kvpb.LeaseAppliedIndex {
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

	ba = &kvpb.BatchRequest{}
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

// TestMVCCStatsGCCommutesWithWrites tests that the MVCCStats updates
// corresponding to writes and GCs are commutative.
//
// This test does so by:
// 1. Initially writing N versions of a key.
// 2. Concurrently GC-ing the N-1 versions written in step 1 while writing N-1
// new versions of the key.
// 3. Concurrently recomputing MVCC stats (via RecomputeStatsRequests) in the
// background and ensuring that the stats are always consistent at all times.
func TestMVCCStatsGCCommutesWithWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := serverutils.StartCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	key := tc.ScratchRange(t)
	store, err := tc.Server(0).GetStores().(*Stores).GetStore(tc.Server(0).GetFirstStoreID())
	require.NoError(t, err)

	write := func() hlc.Timestamp {
		ba := &kvpb.BatchRequest{}
		put := putArgs(key, []byte("0"))
		ba.Add(&put)
		resp, pErr := store.TestSender().Send(ctx, ba)
		require.Nil(t, pErr)
		return resp.Timestamp
	}

	// Write `numIterations` versions for a key.
	const numIterations = 100
	writeTimestamps := make([]hlc.Timestamp, 0, numIterations)
	for i := 0; i < numIterations; i++ {
		writeTimestamps = append(writeTimestamps, write())
	}

	// Now, we GC the first `numIterations-1` versions we wrote above while
	// concurrently writing `numIterations-1` new versions.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for _, ts := range writeTimestamps[:numIterations-1] {
			gcReq := gcArgs(key, key.Next(), gcKey(key, ts))
			_, pErr := kv.SendWrapped(ctx, store.TestSender(), &gcReq)
			require.Nil(t, pErr)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations-1; i++ {
			write()
		}
	}()
	// Also concurrently recompute stats and ensure that they're consistent at all
	// times.
	go func() {
		defer wg.Done()
		expDelta := enginepb.MVCCStats{}
		for i := 0; i < numIterations; i++ {
			recomputeReq := recomputeStatsArgs(key)
			resp, pErr := kv.SendWrapped(ctx, store.TestSender(), &recomputeReq)
			require.Nil(t, pErr)
			delta := enginepb.MVCCStats(resp.(*kvpb.RecomputeStatsResponse).AddedDelta)
			delta.AgeTo(expDelta.LastUpdateNanos)
			require.Equal(t, expDelta, delta)
		}
	}()

	wg.Wait()
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
	if _, err := tc.SendWrappedWith(kvpb.Header{
		Timestamp: ts1,
	}, &gArgs); err != nil {
		t.Fatalf("could not get data: %+v", err)
	}
	// Verify a later Get works.
	if _, err := tc.SendWrappedWith(kvpb.Header{
		Timestamp: ts3,
	}, &gArgs); err != nil {
		t.Fatalf("could not get data: %+v", err)
	}

	// Put some data for use with CP later on.
	pArgs := putArgs(keycp, va)
	if _, err := tc.SendWrappedWith(kvpb.Header{
		Timestamp: ts1,
	}, &pArgs); err != nil {
		t.Fatalf("could not put data: %+v", err)
	}

	// Do a GC.
	gcr := kvpb.GCRequest{
		Threshold: ts2,
	}
	if _, err := tc.SendWrappedWith(kvpb.Header{RangeID: 1}, &gcr); err != nil {
		t.Fatal(err)
	}

	// Do the same Get, which should now fail.
	if _, pErr := tc.SendWrappedWith(kvpb.Header{
		Timestamp: ts1,
	}, &gArgs); !testutils.IsPError(pErr, `batch timestamp 0.\d+,\d+ must be after replica GC threshold 0.\d+,\d+`) {
		t.Fatalf("unexpected error: %v", pErr)
	}

	// Verify a later Get works.
	if _, pErr := tc.SendWrappedWith(kvpb.Header{
		Timestamp: ts3,
	}, &gArgs); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify an early CPut fails.
	cpArgs := cPutArgs(keycp, vb, va)
	if _, pErr := tc.SendWrappedWith(kvpb.Header{
		Timestamp: ts2,
	}, &cpArgs); !testutils.IsPError(pErr, `batch timestamp 0.\d+,\d+ must be after replica GC threshold 0.\d+,\d+`) {
		t.Fatalf("unexpected error: %v", pErr)
	}
	// Verify a later CPut works.
	if _, pErr := tc.SendWrappedWith(kvpb.Header{
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
		var refresh kvpb.Request
		if ranged {
			refresh = &kvpb.RefreshRangeRequest{
				RequestHeader: kvpb.RequestHeader{Key: keyA, EndKey: keyB},
				RefreshFrom:   ts2,
			}
		} else {
			refresh = &kvpb.RefreshRequest{
				RequestHeader: kvpb.RequestHeader{Key: keyA},
				RefreshFrom:   ts2,
			}
		}
		txn := roachpb.MakeTransaction("test", keyA, 0, 0, ts2, 0, 0, 0, false /* omitInRangefeeds */)
		txn.BumpReadTimestamp(ts4)

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
					gcr := kvpb.GCRequest{Threshold: testCase.gc}
					_, pErr := tc.SendWrappedWith(kvpb.Header{Timestamp: testCase.gc}, &gcr)
					require.Nil(t, pErr)
				}

				_, pErr := tc.SendWrappedWith(kvpb.Header{Txn: &txn}, refresh)
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
//   - followerRead: configures whether the read should be served from the
//     leaseholder replica or from a follower replica.
//
//   - thresholdFirst: configures whether the GC operation should be split into
//     two requests, with the first bumping the GC threshold and the second
//     GCing the expired version. This is how the real MVCC GC queue works.
func TestGCThresholdRacesWithRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "followerRead", func(t *testing.T, followerRead bool) {
		testutils.RunTrueAndFalse(t, "thresholdFirst", func(t *testing.T, thresholdFirst bool) {
			ctx := context.Background()
			tc := serverutils.StartCluster(t, 2, base.TestClusterArgs{
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
			h1 := kvpb.Header{RangeID: desc.RangeID, Timestamp: ts1}
			h2 := kvpb.Header{RangeID: desc.RangeID, Timestamp: ts2}
			h3 := kvpb.Header{RangeID: desc.RangeID, Timestamp: ts3}
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
					ba := &kvpb.BatchRequest{}
					ba.RangeID = desc.RangeID
					ba.ReadConsistency = kvpb.INCONSISTENT
					ba.Add(&kvpb.QueryResolvedTimestampRequest{
						RequestHeader: kvpb.RequestHeader{Key: key, EndKey: key.Next()},
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
			require.NotNil(t, resp.(*kvpb.GetResponse).Value)
			b, err := resp.(*kvpb.GetResponse).Value.GetBytes()
			require.Nil(t, err)
			require.Equal(t, va, b)

			// Since the GC request does not acquire latches on the keys being GC'ed,
			// they're not guaranteed to wait for these above Puts to get applied on
			// the leaseholder. See AckCommittedEntriesBeforeApplication() and the the
			// comment above it for more details. So we separately ensure both these
			// Puts have been applied by just trying to read the latest value @ ts2.
			// These Get requests do indeed declare latches on the keys being read, so
			// by the time they return, subsequent GC requests are guaranteed to see
			// the latest keys.
			gArgs = getArgs(key)
			_, pErr = kv.SendWrappedWith(ctx, reader, h2, &gArgs)
			require.Nil(t, pErr)

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
					require.NotNil(t, resp.(*kvpb.GetResponse).Value)
					b, err := resp.(*kvpb.GetResponse).Value.GetBytes()
					require.Nil(t, err)
					require.Equal(t, va, b)
				} else {
					t.Logf("read lost race: %v", pErr)
					gcErr := &kvpb.BatchTimestampBeforeGCError{}
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

// BenchmarkMVCCGCWithForegroundTraffic benchmarks performing GC of a key
// concurrently with reads on that key.
func BenchmarkMVCCGCWithForegroundTraffic(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123))
	tc.StartWithStoreConfig(ctx, b, stopper, sc)

	key := roachpb.Key("test")

	// send sends the Request with a present-time batch timestamp.
	send := func(args kvpb.Request) *kvpb.BatchResponse {
		var header kvpb.Header
		header.Timestamp = tc.Clock().Now()
		ba := &kvpb.BatchRequest{}
		ba.Header = header
		ba.Add(args)
		resp, err := tc.Sender().Send(ctx, ba)
		require.Nil(b, err, "%+v", err)
		return resp
	}

	// gc issues a GC request to garbage collect `key` at `timestamp` with a
	// present-time batch header timestamp.
	gc := func(key roachpb.Key, timestamp hlc.Timestamp) {
		// Note that we're not bumping the GC threshold, just GC'ing the keys.
		gcReq := gcArgs(key, key.Next(), gcKey(key, timestamp))
		send(&gcReq)
	}

	// read issues a present time read over `key`.
	read := func() {
		send(scanArgs(key, key.Next()))
	}

	// put issues a present time put over `key`.
	put := func(key roachpb.Key) (writeTS hlc.Timestamp) {
		putReq := putArgs(key, []byte("00"))
		resp := send(&putReq)
		return resp.Timestamp
	}

	// Issue no-op GC requests every 10 microseconds while reads are being
	// benchmarked.
	b.Run("noop gc with reads", func(b *testing.B) {
		var wg sync.WaitGroup
		wg.Add(2)
		doneCh := make(chan struct{}, 1)

		b.ResetTimer()
		go func() {
			defer wg.Done()
			for {
				gc(key, tc.Clock().Now()) // NB: These are no-op GC requests.
				time.Sleep(10 * time.Microsecond)

				select {
				case <-doneCh:
					return
				default:
				}
			}
		}()

		go func() {
			for i := 0; i < b.N; i++ {
				read()
			}
			close(doneCh)
			wg.Done()
		}()
		wg.Wait()
	})

	// Write and GC the same key indefinitely while benchmarking read performance.
	b.Run("gc with reads and writes", func(b *testing.B) {
		var wg sync.WaitGroup
		wg.Add(2)
		doneCh := make(chan struct{}, 1)
		lastWriteTS := put(key)

		b.ResetTimer()
		go func() {
			defer wg.Done()
			for {
				// Write a new version and immediately GC the previous version.
				writeTS := put(key)
				gc(key, lastWriteTS)
				lastWriteTS = writeTS

				select {
				case <-doneCh:
					return
				default:
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				read()
			}
			close(doneCh)
		}()
		wg.Wait()
	})

	// Write a bunch of versions of a key. Then, GC them while concurrently
	// reading those keys.
	b.Run("gc with reads", func(b *testing.B) {
		var wg sync.WaitGroup
		wg.Add(2)
		doneCh := make(chan struct{}, 1)

		writeTimestamps := make([]hlc.Timestamp, 0, b.N)
		for i := 0; i < b.N; i++ {
			writeTimestamps = append(writeTimestamps, put(key))
		}
		put(key)

		b.ResetTimer()
		go func() {
			defer wg.Done()
			for _, ts := range writeTimestamps {
				gc(key, ts)

				// Stop GC-ing once the reads are done and we're shutting down.
				select {
				case <-doneCh:
					return
				default:
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				read()
			}
			close(doneCh)
		}()
		wg.Wait()
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
		ba := &kvpb.BatchRequest{}
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

	ba := &kvpb.BatchRequest{}
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

	ba := &kvpb.BatchRequest{}
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

	_, batch, _, _, _, pErr := tc.repl.evaluateWriteBatch(ctx, raftlog.MakeCmdIDKey(), ba, allSpansGuard(), nil, uncertainty.Interval{})
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

	progress := func(vals ...uint64) map[raftpb.PeerID]tracker.Progress {
		m := make(map[raftpb.PeerID]tracker.Progress)
		for i, v := range vals {
			m[raftpb.PeerID(i+1)] = tracker.Progress{Match: v}
		}
		return m
	}
	status := func(lead raftpb.PeerID, progress map[raftpb.PeerID]tracker.Progress) *raft.SparseStatus {
		status := &raft.SparseStatus{
			Progress: progress,
		}
		// The commit index is set so that a progress.Match value of 1 is behind
		// and 2 is ok.
		status.HardState.Commit = 12
		if lead == 1 {
			status.SoftState.RaftState = raftpb.StateLeader
		} else {
			status.SoftState.RaftState = raftpb.StateFollower
		}
		status.HardState.Lead = lead
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
	live := func(ids ...roachpb.NodeID) livenesspb.NodeVitalityInterface {
		return livenesspb.TestCreateNodeVitality(ids...)
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
		raftStatus  *raft.SparseStatus
		liveness    livenesspb.NodeVitalityInterface
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
			metrics := calcReplicaMetrics(calcReplicaMetricsInput{
				raftCfg:            &cfg.RaftConfig,
				conf:               spanConfig,
				vitalityMap:        c.liveness.ScanNodeVitalityFromCache(),
				desc:               &c.desc,
				raftStatus:         c.raftStatus,
				storeID:            c.storeID,
				quiescent:          c.expected.Quiescent,
				ticking:            c.expected.Ticking,
				raftLogSize:        c.raftLogSize,
				raftLogSizeTrusted: true,
			})
			require.Equal(t, c.expected, metrics)
		})
	}
}

func TestCalcQuotaPoolPercentUsed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const mb = 1 << 20

	for _, tc := range []struct {
		qpUsed, qpCap int64
		exp           int64
	}{
		{0, 0, 0},
		{0, 16 * mb, 0},
		{1, 16 * mb, 0},
		{8 * mb, 16 * mb, 50},
		{16 * mb, 16 * mb, 100},
		{16*mb - 1, 16 * mb, 100},
		{32 * mb, 16 * mb, 200},
	} {
		assert.Equal(t, tc.exp, calcQuotaPoolPercentUsed(tc.qpUsed, tc.qpCap), "%+v", tc)
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
		if _, ok := p.Request.GetArg(kvpb.Increment); ok {
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

	errChan := make(chan *kvpb.Error, 1)
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
	require.NoError(t, tc.store.RemoveReplica(ctx, tc.repl, tc.repl.Desc().NextReplicaID,
		RemoveOptions{DestroyData: true}))
	pErr := <-errChan
	if _, ok := pErr.GetDetail().(*kvpb.AmbiguousResultError); !ok {
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
	rh := kvpb.RequestHeader{Key: roachpb.Key("a")}
	txn := newTransaction(
		"name",
		rh.Key,
		roachpb.NormalUserPriority,
		cfg.Clock,
	)

	getReq := &kvpb.GetRequest{
		RequestHeader: rh,
	}
	putReq := &kvpb.PutRequest{
		RequestHeader: rh,
		Value:         roachpb.MakeValueFromBytes([]byte("val")),
	}
	deleteReq := &kvpb.DeleteRequest{
		RequestHeader: rh,
	}
	endTxnCommitReq := &kvpb.EndTxnRequest{
		RequestHeader: rh,
		Commit:        true,
	}
	endTxnAbortReq := &kvpb.EndTxnRequest{
		RequestHeader: rh,
		Commit:        true,
	}
	hbTxnReq := &kvpb.HeartbeatTxnRequest{
		RequestHeader: rh,
		Now:           cfg.Clock.Now(),
	}
	pushTxnReq := &kvpb.PushTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.TxnMeta.Key,
		},
		PusheeTxn: txn.TxnMeta,
		PushType:  kvpb.PUSH_ABORT,
		Force:     true,
	}
	resolveCommittedIntentReq := &kvpb.ResolveIntentRequest{
		RequestHeader: rh,
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.COMMITTED,
		Poison:        false,
	}
	resolveAbortedIntentReq := &kvpb.ResolveIntentRequest{
		RequestHeader: rh,
		IntentTxn:     txn.TxnMeta,
		Status:        roachpb.ABORTED,
		Poison:        true,
	}
	barrierReq := &kvpb.BarrierRequest{
		RequestHeader: rh,
	}

	sendReq := func(
		ctx context.Context, repl *Replica, req kvpb.Request, txn *roachpb.Transaction,
	) *kvpb.Error {
		ba := &kvpb.BatchRequest{}
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
		setup       func(context.Context, *Replica) *kvpb.Error // optional
		useTxn      bool
		req         kvpb.Request
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
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
				return sendReq(ctx, repl, hbTxnReq, txn)
			},
			useTxn:      true,
			req:         endTxnCommitReq,
			expProposal: true,
		},
		{
			name: "end txn (abort) with auto-gc, with existing record",
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
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
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
				return sendReq(ctx, repl, hbTxnReq, txn)
			},
			req:         pushTxnReq,
			expProposal: true,
		},
		{
			name: "redundant push txn req",
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
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
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
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
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
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
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
				return sendReq(ctx, repl, resolveAbortedIntentReq, nil /* txn */)
			},
			req: resolveAbortedIntentReq,
			// No-op - the abort span has already been poisoned.
			expProposal: false,
		},
		{
			name: "barrier",
			setup: func(ctx context.Context, repl *Replica) *kvpb.Error {
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
				func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
					if args.Req.Timestamp == markerTS {
						atomic.AddInt32(&propCount, 1)
					}
					return nil
				}
			repl.mu.Unlock()

			ba := &kvpb.BatchRequest{}
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
	kvserverbase.MaxCommandSize.Override(ctx, &st.SV, 1024)

	args := putArgs(roachpb.Key("k"),
		[]byte(strings.Repeat("a", int(kvserverbase.MaxCommandSize.Get(&st.SV)))))
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
	storeKnobs.TestingApplyCalledTwiceFilter = func(filterArgs kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		if atomic.LoadInt32(&filterActive) == 1 {
			return 0, kvpb.NewErrorf("boom")
		}
		return 0, nil
	}
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storeKnobs}})
	defer s.Stopper().Stop(context.Background())

	splitKey := roachpb.Key("b")
	if err := kvDB.AdminSplit(
		ctx,
		splitKey,
		hlc.MaxTimestamp, /* expirationTime */
	); err != nil {
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
	ba := &kvpb.BatchRequest{}
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
	ch, _, _, _, pErr := repl.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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
	tsc.TestingKnobs.TestingApplyCalledTwiceFilter =
		func(filterArgs kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
			if atomic.LoadInt32(&filterActive) == 1 {
				<-blockRaftApplication
			}
			return 0, nil
		}

	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, tsc)
	repl := tc.repl

	ba := &kvpb.BatchRequest{}
	key := roachpb.Key("a")
	put := putArgs(key, []byte("val"))
	ba.Add(&put)
	ba.Timestamp = tc.Clock().Now()
	ba.AsyncConsensus = true

	atomic.StoreInt32(&filterActive, 1)
	st := tc.repl.CurrentLeaseStatus(ctx)
	_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	ch, _, _, _, pErr := repl.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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
	tsc.TestingKnobs.TestingApplyCalledTwiceFilter =
		func(filterArgs kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
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
	ba := &kvpb.BatchRequest{}
	key := roachpb.Key("a")
	put := putArgs(key, []byte("val"))
	ba.Add(&put)
	ba.Timestamp = tc.Clock().Now()

	atomic.StoreInt32(&filterActive, 1)
	st := repl.CurrentLeaseStatus(ctx)
	_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	_, _, _, _, pErr := repl.evalAndPropose(ctx, ba, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Once that command is stuck applying, propose a number of large commands.
	// This will allow them to all build up without any being applied so that
	// their application will require pagination.
	<-blockingRaftApplication
	var ch chan proposalResult
	for i := 0; i < 50; i++ {
		ba2 := &kvpb.BatchRequest{}
		key := roachpb.Key("a")
		put := putArgs(key, make([]byte, 2*tsc.RaftMaxCommittedSizePerReady))
		ba2.Add(&put)
		ba2.Timestamp = tc.Clock().Now()

		var pErr *kvpb.Error
		_, tok := repl.mu.proposalBuf.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		ch, _, _, _, pErr = repl.evalAndPropose(ctx, ba2, allSpansGuard(), &st, uncertainty.Interval{}, tok.Move(ctx))
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

type testQuiescer struct {
	st                     *cluster.Settings
	storeID                roachpb.StoreID
	desc                   roachpb.RangeDescriptor
	numProposals           int
	pendingQuota           bool
	sendTokens             bool
	ticksSinceLastProposal int
	status                 *raft.SparseStatus
	lastIndex              kvpb.RaftIndex
	raftReady              bool
	leaseStatus            kvserverpb.LeaseStatus
	mergeInProgress        bool
	isDestroyed            bool

	// Not used to implement quiescer, but used by tests.
	livenessMap livenesspb.IsLiveMap
	paused      map[roachpb.ReplicaID]struct{}
}

func (q *testQuiescer) ClusterSettings() *cluster.Settings {
	return q.st
}

func (q *testQuiescer) StoreID() roachpb.StoreID {
	return q.storeID
}

func (q *testQuiescer) descRLocked() *roachpb.RangeDescriptor {
	return &q.desc
}

func (q *testQuiescer) isRaftLeaderRLocked() bool {
	return q.status != nil && q.status.RaftState == raftpb.StateLeader
}

func (q *testQuiescer) raftSparseStatusRLocked() *raft.SparseStatus {
	return q.status
}

func (q *testQuiescer) raftBasicStatusRLocked() raft.BasicStatus {
	return q.status.BasicStatus
}

func (q *testQuiescer) raftLastIndexRLocked() kvpb.RaftIndex {
	return q.lastIndex
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

func (q *testQuiescer) hasSendTokensRaftMuLockedReplicaMuLocked() bool {
	return q.sendTokens
}

func (q *testQuiescer) ticksSinceLastProposalRLocked() int {
	return q.ticksSinceLastProposal
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
	test := func(expected bool, transform func(q *testQuiescer)) {
		t.Run("", func(t *testing.T) {
			// A testQuiescer initialized so that shouldReplicaQuiesce will return
			// true. The transform function is intended to perform one mutation to
			// this quiescer so that shouldReplicaQuiesce will return false.
			q := &testQuiescer{
				st:      cluster.MakeTestingClusterSettings(),
				storeID: 1,
				desc: roachpb.RangeDescriptor{
					InternalReplicas: []roachpb.ReplicaDescriptor{
						{NodeID: 1, ReplicaID: 1},
						{NodeID: 2, ReplicaID: 2},
						{NodeID: 3, ReplicaID: 3},
					},
				},
				status: &raft.SparseStatus{
					BasicStatus: raft.BasicStatus{
						ID: 1,
						HardState: raftpb.HardState{
							Commit: logIndex,
						},
						SoftState: raft.SoftState{
							RaftState: raftpb.StateLeader,
						},
						Applied:        logIndex,
						LeadTransferee: 0,
					},
					Progress: map[raftpb.PeerID]tracker.Progress{
						1: {Match: logIndex, State: tracker.StateReplicate},
						2: {Match: logIndex, State: tracker.StateReplicate},
						3: {Match: logIndex, State: tracker.StateReplicate},
					},
				},
				lastIndex:              logIndex,
				raftReady:              false,
				ticksSinceLastProposal: quiesceAfterTicks,
				leaseStatus: kvserverpb.LeaseStatus{
					State: kvserverpb.LeaseState_VALID,
					Lease: roachpb.Lease{
						Sequence: 1,
						Epoch:    1,
						Replica: roachpb.ReplicaDescriptor{
							NodeID:    1,
							StoreID:   1,
							ReplicaID: 1,
						},
					},
				},
				livenessMap: livenesspb.IsLiveMap{
					1: {IsLive: true},
					2: {IsLive: true},
					3: {IsLive: true},
				},
			}
			transform(q)
			_, lagging, ok := shouldReplicaQuiesceRaftMuLockedReplicaMuLocked(
				context.Background(), q, q.leaseStatus, q.livenessMap, q.paused)
			require.Equal(t, expected, ok)
			if ok {
				// Any non-live replicas should be in the laggingReplicaSet.
				var expLagging laggingReplicaSet
				for _, rep := range q.descRLocked().Replicas().Descriptors() {
					if l, ok := q.livenessMap[rep.NodeID]; ok && !l.IsLive {
						expLagging = append(expLagging, l.Liveness)
					}
				}
				slices.SortFunc(expLagging, func(a, b livenesspb.Liveness) int {
					return cmp.Compare(a.NodeID, b.NodeID)
				})
				require.Equal(t, expLagging, lagging)
			}
		})
	}

	test(true, func(q *testQuiescer) {})
	test(false, func(q *testQuiescer) { q.numProposals = 1 })
	test(false, func(q *testQuiescer) { q.pendingQuota = true })
	test(false, func(q *testQuiescer) { q.sendTokens = true })
	test(true, func(q *testQuiescer) {
		q.ticksSinceLastProposal = quiesceAfterTicks // quiesce on quiesceAfterTicks
	})
	test(true, func(q *testQuiescer) {
		q.ticksSinceLastProposal = quiesceAfterTicks + 1 // quiesce above quiesceAfterTicks
	})
	test(false, func(q *testQuiescer) {
		q.ticksSinceLastProposal = quiesceAfterTicks - 1 // don't quiesce below quiesceAfterTicks
	})
	test(false, func(q *testQuiescer) {
		q.ticksSinceLastProposal = 0 // don't quiesce on 0
	})
	test(false, func(q *testQuiescer) {
		q.ticksSinceLastProposal = -1 // don't quiesce on negative (shouldn't happen)
	})
	test(false, func(q *testQuiescer) { q.mergeInProgress = true })
	test(false, func(q *testQuiescer) { q.isDestroyed = true })
	test(false, func(q *testQuiescer) { q.status = nil })
	test(false, func(q *testQuiescer) { q.status.RaftState = raftpb.StateFollower })
	test(false, func(q *testQuiescer) { q.status.RaftState = raftpb.StateCandidate })
	test(false, func(q *testQuiescer) { q.status.LeadTransferee = 1 })
	test(false, func(q *testQuiescer) { q.status.Commit = invalidIndex })
	test(false, func(q *testQuiescer) { q.status.Applied = invalidIndex })
	test(false, func(q *testQuiescer) { q.lastIndex = invalidIndex })
	for _, i := range []raftpb.PeerID{1, 2, 3} {
		test(false, func(q *testQuiescer) {
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
		})
	}
	test(false, func(q *testQuiescer) {
		delete(q.status.Progress, q.status.ID)
	})
	test(false, func(q *testQuiescer) { q.storeID = 9 })

	for _, leaseState := range []kvserverpb.LeaseState{
		kvserverpb.LeaseState_ERROR,
		kvserverpb.LeaseState_UNUSABLE,
		kvserverpb.LeaseState_EXPIRED,
		kvserverpb.LeaseState_PROSCRIBED,
	} {
		test(false, func(q *testQuiescer) { q.leaseStatus.State = leaseState })
	}
	test(false, func(q *testQuiescer) { q.raftReady = true })
	test(false, func(q *testQuiescer) {
		pr := q.status.Progress[2]
		pr.State = tracker.StateProbe
		q.status.Progress[2] = pr
	})
	// Create a mismatch between the raft progress replica IDs and the
	// replica IDs in the range descriptor.
	for i := 0; i < 3; i++ {
		test(false, func(q *testQuiescer) {
			q.desc.InternalReplicas[i].ReplicaID = roachpb.ReplicaID(4 + i)
		})
	}
	// Pass a nil liveness map.
	test(true, func(q *testQuiescer) { q.livenessMap = nil })
	// Verify quiesce even when replica progress doesn't match, if
	// the replica is on a non-live node.
	for _, i := range []raftpb.PeerID{1, 2, 3} {
		test(true, func(q *testQuiescer) {
			nodeID := roachpb.NodeID(i)
			q.livenessMap[nodeID] = livenesspb.IsLiveMapEntry{
				Liveness: livenesspb.Liveness{NodeID: nodeID},
				IsLive:   false,
			}
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
		})
	}
	// Verify no quiescence when replica progress doesn't match, if
	// given a nil liveness map.
	for _, i := range []raftpb.PeerID{1, 2, 3} {
		test(false, func(q *testQuiescer) {
			q.livenessMap = nil
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
		})
	}
	// Verify no quiescence when replica progress doesn't match, if
	// liveness map does not contain the lagging replica.
	for _, i := range []raftpb.PeerID{1, 2, 3} {
		test(false, func(q *testQuiescer) {
			delete(q.livenessMap, roachpb.NodeID(i))
			q.status.Progress[i] = tracker.Progress{Match: invalidIndex}
		})
	}
	// Verify no quiescence when a follower is paused.
	test(false, func(q *testQuiescer) {
		q.paused = map[roachpb.ReplicaID]struct{}{
			q.desc.Replicas().Descriptors()[0].ReplicaID: {},
		}
	})
	// Verify no quiescence with expiration-based leases, regardless
	// of kv.expiration_leases_only.enabled.
	test(false, func(q *testQuiescer) {
		ExpirationLeasesOnly.Override(context.Background(), &q.st.SV, true)
		q.leaseStatus.Lease.Epoch = 0
		q.leaseStatus.Lease.Expiration = &hlc.Timestamp{
			WallTime: timeutil.Now().Add(time.Minute).Unix(),
		}
	})
	test(false, func(q *testQuiescer) {
		ExpirationLeasesOnly.Override(context.Background(), &q.st.SV, false)
		q.leaseStatus.Lease.Epoch = 0
		q.leaseStatus.Lease.Expiration = &hlc.Timestamp{
			WallTime: timeutil.Now().Add(time.Minute).Unix(),
		}
	})
	// Verify no quiescence with leader leases.
	test(false, func(q *testQuiescer) {
		q.leaseStatus.Lease.Epoch = 0
		q.leaseStatus.Lease.Term = 1
	})
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
				status: &raft.SparseStatus{
					BasicStatus: raft.BasicStatus{
						ID: 2,
						HardState: raftpb.HardState{
							Term:   5,
							Commit: 10,
							Lead:   1,
						},
					},
				},
				livenessMap: livenesspb.IsLiveMap{
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
		q.livenessMap[l.NodeID] = livenesspb.IsLiveMapEntry{
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
		q.livenessMap[l.NodeID] = livenesspb.IsLiveMapEntry{
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
		q.livenessMap[l.NodeID] = livenesspb.IsLiveMapEntry{
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
		q.livenessMap[l.NodeID] = livenesspb.IsLiveMapEntry{
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
		q.livenessMap[l.NodeID] = livenesspb.IsLiveMapEntry{
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
			args := &kvpb.RecomputeStatsRequest{
				RequestHeader: kvpb.RequestHeader{
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

			delta := enginepb.MVCCStats(resp.(*kvpb.RecomputeStatsResponse).AddedDelta)
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
	ms := repl.shMu.state.Stats // intentionally mutated below
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
		TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			if _, ok := ba.GetArg(kvpb.ComputeChecksum); ok {
				return kvpb.NewErrorf("boom")
			}
			return nil
		},
	}
	tc := testContext{}
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	confReader, err := tc.store.GetConfReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		// Do this twice because it used to deadlock. See #25456.
		processed, err := tc.store.consistencyQueue.process(ctx, tc.repl, confReader)
		if !testutils.IsError(err, "boom") {
			t.Fatal(err)
		}
		assert.False(t, processed)
	}
}

// TestReplicaServersideRefreshes verifies local retry logic for transactional
// and non-transactional batches. Verifies the timestamp cache is updated to
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
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.manualClock = timeutil.NewManualTime(timeutil.Unix(0, 123))
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

	// Increment the clock so that all the transactions in the tests run at a
	// different physical timestamp than the one used to initialize the replica's
	// timestamp cache. This allows one of the tests to reset the logical part of
	// the timestamp it's operating and not run into the timestamp cache.
	tc.manualClock.Advance(1)

	newTxn := func(key string, ts hlc.Timestamp) *roachpb.Transaction {
		txn := roachpb.MakeTransaction("test", roachpb.Key(key), isolation.Serializable, roachpb.NormalUserPriority, ts, 0, 0, 0, false /* omitInRangefeeds */)
		return &txn
	}
	send := func(ba *kvpb.BatchRequest) (hlc.Timestamp, error) {
		br, pErr := tc.Sender().Send(ctx, ba)
		if pErr != nil {
			return hlc.Timestamp{}, pErr.GoError()
		}

		// Check that we didn't mess up the stats.
		// Regression test for #31870.
		snap := tc.engine.NewSnapshot()
		defer snap.Close()
		res, err := CalcReplicaDigest(ctx, *tc.repl.Desc(), tc.engine, kvpb.ChecksumMode_CHECK_FULL,
			quotapool.NewRateLimiter("test", quotapool.Inf(), 0), nil /* settings */)
		if err != nil {
			return hlc.Timestamp{}, err
		}
		if res.PersistedMS != res.RecomputedMS {
			return hlc.Timestamp{}, errors.Errorf("stats are inconsistent:\npersisted:\n%+v\nrecomputed:\n%+v", res.PersistedMS, res.RecomputedMS)
		}

		return br.Timestamp, nil
	}
	get := func(key string) (hlc.Timestamp, error) {
		ba := &kvpb.BatchRequest{}
		get := getArgs(roachpb.Key(key))
		ba.Add(&get)
		return send(ba)
	}
	put := func(key, val string) (hlc.Timestamp, error) {
		ba := &kvpb.BatchRequest{}
		put := putArgs(roachpb.Key(key), []byte(val))
		ba.Add(&put)
		return send(ba)
	}

	testCases := []struct {
		name    string
		setupFn func() (hlc.Timestamp, error) // returns expected batch execution timestamp
		batchFn func(hlc.Timestamp) (*kvpb.BatchRequest, hlc.Timestamp)
		expErr  string
	}{
		{
			name: "serverside-refresh of write too old on put",
			setupFn: func() (hlc.Timestamp, error) {
				return put("a", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("c-scan", ts.Prev())
				scan := scanArgs(roachpb.Key("c-scan"), roachpb.Key("c-scan\x00"))
				scan.KeyLockingStrength = lock.Exclusive
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("c-cput", ts.Prev())
				ba.CanForwardReadTimestamp = true
				cput := cPutArgs(roachpb.Key("c-cput"), []byte("iput"), []byte("put"))
				ba.Add(&cput)
				assignSeqNumsForReqs(ba.Txn, &cput)
				return
			},
		},
		// This test tests a scenario where an InitPut would fail at its original
		// timestamp, but it succeeds when evaluated at a bumped timestamp after a
		// server-side refresh.
		{
			name: "serverside-refresh of write too old on non-1PC txn initput without prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				// Note there are two different version of the value, but a
				// non-txnal cput will evaluate the most recent version and
				// avoid a condition failed error.
				_, _ = put("c-iput", "put1")
				return put("c-iput", "put2")
			},
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("c-iput", ts.Prev())
				ba.CanForwardReadTimestamp = true
				iput := iPutArgs(roachpb.Key("c-iput"), []byte("put2"))
				ba.Add(&iput)
				assignSeqNumsForReqs(ba.Txn, &iput)
				return
			},
		},
		// Non-1PC serializable txn locking scan with CanForwardReadTimestamp
		// set to true will succeed with write too old error.
		{
			name: "serverside-refresh of write too old on non-1PC txn locking scan without prior reads",
			setupFn: func() (hlc.Timestamp, error) {
				return put("c-scan", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("c-scan", ts.Prev())
				ba.CanForwardReadTimestamp = true
				scan := scanArgs(roachpb.Key("c-scan"), roachpb.Key("c-scan\x00"))
				scan.KeyLockingStrength = lock.Exclusive
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("e", ts.Prev())
				ba.CanForwardReadTimestamp = true // necessary to indicate serverside-refresh is possible
				cput := cPutArgs(ba.Txn.Key, []byte("cput"), []byte("put"))
				et, _ := endTxnArgs(ba.Txn, true /* commit */)
				ba.Add(&cput, &et)
				assignSeqNumsForReqs(ba.Txn, &cput, &et)
				return
			},
		},
		// This test tests a scenario where an CPut would fail at its original
		// timestamp, but it succeeds when evaluated at a bumped timestamp after a
		// server-side refresh.
		// The previous test shows different behavior for a non-transactional
		// request or a 1PC one.
		{
			name: "serverside-refresh with failed cput despite write too old errors on txn",
			setupFn: func() (hlc.Timestamp, error) {
				return put("e1", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				txn := newTxn("e1", ts.Prev())

				// Send write to another key first to avoid 1PC.
				ba = &kvpb.BatchRequest{}
				ba.Txn = txn
				put := putArgs([]byte("e1-other-key"), []byte("otherput"))
				ba.Add(&put)
				assignSeqNumsForReqs(ba.Txn, &put)
				if _, err := send(ba); err != nil {
					panic(err)
				}

				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				// We're going to execute before any of the writes in setupFn.
				ts.Logical = 0
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				// We're going to execute before any of the writes in setupFn.
				ts.Logical = 0
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				txn := newTxn("h", ts.Prev())
				// Send write to another key first to avoid 1PC.
				ba = &kvpb.BatchRequest{}
				ba.Txn = txn
				put := putArgs([]byte("h2"), []byte("otherput"))
				ba.Add(&put)
				assignSeqNumsForReqs(ba.Txn, &put)
				if _, err := send(ba); err != nil {
					panic(err)
				}
				// Send the remainder of the transaction in another batch.
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				// Txn with (read_ts, write_ts) = (1, 4) finds a value with
				// `ts = 2`. Final timestamp should be `ts = 4`.
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("lscan", ts.Prev())
				ba.Txn.WriteTimestamp = ts.Next().Next()
				ba.CanForwardReadTimestamp = true

				expTS = ba.Txn.WriteTimestamp

				scan := scanArgs(roachpb.Key("lscan"), roachpb.Key("lscan\x00"))
				scan.KeyLockingStrength = lock.Exclusive
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				txn := newTxn("i", ts.Prev())
				// Send write to another key first to avoid 1PC.
				ba = &kvpb.BatchRequest{}
				ba.Txn = txn
				put1 := putArgs([]byte("i2"), []byte("otherput"))
				ba.Add(&put1)
				assignSeqNumsForReqs(ba.Txn, &put1)
				if _, err := send(ba); err != nil {
					panic(err)
				}
				// Send the remainder of the transaction in another batch.
				expTS = ts.Next()
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ts = ts.Prev()
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ts = ts.Prev()
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				expTS = ts.Next()
				ts = ts.Prev()
				ba = &kvpb.BatchRequest{}
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
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				ts = ts.Prev()
				ba = &kvpb.BatchRequest{}
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
		{
			name: "server-side refresh with shared locks",
			setupFn: func() (hlc.Timestamp, error) {
				return put("slscan", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				// Txn with (read_ts, write_ts) = (1, 4) finds a value with `ts = 2`.
				// Should get a WTO error and refresh successfully. Final timestamp
				// should be `ts = 4`.
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("slscan", ts.Prev())
				ba.Txn.WriteTimestamp = ts.Next().Next()
				ba.CanForwardReadTimestamp = true

				expTS = ba.Txn.WriteTimestamp

				scan := scanArgs(roachpb.Key("slscan"), roachpb.Key("slscan\x00"))
				scan.KeyLockingStrength = lock.Shared
				ba.Add(scan)
				return
			},
		},
		{
			name: "server-side refresh with shared locks and non-locking reads in the same batch",
			setupFn: func() (hlc.Timestamp, error) {
				return put("slscan2", "put")
			},
			batchFn: func(ts hlc.Timestamp) (ba *kvpb.BatchRequest, expTS hlc.Timestamp) {
				// Txn with (read_ts, write_ts) = (1, 4) finds a value with `ts = 2`.
				// Should get a WTO error but not be able to refresh because of the
				// non-locking get in the same batch.
				ba = &kvpb.BatchRequest{}
				ba.Txn = newTxn("slscan2", ts.Prev())
				ba.Txn.WriteTimestamp = ts.Next().Next()
				ba.CanForwardReadTimestamp = true

				scan := scanArgs(roachpb.Key("slscan2"), roachpb.Key("slscan2\x00"))
				scan.KeyLockingStrength = lock.Shared
				get := getArgs(roachpb.Key("getslscan2"))
				ba.Add(&get)
				ba.Add(scan)
				return
			},
			expErr: "WriteTooOldError",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ts, err := test.setupFn()
			require.NoError(t, err)
			ba, expTS := test.batchFn(ts)
			actualTS, err := send(ba)
			if !testutils.IsError(err, test.expErr) {
				t.Fatalf("expected error %q; got \"%v\"", test.expErr, err)
			}
			require.Equal(t, expTS, actualTS)
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
	txn := roachpb.MakeTransaction("test", k, isolation.Serializable, roachpb.NormalUserPriority, ts1, 0, 0, 0, false /* omitInRangefeeds */)

	// Write a value outside the transaction.
	tc.manualClock.Advance(10)
	ts2 := tc.Clock().Now()
	if _, err := storage.MVCCPut(ctx, tc.engine, k, ts2, roachpb.MakeValueFromString("one"), storage.MVCCWriteOptions{}); err != nil {
		t.Fatalf("writing interfering value: %+v", err)
	}

	// Push the transaction's timestamp. In real-world situations,
	// the only thing that can push a read-only transaction's
	// timestamp is ReadWithinUncertaintyIntervalError, but
	// synthesizing one of those in this single-node test harness is
	// tricky.
	tc.manualClock.Advance(10)
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
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	put := putArgs(k, []byte("two"))
	et, _ := endTxnArgs(&txn, true)
	ba.Add(&put, &et)
	assignSeqNumsForReqs(&txn, &put, &et)
	if br, pErr := tc.Sender().Send(ctx, ba); pErr == nil {
		t.Errorf("did not get expected error. resp=%s", br)
	} else if wtoe, ok := pErr.GetDetail().(*kvpb.WriteTooOldError); !ok {
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

	// Disable txn liveness/deadlock pushes. See below for why.
	st := tc.store.cfg.Settings
	st.Manual.Store(true)
	concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(ctx, &st.SV, 24*time.Hour)

	// Write a value to a key A.
	key := roachpb.Key("a")
	initVal := incrementArgs(key, 1)
	if _, pErr := tc.SendWrapped(initVal); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Create a new transaction and perform a "for update" scan. This should
	// acquire unreplicated, exclusive locks on the key.
	txn := newTransaction("test", key, 1, tc.Clock())
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
	ba.Add(kvpb.NewLockingScan(key, key.Next(), kvpb.ForUpdate, kvpb.BestEffort))
	if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
		t.Fatalf("unexpected error: %s", pErr)
	}

	// Try to write to the key outside of this transaction. Should wait on the
	// "for update" lock in a lock wait-queue in the concurrency manager until the
	// lock is released. If we don't notify the lock-table when the first txn
	// eventually commits, this will wait for much longer than it needs to. It
	// will eventually push the first txn and notice that it has committed.
	// However, we've disabled liveness and deadlock pushes[*] in this test, so the
	// test will block forever without the lock-table notification.
	//
	// [*] The operating push here being the liveness one, as non-transactional
	// requests can't be part of deadlock cycles. However, both of these are
	// controlled by a single cluster setting.
	pErrC := make(chan *kvpb.Error, 1)
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
	ba = &kvpb.BatchRequest{}
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

// TestReplicaAsyncIntentResolutionOn1PC runs a transaction that acquires one or
// more unreplicated locks and then performs a one-phase commit. It tests that
// async resolution is performed for any unreplicated lock that is external to
// the range that the transaction is anchored on, but not for any unreplicated
// lock that is local to that range.
func TestReplicaAsyncIntentResolutionOn1PC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "external", func(t *testing.T, external bool) {
		// Intercept async intent resolution for the test's transaction.
		var storeKnobs StoreTestingKnobs
		var txnIDAtomic atomic.Value
		txnIDAtomic.Store(uuid.Nil)
		resIntentC := make(chan *kvpb.ResolveIntentRequest)
		storeKnobs.TestingRequestFilter = func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			for _, req := range ba.Requests {
				riReq := req.GetResolveIntent()
				if riReq != nil && riReq.IntentTxn.ID == txnIDAtomic.Load().(uuid.UUID) {
					resIntentC <- riReq
				}
			}
			return nil
		}

		ctx := context.Background()
		s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &storeKnobs,
				KVClient: &kvcoord.ClientTestingKnobs{
					// Disable randomization of the transaction's anchor key so that we
					// can predictably make assertions that rely on the transaction record
					// being on a specific range.
					DisableTxnAnchorKeyRandomization: true,
				},
			},
		})
		defer s.Stopper().Stop(ctx)

		store, err := s.GetStores().(*Stores).GetStore(1)
		require.NoError(t, err)
		successfulOnePCBefore := store.Metrics().OnePhaseCommitSuccess.Count()
		failedOnePCBefore := store.Metrics().OnePhaseCommitFailure.Count()

		// Perform a range split between key A and B.
		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		_, _, err = s.SplitRange(keyB)
		require.NoError(t, err)

		// Write a value to a key A and B.
		_, err = kvDB.Inc(ctx, keyA, 1)
		require.Nil(t, err)
		_, err = kvDB.Inc(ctx, keyB, 1)
		require.Nil(t, err)

		// Create a new transaction.
		txn := kvDB.NewTxn(ctx, "test")
		txnIDAtomic.Store(txn.ID())

		// Perform one or more "for update" gets. This should acquire unreplicated,
		// exclusive locks on the keys.
		b := txn.NewBatch()
		b.GetForUpdate(keyA, kvpb.BestEffort)
		if external {
			b.GetForUpdate(keyB, kvpb.BestEffort)
		}
		err = txn.Run(ctx, b)
		require.NoError(t, err)

		// Update the locked value and commit in a single batch. This should hit the
		// one-phase commit fast-path (verified below) and then release the
		// "for update" lock(s).
		b = txn.NewBatch()
		b.Inc(keyA, 1)
		err = txn.CommitInBatch(ctx, b)
		require.NoError(t, err)

		successfulOnePCAfter := store.Metrics().OnePhaseCommitSuccess.Count()
		failedOnePCAfter := store.Metrics().OnePhaseCommitFailure.Count()
		require.Equal(t, failedOnePCBefore, failedOnePCAfter)
		require.Greater(t, successfulOnePCAfter, successfulOnePCBefore)

		// If an external lock was acquired, we should see its resolution.
		if external {
			riReq := <-resIntentC
			require.Equal(t, keyB, riReq.Key)
		}

		// After that, we should see no other intent resolution request for this
		// transaction.
		select {
		case riReq := <-resIntentC:
			t.Fatalf("unexpected intent resolution request: %v", riReq)
		case <-time.After(10 * time.Millisecond):
		}
	})
}

// TestReplicaQueryLocks tests QueryLocks in a number of scenarios while locks are
// held, such as filtering out uncontended locks and limiting results.
func TestReplicaQueryLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "includeUncontended", func(t *testing.T, includeUncontended bool) {
		testutils.RunTrueAndFalse(t, "limitResults", func(t *testing.T, limitResults bool) {

			ctx := context.Background()
			tc := testContext{}
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			tc.Start(ctx, t, stopper)

			// Write a values to keys "a", "b".
			keyA := roachpb.Key("a")
			keyB := roachpb.Key("b")
			initVal := incrementArgs(keyA, 1)
			if _, pErr := tc.SendWrapped(initVal); pErr != nil {
				t.Fatalf("unexpected error: %s", pErr)
			}
			initVal = incrementArgs(keyB, 1)
			if _, pErr := tc.SendWrapped(initVal); pErr != nil {
				t.Fatalf("unexpected error: %s", pErr)
			}

			// Create a new transaction and perform a "for update" scan. This should
			// acquire unreplicated, exclusive locks on keys "a" and "b".
			txn := newTransaction("test", keyA, 1, tc.Clock())
			ba := &kvpb.BatchRequest{}
			ba.Header = kvpb.Header{Txn: txn}
			ba.Add(kvpb.NewLockingScan(keyA, keyB.Next(), kvpb.ForUpdate, kvpb.BestEffort))
			if _, pErr := tc.Sender().Send(ctx, ba); pErr != nil {
				t.Fatalf("unexpected error: %s", pErr)
			}

			// Try to write to key "a" outside this transaction. Should wait on the
			// "for update" lock in a lock wait-queue in the concurrency manager until
			// the lock is released.
			pErrC := make(chan *kvpb.Error, 1)
			go func() {
				otherWrite := incrementArgs(keyA, 1)
				_, pErr := tc.SendWrapped(otherWrite)
				pErrC <- pErr
			}()

			// The non-transactional write should not complete.
			select {
			case pErr := <-pErrC:
				t.Fatalf("write unexpectedly finished with error: %v", pErr)
			case <-time.After(50 * time.Millisecond):
			}

			var h kvpb.Header
			var queryResp *kvpb.QueryLocksResponse
			var ok bool
			if limitResults {
				h.MaxSpanRequestKeys = 1
			}
			queryArgs := queryLocksArgs(keys.MinKey, keys.MaxKey, includeUncontended)
			resp, err := tc.SendWrappedWith(h, &queryArgs)
			if err != nil {
				t.Fatal(err)
			}
			if queryResp, ok = resp.(*kvpb.QueryLocksResponse); !ok {
				t.Fatalf("expected QueryLocksResponse, found %v", resp)
			}

			expectedLen := 1
			if includeUncontended && !limitResults {
				expectedLen = 2
			}

			// First, validate length of results and the expected resume span/resume reason, if any.
			require.Len(t, queryResp.Locks, expectedLen, "expected %d locks in response, got only %d", expectedLen, len(queryResp.Locks))
			if includeUncontended && limitResults {
				require.NotNil(t, resp.Header().ResumeSpan, "expected resume span")
				require.Equal(t, kvpb.RESUME_KEY_LIMIT, resp.Header().ResumeReason)
				require.Equal(t, roachpb.Key("b"), resp.Header().ResumeSpan.Key)
			} else {
				require.Nil(t, resp.Header().ResumeSpan)
				require.Equal(t, kvpb.RESUME_UNKNOWN, resp.Header().ResumeReason)
			}

			// Validate first lock as held by txn, on key "a", at the correct ts, with a single waiter.
			lockInfo := queryResp.Locks[0]
			require.Equal(t, roachpb.Key("a"), lockInfo.Key)
			require.NotNil(t, lockInfo.LockHolder, "expected lock to be held")
			require.Equalf(t, txn.ID, lockInfo.LockHolder.ID,
				"expected lock to be held by txn %s @ %s, observed lock held by txn %s @ %s",
				txn.Short(), txn.WriteTimestamp, lockInfo.LockHolder.Short(), lockInfo.LockHolder.WriteTimestamp,
			)
			require.Equalf(t, txn.WriteTimestamp, lockInfo.LockHolder.WriteTimestamp,
				"expected lock to be held by txn %s @ %s, observed lock held by txn %s @ %s",
				txn.Short(), txn.WriteTimestamp, lockInfo.LockHolder.Short(), lockInfo.LockHolder.WriteTimestamp,
			)
			require.Equal(t, lock.Unreplicated, lockInfo.Durability)
			require.Len(t, lockInfo.Waiters, 1)

			// Validate the second, uncontended lock on key "b", if we expect it in our response.
			if expectedLen == 2 {
				lockInfo = queryResp.Locks[1]

				require.Equal(t, roachpb.Key("b"), lockInfo.Key)
				require.NotNil(t, lockInfo.LockHolder, "expected lock to be held")
				if !lockInfo.LockHolder.ID.Equal(txn.ID) || !lockInfo.LockHolder.WriteTimestamp.Equal(txn.WriteTimestamp) {
					t.Fatalf("expected lock to be held by txn %s @ %s, observed lock held by txn %s @ %s",
						txn.Short(), txn.WriteTimestamp, lockInfo.LockHolder.Short(), lockInfo.LockHolder.WriteTimestamp)
				}
				require.Equal(t, lock.Unreplicated, lockInfo.Durability)
				require.Len(t, lockInfo.Waiters, 0)
			}

			// Update the locked value and commit in a single batch. This should release
			// the "for update" lock.
			ba = &kvpb.BatchRequest{}
			incArgs := incrementArgs(keyA, 1)
			et, etH := endTxnArgs(txn, true /* commit */)
			et.Require1PC = true
			et.LockSpans = []roachpb.Span{{Key: keyA, EndKey: keyB.Next()}}
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

			// Validate that a second QueryLocks request returns no locks.
			resp, err = tc.SendWrappedWith(h, &queryArgs)
			if err != nil {
				t.Fatal(err)
			}
			if queryResp, ok = resp.(*kvpb.QueryLocksResponse); !ok {
				t.Fatalf("expected QueryLocksResponse, found %v", resp)
			}
			require.Empty(t, queryResp.Locks)
			require.Nil(t, resp.Header().ResumeSpan)
			require.Equal(t, kvpb.RESUME_UNKNOWN, resp.Header().ResumeReason)
		})
	})
}

func TestReplicaShouldCampaignOnWake(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type params struct {
		leaseStatus             kvserverpb.LeaseStatus
		storeID                 roachpb.StoreID
		raftStatus              raft.BasicStatus
		livenessMap             livenesspb.IsLiveMap
		desc                    *roachpb.RangeDescriptor
		requiresExpirationLease bool
		now                     hlc.Timestamp
	}

	// Set up a base state that we can vary, representing this node n1 being a
	// follower of n2, but n2 is dead and does not hold a valid lease. We should
	// campaign in this state.
	base := params{
		storeID: 1,
		desc: &roachpb.RangeDescriptor{
			RangeID:  1,
			StartKey: roachpb.RKeyMin,
			EndKey:   roachpb.RKeyMax,
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			},
			NextReplicaID: 4,
		},
		leaseStatus: kvserverpb.LeaseStatus{
			Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{StoreID: 2}},
			State: kvserverpb.LeaseState_EXPIRED,
		},
		raftStatus: raft.BasicStatus{
			SoftState: raft.SoftState{
				RaftState: raftpb.StateFollower,
			},
			HardState: raftpb.HardState{
				Lead: 2,
			},
		},
		livenessMap: livenesspb.IsLiveMap{
			1: livenesspb.IsLiveMapEntry{IsLive: true},
			2: livenesspb.IsLiveMapEntry{IsLive: false},
			3: livenesspb.IsLiveMapEntry{IsLive: false},
		},
	}

	testcases := map[string]struct {
		expect bool
		modify func(*params)
	}{
		"dead leader without lease": {true, func(p *params) {}},
		"valid remote lease": {false, func(p *params) {
			p.leaseStatus.State = kvserverpb.LeaseState_VALID
		}},
		"valid local lease": {true, func(p *params) {
			p.leaseStatus.State = kvserverpb.LeaseState_VALID
			p.leaseStatus.Lease.Replica.StoreID = 1
		}},
		"pre-candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StatePreCandidate
			p.raftStatus.Lead = raft.None
		}},
		"candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateCandidate
			p.raftStatus.Lead = raft.None
		}},
		"leader": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateLeader
			p.raftStatus.Lead = 1
		}},
		"unknown leader": {true, func(p *params) {
			p.raftStatus.Lead = raft.None
		}},
		"requires expiration lease": {false, func(p *params) {
			p.requiresExpirationLease = true
		}},
		"leader not in desc": {false, func(p *params) {
			p.raftStatus.Lead = 4
		}},
		"leader not in liveness": {false, func(p *params) {
			delete(p.livenessMap, 2)
		}},
		"leader is live": {false, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: true,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, 1).ToLegacyTimestamp(),
				},
			}
		}},
		"leader is live according to expiration": {false, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: false,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, 1).ToLegacyTimestamp(),
				},
			}
		}},
		"leader is dead according to expiration": {true, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: true,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, -1).ToLegacyTimestamp(),
				},
			}
		}},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			p := base
			p.livenessMap = livenesspb.IsLiveMap{}
			for k, v := range base.livenessMap {
				p.livenessMap[k] = v
			}
			tc.modify(&p)
			require.Equal(t, tc.expect, shouldCampaignOnWake(p.leaseStatus, p.storeID, p.raftStatus,
				p.livenessMap, p.desc, p.requiresExpirationLease, p.now))
		})
	}
}

func TestReplicaShouldCampaignOnLeaseRequestRedirect(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type params struct {
		raftStatus  raft.BasicStatus
		livenessMap livenesspb.IsLiveMap
		desc        *roachpb.RangeDescriptor
		leaseType   roachpb.LeaseType
		now         hlc.Timestamp
	}

	// Set up a base state that we can vary, representing this node n1 being a
	// follower of n2, but n2 is dead. We should campaign in this state.
	base := params{
		desc: &roachpb.RangeDescriptor{
			RangeID:  1,
			StartKey: roachpb.RKeyMin,
			EndKey:   roachpb.RKeyMax,
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			},
			NextReplicaID: 4,
		},
		raftStatus: raft.BasicStatus{
			SoftState: raft.SoftState{
				RaftState: raftpb.StateFollower,
			},
			HardState: raftpb.HardState{
				Lead: 2,
			},
		},
		livenessMap: livenesspb.IsLiveMap{
			1: livenesspb.IsLiveMapEntry{IsLive: true},
			2: livenesspb.IsLiveMapEntry{IsLive: false},
			3: livenesspb.IsLiveMapEntry{IsLive: false},
		},
		leaseType: roachpb.LeaseEpoch,
		now:       hlc.Timestamp{Logical: 10},
	}

	testcases := map[string]struct {
		expect bool
		modify func(*params)
	}{
		"dead leader": {true, func(p *params) {}},
		"pre-candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StatePreCandidate
			p.raftStatus.Lead = raft.None
		}},
		"candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateCandidate
			p.raftStatus.Lead = raft.None
		}},
		"leader": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateLeader
			p.raftStatus.Lead = 1
		}},
		"unknown leader": {true, func(p *params) {
			p.raftStatus.Lead = raft.None
		}},
		"should use expiration lease": {false, func(p *params) {
			p.leaseType = roachpb.LeaseExpiration
		}},
		"should use leader lease": {false, func(p *params) {
			p.leaseType = roachpb.LeaseLeader
		}},
		"leader not in desc": {false, func(p *params) {
			p.raftStatus.Lead = 4
		}},
		"leader not in liveness": {false, func(p *params) {
			delete(p.livenessMap, 2)
		}},
		"leader is live": {false, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: true,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, 1).ToLegacyTimestamp(),
				},
			}
		}},
		"leader is live according to expiration": {false, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: false,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, 1).ToLegacyTimestamp(),
				},
			}
		}},
		"leader is dead according to expiration": {true, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: true,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, -1).ToLegacyTimestamp(),
				},
			}
		}},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			p := base
			p.livenessMap = livenesspb.IsLiveMap{}
			for k, v := range base.livenessMap {
				p.livenessMap[k] = v
			}
			tc.modify(&p)
			require.Equal(t, tc.expect, shouldCampaignOnLeaseRequestRedirect(
				p.raftStatus, p.livenessMap, p.desc, p.leaseType, p.now))
		})
	}
}

func TestReplicaShouldForgetLeaderOnVoteRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type params struct {
		raftStatus  raft.BasicStatus
		livenessMap livenesspb.IsLiveMap
		desc        *roachpb.RangeDescriptor
		now         hlc.Timestamp
	}

	// Set up a base state that we can vary, representing this node n1 being a
	// follower of n2, but n2 is dead. We should forget the leader in this state.
	base := params{
		desc: &roachpb.RangeDescriptor{
			RangeID:  1,
			StartKey: roachpb.RKeyMin,
			EndKey:   roachpb.RKeyMax,
			InternalReplicas: []roachpb.ReplicaDescriptor{
				{NodeID: 1, StoreID: 1, ReplicaID: 1},
				{NodeID: 2, StoreID: 2, ReplicaID: 2},
				{NodeID: 3, StoreID: 3, ReplicaID: 3},
			},
			NextReplicaID: 4,
		},
		raftStatus: raft.BasicStatus{
			SoftState: raft.SoftState{
				RaftState: raftpb.StateFollower,
			},
			HardState: raftpb.HardState{
				Lead: 2,
			},
		},
		livenessMap: livenesspb.IsLiveMap{
			1: livenesspb.IsLiveMapEntry{IsLive: true},
			2: livenesspb.IsLiveMapEntry{IsLive: false},
			3: livenesspb.IsLiveMapEntry{IsLive: false},
		},
		now: hlc.Timestamp{Logical: 10},
	}

	testcases := map[string]struct {
		expect bool
		modify func(*params)
	}{
		"dead leader": {true, func(p *params) {}},
		"pre-candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StatePreCandidate
			p.raftStatus.Lead = raft.None
		}},
		"candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateCandidate
			p.raftStatus.Lead = raft.None
		}},
		"leader": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateLeader
			p.raftStatus.Lead = 1
		}},
		"unknown leader": {false, func(p *params) {
			p.raftStatus.Lead = raft.None
		}},
		"leader not in desc": {true, func(p *params) {
			p.raftStatus.Lead = 4
		}},
		"leader not in liveness": {true, func(p *params) {
			delete(p.livenessMap, 2)
		}},
		"leader is live": {false, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: true,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, 1).ToLegacyTimestamp(),
				},
			}
		}},
		"leader is live according to expiration": {false, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: false,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, 1).ToLegacyTimestamp(),
				},
			}
		}},
		"leader is dead according to expiration": {true, func(p *params) {
			p.livenessMap[2] = livenesspb.IsLiveMapEntry{
				IsLive: true,
				Liveness: livenesspb.Liveness{
					NodeID:     2,
					Expiration: p.now.Add(0, -1).ToLegacyTimestamp(),
				},
			}
		}},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			p := base
			p.livenessMap = livenesspb.IsLiveMap{}
			for k, v := range base.livenessMap {
				p.livenessMap[k] = v
			}
			tc.modify(&p)
			require.Equal(t, tc.expect, shouldForgetLeaderOnVoteRequest(
				p.raftStatus, p.livenessMap, p.desc, p.now))
		})
	}
}

func TestReplicaShouldTransferRaftLeadershipToLeaseholder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type params struct {
		raftStatus              raft.SparseStatus
		leaseStatus             kvserverpb.LeaseStatus
		leaseAcquisitionPending bool
		storeID                 roachpb.StoreID
		draining                bool
	}

	// Set up a base state that we can vary, representing this node n1 being a
	// leader and n2 being a follower that holds a valid lease and is caught up on
	// its raft log. We should transfer leadership in this state.
	const localID = 1
	const remoteID = 2
	base := params{
		raftStatus: raft.SparseStatus{
			BasicStatus: raft.BasicStatus{
				SoftState: raft.SoftState{
					RaftState: raftpb.StateLeader,
				},
				HardState: raftpb.HardState{
					Lead:   localID,
					Commit: 10,
				},
			},
			Progress: map[raftpb.PeerID]tracker.Progress{
				remoteID: {Match: 10},
			},
		},
		leaseStatus: kvserverpb.LeaseStatus{
			Lease: roachpb.Lease{Replica: roachpb.ReplicaDescriptor{
				ReplicaID: remoteID,
			}},
			State: kvserverpb.LeaseState_VALID,
		},
		leaseAcquisitionPending: false,
		storeID:                 localID,
		draining:                false,
	}

	testcases := map[string]struct {
		expect bool
		modify func(*params)
	}{
		"leader": {
			true, func(p *params) {},
		},
		"follower": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateFollower
			p.raftStatus.Lead = remoteID
		}},
		"pre-candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StatePreCandidate
			p.raftStatus.Lead = raft.None
		}},
		"candidate": {false, func(p *params) {
			p.raftStatus.SoftState.RaftState = raftpb.StateCandidate
			p.raftStatus.Lead = raft.None
		}},
		"invalid lease": {false, func(p *params) {
			p.leaseStatus.State = kvserverpb.LeaseState_EXPIRED
		}},
		"local lease": {false, func(p *params) {
			p.leaseStatus.Lease.Replica.ReplicaID = localID
		}},
		"lease request pending": {false, func(p *params) {
			p.leaseAcquisitionPending = true
		}},
		"no progress": {false, func(p *params) {
			p.raftStatus.Progress = map[raftpb.PeerID]tracker.Progress{}
		}},
		"insufficient progress": {false, func(p *params) {
			p.raftStatus.Progress = map[raftpb.PeerID]tracker.Progress{remoteID: {Match: 9}}
		}},
		"no progress, draining": {true, func(p *params) {
			p.raftStatus.Progress = map[raftpb.PeerID]tracker.Progress{}
			p.draining = true
		}},
		"insufficient progress, draining": {true, func(p *params) {
			p.raftStatus.Progress = map[raftpb.PeerID]tracker.Progress{remoteID: {Match: 9}}
			p.draining = true
		}},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			p := base
			tc.modify(&p)
			require.Equal(t, tc.expect, shouldTransferRaftLeadershipToLeaseholderLocked(
				p.raftStatus, p.leaseStatus, p.leaseAcquisitionPending, p.storeID, p.draining))
		})
	}
}

func TestRangeStatsRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testContext{}
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	sc := TestStoreConfig(nil)
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

	keyPrefix := roachpb.Key("dummy-prefix")

	// Write some random data to the range and verify that a RangeStatsRequest
	// returns the same MVCC stats as the replica's in-memory state.
	WriteRandomDataToRange(t, tc.store, tc.repl.RangeID, keyPrefix)
	expMS := tc.repl.GetMVCCStats()
	res, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{
		RangeID: tc.repl.RangeID,
	}, &kvpb.RangeStatsRequest{})
	if pErr != nil {
		t.Fatal(pErr)
	}
	resMS := res.(*kvpb.RangeStatsResponse).MVCCStats
	require.Equal(t, expMS, resMS)

	// Write another key to the range and verify that the MVCC stats returned
	// by a RangeStatsRequest reflect the additional key.
	key := append(keyPrefix, roachpb.RKey("123")...)
	if err := tc.store.DB().Put(ctx, key, "123"); err != nil {
		t.Fatal(err)
	}
	res, pErr = kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{
		RangeID: tc.repl.RangeID,
	}, &kvpb.RangeStatsRequest{})
	if pErr != nil {
		t.Fatal(pErr)
	}
	resMS = res.(*kvpb.RangeStatsResponse).MVCCStats
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
	ctx := context.Background()

	type runFunc func(testContext, *roachpb.Transaction, hlc.Timestamp) error
	sendWrappedWithErr := func(tc testContext, h kvpb.Header, args kvpb.Request) error {
		_, pErr := kv.SendWrappedWith(ctx, tc.Sender(), h, args)
		return pErr.GoError()
	}

	intents := []roachpb.Span{{Key: roachpb.Key("a")}}
	inFlightWrites := []roachpb.SequencedWrite{{Key: roachpb.Key("a"), Sequence: 1}}
	otherInFlightWrites := []roachpb.SequencedWrite{{Key: roachpb.Key("b"), Sequence: 2}}

	type verifyFunc func(testContext, *roachpb.Transaction, hlc.Timestamp) roachpb.TransactionRecord
	noTxnRecord := verifyFunc(nil)
	txnWithoutChanges := func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) roachpb.TransactionRecord {
		return txn.AsRecord()
	}
	txnWithStatus := func(status roachpb.TransactionStatus) verifyFunc {
		return func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) roachpb.TransactionRecord {
			record := txn.AsRecord()
			record.Status = status
			return record
		}
	}
	txnWithStagingStatusAndInFlightWrites := func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
		record := txnWithStatus(roachpb.STAGING)(tc, txn, now)
		record.InFlightWrites = inFlightWrites
		return record
	}
	getTestPusher := func(tc testContext) *roachpb.Transaction {
		pusher := newTransaction("test", roachpb.Key("a"), 1, tc.Clock())
		pusher.Priority = enginepb.MaxTxnPriority
		return pusher
	}

	type testCase struct {
		name     string
		setup    runFunc // all three functions are provided the same txn and timestamp
		run      runFunc
		expTxn   verifyFunc
		expError string // regexp pattern to match on run error, if not empty
	}
	testsWithEagerGC := []testCase{
		{
			name: "heartbeat transaction",
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.LastHeartbeat.Forward(hbTs)
				return record
			},
		},
		{
			name: "end transaction (stage)",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStagingStatusAndInFlightWrites,
		},
		{
			name: "end transaction (abort)",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit)",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (timestamp)",
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			// If no transaction record exists, the push (timestamp) request does
			// not create one. It only records its push in the tscache.
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (abort)",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			// If no transaction record exists, the push (abort) request does
			// not create one. It only records its push in the tscache.
			expTxn: noTxnRecord,
		},
		{
			// Should not happen because RecoverTxn requests are only
			// sent after observing a STAGING transaction record.
			name: "recover transaction (implicitly committed)",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "txn record synthesized with non-ABORTED status",
			expTxn:   noTxnRecord,
		},
		{
			// Should not happen because RecoverTxn requests are only
			// sent after observing a STAGING transaction record.
			name: "recover transaction (not implicitly committed)",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "txn record synthesized with non-ABORTED status",
			expTxn:   noTxnRecord,
		},
		{
			name: "heartbeat transaction after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now.Add(0, 5))
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.LastHeartbeat.Forward(hbTs.Add(0, 5))
				return record
			},
		},
		{
			name: "heartbeat transaction with epoch bump after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, now.Add(0, 5))
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStagingStatusAndInFlightWrites,
		},
		{
			name: "end transaction (abort) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (timestamp) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			// The transaction record **is not** updated in this case. Instead, the
			// push is communicated through the timestamp cache. When the pushee goes
			// to commit, it will consult the timestamp cache and find that it must
			// commit above the push timestamp.
			expTxn: txnWithoutChanges,
		},
		{
			name: "push transaction (abort) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, now)
				record.Priority = getTestPusher(tc).Priority - 1
				return record
			},
		},
		{
			// Staging transaction records can still be heartbeat.
			name: "heartbeat transaction after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, hbTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, hbTs)
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStagingStatusAndInFlightWrites,
		},
		{
			// Case of a transaction that refreshed after an unsuccessful
			// implicit commit. If the refresh is successful then the
			// transaction coordinator can attempt the implicit commit again.
			name: "end transaction (stage) with timestamp increase after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.ReadTimestamp.Forward(now)
				clone.WriteTimestamp.Forward(now)
				et, etH := endTxnArgs(clone, true /* commit */)
				// Add different in-flight writes to test whether they are
				// replaced by the second EndTxn request.
				et.InFlightWrites = otherInFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, now)
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, true /* commit */)
				// Add different in-flight writes to test whether they are
				// replaced by the second EndTxn request.
				et.InFlightWrites = otherInFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, now)
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			// Case of making a commit "explicit" after a successful implicit commit.
			name: "end transaction (commit) after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The transaction record will be eagerly GC-ed.
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (timestamp) after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expError: "found txn in indeterminate STAGING state",
			expTxn:   txnWithStagingStatusAndInFlightWrites,
		},
		{
			name: "push transaction (abort) after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expError: "found txn in indeterminate STAGING state",
			expTxn:   txnWithStagingStatusAndInFlightWrites,
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// re-writing new intents at higher timestamps. The push should not
			// consider the pushee to be staging.
			name: "push transaction (timestamp) after end transaction (stage) with outdated timestamp",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.WriteTimestamp = clone.WriteTimestamp.Add(0, 1)
				pt := pushTxnArgs(getTestPusher(tc), clone, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, pushTs)
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = getTestPusher(tc).Priority - 1
				return record
			},
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// re-writing new intents at higher timestamps. The push should not
			// consider the pushee to be staging.
			name: "push transaction (abort) after end transaction (stage) with outdated timestamp",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.WriteTimestamp = clone.WriteTimestamp.Add(0, 1)
				pt := pushTxnArgs(getTestPusher(tc), clone, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, pushTs)
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = getTestPusher(tc).Priority - 1
				return record
			},
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// writing new intents in a new epoch. The push should not consider
			// the pushee to be staging.
			name: "push transaction (timestamp) after end transaction (stage) with outdated epoch",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, clone.WriteTimestamp.Add(0, 1))
				pt := pushTxnArgs(getTestPusher(tc), clone, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, pushTs)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = getTestPusher(tc).Priority - 1
				return record
			},
		},
		{
			// The pushee attempted a parallel commit that failed, so it is now
			// writing new intents in a new epoch. The push should not consider
			// the pushee to be staging.
			name: "push transaction (abort) after end transaction (stage) with outdated epoch",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, clone.WriteTimestamp.Add(0, 1))
				pt := pushTxnArgs(getTestPusher(tc), clone, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, pushTs hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, pushTs)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp = record.WriteTimestamp.Add(0, 1)
				record.Priority = getTestPusher(tc).Priority - 1
				return record
			},
		},
		{
			name: "heartbeat transaction after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			name: "heartbeat transaction with epoch bump after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				// Restart the transaction at a higher timestamp. This will
				// increment its ReadTimestamp as well. We used to check the GC
				// threshold against this timestamp instead of its minimum
				// timestamp.
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (abort) after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: noTxnRecord,
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (commit) after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			name: "push transaction (timestamp) after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (abort) after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "heartbeat transaction after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (abort) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: noTxnRecord,
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (commit) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_RECORD_ALREADY_WRITTEN_POSSIBLE_REPLAY)",
			expTxn:   noTxnRecord,
		},
		{
			name: "push transaction (timestamp) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "push transaction (abort) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: noTxnRecord,
		},
		{
			name: "heartbeat transaction after push transaction (timestamp)",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now.Add(0, 5))
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txn.AsRecord()
				record.WriteTimestamp.Forward(now)
				record.LastHeartbeat.Forward(now.Add(0, 5))
				return record
			},
		},
		{
			name: "end transaction (stage) after push transaction (timestamp)",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
			// The end transaction (stage) does not write a transaction record
			// if it hits a serializable retry error.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (abort) after push transaction (timestamp)",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The end transaction (abort) request succeeds and cleans up the
			// transaction record.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit) after push transaction (timestamp)",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
			// The end transaction (commit) does not write a transaction record
			// if it hits a serializable retry error.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (one-phase commit) after push transaction (timestamp)",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionRetryError: retry txn (RETRY_SERIALIZABLE)",
			// The end transaction (commit) does not write a transaction record
			// if it hits a serializable retry error.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (one-phase commit) after push transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				return sendWrappedWithErr(tc, etH, &et)
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: noTxnRecord,
		},
		{
			// 1PC is disabled if the transaction already has a record to ensure
			// that the record is properly cleaned up by the EndTxn request. If
			// we did not disable 1PC then the test would not throw an error.
			name: "end transaction (one-phase commit required) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.Sequence = 1 // qualify for 1PC
				et.Require1PC = true
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "could not commit in one phase as requested",
			expTxn:   txnWithoutChanges,
		},
		{
			name: "heartbeat transaction after push transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			name: "heartbeat transaction with epoch bump after push transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				// Restart the transaction at a higher timestamp. This will
				// increment its ReadTimestamp as well. We used to check the GC
				// threshold against this timestamp instead of its minimum
				// timestamp.
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			name: "end transaction (stage) after push transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			name: "end transaction (abort) after push transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			// The end transaction (abort) request succeeds and cleans up the
			// transaction record.
			expTxn: noTxnRecord,
		},
		{
			name: "end transaction (commit) after push transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   noTxnRecord,
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "found PENDING record for implicitly committed transaction",
			expTxn:   txnWithoutChanges,
		},
		{
			// Typical case of transaction recovery from a STAGING status after
			// a successful implicit commit.
			name: "recover transaction (implicitly committed) after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.COMMITTED)(tc, txn, now)
				// RecoverTxn does not synchronously resolve local intents.
				record.LockSpans = intents
				return record
			},
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (stage) with timestamp increase",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.ReadTimestamp.Forward(now)
				clone.WriteTimestamp.Forward(now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "timestamp change by implicitly committed transaction",
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, now)
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (stage) with epoch bump",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "epoch change by implicitly committed transaction",
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, now)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (abort)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			// The transaction record was cleaned up, so RecoverTxn can't perform
			// the same assertion that it does in the case without eager gc.
			expTxn: noTxnRecord,
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator made its commit explicit.
			name: "recover transaction (implicitly committed) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: noTxnRecord,
		},
		{
			// Should not be possible.
			name: "recover transaction (not implicitly committed) after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "cannot recover PENDING transaction",
			expTxn:   txnWithoutChanges,
		},
		{
			// Transaction coordinator restarted after failing to perform a
			// implicit commit. Common case.
			name: "recover transaction (not implicitly committed) after heartbeat transaction with epoch bump",
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				hb, hbH := heartbeatArgs(clone, clone.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, now)
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
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.ReadTimestamp.Forward(now)
				clone.WriteTimestamp.Forward(now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				// Unchanged by the RecoverTxn request.
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, now)
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
			setup: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStagingStatusAndInFlightWrites(tc, txn, now)
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: noTxnRecord,
		},
		{
			// Should not be possible.
			name: "recover transaction (not implicitly committed) after end transaction (commit)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			// The transaction record was cleaned up, so RecoverTxn can't perform
			// the same assertion that it does in the case without eager gc.
			expTxn: noTxnRecord,
		},
	}
	testsWithoutEagerGC := []testCase{
		{
			name: "end transaction (abort) without eager gc",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStatus(roachpb.ABORTED),
		},
		{
			name: "end transaction (commit) without eager gc",
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
		{
			name: "end transaction (abort) without eager gc after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStatus(roachpb.ABORTED),
		},
		{
			name: "end transaction (commit) without eager gc after heartbeat transaction",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, txn.MinTimestamp)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
		{
			// Case of a rollback after an unsuccessful implicit commit.
			name: "end transaction (abort) with epoch bump, without eager gc after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				clone := txn.Clone()
				clone.Restart(-1, 0, now)
				et, etH := endTxnArgs(clone, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) roachpb.TransactionRecord {
				record := txnWithStatus(roachpb.ABORTED)(tc, txn, now)
				record.Epoch = txn.Epoch + 1
				record.WriteTimestamp.Forward(now)
				return record
			},
		},
		{
			name: "end transaction (commit) without eager gc after end transaction (stage)",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
		{
			name: "heartbeat transaction after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			// The heartbeat request won't throw an error, but also won't update the
			// transaction record. It will simply return the updated transaction state.
			// This is kind of strange, but also doesn't cause any issues.
			expError: "",
			expTxn:   txnWithStatus(roachpb.ABORTED),
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   txnWithStatus(roachpb.ABORTED),
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (abort) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expTxn: txnWithStatus(roachpb.ABORTED),
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (commit) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionAbortedError(ABORT_REASON_ABORTED_RECORD_FOUND)",
			expTxn:   txnWithStatus(roachpb.ABORTED),
		},
		{
			name: "push transaction (timestamp) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: txnWithStatus(roachpb.ABORTED),
		},
		{
			name: "push transaction (abort) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: txnWithStatus(roachpb.ABORTED),
		},
		{
			name: "heartbeat transaction after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				hb, hbH := heartbeatArgs(txn, now)
				return sendWrappedWithErr(tc, hbH, &hb)
			},
			// The heartbeat request won't throw an error, but also won't update the
			// transaction record. It will simply return the updated transaction state.
			// This is kind of strange, but also doesn't cause any issues.
			expError: "",
			expTxn:   txnWithStatus(roachpb.COMMITTED),
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (stage) after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				et.InFlightWrites = inFlightWrites
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
			expTxn:   txnWithStatus(roachpb.COMMITTED),
		},
		{
			// This case shouldn't happen in practice given a well-functioning
			// transaction coordinator, but is handled correctly nevertheless.
			name: "end transaction (abort) after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
			expTxn:   txnWithStatus(roachpb.COMMITTED),
		},
		{
			// Could be a replay or a retry.
			name: "end transaction (commit) after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			expError: "TransactionStatusError: already committed (REASON_TXN_COMMITTED)",
			expTxn:   txnWithStatus(roachpb.COMMITTED),
		},
		{
			name: "push transaction (timestamp) after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, now hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_TIMESTAMP)
				pt.PushTo = now
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
		{
			name: "push transaction (abort) after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				pt := pushTxnArgs(getTestPusher(tc), txn, kvpb.PUSH_ABORT)
				return sendWrappedWithErr(tc, kvpb.Header{}, &pt)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
		{
			// Should not be possible.
			name: "recover transaction (implicitly committed) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expError: "found ABORTED record for implicitly committed transaction",
			expTxn:   txnWithStatus(roachpb.ABORTED),
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator made its commit explicit.
			name: "recover transaction (implicitly committed) after end transaction (commit) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, true /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
		{
			// A concurrent recovery process completed or the transaction
			// coordinator rolled back its transaction record after an
			// unsuccessful implicit commit.
			name: "recover transaction (not implicitly committed) after end transaction (abort) without eager gc",
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, false /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: txnWithStatus(roachpb.ABORTED),
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
			setup: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				et, etH := endTxnArgs(txn, true /* commit */)
				return sendWrappedWithErr(tc, etH, &et)
			},
			run: func(tc testContext, txn *roachpb.Transaction, _ hlc.Timestamp) error {
				rt := recoverTxnArgs(txn, false /* implicitlyCommitted */)
				return sendWrappedWithErr(tc, kvpb.Header{}, &rt)
			},
			expTxn: txnWithStatus(roachpb.COMMITTED),
		},
	}

	testutils.RunTrueAndFalse(t, "disable-auto-gc", func(t *testing.T, disableAutoGC bool) {
		testCases := testsWithEagerGC
		if disableAutoGC {
			testCases = testsWithoutEagerGC
		}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
		tc := testContext{manualClock: manual}
		tsc := TestStoreConfig(hlc.NewClockForTesting(manual))
		tsc.TestingKnobs.DisableGCQueue = true
		tsc.TestingKnobs.DontRetryPushTxnFailures = true
		tsc.TestingKnobs.DontRecoverIndeterminateCommits = true
		tsc.TestingKnobs.EvalKnobs.DisableTxnAutoGC = disableAutoGC
		tc.StartWithStoreConfig(ctx, t, stopper, tsc)
		for _, c := range testCases {
			t.Run(c.name, func(t *testing.T) {
				txn := newTransaction(c.name, roachpb.Key(c.name), 1, tc.Clock())
				manual.Advance(99)
				runTs := tc.Clock().Now()

				if c.setup != nil {
					if err := c.setup(tc, txn, runTs); err != nil {
						t.Fatalf("failed during test setup: %+v", err)
					}
				}

				if err := c.run(tc, txn, runTs); err != nil {
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
					ctx, tc.repl.store.TODOEngine(), keys.TransactionKey(txn.Key, txn.ID),
					hlc.Timestamp{}, &foundRecord, storage.MVCCGetOptions{},
				); err != nil {
					t.Fatal(err)
				} else if found {
					if c.expTxn == nil {
						t.Fatalf("expected no txn record, found %v", found)
					}
					expRecord := c.expTxn(tc, txn, runTs)
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
	})
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

	res, pErr := kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{
		RangeID: tc.repl.RangeID,
		Txn:     txn,
	}, &kvpb.EndTxnRequest{
		RequestHeader: kvpb.RequestHeader{
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
	_, pErr = kv.SendWrappedWith(ctx, tc.Sender(), kvpb.Header{
		RangeID: tc.repl.RangeID,
		Txn:     txn,
	}, &kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{
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

	replicas := func(storeIDs ...roachpb.StoreID) []roachpb.ReplicaDescriptor {
		res := make([]roachpb.ReplicaDescriptor, len(storeIDs))
		for i, storeID := range storeIDs {
			res[i].NodeID = roachpb.NodeID(storeID)
			res[i].StoreID = storeID
			res[i].ReplicaID = roachpb.ReplicaID(i + 1)
		}
		return res
	}

	status := upToDateRaftStatus(replicas(1, 3, 5))
	assert.EqualValues(t, "", splitSnapshotWarningStr(12, status))

	pr := status.Progress[2]
	pr.State = tracker.StateProbe
	status.Progress[2] = pr

	assert.EqualValues(
		t,
		"; r12/2 is being probed (may or may not need a Raft snapshot)",
		splitSnapshotWarningStr(12, status),
	)

	pr.State = tracker.StateSnapshot

	assert.EqualValues(
		t,
		"; r12/2 is being probed (may or may not need a Raft snapshot)",
		splitSnapshotWarningStr(12, status),
	)
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
	putReq := func(key roachpb.Key) *kvpb.PutRequest {
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
				ba := &kvpb.BatchRequest{}
				ba.Add(putReq(keyA))
				minReadTS := r.store.Clock().Now()
				ba.Timestamp = minReadTS.Next()
				_, bumped := r.applyTimestampCache(ctx, ba, minReadTS)
				require.False(t, bumped)
				require.Equal(t, int32(0), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
		{
			// Test the case where the bump occurs due to minReadTS.
			name: "bump due to minTS", f: func(t *testing.T, r *Replica) {
				ba := &kvpb.BatchRequest{}
				ba.Add(putReq(keyA))
				ba.Timestamp = r.store.Clock().Now()
				minReadTS := ba.Timestamp.Next()
				_, bumped := r.applyTimestampCache(ctx, ba, minReadTS)
				require.True(t, bumped)
				require.Equal(t, int32(1), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
		{
			// Test the case where we bump due to the read ts cache rather than the minReadTS.
			name: "bump due to later read ts cache entry", f: func(t *testing.T, r *Replica) {
				ba := &kvpb.BatchRequest{}
				ba.Add(putReq(keyA))
				ba.Timestamp = r.store.Clock().Now()
				minReadTS := ba.Timestamp.Next()
				r.store.tsCache.Add(ctx, keyA, keyA, minReadTS.Next(), uuid.MakeV4())
				_, bumped := r.applyTimestampCache(ctx, ba, minReadTS)
				require.True(t, bumped)
				require.Equal(t, int32(0), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
		{
			// Test the case where we do initially bump due to the minReadTS but then
			// bump again to a higher ts due to the read ts cache.
			name: "higher bump due to read ts cache entry", f: func(t *testing.T, r *Replica) {
				ba := &kvpb.BatchRequest{}
				ba.Add(putReq(keyA))
				ba.Add(putReq(keyAA))
				ba.Timestamp = r.store.Clock().Now()
				minReadTS := ba.Timestamp.Next()
				t.Log(ba.Timestamp, minReadTS, minReadTS.Next())
				r.store.tsCache.Add(ctx, keyAA, keyAA, minReadTS.Next(), uuid.MakeV4())
				_, bumped := r.applyTimestampCache(ctx, ba, minReadTS)
				require.True(t, bumped)
				require.Equal(t, int32(0), telemetry.Read(batchesPushedDueToClosedTimestamp))
			},
		},
	} {
		runTestCase(c)
	}
}

func TestAdminScatterDestroyedReplica(t *testing.T) {
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

	desc := tc.repl.Desc()
	resp, err := tc.repl.adminScatter(ctx, kvpb.AdminScatterRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    roachpb.Key(desc.StartKey),
			EndKey: roachpb.Key(desc.EndKey),
		},
	})
	// The replica is destroyed so it can't be processed underneath the scatter
	// call. Expect that no bytes are scattered as a result.
	require.Equal(t, nil, err)
	require.Equal(t, int64(0), resp.ReplicasScatteredBytes)
}

// TestContainsEstimatesClamp tests the massaging of ContainsEstimates
// before proposing a raft command. It should always be >1 and an even number.
// See the comment on ContainEstimates to understand why.
func TestContainsEstimatesClampProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	someRequestToProposal := func(tc *testContext, ctx context.Context) *ProposalData {
		cmdIDKey := kvserverbase.CmdIDKey("some-cmdid-key")
		ba := &kvpb.BatchRequest{}
		ba.Timestamp = tc.Clock().Now()
		req := putArgs(roachpb.Key("some-key"), []byte("some-value"))
		ba.Add(&req)
		st := tc.repl.CurrentLeaseStatus(ctx)
		proposal, err := tc.repl.requestToProposal(ctx, cmdIDKey, ba, allSpansGuard(), &st, uncertainty.Interval{})
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
	prev, _ := batcheval.LookupCommand(kvpb.Put)

	mockPut := func(
		ctx context.Context, readWriter storage.ReadWriter, cArgs batcheval.CommandArgs, _ kvpb.Response,
	) (result.Result, error) {
		args := cArgs.Args.(*kvpb.PutRequest)
		opts := storage.MVCCWriteOptions{Txn: cArgs.Header.Txn, Stats: cArgs.Stats}
		opts.Stats.ContainsEstimates += containsEstimatesDelta
		ts := cArgs.Header.Timestamp
		_, err := storage.MVCCBlindPut(ctx, readWriter, args.Key, ts, args.Value, opts)
		return result.Result{}, err
	}

	batcheval.UnregisterCommand(kvpb.Put)
	batcheval.RegisterReadWriteCommand(kvpb.Put, batcheval.DefaultDeclareIsolatedKeys, mockPut)
	return func() {
		batcheval.UnregisterCommand(kvpb.Put)
		batcheval.RegisterReadWriteCommand(kvpb.Put, prev.DeclareKeys, prev.EvalRW)
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
		expError   string
	}

	const noop = internalChangeType(0)
	const none = roachpb.ReplicaType(-1)

	mkChanges := func(typs ...typOp) testCase {
		chgs := make([]internalReplicationChange, 0, len(typs))
		rDescs := make([]roachpb.ReplicaDescriptor, 0, len(typs))
		for i, typ := range typs {
			rDesc := roachpb.ReplicaDescriptor{
				ReplicaID: roachpb.ReplicaID(i + 1),
				NodeID:    roachpb.NodeID(100 * (1 + i)),
				StoreID:   roachpb.StoreID(100 * (1 + i)),
				Type:      typ.ReplicaType,
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
			desc: desc,
			chgs: chgs,
		}
	}
	mkTrigger := func(expTrigger string, typs ...typOp) testCase {
		tc := mkChanges(typs...)
		tc.expTrigger = expTrigger
		return tc
	}
	mkError := func(expError string, typs ...typOp) testCase {
		tc := mkChanges(typs...)
		tc.expError = expError
		return tc
	}

	tcs := []testCase{
		// Simple addition of learner.
		mkTrigger(
			"SIMPLE(l2) [(n200,s200):2LEARNER]: after=[(n100,s100):1 (n200,s200):2LEARNER] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{none, internalChangeTypeAddLearner},
		),
		// Simple addition of voter (necessarily via learner).
		mkTrigger(
			"SIMPLE(v2) [(n200,s200):2]: after=[(n100,s100):1 (n200,s200):2] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
		),
		// Simple removal of voter. This is not allowed. We require a demotion to
		// learner first.
		mkError(
			"cannot remove VOTER_FULL target n200,s200, not a LEARNER or NON_VOTER",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemoveLearner},
		),
		// Simple removal of learner.
		mkTrigger(
			"SIMPLE(r2) [(n200,s200):2LEARNER]: after=[(n100,s100):1] next=3",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.LEARNER, internalChangeTypeRemoveLearner},
		),

		// All other cases below need to go through joint quorums (though some
		// of them only due to limitations in etcd/raft).

		// Addition of learner and voter at same time (necessarily via learner).
		mkTrigger(
			"ENTER_JOINT(l3 v2) [(n200,s200):3LEARNER (n300,s300):2VOTER_INCOMING]: after=[(n100,s100):1 (n300,s300):2VOTER_INCOMING (n200,s200):3LEARNER] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{none, internalChangeTypeAddLearner},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
		),

		// Addition of learner and removal of voter at same time. Again, not allowed
		// due to removal of voter without demotion.
		mkError(
			"cannot remove VOTER_FULL target n300,s300, not a LEARNER or NON_VOTER",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{none, internalChangeTypeAddLearner},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemoveLearner},
		),

		// Promotion of two voters.
		mkTrigger(
			"ENTER_JOINT(v2 v3) [(n200,s200):2VOTER_INCOMING (n300,s300):3VOTER_INCOMING]: after=[(n100,s100):1 (n200,s200):2VOTER_INCOMING (n300,s300):3VOTER_INCOMING] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
			typOp{roachpb.LEARNER, internalChangeTypePromoteLearner},
		),

		// Removal of two voters. Again, not allowed due to removal of voter without
		// demotion.
		mkError(
			"cannot remove VOTER_FULL target n200,s200, not a LEARNER or NON_VOTER",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemoveLearner},
			typOp{roachpb.VOTER_FULL, internalChangeTypeRemoveLearner},
		),

		// Demoting two voters.
		mkTrigger(
			"ENTER_JOINT(r2 l2 r3 l3) [(n200,s200):2VOTER_DEMOTING_LEARNER (n300,s300):3VOTER_DEMOTING_LEARNER]: after=[(n100,s100):1 (n200,s200):2VOTER_DEMOTING_LEARNER (n300,s300):3VOTER_DEMOTING_LEARNER] next=4",
			typOp{roachpb.VOTER_FULL, noop},
			typOp{roachpb.VOTER_FULL, internalChangeTypeDemoteVoterToLearner},
			typOp{roachpb.VOTER_FULL, internalChangeTypeDemoteVoterToLearner},
		),
		// Leave joint config entered via demotion.
		mkTrigger(
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
			if tc.expError == "" {
				require.NoError(t, err)
				assert.Equal(t, tc.expTrigger, trigger.String())
			} else {
				require.Zero(t, tc.expTrigger)
				require.Nil(t, trigger)
				require.Regexp(t, tc.expError, err)
			}
		})
	}
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

	// Add a couple of bogus configuration changes to bump the generation to 2,
	// and request a new lease to bump the lease sequence to 2.
	_, err := tc.addBogusReplicaToRangeDesc(ctx)
	require.NoError(t, err)
	_, err = tc.addBogusReplicaToRangeDesc(ctx)
	require.NoError(t, err)

	{
		lease, _ := tc.repl.GetLease()
		tc.repl.RevokeLease(ctx, lease.Sequence)

		tc.repl.mu.Lock()
		st := tc.repl.leaseStatusAtRLocked(ctx, tc.Clock().NowAsClockTimestamp())
		ll := tc.repl.requestLeaseLocked(ctx, st, nil /* limiter */)
		tc.repl.mu.Unlock()
		select {
		case pErr := <-ll.C():
			require.NoError(t, pErr.GoError())
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}

	ri := tc.repl.GetRangeInfo(ctx)
	require.False(t, ri.Lease.Empty())
	require.Equal(t, roachpb.LAG_BY_CLUSTER_SETTING, ri.ClosedTimestampPolicy)
	require.EqualValues(t, 2, ri.Desc.Generation)
	require.EqualValues(t, 2, ri.Lease.Sequence)
	staleDescGen := ri.Desc.Generation - 1
	staleLeaseSeq := ri.Lease.Sequence - 1
	wrongCTPolicy := roachpb.LEAD_FOR_GLOBAL_READS

	for _, test := range []struct {
		cri roachpb.ClientRangeInfo
		exp *roachpb.RangeInfo
	}{
		{
			// Empty client info doesn't return any info. This case shouldn't happen
			// for requests via DistSender, but can happen e.g. with lease requests
			// that are submitted directly to the replica.
			cri: roachpb.ClientRangeInfo{},
			exp: nil,
		},
		{
			// ExplicitlyRequested returns lease info.
			cri: roachpb.ClientRangeInfo{
				ExplicitlyRequested: true,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, missing lease, correct closedts policy.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, stale lease, correct closedts policy.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				LeaseSequence:         staleLeaseSeq,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, correct lease, incorrect closedts policy.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				LeaseSequence:         ri.Lease.Sequence,
				ClosedTimestampPolicy: wrongCTPolicy,
			},
			exp: &ri,
		},
		{
			// Correct descriptor, correct lease, correct closedts policy.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  ri.Desc.Generation,
				LeaseSequence:         ri.Lease.Sequence,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: nil, // No update should be returned.
		},
		{
			// Stale descriptor, no lease, correct closedts policy.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  staleDescGen,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
		{
			// Stale descriptor, stale lease, incorrect closedts policy.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  staleDescGen,
				LeaseSequence:         staleLeaseSeq,
				ClosedTimestampPolicy: wrongCTPolicy,
			},
			exp: &ri,
		},
		{
			// Stale desc, good lease, correct closedts policy. This case
			// shouldn't happen.
			cri: roachpb.ClientRangeInfo{
				DescriptorGeneration:  staleDescGen,
				LeaseSequence:         staleLeaseSeq,
				ClosedTimestampPolicy: ri.ClosedTimestampPolicy,
			},
			exp: &ri,
		},
	} {
		t.Run("", func(t *testing.T) {
			ba := &kvpb.BatchRequest{}
			ba.Header.ClientRangeInfo = test.cri
			req := getArgs(roachpb.Key("a"))
			ba.Add(&req)
			br, pErr := tc.Sender().Send(ctx, ba)
			require.NoError(t, pErr.GoError())
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
	m.tenants.Range(func(tenID roachpb.TenantID, _ *tenantStorageMetrics) bool {
		metricsTenants[tenID] = struct{}{}
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
	ten123 := roachpb.MustMakeTenantID(123)
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
	_, pErr := leftRepl.AdminMerge(context.Background(), kvpb.AdminMergeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: leftRepl.Desc().StartKey.AsRawKey(),
		},
	}, "testing")
	require.NoError(t, pErr.GoError())

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

// TestReplicaRateLimit verifies the behaviour of Replica.maybeRateLimitBatch
// method which Replica.Send uses for rate limiting the incoming batch requests.
//
// In particular, it verifies that rate limiting blocks the flow when the limit
// is reached, and unblocks it with time passage. It also verifies that the
// method returns a correct error when it races with the Replica destruction,
// which causes upper layers to retry the request.
func TestReplicaRateLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	tc := testContext{manualClock: timeutil.NewManualTime(timeutil.Unix(0, 123))}
	cfg := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	// Set a low rate limit so that we saturate it quickly below.
	tenantrate.KVCURateLimit.Override(ctx, &cfg.Settings.SV, 1)
	cfg.TestingKnobs.DisableMergeWaitForReplicasInit = true
	// Use time travel to control the rate limiter in this test. Set authorizer to
	// engage the rate limiter, overriding the default allow-all policy in tests.
	cfg.TestingKnobs.TenantRateKnobs.QuotaPoolOptions = []quotapool.Option{quotapool.WithTimeSource(tc.manualClock)}
	cfg.TestingKnobs.TenantRateKnobs.Authorizer = tenantcapabilitiesauthorizer.New(cfg.Settings, nil)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	// A range for tenant 123 appears via a split.
	ten123 := roachpb.MustMakeTenantID(123)
	splitKey := keys.MustAddr(keys.MakeSQLCodec(ten123).TenantPrefix())
	leftRepl := tc.store.LookupReplica(splitKey)
	require.NotNil(t, leftRepl)
	splitTestRange(tc.store, splitKey, t)
	tenRepl := tc.store.LookupReplica(splitKey)
	require.NotNil(t, tenRepl)
	require.NotNil(t, tenRepl.tenantMetricsRef)
	require.NotNil(t, tenRepl.tenantLimiter)

	tenCtx := roachpb.ContextWithClientTenant(ctx, ten123)
	put := func(timeout time.Duration) error {
		ba := &kvpb.BatchRequest{}
		req := putArgs(splitKey.AsRawKey(), []byte{1, 2, 7})
		ba.Add(&req)
		ctx, cancel := context.WithTimeout(tenCtx, timeout)
		defer cancel()
		return tenRepl.maybeRateLimitBatch(ctx, ba)
	}

	// Verify that first few writes succeed fast, but eventually requests start
	// timing out because of the rate limiter.
	const timeout = 10 * time.Millisecond
	require.NoError(t, put(timeout))
	require.NoError(t, put(timeout))
	block := func() bool {
		for i := 0; i < 1000; i++ {
			if err := put(timeout); errors.Is(err, context.DeadlineExceeded) {
				return true
			}
		}
		return false
	}
	require.True(t, block())

	// Verify that the rate limiter eventually starts allowing requests again.
	tc.manualClock.Advance(100 * time.Second)
	require.NoError(t, put(timeout))
	// But will block them again if there are too many.
	require.True(t, block())

	// Now the rate limiter is saturated. If we try to write a request to the
	// replica now, the rate limiter will block it. If this races with a range
	// destruction (for example, due to a merge like below), maybeRateLimitBatch()
	// returns a quota pool closed error.
	g := ctxgroup.WithContext(ctx)
	g.Go(func() error {
		_, pErr := leftRepl.AdminMerge(ctx, kvpb.AdminMergeRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: leftRepl.Desc().StartKey.AsRawKey(),
			},
		}, "testing")
		return pErr.GoError()
	})
	err := put(5 * time.Second)
	require.True(t, errors.Is(err, &kvpb.RangeNotFoundError{RangeID: 2, StoreID: 1}), err)

	require.NoError(t, g.Wait())
}

// TestRangeSplitRacesWithRead performs a range split and repeatedly reads a
// span that straddles both the LHS and RHS post split. We ensure that as long
// as the read wins it observes the entire result set; if (once) the split wins
// the read should return the appropriate error. However, it should never be
// possible for the read to return without error and with a partial result (e.g.
// just the post split LHS). This would indicate a bug in the synchronization
// between read and split operations.
//
// We include subtests for both follower reads and reads served from the
// leaseholder.
func TestRangeSplitRacesWithRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "followerRead", func(t *testing.T, followerRead bool) {
		ctx := context.Background()
		tc := serverutils.StartCluster(t, 2, base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
		defer tc.Stopper().Stop(ctx)
		key := tc.ScratchRange(t)
		key = key[:len(key):len(key)] // bound capacity, avoid aliasing
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

		keyA := append(key, byte('a'))
		keyB := append(key, byte('b'))
		keyC := append(key, byte('c'))
		keyD := append(key, byte('d'))
		splitKey := keyB

		now := tc.Server(0).Clock().Now()
		ts1 := now.Add(1, 0)
		h1 := kvpb.Header{RangeID: desc.RangeID, Timestamp: ts1}

		val := []byte("value")
		for _, k := range [][]byte{keyA, keyC} {
			pArgs := putArgs(k, val)
			_, pErr := kv.SendWrappedWith(ctx, writer, h1, &pArgs)
			require.Nil(t, pErr)
		}

		// If the test wants to read from a follower, drop the closed timestamp
		// duration and then wait until the follower can serve requests at ts1.
		if followerRead {
			_, err := tc.ServerConn(0).Exec(
				`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
			require.NoError(t, err)

			testutils.SucceedsSoon(t, func() error {
				ba := &kvpb.BatchRequest{}
				ba.RangeID = desc.RangeID
				ba.ReadConsistency = kvpb.INCONSISTENT
				ba.Add(&kvpb.QueryResolvedTimestampRequest{
					RequestHeader: kvpb.RequestHeader{Key: key, EndKey: key.Next()},
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

		read := func() {
			scanArgs := scanArgs(keyA, keyD)
			for {
				resp, pErr := kv.SendWrappedWith(ctx, reader, h1, scanArgs)
				if pErr == nil {
					t.Logf("read won the race: %v", resp)
					require.NotNil(t, resp)
					res := resp.(*kvpb.ScanResponse).Rows
					require.Equal(t, 2, len(res))
					require.Equal(t, keyA, res[0].Key)
					require.Equal(t, keyC, res[1].Key)
				} else {
					t.Logf("read lost the race: %v", pErr)
					mismatchErr := &kvpb.RangeKeyMismatchError{}
					require.ErrorAs(t, pErr.GoError(), &mismatchErr)
					return
				}
			}
		}

		split := func() {
			splitArgs := &kvpb.AdminSplitRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: splitKey,
				},
				SplitKey: splitKey,
			}
			_, pErr := kv.SendWrappedWith(ctx, writer, h1, splitArgs)
			require.Nil(t, pErr, "err: %v", pErr.GoError())
			rhsDesc := tc.LookupRangeOrFatal(t, splitKey.Next())
			// Remove the RHS from the reader.
			if followerRead {
				tc.RemoveVotersOrFatal(t, roachpb.Key(rhsDesc.StartKey), tc.Target(1))
			} else {
				tc.TransferRangeLeaseOrFatal(t, rhsDesc, tc.Target(1))
				tc.RemoveVotersOrFatal(t, roachpb.Key(rhsDesc.StartKey), tc.Target(0))
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); split() }()
		go func() { defer wg.Done(); read() }()
		wg.Wait()
	})
}

// TestRangeSplitAndRHSRemovalRacesWithFollowerReads acts as a regression test
// for the hazard described in
// https://github.com/cockroachdb/cockroach/issues/67016.
//
// Specifically, the test sets up the following scenario:
// - Follower read begins and checks the request is contained entirely within
// the range's bounds. A storage snapshot isn't acquired just quite yet.
// - The range is split such that the follower read is no longer within the post
// split range; the post-split RHS replica is removed from the node serving the
// follower read.
// - Follower read resumes. The expectation is for the follower read to fail
// with a RangeKeyMismatchError.
func TestRangeSplitAndRHSRemovalRacesWithFollowerRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	startSplit := make(chan struct{})
	unblockRead := make(chan struct{})
	scratchRangeID := roachpb.RangeID(-1)
	tc := serverutils.StartCluster(t, 2, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			1: {
				Knobs: base.TestingKnobs{
					Store: &StoreTestingKnobs{
						PreStorageSnapshotButChecksCompleteInterceptor: func(r *Replica) {
							if r.GetRangeID() != scratchRangeID {
								return
							}
							close(startSplit)
							<-unblockRead
						},
					},
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)
	key := tc.ScratchRange(t)
	key = key[:len(key):len(key)] // bound capacity, avoid aliasing
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
	reader := stores[1]

	keyA := append(key, byte('a'))
	keyB := append(key, byte('b'))
	keyC := append(key, byte('c'))
	keyD := append(key, byte('d'))
	splitKey := keyB

	now := tc.Server(0).Clock().Now()
	ts1 := now.Add(1, 0)
	h1 := kvpb.Header{RangeID: desc.RangeID, Timestamp: ts1}

	val := []byte("value")
	for _, k := range [][]byte{keyA, keyC} {
		pArgs := putArgs(k, val)
		_, pErr := kv.SendWrappedWith(ctx, writer, h1, &pArgs)
		require.Nil(t, pErr)
	}

	// Drop the closed timestamp duration and wait until the follower can serve
	// requests at ts1.
	_, err := tc.ServerConn(0).Exec(
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		ba := &kvpb.BatchRequest{}
		ba.RangeID = desc.RangeID
		ba.ReadConsistency = kvpb.INCONSISTENT
		ba.Add(&kvpb.QueryResolvedTimestampRequest{
			RequestHeader: kvpb.RequestHeader{Key: key, EndKey: key.Next()},
		})
		br, pErr := reader.Send(ctx, ba)
		require.Nil(t, pErr)
		rts := br.Responses[0].GetQueryResolvedTimestamp().ResolvedTS
		if rts.Less(ts1) {
			return errors.Errorf("resolved timestamp %s < %s", rts, ts1)
		}
		return nil
	})

	// Set this thing after we've checked for the resolved timestamp, as we don't
	// want the QueryResolvedTimestampRequest to block.
	scratchRangeID = desc.RangeID

	read := func() {
		scanArgs := scanArgs(keyA, keyD)
		_, pErr := kv.SendWrappedWith(ctx, reader, h1, scanArgs)
		require.NotNil(t, pErr)
		mismatchErr := &kvpb.RangeKeyMismatchError{}
		require.ErrorAs(t, pErr.GoError(), &mismatchErr)
	}

	split := func() {
		select {
		case <-startSplit:
		case <-time.After(5 * time.Second):
			panic("timed out waiting for read to block")
		}
		splitArgs := &kvpb.AdminSplitRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: splitKey,
			},
			SplitKey: splitKey,
		}
		_, pErr := kv.SendWrappedWith(ctx, writer, h1, splitArgs)
		require.Nil(t, pErr, "err: %v", pErr.GoError())
		rhsDesc := tc.LookupRangeOrFatal(t, splitKey.Next())
		tc.RemoveVotersOrFatal(t, roachpb.Key(rhsDesc.StartKey), tc.Target(1))
		close(unblockRead)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); split() }()
	go func() { defer wg.Done(); read() }()
	wg.Wait()
}

// TestResolveIntentReplicatedLocksBumsTSCache ensures that performing point
// intent resolution over keys on which we have acquired replicated shared or
// exclusive locks bumps the timestamp cache for those keys if the transaction
// has successfully committed. Otherwise, if the transaction is aborted, intent
// resolution does not bump the timestamp cache.
func TestResolveIntentReplicatedLocksBumpsTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	newTxn := func(key string, ts hlc.Timestamp) *roachpb.Transaction {
		// We're going to use read committed isolation level here, as that's the
		// only one which makes sense in the context of this test. That's because
		// serializable transactions cannot commit before refreshing their reads,
		// and refreshing reads bumps the timestamp cache.
		txn := roachpb.MakeTransaction("test", roachpb.Key(key), isolation.ReadCommitted, roachpb.NormalUserPriority, ts, 0, 0, 0, false /* omitInRangefeeds */)
		return &txn
	}

	run := func(t *testing.T, isCommit, isReplicated bool, str lock.Strength) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		startTime := timeutil.Unix(0, 123)
		tc.manualClock = timeutil.NewManualTime(startTime)
		sc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
		sc.TestingKnobs.DisableCanAckBeforeApplication = true
		tc.StartWithStoreConfig(ctx, t, stopper, sc)

		// Write some keys at time t0.
		t0 := timeutil.Unix(1, 0)
		tc.manualClock.MustAdvanceTo(t0)
		err := tc.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			for _, keyStr := range []string{"a", "b", "c"} {
				err := txn.Put(ctx, roachpb.Key(keyStr), "value")
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		// Scan [a, e) at t1, acquiring locks as dictated by the test setup. Verify
		// the scan result is correct.
		t1 := timeutil.Unix(2, 0)
		ba := &kvpb.BatchRequest{}
		txn := newTxn("a", makeTS(t1.UnixNano(), 0))
		ba.Timestamp = makeTS(t1.UnixNano(), 0)
		ba.Txn = txn
		span := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}
		sArgs := scanArgs(span.Key, span.EndKey)
		sArgs.KeyLockingStrength = str
		sArgs.KeyLockingDurability = lock.Unreplicated
		if isReplicated {
			sArgs.KeyLockingDurability = lock.Replicated
		}
		ba.Add(sArgs)
		_, pErr := tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		// Commit the transaction at t2.
		t2 := timeutil.Unix(3, 0)
		ba = &kvpb.BatchRequest{}
		txn.WriteTimestamp = makeTS(t2.UnixNano(), 0)
		ba.Txn = txn
		et, _ := endTxnArgs(txn, isCommit)
		// Omit lock spans so that we can release locks asynchronously, as if
		// the locks were on a different range than the txn record.
		et.LockSpans = nil
		ba.Add(&et)
		_, pErr = tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		status := roachpb.ABORTED
		if isCommit {
			status = roachpb.COMMITTED
		}

		// Perform point intent resolution.
		ba = &kvpb.BatchRequest{}
		for _, keyStr := range []string{"a", "b", "c"} {
			resolveIntentArgs := resolveIntentArgsString(keyStr, txn.TxnMeta, status)
			ba.Add(&resolveIntentArgs)
		}
		_, pErr = tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		notBumpedTs := makeTS(t1.UnixNano(), 0) // we scanned at t1 over [a, e)
		bumpedTs := makeTS(t2.UnixNano(), 0)    // if we committed, it was at t2
		expTs := notBumpedTs
		if isCommit && isReplicated {
			expTs = bumpedTs
		}

		rTS, _ := tc.store.tsCache.GetMax(ctx, roachpb.Key("a"), nil)
		require.Equal(t, expTs, rTS)
		rTS, _ = tc.store.tsCache.GetMax(ctx, roachpb.Key("b"), nil)
		require.Equal(t, expTs, rTS)
		rTS, _ = tc.store.tsCache.GetMax(ctx, roachpb.Key("c"), nil)
		require.Equal(t, expTs, rTS)
		rTS, _ = tc.store.tsCache.GetMax(ctx, roachpb.Key("d"), nil)
		require.Equal(t, notBumpedTs, rTS)
	}

	testutils.RunTrueAndFalse(t, "isCommit", func(t *testing.T, isCommit bool) {
		testutils.RunTrueAndFalse(t, "isReplicated", func(t *testing.T, isReplicated bool) {
			for _, str := range []lock.Strength{lock.Shared, lock.Exclusive} {
				t.Run(str.String(), func(t *testing.T) {
					run(t, isCommit, isReplicated, str)
				})
			}
		})
	})
}

// TestResolveIntentRangeReplicatedLocksBumpsTSCache is like
// TestResolveIntentReplicatedLocksBumpsTSCache, except it tests
// ResolveIntentRange requests instead of ResolveIntent requests. As a result,
// we assert that the timestamp cache is bumped over the entire range specified
// in the ResolveIntentRangeRequest, not just the point keys that had locks on
// them.
func TestResolveIntentRangeReplicatedLocksBumpsTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	newTxn := func(key string, ts hlc.Timestamp) *roachpb.Transaction {
		// We're going to use read committed isolation level here, as that's the
		// only one which makes sense in the context of this test. That's because
		// serializable transactions cannot commit before refreshing their reads,
		// and refreshing reads bumps the timestamp cache.
		txn := roachpb.MakeTransaction("test", roachpb.Key(key), isolation.ReadCommitted, roachpb.NormalUserPriority, ts, 0, 0, 0, false /* omitInRangefeeds */)
		return &txn
	}

	run := func(t *testing.T, isCommit, isReplicated bool, str lock.Strength) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		startTime := timeutil.Unix(0, 123)
		tc.manualClock = timeutil.NewManualTime(startTime)
		sc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
		sc.TestingKnobs.DisableCanAckBeforeApplication = true
		tc.StartWithStoreConfig(ctx, t, stopper, sc)

		// Write some keys at time t0.
		t0 := timeutil.Unix(1, 0)
		tc.manualClock.MustAdvanceTo(t0)
		err := tc.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			for _, keyStr := range []string{"a", "b", "c"} {
				err := txn.Put(ctx, roachpb.Key(keyStr), "value")
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		// Scan [a, e) at t1, acquiring locks as dictated by the test setup. Verify
		// the scan result is correct.
		t1 := timeutil.Unix(2, 0)
		ba := &kvpb.BatchRequest{}
		txn := newTxn("a", makeTS(t1.UnixNano(), 0))
		ba.Timestamp = makeTS(t1.UnixNano(), 0)
		ba.Txn = txn
		span := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}
		sArgs := scanArgs(span.Key, span.EndKey)
		sArgs.KeyLockingStrength = str
		sArgs.KeyLockingDurability = lock.Unreplicated
		if isReplicated {
			sArgs.KeyLockingDurability = lock.Replicated
		}
		ba.Add(sArgs)
		_, pErr := tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		// Commit the transaction at t2.
		t2 := timeutil.Unix(3, 0)
		ba = &kvpb.BatchRequest{}
		txn.WriteTimestamp = makeTS(t2.UnixNano(), 0)
		ba.Txn = txn
		et, _ := endTxnArgs(txn, isCommit)
		// Omit lock spans so that we can release locks asynchronously, as if
		// the locks were on a different range than the txn record.
		et.LockSpans = nil
		ba.Add(&et)
		_, pErr = tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		status := roachpb.ABORTED
		if isCommit {
			status = roachpb.COMMITTED
		}

		// Perform point intent resolution.
		ba = &kvpb.BatchRequest{}
		resolveIntentRangeArgs := resolveIntentRangeArgsString("a", "e", txn.TxnMeta, status)
		ba.Add(resolveIntentRangeArgs)
		_, pErr = tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		notBumpedTs := makeTS(t1.UnixNano(), 0) // we scanned at t1 over [a, e)
		bumpedTs := makeTS(t2.UnixNano(), 0)    // if we committed, it was at t2
		expTs := notBumpedTs
		if isCommit && isReplicated {
			expTs = bumpedTs
		}

		for _, keyStr := range []string{"a", "b", "c", "d"} {
			rTS, _ := tc.store.tsCache.GetMax(ctx, roachpb.Key(keyStr), nil)
			require.Equal(t, expTs, rTS)
		}
	}

	testutils.RunTrueAndFalse(t, "isCommit", func(t *testing.T, isCommit bool) {
		testutils.RunTrueAndFalse(t, "isReplicated", func(t *testing.T, isReplicated bool) {
			for _, str := range []lock.Strength{lock.Shared, lock.Exclusive} {
				t.Run(str.String(), func(t *testing.T) {
					run(t, isCommit, isReplicated, str)
				})
			}
		})
	})
}

// TestEndTxnReplicatedLocksBumpsTSCache is like
// TestResolveIntentReplicatedLocksBumpsTSCache, except it tests EndTxn requests
// (which synchronously resolve local locks) instead of ResolveIntent requests.
func TestEndTxnReplicatedLocksBumpsTSCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	newTxn := func(key string, ts hlc.Timestamp) *roachpb.Transaction {
		// We're going to use read committed isolation level here, as that's the
		// only one which makes sense in the context of this test. That's because
		// serializable transactions cannot commit before refreshing their reads,
		// and refreshing reads bumps the timestamp cache.
		txn := roachpb.MakeTransaction("test", roachpb.Key(key), isolation.ReadCommitted, roachpb.NormalUserPriority, ts, 0, 0, 0, false /* omitInRangefeeds */)
		return &txn
	}

	run := func(t *testing.T, isCommit, isReplicated bool, str lock.Strength) {
		ctx := context.Background()
		tc := testContext{}
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		startTime := timeutil.Unix(0, 123)
		tc.manualClock = timeutil.NewManualTime(startTime)
		sc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
		sc.TestingKnobs.DisableCanAckBeforeApplication = true
		tc.StartWithStoreConfig(ctx, t, stopper, sc)

		// Write some keys at time t0.
		t0 := timeutil.Unix(1, 0)
		tc.manualClock.MustAdvanceTo(t0)
		err := tc.store.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			for _, keyStr := range []string{"a", "b", "c"} {
				err := txn.Put(ctx, roachpb.Key(keyStr), "value")
				if err != nil {
					return err
				}
			}
			return nil
		})
		require.NoError(t, err)

		// Scan [a, e) at t1, acquiring locks as dictated by the test setup. Verify
		// the scan result is correct.
		t1 := timeutil.Unix(2, 0)
		ba := &kvpb.BatchRequest{}
		txn := newTxn("a", makeTS(t1.UnixNano(), 0))
		ba.Timestamp = makeTS(t1.UnixNano(), 0)
		ba.Txn = txn
		span := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}
		sArgs := scanArgs(span.Key, span.EndKey)
		sArgs.KeyLockingStrength = str
		sArgs.KeyLockingDurability = lock.Unreplicated
		if isReplicated {
			sArgs.KeyLockingDurability = lock.Replicated
		}
		ba.Add(sArgs)
		_, pErr := tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		// Commit the transaction at t2.
		t2 := timeutil.Unix(3, 0)
		ba = &kvpb.BatchRequest{}
		txn.WriteTimestamp = makeTS(t2.UnixNano(), 0)
		ba.Txn = txn
		et, _ := endTxnArgs(txn, isCommit)
		// Assign lock spans to the EndTxn request. Assign one point lock span
		// and one range lock span.
		et.LockSpans = []roachpb.Span{
			{Key: roachpb.Key("a")},
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")},
		}
		ba.Add(&et)
		_, pErr = tc.Sender().Send(ctx, ba)
		require.Nil(t, pErr)

		notBumpedTs := makeTS(t1.UnixNano(), 0) // we scanned at t1 over [a, e)
		bumpedTs := makeTS(t2.UnixNano(), 0)    // if we committed, it was at t2
		expTs := notBumpedTs
		if isCommit && isReplicated {
			expTs = bumpedTs
		}

		rTS, _ := tc.store.tsCache.GetMax(ctx, roachpb.Key("a"), nil)
		require.Equal(t, expTs, rTS)
		rTS, _ = tc.store.tsCache.GetMax(ctx, roachpb.Key("b"), nil)
		require.Equal(t, notBumpedTs, rTS)
		rTS, _ = tc.store.tsCache.GetMax(ctx, roachpb.Key("c"), nil)
		require.Equal(t, expTs, rTS)
		rTS, _ = tc.store.tsCache.GetMax(ctx, roachpb.Key("d"), nil)
		require.Equal(t, expTs, rTS)
	}

	testutils.RunTrueAndFalse(t, "isCommit", func(t *testing.T, isCommit bool) {
		testutils.RunTrueAndFalse(t, "isReplicated", func(t *testing.T, isReplicated bool) {
			for _, str := range []lock.Strength{lock.Shared, lock.Exclusive} {
				t.Run(str.String(), func(t *testing.T) {
					run(t, isCommit, isReplicated, str)
				})
			}
		})
	})
}

// TestReplayWithBumpedTimestamp serves as a regression test for the bug
// identified in https://github.com/cockroachdb/cockroach/pull/113295. It
// ensures that a replay with a bumped timestamp does not incorrectly
// communicate the timestamp at which the intent was actually written -- doing
// so can lead to infinite lock discovery cycles, as illustrated in the linked
// issue.
func TestReplayWithBumpedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	startTime := timeutil.Unix(0, 123)
	tc.manualClock = timeutil.NewManualTime(startTime)
	sc := TestStoreConfig(hlc.NewClockForTesting(tc.manualClock))
	sc.TestingKnobs.DisableCanAckBeforeApplication = true
	tc.StartWithStoreConfig(ctx, t, stopper, sc)

	// We'll issue a put from txn1 at ts0.
	k := roachpb.Key("a")
	t0 := timeutil.Unix(1, 0)
	tc.manualClock.MustAdvanceTo(t0)
	txn1 := roachpb.MakeTransaction(
		"t1", k, isolation.Serializable, roachpb.NormalUserPriority, makeTS(t0.UnixNano(), 0), 0, 0, 0, false, /* omitInRangefeeds */
	)
	pArgs := putArgs(k, []byte("value"))
	ba := &kvpb.BatchRequest{}
	ba.Txn = &txn1
	ba.Add(&pArgs)
	_, pErr := tc.Sender().Send(ctx, ba)
	require.Nil(t, pErr)

	// Sanity check the number of locks in the lock table is expected; we'll then
	// build on this precondition below.
	if tc.repl.concMgr.LockTableMetrics().Locks != 0 {
		t.Fatal("unexpected number of locks")
	}

	// Un-contended replicated locks are dropped by the lock table (as evidenced
	// by the pre-condition above). We need the replicated lock to be tracked to
	// reconstruct the hazard described in
	// https://github.com/cockroachdb/cockroach/pull/113295. We do so by adding a
	// non-locking waiter for this key which will discover and pull the lock into
	// the lock table. Note that we do so before replaying the put at a higher
	// timestamp.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := tc.store.DB().Txn(ctx, func(ctxt context.Context, txn *kv.Txn) error {
			_, err := txn.Get(ctx, k)
			return err
		})
		if err != nil {
			t.Error(err)
		}
	}()

	testutils.SucceedsSoon(t, func() error {
		if tc.repl.concMgr.LockTableMetrics().Locks != 1 {
			return errors.New("waiting for lock to be pulled into the lock table")
		}
		return nil
	})

	// Replay the same put at a higher timestamp.
	t2 := timeutil.Unix(3, 0)
	txn1.WriteTimestamp = makeTS(t2.UnixNano(), 0)
	_, pErr = tc.Sender().Send(ctx, ba)
	require.Nil(t, pErr, "unexpected error : %v", pErr.GoError())

	// Issue a read request at a timestamp t1, t0 < t1 < t2.
	t1 := timeutil.Unix(2, 0)
	tc.manualClock.MustAdvanceTo(t1)
	// We want to ensure that txn2 doesn't end up in an infinite lock discovery
	// cycle and is able to push txn1. We set txn2's priority to high to ensure it
	// can successfully push txn1's timestamp instead of continuing to block
	// indefinitely.
	txn2 := roachpb.MakeTransaction(
		"t2", k, isolation.Serializable, roachpb.MaxUserPriority, makeTS(t1.UnixNano(), 0), 0, 0, 0, false, /* omitInRangefeeds */
	)
	gArgs := getArgs(k)
	ba = &kvpb.BatchRequest{}
	ba.Txn = &txn2
	ba.Add(&gArgs)
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, pErr = tc.Sender().Send(ctx, ba)
		if pErr != nil {
			t.Error(pErr)
		}
	}()

	wg.Wait()
}

// TestLockAcquisition1PCInteractions ensures transactions (regardless of
// isolation level) that acquire replicated locks do not commit using one phase
// commit.
func TestLockAcquisitions1PCInteractions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Construct a new DB with a fresh set of TxnMetrics. This allows the test to
	// precisely assert on successful 1PC attempts without having to worry about
	// other transactions in the system affecting them, such as node liveness
	// heartbeats.
	metrics := kvcoord.MakeTxnMetrics(metric.TestSampleInterval)
	distSender := s.DistSenderI().(*kvcoord.DistSender)
	tcsFactoryCfg := kvcoord.TxnCoordSenderFactoryConfig{
		AmbientCtx: s.AmbientCtx(),
		Settings:   s.ClusterSettings(),
		Clock:      s.Clock(),
		Stopper:    s.Stopper(),
		Metrics:    metrics,
		TestingKnobs: kvcoord.ClientTestingKnobs{
			// This test makes assumptions about which range the transaction record
			// should be on.
			DisableTxnAnchorKeyRandomization: true,
		},
	}
	tcsFactory := kvcoord.NewTxnCoordSenderFactory(tcsFactoryCfg, distSender)
	testDB := kv.NewDBWithContext(s.AmbientCtx(), tcsFactory, s.Clock(), kvDB.Context())

	run := func(
		t *testing.T,
		acquireReplicated bool,
		iso isolation.Level,
		external bool,
		acquisitionInETBatch bool,
	) {
		successful1PCBefore := metrics.Commits1PC.Count()

		// Perform a range split between key A and B.
		keyA, keyB := roachpb.Key("a"), roachpb.Key("b")
		_, _, err := s.SplitRange(keyB)
		require.NoError(t, err)

		// Write a value to a key A and B.
		_, err = testDB.Inc(ctx, keyA, 1)
		require.Nil(t, err)
		_, err = testDB.Inc(ctx, keyB, 1)
		require.Nil(t, err)

		// Create a new transaction.
		txn := testDB.NewTxn(ctx, "test")
		err = txn.SetIsoLevel(iso)
		require.NoError(t, err)

		b := txn.NewBatch()
		// Ensure the txn record is anchored on a key in the same range as the one
		// we will send the EndTxn request to. This is required for us to consider
		// attempting a 1PC.
		b.GetForUpdate(keyA, kvpb.BestEffort)

		lockDur := kvpb.BestEffort
		if acquireReplicated {
			lockDur = kvpb.GuaranteedDurability
		}

		if external {
			b.GetForUpdate(keyB, lockDur)
		} else {
			b.GetForUpdate(keyA, lockDur)
		}
		if !acquisitionInETBatch {
			// Run the locking read batch first.
			err = txn.Run(ctx, b)
			require.NoError(t, err)
			// Then create a new batch to commit.
			b = txn.NewBatch()
		}
		b.Inc(keyA, 1)
		err = txn.CommitInBatch(ctx, b)
		require.NoError(t, err)

		successful1PCAfter := metrics.Commits1PC.Count()
		if acquireReplicated {
			require.Equal(t, successful1PCBefore, successful1PCAfter)
		} else {
			require.Greater(t, successful1PCAfter, successful1PCBefore)
		}
	}

	testutils.RunTrueAndFalse(t, "replicated", func(t *testing.T, acquireReplicated bool) {
		isolation.RunEachLevel(t, func(t *testing.T, iso isolation.Level) {
			testutils.RunTrueAndFalse(t, "external", func(t *testing.T, external bool) {
				testutils.RunTrueAndFalse(t, "acquisitionInETBatch",
					func(t *testing.T, acquisitionInETBatch bool) {
						run(t, acquireReplicated, iso, external, acquisitionInETBatch)
					})
			})
		})
	})
}
