// Copyright 2019 The Cockroach Authors.
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
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/tracker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/sync/errgroup"
)

// testProposer is a testing implementation of proposer.
type testProposer struct {
	syncutil.RWMutex
	clock      *hlc.Clock
	ds         destroyStatus
	lai        uint64
	enqueued   int
	registered int

	// If not nil, this can be a testProposerRaft used to mock the raft group
	// passed to FlushLockedWithRaftGroup().
	raftGroup proposerRaft
	// If not nil, this is called by RejectProposalWithRedirectLocked(). If nil,
	// RejectProposalWithRedirectLocked() panics.
	onRejectProposalWithRedirectLocked func(prop *ProposalData, redirectTo roachpb.ReplicaID)
	// ownsValidLease is returned by ownsValidLeaseRLocked()
	ownsValidLease bool

	// leaderReplicaInDescriptor is set if the leader (as indicated by raftGroup)
	// is known, and that leader is part of the range's descriptor (as seen by the
	// current replica). This can be used to simulate the local replica being so
	// far behind that it doesn't have an up to date descriptor.
	leaderReplicaInDescriptor bool
	// If leaderReplicaInDescriptor is set, this specifies what type of replica it
	// is. Some types of replicas are not eligible to get a lease.
	leaderReplicaType roachpb.ReplicaType
	rangePolicy       roachpb.RangeClosedTimestampPolicy
}

var _ proposer = &testProposer{}

// testProposerRaft is a testing implementation of proposerRaft. It is meant to
// be used as the Raft group used by a testProposer, and it records the commands
// being proposed.
type testProposerRaft struct {
	status raft.BasicStatus
	// proposals are the commands that the propBuf flushed (i.e. passed to the
	// Raft group) and have not yet been consumed with consumeProposals().
	proposals []kvserverpb.RaftCommand
}

var _ proposerRaft = &testProposerRaft{}

func (t *testProposerRaft) Step(msg raftpb.Message) error {
	if msg.Type != raftpb.MsgProp {
		return nil
	}
	// Decode and save all the commands.
	for _, e := range msg.Entries {
		_ /* idKey */, encodedCommand := kvserverbase.DecodeRaftCommand(e.Data)
		t.proposals = append(t.proposals, kvserverpb.RaftCommand{})
		if err := protoutil.Unmarshal(encodedCommand, &t.proposals[len(t.proposals)-1]); err != nil {
			return err
		}
	}
	return nil
}

// consumeProposals returns and resets the accumulated proposals.
func (t *testProposerRaft) consumeProposals() []kvserverpb.RaftCommand {
	res := t.proposals
	t.proposals = nil
	return res
}

func (t testProposerRaft) BasicStatus() raft.BasicStatus {
	return t.status
}

func (t testProposerRaft) ProposeConfChange(i raftpb.ConfChangeI) error {
	// TODO(andrei, nvanbenschoten): Capture the message and test against it.
	return nil
}

func (t *testProposer) locker() sync.Locker {
	return &t.RWMutex
}

func (t *testProposer) rlocker() sync.Locker {
	return t.RWMutex.RLocker()
}

func (t *testProposer) getReplicaID() roachpb.ReplicaID {
	return 1
}

func (t *testProposer) destroyed() destroyStatus {
	return t.ds
}

func (t *testProposer) leaseAppliedIndex() uint64 {
	return t.lai
}

func (t *testProposer) enqueueUpdateCheck() {
	t.enqueued++
}

func (t *testProposer) closedTimestampTarget() hlc.Timestamp {
	if t.clock == nil {
		return hlc.Timestamp{}
	}
	return closedts.TargetForPolicy(
		t.clock.NowAsClockTimestamp(),
		t.clock.MaxOffset(),
		1*time.Second,
		0,
		200*time.Millisecond,
		t.rangePolicy,
	)
}

func (t *testProposer) withGroupLocked(fn func(proposerRaft) error) error {
	// Note that t.raftGroup can be nil, which FlushLockedWithRaftGroup supports.
	return fn(t.raftGroup)
}

func (t *testProposer) leaseDebugRLocked() string {
	return ""
}

func (t *testProposer) registerProposalLocked(p *ProposalData) {
	t.registered++
}

func (t *testProposer) ownsValidLeaseRLocked(ctx context.Context, now hlc.ClockTimestamp) bool {
	return t.ownsValidLease
}

func (t *testProposer) leaderStatusRLocked(raftGroup proposerRaft) rangeLeaderInfo {
	leaderKnown := raftGroup.BasicStatus().Lead != raft.None
	var leaderRep roachpb.ReplicaID
	var iAmTheLeader, leaderEligibleForLease bool
	if leaderKnown {
		leaderRep = roachpb.ReplicaID(raftGroup.BasicStatus().Lead)
		iAmTheLeader = leaderRep == t.getReplicaID()
		repDesc := roachpb.ReplicaDescriptor{
			ReplicaID: leaderRep,
			Type:      &t.leaderReplicaType,
		}

		if t.leaderReplicaInDescriptor {
			// Fill in a RangeDescriptor just enough for the CheckCanReceiveLease()
			// call.
			rngDesc := roachpb.RangeDescriptor{
				InternalReplicas: []roachpb.ReplicaDescriptor{repDesc},
			}
			err := roachpb.CheckCanReceiveLease(repDesc, &rngDesc)
			leaderEligibleForLease = err == nil
		} else {
			// This matches replicaProposed.leaderStatusRLocked().
			leaderEligibleForLease = true
		}
	}
	return rangeLeaderInfo{
		leaderKnown:            leaderKnown,
		leader:                 leaderRep,
		iAmTheLeader:           iAmTheLeader,
		leaderEligibleForLease: leaderEligibleForLease,
	}
}

func (t *testProposer) rejectProposalWithRedirectLocked(
	ctx context.Context, prop *ProposalData, redirectTo roachpb.ReplicaID,
) {
	if t.onRejectProposalWithRedirectLocked == nil {
		panic("unexpected rejectProposalWithRedirectLocked() call")
	}
	t.onRejectProposalWithRedirectLocked(prop, redirectTo)
}

// proposalCreator holds on to a lease and creates proposals using it.
type proposalCreator struct {
	lease kvserverpb.LeaseStatus
}

func (pc proposalCreator) newPutProposal(ts hlc.Timestamp) *ProposalData {
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.PutRequest{})
	ba.Timestamp = ts
	return pc.newProposal(ba)
}

func (pc proposalCreator) newLeaseProposal(lease roachpb.Lease) *ProposalData {
	var ba roachpb.BatchRequest
	ba.Add(&roachpb.RequestLeaseRequest{Lease: lease})
	prop := pc.newProposal(ba)
	prop.command.ReplicatedEvalResult.IsLeaseRequest = true
	return prop
}

func (pc proposalCreator) newProposal(ba roachpb.BatchRequest) *ProposalData {
	var lease *roachpb.Lease
	r, ok := ba.GetArg(roachpb.RequestLease)
	if ok {
		lease = &r.(*roachpb.RequestLeaseRequest).Lease
	}
	p := &ProposalData{
		ctx:   context.Background(),
		idKey: kvserverbase.CmdIDKey("test-cmd"),
		command: &kvserverpb.RaftCommand{
			ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
				State: &kvserverpb.ReplicaState{Lease: lease},
			},
		},
		Request:     &ba,
		leaseStatus: pc.lease,
	}
	p.encodedCommand = pc.encodeProposal(p)
	return p
}

func (pc proposalCreator) encodeProposal(p *ProposalData) []byte {
	cmdLen := p.command.Size()
	needed := kvserverbase.RaftCommandPrefixLen + cmdLen + kvserverpb.MaxRaftCommandFooterSize()
	data := make([]byte, kvserverbase.RaftCommandPrefixLen, needed)
	kvserverbase.EncodeRaftCommandPrefix(data, kvserverbase.RaftVersionStandard, p.idKey)
	data = data[:kvserverbase.RaftCommandPrefixLen+p.command.Size()]
	if _, err := protoutil.MarshalTo(p.command, data[kvserverbase.RaftCommandPrefixLen:]); err != nil {
		panic(err)
	}
	return data
}

// TestProposalBuffer tests the basic behavior of the Raft proposal buffer.
func TestProposalBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	r := &testProposerRaft{}
	p := testProposer{
		raftGroup: r,
	}
	var b propBuf
	var pc proposalCreator
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, tracker.NewLockfreeTracker(), clock, cluster.MakeTestingClusterSettings())

	// Insert propBufArrayMinSize proposals. The buffer should not be flushed.
	num := propBufArrayMinSize
	leaseReqIdx := 3
	for i := 0; i < num; i++ {
		leaseReq := i == leaseReqIdx
		var pd *ProposalData
		if leaseReq {
			pd = pc.newLeaseProposal(roachpb.Lease{})
		} else {
			pd = pc.newPutProposal(hlc.Timestamp{})
		}
		_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		require.NoError(t, b.Insert(ctx, pd, tok))
		require.Equal(t, i+1, b.AllocatedIdx())
		require.Equal(t, 1, p.enqueued)
		require.Equal(t, 0, p.registered)
	}
	require.Equal(t, num, b.evalTracker.Count())
	require.Empty(t, r.consumeProposals())

	// Insert another proposal. This causes the buffer to flush.
	pd := pc.newPutProposal(hlc.Timestamp{})
	_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	err := b.Insert(ctx, pd, tok)
	require.Nil(t, err)
	require.Equal(t, 1, b.AllocatedIdx())
	require.Equal(t, 2, p.enqueued)
	require.Equal(t, num, p.registered)
	// We've flushed num requests, out of which one is a lease request (so that
	// one did not increment the MLAI).
	require.Equal(t, uint64(num)-1, b.assignedLAI)
	require.Equal(t, 2*propBufArrayMinSize, b.arr.len())
	require.Equal(t, 1, b.evalTracker.Count())
	proposals := r.consumeProposals()
	require.Len(t, proposals, propBufArrayMinSize)
	var lai uint64
	for i, p := range proposals {
		if i != leaseReqIdx {
			lai++
			require.Equal(t, lai, p.MaxLeaseIndex)
		} else {
			require.Zero(t, p.MaxLeaseIndex)
		}
	}

	// Flush the buffer repeatedly until its array shrinks.
	for i := 0; i < propBufArrayShrinkDelay; i++ {
		require.Equal(t, 2*propBufArrayMinSize, b.arr.len())
		require.Nil(t, b.flushLocked(ctx))
	}
	require.Equal(t, propBufArrayMinSize, b.arr.len())
}

// TestProposalBufferConcurrentWithDestroy tests the concurrency properties of
// the Raft proposal buffer.
func TestProposalBufferConcurrentWithDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	r := &testProposerRaft{}
	p := testProposer{
		raftGroup: r,
	}
	var b propBuf
	var pc proposalCreator
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, tracker.NewLockfreeTracker(), clock, cluster.MakeTestingClusterSettings())

	dsErr := errors.New("destroyed")

	// Run 20 concurrent producers.
	var g errgroup.Group
	const concurrency = 20
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				pd := pc.newPutProposal(hlc.Timestamp{})
				_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
				err := b.Insert(ctx, pd, tok)
				if err != nil {
					if errors.Is(err, dsErr) {
						return nil
					}
					return errors.Wrap(err, "Insert")
				}
			}
		})
	}

	// Run a concurrent consumer.
	g.Go(func() error {
		for {
			if stop, err := func() (bool, error) {
				p.Lock()
				defer p.Unlock()
				if !p.ds.IsAlive() {
					return true, nil
				}
				if err := b.flushLocked(ctx); err != nil {
					return true, errors.Wrap(err, "flushLocked")
				}
				return false, nil
			}(); stop {
				return err
			}
		}
	})

	// Wait for a random duration before destroying.
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)

	// Destroy the proposer. All producers and consumers should notice.
	p.Lock()
	p.ds.Set(dsErr, destroyReasonRemoved)
	p.Unlock()

	require.Nil(t, g.Wait())
	t.Logf("%d successful proposals before destroy", len(r.consumeProposals()))
}

// TestProposalBufferRegistersAllOnProposalError tests that all proposals in the
// proposal buffer are registered with the proposer when the buffer is flushed,
// even if an error is seen when proposing a batch of entries.
func TestProposalBufferRegistersAllOnProposalError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	raft := &testProposerRaft{}
	var p testProposer
	p.raftGroup = raft
	var b propBuf
	var pc proposalCreator
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, tracker.NewLockfreeTracker(), clock, cluster.MakeTestingClusterSettings())

	num := propBufArrayMinSize
	toks := make([]TrackedRequestToken, num)
	for i := 0; i < num; i++ {
		pd := pc.newPutProposal(hlc.Timestamp{})
		_, toks[i] = b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		err := b.Insert(ctx, pd, toks[i])
		require.Nil(t, err)
	}
	require.Equal(t, num, b.AllocatedIdx())

	propNum := 0
	propErr := errors.New("failed proposal")
	b.testing.submitProposalFilter = func(*ProposalData) (drop bool, err error) {
		propNum++
		require.Equal(t, propNum, p.registered)
		if propNum == 2 {
			return false, propErr
		}
		return false, nil
	}
	err := b.flushLocked(ctx)
	require.Equal(t, propErr, err)
	require.Equal(t, num, p.registered)
	require.Zero(t, b.evalTracker.Count())
}

// Test that the proposal buffer rejects lease acquisition proposals from
// followers. We want the leader to take the lease; see comments in
// FlushLockedWithRaftGroup().
func TestProposalBufferRejectLeaseAcqOnFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	self := uint64(1)
	// Each subtest will try to propose a lease acquisition in a different Raft
	// scenario. Some proposals should be allowed, some should be rejected.
	for _, tc := range []struct {
		name  string
		state raft.StateType
		// raft.None means there's no leader, or the leader is unknown.
		leader uint64
		// Empty means VOTER_FULL.
		leaderRepType roachpb.ReplicaType
		// Set to simulate situations where the local replica is so behind that the
		// leader is not even part of the range descriptor.
		leaderNotInRngDesc bool
		// If true, the follower has a valid lease.
		ownsValidLease bool

		expRejection bool
	}{
		{
			name:   "leader",
			state:  raft.StateLeader,
			leader: self,
			// No rejection. The leader can request a lease.
			expRejection: false,
		},
		{
			name:  "follower, known eligible leader",
			state: raft.StateFollower,
			// Someone else is leader.
			leader: self + 1,
			// Rejection - a follower can't request a lease.
			expRejection: true,
		},
		{
			name:  "follower, lease extension despite known eligible leader",
			state: raft.StateFollower,
			// Someone else is leader, but we're the leaseholder.
			leader:         self + 1,
			ownsValidLease: true,
			// No rejection of lease extensions.
			expRejection: false,
		},
		{
			name:  "follower, known ineligible leader",
			state: raft.StateFollower,
			// Someone else is leader.
			leader: self + 1,
			// The leader type makes it ineligible to get the lease. Thus, the local
			// proposal will not be rejected.
			leaderRepType: roachpb.VOTER_DEMOTING_LEARNER,
			expRejection:  false,
		},
		{
			// Here we simulate the leader being known by Raft, but the local replica
			// is so far behind that it doesn't contain the leader replica.
			name:  "follower, known leader not in range descriptor",
			state: raft.StateFollower,
			// Someone else is leader.
			leader:             self + 1,
			leaderNotInRngDesc: true,
			// We assume that the leader is eligible, and redirect.
			expRejection: true,
		},
		{
			name:  "follower, unknown leader",
			state: raft.StateFollower,
			// Unknown leader.
			leader: raft.None,
			// No rejection if the leader is unknown. See comments in
			// FlushLockedWithRaftGroup().
			expRejection: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p testProposer
			var pc proposalCreator
			// p.getReplicaID() is hardcoded; it'd better be hardcoded to what this
			// test expects.
			require.Equal(t, self, uint64(p.getReplicaID()))

			var rejected roachpb.ReplicaID
			if tc.expRejection {
				p.onRejectProposalWithRedirectLocked = func(_ *ProposalData, redirectTo roachpb.ReplicaID) {
					if rejected != 0 {
						t.Fatalf("unexpected 2nd rejection")
					}
					rejected = redirectTo
				}
			} else {
				p.onRejectProposalWithRedirectLocked = func(_ *ProposalData, _ roachpb.ReplicaID) {
					t.Fatalf("unexpected redirection")
				}
			}

			raftStatus := raft.BasicStatus{
				ID: self,
				SoftState: raft.SoftState{
					RaftState: tc.state,
					Lead:      tc.leader,
				},
			}
			r := &testProposerRaft{
				status: raftStatus,
			}
			p.raftGroup = r
			p.leaderReplicaInDescriptor = !tc.leaderNotInRngDesc
			p.leaderReplicaType = tc.leaderRepType
			p.ownsValidLease = tc.ownsValidLease

			var b propBuf
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			tracker := tracker.NewLockfreeTracker()
			b.Init(&p, tracker, clock, cluster.MakeTestingClusterSettings())

			pd := pc.newLeaseProposal(roachpb.Lease{})
			_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
			err := b.Insert(ctx, pd, tok.Move(ctx))
			require.NoError(t, err)
			require.NoError(t, b.flushLocked(ctx))
			if tc.expRejection {
				require.Equal(t, roachpb.ReplicaID(tc.leader), rejected)
			} else {
				require.Equal(t, roachpb.ReplicaID(0), rejected)
			}
			require.Zero(t, tracker.Count())
		})
	}
}

// Test that the propBuf properly assigns closed timestamps to proposals being
// flushed out of it. Each subtest proposes one command and checks for the
// expected closed timestamp being written to the proposal by the propBuf.
func TestProposalBufferClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const maxOffset = 500 * time.Millisecond
	mc := hlc.NewManualClock((1613588135 * time.Second).Nanoseconds())
	clock := hlc.NewClock(mc.UnixNano, maxOffset)
	st := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(ctx, &st.SV, time.Second)
	closedts.SideTransportCloseInterval.Override(ctx, &st.SV, 200*time.Millisecond)
	now := clock.NowAsClockTimestamp()
	nowTS := now.ToTimestamp()
	nowMinusClosedLag := nowTS.Add(-closedts.TargetDuration.Get(&st.SV).Nanoseconds(), 0)
	nowMinusTwiceClosedLag := nowTS.Add(-2*closedts.TargetDuration.Get(&st.SV).Nanoseconds(), 0)
	nowPlusGlobalReadLead := nowTS.Add((maxOffset +
		275*time.Millisecond /* sideTransportPropTime */ +
		25*time.Millisecond /* bufferTime */).Nanoseconds(), 0).
		WithSynthetic(true)
	expiredLeaseTimestamp := nowTS.Add(-1000, 0)
	someClosedTS := nowTS.Add(-2000, 0)

	type reqType int
	checkClosedTS := func(t *testing.T, r *testProposerRaft, exp hlc.Timestamp) {
		props := r.consumeProposals()
		require.Len(t, props, 1)
		proposal := props[0]
		if exp.IsEmpty() {
			require.Empty(t, proposal.ClosedTimestamp)
		} else {
			require.NotNil(t, proposal.ClosedTimestamp)
			closedTS := *proposal.ClosedTimestamp
			closedTS.Logical = 0 // ignore logical ticks from clock
			require.Equal(t, exp, closedTS)
		}
	}

	// The lease that the proposals are made under.
	curLease := roachpb.Lease{
		Epoch:    0, // Expiration-based lease.
		Sequence: 1,
		Start:    hlc.ClockTimestamp{},
		// Expiration is filled by each test.
		Expiration: nil,
	}

	const (
		regularWrite reqType = iota
		// newLease means that the request is a lease acquisition (new lease or
		// lease extension).
		newLease
		leaseTransfer
	)

	for _, tc := range []struct {
		name string

		reqType reqType
		// The lower bound of all currently-evaluating requests. We can't close this
		// or above.
		trackerLowerBound hlc.Timestamp
		// The expiration of the current lease. The closed timestamp of most
		// proposal is upper-bounded by this, which matters for
		// LEAD_FOR_GLOBAL_READS ranges (on other ranges the propBuf would never
		// like to close a timestamp above the current lease expiration because it
		// wouldn't be processing commands if the lease is expired).
		leaseExp    hlc.Timestamp
		rangePolicy roachpb.RangeClosedTimestampPolicy
		// The highest closed timestamp that the propBuf has previously attached to
		// a proposal. The propBuf should never propose a new closedTS below this.
		prevClosedTimestamp hlc.Timestamp

		// lease is used when reqType = newLease. This will be the lease being
		// proposed.
		lease roachpb.Lease

		// expClosed is the expected closed timestamp carried by the proposal. Empty
		// means the proposal is not expected to carry a closed timestamp update.
		expClosed hlc.Timestamp
		// expAssignedClosedBumped, if set, means that the test expects
		// b.assignedClosedTimestamp to be bumped before proposing. If not set, then
		// the test expects b.assignedClosedTimestamp to be left at
		// prevClosedTimestamp, regardless of whether the proposal carries a closed
		// timestamp or not (expClosed).
		expAssignedClosedBumped bool
	}{
		{
			name:                    "basic",
			reqType:                 regularWrite,
			trackerLowerBound:       hlc.Timestamp{},
			leaseExp:                hlc.MaxTimestamp,
			rangePolicy:             roachpb.LAG_BY_CLUSTER_SETTING,
			prevClosedTimestamp:     hlc.Timestamp{},
			expClosed:               nowMinusClosedLag,
			expAssignedClosedBumped: true,
		},
		{
			// The request tracker will prevent us from closing below its lower bound.
			name:                    "not closing below evaluating requests",
			reqType:                 regularWrite,
			trackerLowerBound:       nowMinusTwiceClosedLag,
			leaseExp:                hlc.MaxTimestamp,
			rangePolicy:             roachpb.LAG_BY_CLUSTER_SETTING,
			prevClosedTimestamp:     hlc.Timestamp{},
			expClosed:               nowMinusTwiceClosedLag.FloorPrev(),
			expAssignedClosedBumped: true,
		},
		{
			// Like the basic test, except that we can't close timestamp below what
			// we've already closed previously.
			name:                    "no regression",
			reqType:                 regularWrite,
			trackerLowerBound:       hlc.Timestamp{},
			leaseExp:                hlc.MaxTimestamp,
			rangePolicy:             roachpb.LAG_BY_CLUSTER_SETTING,
			prevClosedTimestamp:     someClosedTS,
			expClosed:               someClosedTS,
			expAssignedClosedBumped: false,
		},
		{
			name:    "brand new lease",
			reqType: newLease,
			lease: roachpb.Lease{
				// Higher sequence => this is a brand new lease, not an extension.
				Sequence: curLease.Sequence + 1,
				Start:    now,
			},
			trackerLowerBound: hlc.Timestamp{},
			// The current lease can be expired; we won't backtrack the closed
			// timestamp to this expiration.
			leaseExp:    expiredLeaseTimestamp,
			rangePolicy: roachpb.LAG_BY_CLUSTER_SETTING,
			// Lease requests don't carry closed timestamps.
			expClosed: hlc.Timestamp{},
			// Check that the lease proposal does not bump b.assignedClosedTimestamp.
			// The proposer cannot make promises about the write timestamps of further
			// requests based on the start time of a proposed lease. See comments in
			// propBuf.assignClosedTimestampAndLAIToProposalLocked().
			expAssignedClosedBumped: false,
		},
		{
			name:    "lease extension",
			reqType: newLease,
			lease: roachpb.Lease{
				// Same sequence => this is a lease extension.
				Sequence: curLease.Sequence,
				Start:    now,
			},
			trackerLowerBound: hlc.Timestamp{},
			// The current lease can be expired; we won't backtrack the closed
			// timestamp to this expiration.
			leaseExp:    expiredLeaseTimestamp,
			rangePolicy: roachpb.LAG_BY_CLUSTER_SETTING,
			// Lease extensions don't carry closed timestamps.
			expClosed:               hlc.Timestamp{},
			expAssignedClosedBumped: false,
		},
		{
			// Lease transfers behave just like regular writes. The lease start time
			// doesn't matter.
			name:    "lease transfer",
			reqType: leaseTransfer,
			lease: roachpb.Lease{
				Sequence: curLease.Sequence + 1,
				Start:    now,
			},
			trackerLowerBound:       hlc.Timestamp{},
			leaseExp:                hlc.MaxTimestamp,
			rangePolicy:             roachpb.LAG_BY_CLUSTER_SETTING,
			expClosed:               nowMinusClosedLag,
			expAssignedClosedBumped: true,
		},
		{
			// With the LEAD_FOR_GLOBAL_READS policy, we're expecting to close
			// timestamps in the future.
			name:                    "global range",
			reqType:                 regularWrite,
			trackerLowerBound:       hlc.Timestamp{},
			leaseExp:                hlc.MaxTimestamp,
			rangePolicy:             roachpb.LEAD_FOR_GLOBAL_READS,
			prevClosedTimestamp:     hlc.Timestamp{},
			expClosed:               nowPlusGlobalReadLead,
			expAssignedClosedBumped: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testProposerRaft{}
			p := testProposer{
				clock:       clock,
				lai:         10,
				raftGroup:   r,
				rangePolicy: tc.rangePolicy,
			}
			tracker := mockTracker{
				lowerBound: tc.trackerLowerBound,
			}
			pc := proposalCreator{lease: kvserverpb.LeaseStatus{Lease: curLease}}
			pc.lease.Lease.Expiration = &tc.leaseExp

			var b propBuf
			b.Init(&p, tracker, clock, st)
			b.forwardClosedTimestampLocked(tc.prevClosedTimestamp)

			var pd *ProposalData
			switch tc.reqType {
			case regularWrite:
				pd = pc.newPutProposal(now.ToTimestamp())
			case newLease:
				pd = pc.newLeaseProposal(tc.lease)
			case leaseTransfer:
				var ba roachpb.BatchRequest
				ba.Add(&roachpb.TransferLeaseRequest{
					Lease: roachpb.Lease{
						Start:    now,
						Sequence: pc.lease.Lease.Sequence + 1,
					},
					PrevLease: pc.lease.Lease,
				})
				pd = pc.newProposal(ba)
			default:
				t.Fatalf("unknown req type %d", tc.reqType)
			}
			err := b.Insert(ctx, pd, TrackedRequestToken{})
			require.NoError(t, err)
			require.NoError(t, b.flushLocked(ctx))
			checkClosedTS(t, r, tc.expClosed)
			if tc.expAssignedClosedBumped {
				require.Equal(t, tc.expClosed, b.assignedClosedTimestamp)
			} else {
				require.Equal(t, tc.prevClosedTimestamp, b.assignedClosedTimestamp)
			}
		})
	}
}

type mockTracker struct {
	lowerBound hlc.Timestamp
}

func (t mockTracker) Track(ctx context.Context, ts hlc.Timestamp) tracker.RemovalToken {
	panic("unimplemented")
}

func (t mockTracker) Untrack(context.Context, tracker.RemovalToken) {}

func (t mockTracker) LowerBound(context.Context) hlc.Timestamp {
	return t.lowerBound
}

func (t mockTracker) Count() int {
	panic("unimplemented")
}

var _ tracker.Tracker = mockTracker{}
