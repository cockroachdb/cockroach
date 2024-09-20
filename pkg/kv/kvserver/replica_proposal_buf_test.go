// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/tracker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/leases"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	rafttracker "github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// testProposer is a testing implementation of proposer.
type testProposer struct {
	syncutil.RWMutex
	clock      *hlc.Clock
	ds         destroyStatus
	fi         kvpb.RaftIndex
	lai        kvpb.LeaseAppliedIndex
	enqueued   int
	registered int

	// If not nil, this can be a testProposerRaft used to mock the raft group
	// passed to FlushLockedWithRaftGroup.
	raftGroup proposerRaft
	// If not nil, this is called by rejectProposalWithErrLocked.
	// If nil, rejectProposalWithErrLocked panics.
	onRejectProposalWithErrLocked func(err error)
	// If not nil, this is called by onErrProposalDropped.
	onProposalsDropped func(
		ents []raftpb.Entry, proposalData []*ProposalData, stateType raftpb.StateType,
	)
	// validLease is returned by ownsValidLease.
	validLease bool
	// leaderNotLive is returned from shouldCampaignOnRedirect.
	leaderNotLive bool
	// leaderReplicaInDescriptor is set if the leader (as indicated by raftGroup)
	// is known, and that leader is part of the range's descriptor (as seen by the
	// current replica). This can be used to simulate the local replica being so
	// far behind that it doesn't have an up to date descriptor.
	leaderReplicaInDescriptor bool
	// If leaderReplicaInDescriptor is set, this specifies what type of replica it
	// is. Some types of replicas are not eligible to get a lease.
	leaderReplicaType roachpb.ReplicaType
	// rangePolicy is used in closedTimestampTarget.
	rangePolicy roachpb.RangeClosedTimestampPolicy
}

var _ proposer = &testProposer{}

// testProposerRaft is a testing implementation of proposerRaft. It is meant to
// be used as the Raft group used by a testProposer, and it records the commands
// being proposed.
type testProposerRaft struct {
	status raft.Status
	// proposals are the commands that the propBuf flushed (i.e. passed to the
	// Raft group) and have not yet been consumed with consumeProposals().
	proposals  []kvserverpb.RaftCommand
	onProp     func(raftpb.Message) error // invoked on Step with MsgProp
	campaigned bool
}

var _ proposerRaft = &testProposerRaft{}

func (t *testProposerRaft) Step(msg raftpb.Message) error {
	if msg.Type != raftpb.MsgProp {
		return nil
	}
	if t.onProp != nil {
		if err := t.onProp(msg); err != nil {
			return err
		}
	}
	// Decode and save all the commands.
	for _, e := range msg.Entries {
		ent, err := raftlog.NewEntry(e)
		if err != nil {
			return err
		}
		t.proposals = append(t.proposals, ent.Cmd)
	}
	return nil
}

// consumeProposals returns and resets the accumulated proposals.
func (t *testProposerRaft) consumeProposals() []kvserverpb.RaftCommand {
	res := t.proposals
	t.proposals = nil
	return res
}

func (t testProposerRaft) Status() raft.Status {
	return t.status
}

func (t testProposerRaft) BasicStatus() raft.BasicStatus {
	return t.status.BasicStatus
}

func (t *testProposerRaft) Campaign() error {
	t.campaigned = true
	return nil
}

func (t *testProposer) locker() sync.Locker {
	return &t.RWMutex
}

func (t *testProposer) rlocker() sync.Locker {
	return t.RWMutex.RLocker()
}
func (t *testProposer) flowControlHandle(ctx context.Context) kvflowcontrol.Handle {
	return &testFlowTokenHandle{}
}

func (t *testProposer) getStoreID() roachpb.StoreID {
	return 1
}

func (t *testProposer) getReplicaID() roachpb.ReplicaID {
	return 1
}

func (t *testProposer) getReplicaDesc() roachpb.ReplicaDescriptor {
	return roachpb.ReplicaDescriptor{StoreID: t.getStoreID(), ReplicaID: t.getReplicaID()}
}

func (t *testProposer) destroyed() destroyStatus {
	return t.ds
}

func (t *testProposer) leaseAppliedIndex() kvpb.LeaseAppliedIndex {
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

func (rp *testProposer) onErrProposalDropped(
	ents []raftpb.Entry, props []*ProposalData, typ raftpb.StateType,
) {
	if rp.onProposalsDropped == nil {
		return
	}
	rp.onProposalsDropped(ents, props, typ)
}

func (t *testProposer) leaseDebugRLocked() string {
	return ""
}

func (t *testProposer) registerProposalLocked(p *ProposalData) {
	t.registered++
}

func (t *testProposer) shouldCampaignOnRedirect(
	raftGroup proposerRaft, leaseType roachpb.LeaseType,
) bool {
	return t.leaderNotLive
}

func (t *testProposer) campaignLocked(ctx context.Context) {
	if err := t.raftGroup.Campaign(); err != nil {
		panic(err)
	}
}

func (t *testProposer) registerForTracing(*ProposalData, raftpb.Entry) bool { return true }

func (t *testProposer) rejectProposalWithErrLocked(_ context.Context, _ *ProposalData, err error) {
	if t.onRejectProposalWithErrLocked == nil {
		panic(fmt.Sprintf("unexpected rejectProposalWithErrLocked call: err=%v", err))
	}
	t.onRejectProposalWithErrLocked(err)
}

func (t *testProposer) verifyLeaseRequestSafetyRLocked(
	ctx context.Context,
	raftGroup proposerRaft,
	prevLease, nextLease roachpb.Lease,
	bypassSafetyChecks bool,
) error {
	st := leases.Settings{}
	raftStatus := raftGroup.Status()
	desc := &roachpb.RangeDescriptor{
		// TestProposalBufferRejectLeaseAcqOnFollower configures raftStatus.Lead to
		// be either replica 1 or 2. Add a few more replicas to the descriptor to
		// avoid a single-replica range, which bypasses some safety checks during
		// lease acquisition.
		InternalReplicas: []roachpb.ReplicaDescriptor{{ReplicaID: 3}, {ReplicaID: 4}},
	}
	if t.leaderReplicaInDescriptor {
		desc.InternalReplicas = append(desc.InternalReplicas, roachpb.ReplicaDescriptor{
			ReplicaID: roachpb.ReplicaID(raftStatus.Lead),
			Type:      t.leaderReplicaType,
		})
	}
	in := leases.VerifyInput{
		LocalStoreID:       t.getStoreID(),
		LocalReplicaID:     t.getReplicaID(),
		Desc:               desc,
		RaftStatus:         &raftStatus,
		RaftFirstIndex:     t.fi,
		PrevLease:          prevLease,
		PrevLeaseExpired:   !t.validLease,
		NextLeaseHolder:    nextLease.Replica,
		BypassSafetyChecks: bypassSafetyChecks,
	}
	return leases.Verify(ctx, st, in)
}

// proposalCreator holds on to a lease and creates proposals using it.
type proposalCreator struct {
	lease kvserverpb.LeaseStatus
}

func (pc proposalCreator) newPutProposal(ts hlc.Timestamp) *ProposalData {
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.PutRequest{})
	ba.Timestamp = ts
	return pc.newProposal(ba)
}

func (pc proposalCreator) newLeaseRequestProposal(lease roachpb.Lease) *ProposalData {
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.RequestLeaseRequest{Lease: lease, PrevLease: pc.lease.Lease})
	return pc.newProposal(ba)
}

func (pc proposalCreator) newLeaseTransferProposal(lease roachpb.Lease) *ProposalData {
	ba := &kvpb.BatchRequest{}
	ba.Add(&kvpb.TransferLeaseRequest{Lease: lease, PrevLease: pc.lease.Lease})
	return pc.newProposal(ba)
}

func (pc proposalCreator) newProposal(ba *kvpb.BatchRequest) *ProposalData {
	var lease *roachpb.Lease
	var isLeaseRequest bool
	var cr *kvserverpb.ChangeReplicas
	switch v := ba.Requests[0].GetInner().(type) {
	case *kvpb.RequestLeaseRequest:
		lease = &v.Lease
		isLeaseRequest = true
	case *kvpb.TransferLeaseRequest:
		lease = &v.Lease
	case *kvpb.EndTxnRequest:
		if crt := v.InternalCommitTrigger.GetChangeReplicasTrigger(); crt != nil {
			cr = &kvserverpb.ChangeReplicas{
				ChangeReplicasTrigger: *crt,
			}
		}
	}
	p := &ProposalData{
		idKey: kvserverbase.CmdIDKey("test-cmd"),
		command: &kvserverpb.RaftCommand{
			ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
				IsLeaseRequest: isLeaseRequest,
				State:          &kvserverpb.ReplicaState{Lease: lease},
				ChangeReplicas: cr,
			},
		},
		Request:     ba,
		leaseStatus: pc.lease,
	}
	ctx := context.Background()
	p.ctx.Store(&ctx)
	p.encodedCommand = pc.encodeProposal(p)
	return p
}

func (pc proposalCreator) encodeProposal(p *ProposalData) []byte {
	b, err := raftlog.EncodeCommand(context.Background(), p.command, p.idKey, raftlog.EncodeOptions{})
	if err != nil {
		panic(err)
	}
	return b
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
	clock := hlc.NewClockForTesting(nil)
	b.Init(&p, tracker.NewLockfreeTracker(), clock, cluster.MakeTestingClusterSettings())

	// Insert propBufArrayMinSize proposals. The buffer should not be flushed.
	num := propBufArrayMinSize
	leaseReqIdx := 3
	for i := 0; i < num; i++ {
		leaseReq := i == leaseReqIdx
		var pd *ProposalData
		if leaseReq {
			pd = pc.newLeaseRequestProposal(roachpb.Lease{Replica: p.getReplicaDesc()})
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
	require.Equal(t, kvpb.LeaseAppliedIndex(num-1), b.assignedLAI)
	require.Equal(t, 2*propBufArrayMinSize, b.arr.len())
	require.Equal(t, 1, b.evalTracker.Count())
	proposals := r.consumeProposals()
	require.Len(t, proposals, propBufArrayMinSize)
	var lai kvpb.LeaseAppliedIndex
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
	clock := hlc.NewClockForTesting(nil)
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
	clock := hlc.NewClockForTesting(nil)
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

	self := raftpb.PeerID(1)
	// Each subtest will try to propose a lease acquisition in a different Raft
	// scenario. Some proposals should be allowed, some should be rejected.
	for _, tc := range []struct {
		name  string
		state raftpb.StateType
		// raft.None means there's no leader, or the leader is unknown.
		leader raftpb.PeerID
		// Empty means VOTER_FULL.
		leaderRepType roachpb.ReplicaType
		// Set to simulate situations where the local replica is so behind that the
		// leader is not even part of the range descriptor.
		leaderNotInRngDesc bool
		// Set to simulate situations where the Raft leader is not live in the node
		// liveness map.
		leaderNotLive bool
		// If true, the follower has a valid lease.
		ownsValidLease bool

		expRejection bool
		expCampaign  bool
	}{
		{
			name:   "leader",
			state:  raftpb.StateLeader,
			leader: self,
			// No rejection. The leader can request a lease.
			expRejection: false,
		},
		{
			name:  "follower, known eligible leader",
			state: raftpb.StateFollower,
			// Someone else is leader.
			leader: self + 1,
			// Rejection - a follower can't request a lease.
			expRejection: true,
		},
		{
			name:  "follower, lease extension despite known eligible leader",
			state: raftpb.StateFollower,
			// Someone else is leader, but we're the leaseholder.
			leader:         self + 1,
			ownsValidLease: true,
			// No rejection of lease extensions.
			expRejection: false,
		},
		{
			name:  "follower, known ineligible leader",
			state: raftpb.StateFollower,
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
			state: raftpb.StateFollower,
			// Someone else is leader.
			leader:             self + 1,
			leaderNotInRngDesc: true,
			// We assume that the leader is eligible, and redirect.
			expRejection: true,
		},
		{
			name:  "follower, unknown leader",
			state: raftpb.StateFollower,
			// Unknown leader.
			leader: raft.None,
			// No rejection if the leader is unknown. See comments in
			// leases.verifyAcquisition.
			expRejection: false,
		},
		{
			name:  "follower, known eligible non-live leader",
			state: raftpb.StateFollower,
			// Someone else is leader.
			leader:        self + 1,
			leaderNotLive: true,
			// Rejection - a follower can't request a lease.
			expRejection: true,
			// The leader is non-live, so we should campaign.
			expCampaign: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p testProposer
			var pc proposalCreator
			// p.getReplicaID() is hardcoded; it'd better be hardcoded to what this
			// test expects.
			require.Equal(t, self, raftpb.PeerID(p.getStoreID()))
			require.Equal(t, self, raftpb.PeerID(p.getReplicaID()))

			var rejected bool
			var rejectedLease *roachpb.Lease
			if tc.expRejection {
				p.onRejectProposalWithErrLocked = func(err error) {
					if rejected {
						t.Fatalf("unexpected 2nd rejection")
					}
					var nlhe *kvpb.NotLeaseHolderError
					if ok := errors.As(err, &nlhe); !ok {
						t.Fatalf("expected NotLeaseHolderError, got %v", err)
					}
					rejected = true
					rejectedLease = nlhe.Lease
				}
			} else {
				p.onRejectProposalWithErrLocked = func(err error) {
					t.Fatalf("unexpected redirection")
				}
			}

			var raftStatus raft.Status
			raftStatus.ID = self
			raftStatus.RaftState = tc.state
			raftStatus.Lead = tc.leader
			r := &testProposerRaft{
				status: raftStatus,
			}
			p.raftGroup = r
			p.leaderReplicaInDescriptor = !tc.leaderNotInRngDesc
			p.leaderReplicaType = tc.leaderRepType
			p.validLease = tc.ownsValidLease
			p.leaderNotLive = tc.leaderNotLive

			var b propBuf
			clock := hlc.NewClockForTesting(nil)
			tracker := tracker.NewLockfreeTracker()
			b.Init(&p, tracker, clock, cluster.MakeTestingClusterSettings())

			repl := p.getReplicaDesc()
			if tc.ownsValidLease {
				pc.lease.Lease.Replica = repl
			}
			pd := pc.newLeaseRequestProposal(roachpb.Lease{Replica: repl})
			_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
			err := b.Insert(ctx, pd, tok.Move(ctx))
			require.NoError(t, err)
			require.NoError(t, b.flushLocked(ctx))
			if tc.expRejection {
				require.True(t, rejected)
				if tc.leaderNotInRngDesc {
					require.Nil(t, rejectedLease)
				} else {
					require.NotNil(t, rejectedLease)
					require.Equal(t, roachpb.ReplicaID(tc.leader), rejectedLease.Replica.ReplicaID)
				}
			} else {
				require.False(t, rejected)
			}
			require.Equal(t, tc.expCampaign, r.campaigned)
			require.Zero(t, tracker.Count())
		})
	}
}

// Test that the proposal buffer rejects lease transfer proposals to replicas
// that it deems would be unsafe.
func TestProposalBufferRejectUnsafeLeaseTransfer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	proposer := raftpb.PeerID(1)
	proposerFirstIndex := kvpb.RaftIndex(5)
	target := raftpb.PeerID(2)

	// Each subtest will try to propose a lease transfer in a different Raft
	// scenario. Some proposals should be allowed, some should be rejected.
	for _, tc := range []struct {
		name          string
		proposerState raftpb.StateType
		// math.MaxUint64 if the target is not in the raft group.
		targetState rafttracker.StateType
		targetMatch kvpb.RaftIndex

		expRejection       bool
		expRejectionReason raftutil.ReplicaNeedsSnapshotStatus
	}{
		{
			name:               "follower",
			proposerState:      raftpb.StateFollower,
			expRejection:       true,
			expRejectionReason: raftutil.LocalReplicaNotLeader,
		},
		{
			name:               "candidate",
			proposerState:      raftpb.StateCandidate,
			expRejection:       true,
			expRejectionReason: raftutil.LocalReplicaNotLeader,
		},
		{
			name:               "leader, no progress for target",
			proposerState:      raftpb.StateLeader,
			targetState:        math.MaxUint64,
			expRejection:       true,
			expRejectionReason: raftutil.ReplicaUnknown,
		},
		{
			name:               "leader, target state probe",
			proposerState:      raftpb.StateLeader,
			targetState:        rafttracker.StateProbe,
			expRejection:       true,
			expRejectionReason: raftutil.ReplicaStateProbe,
		},
		{
			name:               "leader, target state snapshot",
			proposerState:      raftpb.StateLeader,
			targetState:        rafttracker.StateSnapshot,
			expRejection:       true,
			expRejectionReason: raftutil.ReplicaStateSnapshot,
		},
		{
			name:               "leader, target state replicate, match+1 < firstIndex",
			proposerState:      raftpb.StateLeader,
			targetState:        rafttracker.StateReplicate,
			targetMatch:        proposerFirstIndex - 2,
			expRejection:       true,
			expRejectionReason: raftutil.ReplicaMatchBelowLeadersFirstIndex,
		},
		{
			name:          "leader, target state replicate, match+1 == firstIndex",
			proposerState: raftpb.StateLeader,
			targetState:   rafttracker.StateReplicate,
			targetMatch:   proposerFirstIndex - 1,
			expRejection:  false,
		},
		{
			name:          "leader, target state replicate, match+1 > firstIndex",
			proposerState: raftpb.StateLeader,
			targetState:   rafttracker.StateReplicate,
			targetMatch:   proposerFirstIndex,
			expRejection:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p testProposer
			var pc proposalCreator
			require.Equal(t, proposer, raftpb.PeerID(p.getStoreID()))
			require.Equal(t, proposer, raftpb.PeerID(p.getReplicaID()))

			var rejectedErr error
			if tc.expRejection {
				p.onRejectProposalWithErrLocked = func(err error) {
					if rejectedErr != nil {
						t.Fatalf("unexpected 2nd rejection")
					}
					rejectedErr = err
				}
			} else {
				p.onRejectProposalWithErrLocked = func(err error) {
					t.Fatalf("unexpected rejection")
				}
			}

			var raftStatus raft.Status
			raftStatus.ID = proposer
			raftStatus.RaftState = tc.proposerState
			if tc.proposerState == raftpb.StateLeader {
				raftStatus.Lead = proposer
				raftStatus.Progress = map[raftpb.PeerID]rafttracker.Progress{
					proposer: {State: rafttracker.StateReplicate, Match: uint64(proposerFirstIndex)},
				}
				if tc.targetState != math.MaxUint64 {
					raftStatus.Progress[target] = rafttracker.Progress{
						State: tc.targetState, Match: uint64(tc.targetMatch),
					}
				}
			}
			r := &testProposerRaft{
				status: raftStatus,
			}
			p.raftGroup = r
			p.fi = proposerFirstIndex

			var b propBuf
			clock := hlc.NewClockForTesting(nil)
			tracker := tracker.NewLockfreeTracker()
			b.Init(&p, tracker, clock, cluster.MakeTestingClusterSettings())

			pc.lease.Lease.Replica = p.getReplicaDesc()
			nextLease := roachpb.Lease{
				Start:    clock.NowAsClockTimestamp(),
				Sequence: pc.lease.Lease.Sequence + 1,
				Replica: roachpb.ReplicaDescriptor{
					StoreID:   roachpb.StoreID(target),
					ReplicaID: roachpb.ReplicaID(target),
				},
			}
			pd := pc.newLeaseTransferProposal(nextLease)

			_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
			err := b.Insert(ctx, pd, tok.Move(ctx))
			require.NoError(t, err)
			require.NoError(t, b.flushLocked(ctx))
			if tc.expRejection {
				require.NotNil(t, rejectedErr)
				require.Contains(t, rejectedErr.Error(), tc.expRejectionReason.String())
			} else {
				require.Nil(t, rejectedErr)
			}
			require.Zero(t, tracker.Count())
		})
	}
}

func TestProposalBufferLinesUpEntriesAndProposals(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	proposer := uint64(1)
	proposerFirstIndex := kvpb.RaftIndex(5)

	var matchingDroppedProposalsSeen int
	p := testProposer{
		onProposalsDropped: func(ents []raftpb.Entry, props []*ProposalData, _ raftpb.StateType) {
			require.Equal(t, len(ents), len(props))
			for i := range ents {
				if ents[i].Type == raftpb.EntryNormal {
					require.Nil(t, props[i].command.ReplicatedEvalResult.ChangeReplicas)
				} else {
					require.NotNil(t, props[i].command.ReplicatedEvalResult.ChangeReplicas)
				}
				matchingDroppedProposalsSeen++
			}
		},
	}
	var pc proposalCreator
	require.Equal(t, proposer, uint64(p.getReplicaID()))

	// Drop all proposals, since then we'll see the (ents,props) pair in
	// onErrProposalDropped.
	r := &testProposerRaft{onProp: func(msg raftpb.Message) error {
		return raft.ErrProposalDropped
	}}
	p.raftGroup = r
	p.fi = proposerFirstIndex

	var b propBuf
	// Make the proposal buffer large so that all the proposals we're putting in
	// get flushed together. (At the time of writing, default size is 4).
	b.arr.adjustSize(100)
	clock := hlc.NewClockForTesting(nil)
	tr := tracker.NewLockfreeTracker()
	b.Init(&p, tr, clock, cluster.MakeTestingClusterSettings())

	now := clock.Now()

	// Make seven proposals:
	// [put, put, put, confchange, put, put, put].
	var pds []*ProposalData

	for i := 0; i < 3; i++ {
		pds = append(pds, pc.newPutProposal(now))
	}

	{
		k := keys.LocalMax // unimportant
		var ba kvpb.BatchRequest
		ba.Add(&kvpb.EndTxnRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: k,
			},
			Commit: true,
			InternalCommitTrigger: &roachpb.InternalCommitTrigger{
				ChangeReplicasTrigger: &roachpb.ChangeReplicasTrigger{
					Desc: roachpb.NewRangeDescriptor(1, roachpb.RKeyMin, roachpb.RKeyMax,
						roachpb.MakeReplicaSet([]roachpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1, ReplicaID: 1}}),
					),
				},
			},
		})
		pds = append(pds, pc.newProposal(&ba))
	}

	for i := 0; i < 3; i++ {
		pds = append(pds, pc.newPutProposal(now))
	}

	for _, pd := range pds {
		_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		require.NoError(t, b.Insert(ctx, pd, tok.Move(ctx)))
	}
	require.NoError(t, b.flushLocked(ctx))
	require.Equal(t, len(pds), matchingDroppedProposalsSeen)
}

// Test that the propBuf properly assigns closed timestamps to proposals being
// flushed out of it. Each subtest proposes one command and checks for the
// expected closed timestamp being written to the proposal by the propBuf.
func TestProposalBufferClosedTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const maxOffset = 500 * time.Millisecond
	mc := timeutil.NewManualTime(timeutil.Unix(1613588135, 0))
	clock := hlc.NewClock(mc, maxOffset, maxOffset, hlc.PanicLogger)
	st := cluster.MakeTestingClusterSettings()
	closedts.TargetDuration.Override(ctx, &st.SV, time.Second)
	closedts.SideTransportCloseInterval.Override(ctx, &st.SV, 200*time.Millisecond)
	now := clock.NowAsClockTimestamp()
	nowTS := now.ToTimestamp()
	nowMinusClosedLag := nowTS.Add(-closedts.TargetDuration.Get(&st.SV).Nanoseconds(), 0)
	nowMinusTwiceClosedLag := nowTS.Add(-2*closedts.TargetDuration.Get(&st.SV).Nanoseconds(), 0)
	nowPlusGlobalReadLead := nowTS.Add((maxOffset +
		275*time.Millisecond /* sideTransportPropTime */ +
		25*time.Millisecond /* bufferTime */).Nanoseconds(), 0)
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
		Replica:    roachpb.ReplicaDescriptor{StoreID: 1, ReplicaID: 1},
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
				Replica:  curLease.Replica,
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
				Replica:  curLease.Replica,
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
				Replica:  curLease.Replica,
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
			r.status.Lead = 1
			r.status.RaftState = raftpb.StateLeader
			r.status.Progress = map[raftpb.PeerID]rafttracker.Progress{
				1: {State: rafttracker.StateReplicate},
			}
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
				pd = pc.newLeaseRequestProposal(tc.lease)
			case leaseTransfer:
				pd = pc.newLeaseTransferProposal(tc.lease)
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

type testFlowTokenHandle struct{}

var _ kvflowcontrol.Handle = &testFlowTokenHandle{}

func (t *testFlowTokenHandle) Admit(
	ctx context.Context, priority admissionpb.WorkPriority, t2 time.Time,
) (bool, error) {
	return false, nil
}

func (t *testFlowTokenHandle) DeductTokensFor(
	ctx context.Context,
	priority admissionpb.WorkPriority,
	position kvflowcontrolpb.RaftLogPosition,
	tokens kvflowcontrol.Tokens,
) {
}

func (t *testFlowTokenHandle) ReturnTokensUpto(
	ctx context.Context,
	priority admissionpb.WorkPriority,
	position kvflowcontrolpb.RaftLogPosition,
	stream kvflowcontrol.Stream,
) {
}

func (t *testFlowTokenHandle) ConnectStream(
	ctx context.Context, position kvflowcontrolpb.RaftLogPosition, stream kvflowcontrol.Stream,
) {
}

func (t *testFlowTokenHandle) DisconnectStream(ctx context.Context, stream kvflowcontrol.Stream) {
}

func (t *testFlowTokenHandle) ResetStreams(ctx context.Context) {
}

func (t *testFlowTokenHandle) Inspect(context.Context) kvflowinspectpb.Handle {
	return kvflowinspectpb.Handle{}
}

func (t *testFlowTokenHandle) Close(ctx context.Context) {
}
