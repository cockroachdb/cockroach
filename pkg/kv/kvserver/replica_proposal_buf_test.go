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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"golang.org/x/sync/errgroup"
)

// testProposer is a testing implementation of proposer.
type testProposer struct {
	syncutil.RWMutex
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

	// leaderReplicaInDescriptor is set if the leader (as indicated by raftGroup)
	// is known, and that leader is part of the range's descriptor (as seen by the
	// current replica). This can be used to simulate the local replica being so
	// far behind that it doesn't have an up to date descriptor.
	leaderReplicaInDescriptor bool
	// If leaderReplicaInDescriptor is set, this specifies what type of replica it
	// is. Some types of replicas are not eligible to get a lease.
	leaderReplicaType roachpb.ReplicaType
}

var _ proposer = &testProposer{}

type testProposerRaft struct {
	status raft.BasicStatus
}

var _ proposerRaft = testProposerRaft{}

func (t testProposerRaft) Step(raftpb.Message) error {
	// TODO(andrei, nvanbenschoten): Capture the message and test against it.
	return nil
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

func (t *testProposer) replicaID() roachpb.ReplicaID {
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

func (t *testProposer) closeTimestampPolicy() roachpb.RangeClosedTimestampPolicy {
	return roachpb.LAG_BY_CLUSTER_SETTING
}

func (rp *testProposer) raftAppliedClosedTimestamp() hlc.Timestamp {
	return hlc.MinTimestamp
}

func (t *testProposer) withGroupLocked(fn func(proposerRaft) error) error {
	// Note that t.raftGroup can be nil, which FlushLockedWithRaftGroup supports.
	return fn(t.raftGroup)
}

func (t *testProposer) registerProposalLocked(p *ProposalData) {
	t.registered++
}

func (t *testProposer) leaderStatusRLocked(raftGroup proposerRaft) rangeLeaderInfo {
	leaderKnown := raftGroup.BasicStatus().Lead != raft.None
	var leaderRep roachpb.ReplicaID
	var iAmTheLeader, leaderEligibleForLease bool
	if leaderKnown {
		leaderRep = roachpb.ReplicaID(raftGroup.BasicStatus().Lead)
		iAmTheLeader = leaderRep == t.replicaID()
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

func newPropData(leaseReq bool) (*ProposalData, []byte) {
	var ba roachpb.BatchRequest
	pd := &ProposalData{
		command: &kvserverpb.RaftCommand{
			ReplicatedEvalResult: kvserverpb.ReplicatedEvalResult{
				State: &kvserverpb.ReplicaState{Lease: &roachpb.Lease{}},
			},
		},
		Request: &ba,
	}
	if leaseReq {
		ba.Add(&roachpb.RequestLeaseRequest{})
	} else {
		ba.Add(&roachpb.PutRequest{})
	}
	return pd, make([]byte, 0, kvserverpb.MaxRaftCommandFooterSize())
}

// TestProposalBuffer tests the basic behavior of the Raft proposal buffer.
func TestProposalBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var p testProposer
	var b propBuf
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, clock, cluster.MakeTestingClusterSettings())

	// Insert propBufArrayMinSize proposals. The buffer should not be flushed.
	num := propBufArrayMinSize
	for i := 0; i < num; i++ {
		leaseReq := i == 3
		pd, data := newPropData(leaseReq)
		_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		mlai, err := b.Insert(ctx, pd, data, tok)
		require.Nil(t, err)
		if leaseReq {
			expMlai := uint64(i)
			require.Equal(t, uint64(0), mlai)
			require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
			require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
		} else {
			expMlai := uint64(i + 1)
			require.Equal(t, expMlai, mlai)
			require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
			require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
		}
		require.Equal(t, i+1, b.Len())
		require.Equal(t, 1, p.enqueued)
		require.Equal(t, 0, p.registered)
	}
	require.Equal(t, num, b.evalTracker.Count())

	// Insert another proposal. This causes the buffer to flush. Doing so
	// results in a lease applied index being skipped, which is harmless.
	// Remember that the lease request above did not receive a lease index.
	pd, data := newPropData(false)
	_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	mlai, err := b.Insert(ctx, pd, data, tok)
	require.Nil(t, err)
	expMlai := uint64(num + 1)
	require.Equal(t, expMlai, mlai)
	require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
	require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
	require.Equal(t, 1, b.Len())
	require.Equal(t, 2, p.enqueued)
	require.Equal(t, num, p.registered)
	require.Equal(t, uint64(num), b.liBase)
	require.Equal(t, 2*propBufArrayMinSize, b.arr.len())
	require.Equal(t, 1, b.evalTracker.Count())

	// Increase the proposer's applied lease index and flush. The buffer's
	// lease index offset should jump up.
	p.lai = 10
	require.Nil(t, b.flushLocked(ctx))
	require.Equal(t, 0, b.Len())
	require.Equal(t, 2, p.enqueued)
	require.Equal(t, num+1, p.registered)
	require.Equal(t, p.lai, b.liBase)

	// Insert one more proposal. The lease applied index should adjust to
	// the increase accordingly.
	_, tok = b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
	mlai, err = b.Insert(ctx, pd, data, tok)
	require.Nil(t, err)
	expMlai = p.lai + 1
	require.Equal(t, expMlai, mlai)
	require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
	require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
	require.Equal(t, 1, b.Len())
	require.Equal(t, 3, p.enqueued)
	require.Equal(t, num+1, p.registered)

	// Flush the buffer repeatedly until its array shrinks. We've already
	// flushed once above, so start iterating at 1.
	for i := 1; i < propBufArrayShrinkDelay; i++ {
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

	var p testProposer
	var b propBuf
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, clock, cluster.MakeTestingClusterSettings())

	mlais := make(map[uint64]struct{})
	dsErr := errors.New("destroyed")

	// Run 20 concurrent producers.
	var g errgroup.Group
	const concurrency = 20
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				pd, data := newPropData(false)
				_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
				mlai, err := b.Insert(ctx, pd, data, tok)
				if err != nil {
					if errors.Is(err, dsErr) {
						return nil
					}
					return errors.Wrap(err, "Insert")
				}
				p.Lock()
				if _, ok := mlais[mlai]; ok {
					p.Unlock()
					return errors.New("max lease index collision")
				}
				mlais[mlai] = struct{}{}
				p.Unlock()
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
	t.Logf("%d successful proposals before destroy", len(mlais))
}

// TestProposalBufferRegistersAllOnProposalError tests that all proposals in the
// proposal buffer are registered with the proposer when the buffer is flushed,
// even if an error is seen when proposing a batch of entries.
func TestProposalBufferRegistersAllOnProposalError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var p testProposer
	var b propBuf
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, clock, cluster.MakeTestingClusterSettings())

	num := propBufArrayMinSize
	toks := make([]TrackedRequestToken, num)
	for i := 0; i < num; i++ {
		pd, data := newPropData(false)
		_, toks[i] = b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		_, err := b.Insert(ctx, pd, data, toks[i])
		require.Nil(t, err)
	}
	require.Equal(t, num, b.Len())

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

// TestProposalBufferRegistrationWithInsertionErrors tests that if during
// proposal insertion we reserve array indexes but are unable to actually insert
// them due to errors, we simply ignore said indexes when flushing proposals.
func TestProposalBufferRegistrationWithInsertionErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var p testProposer
	var b propBuf
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	b.Init(&p, clock, cluster.MakeTestingClusterSettings())

	num := propBufArrayMinSize / 2
	toks1 := make([]TrackedRequestToken, num)
	for i := 0; i < num; i++ {
		pd, data := newPropData(i%2 == 0)
		_, toks1[i] = b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		_, err := b.Insert(ctx, pd, data, toks1[i])
		require.Nil(t, err)
	}

	var insertErr = errors.New("failed insertion")
	b.testing.leaseIndexFilter = func(*ProposalData) (indexOverride uint64, err error) {
		return 0, insertErr
	}

	toks2 := make([]TrackedRequestToken, num)
	for i := 0; i < num; i++ {
		pd, data := newPropData(i%2 == 0)
		_, toks2[i] = b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
		_, err := b.Insert(ctx, pd, data, toks2[i])
		require.Equal(t, insertErr, err)
	}
	require.Equal(t, 2*num, b.Len())

	require.Nil(t, b.flushLocked(ctx))

	require.Equal(t, 0, b.Len())
	require.Equal(t, num, p.registered)
	require.Zero(t, b.evalTracker.Count())
}

// TestPropBufCnt tests the basic behavior of the counter maintained by the
// proposal buffer.
func TestPropBufCnt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var count propBufCnt
	const numReqs = 10

	reqLeaseInc := makePropBufCntReq(true)
	reqLeaseNoInc := makePropBufCntReq(false)

	for i := 0; i < numReqs; i++ {
		count.update(reqLeaseInc)
	}

	res := count.read()
	assert.Equal(t, numReqs, res.arrayLen())
	assert.Equal(t, numReqs-1, res.arrayIndex())
	assert.Equal(t, uint64(numReqs), res.leaseIndexOffset())

	for i := 0; i < numReqs; i++ {
		count.update(reqLeaseNoInc)
	}

	res = count.read()
	assert.Equal(t, 2*numReqs, res.arrayLen())
	assert.Equal(t, (2*numReqs)-1, res.arrayIndex())
	assert.Equal(t, uint64(numReqs), res.leaseIndexOffset())

	count.clear()
	res = count.read()
	assert.Equal(t, 0, res.arrayLen())
	assert.Equal(t, -1, res.arrayIndex())
	assert.Equal(t, uint64(0), res.leaseIndexOffset())
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
			name:  "follower, known ineligible leader",
			state: raft.StateFollower,
			// Someone else is leader.
			leader: self + 1,
			// The leader type makes it ineligible to get the lease. Thus, the local
			// proposal will not be rejected.
			leaderRepType: roachpb.VOTER_DEMOTING,
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
			// p.replicaID() is hardcoded; it'd better be hardcoded to what this test
			// expects.
			require.Equal(t, self, uint64(p.replicaID()))

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
			r := testProposerRaft{
				status: raftStatus,
			}
			p.raftGroup = r
			p.leaderReplicaInDescriptor = !tc.leaderNotInRngDesc
			p.leaderReplicaType = tc.leaderRepType

			var b propBuf
			clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
			b.Init(&p, clock, cluster.MakeTestingClusterSettings())

			pd, data := newPropData(true /* leaseReq */)
			_, tok := b.TrackEvaluatingRequest(ctx, hlc.MinTimestamp)
			_, err := b.Insert(ctx, pd, data, tok)
			require.NoError(t, err)
			require.NoError(t, b.flushLocked(ctx))
			if tc.expRejection {
				require.Equal(t, roachpb.ReplicaID(tc.leader), rejected)
			} else {
				require.Equal(t, roachpb.ReplicaID(0), rejected)
			}
		})
	}
}
