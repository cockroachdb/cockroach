// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

type testSplitDelayHelper struct {
	numAttempts int

	rangeID    roachpb.RangeID
	raftStatus *raft.Status
	sleep      func()

	slept time.Duration
}

func (h *testSplitDelayHelper) RaftStatus(context.Context) (roachpb.RangeID, *raft.Status) {
	return h.rangeID, h.raftStatus
}

func (h *testSplitDelayHelper) MaxDelay() time.Duration {
	return time.Duration(h.numAttempts) * h.TickDuration()
}

func (h *testSplitDelayHelper) TickDuration() time.Duration {
	return time.Second
}

func (h *testSplitDelayHelper) Sleep(_ context.Context, dur time.Duration) {
	h.slept += dur
	if h.sleep != nil {
		h.sleep()
	}
}

var _ splitDelayHelperI = (*testSplitDelayHelper)(nil)

func TestSplitDelayToAvoidSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	t.Run("disabled", func(t *testing.T) {
		// Should immediately bail out if told to run zero attempts.
		h := &testSplitDelayHelper{
			numAttempts: 0,
			rangeID:     1,
			raftStatus:  nil,
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.EqualValues(t, "", s)
		assert.EqualValues(t, 0, h.slept)
	})

	statusWithState := func(status raftpb.StateType) *raft.Status {
		return &raft.Status{
			BasicStatus: raft.BasicStatus{
				SoftState: raft.SoftState{
					RaftState: status,
				},
			},
		}
	}

	t.Run("nil", func(t *testing.T) {
		// Should immediately bail out if raftGroup is nil.
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus:  nil,
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.EqualValues(t, "; delayed by 0.0s to resolve: replica is raft follower (without success)", s)
		assert.EqualValues(t, 0, h.slept)
	})

	t.Run("follower", func(t *testing.T) {
		// Should immediately bail out if run on follower.
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus:  statusWithState(raftpb.StateFollower),
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.EqualValues(t, "; delayed by 0.0s to resolve: replica is raft follower (without success)", s)
		assert.EqualValues(t, 0, h.slept)
	})

	for _, state := range []raftpb.StateType{raftpb.StatePreCandidate, raftpb.StateCandidate} {
		t.Run(state.String(), func(t *testing.T) {
			h := &testSplitDelayHelper{
				numAttempts: 5,
				rangeID:     1,
				raftStatus:  statusWithState(state),
			}
			s := maybeDelaySplitToAvoidSnapshot(ctx, h)
			assert.EqualValues(t, "; delayed by 5.5s to resolve: not leader ("+state.String()+") (without success)", s)
		})
	}

	t.Run("inactive", func(t *testing.T) {
		st := statusWithState(raftpb.StateLeader)
		st.Progress = map[raftpb.PeerID]tracker.Progress{
			2: {State: tracker.StateProbe},
		}
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus:  st,
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		// We try to wake up the follower once, but then give up on it.
		assert.EqualValues(t, "; delayed by 1.3s to resolve: r1/2 inactive", s)
		assert.Less(t, int64(h.slept), int64(2*h.TickDuration()))
	})

	for _, state := range []tracker.StateType{tracker.StateProbe, tracker.StateSnapshot} {
		t.Run(state.String(), func(t *testing.T) {
			st := statusWithState(raftpb.StateLeader)
			st.Progress = map[raftpb.PeerID]tracker.Progress{
				2: {
					State:              state,
					RecentActive:       true,
					MsgAppProbesPaused: true, // Unifies string output below.
					Inflights:          &tracker.Inflights{},
				},
				// Healthy follower just for kicks.
				3: {State: tracker.StateReplicate},
			}
			h := &testSplitDelayHelper{
				numAttempts: 5,
				rangeID:     1,
				raftStatus:  st,
			}
			s := maybeDelaySplitToAvoidSnapshot(ctx, h)
			assert.EqualValues(t, "; delayed by 5.5s to resolve: replica r1/2 not caught up: "+
				state.String()+" match=0 next=0 sentCommit=0 matchCommit=0 paused (without success)", s)
		})
	}

	t.Run("immediately-replicating", func(t *testing.T) {
		st := statusWithState(raftpb.StateLeader)
		st.Progress = map[raftpb.PeerID]tracker.Progress{
			2: {State: tracker.StateReplicate}, // intentionally not recently active
		}
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus:  st,
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.EqualValues(t, "", s)
		assert.EqualValues(t, 0, h.slept)
	})

	t.Run("becomes-replicating", func(t *testing.T) {
		st := statusWithState(raftpb.StateLeader)
		st.Progress = map[raftpb.PeerID]tracker.Progress{
			2: {State: tracker.StateProbe, RecentActive: true, Inflights: &tracker.Inflights{}},
		}
		h := &testSplitDelayHelper{
			numAttempts: 5,
			rangeID:     1,
			raftStatus:  st,
		}
		// Once >= 2s have passed, the follower becomes replicating.
		h.sleep = func() {
			if h.slept >= 2*time.Second {
				pr := h.raftStatus.Progress[2]
				pr.State = tracker.StateReplicate
				h.raftStatus.Progress[2] = pr
			}
		}
		s := maybeDelaySplitToAvoidSnapshot(ctx, h)
		assert.EqualValues(t, "; delayed by 2.5s to resolve: replica r1/2 not caught up: "+
			"StateProbe match=0 next=0 sentCommit=0 matchCommit=0", s)
	})
}
