// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/tracker"
)

type splitDelayHelperI interface {
	RaftStatus(context.Context) (roachpb.RangeID, *raft.Status)
	ProposeEmptyCommand(ctx context.Context)
	NumAttempts() int
	Sleep(context.Context) time.Duration
}

type splitDelayHelper Replica

func (sdh *splitDelayHelper) RaftStatus(ctx context.Context) (roachpb.RangeID, *raft.Status) {
	r := (*Replica)(sdh)
	r.mu.RLock()
	raftStatus := r.raftStatusRLocked()
	if raftStatus != nil {
		updateRaftProgressFromActivity(
			ctx, raftStatus.Progress, r.descRLocked().Replicas().All(),
			func(replicaID roachpb.ReplicaID) bool {
				return r.mu.lastUpdateTimes.isFollowerActiveSince(
					ctx, replicaID, timeutil.Now(), r.store.cfg.RangeLeaseActiveDuration())
			},
		)
	}
	r.mu.RUnlock()
	return r.RangeID, raftStatus
}

func (sdh *splitDelayHelper) ProposeEmptyCommand(ctx context.Context) {
	r := (*Replica)(sdh)
	r.raftMu.Lock()
	_ = r.withRaftGroup(true /* campaignOnWake */, func(rawNode *raft.RawNode) (bool, error) {
		// NB: intentionally ignore the error (which can be ErrProposalDropped
		// when there's an SST inflight).
		data := encodeRaftCommand(raftVersionStandard, makeIDKey(), nil)
		_ = rawNode.Propose(data)
		// NB: we need to unquiesce as the group might be quiesced.
		return true /* unquiesceAndWakeLeader */, nil
	})
	r.raftMu.Unlock()
}

func (sdh *splitDelayHelper) NumAttempts() int {
	// There is a related mechanism regarding snapshots and splits that is worth
	// pointing out here: Incoming MsgApp (see the _ assignment below) are
	// dropped if they are addressed to uninitialized replicas likely to become
	// initialized via a split trigger. These MsgApp are sent approximately once
	// per heartbeat interval, but sometimes there's an additional delay thanks
	// to having to wait for a GC run. In effect, it shouldn't take more than a
	// small number of heartbeats until the follower leaves probing status, so
	// NumAttempts should at least match that.
	_ = maybeDropMsgApp // guru assignment
	// Snapshots can come up for other reasons and at the end of the day, the
	// delay introduced here needs to make sure that the snapshot queue
	// processes at a higher rate than splits happen, so the number of attempts
	// will typically be much higher than what's suggested by maybeDropMsgApp.
	return (*Replica)(sdh).store.cfg.RaftDelaySplitToSuppressSnapshotTicks
}

func (sdh *splitDelayHelper) Sleep(ctx context.Context) time.Duration {
	tBegin := timeutil.Now()

	r := (*Replica)(sdh)
	select {
	case <-time.After(r.store.cfg.RaftTickInterval):
	case <-ctx.Done():
	}

	return timeutil.Since(tBegin)
}

func maybeDelaySplitToAvoidSnapshot(ctx context.Context, sdh splitDelayHelperI) string {
	maxDelaySplitToAvoidSnapshotTicks := sdh.NumAttempts()

	var slept time.Duration
	var extra string
	var succeeded bool
	for ticks := 0; ticks < maxDelaySplitToAvoidSnapshotTicks; ticks++ {
		succeeded = false
		extra = ""
		rangeID, raftStatus := sdh.RaftStatus(ctx)

		if raftStatus == nil {
			// Don't delay on followers (we don't know when to stop). This case
			// is hit rarely enough to not matter.
			extra += "; not Raft leader"
			succeeded = true
			break
		}

		done := true
		for replicaID, pr := range raftStatus.Progress {
			if pr.State != tracker.StateReplicate {
				if !pr.RecentActive {
					if ticks == 0 {
						// Having set done = false, we make sure we're not exiting early.
						// This is important because we sometimes need that Raft proposal
						// below to make the followers active as there's no chatter on an
						// idle range. (Note that there's a theoretical race in which the
						// follower becomes inactive again during the sleep, but the
						// inactivity interval is much larger than a tick).
						//
						// Don't do this more than once though: if a follower is down,
						// we don't want to delay splits for it.
						done = false
					}
					extra += fmt.Sprintf("; r%d/%d inactive", rangeID, replicaID)
					continue
				}
				done = false
				extra += fmt.Sprintf("; replica r%d/%d not caught up: %+v", rangeID, replicaID, &pr)
			}
		}
		if done {
			succeeded = true
			break
		}
		// Propose an empty command which works around a Raft bug that can
		// leave a follower in ProgressStateProbe even though it has caught
		// up.
		sdh.ProposeEmptyCommand(ctx)
		slept += sdh.Sleep(ctx)

		if ctx.Err() != nil {
			return ""
		}
	}

	if slept != 0 {
		extra += fmt.Sprintf("; delayed split for %.1fs to avoid Raft snapshot", slept.Seconds())
		if !succeeded {
			extra += " (without success)"
		}
	}

	return extra
}
