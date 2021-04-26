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
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

type splitDelayHelperI interface {
	RaftStatus(context.Context) (roachpb.RangeID, *raft.Status)
	ProposeEmptyCommand(ctx context.Context)
	MaxTicks() int
	TickDuration() time.Duration
	Sleep(context.Context, time.Duration)
}

type splitDelayHelper Replica

func (sdh *splitDelayHelper) RaftStatus(ctx context.Context) (roachpb.RangeID, *raft.Status) {
	r := (*Replica)(sdh)
	r.mu.RLock()
	raftStatus := r.raftStatusRLocked()
	if raftStatus != nil {
		updateRaftProgressFromActivity(
			ctx, raftStatus.Progress, r.descRLocked().Replicas().Descriptors(),
			func(replicaID roachpb.ReplicaID) bool {
				return r.mu.lastUpdateTimes.isFollowerActiveSince(
					ctx, replicaID, timeutil.Now(), r.store.cfg.RangeLeaseActiveDuration())
			},
		)
	}
	r.mu.RUnlock()
	return r.RangeID, raftStatus
}

func (sdh *splitDelayHelper) Sleep(ctx context.Context, dur time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(dur):
	}
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

func (sdh *splitDelayHelper) MaxTicks() int {
	// There is a related mechanism regarding snapshots and splits that is worth
	// pointing out here: Incoming MsgApp (see the _ assignment below) are
	// dropped if they are addressed to uninitialized replicas likely to become
	// initialized via a split trigger. These MsgApp are sent approximately once
	// per heartbeat interval, but sometimes there's an additional delay thanks
	// to having to wait for a GC run. In effect, it shouldn't take more than a
	// small number of heartbeats until the follower leaves probing status, so
	// MaxTicks should at least match that.
	_ = maybeDropMsgApp // guru assignment
	// Snapshots can come up for other reasons and at the end of the day, the
	// delay introduced here needs to make sure that the snapshot queue
	// processes at a higher rate than splits happen, so the number of attempts
	// will typically be much higher than what's suggested by maybeDropMsgApp.
	return (*Replica)(sdh).store.cfg.RaftDelaySplitToSuppressSnapshotTicks
}

func (sdh *splitDelayHelper) TickDuration() time.Duration {
	r := (*Replica)(sdh)
	return r.store.cfg.RaftTickInterval
}

func maybeDelaySplitToAvoidSnapshot(ctx context.Context, sdh splitDelayHelperI) string {
	maxDelaySplitToAvoidSnapshotTicks := sdh.MaxTicks()
	tickDur := sdh.TickDuration()
	budget := tickDur * time.Duration(maxDelaySplitToAvoidSnapshotTicks)

	var slept time.Duration
	var problems []string
	var lastProblems []string
	var i int
	for slept < budget {
		i++
		problems = problems[:0]
		rangeID, raftStatus := sdh.RaftStatus(ctx)

		if raftStatus == nil || raftStatus.RaftState == raft.StateFollower {
			// Don't delay on followers (we don't have information about the
			// peers in that state and thus can't determine when it is safe
			// to continue). This case is hit rarely enough to not matter,
			// as the raft leadership follows the lease and splits get routed
			// to leaseholders only.
			problems = append(problems, "replica is raft follower")
			break
		}

		// If we're not a follower nor a leader, there are elections going on.
		// Wait until these have concluded (at which point we'll either be
		// follower, terminated above, or leader, and will have a populated
		// progress). This is an important step for preventing cascading
		// issues when a range is split rapidly in ascending order (i.e.
		// each split splits the right hand side resulting from the prior
		// split). Without this code, the split delay helper may end up
		// returning early for each split in the following sequence:
		//
		// - r1=[a,z) has members [s1, s2]
		// - r1 splits into r1=[a,b) and r2=[b,z)
		// - s1 applies the split, r2@s1 reaches out to r2@s2 (MsgApp)
		// - r2@s2 (not initialized yet) drops the MsgApp thanks to `maybeDropMsgApp`
		// - r2@s1 marks r2@s1 as probing, will only contact again in 1s
		// - r2 splits again into r2=[b,c), r3=[c,z)
		// - r2@s1 applies the split, r3@s1 reaches out to r3@s2 (which is not initialized)
		// - r3@s2 drops MsgApp due to `maybeDropMsgApp`
		// - r3@s1 marks r3@s2 as probing, will only contact again in 1s
		// - ...
		// - r24@s1 splits into r25=[x,y) and r26=[y,z)
		// - r24@s1 reaches out to r24@s2 (not inited and drops MsgApp)
		//
		// Note that every step here except the fourth one is almost guaranteed.
		// Once an MsgApp has been dropped, the next split is also going to have
		// the same behavior, since the dropped MsgApp prevents the next split
		// from applying on that follower in a timely manner. The issue thus
		// becomes self-sustaining.
		//
		// At some point during this cascade, s2 will likely apply the first split
		// trigger on its replica of r1=[a,z), which will initialize r2@s2. However,
		// since r2@s1 has already marked r2@s2 as probing, it won't contact it, on
		// average, for another 500ms. When it does, it will append the next split to
		// the log which can then be applied, but then there is another ~500ms wait
		// until r3@s2 will be caught up by r3@s1 to learn about the next split. This
		// means that on average, split N is delayed by ~N*500ms. `maybeDropMsgApp` on
		// deeply nested ranges on s2 will eventually give up and this will lead to,
		// roughly, snapshots being requested across most of the ranges, but none
		// of these snapshots can apply because the keyspace is always occupied by
		// one of the descendants of the initial range (however far the splits have
		// made it on s2). On top of log spam and wasted work, this prevents the
		// Raft snapshot queue from doing useful work that may also be necessary.
		//
		// The main contribution of the helper to avoiding this cascade is to wait
		// for the replicas of the right-hand side to be initialized. This breaks
		// the above history because a split will only commence once all prior
		// splits in the chain have applied on all members.
		//
		// See TestSplitBurstWithSlowFollower for end-to-end verification of this
		// mechanism.
		if raftStatus.RaftState != raft.StateLeader {
			problems = append(problems, fmt.Sprintf("not leader (%s)", raftStatus.RaftState))
		}

		for replicaID, pr := range raftStatus.Progress {
			if pr.State != tracker.StateReplicate {
				if !pr.RecentActive {
					if slept < tickDur {
						// We don't want to delay splits for a follower who hasn't responded within a tick.
						problems = append(problems, fmt.Sprintf("r%d/%d inactive", rangeID, replicaID))
						if i == 1 {
							// Propose an empty command which works around a Raft bug that can
							// leave a follower in ProgressStateProbe even though it has caught
							// up.
							//
							// We have long picked up a fix[1] for the bug, but there might be similar
							// issues we're not aware of and this doesn't hurt, so leave it in for now.
							//
							// [1]: https://github.com/etcd-io/etcd/commit/bfaae1ba462c91aaf149a285b8d2369807044f71
							sdh.ProposeEmptyCommand(ctx)
						}
					}
					continue
				}
				problems = append(problems, fmt.Sprintf("replica r%d/%d not caught up: %+v", rangeID, replicaID, &pr))
			}
		}
		if len(problems) == 0 {
			break
		}

		lastProblems = problems

		// The second factor starts out small and reaches ~0.7 approximately at i=maxDelaySplitToAvoidSnapshotTicks.
		// In effect we loop approximately 2*maxDelaySplitToAvoidSnapshotTicks to exhaust the entire budget we have.
		// By having shorter sleeps at the beginning, we optimize for the common case in which things get fixed up
		// quickly early on. In particular, splitting in a tight loop will usually always wait on the election of the
		// previous split's right-hand side, which finishes within a few network latencies (which is typically much
		// less than a full tick).
		sleepDur := time.Duration(float64(tickDur) * (1.0 - math.Exp(-float64(i-1)/float64(maxDelaySplitToAvoidSnapshotTicks+1))))
		sdh.Sleep(ctx, sleepDur)
		slept += sleepDur

		if err := ctx.Err(); err != nil {
			problems = append(problems, err.Error())
			break
		}
	}

	var msg string
	// If we exited the loop with problems, use them as lastProblems
	// and indicate that we did not manage to "delay the problems away".
	if len(problems) != 0 {
		lastProblems = problems
	}
	if len(lastProblems) != 0 {
		msg = fmt.Sprintf("; delayed by %.1fs to resolve: %s", slept.Seconds(), strings.Join(lastProblems, "; "))
		if len(problems) != 0 {
			msg += " (without success)"
		}
	}

	return msg
}
