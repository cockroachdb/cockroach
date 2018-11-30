// Copyright 2018 The Cockroach Authors.
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
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"go.etcd.io/etcd/raft"
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
			ctx, raftStatus.Progress, r.descRLocked().Replicas, r.mu.lastUpdateTimes, timeutil.Now(),
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
		_ = rawNode.Propose(encodeRaftCommandV1(makeIDKey(), nil))
		// NB: we need to unquiesce as the group might be quiesced.
		return true /* unquiesceAndWakeLeader */, nil
	})
	r.raftMu.Unlock()
}

func (sdh *splitDelayHelper) NumAttempts() int {
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
	// We have an "optimization" to avoid Raft snapshots by dropping some
	// outgoing MsgAppResp (see the _ assignment below) which takes effect for
	// RaftPostSplitSuppressSnapshotTicks ticks after an uninitialized replica
	// is created. This check can err, in which case the snapshot will be
	// delayed for that many ticks, and so we want to delay by at least as much
	// plus a bit of padding to give a snapshot a chance to catch the follower
	// up. If we run out of time, we'll resume the split no matter what.
	_ = (*Replica)(nil).maybeDropMsgAppResp // guru assignment
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
			if replicaID == raftStatus.Lead {
				// TODO(tschottdorf): remove this once we have picked up
				// https://github.com/etcd-io/etcd/pull/10279
				continue
			}

			if pr.State != raft.ProgressStateReplicate {
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
