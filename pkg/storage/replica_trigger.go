// Copyright 2016 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
)

// postCommitSplit is emitted when a Replica commits a split trigger and
// signals that the Replica has prepared the on-disk state for both the left
// and right hand sides of the split, and that the left hand side Replica
// should be updated as well as the right hand side created.
type postCommitSplit struct {
	roachpb.SplitTrigger
	// RHSDelta holds the statistics for what was written to what is now the
	// right-hand side of the split during the batch which executed it.
	// The on-disk state of the right-hand side is already correct, but the
	// Store must learn about this delta to update its counters appropriately.
	RightDeltaMS enginepb.MVCCStats
}

type postCommitMerge struct {
	roachpb.MergeTrigger
}

type proposalResult struct {
	*PostCommitTrigger

	// The stats delta that the application of the Raft command would cause.
	// On a split, contains only the contributions to the left-hand side.
	//
	// TODO(tschottdorf): we could also not send this along and compute it
	// from the new stats (which are contained in the write batch). See about
	// a potential performance penalty (reads forcing an index to be built for
	// what is initially a slim Go batch) in doing so.
	//
	// We are interested in this delta only to report it to the Store, which
	// keeps a running total of all of its Replicas' stats.
	delta enginepb.MVCCStats
}

// PostCommitTrigger is returned from Raft processing as a side effect which
// signals that further action should be taken as part of the processing of the
// Raft command.
// Depending on the content, actions may be executed on all Replicas, the lease
// holder, or a Replica determined by other conditions present in the specific
// trigger.
type PostCommitTrigger struct {
	noConcurrentReads bool

	gcThreshold        *hlc.Timestamp
	txnSpanGCThreshold *hlc.Timestamp
	truncatedState     *roachpb.RaftTruncatedState
	raftLogSize        *int64
	frozen             *bool

	// intents stores any intents encountered but not conflicted with. They
	// should be handed off to asynchronous intent processing so that an
	// attempt to resolve them is made.
	intents []intentsWithArg
	// split contains a postCommitSplit trigger emitted on a split.
	split *postCommitSplit
	// merge is emitted on merge.
	merge              *postCommitMerge
	desc               *roachpb.RangeDescriptor
	leaseMetricsResult *bool // increase success or error lease counter
	lease              *roachpb.Lease

	gossipFirstRange        bool
	maybeGossipSystemConfig bool
	maybeAddToSplitQueue    bool
	addToReplicaGCQueue     bool

	maybeGossipNodeLiveness *roachpb.Span

	computeChecksum *roachpb.ComputeChecksumRequest
}

// updateTrigger takes a previous and new commit trigger and combines their
// contents into an updated trigger, consuming both inputs. It will panic on
// illegal combinations (such as being asked to combine two split triggers).
//
// TODO(tschottdorf): refactor, in particular shell out transitions of
// `r.mu.state`.
func updateTrigger(old, new *PostCommitTrigger) *PostCommitTrigger {
	if old == nil {
		old = new
	} else if new != nil {
		if new.gcThreshold != nil {
			old.gcThreshold = new.gcThreshold
		}
		if new.txnSpanGCThreshold != nil {
			old.txnSpanGCThreshold = new.txnSpanGCThreshold
		}
		if new.truncatedState != nil {
			old.truncatedState = new.truncatedState
		}
		if new.raftLogSize != nil {
			old.raftLogSize = new.raftLogSize
		}
		if new.frozen != nil {
			old.frozen = new.frozen
		}

		if new.intents != nil {
			old.intents = append(old.intents, new.intents...)
		}
		if old.split == nil {
			old.split = new.split
		} else if new.split != nil {
			panic("more than one split trigger")
		}
		if old.merge == nil {
			old.merge = new.merge
		} else if new.merge != nil {
			panic("more than one merge trigger")
		}
		if old.desc == nil {
			old.desc = new.desc
		} else if new.desc != nil {
			panic("more than one descriptor update")
		}
		if old.lease == nil {
			old.lease = new.lease
		} else if new.lease != nil {
			panic("more than one lease update")
		}
		if new.leaseMetricsResult != nil {
			old.leaseMetricsResult = new.leaseMetricsResult
		}

		if new.gossipFirstRange {
			old.gossipFirstRange = true
		}
		if new.maybeGossipSystemConfig {
			old.maybeGossipSystemConfig = true
		}
		if new.maybeAddToSplitQueue {
			old.maybeAddToSplitQueue = true
		}
		if new.addToReplicaGCQueue {
			old.addToReplicaGCQueue = true
		}

		if new.maybeGossipNodeLiveness != nil {
			old.maybeGossipNodeLiveness = new.maybeGossipNodeLiveness
		}

		if new.computeChecksum != nil {
			old.computeChecksum = new.computeChecksum
		}
	}
	return old
}

func (r *Replica) gcOldChecksumEntriesLocked(now time.Time) {
	for id, val := range r.mu.checksums {
		// The timestamp is valid only if set.
		if !val.gcTimestamp.IsZero() && now.After(val.gcTimestamp) {
			delete(r.mu.checksums, id)
		}
	}
}

func (r *Replica) computeChecksumTrigger(ctx context.Context, args roachpb.ComputeChecksumRequest) {
	stopper := r.store.Stopper()
	id := args.ChecksumID
	now := timeutil.Now()
	r.mu.Lock()
	var notify chan struct{}
	if c, ok := r.mu.checksums[id]; !ok {
		// There is no record of this ID. Make a new notification.
		notify = make(chan struct{})
	} else if !c.started {
		// A CollectChecksumRequest is waiting on the existing notification.
		notify = c.notify
	} else {
		// A previous attempt was made to compute the checksum.
		r.mu.Unlock()
		return
	}

	r.gcOldChecksumEntriesLocked(now)

	// Create an entry with checksum == nil and gcTimestamp unset.
	r.mu.checksums[id] = replicaChecksum{started: true, notify: notify}
	desc := *r.mu.state.Desc
	r.mu.Unlock()
	snap := r.store.NewSnapshot()

	// Compute SHA asynchronously and store it in a map by UUID.
	if err := stopper.RunAsyncTask(ctx, func(ctx context.Context) {
		defer snap.Close()
		var snapshot *roachpb.RaftSnapshotData
		if args.Snapshot {
			snapshot = &roachpb.RaftSnapshotData{}
		}
		sha, err := r.sha512(desc, snap, snapshot)
		if err != nil {
			log.Errorf(ctx, "%v", err)
			sha = nil
		}
		r.computeChecksumDone(ctx, id, sha, snapshot)
	}); err != nil {
		defer snap.Close()
		log.Error(ctx, errors.Wrapf(err, "could not run async checksum computation (ID = %s)", id))
		// Set checksum to nil.
		r.computeChecksumDone(ctx, id, nil, nil)
	}
}

func (r *Replica) leasePostCommitTrigger(
	ctx context.Context,
	trigger PostCommitTrigger,
	replicaID roachpb.ReplicaID,
	prevLease *roachpb.Lease,
) {
	iAmTheLeaseHolder := trigger.lease.Replica.ReplicaID == replicaID
	leaseChangingHands := prevLease.Replica.StoreID != trigger.lease.Replica.StoreID

	if leaseChangingHands && iAmTheLeaseHolder {
		// If this replica is a new holder of the lease, update the low water
		// mark of the timestamp cache. Note that clock offset scenarios are
		// handled via a stasis period inherent in the lease which is documented
		// in on the Lease struct.
		//
		// The introduction of lease transfers implies that the previous lease
		// may have been shortened and we are now applying a formally overlapping
		// lease (since the old lease holder has promised not to serve any more
		// requests, this is kosher). This means that we don't use the old
		// lease's expiration but instead use the new lease's start to initialize
		// the timestamp cache low water.
		log.Infof(ctx, "new range lease %s following %s [physicalTime=%s]",
			trigger.lease, prevLease, r.store.Clock().PhysicalTime())
		r.mu.Lock()
		r.mu.tsCache.SetLowWater(trigger.lease.Start)
		r.mu.Unlock()

		// Gossip the first range whenever its lease is acquired. We check to
		// make sure the lease is active so that a trailing replica won't process
		// an old lease request and attempt to gossip the first range.
		if r.IsFirstRange() && trigger.lease.Covers(r.store.Clock().Now()) {
			r.gossipFirstRange(ctx)
		}
	}
	if leaseChangingHands && !iAmTheLeaseHolder {
		// We're not the lease holder, reset our timestamp cache, releasing
		// anything currently cached. The timestamp cache is only used by the
		// lease holder. Note that we'll call SetLowWater when we next acquire
		// the lease.
		r.mu.Lock()
		r.mu.tsCache.Clear(r.store.Clock())
		r.mu.Unlock()
	}

	if !iAmTheLeaseHolder && trigger.lease.Covers(r.store.Clock().Now()) {
		// If this replica is the raft leader but it is not the new lease holder,
		// then try to transfer the raft leadership to match the lease. We like it
		// when leases and raft leadership are collocated because that facilitates
		// quick command application (requests generally need to make it to both the
		// lease holder and the raft leader before being applied by other replicas).
		//
		// TODO(andrei): We want to do this attempt when a lease changes hands, and
		// then periodically check that the collocation is fine. So we keep checking
		// it here on lease extensions, which happen periodically, but that's pretty
		// arbitrary. There might be a more natural place elsewhere where this
		// periodic check should happen.
		r.maybeTransferRaftLeadership(ctx, replicaID, trigger.lease.Replica.ReplicaID)
	}
}

// maybeTransferRaftLeadership attempts to transfer the leadership away from
// this node to target, if this node is the current raft leader.
// The transfer might silently fail, particularly (only?) if the transferee is
// behind on applying the log.
func (r *Replica) maybeTransferRaftLeadership(
	ctx context.Context, replicaID roachpb.ReplicaID, target roachpb.ReplicaID,
) {
	err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		if raftGroup.Status().RaftState == raft.StateLeader {
			// Only the raft leader can attempt a leadership transfer.
			log.Infof(ctx, "range %s: transferring raft leadership to replica ID %v",
				r, target)
			raftGroup.TransferLeader(uint64(target))
		}
		return true, nil
	})
	if err != nil {
		// An error here indicates that this Replica has been destroyed
		// while lacking the necessary synchronization (or even worse, it
		// fails spuriously - could be a storage error), and so we avoid
		// sweeping that under the rug.
		//
		// TODO(tschottdorf): this error is not handled any more
		// at this level.
		log.Fatal(ctx, NewReplicaCorruptionError(err))
	}
}

func (r *Replica) handleTrigger(
	ctx context.Context, originReplica roachpb.ReplicaDescriptor, trigger PostCommitTrigger,
) {
	if trigger.noConcurrentReads {
		r.readOnlyCmdMu.Lock()
		defer r.readOnlyCmdMu.Unlock()
	}

	if trigger.split != nil {
		// TODO(tschottdorf): We want to let the usual MVCCStats-delta
		// machinery update our stats for the left-hand side. But there is no
		// way to pass up an MVCCStats object that will clear out the
		// ContainsEstimates flag. We should introduce one, but the migration
		// makes this worth a separate effort (ContainsEstimates would need to
		// have three possible values, 'UNCHANGED', 'NO', and 'YES').
		// Until then, we're left with this rather crude hack.
		{
			r.mu.Lock()
			r.mu.state.Stats.ContainsEstimates = false
			stats := r.mu.state.Stats
			r.mu.Unlock()
			if err := setMVCCStats(ctx, r.store.Engine(), r.RangeID, stats); err != nil {
				log.Fatal(ctx, errors.Wrap(err, "unable to write MVCC stats"))
			}
		}

		splitTriggerPostCommit(
			r.ctx,
			trigger.split.RightDeltaMS,
			&trigger.split.SplitTrigger,
			r,
		)
	}
	if trigger.merge != nil {
		r.mu.Lock()
		r.mu.tsCache.Clear(r.store.Clock())
		r.mu.Unlock()

		if err := r.store.MergeRange(r, trigger.merge.LeftDesc.EndKey,
			trigger.merge.RightDesc.RangeID,
		); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf(ctx, "failed to update store after merging range: %s", err)
		}
	}

	if trigger.gcThreshold != nil {
		r.mu.Lock()
		r.mu.state.GCThreshold = *trigger.gcThreshold
		r.mu.Unlock()
	}

	if trigger.txnSpanGCThreshold != nil {
		r.mu.Lock()
		r.mu.state.TxnSpanGCThreshold = *trigger.txnSpanGCThreshold
		r.mu.Unlock()
	}

	if trigger.truncatedState != nil {
		r.mu.Lock()
		r.mu.state.TruncatedState = trigger.truncatedState
		r.mu.Unlock()
		// Clear any entries in the Raft log entry cache for this range up
		// to and including the most recently truncated index.
		r.store.raftEntryCache.clearTo(r.RangeID, trigger.truncatedState.Index+1)
	}
	if trigger.raftLogSize != nil {
		r.mu.Lock()
		r.mu.raftLogSize = *trigger.raftLogSize
		r.mu.Unlock()
	}
	if trigger.frozen != nil {
		r.mu.Lock()
		r.mu.state.Frozen = *trigger.frozen
		r.mu.Unlock()
	}

	if trigger.desc != nil {
		if err := r.setDesc(trigger.desc); err != nil {
			// Log the error. There's not much we can do because the commit may have already occurred at this point.
			log.Fatalf(ctx, "failed to update range descriptor to %+v: %s",
				trigger.desc, err)
		}
	}
	if trigger.lease != nil {
		r.mu.Lock()
		prevLease := r.mu.state.Lease
		r.mu.state.Lease = trigger.lease
		replicaID := r.mu.replicaID
		r.mu.Unlock()

		r.leasePostCommitTrigger(ctx, trigger, replicaID, prevLease)
	}
	if trigger.leaseMetricsResult != nil {
		r.store.metrics.leaseRequestComplete(*trigger.leaseMetricsResult)
	}

	if trigger.gossipFirstRange {
		// We need to run the gossip in an async task because gossiping requires
		// the range lease and we'll deadlock if we try to acquire it while
		// holding processRaftMu. Specifically, Replica.redirectOnOrAcquireLease
		// blocks waiting for the lease acquisition to finish but it can't finish
		// because we're not processing raft messages due to holding
		// processRaftMu (and running on the processRaft goroutine).
		if err := r.store.Stopper().RunAsyncTask(ctx, func(ctx context.Context) {
			hasLease, pErr := r.getLeaseForGossip(ctx)

			if pErr != nil {
				log.Infof(ctx, "unable to gossip first range; hasLease=%t, err=%s", hasLease, pErr)
			} else if !hasLease {
				return
			}
			r.gossipFirstRange(ctx)
		}); err != nil {
			log.Infof(ctx, "unable to gossip first range: %s", err)
		}
	}

	if trigger.addToReplicaGCQueue {
		if _, err := r.store.replicaGCQueue.Add(r, replicaGCPriorityRemoved); err != nil {
			// Log the error; the range should still be GC'd eventually.
			log.Errorf(ctx, "unable to add to replica GC queue: %s", err)
		}
	}

	if trigger.maybeAddToSplitQueue {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
	}

	if trigger.maybeGossipSystemConfig {
		r.maybeGossipSystemConfig()
	}

	// On the replica on which this command originated, resolve skipped intents
	// asynchronously - even on failure.
	//
	// TODO(tschottdorf): EndTransaction will use this pathway to return
	// intents which should immediately be resolved. However, there's
	// a slight chance that an error between the origin of that intents
	// slice and here still results in that intent slice arriving here
	// without the EndTransaction having committed. We should clearly
	// separate the part of the trigger which also applies on errors.
	if originReplica.StoreID == r.store.StoreID() {
		r.store.intentResolver.processIntentsAsync(r, trigger.intents)
	}

	if trigger.maybeGossipNodeLiveness != nil {
		r.maybeGossipNodeLiveness(*trigger.maybeGossipNodeLiveness)
	}

	if trigger.computeChecksum != nil {
		r.computeChecksumTrigger(ctx, *trigger.computeChecksum)
	}
}
