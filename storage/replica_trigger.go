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
	"bytes"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
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

	gcThreshold    *hlc.Timestamp
	truncatedState *roachpb.RaftTruncatedState
	raftLogSize    *int64
	frozen         *bool

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

	computeChecksum *roachpb.ComputeChecksumRequest
	verifyChecksum  *roachpb.VerifyChecksumRequest
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

		if new.computeChecksum != nil {
			old.computeChecksum = new.computeChecksum
		}
		if new.verifyChecksum != nil {
			old.verifyChecksum = new.verifyChecksum
		}
	}
	return old
}

func (r *Replica) computeChecksumTrigger(
	ctx context.Context, args roachpb.ComputeChecksumRequest,
) {
	stopper := r.store.Stopper()
	id := args.ChecksumID
	now := timeutil.Now()
	r.mu.Lock()
	if _, ok := r.mu.checksums[id]; ok {
		// A previous attempt was made to compute the checksum.
		r.mu.Unlock()
		return
	}

	// GC old entries.
	var oldEntries []uuid.UUID
	for id, val := range r.mu.checksums {
		// The timestamp is only valid when the checksum is set.
		if val.checksum != nil && now.After(val.gcTimestamp) {
			oldEntries = append(oldEntries, id)
		}
	}
	for _, id := range oldEntries {
		delete(r.mu.checksums, id)
	}

	// Create an entry with checksum == nil and gcTimestamp unset.
	r.mu.checksums[id] = replicaChecksum{notify: make(chan struct{})}
	desc := *r.mu.state.Desc
	r.mu.Unlock()
	snap := r.store.NewSnapshot()

	// Compute SHA asynchronously and store it in a map by UUID.
	if err := stopper.RunAsyncTask(func() {
		defer snap.Close()
		var snapshot *roachpb.RaftSnapshotData
		if args.Snapshot {
			snapshot = &roachpb.RaftSnapshotData{}
		}
		sha, err := r.sha512(desc, snap, snapshot)
		if err != nil {
			log.Errorf(ctx, "%s: %v", r, err)
			sha = nil
		}
		r.computeChecksumDone(context.Background(), id, sha, snapshot)
	}); err != nil {
		defer snap.Close()
		// Set checksum to nil.
		r.computeChecksumDone(ctx, id, nil, nil)
	}
}

func (r *Replica) verifyChecksumTrigger(
	ctx context.Context, args roachpb.VerifyChecksumRequest,
) {
	id := args.ChecksumID
	c, ok := r.getChecksum(ctx, id)
	if !ok {
		log.Errorf(ctx, "%s: consistency check skipped: checksum for id = %v doesn't exist", r, id)
		// Return success because a checksum might be missing only on
		// this replica. A checksum might be missing because of a
		// number of reasons: GC-ed, server restart, and ComputeChecksum
		// version incompatibility.
		return
	}
	if c.checksum != nil && !bytes.Equal(c.checksum, args.Checksum) {
		// Replication consistency problem!
		logFunc := log.Errorf

		// Collect some more debug information.
		if args.Snapshot == nil {
			// No debug information; run another consistency check to deliver
			// more debug information.
			if err := r.store.stopper.RunAsyncTask(func() {
				log.Errorf(ctx, "%s: consistency check failed; fetching details", r)
				desc := r.Desc()
				startKey := desc.StartKey.AsRawKey()
				// Can't use a start key less than LocalMax.
				if bytes.Compare(startKey, keys.LocalMax) < 0 {
					startKey = keys.LocalMax
				}
				if err := r.store.db.CheckConsistency(startKey, desc.EndKey.AsRawKey(), true /* withDiff */); err != nil {
					log.Errorf(ctx, "couldn't rerun consistency check: %s", err)
				}
			}); err != nil {
				log.Error(ctx, errors.Wrap(err, "could not rerun consistency check"))
			}
		} else {
			// Compute diff.
			diff := diffRange(args.Snapshot, c.snapshot)
			if diff != nil {
				for _, d := range diff {
					l := "leader"
					if d.LeaseHolder {
						l = "replica"
					}
					log.Errorf(ctx, "%s: consistency check failed: k:v = (%s (%x), %s, %x) not present on %s",
						r, d.Key, d.Key, d.Timestamp, d.Value, l)
				}
			}
			if r.store.ctx.ConsistencyCheckPanicOnFailure {
				if p := r.store.ctx.TestingKnobs.BadChecksumPanic; p != nil {
					p(diff)
				} else {
					logFunc = log.Fatalf
				}
			}
		}

		logFunc(ctx, "consistency check failed on replica: %s, checksum mismatch: e = %x, v = %x", r, args.Checksum, c.checksum)
	}
}

func (r *Replica) leasePostCommitTrigger(
	ctx context.Context,
	trigger PostCommitTrigger,
	replicaID roachpb.ReplicaID,
	prevLease *roachpb.Lease, // TODO(tschottdorf): could this not be nil?
) {
	if prevLease.Replica.StoreID != trigger.lease.Replica.StoreID {
		// The lease is changing hands. Is this replica the new lease holder?
		if trigger.lease.Replica.ReplicaID == replicaID {
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
			log.Infof(ctx, "%s: new range lease %s following %s [physicalTime=%s]",
				r, trigger.lease, prevLease, r.store.Clock().PhysicalTime())
			r.mu.Lock()
			r.mu.tsCache.SetLowWater(trigger.lease.Start)
			r.mu.Unlock()

			// Gossip the first range whenever its lease is acquired. We check to
			// make sure the lease is active so that a trailing replica won't process
			// an old lease request and attempt to gossip the first range.
			if r.IsFirstRange() && trigger.lease.Covers(r.store.Clock().Now()) {
				func() {
					r.mu.Lock()
					defer r.mu.Unlock()
					r.gossipFirstRangeLocked(ctx)
				}()
			}
		} else if trigger.lease.Covers(r.store.Clock().Now()) {
			if err := r.withRaftGroup(func(raftGroup *raft.RawNode) error {
				if raftGroup.Status().RaftState == raft.StateLeader {
					// If this replica is the raft leader but it is not the new lease
					// holder, then try to transfer the raft leadership to match the
					// lease.
					log.Infof(ctx, "range %v: replicaID %v transfer raft leadership to replicaID %v",
						r.RangeID, replicaID, trigger.lease.Replica.ReplicaID)
					raftGroup.TransferLeader(uint64(trigger.lease.Replica.ReplicaID))
				}
				return nil
			}); err != nil {
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
	}
}

func (r *Replica) handleTrigger(
	ctx context.Context,
	originReplica roachpb.ReplicaDescriptor,
	trigger PostCommitTrigger,
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
			context.Background(),
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
			log.Fatalf(ctx, "%s: failed to update store after merging range: %s", r, err)
		}
	}

	if trigger.gcThreshold != nil {
		r.mu.Lock()
		r.mu.state.GCThreshold = *trigger.gcThreshold
		r.mu.Unlock()
	}
	if trigger.truncatedState != nil {
		r.mu.Lock()
		r.mu.state.TruncatedState = trigger.truncatedState
		r.mu.Unlock()
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
			log.Fatalf(ctx, "%s: failed to update range descriptor to %+v: %s",
				r, trigger.desc, err)
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
		if err := r.store.Stopper().RunAsyncTask(func() {
			// Create a new context because this is an asynchronous task and we
			// don't want to share the trace.
			ctxInner := context.Background()
			if hasLease, pErr := r.getLeaseForGossip(ctxInner); hasLease {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.gossipFirstRangeLocked(ctxInner)
			} else {
				log.Infof(ctxInner, "unable to gossip first range; hasLease=%t, err=%v", hasLease, pErr)
			}
		}); err != nil {
			log.Errorf(ctx, "unable to gossip first range: %+v", err)
		}
	}

	if trigger.addToReplicaGCQueue {
		if err := r.store.replicaGCQueue.Add(r, 1.0); err != nil {
			// Log the error; the range should still be GC'd eventually.
			log.Errorf(ctx, "%s: unable to add to GC queue: %s", r, err)
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

	if trigger.computeChecksum != nil {
		r.computeChecksumTrigger(ctx, *trigger.computeChecksum)
	}

	if trigger.verifyChecksum != nil {
		r.verifyChecksumTrigger(ctx, *trigger.verifyChecksum)
	}

}
