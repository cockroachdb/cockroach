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
	merge *postCommitMerge
	desc  *roachpb.RangeDescriptor
	lease *roachpb.Lease

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
