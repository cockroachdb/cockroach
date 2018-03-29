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

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ProposalData is data about a command which allows it to be
// evaluated, proposed to raft, and for the result of the command to
// be returned to the caller.
type ProposalData struct {
	// The caller's context, used for logging proposals and reproposals.
	ctx context.Context

	// idKey uniquely identifies this proposal.
	// TODO(andreimatei): idKey is legacy at this point: We could easily key
	// commands by their MaxLeaseIndex, and doing so should be ok with a stop-
	// the-world migration. However, various test facilities depend on the
	// command ID for e.g. replay protection.
	idKey storagebase.CmdIDKey

	// proposedAtTicks is the (logical) time at which this command was
	// last (re-)proposed.
	proposedAtTicks int

	// command is serialized and proposed to raft. In the event of
	// reproposals its MaxLeaseIndex field is mutated.
	command *storagebase.RaftCommand

	// endCmds.finish is called after command execution to update the timestamp cache &
	// command queue.
	endCmds *endCmds

	// doneCh is used to signal the waiting RPC handler (the contents of
	// proposalResult come from LocalEvalResult).
	//
	// Attention: this channel is not to be signaled directly downstream of Raft.
	// Always use ProposalData.finishApplication().
	doneCh chan proposalResult

	// Local contains the results of evaluating the request
	// tying the upstream evaluation of the request to the
	// downstream application of the command.
	Local *result.LocalResult

	// Request is the client's original BatchRequest.
	// TODO(tschottdorf): tests which use TestingCommandFilter use this.
	// Decide how that will work in the future, presumably the
	// CommandFilter would run at proposal time or we allow an opaque
	// struct to be attached to a proposal which is then available as it
	// applies. Other than tests, we only need a few bits of the request
	// here; this could be replaced with isLease and isChangeReplicas
	// booleans.
	Request *roachpb.BatchRequest
}

// finishApplication is when a command application has finished. This will be
// called downstream of Raft if the command required consensus, but can be
// called upstream of Raft if the command did not and was never proposed.
// proposal.doneCh is signaled with pr so that the proposer is unblocked.
//
// It first invokes the endCmds function and then sends the specified
// proposalResult on the proposal's done channel. endCmds is invoked here in
// order to allow the original client to be canceled and possibly no longer
// listening to this done channel, and so can't be counted on to invoke endCmds
// itself.
//
// Note: this should not be called upstream of Raft because, in case pr.Err is
// set, it clears the intents from pr before sending it on the channel. This
// clearing should not be done upstream of Raft because, in cases of errors
// encountered upstream of Raft, we might still want to resolve intents:
// upstream of Raft, pr.intents represent intents encountered by a request, not
// the current txn's intents.
func (proposal *ProposalData) finishApplication(pr proposalResult) {
	if proposal.endCmds != nil {
		proposal.endCmds.done(pr.Reply, pr.Err, pr.ProposalRetry)
		proposal.endCmds = nil
	}
	proposal.doneCh <- pr
	close(proposal.doneCh)
}

// TODO(tschottdorf): we should find new homes for the checksum, lease
// code, and various others below to leave here only the core logic.
// Not moving anything right now to avoid awkward diffs.

func (r *Replica) gcOldChecksumEntriesLocked(now time.Time) {
	for id, val := range r.mu.checksums {
		// The timestamp is valid only if set.
		if !val.gcTimestamp.IsZero() && now.After(val.gcTimestamp) {
			delete(r.mu.checksums, id)
		}
	}
}

func (r *Replica) computeChecksumPostApply(
	ctx context.Context, args roachpb.ComputeChecksumRequest,
) {
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
	r.mu.checksums[id] = ReplicaChecksum{started: true, notify: notify}
	desc := *r.mu.state.Desc
	r.mu.Unlock()
	// Caller is holding raftMu, so an engine snapshot is automatically
	// Raft-consistent (i.e. not in the middle of an AddSSTable).
	snap := r.store.engine.NewSnapshot()

	// Compute SHA asynchronously and store it in a map by UUID.
	if err := stopper.RunAsyncTask(ctx, "storage.Replica: computing checksum", func(ctx context.Context) {
		defer snap.Close()
		var snapshot *roachpb.RaftSnapshotData
		if args.Snapshot {
			snapshot = &roachpb.RaftSnapshotData{}
		}
		result, err := r.sha512(ctx, desc, snap, snapshot)
		if err != nil {
			log.Errorf(ctx, "%v", err)
			result = nil
		}
		r.computeChecksumDone(ctx, id, result, snapshot)
	}); err != nil {
		defer snap.Close()
		log.Error(ctx, errors.Wrapf(err, "could not run async checksum computation (ID = %s)", id))
		// Set checksum to nil.
		r.computeChecksumDone(ctx, id, nil, nil)
	}
}

// leasePostApply is called when a RequestLease or TransferLease
// request is executed for a range.
func (r *Replica) leasePostApply(ctx context.Context, newLease roachpb.Lease) {
	r.mu.Lock()
	replicaID := r.mu.replicaID
	prevLease := *r.mu.state.Lease
	r.mu.Unlock()

	iAmTheLeaseHolder := newLease.Replica.ReplicaID == replicaID
	// NB: in the case in which a node restarts, minLeaseProposedTS forces it to
	// get a new lease and we make sure it gets a new sequence number, thus
	// causing the right half of the disjunction to fire so that we update the
	// timestamp cache.
	leaseChangingHands := prevLease.Replica.StoreID != newLease.Replica.StoreID || prevLease.Sequence != newLease.Sequence

	if iAmTheLeaseHolder {
		// Always log lease acquisition for epoch-based leases which are
		// infrequent.
		if newLease.Type() == roachpb.LeaseEpoch || (log.V(1) && leaseChangingHands) {
			log.Infof(ctx, "new range lease %s following %s", newLease, prevLease)
		}
	}

	if leaseChangingHands && iAmTheLeaseHolder {
		// If this replica is a new holder of the lease, update the low water
		// mark of the timestamp cache. Note that clock offset scenarios are
		// handled via a stasis period inherent in the lease which is documented
		// in the Lease struct.
		//
		// The introduction of lease transfers implies that the previous lease
		// may have been shortened and we are now applying a formally overlapping
		// lease (since the old lease holder has promised not to serve any more
		// requests, this is kosher). This means that we don't use the old
		// lease's expiration but instead use the new lease's start to initialize
		// the timestamp cache low water.
		desc := r.Desc()
		for _, keyRange := range rditer.MakeReplicatedKeyRanges(desc) {
			r.store.tsCache.SetLowWater(keyRange.Start.Key, keyRange.End.Key, newLease.Start)
		}

		// Reset the request counts used to make lease placement decisions whenever
		// starting a new lease.
		if r.leaseholderStats != nil {
			r.leaseholderStats.resetRequestCounts()
		}
	}

	// Sanity check to make sure that the lease sequence is moving in the right
	// direction.
	if s1, s2 := prevLease.Sequence, newLease.Sequence; s1 != 0 {
		// We're at a version that supports lease sequence numbers.
		switch {
		case s2 < s1:
			log.Fatalf(ctx, "lease sequence inversion, prevLease=%s, newLease=%s", prevLease, newLease)
		case s2 == s1:
			// If the sequence numbers are the same, make sure they're actually
			// the same lease. This can happen when callers are using
			// leasePostApply for some of its side effects, like with
			// splitPostApply. It can also happen during lease extensions.
			if !prevLease.Equivalent(newLease) {
				log.Fatalf(ctx, "sequence identical for different leases, prevLease=%s, newLease=%s",
					prevLease, newLease)
			}
		case s2 == s1+1:
			// Lease sequence incremented by 1. Expected case.
		case s2 > s1+1:
			// Snapshots will never call leasePostApply, so we always expect
			// leases to increment one at a time here.
			log.Fatalf(ctx, "lease sequence jump, prevLease=%s, newLease=%s", prevLease, newLease)
		}
	}

	// We're setting the new lease after we've updated the timestamp cache in
	// order to avoid race conditions where a replica starts serving requests
	// for a lease without first having taken into account requests served
	// by the previous lease holder.
	r.mu.Lock()
	r.mu.state.Lease = &newLease
	r.mu.Unlock()

	// Gossip the first range whenever its lease is acquired. We check to
	// make sure the lease is active so that a trailing replica won't process
	// an old lease request and attempt to gossip the first range.
	if leaseChangingHands && iAmTheLeaseHolder && r.IsFirstRange() && r.IsLeaseValid(newLease, r.store.Clock().Now()) {
		r.gossipFirstRange(ctx)
	}

	if leaseChangingHands && !iAmTheLeaseHolder {
		// Also clear and disable the push transaction queue. Any waiters
		// must be redirected to the new lease holder.
		r.txnWaitQueue.Clear(true /* disable */)
	}

	if !iAmTheLeaseHolder && r.IsLeaseValid(newLease, r.store.Clock().Now()) {
		// If this replica is the raft leader but it is not the new lease holder,
		// then try to transfer the raft leadership to match the lease. We like it
		// when leases and raft leadership are collocated because that facilitates
		// quick command application (requests generally need to make it to both the
		// lease holder and the raft leader before being applied by other replicas).
		// Note that this condition is also checked periodically when computing
		// replica metrics.
		r.maybeTransferRaftLeadership(ctx, newLease.Replica.ReplicaID)
	}

	// Notify the store that a lease change occurred and it may need to
	// gossip the updated store descriptor (with updated capacity).
	if leaseChangingHands && (prevLease.OwnedBy(r.store.StoreID()) ||
		newLease.OwnedBy(r.store.StoreID())) {
		r.store.maybeGossipOnCapacityChange(ctx, leaseChangeEvent)
		if r.leaseholderStats != nil {
			r.leaseholderStats.resetRequestCounts()
		}
	}

	// Potentially re-gossip if the range contains system data (e.g. system
	// config or node liveness). We need to perform this gossip at startup as
	// soon as possible. Trying to minimize how often we gossip is a fool's
	// errand. The node liveness info will be gossiped frequently (every few
	// seconds) in any case due to the liveness heartbeats. And the system config
	// will be gossiped rarely because it falls on a range with an epoch-based
	// range lease that is only reacquired extremely infrequently.
	if iAmTheLeaseHolder {
		if err := r.MaybeGossipSystemConfig(ctx); err != nil {
			log.Error(ctx, err)
		}
		if err := r.MaybeGossipNodeLiveness(ctx, keys.NodeLivenessSpan); err != nil {
			log.Error(ctx, err)
		}
		// Make sure the push transaction queue is enabled.
		r.txnWaitQueue.Enable()
	}

	// Mark the new lease in the replica's lease history.
	if r.leaseHistory != nil {
		r.leaseHistory.add(newLease)
	}
}

func addSSTablePreApply(
	ctx context.Context,
	st *cluster.Settings,
	eng engine.Engine,
	sideloaded sideloadStorage,
	term, index uint64,
	sst storagebase.ReplicatedEvalResult_AddSSTable,
	limiter *rate.Limiter,
) bool {
	checksum := util.CRC32(sst.Data)

	if checksum != sst.CRC32 {
		log.Fatalf(
			ctx,
			"checksum for AddSSTable at index term %d, index %d does not match; at proposal time %x (%d), now %x (%d)",
			term, index, sst.CRC32, sst.CRC32, checksum, checksum,
		)
	}

	const modify, noModify = true, false

	path, err := sideloaded.Filename(ctx, index, term)
	if err != nil {
		log.Fatalf(ctx, "sideloaded SSTable at term %d, index %d is missing", term, index)
	}

	copied := false
	if inmem, ok := eng.(engine.InMem); ok {
		path = fmt.Sprintf("%x", checksum)
		if err := inmem.WriteFile(path, sst.Data); err != nil {
			panic(err)
		}
	} else {
		ingestPath := path + ".ingested"

		// The SST may already be on disk, thanks to the sideloading mechanism.  If
		// so we can try to add that file directly, via a new hardlink if the file-
		// system support it, rather than writing a new copy of it. However, this is
		// only safe if we can do so without modifying the file since it is still
		// part of an immutable raft log message, but in some cases, described in
		// DBIngestExternalFile, RocksDB would modify the file. Fortunately we can
		// tell Rocks that it is not allowed to modify the file, in which case it
		// will return and error if it would have tried to do so, at which point we
		// can fall back to writing a new copy for Rocks to ingest.
		if _, err := os.Stat(path); err == nil {
			// If the fs supports it, make a hard-link for rocks to ingest. We cannot
			// pass it the path in the sideload store as it deletes the passed path on
			// success.
			if linkErr := os.Link(path, ingestPath); linkErr == nil {
				ingestErr := eng.IngestExternalFile(ctx, ingestPath, noModify)
				if ingestErr == nil {
					// Adding without modification succeeded, no copy necessary.
					log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, ingestPath)
					return false
				}
				if rmErr := os.Remove(ingestPath); rmErr != nil {
					log.Fatalf(ctx, "failed to move ingest sst: %v", rmErr)
				}
				const seqNoMsg = "Global seqno is required, but disabled"
				if err, ok := err.(*engine.RocksDBError); ok && !strings.Contains(err.Error(), seqNoMsg) {
					log.Fatalf(ctx, "while ingesting %s: %s", ingestPath, err)
				}
			}
		}

		path = ingestPath

		log.Eventf(ctx, "copying SSTable for ingestion at index %d, term %d: %s", index, term, path)

		// TODO(tschottdorf): remove this once sideloaded storage guarantees its
		// existence.
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			panic(err)
		}
		if _, err := os.Stat(path); err == nil {
			// The file we want to ingest exists. This can happen since the
			// ingestion may apply twice (we ingest before we mark the Raft
			// command as committed). Just unlink the file (RocksDB created a
			// hard link); after that we're free to write it again.
			if err := os.Remove(path); err != nil {
				log.Fatalf(ctx, "while removing existing file during ingestion of %s: %s", path, err)
			}
		}

		if err := writeFileSyncing(ctx, path, sst.Data, 0600, st, limiter); err != nil {
			log.Fatalf(ctx, "while ingesting %s: %s", path, err)
		}
		copied = true
	}

	if err := eng.IngestExternalFile(ctx, path, modify); err != nil {
		log.Fatalf(ctx, "while ingesting %s: %s", path, err)
	}
	log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, path)
	return copied
}

// maybeTransferRaftLeadership attempts to transfer the leadership
// away from this node to target, if this node is the current raft
// leader. We don't attempt to transfer leadership if the transferee
// is behind on applying the log.
func (r *Replica) maybeTransferRaftLeadership(ctx context.Context, target roachpb.ReplicaID) {
	err := r.withRaftGroup(func(raftGroup *raft.RawNode) (bool, error) {
		// Only the raft leader can attempt a leadership transfer.
		if status := raftGroup.Status(); status.RaftState == raft.StateLeader {
			// Only attempt this if the target has all the log entries. Although
			// TransferLeader is supposed to do the right thing if the target is not
			// caught up, this check avoids periods of 0 QPS:
			// https://github.com/cockroachdb/cockroach/issues/22573#issuecomment-366106118
			if pr, ok := status.Progress[uint64(target)]; (ok && pr.Match == r.mu.lastIndex) || r.mu.draining {
				log.VEventf(ctx, 1, "transferring raft leadership to replica ID %v", target)
				r.store.metrics.RangeRaftLeaderTransfers.Inc(1)
				raftGroup.TransferLeader(uint64(target))
			}
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

func (r *Replica) handleReplicatedEvalResult(
	ctx context.Context,
	rResult storagebase.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
) (shouldAssert bool) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		rResult.IsLeaseRequest = false
		rResult.Timestamp = hlc.Timestamp{}
		rResult.DeprecatedStartKey = nil
		rResult.DeprecatedEndKey = nil
		rResult.PrevLeaseProposal = nil
	}

	if rResult.BlockReads {
		r.readOnlyCmdMu.Lock()
		defer r.readOnlyCmdMu.Unlock()
		rResult.BlockReads = false
	}

	// Update MVCC stats and Raft portion of ReplicaState.
	deltaStats := rResult.Delta.ToStats()
	r.mu.Lock()
	r.mu.state.Stats.Add(deltaStats)
	if raftAppliedIndex != 0 {
		r.mu.state.RaftAppliedIndex = raftAppliedIndex
	}
	if leaseAppliedIndex != 0 {
		r.mu.state.LeaseAppliedIndex = leaseAppliedIndex
	}
	needsSplitBySize := r.needsSplitBySizeRLocked()
	r.mu.Unlock()

	r.store.metrics.addMVCCStats(deltaStats)
	rResult.Delta = enginepb.MVCCNetworkStats{}

	if needsSplitBySize {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
	}

	// The above are always present. The following are not always present but
	// should not trigger a ReplicaState assertion because they are either too
	// frequent to do so or because they do not change the ReplicaState.

	if rResult.State != nil {
		// Raft log truncation is too frequent to justify a replica state
		// assertion.
		if newTruncState := rResult.State.TruncatedState; newTruncState != nil {
			rResult.State.TruncatedState = nil // for assertion

			r.mu.Lock()
			r.mu.state.TruncatedState = newTruncState
			r.mu.Unlock()

			// TODO(tschottdorf): everything below doesn't need to be on this
			// goroutine. Worth moving out -- truncations are frequent and missing
			// one of the side effects below doesn't matter. Need to be careful
			// about the interaction with `evalTruncateLog` though, which computes
			// some stats based on the log entries it sees. Also, sideloaded storage
			// needs to hold the raft mu. Perhaps it should just get its own mutex
			// (which is usually held together with raftMu, except when accessing
			// the storage for a truncation). Or, even better, make use of the fact
			// that all we need to synchronize is disk i/o, and there is no overlap
			// between files *removed* during truncation and those active in Raft.

			if r.store.cfg.Settings.Version.IsActive(cluster.VersionRaftLogTruncationBelowRaft) {
				// Truncate the Raft log.
				batch := r.store.Engine().NewWriteOnlyBatch()
				// We know that all of the deletions from here forward will be to distinct keys.
				writer := batch.Distinct()
				start := engine.MakeMVCCMetadataKey(keys.RaftLogKey(r.RangeID, 0))
				end := engine.MakeMVCCMetadataKey(
					keys.RaftLogKey(r.RangeID, newTruncState.Index).PrefixEnd(),
				)
				iter := r.store.Engine().NewIterator(false /* prefix */)
				// Clear the log entries. Intentionally don't use range deletion
				// tombstones (ClearRange()) due to performance concerns connected
				// to having many range deletion tombstones. There is a chance that
				// ClearRange will perform well here because the tombstones could be
				// "collapsed", but it is hardly worth the risk at this point.
				if err := writer.ClearIterRange(iter, start, end); err != nil {
					log.Errorf(ctx, "unable to clear truncated Raft entries for %+v: %s", newTruncState, err)
				}
				iter.Close()
				writer.Close()

				if err := batch.Commit(false); err != nil {
					log.Errorf(ctx, "unable to clear truncated Raft entries for %+v: %s", newTruncState, err)
				}
				batch.Close()
			}

			// Clear any entries in the Raft log entry cache for this range up
			// to and including the most recently truncated index.
			r.store.raftEntryCache.clearTo(r.RangeID, newTruncState.Index+1)

			// Truncate the sideloaded storage. Note that this is safe only if the new truncated state
			// is durably on disk (i.e.) synced. This is true at the time of writing but unfortunately
			// could rot.
			{
				log.Eventf(ctx, "truncating sideloaded storage up to (and including) index %d", newTruncState.Index)
				if err := r.raftMu.sideloaded.TruncateTo(ctx, newTruncState.Index+1); err != nil {
					// We don't *have* to remove these entries for correctness. Log a
					// loud error, but keep humming along.
					log.Errorf(ctx, "while removing sideloaded files during log truncation: %s", err)
				}
			}
		}

		// ReplicaState.Stats was previously non-nullable which caused nodes to
		// send a zero-value MVCCStats structure. If the proposal was generated by
		// an old node, we'll have decoded that zero-value structure setting
		// ReplicaState.Stats to a non-nil value which would trigger the "unhandled
		// field in ReplicatedEvalResult" assertion to fire if we didn't clear it.
		if rResult.State.Stats != nil && (*rResult.State.Stats == enginepb.MVCCStats{}) {
			rResult.State.Stats = nil
		}

		if (*rResult.State == storagebase.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if rResult.RaftLogDelta != 0 {
		r.mu.Lock()
		r.mu.raftLogSize += rResult.RaftLogDelta
		r.mu.raftLogLastCheckSize += rResult.RaftLogDelta
		// Ensure raftLog{,LastCheck}Size is not negative since it isn't persisted
		// between server restarts.
		if r.mu.raftLogSize < 0 {
			r.mu.raftLogSize = 0
		}
		if r.mu.raftLogLastCheckSize < 0 {
			r.mu.raftLogLastCheckSize = 0
		}
		r.mu.Unlock()
		rResult.RaftLogDelta = 0
	} else {
		// Check for whether to queue the range for Raft log truncation if this is
		// not a Raft log truncation command itself. We don't want to check the
		// Raft log for truncation on every write operation or even every operation
		// which occurs after the Raft log exceeds RaftLogQueueStaleSize. The logic
		// below queues the replica for possible Raft log truncation whenever an
		// additional RaftLogQueueStaleSize bytes have been written to the Raft
		// log.
		r.mu.Lock()
		checkRaftLog := r.mu.raftLogSize-r.mu.raftLogLastCheckSize >= RaftLogQueueStaleSize
		if checkRaftLog {
			r.mu.raftLogLastCheckSize = r.mu.raftLogSize
		}
		r.mu.Unlock()
		if checkRaftLog {
			r.store.raftLogQueue.MaybeAdd(r, r.store.Clock().Now())
		}
	}

	for _, sc := range rResult.SuggestedCompactions {
		r.store.compactor.Suggest(ctx, sc)
	}
	rResult.SuggestedCompactions = nil

	// The rest of the actions are "nontrivial" and may have large effects on the
	// in-memory and on-disk ReplicaStates. If any of these actions are present,
	// we want to assert that these two states do not diverge.
	shouldAssert = !rResult.Equal(storagebase.ReplicatedEvalResult{})

	// Process Split or Merge. This needs to happen after stats update because
	// of the ContainsEstimates hack.

	if rResult.Split != nil {
		splitPostApply(
			r.AnnotateCtx(ctx),
			rResult.Split.RHSDelta,
			&rResult.Split.SplitTrigger,
			r,
		)
		rResult.Split = nil
	}

	if rResult.Merge != nil {
		if err := r.store.MergeRange(ctx, r, rResult.Merge.LeftDesc.EndKey,
			rResult.Merge.RightDesc.RangeID,
		); err != nil {
			// Our in-memory state has diverged from the on-disk state.
			log.Fatalf(ctx, "failed to update store after merging range: %s", err)
		}
		rResult.Merge = nil
	}

	// Update the remaining ReplicaState.

	if rResult.State != nil {
		if newDesc := rResult.State.Desc; newDesc != nil {
			if err := r.setDesc(newDesc); err != nil {
				// Log the error. There's not much we can do because the commit may
				// have already occurred at this point.
				log.Fatalf(
					ctx,
					"failed to update range descriptor to %+v: %s",
					newDesc, err,
				)
			}
			rResult.State.Desc = nil
		}

		if newLease := rResult.State.Lease; newLease != nil {
			rResult.State.Lease = nil // for assertion

			r.leasePostApply(ctx, *newLease)
		}

		if newThresh := rResult.State.GCThreshold; newThresh != nil {
			if (*newThresh != hlc.Timestamp{}) {
				r.mu.Lock()
				r.mu.state.GCThreshold = newThresh
				r.mu.Unlock()
			}
			rResult.State.GCThreshold = nil
		}

		if newThresh := rResult.State.TxnSpanGCThreshold; newThresh != nil {
			if (*newThresh != hlc.Timestamp{}) {
				r.mu.Lock()
				r.mu.state.TxnSpanGCThreshold = newThresh
				r.mu.Unlock()
			}
			rResult.State.TxnSpanGCThreshold = nil
		}

		if (*rResult.State == storagebase.ReplicaState{}) {
			rResult.State = nil
		}
	}

	if change := rResult.ChangeReplicas; change != nil {
		if change.ChangeType == roachpb.REMOVE_REPLICA &&
			r.store.StoreID() == change.Replica.StoreID {
			// This wants to run as late as possible, maximizing the chances
			// that the other nodes have finished this command as well (since
			// processing the removal from the queue looks up the Range at the
			// lease holder, being too early here turns this into a no-op).
			if _, err := r.store.replicaGCQueue.Add(r, replicaGCPriorityRemoved); err != nil {
				// Log the error; the range should still be GC'd eventually.
				log.Errorf(ctx, "unable to add to replica GC queue: %s", err)
			}
		}
		rResult.ChangeReplicas = nil
	}

	if rResult.ComputeChecksum != nil {
		r.computeChecksumPostApply(ctx, *rResult.ComputeChecksum)
		rResult.ComputeChecksum = nil
	}

	if !rResult.Equal(storagebase.ReplicatedEvalResult{}) {
		log.Fatalf(ctx, "unhandled field in ReplicatedEvalResult: %s", pretty.Diff(rResult, storagebase.ReplicatedEvalResult{}))
	}
	return shouldAssert
}

func (r *Replica) handleLocalEvalResult(ctx context.Context, lResult result.LocalResult) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Reply = nil
	}

	// ======================
	// Non-state updates and actions.
	// ======================

	// The caller is required to detach and handle intents.
	if lResult.Intents != nil {
		log.Fatalf(ctx, "LocalEvalResult.Intents should be nil: %+v", lResult.Intents)
	}
	if lResult.EndTxns != nil {
		log.Fatalf(ctx, "LocalEvalResult.EndTxns should be nil: %+v", lResult.EndTxns)
	}

	if lResult.GossipFirstRange {
		// We need to run the gossip in an async task because gossiping requires
		// the range lease and we'll deadlock if we try to acquire it while
		// holding processRaftMu. Specifically, Replica.redirectOnOrAcquireLease
		// blocks waiting for the lease acquisition to finish but it can't finish
		// because we're not processing raft messages due to holding
		// processRaftMu (and running on the processRaft goroutine).
		if err := r.store.Stopper().RunAsyncTask(
			ctx, "storage.Replica: gossipping first range",
			func(ctx context.Context) {
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
		lResult.GossipFirstRange = false
	}

	if lResult.MaybeAddToSplitQueue {
		r.store.splitQueue.MaybeAdd(r, r.store.Clock().Now())
		lResult.MaybeAddToSplitQueue = false
	}

	if lResult.MaybeGossipSystemConfig {
		if err := r.MaybeGossipSystemConfig(ctx); err != nil {
			log.Error(ctx, err)
		}
		lResult.MaybeGossipSystemConfig = false
	}
	if lResult.MaybeGossipNodeLiveness != nil {
		if err := r.MaybeGossipNodeLiveness(ctx, *lResult.MaybeGossipNodeLiveness); err != nil {
			log.Error(ctx, err)
		}
		lResult.MaybeGossipNodeLiveness = nil
	}

	if lResult.LeaseMetricsResult != nil {
		switch metric := *lResult.LeaseMetricsResult; metric {
		case result.LeaseRequestSuccess, result.LeaseRequestError:
			r.store.metrics.leaseRequestComplete(metric == result.LeaseRequestSuccess)
		case result.LeaseTransferSuccess, result.LeaseTransferError:
			r.store.metrics.leaseTransferComplete(metric == result.LeaseTransferSuccess)
		}
		lResult.LeaseMetricsResult = nil
	}

	if lResult.UpdatedTxns != nil {
		for _, txn := range *lResult.UpdatedTxns {
			r.txnWaitQueue.UpdateTxn(ctx, txn)
			lResult.UpdatedTxns = nil
		}
	}

	if (lResult != result.LocalResult{}) {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
}

func (r *Replica) handleEvalResultRaftMuLocked(
	ctx context.Context,
	lResult *result.LocalResult,
	rResult storagebase.ReplicatedEvalResult,
	raftAppliedIndex, leaseAppliedIndex uint64,
) {
	shouldAssert := r.handleReplicatedEvalResult(ctx, rResult, raftAppliedIndex, leaseAppliedIndex)
	if lResult != nil {
		r.handleLocalEvalResult(ctx, *lResult)
	}
	if shouldAssert {
		// Assert that the on-disk state doesn't diverge from the in-memory
		// state as a result of the side effects.
		r.mu.Lock()
		r.assertStateLocked(ctx, r.store.Engine())
		r.mu.Unlock()
	}
}
