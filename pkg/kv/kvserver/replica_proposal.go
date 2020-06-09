// Copyright 2016 The Cockroach Authors.
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
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/sysutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/time/rate"
)

// ProposalData is data about a command which allows it to be
// evaluated, proposed to raft, and for the result of the command to
// be returned to the caller.
type ProposalData struct {
	// The caller's context, used for logging proposals, reproposals, message
	// sends, and command application. In order to enable safely tracing events
	// beneath, modifying this ctx field in *ProposalData requires holding the
	// raftMu.
	ctx context.Context

	// An optional tracing span bound to the proposal. Will be cleaned
	// up when the proposal finishes.
	sp opentracing.Span

	// idKey uniquely identifies this proposal.
	// TODO(andreimatei): idKey is legacy at this point: We could easily key
	// commands by their MaxLeaseIndex, and doing so should be ok with a stop-
	// the-world migration. However, various test facilities depend on the
	// command ID for e.g. replay protection.
	idKey kvserverbase.CmdIDKey

	// proposedAtTicks is the (logical) time at which this command was
	// last (re-)proposed.
	proposedAtTicks int

	// command is serialized and proposed to raft. In the event of
	// reproposals its MaxLeaseIndex field is mutated.
	command *kvserverpb.RaftCommand

	// encodedCommand is the encoded Raft command, with an optional prefix
	// containing the command ID.
	encodedCommand []byte

	// quotaAlloc is the allocation retrieved from the proposalQuota. Once a
	// proposal has been passed to raft modifying this field requires holding the
	// raftMu. Once the proposal comes out of Raft, ownerwhip of this quota is
	// passed to r.mu.quotaReleaseQueue.
	quotaAlloc *quotapool.IntAlloc

	// tmpFooter is used to avoid an allocation.
	tmpFooter kvserverpb.RaftCommandFooter

	// ec.done is called after command application to update the timestamp
	// cache and optionally release latches and exits lock wait-queues.
	ec endCmds

	// applied is set when the a command finishes application. It is used to
	// avoid reproposing a failed proposal if an earlier version of the same
	// proposal succeeded in applying.
	applied bool

	// doneCh is used to signal the waiting RPC handler (the contents of
	// proposalResult come from LocalEvalResult).
	//
	// Attention: this channel is not to be signaled directly downstream of Raft.
	// Always use ProposalData.finishApplication().
	doneCh chan proposalResult

	// Local contains the results of evaluating the request tying the upstream
	// evaluation of the request to the downstream application of the command.
	// Nil when the proposal came from another node (i.e. the evaluation wasn't
	// done here).
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

// finishApplication is called when a command application has finished. The
// method will be called downstream of Raft if the command required consensus,
// but can be called upstream of Raft if the command did not and was never
// proposed.
//
// It first invokes the endCmds function and then sends the specified
// proposalResult on the proposal's done channel. endCmds is invoked here in
// order to allow the original client to be canceled. (When the original client
// is canceled, it won't be listening to this done channel, and so it can't be
// counted on to invoke endCmds itself.)
//
// The method is safe to call more than once, but only the first result will be
// returned to the client.
func (proposal *ProposalData) finishApplication(ctx context.Context, pr proposalResult) {
	proposal.ec.done(ctx, proposal.Request, pr.Reply, pr.Err)
	proposal.signalProposalResult(pr)
	if proposal.sp != nil {
		tracing.FinishSpan(proposal.sp)
		proposal.sp = nil
	}
}

// returnProposalResult signals proposal.doneCh with the proposal result if it
// has not already been signaled. The method can be called even before the
// proposal has finished replication and command application, and does not
// release the request's latches.
//
// The method is safe to call more than once, but only the first result will be
// returned to the client.
func (proposal *ProposalData) signalProposalResult(pr proposalResult) {
	if proposal.doneCh != nil {
		proposal.doneCh <- pr
		proposal.doneCh = nil
	}
}

// releaseQuota releases the proposal's quotaAlloc and sets it to nil.
// If the quotaAlloc is already nil it is a no-op.
func (proposal *ProposalData) releaseQuota() {
	if proposal.quotaAlloc != nil {
		proposal.quotaAlloc.Release()
		proposal.quotaAlloc = nil
	}
}

// TODO(tschottdorf): we should find new homes for the checksum, lease
// code, and various others below to leave here only the core logic.
// Not moving anything right now to avoid awkward diffs. These should
// all be moved to replica_application_result.go.

func (r *Replica) gcOldChecksumEntriesLocked(now time.Time) {
	for id, val := range r.mu.checksums {
		// The timestamp is valid only if set.
		if !val.gcTimestamp.IsZero() && now.After(val.gcTimestamp) {
			delete(r.mu.checksums, id)
		}
	}
}

func (r *Replica) computeChecksumPostApply(ctx context.Context, cc kvserverpb.ComputeChecksum) {
	stopper := r.store.Stopper()
	now := timeutil.Now()
	r.mu.Lock()
	var notify chan struct{}
	if c, ok := r.mu.checksums[cc.ChecksumID]; !ok {
		// There is no record of this ID. Make a new notification.
		notify = make(chan struct{})
	} else if !c.started {
		// A CollectChecksumRequest is waiting on the existing notification.
		notify = c.notify
	} else {
		log.Fatalf(ctx, "attempted to apply ComputeChecksum command with duplicated checksum ID %s",
			cc.ChecksumID)
	}

	r.gcOldChecksumEntriesLocked(now)

	// Create an entry with checksum == nil and gcTimestamp unset.
	r.mu.checksums[cc.ChecksumID] = ReplicaChecksum{started: true, notify: notify}
	desc := *r.mu.state.Desc
	r.mu.Unlock()

	if cc.Version != batcheval.ReplicaChecksumVersion {
		r.computeChecksumDone(ctx, cc.ChecksumID, nil, nil)
		log.Infof(ctx, "incompatible ComputeChecksum versions (requested: %d, have: %d)",
			cc.Version, batcheval.ReplicaChecksumVersion)
		return
	}

	// Caller is holding raftMu, so an engine snapshot is automatically
	// Raft-consistent (i.e. not in the middle of an AddSSTable).
	snap := r.store.engine.NewSnapshot()
	if cc.Checkpoint {
		sl := stateloader.Make(r.RangeID)
		rai, _, err := sl.LoadAppliedIndex(ctx, snap)
		if err != nil {
			log.Warningf(ctx, "unable to load applied index, continuing anyway")
		}
		// NB: the names here will match on all nodes, which is nice for debugging.
		tag := fmt.Sprintf("r%d_at_%d", r.RangeID, rai)
		if dir, err := r.store.checkpoint(ctx, tag); err != nil {
			log.Warningf(ctx, "unable to create checkpoint %s: %+v", dir, err)
		} else {
			log.Warningf(ctx, "created checkpoint %s", dir)
		}
	}

	limiter := limit.NewLimiter(rate.Limit(consistencyCheckRate.Get(&r.store.ClusterSettings().SV)))

	// Compute SHA asynchronously and store it in a map by UUID.
	if err := stopper.RunAsyncTask(ctx, "storage.Replica: computing checksum", func(ctx context.Context) {
		func() {
			defer snap.Close()
			var snapshot *roachpb.RaftSnapshotData
			if cc.SaveSnapshot {
				snapshot = &roachpb.RaftSnapshotData{}
			}

			result, err := r.sha512(ctx, desc, snap, snapshot, cc.Mode, limiter)
			if err != nil {
				log.Errorf(ctx, "%v", err)
				result = nil
			}
			r.computeChecksumDone(ctx, cc.ChecksumID, result, snapshot)
		}()

		var shouldFatal bool
		for _, rDesc := range cc.Terminate {
			if rDesc.StoreID == r.store.StoreID() && rDesc.ReplicaID == r.mu.replicaID {
				shouldFatal = true
			}
		}

		if shouldFatal {
			// This node should fatal as a result of a previous consistency
			// check (i.e. this round is carried out only to obtain a diff).
			// If we fatal too early, the diff won't make it back to the lease-
			// holder and thus won't be printed to the logs. Since we're already
			// in a goroutine that's about to end, simply sleep for a few seconds
			// and then terminate.
			auxDir := r.store.engine.GetAuxiliaryDir()
			_ = r.store.engine.MkdirAll(auxDir)
			path := base.PreventedStartupFile(auxDir)

			preventStartupMsg := fmt.Sprintf(`ATTENTION:

this node is terminating because a replica inconsistency was detected between %s
and its other replicas. Please check your cluster-wide log files for more
information and contact the CockroachDB support team. It is not necessarily safe
to replace this node; cluster data may still be at risk of corruption.

A checkpoints directory to aid (expert) debugging should be present in:
%s

A file preventing this node from restarting was placed at:
%s
`, r, auxDir, path)

			if err := fs.WriteFile(r.store.engine, path, []byte(preventStartupMsg)); err != nil {
				log.Warningf(ctx, "%v", err)
			}

			if p := r.store.cfg.TestingKnobs.ConsistencyTestingKnobs.OnBadChecksumFatal; p != nil {
				p(*r.store.Ident)
			} else {
				time.Sleep(10 * time.Second)
				log.Fatalf(r.AnnotateCtx(context.Background()), preventStartupMsg)
			}
		}

	}); err != nil {
		defer snap.Close()
		log.Errorf(ctx, "could not run async checksum computation (ID = %s): %v", cc.ChecksumID, err)
		// Set checksum to nil.
		r.computeChecksumDone(ctx, cc.ChecksumID, nil, nil)
	}
}

// leasePostApply updates the Replica's internal state to reflect the
// application of a new Range lease. The method is idempotent, so it can be
// called repeatedly for the same lease safely. However, the method will panic
// if passed a lease with a lower sequence number than the current lease. By
// default, the method will also panic if passed a lease that indicates a
// forward sequence number jump (i.e. a skipped lease). This behavior can
// be disabled by passing permitJump as true.
func (r *Replica) leasePostApply(ctx context.Context, newLease roachpb.Lease, permitJump bool) {
	r.mu.Lock()
	replicaID := r.mu.replicaID
	// Pull out the last lease known to this Replica. It's possible that this is
	// not actually the last lease in the Range's lease sequence because the
	// Replica may have missed the application of a lease between prevLease and
	// newLease. However, this should only be possible if a snapshot includes a
	// lease update. All other forms of lease updates should be continuous
	// without jumps (see permitJump).
	prevLease := *r.mu.state.Lease
	r.mu.Unlock()

	iAmTheLeaseHolder := newLease.Replica.ReplicaID == replicaID
	// NB: in the case in which a node restarts, minLeaseProposedTS forces it to
	// get a new lease and we make sure it gets a new sequence number, thus
	// causing the right half of the disjunction to fire so that we update the
	// timestamp cache.
	leaseChangingHands := prevLease.Replica.StoreID != newLease.Replica.StoreID || prevLease.Sequence != newLease.Sequence

	if iAmTheLeaseHolder {
		// Log lease acquisition whenever an Epoch-based lease changes hands (or verbose
		// logging is enabled).
		if newLease.Type() == roachpb.LeaseEpoch && leaseChangingHands || log.V(1) {
			log.VEventf(ctx, 1, "new range lease %s following %s", newLease, prevLease)
		}
	}

	if leaseChangingHands && iAmTheLeaseHolder {
		// When taking over the lease, we need to check whether a merge is in
		// progress, as only the old leaseholder would have been explicitly notified
		// of the merge. If there is a merge in progress, maybeWatchForMerge will
		// arrange to block all traffic to this replica unless the merge aborts.
		if err := r.maybeWatchForMerge(ctx); err != nil {
			// We were unable to determine whether a merge was in progress. We cannot
			// safely proceed.
			log.Fatalf(ctx, "failed checking for in-progress merge while installing new lease %s: %s",
				newLease, err)
		}

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
		setTimestampCacheLowWaterMark(r.store.tsCache, r.Desc(), newLease.Start)

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
			log.Fatalf(ctx, "lease sequence inversion, prevLease=%s, newLease=%s",
				log.Safe(prevLease), log.Safe(newLease))
		case s2 == s1:
			// If the sequence numbers are the same, make sure they're actually
			// the same lease. This can happen when callers are using
			// leasePostApply for some of its side effects, like with
			// splitPostApply. It can also happen during lease extensions.
			if !prevLease.Equivalent(newLease) {
				log.Fatalf(ctx, "sequence identical for different leases, prevLease=%s, newLease=%s",
					log.Safe(prevLease), log.Safe(newLease))
			}
		case s2 == s1+1:
			// Lease sequence incremented by 1. Expected case.
		case s2 > s1+1 && !permitJump:
			log.Fatalf(ctx, "lease sequence jump, prevLease=%s, newLease=%s",
				log.Safe(prevLease), log.Safe(newLease))
		}
	}

	// Ordering is critical here. We only install the new lease after we've
	// checked for an in-progress merge and updated the timestamp cache. If the
	// ordering were reversed, it would be possible for requests to see the new
	// lease but not the updated merge or timestamp cache state, which can result
	// in serializability violations.
	r.mu.Lock()
	r.mu.state.Lease = &newLease
	expirationBasedLease := r.requiresExpiringLeaseRLocked()
	r.mu.Unlock()

	// Gossip the first range whenever its lease is acquired. We check to make
	// sure the lease is active so that a trailing replica won't process an old
	// lease request and attempt to gossip the first range.
	if leaseChangingHands && iAmTheLeaseHolder && r.IsFirstRange() && r.IsLeaseValid(newLease, r.store.Clock().Now()) {
		r.gossipFirstRange(ctx)
	}

	// Whenever we first acquire an expiration-based lease, notify the lease
	// renewer worker that we want it to keep proactively renewing the lease
	// before it expires.
	if leaseChangingHands && iAmTheLeaseHolder && expirationBasedLease && r.IsLeaseValid(newLease, r.store.Clock().Now()) {
		r.store.renewableLeases.Store(int64(r.RangeID), unsafe.Pointer(r))
		select {
		case r.store.renewableLeasesSignal <- struct{}{}:
		default:
		}
	}

	// If we're the current raft leader, may want to transfer the leadership to
	// the new leaseholder. Note that this condition is also checked periodically
	// when ticking the replica.
	r.maybeTransferRaftLeadership(ctx)

	// Notify the store that a lease change occurred and it may need to
	// gossip the updated store descriptor (with updated capacity).
	prevOwner := prevLease.OwnedBy(r.store.StoreID())
	currentOwner := newLease.OwnedBy(r.store.StoreID())
	if leaseChangingHands && (prevOwner || currentOwner) {
		if currentOwner {
			r.store.maybeGossipOnCapacityChange(ctx, leaseAddEvent)
		} else if prevOwner {
			r.store.maybeGossipOnCapacityChange(ctx, leaseRemoveEvent)
		}
		if r.leaseholderStats != nil {
			r.leaseholderStats.resetRequestCounts()
		}
	}

	// Inform the concurrency manager that the lease holder has been updated.
	r.concMgr.OnRangeLeaseUpdated(iAmTheLeaseHolder)

	// Potentially re-gossip if the range contains system data (e.g. system
	// config or node liveness). We need to perform this gossip at startup as
	// soon as possible. Trying to minimize how often we gossip is a fool's
	// errand. The node liveness info will be gossiped frequently (every few
	// seconds) in any case due to the liveness heartbeats. And the system config
	// will be gossiped rarely because it falls on a range with an epoch-based
	// range lease that is only reacquired extremely infrequently.
	if iAmTheLeaseHolder {
		if err := r.MaybeGossipSystemConfig(ctx); err != nil {
			log.Errorf(ctx, "%v", err)
		}
		if err := r.MaybeGossipNodeLiveness(ctx, keys.NodeLivenessSpan); err != nil {
			log.Errorf(ctx, "%v", err)
		}

		// Emit an MLAI on the leaseholder replica, as follower will be looking
		// for one and if we went on to quiesce, they wouldn't necessarily get
		// one otherwise (unless they ask for it, which adds latency).
		r.EmitMLAI()

		if leaseChangingHands && log.V(1) {
			// This logging is useful to troubleshoot incomplete drains.
			log.Info(ctx, "is now leaseholder")
		}
	}

	// Mark the new lease in the replica's lease history.
	if r.leaseHistory != nil {
		r.leaseHistory.add(newLease)
	}
}

func addSSTablePreApply(
	ctx context.Context,
	st *cluster.Settings,
	eng storage.Engine,
	sideloaded SideloadStorage,
	term, index uint64,
	sst kvserverpb.ReplicatedEvalResult_AddSSTable,
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

	path, err := sideloaded.Filename(ctx, index, term)
	if err != nil {
		log.Fatalf(ctx, "sideloaded SSTable at term %d, index %d is missing", term, index)
	}

	eng.PreIngestDelay(ctx)

	copied := false
	if eng.InMem() {
		path = fmt.Sprintf("%x", checksum)
		if err := eng.WriteFile(path, sst.Data); err != nil {
			panic(err)
		}
	} else {
		ingestPath := path + ".ingested"

		canLinkToRaftFile := false
		// The SST may already be on disk, thanks to the sideloading mechanism.  If
		// so we can try to add that file directly, via a new hardlink if the file-
		// system support it, rather than writing a new copy of it. However, this is
		// only safe if we can do so without modifying the file since it is still
		// part of an immutable raft log message, but in some cases, described in
		// DBIngestExternalFile, RocksDB would modify the file. Fortunately we can
		// tell Rocks that it is not allowed to modify the file, in which case it
		// will return and error if it would have tried to do so, at which point we
		// can fall back to writing a new copy for Rocks to ingest.
		if _, links, err := sysutil.StatAndLinkCount(path); err == nil {
			// HACK: RocksDB does not like ingesting the same file (by inode) twice.
			// See facebook/rocksdb#5133. We can tell that we have tried to ingest
			// this file already if it has more than one link – one from the file raft
			// wrote and one from rocks. In that case, we should not try to give
			// rocks a link to the same file again.
			if links == 1 {
				canLinkToRaftFile = true
			} else {
				log.Warningf(ctx, "SSTable at index %d term %d may have already been ingested (link count %d) -- falling back to ingesting a copy",
					index, term, links)
			}
		}

		if canLinkToRaftFile {
			// If the fs supports it, make a hard-link for rocks to ingest. We cannot
			// pass it the path in the sideload store as it deletes the passed path on
			// success.
			if linkErr := eng.Link(path, ingestPath); linkErr == nil {
				ingestErr := eng.IngestExternalFiles(ctx, []string{ingestPath})
				if ingestErr == nil {
					// Adding without modification succeeded, no copy necessary.
					log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, ingestPath)
					return false
				}
				if rmErr := eng.Remove(ingestPath); rmErr != nil {
					log.Fatalf(ctx, "failed to move ingest sst: %v", rmErr)
				}
				const seqNoMsg = "Global seqno is required, but disabled"
				const seqNoOnReIngest = "external file have non zero sequence number"
				// Repeated ingestion is still possible even with the link count checked
				// above, since rocks might have already compacted away the file.
				// However it does not flush compacted files from its cache, so it can
				// still react poorly to attempting to ingest again. If we get an error
				// that indicates we can't ingest, we'll make a copy and try again. That
				// attempt must succeed or we'll fatal, so any persistent error is still
				// going to be surfaced.
				ingestErrMsg := ingestErr.Error()
				isSeqNoErr := strings.Contains(ingestErrMsg, seqNoMsg) || strings.Contains(ingestErrMsg, seqNoOnReIngest)
				if ingestErr := (*storage.Error)(nil); !errors.As(err, &ingestErr) || !isSeqNoErr {
					log.Fatalf(ctx, "while ingesting %s: %v", ingestPath, ingestErr)
				}
			}
		}

		path = ingestPath

		log.Eventf(ctx, "copying SSTable for ingestion at index %d, term %d: %s", index, term, path)

		// TODO(tschottdorf): remove this once sideloaded storage guarantees its
		// existence.
		if err := eng.MkdirAll(filepath.Dir(path)); err != nil {
			panic(err)
		}
		if _, err := eng.Stat(path); err == nil {
			// The file we want to ingest exists. This can happen since the
			// ingestion may apply twice (we ingest before we mark the Raft
			// command as committed). Just unlink the file (RocksDB created a
			// hard link); after that we're free to write it again.
			if err := eng.Remove(path); err != nil {
				log.Fatalf(ctx, "while removing existing file during ingestion of %s: %+v", path, err)
			}
		}

		if err := writeFileSyncing(ctx, path, sst.Data, eng, 0600, st, limiter); err != nil {
			log.Fatalf(ctx, "while ingesting %s: %+v", path, err)
		}
		copied = true
	}

	if err := eng.IngestExternalFiles(ctx, []string{path}); err != nil {
		log.Fatalf(ctx, "while ingesting %s: %+v", path, err)
	}
	log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, path)
	return copied
}

func (r *Replica) handleReadWriteLocalEvalResult(ctx context.Context, lResult result.LocalResult) {
	// Fields for which no action is taken in this method are zeroed so that
	// they don't trigger an assertion at the end of the method (which checks
	// that all fields were handled).
	{
		lResult.Reply = nil
	}

	// The caller is required to detach and handle the following three fields.
	if lResult.EncounteredIntents != nil {
		log.Fatalf(ctx, "LocalEvalResult.EncounteredIntents should be nil: %+v", lResult.EncounteredIntents)
	}
	if lResult.EndTxns != nil {
		log.Fatalf(ctx, "LocalEvalResult.EndTxns should be nil: %+v", lResult.EndTxns)
	}
	if lResult.MaybeWatchForMerge {
		log.Fatalf(ctx, "LocalEvalResult.MaybeWatchForMerge should be false")
	}

	if lResult.AcquiredLocks != nil {
		for i := range lResult.AcquiredLocks {
			r.concMgr.OnLockAcquired(ctx, &lResult.AcquiredLocks[i])
		}
		lResult.AcquiredLocks = nil
	}

	if lResult.ResolvedLocks != nil {
		for i := range lResult.ResolvedLocks {
			r.concMgr.OnLockUpdated(ctx, &lResult.ResolvedLocks[i])
		}
		lResult.ResolvedLocks = nil
	}

	if lResult.UpdatedTxns != nil {
		for _, txn := range lResult.UpdatedTxns {
			r.concMgr.OnTransactionUpdated(ctx, txn)
		}
		lResult.UpdatedTxns = nil
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
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().Now())
		lResult.MaybeAddToSplitQueue = false
	}

	if lResult.MaybeGossipSystemConfig {
		if err := r.MaybeGossipSystemConfig(ctx); err != nil {
			log.Errorf(ctx, "%v", err)
		}
		lResult.MaybeGossipSystemConfig = false
	}

	if lResult.MaybeGossipSystemConfigIfHaveFailure {
		if err := r.MaybeGossipSystemConfigIfHaveFailure(ctx); err != nil {
			log.Errorf(ctx, "%v", err)
		}
		lResult.MaybeGossipSystemConfigIfHaveFailure = false
	}

	if lResult.MaybeGossipNodeLiveness != nil {
		if err := r.MaybeGossipNodeLiveness(ctx, *lResult.MaybeGossipNodeLiveness); err != nil {
			log.Errorf(ctx, "%v", err)
		}
		lResult.MaybeGossipNodeLiveness = nil
	}

	if lResult.Metrics != nil {
		r.store.metrics.handleMetricsResult(ctx, *lResult.Metrics)
		lResult.Metrics = nil
	}

	if !lResult.IsZero() {
		log.Fatalf(ctx, "unhandled field in LocalEvalResult: %s", pretty.Diff(lResult, result.LocalResult{}))
	}
}

// proposalResult indicates the result of a proposal. Exactly one of
// Reply and Err is set, and it represents the result of the proposal.
type proposalResult struct {
	Reply              *roachpb.BatchResponse
	Err                *roachpb.Error
	EncounteredIntents []roachpb.Intent
	EndTxns            []result.EndTxnIntents
}

// evaluateProposal generates a Result from the given request by
// evaluating it, returning both state which is held only on the
// proposer and that which is to be replicated through Raft. The
// return value is ready to be inserted into Replica's proposal map
// and subsequently passed to submitProposalLocked.
//
// The method also returns a flag indicating if the request needs to
// be proposed through Raft and replicated. This flag will be false
// either if the request was a no-op or if it hit an error. In this
// case, the result can be sent directly back to the client without
// going through Raft, but while still handling LocalEvalResult.
//
// Replica.mu must not be held.
func (r *Replica) evaluateProposal(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (*result.Result, bool, *roachpb.Error) {
	if ba.Timestamp == (hlc.Timestamp{}) {
		return nil, false, roachpb.NewErrorf("can't propose Raft command with zero timestamp")
	}

	// Evaluate the commands. If this returns without an error, the batch should
	// be committed. Note that we don't hold any locks at this point. This is
	// important since evaluating a proposal is expensive.
	// TODO(tschottdorf): absorb all returned values in `res` below this point
	// in the call stack as well.
	batch, ms, br, res, pErr := r.evaluateWriteBatch(ctx, idKey, ba, latchSpans)

	// Note: reusing the proposer's batch when applying the command on the
	// proposer was explored as an optimization but resulted in no performance
	// benefit.
	if batch != nil {
		defer batch.Close()
	}

	if pErr != nil {
		pErr = r.maybeSetCorrupt(ctx, pErr)

		txn := pErr.GetTxn()
		if txn != nil && ba.Txn == nil {
			log.Fatalf(ctx, "error had a txn but batch is non-transactional. Err txn: %s", txn)
		}

		// Failed proposals can't have any Result except for what's
		// allowlisted here.
		res.Local = result.LocalResult{
			EncounteredIntents: res.Local.DetachEncounteredIntents(),
			EndTxns:            res.Local.DetachEndTxns(true /* alwaysOnly */),
			Metrics:            res.Local.Metrics,
		}
		res.Replicated.Reset()
		return &res, false /* needConsensus */, pErr
	}

	// Set the local reply, which is held only on the proposing replica and is
	// returned to the client after the proposal completes, or immediately if
	// replication is not necessary.
	res.Local.Reply = br

	// needConsensus determines if the result needs to be replicated and
	// proposed through Raft. This is necessary if at least one of the
	// following conditions is true:
	// 1. the request created a non-empty write batch.
	// 2. the request had an impact on the MVCCStats. NB: this is possible
	//    even with an empty write batch when stats are recomputed.
	// 3. the request has replicated side-effects.
	needConsensus := !batch.Empty() ||
		ms != (enginepb.MVCCStats{}) ||
		!res.Replicated.Equal(kvserverpb.ReplicatedEvalResult{})

	if needConsensus {
		// Set the proposal's WriteBatch, which is the serialized representation of
		// the proposals effect on RocksDB.
		res.WriteBatch = &kvserverpb.WriteBatch{
			Data: batch.Repr(),
		}

		// Set the proposal's replicated result, which contains metadata and
		// side-effects that are to be replicated to all replicas.
		res.Replicated.IsLeaseRequest = ba.IsLeaseRequest()
		res.Replicated.Timestamp = ba.Timestamp
		res.Replicated.Delta = ms.ToStatsDelta()

		_ = clusterversion.VersionContainsEstimatesCounter // see for info on ContainsEstimates migration
		if r.ClusterSettings().Version.IsActive(ctx, clusterversion.VersionContainsEstimatesCounter) {
			// Encode that this command (and any that follow) uses regular arithmetic for ContainsEstimates
			// by making sure ContainsEstimates is > 1.
			// This will be interpreted during command application.
			if res.Replicated.Delta.ContainsEstimates > 0 {
				res.Replicated.Delta.ContainsEstimates *= 2
			}
		} else {
			// This range may still need to have its commands processed by nodes which treat ContainsEstimates
			// as a bool, so clamp it to {0,1}. This enables use of bool semantics in command application.
			if res.Replicated.Delta.ContainsEstimates > 1 {
				res.Replicated.Delta.ContainsEstimates = 1
			} else if res.Replicated.Delta.ContainsEstimates < 0 {
				// The caller should have checked the cluster version. At the
				// time of writing, this is only RecomputeStats and the split
				// trigger, which both have the check, but better safe than sorry.
				log.Fatalf(ctx, "cannot propose negative ContainsEstimates "+
					"without VersionContainsEstimatesCounter in %s", ba.Summary())
			}
		}

		// If the RangeAppliedState key is not being used and the cluster version is
		// high enough to guarantee that all current and future binaries will
		// understand the key, we send the migration flag through Raft. Because
		// there is a delay between command proposal and application, we may end up
		// setting this migration flag multiple times. This is ok, because the
		// migration is idempotent.
		// TODO(nvanbenschoten): This will be baked in to 2.1, so it can be removed
		// in the 2.2 release.
		r.mu.RLock()
		usingAppliedStateKey := r.mu.state.UsingAppliedStateKey
		r.mu.RUnlock()
		if !usingAppliedStateKey {
			// The range applied state was introduced in v2.1. It's possible to
			// still find ranges that haven't activated it. If so, activate it.
			// We can remove this code if we introduce a boot-time check that
			// fails the startup process when any legacy replicas are found. The
			// operator can then run the old binary for a while to upgrade the
			// stragglers.
			if res.Replicated.State == nil {
				res.Replicated.State = &kvserverpb.ReplicaState{}
			}
			res.Replicated.State.UsingAppliedStateKey = true
		}
	}

	return &res, needConsensus, nil
}

// requestToProposal converts a BatchRequest into a ProposalData, by
// evaluating it. The returned ProposalData is partially valid even
// on a non-nil *roachpb.Error and should be proposed through Raft
// if ProposalData.command is non-nil.
//
// TODO(nvanbenschoten): combine idKey, ba, and latchSpans into a
// `serializedRequest` struct.
func (r *Replica) requestToProposal(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (*ProposalData, *roachpb.Error) {
	res, needConsensus, pErr := r.evaluateProposal(ctx, idKey, ba, latchSpans)

	// Fill out the results even if pErr != nil; we'll return the error below.
	proposal := &ProposalData{
		ctx:     ctx,
		idKey:   idKey,
		doneCh:  make(chan proposalResult, 1),
		Local:   &res.Local,
		Request: ba,
	}

	if needConsensus {
		proposal.command = &kvserverpb.RaftCommand{
			ReplicatedEvalResult: res.Replicated,
			WriteBatch:           res.WriteBatch,
			LogicalOpLog:         res.LogicalOpLog,
			TraceData:            r.getTraceData(ctx),
		}
	}

	return proposal, pErr
}

// getTraceData extracts the SpanContext of the current span.
func (r *Replica) getTraceData(ctx context.Context) opentracing.TextMapCarrier {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return nil
	}
	if tracing.IsBlackHoleSpan(sp) {
		return nil
	}
	traceData := opentracing.TextMapCarrier{}
	if err := r.AmbientContext.Tracer.Inject(
		sp.Context(), opentracing.TextMap, traceData,
	); err != nil {
		log.Errorf(ctx, "failed to inject sp context (%+v) as trace data: %s", sp.Context(), err)
		return nil
	}
	return traceData
}
