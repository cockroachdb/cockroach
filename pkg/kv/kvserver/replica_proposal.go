// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/redact"
	"github.com/kr/pretty"
	"golang.org/x/time/rate"
)

// ProposalData is data about a command which allows it to be
// evaluated, proposed to raft, and for the result of the command to
// be returned to the caller.
//
// A proposal (=ProposalData) is created during write request evaluation and
// then handed to the proposal buffer (Replica.mu.proposalBuf) via
// propBuf.Insert. In the common case, the proposal transfers from there into
// Replica.mu.proposals (and the raft log) via propBuf.FlushLockedWithRaftGroup
// (called from handleRaftReady). But if the proposal buffer is full, Insert may
// also flush the buffer to make room for a new proposal.
//
//	       	 ┌──────►created
//	         │         │
//	         │         │[1]
//	         │         ▼
//	  new    │      propBuf
//	proposal │         │
//	   [5]   │         │[2]
//	         │         ▼       [4]
//	         │    proposal map───►proposal map
//	         │         │          and propBuf
//	         │         │[3]            │
//	         │         │               │
//	         │         ▼               │[4]
//	         └──apply goroutine◄───────┘
//	                   │
//	                   ▼
//	                finished
//
// [1]: `(*Replica).propose` calls `(*proposalBuf).Insert`
// [2]: `(*proposalBuf).FlushLockedWithRaftGroup` on the `handleRaftReady`
// goroutine, or `(*proposalBuf).Insert` for another command on a full
// `proposalBuf`, or `FlushLockedWithoutProposing` (less interesting).
// [3]: picked up by `(*replicaDecoder).retrieveLocalProposals` when the entry
// comes up for application. This atomically removes from r.my.proposals and
// sets the `seenDuringApplication` field, which prevents situations in which
// the proposal may otherwise re-enter the proposal buffer and from there the
// proposals map, despite already having finished. (The exact mechanism drops
// the proposal when it would otherwise be flushed into the map since that is
// more reliable, but the diagram omits this detail).
// [4]: if not being observed in log application for some time,
// `refreshProposalsLocked` re-submits to the proposal buffer, which will result
// in an (identical) copy of the proposal being added to the log. (All but the
// first copy that will be applied will not be associated with a proposal).
// [5]: if the entry applies under an already-consumed LeaseAppliedIndex,
// tryReproposeWithNewLeaseIndexRaftMuLocked creates a *new* proposal (which inherits the
// waiting caller, latches, etc) and the cycle begins again whereas the current
// proposal results in an error (which nobody is listening to).
//
// The access rules for this field are as follows:
//   - if the proposal has not yet been passed successfully to Replica.propose,
//     can access freely on the originating goroutine.
//   - otherwise, once the proposal has been seen in `retrieveLocalProposals`
//     and removed from r.mu.proposals, can access freely under raftMu.
//   - otherwise, must hold r.mu across the access.
//
// This reflects the lifecycle of the proposal, which enters first the proposal
// buffer (guarded by r.mu) and then is flushed into the proposals map, from
// which it is consumed during log application (which holds raftMu).
type ProposalData struct {
	// The caller's context, used for logging proposals, reproposals, message
	// sends, but not command application.
	//
	// This is either the caller's context (if they are waiting for the result)
	// or a "background" context, perhaps with a span in it (for async consensus
	// or in case the caller has given up).
	//
	// Note that there is also replicatedCmd.{ctx,sp} and so confusion may arise
	// about which one to log to. Generally if p.ctx has a span, replicatedCmd.ctx
	// has a span that follows from it. However, if p.ctx has no span or the
	// replicatedCmd is not associated to a local ProposalData, replicatedCmd.ctx
	// may still have a span, if the remote proposer requested tracing. It follows
	// that during command application one should always use `replicatedCmd.ctx`
	// for best coverage. `p.ctx` should be used when a `replicatedCmd` is not in
	// scope, i.e. outside of raft command application.
	//
	// The context may be updated during the proposal lifecycle but will never
	// be nil. To clear out the context, set it to context.Background().  It is
	// protected by an atomic pointer because it can be read without holding the
	// raftMu. Use ProposalData.Context() to read it.
	//
	// TODO(baptist): Track down all the places where we read and write ctx and
	// determine whether we can convert this back to non-atomic field.
	ctx atomic.Pointer[context.Context]

	// An optional tracing span bound to the proposal in the case of async
	// consensus (it will be referenced by p.ctx). We need to finish this span
	// after applying this proposal, since we created it. It is not used for
	// anything else (all tracing goes through `p.ctx`).
	sp *tracing.Span

	// idKey uniquely identifies this proposal. Immutable.
	idKey kvserverbase.CmdIDKey

	// proposedAtTicks is the (logical) time at which this command was
	// last (re-)proposed.
	proposedAtTicks int

	// createdAtTicks is the (logical) time at which this command was
	// *first* proposed.
	createdAtTicks int

	// command is the log entry that is encoded into encodedCommand and proposed
	// to raft. Never mutated.
	command *kvserverpb.RaftCommand

	// encodedCommand is the encoded Raft command, with an optional prefix
	// containing the command ID, depending on the raftlog.EntryEncoding.
	// Immutable.
	encodedCommand []byte

	// quotaAlloc is the allocation retrieved from the proposalQuota. The quota is
	// released when the command comes up for application (even if it will be
	// reproposed). See retrieveLocalProposals and tryReproposeWithNewLeaseIndexRaftMuLocked.
	quotaAlloc *quotapool.IntAlloc

	// ec.done is called after command application to update the timestamp
	// cache and optionally release latches and exits lock wait-queues.
	//
	// TODO(repl): the contract about when this is called is a bit muddy. In
	// particular, it seems that this can be called multiple times. We ought to
	// work towards a world where it's exactly once. A similar question applies
	// to endCmds.poison.
	ec endCmds

	// applied is set when the a command finishes application. It is a remnant of
	// an earlier version of tryReproposeWithNewLeaseIndexRaftMuLocked that has yet to be
	// phased out.
	//
	// TODO(repl): phase this field out.
	applied bool

	// doneCh is used to signal the waiting RPC handler (the contents of
	// proposalResult come from LocalEvalResult).
	//
	// Attention: this channel is not to be signaled directly downstream of Raft.
	// Always use ProposalData.finishApplication().
	doneCh chan proposalResult

	// Local contains the results of evaluating the request tying the upstream
	// evaluation of the request to the downstream application of the command.
	Local *result.LocalResult

	// Request is the client's original BatchRequest.
	Request *kvpb.BatchRequest

	// leaseStatus represents the lease under which the Request was evaluated and
	// under which this proposal is being made. For lease requests, this is the
	// previous lease that the requester was aware of.
	leaseStatus kvserverpb.LeaseStatus

	// tok identifies the request to the propBuf. Once the proposal is made, the
	// token will be used to stop tracking this request.
	tok TrackedRequestToken

	// raftAdmissionMeta captures the metadata we encode as part of the command
	// when first proposed for replication admission control.
	raftAdmissionMeta *kvflowcontrolpb.RaftAdmissionMeta

	// v2SeenDuringApplication is set to true right at the very beginning of
	// processing this proposal for application (regardless of what the outcome of
	// application is). This flag makes sure that the proposal buffer won't
	// accidentally reinsert a finished proposal into the map.
	v2SeenDuringApplication bool

	// seedProposal points at the seed proposal in the chain of (re-)proposals, if
	// this ProposalData is a reproposal. The field is nil for the seed proposal.
	seedProposal *ProposalData
	// lastReproposal is the last proposal that superseded the seed proposal.
	// Superseding proposals form a chain starting from the seed proposal. This
	// field is set only on the seed proposal, and is updated every time a new
	// reproposal is cast.
	//
	// See Replica.mu.proposals comment for more details.
	//
	// TODO(pavelkalinnikov): We are referencing only the last reproposal in the
	// chain, so that the intermediate ones can be GCed in case the chain is very
	// long. This chain reasoning is subtle, we should decompose ProposalData in
	// such a way that the "common" parts of the (re-)proposals are shared and
	// chaining isn't necessary.
	lastReproposal *ProposalData
}

// Context returns the context associated with the proposal. The context may
// change during the lifetime of the proposal.
func (proposal *ProposalData) Context() context.Context {
	return *proposal.ctx.Load()
}

// useReplicationAdmissionControl indicates whether this raft command should
// be subject to replication admission control.
func (proposal *ProposalData) useReplicationAdmissionControl() bool {
	return proposal.raftAdmissionMeta != nil
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
		proposal.sp.Finish()
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
//
// TODO(tbg): a stricter invariant should hold: if a proposal is signaled
// multiple times, at most one of them is not an AmbiguousResultError. In
// other words, we get at most one result from log application, all other
// results are from mechanisms that unblock the client despite not knowing
// the outcome of the proposal.
func (proposal *ProposalData) signalProposalResult(pr proposalResult) {
	if proposal.doneCh != nil {
		proposal.doneCh <- pr
		proposal.doneCh = nil
		// Need to remove any span from the proposal, as the signalled caller
		// will likely finish it, and if we then end up applying this proposal
		// we'll try to make a ChildSpan off `proposal.ctx` and this will
		// trigger the Span use-after-finish assertions.
		//
		// See: https://github.com/cockroachdb/cockroach/pull/76858#issuecomment-1048179588
		//
		// NB: `proposal.ec.repl` might already have been cleared if we arrive here
		// through finishApplication.
		ctx := context.Background()
		proposal.ctx.Store(&ctx)
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

// leaseJumpOption controls what assertions leasePostApplyLocked can make.
type leaseJumpOption bool

const (
	// assertNoLeaseJump means that the new lease must follow the old lease, with
	// no gaps in the sequence number.
	assertNoLeaseJump leaseJumpOption = false
	// allowLeaseJump meanms that sequence number gaps must be tolerated. This is
	// used when we've found out about the new lease through a snapshot and we
	// don't know what other previous leases we haven't applied.
	allowLeaseJump = true
)

// leasePostApplyLocked updates the Replica's internal state to reflect the
// application of a new Range lease. The method is idempotent, so it can be
// called repeatedly for the same lease safely. However, the method will panic
// if newLease has a lower sequence number than the current lease. Depending on
// jumpOpt, we'll also panic if newLease indicates a forward sequence number
// jump compared to prevLease (i.e. a skipped lease).
//
// prevLease represents the most recent lease this replica was aware of before
// newLease came along. This is usually (but not necessarily) the latest lease
// ever applied to the range. However, there's also the case when the replica
// found out about newLease through a snapshot; in this case the replica might
// not be aware of other lease changes that happened before the snapshot was
// generated. This method thus tolerates prevLease being "stale" when
// allowLeaseJump is passed. prevLease can also be the same as newLease; see
// below.
//
// newLease represents the lease being applied. Can be the same as prevLease.
// This allows leasePostApplyLocked to be called for some of its side-effects
// even if the lease in question has otherwise already been applied to the
// range.
//
// In addition to the leases, the method accepts a summary of the reads served
// on the range by prior leaseholders. This can be used by the new leaseholder
// to ensure that no future writes are allowed to invalidate prior reads. If a
// summary is not provided, the method pessimistically assumes that prior
// leaseholders served reads all the way up to the start of the new lease.
func (r *Replica) leasePostApplyLocked(
	ctx context.Context,
	prevLease, newLease *roachpb.Lease,
	priorReadSum *rspb.ReadSummary,
	jumpOpt leaseJumpOption,
) {
	// Note that we actually install the lease further down in this method.
	// Everything we do before then doesn't need to worry about requests being
	// evaluated under the new lease.

	// Sanity check to make sure that the lease sequence is moving in the right
	// direction.
	if s1, s2 := prevLease.Sequence, newLease.Sequence; s1 != 0 {
		// We're at a version that supports lease sequence numbers.
		switch {
		case s2 < s1:
			log.Fatalf(ctx, "lease sequence inversion, prevLease=%s, newLease=%s",
				redact.Safe(prevLease), redact.Safe(newLease))
		case s2 == s1:
			// If the sequence numbers are the same, make sure they're actually
			// the same lease. This can happen when callers are using
			// leasePostApply for some of its side effects, like with
			// splitPostApply. It can also happen during lease extensions.
			//
			// NOTE: we pass true for expToEpochEquiv because we may be in a cluster
			// where some node has detected the version upgrade and is considering
			// this lease type promotion to be valid, even if our local node has not
			// yet detected the upgrade. Passing true broadens the definition of
			// equivalence and weakens the assertion.
			if !prevLease.Equivalent(*newLease, true /* expToEpochEquiv */) {
				log.Fatalf(ctx, "sequence identical for different leases, prevLease=%s, newLease=%s",
					redact.Safe(prevLease), redact.Safe(newLease))
			}
		case s2 == s1+1:
			// Lease sequence incremented by 1. Expected case.
		case s2 > s1+1 && jumpOpt == assertNoLeaseJump:
			log.Fatalf(ctx, "lease sequence jump, prevLease=%s, newLease=%s",
				redact.Safe(prevLease), redact.Safe(newLease))
		}
	}

	iAmTheLeaseHolder := newLease.Replica.ReplicaID == r.replicaID
	// NB: in the case in which a node restarts, minLeaseProposedTS forces it to
	// get a new lease and we make sure it gets a new sequence number, thus
	// causing the right half of the disjunction to fire so that we update the
	// timestamp cache.
	leaseChangingHands := prevLease.Replica.StoreID != newLease.Replica.StoreID || prevLease.Sequence != newLease.Sequence

	if iAmTheLeaseHolder {
		// Log lease acquisitions loudly when verbose logging is enabled or when the
		// new leaseholder is draining, in which case it should be shedding leases.
		// Otherwise, log a trace event.
		if log.V(1) || (leaseChangingHands && r.store.IsDraining()) {
			log.Infof(ctx, "new range lease %s following %s", newLease, prevLease)
		} else {
			log.Eventf(ctx, "new range lease %s following %s", newLease, prevLease)
		}
	}

	if leaseChangingHands && iAmTheLeaseHolder {
		// When taking over the lease, we need to check whether a merge is in
		// progress, as only the old leaseholder would have been explicitly notified
		// of the merge. If there is a merge in progress, maybeWatchForMerge will
		// arrange to block all traffic to this replica unless the merge aborts.
		if _, err := r.maybeWatchForMergeLocked(ctx); err != nil {
			// We were unable to determine whether a merge was in progress. We cannot
			// safely proceed.
			log.Fatalf(ctx, "failed checking for in-progress merge while installing new lease %s: %s",
				newLease, err)
		}

		// Forward the node clock to the start time of the new lease. This ensures
		// that the leaseholder's clock always leads its lease's start time. For an
		// explanation about why this is needed, see "Cooperative lease transfers"
		// in pkg/util/hlc/doc.go.
		r.Clock().Update(newLease.Start)

		// As a result of moving the lease, update the minimum valid observed
		// timestamp so that times before the lease start time are no longer
		// respected. The observed timestamp on transactions refer to this node's
		// clock. In range merges or lease transfers, a node becomes a leaseholder
		// for data that it previously did not own and the transaction observed
		// timestamp is no longer valid, so ignore observed timestamps before this
		// time.
		r.mu.minValidObservedTimestamp.Forward(newLease.Start)

		// If this replica is a new holder of the lease, update the timestamp
		// cache. Note that clock offset scenarios are handled via a stasis
		// period inherent in the lease which is documented in the Lease struct.
		//
		// If the Raft entry included a prior read summary then we can use that
		// directly to update the timestamp cache. Otherwise, we pessimistically
		// assume that prior leaseholders served reads all the way up to the
		// start of the new lease.
		//
		// The introduction of lease transfers implies that the previous lease
		// may have been shortened and we are now applying a formally
		// overlapping lease (since the old lease holder has promised not to
		// serve any more requests, this is kosher). This means that we don't
		// use the old lease's expiration but instead use the new lease's start
		// to initialize the timestamp cache low water.
		var sum rspb.ReadSummary
		if priorReadSum != nil {
			sum = *priorReadSum
		} else {
			sum = rspb.FromTimestamp(newLease.Start.ToTimestamp())
		}
		applyReadSummaryToTimestampCache(ctx, r.store.tsCache, r.descRLocked(), sum)

		// Reset the request counts used to make lease placement decisions and
		// load-based splitting/merging decisions whenever starting a new lease.
		if r.loadStats != nil {
			r.loadStats.Reset()
		}
		r.loadBasedSplitter.Reset(r.Clock().PhysicalTime())
	}

	// Inform the concurrency manager that the lease holder has been updated.
	// We do this before installing the new lease in `r.mu.state` as we have
	// an invariant that any replica with a lease has the concurrency manager
	// enabled. (In practice, since both happen under `r.mu`, it is likely
	// to not matter).
	r.concMgr.OnRangeLeaseUpdated(newLease.Sequence, iAmTheLeaseHolder)

	// Inform the propBuf about the new lease so that it can initialize its closed
	// timestamp tracking.
	r.mu.proposalBuf.OnLeaseChangeLocked(iAmTheLeaseHolder,
		r.shMu.state.RaftClosedTimestamp, r.shMu.state.LeaseAppliedIndex)

	// Ordering is critical here. We only install the new lease after we've
	// checked for an in-progress merge and updated the timestamp cache. If the
	// ordering were reversed, it would be possible for requests to see the new
	// lease but not the updated merge or timestamp cache state, which can result
	// in serializability violations.
	r.shMu.state.Lease = newLease

	now := r.store.Clock().NowAsClockTimestamp()

	// Gossip the first range whenever its lease is acquired. We check to make
	// sure the lease is active so that a trailing replica won't process an old
	// lease request and attempt to gossip the first range.
	if leaseChangingHands && iAmTheLeaseHolder && r.IsFirstRange() && r.ownsValidLeaseRLocked(ctx, now) {
		r.gossipFirstRangeLocked(ctx)
	}

	// Log the lease acquisition, if appropriate.
	if leaseChangingHands && iAmTheLeaseHolder {
		r.maybeLogLeaseAcquisition(ctx, now, prevLease, newLease)
	}

	st := r.leaseStatusAtRLocked(ctx, now)
	if leaseChangingHands && newLease.Type() == roachpb.LeaseExpiration &&
		r.ownsValidLeaseRLocked(ctx, now) && !r.shouldUseExpirationLeaseRLocked() {
		// We've received and applied an expiration lease for a range that shouldn't
		// keep using it, most likely as part of a lease transfer (which is always
		// expiration-based). The lease is also still valid. Upgrade this lease to
		// the more efficient epoch or leader lease.
		if log.V(1) {
			log.VEventf(ctx, 1, "upgrading expiration lease %s to an epoch/leader lease", newLease)
		}

		if r.store.TestingKnobs().LeaseUpgradeInterceptor != nil {
			r.store.TestingKnobs().LeaseUpgradeInterceptor(newLease)
		}
		// Ignore the returned handle as we won't block on it.
		_ = r.requestLeaseLocked(ctx, st, nil /* limiter */)
	}

	// If we're the current raft leader, may want to transfer the leadership to
	// the new leaseholder. Note that this condition is also checked periodically
	// when ticking the replica.
	r.maybeTransferRaftLeadershipToLeaseholderLocked(ctx, st)

	// Notify the store that a lease change occurred and it may need to
	// gossip the updated store descriptor (with updated capacity).
	prevOwner := prevLease.OwnedBy(r.store.StoreID())
	currentOwner := newLease.OwnedBy(r.store.StoreID())
	if leaseChangingHands && (prevOwner || currentOwner) {
		if currentOwner {
			r.store.storeGossip.MaybeGossipOnCapacityChange(ctx, LeaseAddEvent)
		} else if prevOwner {
			r.store.storeGossip.MaybeGossipOnCapacityChange(ctx, LeaseRemoveEvent)
		}
		if r.loadStats != nil {
			r.loadStats.Reset()
		}
	}

	// Potentially re-gossip if the range contains system data (e.g. system
	// config or node liveness). We need to perform this gossip at startup as
	// soon as possible. Trying to minimize how often we gossip is a fool's
	// errand. The node liveness info will be gossiped frequently (every few
	// seconds) in any case due to the liveness heartbeats. And the system config
	// will be gossiped rarely because it falls on a range with an epoch-based
	// range lease that is only reacquired extremely infrequently.
	//
	// TODO(erikgrinaker): This and MaybeGossipNodeLivenessRaftMuLocked should
	// check whether the replica intersects the liveness span rather than contains
	// the entirety of it.
	if iAmTheLeaseHolder && kvserverbase.ContainsKeyRange(
		r.descRLocked(), keys.NodeLivenessSpan.Key, keys.NodeLivenessSpan.EndKey) {
		// NB: run these in an async task to keep them out of the critical section
		// (r.mu is held here).
		ctx := r.AnnotateCtx(context.Background())
		_ = r.store.stopper.RunAsyncTask(ctx, "lease-gossip", func(ctx context.Context) {
			// Re-acquire the raftMu, as we are now in an async task.
			r.raftMu.Lock()
			defer r.raftMu.Unlock()
			if _, err := r.IsDestroyed(); err != nil {
				// Nothing to do.
				return
			}
			if err := r.MaybeGossipNodeLivenessRaftMuLocked(ctx, keys.NodeLivenessSpan); err != nil {
				log.Errorf(ctx, "%v", err)
			}
		})
	}

	// If we acquired a lease, and it violates the lease preferences, enqueue it
	// in the replicate queue. NOTE: We don't check whether the lease is valid,
	// it is possible that the lease being applied is invalid due to replication
	// lag, or previously needing a snapshot. The replicate queue will ensure the
	// lease is valid and owned by the replica before processing.
	if iAmTheLeaseHolder && leaseChangingHands &&
		LeaseCheckPreferencesOnAcquisitionEnabled.Get(&r.store.cfg.Settings.SV) {
		preferenceStatus := CheckStoreAgainstLeasePreferences(r.store.StoreID(), r.store.Attrs(),
			r.store.nodeDesc.Attrs, r.store.nodeDesc.Locality, r.mu.conf.LeasePreferences)
		switch preferenceStatus {
		case LeasePreferencesOK:
		case LeasePreferencesViolating:
			log.VEventf(ctx, 2,
				"acquired lease violates lease preferences, enqueuing for transfer [lease=%v preferences=%v]",
				newLease, r.mu.conf.LeasePreferences)
			r.store.leaseQueue.AddAsync(ctx, r, allocatorimpl.TransferLeaseForPreferences.Priority())
		case LeasePreferencesLessPreferred:
			// Enqueue the replica at a slightly lower priority than violation to
			// process the lease transfer after ranges where the leaseholder is
			// violating the preference.
			log.VEventf(ctx, 2,
				"acquired lease is less preferred, enqueuing for transfer [lease=%v preferences=%v]",
				newLease, r.mu.conf.LeasePreferences)
			r.store.leaseQueue.AddAsync(ctx, r, allocatorimpl.TransferLeaseForPreferences.Priority()-1)
		default:
			log.Fatalf(ctx, "unknown lease preferences status: %v", preferenceStatus)
		}
	}

	// Inform the store of this lease.
	if iAmTheLeaseHolder {
		r.store.registerLeaseholder(ctx, r, newLease.Sequence)
	} else {
		r.store.unregisterLeaseholder(ctx, r)
	}

	// Mark the new lease in the replica's lease history.
	if r.leaseHistory != nil {
		r.leaseHistory.add(*newLease)
	}
}

// maybeLogLeaseAcquisition is called on the new leaseholder when the lease
// changes hands, to log the lease acquisition if appropriate.
func (r *Replica) maybeLogLeaseAcquisition(
	ctx context.Context, now hlc.ClockTimestamp, prevLease, newLease *roachpb.Lease,
) {
	// Log acquisition of meta and liveness range leases. These are critical to
	// cluster health, so it's useful to know their location over time.
	if r.descRLocked().StartKey.Less(roachpb.RKey(keys.NodeLivenessKeyMax)) {
		if r.ownsValidLeaseRLocked(ctx, now) {
			log.Health.Infof(ctx, "acquired system range lease: %s [acquisition-type=%s]",
				newLease, newLease.AcquisitionType)
		} else {
			log.Health.Warningf(ctx, "applied system range lease after it expired: %s [acquisition-type=%s]",
				newLease, newLease.AcquisitionType)
		}
	}

	const slowLeaseApplyWarnThreshold = time.Second
	newLeaseAppDelay := time.Duration(now.WallTime - newLease.ProposedTS.WallTime)
	if newLeaseAppDelay > slowLeaseApplyWarnThreshold {
		// If we hold the lease now and the lease was proposed "earlier", there
		// must have been replication lag, and possibly reads and/or writes were
		// delayed.
		//
		// We see this most commonly with lease transfers targeting a behind replica,
		// or, in the worst case, a snapshot. We are constantly improving our
		// heuristics for avoiding that[^1] but if it does happen it's good to know
		// from the logs.
		//
		// In the case of a lease transfer, the two timestamps compared below are from
		// different clocks, so there could be skew. We just pretend this is not the
		// case, which is good enough here.
		//
		// [^1]: https://github.com/cockroachdb/cockroach/pull/82758
		log.Health.Warningf(ctx,
			"applied lease after ~%.2fs replication lag, client traffic may have "+
				"been delayed [lease=%v prev=%v acquisition-type=%s]",
			newLeaseAppDelay.Seconds(), newLease, prevLease, newLease.AcquisitionType)
	} else if prevLease.Type() == roachpb.LeaseExpiration &&
		newLease.Type() != roachpb.LeaseExpiration &&
		prevLease.Expiration != nil && // nil when there is no previous lease
		prevLease.Expiration.LessEq(newLease.Start.ToTimestamp()) {
		// If the previous lease is expiration-based, but the new lease is not and
		// starts at or after its expiration, it is likely that a lease transfer
		// (which is expiration-based) went to a follower that then couldn't upgrade
		// it to an epoch lease (for example, didn't apply it in time for it to
		// actually serve any traffic). The result was likely an outage which
		// resolves right now, so log to point this out.
		log.Health.Warningf(ctx,
			"lease expired before epoch/leader lease upgrade, client traffic may "+
				"have been delayed [lease=%v prev=%v acquisition-type=%s]",
			newLease, prevLease, newLease.AcquisitionType)
	}
}

var addSSTPreApplyWarn = struct {
	threshold time.Duration
	log.EveryN
}{500 * time.Millisecond, log.Every(time.Second)}

func addSSTablePreApply(
	ctx context.Context,
	env postAddEnv,
	term kvpb.RaftTerm,
	index kvpb.RaftIndex,
	sst kvserverpb.ReplicatedEvalResult_AddSSTable,
) bool {
	checksum := util.CRC32(sst.Data)

	if checksum != sst.CRC32 {
		log.Fatalf(
			ctx,
			"checksum for AddSSTable at index term %d, index %d does not match; at proposal time %x (%d), now %x (%d)",
			term, index, sst.CRC32, sst.CRC32, checksum, checksum,
		)
	}

	path, err := env.sideloaded.Filename(ctx, index, term)
	if err != nil {
		log.Fatalf(ctx, "sideloaded SSTable at term %d, index %d is missing", term, index)
	}

	tBegin := timeutil.Now()
	defer func() {
		if dur := timeutil.Since(tBegin); dur > addSSTPreApplyWarn.threshold && addSSTPreApplyWarn.ShouldLog() {
			log.Infof(ctx,
				"ingesting SST of size %s at index %d took %.2fs",
				humanizeutil.IBytes(int64(len(sst.Data))), index, dur.Seconds(),
			)
		}
	}()

	ingestPath := path + ".ingested"

	// The SST may already be on disk, thanks to the sideloading mechanism. If
	// so we can try to add that file directly, via a new hardlink if the
	// filesystem supports it, rather than writing a new copy of it. We cannot
	// pass it the path in the sideload store as the engine deletes the passed
	// path on success.
	if linkErr := env.eng.Env().Link(path, ingestPath); linkErr != nil {
		// We're on a weird file system that doesn't support Link. This is unlikely
		// to happen in any "normal" deployment but we have a fallback path anyway.
		log.Eventf(ctx, "copying SSTable for ingestion at index %d, term %d: %s", index, term, ingestPath)
		if err := ingestViaCopy(ctx, env.st, env.eng, ingestPath, term, index, sst, env.bulkLimiter); err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		return true /* copied */
	}

	// Regular path - we made a hard link, so we can ingest the hard link now.
	ingestErr := env.eng.IngestLocalFiles(ctx, []string{ingestPath})
	if ingestErr != nil {
		log.Fatalf(ctx, "while ingesting %s: %v", ingestPath, ingestErr)
	}
	// Adding without modification succeeded, no copy necessary.
	log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, ingestPath)

	return false /* copied */
}

func linkExternalSStablePreApply(
	ctx context.Context,
	env postAddEnv,
	term kvpb.RaftTerm,
	index kvpb.RaftIndex,
	sst kvserverpb.ReplicatedEvalResult_LinkExternalSSTable,
) {
	log.VInfof(ctx, 1,
		"linking external sstable %s (size %d, span %s) from %s (size %d) at rewrite ts %s, synth prefix %s",
		sst.RemoteFilePath,
		sst.ApproximatePhysicalSize,
		sst.Span,
		sst.RemoteFileLoc,
		sst.BackingFileSize,
		sst.RemoteRewriteTimestamp,
		roachpb.Key(sst.RemoteSyntheticPrefix),
	)

	start := storage.EngineKey{Key: sst.Span.Key}
	// NB: sst.Span.EndKey may be nil if the span represents a single key. In
	// that case, we produce an inclusive end bound equal to the start.
	// Otherwise we produce an exclusive end bound.
	end := start
	endInclusive := true
	if sst.Span.EndKey != nil {
		end = storage.EngineKey{Key: sst.Span.EndKey}
		endInclusive = false
	}
	var syntheticSuffix []byte
	if sst.RemoteRewriteTimestamp.IsSet() {
		syntheticSuffix = storage.EncodeMVCCTimestampSuffix(sst.RemoteRewriteTimestamp)
	}
	var syntheticPrefix []byte
	if len(sst.RemoteSyntheticPrefix) > 0 {
		syntheticPrefix = sst.RemoteSyntheticPrefix
	}

	externalFile := pebble.ExternalFile{
		Locator:           remote.Locator(sst.RemoteFileLoc),
		ObjName:           sst.RemoteFilePath,
		Size:              sst.ApproximatePhysicalSize,
		StartKey:          start.Encode(),
		EndKey:            end.Encode(),
		EndKeyIsInclusive: endInclusive,
		SyntheticSuffix:   syntheticSuffix,
		SyntheticPrefix:   syntheticPrefix,
		HasPointKey:       true,
	}
	tBegin := timeutil.Now()
	defer func() {
		if dur := timeutil.Since(tBegin); dur > addSSTPreApplyWarn.threshold && addSSTPreApplyWarn.ShouldLog() {
			log.Infof(ctx,
				"ingesting External SST at index %d took %.2fs", index, dur.Seconds(),
			)
		}
	}()

	_, ingestErr := env.eng.IngestExternalFiles(ctx, []pebble.ExternalFile{externalFile})
	if ingestErr != nil {
		log.Fatalf(ctx, "while ingesting %s: %v", sst.RemoteFilePath, ingestErr)
	}
	log.Eventf(ctx, "ingested SSTable at index %d, term %d: external %s", index, term, sst.RemoteFilePath)
}

// ingestViaCopy writes the SST to ingestPath (with rate limiting) and then ingests it
// into the Engine.
//
// This is not normally called, as we prefer to make a hard-link and ingest that instead.
func ingestViaCopy(
	ctx context.Context,
	st *cluster.Settings,
	eng storage.Engine,
	ingestPath string,
	term kvpb.RaftTerm,
	index kvpb.RaftIndex,
	sst kvserverpb.ReplicatedEvalResult_AddSSTable,
	limiter *rate.Limiter,
) error {
	// TODO(tschottdorf): remove this once sideloaded storage guarantees its
	// existence.
	if err := eng.Env().MkdirAll(filepath.Dir(ingestPath), os.ModePerm); err != nil {
		panic(err)
	}
	if _, err := eng.Env().Stat(ingestPath); err == nil {
		// The file we want to ingest exists. This can happen since the
		// ingestion may apply twice (we ingest before we mark the Raft
		// command as committed). Just unlink the file (the storage engine
		// created a hard link); after that we're free to write it again.
		if err := eng.Env().Remove(ingestPath); err != nil {
			return errors.Wrapf(err, "while removing existing file during ingestion of %s", ingestPath)
		}
	}
	if err := kvserverbase.WriteFileSyncing(ctx, ingestPath, sst.Data, eng.Env(), 0600, st, limiter, fs.PebbleIngestionWriteCategory); err != nil {
		return errors.Wrapf(err, "while ingesting %s", ingestPath)
	}
	if err := eng.IngestLocalFiles(ctx, []string{ingestPath}); err != nil {
		return errors.Wrapf(err, "while ingesting %s", ingestPath)
	}
	log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, ingestPath)
	return nil
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
		r.store.splitQueue.MaybeAddAsync(ctx, r, r.store.Clock().NowAsClockTimestamp())
		lResult.MaybeAddToSplitQueue = false
	}

	if lResult.MaybeGossipNodeLiveness != nil {
		if err := r.MaybeGossipNodeLivenessRaftMuLocked(ctx, *lResult.MaybeGossipNodeLiveness); err != nil {
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
	Reply              *kvpb.BatchResponse
	Err                *kvpb.Error
	EncounteredIntents []roachpb.Intent
	EndTxns            []result.EndTxnIntents
}

func makeProposalResult(
	br *kvpb.BatchResponse, pErr *kvpb.Error, ei []roachpb.Intent, eti []result.EndTxnIntents,
) proposalResult {
	return proposalResult{
		Reply:              br,
		Err:                pErr,
		EncounteredIntents: ei,
		EndTxns:            eti,
	}
}

func makeProposalResultPErr(err *kvpb.Error) proposalResult {
	return proposalResult{Err: err}
}

func makeProposalResultErr(err error) proposalResult {
	return proposalResult{Err: kvpb.NewError(err)}
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
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
) (*kvpb.BatchRequest, *result.Result, bool, *kvpb.Error) {
	if ba.Timestamp.IsEmpty() {
		return ba, nil, false, kvpb.NewErrorf("can't propose Raft command with zero timestamp")
	}

	// Evaluate the commands. If this returns without an error, the batch should
	// be committed. Note that we don't hold any locks at this point, except a
	// shared RLock on raftMuReadOnlyMu. This is important since evaluating a
	// proposal is expensive.
	//
	// Note that, during evaluation, ba's read and write timestamps might get
	// bumped (see evaluateWriteBatchWithServersideRefreshes).
	//
	// TODO(tschottdorf): absorb all returned values in `res` below this point
	// in the call stack as well.
	ba, batch, ms, br, res, pErr := r.evaluateWriteBatch(ctx, idKey, ba, g, st, ui)

	// Note: reusing the proposer's batch when applying the command on the
	// proposer was explored as an optimization but resulted in no performance
	// benefit.
	if batch != nil {
		defer batch.Close()
	}

	if pErr != nil {
		if _, ok := pErr.GetDetail().(*kvpb.ReplicaCorruptionError); ok {
			return ba, &res, false /* needConsensus */, pErr
		}

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
		return ba, &res, false /* needConsensus */, pErr
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
	// 4. the request is of a type that requires consensus (eg. Barrier).
	// 5. the request has side-effects that must be applied under Raft.
	needConsensus := !batch.Empty() ||
		ms != (enginepb.MVCCStats{}) ||
		!res.Replicated.IsZero() ||
		ba.RequiresConsensus() ||
		res.Local.RequiresRaft()

	if needConsensus {
		log.VEventf(ctx, 2, "need consensus on write batch with op count=%d", batch.Count())

		// Set the proposal's WriteBatch, which is the serialized representation of
		// the proposals effect on RocksDB.
		res.WriteBatch = &kvserverpb.WriteBatch{
			Data: batch.Repr(),
		}

		// Set the proposal's replicated result, which contains metadata and
		// side-effects that are to be replicated to all replicas.
		res.Replicated.IsLeaseRequest = ba.IsSingleRequestLeaseRequest()
		if res.Replicated.IsLeaseRequest {
			// NOTE: this cluster version check may return true even after the
			// corresponding check in evalNewLease has returned false. That's ok, as
			// this check is used to inform raft about whether an expiration-based
			// lease **can** be promoted to an epoch-based lease without a sequence
			// change, not that it **is** being promoted without a sequence change.
			res.Replicated.IsLeaseRequestWithExpirationToEpochEquivalent = true
		}
		if ba.AppliesTimestampCache() {
			res.Replicated.WriteTimestamp = ba.WriteTimestamp()
		}
		res.Replicated.Delta = ms.ToStatsDelta()

		// This is the result of a migration. See the field for more details.
		if res.Replicated.Delta.ContainsEstimates > 0 {
			res.Replicated.Delta.ContainsEstimates *= 2
		}
	}

	return ba, &res, needConsensus, nil
}

// requestToProposal converts a BatchRequest into a ProposalData, by
// evaluating it. The returned ProposalData is partially valid even
// on a non-nil *kvpb.Error and should be proposed through Raft
// if ProposalData.command is non-nil.
func (r *Replica) requestToProposal(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	st *kvserverpb.LeaseStatus,
	ui uncertainty.Interval,
) (*ProposalData, *kvpb.Error) {
	ba, res, needConsensus, pErr := r.evaluateProposal(ctx, idKey, ba, g, st, ui)

	// Fill out the results even if pErr != nil; we'll return the error below.
	proposal := &ProposalData{
		idKey:       idKey,
		doneCh:      make(chan proposalResult, 1),
		Local:       &res.Local,
		Request:     ba,
		leaseStatus: *st,
	}
	proposal.ctx.Store(&ctx)

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

// getTraceData extracts the SpanMeta of the current span.
func (r *Replica) getTraceData(ctx context.Context) map[string]string {
	sp := tracing.SpanFromContext(ctx)
	if sp == nil {
		return nil
	}
	// TODO(andrei): We should propagate trace info even for non-verbose spans.
	// We'd probably want to use a cheaper mechanism than `InjectMetaInto`,
	// though.
	if !sp.IsVerbose() {
		return nil
	}

	traceCarrier := tracing.MapCarrier{
		Map: make(map[string]string),
	}
	r.AmbientContext.Tracer.InjectMetaInto(sp.Meta(), traceCarrier)
	return traceCarrier.Map
}
