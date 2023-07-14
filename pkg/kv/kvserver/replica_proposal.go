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
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/readsummary/rspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
// also flush the buffer to make room for a new proposal. ProposalData itself
// has no mutex and locking is provided by `Replica.mu` which must always be
// held when accessing a ProposalData (except before the first insertion into
// the proposal buffer). However, at the time of writing, this locking is
// possibly insufficient and the above rule is not adhered to at all times. In
// particular, during raft application, the ProposalData is mutated without the
// Replica.mu lock being held in all places. As log application is a
// well-trodden code path and no relevant data races are known, they are either
// absent or extremely rare. Still, an improvement seems advisable.
//
// This goes for the locking, but also for the lifecycle of ProposalData in
// aggregate. It is extremely complex due to internal reproposals and the
// multiple, poorly synchronized, and circular hand-offs that exist between the
// proposals map and the proposal buffer. The following diagram attempts to
// describe the status quo.
//
//	         created
//	           │                proposal map, apply
//	           │[1]             goroutine, and propBuf
//	           ▼                        ▲    │
//	┌─┬────►propBuf  ┌────────────┐     │[6] │[2]
//	│ │        │     │            │     │    ▼
//	│ │        │[2]  │         proposal map and
//	│ │        ▼     ▼         apply goroutine
//	│ │   proposal map────┐       ▲
//	│ │        │     │    │  [4]  │
//	│ │[5]     │[3]  │[6] └───────┘
//	│ │        │     │
//	│ │        ▼     └──────►proposal map
//	│ └─apply goroutine      and propBuf
//	│          │                 │[7]
//	│          ▼                 ▼
//	│[8]    finished         apply goroutine and propBuf
//	│                            │
//	└────────────────────────────┘
//
// [1]: `(*Replica).propose` calls `(*proposalBuf).Insert`
// [2]: `(*proposalBuf).FlushLockedWithRaftGroup` on the `handleRaftReady`
// goroutine, or `(*proposalBuf).Insert` for another command on a full
// `proposalBuf`, or `FlushLockedWithoutProposing` (less interesting).
// [3]: picked up by `(*replicaDecoder).retrieveLocalProposals` when the log
// entry has the most recent MaxLeaseIndex (common path)
// [4]: picked up by `(*replicaDecoder).retrieveLocalProposals` but the log
// entry is a past incarnation, i.e. `MaxLeaseIndex` of `ProposalData` is newer.
// In this case, the command will be rejected (apply as a no-op and not signal a
// result) since a newer copy of it is inflight. (Unhappy path).
// [5]: log entry failed MaxLeaseIndex check, and we are reproposing with a
// newer MaxLeaseIndex in `(*Replica).tryReproposeWithNewLeaseIndex`.
// [6]: while being applied (rejected), the `*ProposalData` may be reinserted
// into the `propBuf` by `(*Replica).refreshProposalsLocked`, which finds it in
// the map.
// [7]: like [3] but the goroutine is now also in the propBuf.
// [8]: either [5] or the proposal applies successfully but is still in the
// propBuf since it was there initially. In that latter case we now have a
// finished proposal in the propBuf
// (https://github.com/cockroachdb/cockroach/issues/97605)
//
// See also the prose near the call to `tryReproposeWithNewLeaseIndex`.
type ProposalData struct {
	// The caller's context, used for logging proposals, reproposals, message
	// sends, but not command application. In order to enable safely tracing events
	// beneath, modifying this ctx field in *ProposalData requires holding the
	// raftMu.
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
	// TODO(tbg): under useReproposalsV2, the field can be modified safely as long
	// as the ProposalData is still in `r.mu.proposals` and `r.mu` is held. If it's
	// not in that map, we are log application and have exclusive access. Add a more
	// general comment that explains what can be accessed where (I think I wrote one
	// somewhere but it should live on ProposalData).
	ctx context.Context

	// An optional tracing span bound to the proposal in the case of async
	// consensus (it will be referenced by p.ctx). We need to finish this span
	// after applying this proposal, since we created it. It is not used for
	// anything else (all tracing goes through `p.ctx`).
	sp *tracing.Span

	// idKey uniquely identifies this proposal.
	idKey kvserverbase.CmdIDKey

	// proposedAtTicks is the (logical) time at which this command was
	// last (re-)proposed.
	proposedAtTicks int

	// createdAtTicks is the (logical) time at which this command was
	// *first* proposed.
	createdAtTicks int

	// command is serialized and proposed to raft. In the event of
	// reproposals its MaxLeaseIndex field is mutated.
	//
	// TODO(tbg): when useReproposalsV2==true is baked in, the above comment
	// is stale - MLI never gets mutated.
	command *kvserverpb.RaftCommand

	// encodedCommand is the encoded Raft command, with an optional prefix
	// containing the command ID.
	encodedCommand []byte

	// quotaAlloc is the allocation retrieved from the proposalQuota. Once a
	// proposal has been passed to raft modifying this field requires holding the
	// raftMu. Once the proposal comes out of Raft, ownerwhip of this quota is
	// passed to r.mu.quotaReleaseQueue.
	quotaAlloc *quotapool.IntAlloc

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
	// application is). Under useReproposalsV2, a local proposal is bound to an
	// entry only once and the proposals map entry removed. This flag makes sure
	// that the proposal buffer won't accidentally reinsert the proposal into the
	// map. In doing so, this field also addresses locking concerns. As long as
	// the ProposalData is in the `proposals` map, replicaMu must be held. But
	// command application unlinks the command from the map and wants to be able
	// to access it without acquiring replicaMu. The only other actor that can
	// access the proposal while it is being applied is the proposal buffer (which
	// always holds replicaMu); since v2SeenDuringApplication is flipped while
	// under the lock (which log application holds at that point in time) and is
	// never mutated afterwards, the proposal buffer is allowed to access that
	// particular field and use it to avoid touching the ProposalData. A similar
	// strategy is not possible under !useReproposalsV2 because by "design",
	// proposals may repeatedly leave and re-enter the map and ultimately still
	// apply successfully.
	//
	// See: https://github.com/cockroachdb/cockroach/issues/97605
	//
	// Never set unless useReproposalsV2 is active.
	v2SeenDuringApplication bool
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
		proposal.ctx = context.Background()
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
			if !prevLease.Equivalent(*newLease) {
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
		if log.V(1) || r.store.IsDraining() {
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
		applyReadSummaryToTimestampCache(r.store.tsCache, r.descRLocked(), sum)

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
	r.mu.proposalBuf.OnLeaseChangeLocked(iAmTheLeaseHolder, r.mu.state.RaftClosedTimestamp, r.mu.state.LeaseAppliedIndex)

	// Ordering is critical here. We only install the new lease after we've
	// checked for an in-progress merge and updated the timestamp cache. If the
	// ordering were reversed, it would be possible for requests to see the new
	// lease but not the updated merge or timestamp cache state, which can result
	// in serializability violations.
	r.mu.state.Lease = newLease

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
		// the more efficient epoch-based one.
		if log.V(1) {
			log.VEventf(ctx, 1, "upgrading expiration lease %s to an epoch-based one", newLease)
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
			log.Health.Infof(ctx, "acquired system range lease: %s", newLease)
		} else {
			log.Health.Warningf(ctx, "applied system range lease after it expired: %s", newLease)
		}
	}

	const slowLeaseApplyWarnThreshold = time.Second
	var newLeaseAppDelay time.Duration
	if newLease.ProposedTS != nil { // non-nil in practice, but never migrated
		newLeaseAppDelay = time.Duration(now.WallTime - newLease.ProposedTS.WallTime)
	}
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
			"applied lease after ~%.2fs replication lag, client traffic may have been delayed [lease=%v prev=%v]",
			newLeaseAppDelay.Seconds(), newLease, prevLease)
	} else if prevLease.Type() == roachpb.LeaseExpiration &&
		newLease.Type() == roachpb.LeaseEpoch &&
		prevLease.Expiration != nil && // nil when there is no previous lease
		prevLease.Expiration.LessEq(newLease.Start.ToTimestamp()) {
		// If the previous lease is expiration-based, but the new lease is not and
		// starts at or after its expiration, it is likely that a lease transfer
		// (which is expiration-based) went to a follower that then couldn't upgrade
		// it to an epoch lease (for example, didn't apply it in time for it to
		// actually serve any traffic). The result was likely an outage which
		// resolves right now, so log to point this out.
		log.Health.Warningf(ctx,
			"lease expired before epoch lease upgrade, client traffic may have been delayed [lease=%v prev=%v]",
			newLease, prevLease)
	}
}

var addSSTPreApplyWarn = struct {
	threshold time.Duration
	log.EveryN
}{500 * time.Millisecond, log.Every(time.Second)}

func addSSTablePreApply(
	ctx context.Context,
	st *cluster.Settings,
	eng storage.Engine,
	sideloaded logstore.SideloadStorage,
	term kvpb.RaftTerm,
	index kvpb.RaftIndex,
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
	if linkErr := eng.Link(path, ingestPath); linkErr != nil {
		// We're on a weird file system that doesn't support Link. This is unlikely
		// to happen in any "normal" deployment but we have a fallback path anyway.
		log.Eventf(ctx, "copying SSTable for ingestion at index %d, term %d: %s", index, term, ingestPath)
		if err := ingestViaCopy(ctx, st, eng, ingestPath, term, index, sst, limiter); err != nil {
			log.Fatalf(ctx, "%v", err)
		}
		return true /* copied */
	}

	// Regular path - we made a hard link, so we can ingest the hard link now.
	ingestErr := eng.IngestExternalFiles(ctx, []string{ingestPath})
	if ingestErr != nil {
		log.Fatalf(ctx, "while ingesting %s: %v", ingestPath, ingestErr)
	}
	// Adding without modification succeeded, no copy necessary.
	log.Eventf(ctx, "ingested SSTable at index %d, term %d: %s", index, term, ingestPath)

	return false /* copied */
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
	if err := eng.MkdirAll(filepath.Dir(ingestPath), os.ModePerm); err != nil {
		panic(err)
	}
	if _, err := eng.Stat(ingestPath); err == nil {
		// The file we want to ingest exists. This can happen since the
		// ingestion may apply twice (we ingest before we mark the Raft
		// command as committed). Just unlink the file (the storage engine
		// created a hard link); after that we're free to write it again.
		if err := eng.Remove(ingestPath); err != nil {
			return errors.Wrapf(err, "while removing existing file during ingestion of %s", ingestPath)
		}
	}
	if err := kvserverbase.WriteFileSyncing(ctx, ingestPath, sst.Data, eng, 0600, st, limiter); err != nil {
		return errors.Wrapf(err, "while ingesting %s", ingestPath)
	}
	if err := eng.IngestExternalFiles(ctx, []string{ingestPath}); err != nil {
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
) (*result.Result, bool, *kvpb.Error) {
	if ba.Timestamp.IsEmpty() {
		return nil, false, kvpb.NewErrorf("can't propose Raft command with zero timestamp")
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
	batch, ms, br, res, pErr := r.evaluateWriteBatch(ctx, idKey, ba, g, st, ui)

	// Note: reusing the proposer's batch when applying the command on the
	// proposer was explored as an optimization but resulted in no performance
	// benefit.
	if batch != nil {
		defer batch.Close()
	}

	if pErr != nil {
		if _, ok := pErr.GetDetail().(*kvpb.ReplicaCorruptionError); ok {
			return &res, false /* needConsensus */, pErr
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
		if ba.AppliesTimestampCache() {
			res.Replicated.WriteTimestamp = ba.WriteTimestamp()
		}
		res.Replicated.Delta = ms.ToStatsDelta()

		// This is the result of a migration. See the field for more details.
		if res.Replicated.Delta.ContainsEstimates > 0 {
			res.Replicated.Delta.ContainsEstimates *= 2
		}
	}

	return &res, needConsensus, nil
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
	res, needConsensus, pErr := r.evaluateProposal(ctx, idKey, ba, g, st, ui)

	// Fill out the results even if pErr != nil; we'll return the error below.
	proposal := &ProposalData{
		ctx:         ctx,
		idKey:       idKey,
		doneCh:      make(chan proposalResult, 1),
		Local:       &res.Local,
		Request:     ba,
		leaseStatus: *st,
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
