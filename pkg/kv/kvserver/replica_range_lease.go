// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file contains replica methods related to range leases.
//
// Here be dragons: The lease system (especially for epoch-based
// leases) relies on multiple interlocking conditional puts (here and
// in NodeLiveness). Reads (to get expected values) and conditional
// puts have to happen in a certain order, leading to surprising
// dependencies at a distance (for example, there's a LeaseStatus
// object that gets plumbed most of the way through this file.
// LeaseStatus bundles the results of multiple checks with the time at
// which they were performed, so that timestamp must be used for later
// operations). The current arrangement is not perfect, and some
// opportunities for improvement appear, but any changes must be made
// very carefully.
//
// NOTE(bdarnell): The biggest problem with the current code is that
// with epoch-based leases, we may do two separate slow operations
// (IncrementEpoch/Heartbeat and RequestLease/AdminTransferLease). In
// the organization that was inherited from expiration-based leases,
// we prepare the arguments we're going to use for the lease
// operations before performing the liveness operations, and by the
// time the liveness operations complete those may be stale.
//
// Therefore, my suggested refactoring would be to move the liveness
// operations earlier in the process, soon after the initial
// leaseStatus call. If a liveness operation is required, do it and
// start over, with a fresh leaseStatus.
//
// This could also allow the liveness operations to be coalesced per
// node instead of having each range separately queue up redundant
// liveness operations. (The InitOrJoin model predates the
// singleflight package; could we simplify things by using it?)

package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

var transferExpirationLeasesFirstEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.transfer_expiration_leases_first.enabled",
	"controls whether we transfer expiration-based leases that are later upgraded to epoch-based ones",
	true,
)

var leaseStatusLogLimiter = func() *log.EveryN {
	e := log.Every(15 * time.Second)
	e.ShouldLog() // waste the first shot
	return &e
}()

// leaseRequestHandle is a handle to an asynchronous lease request.
type leaseRequestHandle struct {
	p *pendingLeaseRequest
	c chan *roachpb.Error
}

// C returns the channel where the lease request's result will be sent on.
func (h *leaseRequestHandle) C() <-chan *roachpb.Error {
	if h.c == nil {
		panic("handle already canceled")
	}
	return h.c
}

// Cancel cancels the request handle. It also cancels the asynchronous
// lease request task if its reference count drops to zero.
func (h *leaseRequestHandle) Cancel() {
	h.p.repl.mu.Lock()
	defer h.p.repl.mu.Unlock()
	if len(h.c) == 0 {
		// Our lease request is ongoing...
		// Unregister handle.
		delete(h.p.llHandles, h)
		// Cancel request, if necessary.
		if len(h.p.llHandles) == 0 {
			h.p.cancelLocked()
		}
	}
	// Mark handle as canceled.
	h.c = nil
}

// resolve notifies the handle of the request's result.
//
// Requires repl.mu is exclusively locked.
func (h *leaseRequestHandle) resolve(pErr *roachpb.Error) { h.c <- pErr }

// pendingLeaseRequest coalesces RequestLease requests and lets
// callers join an in-progress lease request and wait for the result.
// The actual execution of the RequestLease Raft request is delegated
// to a replica.
//
// There are two types of leases: expiration-based and epoch-based.
// Expiration-based leases are considered valid as long as the wall
// time is less than the lease expiration timestamp minus the maximum
// clock offset. Epoch-based leases do not expire, but rely on the
// leaseholder maintaining its node liveness record (also a lease, but
// at the node level). All ranges up to and including the node
// liveness table must use expiration-based leases to avoid any
// circular dependencies.
//
// Methods are not thread-safe; a pendingLeaseRequest is logically part
// of the replica it references, so replica.mu should be used to
// synchronize all calls.
type pendingLeaseRequest struct {
	// The replica that the pendingLeaseRequest is a part of.
	repl *Replica
	// Set of request handles attached to the lease acquisition.
	// All accesses require repl.mu to be exclusively locked.
	llHandles map[*leaseRequestHandle]struct{}
	// cancelLocked is a context cancellation function for the async lease
	// request, if one exists. It cancels an ongoing lease request and cleans up
	// the requests state, including setting the cancelLocked function itself to
	// nil. It will be called when a lease request is canceled because all
	// handles cancel or when a lease request completes. If nil, then no request
	// is in progress. repl.mu to be exclusively locked to call the function.
	cancelLocked func()
	// nextLease is the pending RequestLease request, if any. It can be used to
	// figure out if we're in the process of extending our own lease, or
	// transferring it to another replica.
	nextLease roachpb.Lease
}

func makePendingLeaseRequest(repl *Replica) pendingLeaseRequest {
	return pendingLeaseRequest{
		repl:      repl,
		llHandles: make(map[*leaseRequestHandle]struct{}),
	}
}

// RequestPending returns the pending Lease, if one is in progress.
// The second return val is true if a lease request is pending.
//
// Requires repl.mu is read locked.
func (p *pendingLeaseRequest) RequestPending() (roachpb.Lease, bool) {
	pending := p.cancelLocked != nil
	if pending {
		return p.nextLease, true
	}
	return roachpb.Lease{}, false
}

// InitOrJoinRequest executes a RequestLease command asynchronously and returns a
// handle on which the result will be posted. If there's already a request in
// progress, we join in waiting for the results of that request.
// It is an error to call InitOrJoinRequest() while a request is in progress
// naming another replica as lease holder.
//
// replica is used to schedule and execute async work (proposing a RequestLease
// command). replica.mu is locked when delivering results, so calls from the
// replica happen either before or after a result for a pending request has
// happened.
//
// The new lease will be a successor to the one in the status
// argument, and its fields will be used to fill in the expected
// values for liveness and lease operations.
//
// transfer needs to be set if the request represents a lease transfer (as
// opposed to an extension, or acquiring the lease when none is held).
//
// Requires repl.mu is exclusively locked.
func (p *pendingLeaseRequest) InitOrJoinRequest(
	ctx context.Context,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	status kvserverpb.LeaseStatus,
	startKey roachpb.Key,
	transfer bool,
	bypassSafetyChecks bool,
) *leaseRequestHandle {
	if nextLease, ok := p.RequestPending(); ok {
		if nextLease.Replica.ReplicaID == nextLeaseHolder.ReplicaID {
			// Join a pending request asking for the same replica to become lease
			// holder.
			return p.JoinRequest()
		}

		// We can't join the request in progress.
		// TODO(nvanbenschoten): should this return a LeaseRejectedError? Should
		// it cancel and replace the request in progress? Reconsider.
		return p.newResolvedHandle(roachpb.NewErrorf(
			"request for different replica in progress (requesting: %+v, in progress: %+v)",
			nextLeaseHolder.ReplicaID, nextLease.Replica.ReplicaID))
	}

	acquisition := !status.Lease.OwnedBy(p.repl.store.StoreID())
	extension := !transfer && !acquisition
	_ = extension // not used, just documentation

	if acquisition {
		// If this is a non-cooperative lease change (i.e. an acquisition), it
		// is up to us to ensure that Lease.Start is greater than the end time
		// of the previous lease. This means that if status refers to an expired
		// epoch lease, we must increment the liveness epoch of the previous
		// leaseholder *using status.Liveness*, which we know to be expired *at
		// status.Timestamp*, before we can propose this lease. If this
		// increment fails, we cannot propose this new lease (see handling of
		// ErrEpochAlreadyIncremented in requestLeaseAsync).
		//
		// Note that the request evaluation may decrease our proposed start time
		// if it decides that it is safe to do so (for example, this happens
		// when renewing an expiration-based lease), but it will never increase
		// it (and a start timestamp that is too low is unsafe because it
		// results in incorrect initialization of the timestamp cache on the new
		// leaseholder). For expiration-based leases, we have a safeguard during
		// evaluation - we simply check that the new lease starts after the old
		// lease ends and throw an error if now. But for epoch-based leases, we
		// don't have the benefit of such a safeguard during evaluation because
		// the expiration is indirectly stored in the referenced liveness record
		// and not in the lease itself. So for epoch-based leases, enforcing
		// this safety condition is truly up to us.
		if status.State != kvserverpb.LeaseState_EXPIRED {
			log.Fatalf(ctx, "cannot acquire lease from another node before it has expired: %v", status)
		}
	}

	// No request in progress. Let's propose a Lease command asynchronously.
	llHandle := p.newHandle()
	reqHeader := roachpb.RequestHeader{
		Key: startKey,
	}
	reqLease := roachpb.Lease{
		Start:      status.Now,
		Replica:    nextLeaseHolder,
		ProposedTS: &status.Now,
	}

	if p.repl.requiresExpiringLeaseRLocked() ||
		(transfer &&
			transferExpirationLeasesFirstEnabled.Get(&p.repl.store.ClusterSettings().SV) &&
			p.repl.store.ClusterSettings().Version.IsActive(ctx, clusterversion.V22_2EnableLeaseUpgrade)) {
		// In addition to ranges that unconditionally require expiration-based
		// leases (node liveness and earlier), we also use them during lease
		// transfers for all other ranges. After acquiring these expiration
		// based leases, the leaseholders are expected to upgrade them to the
		// more efficient epoch-based ones. But by transferring an
		// expiration-based lease, we can limit the effect of an ill-advised
		// lease transfer since the incoming leaseholder needs to recognize
		// itself as such within a few seconds; if it doesn't (we accidentally
		// sent the lease to a replica in need of a snapshot or far behind on
		// its log), the lease is up for grabs. If we simply transferred epoch
		// based leases, it's possible for the new leaseholder that's delayed
		// in applying the lease transfer to maintain its lease (assuming the
		// node it's on is able to heartbeat its liveness record).
		reqLease.Expiration = &hlc.Timestamp{}
		*reqLease.Expiration = status.Now.ToTimestamp().Add(int64(p.repl.store.cfg.RangeLeaseActiveDuration()), 0)
	} else {
		// Get the liveness for the next lease holder and set the epoch in the lease request.
		l, ok := p.repl.store.cfg.NodeLiveness.GetLiveness(nextLeaseHolder.NodeID)
		if !ok || l.Epoch == 0 {
			llHandle.resolve(roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  status.Lease,
				Requested: reqLease,
				Message:   fmt.Sprintf("couldn't request lease for %+v: %v", nextLeaseHolder, liveness.ErrRecordCacheMiss),
			}))
			return llHandle
		}
		reqLease.Epoch = l.Epoch
	}

	var leaseReq roachpb.Request
	if transfer {
		leaseReq = &roachpb.TransferLeaseRequest{
			RequestHeader:      reqHeader,
			Lease:              reqLease,
			PrevLease:          status.Lease,
			BypassSafetyChecks: bypassSafetyChecks,
		}
	} else {
		if bypassSafetyChecks {
			// TODO(nvanbenschoten): we could support a similar bypassSafetyChecks
			// flag for RequestLeaseRequest, which would disable the protection in
			// propBuf.maybeRejectUnsafeProposalLocked. For now, we use a testing
			// knob.
			log.Fatal(ctx, "bypassSafetyChecks not supported for RequestLeaseRequest")
		}
		minProposedTS := p.repl.mu.minLeaseProposedTS
		leaseReq = &roachpb.RequestLeaseRequest{
			RequestHeader: reqHeader,
			Lease:         reqLease,
			// PrevLease must match for our lease to be accepted. If another
			// lease is applied between our previous call to leaseStatus and
			// our lease request applying, it will be rejected.
			PrevLease:     status.Lease,
			MinProposedTS: &minProposedTS,
		}
	}

	if err := p.requestLeaseAsync(ctx, nextLeaseHolder, reqLease, status, leaseReq); err != nil {
		// We failed to start the asynchronous task. Send a blank NotLeaseHolderError
		// back to indicate that we have no idea who the range lease holder might
		// be; we've withdrawn from active duty.
		llHandle.resolve(roachpb.NewError(
			roachpb.NewNotLeaseHolderError(roachpb.Lease{}, p.repl.store.StoreID(), p.repl.mu.state.Desc,
				"lease acquisition task couldn't be started; node is shutting down")))
		return llHandle
	}
	// InitOrJoinRequest requires that repl.mu is exclusively locked. requestLeaseAsync
	// also requires this lock to send results on all waiter channels. This means that
	// no results will be sent until we've release the lock, so there's no race between
	// adding our new channel to p.llHandles below and requestLeaseAsync sending results
	// on all channels in p.llHandles. The same logic applies to p.nextLease.
	p.llHandles[llHandle] = struct{}{}
	p.nextLease = reqLease
	return llHandle
}

// requestLeaseAsync sends a transfer lease or lease request to the
// specified replica. The request is sent in an async task.
//
// The status argument is used as the expected value for liveness operations.
// reqLease and leaseReq must be consistent with the LeaseStatus.
func (p *pendingLeaseRequest) requestLeaseAsync(
	parentCtx context.Context,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	reqLease roachpb.Lease,
	status kvserverpb.LeaseStatus,
	leaseReq roachpb.Request,
) error {
	// Create a new context. We multiplex the cancellation of all contexts onto
	// this new one, canceling it if all coalesced requests timeout/cancel.
	// p.cancelLocked (defined below) is the cancel function that must be called;
	// calling just cancel is insufficient.
	ctx := p.repl.AnnotateCtx(context.Background())

	const opName = "request range lease"
	tr := p.repl.AmbientContext.Tracer
	tagsOpt := tracing.WithLogTags(logtags.FromContext(parentCtx))
	var sp *tracing.Span
	if parentSp := tracing.SpanFromContext(parentCtx); parentSp != nil {
		// We use FollowsFrom because the lease request's span can outlive the
		// parent request. This is possible if parentCtx is canceled after others
		// have coalesced on to this lease request (see leaseRequestHandle.Cancel).
		ctx, sp = tr.StartSpanCtx(
			ctx,
			opName,
			tracing.WithParent(parentSp),
			tracing.WithFollowsFrom(),
			tagsOpt,
		)
	} else {
		ctx, sp = tr.StartSpanCtx(ctx, opName, tagsOpt)
	}

	ctx, cancel := context.WithCancel(ctx)

	// Make sure we clean up the context and request state. This will be called
	// either when the request completes cleanly or when it is terminated early.
	p.cancelLocked = func() {
		cancel()
		p.cancelLocked = nil
		p.nextLease = roachpb.Lease{}
	}

	err := p.repl.store.Stopper().RunAsyncTaskEx(
		ctx,
		stop.TaskOpts{
			TaskName: "pendingLeaseRequest: requesting lease",
			// Trace the lease acquisition as a child even though it might outlive the
			// parent in case the parent's ctx is canceled. Other requests might
			// later block on this lease acquisition too, and we can't include the
			// acquisition's trace in all of them, but let's at least include it in
			// the request that triggered it.
			SpanOpt: stop.ChildSpan,
		},
		func(ctx context.Context) {
			defer sp.Finish()

			err := p.requestLease(ctx, nextLeaseHolder, reqLease, status, leaseReq)
			// Error will be handled below.

			// We reset our state below regardless of whether we've gotten an error or
			// not, but note that an error is ambiguous - there's no guarantee that the
			// transfer will not still apply. That's OK, however, as the "in transfer"
			// state maintained by the pendingLeaseRequest is not relied on for
			// correctness (see repl.mu.minLeaseProposedTS), and resetting the state
			// is beneficial as it'll allow the replica to attempt to transfer again or
			// extend the existing lease in the future.
			p.repl.mu.Lock()
			defer p.repl.mu.Unlock()
			if ctx.Err() != nil {
				// We were canceled and this request was already cleaned up
				// under lock. At this point, another async request could be
				// active so we don't want to do anything else.
				return
			}
			// Send result of lease to all waiter channels and cleanup request.
			for llHandle := range p.llHandles {
				// Don't send the same transaction object twice; this can lead to races.
				if err != nil {
					pErr := roachpb.NewError(err)
					// TODO(tbg): why?
					pErr.SetTxn(pErr.GetTxn())
					llHandle.resolve(pErr)
				} else {
					llHandle.resolve(nil)
				}
				delete(p.llHandles, llHandle)
			}
			p.cancelLocked()
		})
	if err != nil {
		p.cancelLocked()
		sp.Finish()
		return err
	}
	return nil
}

// requestLease sends a synchronous transfer lease or lease request to the
// specified replica. It is only meant to be called from requestLeaseAsync,
// since it does not coordinate with other in-flight lease requests.
func (p *pendingLeaseRequest) requestLease(
	ctx context.Context,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	reqLease roachpb.Lease,
	status kvserverpb.LeaseStatus,
	leaseReq roachpb.Request,
) error {
	// If requesting an epoch-based lease & current state is expired,
	// potentially heartbeat our own liveness or increment epoch of
	// prior owner. Note we only do this if the previous lease was
	// epoch-based.
	if reqLease.Type() == roachpb.LeaseEpoch && status.State == kvserverpb.LeaseState_EXPIRED &&
		status.Lease.Type() == roachpb.LeaseEpoch {
		var err error
		// If this replica is previous & next lease holder, manually heartbeat to become live.
		if status.OwnedBy(nextLeaseHolder.StoreID) &&
			p.repl.store.StoreID() == nextLeaseHolder.StoreID {
			if err = p.repl.store.cfg.NodeLiveness.Heartbeat(ctx, status.Liveness); err != nil {
				log.Errorf(ctx, "failed to heartbeat own liveness record: %s", err)
			}
		} else if status.Liveness.Epoch == status.Lease.Epoch {
			// If not owner, increment epoch if necessary to invalidate lease.
			// However, we only do so in the event that the next leaseholder is
			// considered live at this time. If not, there's no sense in
			// incrementing the expired leaseholder's epoch.
			if live, liveErr := p.repl.store.cfg.NodeLiveness.IsLive(nextLeaseHolder.NodeID); !live || liveErr != nil {
				if liveErr != nil {
					err = errors.Wrapf(liveErr, "not incrementing epoch on n%d because next leaseholder (n%d) not live",
						status.Liveness.NodeID, nextLeaseHolder.NodeID)
				} else {
					err = errors.Errorf("not incrementing epoch on n%d because next leaseholder (n%d) not live (err = nil)",
						status.Liveness.NodeID, nextLeaseHolder.NodeID)
				}
				log.VEventf(ctx, 1, "%v", err)
			} else if err = p.repl.store.cfg.NodeLiveness.IncrementEpoch(ctx, status.Liveness); err != nil {
				// If we get ErrEpochAlreadyIncremented, someone else beat
				// us to it. This proves that the target node is truly
				// dead *now*, but it doesn't prove that it was dead at
				// status.Timestamp (which we've encoded into our lease
				// request). It's possible that the node was temporarily
				// considered dead but revived without having its epoch
				// incremented, i.e. that it was in fact live at
				// status.Timestamp.
				//
				// It would be incorrect to simply proceed to sending our
				// lease request since our lease.Start may precede the
				// effective end timestamp of the predecessor lease (the
				// expiration of the last successful heartbeat before the
				// epoch increment), and so under this lease this node's
				// timestamp cache would not necessarily reflect all reads
				// served by the prior leaseholder.
				//
				// It would be correct to bump the timestamp in the lease
				// request and proceed, but that just sets up another race
				// between this node and the one that already incremented
				// the epoch. They're probably going to beat us this time
				// too, so just return the NotLeaseHolderError here
				// instead of trying to fix up the timestamps and submit
				// the lease request.
				//
				// ErrEpochAlreadyIncremented is not an unusual situation,
				// so we don't log it as an error.
				//
				// https://github.com/cockroachdb/cockroach/issues/35986
				if !errors.Is(err, liveness.ErrEpochAlreadyIncremented) {
					log.Errorf(ctx, "failed to increment leaseholder's epoch: %s", err)
				}
			}
		}
		if err != nil {
			// Return an NLHE with an empty lease, since we know the previous lease
			// isn't valid. In particular, if it was ours but we failed to reacquire
			// it (e.g. because our heartbeat failed due to a stalled disk) then we
			// don't want DistSender to retry us.
			return roachpb.NewNotLeaseHolderError(roachpb.Lease{}, p.repl.store.StoreID(), p.repl.Desc(),
				fmt.Sprintf("failed to manipulate liveness record: %s", err))
		}
	}

	// Send the RequestLeaseRequest or TransferLeaseRequest and wait for the new
	// lease to be applied.
	//
	// The Replica circuit breakers together with round-tripping a ProbeRequest
	// here before asking for the lease could provide an alternative, simpler
	// solution to the below issue:
	//
	// https://github.com/cockroachdb/cockroach/issues/37906
	ba := &roachpb.BatchRequest{}
	ba.Timestamp = p.repl.store.Clock().Now()
	ba.RangeID = p.repl.RangeID
	// NB:
	// RequestLease always bypasses the circuit breaker (i.e. will prefer to
	// get stuck on an unavailable range rather than failing fast; see
	// `(*RequestLeaseRequest).flags()`). This enables the caller to chose
	// between either behavior for themselves: if they too want to bypass
	// the circuit breaker, they simply don't check for the circuit breaker
	// while waiting for their lease handle. If they want to fail-fast, they
	// do. If the lease instead adopted the caller's preference, we'd have
	// to handle the case of multiple preferences joining onto one lease
	// request, which is more difficult.
	//
	// TransferLease will observe the circuit breaker, as transferring a
	// lease when the range is unavailable results in, essentially, giving
	// up on the lease and thus worsening the situation.
	ba.Add(leaseReq)
	_, pErr := p.repl.Send(ctx, ba)
	return pErr.GoError()
}

// JoinRequest adds one more waiter to the currently pending request.
// It is the caller's responsibility to ensure that there is a pending request,
// and that the request is compatible with whatever the caller is currently
// wanting to do (i.e. the request is naming the intended node as the next
// lease holder).
//
// Requires repl.mu is exclusively locked.
func (p *pendingLeaseRequest) JoinRequest() *leaseRequestHandle {
	llHandle := p.newHandle()
	if _, ok := p.RequestPending(); !ok {
		llHandle.resolve(roachpb.NewErrorf("no request in progress"))
		return llHandle
	}
	p.llHandles[llHandle] = struct{}{}
	return llHandle
}

// TransferInProgress returns whether the replica is in the process of
// transferring away its range lease. Note that the return values are
// best-effort and shouldn't be relied upon for correctness: if a previous
// transfer has returned an error, TransferInProgress will return `false`, but
// that doesn't necessarily mean that the transfer cannot still apply (see
// replica.mu.minLeaseProposedTS).
//
// It is assumed that the replica owning this pendingLeaseRequest owns the
// LeaderLease.
//
// replicaID is the ID of the parent replica.
//
// Requires repl.mu is read locked.
func (p *pendingLeaseRequest) TransferInProgress(replicaID roachpb.ReplicaID) bool {
	if nextLease, ok := p.RequestPending(); ok {
		// Is the lease being transferred? (as opposed to just extended)
		return replicaID != nextLease.Replica.ReplicaID
	}
	return false
}

// newHandle creates a new leaseRequestHandle referencing the pending lease
// request.
func (p *pendingLeaseRequest) newHandle() *leaseRequestHandle {
	return &leaseRequestHandle{
		p: p,
		c: make(chan *roachpb.Error, 1),
	}
}

// newResolvedHandle creates a new leaseRequestHandle referencing the pending
// lease request. It then resolves the handle with the provided error.
func (p *pendingLeaseRequest) newResolvedHandle(pErr *roachpb.Error) *leaseRequestHandle {
	h := p.newHandle()
	h.resolve(pErr)
	return h
}

// leaseStatus returns a lease status. The lease status is linked to the desire
// to serve a request at a specific timestamp (which may be a future timestamp)
// under the lease, as well as a notion of the current hlc time (now).
//
// # Explanation
//
// A status of ERROR indicates a failure to determine the correct lease status,
// and should not occur under normal operations. The caller's only recourse is
// to give up or to retry.
//
// If the lease is expired according to the now timestamp (and, in the case of
// epoch-based leases, the liveness epoch), a status of EXPIRED is returned.
// Note that this ignores the timestamp of the request, which may well
// technically be eligible to be served under the lease. The key feature of an
// EXPIRED status is that it reflects that a new lease with a start timestamp
// greater than or equal to now can be acquired non-cooperatively.
//
// If the lease is not EXPIRED, the request timestamp is taken into account. The
// expiration timestamp is adjusted for clock offset; if the request timestamp
// falls into the so-called "stasis period" at the end of the lifetime of the
// lease, or if the request timestamp is beyond the end of the lifetime of the
// lease, the status is UNUSABLE. Callers typically want to react to an UNUSABLE
// lease status by extending the lease, if they are in a position to do so.
//
// For request timestamps falling before the lease's stasis period, the lease's
// start timestamp is checked against the minProposedTimestamp. This timestamp
// indicates the oldest timestamp that a lease can have as its start time and
// still be used by the node. It is set both in cooperative lease transfers and
// to prevent reuse of leases across node restarts (which would result in
// latching violations). Leases with start times preceding this timestamp are
// assigned a status of PROSCRIBED and can not be not be used. Instead, a new
// lease should be acquired by callers.
//
// Finally, for requests timestamps falling before the stasis period of a lease
// that is not EXPIRED and also not PROSCRIBED, the status is VALID.
//
// # Implementation Note
//
// On the surface, it might seem like we could easily abandon the lease stasis
// concept in favor of consulting a request's uncertainty interval. We would
// then define a request's timestamp as the maximum of its read_timestamp and
// its global_uncertainty_limit, and simply check whether this timestamp falls
// below a lease's expiration. This could allow certain transactional requests
// to operate more closely to a lease's expiration. But not all requests that
// expect linearizability use an uncertainty interval (e.g. non-transactional
// requests), and so the lease stasis period serves as a kind of catch-all
// uncertainty interval for non-transactional and admin requests.
//
// Without that stasis period, the following linearizability violation could
// occur for two non-transactional requests operating on a single register
// during a lease change:
//
//   - a range lease gets committed on the new lease holder (but not the old).
//   - client proposes and commits a write on new lease holder (with a timestamp
//     just greater than the expiration of the old lease).
//   - client tries to read what it wrote, but hits a slow coordinator (which
//     assigns a timestamp covered by the old lease).
//   - the read is served by the old lease holder (which has not processed the
//     change in lease holdership).
//   - the client fails to read their own write.
func (r *Replica) leaseStatus(
	ctx context.Context,
	lease roachpb.Lease,
	now hlc.ClockTimestamp,
	minProposedTS hlc.ClockTimestamp,
	minValidObservedTS hlc.ClockTimestamp,
	reqTS hlc.Timestamp,
) kvserverpb.LeaseStatus {
	status := kvserverpb.LeaseStatus{
		Lease: lease,
		// NOTE: it would not be correct to accept either only the request time
		// or only the current time in this method, we need both. We need the
		// request time to determine whether the current lease can serve a given
		// request, even if that request has a timestamp in the future of
		// present time. We need the current time to distinguish between an
		// EXPIRED lease and an UNUSABLE lease. Only an EXPIRED lease can change
		// hands through a lease acquisition.
		Now:                       now,
		RequestTime:               reqTS,
		MinValidObservedTimestamp: minValidObservedTS,
	}
	var expiration hlc.Timestamp
	if lease.Type() == roachpb.LeaseExpiration {
		expiration = lease.GetExpiration()
	} else {
		l, ok := r.store.cfg.NodeLiveness.GetLiveness(lease.Replica.NodeID)
		status.Liveness = l.Liveness
		if !ok || status.Liveness.Epoch < lease.Epoch {
			// If lease validity can't be determined (e.g. gossip is down
			// and liveness info isn't available for owner), we can neither
			// use the lease nor do we want to attempt to acquire it.
			var msg redact.StringBuilder
			if !ok {
				msg.Printf("can't determine lease status of %s due to node liveness error: %v",
					lease.Replica, liveness.ErrRecordCacheMiss)
			} else {
				msg.Printf("can't determine lease status of %s because node liveness info for n%d is stale. lease: %s, liveness: %s",
					lease.Replica, lease.Replica.NodeID, lease, l.Liveness)
			}
			if leaseStatusLogLimiter.ShouldLog() {
				log.Infof(ctx, "%s", msg)
			}
			status.State = kvserverpb.LeaseState_ERROR
			status.ErrInfo = msg.String()
			return status
		}
		if status.Liveness.Epoch > lease.Epoch {
			status.State = kvserverpb.LeaseState_EXPIRED
			return status
		}
		expiration = status.Liveness.Expiration.ToTimestamp()
	}
	maxOffset := r.store.Clock().MaxOffset()
	stasis := expiration.Add(-int64(maxOffset), 0)
	ownedLocally := lease.OwnedBy(r.store.StoreID())
	if expiration.LessEq(now.ToTimestamp()) {
		status.State = kvserverpb.LeaseState_EXPIRED
	} else if stasis.LessEq(reqTS) {
		status.State = kvserverpb.LeaseState_UNUSABLE
	} else if ownedLocally && lease.ProposedTS != nil && lease.ProposedTS.Less(minProposedTS) {
		// If the replica owns the lease, additional verify that the lease's
		// proposed timestamp is not earlier than the min proposed timestamp.
		status.State = kvserverpb.LeaseState_PROSCRIBED
	} else {
		status.State = kvserverpb.LeaseState_VALID
	}
	return status
}

// CurrentLeaseStatus returns the status of the current lease for the
// current time.
//
// Common operations to perform on the resulting status are to check if
// it is valid using the IsValid method and to check whether the lease
// is held locally using the OwnedBy method.
//
// Note that this method does not check to see if a transfer is pending,
// but returns the status of the current lease and ownership at the
// specified point in time.
func (r *Replica) CurrentLeaseStatus(ctx context.Context) kvserverpb.LeaseStatus {
	return r.LeaseStatusAt(ctx, r.Clock().NowAsClockTimestamp())
}

// LeaseStatusAt is like CurrentLeaseStatus, but accepts a now timestamp.
func (r *Replica) LeaseStatusAt(
	ctx context.Context, now hlc.ClockTimestamp,
) kvserverpb.LeaseStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaseStatusAtRLocked(ctx, now)
}

func (r *Replica) leaseStatusAtRLocked(
	ctx context.Context, now hlc.ClockTimestamp,
) kvserverpb.LeaseStatus {
	return r.leaseStatusForRequestRLocked(ctx, now, hlc.Timestamp{})
}

func (r *Replica) leaseStatusForRequestRLocked(
	ctx context.Context, now hlc.ClockTimestamp, reqTS hlc.Timestamp,
) kvserverpb.LeaseStatus {
	if reqTS.IsEmpty() {
		// If the request timestamp is empty, return the status that
		// would be given to a request with a timestamp of now.
		reqTS = now.ToTimestamp()
	}
	return r.leaseStatus(ctx, *r.mu.state.Lease, now, r.mu.minLeaseProposedTS,
		r.mu.minValidObservedTimestamp, reqTS)
}

// OwnsValidLease returns whether this replica is the current valid
// leaseholder.
//
// Note that this method does not check to see if a transfer is pending,
// but returns the status of the current lease and ownership at the
// specified point in time.
func (r *Replica) OwnsValidLease(ctx context.Context, now hlc.ClockTimestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ownsValidLeaseRLocked(ctx, now)
}

func (r *Replica) ownsValidLeaseRLocked(ctx context.Context, now hlc.ClockTimestamp) bool {
	st := r.leaseStatusAtRLocked(ctx, now)
	return st.IsValid() && st.OwnedBy(r.store.StoreID())
}

// requiresExpiringLeaseRLocked returns whether this range unconditionally uses
// an expiration-based lease. Ranges located before or including the node
// liveness table must always use expiration leases to avoid circular
// dependencies on the node liveness table. All other ranges typically use
// epoch-based leases, but may temporarily use expiration based leases during
// lease transfers.
func (r *Replica) requiresExpiringLeaseRLocked() bool {
	return r.store.cfg.NodeLiveness == nil ||
		r.mu.state.Desc.StartKey.Less(roachpb.RKey(keys.NodeLivenessKeyMax))
}

// requestLeaseLocked executes a request to obtain or extend a lease
// asynchronously and returns a channel on which the result will be posted. If
// there's already a request in progress, we join in waiting for the results of
// that request. Unless an error is returned, the obtained lease will be valid
// for a time interval containing the requested timestamp.
func (r *Replica) requestLeaseLocked(
	ctx context.Context, status kvserverpb.LeaseStatus,
) *leaseRequestHandle {
	if r.store.TestingKnobs().LeaseRequestEvent != nil {
		if err := r.store.TestingKnobs().LeaseRequestEvent(status.Now.ToTimestamp(), r.StoreID(), r.GetRangeID()); err != nil {
			return r.mu.pendingLeaseRequest.newResolvedHandle(err)
		}
	}
	if pErr := r.store.TestingKnobs().PinnedLeases.rejectLeaseIfPinnedElsewhere(r); pErr != nil {
		return r.mu.pendingLeaseRequest.newResolvedHandle(pErr)
	}

	// Propose a Raft command to get a lease for this replica.
	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return r.mu.pendingLeaseRequest.newResolvedHandle(roachpb.NewError(err))
	}
	return r.mu.pendingLeaseRequest.InitOrJoinRequest(
		ctx, repDesc, status, r.mu.state.Desc.StartKey.AsRawKey(),
		false /* transfer */, false /* bypassSafetyChecks */)
}

// AdminTransferLease transfers the LeaderLease to another replica. Only the
// current holder of the LeaderLease can do a transfer, because it needs to stop
// serving reads and proposing Raft commands (CPut is a read) while evaluating
// and proposing the TransferLease request. This synchronization with all other
// requests on the leaseholder is enforced through latching. The TransferLease
// request grabs a write latch over all keys in the range.
//
// If the leaseholder did not respect latching and did not stop serving reads
// during the lease transfer, it would potentially serve reads with timestamps
// greater than the start timestamp of the new (transferred) lease, which is
// determined during the evaluation of the TransferLease request. More subtly,
// the replica can't even serve reads or propose commands with timestamps lower
// than the start of the new lease because it could lead to read your own write
// violations (see comments on the stasis period on leaseStatus). We could, in
// principle, serve reads more than the maximum clock offset in the past.
//
// The method waits for any in-progress lease extension to be done, and it also
// blocks until the transfer is done. If a transfer is already in progress, this
// method joins in waiting for it to complete if it's transferring to the same
// replica. Otherwise, a NotLeaseHolderError is returned.
//
// AdminTransferLease implements the ReplicaLeaseMover interface.
func (r *Replica) AdminTransferLease(
	ctx context.Context, target roachpb.StoreID, bypassSafetyChecks bool,
) error {
	// initTransferHelper inits a transfer if no extension is in progress.
	// It returns a channel for waiting for the result of a pending
	// extension (if any is in progress) and a channel for waiting for the
	// transfer (if it was successfully initiated).
	var nextLeaseHolder roachpb.ReplicaDescriptor
	initTransferHelper := func() (extension, transfer *leaseRequestHandle, err error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		now := r.store.Clock().NowAsClockTimestamp()
		status := r.leaseStatusAtRLocked(ctx, now)
		if status.Lease.OwnedBy(target) {
			// The target is already the lease holder. Nothing to do.
			return nil, nil, nil
		}
		desc := r.mu.state.Desc
		if !status.Lease.OwnedBy(r.store.StoreID()) {
			return nil, nil, roachpb.NewNotLeaseHolderError(status.Lease, r.store.StoreID(), desc,
				"can't transfer the lease because this store doesn't own it")
		}
		// Verify the target is a replica of the range.
		var ok bool
		if nextLeaseHolder, ok = desc.GetReplicaDescriptor(target); !ok {
			return nil, nil, errors.Errorf("unable to find store %d in range %+v", target, desc)
		}

		if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok &&
			nextLease.Replica != nextLeaseHolder {
			repDesc, err := r.getReplicaDescriptorRLocked()
			if err != nil {
				return nil, nil, err
			}
			if nextLease.Replica == repDesc {
				// There's an extension in progress. Let's wait for it to succeed and
				// try again.
				return r.mu.pendingLeaseRequest.JoinRequest(), nil, nil
			}
			// Another transfer is in progress, and it's not transferring to the
			// same replica we'd like.
			return nil, nil, roachpb.NewNotLeaseHolderError(nextLease, r.store.StoreID(), desc,
				"another transfer to a different store is in progress")
		}

		// Verify that the lease transfer would be safe. This check is best-effort
		// in that it can race with Raft leadership changes and log truncation. See
		// propBuf.maybeRejectUnsafeProposalLocked for a non-racy version of this
		// check, along with a full explanation of why it is important. We include
		// both because rejecting a lease transfer in the propBuf after we have
		// revoked our current lease is more disruptive than doing so here, before
		// we have revoked our current lease.
		raftStatus := r.raftStatusRLocked()
		raftFirstIndex := r.raftFirstIndexRLocked()
		snapStatus := raftutil.ReplicaMayNeedSnapshot(raftStatus, raftFirstIndex, nextLeaseHolder.ReplicaID)
		if snapStatus != raftutil.NoSnapshotNeeded && !bypassSafetyChecks {
			r.store.metrics.LeaseTransferErrorCount.Inc(1)
			log.VEventf(ctx, 2, "not initiating lease transfer because the target %s may "+
				"need a snapshot: %s", nextLeaseHolder, snapStatus)
			err := NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(nextLeaseHolder, snapStatus)
			return nil, nil, err
		}

		transfer = r.mu.pendingLeaseRequest.InitOrJoinRequest(
			ctx, nextLeaseHolder, status, desc.StartKey.AsRawKey(), true /* transfer */, bypassSafetyChecks,
		)
		return nil, transfer, nil
	}

	// Before transferring a lease, we ensure that the lease transfer is safe. If
	// the leaseholder cannot guarantee this, we reject the lease transfer. To
	// make such a claim, the leaseholder needs to become the Raft leader and
	// probe the lease target's log. Doing so may take time, so we use a small
	// exponential backoff loop with a maximum retry count before returning the
	// rejection to the client. As configured, this retry loop should back off
	// for about 6 seconds before returning an error.
	retryOpts := retry.Options{
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Multiplier:     2,
		MaxRetries:     10,
	}
	if count := r.store.TestingKnobs().LeaseTransferRejectedRetryLoopCount; count != 0 {
		retryOpts.MaxRetries = count
	}
	transferRejectedRetry := retry.StartWithCtx(ctx, retryOpts)
	transferRejectedRetry.Next() // The first call to Next does not block.

	// Loop while there's an extension in progress.
	for {
		// See if there's an extension in progress that we have to wait for.
		// If there isn't, request a transfer.
		extension, transfer, err := initTransferHelper()
		if err != nil {
			if IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(err) && transferRejectedRetry.Next() {
				// If the lease transfer was rejected because the target may need a
				// snapshot, try again. After the backoff, we may have become the Raft
				// leader (through maybeTransferRaftLeadershipToLeaseholderLocked) or
				// may have learned more about the state of the lease target's log.
				log.VEventf(ctx, 2, "retrying lease transfer to store %d after rejection", target)
				continue
			}
			return err
		}
		if extension == nil {
			if transfer == nil {
				// The target is us and we're the lease holder.
				return nil
			}
			select {
			case pErr := <-transfer.C():
				return pErr.GoError()
			case <-ctx.Done():
				transfer.Cancel()
				return ctx.Err()
			}
		}
		// Wait for the in-progress extension without holding the mutex.
		if r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent != nil {
			r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent(nextLeaseHolder)
		}
		select {
		case <-extension.C():
			continue
		case <-ctx.Done():
			extension.Cancel()
			return ctx.Err()
		}
	}
}

// GetLease returns the lease and, if available, the proposed next lease.
func (r *Replica) GetLease() (roachpb.Lease, roachpb.Lease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getLeaseRLocked()
}

func (r *Replica) getLeaseRLocked() (roachpb.Lease, roachpb.Lease) {
	if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return *r.mu.state.Lease, nextLease
	}
	return *r.mu.state.Lease, roachpb.Lease{}
}

// RevokeLease stops the replica from using its current lease, if that lease
// matches the provided lease sequence. All future calls to leaseStatus on this
// node with the current lease will now return a PROSCRIBED status.
func (r *Replica) RevokeLease(ctx context.Context, seq roachpb.LeaseSequence) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.state.Lease.Sequence == seq {
		r.mu.minLeaseProposedTS = r.Clock().NowAsClockTimestamp()
	}
}

// NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError return an error
// indicating that a lease transfer failed because the current leaseholder could
// not prove that the lease transfer target did not need a Raft snapshot.
func NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(
	target roachpb.ReplicaDescriptor, snapStatus raftutil.ReplicaNeedsSnapshotStatus,
) error {
	err := errors.Errorf("refusing to transfer lease to %d because target may need a Raft snapshot: %s",
		target, snapStatus)
	return errors.Mark(err, errMarkLeaseTransferRejectedBecauseTargetMayNeedSnapshot)
}

// checkRequestTimeRLocked checks that the provided request timestamp is not
// too far in the future. We define "too far" as a time that would require a
// lease extension even if we were perfectly proactive about extending our
// lease asynchronously to always ensure at least a "leaseRenewal" duration
// worth of runway. Doing so ensures that we detect client behavior that
// will inevitably run into frequent synchronous lease extensions.
//
// This serves as a stricter version of a check that if we were to perform
// a lease extension at now, the request would be contained within the new
// lease's expiration (and stasis period).
func (r *Replica) checkRequestTimeRLocked(now hlc.ClockTimestamp, reqTS hlc.Timestamp) error {
	var leaseRenewal time.Duration
	if r.requiresExpiringLeaseRLocked() {
		_, leaseRenewal = r.store.cfg.RangeLeaseDurations()
	} else {
		_, leaseRenewal = r.store.cfg.NodeLivenessDurations()
	}
	leaseRenewalMinusStasis := leaseRenewal - r.store.Clock().MaxOffset()
	if leaseRenewalMinusStasis < 0 {
		// If maxOffset > leaseRenewal, such that present time operations risk
		// ending up in the stasis period, allow requests up to clock.Now(). Can
		// happen in tests.
		leaseRenewalMinusStasis = 0
	}
	maxReqTS := now.ToTimestamp().Add(leaseRenewalMinusStasis.Nanoseconds(), 0)
	if maxReqTS.Less(reqTS) {
		return errors.Errorf("request timestamp %s too far in future (> %s)", reqTS, maxReqTS)
	}
	return nil
}

// leaseGoodToGoRLocked verifies that the replica has a lease that is
// valid, owned by the current replica, and usable to serve requests at
// the specified timestamp. The method will return the lease status if
// these conditions are satisfied or an error if they are unsatisfied.
// The lease status is either empty or fully populated.
//
// Latches must be acquired on the range before calling this method.
// This ensures that callers are properly sequenced with TransferLease
// requests, which declare a conflict with all other commands.
//
// The method can has four possible outcomes:
//
// (1) the request timestamp is too far in the future. In this case,
//
//	a nonstructured error is returned. This shouldn't happen.
//
// (2) the lease is invalid or otherwise unable to serve a request at
//
//	the specified timestamp. In this case, an InvalidLeaseError is
//	returned, which is caught in executeBatchWithConcurrencyRetries
//	and used to trigger a lease acquisition/extension.
//
// (3) the lease is valid but held by a different replica. In this case,
//
//	a NotLeaseHolderError is returned, which is propagated back up to
//	the DistSender and triggers a redirection of the request.
//
// (4) the lease is valid, held locally, and capable of serving the
//
//	given request. In this case, no error is returned.
//
// In addition to the lease status, the method also returns whether the
// lease should be considered for extension using maybeExtendLeaseAsync
// after the replica's read lock has been dropped.
func (r *Replica) leaseGoodToGoRLocked(
	ctx context.Context, now hlc.ClockTimestamp, reqTS hlc.Timestamp,
) (_ kvserverpb.LeaseStatus, shouldExtend bool, _ error) {
	st := r.leaseStatusForRequestRLocked(ctx, now, reqTS)
	shouldExtend, err := r.leaseGoodToGoForStatusRLocked(ctx, now, reqTS, st)
	if err != nil {
		return kvserverpb.LeaseStatus{}, false, err
	}
	return st, shouldExtend, err
}

func (r *Replica) leaseGoodToGoForStatusRLocked(
	ctx context.Context, now hlc.ClockTimestamp, reqTS hlc.Timestamp, st kvserverpb.LeaseStatus,
) (shouldExtend bool, _ error) {
	if err := r.checkRequestTimeRLocked(now, reqTS); err != nil {
		// Case (1): invalid request.
		return false, err
	}
	if !st.IsValid() {
		// Case (2): invalid lease.
		return false, &roachpb.InvalidLeaseError{}
	}
	if !st.Lease.OwnedBy(r.store.StoreID()) {
		// Case (3): not leaseholder.
		_, stillMember := r.mu.state.Desc.GetReplicaDescriptor(st.Lease.Replica.StoreID)
		if !stillMember {
			// This would be the situation in which the lease holder gets removed when
			// holding the lease, or in which a lease request erroneously gets accepted
			// for a replica that is not in the replica set. Neither of the two can
			// happen in normal usage since appropriate mechanisms have been added:
			//
			// 1. Only the lease holder (at the time) schedules removal of a replica,
			// but the lease can change hands and so the situation in which a follower
			// coordinates a replica removal of the (new) lease holder is possible (if
			// unlikely) in practice. In this situation, the new lease holder would at
			// some point be asked to propose the replica change's EndTxn to Raft. A
			// check has been added that prevents proposals that amount to the removal
			// of the proposer's (and hence lease holder's) Replica, preventing this
			// scenario.
			//
			// 2. A lease is accepted for a Replica that has been removed. Without
			// precautions, this could happen because lease requests are special in
			// that they are the only command that is proposed on a follower (other
			// commands may be proposed from followers, but not successfully so). For
			// all proposals, processRaftCommand checks that their ProposalLease is
			// compatible with the active lease for the log position. For commands
			// proposed on the lease holder, the spanlatch manager then serializes
			// everything. But lease requests get created on followers based on their
			// local state and thus without being sequenced through latching. Thus
			// a recently removed follower (unaware of its own removal) could submit
			// a proposal for the lease (correctly using as a ProposerLease the last
			// active lease), and would receive it given the up-to-date ProposerLease.
			// Hence, an extra check is in order: processRaftCommand makes sure that
			// lease requests for a replica not in the descriptor are bounced.
			//
			// However, this is possible if the `cockroach debug recover` command has
			// been used, so this is just a logged error instead of a fatal assertion.
			log.Errorf(ctx, "lease %s owned by replica %+v that no longer exists",
				st.Lease, st.Lease.Replica)
		}
		// Otherwise, if the lease is currently held by another replica, redirect
		// to the holder.
		return false, roachpb.NewNotLeaseHolderError(
			st.Lease, r.store.StoreID(), r.descRLocked(), "lease held by different store",
		)
	}
	// Case (4): all good.
	return r.shouldExtendLeaseRLocked(st), nil
}

// leaseGoodToGo is like leaseGoodToGoRLocked, but will acquire the replica read
// lock.
func (r *Replica) leaseGoodToGo(
	ctx context.Context, now hlc.ClockTimestamp, reqTS hlc.Timestamp,
) (_ kvserverpb.LeaseStatus, shouldExtend bool, _ error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaseGoodToGoRLocked(ctx, now, reqTS)
}

// redirectOnOrAcquireLease checks whether this replica has the lease at
// the current timestamp. If it does, returns the lease and its status.
// If another replica currently holds the lease, redirects by returning
// NotLeaseHolderError and an empty lease status.
//
// If the lease is expired, a renewal is synchronously requested.
// Expiration-based leases are eagerly renewed when a request with a
// timestamp within RangeLeaseRenewalDuration of the lease expiration is
// served.
//
// TODO(spencer): for write commands, don't wait while requesting
//
//	the range lease. If the lease acquisition fails, the write cmd
//	will fail as well. If it succeeds, as is likely, then the write
//	will not incur latency waiting for the command to complete.
//	Reads, however, must wait.
func (r *Replica) redirectOnOrAcquireLease(
	ctx context.Context,
) (kvserverpb.LeaseStatus, *roachpb.Error) {
	return r.redirectOnOrAcquireLeaseForRequest(ctx, hlc.Timestamp{}, r.breaker.Signal())
}

// TestingAcquireLease is redirectOnOrAcquireLease exposed for tests.
func (r *Replica) TestingAcquireLease(ctx context.Context) (kvserverpb.LeaseStatus, error) {
	ctx = r.AnnotateCtx(ctx)
	ctx = logtags.AddTag(ctx, "lease-acq", nil)
	l, pErr := r.redirectOnOrAcquireLease(ctx)
	return l, pErr.GoError()
}

// redirectOnOrAcquireLeaseForRequest is like redirectOnOrAcquireLease,
// but it accepts a specific request timestamp instead of assuming that
// the request is operating at the current time.
func (r *Replica) redirectOnOrAcquireLeaseForRequest(
	ctx context.Context, reqTS hlc.Timestamp, brSig signaller,
) (status kvserverpb.LeaseStatus, pErr *roachpb.Error) {
	// Does not use RunWithTimeout(), because we do not want to mask the
	// NotLeaseHolderError on context cancellation.
	ctx, cancel := context.WithTimeout(ctx, r.store.cfg.RangeLeaseAcquireTimeout()) // nolint:context
	defer cancel()

	// Try fast-path.
	now := r.store.Clock().NowAsClockTimestamp()
	{
		status, shouldExtend, err := r.leaseGoodToGo(ctx, now, reqTS)
		if err == nil {
			if shouldExtend {
				r.maybeExtendLeaseAsync(ctx, status)
			}
			return status, nil
		} else if !errors.HasType(err, (*roachpb.InvalidLeaseError)(nil)) {
			return kvserverpb.LeaseStatus{}, roachpb.NewError(err)
		}
	}

	if err := brSig.Err(); err != nil {
		return kvserverpb.LeaseStatus{}, roachpb.NewError(err)
	}

	// Loop until the lease is held or the replica ascertains the actual lease
	// holder. Returns also on context.Done() (timeout or cancellation).
	for attempt := 1; ; attempt++ {
		now = r.store.Clock().NowAsClockTimestamp()
		llHandle, status, transfer, pErr := func() (*leaseRequestHandle, kvserverpb.LeaseStatus, bool, *roachpb.Error) {
			r.mu.Lock()
			defer r.mu.Unlock()

			// Check that we're not in the process of transferring the lease
			// away. If we are doing so, we can't serve reads or propose Raft
			// commands - see comments on AdminTransferLease and TransferLease.
			// So wait on the lease transfer to complete either successfully or
			// unsuccessfully before redirecting or retrying.
			repDesc, err := r.getReplicaDescriptorRLocked()
			if err != nil {
				return nil, kvserverpb.LeaseStatus{}, false, roachpb.NewError(err)
			}
			if ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); ok {
				return r.mu.pendingLeaseRequest.JoinRequest(), kvserverpb.LeaseStatus{}, true /* transfer */, nil
			}

			status := r.leaseStatusForRequestRLocked(ctx, now, reqTS)
			switch status.State {
			case kvserverpb.LeaseState_ERROR:
				// Lease state couldn't be determined.
				msg := status.ErrInfo
				if msg == "" {
					msg = "lease state could not be determined"
				}
				log.VEventf(ctx, 2, "%s", msg)
				return nil, kvserverpb.LeaseStatus{}, false, roachpb.NewError(
					roachpb.NewNotLeaseHolderError(roachpb.Lease{}, r.store.StoreID(), r.mu.state.Desc, msg))

			case kvserverpb.LeaseState_VALID, kvserverpb.LeaseState_UNUSABLE:
				if !status.Lease.OwnedBy(r.store.StoreID()) {
					_, stillMember := r.mu.state.Desc.GetReplicaDescriptor(status.Lease.Replica.StoreID)
					if !stillMember {
						// See corresponding comment in leaseGoodToGoRLocked.
						log.Errorf(ctx, "lease %s owned by replica %+v that no longer exists",
							status.Lease, status.Lease.Replica)
					}
					// Otherwise, if the lease is currently held by another replica, redirect
					// to the holder.
					return nil, kvserverpb.LeaseStatus{}, false, roachpb.NewError(
						roachpb.NewNotLeaseHolderError(status.Lease, r.store.StoreID(), r.mu.state.Desc,
							"lease held by different store"))
				}

				// If the lease is in stasis, we can't serve requests until we've
				// renewed the lease, so we return the handle to block on renewal.
				if status.State == kvserverpb.LeaseState_UNUSABLE {
					return r.requestLeaseLocked(ctx, status), kvserverpb.LeaseStatus{}, false, nil
				}

				// Return a nil handle and status to signal that we have a valid lease.
				return nil, status, false, nil

			case kvserverpb.LeaseState_EXPIRED:
				// No active lease: Request renewal if a renewal is not already pending.
				log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
				return r.requestLeaseLocked(ctx, status), kvserverpb.LeaseStatus{}, false, nil

			case kvserverpb.LeaseState_PROSCRIBED:
				// Lease proposed timestamp is earlier than the min proposed
				// timestamp limit this replica must observe. If this store
				// owns the lease, re-request. Otherwise, redirect.
				if status.Lease.OwnedBy(r.store.StoreID()) {
					log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
					return r.requestLeaseLocked(ctx, status), kvserverpb.LeaseStatus{}, false, nil
				}
				// If lease is currently held by another, redirect to holder.
				return nil, kvserverpb.LeaseStatus{}, false, roachpb.NewError(
					roachpb.NewNotLeaseHolderError(status.Lease, r.store.StoreID(), r.mu.state.Desc, "lease proscribed"))

			default:
				return nil, kvserverpb.LeaseStatus{}, false, roachpb.NewErrorf("unknown lease status state %v", status)
			}
		}()
		if pErr != nil {
			return kvserverpb.LeaseStatus{}, pErr
		}
		if llHandle == nil {
			// We own a valid lease.
			log.Eventf(ctx, "valid lease %+v", status)
			return status, nil
		}

		// Wait for the range lease acquisition/transfer to finish, or the
		// context to expire.
		//
		// Note that even if the operation completes successfully, we can't
		// assume that we have the lease. This is clearly not the case when
		// waiting on a lease transfer and also not the case if our request
		// timestamp is not covered by the new lease (though we try to protect
		// against this in checkRequestTimeRLocked). So instead of assuming
		// anything, we iterate and check again.
		pErr = func() (pErr *roachpb.Error) {
			slowTimer := timeutil.NewTimer()
			defer slowTimer.Stop()
			slowTimer.Reset(base.SlowRequestThreshold)
			tBegin := timeutil.Now()
			for {
				select {
				case pErr = <-llHandle.C():
					if transfer {
						// We were waiting on a transfer to finish. Ignore its
						// result and try again.
						return nil
					}

					if pErr != nil {
						goErr := pErr.GoError()
						switch {
						case errors.HasType(goErr, (*roachpb.AmbiguousResultError)(nil)):
							// This can happen if the RequestLease command we sent has been
							// applied locally through a snapshot: the RequestLeaseRequest
							// cannot be reproposed so we get this ambiguity.
							// We'll just loop around.
							return nil
						case errors.HasType(goErr, (*roachpb.LeaseRejectedError)(nil)):
							var tErr *roachpb.LeaseRejectedError
							errors.As(goErr, &tErr)
							if tErr.Existing.OwnedBy(r.store.StoreID()) {
								// The RequestLease command we sent was rejected because another
								// lease was applied in the meantime, but we own that other
								// lease. So, loop until the current node becomes aware that
								// it's the leaseholder.
								return nil
							}

							// Getting a LeaseRejectedError back means someone else got there
							// first, or the lease request was somehow invalid due to a concurrent
							// change. That concurrent change could have been that this replica was
							// removed (see processRaftCommand), so check for that case before
							// falling back to a NotLeaseHolderError.
							var err error
							if _, descErr := r.GetReplicaDescriptor(); descErr != nil {
								err = descErr
							} else if st := r.CurrentLeaseStatus(ctx); !st.IsValid() {
								err = roachpb.NewNotLeaseHolderError(roachpb.Lease{}, r.store.StoreID(), r.Desc(),
									"lease acquisition attempt lost to another lease, which has expired in the meantime")
							} else {
								err = roachpb.NewNotLeaseHolderError(st.Lease, r.store.StoreID(), r.Desc(),
									"lease acquisition attempt lost to another lease")
							}
							pErr = roachpb.NewError(err)
						}
						return pErr
					}
					log.VEventf(ctx, 2, "lease acquisition succeeded: %+v", status.Lease)
					return nil
				case <-brSig.C():
					llHandle.Cancel()
					err := brSig.Err()
					log.VErrEventf(ctx, 2, "lease acquisition failed: %s", err)
					return roachpb.NewError(err)
				case <-slowTimer.C:
					slowTimer.Read = true
					log.Warningf(ctx, "have been waiting %s attempting to acquire lease (%d attempts)",
						base.SlowRequestThreshold, attempt)
					r.store.metrics.SlowLeaseRequests.Inc(1)
					defer func(attempt int) {
						r.store.metrics.SlowLeaseRequests.Dec(1)
						log.Infof(ctx, "slow lease acquisition finished after %s with error %v after %d attempts", timeutil.Since(tBegin), pErr, attempt)
					}(attempt)
				case <-ctx.Done():
					llHandle.Cancel()
					log.VErrEventf(ctx, 2, "lease acquisition failed: %s", ctx.Err())
					return roachpb.NewError(roachpb.NewNotLeaseHolderError(roachpb.Lease{}, r.store.StoreID(), r.Desc(),
						"lease acquisition canceled because context canceled"))
				case <-r.store.Stopper().ShouldQuiesce():
					llHandle.Cancel()
					return roachpb.NewError(roachpb.NewNotLeaseHolderError(roachpb.Lease{}, r.store.StoreID(), r.Desc(),
						"lease acquisition canceled because node is stopping"))
				}
			}
		}()
		if pErr != nil {
			return kvserverpb.LeaseStatus{}, pErr
		}
		// Retry...
	}
}

// shouldExtendLeaseRLocked determines whether the lease should be
// extended asynchronously, even if it is currently valid. The method
// returns true if this range uses expiration-based leases, the lease is
// in need of renewal, and there's not already an extension pending.
func (r *Replica) shouldExtendLeaseRLocked(st kvserverpb.LeaseStatus) bool {
	if st.Lease.Type() != roachpb.LeaseExpiration {
		return false
	}
	if _, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return false
	}
	renewal := st.Lease.Expiration.Add(-r.store.cfg.RangeLeaseRenewalDuration().Nanoseconds(), 0)
	return renewal.LessEq(st.Now.ToTimestamp())
}

// maybeExtendLeaseAsync attempts to extend the expiration-based lease
// asynchronously, if doing so is deemed beneficial by an earlier call
// to shouldExtendLeaseRLocked.
func (r *Replica) maybeExtendLeaseAsync(ctx context.Context, st kvserverpb.LeaseStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.maybeExtendLeaseAsyncLocked(ctx, st)
}

func (r *Replica) maybeExtendLeaseAsyncLocked(ctx context.Context, st kvserverpb.LeaseStatus) {
	// Check shouldExtendLeaseRLocked again, because others may have raced to
	// extend the lease and beaten us here after we made the determination
	// (under a shared lock) that the extension was needed.
	if !r.shouldExtendLeaseRLocked(st) {
		return
	}
	if log.ExpensiveLogEnabled(ctx, 2) {
		log.Infof(ctx, "extending lease %s at %s", st.Lease, st.Now)
	}
	// We explicitly ignore the returned handle as we won't block on it.
	//
	// TODO(tbg): this ctx is likely cancelled very soon, which will in turn
	// cancel the lease acquisition (unless joined by another more long-lived
	// ctx). So this possibly isn't working as advertised (which only plays a role
	// for expiration-based leases, at least).
	_ = r.requestLeaseLocked(ctx, st)
}

// leaseViolatesPreferences checks if current replica owns the lease and if it
// violates the lease preferences defined in the span config. If there is an
// error or no preferences defined then it will return false and consider that
// to be in-conformance.
func (r *Replica) leaseViolatesPreferences(ctx context.Context) bool {
	storeDesc, err := r.store.Descriptor(ctx, true /* useCached */)
	if err != nil {
		log.Infof(ctx, "Unable to load the descriptor %v: cannot check if lease violates preference", err)
		return false
	}
	conf := r.SpanConfig()
	if len(conf.LeasePreferences) == 0 {
		return false
	}
	for _, preference := range conf.LeasePreferences {
		if constraint.ConjunctionsCheck(*storeDesc, preference.Constraints) {
			return false
		}
	}
	// We have at lease one preference set up, but we don't satisfy any.
	return true
}
