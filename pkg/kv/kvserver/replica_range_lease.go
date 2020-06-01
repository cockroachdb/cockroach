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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/opentracing/opentracing-go"
)

var leaseStatusLogLimiter = log.Every(5 * time.Second)

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

	// No request in progress. Let's propose a Lease command asynchronously.
	llHandle := p.newHandle()
	reqHeader := roachpb.RequestHeader{
		Key: startKey,
	}
	var leaseReq roachpb.Request
	now := p.repl.store.Clock().Now()
	reqLease := roachpb.Lease{
		// It's up to us to ensure that Lease.Start is greater than the
		// end time of the previous lease. This means that if status
		// refers to an expired epoch lease, we must increment the epoch
		// *at status.Timestamp* before we can propose this lease.
		//
		// Note that the server may decrease our proposed start time if it
		// decides that it is safe to do so (for example, this happens
		// when renewing an expiration-based lease), but it will never
		// increase it (and a start timestamp that is too low is unsafe
		// because it results in incorrect initialization of the timestamp
		// cache on the new leaseholder).
		Start:      status.Timestamp,
		Replica:    nextLeaseHolder,
		ProposedTS: &now,
	}

	if p.repl.requiresExpiringLeaseRLocked() {
		reqLease.Expiration = &hlc.Timestamp{}
		*reqLease.Expiration = status.Timestamp.Add(int64(p.repl.store.cfg.RangeLeaseActiveDuration()), 0)
	} else {
		// Get the liveness for the next lease holder and set the epoch in the lease request.
		liveness, err := p.repl.store.cfg.NodeLiveness.GetLiveness(nextLeaseHolder.NodeID)
		if err != nil {
			llHandle.resolve(roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  status.Lease,
				Requested: reqLease,
				Message:   fmt.Sprintf("couldn't request lease for %+v: %v", nextLeaseHolder, err),
			}))
			return llHandle
		}
		reqLease.Epoch = liveness.Epoch
	}

	if transfer {
		leaseReq = &roachpb.TransferLeaseRequest{
			RequestHeader: reqHeader,
			Lease:         reqLease,
			PrevLease:     status.Lease,
		}
	} else {
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
			newNotLeaseHolderError(nil, p.repl.store.StoreID(), p.repl.mu.state.Desc)))
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
	const opName = "request range lease"
	var sp opentracing.Span
	tr := p.repl.AmbientContext.Tracer
	if parentSp := opentracing.SpanFromContext(parentCtx); parentSp != nil {
		// We use FollowsFrom because the lease request's span can outlive the
		// parent request. This is possible if parentCtx is canceled after others
		// have coalesced on to this lease request (see leaseRequestHandle.Cancel).
		// TODO(andrei): we should use Tracer.StartChildSpan() for efficiency,
		// except that one does not currently support FollowsFrom relationships.
		sp = tr.StartSpan(
			opName,
			opentracing.FollowsFrom(parentSp.Context()),
			tracing.LogTagsFromCtx(parentCtx),
		)
	} else {
		sp = tr.(*tracing.Tracer).StartRootSpan(
			opName, logtags.FromContext(parentCtx), tracing.NonRecordableSpan)
	}

	// Create a new context *without* a timeout. Instead, we multiplex the
	// cancellation of all contexts onto this new one, only canceling it if all
	// coalesced requests timeout/cancel. p.cancelLocked (defined below) is the
	// cancel function that must be called; calling just cancel is insufficient.
	ctx := p.repl.AnnotateCtx(context.Background())
	ctx = opentracing.ContextWithSpan(ctx, sp)
	ctx, cancel := context.WithCancel(ctx)

	// Make sure we clean up the context and request state. This will be called
	// either when the request completes cleanly or when it is terminated early.
	p.cancelLocked = func() {
		cancel()
		p.cancelLocked = nil
		p.nextLease = roachpb.Lease{}
	}

	err := p.repl.store.Stopper().RunAsyncTask(
		ctx, "storage.pendingLeaseRequest: requesting lease", func(ctx context.Context) {
			defer sp.Finish()

			// If requesting an epoch-based lease & current state is expired,
			// potentially heartbeat our own liveness or increment epoch of
			// prior owner. Note we only do this if the previous lease was
			// epoch-based.
			var pErr *roachpb.Error
			if reqLease.Type() == roachpb.LeaseEpoch && status.State == kvserverpb.LeaseState_EXPIRED &&
				status.Lease.Type() == roachpb.LeaseEpoch {
				var err error
				// If this replica is previous & next lease holder, manually heartbeat to become live.
				if status.Lease.OwnedBy(nextLeaseHolder.StoreID) &&
					p.repl.store.StoreID() == nextLeaseHolder.StoreID {
					if err = p.repl.store.cfg.NodeLiveness.Heartbeat(ctx, status.Liveness); err != nil {
						log.Errorf(ctx, "%v", err)
					}
				} else if status.Liveness.Epoch == status.Lease.Epoch {
					// If not owner, increment epoch if necessary to invalidate lease.
					// However, we only do so in the event that the next leaseholder is
					// considered live at this time. If not, there's no sense in
					// incrementing the expired leaseholder's epoch.
					if live, liveErr := p.repl.store.cfg.NodeLiveness.IsLive(nextLeaseHolder.NodeID); !live || liveErr != nil {
						err = errors.Errorf("not incrementing epoch on n%d because next leaseholder (n%d) not live (err = %v)",
							status.Liveness.NodeID, nextLeaseHolder.NodeID, liveErr)
						if log.V(1) {
							log.Infof(ctx, "%v", err)
						}
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
						if !errors.Is(err, ErrEpochAlreadyIncremented) {
							log.Errorf(ctx, "%v", err)
						}
					}
				}
				// Set error for propagation to all waiters below.
				if err != nil {
					// TODO(bdarnell): is status.Lease really what we want to put in the NotLeaseHolderError here?
					pErr = roachpb.NewError(newNotLeaseHolderError(&status.Lease, p.repl.store.StoreID(), p.repl.Desc()))
				}
			}

			// Send the RequestLeaseRequest or TransferLeaseRequest and wait for the new
			// lease to be applied.
			if pErr == nil {
				ba := roachpb.BatchRequest{}
				ba.Timestamp = p.repl.store.Clock().Now()
				ba.RangeID = p.repl.RangeID
				ba.Add(leaseReq)
				_, pErr = p.repl.Send(ctx, ba)
			}
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
				if pErr != nil {
					pErrClone := *pErr
					pErrClone.SetTxn(pErr.GetTxn())
					llHandle.resolve(&pErrClone)
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

// TransferInProgress returns the next lease, if the replica is in the process
// of transferring away its range lease. This next lease indicates the next
// lease holder. The second return val is true if a transfer is in progress.
// Note that the return values are best-effort and shouldn't be relied upon for
// correctness: if a previous transfer has returned an error, TransferInProgress
// will return `false`, but that doesn't necessarily mean that the transfer
// cannot still apply (see replica.mu.minLeaseProposedTS).
//
// It is assumed that the replica owning this pendingLeaseRequest owns the
// LeaderLease.
//
// replicaID is the ID of the parent replica.
//
// Requires repl.mu is read locked.
func (p *pendingLeaseRequest) TransferInProgress(
	replicaID roachpb.ReplicaID,
) (roachpb.Lease, bool) {
	if nextLease, ok := p.RequestPending(); ok {
		// Is the lease being transferred? (as opposed to just extended)
		if replicaID != nextLease.Replica.ReplicaID {
			return nextLease, true
		}
	}
	return roachpb.Lease{}, false
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

// leaseStatus returns lease status. If the lease is epoch-based,
// the liveness field will be set to the liveness used to compute
// its state, unless state == leaseError.
//
// - The lease is considered valid if the timestamp is covered by the
//   supplied lease. This is determined differently depending on the
//   lease properties. For expiration-based leases, the timestamp is
//   covered if it's less than the expiration (minus the maximum
//   clock offset). For epoch-based "node liveness" leases, the lease
//   epoch must match the owner node's liveness epoch -AND- the
//   timestamp must be within the node's liveness expiration (also
//   minus the maximum clock offset).
//
//   To be valid, a lease which contains a valid ProposedTS must have
//   a proposed timestamp greater than the minimum proposed timestamp,
//   which prevents a restarted process from serving commands, since
//   the spanlatch manager has been wiped through the restart.
//
// - The lease is considered in stasis if the timestamp is within the
//   maximum clock offset window of the lease expiration.
//
// - The lease is considered expired in all other cases.
//
// The maximum clock offset must always be taken into consideration to
// avoid a failure of linearizability on a single register during
// lease changes. Without that stasis period, the following could
// occur:
//
// * a range lease gets committed on the new lease holder (but not the old).
// * client proposes and commits a write on new lease holder (with a
//   timestamp just greater than the expiration of the old lease).
// * client tries to read what it wrote, but hits a slow coordinator
//   (which assigns a timestamp covered by the old lease).
// * the read is served by the old lease holder (which has not
//   processed the change in lease holdership).
// * the client fails to read their own write.
func (r *Replica) leaseStatus(
	lease roachpb.Lease, timestamp, minProposedTS hlc.Timestamp,
) kvserverpb.LeaseStatus {
	status := kvserverpb.LeaseStatus{Timestamp: timestamp, Lease: lease}
	var expiration hlc.Timestamp
	if lease.Type() == roachpb.LeaseExpiration {
		expiration = lease.GetExpiration()
	} else {
		l, err := r.store.cfg.NodeLiveness.GetLiveness(lease.Replica.NodeID)
		status.Liveness = l.Liveness
		if err != nil || status.Liveness.Epoch < lease.Epoch {
			// If lease validity can't be determined (e.g. gossip is down
			// and liveness info isn't available for owner), we can neither
			// use the lease nor do we want to attempt to acquire it.
			if err != nil {
				if leaseStatusLogLimiter.ShouldLog() {
					log.Warningf(context.TODO(), "can't determine lease status due to node liveness error: %+v", err)
				}
			}
			status.State = kvserverpb.LeaseState_ERROR
			return status
		}
		if status.Liveness.Epoch > lease.Epoch {
			status.State = kvserverpb.LeaseState_EXPIRED
			return status
		}
		expiration = hlc.Timestamp(status.Liveness.Expiration)
	}
	maxOffset := r.store.Clock().MaxOffset()
	stasis := expiration.Add(-int64(maxOffset), 0)
	if timestamp.Less(stasis) {
		status.State = kvserverpb.LeaseState_VALID
		// If the replica owns the lease, additional verify that the lease's
		// proposed timestamp is not earlier than the min proposed timestamp.
		if lease.Replica.StoreID == r.store.StoreID() &&
			lease.ProposedTS != nil && lease.ProposedTS.Less(minProposedTS) {
			status.State = kvserverpb.LeaseState_PROSCRIBED
		}
	} else if timestamp.Less(expiration) {
		status.State = kvserverpb.LeaseState_STASIS
	} else {
		status.State = kvserverpb.LeaseState_EXPIRED
	}
	return status
}

// requiresExpiringLeaseRLocked returns whether this range uses an
// expiration-based lease; false if epoch-based. Ranges located before or
// including the node liveness table must use expiration leases to avoid
// circular dependencies on the node liveness table.
func (r *Replica) requiresExpiringLeaseRLocked() bool {
	return r.store.cfg.NodeLiveness == nil || !r.store.cfg.EnableEpochRangeLeases ||
		r.mu.state.Desc.StartKey.Less(roachpb.RKey(keys.NodeLivenessKeyMax))
}

// requestLeaseLocked executes a request to obtain or extend a lease
// asynchronously and returns a channel on which the result will be posted. If
// there's already a request in progress, we join in waiting for the results of
// that request. Unless an error is returned, the obtained lease will be valid
// for a time interval containing the requested timestamp.
// If a transfer is in progress, a NotLeaseHolderError directing to the recipient is
// sent on the returned chan.
func (r *Replica) requestLeaseLocked(
	ctx context.Context, status kvserverpb.LeaseStatus,
) *leaseRequestHandle {
	if r.store.TestingKnobs().LeaseRequestEvent != nil {
		r.store.TestingKnobs().LeaseRequestEvent(status.Timestamp)
	}
	// Propose a Raft command to get a lease for this replica.
	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return r.mu.pendingLeaseRequest.newResolvedHandle(roachpb.NewError(err))
	}
	if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); ok {
		return r.mu.pendingLeaseRequest.newResolvedHandle(roachpb.NewError(
			newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc)))
	}
	if r.store.IsDraining() {
		// We've retired from active duty.
		return r.mu.pendingLeaseRequest.newResolvedHandle(roachpb.NewError(
			newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc)))
	}
	return r.mu.pendingLeaseRequest.InitOrJoinRequest(
		ctx, repDesc, status, r.mu.state.Desc.StartKey.AsRawKey(), false /* transfer */)
}

// AdminTransferLease transfers the LeaderLease to another replica. A
// valid LeaseStatus must be supplied. Only the current holder of the
// LeaderLease can do a transfer, because it needs to stop serving
// reads and proposing Raft commands (CPut is a read) after sending
// the transfer command. If it did not stop serving reads immediately,
// it would potentially serve reads with timestamps greater than the
// start timestamp of the new (transferred) lease. More subtly, the
// replica can't even serve reads or propose commands with timestamps
// lower than the start of the new lease because it could lead to read
// your own write violations (see comments on the stasis period in
// IsLeaseValid). We could, in principle, serve reads more than the
// maximum clock offset in the past.
//
// The method waits for any in-progress lease extension to be done, and it also
// blocks until the transfer is done. If a transfer is already in progress,
// this method joins in waiting for it to complete if it's transferring to the
// same replica. Otherwise, a NotLeaseHolderError is returned.
func (r *Replica) AdminTransferLease(ctx context.Context, target roachpb.StoreID) error {
	// initTransferHelper inits a transfer if no extension is in progress.
	// It returns a channel for waiting for the result of a pending
	// extension (if any is in progress) and a channel for waiting for the
	// transfer (if it was successfully initiated).
	var nextLeaseHolder roachpb.ReplicaDescriptor
	initTransferHelper := func() (extension, transfer *leaseRequestHandle, err error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		status := r.leaseStatus(*r.mu.state.Lease, r.store.Clock().Now(), r.mu.minLeaseProposedTS)
		if status.Lease.OwnedBy(target) {
			// The target is already the lease holder. Nothing to do.
			return nil, nil, nil
		}
		desc := r.mu.state.Desc
		if !status.Lease.OwnedBy(r.store.StoreID()) {
			return nil, nil, newNotLeaseHolderError(&status.Lease, r.store.StoreID(), desc)
		}
		// Verify the target is a replica of the range.
		var ok bool
		if nextLeaseHolder, ok = desc.GetReplicaDescriptor(target); !ok {
			return nil, nil, errors.Errorf("unable to find store %d in range %+v", target, desc)
		}

		// For now, don't allow replicas of type LEARNER to be leaseholders, see
		// comments in RequestLease and TransferLease for why.
		//
		// TODO(dan): We shouldn't need this, the checks in RequestLease and
		// TransferLease are the canonical ones and should be sufficient. Sadly, the
		// `r.mu.minLeaseProposedTS = status.Timestamp` line below will likely play
		// badly with that. This would be an issue even without learners, but
		// omitting this check would make it worse. Fixme.
		if t := nextLeaseHolder.GetType(); t != roachpb.VOTER_FULL {
			return nil, nil, errors.Errorf(`cannot transfer lease to replica of type %s`, t)
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
			return nil, nil, newNotLeaseHolderError(&nextLease, r.store.StoreID(), desc)
		}
		// Stop using the current lease.
		r.mu.minLeaseProposedTS = status.Timestamp
		transfer = r.mu.pendingLeaseRequest.InitOrJoinRequest(
			ctx, nextLeaseHolder, status, desc.StartKey.AsRawKey(), true, /* transfer */
		)
		return nil, transfer, nil
	}

	// Loop while there's an extension in progress.
	for {
		// See if there's an extension in progress that we have to wait for.
		// If there isn't, request a transfer.
		extension, transfer, err := initTransferHelper()
		if err != nil {
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

// GetDescAndLease atomically reads the range's current descriptor and lease.
func (r *Replica) GetDescAndLease(ctx context.Context) (roachpb.RangeDescriptor, roachpb.Lease) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	l, _ /* nextLease */ := r.getLeaseRLocked()
	desc := r.descRLocked()

	// Sanity check the lease.
	if !l.Empty() {
		if _, ok := desc.GetReplicaDescriptorByID(l.Replica.ReplicaID); !ok {
			log.Fatalf(ctx, "leaseholder replica not in descriptor; desc: %s, lease: %s", desc, l)
		}
	}

	return *desc, l
}

func (r *Replica) getLeaseRLocked() (roachpb.Lease, roachpb.Lease) {
	if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok {
		return *r.mu.state.Lease, nextLease
	}
	return *r.mu.state.Lease, roachpb.Lease{}
}

// OwnsValidLease returns whether this replica is the current valid
// leaseholder. Note that this method does not check to see if a transfer is
// pending, but returns the status of the current lease and ownership at the
// specified point in time.
func (r *Replica) OwnsValidLease(ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ownsValidLeaseRLocked(ts)
}

func (r *Replica) ownsValidLeaseRLocked(ts hlc.Timestamp) bool {
	return r.mu.state.Lease.OwnedBy(r.store.StoreID()) &&
		r.leaseStatus(*r.mu.state.Lease, ts, r.mu.minLeaseProposedTS).State == kvserverpb.LeaseState_VALID
}

// IsLeaseValid returns true if the replica's lease is owned by this
// replica and is valid (not expired, not in stasis).
func (r *Replica) IsLeaseValid(lease roachpb.Lease, ts hlc.Timestamp) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.isLeaseValidRLocked(lease, ts)
}

func (r *Replica) isLeaseValidRLocked(lease roachpb.Lease, ts hlc.Timestamp) bool {
	return r.leaseStatus(lease, ts, r.mu.minLeaseProposedTS).State == kvserverpb.LeaseState_VALID
}

// newNotLeaseHolderError returns a NotLeaseHolderError initialized with the
// replica for the holder (if any) of the given lease.
//
// Note that this error can be generated on the Raft processing goroutine, so
// its output should be completely determined by its parameters.
func newNotLeaseHolderError(
	l *roachpb.Lease, proposerStoreID roachpb.StoreID, rangeDesc *roachpb.RangeDescriptor,
) *roachpb.NotLeaseHolderError {
	err := &roachpb.NotLeaseHolderError{
		RangeID: rangeDesc.RangeID,
	}
	if proposerStoreID != 0 {
		err.Replica, _ = rangeDesc.GetReplicaDescriptor(proposerStoreID)
	}
	if l != nil {
		// Normally, we return the lease-holding Replica here. However, in the
		// case in which a leader removes itself, we want the followers to
		// avoid handing out a misleading clue (which in itself shouldn't be
		// overly disruptive as the lease would expire and then this method
		// shouldn't be called for it any more, but at the very least it
		// could catch tests in a loop, presumably due to manual clocks).
		_, stillMember := rangeDesc.GetReplicaDescriptor(l.Replica.StoreID)
		if stillMember {
			err.LeaseHolder = &l.Replica
			err.Lease = l
		}
	}
	return err
}

// leaseGoodToGo is a fast-path for lease checks which verifies that an
// existing lease is valid and owned by the current store. This method should
// not be called directly. Use redirectOnOrAcquireLease instead.
func (r *Replica) leaseGoodToGo(ctx context.Context) (kvserverpb.LeaseStatus, bool) {
	timestamp := r.store.Clock().Now()
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.requiresExpiringLeaseRLocked() {
		// Slow-path for expiration-based leases.
		return kvserverpb.LeaseStatus{}, false
	}

	status := r.leaseStatus(*r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
	if status.State == kvserverpb.LeaseState_VALID && status.Lease.OwnedBy(r.store.StoreID()) {
		// We own the lease...
		if repDesc, err := r.getReplicaDescriptorRLocked(); err == nil {
			if _, ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); !ok {
				// ...and there is no transfer pending.
				return status, true
			}
		}
	}
	return kvserverpb.LeaseStatus{}, false
}

// redirectOnOrAcquireLease checks whether this replica has the lease at the
// current timestamp. If it does, returns the lease and its status. If
// another replica currently holds the lease, redirects by returning
// NotLeaseHolderError. If the lease is expired, a renewal is synchronously
// requested. Leases are eagerly renewed when a request with a timestamp
// within rangeLeaseRenewalDuration of the lease expiration is served.
//
// TODO(spencer): for write commands, don't wait while requesting
//  the range lease. If the lease acquisition fails, the write cmd
//  will fail as well. If it succeeds, as is likely, then the write
//  will not incur latency waiting for the command to complete.
//  Reads, however, must wait.
//
// TODO(rangeLeaseRenewalDuration): what is rangeLeaseRenewalDuration
//  referring to? It appears to have rotted.
func (r *Replica) redirectOnOrAcquireLease(
	ctx context.Context,
) (kvserverpb.LeaseStatus, *roachpb.Error) {
	if status, ok := r.leaseGoodToGo(ctx); ok {
		return status, nil
	}

	// Loop until the lease is held or the replica ascertains the actual
	// lease holder. Returns also on context.Done() (timeout or cancellation).
	var status kvserverpb.LeaseStatus
	for attempt := 1; ; attempt++ {
		timestamp := r.store.Clock().Now()
		llHandle, pErr := func() (*leaseRequestHandle, *roachpb.Error) {
			r.mu.Lock()
			defer r.mu.Unlock()

			status = r.leaseStatus(*r.mu.state.Lease, timestamp, r.mu.minLeaseProposedTS)
			switch status.State {
			case kvserverpb.LeaseState_ERROR:
				// Lease state couldn't be determined.
				log.VEventf(ctx, 2, "lease state couldn't be determined")
				return nil, roachpb.NewError(
					newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc))

			case kvserverpb.LeaseState_VALID, kvserverpb.LeaseState_STASIS:
				if !status.Lease.OwnedBy(r.store.StoreID()) {
					_, stillMember := r.mu.state.Desc.GetReplicaDescriptor(status.Lease.Replica.StoreID)
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
						// However, this is possible if the `cockroach debug
						// unsafe-remove-dead-replicas` command has been used, so
						// this is just a logged error instead of a fatal
						// assertion.
						log.Errorf(ctx, "lease %s owned by replica %+v that no longer exists",
							status.Lease, status.Lease.Replica)
					}
					// Otherwise, if the lease is currently held by another replica, redirect
					// to the holder.
					return nil, roachpb.NewError(
						newNotLeaseHolderError(&status.Lease, r.store.StoreID(), r.mu.state.Desc))
				}
				// Check that we're not in the process of transferring the lease away.
				// If we are transferring the lease away, we can't serve reads or
				// propose Raft commands - see comments on TransferLease.
				// TODO(andrei): If the lease is being transferred, consider returning a
				// new error type so the client backs off until the transfer is
				// completed.
				repDesc, err := r.getReplicaDescriptorRLocked()
				if err != nil {
					return nil, roachpb.NewError(err)
				}
				if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(
					repDesc.ReplicaID); ok {
					return nil, roachpb.NewError(
						newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc))
				}

				// If the lease is in stasis, we can't serve requests until we've
				// renewed the lease, so we return the handle to block on renewal.
				// Otherwise, we don't need to wait for the extension and simply
				// ignore the returned handle (whose channel is buffered) and continue.
				if status.State == kvserverpb.LeaseState_STASIS {
					return r.requestLeaseLocked(ctx, status), nil
				}

				// Extend the lease if this range uses expiration-based
				// leases, the lease is in need of renewal, and there's not
				// already an extension pending.
				_, requestPending := r.mu.pendingLeaseRequest.RequestPending()
				if !requestPending && r.requiresExpiringLeaseRLocked() {
					renewal := status.Lease.Expiration.Add(-r.store.cfg.RangeLeaseRenewalDuration().Nanoseconds(), 0)
					if renewal.LessEq(timestamp) {
						if log.V(2) {
							log.Infof(ctx, "extending lease %s at %s", status.Lease, timestamp)
						}
						// We had an active lease to begin with, but we want to trigger
						// a lease extension. We explicitly ignore the returned handle
						// as we won't block on it.
						_ = r.requestLeaseLocked(ctx, status)
					}
				}

			case kvserverpb.LeaseState_EXPIRED:
				// No active lease: Request renewal if a renewal is not already pending.
				log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
				return r.requestLeaseLocked(ctx, status), nil

			case kvserverpb.LeaseState_PROSCRIBED:
				// Lease proposed timestamp is earlier than the min proposed
				// timestamp limit this replica must observe. If this store
				// owns the lease, re-request. Otherwise, redirect.
				if status.Lease.OwnedBy(r.store.StoreID()) {
					log.VEventf(ctx, 2, "request range lease (attempt #%d)", attempt)
					return r.requestLeaseLocked(ctx, status), nil
				}
				// If lease is currently held by another, redirect to holder.
				return nil, roachpb.NewError(
					newNotLeaseHolderError(&status.Lease, r.store.StoreID(), r.mu.state.Desc))
			}

			// Return a nil handle to signal that we have a valid lease.
			return nil, nil
		}()
		if pErr != nil {
			return kvserverpb.LeaseStatus{}, pErr
		}
		if llHandle == nil {
			// We own a valid lease.
			return status, nil
		}

		// Wait for the range lease to finish, or the context to expire.
		pErr = func() (pErr *roachpb.Error) {
			slowTimer := timeutil.NewTimer()
			defer slowTimer.Stop()
			slowTimer.Reset(base.SlowRequestThreshold)
			tBegin := timeutil.Now()
			for {
				select {
				case pErr = <-llHandle.C():
					if pErr != nil {
						switch tErr := pErr.GetDetail().(type) {
						case *roachpb.AmbiguousResultError:
							// This can happen if the RequestLease command we sent has been
							// applied locally through a snapshot: the RequestLeaseRequest
							// cannot be reproposed so we get this ambiguity.
							// We'll just loop around.
							return nil
						case *roachpb.LeaseRejectedError:
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
							} else if lease, _ := r.GetLease(); !r.IsLeaseValid(lease, r.store.Clock().Now()) {
								err = newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc())
							} else {
								err = newNotLeaseHolderError(&lease, r.store.StoreID(), r.Desc())
							}
							pErr = roachpb.NewError(err)
						}
						return pErr
					}
					log.Eventf(ctx, "lease acquisition succeeded: %+v", status.Lease)
					return nil
				case <-slowTimer.C:
					slowTimer.Read = true
					log.Warningf(ctx, "have been waiting %s attempting to acquire lease",
						base.SlowRequestThreshold)
					r.store.metrics.SlowLeaseRequests.Inc(1)
					defer func() {
						r.store.metrics.SlowLeaseRequests.Dec(1)
						log.Infof(ctx, "slow lease acquisition finished after %s with error %v after %d attempts", timeutil.Since(tBegin), pErr, attempt)
					}()
				case <-ctx.Done():
					llHandle.Cancel()
					log.VErrEventf(ctx, 2, "lease acquisition failed: %s", ctx.Err())
					return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
				case <-r.store.Stopper().ShouldStop():
					llHandle.Cancel()
					return roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.Desc()))
				}
			}
		}()
		if pErr != nil {
			return kvserverpb.LeaseStatus{}, pErr
		}
	}
}
