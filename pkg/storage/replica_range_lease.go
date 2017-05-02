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
// Author: Andrei Matei (andreimatei1@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

// This file contains replica methods related to range leases.

package storage

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

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
// Methods are not thread-safe; a pendingLeaseRequest is logically part of
// a replica, so replica.mu should be used to synchronize all calls.
type pendingLeaseRequest struct {
	// Slice of channels to send on after lease acquisition.
	// If empty, then no request is in progress.
	llChans []chan *roachpb.Error
	// nextLease is the pending RequestLease request, if any. It can be used to
	// figure out if we're in the process of extending our own lease, or
	// transferring it to another replica.
	nextLease roachpb.Lease
}

// RequestPending returns the pending Lease, if one is in progress.
// The second return val is true if a lease request is pending.
func (p *pendingLeaseRequest) RequestPending() (roachpb.Lease, bool) {
	pending := len(p.llChans) > 0
	if pending {
		return p.nextLease, true
	}
	return roachpb.Lease{}, false
}

// InitOrJoinRequest executes a RequestLease command asynchronously and returns a
// channel on which the result will be posted. If there's already a request in
// progress, we join in waiting for the results of that request.
// It is an error to call InitOrJoinRequest() while a request is in progress
// naming another replica as lease holder.
//
// replica is used to schedule and execute async work (proposing a RequestLease
// command). replica.mu is locked when delivering results, so calls from the
// replica happen either before or after a result for a pending request has
// happened.
//
// transfer needs to be set if the request represents a lease transfer (as
// opposed to an extension, or acquiring the lease when none is held).
//
// Note: Once this function gets a context to be used for cancellation, instead
// of replica.store.Stopper().ShouldQuiesce(), care will be needed for cancelling
// the Raft command, similar to replica.executeWriteBatch.
//
// Requires repl.mu is exclusively locked.
func (p *pendingLeaseRequest) InitOrJoinRequest(
	ctx context.Context,
	repl *Replica,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	status LeaseStatus,
	startKey roachpb.Key,
	transfer bool,
) <-chan *roachpb.Error {
	if nextLease, ok := p.RequestPending(); ok {
		if nextLease.Replica.ReplicaID == nextLeaseHolder.ReplicaID {
			// Join a pending request asking for the same replica to become lease
			// holder.
			return p.JoinRequest()
		}
		llChan := make(chan *roachpb.Error, 1)
		// We can't join the request in progress.
		llChan <- roachpb.NewErrorf("request for different replica in progress "+
			"(requesting: %+v, in progress: %+v)",
			nextLeaseHolder.ReplicaID, nextLease.Replica.ReplicaID)
		return llChan
	}
	llChan := make(chan *roachpb.Error, 1)
	// No request in progress. Let's propose a Lease command asynchronously.
	reqSpan := roachpb.Span{
		Key: startKey,
	}
	var leaseReq roachpb.Request
	now := repl.store.Clock().Now()
	reqLease := roachpb.Lease{
		Start:      status.timestamp,
		Replica:    nextLeaseHolder,
		ProposedTS: &now,
	}

	if repl.requiresExpiringLeaseRLocked() {
		reqLease.Expiration = status.timestamp.Add(int64(repl.store.cfg.RangeLeaseActiveDuration), 0)
	} else {
		// Get the liveness for the next lease holder and set the epoch in the lease request.
		liveness, err := repl.store.cfg.NodeLiveness.GetLiveness(nextLeaseHolder.NodeID)
		if err != nil {
			llChan <- roachpb.NewError(&roachpb.LeaseRejectedError{
				Existing:  *status.lease,
				Requested: reqLease,
				Message:   fmt.Sprintf("couldn't request lease for %+v: %v", nextLeaseHolder, err),
			})
			return llChan
		}
		reqLease.Epoch = proto.Int64(liveness.Epoch)
	}

	if transfer {
		leaseReq = &roachpb.TransferLeaseRequest{
			Span:      reqSpan,
			Lease:     reqLease,
			PrevLease: status.lease,
		}
	} else {
		leaseReq = &roachpb.RequestLeaseRequest{
			Span:      reqSpan,
			Lease:     reqLease,
			PrevLease: status.lease,
		}
	}

	if err := p.requestLeaseAsync(repl, nextLeaseHolder, reqLease, status, leaseReq); err != nil {
		// We failed to start the asynchronous task. Send a blank NotLeaseHolderError
		// back to indicate that we have no idea who the range lease holder might
		// be; we've withdrawn from active duty.
		llChan <- roachpb.NewError(
			newNotLeaseHolderError(nil, repl.store.StoreID(), repl.mu.state.Desc))
		return llChan
	}
	// TODO(andrei): document this subtlety.
	p.llChans = append(p.llChans, llChan)
	p.nextLease = reqLease
	return llChan
}

// requestLeaseAsync sends a transfer lease or lease request to the
// specified replica. The request is sent in an async task.
func (p *pendingLeaseRequest) requestLeaseAsync(
	repl *Replica,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	reqLease roachpb.Lease,
	status LeaseStatus,
	leaseReq roachpb.Request,
) error {
	return repl.store.Stopper().RunAsyncTask(context.TODO(), func(ctx context.Context) {
		ctx = repl.AnnotateCtx(ctx)
		var pErr *roachpb.Error

		// If requesting an epoch-based lease & current state is expired,
		// potentially heartbeat our own liveness or increment epoch of
		// prior owner. Note we only do this if the previous lease was
		// epoch-based.
		if reqLease.Type() == roachpb.LeaseEpoch && status.state == leaseExpired &&
			status.lease.Type() == roachpb.LeaseEpoch {
			var err error
			// If this replica is previous & next lease holder, manually heartbeat to become live.
			if status.lease.OwnedBy(nextLeaseHolder.StoreID) &&
				repl.store.StoreID() == nextLeaseHolder.StoreID {
				if err = repl.store.cfg.NodeLiveness.Heartbeat(ctx, status.liveness); err != nil {
					log.Error(ctx, err)
				}
			} else if status.liveness.Epoch == *status.lease.Epoch {
				// If not owner, increment epoch if necessary to invalidate lease.
				if err = repl.store.cfg.NodeLiveness.IncrementEpoch(ctx, status.liveness); err != nil {
					log.Error(ctx, err)
				}
			}
			// Set error for propagation to all waiters below.
			if err != nil {
				pErr = roachpb.NewError(newNotLeaseHolderError(status.lease, repl.store.StoreID(), repl.Desc()))
			}
		}

		// Propose a RequestLease command and wait for it to apply.
		if pErr == nil {
			ba := roachpb.BatchRequest{}
			ba.Timestamp = repl.store.Clock().Now()
			ba.RangeID = repl.RangeID
			ba.Add(leaseReq)
			_, pErr = repl.Send(ctx, ba)
		}
		// We reset our state below regardless of whether we've gotten an error or
		// not, but note that an error is ambiguous - there's no guarantee that the
		// transfer will not still apply. That's OK, however, as the "in transfer"
		// state maintained by the pendingLeaseRequest is not relied on for
		// correctness (see repl.mu.minLeaseProposedTS), and resetting the state
		// is beneficial as it'll allow the replica to attempt to transfer again or
		// extend the existing lease in the future.

		// Send result of lease to all waiter channels.
		repl.mu.Lock()
		defer repl.mu.Unlock()
		for _, llChan := range p.llChans {
			// Don't send the same transaction object twice; this can lead to races.
			if pErr != nil {
				pErrClone := *pErr
				pErrClone.SetTxn(pErr.GetTxn())
				llChan <- &pErrClone
			} else {
				llChan <- nil
			}
		}
		p.llChans = p.llChans[:0]
		p.nextLease = roachpb.Lease{}
	})
}

// JoinRequest adds one more waiter to the currently pending request.
// It is the caller's responsibility to ensure that there is a pending request,
// and that the request is compatible with whatever the caller is currently
// wanting to do (i.e. the request is naming the intended node as the next
// lease holder).
func (p *pendingLeaseRequest) JoinRequest() <-chan *roachpb.Error {
	llChan := make(chan *roachpb.Error, 1)
	if len(p.llChans) == 0 {
		llChan <- roachpb.NewErrorf("no request in progress")
		return llChan
	}
	p.llChans = append(p.llChans, llChan)
	return llChan
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

type leaseState int

const (
	leaseError      leaseState = iota // can't be used or acquired
	leaseValid                        // can be used
	leaseStasis                       // not expired, but can't be used
	leaseExpired                      // expired, can't be used
	leaseProscribed                   // proposed timestamp earlier than allowed
)

// LeaseStatus holds the lease state, the timestamp at which the state
// is accurate, the lease and optionally the liveness if the lease is
// epoch-based.
type LeaseStatus struct {
	state     leaseState     // state of the lease @timestamp
	timestamp hlc.Timestamp  // timestamp the lease was evaluated at
	lease     *roachpb.Lease // lease which status describes
	liveness  *Liveness      // liveness if epoch-based lease
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
//   the command queue has been wiped through the restart.
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
	lease *roachpb.Lease, timestamp, minProposedTS hlc.Timestamp,
) LeaseStatus {
	status := LeaseStatus{timestamp: timestamp, lease: lease}
	if lease == nil {
		status.state = leaseExpired
		return status
	}
	var expiration hlc.Timestamp
	if lease.Type() == roachpb.LeaseExpiration {
		expiration = lease.Expiration
	} else {
		var err error
		status.liveness, err = r.store.cfg.NodeLiveness.GetLiveness(lease.Replica.NodeID)
		if err != nil || status.liveness.Epoch < *lease.Epoch {
			// If lease validity can't be determined (e.g. gossip is down
			// and liveness info isn't available for owner), we can neither
			// use the lease nor do we want to attempt to acquire it.
			status.state = leaseError
			return status
		}
		if status.liveness.Epoch > *lease.Epoch {
			status.state = leaseExpired
			return status
		}
		expiration = status.liveness.Expiration
	}
	stasis := expiration.Add(-int64(r.store.Clock().MaxOffset()), 0)
	if timestamp.Less(stasis) {
		status.state = leaseValid
		// If the replica owns the lease, additional verify that the lease's
		// proposed timestamp is not earlier than the min proposed timestamp.
		if lease.Replica.StoreID == r.store.StoreID() &&
			lease.ProposedTS != nil && lease.ProposedTS.Less(minProposedTS) {
			status.state = leaseProscribed
		}
	} else if timestamp.Less(expiration) {
		status.state = leaseStasis
	} else {
		status.state = leaseExpired
	}
	return status
}

// requiresExpiringLeaseRLocked returns whether this range uses an
// expiration-based lease; false if epoch-based. Ranges located before or
// including the node liveness table must use expiration leases to avoid
// circular dependencies on the node liveness table. The replica mutex must be
// held.
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
	ctx context.Context, status LeaseStatus,
) <-chan *roachpb.Error {
	if r.store.TestingKnobs().LeaseRequestEvent != nil {
		r.store.TestingKnobs().LeaseRequestEvent(status.timestamp)
	}
	// Propose a Raft command to get a lease for this replica.
	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(err)
		return llChan
	}
	if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(repDesc.ReplicaID); ok {
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(
			newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc))
		return llChan
	}
	if r.store.IsDraining() {
		// We've retired from active duty.
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc))
		return llChan
	}
	return r.mu.pendingLeaseRequest.InitOrJoinRequest(
		ctx, r, repDesc, status, r.mu.state.Desc.StartKey.AsRawKey(), false /* transfer */)
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
	initTransferHelper := func() (<-chan *roachpb.Error, <-chan *roachpb.Error, error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		status := r.leaseStatus(r.mu.state.Lease, r.store.Clock().Now(), r.mu.minLeaseProposedTS)
		if status.lease.OwnedBy(target) {
			// The target is already the lease holder. Nothing to do.
			return nil, nil, nil
		}
		desc := r.mu.state.Desc
		if !status.lease.OwnedBy(r.store.StoreID()) {
			return nil, nil, newNotLeaseHolderError(status.lease, r.store.StoreID(), desc)
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
			return nil, nil, newNotLeaseHolderError(&nextLease, r.store.StoreID(), desc)
		}
		// Stop using the current lease.
		r.mu.minLeaseProposedTS = status.timestamp
		transfer := r.mu.pendingLeaseRequest.InitOrJoinRequest(
			ctx, r, nextLeaseHolder, status, desc.StartKey.AsRawKey(), true, /* transfer */
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
			return (<-transfer).GoError()
		}
		// Wait for the in-progress extension without holding the mutex.
		if r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent != nil {
			r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent(nextLeaseHolder)
		}
		<-extension
	}
}
