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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// pendingLeaseRequest coalesces RequestLease requests and lets callers initiate
// a lease request (extension or transfer) or join an in-progress one and wait
// for the result.
// pendingLeaseRequest deals with persisting the state of transferred leases to
// disk and restoring it on node restart.
// The actual execution of the RequestLease Raft request is delegated to the
// parent replica.
//
// The pendingLeaseRequest deals with the trickiness of concurrent lease
// requests and races thereof by making rather weak guarantees: a client can
// propose a new lease and block until some new lease is applied, but there's no
// guaranteed that the applied lease is the proposed one. However, this struct
// also serializes proposals coming from the current replica (and assumes that,
// while there's a valid lease, only the lease holder will propose new leases),
// so cases where a waiter is notified of a different lease than the one it was
// waiting on should be rare - node restarts and races of proposals close to a
// lease's expiration time.
// Guarantees on error are stronger: if an error is reported back to a waiting
// client, that error is guaranteed to be related to the lease it proposed. But
// the usual caveats apply: an error doesn't necessarily mean that the command
// hasn't applied / will not apply successfully.
//
// Waiters are woken when peningLeaseRequest.SignalLeaseApplied is called from
// the commit trigger of a transaction that has applied a new lease.
//
// Methods are not thread-safe; a pendingLeaseRequest is logically part of
// a replica, so replica.mu should be used to synchronize all calls.
type pendingLeaseRequest struct {
	replica *Replica
	// nextLease is the pending RequestLease request, if any. It can be used to
	// figure out if we're in the process of extending our own lease, or
	// transferring it to another replica.
	nextLease roachpb.Lease
	// Slice of channels to send on after lease acquisition.
	// If empty, then no request is in progress.
	waiters []chan leaseOrErr
}

func newPendingLeaseRequest(
	ctx context.Context, replica *Replica,
) (*pendingLeaseRequest, error) {
	// Load the nextLease from disk.
	nextLease, err := loadNextLease(ctx, replica.store.Engine(), replica.RangeID)
	if err != nil {
		return nil, err
	}
	// A bit of magic here: if, upon a restart, nextLease is found to be an
	// extension, then we ignore it. The important thing to restore after a
	// restart is a lease transfer (so, when we proposed the transfer, we promised
	// not to use the existing lease any more). If we were to restore an extension
	// too, we would likely block new attempts to extend the lease.
	if isExtension := nextLease.Replica.ReplicaID == replica.mu.replicaID; isExtension {
		nextLease = roachpb.Lease{}
	}
	return &pendingLeaseRequest{
		replica:   replica,
		nextLease: nextLease,
		waiters:   nil,
	}, nil
}

// leaseOrErr represents the result a waiter gets after a lease has been
// applied. It will have either a new lease that recently applied, or an error.
// If it's an error, it is what replica.Send() got when it proposed the specific
// lease the waiter was waiting on.
type leaseOrErr struct {
	pErr  *roachpb.Error
	lease roachpb.Lease
}

// SignalLeaseApplied signals all the waiters either that a lease has just been
// applied, or that the proposal for a lease has failed.
func (p *pendingLeaseRequest) SignalLeaseApplied(
	ctx context.Context, lease roachpb.Lease, pErr *roachpb.Error,
) {
	if pErr == nil {
		p.notifyWaiters(leaseOrErr{lease: lease})
		p.WipeNextLease(ctx)
	} else {
		// We need to protect against spurious result errors by checking which lease
		// request this signal corresponds too. It might not correspond to the
		// request the current waiters are waiting on, since a signal delivering a
		// success can race with a signal signaling an error for the same request.
		// Also, an error can in theory lose a race with a successful application of
		// a lease proposed by another node.
		if !proto.Equal(&lease, &p.nextLease) {
			return
		}
		p.notifyWaiters(leaseOrErr{pErr: pErr})
		// In case of an error, we can wipe our state, but only if we were trying to
		// extend the lease. The idea is that, if we were trying to extend the
		// lease, we want to allow others to try again. If, on the other hand, we
		// were trying to transfer the lease, then we'd still in principle want to
		// allow others to try again, but, since there's still a chance this
		// transfer will succeed in the future (proposing a command can return an
		// error even if the command applies successfully), we can't just wipe the
		// state: we still need to be in "transfer" mode and not allow the current
		// lease to be used for serving.
		if isExtension := lease.Replica.ReplicaID == p.replica.mu.replicaID; isExtension {
			p.WipeNextLease(ctx)
		}
	}
}

// WipeNextLease clears the "nextLease" state, in memory and on disk. Calling
// InitOrJoinRequest() after this returns will propose a new lease request.
func (p *pendingLeaseRequest) WipeNextLease(ctx context.Context) {
	if len(p.waiters) != 0 {
		panic("wiping state but someone's waiting for a result")
	}
	err := setNextLease(
		ctx, p.replica.store.engine, p.replica.RangeID, roachpb.Lease{})
	if err != nil {
		// TODO(andrei): What should we do for such replica corruption errors?
		panic(errors.Wrapf(err, "failed to persist lease to disk"))
	}
	p.nextLease = roachpb.Lease{}
}

// getWaitChan returns a channel that will be signaled the next time the replica
// applies a lease (the lease the caller has just proposed, or a completely
// different one).
func (p *pendingLeaseRequest) getWaitChan(ctx context.Context) chan leaseOrErr {
	// Add myself to the waiters.
	myCh := make(chan leaseOrErr, 1)
	p.waiters = append(p.waiters, myCh)
	return myCh
}

// notifyWaiters sends a notification to the current batch of waiters. It then
// clears the list of waiters. Callers should also call WipeNextLease atomically
// (wrt InitOrJoinRequest() calls) after calling this if they don't want future
// clients to continue to wait for the same lease the current epoch's waiters
// were blocked on.
func (p *pendingLeaseRequest) notifyWaiters(loe leaseOrErr) {
	for i, ch := range p.waiters {
		// Don't send the same pErr object twice; this can lead to races. We could
		// clone every time but it's more efficient to send pErr itself to one of
		// the channels (the last one; if we send it earlier the race can still
		// happen).
		if loe.pErr == nil || i == len(p.waiters)-1 {
			ch <- loe
		} else {
			loeClone := loe
			loeClone.pErr = protoutil.Clone(loe.pErr).(*roachpb.Error)
			ch <- loeClone
		}
	}
	p.waiters = nil
}

// RequestPending returns the pending Lease, if one is in progress.
// The second return val is true if a lease request is pending.
func (p *pendingLeaseRequest) RequestPending() (roachpb.Lease, bool) {
	if p.nextLease.Replica.ReplicaID != 0 {
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
// command). replica.mu is locked when delivering results, so calls to
// InitOrJoinRequest happen either before or after a previous lease request had
// finished.
//
// transfer needs to be set if the request represents a lease transfer (as
// opposed to an extension, or acquiring the lease when none is held).
//
// Note: Once this function gets a context to be used for cancellation, instead
// of replica.store.Stopper().ShouldQuiesce(), care will be needed for cancelling
// the Raft command, similar to replica.addWriteCmd.
func (p *pendingLeaseRequest) InitOrJoinRequest(
	ctx context.Context,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	timestamp hlc.Timestamp,
	startKey roachpb.Key,
	transfer bool,
) <-chan leaseOrErr {
	if _, ok := p.RequestPending(); !ok {
		// No request in progress. Let's propose a Lease command asynchronously.
		// Sanity check that there's no waiters. We should become the first waiter.
		if len(p.waiters) != 0 {
			panic("no lease request in progress, but someone's waiting for a result")
		}
		startStasis := timestamp.Add(int64(p.replica.store.cfg.rangeLeaseActiveDuration), 0)
		expiration := startStasis.Add(int64(p.replica.store.Clock().MaxOffset()), 0)
		reqSpan := roachpb.Span{
			Key: startKey,
		}
		var leaseReq roachpb.Request
		reqLease := roachpb.Lease{
			Start:       timestamp,
			StartStasis: startStasis,
			Expiration:  expiration,
			Replica:     nextLeaseHolder,
		}
		if transfer {
			leaseReq = &roachpb.TransferLeaseRequest{
				Span:  reqSpan,
				Lease: reqLease,
			}
		} else {
			leaseReq = &roachpb.RequestLeaseRequest{
				Span:  reqSpan,
				Lease: reqLease,
			}
		}

		// Save state in-memory and on-disk.
		err := setNextLease(
			ctx, p.replica.store.engine, p.replica.RangeID, reqLease)
		if err != nil {
			// NOTE(andrei): We could return an error to the caller here since we
			// haven't sent the request yet, but we can't similarly handle the same
			// kind of corruption error in GetWaitChan(), so we just panic here too
			// for symmetry.
			panic(errors.Wrapf(err, "failed to persist lease to disk"))
		}
		p.nextLease = reqLease

		if p.replica.store.Stopper().RunAsyncTask(p.replica.store.Ctx(), func(ctx context.Context) {
			// Propose a RequestLease command and wait for it to apply.
			ba := roachpb.BatchRequest{}
			ba.Timestamp = p.replica.store.Clock().Now()
			ba.RangeID = p.replica.RangeID
			ba.Add(leaseReq)
			_, pErr := p.replica.Send(ctx, ba)
			if pErr != nil {
				// If the request failed, signal the waiters. If it succeeded, they'll
				// be signaled by the commit trigger that applies the new lease.
				p.replica.mu.Lock()
				p.SignalLeaseApplied(ctx, p.nextLease, pErr)
				p.replica.mu.Unlock()
			}
		}) != nil {
			p.WipeNextLease(ctx)
			errCh := make(chan leaseOrErr, 1)
			// We failed to start the asynchronous task. Send a blank NotLeaseHolderError
			// back to indicate that we have no idea who the range lease holder might
			// be; we've withdrawn from active duty.
			rangeDesc := p.replica.mu.state.Desc
			errCh <- leaseOrErr{pErr: roachpb.NewError(
				newNotLeaseHolderError(nil, p.replica.store.StoreID(), rangeDesc))}
			return errCh
		}
		// Add ourselves as a waiter.
		retCh := p.getWaitChan(ctx)
		return retCh
	}

	// A lease request is in progress. If it's for the same target, join it.
	if p.nextLease.Replica.ReplicaID == nextLeaseHolder.ReplicaID {
		// Join a pending request asking for the same replica to become lease
		// holder.
		return p.getWaitChan(ctx)
	}
	errChan := make(chan leaseOrErr, 1)
	errChan <- leaseOrErr{pErr: roachpb.NewErrorf(
		"request for different replica in progress "+
			"(requesting: %+v, in progress: %+v)",
		nextLeaseHolder.ReplicaID, p.nextLease.Replica.ReplicaID)}
	return errChan
}

// TransferInProgress returns the next lease, if the replica is in the process
// of transferring away its range lease. This next lease indicates the next
// lease holder. The second return val is true if a transfer is in progress.
//
// It is assumed that the replica owning this pendingLeaseRequest owns the
// LeaderLease.
//
// Note that the pendingLeaseRequest, and hence this method, is unaware of any
// transfer that might have been in progress before a restart. The caller must
// check for that separately.
//
// replicaID is the ID of the parent replica.
func (p *pendingLeaseRequest) TransferInProgress(replicaID roachpb.ReplicaID) (roachpb.Lease, bool) {
	if nextLease, ok := p.RequestPending(); ok {
		// Is the lease being transferred? (as opposed to just extended)
		if replicaID != nextLease.Replica.ReplicaID {
			return nextLease, true
		}
	}
	return roachpb.Lease{}, false
}

// requestLeaseLocked executes a request to obtain or extend a lease
// asynchronously and returns a channel on which the result will be posted. If
// there's already a request in progress, we join in waiting for the results of
// that request. The returned channel is signaled once a new lease has been
// applied; there's no guarantee that that lease is owned by the current replica
// or that it covers the requested timestamp. Callers are expected to test the
// resulting lease and possibly retry.
// If a transfer is in progress, a NotLeaderError directing to the recipient is
// sent on the returned chan.
func (r *Replica) requestLeaseLocked(
	ctx context.Context, timestamp hlc.Timestamp,
) <-chan leaseOrErr {
	// Propose a Raft command to get a lease for this replica.
	repDesc, err := r.getReplicaDescriptorLocked()
	if err != nil {
		errChan := make(chan leaseOrErr, 1)
		errChan <- leaseOrErr{pErr: roachpb.NewError(err)}
		return errChan
	}
	if transferLease, ok := r.mu.pendingLeaseRequest.TransferInProgress(
		r.mu.replicaID); ok {
		errChan := make(chan leaseOrErr, 1)
		errChan <- leaseOrErr{pErr: roachpb.NewError(
			newNotLeaseHolderError(&transferLease, r.store.StoreID(), r.mu.state.Desc))}
		return errChan
	}
	if r.store.IsDrainingLeases() {
		// We've retired from active duty.
		errChan := make(chan leaseOrErr, 1)
		errChan <- leaseOrErr{pErr: roachpb.NewError(newNotLeaseHolderError(nil, r.store.StoreID(), r.mu.state.Desc))}
		return errChan
	}
	return r.mu.pendingLeaseRequest.InitOrJoinRequest(
		ctx, repDesc, timestamp, r.mu.state.Desc.StartKey.AsRawKey(), false /* transfer */)
}

// AdminTransferLease transfers the LeaderLease to another replica. Only the
// current holder of the LeaderLease can do a transfer, because it needs to
// stop serving reads and proposing Raft commands (CPut is a read) after
// sending the transfer command. If it did not stop serving reads immediately,
// it would potentially serve reads with timestamps greater than the start
// timestamp of the new (transferred) lease. More subtly, the replica can't
// even serve reads or propose commands with timestamps lower than the start of
// the new lease because it could lead to read your own write violations (see
// comments on the stasis period in the Lease proto). We could, in principle,
// serve reads more than the maximum clock offset in the past. After a transfer
// is initiated, Replica.mu.pendingLeaseRequest.TransferInProgress() will start
// returning true.
//
// The method waits for any in-progress lease extension to be done, and it also
// blocks until the transfer is done. If a transfer is already in progress,
// this method joins in waiting for it to complete if it's transferring to the
// same replica. Otherwise, a NotLeaderError is returned.
func (r *Replica) AdminTransferLease(
	ctx context.Context, target roachpb.StoreID,
) error {
	// initTransferHelper inits a transfer if no extension is in progress.
	// It returns a channel for waiting for the result of a pending
	// extension (if any is in progress) and a channel for waiting for the
	// transfer (if it was successfully initiated).
	var nextLeaseHolder roachpb.ReplicaDescriptor
	initTransferHelper := func() (<-chan leaseOrErr, <-chan leaseOrErr, error) {
		r.mu.Lock()
		defer r.mu.Unlock()

		lease := r.mu.state.Lease
		if lease.OwnedBy(target) {
			// The target is already the lease holder. Nothing to do.
			return nil, nil, nil
		}
		desc := r.mu.state.Desc
		if !lease.OwnedBy(r.store.StoreID()) {
			return nil, nil, newNotLeaseHolderError(lease, r.store.StoreID(), desc)
		}
		// Verify the target is a replica of the range.
		var ok bool
		if nextLeaseHolder, ok = desc.GetReplicaDescriptor(target); !ok {
			return nil, nil, errors.Errorf("unable to find store %d in range %+v", target, desc)
		}

		if nextLease, ok := r.mu.pendingLeaseRequest.RequestPending(); ok &&
			nextLease.Replica != nextLeaseHolder {
			repDesc, err := r.getReplicaDescriptorLocked()
			if err != nil {
				return nil, nil, err
			}
			if nextLease.Replica == repDesc {
				// There's an extension in progress. Let's wait for it to succeed and
				// try again.
				return r.mu.pendingLeaseRequest.InitOrJoinRequest(
					ctx, nextLease.Replica,
					// the rest of the arguments don't matter; we know we're joining a
					// request, not initiating one.
					nextLease.Start, desc.StartKey.AsRawKey(), false /* transfer */), nil, nil
			}
			// Another transfer is in progress, and it's not transferring to the
			// same replica we'd like.
			return nil, nil, newNotLeaseHolderError(&nextLease, r.store.StoreID(), desc)
		}
		// No extension in progress; start a transfer.
		nextLeaseBegin := r.store.Clock().Now()
		// Don't transfer immediately after a node restart since we might have
		// served higher timestamps before the restart.
		nextLeaseBegin.Forward(
			hlc.ZeroTimestamp.Add(r.store.startedAt+int64(r.store.Clock().MaxOffset()), 0))
		transfer := r.mu.pendingLeaseRequest.InitOrJoinRequest(
			ctx, nextLeaseHolder, nextLeaseBegin,
			desc.StartKey.AsRawKey(), true /* transfer */)
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
			loe := <-transfer
			if loe.pErr != nil {
				return (<-transfer).pErr.GoError()
			}
			if loe.lease.Replica.StoreID != target {
				// The lease was transferred somewhere else.
				r.mu.Lock()
				rangeDesc := r.mu.state.Desc
				r.mu.Unlock()
				return newNotLeaseHolderError(&loe.lease, r.store.StoreID(), rangeDesc)
			}
			return nil
		}
		// Wait for the in-progress extension without holding the mutex.
		if r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent != nil {
			r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent(nextLeaseHolder)
		}
		<-extension
	}
}
