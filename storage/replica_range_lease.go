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

// This file contains replica methods related to LeaderLeases.

package storage

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

// pendingLeaseRequest coalesces LeaderLease requests and lets callers
// join an in-progress one and wait for the result.
// The actual execution of the LeaderLease Raft request is delegated to a
// replica.
// Methods are not thread-safe; a pendingLeaseRequest is logically part of
// a replica, so replica.mu should be used to synchronize all calls.
type pendingLeaseRequest struct {
	// Slice of channels to send on after lease acquisition.
	// If empty, then no request is in progress.
	llChans []chan *roachpb.Error
	// nextLease is the pending LeaderLease request, if any. It can be used to
	// figure out if we're in the process of extending our own lease, or
	// transferring it to another replica.
	nextLease roachpb.Lease
}

// RequestPending returns the pending Lease, if one is in progress.
// Otherwise, nil.
func (p *pendingLeaseRequest) RequestPending() *roachpb.Lease {
	pending := len(p.llChans) > 0
	if pending {
		return &p.nextLease
	}
	return nil
}

// InitOrJoinRequest executes a LeaderLease command asynchronously and returns a
// channel on which the result will be posted. If there's already a request in
// progress, we join in waiting for the results of that request.
// It is an error to call InitOrJoinRequest() while a request is in progress
// naming another replica as lease holder.
//
// replica is used to schedule and execute async work (proposing a LeaderLease
// command). replica.mu is locked when delivering results, so calls from the
// replica happen either before or after a result for a pending request has
// happened.
//
// transfer needs to be set if the request represents a lease transfer (as
// opposed to an extension, or acquiring the lease when there's no leader).
//
// Note: Once this function gets a context to be used for cancellation, instead
// of replica.store.Stopper().ShouldDrain(), care will be needed for cancelling
// the Raft command, similar to replica.addWriteCmd.
func (p *pendingLeaseRequest) InitOrJoinRequest(
	replica *Replica,
	nextLeaseHolder roachpb.ReplicaDescriptor,
	timestamp hlc.Timestamp,
	startKey roachpb.Key,
	transfer bool,
) <-chan *roachpb.Error {
	if nextLease := p.RequestPending(); nextLease != nil {
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
	// No request in progress. Let's propose a LeaderLease command asynchronously.
	// TODO(tschottdorf): get duration from configuration, either as a
	// config flag or, later, dynamically adjusted.
	startStasis := timestamp.Add(int64(replica.store.ctx.leaderLeaseActiveDuration), 0)
	expiration := startStasis.Add(int64(replica.store.Clock().MaxOffset()), 0)
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
		leaseReq = &roachpb.LeaseTransferRequest{
			Span:  reqSpan,
			Lease: reqLease,
		}
	} else {
		leaseReq = &roachpb.LeaderLeaseRequest{
			Span:  reqSpan,
			Lease: reqLease,
		}
	}
	if !replica.store.Stopper().RunAsyncTask(func() {
		// Propose a LeaderLease command and wait for it to apply.
		var execPErr *roachpb.Error
		ba := roachpb.BatchRequest{}
		ba.Timestamp = replica.store.Clock().Now()
		ba.RangeID = replica.RangeID
		ba.Add(leaseReq)
		// Send lease request directly to raft in order to skip unnecessary
		// checks from normal request machinery, (e.g. the command queue).
		// Note that the command itself isn't traced, but usually the caller
		// waiting for the result has an active Trace.
		ch, _, err := replica.proposeRaftCommand(
			replica.context(context.Background()), ba)
		if err != nil {
			execPErr = roachpb.NewError(err)
		} else {
			// If the command was committed, wait for the range to apply it.
			select {
			case c := <-ch:
				if c.Err != nil {
					if log.V(1) {
						log.Infof("failed to acquire lease for replica %s: %s", replica.store, c.Err)
					}
					execPErr = c.Err
				}
			case <-replica.store.Stopper().ShouldDrain():
				execPErr = roachpb.NewError(
					replica.newNotLeaderError(nil, replica.store.StoreID(), replica.Desc()))
			}
		}

		// Send result of lease to all waiter channels.
		replica.mu.Lock()
		defer replica.mu.Unlock()
		for i, llChan := range p.llChans {
			// Don't send the same pErr object twice; this can lead to races. We could
			// clone every time but it's more efficient to send pErr itself to one of
			// the channels (the last one; if we send it earlier the race can still
			// happen).
			if i == len(p.llChans)-1 {
				llChan <- execPErr
			} else {
				llChan <- protoutil.Clone(execPErr).(*roachpb.Error) // works with `nil`
			}
		}
		p.llChans = p.llChans[:0]
		p.nextLease = roachpb.Lease{}
	}) {
		// We failed to start the asynchronous task. Send a blank NotLeaderError
		// back to indicate that we have no idea who the range lease holder might
		// be; we've withdrawn from active duty.
		llChan <- roachpb.NewError(
			replica.newNotLeaderError(nil, replica.store.StoreID(), replica.mu.state.Desc))
		return llChan
	}
	p.llChans = append(p.llChans, llChan)
	p.nextLease = reqLease
	return llChan
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
// of transferring aways its range lease. This next lease indicates the next
// lease holder. Returns nil if no lease transfer is in progress.
//
// It is assumed that the replica owning this pendingLeaseRequest owns the
// LeaderLease.
//
// replicaID is the ID of the parent replica.
func (p *pendingLeaseRequest) TransferInProgress(
	replicaID roachpb.ReplicaID,
) *roachpb.Lease {
	if nextLease := p.RequestPending(); nextLease != nil {
		// Is the lease being transferred? (as opposed to just extended)
		if replicaID != nextLease.Replica.ReplicaID {
			return nextLease
		}
	}
	return nil
}

// requestLeaseLocked executes a request to obtain or extend a lease
// asynchronously and returns a channel on which the result will be posted. If
// there's already a request in progress, we join in waiting for the results of
// that request. Unless an error is returned, the obtained lease will be valid
// for a time interval containing the requested timestamp.
// If a transfer is in progress, a NotLeaderError directing to the transferee is
// sent on the returned chan.
func (r *Replica) requestLeaseLocked(timestamp hlc.Timestamp) <-chan *roachpb.Error {
	// Propose a Raft command to get a lease for this replica.
	replica, err := r.getReplicaLocked()
	if err != nil {
		llChan := make(chan *roachpb.Error, 1)
		if _, ok := err.(*errReplicaNotInRange); ok {
			llChan <- roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
		} else {
			llChan <- roachpb.NewError(err)
		}
		return llChan
	}
	transferLease := r.mu.pendingLeaseRequest.TransferInProgress(replica.ReplicaID)
	if transferLease != nil {
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(
			r.newNotLeaderError(transferLease, r.store.StoreID(), r.mu.state.Desc))
		return llChan
	}
	if r.store.IsDrainingLeadership() {
		// We've retired from active duty.
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID(), r.mu.state.Desc))
		return llChan
	}
	return r.mu.pendingLeaseRequest.InitOrJoinRequest(
		r, *replica, timestamp, r.mu.state.Desc.StartKey.AsRawKey(), false /* transfer */)
}

// TransferLease transfers the LeaderLease to another replica. Only the current
// holder of the LeaderLease can do a transfer, because it needs to stop serving
// reads and proposing Raft commands (CPUT is a read) after sending the transfer
// command. If it were stop serving reads immediately, it would potentially
// serve reads with timestamps greater than the start timestamp of the new
// (transferred) lease. More subtly, the replica can't even serve reads or
// propose commands with timestamps lower than the start of the new lease
// because it could lead to read your own write violations (see comments on the
// stasis period in the Lease proto). We could, in principle, serve reads more
// than the maximum clock offset in the past.
//
// The method waits for any in-progress lease extension to be done, and it also
// blocks until the transfer is done.
//
// TODO(andrei): figure out how to persist the "not serving" state across node
// restarts.
func (r *Replica) TransferLease(
	nextLeader roachpb.ReplicaDescriptor,
) error {
	// initTransferHelper inits a transfer if no extension is in progress.
	// It returns a channel for waiting for the result of a pending
	// extension (if any is in progress) and a channel for waiting for the
	// transfer (if it was successfully initiated).
	initTransferHelper := func() (<-chan *roachpb.Error, <-chan *roachpb.Error, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		lease := r.mu.state.Lease
		if !lease.OwnedBy(r.store.StoreID()) {
			return nil, nil, r.newNotLeaderError(lease, r.store.StoreID(), r.Desc())
		}
		nextLease := r.mu.pendingLeaseRequest.RequestPending()
		if nextLease != nil && nextLease.Replica != nextLeader {
			replica, err := r.getReplicaLocked()
			if err != nil {
				return nil, nil, err
			}
			if nextLease.Replica == *replica {
				// There's an extension in progress. Let's wait for it to succeed and
				// try again.
				return r.mu.pendingLeaseRequest.JoinRequest(), nil, nil
			}
			// Another transfer is in progress, and it's not transferring to the
			// same replica we'd like.
			return nil, nil, r.newNotLeaderError(nextLease, r.store.StoreID(), r.mu.state.Desc)
		}
		// No extension in progress; start a transfer.
		nextLeaseBegin := r.store.Clock().Now()
		// Don't transfer immediately after a node restart since we might have
		// served higher timestamps before the restart.
		nextLeaseBegin.Forward(
			hlc.Timestamp{WallTime: r.store.startedAt, Logical: 0}.Add(
				int64(r.store.Clock().MaxOffset()), 0))
		transfer := r.mu.pendingLeaseRequest.InitOrJoinRequest(
			r, nextLeader, nextLeaseBegin,
			r.mu.state.Desc.StartKey.AsRawKey(), true /* transfer */)
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
			return (<-transfer).GoError()
		}
		// Wait for the in-progress extension without holding the mutex.
		if extension != nil {
			if r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent != nil {
				r.store.TestingKnobs().LeaseTransferBlockedOnExtensionEvent(nextLeader)
			}
			<-extension
		}
	}
}
