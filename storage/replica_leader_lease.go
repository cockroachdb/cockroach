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
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/protoutil"
)

// pendingLeaderLeaseRequest coalesces LeaderLease requests and lets callers
// join an in-progress one and wait for the result.
// The actual execution of the LeaderLease Raft request is delegated to a
// replica.
// Methods are not thread-safe; a pendingLeaderLeaseRequest is logically part of
// a replica, so replica.mu should be used to synchronize all calls.
type pendingLeaderLeaseRequest struct {
	// Slice of channels to send on after leader lease acquisition.
	// If empty, then no request is in progress.
	llChans []chan *roachpb.Error
	// nextLease is the pending LeaderLease request, if any. It can be used to
	// figure out if we're in the process of extending our own lease, or
	// transferring it to another replica.
	nextLease roachpb.Lease
}

// RequestPending returns the pending Lease, if one is in progress.
// Otherwise, nil.
func (p *pendingLeaderLeaseRequest) RequestPending() *roachpb.Lease {
	pending := len(p.llChans) > 0
	if pending {
		return &p.nextLease
	}
	return nil
}

// InitOrJoinRequest executes a LeaderLease command asynchronously and returns a
// promise for the result. If there's already a request in progress, we check
// that the request is asking for a lease for the same replica as the one this
// call wants to crown leader. If so, we join in waiting the results of that
// request. If not, an error is immediately returned.
//
// replica is used to schedule and execute async work (proposing a LeaderLease
// command). replica.mu is locked when delivering results, so calls from the
// replica happen either before or after a result for a pending request has
// happened.
// transfer needs to be set if the request represents a lease transfer (as
// opposed to an extension, or acquiring the lease when there's no leader).
func (p *pendingLeaderLeaseRequest) InitOrJoinRequest(
	replica *Replica,
	nextLeader roachpb.ReplicaDescriptor,
	timestamp roachpb.Timestamp,
	startKey roachpb.Key,
	transfer bool,
) <-chan *roachpb.Error {
	llChan := make(chan *roachpb.Error, 1)
	if nextLease := p.RequestPending(); nextLease != nil {
		if nextLease.Replica.ReplicaID == nextLeader.ReplicaID {
			// Join a pending request asking for the same replica to become leader.
			p.llChans = append(p.llChans, llChan)
			return llChan
		}
		llChan <- roachpb.NewErrorf("request for different replica in progress "+
			"(requesting: %+v, in progress: %+v)",
			nextLeader.ReplicaID, nextLease.Replica.ReplicaID)
		return llChan
	}
	// No request in progress. Let's propose a LeaderLease command asynchronously.
	// TODO(tschottdorf): get duration from configuration, either as a
	// config flag or, later, dynamically adjusted.
	startStasis := timestamp.Add(int64(replica.store.ctx.leaderLeaseActiveDuration), 0)
	expiration := startStasis.Add(int64(replica.leaseTransferStasisDuration()), 0)
	leaseReq := roachpb.LeaderLeaseRequest{
		Span: roachpb.Span{
			Key: startKey,
		},
		Lease: roachpb.Lease{
			Start:       timestamp,
			StartStasis: startStasis,
			Expiration:  expiration,
			Replica:     nextLeader,
		},
		Transfer: transfer,
	}
	if !replica.store.Stopper().RunAsyncTask(func() {
		pErr := replica.executeLeaderLeaseCommand(leaseReq, timestamp, startKey)

		// Send result of leader lease to all waiter channels.
		replica.mu.Lock()
		defer replica.mu.Unlock()
		for i, llChan := range p.llChans {
			// Don't send the same pErr object twice; this can lead to races. We could
			// clone every time but it's more efficient to send pErr itself to one of
			// the channels (the last one; if we send it earlier the race can still
			// happen).
			if i == len(p.llChans)-1 {
				llChan <- pErr
			} else {
				llChan <- protoutil.Clone(pErr).(*roachpb.Error) // works with `nil`
			}
		}
		p.llChans = p.llChans[:0]
		p.nextLease = roachpb.Lease{}
	}) {
		// We failed to start the asynchronous task. Send a blank NotLeaderError
		// back to indicate that we have no idea who the leader might be; we've
		// withdrawn from active duty.
		llChan <- roachpb.NewError(
			replica.newNotLeaderError(nil, replica.store.StoreID(), replica.mu.desc))
		return llChan
	}
	p.llChans = append(p.llChans, llChan)
	p.nextLease = leaseReq.Lease
	return llChan
}

// InTransferStasis returns true if the replica cannot propose a Raft command with the
// given timestamp because its LeaderLease is in the stasis period induced by a
// lease transfer. If the first result is true, a lease indicating the next
// leader is also returned.
//
// It is assumed that the replica owning this pendingLeaderLeaseRequest owns the
// LeaderLease.
//
// replicaID is the ID of the owning replica.
func (p *pendingLeaderLeaseRequest) InTransferStasis(
	timestamp roachpb.Timestamp,
	replicaID roachpb.ReplicaID,
	leaseTransferStasisDuration time.Duration,
) (bool, *roachpb.Lease) {
	if nextLease := p.RequestPending(); nextLease != nil {
		// Is the lease being transferred? (as opposed to just extended)
		if replicaID != nextLease.Replica.ReplicaID {
			// Does the requested timestamp fall in the transfer's stasis period?
			stasisStart := nextLease.Start.Add(-leaseTransferStasisDuration.Nanoseconds(), 0)
			if stasisStart.Less(timestamp) {
				return true, nextLease
			}
		}
	}
	return false, nil
}

func (r *Replica) getOrExtendLeaderLeaseLocked(timestamp roachpb.Timestamp) <-chan *roachpb.Error {
	// Propose a Raft command to get a leader lease for this replica.
	desc := r.mu.desc
	_, replica := desc.FindReplica(r.store.StoreID())
	if replica == nil {
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
		return llChan
	}
	if r.store.IsDrainingLeadership() {
		// We've retired from active duty.
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID(), r.mu.desc))
		return llChan
	}
	return r.mu.pendingLeaderLeaseRequest.InitOrJoinRequest(
		r, *replica, timestamp, desc.StartKey.AsRawKey(), false /* transfer */)
}

// TransferLeaderLease transfers the LeaderLease to another replica. Only the
// current holder of the LeaderLease can do a transfer.
// TransferLeaderLease blocks until the transfer is done.
//
// Upon a transfer, the current leader enters a stasis period to avoid the
// "single-register linearizability failure" described in
// https://github.com/cockroachdb/cockroach/blob/ce67233/roachpb/data.proto#L329
// The next lease will begin after the stasis period is over.
func (r *Replica) TransferLeaderLease(
	nextLeader roachpb.ReplicaDescriptor,
) *roachpb.Error {
	r.mu.Lock()

	lease := r.mu.leaderLease
	if !lease.OwnedBy(r.store.StoreID()) {
		r.mu.Unlock()
		return roachpb.NewError(r.newNotLeaderError(lease, r.store.StoreID(), r.Desc()))
	}
	// !!! do I need to check r.store.IsDrainingLeadership() ?
	nextLeaseBegin := r.store.Clock().Now().Add(r.leaseTransferStasisDuration().Nanoseconds(), 0)

	// This will err if there's already an extension or transfer in progress.
	future := r.mu.pendingLeaderLeaseRequest.InitOrJoinRequest(
		r, nextLeader, nextLeaseBegin, r.mu.desc.StartKey.AsRawKey(), true /* transfer */)

	r.mu.Unlock()
	return <-future
}

// leaseTransferStasisDuration returns the duration of the stasis period that
// a leader must observe after intiating a lease transfer. If the lease
// transferred starts at t, the current leader cannot serve reads with timestamp
// > t - r.leaseTransferStasisDuration().
// That interval constitutes a period of unavailability (nobody'll serve reads
// with those timestamps) that's necessary for preventing the linearizability
// violation described in the Lease proto.
func (r *Replica) leaseTransferStasisDuration() time.Duration {
	return r.store.Clock().MaxOffset()
}

// executeLeaderLeaseCommand proposes a LeaderLeader and waits for its
// completion.
func (r *Replica) executeLeaderLeaseCommand(
	nextLease roachpb.LeaderLeaseRequest,
	timestamp roachpb.Timestamp,
	startKey roachpb.Key,
) *roachpb.Error {
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.RangeID = r.RangeID
	ba.Add(&nextLease)

	// Send lease request directly to raft in order to skip unnecessary
	// checks from normal request machinery, (e.g. the command queue).
	// Note that the command itself isn't traced, but usually the caller
	// waiting for the result has an active Trace.
	ch, _, err := r.proposeRaftCommand(r.context(context.Background()), ba)
	if err != nil {
		return roachpb.NewError(err)
	}

	// If the command was committed, wait for the range to apply it.
	select {
	case c := <-ch:
		if c.Err != nil {
			if log.V(1) {
				log.Infof("failed to acquire leader lease for replica %s: %s", r.store, c.Err)
			}
		}
		return c.Err
	case <-r.store.Stopper().ShouldDrain():
		return roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID(), r.Desc()))
	}
}
