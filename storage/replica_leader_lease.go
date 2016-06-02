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
	"sync"
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
type pendingLeaderLeaseRequest struct {
	mu sync.Mutex
	// Slice of channels to send on after leader lease acquisition.
	// If empty, then no request is in progress.
	llChans []chan *roachpb.Error
	// request is the pending LeaderLease request, if any. It can be used to
	// figure out if we're in the process of extending our own lease, or
	// transferring it to another replica.
	request roachpb.LeaderLeaseRequest
}

// RequestPending returns the pending LeaderLeaseRequest, if one is in progress.
func (p *pendingLeaderLeaseRequest) RequestPending() (roachpb.LeaderLeaseRequest, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requestPendingLocked()
}

func (p *pendingLeaderLeaseRequest) requestPendingLocked() (roachpb.LeaderLeaseRequest, bool) {
	pending := len(p.llChans) > 0
	if pending {
		return p.request, true
	}
	return roachpb.LeaderLeaseRequest{}, false
}

// InitOrJoinRequest executes a LeaderLease command asynchronously and returns a
// promise for the result. If there's already a request in progress, we check
// that the request is asking for a lease for the same replica as the one this
// call wants to crown leader. If so, we join in waiting the results of that
// request. If not, an error is immediately returned.
func (p *pendingLeaderLeaseRequest) InitOrJoinRequest(
	replica *Replica,
	nextLeader roachpb.ReplicaDescriptor,
	timestamp roachpb.Timestamp,
	startKey roachpb.Key,
) <-chan *roachpb.Error {
	p.mu.Lock()
	defer p.mu.Unlock()
	llChan := make(chan *roachpb.Error, 1)
	if _, ok := p.requestPendingLocked(); ok {
		if p.request.Lease.Replica.ReplicaID == replica.GetReplica().ReplicaID {
			// Join a pending request asking for the same replica to become leader.
			p.llChans = append(p.llChans, llChan)
			return llChan
		}
		llChan <- roachpb.NewErrorf("request for different replica in progress")
		return llChan
	}

	// No request in progress. Let's propose a LeaderLease command asynchronously.
	if !replica.store.Stopper().RunAsyncTask(func() {
		pErr := replica.executeLeaderLeaseCommand(nextLeader, timestamp, startKey)

		// Send result of leader lease to all waiter channels.
		p.mu.Lock()
		defer p.mu.Unlock()
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
	}) {
		// We failed to start the asynchronous task. Send a blank NotLeaderError
		// back to indicate that we have no idea who the leader might be; we've
		// withdrawn from active duty.
		llChan <- roachpb.NewError(replica.newNotLeaderError(nil, replica.store.StoreID()))
		return llChan
	}
	p.llChans = append(p.llChans, llChan)
	return llChan
}

// getOrExtendLeaderLease sends a request to obtain or extend a leader
// lease for this replica. Unless an error is returned, the obtained
// lease will be valid for a time interval containing the requested
// timestamp. Only a single lease request may be pending at a time.
func (r *Replica) getOrExtendLeaderLease(timestamp roachpb.Timestamp) <-chan *roachpb.Error {
	// Propose a Raft command to get a leader lease for this replica.
	desc := r.Desc()
	_, replica := desc.FindReplica(r.store.StoreID())
	if replica == nil {
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(roachpb.NewRangeNotFoundError(r.RangeID))
		return llChan
	}
	if r.store.IsDrainingLeadership() {
		// We've retired from active duty.
		llChan := make(chan *roachpb.Error, 1)
		llChan <- roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID()))
		return llChan
	}
	return r.pendingLeaderLeaseRequest.InitOrJoinRequest(
		r, *replica, timestamp, desc.StartKey.AsRawKey())
}

// transferLeaderLease transfers the LeaderLease to another replica. Only the
// current holder of the LeaderLease can do a transfer.
// transferLeaderLease blocks until the transfer is done.
//
// Upon a transfer, the current leader enters a stasis period to avoid the
// "single-register linearizability failure" described in
// https://github.com/cockroachdb/cockroach/blob/ce67233/roachpb/data.proto#L329
// The next lease will begin after the stasis period is over.
func (r *Replica) transferLeaderLease(
	nextLeader roachpb.ReplicaDescriptor,
) *roachpb.Error {
	r.mu.Lock()

	lease, _ := r.getLeaderLease()
	if !lease.OwnedBy(r.store.StoreID()) {
		r.mu.Unlock()
		return roachpb.NewError(r.newNotLeaderError(lease, r.store.StoreID()))
	}
	// !!! do I need to check r.store.IsDrainingLeadership() ?
	nextLeaseBegin := r.store.Clock().Now().Add(r.leaseTransferStasisDuration().Nanoseconds(), 0)

	// This will err if there's already an extension or transfer in progress.
	future := r.pendingLeaderLeaseRequest.InitOrJoinRequest(
		r, nextLeader, nextLeaseBegin, r.Desc().StartKey.AsRawKey())

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
	nextLeader roachpb.ReplicaDescriptor,
	timestamp roachpb.Timestamp,
	startKey roachpb.Key,
) *roachpb.Error {
	// TODO(tschottdorf): get duration from configuration, either as a
	// config flag or, later, dynamically adjusted.
	startStasis := timestamp.Add(int64(LeaderLeaseActiveDuration), 0)
	expiration := startStasis.Add(int64(r.store.Clock().MaxOffset()), 0)

	// Prepare a Raft command to get a leader lease for this replica.
	args := &roachpb.LeaderLeaseRequest{
		Span: roachpb.Span{
			Key: startKey,
		},
		Lease: roachpb.Lease{
			Start:       timestamp,
			StartStasis: startStasis,
			Expiration:  expiration,
			Replica:     nextLeader,
		},
	}
	ba := roachpb.BatchRequest{}
	ba.Timestamp = r.store.Clock().Now()
	ba.RangeID = r.RangeID
	ba.Add(args)

	// Send lease request directly to raft in order to skip unnecessary
	// checks from normal request machinery, (e.g. the command queue).
	// Note that the command itself isn't traced, but usually the caller
	// waiting for the result has an active Trace.
	cmd, err := r.proposeRaftCommand(r.context(context.Background()), ba)
	if err != nil {
		return roachpb.NewError(err)
	}

	// If the command was committed, wait for the range to apply it.
	select {
	case c := <-cmd.done:
		if c.Err != nil {
			if log.V(1) {
				log.Infof("failed to acquire leader lease for replica %s: %s", r.store, c.Err)
			}
		}
		return c.Err
	case <-r.store.Stopper().ShouldDrain():
		return roachpb.NewError(r.newNotLeaderError(nil, r.store.StoreID()))
	}
}

// CanPropose returns true if the replica can propose a Raft command with the
// given timestamp. In order for that to be true, the replica needs both:
// a) to own the LeaderLease, and the lease must cover the timestamp (this takes
// into account the stasis period of the lease)
// b) to not be in the process of transferring away the LeaderLease
// It also returns the lease to be used in case the client needs to be
// redirected to a new leader.
func (r *Replica) CanPropose(timestamp roachpb.Timestamp) (bool, roachpb.Lease) {
	lease, _ := r.getLeaderLease()
	if !(lease.OwnedBy(r.store.StoreID()) && lease.Covers(timestamp)) {
		return false, *lease
	}
	transferStartTs := roachpb.ZeroTimestamp
	if req, ok := r.pendingLeaderLeaseRequest.RequestPending(); ok {
		// Is the lease being transferred? (as opposed to just extended)
		if r.GetReplica().ReplicaID != req.Lease.Replica.ReplicaID {
			transferStartTs = req.Lease.Start
			stasisStart := transferStartTs.Add(-r.leaseTransferStasisDuration().Nanoseconds(), 0)
			if stasisStart.Less(timestamp) {
				return false, req.Lease
			}
		}
	}
	return true, *lease
}
