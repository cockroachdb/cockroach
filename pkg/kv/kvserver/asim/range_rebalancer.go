// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package asim

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// ReplicaLeaseMover handles lease transfers for a single range.
type ReplicaLeaseMover struct {
	changer state.Changer
	state   state.State
	rangeID state.RangeID
	tick    time.Time
	delay   func() time.Duration
}

// AdminTransferLease moves the lease to the requested store.
func (rlm *ReplicaLeaseMover) AdminTransferLease(
	ctx context.Context, target roachpb.StoreID,
) error {
	if !rlm.state.ValidTransfer(rlm.rangeID, state.StoreID(target)) {
		return errors.Errorf(
			"unable to transfer lease for r%d to store %d, invalid transfer.",
			rlm.rangeID, target)
	}

	if _, ok := rlm.changer.Push(rlm.tick, &state.LeaseTransferChange{
		RangeID:        rlm.rangeID,
		TransferTarget: state.StoreID(target),
		Wait:           rlm.delay(),
	}); !ok {
		return errors.Errorf(
			"unable to transfer lease for r%d to store %d, application failed.",
			rlm.rangeID, target)
	}
	return nil
}

// String returns info about the replica.
func (rr *RangeRebalancer) String() string {
	// TODO(kvoli): This method is on a replica / range in the actual code,
	// however it would be better to tick more than just a single range.
	return ""
}

// RangeRebalancer relocates replicas to the requested stores, and can transfer
// the lease for the range to the first target voter.
type RangeRebalancer struct {
	// TODO(kvoli): When using this struct, have a separate, per store range
	// rebalancer - as the allocator used is tied to it.
	changer   state.Changer
	allocator allocatorimpl.Allocator
	state     state.State
	delay     func(rangeSize int64, add bool) time.Duration
	tick      time.Time
	pending   []*relocateRangeArgs
	finished  map[state.Key][]*relocateRangeArgs
}

// NewRangeRebalancer returns a new range rebalancer, that may be used to
// enqueue range relocations and lease transfers.
func NewRangeRebalancer(
	changer state.Changer,
	allocator allocatorimpl.Allocator,
	s state.State,
	delay func(rangeSize int64, add bool) time.Duration,
	tick time.Time,
) *RangeRebalancer {
	return &RangeRebalancer{
		changer:   changer,
		allocator: allocator,
		state:     s,
		delay:     delay,
		tick:      tick,
		pending:   []*relocateRangeArgs{},
		finished:  make(map[state.Key][]*relocateRangeArgs),
	}
}

// Len is part of the container.Heap interface.
func (rr *RangeRebalancer) Len() int { return len(rr.pending) }

// Less is part of the container.Heap interface.
func (rr *RangeRebalancer) Less(i, j int) bool {
	a, b := rr.pending[i], rr.pending[j]
	if a.nextCheck.Equal(b.nextCheck) {
		return a.key < b.key
	}
	return a.nextCheck.Before(b.nextCheck)
}

// Swap is part of the container.Heap interface.
func (rr *RangeRebalancer) Swap(i, j int) {
	rr.pending[i], rr.pending[j] = rr.pending[j], rr.pending[i]
	rr.pending[i].index, rr.pending[j].index = i, j
}

// Push is part of the container.Heap interface.
func (rr *RangeRebalancer) Push(x interface{}) {
	n := len(rr.pending)
	item := x.(*relocateRangeArgs)
	item.index = n
	rr.pending = append(rr.pending, item)
}

// Pop is part of the container.Heap interface.
func (rr *RangeRebalancer) Pop() interface{} {
	old := rr.pending
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	old[n-1] = nil  // for gc
	rr.pending = old[0 : n-1]
	return item
}

// TransferLease uses a LeaseMover interface to move a lease between stores.
// The QPS is used to update stats for the stores.
func (rr *RangeRebalancer) TransferLease(
	ctx context.Context, rlm ReplicaLeaseMover, source, target roachpb.StoreID, rangeQPS float64,
) error {
	return rlm.AdminTransferLease(ctx, target)
}

// RelocateRange relocates replicas to the requested stores, and can transfer
// the lease for the range to the first target voter.
func (rr *RangeRebalancer) RelocateRange(
	ctx context.Context,
	key interface{},
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget,
	transferLeaseToFirstVoter bool,
) error {
	simKey, _ := key.(roachpb.Key)
	args := relocateRangeArgs{
		ctx:                       ctx,
		nextCheck:                 rr.tick,
		key:                       state.ToKey(simKey),
		voterTargets:              voterTargets,
		nonVoterTargets:           nonVoterTargets,
		transferLeaseToFirstVoter: transferLeaseToFirstVoter,
		errs:                      []error{},
	}
	heap.Push(rr, &args)

	// We return nil here and don't block on the changes. Any errors and the
	// success/fail of the operation will be reported associated with the key
	// given, on this struct.
	return nil
}

// relocateRangeArgs captures the arguments for a call to RelocateRange.
// RelocateRange is not atomic, it occurs using multiple "atomic" operations,
// iteratively. relocateRangeArgs captures these arguments and the
// nextCheckTick, representing the next tick time to try continuing the
// RelocateRange operation.
type relocateRangeArgs struct {
	ctx                           context.Context
	key                           state.Key
	voterTargets, nonVoterTargets []roachpb.ReplicationTarget
	transferLeaseToFirstVoter     bool

	// Control variables that are not arguments.
	nextCheck time.Time
	errs      []error
	index     int
}

func (ra *relocateRangeArgs) errors() error {
	var err error
	for i := range ra.errs {
		if i == 0 {
			err = ra.errs[i]
		} else {
			err = errors.CombineErrors(err, ra.errs[i])
		}
	}
	return err
}

func (ra *relocateRangeArgs) error(rng state.Range, err error) {
	augmentedErr := errors.Wrapf(err, "Unable to relocate key=%d %s,voters=%v,nonvoters=%v",
		ra.key, rng.Descriptor(), ra.voterTargets, ra.nonVoterTargets)
	ra.errs = append(ra.errs, augmentedErr)
}

// Tick iterates through pending relocate range changes and processes them up
// to the current tick.
func (rr *RangeRebalancer) Tick(tick time.Time) {
	// Manually set the tick for future operations, as the caller doesn't have
	// access to the tick time.
	rr.tick = tick

	for len(rr.pending) > 0 {
		i := heap.Pop(rr)
		nextRelocation, _ := i.(*relocateRangeArgs)

		// There are no more pending checks.
		if nextRelocation.nextCheck.After(tick) {
			heap.Push(rr, nextRelocation)
			return
		}
		// There are still pending checks, process and push back to pending if
		// not done.
		if nextCheck, done := rr.process(nextRelocation); !done {
			nextRelocation.nextCheck = nextCheck
			heap.Push(rr, nextRelocation)
		} else {
			// This relocation has finished, either by erroring or succeeding.
			// Push it onto the finished stack.
			if _, ok := rr.finished[nextRelocation.key]; !ok {
				rr.finished[nextRelocation.key] = []*relocateRangeArgs{}
			}
			rr.finished[nextRelocation.key] = append(rr.finished[nextRelocation.key], nextRelocation)
		}
	}
}

// Check returns true and the error, if any, for relocations on the given key.
func (rr *RangeRebalancer) Check(key state.Key) (done bool, _ error) {
	if args, ok := rr.finished[key]; ok && len(args) > 0 {
		nextDone := args[0]
		rr.finished[key] = rr.finished[key][1:]
		return true, nextDone.errors()
	}

	return false, nil
}

// process operates on an ongoing range relocation, it attempts to find the
// next change to process for the relocation and enqueues it into the state
// changer. When complete, it returns true, else the next time to check this
// pending relocation.
func (rr *RangeRebalancer) process(args *relocateRangeArgs) (nextCheck time.Time, done bool) {
	tick := args.nextCheck
	rng := rr.state.RangeFor(args.key)
	options := SimRelocateOneOptions{allocator: rr.allocator, state: rr.state}
	ops, leaseTarget, err := kvserver.RelocateOne(
		args.ctx,
		rng.Descriptor(),
		args.voterTargets,
		args.nonVoterTargets,
		args.transferLeaseToFirstVoter,
		&options,
	)
	if err != nil {
		args.error(rng, err)
		return tick, true
	}

	if leaseTarget != nil {
		leaseholderStore, ok := rr.state.LeaseholderStore(rng.RangeID())
		if !ok {
			err = errors.Newf(" Lease transfer failed to %s. cannot find leaseholder", leaseTarget.StoreID.String())
			args.error(rng, err)
			return tick, true
		}

		if leaseholderStore.StoreID() != state.StoreID(leaseTarget.StoreID) {
			if ok := rr.state.TransferLease(rng.RangeID(), state.StoreID(leaseTarget.StoreID)); !ok {
				leaseholder, err := options.Leaseholder(args.ctx, args.key.ToRKey())
				if err != nil {
					args.error(rng, err)
					return tick, true
				}
				err = errors.Newf("Lease transfer failed to %s. Existing leaseholder %s", leaseTarget.StoreID.String(), leaseholder)
				args.error(rng, err)
				return tick, true
			}
		}
	}

	if len(ops) == 0 {
		return tick, true
	}

	change := state.ReplicaChange{
		RangeID: rng.RangeID(),
	}

	// The replica changer currently only supports at most two operations
	// atomically; an add and a remove of a voter. When there are more than
	// this number of changes, error.
	// TOOD(kvoli): Support arbitrary number of operations for changes.
	if len(ops) > 2 {
		err = errors.Newf("Expected 2 ops, found %d", len(ops))
		args.error(rng, err)
		return tick, true
	}

	var add, remove int
	for _, op := range ops {
		switch op.ChangeType {
		case roachpb.ADD_VOTER:
			add++
			change.Add = state.StoreID(op.Target.StoreID)
			change.Wait = rr.delay(rng.Size(), true /* use range size */)
		case roachpb.REMOVE_VOTER:
			remove++
			change.Remove = state.StoreID(op.Target.StoreID)
		default:
			err = errors.Newf("Unrecognized operation type %s", op)
			args.error(rng, err)
			return tick, true
		}
	}

	if add > 1 || remove > 1 {
		err = errors.Newf("Expected at most 1 add or remove each %d adds, %d removes", add, remove)
		args.error(rng, err)
		return tick, true
	}

	completeAt, ok := rr.changer.Push(tick, &change)
	if !ok {
		err = errors.Newf("tick %d: Changer did not accept op %+v", change)
		args.error(rng, err)
		return tick, true
	}

	return completeAt, false
}

// SimRelocateOneOptions holds the necessary information to call RelocateOne,
// to generate a suggested replication change.
type SimRelocateOneOptions struct {
	allocator allocatorimpl.Allocator
	state     state.State
}

// Allocator returns the allocator for the store this replica is on.
func (s *SimRelocateOneOptions) Allocator() allocatorimpl.Allocator {
	return s.allocator
}

// SpanConfig returns the span configuration for the range with start key.
func (s *SimRelocateOneOptions) SpanConfig(
	ctx context.Context, startKey roachpb.RKey,
) (roachpb.SpanConfig, error) {
	return s.state.RangeFor(state.ToKey(startKey.AsRawKey())).SpanConfig(), nil
}

// Leaseholder returns the descriptor of the replica which holds the lease on
// the range with start key.
func (s *SimRelocateOneOptions) Leaseholder(
	ctx context.Context, startKey roachpb.RKey,
) (roachpb.ReplicaDescriptor, error) {
	if desc, ok := s.state.LeaseHolderReplica(s.state.RangeFor(state.ToKey(startKey.AsRawKey())).RangeID()); ok {
		return desc.Descriptor(), nil
	}
	return roachpb.ReplicaDescriptor{}, errors.Errorf("Unable to find leaseholder for key %s.", startKey)
}

// LHRemovalAllowed returns true if the lease holder may be removed during
// a replication change.
func (s *SimRelocateOneOptions) LHRemovalAllowed(ctx context.Context) bool {
	return true
}
