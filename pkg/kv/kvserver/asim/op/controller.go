// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package op

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator/allocatorimpl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/config"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// DispatchedTicket associates a dispatched operation with a ticket id. It can
// be used to retrieve the status of a dispatched operation.
type DispatchedTicket int

// Controller manages scheduling and monitoring of reconfiguration operations
// such as relocating replicas and lease transfers. It represents a higher
// level construct than the state changer, using it internally.
type Controller interface {
	// Dispatch enqueues an operation to be processed. It returns a ticket
	// associated with the operation that may be used to check on the operation
	// progress.
	Dispatch(context.Context, time.Time, state.State, ControlledOperation) DispatchedTicket
	// Tick iterates through pending operations and processes them up to the
	// current tick.
	Tick(context.Context, time.Time, state.State)
	// Check checks the progress of the operation associated with the ticket
	// given. If the ticket exists, it returns the operation and true, else
	// false.
	Check(DispatchedTicket) (ControlledOperation, bool)
}

type controller struct {
	changer   state.Changer
	allocator allocatorimpl.Allocator
	settings  *config.SimulationSettings

	pending   *priorityQueue
	ticketGen DispatchedTicket
	tickets   map[DispatchedTicket]ControlledOperation

	storeID state.StoreID
}

// NewController returns a new Controller implementation.
func NewController(
	changer state.Changer,
	allocator allocatorimpl.Allocator,
	settings *config.SimulationSettings,
	storeID state.StoreID,
) Controller {
	return &controller{
		changer:   changer,
		allocator: allocator,
		settings:  settings,
		pending:   &priorityQueue{items: []*queuedOp{}},
		tickets:   make(map[DispatchedTicket]ControlledOperation),
		storeID:   storeID,
	}
}

// Dispatch enqueues an operation to be processed. It returns a ticket
// associated with the operation that may be used to check on the operation
// progress.
func (c *controller) Dispatch(
	ctx context.Context, tick time.Time, state state.State, co ControlledOperation,
) DispatchedTicket {
	c.ticketGen++
	ticket := c.ticketGen
	c.tickets[ticket] = co

	qop := &queuedOp{ControlledOperation: co}
	heap.Push(c.pending, qop)
	c.Tick(ctx, tick, state)
	return ticket
}

// Tick iterates through pending operations and processes them up to the
// current tick.
func (c *controller) Tick(ctx context.Context, tick time.Time, state state.State) {
	for c.pending.Len() > 0 {
		i := heap.Pop(c.pending)
		qop, _ := i.(*queuedOp)
		nextOp := qop.ControlledOperation

		// There are no more pending checks.
		if nextOp.Next().After(tick) {
			heap.Push(c.pending, &queuedOp{ControlledOperation: nextOp})
			return
		}

		c.process(ctx, tick, state, nextOp)

		// There are still pending checks, process and push back to pending if
		// not done.
		if done, _ := nextOp.Done(); !done {
			heap.Push(c.pending, &queuedOp{ControlledOperation: nextOp})
		}
	}
}

// Check checks the progress of the operation associated with the ticket given.
// If the ticket exists, it returns the operation and true, else false.
func (c *controller) Check(ticket DispatchedTicket) (op ControlledOperation, ok bool) {
	op, ok = c.tickets[ticket]
	if ok {
		delete(c.tickets, ticket)
	}
	return op, ok
}

func (c *controller) process(
	ctx context.Context, tick time.Time, state state.State, co ControlledOperation,
) {
	switch op := co.(type) {
	case *RelocateRangeOp:
		if err := c.processRelocateRange(ctx, tick, state, op); err != nil {
			op.error(err)
			op.done = true
			op.complete = tick
		}
	case *TransferLeaseOp:
		if err := c.processTransferLease(ctx, tick, state, op); err != nil {
			op.error(err)
			op.done = true
			op.complete = tick
		}

	default:
		return
	}
}

func (c *controller) processRelocateRange(
	ctx context.Context, tick time.Time, s state.State, ro *RelocateRangeOp,
) error {
	rng := s.RangeFor(ro.key)
	options := SimRelocateOneOptions{allocator: c.allocator, state: s}
	ops, leaseTarget, err := kvserver.RelocateOne(
		ctx,
		rng.Descriptor(),
		ro.voterTargets,
		ro.nonVoterTargets,
		ro.transferLeaseToFirstVoter,
		&options,
	)
	if err != nil {
		return err
	}

	if leaseTarget != nil {
		leaseholderStore, ok := s.LeaseholderStore(rng.RangeID())
		if !ok {
			return errors.Newf(" Lease transfer failed to %s. cannot find leaseholder", leaseTarget.StoreID.String())
		}

		if leaseholderStore.StoreID() != state.StoreID(leaseTarget.StoreID) {
			if ok := s.TransferLease(rng.RangeID(), state.StoreID(leaseTarget.StoreID)); !ok {
				leaseholder, err := options.Leaseholder(ctx, ro.key.ToRKey())
				if err != nil {
					return err
				}
				return errors.Newf("Lease transfer failed to %s. Existing leaseholder %s", leaseTarget.StoreID.String(), leaseholder)
			}
		}
	}

	if len(ops) == 0 {
		ro.complete = tick
		ro.done = true
		return nil
	}

	change := state.ReplicaChange{
		RangeID: rng.RangeID(),
		Author:  c.storeID,
	}

	// The replica changer currently only supports at most two operations
	// atomically; an add and a remove of a voter. When there are more than
	// this number of changes, error.
	// TOOD(kvoli): Support arbitrary number of operations for changes.
	if len(ops) > 2 {
		return errors.Newf("Expected 2 ops, found %d", len(ops))
	}

	var add, remove int
	for _, op := range ops {
		switch op.ChangeType {
		case roachpb.ADD_VOTER:
			add++
			change.Add = state.StoreID(op.Target.StoreID)
			change.Wait = c.settings.ReplicaChangeDelayFn()(rng.Size(), true /* use range size */)
		case roachpb.REMOVE_VOTER:
			remove++
			change.Remove = state.StoreID(op.Target.StoreID)
		default:
			return errors.Newf("Unrecognized operation type %s", op)
		}
	}

	if add > 1 || remove > 1 {
		return errors.Newf("Expected at most 1 add or remove each %d adds, %d removes", add, remove)
	}

	completeAt, ok := c.changer.Push(tick, &change)
	if !ok {
		return errors.Newf("tick %d: Changer did not accept op %+v", change)
	}
	ro.next = completeAt
	return nil
}

func (c *controller) processTransferLease(
	ctx context.Context, tick time.Time, s state.State, ro *TransferLeaseOp,
) error {
	if store, ok := s.LeaseholderStore(ro.rangeID); ok && store.StoreID() == ro.target {
		ro.done = true
		ro.complete = tick
		return nil
	}

	if !s.ValidTransfer(ro.rangeID, ro.target) {
		return errors.Errorf(
			"unable to transfer lease for r%d to store %d, invalid transfer.",
			ro.rangeID, ro.target)
	}

	delay := c.settings.ReplicaChangeBaseDelay
	if _, ok := c.changer.Push(tick, &state.LeaseTransferChange{
		RangeID:        ro.rangeID,
		TransferTarget: ro.target,
		Wait:           c.settings.ReplicaChangeBaseDelay,
		Author:         c.storeID,
	}); !ok {
		return errors.Errorf(
			"unable to transfer lease for r%d to store %d, application failed.",
			ro.rangeID, ro.target)
	}

	ro.next = tick.Add(delay)
	return nil
}
