// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// propBufCnt is a counter maintained by proposal buffer that tracks an index
// into the buffer's array and an offset from the buffer's base lease index.
// The counter is accessed atomically.
//
// Bit layout (LSB to MSB):
//  bits 0  - 31: index into array
//  bits 32 - 63: lease index offset
type propBufCnt uint64

// propBufCntReq is a request to atomically update the proposal buffer's
// counter. The bit layout of the request is similar to that of propBufCnt,
// except that the two 32-bit segments represent deltas instead of absolute
// values.
//
// In practice, there are only two variants of requests. The first variant
// consists of requests that want to increment only the counter's array index
// by one. These are represented like:
//
//   0 0 0 ..[63 times].. 1
//
// The second variant consists of requests that want to increment the counter's
// array index by one and want to increment the counter's lease index offset by
// one. These are represented like:
//
//   0 0 0 ..[31 times].. 1 0 0 0 ..[31 times].. 1
//
// Representing requests like this allows them to be atomically added directly
// to the proposal buffer counter to reserve an array index and optionally
// reserve a lease index.
type propBufCntReq uint64

// propBufCntRes is a response from updating or reading the proposal buffer's
// counter. It can be understood as a snapshot of the counter.
type propBufCntRes uint64

// makePropBufCntReq creates a new proposal buffer request. The incLeaseIndex
// arg indicates whether the request would like a new maximum lease index or
// whether it would like the same maximum lease index as the previous request.
func makePropBufCntReq(incLeaseIndex bool) propBufCntReq {
	r := propBufCntReq(1)
	if incLeaseIndex {
		r |= (1 << 32)
	}
	return r
}

// arrayIndex returns the index into the proposal buffer that was reserved for
// the request. The returned index will be -1 if no index was reserved (e.g. by
// propBufCnt.read) and if the buffer is empty.
func (r propBufCntRes) arrayIndex() int {
	// NB: -1 because the array is 0-indexed.
	return int(r&(1<<32-1)) - 1
}

// leaseIndexOffset returns the offset from the proposal buffer's current lease
// index base that was reserved for the request's maximum lease index.
func (r propBufCntRes) leaseIndexOffset() uint64 {
	return uint64(r >> 32)
}

// update accepts a proposal buffer request and applies it to the proposal
// buffer counter, returning the response.
func (c *propBufCnt) update(r propBufCntReq) propBufCntRes {
	return propBufCntRes(atomic.AddUint64((*uint64)(c), uint64(r)))
}

// clear resets a proposal buffer counter to its zero value and returns the
// response returned to the last accepted request.
func (c *propBufCnt) clear() propBufCntRes {
	return propBufCntRes(atomic.SwapUint64((*uint64)(c), 0))
}

// read reads from the proposal buffer counter.
func (c *propBufCnt) read() propBufCntRes {
	return propBufCntRes(atomic.LoadUint64((*uint64)(c)))
}

// propBuf is a multi-producer, single-consumer buffer for Raft proposals. The
// buffer supports concurrent insertion of proposals.
//
// The proposal buffer also handles the assignment of maximum lease indexes for
// commands. Picking the maximum lease index for commands is done atomically
// with determining the order in which they are inserted into the buffer to
// ensure that lease indexes are not assigned in a different order from that
// which commands are proposed (and thus likely applied). If this order was to
// get out of sync then some commands would necessarily be rejected beneath Raft
// during application (see checkForcedErrLocked).
//
// propBuf inherits the locking of the proposer that it is bound to during
// initialization. Methods called "...Locked" and "...RLocked" expect the
// corresponding locker() and rlocker() to be held.
type propBuf struct {
	p    proposer
	full sync.Cond

	liBase uint64
	cnt    propBufCnt
	arr    propBufArray

	testing struct {
		// leaseIndexFilter can be used by tests to override the max lease index
		// assigned to a proposal by returning a non-zero lease index.
		leaseIndexFilter func(*ProposalData) (indexOverride uint64)
		// submitProposalFilter can be used by tests to observe and optionally
		// drop Raft proposals before they are handed to etcd/raft to begin the
		// process of replication. Dropped proposals are still eligible to be
		// reproposed due to ticks.
		submitProposalFilter func(*ProposalData) (drop bool)
	}
}

// A proposer is an object that uses a propBuf to coordinate Raft proposals.
type proposer interface {
	locker() sync.Locker
	rlocker() sync.Locker
	// The following require the proposer to hold (at least) a shared lock.
	replicaID() roachpb.ReplicaID
	destroyed() destroyStatus
	leaseAppliedIndex() uint64
	enqueueUpdateCheck()
	// The following require the proposer to hold an exclusive lock.
	withGroupLocked(func(*raft.RawNode) error) error
	registerProposalLocked(*ProposalData)
}

// Init initializes the proposal buffer and binds it to the provided proposer.
func (b *propBuf) Init(p proposer) {
	b.p = p
	b.full.L = p.rlocker()
}

// Len returns the number of proposals currently in the buffer.
func (b *propBuf) Len() int {
	return b.cnt.read().arrayIndex() + 1
}

// LastAssignedLeaseIndex returns the last assigned lease index.
func (b *propBuf) LastAssignedLeaseIndexRLocked() uint64 {
	return b.liBase + b.cnt.read().leaseIndexOffset()
}

// Insert inserts a new command into the proposal buffer to be proposed to the
// proposer's Raft group. The method accepts the Raft command as part of the
// ProposalData struct, along with a partial encoding of the command in the
// provided byte slice. It is expected that the byte slice contains marshaled
// information for all of the command's fields except for its max lease index,
// which is assigned by the method when the command is sequenced in the buffer.
// It is also expected that the byte slice has sufficient capacity to marshal
// the maximum lease index field into it. After adding the proposal to the
// buffer, the assigned max lease index is returned.
func (b *propBuf) Insert(p *ProposalData, data []byte) (uint64, error) {
	// Hold the read lock while inserting into the proposal buffer. Other
	// insertion attempts will also grab the read lock, so they can insert
	// concurrently. Consumers of the proposal buffer will grab the write lock,
	// so they must wait for concurrent insertion attempts to finish.
	b.p.rlocker().Lock()
	defer b.p.rlocker().Unlock()

	// Request a new max lease applied index for any request that isn't itself
	// a lease request. Lease requests don't need unique max lease index values
	// because their max lease indexes are ignored. See checkForcedErrLocked.
	isLease := p.Request.IsLeaseRequest()
	req := makePropBufCntReq(!isLease)

	// Update the proposal buffer counter and determine which index we should
	// insert at. Since we may be inserting concurrently with others, we pass
	// false for alwaysFlush.
	res, err := b.handleCounterRequestRLocked(req, b.flushRLocked, false /* alwaysFlush */)
	if err != nil {
		return 0, err
	}

	// Assign the command's maximum lease index.
	p.command.MaxLeaseIndex = b.liBase + res.leaseIndexOffset()
	if filter := b.testing.leaseIndexFilter; filter != nil {
		if override := filter(p); override != 0 {
			p.command.MaxLeaseIndex = override
		}
	}
	if log.V(4) {
		log.Infof(p.ctx, "submitting proposal %x: maxLeaseIndex=%d", p.idKey, p.command.MaxLeaseIndex)
	}

	// Marshal the command's footer with the newly assigned maximum lease index
	// into the command's pre-allocated buffer. It should already have enough
	// room to accommodate the command footer without needing an allocation.
	f := &p.tmpFooter
	f.MaxLeaseIndex = p.command.MaxLeaseIndex
	footerLen := f.Size()

	preLen := len(data)
	p.encodedCommand = data[:preLen+footerLen]
	if _, err := protoutil.MarshalToWithoutFuzzing(f, p.encodedCommand[preLen:]); err != nil {
		return 0, err
	}

	// Insert the proposal into the buffer's array.
	b.insertIntoArray(p, res.arrayIndex())

	// Return the maximum lease index that the proposal's command was given.
	if isLease {
		// For lease requests, we return zero because no real MaxLeaseIndex is
		// assigned. We could also return command.MaxLeaseIndex but this invites
		// confusion.
		return 0, nil
	}
	return p.command.MaxLeaseIndex, nil
}

// ReinsertLocked inserts a command that has already passed through the proposal
// buffer back into the buffer to be reproposed at a new Raft log index. Unlike
// insert, it does not modify the command or assign a new maximum lease index.
func (b *propBuf) ReinsertLocked(p *ProposalData) error {
	// When re-inserting a command into the proposal buffer, the command never
	// wants a new lease index. Simply add it back to the buffer and let it be
	// reproposed.
	req := makePropBufCntReq(false /* incLeaseIndex */)

	// Update the proposal buffer counter and determine which index we should
	// insert at. Since we hold an exclusive lock, we pass true for alwaysFlush.
	res, err := b.handleCounterRequestRLocked(req, b.flushLocked, true /* alwaysFlush */)
	if err != nil {
		return err
	}

	// Insert the proposal into the buffer's array.
	b.insertIntoArray(p, res.arrayIndex())
	return nil
}

// handleCounterRequestRLocked accepts a proposal buffer counter request and
// uses it to update the proposal buffer counter. The method will repeat the
// atomic update operation until it is able to successfully reserve an index in
// the array.
//
// If an attempt finds that the array is full then it may use the provided flush
// function to flush the array before trying again. If alwaysFlush is true then
// the function will always be called when the buffer is full. If not then the
// function will only be called by the first request to not fit in the array.
// This allows callers that expect concurrent access to avoid multiple writers
// all racing to flush at the same time.
//
// The method expects that either the proposer's read lock or write lock is held.
// It does not mandate which.
func (b *propBuf) handleCounterRequestRLocked(
	req propBufCntReq, flush func() error, alwaysFlush bool,
) (propBufCntRes, error) {
	// Repeatedly attempt to find an open index in the buffer's array.
	for {
		// NB: We need to check whether the proposer is destroyed before each
		// iteration in case the proposer has been destroyed between the initial
		// check and the current acquisition of the read lock. Failure to do so
		// will leave pending proposals that never get cleared.
		if status := b.p.destroyed(); !status.IsAlive() {
			return 0, status.err
		}

		res := b.cnt.update(req)
		idx := res.arrayIndex()
		if idx < b.arr.len() {
			// The buffer is not full. Our slot in the array is reserved.
			return res, nil
		} else if idx == b.arr.len() || alwaysFlush {
			// The buffer is full and either we were the first request to notice
			// or we're ok with always flushing when full. Eagerly attempt to
			// flush the buffer before trying again.
			if err := flush(); err != nil {
				return 0, err
			}
		} else {
			// The buffer is full and we were not the first request to notice.
			// Wait for the buffer to be flushed by someone else before trying
			// again.
			b.full.Wait()
		}
	}
}

// insertIntoArray inserts the proposal into the proposal buffer's array at the
// specified index. It also schedules a Raft update check if necessary.
func (b *propBuf) insertIntoArray(p *ProposalData, idx int) {
	b.arr.asSlice()[idx] = p
	if idx == 0 {
		// If this is the first proposal in the buffer, schedule a Raft update
		// check to inform Raft processing about the new proposal. Everyone else
		// can rely on the request that added the first proposal to the buffer
		// having already scheduled a Raft update check.
		b.p.enqueueUpdateCheck()
	}
}

func (b *propBuf) flushRLocked() error {
	// Upgrade the shared lock to an exclusive lock. After doing so, check again
	// whether the proposer has been destroyed. If so, wake up other goroutines
	// waiting for the flush.
	b.p.rlocker().Unlock()
	defer b.p.rlocker().Lock()
	b.p.locker().Lock()
	defer b.p.locker().Unlock()
	if status := b.p.destroyed(); !status.IsAlive() {
		b.full.Broadcast()
		return status.err
	}
	return b.flushLocked()
}

func (b *propBuf) flushLocked() error {
	return b.p.withGroupLocked(func(raftGroup *raft.RawNode) error {
		return b.FlushLockedWithRaftGroup(raftGroup)
	})
}

// FlushLockedWithRaftGroup flushes the commands from the proposal buffer and
// resets the buffer back to an empty state. Each command is handed off to the
// Raft proposals map, at which point they are owned by the Raft processor.
//
// If raftGroup if non-nil (the common case) then the commands will also be
// proposed to the RawNode. This initiates Raft replication of the commands.
func (b *propBuf) FlushLockedWithRaftGroup(raftGroup *raft.RawNode) error {
	// Before returning, make sure to forward the lease index base to at
	// least the proposer's currently applied lease index.
	defer b.forwardLeaseIndexBase(b.p.leaseAppliedIndex())

	// We hold the write lock while reading from and flushing the proposal
	// buffer. This ensures that we synchronize with all producers and other
	// consumers.
	res := b.cnt.clear()
	used := res.arrayIndex() + 1
	// Before returning, consider resizing the proposal buffer's array,
	// depending on how much of it was used before the current flush.
	defer b.arr.adjustSize(used)
	if used == 0 {
		// The buffer is empty. Nothing to do.
		return nil
	} else if used > b.arr.len() {
		// The buffer is full. Inform any writers who may be waiting
		// for it to be flushed.
		used = b.arr.len()
		b.full.Broadcast()
	}

	// Update the maximum lease index base value, based on the maximum lease
	// index assigned since the last flush.
	b.forwardLeaseIndexBase(b.liBase + res.leaseIndexOffset())

	// Iterate through the proposals in the buffer and propose them to Raft.
	// While doing so, build up batches of entries and submit them to Raft all
	// at once. Building up batches of entries and proposing them with a single
	// Step can dramatically reduce the number of messages required to commit
	// and apply them.
	buf := b.arr.asSlice()[:used]
	ents := make([]raftpb.Entry, 0, used)
	for i, p := range buf {
		buf[i] = nil // clear buffer

		// Raft processing bookkeeping.
		b.p.registerProposalLocked(p)

		// Potentially drop the proposal before passing it to etcd/raft, but
		// only after performing necessary bookkeeping.
		if filter := b.testing.submitProposalFilter; filter != nil {
			if drop := filter(p); drop {
				continue
			}
		}

		// If we don't have a raft group, we can't propose the command.
		if raftGroup == nil {
			continue
		}

		// Coordinate proposing the command to etcd/raft.
		if crt := p.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
			// Flush any previously batched (non-conf change) proposals to
			// preserve the correct ordering or proposals. Later proposals
			// will start a new batch.
			if err := proposeBatch(raftGroup, b.p.replicaID(), ents); err != nil {
				return err
			}
			ents = ents[len(ents):]

			confChangeCtx := ConfChangeContext{
				CommandID: string(p.idKey),
				Payload:   p.encodedCommand,
				Replica:   crt.Replica,
			}
			encodedCtx, err := protoutil.Marshal(&confChangeCtx)
			if err != nil {
				return err
			}

			if err := raftGroup.ProposeConfChange(raftpb.ConfChange{
				Type:    changeTypeInternalToRaft[crt.ChangeType],
				NodeID:  uint64(crt.Replica.ReplicaID),
				Context: encodedCtx,
			}); err != nil && err != raft.ErrProposalDropped {
				// Silently ignore dropped proposals (they were always silently
				// ignored prior to the introduction of ErrProposalDropped).
				// TODO(bdarnell): Handle ErrProposalDropped better.
				// https://github.com/cockroachdb/cockroach/issues/21849
				return err
			}
		} else {
			// Add to the batch of entries that will soon be proposed.
			ents = append(ents, raftpb.Entry{
				Data: p.encodedCommand,
			})
		}
	}
	return proposeBatch(raftGroup, b.p.replicaID(), ents)
}

func (b *propBuf) forwardLeaseIndexBase(v uint64) {
	if b.liBase < v {
		b.liBase = v
	}
}

func proposeBatch(raftGroup *raft.RawNode, replID roachpb.ReplicaID, ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	if err := raftGroup.Step(raftpb.Message{
		Type:    raftpb.MsgProp,
		From:    uint64(replID),
		Entries: ents,
	}); err == raft.ErrProposalDropped {
		// Silently ignore dropped proposals (they were always silently
		// ignored prior to the introduction of ErrProposalDropped).
		// TODO(bdarnell): Handle ErrProposalDropped better.
		// https://github.com/cockroachdb/cockroach/issues/21849
		return nil
	} else if err != nil {
		return err
	}
	return nil
}

// FlushLockedWithoutProposing is like FlushLockedWithRaftGroup but it does not
// attempt to propose any of the commands that it is flushing. Instead, it is
// used exclusively to flush all entries in the buffer into the proposals map.
func (b *propBuf) FlushLockedWithoutProposing() {
	if err := b.FlushLockedWithRaftGroup(nil /* raftGroup */); err != nil {
		log.Fatalf(context.Background(), "unexpected error: %v", err)
	}
}

const propBufArrayMinSize = 4
const propBufArrayMaxSize = 256
const propBufArrayShrinkDelay = 16

// propBufArray is a dynamically-sized array of ProposalData pointers. The
// array grows when it repeatedely fills up between flushes and shrinks when
// it repeatedly stays below a certainly level of utilization. Sufficiently
// small arrays avoid indirection and are stored inline.
type propBufArray struct {
	small  [propBufArrayMinSize]*ProposalData
	large  []*ProposalData
	shrink int
}

func (a *propBufArray) asSlice() []*ProposalData {
	if a.large != nil {
		return a.large
	}
	return a.small[:]
}

func (a *propBufArray) len() int {
	return len(a.asSlice())
}

// adjustSize adjusts the proposal buffer array's size based on how much of the
// array was used before the last flush and whether the size was observed to be
// too small, too large, or just right. The size grows quickly and shrinks
// slowly to prevent thrashing and oscillation.
func (a *propBufArray) adjustSize(used int) {
	cur := a.len()
	switch {
	case used <= cur/4:
		// The array is too small. Shrink it if possible.
		if cur == propBufArrayMinSize {
			return
		}
		a.shrink++
		// Require propBufArrayShrinkDelay straight periods of underutilization
		// before shrinking. An array that is too big is better than an array
		// that is too small, and we don't want oscillation.
		if a.shrink == propBufArrayShrinkDelay {
			a.shrink = 0
			next := cur / 2
			if next == propBufArrayMinSize {
				a.large = nil
			} else {
				a.large = make([]*ProposalData, next)
			}
		}
	case used >= cur:
		// The array is too small. Grow it if possible.
		a.shrink = 0
		next := 2 * cur
		if next <= propBufArrayMaxSize {
			a.large = make([]*ProposalData, next)
		}
	default:
		// The array is a good size. Do nothing.
		a.shrink = 0
	}
}

// replicaProposer is an implementation of proposer that wraps a Replica.
type replicaProposer struct {
	r *Replica
}

func (rp *replicaProposer) locker() sync.Locker {
	return &rp.r.mu.RWMutex
}

func (rp *replicaProposer) rlocker() sync.Locker {
	return rp.r.mu.RWMutex.RLocker()
}

func (rp *replicaProposer) replicaID() roachpb.ReplicaID {
	return rp.r.mu.replicaID
}

func (rp *replicaProposer) destroyed() destroyStatus {
	return rp.r.mu.destroyStatus
}

func (rp *replicaProposer) leaseAppliedIndex() uint64 {
	return rp.r.mu.state.LeaseAppliedIndex
}

func (rp *replicaProposer) enqueueUpdateCheck() {
	rp.r.store.enqueueRaftUpdateCheck(rp.r.RangeID)
}

func (rp *replicaProposer) withGroupLocked(fn func(*raft.RawNode) error) error {
	return rp.r.withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		// We're proposing a command here so there is no need to wake the leader
		// if we were quiesced. However, we should make sure we are unquiesced.
		rp.r.unquiesceLocked()
		return false /* unquiesceLocked */, fn(raftGroup)
	})
}

func (rp *replicaProposer) registerProposalLocked(p *ProposalData) {
	// Record when the proposal was submitted to Raft so that we can later
	// decide if/when to re-propose it.
	p.proposedAtTicks = rp.r.mu.ticks
	rp.r.mu.proposals[p.idKey] = p
	if rp.r.mu.commandSizes != nil {
		rp.r.mu.commandSizes[p.idKey] = p.quotaSize
	}
}
