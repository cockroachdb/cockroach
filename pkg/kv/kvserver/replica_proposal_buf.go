// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

// arrayLen returns the number of elements in the proposal buffer's array.
func (r propBufCntRes) arrayLen() int {
	return int(r & (1<<32 - 1))
}

// arrayIndex returns the index into the proposal buffer that was reserved for
// the request. The returned index will be -1 if no index was reserved (e.g. by
// propBufCnt.read) and if the buffer is empty.
func (r propBufCntRes) arrayIndex() int {
	// NB: -1 because the array is 0-indexed.
	return r.arrayLen() - 1
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
// ensure that lease indexes are not assigned in a different order from that in
// which commands are proposed (and thus likely applied). If this order was to
// get out of sync then some commands would necessarily be rejected beneath Raft
// during application (see checkForcedErr).
//
// Proposals enter the buffer via Insert() or ReinsertLocked(). They are moved
// into Raft via FlushLockedWithRaftGroup() when the buffer fills up, or during
// the next handleRaftReady iteration, whichever happens earlier. This
// introduces no additional latency into the replication pipeline compared to
// moving them into Raft directly because Raft would not begin replicating the
// proposals until the next handleRaftReady iteration anyway.
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
		leaseIndexFilter func(*ProposalData) (indexOverride uint64, err error)
		// submitProposalFilter can be used by tests to observe and optionally
		// drop Raft proposals before they are handed to etcd/raft to begin the
		// process of replication. Dropped proposals are still eligible to be
		// reproposed due to ticks.
		submitProposalFilter func(*ProposalData) (drop bool, err error)
		// allowLeaseProposalWhenNotLeader, if set, makes the proposal buffer allow
		// lease request proposals even when the replica inserting that proposal is
		// not the Raft leader. This can be used in tests to allow a replica to
		// acquire a lease without first moving the Raft leadership to it (e.g. it
		// allows tests to expire leases by stopping the old leaseholder's liveness
		// heartbeats and then expect other replicas to take the lease without
		// worrying about Raft).
		allowLeaseProposalWhenNotLeader bool
	}
}

type rangeLeaderInfo struct {
	// leaderKnown is set if the local Raft machinery knows who the leader is. If
	// not set, all other fields are empty.
	leaderKnown bool

	// leader represents the Raft group's leader. Not set if leaderKnown is not
	// set.
	leader roachpb.ReplicaID
	// iAmTheLeader is set if the local replica is the leader.
	iAmTheLeader bool
	// leaderEligibleForLease is set if the leader is known and its type of
	// replica allows it to acquire a lease.
	leaderEligibleForLease bool
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
	withGroupLocked(func(proposerRaft) error) error
	registerProposalLocked(*ProposalData)
	leaderStatusRLocked(raftGroup proposerRaft) rangeLeaderInfo
	// rejectProposalWithRedirectLocked rejects a proposal and redirects the
	// proposer to try it on another node. This is used to sometimes reject lease
	// acquisitions when another replica is the leader; the intended consequence
	// of the rejection is that the request that caused the lease acquisition
	// attempt is tried on the leader, at which point it should result in a lease
	// acquisition attempt by that node (or, perhaps by then the leader will have
	// already gotten a lease and the request can be serviced directly).
	rejectProposalWithRedirectLocked(
		ctx context.Context,
		prop *ProposalData,
		redirectTo roachpb.ReplicaID,
	)
}

// proposerRaft abstracts the propBuf's dependency on *raft.RawNode, to help
// testing.
type proposerRaft interface {
	Step(raftpb.Message) error
	BasicStatus() raft.BasicStatus
	ProposeConfChange(raftpb.ConfChangeI) error
}

// Init initializes the proposal buffer and binds it to the provided proposer.
func (b *propBuf) Init(p proposer) {
	b.p = p
	b.full.L = p.rlocker()
}

// Len returns the number of proposals currently in the buffer.
func (b *propBuf) Len() int {
	return b.cnt.read().arrayLen()
}

// LastAssignedLeaseIndexRLocked returns the last assigned lease index.
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
func (b *propBuf) Insert(ctx context.Context, p *ProposalData, data []byte) (uint64, error) {
	// Request a new max lease applied index for any request that isn't itself
	// a lease request. Lease requests don't need unique max lease index values
	// because their max lease indexes are ignored. See checkForcedErr.
	isLease := p.Request.IsLeaseRequest()
	req := makePropBufCntReq(!isLease)

	// Hold the read lock while inserting into the proposal buffer. Other
	// insertion attempts will also grab the read lock, so they can insert
	// concurrently. Consumers of the proposal buffer will grab the write lock,
	// so they must wait for concurrent insertion attempts to finish.
	b.p.rlocker().Lock()
	defer b.p.rlocker().Unlock()

	// Update the proposal buffer counter and determine which index we should
	// insert at.
	res, err := b.handleCounterRequestRLocked(ctx, req, false /* wLocked */)
	if err != nil {
		return 0, err
	}

	// Assign the command's maximum lease index.
	p.command.MaxLeaseIndex = b.liBase + res.leaseIndexOffset()
	if filter := b.testing.leaseIndexFilter; filter != nil {
		if override, err := filter(p); err != nil {
			return 0, err
		} else if override != 0 {
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
	if _, err := protoutil.MarshalTo(f, p.encodedCommand[preLen:]); err != nil {
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
func (b *propBuf) ReinsertLocked(ctx context.Context, p *ProposalData) error {
	// When re-inserting a command into the proposal buffer, the command never
	// wants a new lease index. Simply add it back to the buffer and let it be
	// reproposed.
	req := makePropBufCntReq(false /* incLeaseIndex */)

	// Update the proposal buffer counter and determine which index we should
	// insert at.
	res, err := b.handleCounterRequestRLocked(ctx, req, true /* wLocked */)
	if err != nil {
		return err
	}

	// Insert the proposal into the buffer's array.
	b.insertIntoArray(p, res.arrayIndex())
	return nil
}

// handleCounterRequestRLocked accepts a proposal buffer counter request and
// uses it to update the proposal buffer counter. The method will repeat the
// atomic update operation until it is able to successfully reserve an index
// in the array. If an attempt finds that the array is full then it may flush
// the array before trying again.
//
// The method expects that either the proposer's read lock or write lock is
// held. It does not mandate which, but expects the caller to specify using
// the wLocked argument.
func (b *propBuf) handleCounterRequestRLocked(
	ctx context.Context, req propBufCntReq, wLocked bool,
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
		} else if wLocked {
			// The buffer is full and we're holding the exclusive lock. Flush
			// the buffer before trying again.
			if err := b.flushLocked(ctx); err != nil {
				return 0, err
			}
		} else if idx == b.arr.len() {
			// The buffer is full and we were the first request to notice out of
			// potentially many requests holding the shared lock and trying to
			// insert concurrently. Eagerly attempt to flush the buffer before
			// trying again.
			if err := b.flushRLocked(ctx); err != nil {
				return 0, err
			}
		} else {
			// The buffer is full and we were not the first request to notice
			// out of potentially many requests holding the shared lock and
			// trying to insert concurrently. Wait for the buffer to be flushed
			// by someone else before trying again.
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

func (b *propBuf) flushRLocked(ctx context.Context) error {
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
	return b.flushLocked(ctx)
}

func (b *propBuf) flushLocked(ctx context.Context) error {
	return b.p.withGroupLocked(func(raftGroup proposerRaft) error {
		_, err := b.FlushLockedWithRaftGroup(ctx, raftGroup)
		return err
	})
}

// FlushLockedWithRaftGroup flushes the commands from the proposal buffer and
// resets the buffer back to an empty state. Each command is handed off to the
// Raft proposals map, at which point they are owned by the Raft processor.
//
// If raftGroup is non-nil (the common case) then the commands will also be
// proposed to the RawNode. This initiates Raft replication of the commands.
//
// Returns the number of proposals handed to the RawNode.
func (b *propBuf) FlushLockedWithRaftGroup(
	ctx context.Context, raftGroup proposerRaft,
) (int, error) {
	// Before returning, make sure to forward the lease index base to at least
	// the proposer's currently applied lease index. This ensures that if the
	// lease applied index advances outside of this proposer's control (i.e.
	// other leaseholders commit some stuff and then we get the lease back),
	// future proposals will be given sufficiently high max lease indexes.
	defer b.forwardLeaseIndexBase(b.p.leaseAppliedIndex())

	// We hold the write lock while reading from and flushing the proposal
	// buffer. This ensures that we synchronize with all producers and other
	// consumers.
	res := b.cnt.clear()
	used := res.arrayLen()
	// Before returning, consider resizing the proposal buffer's array,
	// depending on how much of it was used before the current flush.
	defer b.arr.adjustSize(used)
	if used == 0 {
		// The buffer is empty. Nothing to do.
		return 0, nil
	} else if used > b.arr.len() {
		// The buffer is full and at least one writer has tried to allocate
		// on top of the full buffer, so notify them to try again.
		used = b.arr.len()
		defer b.full.Broadcast()
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

	// Figure out leadership info. We'll use it to conditionally drop some
	// requests.
	var leaderInfo rangeLeaderInfo
	if raftGroup != nil {
		leaderInfo = b.p.leaderStatusRLocked(raftGroup)
		// Sanity check.
		if leaderInfo.leaderKnown && leaderInfo.leader == b.p.replicaID() && !leaderInfo.iAmTheLeader {
			log.Fatalf(ctx,
				"inconsistent Raft state: state %s while the current replica is also the lead: %d",
				raftGroup.BasicStatus().RaftState, leaderInfo.leader)
		}
	}

	// Remember the first error that we see when proposing the batch. We don't
	// immediately return this error because we want to finish clearing out the
	// buffer and registering each of the proposals with the proposer, but we
	// stop trying to propose commands to raftGroup.
	var firstErr error
	for i, p := range buf {
		if p == nil {
			// If we run into an error during proposal insertion, we may have reserved
			// an array index without actually inserting a proposal.
			continue
		}
		buf[i] = nil // clear buffer

		// Handle an edge case about lease acquisitions: we don't want to forward
		// lease acquisitions to another node (which is what happens when we're not
		// the leader) because:
		// a) if there is a different leader, that leader should acquire the lease
		// itself and thus avoid a change of leadership caused by the leaseholder
		// and leader being different (Raft leadership follows the lease), and
		// b) being a follower, it's possible that this replica is behind in
		// applying the log. Thus, there might be another lease in place that this
		// follower doesn't know about, in which case the lease we're proposing here
		// would be rejected. Not only would proposing such a lease be wasted work,
		// but we're trying to protect against pathological cases where it takes a
		// long time for this follower to catch up (for example because it's waiting
		// for a snapshot, and the snapshot is queued behind many other snapshots).
		// In such a case, we don't want all requests arriving at this node to be
		// blocked on this lease acquisition (which is very likely to eventually
		// fail anyway).
		//
		// Thus, we do one of two things:
		// - if the leader is known, we reject this proposal and make sure the
		// request that needed the lease is redirected to the leaseholder;
		// - if the leader is not known, we don't do anything special here to
		// terminate the proposal, but we know that Raft will reject it with a
		// ErrProposalDropped. We'll eventually re-propose it once a leader is
		// known, at which point it will either go through or be rejected based on
		// whether or not it is this replica that became the leader.
		//
		// A special case is when the leader is known, but is ineligible to get the
		// lease. In that case, we have no choice but to continue with the proposal.
		if !leaderInfo.iAmTheLeader && p.Request.IsLeaseRequest() {
			leaderKnownAndEligible := leaderInfo.leaderKnown && leaderInfo.leaderEligibleForLease
			if leaderKnownAndEligible && !b.testing.allowLeaseProposalWhenNotLeader {
				log.VEventf(ctx, 2, "not proposing lease acquisition because we're not the leader; replica %d is",
					leaderInfo.leader)
				b.p.rejectProposalWithRedirectLocked(ctx, p, leaderInfo.leader)
				continue
			}
			// If the leader is not known, or if it is known but it's ineligible for
			// the lease, continue with the proposal as explained above.
			if !leaderInfo.leaderKnown {
				log.VEventf(ctx, 2, "proposing lease acquisition even though we're not the leader; the leader is unknown")
			} else {
				log.VEventf(ctx, 2, "proposing lease acquisition even though we're not the leader; the leader is ineligible")
			}
		}

		// Raft processing bookkeeping.
		b.p.registerProposalLocked(p)

		// Potentially drop the proposal before passing it to etcd/raft, but
		// only after performing necessary bookkeeping.
		if filter := b.testing.submitProposalFilter; filter != nil {
			if drop, err := filter(p); drop || err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}

		// If we don't have a raft group or if the raft group has rejected one
		// of the proposals, we don't try to propose any more proposals. The
		// rest of the proposals will still be registered with the proposer, so
		// they will eventually be reproposed.
		if raftGroup == nil || firstErr != nil {
			continue
		}

		// Coordinate proposing the command to etcd/raft.
		if crt := p.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
			// Flush any previously batched (non-conf change) proposals to
			// preserve the correct ordering or proposals. Later proposals
			// will start a new batch.
			if err := proposeBatch(raftGroup, b.p.replicaID(), ents); err != nil {
				firstErr = err
				continue
			}
			ents = ents[len(ents):]

			confChangeCtx := ConfChangeContext{
				CommandID: string(p.idKey),
				Payload:   p.encodedCommand,
			}
			encodedCtx, err := protoutil.Marshal(&confChangeCtx)
			if err != nil {
				firstErr = err
				continue
			}

			cc, err := crt.ConfChange(encodedCtx)
			if err != nil {
				firstErr = err
				continue
			}

			if err := raftGroup.ProposeConfChange(
				cc,
			); err != nil && !errors.Is(err, raft.ErrProposalDropped) {
				// Silently ignore dropped proposals (they were always silently
				// ignored prior to the introduction of ErrProposalDropped).
				// TODO(bdarnell): Handle ErrProposalDropped better.
				// https://github.com/cockroachdb/cockroach/issues/21849
				firstErr = err
				continue
			}
		} else {
			// Add to the batch of entries that will soon be proposed. It is
			// possible that this batching can cause the batched MsgProp to grow
			// past the size limit where etcd/raft will drop the entire thing
			// (see raft.Config.MaxUncommittedEntriesSize), but that's not a
			// concern. This setting is configured to twice the maximum quota in
			// the proposal quota pool, so for batching to cause a message to be
			// dropped the uncommitted portion of the Raft log would already
			// need to be at least as large as the proposal quota size, assuming
			// that all in-flight proposals are reproposed in a single batch.
			ents = append(ents, raftpb.Entry{
				Data: p.encodedCommand,
			})
		}
	}
	if firstErr != nil {
		return 0, firstErr
	}
	return used, proposeBatch(raftGroup, b.p.replicaID(), ents)
}

func (b *propBuf) forwardLeaseIndexBase(v uint64) {
	if b.liBase < v {
		b.liBase = v
	}
}

func proposeBatch(raftGroup proposerRaft, replID roachpb.ReplicaID, ents []raftpb.Entry) error {
	if len(ents) == 0 {
		return nil
	}
	if err := raftGroup.Step(raftpb.Message{
		Type:    raftpb.MsgProp,
		From:    uint64(replID),
		Entries: ents,
	}); errors.Is(err, raft.ErrProposalDropped) {
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
//
// The intended usage of this method is to flush all proposals in the buffer
// into the proposals map so that they can all be manipulated in a single place.
// The representative example of this is a caller that wants to flush the buffer
// into the proposals map before canceling all proposals.
func (b *propBuf) FlushLockedWithoutProposing(ctx context.Context) {
	if _, err := b.FlushLockedWithRaftGroup(ctx, nil /* raftGroup */); err != nil {
		log.Fatalf(ctx, "unexpected error: %+v", err)
	}
}

const propBufArrayMinSize = 4
const propBufArrayMaxSize = 256
const propBufArrayShrinkDelay = 16

// propBufArray is a dynamically-sized array of ProposalData pointers. The
// array grows when it repeatedly fills up between flushes and shrinks when
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
		// The array is too large. Shrink it if possible.
		if cur <= propBufArrayMinSize {
			return
		}
		a.shrink++
		// Require propBufArrayShrinkDelay straight periods of underutilization
		// before shrinking. An array that is too big is better than an array
		// that is too small, and we don't want oscillation.
		if a.shrink == propBufArrayShrinkDelay {
			a.shrink = 0
			next := cur / 2
			if next <= propBufArrayMinSize {
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

// replicaProposer implements the proposer interface.
type replicaProposer Replica

func (rp *replicaProposer) locker() sync.Locker {
	return &rp.mu.RWMutex
}

func (rp *replicaProposer) rlocker() sync.Locker {
	return rp.mu.RWMutex.RLocker()
}

func (rp *replicaProposer) replicaID() roachpb.ReplicaID {
	return rp.mu.replicaID
}

func (rp *replicaProposer) destroyed() destroyStatus {
	return rp.mu.destroyStatus
}

func (rp *replicaProposer) leaseAppliedIndex() uint64 {
	return rp.mu.state.LeaseAppliedIndex
}

func (rp *replicaProposer) enqueueUpdateCheck() {
	rp.store.enqueueRaftUpdateCheck(rp.RangeID)
}

func (rp *replicaProposer) withGroupLocked(fn func(raftGroup proposerRaft) error) error {
	// Pass true for mayCampaignOnWake because we're about to propose a command.
	return (*Replica)(rp).withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		// We're proposing a command here so there is no need to wake the leader
		// if we were quiesced. However, we should make sure we are unquiesced.
		(*Replica)(rp).unquiesceLocked()
		return false /* unquiesceLocked */, fn(raftGroup)
	})
}

func (rp *replicaProposer) registerProposalLocked(p *ProposalData) {
	// Record when the proposal was submitted to Raft so that we can later
	// decide if/when to re-propose it.
	p.proposedAtTicks = rp.mu.ticks
	rp.mu.proposals[p.idKey] = p
}

func (rp *replicaProposer) leaderStatusRLocked(raftGroup proposerRaft) rangeLeaderInfo {
	r := (*Replica)(rp)

	status := raftGroup.BasicStatus()
	iAmTheLeader := status.RaftState == raft.StateLeader
	leader := status.Lead
	leaderKnown := leader != raft.None
	var leaderEligibleForLease bool
	rangeDesc := r.descRLocked()
	if leaderKnown {
		// Figure out if the leader is eligible for getting a lease.
		leaderRep, ok := rangeDesc.GetReplicaDescriptorByID(roachpb.ReplicaID(leader))
		if !ok {
			// There is a leader, but it's not part of our descriptor. The descriptor
			// must be stale, so we are behind in applying the log. We don't want the
			// lease ourselves (as we're behind), so let's assume that the leader is
			// eligible. If it proves that it isn't, we might be asked to get the
			// lease again, and by then hopefully we will have caught up.
			leaderEligibleForLease = true
		} else {
			err := batcheval.CheckCanReceiveLease(leaderRep, rangeDesc)
			leaderEligibleForLease = err == nil
		}
	}
	return rangeLeaderInfo{
		leaderKnown:            leaderKnown,
		leader:                 roachpb.ReplicaID(leader),
		iAmTheLeader:           iAmTheLeader,
		leaderEligibleForLease: leaderEligibleForLease,
	}
}

// rejectProposalWithRedirectLocked is part of the proposer interface.
func (rp *replicaProposer) rejectProposalWithRedirectLocked(
	ctx context.Context, prop *ProposalData, redirectTo roachpb.ReplicaID,
) {
	r := (*Replica)(rp)
	rangeDesc := r.descRLocked()
	storeID := r.store.StoreID()
	redirectRep, _ /* ok */ := rangeDesc.GetReplicaDescriptorByID(redirectTo)
	speculativeLease := roachpb.Lease{
		Replica: redirectRep,
	}
	log.VEventf(ctx, 2, "redirecting proposal to node %s; request: %s", redirectRep.NodeID, prop.Request)
	r.cleanupFailedProposalLocked(prop)
	prop.finishApplication(ctx, proposalResult{
		Err: roachpb.NewError(newNotLeaseHolderError(
			speculativeLease, storeID, rangeDesc, "refusing to acquire lease on follower")),
	})
}
