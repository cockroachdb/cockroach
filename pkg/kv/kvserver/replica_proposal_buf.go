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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/tracker"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// propBuf is a multi-producer, single-consumer buffer for Raft proposals on a
// range. The buffer supports concurrent insertion of proposals.
//
// The proposal buffer also handles the assignment of maximum lease indexes for
// commands. Picking the maximum lease index for commands is done atomically
// with determining the order in which they are inserted into the buffer to
// ensure that lease indexes are not assigned in a different order from that in
// which commands are proposed (and thus likely applied). If this order was to
// get out of sync then some commands would necessarily be rejected beneath Raft
// during application (see checkForcedErr).
//
// The proposal buffer also is in charge of advancing the respective range's
// closed timestamp by assigning closed timestamp to proposals. For this
// purpose, new requests starting evaluation needs to synchronize with the
// proposal buffer (see TrackEvaluatingRequest).
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
	p        proposer
	clock    *hlc.Clock
	settings *cluster.Settings
	// evalTracker tracks currently-evaluating requests, making sure that
	// proposals coming out of the propBuf don't carry closed timestamps below
	// currently-evaluating requests.
	evalTracker tracker.Tracker
	full        sync.Cond

	// arr contains the buffered proposals.
	arr propBufArray
	// allocatedIdx is the next index into propBufArray to allocate. Accessed
	// atomically.
	allocatedIdx int64

	// assignedLAI represents the highest LAI that was assigned to a proposal.
	// This is set at the same time as assignedClosedTimestamp.
	assignedLAI uint64
	// assignedClosedTimestamp is the largest "closed timestamp" - i.e. the
	// largest timestamp that was communicated to other replicas as closed,
	// representing a promise that this leaseholder will not evaluate writes with
	// timestamp <= assignedClosedTimestamp anymore. It is set when proposals are
	// flushed from the buffer, and also by the side-transport which closes
	// timestamps out of band.
	//
	// Note that this field is not used by the local replica (or by anybody)
	// directly to decide whether follower reads can be served. See
	// ReplicaState.closed_timestamp.
	//
	// This field can be read under the proposer's read lock, and written to under
	// the write lock.
	assignedClosedTimestamp hlc.Timestamp

	// Buffer used to avoid allocations.
	scratchFooter kvserverpb.RaftCommandFooter

	testing struct {
		// leaseIndexFilter can be used by tests to override the max lease index
		// assigned to a proposal by returning a non-zero lease index.
		leaseIndexFilter func(*ProposalData) (indexOverride uint64)
		// insertFilter allows tests to inject errors at Insert() time.
		insertFilter func(*ProposalData) error
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
		// dontCloseTimestamps inhibits the closing of timestamps.
		dontCloseTimestamps bool
	}
}

type rangeLeaderInfo struct {
	// iAmTheLeader is set if the local replica is the leader.
	iAmTheLeader bool
	// leaderKnown is set if the local Raft machinery knows who the leader is. If
	// not set, all other fields are empty.
	leaderKnown bool
	// leader represents the Raft group's leader. Not set if leaderKnown is not
	// set.
	leader roachpb.ReplicaID
	// leaderEligibleForLease is set if the leader is known and its type of
	// replica allows it to acquire a lease.
	leaderEligibleForLease bool
}

// A proposer is an object that uses a propBuf to coordinate Raft proposals.
type proposer interface {
	locker() sync.Locker
	rlocker() sync.Locker
	// The following require the proposer to hold (at least) a shared lock.
	getReplicaID() roachpb.ReplicaID
	destroyed() destroyStatus
	firstIndex() uint64
	leaseAppliedIndex() uint64
	enqueueUpdateCheck()
	closedTimestampTarget() hlc.Timestamp
	leaderStatus(ctx context.Context, raftGroup proposerRaft) rangeLeaderInfo
	ownsValidLease(ctx context.Context, now hlc.ClockTimestamp) bool
	shouldCampaignOnRedirect(raftGroup proposerRaft) bool

	// The following require the proposer to hold an exclusive lock.
	withGroupLocked(func(proposerRaft) error) error
	registerProposalLocked(*ProposalData)
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
	// rejectProposalWithLeaseTransferRejectedLocked rejects a proposal for a
	// lease transfer when the transfer is deemed to be unsafe. The intended
	// consequence of the rejection is that the lease transfer attempt will be
	// rejected. Higher levels that decide whether or not to attempt a lease
	// transfer have weaker versions of the same check, so we don't expect to see
	// repeated lease transfer rejections.
	rejectProposalWithLeaseTransferRejectedLocked(
		ctx context.Context,
		prop *ProposalData,
		lease *roachpb.Lease,
		reason raftutil.ReplicaNeedsSnapshotStatus,
	)

	// leaseDebugRLocked returns info on the current lease.
	leaseDebugRLocked() string
}

// proposerRaft abstracts the propBuf's dependency on *raft.RawNode, to help
// testing.
type proposerRaft interface {
	Step(raftpb.Message) error
	Status() raft.Status
	BasicStatus() raft.BasicStatus
	ProposeConfChange(raftpb.ConfChangeI) error
	Campaign() error
}

// Init initializes the proposal buffer and binds it to the provided proposer.
func (b *propBuf) Init(
	p proposer, tracker tracker.Tracker, clock *hlc.Clock, settings *cluster.Settings,
) {
	b.p = p
	b.full.L = p.rlocker()
	b.clock = clock
	b.evalTracker = tracker
	b.settings = settings
	b.assignedLAI = p.leaseAppliedIndex()
}

// AllocatedIdx returns the highest index that was allocated. This generally
// corresponds to the size of the buffer but, if the buffer is full, the
// allocated index can temporarily be in advance of the size.
func (b *propBuf) AllocatedIdx() int {
	return int(atomic.LoadInt64(&b.allocatedIdx))
}

// clearAllocatedIdx resets the allocated index, emptying the buffer. Returns
// the number of elements that were in the buffer.
func (b *propBuf) clearAllocatedIdx() int {
	return int(atomic.SwapInt64(&b.allocatedIdx, 0))
}

// incAllocatedIdx allocates a slot into the the buffer that a new proposal can
// be written to. Returns the index of the slot.
func (b *propBuf) incAllocatedIdx() int {
	return int(atomic.AddInt64(&b.allocatedIdx, 1)) - 1 // -1 since the index is 0-based
}

// Insert inserts a new command into the proposal buffer to be proposed to the
// proposer's Raft group. The method accepts the Raft command as part of the
// ProposalData struct. ProposalData.encodedCommand is expected to contain a
// partial encoding of the command. That byte slice is expected to contain
// marshaled information for all of the command's fields except for
// MaxLeaseIndex, and ClosedTimestamp. These fields will be assigned later, when
// the buffer is flushed (after the command is sequenced in the buffer). It is
// also expected that the byte slice has sufficient capacity to marshal these
// fields into it.
//
// Insert takes ownership of the supplied token; the caller should tok.Move() it
// into this method. It will be used to untrack the request once it comes out of the
// proposal buffer.
func (b *propBuf) Insert(ctx context.Context, p *ProposalData, tok TrackedRequestToken) error {
	defer tok.DoneIfNotMoved(ctx)

	// Hold the read lock while inserting into the proposal buffer. Other
	// insertion attempts will also grab the read lock, so they can insert
	// concurrently. Consumers of the proposal buffer will grab the write lock,
	// so they must wait for concurrent insertion attempts to finish.
	b.p.rlocker().Lock()
	defer b.p.rlocker().Unlock()

	if filter := b.testing.insertFilter; filter != nil {
		if err := filter(p); err != nil {
			return err
		}
	}

	// Update the proposal buffer counter and determine which index we should
	// insert at.
	idx, err := b.allocateIndex(ctx, false /* wLocked */)
	if err != nil {
		return err
	}

	if log.V(4) {
		log.Infof(p.ctx, "submitting proposal %x", p.idKey)
	}

	// Insert the proposal into the buffer's array. The buffer now takes ownership
	// of the token.
	p.tok = tok.Move(ctx)
	b.insertIntoArray(p, idx)
	return nil
}

// ReinsertLocked inserts a command that has already passed through the proposal
// buffer back into the buffer to be reproposed at a new Raft log index. Unlike
// Insert, it does not modify the command.
func (b *propBuf) ReinsertLocked(ctx context.Context, p *ProposalData) error {
	// Update the proposal buffer counter and determine which index we should
	// insert at.
	idx, err := b.allocateIndex(ctx, true /* wLocked */)
	if err != nil {
		return err
	}

	// Insert the proposal into the buffer's array.
	b.insertIntoArray(p, idx)
	return nil
}

// allocateIndex allocates a buffer index to be used for storing a proposal. The
// method will repeat the atomic update operation until it is able to
// successfully reserve an index in the array. If an attempt finds that the
// array is full then it may flush the array before trying again.
//
// The method expects that either the proposer's read lock or write lock is
// held. It does not mandate which, but expects the caller to specify using
// the wLocked argument.
func (b *propBuf) allocateIndex(ctx context.Context, wLocked bool) (int, error) {
	// Repeatedly attempt to find an open index in the buffer's array.
	for {
		// NB: We need to check whether the proposer is destroyed before each
		// iteration in case the proposer has been destroyed between the initial
		// check and the current acquisition of the read lock. Failure to do so
		// will leave pending proposals that never get cleared.
		if status := b.p.destroyed(); !status.IsAlive() {
			return 0, status.err
		}

		idx := b.incAllocatedIdx()
		if idx < b.arr.len() {
			// The buffer is not full. Our slot in the array is reserved.
			return idx, nil
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
// Returns the number of proposals flushed from the proposal buffer, counting
// proposals even if they were dropped and never handed to the RawNode. This
// second part is important, because it ensures that even if we drop a lease
// request by calling rejectProposalWithRedirectLocked, we still inform the
// caller of its presence. This ensures that callers like handleRaftReady
// consider unquiescing and waking the Raft leader, which may be necessary to
// notice the failure of the leader and allow a future lease request through.
func (b *propBuf) FlushLockedWithRaftGroup(
	ctx context.Context, raftGroup proposerRaft,
) (int, error) {
	// We hold the write lock while reading from and flushing the proposal
	// buffer. This ensures that we synchronize with all producers and other
	// consumers.
	used := b.clearAllocatedIdx()
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

	// Iterate through the proposals in the buffer and propose them to Raft.
	// While doing so, build up batches of entries and submit them to Raft all
	// at once. Building up batches of entries and proposing them with a single
	// Step can dramatically reduce the number of messages required to commit
	// and apply them.
	buf := b.arr.asSlice()[:used]
	ents := make([]raftpb.Entry, 0, used)

	// Compute the closed timestamp target, which will be used to assign a closed
	// timestamp to all proposals in this batch.
	closedTSTarget := b.p.closedTimestampTarget()

	// Remember the first error that we see when proposing the batch. We don't
	// immediately return this error because we want to finish clearing out the
	// buffer and registering each of the proposals with the proposer, but we
	// stop trying to propose commands to raftGroup.
	var firstErr error
	for i, p := range buf {
		if p == nil {
			log.Fatalf(ctx, "unexpected nil proposal in buffer")
			return 0, nil // unreachable, for linter
		}
		buf[i] = nil // clear buffer
		reproposal := !p.tok.stillTracked()

		// Conditionally reject the proposal based on the state of the raft group.
		if b.maybeRejectUnsafeProposalLocked(ctx, raftGroup, p) {
			p.tok.doneIfNotMovedLocked(ctx)
			continue
		}

		// Raft processing bookkeeping.
		b.p.registerProposalLocked(p)

		// Exit the tracker.
		if !reproposal && p.Request.AppliesTimestampCache() {
			// Sanity check that the request is tracked by the evaluation tracker at
			// this point. It's supposed to be tracked until the
			// doneIfNotMovedLocked() call below.
			wts := p.Request.WriteTimestamp()
			lb := b.evalTracker.LowerBound(ctx)
			if wts.Less(lb) {
				wts, lb := wts, lb // copies escape to heap
				log.Fatalf(ctx, "%v", errorutil.UnexpectedWithIssueErrorf(72428,
					"request writing below tracked lower bound: wts: %s < lb: %s; ba: %s; lease: %s.",
					wts, lb, p.Request, b.p.leaseDebugRLocked()))
			}
		}
		p.tok.doneIfNotMovedLocked(ctx)

		// If we don't have a raft group or if the raft group has rejected one
		// of the proposals, we don't try to propose any more proposals. The
		// rest of the proposals will still be registered with the proposer, so
		// they will eventually be reproposed.
		if raftGroup == nil || firstErr != nil {
			continue
		}

		// Figure out what closed timestamp this command will carry.
		//
		// If this is a reproposal, we don't reassign the LAI. We also don't
		// reassign the closed timestamp: we could, in principle, but we'd have to
		// make a copy of the encoded command as to not modify the copy that's
		// already stored in the local replica's raft entry cache.
		if !reproposal {
			lai, closedTimestamp, err := b.allocateLAIAndClosedTimestampLocked(ctx, p, closedTSTarget)
			if err != nil {
				firstErr = err
				continue
			}
			err = b.marshallLAIAndClosedTimestampToProposalLocked(ctx, p, lai, closedTimestamp)
			if err != nil {
				firstErr = err
				continue
			}
		}

		// Potentially drop the proposal before passing it to etcd/raft, but
		// only after performing necessary bookkeeping.
		if filter := b.testing.submitProposalFilter; filter != nil {
			if drop, err := filter(p); drop || err != nil {
				firstErr = err
				continue
			}
		}

		// Coordinate proposing the command to etcd/raft.
		if crt := p.command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
			// Flush any previously batched (non-conf change) proposals to
			// preserve the correct ordering or proposals. Later proposals
			// will start a new batch.
			if err := proposeBatch(raftGroup, b.p.getReplicaID(), ents); err != nil {
				firstErr = err
				continue
			}
			ents = ents[len(ents):]

			confChangeCtx := kvserverpb.ConfChangeContext{
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
	return used, proposeBatch(raftGroup, b.p.getReplicaID(), ents)
}

// maybeRejectUnsafeProposalLocked conditionally rejects proposals that are
// deemed unsafe, given the current state of the raft group. Requests that may
// be deemed unsafe and rejected at this level are those whose safety has some
// dependency on raft leadership, follower progress, leadership term, commit
// index, or other properties of raft. By rejecting these requests on the
// "flushing" side of the proposal buffer (i.e. while holding the raftMu), we
// can perform the safety checks without risk of the state of the raft group
// changing before the proposal is passed to etcd/raft.
//
// Currently, the request types which may be rejected by this function are:
//   - RequestLease when the proposer is not the raft leader (with caveats).
//   - TransferLease when the proposer cannot guarantee that the lease transfer
//     target does not currently need a Raft snapshot to catch up on its Raft log.
//     In such cases, the proposer cannot guarantee that the lease transfer target
//     will not need a Raft snapshot to catch up to and apply the lease transfer.
//     This requires that the proposer is the raft leader.
//
// The function returns true if the proposal was rejected, and false if not.
// If the proposal was rejected and true is returned, it will have been cleaned
// up (passed to Replica.cleanupFailedProposalLocked) and finished
// (ProposalData.finishApplication called).
func (b *propBuf) maybeRejectUnsafeProposalLocked(
	ctx context.Context, raftGroup proposerRaft, p *ProposalData,
) (rejected bool) {
	if raftGroup == nil {
		// If we do not have a raft group, we won't try to propose this proposal.
		// Instead, we will register the proposal so that it can be reproposed later
		// with a raft group. Wait until that point to determine whether to reject
		// the proposal or not.
		return false
	}
	switch {
	case p.Request.IsSingleRequestLeaseRequest():
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
		//
		// Lease extensions for a currently held lease always go through, to
		// keep the lease alive until the normal lease transfer mechanism can
		// colocate it with the leader.
		li := b.leaderStatusRLocked(ctx, raftGroup)
		if li.iAmTheLeader {
			return false
		}
		leaderKnownAndEligible := li.leaderKnown && li.leaderEligibleForLease
		ownsCurrentLease := b.p.ownsValidLease(ctx, b.clock.NowAsClockTimestamp())
		if leaderKnownAndEligible && !ownsCurrentLease && !b.testing.allowLeaseProposalWhenNotLeader {
			log.VEventf(ctx, 2, "not proposing lease acquisition because we're not the leader; replica %d is",
				li.leader)
			b.p.rejectProposalWithRedirectLocked(ctx, p, li.leader)
			if b.p.shouldCampaignOnRedirect(raftGroup) {
				log.VEventf(ctx, 2, "campaigning because Raft leader not live in node liveness map")
				if err := raftGroup.Campaign(); err != nil {
					log.VEventf(ctx, 1, "failed to campaign: %s", err)
				}
			}
			return true
		}
		// If the leader is not known, or if it is known but it's ineligible
		// for the lease, continue with the proposal as explained above. We
		// also send lease extensions for an existing leaseholder.
		if ownsCurrentLease {
			log.VEventf(ctx, 2, "proposing lease extension even though we're not the leader; we hold the current lease")
		} else if !li.leaderKnown {
			log.VEventf(ctx, 2, "proposing lease acquisition even though we're not the leader; the leader is unknown")
		} else {
			log.VEventf(ctx, 2, "proposing lease acquisition even though we're not the leader; the leader is ineligible")
		}
		return false

	case p.Request.IsSingleTransferLeaseRequest():
		// When performing a lease transfer, the outgoing leaseholder revokes its
		// lease before proposing the lease transfer request, meaning that it
		// promises to stop using the previous lease to serve reads or writes. The
		// lease transfer request is then proposed and committed to the Raft log, at
		// which point the new lease officially becomes active. However, this new
		// lease is not usable until the incoming leaseholder applies the Raft entry
		// that contains the lease transfer and notices that it is now the
		// leaseholder for the range.
		//
		// The effect of this handoff is that there exists a "power vacuum" time
		// period when the outgoing leaseholder has revoked its previous lease but
		// the incoming leaseholder has not yet applied its new lease. During this
		// time period, a range is effectively unavailable for strong reads and
		// writes, because no replica will act as the leaseholder. Instead, requests
		// that require the lease will be redirected back and forth between the
		// outgoing leaseholder and the incoming leaseholder (the client backs off).
		// To minimize the disruption caused by lease transfers, we need to minimize
		// this time period.
		//
		// We assume that if a lease transfer target is sufficiently caught up on
		// its log such that it will be able to apply the lease transfer through log
		// entry application then this unavailability window will be acceptable.
		// This may be a faulty assumption in cases with severe replication lag, but
		// we must balance any heuristics here that attempts to determine "too much
		// lag" with the possibility of starvation of lease transfers under
		// sustained write load and a resulting sustained replication lag. See
		// #38065 and #42379, which removed such a heuristic. For now, we don't try
		// to make such a determination.
		//
		// However, we draw a distinction between lease transfer targets that will
		// be able to apply the lease transfer through log entry application and
		// those that will require a Raft snapshot to catch up and apply the lease
		// transfer. Raft snapshots are more expensive than Raft entry replication.
		// They are also significantly more likely to be delayed due to queueing
		// behind other snapshot traffic in the system. This potential for delay
		// makes transferring a lease to a replica that needs a snapshot very risky,
		// as doing so has the effect of inducing range unavailability until the
		// snapshot completes, which could take seconds, minutes, or hours.
		//
		// In the future, we will likely get better at prioritizing snapshots to
		// improve the responsiveness of snapshots that are needed to recover
		// availability. However, even in this world, it is not worth inducing
		// unavailability that can only be recovered through a Raft snapshot. It is
		// better to catch the desired lease target up on the log first and then
		// initiate the lease transfer once its log is connected to the leader's.
		//
		// For this reason, unless we can guarantee that the lease transfer target
		// does not need a Raft snapshot, we don't let it through. This same check
		// lives at higher levels in the stack as well (i.e. in the allocator). The
		// higher level checks avoid wasted work and respond more gracefully to
		// invalid targets (e.g. they pick the next best target). However, this is
		// the only place where the protection is airtight against race conditions
		// because the check is performed:
		// 1. by the current Raft leader, else the proposal will fail
		// 2. while holding latches that prevent interleaving log truncation
		//
		// If an error is thrown here, the outgoing leaseholder still won't be able
		// to use its revoked lease. However, it will be able to immediately request
		// a new lease. This may be disruptive, which is why we try to avoid hitting
		// this airtight protection as much as possible by detecting the failure
		// scenario before revoking the outgoing lease.
		status := raftGroup.Status()
		firstIndex := b.p.firstIndex()
		newLease := p.command.ReplicatedEvalResult.State.Lease
		newLeaseTarget := newLease.Replica.ReplicaID
		snapStatus := raftutil.ReplicaMayNeedSnapshot(&status, firstIndex, newLeaseTarget)
		if snapStatus != raftutil.NoSnapshotNeeded && !p.Request.Requests[0].GetTransferLease().BypassSafetyChecks {
			b.p.rejectProposalWithLeaseTransferRejectedLocked(ctx, p, newLease, snapStatus)
			return true
		}
		return false

	default:
		return false
	}
}

// leaderStatusRLocked returns the rangeLeaderInfo for the provided raft group,
// or an empty rangeLeaderInfo if the raftGroup is nil.
func (b *propBuf) leaderStatusRLocked(ctx context.Context, raftGroup proposerRaft) rangeLeaderInfo {
	leaderInfo := b.p.leaderStatus(ctx, raftGroup)
	// Sanity check.
	if leaderInfo.leaderKnown && leaderInfo.leader == b.p.getReplicaID() &&
		!leaderInfo.iAmTheLeader {
		log.Fatalf(ctx,
			"inconsistent Raft state: state %s while the current replica is also the lead: %d",
			raftGroup.BasicStatus().RaftState, leaderInfo.leader)
	}
	return leaderInfo
}

// allocateLAIAndClosedTimestampLocked computes a LAI and closed timestamp to be
// carried by an outgoing proposal.
//
// closedTSTarget is the timestamp that should be closed for this range
// according to the range's closing policy. This function will look at the
// particularities of the range and of the proposal and decide to close a
// different timestamp.
//
// This shouldn't be called for reproposals; we don't want to update the closed
// timestamp and LAI they carry (we could, in principle, but we'd have to make a
// copy of the encoded command as to not modify the copy that's already stored
// in the local replica's raft entry cache).
func (b *propBuf) allocateLAIAndClosedTimestampLocked(
	ctx context.Context, p *ProposalData, closedTSTarget hlc.Timestamp,
) (uint64, hlc.Timestamp, error) {

	// Assign a LeaseAppliedIndex (see checkForcedErr). These provide replay
	// protection.
	//
	// Proposals coming from lease requests (not transfers) have their own replay
	// protection, via the lease sequence and the previous lease's proposal
	// timestamp; this is necessary as lease requests are proposed while not
	// holding the lease (and so the proposed does not know a valid LAI to use).
	// They will not check the lease applied index proposed from followers). While
	// it would be legal to still assign a LAI to lease requests, historically it
	// has been mildly inconvenient in testing, and might belie the fact that
	// LAI-related concepts just don't apply. Instead, we assign a zero LAI to
	// lease proposals, with a condition that matches that used in
	// checkForcedError to identify lease requests. Note that lease *transfers*
	// are only ever proposed by leaseholders, and they use the LAI to prevent
	// replays (though they could in principle also be handled like lease
	// requests).
	var lai uint64
	if !p.Request.IsSingleRequestLeaseRequest() {
		b.assignedLAI++
		lai = b.assignedLAI
	}

	if filter := b.testing.leaseIndexFilter; filter != nil {
		if override := filter(p); override != 0 {
			lai = override
		}
	}

	if b.testing.dontCloseTimestamps {
		return lai, hlc.Timestamp{}, nil
	}

	// Lease requests don't carry closed timestamps. The reason for this differ
	// between lease extensions and brand new leases:
	// - A lease extension cannot carry a closed timestamp assigned in the same
	// way as we do for regular proposal because they're proposed without a MLAI,
	// and so two lease extensions might commute and both apply, which would
	// result in a closed timestamp regression when the reordered extension
	// applies. The command application side doesn't bother protecting against
	// such regressions. Besides, the considerations for brand new leases below
	// also apply.
	// - For a brand new lease, one might think that the lease start time can be
	// considered a closed timestamp(*) since, if this replica gets the lease, it
	// will not evaluate writes at lower timestamps. Unfortunately, there's a
	// problem: while it's true that this replica, and this range in general, will
	// not permit writes at timestamps below this lease's start time, it might
	// happen that the range is in the process of merging with its left neighbor.
	// If this range has already been Subsumed as the RHS of a merge then, after
	// merge, the joint range will allow writes to the former RHS's key space at
	// timestamps above the RHS's freeze start (which is below the start time of
	// this lease). Thus, if this lease were to close its start timestamp while
	// subsumed, then it'd be possible for follower reads to be served before the
	// merge finalizes at timestamps that would become un-closed after the merge.
	// Since this scenario is about subsumed ranges, we could make a distinction
	// between brand new leases for subsumed ranges versus other brand new leases,
	// and let the former category close the lease start time. But, for
	// simplicity, we don't close timestamps on any lease requests.
	//
	// As opposed to lease requests, lease transfers behave like regular
	// proposals: they get a closed timestamp based on closedTSTarget. Note that
	// transfers carry a summary of the timestamp cache, so the new leaseholder
	// will be aware of all the reads performed by the previous leaseholder.
	//
	// (*) If we were to close the lease start time, we'd have to forward the
	// lease start time to b.assignedClosedTimestamp. We surprisingly might have
	// previously closed a timestamp above the lease start time - when we close
	// timestamps in the future, then attempt to transfer our lease away (and thus
	// proscribe it) but the transfer fails and we're now acquiring a new lease to
	// replace the proscribed one. Also, if we ever re-introduce closed
	// timestamps carried by lease requests, make sure to resurrect the old
	// TestRejectedLeaseDoesntDictateClosedTimestamp and protect against that
	// scenario.
	if p.Request.IsSingleRequestLeaseRequest() {
		return lai, hlc.Timestamp{}, nil
	}

	// Note that under a steady lease, for requests that leave intents we must
	// have WriteTimestamp.Less(b.assignedClosedTimestamp) and we used to assert
	// that here. However, this does not have to be true for proposals that
	// evaluated under an old lease and which are only entering the proposal
	// buffer after the lease has returned and in the process of doing so
	// incremented b.assignedClosedTimestamp. These proposals have no effect (as
	// they apply as a no-op) but the proposal tracker has no knowledge of the
	// lease changes and would therefore witness what looks like a violation of
	// the invariant above. We have an authoritative assertion in
	// (*replicaAppBatch).assertNoWriteBelowClosedTimestamp that is not
	// susceptible to the above false positive.
	//
	// See https://github.com/cockroachdb/cockroach/issues/72428#issuecomment-976428551.

	lb := b.evalTracker.LowerBound(ctx)
	if !lb.IsEmpty() {
		// If the tracker told us that requests are currently evaluating at
		// timestamps >= lb, then we can close up to lb.Prev(). We use FloorPrev()
		// to get rid of the logical ticks; we try to not publish closed ts with
		// logical ticks when there's no good reason for them.
		closedTSTarget.Backward(lb.FloorPrev())
	}
	// We can't close timestamps above the current lease's expiration.
	closedTSTarget.Backward(p.leaseStatus.ClosedTimestampUpperBound())

	// We're about to close closedTSTarget. The propBuf needs to remember that
	// in order for incoming requests to be bumped above it (through
	// TrackEvaluatingRequest).
	if !b.forwardClosedTimestampLocked(closedTSTarget) {
		closedTSTarget = b.assignedClosedTimestamp
	}

	return lai, closedTSTarget, nil
}

// marshallLAIAndClosedTimestampToProposalLocked modifies p.encodedCommand,
// adding the LAI and closed timestamp.
func (b *propBuf) marshallLAIAndClosedTimestampToProposalLocked(
	ctx context.Context, p *ProposalData, lai uint64, closedTimestamp hlc.Timestamp,
) error {
	buf := &b.scratchFooter
	buf.MaxLeaseIndex = lai
	// Also assign MaxLeaseIndex to the in-memory copy. The in-memory copy is
	// checked for sanity at application time, on the proposing replica.
	p.command.MaxLeaseIndex = lai

	// Fill in the closed ts in the proposal.
	buf.ClosedTimestamp = closedTimestamp
	// NOTE(andrei): We don't assigned to the in-memory command
	// (p.command.ClosedTimestamp) because that would cause an allocation (see
	// comments on the proto field about why it needs to be nullable). It'd be
	// nice to assign to it, for consistency, but nobody needs it.

	if log.ExpensiveLogEnabled(ctx, 4) {
		log.VEventf(ctx, 4, "attaching closed timestamp %s to proposal %x",
			closedTimestamp, p.idKey)
	}

	// Here we rely on p.encodedCommand to have been allocated with enough
	// capacity for this footer.
	preLen := len(p.encodedCommand)
	p.encodedCommand = p.encodedCommand[:preLen+buf.Size()]
	_, err := protoutil.MarshalTo(buf, p.encodedCommand[preLen:])
	return err
}

func (b *propBuf) forwardAssignedLAILocked(v uint64) {
	if b.assignedLAI < v {
		b.assignedLAI = v
	}
}

// forwardClosedTimestamp forwards the closed timestamp tracked by the propBuf.
func (b *propBuf) forwardClosedTimestampLocked(closedTS hlc.Timestamp) bool {
	return b.assignedClosedTimestamp.Forward(closedTS)
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

// OnLeaseChangeLocked is called when a new lease is applied to this range.
// appliedClosedTS is the range's closed timestamp after the new lease was
// applied; the closed timestamp tracked by the propBuf is updated accordingly.
// Similarly, appliedLAI is the highest LAI of an applied command; the propBuf
// will propose commands with higher LAIs.
func (b *propBuf) OnLeaseChangeLocked(
	leaseOwned bool, appliedClosedTS hlc.Timestamp, appliedLAI uint64,
) {
	if leaseOwned {
		b.forwardClosedTimestampLocked(appliedClosedTS)
		b.forwardAssignedLAILocked(appliedLAI)
	} else {
		// Zero out to avoid any confusion.
		b.assignedClosedTimestamp = hlc.Timestamp{}
		b.assignedLAI = 0
	}
}

// EvaluatingRequestsCount returns the count of requests currently tracked by
// the propBuf.
func (b *propBuf) EvaluatingRequestsCount() int {
	b.p.rlocker().Lock()
	defer b.p.rlocker().Unlock()
	return b.evalTracker.Count()
}

// TrackedRequestToken represents the result of propBuf.TrackEvaluatingRequest:
// a token to be later used for untracking the respective request.
//
// This token tries to make it easy to pass responsibility for untracking. The
// intended pattern is:
// tok := propbBuf.TrackEvaluatingRequest()
// defer tok.DoneIfNotMoved()
// fn(tok.Move())
//
// A zero value TrackedRequestToken acts as a no-op: calling DoneIfNotMoved() on
// it will not interact with the tracker at all, but will cause stillTracked()
// to switch from true->false.
type TrackedRequestToken struct {
	done bool
	tok  tracker.RemovalToken
	b    *propBuf
}

// DoneIfNotMoved untracks the request if Move had not been called on the token
// previously. If Move had been called, this is a no-op.
//
// Note that if this ends up actually destroying the token (i.e. if Move() had
// not been called previously) this takes r.mu, so it's pretty expensive. On
// happy paths, the token is expected to have been Move()d, and a batch of
// tokens are expected to be destroyed at once by the propBuf (which calls
// doneLocked).
func (t *TrackedRequestToken) DoneIfNotMoved(ctx context.Context) {
	if t.done {
		return
	}
	if t.b != nil {
		t.b.p.locker().Lock()
		defer t.b.p.locker().Unlock()
	}
	t.doneIfNotMovedLocked(ctx)
}

// doneIfNotMovedLocked untrackes the request. It is idempotent; in particular,
// this is used when wanting to untrack a proposal that might, in fact, be a
// reproposal.
func (t *TrackedRequestToken) doneIfNotMovedLocked(ctx context.Context) {
	if t.done {
		return
	}
	t.done = true
	if t.b != nil {
		t.b.evalTracker.Untrack(ctx, t.tok)
	}
}

// stillTracked returns true if no Done* method has been called.
func (t *TrackedRequestToken) stillTracked() bool {
	return !t.done
}

// Move returns a new token which can untrack the request. The original token is
// neutered; calling DoneIfNotMoved on it becomes a no-op.
func (t *TrackedRequestToken) Move(ctx context.Context) TrackedRequestToken {
	if t.done {
		log.Fatalf(ctx, "attempting to Move() after Done() call")
	}
	cpy := *t
	t.done = true
	return cpy
}

// TrackEvaluatingRequest atomically starts tracking an evaluating request and
// returns the minimum timestamp at which this request can write. The tracked
// request is identified by its tentative write timestamp. After calling this,
// the caller must bump the write timestamp to at least the returned minTS.
//
// The returned token must be used to eventually remove this request from the
// tracked set by calling tok.Done(); the removal will allow timestamps above
// its write timestamp to be closed. If the evaluation results in a proposal,
// the token will make it back to this propBuf through Insert; in this case it
// will be the propBuf itself that ultimately stops tracking the request once
// the proposal is flushed from the buffer.
func (b *propBuf) TrackEvaluatingRequest(
	ctx context.Context, wts hlc.Timestamp,
) (minTS hlc.Timestamp, _ TrackedRequestToken) {
	b.p.rlocker().Lock()
	defer b.p.rlocker().Unlock()

	minTS = b.assignedClosedTimestamp.Next()
	wts.Forward(minTS)
	tok := b.evalTracker.Track(ctx, wts)
	return minTS, TrackedRequestToken{tok: tok, b: b}
}

// MaybeForwardClosedLocked checks whether the closed timestamp can be advanced
// to target. If so, the assigned closed timestamp is forwarded to the target,
// ensuring that no future writes ever write below it.
//
// Returns false in the following cases:
//  1. target is below the propBuf's closed timestamp. This ensures that the
//     side-transport (the caller) is prevented from publishing closed timestamp
//     regressions. In other words, for a given LAI, the side-transport only
//     publishes closed timestamps higher than what Raft published.
//  2. There are requests evaluating at timestamps equal to or below target (as
//     tracked by the evalTracker). We can't close timestamps at or above these
//     requests' write timestamps.
func (b *propBuf) MaybeForwardClosedLocked(ctx context.Context, target hlc.Timestamp) bool {
	if lb := b.evalTracker.LowerBound(ctx); !lb.IsEmpty() && lb.LessEq(target) {
		return false
	}
	return b.forwardClosedTimestampLocked(target)
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

var _ proposer = &replicaProposer{}

func (rp *replicaProposer) locker() sync.Locker {
	return &rp.mu.RWMutex
}

func (rp *replicaProposer) rlocker() sync.Locker {
	return rp.mu.RWMutex.RLocker()
}

func (rp *replicaProposer) getReplicaID() roachpb.ReplicaID {
	return rp.replicaID
}

func (rp *replicaProposer) destroyed() destroyStatus {
	return rp.mu.destroyStatus
}

func (rp *replicaProposer) firstIndex() uint64 {
	return (*Replica)(rp).raftFirstIndexRLocked()
}

func (rp *replicaProposer) leaseAppliedIndex() uint64 {
	return rp.mu.state.LeaseAppliedIndex
}

func (rp *replicaProposer) enqueueUpdateCheck() {
	rp.store.enqueueRaftUpdateCheck(rp.RangeID)
}

func (rp *replicaProposer) closedTimestampTarget() hlc.Timestamp {
	return (*Replica)(rp).closedTimestampTargetRLocked()
}

func (rp *replicaProposer) withGroupLocked(fn func(raftGroup proposerRaft) error) error {
	// Pass true for mayCampaignOnWake because we're about to propose a command.
	return (*Replica)(rp).withRaftGroupLocked(true, func(raftGroup *raft.RawNode) (bool, error) {
		// We're proposing a command here so there is no need to wake the leader
		// if we were quiesced. However, we should make sure we are unquiesced.
		(*Replica)(rp).maybeUnquiesceLocked()
		return false /* maybeUnquiesceLocked */, fn(raftGroup)
	})
}

func (rp *replicaProposer) leaseDebugRLocked() string {
	return rp.mu.state.Lease.String()
}

func (rp *replicaProposer) registerProposalLocked(p *ProposalData) {
	// Record when the proposal was submitted to Raft so that we can later
	// decide if/when to re-propose it.
	p.proposedAtTicks = rp.mu.ticks
	if p.createdAtTicks == 0 {
		p.createdAtTicks = rp.mu.ticks
	}
	rp.mu.proposals[p.idKey] = p
}

func (rp *replicaProposer) leaderStatus(
	ctx context.Context, raftGroup proposerRaft,
) rangeLeaderInfo {
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
			// If the current leader is a VOTER_DEMOTING and it was the last one to
			// hold the lease (according to our possibly stale applied lease state),
			// CheckCanReceiveLease considers it eligible to continue holding the
			// lease, so we don't allow our proposal through. Otherwise, if it was not
			// the last one to hold the lease, it will never be allowed to acquire it
			// again, so we don't consider it eligible.
			lastLease, _ := r.getLeaseRLocked()
			wasLastLeaseholder := leaderRep.ReplicaID == lastLease.Replica.ReplicaID
			err := roachpb.CheckCanReceiveLease(
				leaderRep, rangeDesc.Replicas(), wasLastLeaseholder)
			leaderEligibleForLease = err == nil
		}
	}
	return rangeLeaderInfo{
		iAmTheLeader:           iAmTheLeader,
		leaderKnown:            leaderKnown,
		leader:                 roachpb.ReplicaID(leader),
		leaderEligibleForLease: leaderEligibleForLease,
	}
}

func (rp *replicaProposer) ownsValidLease(ctx context.Context, now hlc.ClockTimestamp) bool {
	return (*Replica)(rp).ownsValidLeaseRLocked(ctx, now)
}

func (rp *replicaProposer) shouldCampaignOnRedirect(raftGroup proposerRaft) bool {
	r := (*Replica)(rp)
	livenessMap, _ := r.store.livenessMap.Load().(livenesspb.IsLiveMap)
	return shouldCampaignOnLeaseRequestRedirect(
		raftGroup.BasicStatus(),
		livenessMap,
		r.descRLocked(),
		r.requiresExpiringLeaseRLocked(),
		r.store.Clock().Now(),
	)
}

// rejectProposalWithRedirectLocked is part of the proposer interface.
func (rp *replicaProposer) rejectProposalWithRedirectLocked(
	ctx context.Context, prop *ProposalData, redirectTo roachpb.ReplicaID,
) {
	r := (*Replica)(rp)
	rangeDesc := r.descRLocked()
	storeID := r.store.StoreID()
	r.store.metrics.LeaseRequestErrorCount.Inc(1)
	redirectRep, _ /* ok */ := rangeDesc.GetReplicaDescriptorByID(redirectTo)
	log.VEventf(ctx, 2, "redirecting proposal to node %s; request: %s", redirectRep.NodeID, prop.Request)
	rp.rejectProposalWithErrLocked(ctx, prop, roachpb.NewError(
		roachpb.NewNotLeaseHolderErrorWithSpeculativeLease(
			redirectRep,
			storeID,
			rangeDesc,
			"refusing to acquire lease on follower"),
	))
}

func (rp *replicaProposer) rejectProposalWithLeaseTransferRejectedLocked(
	ctx context.Context,
	prop *ProposalData,
	lease *roachpb.Lease,
	reason raftutil.ReplicaNeedsSnapshotStatus,
) {
	rp.store.metrics.LeaseTransferErrorCount.Inc(1)
	log.VEventf(ctx, 2, "not proposing lease transfer because the target %s may "+
		"need a snapshot: %s", lease.Replica, reason)
	err := NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(lease.Replica, reason)
	rp.rejectProposalWithErrLocked(ctx, prop, roachpb.NewError(err))
}

func (rp *replicaProposer) rejectProposalWithErrLocked(
	ctx context.Context, prop *ProposalData, pErr *roachpb.Error,
) {
	(*Replica)(rp).cleanupFailedProposalLocked(prop)
	prop.finishApplication(ctx, proposalResult{Err: pErr})
}
