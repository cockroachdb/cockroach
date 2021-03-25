// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// StateMachine represents an instance of a replicated state machine being
// driven by a replication group. The state machine accepts Commands that
// have been committed to the replication group's log and applies them to
// advance to a new state.
//
// All state transitions performed by the state machine are expected to be
// deterministic, which ensures that if each instance is driven from the
// same consistent shared log, they will all stay in sync.
type StateMachine interface {
	// NewBatch creates a new batch that is suitable for accumulating the
	// effects that a group of Commands will have on the replicated state
	// machine. Commands are staged in the batch one-by-one and then the
	// entire batch is committed at once.
	//
	// Batch comes in two flavors - real batches and ephemeral batches.
	// Real batches are capable of accumulating updates from commands and
	// applying them to the state machine. Ephemeral batches are not able
	// to make changes to the durable state machine, but can still be used
	// for the purpose of checking commands to determine whether they will
	// be rejected or not when staged in a real batch. The principal user
	// of ephemeral batches is AckCommittedEntriesBeforeApplication.
	NewBatch(ephemeral bool) Batch
	// ApplySideEffects applies the in-memory side-effects of a Command to
	// the replicated state machine. The method will be called in the order
	// that the commands are committed to the state machine's log. Once the
	// in-memory side-effects of the Command are applied, an AppliedCommand
	// is returned so that it can be finished and acknowledged.
	//
	// The method will always be called with a Command that has been checked
	// and whose persistent state transition has been applied to the state
	// machine. Because this method is called after applying the persistent
	// state transition for a Command, it may not be called in the case of
	// an untimely crash. This means that applying these side-effects will
	// typically update the in-memory representation of the state machine
	// to the same state that it would be in if the process restarted.
	ApplySideEffects(CheckedCommand) (AppliedCommand, error)
}

// ErrRemoved can be returned from ApplySideEffects which will stop the task
// from processing more commands and return immediately. The error should
// only be thrown by non-trivial commands.
var ErrRemoved = errors.New("replica removed")

// Batch accumulates a series of updates from Commands and performs them
// all at once to its StateMachine when applied. Groups of Commands will be
// staged in the Batch such that one or more trivial Commands are staged or
// exactly one non-trivial Command is staged.
type Batch interface {
	// Stage inserts a Command into the Batch. In doing so, the Command is
	// checked for rejection and a CheckedCommand is returned.
	Stage(Command) (CheckedCommand, error)
	// ApplyToStateMachine applies the persistent state transitions staged
	// in the Batch to the StateMachine, atomically.
	ApplyToStateMachine(context.Context) error
	// Close closes the batch and releases any resources that it holds.
	Close()
}

// Decoder is capable of decoding a list of committed raft entries and
// binding any that were locally proposed to their local proposals.
type Decoder interface {
	// DecodeAndBind decodes each of the provided raft entries into commands
	// and binds any that were proposed locally to their local proposals.
	// The method must only be called once per Decoder. It returns whether
	// any of the commands were bound to local proposals waiting for
	// acknowledgement.
	DecodeAndBind(context.Context, []raftpb.Entry) (anyLocal bool, _ error)
	// NewCommandIter creates an iterator over the replicated commands that
	// were passed to DecodeAndBind. The method must not be called until
	// after DecodeAndBind is called.
	NewCommandIter() CommandIterator
	// Reset resets the Decoder and releases any resources that it holds.
	Reset()
}

// Task is an object capable of coordinating the application of commands to
// a replicated state machine after they have been durably committed to a
// raft log.
//
// Committed raft entries are provided to the task through its Decode
// method. The task will then apply these entries to the provided state
// machine when ApplyCommittedEntries is called.
type Task struct {
	sm  StateMachine
	dec Decoder

	// Have entries been decoded yet?
	decoded bool
	// Were any of the decoded commands locally proposed?
	anyLocal bool
	// The maximum number of commands that can be applied in a batch.
	batchSize int32
}

// MakeTask creates a new task with the provided state machine and decoder.
func MakeTask(sm StateMachine, dec Decoder) Task {
	return Task{sm: sm, dec: dec}
}

// Decode decodes the committed raft entries into commands and prepared for the
// commands to be applied to the replicated state machine.
func (t *Task) Decode(ctx context.Context, committedEntries []raftpb.Entry) error {
	var err error
	t.anyLocal, err = t.dec.DecodeAndBind(ctx, committedEntries)
	t.decoded = true
	return err
}

func (t *Task) assertDecoded() {
	if !t.decoded {
		panic("Task.Decode not called yet")
	}
}

// AckCommittedEntriesBeforeApplication attempts to acknowledge the success of
// raft entries that have been durably committed to the raft log but have not
// yet been applied to the proposer replica's replicated state machine.
//
// This is safe because a proposal through raft can be known to have succeeded
// as soon as it is durably replicated to a quorum of replicas (i.e. has
// committed in the raft log). The proposal does not need to wait for the
// effects of the proposal to be applied in order to know whether its changes
// will succeed or fail. This is because the raft log is the provider of
// atomicity and durability for replicated writes, not (ignoring log
// truncation) the replicated state machine itself.
//
// However, there are a few complications to acknowledging the success of a
// proposal at this stage:
//
//  1. Committing an entry in the raft log and having the command in that entry
//     succeed are similar but not equivalent concepts. Even if the entry succeeds
//     in achieving durability by replicating to a quorum of replicas, its command
//     may still be rejected "beneath raft". This means that a (deterministic)
//     check after replication decides that the command will not be applied to the
//     replicated state machine. In that case, the client waiting on the result of
//     the command should not be informed of its success. Luckily, this check is
//     cheap to perform so we can do it here and when applying the command.
//
//     Determining whether the command will succeed or be rejected before applying
//     it for real is accomplished using an ephemeral batch. Commands are staged in
//     the ephemeral batch to acquire CheckedCommands, which can then be acknowledged
//     immediately even though the ephemeral batch itself cannot be used to update
//     the durable state machine. Once the rejection status of each command is
//     determined, any successful commands that permit acknowledgement before
//     application (see CanAckBeforeApplication) are acknowledged. The ephemeral
//     batch is then thrown away.
//
//  2. Some commands perform non-trivial work such as updating Replica configuration
//     state or performing Range splits. In those cases, it's likely that the client
//     is interested in not only knowing whether it has succeeded in sequencing the
//     change in the raft log, but also in knowing when the change has gone into
//     effect. There's currently no exposed hook to ask for an acknowledgement only
//     after a command has been applied, so for simplicity the current implementation
//     only ever acks transactional writes before they have gone into effect. All
//     other commands wait until they have been applied to ack their client.
//
//  3. Even though we can determine whether a command has succeeded without applying
//     it, the effect of the command will not be visible to conflicting commands until
//     it is applied. Because of this, the client can be informed of the success of
//     a write at this point, but we cannot release that write's latches until the
//     write has applied. See ProposalData.signalProposalResult/finishApplication.
//
//  4. etcd/raft may provided a series of CommittedEntries in a Ready struct that
//     haven't actually been appended to our own log. This is most common in single
//     node replication groups, but it is possible when a follower in a multi-node
//     replication group is catching up after falling behind. In the first case,
//     the entries are not yet committed so acknowledging them would be a lie. In
//     the second case, the entries are committed so we could acknowledge them at
//     this point, but doing so seems risky. To avoid complications in either case,
//     the method takes a maxIndex parameter that limits the indexes that it will
//     acknowledge. Typically, callers will supply the highest index that they have
//     durably written to their raft log for this upper bound.
//
func (t *Task) AckCommittedEntriesBeforeApplication(ctx context.Context, maxIndex uint64) error {
	t.assertDecoded()
	if !t.anyLocal {
		return nil // fast-path
	}

	// Create a new ephemeral application batch. All we're interested in is
	// whether commands will be rejected or not when staged in a real batch.
	batch := t.sm.NewBatch(true /* ephemeral */)
	defer batch.Close()

	iter := t.dec.NewCommandIter()
	defer iter.Close()

	// Collect a batch of trivial commands from the applier. Stop at the first
	// non-trivial command or at the first command with an index above maxIndex.
	batchIter := takeWhileCmdIter(iter, func(cmd Command) bool {
		if cmd.Index() > maxIndex {
			return false
		}
		return cmd.IsTrivial()
	})

	// Stage the commands in the (ephemeral) batch.
	stagedIter, err := mapCmdIter(batchIter, batch.Stage)
	if err != nil {
		return err
	}

	// Acknowledge any locally-proposed commands that succeeded in being staged
	// in the batch and can be acknowledged before they are actually applied.
	// Don't acknowledge rejected proposals early because the StateMachine may
	// want to retry the command instead of returning the error to the client.
	return forEachCheckedCmdIter(ctx, stagedIter, func(cmd CheckedCommand, ctx context.Context) error {
		if !cmd.Rejected() && cmd.IsLocal() && cmd.CanAckBeforeApplication() {
			return cmd.AckSuccess(ctx)
		}
		return nil
	})
}

// SetMaxBatchSize sets the maximum application batch size. If 0, no limit
// will be placed on the number of commands that can be applied in a batch.
func (t *Task) SetMaxBatchSize(size int) {
	t.batchSize = int32(size)
}

// ApplyCommittedEntries applies raft entries that have been committed to the
// raft log but have not yet been applied to the replicated state machine.
func (t *Task) ApplyCommittedEntries(ctx context.Context) error {
	t.assertDecoded()

	iter := t.dec.NewCommandIter()
	for iter.Valid() {
		if err := t.applyOneBatch(ctx, iter); err != nil {
			// If the batch threw an error, reject all remaining commands in the
			// iterator to avoid leaking resources or leaving a proposer hanging.
			//
			// NOTE: forEachCmdIter closes iter.
			if rejectErr := forEachCmdIter(ctx, iter, func(cmd Command, ctx context.Context) error {
				return cmd.AckErrAndFinish(ctx, err)
			}); rejectErr != nil {
				return rejectErr
			}
			return err
		}
	}
	iter.Close()
	return nil
}

// applyOneBatch consumes a batch-worth of commands from the provided iter and
// applies them atomically to the StateMachine. A batch will contain either:
// a) one or more trivial commands
// b) exactly one non-trivial command
func (t *Task) applyOneBatch(ctx context.Context, iter CommandIterator) error {
	// Create a new application batch.
	batch := t.sm.NewBatch(false /* ephemeral */)
	defer batch.Close()

	// Consume a batch-worth of commands.
	pol := trivialPolicy{maxCount: t.batchSize}
	batchIter := takeWhileCmdIter(iter, func(cmd Command) bool {
		return pol.maybeAdd(cmd.IsTrivial())
	})

	// Stage each command in the batch.
	stagedIter, err := mapCmdIter(batchIter, batch.Stage)
	if err != nil {
		return err
	}

	// Apply the persistent state transitions to the state machine.
	if err := batch.ApplyToStateMachine(ctx); err != nil {
		return err
	}

	// Apply the side-effects of each command to the state machine.
	appliedIter, err := mapCheckedCmdIter(stagedIter, t.sm.ApplySideEffects)
	if err != nil {
		return err
	}

	// Finish and acknowledge the outcome of each command.
	return forEachAppliedCmdIter(ctx, appliedIter, AppliedCommand.AckOutcomeAndFinish)
}

// trivialPolicy encodes a batching policy that allows a batch to consist of
// either one or more trivial commands or exactly one non-trivial command.
type trivialPolicy struct {
	maxCount int32

	trivialCount    int32
	nonTrivialCount int32
}

// maybeAdd returns whether a command with the specified triviality should be
// added to a batch given the batching policy. If the method returns true, the
// command is considered to have been added.
func (p *trivialPolicy) maybeAdd(trivial bool) bool {
	if !trivial {
		if p.trivialCount+p.nonTrivialCount > 0 {
			return false
		}
		p.nonTrivialCount++
		return true
	}
	if p.nonTrivialCount > 0 {
		return false
	}
	if p.maxCount > 0 && p.maxCount == p.trivialCount {
		return false
	}
	p.trivialCount++
	return true
}

// Close ends the task, releasing any resources that it holds and resetting the
// Decoder. The Task cannot be used again after being closed.
func (t *Task) Close() {
	t.dec.Reset()
	*t = Task{}
}
