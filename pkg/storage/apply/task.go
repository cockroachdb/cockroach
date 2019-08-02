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

	"go.etcd.io/etcd/raft/raftpb"
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
	// Batch comes in two flavors - real batches and mock batches. Real
	// batches are capable of accumulating updates from commands and
	// applying them to the state machine. Mock batches are not able to
	// make changes to the state machine, but can still be used for the
	// purpose of checking commands to determine whether they will be
	// rejected or not when staged in a real batch. The principal user
	// of mock batches is AckCommittedEntriesBeforeApplication.
	NewBatch(mock bool) Batch
	// ApplySideEffects applies the in-memory side-effects of a Command to
	// the replicated state machine. The method will be called in the order
	// that the commands are committed to the state machine's log. It will
	// always be called with a Command that has been checked and whose Batch
	// has already been committed.
	ApplySideEffects(CheckedCommand) (AppliedCommand, error)
}

// Batch accumulates a series of updates from Commands and applies them all
// at once to its StateMachine when committed. Groups of Commands will be
// staged in the Batch such that one or more trivial Commands are staged or
// exactly one non-trivial Command is staged.
type Batch interface {
	// Stage inserts a Command into the Batch.
	Stage(Command) (CheckedCommand, error)
	// Commit commits the updates staged in the Batch to the StateMachine.
	Commit(context.Context) error
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
// as soon as it is durability replicated to a quorum of replicas (i.e. has
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
//     it for real is accomplished using a mock batch. Commands are staged in the
//     mock batch to acquire CheckedCommands, which can be acknowledged even though
//     the mock batch itself cannot be committed. Once the rejection status of each
//     command is determined, any successful commands that permit acknowledgement
//     before application (see CanAckBeforeApplication) are acknowledged. The mock
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

	// Create a new mock application batch. All we're interested in is whether
	// commands will be rejected or not when staged in a real batch.
	batch := t.sm.NewBatch(true /* mock */)
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

	// Stage the commands in the (mock) batch.
	stagedIter, err := mapCmdIter(batchIter, batch.Stage)
	if err != nil {
		return err
	}

	// Acknowledge any locally-proposed commands that succeeded in being staged
	// in the batch and can be acknowledged before they are actually applied.
	return forEachCheckedCmdIter(stagedIter, func(cmd CheckedCommand) error {
		if !cmd.Rejected() && cmd.IsLocal() && cmd.CanAckBeforeApplication() {
			return cmd.AckSuccess()
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
	defer iter.Close()
	for iter.Valid() {
		if err := t.applyOneBatch(ctx, iter); err != nil {
			return err
		}
	}
	return nil
}

// applyOneBatch consumes a batch-worth of commands from the provided iter and
// applies them atomically using the applier. A batch will contain either:
// a) one or more trivial commands
// b) exactly one non-trivial command
func (t *Task) applyOneBatch(ctx context.Context, iter CommandIterator) error {
	// Create a new application batch.
	batch := t.sm.NewBatch(false /* mock */)
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

	// Commit the batch to the storage engine.
	if err := batch.Commit(ctx); err != nil {
		return err
	}

	// Apply the side-effects of each command.
	appliedIter, err := mapCheckedCmdIter(stagedIter, t.sm.ApplySideEffects)
	if err != nil {
		return err
	}

	// Finish and acknowledge the outcome of each command.
	return forEachAppliedCmdIter(appliedIter, AppliedCommand.FinishAndAckOutcome)
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
