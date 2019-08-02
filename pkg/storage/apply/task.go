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
	NewBatch() Batch
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
// applies them atomically to the StateMachine. A batch will contain either:
// a) one or more trivial commands
// b) exactly one non-trivial command
func (t *Task) applyOneBatch(ctx context.Context, iter CommandIterator) error {
	// Create a new application batch.
	batch := t.sm.NewBatch()
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
