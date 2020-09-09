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

import "context"

// Command is a command that has been successfully replicated through raft
// by being durably committed to the raft log of a quorum of peers in a raft
// group.
type Command interface {
	// Index is the log index of the corresponding raft entry.
	Index() uint64
	// IsTrivial returns whether the command can apply in a batch.
	IsTrivial() bool
	// IsLocal returns whether the command was locally proposed. Command
	// that were locally proposed typically have a client waiting on a
	// response, so there is additional urgency to apply them quickly.
	IsLocal() bool
	// AckErrAndFinish signals that the application of the command has been
	// rejected due to the provided error. It also relays this rejection of
	// the command to its client if it was proposed locally. An error will
	// immediately stall entry application, so one must only be returned if
	// the state machine is no longer able to make progress.
	//
	// Either AckOutcomeAndFinish or AckErrAndFinish will be called exactly
	// once per Command.
	AckErrAndFinish(context.Context, error) error
}

// CheckedCommand is a command that has been checked to see whether it can
// apply successfully or not. Committing an entry in a raft log and having
// the command in that entry succeed are similar but not equivalent concepts.
// A successfully committed entry may contain a command that the replicated
// state machine decides to reject (deterministically).
type CheckedCommand interface {
	Command
	// Rejected returns whether the command was rejected.
	Rejected() bool
	// CanAckBeforeApplication returns whether the success of the command
	// can be acknowledged before the command has been applied to the state
	// machine.
	CanAckBeforeApplication() bool
	// AckSuccess acknowledges the success of the command to its client.
	// Must only be called if !Rejected.
	AckSuccess(context.Context) error
}

// AppliedCommand is a command that has been applied to the replicated state
// machine. A command is considered "applied" if it has been staged in a
// Batch which has been committed and had its side-effects run on the state
// machine. If the command was rejected (see CheckedCommand), applying the
// command will likely be a no-op, but that is up to the implementation of
// the state machine.
type AppliedCommand interface {
	CheckedCommand
	// AckOutcomeAndFinish signals that the application of the command has
	// completed. It also acknowledges the outcome of the command to its
	// client if it was proposed locally. An error will immediately stall
	// entry application, so one must only be returned if the state machine
	// is no longer able to make progress.
	//
	// Either AckOutcomeAndFinish or AckErrAndFinish will be called exactly
	// once per Command.
	AckOutcomeAndFinish(context.Context) error
}

// CommandIteratorBase is a common interface extended by all iterator and
// list variants. It is exported so its methods are displayed in godoc when
// it is embedded in other interfaces.
type CommandIteratorBase interface {
	// Valid returns whether the iterator is pointing at a valid element.
	Valid() bool
	// Next advances the iterator. Must not be called if valid is false.
	Next()
	// Close closes the iterator. Once closed, it must not be used.
	Close()
}

// CommandIterator is an iterator over replicated commands.
type CommandIterator interface {
	CommandIteratorBase
	// Cur returns the command that the iterator is currently pointing at.
	// Must not be called if valid is false.
	Cur() Command
	// NewList returns a new empty command list. Usages of the list will
	// always advance the iterator before pushing in to the list, so
	// implementors are free to share backing memory between the two.
	NewList() CommandList
	// NewCheckedList returns a new empty checked command list. Usages
	// of the list will always advance the iterator before pushing into
	// to the list, so implementors are free to share backing memory
	// between the two.
	NewCheckedList() CheckedCommandList
}

// CommandList is a list of replicated commands.
type CommandList interface {
	CommandIterator
	// Append adds the command to the end of the list.
	Append(Command)
}

// CheckedCommandIterator is an iterator over checked replicated
// commands.
type CheckedCommandIterator interface {
	CommandIteratorBase
	// CurChecked returns the checked command that the iterator is
	// currently pointing at. Must not be called if valid is false.
	CurChecked() CheckedCommand
	// NewAppliedList returns a new empty applied command list. Usages
	// of the list will always advance the iterator before pushing into
	// to the list, so implementors are free to share backing memory
	// between the two.
	NewAppliedList() AppliedCommandList
}

// CheckedCommandList is a list of checked replicated commands.
type CheckedCommandList interface {
	CheckedCommandIterator
	// AppendChecked adds the checked command to the end of the list.
	AppendChecked(CheckedCommand)
}

// AppliedCommandIterator is an iterator over applied replicated commands.
type AppliedCommandIterator interface {
	CommandIteratorBase
	// CurApplied returns the applied command that the iterator is
	// currently pointing at. Must not be called if valid is false.
	CurApplied() AppliedCommand
}

// AppliedCommandList is a list of applied replicated commands.
type AppliedCommandList interface {
	AppliedCommandIterator
	// AppendApplied adds the applied command to the end of the list.
	AppendApplied(AppliedCommand)
}

// takeWhileCmdIter returns an iterator that yields commands based on a
// predicate. It will call the predicate on each command in the provided
// iterator and yield elements while it returns true. The function does
// NOT close the provided iterator, but does drain it of any commands
// that are moved to the returned iterator.
func takeWhileCmdIter(iter CommandIterator, pred func(Command) bool) CommandIterator {
	ret := iter.NewList()
	for iter.Valid() {
		cmd := iter.Cur()
		if !pred(cmd) {
			break
		}
		iter.Next()
		ret.Append(cmd)
	}
	return ret
}

// mapCmdIter returns an iterator that contains the result of each command
// from the provided iterator transformed by a closure. The closure is
// responsible for converting Commands into CheckedCommand. The function
// closes the provided iterator.
func mapCmdIter(
	iter CommandIterator, fn func(Command) (CheckedCommand, error),
) (CheckedCommandIterator, error) {
	defer iter.Close()
	ret := iter.NewCheckedList()
	for iter.Valid() {
		checked, err := fn(iter.Cur())
		if err != nil {
			ret.Close()
			return nil, err
		}
		iter.Next()
		ret.AppendChecked(checked)
	}
	return ret, nil
}

// mapCheckedCmdIter returns an iterator that contains the result of each
// command from the provided iterator transformed by a closure. The closure
// is responsible for converting CheckedCommand into AppliedCommand. The
// function closes the provided iterator.
func mapCheckedCmdIter(
	iter CheckedCommandIterator, fn func(CheckedCommand) (AppliedCommand, error),
) (AppliedCommandIterator, error) {
	defer iter.Close()
	ret := iter.NewAppliedList()
	for iter.Valid() {
		applied, err := fn(iter.CurChecked())
		if err != nil {
			ret.Close()
			return nil, err
		}
		iter.Next()
		ret.AppendApplied(applied)
	}
	return ret, nil
}

// In the following three functions, fn is written with ctx as a 2nd param
// because callers want to bind it to methods that have Commands (or variants)
// as the receiver, which mandates that to be the first param. The caller didn't
// want to introduce a callback instead to make it clear that nothing escapes to
// the heap.

// forEachCmdIter calls a closure on each command in the provided iterator. The
// function closes the provided iterator.
func forEachCmdIter(
	ctx context.Context, iter CommandIterator, fn func(Command, context.Context) error,
) error {
	defer iter.Close()
	for iter.Valid() {
		if err := fn(iter.Cur(), ctx); err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}

// forEachCheckedCmdIter calls a closure on each command in the provided
// iterator. The function closes the provided iterator.
func forEachCheckedCmdIter(
	ctx context.Context, iter CheckedCommandIterator, fn func(CheckedCommand, context.Context) error,
) error {
	defer iter.Close()
	for iter.Valid() {
		if err := fn(iter.CurChecked(), ctx); err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}

// forEachAppliedCmdIter calls a closure on each command in the provided
// iterator. The function closes the provided iterator.
func forEachAppliedCmdIter(
	ctx context.Context, iter AppliedCommandIterator, fn func(AppliedCommand, context.Context) error,
) error {
	defer iter.Close()
	for iter.Valid() {
		if err := fn(iter.CurApplied(), ctx); err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}
