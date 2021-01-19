// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// DeclareKeysFunc adds all key spans that a command touches to the latchSpans
// set. It then adds all key spans within which that the command expects to have
// isolation from conflicting transactions to the lockSpans set.
type DeclareKeysFunc func(
	rs ImmutableRangeState,
	header roachpb.Header,
	request roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
)

// ImmutableRangeState exposes the properties of a Range that cannot change
// across a Range's lifetime. The interface is used to manage the visibility
// that DeclareKeysFunc implementations have into the Range's descriptor, so
// that implementations don't rely on information that can change between the
// time that a command declares its keys and the time that it evaluates.
//
// The quintessential example of a property of a Range that is not immutable is
// its end key. It would be incorrect to declare keys between a Range's start
// key and its current end key as a means of latching the entire range, because
// a merge of a right-hand neighbor could complete in between the time that a
// request declares its keys and the time that it evaluates. This could lead to
// a violation of the mutual exclusion that the command was expecting to have.
type ImmutableRangeState interface {
	GetRangeID() roachpb.RangeID
	GetStartKey() roachpb.RKey
}

// A Command is the implementation of a single request within a BatchRequest.
type Command struct {
	// DeclareKeys adds all keys this command touches, and when (if applicable),
	// to the given SpanSet.
	DeclareKeys DeclareKeysFunc

	// Eval{RW,RO} evaluates a read-{write,only} command respectively on the
	// given engine.{ReadWriter,Reader}. This is typically derived from
	// engine.NewBatch or engine.NewReadOnly (which is more performant than
	// engine.Batch for read-only commands).
	// It should populate the supplied response (always a non-nil pointer to the
	// correct type) and return special side effects (if any) in the Result. If
	// it writes to the engine it should also update *CommandArgs.Stats. It
	// should treat the provided request as immutable.
	//
	// Only one of these is ever set at a time.
	EvalRW func(context.Context, storage.ReadWriter, CommandArgs, roachpb.Response) (result.Result, error)
	EvalRO func(context.Context, storage.Reader, CommandArgs, roachpb.Response) (result.Result, error)
}

var cmds = make(map[roachpb.Method]Command)

// RegisterReadWriteCommand makes a read-write command available for execution.
// It must only be called before any evaluation takes place.
func RegisterReadWriteCommand(
	method roachpb.Method,
	declare DeclareKeysFunc,
	impl func(context.Context, storage.ReadWriter, CommandArgs, roachpb.Response) (result.Result, error),
) {
	register(method, Command{
		DeclareKeys: declare,
		EvalRW:      impl,
	})
}

// RegisterReadOnlyCommand makes a read-only command available for execution. It
// must only be called before any evaluation takes place.
func RegisterReadOnlyCommand(
	method roachpb.Method,
	declare DeclareKeysFunc,
	impl func(context.Context, storage.Reader, CommandArgs, roachpb.Response) (result.Result, error),
) {
	register(method, Command{
		DeclareKeys: declare,
		EvalRO:      impl,
	})
}

func register(method roachpb.Method, command Command) {
	if _, ok := cmds[method]; ok {
		log.Fatalf(context.TODO(), "cannot overwrite previously registered method %v", method)
	}
	cmds[method] = command
}

// UnregisterCommand is provided for testing and allows removing a command.
// It is a no-op if the command is not registered.
func UnregisterCommand(method roachpb.Method) {
	delete(cmds, method)
}

// LookupCommand returns the command for the given method, with the boolean
// indicating success or failure.
func LookupCommand(method roachpb.Method) (Command, bool) {
	cmd, ok := cmds[method]
	return cmd, ok
}
