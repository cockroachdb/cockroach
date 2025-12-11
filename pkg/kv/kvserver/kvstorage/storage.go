// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
)

// The following are type aliases that help annotating various storage
// interaction functions in this package and its clients, accordingly to the
// logical type of the storage engine being used.
//
// From the perspective of this package, the storage engine is logically
// separated into state machine engine and raft engine. The raft engine contains
// raft state: mainly HardState, raft log, and RaftTruncatedState.
//
// The state machine engine contains mainly the replicated state machine (RSM)
// of all ranges hosted by the store, as well as the supporting unreplicated
// metadata of these ranges, such as RaftReplicaID and RangeTombstone.
//
// TODO(pav-kv): as of now, these types are transparently convertible to one
// another and storage package interfaces. It is possible to mis-use them
// without compilation errors. This is fine while separated storage is not
// supported, but we want to make type-checking stronger eventually.
type (
	StateRO storage.Reader
	StateWO storage.Writer
	StateRW storage.ReadWriter
	RaftRO  storage.Reader
	RaftWO  storage.Writer
	RaftRW  storage.ReadWriter
)

// State gives read and write access to the state machine. Note that writes may
// or may not be visible to reads, depending on the user's semantics.
type State struct {
	RO StateRO
	WO StateWO
}

// Raft gives read and write access to the raft engine. Note that writes may or
// may not be visible to reads, depending on the user's semantics.
type Raft struct {
	RO RaftRO
	WO RaftWO
}

// ReadWriter gives read and write access to both engines. Note that writes may
// or may not be visible to reads, depending on the user's semantics.
type ReadWriter struct {
	State State
	Raft  Raft
}

// TODOState interprets the provided storage accessor as the State engine.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns,
// and switched to WrapState() or other explicitly defined engine types.
func TODOState(rw storage.ReadWriter) State {
	return State{RO: rw, WO: rw}
}

// WrapState interprets the provided storage accessor as the State engine.
func WrapState(rw StateRW) State {
	return State{RO: rw, WO: rw}
}

// TODORaft interprets the provided storage accessor as the Raft engine.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns.
func TODORaft(rw storage.ReadWriter) Raft {
	return Raft{RO: rw, WO: rw}
}

// TODOReaderWriter interprets the given reader and writer as if they are able
// to read and write both engines.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns.
func TODOReaderWriter(r storage.Reader, w storage.Writer) ReadWriter {
	return ReadWriter{
		State: State{RO: r, WO: w},
		Raft:  Raft{RO: r, WO: w},
	}
}

// TODOReadWriter interprets the given accessor as reader and writer for both
// engines.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns.
func TODOReadWriter(rw storage.ReadWriter) ReadWriter {
	return TODOReaderWriter(rw, rw)
}

// Engines contains the engines that support the operations of the Store. At the
// time of writing, all three fields will be populated with the same Engine. As
// work on separate raft log proceeds, we will be able to experimentally run
// with a separate log engine, and ultimately allow doing so in production
// deployments.
type Engines struct {
	// stateEngine is the state machine engine, in which the committed raft state
	// materializes after being "applied".
	stateEngine storage.Engine
	// todoEngine is a placeholder used in cases where:
	// - the code does not yet cleanly separate between state and log engine
	// - it is still unclear which of the two engines is the better choice for a
	//   particular write, or there is a candidate, but it needs to be verified.
	todoEngine storage.Engine
	// logEngine is the engine holding mainly the raft state, such as HardState
	// and logs, and the Store-local keys. This engine provides timely durability,
	// by frequent and on-demand syncing.
	logEngine storage.Engine
	// separated is true iff the engines are logically or physically separated.
	// Can be true only in tests.
	separated bool
}

// MakeEngines creates an Engines handle in which both state machine and log
// engine reside in the same physical engine.
func MakeEngines(eng storage.Engine) Engines {
	// TODO(#158281): in test builds, wrap the engines in a way that allows
	// verifying that all accesses touch the correct subset of keys.
	return Engines{
		stateEngine: eng,
		todoEngine:  eng,
		logEngine:   eng,
	}
}

// MakeSeparatedEnginesForTesting creates an Engines handle in which the state
// machine and log engines are logically (or physically) separated. To be used
// only in tests, until separated engines are correctly supported.
func MakeSeparatedEnginesForTesting(state, log storage.Engine) Engines {
	if !buildutil.CrdbTestBuild {
		panic("separated engines are not supported")
	}
	return Engines{
		stateEngine: state,
		todoEngine:  nil,
		logEngine:   log,
		separated:   true,
	}
}

// Engine returns the single engine. Used when the caller implements backwards
// compatible code and neither StateEngine nor LogEngine can be used. This is
// different from TODOEngine in that the caller explicitly acknowledges the fact
// that they are using a combined engine.
func (e *Engines) Engine() storage.Engine {
	if buildutil.CrdbTestBuild && e.separated {
		panic("engines are separated")
	}
	return e.todoEngine
}

// StateEngine returns the state machine engine.
func (e *Engines) StateEngine() storage.Engine {
	return e.stateEngine
}

// LogEngine returns the raft/log engine.
func (e *Engines) LogEngine() storage.Engine {
	return e.logEngine
}

// TODOEngine returns the combined engine, used in the code which currently does
// not support separated engines. The caller must eventually "resolve" this call
// to one of StateEngine, LogEngine, or Engine.
func (e *Engines) TODOEngine() storage.Engine {
	return e.todoEngine
}

// Separated returns true iff the engines are logically or physically separated.
// Can return true only in tests, until separated engines are supported.
func (e *Engines) Separated() bool {
	return e.separated
}
