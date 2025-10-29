// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import "github.com/cockroachdb/cockroach/pkg/storage"

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
