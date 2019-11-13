// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package raftstorage defines the API boundaries for a store to interact with
// the Raft state for the replicas it contains. The exposed interfaces are a
// subset of those also found in the `engine` package. It itself does not expose
// the `engine` API.
//
// Currently this package serves as glue code to cleanly separate raft storage
// code in pkg/storage from directly accessing the underlying
// engine.{Engine,Batch,...}. As a result, its usage closely mimics that of the
// lower-level engine package. Over time we expect to pull in more code sitting
// below this glue code and expose Raft state (i.e. log entries, truncated
// state, etc.) as first class primitives. Long term, think `etcd/raft.Storage`
// but for multi-raft.
//
// We allow implicit conversions between {Writer,Reader,ReadWriter} and
// engine.{Writer,Reader,ReadWriter} respectively. This in intentional as the
// engine.MVCC{...} APIs expect engine.{Writer,Reader,ReadWriter}s. We don't,
// however, allow implicit conversion between {Engine,Batch} and
// engine.{Engine,Batch} respectively. We expect all usage to derive through
// Engines constructed through this package.
//
// (Use of this package also introduces the nice property that lets us now jump
// to usage of all code pertaining to "raft storage".)
package raftstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

// Writer is a thin wrapper around engine.Writer, and represents an
// engine.Writer derived from a raftstorage.{Engine,Batch}.
type Writer interface {
	engine.Writer
}

// Reader is a thin wrapper around engine.Reader, and represents an
// engine.Reader derived from a raftstorage.{Engine,Batch}.
type Reader interface {
	engine.Reader
}

// ReadWriter is a thin wrapper around engine.Writer, and represents an
// engine.ReadWriter derived from a raft storage engine.
type ReadWriter interface {
	Reader
	Writer
}

// TODO(irfansharif): Encapsulate engine specific code in implementation here.
// Right now everything passes through thin shims that simply forward to the
// underlying engine interfaces. This was done to make it easier to wean out
// raft storage addressing code within pkg/storage. This package, however,
// should should be a "deeper" one, pulling in all raft storage abstractions.
// Merging in the RaftEntryCache and replica_raftstorage.go would be good
// places to start. We should create a thin wrapper around the Engine type to
// act as a "handler" for a given replica, with all the key generation happening
// internally (instead of constructing raft keys manually at the caller).

// Engine represents a raft storage engine. It's a subset of the interface
// defined in engine.Engine. Refer to the documentation in pkg/storage/engine
// for usage.
type Engine interface {
	ReadWriter
	NewBatch() Batch
	NewWriteOnlyBatch() Batch
	NewReadOnly() ReadWriter
	NewSnapshot() Reader
	UnderlyingEngine() engine.Engine

	IngestExternalFiles(ctx context.Context, paths []string) error
	CreateCheckpoint(dir string) error
}

// Batch represents a batch derived from Engine. It's a subset of the interface
// defined in engine.Batch. Refer to the documentation in pkg/storage/engine for
// usage.
type Batch interface {
	ReadWriter
	Distinct() ReadWriter
	Empty() bool
	Commit(sync bool) error
}

// raftEngine is a concrete implementation of Engine.
type raftEngine struct {
	engine.Engine
}

var _ Engine = &raftEngine{}
var _ ReadWriter = &raftEngine{}

// Wrap wraps an existing engine.Engine that is to be used as a raft storage
// engine.
//
// TODO(irfansharif): This exists as a stop gap for the migration story around
// raft storage. Once that's sorted out, a separate engine will need to be
// constructed at all call sites. The same applies for WrapBatch below, which
// needs to be removed entirely.
func Wrap(eng engine.Engine) Engine {
	return &raftEngine{Engine: eng}
}

func (e *raftEngine) NewBatch() Batch {
	return &raftBatch{
		Batch: e.UnderlyingEngine().NewBatch(),
	}
}

func (e *raftEngine) NewWriteOnlyBatch() Batch {
	return &raftBatch{
		Batch: e.UnderlyingEngine().NewWriteOnlyBatch(),
	}
}

func (e *raftEngine) NewReadOnly() ReadWriter {
	return e.UnderlyingEngine().NewReadOnly()
}

func (e *raftEngine) NewSnapshot() Reader {
	return e.UnderlyingEngine().NewSnapshot()
}

func (e *raftEngine) UnderlyingEngine() engine.Engine {
	return e.Engine
}

// Close closes the raft engine.
//
// TODO(irfansharif): Close is a no-op for now, while the raftstorage engine is
// backed by and existing engine (so we avoid double closing). This will need be
// changed when we're using a separate engine.
func (e *raftEngine) Close() {}

type raftBatch struct {
	engine.Batch
}

var _ Batch = &raftBatch{}
var _ ReadWriter = &raftBatch{}

// WrapBatch wraps an existing engine.Batch that is to be used as a raft storage
// batch.
func WrapBatch(batch engine.Batch) Batch {
	return &raftBatch{Batch: batch}
}

func (b *raftBatch) Distinct() ReadWriter {
	return b.Batch.Distinct()
}
