// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/errors"
)

// Writer stages WAG nodes during raft command application. Replica lifecycle
// events (splits, merges, destroys, etc.) use it to add dependencies as they
// run, stage their mutation, and flush WAG nodes to the RaftBatch before
// committing it towards the end of application.
//
// The zero value is a valid, disabled Writer. Use MakeWriter to create an
// enabled Writer.
type Writer struct {
	// seq allocates WAG sequence numbers using a store-level singleton. A nil
	// seq indicates that the Writer is disabled.
	seq *Seq
	// deps tracks all dependency nodes. These carry a "happens before"
	// relationship to the mutation node, but don't carry a mutation themselves.
	// They are added via AddDep.
	//
	// For instance, a split at Raft index I needs a NodeApply at I-1 to
	// indicate that the state machine must be caught up to that point before
	// the split can be replayed.
	deps []wagpb.Node
	// mutation is the primary lifecycle event. Its Mutation.Batch gets filled
	// in at Flush time, once the State engine's batch has been fully prepared.
	//
	// There's exactly one of these per-Writer, and can be set at any point
	// during command application. A zero-value (Type == NodeEmpty) means
	// nothing's been staged yet, and is used to uphold this invariant.
	mutation wagpb.Node
}

// MakeWriter creates a new, enabled Writer.
func MakeWriter(seq *Seq) Writer {
	return Writer{seq: seq}
}

// disabled returns true if the Writer is disabled and no WAG nodes should be
// written.
func (w *Writer) disabled() bool {
	return w.seq == nil
}

// Empty returns true if no WAG nodes have been staged on this Writer.
func (w *Writer) Empty() bool {
	return w.disabled() || w.mutation.Type == wagpb.NodeType_NodeEmpty
}

// AddDep stages a dependency node. These are written before the mutation node
// and express that the state machine must be at a certain applied index before
// the mutation can be replayed.
func (w *Writer) AddDep(node wagpb.Node) {
	if w.disabled() {
		return
	}
	w.deps = append(w.deps, node)
}

// SetMutation stages the primary lifecycle node. May not be called more than 
// once per-command application.
func (w *Writer) SetMutation(node wagpb.Node) {
	if w.disabled() {
		return
	}
	if w.mutation.Type != wagpb.NodeType_NodeEmpty {
		panic(errors.AssertionFailedf(
			"WagWriter.SetMutation called twice: existing %s, new %s",
			w.mutation.Addr, node.Addr,
		))
	}
	w.mutation = node
}

// Flush writes all staged WAG nodes to the raft engine. The mutation node's
// Mutation.Batch is populated with stateBatchRepr, which should be the Repr()
// of the fully prepared State engine batch. No-ops if no mutation was staged.
//
// Resets the Writer after flushing, so it can be reused for the next batch.
func (w *Writer) Flush(raftBatch kvstorage.RaftWO, stateBatchRepr []byte) error {
	if w.disabled() {
		return nil
	}
	if w.mutation.Type == wagpb.NodeType_NodeEmpty {
		if len(w.deps) != 0 {
			return errors.AssertionFailedf(
				"WAG dependency nodes staged without a mutation node",
			)
		}
		return nil
	}
	seq := w.seq.Next(uint64(len(w.deps)) + 1)

	for _, dep := range w.deps {
		if err := Write(raftBatch, seq, dep); err != nil {
			return errors.Wrap(err, "writing WAG dependency node")
		}
		seq++
	}

	w.mutation.Mutation.Batch = stateBatchRepr
	if err := Write(raftBatch, seq, w.mutation); err != nil {
		return errors.Wrap(err, "writing WAG mutation node")
	}

	w.Reset()
	return nil
}

// Reset clears the writer so it can be reused for the next batch.
func (w *Writer) Reset() {
	if w.disabled() {
		return
	}
	clear(w.deps)
	w.deps = w.deps[:0]
	w.mutation = wagpb.Node{}
}
