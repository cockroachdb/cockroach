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
type Writer struct {
	// disabledd, if set, disables the Writing of WAG nodes. Typically, this
	// happens when the engines aren't separted.
	disabled bool
	// seq allocates WAG sequence numbers using a store-level singleton.
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

// MakeWriter creates a new Writer.
func MakeWriter(seq *Seq) Writer {
	return Writer{seq: seq}
}

// Disable disables the Writer. Should only be used when running in a
// non-separted engines world.
func (w *Writer) Disable() {
	w.disabled = true
}

// AddDep stages a dependency node. These are written before the mutation node
// and express that the state machine must be at a certain applied index before
// the mutation can be replayed.
func (w *Writer) AddDep(node wagpb.Node) {
	if w.disabled {
		return
	}
	w.deps = append(w.deps, node)
}

// SetMutation stages the primary lifecycle node. May not be called more than 
// once per-command application.
func (w *Writer) SetMutation(node wagpb.Node) {
	if w.disabled {
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
	if w.disabled {
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
	count := uint64(len(w.deps)) + 1
	base := w.seq.Next(count)

	for i, dep := range w.deps {
		if err := Write(raftBatch, base+uint64(i), dep); err != nil {
			return errors.Wrap(err, "writing WAG dependency node")
		}
	}

	w.mutation.Mutation.Batch = stateBatchRepr
	if err := Write(raftBatch, base+count-1, w.mutation); err != nil {
		return errors.Wrap(err, "writing WAG mutation node")
	}

	w.Reset()
	return nil
}

// Reset clears the writer so it can be reused for the next batch.
func (w *Writer) Reset() {
	if w.disabled {
		return
	}
	w.deps = w.deps[:0]
	w.mutation = wagpb.Node{}
}
