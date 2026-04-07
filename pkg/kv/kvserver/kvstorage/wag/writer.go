// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package wag

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage/wag/wagpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

// Writer prepares a WAG node encoding a single state machine mutation and the
// replica lifecycle events it encompasses. Callers add events via AddEvent as
// they run, then call Flush to write the node to the raft engine when the time
// comes.
//
// The zero value is a valid, disabled Writer. Use MakeWriter to create an
// enabled Writer.
type Writer struct {
	// seq allocates WAG sequence numbers using a store-level singleton. A nil
	// seq indicates that the Writer is disabled.
	seq *Seq
	// events accumulates the lifecycle events for this node. Each event
	// identifies a replica and the type of lifecycle transition it undergoes.
	// A given RangeID must appear at most once.
	events []wagpb.Event
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

// Empty returns true if no events have been staged on this Writer.
func (w *Writer) Empty() bool {
	return w.disabled() || len(w.events) == 0
}

// AddEvent stages a lifecycle event. Each event identifies a replica and the
// type of lifecycle transition (create, split, destroy, etc.). A given RangeID
// must appear at most once across all events in a Writer.
func (w *Writer) AddEvent(addr wagpb.Addr, eventType wagpb.EventType) {
	if w.disabled() {
		return
	}
	w.events = append(w.events, wagpb.Event{Addr: addr, Type: eventType})
}

// Flush writes the staged WAG node to the raft engine. The node's
// Mutation.Batch is populated with stateBatchRepr, which should be the Repr()
// of the fully prepared state engine batch. No-ops if no events were staged.
func (w *Writer) Flush(raftBatch storage.Writer, stateBatchRepr []byte) error {
	if w.disabled() || len(w.events) == 0 {
		return nil
	}
	return Write(raftBatch, w.seq.Next(), wagpb.Node{
		Events:   w.events,
		Mutation: wagpb.Mutation{Batch: stateBatchRepr},
	})
}

// Clear empties the staged WAG events.
func (w *Writer) Clear() {
	clear(w.events)
	w.events = w.events[:0]
}
