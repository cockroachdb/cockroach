// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// attributes contain additional metadata which may be emitted alongside a row
// but separate from the encoded keys and values.
type attributes struct {
	tableName       string
	headers         map[string][]byte
	mvcc            hlc.Timestamp
	csvColumnHeader []byte
}

// rowEvent is the per-row payload that flows from EmitRow into a batching
// sink (batchingSink or noLingerSink). It carries everything a SinkClient's
// BatchBuffer.Append needs, plus the kvevent.Alloc that must be released
// after the row is fully processed.
type rowEvent struct {
	key             []byte
	val             []byte
	topicDescriptor TopicDescriptor
	headers         rowHeaders
	csvColumnHeader []byte

	alloc kvevent.Alloc
	mvcc  hlc.Timestamp
}

// rowEvent values are claimed and freed from a sync.Pool to avoid GC
// thrashing in the steady-state where every event is allocated, passed
// across goroutines, and then released.
var eventPool = sync.Pool{
	New: func() interface{} {
		return new(rowEvent)
	},
}

func newRowEvent() *rowEvent {
	return eventPool.Get().(*rowEvent)
}

func freeRowEvent(e *rowEvent) {
	*e = rowEvent{}
	eventPool.Put(e)
}
