// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvevent defines kvfeed events and buffers to communicate them
// locally.
package kvevent

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ErrBufferClosed is returned by Readers when no more values will be
// returned from the buffer.
type ErrBufferClosed struct {
	reason error
}

// Error() implements the error interface
func (e ErrBufferClosed) Error() string {
	return "buffer closed"
}

func (e ErrBufferClosed) Unwrap() error {
	return e.reason
}

// ErrNormalRestartReason is a sentinel error to indicate the
// ErrBufferClosed's reason is a pending restart
var ErrNormalRestartReason = errors.New("writer can restart")

// Buffer is an interface for communicating kvfeed entries between processors.
type Buffer interface {
	Reader
	Writer
}

// Reader is the read portion of the Buffer interface.
type Reader interface {
	// Get retrieves an entry from the buffer.
	Get(ctx context.Context) (Event, error)
}

// Writer is the write portion of the Buffer interface.
type Writer interface {
	// Add adds event to this writer.
	Add(ctx context.Context, event Event) error
	// Drain waits until all events buffered by this writer has been consumed.
	// It then closes the writer with reason ErrNormalRestartReason.
	Drain(ctx context.Context) error
	// CloseWithReason closes this writer. reason may be added as a detail to ErrBufferClosed.
	CloseWithReason(ctx context.Context, reason error) error
}

// MemAllocator is an interface for acquiring memory.
type MemAllocator interface {
	AcquireMemory(ctx context.Context, n int64) (Alloc, error)
}

// Type indicates the type of the event.
// Different types indicate which methods will be meaningful.
// Events are implemented this way rather than as an interface to remove the
// need to box the events and allow for events to be used in slices directly.
type Type uint8

const (
	// TypeFlush indicates a request to flush buffered data.
	// This request type is emitted by blocking buffer when it's blocked, waiting
	// for more memory.
	TypeFlush Type = iota

	// TypeKV indicates that the KV, PrevKeyValue, and BackfillTimestamp methods
	// on the Event meaningful.
	TypeKV

	// Private fields indicating the type of the resolved event.
	resolvedNone
	resolvedBackfill
	resolvedRestart
	resolvedExit

	// TypeResolved indicates that the Resolved method on the Event will be
	// meaningful.
	TypeResolved = resolvedNone

	// number of event types.
	numEventTypes = TypeResolved + 1
)

// Event represents an event emitted by a kvfeed. It is either a KV or a
// resolved timestamp.
type Event struct {
	ev                 *kvpb.RangeFeedEvent
	et                 Type
	backfillTimestamp  hlc.Timestamp
	bufferAddTimestamp time.Time
	alloc              Alloc
}

// Type returns the event's Type.
func (e *Event) Type() Type {
	switch e.et {
	case resolvedNone, resolvedBackfill, resolvedRestart, resolvedExit:
		return TypeResolved
	default:
		return e.et
	}
}

// Index returns numerical/ordinal type index suitable for indexing into arrays.
func (t Type) Index() int {
	switch t {
	case TypeFlush:
		return int(TypeFlush)
	case TypeKV:
		return int(TypeKV)
	case TypeResolved, resolvedBackfill, resolvedRestart, resolvedExit:
		return int(TypeResolved)
	default:
		log.Warningf(context.TODO(),
			"returning TypeFlush boundary type for unknown event type %d", t)
		return int(TypeFlush)
	}
}

// Raw returns the underlying RangeFeedEvent.
func (e *Event) Raw() *kvpb.RangeFeedEvent {
	return e.ev
}

// ApproximateSize returns events approximate size in bytes.
func (e *Event) ApproximateSize() int {
	if e.et == TypeFlush {
		return 0
	}
	return e.ev.Size() + int(unsafe.Sizeof(Event{}))
}

// KV is populated if this event returns true for IsKV().
func (e *Event) KV() roachpb.KeyValue {
	v := e.ev.Val
	return roachpb.KeyValue{Key: v.Key, Value: v.Value}
}

// PrevKeyValue returns the previous value for this event. PrevKeyValue is non-zero
// if this is a KV event and the key had a non-tombstone value before the change
// and the before value of each change was requested (optDiff).
func (e *Event) PrevKeyValue() roachpb.KeyValue {
	v := e.ev.Val
	return roachpb.KeyValue{Key: v.Key, Value: v.PrevValue}
}

func (e *Event) boundaryType() jobspb.ResolvedSpan_BoundaryType {
	switch e.et {
	case resolvedNone:
		return jobspb.ResolvedSpan_NONE
	case resolvedBackfill:
		return jobspb.ResolvedSpan_BACKFILL
	case resolvedRestart:
		return jobspb.ResolvedSpan_RESTART
	case resolvedExit:
		return jobspb.ResolvedSpan_EXIT
	default:
		log.Warningf(context.TODO(),
			"returning jobspb.ResolvedSpan_EXIT boundary type for unknown boundary")
		return jobspb.ResolvedSpan_EXIT
	}
}

// Resolved will be non-nil if this is a resolved timestamp event (i.e. IsKV()
// returns false).
func (e *Event) Resolved() jobspb.ResolvedSpan {
	return jobspb.ResolvedSpan{
		Span:         e.ev.Checkpoint.Span,
		Timestamp:    e.ev.Checkpoint.ResolvedTS,
		BoundaryType: e.boundaryType(),
	}
}

// BackfillTimestamp overrides the timestamp of the schema that should be
// used to interpret this KV. If set and prevVal is provided, the previous
// timestamp will be used to interpret the previous value.
//
// If unset (zero-valued), the KV's timestamp will be used to interpret both
// of the current and previous values instead.
func (e *Event) BackfillTimestamp() hlc.Timestamp {
	return e.backfillTimestamp
}

// BufferAddTimestamp is the time this event came into  the buffer.
func (e *Event) BufferAddTimestamp() time.Time {
	return e.bufferAddTimestamp
}

// Timestamp returns the timestamp of the write if this is a KV event.
// If there is a non-zero BackfillTimestamp, that is returned.
// If this is a resolved timestamp event, the timestamp is the resolved
// timestamp.
func (e *Event) Timestamp() hlc.Timestamp {
	return e.getTimestamp(e.backfillTimestamp)
}

// MVCCTimestamp returns the Timestamp of the KV, ignoring the
// backfillTimestamp if present. This helps distinguish backfills from
// other events.
func (e *Event) MVCCTimestamp() hlc.Timestamp {
	return e.getTimestamp(hlc.Timestamp{})
}

func (e *Event) getTimestamp(backfillTS hlc.Timestamp) hlc.Timestamp {
	switch e.Type() {
	case TypeResolved:
		return e.ev.Checkpoint.ResolvedTS
	case TypeKV:
		if !backfillTS.IsEmpty() {
			return backfillTS
		}
		return e.ev.Val.Value.Timestamp
	case TypeFlush:
		return hlc.Timestamp{}
	default:
		log.Warningf(context.TODO(),
			"setting empty timestamp for unknown event type")
		return hlc.Timestamp{}
	}
}

// DetachAlloc detaches and returns allocation associated with this event.
func (e *Event) DetachAlloc() Alloc {
	a := e.alloc
	e.alloc.clear()
	return a
}

// String implements Stringer.
func (e *Event) String() string {
	switch {
	case e.et == TypeFlush:
		return "flush"
	case e.et == TypeKV:
		kv := e.KV()
		return fmt.Sprintf("%s@%s", roachpb.PrettyPrintKey(nil, kv.Key), kv.Value.Timestamp)
	default:
		r := e.Resolved()
		return fmt.Sprintf("resolved %s@%s (bt=%s)", r.Span, r.Timestamp, r.BoundaryType)
	}
}

func getTypeForBoundary(bt jobspb.ResolvedSpan_BoundaryType) Type {
	switch bt {
	case jobspb.ResolvedSpan_NONE:
		return resolvedNone
	case jobspb.ResolvedSpan_BACKFILL:
		return resolvedBackfill
	case jobspb.ResolvedSpan_RESTART:
		return resolvedRestart
	case jobspb.ResolvedSpan_EXIT:
		return resolvedExit
	default:
		panic("unknown boundary type")
	}
}

// MakeResolvedEvent returns resolved event constructed from existing RangeFeedEvent.
func MakeResolvedEvent(
	ev *kvpb.RangeFeedEvent, boundaryType jobspb.ResolvedSpan_BoundaryType,
) Event {
	if ev.Checkpoint == nil {
		panic("expected initialized RangeFeedCheckpoint")
	}
	return Event{ev: ev, et: getTypeForBoundary(boundaryType)}
}

// NewBackfillResolvedEvent returns new resolved event.  Method intended to be
// used during backfill.
func NewBackfillResolvedEvent(
	span roachpb.Span, ts hlc.Timestamp, boundaryType jobspb.ResolvedSpan_BoundaryType,
) Event {
	rfe := &kvpb.RangeFeedEvent{
		Checkpoint: &kvpb.RangeFeedCheckpoint{
			Span:       span,
			ResolvedTS: ts,
		},
	}
	return MakeResolvedEvent(rfe, boundaryType)
}

// MakeKVEvent returns KV event constructed from existing RangeFeedEvent.
func MakeKVEvent(ev *kvpb.RangeFeedEvent) Event {
	if ev.Val == nil {
		panic("expected initialized RangeFeedValue")
	}
	return Event{ev: ev, et: TypeKV}
}

// NewBackfillKVEvent returns new KV event constructed during the backfill.
// Method intended to be used during backfill.
func NewBackfillKVEvent(
	key []byte, ts hlc.Timestamp, val []byte, withDiff bool, backfillTS hlc.Timestamp,
) Event {
	rfe := &kvpb.RangeFeedEvent{
		Val: &kvpb.RangeFeedValue{
			Key: key,
			Value: roachpb.Value{
				RawBytes:  val,
				Timestamp: ts,
			},
		}}

	if withDiff {
		// Include the same value for the "before" and "after" KV, but
		// interpret them at different timestamp. Specifically, interpret
		// the "before" KV at the timestamp immediately before the schema
		// change.
		rfe.Val.PrevValue = rfe.Val.Value
	}
	return Event{
		ev:                rfe,
		et:                TypeKV,
		backfillTimestamp: backfillTS,
	}
}
