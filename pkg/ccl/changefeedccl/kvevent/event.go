// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package kvevent defines kvfeed events and buffers to communicate them
// locally.
package kvevent

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ErrBufferClosed is returned by Readers when no more values will be
// returned from the buffer.
var ErrBufferClosed = errors.New("buffer closed")

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
	Drain(ctx context.Context) error
	// Close closes this writer.
	Close(ctx context.Context) error
}

// Type indicates the type of the event.
// Different types indicate which methods will be meaningful.
// Events are implemented this way rather than as an interface to remove the
// need to box the events and allow for events to be used in slices directly.
type Type int

const (
	// TypeKV indicates that the KV, PrevValue, and BackfillTimestamp methods
	// on the Event meaningful.
	TypeKV Type = iota

	// TypeResolved indicates that the Resolved method on the Event will be
	// meaningful.
	TypeResolved

	// TypeUnknown indicates the event could not be parsed. Will fail the feed.
	TypeUnknown
)

// Event represents an event emitted by a kvfeed. It is either a KV or a
// resolved timestamp.
type Event struct {
	kv                 roachpb.KeyValue
	prevVal            roachpb.Value
	resolved           *jobspb.ResolvedSpan
	backfillTimestamp  hlc.Timestamp
	bufferGetTimestamp time.Time
	approxSize         int
	alloc              Alloc
}

// Type returns the event's Type.
func (b *Event) Type() Type {
	if b.kv.Key != nil {
		return TypeKV
	}
	if b.resolved != nil {
		return TypeResolved
	}
	return TypeUnknown
}

// ApproximateSize returns events approximate size in bytes.
func (b *Event) ApproximateSize() int {
	return b.approxSize
}

// KV is populated if this event returns true for IsKV().
func (b *Event) KV() roachpb.KeyValue {
	return b.kv
}

// PrevValue returns the previous value for this event. PrevValue is non-zero
// if this is a KV event and the key had a non-tombstone value before the change
// and the before value of each change was requested (optDiff).
func (b *Event) PrevValue() roachpb.Value {
	return b.prevVal
}

// Resolved will be non-nil if this is a resolved timestamp event (i.e. IsKV()
// returns false).
func (b *Event) Resolved() *jobspb.ResolvedSpan {
	return b.resolved
}

// BackfillTimestamp overrides the timestamp of the schema that should be
// used to interpret this KV. If set and prevVal is provided, the previous
// timestamp will be used to interpret the previous value.
//
// If unset (zero-valued), the KV's timestamp will be used to interpret both
// of the current and previous values instead.
func (b *Event) BackfillTimestamp() hlc.Timestamp {
	return b.backfillTimestamp
}

// BufferGetTimestamp is the time this event came out of the buffer.
func (b *Event) BufferGetTimestamp() time.Time {
	return b.bufferGetTimestamp
}

// Timestamp returns the timestamp of the write if this is a KV event.
// If there is a non-zero BackfillTimestamp, that is returned.
// If this is a resolved timestamp event, the timestamp is the resolved
// timestamp.
func (b *Event) Timestamp() hlc.Timestamp {
	switch b.Type() {
	case TypeResolved:
		return b.resolved.Timestamp
	case TypeKV:
		if !b.backfillTimestamp.IsEmpty() {
			return b.backfillTimestamp
		}
		return b.kv.Value.Timestamp
	default:
		log.Warningf(context.TODO(),
			"setting empty timestamp for unknown event type")
		return hlc.Timestamp{}
	}
}

// MVCCTimestamp returns the Timestamp of the KV, ignoring the
// backfillTimestamp if present. This helps distinguish backfills from
// other events.
func (b *Event) MVCCTimestamp() hlc.Timestamp {
	switch b.Type() {
	case TypeResolved:
		return b.resolved.Timestamp
	case TypeKV:
		return b.kv.Value.Timestamp
	default:
		log.Warningf(context.TODO(),
			"setting empty timestamp for unknown event type")
		return hlc.Timestamp{}
	}
}

// DetachAlloc detaches and returns allocation associated with this event.
func (b *Event) DetachAlloc() Alloc {
	a := b.alloc
	b.alloc.entries = 0
	b.alloc.bytes = 0
	return a
}

// MakeResolvedEvent returns resolved event.
func MakeResolvedEvent(
	span roachpb.Span, ts hlc.Timestamp, boundaryType jobspb.ResolvedSpan_BoundaryType,
) Event {
	return Event{
		resolved: &jobspb.ResolvedSpan{
			Span:         span,
			Timestamp:    ts,
			BoundaryType: boundaryType,
		},
		approxSize: span.Size() + ts.Size() + 4,
	}
}

// MakeKVEvent returns KV event.
func MakeKVEvent(
	kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) Event {
	return Event{
		kv:                kv,
		prevVal:           prevVal,
		backfillTimestamp: backfillTimestamp,
		approxSize:        kv.Size() + prevVal.Size() + backfillTimestamp.Size(),
	}
}
