// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// YYY: This is nearly identical to the KV feed events we emit. These events
// will most likely eventually be protobufs to serialize the entire event over
// the stream connection.

// EventType enumerates all possible events emitted over a cluster stream.
type EventType int

const (
	// KVEvent indicates that the KV field of an event holds an updated KV which
	// needs to be ingested.
	KVEvent EventType = iota
	// ResolvedEvent indicates that the Resolved field of an event holds a
	// timestamp indicating that all events for this partition up to this
	// timestamp have been emitted.
	ResolvedEvent
)

// Event describes an event emitted by a cluster to cluster stream.
// Its EventType field indicates which other fields are meaningful.
type Event struct {
	// EventType indicates which fields on the event will be meaningful.
	EventType EventType

	// KV is populated when eventType is KVEvent.
	KV roachpb.KeyValue
	// Resolved is populated when eventType is ResolvedEvent.
	Resolved time.Time
}

// MakeKVEvent creates a KV typed Event.
func MakeKVEvent(kv roachpb.KeyValue) Event {
	return Event{
		EventType: KVEvent,
		KV:        kv,
	}
}

// MakeResolvedEvent makes a resolved timestamp Event.
func MakeResolvedEvent(resolvedTimestamp time.Time) Event {
	return Event{
		EventType: ResolvedEvent,
		Resolved:  resolvedTimestamp,
	}
}
