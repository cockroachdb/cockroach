// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package streamingccl

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"time"
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
