// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// EventType enumerates all possible events emitted over a cluster stream.
type EventType int

const (
	// KVEvent indicates that the KV field of an event holds an updated KV which
	// needs to be ingested.
	KVEvent EventType = iota
	// CheckpointEvent indicates that GetResolved will be meaningful. The resolved
	// timestamp indicates that all KVs have been emitted up to this timestamp.
	CheckpointEvent
)

// Event describes an event emitted by a cluster to cluster stream.  Its Type
// field indicates which other fields are meaningful.
type Event interface {
	// Type specifies which accessor will be meaningful.
	Type() EventType

	// GetKV returns a KV event if the EventType is KVEvent.
	GetKV() *roachpb.KeyValue
	// GetResolved returns a resolved timestamp if the EventType is
	// CheckpointEvent. The resolved timestamp indicates that all KV events until
	// this time have been emitted.
	GetResolved() *time.Time
}

// kvEvent is a key value pair that needs to be ingested.
type kvEvent struct {
	kv roachpb.KeyValue
}

var _ Event = kvEvent{}

// Type implements the Event interface.
func (kve kvEvent) Type() EventType {
	return KVEvent
}

// GetKV implements the Event interface.
func (kve kvEvent) GetKV() *roachpb.KeyValue {
	return &kve.kv
}

// GetResolved implements the Event interface.
func (kve kvEvent) GetResolved() *time.Time {
	return nil
}

// checkpointEvent indicates that the stream has emitted every change for all
// keys in the span it is responsible for up until this timestamp.
type checkpointEvent struct {
	resolvedTimestamp time.Time
}

var _ Event = checkpointEvent{}

// Type implements the Event interface.
func (ce checkpointEvent) Type() EventType {
	return CheckpointEvent
}

// GetKV implements the Event interface.
func (ce checkpointEvent) GetKV() *roachpb.KeyValue {
	return nil
}

// GetResolved implements the Event interface.
func (ce checkpointEvent) GetResolved() *time.Time {
	return &ce.resolvedTimestamp
}

// MakeKVEvent creates an Event from a KV.
func MakeKVEvent(kv roachpb.KeyValue) Event {
	return kvEvent{kv: kv}
}

// MakeCheckpointEvent creates an Event from a resolved timestamp.
func MakeCheckpointEvent(resolvedTimestamp time.Time) Event {
	return checkpointEvent{resolvedTimestamp: resolvedTimestamp}
}
