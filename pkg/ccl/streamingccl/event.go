// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// EventType enumerates all possible events emitted over a cluster stream.
type EventType int

const (
	// KVEvent indicates that the KV field of an event holds an updated KV which
	// needs to be ingested.
	KVEvent EventType = iota
	// SSTableEvent indicates that the SSTable field of an event holds an updated
	// SSTable which needs to be ingested.
	SSTableEvent
	// DeleteRangeEvent indicates that the DeleteRange field of an event holds a
	// DeleteRange which needs to be ingested.
	DeleteRangeEvent
	// CheckpointEvent indicates that GetResolvedSpans will be meaningful. The resolved
	// timestamp indicates that all KVs have been emitted up to this timestamp.
	CheckpointEvent
)

// Event describes an event emitted by a cluster to cluster stream.  Its Type
// field indicates which other fields are meaningful.
// TODO(casper): refactor this to use a protobuf message type that has one of
// union of event types below.
type Event interface {
	// Type specifies which accessor will be meaningful.
	Type() EventType

	// GetKV returns a KV event if the EventType is KVEvent.
	GetKV() *roachpb.KeyValue

	// GetSSTable returns a AddSSTable event if the EventType is SSTableEvent.
	GetSSTable() *roachpb.RangeFeedSSTable

	// GetDeleteRange returns a DeleteRange event if the EventType is DeleteRangeEvent.
	GetDeleteRange() *roachpb.RangeFeedDeleteRange

	// GetResolvedSpans returns a list of span-time pairs indicating the time for
	// which all KV events within that span has been emitted.
	GetResolvedSpans() []jobspb.ResolvedSpan
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

// GetSSTable implements the Event interface.
func (kve kvEvent) GetSSTable() *roachpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (kve kvEvent) GetDeleteRange() *roachpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (kve kvEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// sstableEvent is a sstable that needs to be ingested.
type sstableEvent struct {
	sst roachpb.RangeFeedSSTable
}

// Type implements the Event interface.
func (sste sstableEvent) Type() EventType {
	return SSTableEvent
}

// GetKV implements the Event interface.
func (sste sstableEvent) GetKV() *roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (sste sstableEvent) GetSSTable() *roachpb.RangeFeedSSTable {
	return &sste.sst
}

// GetDeleteRange implements the Event interface.
func (sste sstableEvent) GetDeleteRange() *roachpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (sste sstableEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

var _ Event = sstableEvent{}

// delRangeEvent is a DeleteRange event that needs to be ingested.
type delRangeEvent struct {
	delRange roachpb.RangeFeedDeleteRange
}

// Type implements the Event interface.
func (dre delRangeEvent) Type() EventType {
	return DeleteRangeEvent
}

// GetKV implements the Event interface.
func (dre delRangeEvent) GetKV() *roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (dre delRangeEvent) GetSSTable() *roachpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (dre delRangeEvent) GetDeleteRange() *roachpb.RangeFeedDeleteRange {
	return &dre.delRange
}

// GetResolvedSpans implements the Event interface.
func (dre delRangeEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

var _ Event = delRangeEvent{}

// checkpointEvent indicates that the stream has emitted every change for all
// keys in the span it is responsible for up until this timestamp.
type checkpointEvent struct {
	resolvedSpans []jobspb.ResolvedSpan
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

// GetSSTable implements the Event interface.
func (ce checkpointEvent) GetSSTable() *roachpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (ce checkpointEvent) GetDeleteRange() *roachpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (ce checkpointEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return ce.resolvedSpans
}

// MakeKVEvent creates an Event from a KV.
func MakeKVEvent(kv roachpb.KeyValue) Event {
	return kvEvent{kv: kv}
}

// MakeSSTableEvent creates an Event from a SSTable.
func MakeSSTableEvent(sst roachpb.RangeFeedSSTable) Event {
	return sstableEvent{sst: sst}
}

// MakeDeleteRangeEvent creates an Event from a DeleteRange.
func MakeDeleteRangeEvent(delRange roachpb.RangeFeedDeleteRange) Event {
	return delRangeEvent{delRange: delRange}
}

// MakeCheckpointEvent creates an Event from a resolved timestamp.
func MakeCheckpointEvent(resolvedSpans []jobspb.ResolvedSpan) Event {
	return checkpointEvent{resolvedSpans: resolvedSpans}
}
