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
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
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
	// SpanConfigEvent indicates that the SpanConfig field of an event holds an updated
	// SpanConfigRecord.
	SpanConfigEvent
	// SplitEvent indicates that the SplitKey field of an event holds a split key.
	SplitEvent
)

// Event describes an event emitted by a cluster to cluster stream.  Its Type
// field indicates which other fields are meaningful.
// TODO(casper): refactor this to use a protobuf message type that has one of
// union of event types below.
type Event interface {
	// Type specifies which accessor will be meaningful.
	Type() EventType

	// GetKVs returns a KV event if the EventType is KVEvent.
	GetKVs() []streampb.StreamEvent_KV

	// GetSSTable returns a AddSSTable event if the EventType is SSTableEvent.
	GetSSTable() *kvpb.RangeFeedSSTable

	// GetDeleteRange returns a DeleteRange event if the EventType is DeleteRangeEvent.
	GetDeleteRange() *kvpb.RangeFeedDeleteRange

	// GetResolvedSpans returns a list of span-time pairs indicating the time for
	// which all KV events within that span has been emitted.
	GetResolvedSpans() []jobspb.ResolvedSpan

	// GetSpanConfigEvent returns a SpanConfig event if the EventType is SpanConfigEvent
	GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry

	// GetSplitEvent returns the split event if the EventType is a SplitEvent
	GetSplitEvent() *roachpb.Key
}

// kvEvent is a key value pair that needs to be ingested.
type kvEvent struct {
	emptyEvent
	kv []streampb.StreamEvent_KV
}

var _ Event = kvEvent{}

// Type implements the Event interface.
func (kve kvEvent) Type() EventType {
	return KVEvent
}

// GetKVs implements the Event interface.
func (kve kvEvent) GetKVs() []streampb.StreamEvent_KV {
	return kve.kv
}

// sstableEvent is a sstable that needs to be ingested.
type sstableEvent struct {
	emptyEvent
	sst kvpb.RangeFeedSSTable
}

// Type implements the Event interface.
func (sste sstableEvent) Type() EventType {
	return SSTableEvent
}

// GetSSTable implements the Event interface.
func (sste sstableEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return &sste.sst
}

var _ Event = sstableEvent{}

// delRangeEvent is a DeleteRange event that needs to be ingested.
type delRangeEvent struct {
	emptyEvent
	delRange kvpb.RangeFeedDeleteRange
}

// Type implements the Event interface.
func (dre delRangeEvent) Type() EventType {
	return DeleteRangeEvent
}

// GetDeleteRange implements the Event interface.
func (dre delRangeEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return &dre.delRange
}

var _ Event = delRangeEvent{}

// checkpointEvent indicates that the stream has emitted every change for all
// keys in the span it is responsible for up until this timestamp.
type checkpointEvent struct {
	emptyEvent
	resolvedSpans []jobspb.ResolvedSpan
}

var _ Event = checkpointEvent{}

// Type implements the Event interface.
func (ce checkpointEvent) Type() EventType {
	return CheckpointEvent
}

// GetResolvedSpans implements the Event interface.
func (ce checkpointEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return ce.resolvedSpans
}

type spanConfigEvent struct {
	emptyEvent
	spanConfig streampb.StreamedSpanConfigEntry
}

var _ Event = spanConfigEvent{}

// Type implements the Event interface.
func (spe spanConfigEvent) Type() EventType {
	return SpanConfigEvent
}

// GetSpanConfigEvent implements the Event interface.
func (spe spanConfigEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return &spe.spanConfig
}

type splitEvent struct {
	emptyEvent
	splitKey roachpb.Key
}

var _ Event = splitEvent{}

// Type implements the Event interface.
func (se splitEvent) Type() EventType {
	return SplitEvent
}

// GetSplitEvent implements the Event interface.
func (se splitEvent) GetSplitEvent() *roachpb.Key {
	return &se.splitKey
}

// MakeKVEvent creates an Event from a KV.
func MakeKVEventFromKVs(kv []roachpb.KeyValue) Event {
	kvs := make([]streampb.StreamEvent_KV, len(kv))
	for i := range kv {
		kvs[i].KeyValue = kv[i]
	}
	return kvEvent{kv: kvs}
}

// MakeKVEvent creates an Event from a KV.
func MakeKVEvent(kv []streampb.StreamEvent_KV) Event {
	return kvEvent{kv: kv}
}

// MakeSSTableEvent creates an Event from a SSTable.
func MakeSSTableEvent(sst kvpb.RangeFeedSSTable) Event {
	return sstableEvent{sst: sst}
}

// MakeDeleteRangeEvent creates an Event from a DeleteRange.
func MakeDeleteRangeEvent(delRange kvpb.RangeFeedDeleteRange) Event {
	return delRangeEvent{delRange: delRange}
}

// MakeCheckpointEvent creates an Event from a resolved timestamp.
func MakeCheckpointEvent(resolvedSpans []jobspb.ResolvedSpan) Event {
	return checkpointEvent{resolvedSpans: resolvedSpans}
}

func MakeSpanConfigEvent(streamedSpanConfig streampb.StreamedSpanConfigEntry) Event {
	return spanConfigEvent{spanConfig: streamedSpanConfig}
}

func MakeSplitEvent(splitKey roachpb.Key) Event {
	return splitEvent{splitKey: splitKey}
}

// emptyEvent is not an event (no Type method) but it is used to
// reduce the boilerplate above.
type emptyEvent struct{}

// GetKVs implements the Event interface.
func (ee emptyEvent) GetKVs() []streampb.StreamEvent_KV {
	return nil
}

// GetSSTable implements the Event interface.
func (ee emptyEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (ee emptyEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (ee emptyEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// GetSpanConfigEvent implements the Event interface.
func (ee emptyEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return nil
}

// GetSplitEvent implements the Event interface.
func (ee emptyEvent) GetSplitEvent() *roachpb.Key {
	return nil
}
