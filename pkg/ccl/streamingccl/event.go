// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	GetKVs() []roachpb.KeyValue

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
	kv []roachpb.KeyValue
}

var _ Event = kvEvent{}

// Type implements the Event interface.
func (kve kvEvent) Type() EventType {
	return KVEvent
}

// GetKVs implements the Event interface.
func (kve kvEvent) GetKVs() []roachpb.KeyValue {
	return kve.kv
}

// GetSSTable implements the Event interface.
func (kve kvEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (kve kvEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (kve kvEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// GetSpanConfigEvent implements the Event interface.
func (kve kvEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return nil
}

// GetSplitEvent implements the Event interface.
func (kve kvEvent) GetSplitEvent() *roachpb.Key {
	return nil
}

// sstableEvent is a sstable that needs to be ingested.
type sstableEvent struct {
	sst kvpb.RangeFeedSSTable
}

// Type implements the Event interface.
func (sste sstableEvent) Type() EventType {
	return SSTableEvent
}

// GetKVs implements the Event interface.
func (sste sstableEvent) GetKVs() []roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (sste sstableEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return &sste.sst
}

// GetDeleteRange implements the Event interface.
func (sste sstableEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (sste sstableEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// GetSpanConfigEvent implements the Event interface.
func (sste sstableEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return nil
}

// GetSplitEvent implements the Event interface.
func (sste sstableEvent) GetSplitEvent() *roachpb.Key {
	return nil
}

var _ Event = sstableEvent{}

// delRangeEvent is a DeleteRange event that needs to be ingested.
type delRangeEvent struct {
	delRange kvpb.RangeFeedDeleteRange
}

// Type implements the Event interface.
func (dre delRangeEvent) Type() EventType {
	return DeleteRangeEvent
}

// GetKVs implements the Event interface.
func (dre delRangeEvent) GetKVs() []roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (dre delRangeEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (dre delRangeEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return &dre.delRange
}

// GetResolvedSpans implements the Event interface.
func (dre delRangeEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// GetSpanConfigEvent implements the Event interface.
func (dre delRangeEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return nil
}

// GetSplitEvent implements the Event interface.
func (dre delRangeEvent) GetSplitEvent() *roachpb.Key {
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

// GetKVs implements the Event interface.
func (ce checkpointEvent) GetKVs() []roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (ce checkpointEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (ce checkpointEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (ce checkpointEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return ce.resolvedSpans
}

// GetSpanConfigEvent implements the Event interface.
func (ce checkpointEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return nil
}

// GetSplitEvent implements the Event interface.
func (ce checkpointEvent) GetSplitEvent() *roachpb.Key {
	return nil
}

type spanConfigEvent struct {
	spanConfig streampb.StreamedSpanConfigEntry
}

var _ Event = spanConfigEvent{}

// Type implements the Event interface.
func (spe spanConfigEvent) Type() EventType {
	return SpanConfigEvent
}

// GetKVs implements the Event interface.
func (spe spanConfigEvent) GetKVs() []roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (spe spanConfigEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (spe spanConfigEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (spe spanConfigEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// GetSpanConfigEvent implements the Event interface.
func (spe spanConfigEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return &spe.spanConfig
}

// GetSplitEvent implements the Event interface.
func (spe spanConfigEvent) GetSplitEvent() *roachpb.Key {
	return nil
}

type splitEvent struct {
	splitKey roachpb.Key
}

var _ Event = splitEvent{}

// Type implements the Event interface.
func (se splitEvent) Type() EventType {
	return SplitEvent
}

// GetKV implements the Event interface.
func (se splitEvent) GetKVs() []roachpb.KeyValue {
	return nil
}

// GetSSTable implements the Event interface.
func (se splitEvent) GetSSTable() *kvpb.RangeFeedSSTable {
	return nil
}

// GetDeleteRange implements the Event interface.
func (se splitEvent) GetDeleteRange() *kvpb.RangeFeedDeleteRange {
	return nil
}

// GetResolvedSpans implements the Event interface.
func (se splitEvent) GetResolvedSpans() []jobspb.ResolvedSpan {
	return nil
}

// GetSpanConfigEvent implements the Event interface.
func (se splitEvent) GetSpanConfigEvent() *streampb.StreamedSpanConfigEntry {
	return nil
}

// GetSplitEvent implements the Event interface.
func (se splitEvent) GetSplitEvent() *roachpb.Key {
	return &se.splitKey
}

// MakeKVEvent creates an Event from a KV.
func MakeKVEvent(kv []roachpb.KeyValue) Event {
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
