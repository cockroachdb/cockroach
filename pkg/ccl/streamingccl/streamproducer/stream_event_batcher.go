// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type streamEventBatcher struct {
	batch              streampb.StreamEvent_Batch
	size               int
	spanConfigFrontier hlc.Timestamp
}

func makeStreamEventBatcher() *streamEventBatcher {
	return &streamEventBatcher{
		batch: streampb.StreamEvent_Batch{},
	}
}

func (seb *streamEventBatcher) reset() {
	seb.size = 0
	seb.batch.KeyValues = seb.batch.KeyValues[:0]
	seb.batch.Ssts = seb.batch.Ssts[:0]
	seb.batch.DelRanges = seb.batch.DelRanges[:0]
	seb.batch.SpanConfigs = seb.batch.SpanConfigs[:0]
	seb.batch.SplitPoints = seb.batch.SplitPoints[:0]
}

func (seb *streamEventBatcher) addSST(sst kvpb.RangeFeedSSTable) {
	seb.batch.Ssts = append(seb.batch.Ssts, sst)
	seb.size += sst.Size()
}

func (seb *streamEventBatcher) addKV(kv roachpb.KeyValue) {
	seb.batch.KeyValues = append(seb.batch.KeyValues, kv)
	seb.size += kv.Size()
}

func (seb *streamEventBatcher) addDelRange(d kvpb.RangeFeedDeleteRange) {
	// DelRange's span is already trimmed to enclosed within
	// the subscribed span, just emit it.
	seb.batch.DelRanges = append(seb.batch.DelRanges, d)
	seb.size += d.Size()
}

func (seb *streamEventBatcher) addSplitPoint(k roachpb.Key) {
	seb.batch.SplitPoints = append(seb.batch.SplitPoints, k)
	seb.size += len(k)
}

// addSpanConfigs adds a slice of spanConfig entries that were recently flushed
// by the rangefeed cache. The function elides duplicate updates by checking
// that an update is newer than the tracked frontier and by checking for
// duplicates within the inputEvents. This function assumes that the input events are
// timestamp ordered and that the largest timestamp in the batch is the rangefeed frontier.
func (seb *streamEventBatcher) addSpanConfigs(
	inputEvents []streampb.StreamedSpanConfigEntry, inputFrontier hlc.Timestamp,
) {

	// eventSeenAtCurrentTimestamp is used to track unique events within the
	// inputEvents at the current timestamp.
	eventsSeenAtCurrentTimestamp := make(map[string]struct{})
	var currentTimestamp hlc.Timestamp

	for _, event := range inputEvents {
		if event.Timestamp.LessEq(seb.spanConfigFrontier) {
			// Any event with a timestamp at or below the rangefeed frontier is a duplicate.
			continue
		}
		if currentTimestamp.Less(event.Timestamp) {
			for key := range eventsSeenAtCurrentTimestamp {
				delete(eventsSeenAtCurrentTimestamp, key)
			}
			currentTimestamp = event.Timestamp
		}
		stringedEvent := event.String()
		if _, ok := eventsSeenAtCurrentTimestamp[stringedEvent]; !ok {
			eventsSeenAtCurrentTimestamp[stringedEvent] = struct{}{}
			seb.batch.SpanConfigs = append(seb.batch.SpanConfigs, event)
			seb.size += event.Size()
		}
	}
	seb.spanConfigFrontier = inputFrontier
}

func (seb *streamEventBatcher) getSize() int {
	return seb.size
}
