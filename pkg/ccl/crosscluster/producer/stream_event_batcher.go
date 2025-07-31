// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

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
	wrappedKVs         bool
}

func makeStreamEventBatcher(supportsWrapping bool) *streamEventBatcher {
	return &streamEventBatcher{wrappedKVs: supportsWrapping}
}

func (seb *streamEventBatcher) reset() {
	seb.size = 0
	seb.batch.KVs = seb.batch.KVs[:0]
	seb.batch.Ssts = seb.batch.Ssts[:0]
	seb.batch.DelRanges = seb.batch.DelRanges[:0]
	seb.batch.SpanConfigs = seb.batch.SpanConfigs[:0]
	seb.batch.SplitPoints = seb.batch.SplitPoints[:0]
}

func (seb *streamEventBatcher) addSST(sst kvpb.RangeFeedSSTable) {
	seb.batch.Ssts = append(seb.batch.Ssts, sst)
	seb.size += sst.Size()
}

func (seb *streamEventBatcher) addKV(kv streampb.StreamEvent_KV) {
	if seb.wrappedKVs {
		seb.batch.KVs = append(seb.batch.KVs, kv)
		seb.size += (kv.Size())
	} else {
		seb.batch.DeprecatedKeyValues = append(seb.batch.DeprecatedKeyValues, kv.KeyValue)
		seb.size += (kv.Size())
	}
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
