// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/jackc/pgx/v4"
)

func subscribeInternal(
	ctx context.Context, feed pgx.Rows, eventsChan chan streamingccl.Event, closeChan chan struct{},
) error {
	// Get the next event from the cursor.
	var bufferedEvent *streampb.StreamEvent
	getNextEvent := func() (streamingccl.Event, error) {
		if e := parseEvent(bufferedEvent); e != nil {
			return e, nil
		}

		if !feed.Next() {
			if err := feed.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		var data []byte
		if err := feed.Scan(&data); err != nil {
			return nil, err
		}
		var streamEvent streampb.StreamEvent
		if err := protoutil.Unmarshal(data, &streamEvent); err != nil {
			return nil, err
		}
		bufferedEvent = &streamEvent
		return parseEvent(bufferedEvent), nil
	}

	for {
		event, err := getNextEvent()
		if err != nil {
			return err
		}
		select {
		case eventsChan <- event:
		case <-closeChan:
			// Exit quietly to not cause other subscriptions in the same
			// ctxgroup.Group to exit.
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// parseEvent parses next event from the batch of events inside streampb.StreamEvent.
func parseEvent(streamEvent *streampb.StreamEvent) streamingccl.Event {
	if streamEvent == nil {
		return nil
	}

	if streamEvent.Checkpoint != nil {
		event := streamingccl.MakeCheckpointEvent(streamEvent.Checkpoint.ResolvedSpans)
		streamEvent.Checkpoint = nil
		return event
	}

	var event streamingccl.Event
	if streamEvent.Batch != nil {
		switch {
		case len(streamEvent.Batch.Ssts) > 0:
			event = streamingccl.MakeSSTableEvent(streamEvent.Batch.Ssts[0])
			streamEvent.Batch.Ssts = streamEvent.Batch.Ssts[1:]
		case len(streamEvent.Batch.KeyValues) > 0:
			event = streamingccl.MakeKVEvent(streamEvent.Batch.KeyValues[0])
			streamEvent.Batch.KeyValues = streamEvent.Batch.KeyValues[1:]
		case len(streamEvent.Batch.DelRanges) > 0:
			event = streamingccl.MakeDeleteRangeEvent(streamEvent.Batch.DelRanges[0])
			streamEvent.Batch.DelRanges = streamEvent.Batch.DelRanges[1:]
		case len(streamEvent.Batch.SpanConfigs) > 0:
			event = streamingccl.MakeSpanConfigEvent(streamEvent.Batch.SpanConfigs[0])
			streamEvent.Batch.SpanConfigs = streamEvent.Batch.SpanConfigs[1:]
		}

		if len(streamEvent.Batch.KeyValues) == 0 &&
			len(streamEvent.Batch.Ssts) == 0 &&
			len(streamEvent.Batch.DelRanges) == 0 &&
			len(streamEvent.Batch.SpanConfigs) == 0 {
			streamEvent.Batch = nil
		}
	}
	return event
}
