// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
	"github.com/jackc/pgx/v5"
)

func subscribeInternal(
	ctx context.Context,
	feed pgx.Rows,
	eventCh chan crosscluster.Event,
	closeCh chan struct{},
	compressed bool,
) error {
	// Get the next event from the cursor.
	var bufferedEvent *streampb.StreamEvent
	getNextEvent := func() (crosscluster.Event, error) {
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
		var decompressionErr error

		if compressed {
			decompressed, err := snappy.Decode(nil, data)
			if err != nil {
				// Maybe it just wasn't compressed by an older source node; proceed to
				// try to decode it as-is but then if that fails, return this error.
				decompressionErr = err
			} else {
				data = decompressed
			}
		}

		if err := protoutil.Unmarshal(data, &streamEvent); err != nil {
			if decompressionErr != nil {
				return nil, errors.Wrap(err, "decompression failed")
			}
			return nil, err
		}
		if streamEvent.Batch != nil && isEmptyBatch(streamEvent.Batch) {
			return nil, errors.New("unexpected empty batch in stream event (source cluster version may not be supported)")
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
		case eventCh <- event:
		case <-closeCh:
			// Exit quietly to not cause other subscriptions in the same
			// ctxgroup.Group to exit.
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// parseEvent parses next event from the batch of events inside streampb.StreamEvent.
func parseEvent(streamEvent *streampb.StreamEvent) crosscluster.Event {
	if streamEvent == nil {
		return nil
	}

	if streamEvent.Checkpoint != nil {
		event := crosscluster.MakeCheckpointEvent(streamEvent.Checkpoint)
		streamEvent.Checkpoint = nil
		return event
	}

	var event crosscluster.Event
	if streamEvent.Batch != nil {
		switch {
		case len(streamEvent.Batch.Ssts) > 0:
			event = crosscluster.MakeSSTableEvent(streamEvent.Batch.Ssts[0])
			streamEvent.Batch.Ssts = streamEvent.Batch.Ssts[1:]
		case len(streamEvent.Batch.DeprecatedKeyValues) > 0:
			event = crosscluster.MakeKVEventFromKVs(streamEvent.Batch.DeprecatedKeyValues)
			streamEvent.Batch.DeprecatedKeyValues = nil
		case len(streamEvent.Batch.KVs) > 0:
			event = crosscluster.MakeKVEvent(streamEvent.Batch.KVs)
			streamEvent.Batch.KVs = nil
		case len(streamEvent.Batch.DelRanges) > 0:
			event = crosscluster.MakeDeleteRangeEvent(streamEvent.Batch.DelRanges[0])
			streamEvent.Batch.DelRanges = streamEvent.Batch.DelRanges[1:]
		case len(streamEvent.Batch.SpanConfigs) > 0:
			event = crosscluster.MakeSpanConfigEvent(streamEvent.Batch.SpanConfigs[0])
			streamEvent.Batch.SpanConfigs = streamEvent.Batch.SpanConfigs[1:]
		case len(streamEvent.Batch.SplitPoints) > 0:
			event = crosscluster.MakeSplitEvent(streamEvent.Batch.SplitPoints[0])
			streamEvent.Batch.SplitPoints = streamEvent.Batch.SplitPoints[1:]
		}

		if isEmptyBatch(streamEvent.Batch) {
			streamEvent.Batch = nil
		}
	}
	return event
}

func isEmptyBatch(b *streampb.StreamEvent_Batch) bool {
	return len(b.KVs) == 0 &&
		len(b.DeprecatedKeyValues) == 0 &&
		len(b.Ssts) == 0 &&
		len(b.DelRanges) == 0 &&
		len(b.SpanConfigs) == 0 &&
		len(b.SplitPoints) == 0
}
