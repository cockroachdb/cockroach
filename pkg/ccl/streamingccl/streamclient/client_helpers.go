// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamclient

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/codahale/hdrhistogram"
	"github.com/jackc/pgx/v4"
)

func subscribeInternal(
	ctx context.Context, feed pgx.Rows, eventsChan chan streamingccl.Event, closeChan chan struct{},
) error {
	ctx, sp := tracing.ChildSpan(ctx, "streamclient.subscribeInternal")
	if sp != nil {
		defer sp.Finish()
	}

	const (
		sigFigs    = 1
		minLatency = time.Microsecond
		maxLatency = 100 * time.Second
	)
	receiveHistogram := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), sigFigs)
	subscriptionStats := &StreamSubscriptionStats{}
	// Get the next event from the cursor.
	var bufferedEvent *streampb.StreamEvent
	getNextEvent := func() (streamingccl.Event, error) {
		if e := parseEvent(bufferedEvent); e != nil {
			return e, nil
		}

		beforeNext := timeutil.Now()
		if !feed.Next() {
			if err := feed.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		timeSinceNext := timeutil.Since(beforeNext)
		subscriptionStats.StreamEventReceiveWait += timeSinceNext
		if err := receiveHistogram.RecordValue(timeSinceNext.Nanoseconds()); err != nil {
			log.Warningf(ctx, "failed to record value in histogram: %v", err)
		}
		var data []byte
		if err := feed.Scan(&data); err != nil {
			return nil, err
		}
		var streamEvent streampb.StreamEvent
		if err := protoutil.Unmarshal(data, &streamEvent); err != nil {
			return nil, err
		}
		if streamEvent.Batch != nil {
			subscriptionStats.RecvdBatches++
		} else {
			subscriptionStats.RecvdCheckpoints++
		}
		bufferedEvent = &streamEvent
		return parseEvent(bufferedEvent), nil
	}

	var lastAggregatorStatsEmitted time.Time
	for {
		event, err := getNextEvent()
		if err != nil {
			return err
		}
		if timeutil.Since(lastAggregatorStatsEmitted) > 10*time.Second {
			lastAggregatorStatsEmitted = timeutil.Now()
			if sp != nil {
				subscriptionStats.ReceiveWait = &HistogramData{
					Min:   receiveHistogram.Min(),
					P5:    receiveHistogram.ValueAtQuantile(5),
					P50:   receiveHistogram.ValueAtQuantile(50),
					P90:   receiveHistogram.ValueAtQuantile(90),
					P99:   receiveHistogram.ValueAtQuantile(99),
					P99_9: receiveHistogram.ValueAtQuantile(99.9),
					Max:   receiveHistogram.Max(),
					Mean:  float32(receiveHistogram.Mean()),
					Count: receiveHistogram.TotalCount(),
				}
				sp.RecordStructured(subscriptionStats)
				subscriptionStats = &StreamSubscriptionStats{}
			}
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
