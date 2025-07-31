// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// runInitialScan will attempt to perform an initial data scan of all spans in
// the passed frontier that are below f.initialTimestamp.
//
// It will retry in the face of errors and will only return upon
// success, context cancellation, or an error handling function which indicates
// that an error is unrecoverable. The return value will be true if the context
// was canceled or if the OnInitialScanError function indicated that the
// RangeFeed should stop.
func (f *RangeFeed) runInitialScan(
	ctx context.Context, n *log.EveryN, r *retry.Retry, frontier span.Frontier,
) (canceled bool) {
	onValue := func(kv roachpb.KeyValue) {
		v := kvpb.RangeFeedValue{
			Key:   kv.Key,
			Value: kv.Value,
		}

		// Mark the data as occurring at the initial timestamp, which is the
		// timestamp at which it was read.
		if !f.useRowTimestampInInitialScan {
			v.Value.Timestamp = f.initialTimestamp
		}

		// Supply the value from the scan as also the previous value to avoid
		// indicating that the value was previously deleted.
		if f.withDiff {
			v.PrevValue = v.Value
			v.PrevValue.Timestamp = hlc.Timestamp{}
		}

		// It's something of a bummer that we must allocate a new value for each
		// of these but the contract doesn't indicate that the value cannot be
		// retained so we have to assume that the callback may retain the value.
		f.onValue(ctx, &v)
	}

	var onValues func(kvs []kv.KeyValue)
	if f.onValues != nil {
		onValues = func(kvs []kv.KeyValue) {
			f.onValues(ctx, kvs)
		}
	}

	// Ensure the frontier is compatible with scan parallelism, which could be
	// enabled mid-call since it is controlled via callback.
	frontier = span.MakeConcurrentFrontier(frontier)

	// Adjust the span completion callback to advance the frontier in addition to
	// calling the user's callback, if any. We don't need to undo after we're done
	// since it is never called again once we're done.
	userSpanDoneCallback := f.scanConfig.OnSpanDone
	f.scanConfig.OnSpanDone = func(ctx context.Context, sp roachpb.Span) error {
		if userSpanDoneCallback != nil {
			if err := userSpanDoneCallback(ctx, sp); err != nil {
				return err
			}
		}
		advanced, err := frontier.Forward(sp, f.initialTimestamp)
		if err != nil {
			return err
		}
		if f.frontierVisitor != nil {
			f.frontierVisitor(ctx, advanced, frontier)
		}
		return nil
	}

	toScan := make(roachpb.Spans, 0, frontier.Len())

	// We will exit this for loop only when we return false for failed/cancelled
	// from inside the loop, or when we break due to the callback or the retry
	// helper returns false for Next (ctx cancel/retry limit).
	r.Reset()
	for r.Next() {
		// Figure out what spans are left to scan.
		toScan = toScan[:0]
		frontier.Entries(func(sp roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
			if ts.IsEmpty() || ts.Less(f.initialTimestamp) {
				toScan = append(toScan, sp)
			}
			return span.ContinueMatch
		})

		// Scan the spans.
		if len(toScan) > 0 {
			if err := f.client.Scan(ctx, toScan, f.initialTimestamp, onValue, onValues, f.scanConfig); err != nil {
				if f.onInitialScanError != nil {
					if shouldStop := f.onInitialScanError(ctx, err); shouldStop {
						log.VEventf(ctx, 1, "stopping due to error: %v", err)
						break
					}
				}
				if n.ShouldLog() {
					log.Warningf(ctx, "failed to perform initial scan: %v", err)
				}
				continue
			}
		}

		// If we got here, we either had nothing to do or did it all; we're done.
		if f.onInitialScanDone != nil {
			f.onInitialScanDone(ctx)
		}
		return false
	}

	// We left the for loop without returning false, so we were cancelled.
	return true
}
