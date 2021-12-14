// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// runInitialScan will attempt to perform an initial data scan.
// It will retry in the face of errors and will only return upon
// success, context cancellation, or an error handling function which indicates
// that an error is unrecoverable. The return value will be true if the context
// was canceled or if the OnInitialScanError function indicated that the
// RangeFeed should stop.
func (f *RangeFeed) runInitialScan(
	ctx context.Context, n *log.EveryN, r *retry.Retry,
) (canceled bool) {
	onValue := func(kv roachpb.KeyValue) {
		v := roachpb.RangeFeedValue{
			Key:   kv.Key,
			Value: kv.Value,
		}

		// Mark the data as occurring at the initial timestamp, which is the
		// timestamp at which it was read.
		v.Value.Timestamp = f.initialTimestamp

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

	r.Reset()
	for r.Next() {
		if err := f.client.Scan(ctx, f.spans, f.initialTimestamp, onValue, f.scanConfig); err != nil {
			if f.onInitialScanError != nil {
				if shouldStop := f.onInitialScanError(ctx, err); shouldStop {
					log.VEventf(ctx, 1, "stopping due to error: %v", err)
					return true
				}
			}
			if n.ShouldLog() {
				log.Warningf(ctx, "failed to perform initial scan: %v", err)
			}
		} else /* err == nil */ {
			if f.onInitialScanDone != nil {
				f.onInitialScanDone(ctx)
			}
			break
		}
	}

	return false
}
