// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

var backpressureLogLimiter = log.Every(500 * time.Millisecond)

// backpressureRangeSizeMultiplier is the multiple of range_max_bytes that a
// range's size must grow to before backpressure will be applied on writes. Set
// to 0 to disable backpressure altogether.
var backpressureRangeSizeMultiplier = settings.RegisterValidatedFloatSetting(
	"kv.range.backpressure_range_size_multiplier",
	"multiple of range_max_bytes that a range is allowed to grow to without "+
		"splitting before writes to that range are blocked, or 0 to disable",
	2.0,
	func(v float64) error {
		if v != 0 && v < 1 {
			return errors.Errorf("backpressure multiplier cannot be smaller than 1: %f", v)
		}
		return nil
	},
)

// backpressurableReqMethods is the set of all request methods that can
// be backpressured. If a batch contains any method outside of this set,
// it will not be backpressured.
var backpressurableReqMethods = util.MakeFastIntSet(
	int(roachpb.BeginTransaction),
	int(roachpb.EndTransaction),
	int(roachpb.Put),
	int(roachpb.InitPut),
	int(roachpb.ConditionalPut),
	int(roachpb.Merge),
	int(roachpb.Increment),
	int(roachpb.Delete),
	int(roachpb.DeleteRange),
	int(roachpb.ClearRange),
)

// backpressurableSpans contains spans of keys where write backpressuring
// is permitted. Writes to any keys outside of these spans will never be
// backpressured.
var backpressurableSpans = []roachpb.Span{
	{Key: keys.TimeseriesPrefix, EndKey: keys.TimeseriesKeyMax},
	{Key: keys.TableDataMin, EndKey: keys.TableDataMax},
}

// canBackpressureBatch returns whether the provided BatchRequest is eligible
// for backpressure.
func canBackpressureBatch(ba roachpb.BatchRequest) bool {
	// Don't backpressure splits themselves.
	if ba.Txn != nil && ba.Txn.Name == splitTxnName {
		return false
	}

	// Only backpressure batches consisting exclusively of "backpressurable"
	// methods that are all within "backpressurable" key spans.
	for _, union := range ba.Requests {
		req := union.GetInner()
		if !backpressurableReqMethods.Contains(int(req.Method())) {
			return false
		}

		inSpan := false
		for _, s := range backpressurableSpans {
			if s.Contains(req.Header()) {
				inSpan = true
				break
			}
		}
		if !inSpan {
			return false
		}
	}
	return true
}

// shouldBackpressureWrites returns whether writes to the range should be
// subject to backpressure. This is based on the size of the range in
// relation to the split size. The method returns true if the range is more
// than backpressureRangeSizeMultiplier times larger than the split size.
func (r *Replica) shouldBackpressureWrites() bool {
	mult := backpressureRangeSizeMultiplier.Get(&r.store.cfg.Settings.SV)
	if mult == 0 {
		// Disabled.
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.exceedsMultipleOfSplitSizeRLocked(mult)
}

// maybeBackpressureWriteBatch blocks to apply backpressure if the replica
// deems that backpressure is necessary.
func (r *Replica) maybeBackpressureWriteBatch(ctx context.Context, ba roachpb.BatchRequest) error {
	if !canBackpressureBatch(ba) {
		return nil
	}

	// If we need to apply backpressure, wait for an ongoing split to finish
	// if one exists. This does not place a hard upper bound on the size of
	// a range because we don't track all in-flight requests (like we do for
	// the quota pool), but it does create an effective soft upper bound.
	for first := true; r.shouldBackpressureWrites(); first = false {
		if first {
			r.store.metrics.BackpressuredOnSplitRequests.Inc(1)
			defer r.store.metrics.BackpressuredOnSplitRequests.Dec(1)

			if backpressureLogLimiter.ShouldLog() {
				log.Warningf(ctx, "applying backpressure to limit range growth on batch %s", ba)
			}
		}

		// Register a callback on an ongoing split for this range in the splitQueue.
		splitC := make(chan error, 1)
		if !r.store.splitQueue.MaybeAddCallback(r.RangeID, func(err error) {
			splitC <- err
		}) {
			// No split ongoing. We may have raced with its completion.
			return nil
		}

		// Wait for the callback to be called.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-splitC:
			if err != nil {
				return errors.Wrap(err, "waiting on split that failed")
			}
		}
	}
	return nil
}
