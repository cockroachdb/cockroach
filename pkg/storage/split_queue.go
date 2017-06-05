// Copyright 2015 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

const (
	// splitQueueTimerDuration is the duration between splits of queued ranges.
	splitQueueTimerDuration = 0 // zero duration to process splits greedily.
)

// splitQueue manages a queue of ranges slated to be split due to size
// or along intersecting zone config boundaries.
type splitQueue struct {
	*baseQueue
	db *client.DB
}

// newSplitQueue returns a new instance of splitQueue.
func newSplitQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *splitQueue {
	sq := &splitQueue{
		db: db,
	}
	sq.baseQueue = newBaseQueue(
		"split", sq, store, gossip,
		queueConfig{
			maxSize:              defaultQueueMaxSize,
			needsLease:           true,
			needsSystemConfig:    true,
			acceptsUnsplitRanges: true,
			successes:            store.metrics.SplitQueueSuccesses,
			failures:             store.metrics.SplitQueueFailures,
			pending:              store.metrics.SplitQueuePending,
			processingNanos:      store.metrics.SplitQueueProcessingNanos,
		},
	)
	return sq
}

// shouldQueue determines whether a range should be queued for
// splitting. This is true if the range is intersected by a zone config
// prefix or if the range's size in bytes exceeds the limit for the zone.
func (sq *splitQueue) shouldQueue(
	ctx context.Context, now hlc.Timestamp, repl *Replica, sysCfg config.SystemConfig,
) (shouldQ bool, priority float64) {
	desc := repl.Desc()
	if sysCfg.NeedsSplit(desc.StartKey, desc.EndKey) {
		// Set priority to 1 in the event the range is split by zone configs.
		priority = 1
		shouldQ = true
	}

	// Add priority based on the size of range compared to the max
	// size for the zone it's in.
	if ratio := float64(repl.GetMVCCStats().Total()) / float64(repl.GetMaxBytes()); ratio > 1 {
		priority += ratio
		shouldQ = true
	}
	return
}

// process synchronously invokes admin split for each proposed split key.
func (sq *splitQueue) process(ctx context.Context, r *Replica, sysCfg config.SystemConfig) error {
	// First handle case of splitting due to zone config maps.
	desc := r.Desc()
	if splitKey := sysCfg.ComputeSplitKey(desc.StartKey, desc.EndKey); splitKey != nil {
		log.Infof(ctx, "splitting at key %v", splitKey)
		if _, _, pErr := r.adminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{
				SplitKey: splitKey.AsRawKey(),
			},
			desc,
		); pErr != nil {
			return errors.Wrapf(pErr.GoError(), "unable to split %s at key %q", r, splitKey)
		}
		return nil
	}

	// Next handle case of splitting due to size. Note that we don't perform
	// size-based splitting if maxBytes is 0 (happens in certain test
	// situations).
	size := r.GetMVCCStats().Total()
	maxBytes := r.GetMaxBytes()
	if maxBytes > 0 && float64(size)/float64(maxBytes) > 1 {
		log.Infof(ctx, "splitting size=%d max=%d", size, maxBytes)
		if _, validSplitKey, pErr := r.adminSplitWithDescriptor(
			ctx,
			roachpb.AdminSplitRequest{},
			desc,
		); pErr != nil {
			return pErr.GoError()
		} else if !validSplitKey {
			// If we couldn't find a split key, set the max-bytes for the range to
			// double its current size to prevent future attempts to split the range
			// until it grows again.
			r.SetMaxBytes(size * 2)
		} else {
			// We successfully split the range, reset max-bytes to the zone setting.
			zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
			if err != nil {
				return err
			}
			r.SetMaxBytes(zone.RangeMaxBytes)
		}
	}
	return nil
}

// timer returns interval between processing successive queued splits.
func (*splitQueue) timer(_ time.Duration) time.Duration {
	return splitQueueTimerDuration
}

// purgatoryChan returns nil.
func (*splitQueue) purgatoryChan() <-chan struct{} {
	return nil
}
