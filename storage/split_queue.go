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

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// splitQueueMaxSize is the max size of the split queue.
	splitQueueMaxSize = 100
	// splitQueueTimerDuration is the duration between splits of queued ranges.
	splitQueueTimerDuration = 0 // zero duration to process splits greedily.
)

// splitQueue manages a queue of ranges slated to be split due to size
// or along intersecting zone config boundaries.
type splitQueue struct {
	baseQueue
	db *client.DB
}

// newSplitQueue returns a new instance of splitQueue.
func newSplitQueue(store *Store, db *client.DB, gossip *gossip.Gossip) *splitQueue {
	sq := &splitQueue{
		db: db,
	}
	sq.baseQueue = makeBaseQueue("split", sq, store, gossip, queueConfig{
		maxSize:              splitQueueMaxSize,
		needsLease:           true,
		acceptsUnsplitRanges: true,
	})
	return sq
}

// shouldQueue determines whether a range should be queued for
// splitting. This is true if the range is intersected by a zone config
// prefix or if the range's size in bytes exceeds the limit for the zone.
func (*splitQueue) shouldQueue(now hlc.Timestamp, rng *Replica,
	sysCfg config.SystemConfig) (shouldQ bool, priority float64) {

	desc := rng.Desc()
	if len(sysCfg.ComputeSplitKeys(desc.StartKey, desc.EndKey)) > 0 {
		// Set priority to 1 in the event the range is split by zone configs.
		priority = 1
		shouldQ = true
	}

	// Add priority based on the size of range compared to the max
	// size for the zone it's in.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		log.Error(context.TODO(), err)
		return
	}

	if ratio := float64(rng.GetMVCCStats().Total()) / float64(zone.RangeMaxBytes); ratio > 1 {
		priority += ratio
		shouldQ = true
	}
	return
}

// process synchronously invokes admin split for each proposed split key.
func (sq *splitQueue) process(
	ctx context.Context,
	now hlc.Timestamp,
	rng *Replica,
	sysCfg config.SystemConfig,
) error {
	// First handle case of splitting due to zone config maps.
	desc := rng.Desc()
	splitKeys := sysCfg.ComputeSplitKeys(desc.StartKey, desc.EndKey)
	if len(splitKeys) > 0 {
		log.Infof(ctx, "splitting %s at keys %v", rng, splitKeys)
		for _, splitKey := range splitKeys {
			if err := sq.db.AdminSplit(splitKey.AsRawKey()); err != nil {
				return errors.Errorf("unable to split %s at key %q: %s", rng, splitKey, err)
			}
		}
		return nil
	}

	// Next handle case of splitting due to size.
	zone, err := sysCfg.GetZoneConfigForKey(desc.StartKey)
	if err != nil {
		return err
	}
	size := rng.GetMVCCStats().Total()
	// FIXME: why is this implementation not the same as the one above?
	if float64(size)/float64(zone.RangeMaxBytes) > 1 {
		log.Infof(ctx, "splitting %s size=%d max=%d", rng, size, zone.RangeMaxBytes)
		if _, pErr := client.SendWrappedWith(rng, ctx, roachpb.Header{
			Timestamp: now,
		}, &roachpb.AdminSplitRequest{
			Span: roachpb.Span{Key: desc.StartKey.AsRawKey()},
		}); pErr != nil {
			return pErr.GoError()
		}
	}
	return nil
}

// timer returns interval between processing successive queued splits.
func (*splitQueue) timer() time.Duration {
	return splitQueueTimerDuration
}

// purgatoryChan returns nil.
func (*splitQueue) purgatoryChan() <-chan struct{} {
	return nil
}
