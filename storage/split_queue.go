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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

const (
	// splitQueueMaxSize is the max size of the split queue.
	splitQueueMaxSize = 100
	// splitQueueTimerDuration is the duration between splits of queued ranges.
	splitQueueTimerDuration = 0 * time.Second // zero duration to process splits greedily.
)

// splitQueue manages a queue of ranges slated to be split due to size
// or along intersecting accounting or zone config boundaries.
type splitQueue struct {
	*baseQueue
	db     *client.DB
	gossip *gossip.Gossip
}

// newSplitQueue returns a new instance of splitQueue.
func newSplitQueue(db *client.DB, gossip *gossip.Gossip) *splitQueue {
	sq := &splitQueue{
		db:     db,
		gossip: gossip,
	}
	sq.baseQueue = newBaseQueue("split", sq, splitQueueMaxSize)
	return sq
}

func (sq *splitQueue) needsLeaderLease() bool {
	return true
}

// shouldQueue determines whether a range should be queued for
// splitting. This is true if the range is intersected by any
// accounting or zone config prefix or if the range's size in
// bytes exceeds the limit for the zone.
func (sq *splitQueue) shouldQueue(now proto.Timestamp, rng *Range) (shouldQ bool, priority float64) {
	// Set priority to 1 in the event the range is split by acct or zone configs.
	if len(computeSplitKeys(sq.gossip, rng)) > 0 {
		priority = 1
		shouldQ = true
	}

	// Add priority based on the size of range compared to the max
	// size for the zone it's in.
	zone, err := lookupZoneConfig(sq.gossip, rng)
	if err != nil {
		log.Error(err)
		return
	}
	if ratio := float64(rng.stats.GetSize()) / float64(zone.RangeMaxBytes); ratio > 1 {
		priority += ratio
		shouldQ = true
	}
	return
}

// process synchronously invokes admin split for each proposed split key.
func (sq *splitQueue) process(now proto.Timestamp, rng *Range) error {
	// First handle case of splitting due to accounting and zone config maps.
	splitKeys := computeSplitKeys(sq.gossip, rng)
	if len(splitKeys) > 0 {
		log.Infof("splitting %s at keys %v", rng, splitKeys)
		for _, splitKey := range splitKeys {
			if err := sq.db.AdminSplit(splitKey); err != nil {
				return util.Errorf("unable to split %s at key %q: %s", rng, splitKey, err)
			}
		}
		return nil
	}
	// Next handle case of splitting due to size.
	zone, err := lookupZoneConfig(sq.gossip, rng)
	if err != nil {
		return err
	}
	// FIXME: why is this implementation not the same as the one above?
	if float64(rng.stats.GetSize())/float64(zone.RangeMaxBytes) > 1 {
		log.Infof("splitting %s size=%d max=%d", rng, rng.stats.GetSize(), zone.RangeMaxBytes)
		if err = rng.AddCmd(rng.context(),
			proto.Call{
				Args: &proto.AdminSplitRequest{
					RequestHeader: proto.RequestHeader{Key: rng.Desc().StartKey},
				},
				Reply: &proto.AdminSplitResponse{},
			}, true); err != nil {
			return err
		}
	}
	return nil
}

// timer returns interval between processing successive queued splits.
func (sq *splitQueue) timer() time.Duration {
	return splitQueueTimerDuration
}

// computeSplitKeys returns an array of keys at which the supplied
// range should be split, as computed by intersecting the range with
// accounting and zone config map boundaries.
func computeSplitKeys(g *gossip.Gossip, rng *Range) []proto.Key {
	// Now split the range into pieces by intersecting it with the
	// boundaries of the config map.
	splitKeys := proto.KeySlice{}
	for _, configKey := range []string{gossip.KeyConfigAccounting, gossip.KeyConfigZone} {
		info, err := g.GetInfo(configKey)
		if err != nil {
			log.Errorf("unable to fetch %s config from gossip: %s", configKey, err)
			continue
		}
		configMap := info.(PrefixConfigMap)
		splits, err := configMap.SplitRangeByPrefixes(rng.Desc().StartKey, rng.Desc().EndKey)
		if err != nil {
			log.Errorf("unable to split %s by prefix map %s", rng, configMap)
			continue
		}
		// Gather new splits.
		for _, split := range splits {
			if split.end.Less(rng.Desc().EndKey) {
				splitKeys = append(splitKeys, split.end)
			}
		}
	}

	// Sort and unique the combined split keys from intersections with
	// both the accounting and zone config maps.
	sort.Sort(splitKeys)
	var unique []proto.Key
	for i, key := range splitKeys {
		if i == 0 || !key.Equal(splitKeys[i-1]) {
			unique = append(unique, key)
		}
	}
	return unique
}

// lookupZoneConfig returns the zone config matching the range.
func lookupZoneConfig(g *gossip.Gossip, rng *Range) (proto.ZoneConfig, error) {
	zoneMap, err := g.GetInfo(gossip.KeyConfigZone)
	if err != nil || zoneMap == nil {
		return proto.ZoneConfig{}, util.Errorf("unable to lookup zone config for range %s: %s", rng, err)
	}
	prefixConfig := zoneMap.(PrefixConfigMap).MatchByPrefix(rng.Desc().StartKey)
	return *prefixConfig.Config.(*proto.ZoneConfig), nil
}
