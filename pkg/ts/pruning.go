// Copyright 2016 The Cockroach Authors.
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
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

var (
	firstTSRKey = roachpb.RKey(keys.TimeseriesPrefix)
	lastTSRKey  = firstTSRKey.PrefixEnd()
)

// ContainsTimeSeries returns true if the given key range overlaps the
// range of possible time series keys.
func (tsdb *DB) ContainsTimeSeries(start, end roachpb.RKey) bool {
	return !lastTSRKey.Less(start) && !end.Less(firstTSRKey)
}

// PruneTimeSeries prunes old data for any time series found in the supplied
// key range.
//
// The snapshot should be supplied by a local store, and is used only to
// discover the names of time series which are store in that snapshot. The KV
// client is then used to prune old data from the discovered series.
//
// The snapshot is used for key discovery (as opposed to the KV client) because
// the task of pruning time series is distributed across the cluster to the
// individual ranges which contain that time series data. Because replicas of
// those ranges are guaranteed to have time series data locally, we can use the
// snapshot to quickly obtain a set of keys to be pruned with no network calls.
func (tsdb *DB) PruneTimeSeries(
	ctx context.Context,
	snapshot engine.Reader,
	start, end roachpb.RKey,
	db *client.DB,
	timestamp hlc.Timestamp,
) error {
	series, err := findTimeSeries(snapshot, start, end, timestamp)
	if err != nil {
		return err
	}
	return pruneTimeSeries(ctx, db, series, timestamp)
}

// Assert that DB implements the necessary interface from the storage package.
var _ storage.TimeSeriesDataStore = (*DB)(nil)

type timeSeriesResolutionInfo struct {
	Name       string
	Resolution Resolution
}

// findTimeSeries searches the supplied engine over the supplied key range,
// identifying time series which have stored data in the range, along with the
// resolutions at which time series data is stored. A unique name/resolution
// pair will only be identified once, even if the range contains keys for that
// name/resolution pair at multiple timestamps or from multiple sources.
//
// An engine snapshot is used, rather than a client, because this function is
// intended to be called by a storage queue which can inspect the local data for
// a single range without the need for expensive network calls.
func findTimeSeries(
	snapshot engine.Reader, startKey, endKey roachpb.RKey, now hlc.Timestamp,
) ([]timeSeriesResolutionInfo, error) {
	var results []timeSeriesResolutionInfo

	iter := snapshot.NewIterator(false)
	defer iter.Close()

	// Set start boundary for the search, which is the lesser of the range start
	// key and the beginning of time series data.
	start := engine.MakeMVCCMetadataKey(startKey.AsRawKey())
	next := engine.MakeMVCCMetadataKey(keys.TimeseriesPrefix)
	if next.Less(start) {
		next = start
	}

	// Set end boundary for the search, which is the lesser of the range end key
	// and the end of time series data.
	end := engine.MakeMVCCMetadataKey(endKey.AsRawKey())
	lastTS := engine.MakeMVCCMetadataKey(keys.TimeseriesPrefix.PrefixEnd())
	if lastTS.Less(end) {
		end = lastTS
	}

	thresholds := computeThresholds(now.WallTime)

	for iter.Seek(next); ; iter.Seek(next) {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok || !iter.Less(end) {
			break
		}
		foundKey := iter.Key().Key

		// Extract the name and resolution from the discovered key.
		name, _, res, tsNanos, err := DecodeDataKey(foundKey)
		if err != nil {
			return nil, err
		}
		// Skip this time series if there's nothing to prune. We check the
		// oldest (first) time series record's timestamp against the
		// pruning threshold.
		if threshold, ok := thresholds[res]; !ok || threshold > tsNanos {
			results = append(results, timeSeriesResolutionInfo{
				Name:       name,
				Resolution: res,
			})
		}

		// Set 'next' is initialized to the next possible time series key
		// which could belong to a previously undiscovered time series.
		next = engine.MakeMVCCMetadataKey(makeDataKeySeriesPrefix(name, res).PrefixEnd())
	}

	return results, nil
}

// pruneTimeSeries will prune data for the supplied set of time series. Time
// series series are identified by name and resolution.
//
// For each time series supplied, the pruning operation will delete all data
// older than a constant threshold. The threshold is different depending on the
// resolution; typically, lower-resolution time series data will be retained for
// a longer period.
//
// If data is stored at a resolution which is not known to the system, it is
// assumed that the resolution has been deprecated and all data for that time
// series at that resolution will be deleted.
//
// As range deletion of inline data is an idempotent operation, it is safe to
// run this operation concurrently on multiple nodes at the same time.
func pruneTimeSeries(
	ctx context.Context, db *client.DB, timeSeriesList []timeSeriesResolutionInfo, now hlc.Timestamp,
) error {
	thresholds := computeThresholds(now.WallTime)

	b := &client.Batch{}
	for _, timeSeries := range timeSeriesList {
		// Time series data for a specific resolution falls in a contiguous key
		// range, and can be deleted with a DelRange command.

		// The start key is the prefix unique to this name/resolution pair.
		start := makeDataKeySeriesPrefix(timeSeries.Name, timeSeries.Resolution)

		// The end key can be created by generating a time series key with the
		// threshold timestamp for the resolution. If the resolution is not
		// supported, the start key's PrefixEnd is used instead (which will clear
		// the time series entirely).
		var end roachpb.Key
		threshold, ok := thresholds[timeSeries.Resolution]
		if ok {
			end = MakeDataKey(timeSeries.Name, "", timeSeries.Resolution, threshold)
		} else {
			end = start.PrefixEnd()
		}

		b.AddRawRequest(&roachpb.DeleteRangeRequest{
			Span: roachpb.Span{
				Key:    start,
				EndKey: end,
			},
			Inline: true,
		})
	}

	return db.Run(ctx, b)
}

// computeThresholds returns a map of timestamps for each resolution supported
// by the system. Data at a resolution which is older than the threshold
// timestamp for that resolution is considered eligible for deletion.
func computeThresholds(timestamp int64) map[Resolution]int64 {
	result := make(map[Resolution]int64, len(pruneThresholdByResolution))
	for k, v := range pruneThresholdByResolution {
		result[k] = timestamp - v
	}
	return result
}
