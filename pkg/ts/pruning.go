// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

var (
	firstTSRKey = roachpb.RKey(keys.TimeseriesPrefix)
	lastTSRKey  = firstTSRKey.PrefixEnd()
)

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
func (tsdb *DB) findTimeSeries(
	snapshot storage.Reader, startKey, endKey roachpb.RKey, now hlc.Timestamp,
) ([]timeSeriesResolutionInfo, error) {
	var results []timeSeriesResolutionInfo

	// Set start boundary for the search, which is the lesser of the range start
	// key and the beginning of time series data.
	start := storage.MakeMVCCMetadataKey(startKey.AsRawKey())
	next := storage.MakeMVCCMetadataKey(keys.TimeseriesPrefix)
	if next.Less(start) {
		next = start
	}

	// Set end boundary for the search, which is the lesser of the range end key
	// and the end of time series data.
	end := storage.MakeMVCCMetadataKey(endKey.AsRawKey())
	lastTS := storage.MakeMVCCMetadataKey(keys.TimeseriesPrefix.PrefixEnd())
	if lastTS.Less(end) {
		end = lastTS
	}

	thresholds := tsdb.computeThresholds(now.WallTime)

	// NB: timeseries don't have intents.
	iter := snapshot.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{UpperBound: endKey.AsRawKey()})
	defer iter.Close()

	for iter.SeekGE(next); ; iter.SeekGE(next) {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok || !iter.UnsafeKey().Less(end) {
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
		next = storage.MakeMVCCMetadataKey(makeDataKeySeriesPrefix(name, res).PrefixEnd())
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
func (tsdb *DB) pruneTimeSeries(
	ctx context.Context, db *kv.DB, timeSeriesList []timeSeriesResolutionInfo, now hlc.Timestamp,
) error {
	thresholds := tsdb.computeThresholds(now.WallTime)

	b := &kv.Batch{}
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
			RequestHeader: roachpb.RequestHeader{
				Key:    start,
				EndKey: end,
			},
			Inline: true,
		})
	}

	return db.Run(ctx, b)
}
