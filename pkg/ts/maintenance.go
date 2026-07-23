// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// ContainsTimeSeries returns true if the given key range overlaps the
// range of possible time series keys.
func (tsdb *DB) ContainsTimeSeries(start, end roachpb.RKey) bool {
	return !lastTSRKey.Less(start) && !end.Less(firstTSRKey)
}

// MaintainTimeSeries provides a function that can be called from an external
// process periodically in order to perform "maintenance" work on time series
// data. Currently, this includes computing rollups and pruning data which has
// exceeded its retention threshold, as well as computing low-resolution rollups
// of data. This system was designed specifically to be used by scanner queue
// from the storage package.
//
// The storage Reader should be supplied by a local store, and is used only to
// discover the names of time series which are present in the engine. The KV
// client is then used to interact with data from the time series that are
// discovered; this may result in data being deleted, but may also write new
// data in the form of rollups.
//
// The reader is used for key discovery (as opposed to the KV client) because
// the task of pruning time series is distributed across the cluster to the
// individual ranges which contain that time series data. Because replicas of
// those ranges are guaranteed to have time series data locally, we can use the
// reader to quickly obtain a set of keys to be pruned with no network calls.
func (tsdb *DB) MaintainTimeSeries(
	ctx context.Context,
	reader storage.Reader,
	start, end roachpb.RKey,
	db *kv.DB,
	mem *mon.BytesMonitor,
	budgetBytes int64,
	now hlc.Timestamp,
) error {
	series, err := tsdb.findTimeSeries(ctx, reader, start, end, now)
	if err != nil {
		return err
	}
	if tsdb.WriteRollups() {
		qmc := MakeQueryMemoryContext(mem, mem, QueryMemoryOptions{
			BudgetBytes: budgetBytes,
			Columnar:    tsdb.WriteColumnar(),
		})
		if err := tsdb.rollupTimeSeries(ctx, series, now, qmc); err != nil {
			return err
		}
	}
	return tsdb.pruneTimeSeries(ctx, db, series, now)
}

// Assert that DB implements the necessary interface from the storage package.
var _ kvserver.TimeSeriesDataStore = (*DB)(nil)
