// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package promql

import (
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// tsdbSeriesSet implements storage.SeriesSet, wrapping a slice of
// storage.Series instances built from TSDB query results.
type tsdbSeriesSet struct {
	series []storage.Series
	idx    int
}

var _ storage.SeriesSet = (*tsdbSeriesSet)(nil)

func newSeriesSet(series []storage.Series) *tsdbSeriesSet {
	// Sort by labels for deterministic output and to satisfy the PromQL engine's
	// expectation that series within a set are ordered.
	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].Labels(), series[j].Labels()) < 0
	})
	return &tsdbSeriesSet{series: series, idx: -1}
}

func (s *tsdbSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *tsdbSeriesSet) At() storage.Series {
	return s.series[s.idx]
}

func (s *tsdbSeriesSet) Err() error {
	return nil
}

func (s *tsdbSeriesSet) Warnings() storage.Warnings {
	return nil
}

// tsdbSeries implements storage.Series, wrapping a single time series (one
// source of one metric) from TSDB query results.
type tsdbSeries struct {
	lbls       labels.Labels
	datapoints []tspb.TimeSeriesDatapoint
}

var _ storage.Series = (*tsdbSeries)(nil)

func (s *tsdbSeries) Labels() labels.Labels {
	return s.lbls
}

func (s *tsdbSeries) Iterator() chunkenc.Iterator {
	return &tsdbIterator{datapoints: s.datapoints, idx: -1}
}

// tsdbIterator implements chunkenc.Iterator over TSDB datapoints. It converts
// TSDB nanosecond timestamps to Prometheus millisecond timestamps.
//
// Note: the chunkenc.Iterator interface defines Seek(int64) bool, which
// conflicts with Go vet's stdmethods check (io.Seeker). This file is excluded
// from the stdmethods nogo analyzer in build/bazelutil/nogo_config.json.
type tsdbIterator struct {
	datapoints []tspb.TimeSeriesDatapoint
	idx        int
}

var _ chunkenc.Iterator = (*tsdbIterator)(nil)

func (it *tsdbIterator) Next() bool {
	it.idx++
	return it.idx < len(it.datapoints)
}

func (it *tsdbIterator) Seek(t int64) bool {
	// t is in milliseconds. Find the first datapoint with timestamp >= t.
	for it.idx < len(it.datapoints) {
		if it.idx < 0 {
			it.idx = 0
		}
		tsMillis := it.datapoints[it.idx].TimestampNanos / 1e6
		if tsMillis >= t {
			return true
		}
		it.idx++
	}
	return false
}

func (it *tsdbIterator) At() (int64, float64) {
	if it.idx < 0 || it.idx >= len(it.datapoints) {
		return 0, 0
	}
	dp := it.datapoints[it.idx]
	return dp.TimestampNanos / 1e6, dp.Value
}

func (it *tsdbIterator) Err() error {
	return nil
}

// errSeriesSet implements storage.SeriesSet, returning an error.
type errSeriesSet struct {
	err error
}

func (s errSeriesSet) Next() bool                 { return false }
func (s errSeriesSet) At() storage.Series         { return nil }
func (s errSeriesSet) Err() error                 { return s.err }
func (s errSeriesSet) Warnings() storage.Warnings { return nil }

// buildSeries constructs a tsdbSeries from TSDB query result data.
func buildSeries(
	promName string, instanceType string, source string, datapoints []tspb.TimeSeriesDatapoint,
) *tsdbSeries {
	srcLabel := SourceLabel(instanceType)
	lbls := labels.Labels{
		{Name: labels.MetricName, Value: promName},
		{Name: instanceTypeLabel, Value: instanceType},
		{Name: srcLabel, Value: source},
	}
	sort.Sort(lbls)

	// Filter out NaN datapoints which TSDB may produce for missing data.
	filtered := make([]tspb.TimeSeriesDatapoint, 0, len(datapoints))
	for _, dp := range datapoints {
		if !math.IsNaN(dp.Value) {
			filtered = append(filtered, dp)
		}
	}

	return &tsdbSeries{
		lbls:       lbls,
		datapoints: filtered,
	}
}
