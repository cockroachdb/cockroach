// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterstats

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/prometheus"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// setupPrometheus initializes prometheus to run against the provided
// PrometheusConfig. If no PrometheusConfig is provided, it creates a
// prometheus scraper for all CockroachDB nodes. If workload instances are
// passed in, they also have instances created for them. Returns the created
// PrometheusConfig if prometheus is initialized, as well as a cleanup function
// which should be called in a defer statement.
func setupPrometheus(
	ctx context.Context, t test.Test, c cluster.Cluster, cfg *prometheus.Config,
) (*prometheus.Config, func()) {
	if cfg == nil {
		// Avoid setting prometheus automatically up for local clusters.
		if c.IsLocal() {
			return nil, func() {}
		}
		// Use the last node as the prometheus node.
		promNode := c.Node(c.Spec().NodeCount)
		cfg = &prometheus.Config{
			PrometheusNode: promNode,
			ScrapeConfigs: []prometheus.ScrapeConfig{
				prometheus.MakeInsecureCockroachScrapeConfig(
					"cockroach",
					c.Range(1, c.Spec().NodeCount-1),
				),
			},
		}
	}
	if c.IsLocal() {
		t.Skip("skipping test as prometheus is needed, but prometheus does not yet work locally")
		return nil, func() {}
	}
	p, err := prometheus.Init(
		ctx,
		*cfg,
		c,
		t.L(),
		func(ctx context.Context, nodes option.NodeListOption, operation string, args ...string) error {
			return c.RepeatRunE(
				ctx,
				t,
				nodes,
				operation,
				args...,
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	return cfg, func() {
		// Use a context that will not time out to avoid the issue where
		// ctx gets canceled if t.Fatal gets called.
		snapshotCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := p.Snapshot(
			snapshotCtx,
			c,
			t.L(),
			t.ArtifactsDir(),
		); err != nil {
			t.L().Printf("failed to get prometheus snapshot: %v", err)
		}
	}
}

func (i Interval) valid() bool {
	return i.From.Before(i.To)
}

// TrimTaggedSeries returns a map of labeled time series values that have the
// same start and end time.
func TrimTaggedSeries(
	ctx context.Context, l *logger.Logger, taggedSeries map[string]map[string]StatSeries,
) (map[string]map[string]StatSeries, int, Interval) {
	lowTS := int64(math.MaxInt64)
	highTS := int64(math.MinInt64)
	longest := 0
	result := make(map[string]map[string]StatSeries)
	skip := make(map[string]bool)

	// Find the highest start timestamp and the lowest end timestamp. These
	// will be used as the bounds for all the series. If there exists a nil or
	// zero length timestamp, we ignore this series and filter it out, not
	// affecting the high or low timestamp bounds used.
	for labelName, labelValues := range taggedSeries {
		for _, series := range labelValues {
			// We cannot process this label name, as at least one of the
			// entries is nil or 0 length. Filter it out and continue to the
			// next.
			if len(series) < 1 {
				skip[labelName] = true
				break
			}
			seriesLength := len(series)
			startTS, endTS := series[0], series[seriesLength-1]
			if longest < seriesLength {
				longest = seriesLength
			}
			if highTS < endTS.Time {
				highTS = endTS.Time
			}
			if lowTS > startTS.Time {
				lowTS = startTS.Time
			}
		}
	}

	trimInterval := Interval{From: timeutil.Unix(0, lowTS), To: timeutil.Unix(0, highTS)}

	shortest := longest
	for labelName, labelValues := range taggedSeries {
		if _, shouldSkip := skip[labelName]; shouldSkip {
			continue
		}
		result[labelName] = make(map[string]StatSeries)
		for labelValue, series := range labelValues {
			trimmedSeries, err := trimSeries(trimInterval, series)
			if err != nil {
				// There was an issue trimming the timeseries for this label
				// value, skip all label values associated with the
				// corresponding label name and delete any existing entries.
				l.PrintfCtx(ctx, "Unable to trim series %s, skipping label name %s", err.Error(), labelName)
				delete(result, labelName)
				break
			}
			if len(trimmedSeries) < shortest {
				shortest = len(trimmedSeries)
			}
			result[labelName][labelValue] = trimmedSeries
		}
	}
	return result, shortest, trimInterval
}

// trimSeries trims a series such that the smallest timestamp is at least as
// great as interval.From and the largest timestamp is at least as small as
// interval.To. e.g. trimSeries([1,2,3,4], (2,4)) returns [2,3,4].
func trimSeries(interval Interval, series StatSeries) (StatSeries, error) {
	if len(series) < 1 {
		return series, nil
	}
	start, end := 0, len(series)-1
	// If there is only a single point, check that it is in the interval
	// without bisecting.
	if start == end {
		singleTS := series[start].Time
		if singleTS >= interval.From.UnixNano() && singleTS <= interval.To.UnixNano() {
			return series[:], nil
		}
		return series[:0], nil
	}

	// Bisect the upper and lower bound timestamps of the series that are
	// contained within the interval given.
	lowerS := search.NewBinarySearcher(start, end, 1)
	lb, err := lowerS.Search(func(i int) (bool, error) {
		return series[i].Time <= interval.From.UnixNano(), nil
	})
	if err != nil {
		return series[:0], err
	}
	// Increment the lower bound if it matches a value less than the lower
	// bound.
	if series[lb].Time < interval.From.UnixNano() {
		lb++
	}

	upperS := search.NewBinarySearcher(start, end, 1)
	ub, err := upperS.Search(func(i int) (bool, error) {
		return series[i].Time <= interval.To.UnixNano(), nil
	})
	if err != nil {
		return series[:0], err
	}
	// Increment the upper bound, as returning a slice is non-inclusive. i.e.
	// [lb, ub).
	ub++

	// There are no timestamps that satisfy the value, we cannot trim the time
	// series.
	if lb > ub {
		return series[:0], errors.Newf("Unable to find slice of series that is contained in the interval, [%s,%s]", interval.From, interval.To)
	}

	return series[lb:ub], nil
}
