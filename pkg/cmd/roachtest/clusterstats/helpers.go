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
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/search"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
)

// SetupCollectorPromClient instantiates a prometheus client for the given
// configuration. It assumes that a prometheus instance has been created for
// the configuration already.
func SetupCollectorPromClient(
	ctx context.Context, c cluster.Cluster, l *logger.Logger, cfg *prometheus.Config,
) (prometheus.Client, error) {
	prometheusNodeIP, err := c.ExternalIP(ctx, l, c.Node(int(cfg.PrometheusNode[0])))
	if err != nil {
		return nil, err
	}

	client, err := promapi.NewClient(promapi.Config{
		Address: fmt.Sprintf("http://%s:9090", prometheusNodeIP[0]),
	})
	if err != nil {
		return nil, err
	}

	return promv1.NewAPI(client), nil
}

func (i Interval) valid() bool {
	return i.From.Before(i.To)
}

// TrimTaggedSeries returns a map of labeled time series values that have equal
// start times, end times, and length.
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

// ConvertEqualLengthMapToMat converts a map of string:slice pairs, into a
// matrix, where each row (i) are the sorted label values at index [i]. This
// function requires that all the slice values in the map are of equal length.
func ConvertEqualLengthMapToMat(vals map[string][]float64) [][]float64 {
	numLabels := len(vals)
	labels := make([]string, numLabels)

	i := 0
	for label := range vals {
		labels[i] = label
		i++
	}
	sort.Strings(labels)

	seriesLength := 0
	if numLabels > 0 {
		seriesLength = len(vals[labels[0]])
	}

	matrix := make([][]float64, seriesLength)
	for i := 0; i < seriesLength; i++ {
		matrix[i] = make([]float64, numLabels)
		for j, label := range labels {
			matrix[i][j] = vals[label][i]
		}
	}
	return matrix
}
