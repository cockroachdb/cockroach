// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// asSortedSlice returns the set data in sorted order.
//
// Too inefficient for production.
func (s *condensableSpanSet) asSortedSlice() []roachpb.Span {
	set := s.asSlice()
	cpy := make(roachpb.Spans, len(set))
	copy(cpy, set)
	sort.Sort(cpy)
	return cpy
}

// TestingSenderConcurrencyLimit exports the cluster setting for testing
// purposes.
var TestingSenderConcurrencyLimit = senderConcurrencyLimit

// TestingGetLockFootprint returns the internal lock footprint for testing
// purposes.
func (tc *TxnCoordSender) TestingGetLockFootprint(mergeAndSort bool) []roachpb.Span {
	if mergeAndSort {
		tc.interceptorAlloc.txnPipeliner.lockFootprint.mergeAndSort()
	}
	return tc.interceptorAlloc.txnPipeliner.lockFootprint.asSlice()
}

// TestingGetRefreshFootprint returns the internal refresh footprint for testing
// purposes.
func (tc *TxnCoordSender) TestingGetRefreshFootprint() []roachpb.Span {
	return tc.interceptorAlloc.txnSpanRefresher.refreshFootprint.asSlice()
}

// TestingSetLinearizable allows tests to enable linearizable behavior.
func (tcf *TxnCoordSenderFactory) TestingSetLinearizable(linearizable bool) {
	tcf.linearizable = linearizable
}

// TestingSetMetrics allows tests to override the factory's metrics struct.
func (tcf *TxnCoordSenderFactory) TestingSetMetrics(metrics TxnMetrics) {
	tcf.metrics = metrics
}

// TestingSetCommitWaitFilter allows tests to instrument the beginning of a
// transaction commit wait sleep.
func (tcf *TxnCoordSenderFactory) TestingSetCommitWaitFilter(filter func()) {
	tcf.testingKnobs.CommitWaitFilter = filter
}

// getDistSenderCounterMetrics fetches the count of each specified DisSender
// metric from the `metricNames` parameter and returns the result as a map. The
// keys in the map represent the metric metadata names, while the corresponding
// values indicate the count of each metric. If any of the specified metric
// cannot be found or is not a counter, the function will return an error.
//
// Assumption: 1. The metricNames parameter should consist of string literals
// that match the metadata names used for metric counters. 2. Each metric name
// provided in `metricNames` must exist, unique and be a counter type.
func (dm *DistSenderMetrics) getDistSenderCounterMetrics(
	metricsName []string,
) (map[string]int64, error) {
	metricCountMap := make(map[string]int64)
	getFirstDistSenderMetric := func(metricName string) int64 {
		metricsStruct := reflect.ValueOf(*dm)
		for i := 0; i < metricsStruct.NumField(); i++ {
			field := metricsStruct.Field(i)
			switch t := field.Interface().(type) {
			case *metric.Counter:
				if t.Name == metricName {
					return t.Count()
				}
			}
		}
		return -1
	}

	for _, metricName := range metricsName {
		count := getFirstDistSenderMetric(metricName)
		if count == -1 {
			return map[string]int64{}, errors.Errorf("cannot find metric for %s", metricName)
		}
		metricCountMap[metricName] = count
	}
	return metricCountMap, nil
}

// getMapsDiff returns the difference between the values of corresponding
// metrics in two maps. Assumption: beforeMap and afterMap contain the same set
// of keys.
func getMapsDiff(beforeMap map[string]int64, afterMap map[string]int64) map[string]int64 {
	diffMap := make(map[string]int64)
	for metricName, beforeValue := range beforeMap {
		if v, ok := afterMap[metricName]; ok {
			diffMap[metricName] = v - beforeValue
		}
	}
	return diffMap
}
