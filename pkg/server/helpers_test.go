// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// getNodeCounterMetrics fetches the count of each specified node metric from
// the `metricNames` parameter and returns the result as a map. The keys in the
// map represent the metric metadata names, while the corresponding values
// indicate the count of each metric. If any of the specified metric cannot be
// found or is not a counter, the function will return an error.
//
// Assumption: 1. The metricNames parameter should consist of string literals
// that match the metadata names used for metric counters. 2. Each metric name
// provided in `metricNames` must exist, unique and be a counter type.
func (nm *nodeMetrics) getNodeCounterMetrics(metricsName []string) (map[string]int64, error) {
	metricCountMap := make(map[string]int64)
	getFirstNodeMetric := func(metricName string) int64 {
		metricsStruct := reflect.ValueOf(*nm)
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
		count := getFirstNodeMetric(metricName)
		if count == -1 {
			return map[string]int64{}, errors.Errorf("cannot find metric for %s", metricName)
		}
		metricCountMap[metricName] = count
	}
	return metricCountMap, nil
}

// getMapsDiff returns the difference between the values of corresponding
// metrics in two maps.
// Assumption: beforeMap and afterMap contain the same set of keys.
func getMapsDiff(beforeMap map[string]int64, afterMap map[string]int64) map[string]int64 {
	diffMap := make(map[string]int64)
	for metricName, beforeValue := range beforeMap {
		if v, ok := afterMap[metricName]; ok {
			diffMap[metricName] = v - beforeValue
		}
	}
	return diffMap
}
