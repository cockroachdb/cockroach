// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdcutils

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// maxSLIScopes is a static limit on the number of SLI scopes -- that is the number
// of SLI metrics we will keep track of.
// The limit is static due to metric.Registry limitations.
const maxSLIScopes = 8

// SLIMetrics is the list of SLI related metrics for changefeeds.
type SLIMetrics struct {
	ErrorRetries *metric.Counter
	// TODO(yevgeniy): Add more SLI related metrics.
}

// MetricStruct implements metric.Struct interface
func (*SLIMetrics) MetricStruct() {}

func makeSLIMetrics(prefix string) *SLIMetrics {
	withPrefix := func(meta metric.Metadata) metric.Metadata {
		meta.Name = fmt.Sprintf("%s.%s", prefix, meta.Name)
		return meta
	}

	return &SLIMetrics{
		ErrorRetries: metric.NewCounter(withPrefix(metric.Metadata{
			Name:        "error_retries",
			Help:        "Total retryable errors encountered this SLI",
			Measurement: "Errors",
			Unit:        metric.Unit_COUNT,
		})),
	}
}

// SLIScopes represents a set of SLI related metrics for a particular "scope".
type SLIScopes struct {
	Scopes [maxSLIScopes]*SLIMetrics // Exported so that we can register w/ metrics registry.
	names  map[string]*SLIMetrics
}

// MetricStruct implements metric.Struct interface
func (*SLIScopes) MetricStruct() {}

// CreateSLIScopes creates changefeed specific SLI scope: a metric.Struct containing
// SLI specific metrics for each scope.
// The scopes are statically named "tier<number>", and each metric name
// contained in SLIMetrics will be prefixed by "changefeed.tier<number" prefix.
func CreateSLIScopes() *SLIScopes {
	scope := &SLIScopes{
		names: make(map[string]*SLIMetrics, maxSLIScopes),
	}

	for i := 0; i < maxSLIScopes; i++ {
		scopeName := fmt.Sprintf("tier%d", i)
		scope.Scopes[i] = makeSLIMetrics(fmt.Sprintf("changefeed.%s", scopeName))
		scope.names[scopeName] = scope.Scopes[i]
	}
	return scope
}

// GetSLIMetrics returns a metric.Struct associated with the specified scope, or nil
// of no such scope exists.
func (s *SLIScopes) GetSLIMetrics(scopeName string) *SLIMetrics {
	return s.names[scopeName]
}
