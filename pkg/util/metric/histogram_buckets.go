// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import (
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/prometheus/client_golang/prometheus"
)

type staticBucketConfig struct {
	min          float64
	max          float64
	count        int
	histType     histType
	distribution distribution
}

type distribution int

// Distributions
const (
	Uniform distribution = iota
	Exponential
	// Normal
	// LogNormal
)

type histType int

const (
	LATENCY histType = iota
	SIZE
	COUNT
)

// precisionTestEnabledEnv enables precision testing buckets for histograms.
const precisionTestEnabledEnv = "COCKROACH_HISTOGRAM_PRECISION_TESTING"
const precisionTestBucketCount = 200

var StaticBucketConfigs = map[string]staticBucketConfig{
	"IOLatencyBuckets": {
		min:          10e3,
		max:          10e9,
		count:        60,
		histType:     LATENCY,
		distribution: Exponential,
	},
	"BatchProcessLatencyBuckets": {
		min:          500e6,
		max:          300e9,
		count:        60,
		histType:     LATENCY,
		distribution: Exponential,
	},
	"LongRunning60mLatencyBuckets": {
		min:          500e6,
		max:          3600e9,
		count:        60,
		histType:     LATENCY,
		distribution: Exponential,
	},
	"Count1KBuckets": {
		min:          1,
		max:          1024,
		count:        11,
		histType:     COUNT,
		distribution: Exponential,
	},
	"Percent100Buckets": {
		min:          0,
		max:          100,
		count:        10,
		histType:     COUNT,
		distribution: Uniform,
	},
	"DataSize16MBBuckets": {
		min:          1e3,
		max:          16384e3,
		count:        15,
		histType:     SIZE,
		distribution: Exponential,
	},
	"MemoryUsage64MBBuckets": {
		min:          1,
		max:          64e6,
		count:        15,
		histType:     SIZE,
		distribution: Exponential,
	},
	"ReplicaCPUTimeBuckets": {
		min:          50e4,
		max:          5e9,
		count:        20,
		histType:     LATENCY,
		distribution: Exponential,
	},
	"ReplicaBatchRequestCountBuckets": {
		min:          1,
		max:          16e3,
		count:        20,
		histType:     COUNT,
		distribution: Exponential,
	},
}

func GetBucketsFromBucketConfig(config staticBucketConfig) []float64 {
	var buckets []float64
	if envutil.EnvOrDefaultBool(precisionTestEnabledEnv, false) {
		config.distribution = Uniform
		config.count = precisionTestBucketCount
	}
	if config.distribution == Uniform {
		width := (config.max - config.min) / float64(config.count)
		buckets = prometheus.LinearBuckets(config.min, width, config.count)
	} else if config.distribution == Exponential {
		buckets = prometheus.ExponentialBucketsRange(config.min, config.max,
			config.count)
	}
	return buckets
}
