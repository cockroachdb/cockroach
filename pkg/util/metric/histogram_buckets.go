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

import "github.com/prometheus/client_golang/prometheus"

// staticBucketConfig describes the buckets we want to generate for a specific
// category of metrics.
type staticBucketConfig struct {
	category     string
	min          float64
	max          float64
	count        int
	units        unitType
	distribution distribution
}

// distribution describes the population distribution that best describes the
// metric for which we record histogram data
type distribution int

const (
	Uniform distribution = iota
	Exponential
	// TODO(ericharmeling): add more distributions
)

// unitType describes the unit type of the metric for which we record
// histogram data
type unitType int

const (
	LATENCY unitType = iota
	SIZE
	COUNT
)

// histType describes the category of the metric for which we record
// histogram data
type histType int

const (
	IOLatencyBuckets histType = iota
	BatchProcessLatencyBuckets
	LongRunning60mLatencyBuckets
	DataSize16MBBuckets
	MemoryUsage64MBBuckets
	ReplicaCPUTimeBuckets
	ReplicaBatchRequestCountBuckets
	Count1KBuckets
	Percent100Buckets
)

var StaticBucketConfigs = map[histType]staticBucketConfig{
	IOLatencyBuckets: {
		category:     "IOLatencyBuckets",
		min:          10e3, // 10µs
		max:          10e9, // 10s
		count:        60,
		units:        LATENCY,
		distribution: Exponential,
	},
	BatchProcessLatencyBuckets: {
		category:     "BatchProcessLatencyBuckets",
		min:          500e6, // 500ms
		max:          300e9, // 5m
		count:        60,
		units:        LATENCY,
		distribution: Exponential,
	},
	LongRunning60mLatencyBuckets: {
		category:     "LongRunning60mLatencyBuckets",
		min:          500e6,  // 500ms
		max:          3600e9, // 1h
		count:        60,
		units:        LATENCY,
		distribution: Exponential,
	},
	DataSize16MBBuckets: {
		category:     "DataSize16MBBuckets",
		min:          1e3,     // 1kB
		max:          16384e3, // 16MB
		count:        15,
		units:        SIZE,
		distribution: Exponential,
	},
	MemoryUsage64MBBuckets: {
		category:     "MemoryUsage64MBBuckets",
		min:          1,    // 1B
		max:          64e6, // 64MB
		count:        15,
		units:        SIZE,
		distribution: Exponential,
	},
	ReplicaCPUTimeBuckets: {
		category:     "ReplicaCPUTimeBuckets",
		min:          50e4, // 500µs
		max:          5e9,  // 5s
		count:        20,
		units:        LATENCY,
		distribution: Exponential,
	},
	ReplicaBatchRequestCountBuckets: {
		category:     "ReplicaBatchRequestCountBuckets",
		min:          1,
		max:          16e3,
		count:        20,
		units:        COUNT,
		distribution: Exponential,
	},
	Count1KBuckets: {
		category:     "Count1KBuckets",
		min:          1,
		max:          1024,
		count:        11,
		units:        COUNT,
		distribution: Exponential,
	},
	Percent100Buckets: {
		category:     "Percent100Buckets",
		min:          0,
		max:          100,
		count:        10,
		units:        COUNT,
		distribution: Uniform,
	},
}

func (config staticBucketConfig) GetBucketsFromBucketConfig() []float64 {
	var buckets []float64
	if config.distribution == Uniform {
		width := (config.max - config.min) / float64(config.count)
		buckets = prometheus.LinearBuckets(config.min, width, config.count)
	} else if config.distribution == Exponential {
		buckets = prometheus.ExponentialBucketsRange(config.min, config.max,
			config.count)
	}
	return buckets
}
