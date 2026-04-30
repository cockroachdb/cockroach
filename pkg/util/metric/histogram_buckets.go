// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"math"

	"github.com/cockroachdb/goodhistogram"
	"github.com/prometheus/client_golang/prometheus"
)

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
	DURATION
)

var IOLatencyBuckets = staticBucketConfig{
	category:     "IOLatencyBuckets",
	min:          10e3, // 10µs
	max:          10e9, // 10s
	count:        60,
	units:        LATENCY,
	distribution: Exponential,
}

var BatchProcessLatencyBuckets = staticBucketConfig{
	category:     "BatchProcessLatencyBuckets",
	min:          500e6, // 500ms
	max:          300e9, // 5m
	count:        60,
	units:        LATENCY,
	distribution: Exponential,
}

var ChangefeedBatchLatencyBuckets = staticBucketConfig{
	category:     "ChangefeedBatchLatencyBuckets",
	min:          5e6,   // 5ms
	max:          600e9, // 10m
	count:        60,
	units:        LATENCY,
	distribution: Exponential,
}

var ChangefeedPipelineLatencyBuckets = staticBucketConfig{
	category:     "ChangefeedPipelineLatencyBuckets",
	min:          5e6,    // 5ms
	max:          3600e9, // 1h
	count:        60,
	units:        LATENCY,
	distribution: Exponential,
}

var LongRunning60mLatencyBuckets = staticBucketConfig{
	category:     "LongRunning60mLatencyBuckets",
	min:          500e6,  // 500ms
	max:          3600e9, // 1h
	count:        60,
	units:        LATENCY,
	distribution: Exponential,
}

var DataCount16MBuckets = staticBucketConfig{
	category:     "DataCount16MBuckets",
	min:          1,
	max:          16e6,
	count:        24,
	units:        COUNT,
	distribution: Exponential,
}

var DataSize16MBBuckets = staticBucketConfig{
	category:     "DataSize16MBBuckets",
	min:          1e3,     // 1kB
	max:          16384e3, // 16MB
	count:        15,
	units:        SIZE,
	distribution: Exponential,
}

var MemoryUsage64MBBuckets = staticBucketConfig{
	category:     "MemoryUsage64MBBuckets",
	min:          1,    // 1B
	max:          64e6, // 64MB
	count:        15,
	units:        SIZE,
	distribution: Exponential,
}

var ReplicaCPUTimeBuckets = staticBucketConfig{
	category:     "ReplicaCPUTimeBuckets",
	min:          50e4, // 500µs
	max:          5e9,  // 5s
	count:        20,
	units:        LATENCY,
	distribution: Exponential,
}

var ReplicaBatchRequestCountBuckets = staticBucketConfig{
	category:     "ReplicaBatchRequestCountBuckets",
	min:          1,
	max:          16e3,
	count:        20,
	units:        COUNT,
	distribution: Exponential,
}

var Count1KBuckets = staticBucketConfig{
	category:     "Count1KBuckets",
	min:          1,
	max:          1024,
	count:        11,
	units:        COUNT,
	distribution: Exponential,
}
var Percent100Buckets = staticBucketConfig{
	category:     "Percent100Buckets",
	min:          0,
	max:          100,
	count:        10,
	units:        COUNT,
	distribution: Uniform,
}
var ResponseTime30sBuckets = staticBucketConfig{
	category:     "ResponseTime30sBuckets",
	min:          1e6,  // 1ms
	max:          30e9, // 30s
	count:        24,
	units:        DURATION,
	distribution: Exponential,
}

var StaticBucketConfigs = []staticBucketConfig{IOLatencyBuckets,
	BatchProcessLatencyBuckets, LongRunning60mLatencyBuckets, DataCount16MBuckets,
	DataSize16MBBuckets, MemoryUsage64MBBuckets, ReplicaCPUTimeBuckets,
	ReplicaBatchRequestCountBuckets, Count1KBuckets, Percent100Buckets, ResponseTime30sBuckets}

// ToGoodHistogramParams converts the bucket configuration into
// goodhistogram.Params. For exponential distributions, the ErrorBound
// is derived from the number of buckets spanning the [min, max] range.
// For uniform distributions, a default ErrorBound is used.
func (config staticBucketConfig) ToGoodHistogramParams() goodhistogram.Params {
	if config.count == 0 {
		return goodhistogram.Params{}
	}
	lo := config.min
	if lo <= 0 {
		lo = 1 // goodhistogram requires Lo > 0
	}
	hi := config.max
	if hi <= lo {
		hi = lo + 1
	}
	// Derive ErrorBound from the bucket count. For exponential
	// distributions the bucket width ratio is (Hi/Lo)^(1/count), so the
	// relative error within a bucket is approximately that ratio minus 1.
	var errorBound float64
	if config.distribution == Exponential && config.count > 1 {
		errorBound = math.Pow(hi/lo, 1.0/float64(config.count)) - 1.0
	} else {
		errorBound = 0.05
	}
	// Clamp to goodhistogram's supported range.
	if errorBound < 0.001 {
		errorBound = 0.001
	}
	if errorBound > 0.5 {
		errorBound = 0.5
	}
	return goodhistogram.Params{
		Lo:         lo,
		Hi:         hi,
		ErrorBound: errorBound,
	}
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
