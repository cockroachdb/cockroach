// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"math"

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

// toGoodHistogramConfig converts a staticBucketConfig to the (lo, hi,
// desiredError) parameters needed by goodhistogram.NewConfig. For Uniform
// distributions (e.g. Percent100Buckets with min=0), lo is set to 1 since
// goodhistogram requires lo > 0. The desired error is chosen so that the
// resulting histogram has at least as many buckets as config.count.
func (config staticBucketConfig) toGoodHistogramConfig() (lo, hi, desiredError float64) {
	lo = config.min
	hi = config.max
	if lo <= 0 {
		lo = 1.0
	}
	desiredError = desiredErrorForBucketCount(lo, hi, config.count)
	return lo, hi, desiredError
}

// bucketsToGoodHistogramConfig infers goodhistogram parameters from an
// explicit bucket boundary slice. The lo and hi are derived from the first
// positive boundary and the last boundary. The desired error is chosen to
// approximate the same number of buckets.
func bucketsToGoodHistogramConfig(buckets []float64) (lo, hi, desiredError float64) {
	lo = buckets[0]
	for _, b := range buckets {
		if b > 0 {
			lo = b
			break
		}
	}
	hi = buckets[len(buckets)-1]
	if hi <= lo {
		hi = lo * 2
	}
	desiredError = desiredErrorForBucketCount(lo, hi, len(buckets))
	return lo, hi, desiredError
}

// desiredErrorForBucketCount picks the coarsest Prometheus schema whose
// bucket count for the range [lo, hi] is at least targetCount. This ensures
// the goodhistogram has at least as many buckets as the original config.
func desiredErrorForBucketCount(lo, hi float64, targetCount int) float64 {
	// Walk schemas from coarsest (0) to finest (8), return the first one
	// that produces enough buckets.
	for s := int32(0); s <= 8; s++ {
		gamma := math.Pow(2, math.Pow(2, float64(-s)))
		numBuckets := int(math.Ceil(math.Log(hi/lo) / math.Log(gamma)))
		if numBuckets >= targetCount {
			return (gamma - 1) / (gamma + 1)
		}
	}
	// Finest schema.
	gamma := math.Pow(2, math.Pow(2, float64(-8)))
	return (gamma - 1) / (gamma + 1)
}
