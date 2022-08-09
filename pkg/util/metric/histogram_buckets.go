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

// IOLatencyBuckets are prometheus histogram buckets suitable for a histogram
// that records a quantity (nanosecond-denominated) in which most measurements
// resemble those of typical disk latencies, i.e. which are in the micro- and
// millisecond range during normal operation.
var IOLatencyBuckets = []float64{
	// Generated via TestHistogramBuckets/IOLatencyBuckets.
	10000.000000,      // 10µs
	26826.957953,      // 26.826µs
	71968.567300,      // 71.968µs
	193069.772888,     // 193.069µs
	517947.467923,     // 517.947µs
	1389495.494373,    // 1.389495ms
	3727593.720315,    // 3.727593ms
	10000000.000000,   // 9.999999ms
	26826957.952797,   // 26.826957ms
	71968567.300115,   // 71.968567ms
	193069772.888325,  // 193.069772ms
	517947467.923120,  // 517.947467ms
	1389495494.373135, // 1.389495494s
	3727593720.314933, // 3.72759372s
	9999999999.999981, // 9.999999999s
}

// NetworkLatencyBuckets are prometheus histogram buckets suitable for a histogram
// that records a quantity (nanosecond-denominated) in which most measurements
// behave like network latencies, i.e. most measurements are in the ms to sub-second
// range during normal operation.
var NetworkLatencyBuckets = []float64{
	// Generated via TestHistogramBuckets/NetworkLatencyBuckets.
	500000.000000,    // 500µs
	860513.842995,    // 860.513µs
	1480968.147973,   // 1.480968ms
	2548787.184731,   // 2.548787ms
	4386533.310619,   // 4.386533ms
	7549345.273094,   // 7.549345ms
	12992632.226094,  // 12.992632ms
	22360679.774998,  // 22.360679ms
	38483348.970335,  // 38.483348ms
	66230909.027573,  // 66.230909ms
	113985228.104760, // 113.985228ms
	196171733.362212, // 196.171733ms
	337616984.325077, // 337.616984ms
	581048177.284016, // 581.048177ms
	999999999.999999, // 999.999999ms
}

// BatchProcessLatencyBuckets are prometheus histogram buckets suitable for a
// histogram that records a quantity (nanosecond-denominated) in which most
// measurements are in the seconds to minutes range during normal operation.
var BatchProcessLatencyBuckets = []float64{
	// Generated via TestHistogramBuckets/BatchProcessLatencyBuckets.
	500000000.000000,    // 500ms
	789604072.059876,    // 789.604072ms
	1246949181.227077,   // 1.246949181s
	1969192302.297256,   // 1.969192302s
	3109764521.125753,   // 3.109764521s
	4910965458.056452,   // 4.910965458s
	7755436646.853539,   // 7.755436646s
	12247448713.915894,  // 12.247448713s
	19341270753.704967,  // 19.341270753s
	30543892291.876068,  // 30.543892291s
	48235163460.447227,  // 48.23516346s
	76173362969.685760,  // 1m16.173362969s
	120293595166.717728, // 2m0.293595166s
	189968625172.725128, // 3m9.968625172s
	300000000000.000183, // 5m0s
}

// LongRunningProcessLatencyBuckets are prometheus histogram buckets suitable
// for a histogram that records a quantity (nanosecond-denominated) for
// long-running processes (multiple minutes).
var LongRunningProcessLatencyBuckets = []float64{
	// Generated via TestHistogramBuckets/LongRunningProcessLatencyBuckets.
	500000000.000000,     // 500ms
	942961049.923126,     // 942.961049ms
	1778351083.344248,    // 1.778351083s
	3353831609.364442,    // 3.353831609s
	6325065151.263324,    // 6.325065151s
	11928580151.734879,   // 11.928580151s
	22496372927.944168,   // 22.496372927s
	42426406871.192848,   // 42.426406871s
	80012898335.451462,   // 1m20.012898335s
	150898093243.579315,  // 2m30.898093243s
	284582048872.726685,  // 4m44.582048872s
	536699575188.601318,  // 8m56.699575188s
	1012173589826.278687, // 16m52.173589826s
	1908880541934.094238, // 31m48.880541934s
	3599999999999.998535, // 59m59.999999999s
}

// CountBuckets are prometheus histogram buckets suitable for a histogram that
// records a quantity that is a count (unit-less) in which most measurements are
// in the 1 to ~1000 range during normal operation.
var CountBuckets = []float64{
	// Generated via TestHistogramBuckets/CountBuckets.
	1.000000,
	2.000000,
	4.000000,
	8.000000,
	16.000000,
	32.000000,
	64.000000,
	128.000000,
	256.000000,
	512.000000,
	1024.000000,
}

// PercentBuckets are prometheus histogram buckets suitable for a histogram that
// records a percent quantity [0,100]
var PercentBuckets = []float64{
	// Generated via TestHistogramBuckets/PercentBuckets.
	10.000000,
	20.000000,
	30.000000,
	40.000000,
	50.000000,
	60.000000,
	70.000000,
	80.000000,
	90.000000,
	100.000000,
}

// DataSizeBuckets are prometheus histogram buckets suitable for a histogram that
// records a quantity that is a size (byte-denominated) in which most measurements are
// in the kB to MB range during normal operation.
var DataSizeBuckets = []float64{
	// Generated via TestHistogramBuckets/DataSizeBuckets.
	1000.000000,     // 1.0 kB
	2000.000000,     // 2.0 kB
	4000.000000,     // 4.0 kB
	8000.000000,     // 8.0 kB
	16000.000000,    // 16 kB
	32000.000000,    // 32 kB
	64000.000000,    // 64 kB
	128000.000000,   // 128 kB
	256000.000000,   // 256 kB
	512000.000000,   // 512 kB
	1024000.000000,  // 1.0 MB
	2048000.000000,  // 2.0 MB
	4096000.000000,  // 4.1 MB
	8192000.000000,  // 8.2 MB
	16384000.000000, // 16 MB
}

// MemoryUsageBuckets are prometheus histogram buckets suitable for a histogram that
// records memory usage (in Bytes)
var MemoryUsageBuckets = []float64{
	// Generated via TestHistogramBuckets/MemoryUsageBuckets.
	1.000000,     // 1 B
	2.021274,     // 2 B
	4.085550,     // 4 B
	8.258017,     // 8 B
	16.691718,    // 16 B
	33.738540,    // 33 B
	68.194844,    // 68 B
	137.840488,   // 137 B
	278.613437,   // 278 B
	563.154184,   // 563 B
	1138.289087,  // 1.1 kB
	2300.794494,  // 2.3 kB
	4650.536813,  // 4.7 kB
	9400.010609,  // 9.4 kB
	19000.000000, // 19 kB
}
