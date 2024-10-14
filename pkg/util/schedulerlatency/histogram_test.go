// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulerlatency

import (
	"fmt"
	"math"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestHistogramBuckets is a datadriven test that's used to generate
// prometheus buckets to be used with the scheduler latency histogram exported
// by this package. It comes with the following commands.
//
//   - "buckets"
//     Print out the histogram bucket boundaries maintained by Go when
//     collecting data for scheduling latencies.
//
//   - "rebucket" base=<float64> min=<duration> max=duration
//     Rebucket the default set of bucket boundaries such that they're a
//     multiple of base apart. It also trims the bucket range to the specified
//     min/max values; everything outside the range is merged into (-Inf, ..]
//     and [.., +Inf) buckets.
func TestHistogramBuckets(t *testing.T) {
	buckets := sample().Buckets
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "histogram_buckets"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "buckets":
				return printBuckets(buckets)

			case "rebucket":
				base := parseFloat(t, d, "base")
				min, max := parseDuration(t, d, "min"), parseDuration(t, d, "max")
				rebucketed := reBucketExpAndTrim(buckets, base, min.Seconds(), max.Seconds())
				require.Subset(t, buckets, rebucketed)
				return printBuckets(rebucketed)

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		},
	)
}

// TestRuntimeHistogram is a datadriven test for the runtimeHistogram type. It
// comes with the following commands.
//
//   - "init"
//     bucket=[<float>,<float>)
//     bucket=[<float>,<float>)
//
//   - "update"
//     bucket=[<float>,<float>) count=<int>
//     bucket=[<float>,<float>) count=<int>
//     ...
//
//   - "print"
//
// NB: <float> is also allowed to be "-inf" or "+inf".
func TestRuntimeHistogram(t *testing.T) {
	var rh *runtimeHistogram
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "runtime_histogram"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				buckets := parseBuckets(t, d.Input)
				rh = newRuntimeHistogram(metric.Metadata{}, buckets)
				rh.mult = 1.0
				return ""

			case "update":
				his := &metrics.Float64Histogram{
					Counts:  parseCounts(t, d.Input),
					Buckets: parseBuckets(t, d.Input),
				}
				require.True(t, len(his.Buckets) == len(his.Counts)+1)
				rh.update(his)
				return ""

			case "print":
				var buf strings.Builder
				count, sum := rh.CumulativeSnapshot().Total()
				buf.WriteString(fmt.Sprintf("count=%d sum=%0.2f\n", count, sum))
				hist := rh.ToPrometheusMetric().GetHistogram()
				require.NotNil(t, hist)
				buf.WriteString("buckets:\n")
				for _, bucket := range hist.Bucket {
					buf.WriteString(fmt.Sprintf("  upper-bound=%0.2f cumulative-count=%d\n",
						*bucket.UpperBound,
						*bucket.CumulativeCount,
					))
				}
				return buf.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		},
	)
}

// parseBuckets parses out the list of bucket boundaries when the given input is
// of the form:
//
//	bucket=[<float>,<float>) ...
//	bucket=[<float>,<float>) ...
//
// NB: <float> is also allowed to be "-inf" or "+inf".
func parseBuckets(t *testing.T, input string) []float64 {
	var buckets []float64
	for _, line := range strings.Split(input, "\n") {
		token := strings.Fields(line)[0]                                // bucket=[<float>,<float>)
		token = strings.TrimPrefix(strings.TrimSpace(token), "bucket=") // [<float>,<float>)
		token = strings.TrimPrefix(strings.TrimSpace(token), "[")       // <float>,<float>)
		token = strings.TrimSuffix(strings.TrimSpace(token), ")")       // <float>,<float>

		parts := strings.Split(token, ",")
		var start, end float64
		var err error

		if parts[0] == "-inf" {
			start = math.Inf(-1)
		} else if parts[0] == "+inf" {
			start = math.Inf(1)
		} else {
			start, err = strconv.ParseFloat(parts[0], 64)
			require.NoError(t, err)
		}

		if parts[1] == "-inf" {
			end = math.Inf(-1)
		} else if parts[1] == "+inf" {
			end = math.Inf(1)
		} else {
			end, err = strconv.ParseFloat(parts[1], 64)
			require.NoError(t, err)
		}

		if len(buckets) == 0 {
			buckets = append(buckets, start)
		} else {
			require.Equalf(t, buckets[len(buckets)-1], start,
				"expected end of last bucket to be equal to start of next (around line: %q)", line)
		}
		buckets = append(buckets, end)
	}
	return buckets
}

// parseCounts parses out the list of bucket boundaries when the given input is
// of the form:
//
//	... count=<int>
//	... count=<int>
func parseCounts(t *testing.T, input string) []uint64 {
	var counts []uint64
	for _, line := range strings.Split(input, "\n") {
		fields := strings.Fields(line) // ... count=<int>
		for _, field := range fields {
			if !strings.HasPrefix(field, "count=") {
				continue
			}
			token := field                                                 // count=<int>
			token = strings.TrimPrefix(strings.TrimSpace(token), "count=") // <int>
			count, err := strconv.ParseUint(token, 10, 64)
			require.NoError(t, err)
			counts = append(counts, count)
		}
	}
	return counts
}

func parseFloat(t *testing.T, d *datadriven.TestData, key string) float64 {
	var floatStr string
	d.ScanArgs(t, key, &floatStr)
	f, err := strconv.ParseFloat(floatStr, 64)
	require.NoError(t, err)
	return f
}

func parseDuration(t *testing.T, d *datadriven.TestData, key string) time.Duration {
	var durationStr string
	d.ScanArgs(t, key, &durationStr)
	duration, err := time.ParseDuration(durationStr)
	require.NoError(t, err)
	return duration
}

func printBuckets(buckets []float64) string {
	var buf strings.Builder
	for i := 0; i < len(buckets)-1; i++ {
		sd := time.Duration(buckets[i] * float64(time.Second.Nanoseconds()))
		ed := time.Duration(buckets[i+1] * float64(time.Second.Nanoseconds()))
		s, e := sd.String(), ed.String()
		d := time.Duration(ed.Nanoseconds() - sd.Nanoseconds()).String()
		if math.IsInf(buckets[i], -1) {
			s = "-Inf"
			d = e
		}
		if math.IsInf(buckets[i+1], +1) {
			e = "+Inf"
			d = "Inf"
		}
		buf.WriteString(fmt.Sprintf("bucket[%3d] width=%-18s boundary=[%s, %s)\n", i, d, s, e))
	}
	return buf.String()
}
