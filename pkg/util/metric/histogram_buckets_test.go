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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const LATENCY = "LATENCY"
const SIZE = "SIZE"

const log10int64times1000 = 19 * 1000

// TestHistogramBuckets is used to generate additional prometheus buckets to be
// used with Histogram. Please include obs-inf in the review process of new
// buckets.
func TestHistogramBuckets(t *testing.T) {
	verifyAndPrint := func(t *testing.T, exp, act []float64, histType string) {
		t.Helper()
		var buf strings.Builder
		for idx, f := range exp {
			if idx == 0 {
				fmt.Fprintf(&buf, "// Generated via %s.", t.Name())
			}
			switch histType {
			case LATENCY:
				fmt.Fprintf(&buf, "\n%f, // %s", f, time.Duration(f))
			case SIZE:
				fmt.Fprintf(&buf, "\n%f, // %s", f, humanize.Bytes(uint64(f)))
			default:
				fmt.Fprintf(&buf, "\n%f,", f)
			}
		}
		t.Logf("%s", &buf)
		require.InDeltaSlice(t, exp, act, 1 /* delta */, "Please update the bucket boundaries for %s", t.Name())
	}
	t.Run("IOLatencyBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBucketsRange(10e3, 10e9, 15)
		verifyAndPrint(t, exp, IOLatencyBuckets, LATENCY)
	})

	t.Run("NetworkLatencyBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBucketsRange(500e3, 1e9, 15)
		verifyAndPrint(t, exp, NetworkLatencyBuckets, LATENCY)
	})

	t.Run("BatchProcessLatencyBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBucketsRange(500e6, 300e9, 15)
		verifyAndPrint(t, exp, BatchProcessLatencyBuckets, LATENCY)
	})

	t.Run("LongRunningProcessLatencyBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBucketsRange(500e6, 3600e9, 15)
		verifyAndPrint(t, exp, LongRunningProcessLatencyBuckets, LATENCY)
	})

	t.Run("CountBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBuckets(1, 2, 11)
		verifyAndPrint(t, exp, CountBuckets, "")
	})

	t.Run("PercentBuckets", func(t *testing.T) {
		exp := prometheus.LinearBuckets(10, 10, 10)
		verifyAndPrint(t, exp, PercentBuckets, "")
	})

	t.Run("DataSizeBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBuckets(1e3, 2, 15)
		verifyAndPrint(t, exp, DataSizeBuckets, SIZE)
	})

	t.Run("MemoryUsageBuckets", func(t *testing.T) {
		exp := prometheus.ExponentialBucketsRange(1, log10int64times1000, 15)
		verifyAndPrint(t, exp, MemoryUsageBuckets, SIZE)
	})
}
