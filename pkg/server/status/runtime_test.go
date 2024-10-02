// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"math"
	"reflect"
	"runtime/metrics"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/shirou/gopsutil/v3/net"
	"github.com/stretchr/testify/require"
)

func TestSumAndFilterDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := []DiskStats{
		{
			ReadBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			WriteBytes:     1,
			writeCount:     1,
		},
		{
			ReadBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			WriteBytes:     1,
			writeCount:     1,
		},
	}
	summed, err := sumAndFilterDiskCounters(counters)
	if err != nil {
		t.Fatalf("error: %s", err.Error())
	}
	expected := DiskStats{
		ReadBytes:      2,
		readCount:      2,
		WriteBytes:     2,
		writeCount:     2,
		iopsInProgress: 2,
	}
	if !reflect.DeepEqual(summed, expected) {
		t.Fatalf("expected %+v; got %+v", expected, summed)
	}
}

func TestSumNetCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := []net.IOCountersStat{
		{
			BytesRecv:   1,
			PacketsRecv: 1,
			BytesSent:   1,
			PacketsSent: 1,
		},
		{
			BytesRecv:   1,
			PacketsRecv: 1,
			Errin:       1,
			Dropin:      1,
			BytesSent:   1,
			PacketsSent: 1,
			Errout:      1,
			Dropout:     1,
		},
		{
			BytesRecv:   3,
			PacketsRecv: 3,
			Errin:       1,
			Dropin:      1,
			BytesSent:   3,
			PacketsSent: 3,
			Errout:      1,
			Dropout:     1,
		},
	}
	summed := sumNetworkCounters(counters)
	expected := net.IOCountersStat{
		BytesRecv:   5,
		PacketsRecv: 5,
		Errin:       2,
		Dropin:      2,
		BytesSent:   5,
		PacketsSent: 5,
		Errout:      2,
		Dropout:     2,
	}
	if !reflect.DeepEqual(summed, expected) {
		t.Fatalf("expected %+v; got %+v", expected, summed)
	}
}

func TestSubtractDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	from := DiskStats{
		ReadBytes:      3,
		readCount:      3,
		WriteBytes:     3,
		writeCount:     3,
		iopsInProgress: 3,
	}
	sub := DiskStats{
		ReadBytes:      1,
		readCount:      1,
		iopsInProgress: 1,
		WriteBytes:     1,
		writeCount:     1,
	}
	expected := DiskStats{
		ReadBytes:  2,
		readCount:  2,
		WriteBytes: 2,
		writeCount: 2,
		// Don't touch iops in progress; it is a gauge, not a counter.
		iopsInProgress: 3,
	}
	subtractDiskCounters(&from, sub)
	if !reflect.DeepEqual(from, expected) {
		t.Fatalf("expected %+v; got %+v", expected, from)
	}
}

func TestSubtractNetCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	from := net.IOCountersStat{
		PacketsRecv: 3,
		BytesRecv:   3,
		Errin:       2,
		Dropin:      2,
		BytesSent:   3,
		PacketsSent: 3,
		Errout:      2,
		Dropout:     2,
	}
	sub := net.IOCountersStat{
		PacketsRecv: 1,
		BytesRecv:   1,
		Errin:       1,
		Dropin:      1,
		BytesSent:   1,
		PacketsSent: 1,
		Errout:      1,
		Dropout:     1,
	}
	expected := net.IOCountersStat{
		BytesRecv:   2,
		PacketsRecv: 2,
		Dropin:      1,
		Errin:       1,
		BytesSent:   2,
		PacketsSent: 2,
		Errout:      1,
		Dropout:     1,
	}
	subtractNetworkCounters(&from, sub)
	if !reflect.DeepEqual(from, expected) {
		t.Fatalf("expected %+v; got %+v", expected, from)
	}
}

func TestFloat64HistogramSum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type testCase struct {
		h   metrics.Float64Histogram
		sum int64
	}
	testCases := []testCase{
		{
			h: metrics.Float64Histogram{
				Counts:  []uint64{9, 7, 6, 5, 4, 2, 0, 1, 2, 5},
				Buckets: []float64{0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100},
			},
			sum: 1485,
		},
		{
			h: metrics.Float64Histogram{
				Counts:  []uint64{9, 7, 6, 5, 4, 2, 0, 1, 2, 5},
				Buckets: []float64{math.Inf(-1), 11.1, 22.2, 33.3, 44.4, 55.5, 66.6, 77.7, 88.8, 99.9, math.Inf(1)},
			},
			sum: 1670,
		},
	}
	for _, tc := range testCases {
		require.Equal(t, tc.sum, int64(float64HistogramSum(&tc.h)))
	}
}
