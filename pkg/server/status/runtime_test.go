// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package status

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/shirou/gopsutil/net"
)

func TestSumDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := []diskStats{
		{
			readBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			writeBytes:     1,
			writeCount:     1,
		},
		{
			readBytes:      1,
			readCount:      1,
			iopsInProgress: 1,
			writeBytes:     1,
			writeCount:     1,
		},
	}
	summed := sumDiskCounters(counters)
	expected := diskStats{
		readBytes:      2,
		readCount:      2,
		writeBytes:     2,
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
			PacketsSent: 1,
			PacketsRecv: 1,
			BytesSent:   1,
			BytesRecv:   1,
		},
		{
			PacketsSent: 1,
			PacketsRecv: 1,
			BytesSent:   1,
			BytesRecv:   1,
		},
	}
	summed := sumNetworkCounters(counters)
	expected := net.IOCountersStat{
		PacketsSent: 2,
		PacketsRecv: 2,
		BytesSent:   2,
		BytesRecv:   2,
	}
	if !reflect.DeepEqual(summed, expected) {
		t.Fatalf("expected %+v; got %+v", expected, summed)
	}
}

func TestSubtractDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	from := diskStats{
		readBytes:      3,
		readCount:      3,
		writeBytes:     3,
		writeCount:     3,
		iopsInProgress: 3,
	}
	sub := diskStats{
		readBytes:      1,
		readCount:      1,
		iopsInProgress: 1,
		writeBytes:     1,
		writeCount:     1,
	}
	expected := diskStats{
		readBytes:  2,
		readCount:  2,
		writeBytes: 2,
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
		PacketsSent: 3,
		PacketsRecv: 3,
		BytesSent:   3,
		BytesRecv:   3,
	}
	sub := net.IOCountersStat{
		PacketsSent: 1,
		PacketsRecv: 1,
		BytesSent:   1,
		BytesRecv:   1,
	}
	expected := net.IOCountersStat{
		PacketsSent: 2,
		PacketsRecv: 2,
		BytesSent:   2,
		BytesRecv:   2,
	}
	subtractNetworkCounters(&from, sub)
	if !reflect.DeepEqual(from, expected) {
		t.Fatalf("expected %+v; got %+v", expected, from)
	}
}
