// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package status

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/shirou/gopsutil/v3/net"
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
