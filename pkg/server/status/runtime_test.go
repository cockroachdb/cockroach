// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package status

import (
	"reflect"
	"testing"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/net"
)

func TestSumDiskCounters(t *testing.T) {
	defer leaktest.AfterTest(t)()

	counters := map[string]disk.IOCountersStat{
		"disk0": {
			ReadBytes:      1,
			ReadTime:       1,
			ReadCount:      1,
			IopsInProgress: 1,
			WriteBytes:     1,
			WriteTime:      1,
			WriteCount:     1,
		},
		"disk1": {
			ReadBytes:      1,
			ReadTime:       1,
			ReadCount:      1,
			IopsInProgress: 1,
			WriteBytes:     1,
			WriteTime:      1,
			WriteCount:     1,
		},
	}
	summed := sumDiskCounters(counters)
	expected := disk.IOCountersStat{
		ReadBytes:      2,
		ReadTime:       2,
		ReadCount:      2,
		WriteBytes:     2,
		WriteTime:      2,
		WriteCount:     2,
		IopsInProgress: 2,
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

	from := disk.IOCountersStat{
		ReadBytes:      3,
		ReadTime:       3,
		ReadCount:      3,
		WriteBytes:     3,
		WriteTime:      3,
		WriteCount:     3,
		IopsInProgress: 3,
	}
	sub := disk.IOCountersStat{
		ReadBytes:      1,
		ReadTime:       1,
		ReadCount:      1,
		IopsInProgress: 1,
		WriteBytes:     1,
		WriteTime:      1,
		WriteCount:     1,
	}
	expected := disk.IOCountersStat{
		ReadBytes:      2,
		ReadTime:       2,
		ReadCount:      2,
		WriteBytes:     2,
		WriteTime:      2,
		WriteCount:     2,
		IopsInProgress: 2,
	}
	subtractDiskCounters(&from, sub)
	if !reflect.DeepEqual(from, expected) {
		fmt.Println()
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
		fmt.Println()
	}
}
