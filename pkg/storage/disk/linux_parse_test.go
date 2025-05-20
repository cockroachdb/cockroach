// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package disk

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func compareDeviceIDs(a, b DeviceID) int {
	if v := cmp.Compare(a.major, b.major); v != 0 {
		return v
	}
	return cmp.Compare(a.minor, b.minor)
}

func TestLinux_CollectDiskStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf bytes.Buffer
	datadriven.RunTest(t, "testdata/linux_diskstats", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "parse":
			var disks []*monitoredDisk
			for _, cmdArg := range td.CmdArgs {
				var deviceID DeviceID
				v, err := strconv.ParseUint(cmdArg.Vals[0], 10, 32)
				require.NoError(t, err)
				deviceID.major = uint32(v)
				v, err = strconv.ParseUint(cmdArg.Vals[1], 10, 32)
				require.NoError(t, err)
				deviceID.minor = uint32(v)
				tracer := newMonitorTracer(1000)
				disks = append(disks, &monitoredDisk{deviceID: deviceID, tracer: tracer})
			}
			slices.SortFunc(disks, func(a, b *monitoredDisk) int { return compareDeviceIDs(a.deviceID, b.deviceID) })

			buf.Reset()
			s := &linuxStatsCollector{
				File: vfs.NewMemFile([]byte(td.Input)),
				// Use a small initial buffer size to exercise the buffer
				// resizing logic.
				buf: make([]byte, 16),
			}
			_, err := s.collect(disks, timeutil.Now())
			if err != nil {
				return err.Error()
			}
			for i := range disks {
				monitor := Monitor{monitoredDisk: disks[i]}
				stats, err := monitor.CumulativeStats()
				require.NoError(t, err)

				if i > 0 {
					fmt.Fprintln(&buf)
				}
				fmt.Fprintf(&buf, "%s: ", disks[i].deviceID)
				fmt.Fprint(&buf, stats.String())
			}
			return buf.String()
		default:
			panic(fmt.Sprintf("unrecognized command %q", td.Cmd))
		}
	})
	time.Sleep(time.Second)
}

func TestLinux_zfsParseDF(t *testing.T) {
	testCases := []struct {
		name        string
		path        string
		mockOutput  string
		mockError   error
		expectName  _ZPoolName
		expectError bool
	}{
		{
			name:       "valid ZFS pool with nested dataset",
			path:       "/mnt/data1/",
			mockOutput: "Filesystem     Type\ndata1/crdb1    zfs\n",
			expectName: "data1",
		},
		{
			name:       "valid ZFS pool without nested dataset",
			path:       "/mnt/data2/",
			mockOutput: "Filesystem     Type\ndata2    zfs\n",
			expectName: "data2",
		},
		{
			name:        "unexpected filesystem type",
			path:        "/mnt/other/",
			mockOutput:  "Filesystem     Type\n/dev/sda1    ext4\n",
			expectError: true,
		},
		{
			name:        "unexpected output format",
			path:        "/mnt/bad/",
			mockOutput:  "Filesystem\n",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			poolName, err := zfsParseDF([]byte(tc.mockOutput))
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectName, poolName)
			}
		})
	}
}

func TestLinux_zpoolParseStatus(t *testing.T) {
	ctx := context.TODO()

	testCases := []struct {
		name        string
		poolName    _ZPoolName
		output      string
		expectDev   string
		expectError bool
	}{
		{
			name:     "Valid single device",
			poolName: "data1",
			output: `
  pool: data1
 state: ONLINE
  scan: resilvered 72.9G in 00:18:00 with 0 errors on Mon May 19 19:18:10 2025
config:

	NAME              STATE     READ WRITE CKSUM
	data1             ONLINE       0     0     0
	  /dev/nvme1n1p1  ONLINE       0     0     0

errors: No known data errors
`,
			expectDev: "/dev/nvme1n1",
		},
		{
			name:     "Invalid multiple devices",
			poolName: "data1",
			output: `
  pool: data1
 state: ONLINE
status: One or more devices is currently being resilvered.
	continue to function, possibly in a degraded state.
action: Wait for the resilver to complete.
  scan: resilver in progress since Tue May 20 22:22:02 2025
	18.3G / 18.4G scanned, 2.10G / 17.9G issued at 79.8M/s
	2.12G resilvered, 11.75% done, 00:03:22 to go
config:

	NAME                STATE     READ WRITE CKSUM
	data1               ONLINE       0     0     0
	  mirror-0          ONLINE       0     0     0
	    /dev/nvme1n1p1  ONLINE       0     0     0
	    /dev/nvme5n1p1  ONLINE       0     0     0  (resilvering)

errors: No known data errors
`,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			device, err := zpoolParseStatus(ctx, tc.poolName, []byte(tc.output))
			if tc.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if device != tc.expectDev {
					t.Errorf("expected device %q, got %q", tc.expectDev, device)
				}
			}
		})
	}
}

func TestLinux_stripDevicePartition(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"NVME with partition", "/dev/nvme0n1p1", "nvme0n1"},
		{"NVME without partition", "/dev/nvme0n1", "nvme0n1"},
		{"SCSI with partition", "/dev/sda1", "sda"},
		{"SCSI without partition", "/dev/sda", "sda"},
		{"RAM device with partition", "/dev/ram0", "ram"},
		{"Loop device", "/dev/loop0", "loop"},
		{"Invalid device", "/dev/randomdevice", "/dev/randomdevice"},
		{"Empty string", "", ""},
		{"Device path without prefix", "nvme0n1p3", "nvme0n1"},
		{"Complex invalid input", "/dev/nvme0n1p1x", "/dev/nvme0n1p1x"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := stripDevicePartition(test.input)
			if got != test.expected {
				t.Errorf("stripDevicePartition(%q) = %q; want %q", test.input, got, test.expected)
			}
		})
	}
}

func TestLinux_parseDeviceID(t *testing.T) {
	testCases := []struct {
		name        string
		devFilePath string
		data        []byte
		wantMaj     uint32
		wantMin     uint32
		wantErr     bool
	}{
		{
			name:        "valid device numbers",
			devFilePath: "/sys/block/sda/dev",
			data:        []byte("8:0\n"),
			wantMaj:     8,
			wantMin:     0,
			wantErr:     false,
		},
		{
			name:        "valid device numbers with whitespace",
			devFilePath: "/sys/block/nvme1n1/dev",
			data:        []byte("  259:5\n"),
			wantMaj:     259,
			wantMin:     3,
			wantErr:     false,
		},
		{
			name:        "invalid format missing colon",
			devFilePath: "/sys/block/sdc/dev",
			data:        []byte("2593\n"),
			wantErr:     true,
		},
		{
			name:        "non-numeric values",
			devFilePath: "/sys/block/sdd/dev",
			data:        []byte("a:b\n"),
			wantErr:     true,
		},
		{
			name:        "empty data",
			devFilePath: "/sys/block/sde/dev",
			data:        []byte("\n"),
			wantErr:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			maj, min, err := parseDeviceID(tc.devFilePath, tc.data)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseDeviceID() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				got := []uint32{maj, min}
				want := []uint32{tc.wantMaj, tc.wantMin}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("parseDeviceID() = %v, want %v", got, want)
				}
			}
		})
	}
}
