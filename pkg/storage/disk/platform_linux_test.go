// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package disk

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
	ctx := context.Background()

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
			expectDev: "nvme1n1",
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
			expectDev:   "nvme1n1",
			expectError: true,
		},
		{
			name:     "Invalid output",
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

errors: No known data errors
`,
			expectDev:   "nvme1n1",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			device, err := zpoolParseStatus(ctx, tc.poolName, []byte(tc.output))
			if tc.expectError && device != "" {
				require.Error(t, err)
				require.Equal(t, tc.expectDev, device)
			} else if tc.expectError && device == "" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectDev, device)
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
			require.Equal(t, test.expected, got)
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
			wantMin:     5,
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
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantMaj, maj)
				require.Equal(t, tc.wantMin, min)
			}
		})
	}
}
