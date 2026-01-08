// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

// TestWriteStartupScriptTemplate mainly tests the startup script tpl compiles.
func TestWriteStartupScriptTemplate(t *testing.T) {
	file, err := writeStartupScript("vm_name", "", vm.Zfs, false,
		false, "ubuntu", false)
	require.NoError(t, err)

	f, err := os.ReadFile(file)
	require.NoError(t, err)

	echotest.Require(t, string(f), datapathutils.TestDataPath(t, "startup_script"))
}

// TestIOPSCalculation tests that IOPS are calculated correctly for io1/io2 volumes,
// respecting AWS's maximum IOPS-to-size ratio constraints.
func TestIOPSCalculation(t *testing.T) {
	testCases := []struct {
		name         string
		volumeType   string
		volumeSize   int
		expectedIOPS int
		description  string
	}{
		// io1 volume tests (50 IOPS/GB max)
		{
			name:         "io1_50gb_should_cap_at_2500",
			volumeType:   "io1",
			volumeSize:   50,
			expectedIOPS: 2500, // 50 GB * 50 IOPS/GB = 2,500 (not 3,000)
			description:  "50GB io1 volume should cap at 2,500 IOPS, not apply 3,000 minimum",
		},
		{
			name:         "io1_30gb_should_cap_at_1500",
			volumeType:   "io1",
			volumeSize:   30,
			expectedIOPS: 1500, // 30 GB * 50 IOPS/GB = 1,500 (not 3,000)
			description:  "30GB io1 volume should cap at 1,500 IOPS",
		},
		{
			name:         "io1_59gb_should_cap_at_2950",
			volumeType:   "io1",
			volumeSize:   59,
			expectedIOPS: 2950, // 59 GB * 50 IOPS/GB = 2,950 (not 3,000)
			description:  "59GB io1 volume should cap at 2,950 IOPS",
		},
		{
			name:         "io1_60gb_should_use_3000_minimum",
			volumeType:   "io1",
			volumeSize:   60,
			expectedIOPS: 3000, // 60 GB * 50 IOPS/GB = 3,000 (minimum applies)
			description:  "60GB io1 volume should use 3,000 IOPS minimum",
		},
		{
			name:         "io1_100gb_uses_calculated_1000_iops",
			volumeType:   "io1",
			volumeSize:   100,
			expectedIOPS: 3000, // 100 GB * 10 IOPS/GB = 1,000, but minimum is 3,000
			description:  "100GB io1 volume uses 3,000 IOPS minimum (calculated 1,000 < 3,000)",
		},
		{
			name:         "io1_500gb_uses_5000_iops",
			volumeType:   "io1",
			volumeSize:   500,
			expectedIOPS: 5000, // 500 GB * 10 IOPS/GB = 5,000
			description:  "500GB io1 volume uses calculated 5,000 IOPS",
		},
		{
			name:         "io1_max_size_caps_at_64000",
			volumeType:   "io1",
			volumeSize:   10000,
			expectedIOPS: 64000, // 10,000 GB * 10 IOPS/GB = 100,000, but max is 64,000
			description:  "Large io1 volume should cap at 64,000 IOPS",
		},

		// io2 volume tests (500 IOPS/GB max for standard, using conservative limit)
		{
			name:         "io2_5gb_should_cap_at_2500",
			volumeType:   "io2",
			volumeSize:   5,
			expectedIOPS: 2500, // 5 GB * 500 IOPS/GB = 2,500 (not 3,000)
			description:  "5GB io2 volume should cap at 2,500 IOPS",
		},
		{
			name:         "io2_6gb_should_use_3000_minimum",
			volumeType:   "io2",
			volumeSize:   6,
			expectedIOPS: 3000, // 6 GB * 500 IOPS/GB = 3,000 (minimum applies)
			description:  "6GB io2 volume should use 3,000 IOPS minimum",
		},
		{
			name:         "io2_500gb_uses_5000_iops",
			volumeType:   "io2",
			volumeSize:   500,
			expectedIOPS: 5000, // 500 GB * 10 IOPS/GB = 5,000
			description:  "500GB io2 volume uses calculated 5,000 IOPS",
		},
		{
			name:         "io2_max_size_caps_at_64000",
			volumeType:   "io2",
			volumeSize:   10000,
			expectedIOPS: 64000, // 10,000 GB * 10 IOPS/GB = 100,000, but max is 64,000
			description:  "Large io2 volume should cap at 64,000 IOPS",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			iops := calculateProvisionedIOPS(tc.volumeType, tc.volumeSize)
			require.Equal(t, tc.expectedIOPS, iops, tc.description)
		})
	}
}
