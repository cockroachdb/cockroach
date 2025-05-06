// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValInRange tests the behavior of the valInRange helper function
// for checking if integer values are within specified ranges.
func TestValInRange(t *testing.T) {
	p := &Provider{}

	testCases := []struct {
		name      string
		value     int
		rangeStr  string
		expected  bool
		expectErr bool
	}{
		// Valid ranges with inclusive/exclusive bounds
		{"inclusive-inclusive", 5, "[1-10]", true, false},
		{"inclusive-exclusive", 5, "[1-10)", true, false},
		{"exclusive-inclusive", 5, "(1-10]", true, false},
		{"exclusive-exclusive", 5, "(1-10)", true, false},

		// Edge cases - lower bound
		{"at-inclusive-lower", 1, "[1-10]", true, false},
		{"at-exclusive-lower", 1, "(0-10]", true, false},
		{"below-inclusive-lower", 0, "[1-10]", false, false},
		{"at-exclusive-lower", 1, "(1-10]", false, false},

		// Edge cases - upper bound
		{"at-inclusive-upper", 10, "[1-10]", true, false},
		{"at-exclusive-upper", 10, "[1-10)", false, false},
		{"above-inclusive-upper", 11, "[1-10]", false, false},
		{"above-exclusive-upper", 10, "[1-9)", false, false},

		// Empty bounds
		{"empty-lower", 5, "[-10]", true, false},
		{"empty-upper", 5, "[1-]", true, false},
		{"empty-both", 5, "[-]", true, false},
		{"min-int-check", math.MinInt + 1, "[-]", true, false},
		{"max-int-check", math.MaxInt - 1, "[-]", true, false},

		// Invalid ranges
		{"too-short", 5, "[]", false, true},
		{"invalid-format", 5, "[1,10]", false, true}, // uses comma instead of hyphen
		{"no-delimiter", 5, "[110]", false, true},

		// Non-numeric bounds
		{"invalid-lower", 5, "[a-10]", false, true},
		{"invalid-upper", 5, "[1-b]", false, true},
		{"both-invalid", 5, "[a-b]", false, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := p.valInRange(tc.value, tc.rangeStr)

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result, "expected value %d to be in range %s: %v, got: %v",
					tc.value, tc.rangeStr, tc.expected, result)
			}
		})
	}

	// Additional tests for specific error messages
	t.Run("specific-errors", func(t *testing.T) {
		_, err := p.valInRange(5, "x")
		require.Error(t, err)
		require.Contains(t, err.Error(), "range too short")

		_, err = p.valInRange(5, "[1:10]")
		require.Error(t, err)
		require.Contains(t, err.Error(), "divider '-' not found")

		_, err = p.valInRange(5, "{1-10")
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing or invalid opening bracket")

		_, err = p.valInRange(5, "[1-10")
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing or invalid closing bracket")
	})
}

// TestValInRange_Comprehensive verifies more complex and real-world use cases
// of the valInRange function.
func TestValInRange_Comprehensive(t *testing.T) {
	p := &Provider{}

	type testCase struct {
		value    int
		rangeStr string
		inRange  bool
	}

	testVolumeSizes := []testCase{
		{9, "[10-160000]", false},      // Below min
		{10, "[10-160000]", true},      // At min
		{160000, "[10-160000]", true},  // At max
		{160001, "[10-160000]", false}, // Above max
	}

	testIOPS := []testCase{
		{50, "[100-1000]", false},   // Below min
		{100, "[100-1000]", true},   // At min
		{1000, "[100-1000]", true},  // At max
		{1500, "[100-1000]", false}, // Above max
	}

	// Test volume size ranges
	for _, tc := range testVolumeSizes {
		t.Run(fmt.Sprintf("volume-size-%d", tc.value), func(t *testing.T) {
			result, err := p.valInRange(tc.value, tc.rangeStr)
			require.NoError(t, err)
			require.Equal(t, tc.inRange, result)
		})
	}

	// Test IOPS ranges
	for _, tc := range testIOPS {
		t.Run(fmt.Sprintf("iops-%d", tc.value), func(t *testing.T) {
			result, err := p.valInRange(tc.value, tc.rangeStr)
			require.NoError(t, err)
			require.Equal(t, tc.inRange, result)
		})
	}
}

// TestCheckAttachedVolumeTypeSizeIops tests validation of volume configurations
func TestCheckAttachedVolumeTypeSizeIops(t *testing.T) {
	p := &Provider{}

	testCases := []struct {
		name        string
		volumeType  string
		size        int
		iops        int
		expectError bool
		errorMsg    string
	}{
		// Valid configurations
		{"general-purpose-valid", "general-purpose", 50, 0, false, ""},
		{"5iops-tier-valid", "5iops-tier", 100, 0, false, ""},
		{"10iops-tier-valid", "10iops-tier", 100, 0, false, ""},
		{"custom-valid", "custom", 50, 500, false, ""},

		// Invalid types
		{"invalid-type", "invalid-type", 100, 0, true, "volume type invalid-type is not supported"},

		// Size out of range
		{"size-too-small", "general-purpose", 5, 0, true, "volume size 5 is out of range"},
		{"size-too-large", "general-purpose", 200000, 0, true, "volume size 200000 is out of range"},

		// IOPS validation
		{"no-custom-iops", "general-purpose", 100, 4000, true, "custom IOPS is not supported"},
		{"custom-iops-too-low", "custom", 50, 50, true, "volume IOPS 50 is out of range"},
		{"custom-iops-too-high", "custom", 50, 2001, true, "volume IOPS 2001 is out of range"},
		{"custom-valid-iops", "custom", 2500, 30000, false, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := p.checkAttachedVolumeTypeSizeIops(tc.volumeType, tc.size, tc.iops)
			if tc.expectError {
				require.Error(t, err)
				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestZoneToRegion tests the zone to region conversion logic
func TestZoneToRegion(t *testing.T) {
	p := &Provider{}

	testCases := []struct {
		zone        string
		region      string
		expectError bool
	}{
		// Valid zones
		{"us-east-1", "us-east", false},
		{"eu-west-2", "eu-west", false},
		{"us-south-3", "us-south", false},
		{"jp-tok-1", "jp-tok", false},
		{"au-syd-2", "au-syd", false},

		// Invalid zones
		{"invalid", "", true},
		{"us-east", "", true},
		{"", "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.zone, func(t *testing.T) {
			region, err := p.zoneToRegion(tc.zone)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.region, region)
			}
		})
	}
}

// TestZonesToRegions tests converting multiple zones to unique regions
func TestZonesToRegions(t *testing.T) {
	p := &Provider{}

	testCases := []struct {
		name        string
		zones       []string
		regions     []string
		expectError bool
	}{
		{
			"single-zone",
			[]string{"us-east-1"},
			[]string{"us-east"},
			false,
		},
		{
			"multiple-zones-same-region",
			[]string{"us-east-1", "us-east-2", "us-east-3"},
			[]string{"us-east"},
			false,
		},
		{
			"multiple-zones-different-regions",
			[]string{"us-east-1", "eu-west-2", "jp-tok-1"},
			[]string{"us-east", "eu-west", "jp-tok"},
			false,
		},
		{
			"invalid-zone",
			[]string{"us-east-1", "invalid"},
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			regions, err := p.zonesToRegions(tc.zones)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Check if all expected regions are present (order may vary)
				require.Equal(t, len(tc.regions), len(regions), "regions count mismatch")
				for _, expectedRegion := range tc.regions {
					found := false
					for _, region := range regions {
						if region == expectedRegion {
							found = true
							break
						}
					}
					assert.True(t, found, "expected region %s not found", expectedRegion)
				}
			}
		})
	}
}

// TestComputeLabels tests the label/tag generation functionality
func TestComputeLabels(t *testing.T) {

	p := &Provider{}

	t.Run("default-labels", func(t *testing.T) {
		opts := vm.CreateOpts{
			ClusterName: "test-cluster",
			Lifetime:    24 * time.Hour,
		}

		tags, err := p.computeLabels(opts)
		require.NoError(t, err)

		// Check cluster tag
		assert.Contains(t, tags, fmt.Sprintf("%s:%s", vm.TagCluster, "test-cluster"))

		// Check lifetime tag
		assert.Contains(t, tags, fmt.Sprintf("%s:%s", vm.TagLifetime, "24h0m0s"))
	})

	t.Run("custom-labels", func(t *testing.T) {
		opts := vm.CreateOpts{
			CustomLabels: map[string]string{
				"team":        "cockroach",
				"environment": "test",
			},
		}

		tags, err := p.computeLabels(opts)
		require.NoError(t, err)

		// Check custom tags
		assert.Contains(t, tags, "team:cockroach")
		assert.Contains(t, tags, "environment:test")
	})

	t.Run("duplicate-labels", func(t *testing.T) {
		opts := vm.CreateOpts{
			Lifetime: 24 * time.Hour,
			CustomLabels: map[string]string{
				"lifetime": "48h", // This conflicts with the default lifetime label
			},
		}

		_, err := p.computeLabels(opts)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicated label name")
	})
}

// TestCheckCreateOpts tests validation of VM creation options
func TestCheckCreateOpts(t *testing.T) {
	p := &Provider{}

	loggerOutput := bytes.NewBuffer(nil)
	l, _ := (&logger.Config{
		Stdout: loggerOutput,
		Stderr: loggerOutput,
	}).NewLogger("")

	// Mock provider configuration
	p.config = &ibmCloudConfig{
		regions: map[string]regionDetails{
			"us-east": {
				zones: map[string]zoneDetails{
					"us-east-1": {},
					"us-east-2": {},
				},
			},
		},
	}

	t.Run("valid-options", func(t *testing.T) {
		opts := &vm.CreateOpts{
			Arch:         string(vm.ArchS390x),
			OsVolumeSize: 100,
		}
		providerOpts := &ProviderOpts{
			DefaultVolume: DefaultProviderOpts().DefaultVolume,
		}
		zones := []string{"us-east-1"}

		err := p.checkCreateOpts(l, opts, providerOpts, zones)
		require.NoError(t, err)
	})

	t.Run("defaults-applied", func(t *testing.T) {
		opts := &vm.CreateOpts{
			OsVolumeSize: 0, // Should be set to default
		}
		providerOpts := &ProviderOpts{
			DefaultVolume: DefaultProviderOpts().DefaultVolume,
		}
		zones := []string{"us-east-1"}

		err := p.checkCreateOpts(l, opts, providerOpts, zones)
		require.NoError(t, err)
		assert.Equal(t, string(vm.ArchS390x), opts.Arch, "default architecture not applied")
		assert.Equal(t, defaultSystemVolumeSize, opts.OsVolumeSize, "default volume size not applied")
	})

	t.Run("invalid-architecture", func(t *testing.T) {
		opts := &vm.CreateOpts{
			Arch: string(vm.ArchAMD64),
		}
		providerOpts := &ProviderOpts{
			DefaultVolume: DefaultProviderOpts().DefaultVolume,
		}
		zones := []string{"us-east-1"}

		err := p.checkCreateOpts(l, opts, providerOpts, zones)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "IBM only supports S390x architecture")
	})

	// Local SSD is not supported by IBM Cloud, and we silently set it to false
	// if the user tries to use it. A warning message is logged on the logger.
	t.Run("local-ssd-not-supported", func(t *testing.T) {
		opts := &vm.CreateOpts{
			Arch: string(vm.ArchS390x),
			SSDOpts: struct {
				UseLocalSSD   bool
				NoExt4Barrier bool
				FileSystem    string
			}{
				UseLocalSSD: true,
			},
		}
		providerOpts := &ProviderOpts{
			DefaultVolume: DefaultProviderOpts().DefaultVolume,
		}
		zones := []string{"us-east-1"}

		err := p.checkCreateOpts(l, opts, providerOpts, zones)
		require.NoError(t, err)
		assert.Contains(t, loggerOutput.String(), ErrUnsupportedLocalSSDs)
		require.Equal(t, false, opts.SSDOpts.UseLocalSSD)
	})

	t.Run("invalid-zone", func(t *testing.T) {
		opts := &vm.CreateOpts{
			Arch: string(vm.ArchS390x),
		}
		providerOpts := &ProviderOpts{
			DefaultVolume: DefaultProviderOpts().DefaultVolume,
		}
		zones := []string{"invalid-zone"}

		err := p.checkCreateOpts(l, opts, providerOpts, zones)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid zone")
	})

	t.Run("invalid-attached-volume", func(t *testing.T) {
		opts := &vm.CreateOpts{
			Arch: string(vm.ArchS390x),
		}
		providerOpts := &ProviderOpts{
			DefaultVolume: DefaultProviderOpts().DefaultVolume,
			AttachedVolumes: IbmVolumeList{
				{
					VolumeType: "invalid-type",
					VolumeSize: 100,
				},
			},
		}
		zones := []string{"us-east-1"}

		err := p.checkCreateOpts(l, opts, providerOpts, zones)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "attached volume 1 is invalid")
	})
}
