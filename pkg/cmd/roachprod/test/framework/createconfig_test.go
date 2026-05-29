// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package framework

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce/gcedb"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// TestRandomGCECreateOptions runs RandomGCECreateOptions with many seeds and
// verifies that every generated configuration satisfies compatibility
// invariants derived from gcedb.GetMachineInfo.
func TestRandomGCECreateOptions(t *testing.T) {
	const numIterations = 100

	for i := 0; i < numIterations; i++ {
		seed := int64(i)
		rng := rand.New(rand.NewSource(seed))
		cfg := RandomGCECreateOptions(rng)

		gceOpts, ok := cfg.ProviderOpts.(*gce.ProviderOpts)
		require.True(t, ok, "seed %d: ProviderOpts should be *gce.ProviderOpts", seed)

		info, err := gcedb.GetMachineInfo(gceOpts.MachineType)
		require.NoError(t, err, "seed %d: machine type %s should be recognized by gcedb",
			seed, gceOpts.MachineType)

		desc := fmt.Sprintf("seed=%d machine=%s", seed, gceOpts.MachineType)

		// Architecture must match machine type or be "fips" (amd64 only).
		if cfg.CreateOpts.Arch == "fips" {
			require.Equal(t, gcedb.ArchAMD64, info.Architecture,
				"%s: fips is only valid on amd64 machine types", desc)
		} else {
			require.Equal(t, info.Architecture, cfg.CreateOpts.Arch,
				"%s: arch should match machine type", desc)
		}

		// Local SSD should only be enabled when the machine type supports it.
		if cfg.CreateOpts.SSDOpts.UseLocalSSD {
			require.NotEmpty(t, info.AllowedLocalSSDCount,
				"%s: local SSD enabled but machine type does not support it", desc)
		}

		// PD volume type should be in the machine's supported storage types.
		if !cfg.CreateOpts.SSDOpts.UseLocalSSD && gceOpts.PDVolumeType != "" {
			require.Contains(t, info.StorageTypes, gceOpts.PDVolumeType,
				"%s: PD volume type should be in machine's storage types", desc)
		}

		// Boot disk type (when set) should be in the machine's storage types.
		if gceOpts.BootDiskOnly && gceOpts.BootDiskType != "" {
			require.Contains(t, info.StorageTypes, gceOpts.BootDiskType,
				"%s: boot disk type should be in machine's storage types", desc)
		}

		// Provisioned IOPS: pd-extreme must always have it; values within bounds.
		if gceOpts.PDVolumeType == "pd-extreme" {
			require.Greater(t, gceOpts.PDVolumeProvisionedIOPS, 0,
				"%s: pd-extreme must have provisioned IOPS", desc)
			require.GreaterOrEqual(t, gceOpts.PDVolumeProvisionedIOPS, minPDExtremeIOPS,
				"%s: pd-extreme IOPS below minimum", desc)
			require.LessOrEqual(t, gceOpts.PDVolumeProvisionedIOPS, maxPDExtremeIOPS,
				"%s: pd-extreme IOPS above maximum", desc)
		}

		// Provisioned throughput should only be set for hyperdisk-balanced.
		if gceOpts.PDVolumeProvisionedThroughput > 0 {
			require.Equal(t, "hyperdisk-balanced", gceOpts.PDVolumeType,
				"%s: provisioned throughput only valid for hyperdisk-balanced", desc)
			require.GreaterOrEqual(t, gceOpts.PDVolumeProvisionedThroughput, minHyperdiskThroughputMiBps,
				"%s: hyperdisk throughput below minimum", desc)
			require.LessOrEqual(t, gceOpts.PDVolumeProvisionedThroughput, maxHyperdiskThroughputMiBps,
				"%s: hyperdisk throughput above maximum", desc)
		}

		// When hyperdisk-balanced has IOPS, verify within bounds.
		if gceOpts.PDVolumeType == "hyperdisk-balanced" && gceOpts.PDVolumeProvisionedIOPS > 0 {
			require.GreaterOrEqual(t, gceOpts.PDVolumeProvisionedIOPS, minHyperdiskIOPS,
				"%s: hyperdisk IOPS below minimum", desc)
			require.LessOrEqual(t, gceOpts.PDVolumeProvisionedIOPS, maxHyperdiskIOPS,
				"%s: hyperdisk IOPS above maximum", desc)
		}

		// T2A machine types should only use T2A-supported zones.
		if strings.HasPrefix(gceOpts.MachineType, "t2a-") {
			for _, zone := range gceOpts.Zones {
				require.Contains(t, gce.SupportedT2AZones, zone,
					"%s: T2A zone %s not in SupportedT2AZones", desc, zone)
			}
		} else {
			for _, zone := range gceOpts.Zones {
				require.Contains(t, SupportedGCEZones, zone,
					"%s: zone %s not in SupportedGCEZones", desc, zone)
			}
		}

		// Zones should have no duplicates.
		seen := make(map[string]bool)
		for _, zone := range gceOpts.Zones {
			require.False(t, seen[zone],
				"%s: duplicate zone %s", desc, zone)
			seen[zone] = true
		}

		// Node count within bounds.
		require.GreaterOrEqual(t, cfg.NumNodes, minNodes,
			"%s: node count below minimum", desc)
		require.LessOrEqual(t, cfg.NumNodes, maxNodes,
			"%s: node count above maximum", desc)

		// Lifetime within bounds and whole hours.
		hours := cfg.CreateOpts.Lifetime.Hours()
		require.GreaterOrEqual(t, hours, float64(minLifetimeHours),
			"%s: lifetime below minimum", desc)
		require.LessOrEqual(t, hours, float64(maxLifetimeHours),
			"%s: lifetime above maximum", desc)
		require.Equal(t, 0, int(cfg.CreateOpts.Lifetime%time.Hour),
			"%s: lifetime should be whole hours", desc)

		// Filesystem must be one of the supported options.
		validFS := false
		for _, fs := range SupportedFilesystems {
			if cfg.CreateOpts.SSDOpts.FileSystem == fs {
				validFS = true
				break
			}
		}
		require.True(t, validFS,
			"%s: filesystem %s not in SupportedFilesystems", desc, cfg.CreateOpts.SSDOpts.FileSystem)

		// At least one zone should always be set.
		require.NotEmpty(t, gceOpts.Zones,
			"%s: zones should not be empty", desc)
	}
}

// TestFilterPDTypes verifies filterPDTypes correctly filters storage types.
func TestFilterPDTypes(t *testing.T) {
	tests := []struct {
		name  string
		input []string
		want  []string
	}{{
		name:  "mixed types",
		input: []string{"local-ssd", "pd-ssd", "pd-balanced", "hyperdisk-balanced"},
		want:  []string{"pd-ssd", "pd-balanced", "hyperdisk-balanced"},
	}, {
		name:  "includes pd-extreme",
		input: []string{"pd-ssd", "pd-extreme", "pd-balanced"},
		want:  []string{"pd-ssd", "pd-extreme", "pd-balanced"},
	}, {
		name:  "empty input",
		input: []string{},
		want:  nil,
	}, {
		name:  "nil input",
		input: nil,
		want:  nil,
	}, {
		name:  "no matching prefixes",
		input: []string{"local-ssd", "something-else"},
		want:  nil,
	}, {
		name:  "only pd-extreme",
		input: []string{"pd-extreme"},
		want:  []string{"pd-extreme"},
	}, {
		name:  "hyperdisk variants",
		input: []string{"hyperdisk-balanced", "hyperdisk-extreme", "hyperdisk-throughput"},
		want:  []string{"hyperdisk-balanced", "hyperdisk-extreme", "hyperdisk-throughput"},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterPDTypes(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestToCreateArgs verifies argument construction from a known config.
func TestToCreateArgs(t *testing.T) {
	t.Run("local SSD", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "n2-standard-4"
		providerOpts.UseSpot = true
		providerOpts.Zones = []string{"us-east1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = true
		createOpts.SSDOpts.FileSystem = vm.Ext4
		createOpts.Lifetime = 3 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     3,
		}

		args := cfg.ToCreateArgs("test-cluster")
		require.Contains(t, args, "create")
		require.Contains(t, args, "test-cluster")
		require.Contains(t, args, "--local-ssd")
		require.NotContains(t, args, "--local-ssd=false")
		require.Contains(t, args, "--gce-use-spot")
		require.Contains(t, args, "--arch")
	})

	t.Run("persistent disk", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "n2-standard-4"
		providerOpts.PDVolumeType = "pd-ssd"
		providerOpts.PDVolumeSize = 500
		providerOpts.Zones = []string{"us-east1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = false
		createOpts.SSDOpts.FileSystem = vm.Ext4
		createOpts.Lifetime = 2 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     1,
		}

		args := cfg.ToCreateArgs("pd-cluster")
		require.Contains(t, args, "--local-ssd=false")
		require.Contains(t, args, "--gce-pd-volume-type")
		require.Contains(t, args, "pd-ssd")
		require.Contains(t, args, "--gce-pd-volume-size")
		require.Contains(t, args, "500")
	})

	t.Run("boot disk only with cron", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "c2-standard-4"
		providerOpts.BootDiskOnly = true
		providerOpts.BootDiskType = "pd-balanced"
		providerOpts.EnableCron = true
		providerOpts.Zones = []string{"us-central1-a", "us-east1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "fips"
		createOpts.SSDOpts.UseLocalSSD = false
		createOpts.SSDOpts.FileSystem = vm.Zfs
		createOpts.Lifetime = 1 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     5,
		}

		args := cfg.ToCreateArgs("boot-cluster")
		require.Contains(t, args, "--gce-boot-disk-only")
		require.Contains(t, args, "--gce-boot-disk-type")
		require.Contains(t, args, "pd-balanced")
		require.Contains(t, args, "--gce-enable-cron")
		require.Contains(t, args, "--gce-zones")
		require.Contains(t, args, "us-central1-a,us-east1-b")
		require.Contains(t, args, "fips")
	})

	t.Run("hyperdisk persistent disk", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "c4-standard-4"
		providerOpts.PDVolumeType = "hyperdisk-balanced"
		providerOpts.PDVolumeSize = 300
		providerOpts.Zones = []string{"us-west1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = false
		createOpts.SSDOpts.FileSystem = vm.Ext4
		createOpts.Lifetime = 2 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     2,
		}

		args := cfg.ToCreateArgs("hyperdisk-cluster")
		require.Contains(t, args, "--local-ssd=false")
		require.Contains(t, args, "--gce-pd-volume-type")
		require.Contains(t, args, "hyperdisk-balanced")
		require.Contains(t, args, "--gce-pd-volume-size")
		require.Contains(t, args, "300")
		require.Contains(t, args, "--gce-machine-type")
		require.Contains(t, args, "c4-standard-4")
	})

	t.Run("pd-extreme with provisioned IOPS", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "n2-standard-8"
		providerOpts.PDVolumeType = "pd-extreme"
		providerOpts.PDVolumeSize = 500
		providerOpts.PDVolumeProvisionedIOPS = 15000
		providerOpts.Zones = []string{"us-east1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = false
		createOpts.SSDOpts.FileSystem = vm.Ext4
		createOpts.Lifetime = 3 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     2,
		}

		args := cfg.ToCreateArgs("pd-extreme-cluster")
		require.Contains(t, args, "--gce-pd-volume-type")
		require.Contains(t, args, "pd-extreme")
		require.Contains(t, args, "--gce-pd-volume-provisioned-iops")
		require.Contains(t, args, "15000")
		require.NotContains(t, args, "--gce-pd-volume-provisioned-throughput")
		require.Contains(t, args, "--gce-use-bulk-insert=false")
	})

	t.Run("hyperdisk-balanced with provisioned IOPS and throughput", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "c4-standard-4"
		providerOpts.PDVolumeType = "hyperdisk-balanced"
		providerOpts.PDVolumeSize = 300
		providerOpts.PDVolumeProvisionedIOPS = 4000
		providerOpts.PDVolumeProvisionedThroughput = 200
		providerOpts.Zones = []string{"us-west1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = false
		createOpts.SSDOpts.FileSystem = vm.Ext4
		createOpts.Lifetime = 2 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     2,
		}

		args := cfg.ToCreateArgs("hd-provisioned-cluster")
		require.Contains(t, args, "--gce-pd-volume-provisioned-iops")
		require.Contains(t, args, "4000")
		require.Contains(t, args, "--gce-pd-volume-provisioned-throughput")
		require.Contains(t, args, "200")
		require.Contains(t, args, "--gce-use-bulk-insert=false")
	})

	t.Run("lssd machine with local SSD", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "c3-standard-4-lssd"
		providerOpts.Zones = []string{"us-central1-a"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = true
		createOpts.SSDOpts.FileSystem = vm.Xfs
		createOpts.Lifetime = 4 * time.Hour

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     3,
		}

		args := cfg.ToCreateArgs("lssd-cluster")
		require.Contains(t, args, "--local-ssd")
		require.NotContains(t, args, "--local-ssd=false")
		require.Contains(t, args, "--gce-machine-type")
		require.Contains(t, args, "c3-standard-4-lssd")
		require.Contains(t, args, "--filesystem")
		require.Contains(t, args, "xfs")
	})
}

// TestRandomizedClusterConfigString verifies the human-readable output.
func TestRandomizedClusterConfigString(t *testing.T) {
	t.Run("local SSD with spot", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "n2-standard-8"
		providerOpts.UseSpot = true
		providerOpts.Zones = []string{"us-east1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		createOpts.SSDOpts.UseLocalSSD = true
		createOpts.SSDOpts.FileSystem = vm.Ext4

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     3,
		}

		s := cfg.String()
		require.Contains(t, s, "3 nodes")
		require.Contains(t, s, "n2-standard-8")
		require.Contains(t, s, "amd64")
		require.Contains(t, s, "local-ssd")
		require.Contains(t, s, "(spot)")
	})

	t.Run("persistent disk", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "c2-standard-4"
		providerOpts.PDVolumeType = "pd-ssd"
		providerOpts.PDVolumeSize = 200
		providerOpts.Zones = []string{"us-west1-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "arm64"
		createOpts.SSDOpts.UseLocalSSD = false
		createOpts.SSDOpts.FileSystem = vm.Zfs

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     1,
		}

		s := cfg.String()
		require.Contains(t, s, "1 nodes")
		require.Contains(t, s, "pd-ssd-200GB")
		require.Contains(t, s, "arm64")
		require.NotContains(t, s, "(spot)")
	})

	t.Run("boot disk only with cron", func(t *testing.T) {
		providerOpts := gce.DefaultProviderOpts()
		providerOpts.MachineType = "n2-standard-4"
		providerOpts.BootDiskOnly = true
		providerOpts.BootDiskType = "pd-balanced"
		providerOpts.EnableCron = true
		providerOpts.Zones = []string{"europe-west2-b"}

		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "fips"
		createOpts.SSDOpts.FileSystem = vm.Ext4

		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: providerOpts,
			NumNodes:     2,
		}

		s := cfg.String()
		require.Contains(t, s, "boot-disk-only(pd-balanced)")
		require.Contains(t, s, "(cron)")
	})

	t.Run("non-GCE provider opts", func(t *testing.T) {
		createOpts := vm.DefaultCreateOpts()
		createOpts.Arch = "amd64"
		cfg := &RandomizedClusterConfig{
			CreateOpts:   createOpts,
			ProviderOpts: &mockProviderOpts{},
			NumNodes:     1,
		}
		s := cfg.String()
		require.Contains(t, s, "unknown provider")
	})
}

// TestRunResultSuccess verifies the Success() method on RunResult.
func TestRunResultSuccess(t *testing.T) {
	tests := []struct {
		name string
		r    RunResult
		want bool
	}{{
		name: "success",
		r:    RunResult{ExitCode: 0, Err: nil},
		want: true,
	}, {
		name: "non-zero exit",
		r:    RunResult{ExitCode: 1, Err: nil},
		want: false,
	}, {
		name: "error",
		r:    RunResult{ExitCode: 0, Err: errors.New("timeout")},
		want: false,
	}, {
		name: "both",
		r:    RunResult{ExitCode: 2, Err: errors.New("signal")},
		want: false,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.r.Success())
		})
	}
}

// mockProviderOpts implements vm.ProviderOpts for testing the String()
// fallback path when ProviderOpts is not *gce.ProviderOpts.
type mockProviderOpts struct{}

func (m mockProviderOpts) ConfigureCreateFlags(*pflag.FlagSet) {}
