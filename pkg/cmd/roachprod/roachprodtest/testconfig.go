// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodtest

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
)

// Randomization probabilities and ranges for cluster configuration.
// These control the distribution of various options in RandomGCECreateOptions.
const (
	// ========================================
	// Cloud-agnostic randomization settings
	// ========================================

	// Architecture distribution (cumulative probabilities)
	probArchAMD64 = 0.70 // 70% AMD64
	probArchARM64 = 0.95 // 25% ARM64 (0.95 - 0.70)
	// Remaining 5% is FIPS (amd64 with openssl)

	// Storage: local SSD vs persistent disk
	probLocalSSD = 0.50 // 50% chance of using local SSD vs persistent disk

	// Node count range
	minNodes = 1
	maxNodes = 5

	// Lifetime range (in hours)
	minLifetimeHours = 1
	maxLifetimeHours = 12

	// ========================================
	// GCE-specific randomization settings
	// ========================================

	// Instance options
	probSpotInstance = 0.90 // 90% chance of using spot instances (cost optimization)
	probBootDiskOnly = 0.05 // 5% chance of boot disk only (no additional volumes)
	probEnableCron   = 0.05 // 5% chance of enabling cron service

	// Zone distribution
	probMultiZone = 0.30 // 30% chance of multi-zone cluster vs single-zone

	// Persistent disk size range (when not using local SSD)
	minPDSizeGB       = 100
	maxPDSizeGB       = 2000
	pdSizeIncrementGB = 100
)

// RandomizedClusterConfig wraps vm.CreateOpts and vm.ProviderOpts along with
// cluster-level settings (like node count) for randomized testing.
// This struct is cloud-provider agnostic and can be used with GCE, AWS, Azure, etc.
type RandomizedClusterConfig struct {
	CreateOpts   vm.CreateOpts
	ProviderOpts vm.ProviderOpts
	NumNodes     int
}

// SupportedGCEMachineTypes lists commonly used GCE machine types
// https://docs.cloud.google.com/compute/docs/machine-resource
// TODO: Which Machine types are people / systems using?
// TODO: edit this list?
// Note: Some machine types are not compatible with other settings
// Will either need to validate the machine type and other settings before hand (meh seems like a lot)
// Or just let roachprod fail
// I'm liking that negative testing scenario... but how to report that
// Maybe just catch that specific error... but what if that's masking a scenario in which we expect cluster creation
// to succeed but it actually fails.... I guess just report the failure for now, make the error message clear
// Maybe an LLM can verify these cases i.e. input is the settings and failure and just have it verify
var SupportedGCEMachineTypes = []string{
	"n2-standard-4", // Roachprod GCE Default
	"n2-standard-8",
	"n2-standard-16",
	"n2-highcpu-4",
	"n2-highcpu-8",
	"n2-highcpu-16",
	"c2-standard-4",
	"c2-standard-8",
}

// SupportedGCEPDVolumeTypes lists persistent disk types used in randomized tests
// Type of the persistent disk volume, only used if local-ssd=false
// Supported: pd-ssd, pd-balanced, pd-extreme, pd-standard, hyperdisk-balanced
// TODO: anything else to be added here?
var SupportedGCEPDVolumeTypes = []string{
	"pd-ssd",
	"pd-balanced",
	"pd-extreme",
	"pd-standard",
	// https://docs.cloud.google.com/compute/docs/disks/hyperdisks
	"hyperdisk-balanced", // Only on certain machine types
}

// SupportedGCEZones lists commonly used GCE zones for randomized tests
// TODO: consider adding more zones
var SupportedGCEZones = []string{
	"us-east1-b",
	"us-west1-b",
	"europe-west2-b",
	"us-central1-a",
}

// SupportedFilesystems lists filesystems used in randomized tests
var SupportedFilesystems = []vm.Filesystem{
	vm.Ext4,
	vm.Zfs,
}

// SupportedArchitectures lists architectures used in randomized tests
var SupportedArchitectures = []string{
	"amd64",
	"arm64",
	"fips", // fips implies amd64 with openssl
}

// RandomGCECreateOptions generates a random valid GCE create configuration
// using the provided random number generator.
func RandomGCECreateOptions(rng *rand.Rand) *RandomizedClusterConfig {
	// Start with defaults
	createOpts := vm.DefaultCreateOpts()
	providerOpts := *gce.DefaultProviderOpts()

	// Weighted architecture selection
	archRoll := rng.Float32()
	if archRoll < probArchAMD64 {
		createOpts.Arch = "amd64"
	} else if archRoll < probArchARM64 {
		createOpts.Arch = "arm64"
	} else {
		createOpts.Arch = "fips"
	}

	// Randomize machine type
	providerOpts.MachineType = SupportedGCEMachineTypes[rng.Intn(len(SupportedGCEMachineTypes))]

	// Random lifetime (whole hours only to avoid precision issues)
	lifetimeHours := minLifetimeHours + rng.Intn(maxLifetimeHours-minLifetimeHours+1)
	createOpts.Lifetime = time.Duration(lifetimeHours) * time.Hour

	// Random filesystem
	createOpts.SSDOpts.FileSystem = SupportedFilesystems[rng.Intn(len(SupportedFilesystems))]

	// Local SSD vs persistent disk
	createOpts.SSDOpts.UseLocalSSD = rng.Float32() < probLocalSSD

	// If not using local SSD, configure persistent disk
	if !createOpts.SSDOpts.UseLocalSSD {
		providerOpts.PDVolumeType = SupportedGCEPDVolumeTypes[rng.Intn(len(SupportedGCEPDVolumeTypes))]
		// Random size in increments
		sizeIncrements := (maxPDSizeGB - minPDSizeGB) / pdSizeIncrementGB
		providerOpts.PDVolumeSize = minPDSizeGB + (rng.Intn(sizeIncrements+1) * pdSizeIncrementGB)
	}

	// Spot instance usage
	providerOpts.UseSpot = rng.Float32() < probSpotInstance

	// Boot disk only (no additional volumes)
	providerOpts.BootDiskOnly = rng.Float32() < probBootDiskOnly
	if providerOpts.BootDiskOnly {
		// Randomize boot disk type when using boot disk only
		providerOpts.BootDiskType = SupportedGCEPDVolumeTypes[rng.Intn(len(SupportedGCEPDVolumeTypes))]
	}

	// Cron service
	providerOpts.EnableCron = rng.Float32() < probEnableCron

	// Single zone vs multi-zone
	if rng.Float32() < probMultiZone {
		// Multi-zone: pick 2-3 zones
		numZones := 2 + rng.Intn(2)
		zones := make([]string, numZones)
		picked := make(map[int]bool)
		for i := 0; i < numZones; i++ {
			idx := rng.Intn(len(SupportedGCEZones))
			for picked[idx] {
				idx = rng.Intn(len(SupportedGCEZones))
			}
			picked[idx] = true
			zones[i] = SupportedGCEZones[idx]
		}
		providerOpts.Zones = zones
	} else {
		// Single zone
		providerOpts.Zones = []string{SupportedGCEZones[rng.Intn(len(SupportedGCEZones))]}
	}

	return &RandomizedClusterConfig{
		CreateOpts:   createOpts,
		ProviderOpts: &providerOpts,
		NumNodes:     minNodes + rng.Intn(maxNodes-minNodes+1),
	}
}

// ToCreateArgs converts RandomizedClusterConfig to roachprod create command arguments.
// Currently only supports GCE provider options.
// TODO is it worth setting flags as their default values? Seems like more of a strictly property testing based thing
// Could do it here, but is it worth? At that point only testing the CLI arg parsing logic, so nah
func (cfg *RandomizedClusterConfig) ToCreateArgs(clusterName string) []string {
	// Type assert to GCE-specific provider options
	gceOpts, ok := cfg.ProviderOpts.(*gce.ProviderOpts)
	if !ok {
		panic("ToCreateArgs currently only supports GCE provider options")
	}

	args := []string{
		"create", clusterName,
		"-n", fmt.Sprintf("%d", cfg.NumNodes),
		"--clouds", "gce",
		"--lifetime", cfg.CreateOpts.Lifetime.String(),
		"--gce-machine-type", gceOpts.MachineType,
		"--filesystem", string(cfg.CreateOpts.SSDOpts.FileSystem),
	}

	if cfg.CreateOpts.Arch != "" {
		args = append(args, "--arch", cfg.CreateOpts.Arch)
	}

	if cfg.CreateOpts.SSDOpts.UseLocalSSD {
		args = append(args, "--local-ssd")
	} else {
		args = append(args,
			"--local-ssd=false",
			"--gce-pd-volume-type", gceOpts.PDVolumeType,
			"--gce-pd-volume-size", fmt.Sprintf("%d", gceOpts.PDVolumeSize),
		)
	}

	if gceOpts.UseSpot {
		args = append(args, "--gce-use-spot")
	}

	if gceOpts.BootDiskOnly {
		args = append(args, "--gce-boot-disk-only")
		if gceOpts.BootDiskType != "" {
			args = append(args, "--gce-boot-disk-type", gceOpts.BootDiskType)
		}
	}

	if gceOpts.EnableCron {
		args = append(args, "--gce-enable-cron")
	}

	if len(gceOpts.Zones) > 0 {
		args = append(args, "--gce-zones", joinZones(gceOpts.Zones))
	}

	return args
}

// String returns a human-readable description of the configuration.
// Currently only supports GCE provider options.
func (cfg *RandomizedClusterConfig) String() string {
	// Type assert to GCE-specific provider options
	gceOpts, ok := cfg.ProviderOpts.(*gce.ProviderOpts)
	if !ok {
		return fmt.Sprintf("%d nodes, %s, unknown provider", cfg.NumNodes, cfg.CreateOpts.Arch)
	}

	storage := "local-ssd"
	if gceOpts.BootDiskOnly {
		storage = fmt.Sprintf("boot-disk-only(%s)", gceOpts.BootDiskType)
	} else if !cfg.CreateOpts.SSDOpts.UseLocalSSD {
		storage = fmt.Sprintf("%s-%dGB", gceOpts.PDVolumeType, gceOpts.PDVolumeSize)
	}

	spotStr := ""
	if gceOpts.UseSpot {
		spotStr = " (spot)"
	}

	cronStr := ""
	if gceOpts.EnableCron {
		cronStr = " (cron)"
	}

	return fmt.Sprintf("%d nodes, %s, %s, %s, %s%s%s, zones=%v",
		cfg.NumNodes, gceOpts.MachineType, cfg.CreateOpts.Arch, storage,
		cfg.CreateOpts.SSDOpts.FileSystem, spotStr, cronStr, gceOpts.Zones)
}

// joinZones formats zones for the --gce-zones flag
func joinZones(zones []string) string {
	result := ""
	for i, z := range zones {
		if i > 0 {
			result += ","
		}
		result += z
	}
	return result
}
