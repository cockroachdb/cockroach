// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// createconfig.go generates randomized but valid cluster configurations for
// testing. RandomGCECreateOptions picks a machine type, then derives compatible
// settings (architecture, local SSD support, storage types, zones) from gcedb
// to avoid incompatible flag combinations. Configurations are reproducible via
// a seeded RNG.
package framework

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce/gcedb"
)

// Randomization probabilities and ranges for cluster configuration.
// These control the distribution of various options in RandomGCECreateOptions.
const (
	// ========================================
	// Cloud-agnostic randomization settings
	// ========================================

	// When the machine type is amd64, probability of using FIPS mode.
	probFIPSOnAMD64 = 0.05

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

	// Provisioned IOPS for pd-extreme (always set when pd-extreme is selected)
	minPDExtremeIOPS       = 10000
	maxPDExtremeIOPS       = 20000
	pdExtremeIOPSIncrement = 1000

	// Provisioned IOPS/throughput for hyperdisk-balanced (optional)
	probHyperdiskProvisioned     = 0.30 // 30% chance of setting IOPS/throughput
	minHyperdiskIOPS             = 3000
	maxHyperdiskIOPS             = 6000
	hyperdiskIOPSIncrement       = 1000
	minHyperdiskThroughputMiBps  = 140
	maxHyperdiskThroughputMiBps  = 290
	hyperdiskThroughputIncrement = 10
)

// RandomizedClusterConfig wraps vm.CreateOpts and vm.ProviderOpts along with
// cluster-level settings (like node count) for randomized testing.
// This struct is cloud-provider agnostic and can be used with GCE, AWS, Azure, etc.
type RandomizedClusterConfig struct {
	CreateOpts   vm.CreateOpts
	ProviderOpts vm.ProviderOpts
	NumNodes     int
}

// SupportedGCEMachineTypes lists GCE machine types used in randomized tests.
// Includes both amd64 and arm64 types across multiple families to exercise
// different storage profiles (pd, hyperdisk, mixed) and local SSD models
// (variable count, fixed -lssd count, none). Architecture is derived from
// gcedb at randomization time, so there is no need to maintain a separate
// arch list.
// This is a curated subset of the full GCE machine type catalog in gcedb.go,
// chosen to exercise distinct roachprod code paths (storage profiles, local SSD
// models, architectures) rather than exhaustively cover every GCE family.
// Each type is validated against gcedb at runtime by RandomGCECreateOptions.
// Families like GPU/accelerator (A2, A3, G2), memory-optimized (M1-M4), and
// legacy (N1, E2, F1, G1) are excluded because they don't exercise additional
// roachprod code paths beyond what's already covered here.
var SupportedGCEMachineTypes = []string{
	// amd64 - N2 (Intel Cascade Lake / Ice Lake)
	"n2-standard-4", // Roachprod GCE Default
	"n2-standard-8",
	"n2-standard-16",
	"n2-highcpu-4",
	"n2-highcpu-8",
	"n2-highcpu-16",
	// amd64 - N2D (AMD EPYC Milan)
	"n2d-standard-4",
	// amd64 - C2 (Intel Cascade Lake, compute-optimized)
	"c2-standard-4",
	"c2-standard-8",
	// amd64 - C3 (Intel Sapphire Rapids, pd+hyperdisk storage)
	"c3-standard-4",
	"c3-standard-4-lssd",
	// amd64 - C4 (Intel Emerald/Granite Rapids, hyperdisk-only)
	"c4-standard-4",
	// amd64 - C4D (AMD Turin, hyperdisk-only)
	"c4d-standard-4",
	// arm64 - T2A (Ampere Altra)
	"t2a-standard-4",
	"t2a-standard-8",
	// arm64 - C4A (Google Axion, hyperdisk-only)
	"c4a-standard-4",
}

// pdTypePrefixes lists prefixes that identify persistent/hyperdisk volume types
// in a gcedb StorageTypes list. Used by filterPDTypes to select only disk types
// that can be used as PD volumes or boot disks.
var pdTypePrefixes = []string{"pd-", "hyperdisk-"}

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
	vm.Xfs,
	vm.F2fs,
	vm.Btrfs,
}

// excludedPDTypes lists persistent disk types that should not be picked during
// randomization. Previously excluded pd-extreme, which is now supported via
// provisioned IOPS randomization in RandomGCECreateOptions.
var excludedPDTypes = map[string]bool{}

// RandomGCECreateOptions generates a random valid GCE create configuration
// using the provided random number generator. It uses gcedb.GetMachineInfo
// to derive compatible settings (architecture, local SSD, storage types)
// from the chosen machine type, preventing incompatible argument combinations.
func RandomGCECreateOptions(rng *rand.Rand) *RandomizedClusterConfig {
	createOpts := vm.DefaultCreateOpts()
	providerOpts := *gce.DefaultProviderOpts()

	// 1. Pick machine type first — everything else derives from it.
	providerOpts.MachineType = SupportedGCEMachineTypes[rng.Intn(len(SupportedGCEMachineTypes))]
	info, err := gcedb.GetMachineInfo(providerOpts.MachineType)
	if err != nil {
		panic(fmt.Sprintf("unsupported machine type in SupportedGCEMachineTypes: %s: %v",
			providerOpts.MachineType, err))
	}

	// 2. Architecture: derived from machine type. FIPS only on amd64.
	createOpts.Arch = info.Architecture
	if info.Architecture == gcedb.ArchAMD64 && rng.Float32() < probFIPSOnAMD64 {
		createOpts.Arch = "fips"
	}

	// 3. Local SSD: only enable when the machine type supports it.
	if len(info.AllowedLocalSSDCount) > 0 {
		createOpts.SSDOpts.UseLocalSSD = rng.Float32() < probLocalSSD
	} else {
		createOpts.SSDOpts.UseLocalSSD = false
	}

	// 4. Persistent disk config (when not using local SSD).
	pdTypes := filterPDTypes(info.StorageTypes)
	if !createOpts.SSDOpts.UseLocalSSD && len(pdTypes) > 0 {
		providerOpts.PDVolumeType = pdTypes[rng.Intn(len(pdTypes))]
		sizeIncrements := (maxPDSizeGB - minPDSizeGB) / pdSizeIncrementGB
		providerOpts.PDVolumeSize = minPDSizeGB + (rng.Intn(sizeIncrements+1) * pdSizeIncrementGB)

		// 4a. Provisioned IOPS/throughput for disk types that need it.
		switch providerOpts.PDVolumeType {
		case "pd-extreme":
			iopsIncrements := (maxPDExtremeIOPS - minPDExtremeIOPS) / pdExtremeIOPSIncrement
			providerOpts.PDVolumeProvisionedIOPS = minPDExtremeIOPS + (rng.Intn(iopsIncrements+1) * pdExtremeIOPSIncrement)
		case "hyperdisk-balanced":
			if rng.Float32() < probHyperdiskProvisioned {
				iopsIncrements := (maxHyperdiskIOPS - minHyperdiskIOPS) / hyperdiskIOPSIncrement
				providerOpts.PDVolumeProvisionedIOPS = minHyperdiskIOPS + (rng.Intn(iopsIncrements+1) * hyperdiskIOPSIncrement)
				tpIncrements := (maxHyperdiskThroughputMiBps - minHyperdiskThroughputMiBps) / hyperdiskThroughputIncrement
				providerOpts.PDVolumeProvisionedThroughput = minHyperdiskThroughputMiBps + (rng.Intn(tpIncrements+1) * hyperdiskThroughputIncrement)
			}
		}
	}

	// 5. Random lifetime (whole hours).
	lifetimeHours := minLifetimeHours + rng.Intn(maxLifetimeHours-minLifetimeHours+1)
	createOpts.Lifetime = time.Duration(lifetimeHours) * time.Hour

	// 6. Random filesystem.
	createOpts.SSDOpts.FileSystem = SupportedFilesystems[rng.Intn(len(SupportedFilesystems))]

	// 7. Spot instance usage.
	providerOpts.UseSpot = rng.Float32() < probSpotInstance

	// 8. Boot disk only (no additional volumes).
	// Skip when local SSD is enabled — the two are incompatible since
	// boot-disk-only means no additional volumes (including local SSDs).
	if !createOpts.SSDOpts.UseLocalSSD {
		providerOpts.BootDiskOnly = rng.Float32() < probBootDiskOnly
	}
	if providerOpts.BootDiskOnly && len(pdTypes) > 0 {
		providerOpts.BootDiskType = pdTypes[rng.Intn(len(pdTypes))]
	}

	// 9. Cron service.
	providerOpts.EnableCron = rng.Float32() < probEnableCron

	// 10. Zones: T2A machine types have restricted zone availability.
	availableZones := SupportedGCEZones
	if strings.HasPrefix(providerOpts.MachineType, "t2a-") {
		availableZones = gce.SupportedT2AZones
	}
	if rng.Float32() < probMultiZone {
		numZones := 2 + rng.Intn(2)
		if numZones > len(availableZones) {
			numZones = len(availableZones)
		}
		zones := make([]string, numZones)
		picked := make(map[int]bool)
		for i := 0; i < numZones; i++ {
			idx := rng.Intn(len(availableZones))
			for picked[idx] {
				idx = rng.Intn(len(availableZones))
			}
			picked[idx] = true
			zones[i] = availableZones[idx]
		}
		providerOpts.Zones = zones
	} else {
		providerOpts.Zones = []string{availableZones[rng.Intn(len(availableZones))]}
	}

	return &RandomizedClusterConfig{
		CreateOpts:   createOpts,
		ProviderOpts: &providerOpts,
		NumNodes:     minNodes + rng.Intn(maxNodes-minNodes+1),
	}
}

// filterPDTypes returns persistent/hyperdisk volume types from a gcedb
// StorageTypes list, excluding types in excludedPDTypes (e.g. pd-extreme).
func filterPDTypes(storageTypes []string) []string {
	var result []string
	for _, st := range storageTypes {
		if excludedPDTypes[st] {
			continue
		}
		for _, prefix := range pdTypePrefixes {
			if strings.HasPrefix(st, prefix) {
				result = append(result, st)
				break
			}
		}
	}
	return result
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
		if gceOpts.PDVolumeProvisionedIOPS > 0 || gceOpts.PDVolumeProvisionedThroughput > 0 {
			// BulkInsert API silently ignores provisioned IOPS/throughput
			// (create_sdk.go only sets DiskSizeGb and DiskType). Force the CLI
			// path which correctly emits provisioned-iops/throughput in
			// --create-disk args.
			args = append(args, "--gce-use-bulk-insert=false")
		}
		if gceOpts.PDVolumeProvisionedIOPS > 0 {
			args = append(args, "--gce-pd-volume-provisioned-iops",
				fmt.Sprintf("%d", gceOpts.PDVolumeProvisionedIOPS))
		}
		if gceOpts.PDVolumeProvisionedThroughput > 0 {
			args = append(args, "--gce-pd-volume-provisioned-throughput",
				fmt.Sprintf("%d", gceOpts.PDVolumeProvisionedThroughput))
		}
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
		args = append(args, "--gce-zones", strings.Join(gceOpts.Zones, ","))
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
		if gceOpts.PDVolumeProvisionedIOPS > 0 && gceOpts.PDVolumeProvisionedThroughput > 0 {
			storage += fmt.Sprintf(" (%d IOPS, %d MiB/s)",
				gceOpts.PDVolumeProvisionedIOPS, gceOpts.PDVolumeProvisionedThroughput)
		} else if gceOpts.PDVolumeProvisionedIOPS > 0 {
			storage += fmt.Sprintf(" (%d IOPS)", gceOpts.PDVolumeProvisionedIOPS)
		}
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
