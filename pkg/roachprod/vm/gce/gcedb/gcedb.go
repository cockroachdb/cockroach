// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package gcedb provides a static knowledge database for GCE machine types.
// It returns machine specifications based on machine type names.
//
// Data sources:
//   - https://docs.cloud.google.com/compute/docs/machine-resource
//   - https://docs.cloud.google.com/compute/docs/general-purpose-machines
//   - https://docs.cloud.google.com/compute/docs/compute-optimized-machines
//   - https://docs.cloud.google.com/compute/docs/memory-optimized-machines
//   - https://docs.cloud.google.com/compute/docs/accelerator-optimized-machines
//   - https://docs.cloud.google.com/compute/docs/disks/local-ssd
package gcedb

import (
	"fmt"
	"strconv"
	"strings"
)

// Architecture constants.
const (
	ArchAMD64 = "amd64"
	ArchARM64 = "arm64"
)

// CPU Platform constants - exact names from GCE documentation.
// Reference: https://docs.cloud.google.com/compute/docs/instances/specify-min-cpu-platform
const (
	// Intel platforms (oldest to newest).
	PlatformSandyBridge    = "Intel Sandy Bridge"
	PlatformIvyBridge      = "Intel Ivy Bridge"
	PlatformHaswell        = "Intel Haswell"
	PlatformBroadwell      = "Intel Broadwell"
	PlatformSkylake        = "Intel Skylake"
	PlatformCascadeLake    = "Intel Cascade Lake"
	PlatformIceLake        = "Intel Ice Lake"
	PlatformSapphireRapids = "Intel Sapphire Rapids"
	PlatformEmeraldRapids  = "Intel Emerald Rapids"
	PlatformGraniteRapids  = "Intel Granite Rapids"

	// AMD platforms (oldest to newest).
	PlatformRome  = "AMD Rome"
	PlatformMilan = "AMD Milan"
	PlatformGenoa = "AMD Genoa"
	PlatformTurin = "AMD Turin"

	// ARM platforms.
	PlatformAmpereAltra = "Ampere Altra"
	PlatformAxion       = "Google Axion"
)

// MachineInfo contains the specifications for a GCE machine type.
type MachineInfo struct {
	// Number of CPU cores. Always at least 1, even for shared-core machines.
	CPUCores int
	// Amount of memory in GiB (2^30 bytes). Always at least 1.
	MemoryGiB int
	// CPU architecture: "amd64" or "arm64".
	Architecture string
	// Allowed number of local SSDs, in ascending order; empty if local SSD not
	// supported.
	AllowedLocalSSDCount []int
	// List of allowed storage types, e.g. "pd-ssd", "hyperdisk-balanced".
	StorageTypes []string
	// CPU platforms supported by this machine type, in oldest-to-newest order.
	// Empty if documentation doesn't specify exact platforms (e.g., E2).
	CPUPlatforms []string
}

// String returns a human-readable representation of MachineInfo.
func (m MachineInfo) String() string {
	return fmt.Sprintf("CPUCores: %d\nMemoryGiB: %d\nArchitecture: %s\nAllowedLocalSSDCount: %s\nStorageTypes: %s\nCPUPlatforms: %s",
		m.CPUCores, m.MemoryGiB, m.Architecture,
		formatIntSlice(m.AllowedLocalSSDCount),
		formatStringSlice(m.StorageTypes),
		formatStringSlice(m.CPUPlatforms))
}

// formatIntSlice formats an int slice as {1, 2, 4}.
func formatIntSlice(s []int) string {
	if len(s) == 0 {
		return "{}"
	}
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = strconv.Itoa(v)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// formatStringSlice formats a string slice as (x, y, z).
func formatStringSlice(s []string) string {
	if len(s) == 0 {
		return "()"
	}
	return "(" + strings.Join(s, ", ") + ")"
}

// Storage type constants.
var (
	// Standard persistent disk types available on older machine families.
	storagePDStandard = []string{"pd-standard", "pd-balanced", "pd-ssd"}
	// Full persistent disk types including extreme.
	storagePDFull = []string{"pd-standard", "pd-balanced", "pd-ssd", "pd-extreme"}
	// N2/N2D support: full PD plus hyperdisk-balanced and hyperdisk-throughput.
	storageN2 = []string{"pd-standard", "pd-balanced", "pd-ssd", "pd-extreme", "hyperdisk-balanced", "hyperdisk-throughput"}
	// C3/C3D support: pd-balanced, pd-ssd plus hyperdisk types.
	storageC3 = []string{"pd-balanced", "pd-ssd", "hyperdisk-balanced", "hyperdisk-extreme", "hyperdisk-throughput"}
	// Hyperdisk-only for newer machines (N4, C4, etc.).
	storageHyperdiskOnly = []string{"hyperdisk-balanced", "hyperdisk-throughput"}
	// Memory-optimized M3 storage.
	storageM3 = []string{"pd-balanced", "pd-ssd", "hyperdisk-balanced", "hyperdisk-extreme", "hyperdisk-throughput"}
	// Memory-optimized M2 storage.
	storageM2 = []string{"pd-balanced", "pd-ssd", "pd-extreme", "hyperdisk-balanced", "hyperdisk-extreme"}
	// Memory-optimized M4/X4 storage.
	storageM4 = []string{"hyperdisk-balanced", "hyperdisk-extreme"}
	// T2D storage.
	storageT2D = []string{"pd-standard", "pd-balanced", "pd-ssd", "hyperdisk-throughput"}
	// A2/G2 storage.
	storageAccelerator = []string{"pd-balanced", "pd-ssd", "hyperdisk-balanced", "hyperdisk-ml"}
)

// CPU platform slices for each machine family.
// Reference: https://docs.cloud.google.com/compute/docs/instances/specify-min-cpu-platform
var (
	// N1: supports multiple Intel generations.
	platformsN1 = []string{PlatformSandyBridge, PlatformIvyBridge, PlatformHaswell, PlatformBroadwell, PlatformSkylake}

	// N2: Cascade Lake and Ice Lake.
	platformsN2 = []string{PlatformCascadeLake, PlatformIceLake}

	// N2D: AMD Rome and Milan.
	platformsN2D = []string{PlatformRome, PlatformMilan}

	// N4: Intel Emerald Rapids only.
	platformsN4 = []string{PlatformEmeraldRapids}

	// N4D: AMD Turin.
	platformsN4D = []string{PlatformTurin}

	// N4A: Google Axion (ARM).
	platformsN4A = []string{PlatformAxion}

	// E2: Intel or AMD - auto-selected at VM creation.
	// CPUPlatforms is nil because docs don't specify exact platforms.
	platformsE2 []string = nil

	// T2A: Ampere Altra (ARM).
	platformsT2A = []string{PlatformAmpereAltra}

	// T2D: AMD Milan.
	platformsT2D = []string{PlatformMilan}

	// C2: Intel Cascade Lake.
	platformsC2 = []string{PlatformCascadeLake}

	// C2D: AMD Milan.
	platformsC2D = []string{PlatformMilan}

	// C3: Intel Sapphire Rapids.
	platformsC3 = []string{PlatformSapphireRapids}

	// C3D: AMD Genoa.
	platformsC3D = []string{PlatformGenoa}

	// C4: Intel Emerald Rapids and Granite Rapids.
	platformsC4 = []string{PlatformEmeraldRapids, PlatformGraniteRapids}

	// C4A: Google Axion (ARM).
	platformsC4A = []string{PlatformAxion}

	// C4D: AMD Turin.
	platformsC4D = []string{PlatformTurin}

	// H3: Intel Sapphire Rapids.
	platformsH3 = []string{PlatformSapphireRapids}

	// M1: Intel Broadwell/Skylake (memory-optimized).
	platformsM1 = []string{PlatformBroadwell, PlatformSkylake}

	// M2: Intel Cascade Lake.
	platformsM2 = []string{PlatformCascadeLake}

	// M3: Intel Ice Lake.
	platformsM3 = []string{PlatformIceLake}

	// M4: Intel Sapphire Rapids.
	platformsM4 = []string{PlatformSapphireRapids}

	// A2: Intel Cascade Lake.
	platformsA2 = []string{PlatformCascadeLake}

	// A3: varies by variant (see getA3MachineInfo).
	platformsA3Sapphire = []string{PlatformSapphireRapids} // High, Mega, Edge
	platformsA3Emerald  = []string{PlatformEmeraldRapids}  // Ultra

	// G2: Intel Cascade Lake.
	platformsG2 = []string{PlatformCascadeLake}
)

// Memory ratios (GiB per vCPU) for each machine family and variant.
// Reference: https://docs.cloud.google.com/compute/docs/general-purpose-machines
const (
	// N1 series: Sandy Bridge through Skylake, 1-96 vCPUs.
	n1Standard = 3.75 // n1-standard: 3.75 GiB/vCPU
	n1HighMem  = 6.5  // n1-highmem: 6.5 GiB/vCPU
	n1HighCPU  = 0.9  // n1-highcpu: 0.9 GiB/vCPU

	// N2 series: Cascade Lake/Ice Lake, 2-128 vCPUs.
	n2Standard = 4 // n2-standard: 4 GiB/vCPU
	n2HighMem  = 8 // n2-highmem: 8 GiB/vCPU
	n2HighCPU  = 1 // n2-highcpu: 1 GiB/vCPU

	// N2D series: AMD EPYC Milan, 2-224 vCPUs.
	n2dStandard = 4 // n2d-standard: 4 GiB/vCPU
	n2dHighMem  = 8 // n2d-highmem: 8 GiB/vCPU
	n2dHighCPU  = 1 // n2d-highcpu: 1 GiB/vCPU

	// N4 series: Intel Emerald Rapids, 2-80 vCPUs.
	n4Standard = 4 // n4-standard: 4 GiB/vCPU
	n4HighMem  = 8 // n4-highmem: 8 GiB/vCPU
	n4HighCPU  = 2 // n4-highcpu: 2 GiB/vCPU

	// N4D series: AMD EPYC Turin, 2-96 vCPUs.
	n4dStandard = 4 // n4d-standard: 4 GiB/vCPU
	n4dHighMem  = 8 // n4d-highmem: 8 GiB/vCPU
	n4dHighCPU  = 2 // n4d-highcpu: 2 GiB/vCPU

	// N4A series: Google Axion (Arm Neoverse N3), 2-64 vCPUs.
	n4aStandard = 4 // n4a-standard: 4 GiB/vCPU
	n4aHighMem  = 8 // n4a-highmem: 8 GiB/vCPU
	n4aHighCPU  = 2 // n4a-highcpu: 2 GiB/vCPU

	// E2 series: Intel or AMD (cost-optimized), 2-32 vCPUs.
	e2Standard = 4 // e2-standard: 4 GiB/vCPU
	e2HighMem  = 8 // e2-highmem: 8 GiB/vCPU
	e2HighCPU  = 1 // e2-highcpu: 1 GiB/vCPU

	// T2D series: AMD EPYC Milan (Tau), 1-60 vCPUs, fixed 4 GiB/vCPU.
	t2dFixed = 4
	// T2A series: Ampere Altra Arm, 1-48 vCPUs, fixed 4 GiB/vCPU.
	t2aFixed = 4

	// C2 series: Intel Cascade Lake (compute-optimized), 4-60 vCPUs.
	c2Standard = 4 // c2-standard: 4 GiB/vCPU

	// C2D series: AMD EPYC Milan, 2-112 vCPUs.
	c2dStandard = 4 // c2d-standard: 4 GiB/vCPU
	c2dHighMem  = 8 // c2d-highmem: 8 GiB/vCPU
	c2dHighCPU  = 2 // c2d-highcpu: 2 GiB/vCPU

	// C3 series: Intel Sapphire Rapids, 4-176 vCPUs.
	c3Standard = 4 // c3-standard: 4 GiB/vCPU
	c3HighMem  = 8 // c3-highmem: 8 GiB/vCPU
	c3HighCPU  = 2 // c3-highcpu: 2 GiB/vCPU

	// C3D series: AMD EPYC Genoa, 4-360 vCPUs.
	c3dStandard = 4 // c3d-standard: 4 GiB/vCPU
	c3dHighMem  = 8 // c3d-highmem: 8 GiB/vCPU
	c3dHighCPU  = 2 // c3d-highcpu: 2 GiB/vCPU

	// C4 series: Intel Granite/Emerald Rapids, 2-288 vCPUs.
	c4Standard = 3.75 // c4-standard: 3.75 GiB/vCPU
	c4HighMem  = 7.75 // c4-highmem: 7.75 GiB/vCPU
	c4HighCPU  = 2    // c4-highcpu: 2 GiB/vCPU

	// C4D series: AMD EPYC Turin, 2-384 vCPUs.
	c4dStandard = 3.1 // c4d-standard: 3.1 GiB/vCPU
	c4dHighMem  = 7.9 // c4d-highmem: 7.9 GiB/vCPU
	c4dHighCPU  = 1.9 // c4d-highcpu: 1.9 GiB/vCPU

	// M1 series: Intel Skylake/Broadwell, 40-160 vCPUs.
	m1MegaMem  = 14.9375 // m1-megamem: ~15 GiB/vCPU
	m1UltraMem = 24.025  // m1-ultramem: ~24 GiB/vCPU

	// M2 series: Intel Cascade Lake, 208-416 vCPUs.
	m2UltraMem = 28.3654 // m2-ultramem: ~28 GiB/vCPU
	m2MegaMem  = 14.25   // m2-megamem: ~14 GiB/vCPU

	// M3 series: Intel Ice Lake, 32-128 vCPUs.
	m3MegaMem  = 15.25 // m3-megamem: ~15 GiB/vCPU
	m3UltraMem = 30.5  // m3-ultramem: ~30.5 GiB/vCPU

	// M4 series: Intel Emerald Rapids, 16-224 vCPUs.
	m4MegaMem  = 14.875 // m4-megamem: ~15 GiB/vCPU
	m4UltraMem = 26.5   // m4-ultramem: ~26.5 GiB/vCPU
)

// GetMachineInfo returns the specifications for the given GCE machine type.
// Machine type format: {family}-{variant}-{vcpus} (e.g., "n2-standard-32")
// or custom: {family}-custom-{vcpus}-{memory_mb} (e.g., "n2-custom-16-32768").
func GetMachineInfo(machineType string) (MachineInfo, error) {
	// Handle shared-core machine types first.
	if info, ok := sharedCoreMachines[machineType]; ok {
		return info, nil
	}

	parts := strings.Split(machineType, "-")
	if len(parts) < 3 {
		return MachineInfo{}, fmt.Errorf("invalid machine type format: %s", machineType)
	}

	family := parts[0]

	// Handle custom machine types: {family}-custom-{vcpus}-{memory_mb}[-ext].
	if parts[1] == "custom" {
		return parseCustomMachineType(family, parts)
	}

	// Handle predefined machine types: {family}-{variant}-{vcpus}[-lssd][-metal].
	variant := parts[1]
	vcpuStr := parts[2]
	vcpus, err := strconv.Atoi(vcpuStr)
	if err != nil {
		return MachineInfo{}, fmt.Errorf("invalid vCPU count in machine type %s: %w", machineType, err)
	}

	// Check for -lssd and -metal suffixes (can appear in any order after vcpus).
	// Examples: c4a-standard-8-lssd, c4a-highmem-96-metal, c4a-standard-8-lssd-metal
	var hasLSSD, hasMetal bool
	for i := 3; i < len(parts); i++ {
		switch parts[i] {
		case "lssd":
			hasLSSD = true
		case "metal":
			hasMetal = true
		}
	}

	return getPredefinedMachineInfo(family, variant, vcpus, hasLSSD, hasMetal)
}

// sharedCoreMachines defines shared-core machine types with fractional vCPUs.
// Reference: https://docs.cloud.google.com/compute/docs/general-purpose-machines#sharedcore
var sharedCoreMachines = map[string]MachineInfo{
	// E2 shared-core (Intel or AMD, architecture assumed amd64).
	// CPUCores is 1 even though these have fractional vCPUs.
	// CPUPlatforms inherits from E2 (nil - platform auto-selected).
	"e2-micro":  {CPUCores: 1, MemoryGiB: 1, Architecture: ArchAMD64, StorageTypes: storagePDStandard, CPUPlatforms: platformsE2},
	"e2-small":  {CPUCores: 1, MemoryGiB: 2, Architecture: ArchAMD64, StorageTypes: storagePDStandard, CPUPlatforms: platformsE2},
	"e2-medium": {CPUCores: 1, MemoryGiB: 4, Architecture: ArchAMD64, StorageTypes: storagePDStandard, CPUPlatforms: platformsE2},
	// N1 shared-core (legacy, Intel). Memory rounded down to int.
	// CPUCores is 1 even though these have fractional vCPUs.
	"f1-micro": {CPUCores: 1, MemoryGiB: 1, Architecture: ArchAMD64, StorageTypes: storagePDStandard, CPUPlatforms: platformsN1}, // 0.6 GiB -> 1
	"g1-small": {CPUCores: 1, MemoryGiB: 1, Architecture: ArchAMD64, StorageTypes: storagePDStandard, CPUPlatforms: platformsN1}, // 1.7 GiB -> 1
}

// parseCustomMachineType parses custom machine types: {family}-custom-{vcpus}-{memory_mb}[-ext].
func parseCustomMachineType(family string, parts []string) (MachineInfo, error) {
	if len(parts) < 4 {
		return MachineInfo{}, fmt.Errorf("invalid custom machine type format: need at least 4 parts")
	}

	vcpus, err := strconv.Atoi(parts[2])
	if err != nil {
		return MachineInfo{}, fmt.Errorf("invalid vCPU count: %w", err)
	}

	memoryMB, err := strconv.Atoi(parts[3])
	if err != nil {
		return MachineInfo{}, fmt.Errorf("invalid memory MB: %w", err)
	}

	// Convert MiB to GiB (the custom format uses MiB, not MB).
	memoryGiB := memoryMB / 1024

	info := MachineInfo{
		CPUCores:  vcpus,
		MemoryGiB: memoryGiB,
	}

	// Set family-specific defaults for custom machine types.
	switch family {
	case "n1":
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsN1()
		info.StorageTypes = storagePDStandard
		info.CPUPlatforms = platformsN1
	case "n2":
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsN2(vcpus)
		info.StorageTypes = storageN2
		info.CPUPlatforms = platformsN2
	case "n2d":
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsN2D(vcpus)
		info.StorageTypes = storageN2
		info.CPUPlatforms = platformsN2D
	case "n4":
		info.Architecture = ArchAMD64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsN4
	case "e2":
		info.Architecture = ArchAMD64
		info.StorageTypes = storagePDStandard
		info.CPUPlatforms = platformsE2
	case "c2d":
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsC2D(vcpus)
		info.StorageTypes = storagePDFull
		info.CPUPlatforms = platformsC2D
	default:
		return MachineInfo{}, fmt.Errorf("custom machine types not supported for family: %s", family)
	}

	return info, nil
}

// getPredefinedMachineInfo returns info for predefined machine types.
// hasLSSD indicates the -lssd suffix is present (e.g., c4a-standard-8-lssd).
// hasMetal indicates the -metal suffix is present (bare metal instance).
func getPredefinedMachineInfo(
	family, variant string, vcpus int, hasLSSD, hasMetal bool,
) (MachineInfo, error) {
	// Note: hasMetal currently doesn't change the returned info, but is parsed
	// for completeness. Bare metal instances have the same specs as regular VMs.
	info := MachineInfo{CPUCores: vcpus}

	switch family {
	case "n1":
		info.MemoryGiB = n1MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsN1()
		info.StorageTypes = storagePDStandard
		info.CPUPlatforms = platformsN1

	case "n2":
		info.MemoryGiB = n2MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsN2(vcpus)
		info.StorageTypes = storageN2
		info.CPUPlatforms = platformsN2

	case "n2d":
		info.MemoryGiB = n2dMemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsN2D(vcpus)
		info.StorageTypes = storageN2
		info.CPUPlatforms = platformsN2D

	case "n4":
		info.MemoryGiB = n4MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsN4

	case "n4d":
		info.MemoryGiB = n4dMemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsN4D

	case "n4a":
		// N4A uses Google Axion (Arm Neoverse N3).
		info.MemoryGiB = n4aMemoryGiB(variant, vcpus)
		info.Architecture = ArchARM64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsN4A

	case "e2":
		info.MemoryGiB = e2MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storagePDStandard
		info.CPUPlatforms = platformsE2

	case "t2d":
		info.MemoryGiB = vcpus * t2dFixed
		info.Architecture = ArchAMD64
		info.StorageTypes = storageT2D
		info.CPUPlatforms = platformsT2D

	case "t2a":
		// T2A uses Ampere Altra (Arm Neoverse N1).
		info.MemoryGiB = vcpus * t2aFixed
		info.Architecture = ArchARM64
		info.StorageTypes = storagePDStandard
		info.CPUPlatforms = platformsT2A

	case "c2":
		info.MemoryGiB = vcpus * c2Standard
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsC2(vcpus)
		info.StorageTypes = storagePDFull
		info.CPUPlatforms = platformsC2

	case "c2d":
		info.MemoryGiB = c2dMemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsC2D(vcpus)
		info.StorageTypes = storagePDFull
		info.CPUPlatforms = platformsC2D

	case "c3":
		info.MemoryGiB = c3MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageC3
		info.CPUPlatforms = platformsC3
		if hasLSSD {
			info.AllowedLocalSSDCount = []int{lssdCountC3(vcpus)}
		}

	case "c3d":
		info.MemoryGiB = c3dMemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageC3
		info.CPUPlatforms = platformsC3D
		if hasLSSD {
			info.AllowedLocalSSDCount = []int{lssdCountC3D(vcpus)}
		}

	case "c4":
		info.MemoryGiB = c4MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsC4
		if hasLSSD {
			info.AllowedLocalSSDCount = []int{lssdCountC4(vcpus)}
		}

	case "c4a":
		// C4A uses Google Axion (Arm Neoverse V2).
		info.MemoryGiB = c3MemoryGiB(variant, vcpus) // Same ratios as C3.
		info.Architecture = ArchARM64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsC4A
		if hasLSSD {
			info.AllowedLocalSSDCount = []int{lssdCountC4A(vcpus)}
		}

	case "c4d":
		info.MemoryGiB = c4dMemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageHyperdiskOnly
		info.CPUPlatforms = platformsC4D
		if hasLSSD {
			info.AllowedLocalSSDCount = []int{lssdCountC4D(vcpus)}
		}

	case "h3":
		// H3 is fixed at 88 vCPUs, 352 GiB.
		info.CPUCores = 88
		info.MemoryGiB = 352
		info.Architecture = ArchAMD64
		info.StorageTypes = []string{"pd-balanced", "hyperdisk-balanced", "hyperdisk-throughput"}
		info.CPUPlatforms = platformsH3

	case "m1":
		info.MemoryGiB = m1MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsM1(vcpus)
		info.StorageTypes = storagePDFull
		info.CPUPlatforms = platformsM1

	case "m2":
		info.MemoryGiB = m2MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageM2
		info.CPUPlatforms = platformsM2

	case "m3":
		info.MemoryGiB = m3MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.AllowedLocalSSDCount = localSSDCountsM3(vcpus)
		info.StorageTypes = storageM3
		info.CPUPlatforms = platformsM3

	case "m4":
		info.MemoryGiB = m4MemoryGiB(variant, vcpus)
		info.Architecture = ArchAMD64
		info.StorageTypes = storageM4
		info.CPUPlatforms = platformsM4

	case "a2":
		return getA2MachineInfo(variant, vcpus)

	case "a3":
		return getA3MachineInfo(variant, vcpus)

	case "g2":
		return getG2MachineInfo(variant, vcpus)

	default:
		return MachineInfo{}, fmt.Errorf("unknown machine family: %s", family)
	}

	return info, nil
}

// Memory calculation functions for each family. Return int (GiB), rounded down.

func n1MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return int(float64(vcpus) * n1HighMem)
	case "highcpu":
		return int(float64(vcpus) * n1HighCPU)
	default:
		return int(float64(vcpus) * n1Standard)
	}
}

func n2MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * n2HighMem
	case "highcpu":
		return vcpus * n2HighCPU
	default:
		return vcpus * n2Standard
	}
}

func n2dMemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * n2dHighMem
	case "highcpu":
		return vcpus * n2dHighCPU
	default:
		return vcpus * n2dStandard
	}
}

func n4MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * n4HighMem
	case "highcpu":
		return vcpus * n4HighCPU
	default:
		return vcpus * n4Standard
	}
}

func n4dMemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * n4dHighMem
	case "highcpu":
		return vcpus * n4dHighCPU
	default:
		return vcpus * n4dStandard
	}
}

func n4aMemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * n4aHighMem
	case "highcpu":
		return vcpus * n4aHighCPU
	default:
		return vcpus * n4aStandard
	}
}

func e2MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * e2HighMem
	case "highcpu":
		return vcpus * e2HighCPU
	default:
		return vcpus * e2Standard
	}
}

func c2dMemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * c2dHighMem
	case "highcpu":
		return vcpus * c2dHighCPU
	default:
		return vcpus * c2dStandard
	}
}

func c3MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * c3HighMem
	case "highcpu":
		return vcpus * c3HighCPU
	default:
		return vcpus * c3Standard
	}
}

func c3dMemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return vcpus * c3dHighMem
	case "highcpu":
		return vcpus * c3dHighCPU
	default:
		return vcpus * c3dStandard
	}
}

func c4MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return int(float64(vcpus) * c4HighMem)
	case "highcpu":
		return vcpus * c4HighCPU
	default:
		return int(float64(vcpus) * c4Standard)
	}
}

func c4dMemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "highmem":
		return int(float64(vcpus) * c4dHighMem)
	case "highcpu":
		return int(float64(vcpus) * c4dHighCPU)
	default:
		return int(float64(vcpus) * c4dStandard)
	}
}

func m1MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "ultramem":
		return int(float64(vcpus) * m1UltraMem)
	default:
		return int(float64(vcpus) * m1MegaMem)
	}
}

func m2MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "megamem":
		return int(float64(vcpus) * m2MegaMem)
	default:
		return int(float64(vcpus) * m2UltraMem)
	}
}

func m3MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "ultramem":
		return int(float64(vcpus) * m3UltraMem)
	default:
		return int(float64(vcpus) * m3MegaMem)
	}
}

func m4MemoryGiB(variant string, vcpus int) int {
	switch variant {
	case "ultramem":
		return int(float64(vcpus) * m4UltraMem)
	default:
		return int(float64(vcpus) * m4MegaMem)
	}
}

// Local SSD count functions.
// Reference: https://docs.cloud.google.com/compute/docs/disks/local-ssd#choose_number_local_ssds

func localSSDCountsN1() []int {
	return []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}
}

func localSSDCountsN2(vcpus int) []int {
	// N2: varies by vCPU count, max 24.
	switch {
	case vcpus <= 10:
		return []int{1, 2, 4, 8, 16, 24}
	case vcpus <= 20:
		return []int{2, 4, 8, 16, 24}
	case vcpus <= 40:
		return []int{4, 8, 16, 24}
	case vcpus <= 80:
		return []int{8, 16, 24}
	default:
		return []int{16, 24}
	}
}

func localSSDCountsN2D(vcpus int) []int {
	// N2D: varies by vCPU count, max 24.
	switch {
	case vcpus <= 16:
		return []int{1, 2, 4, 8, 16, 24}
	case vcpus <= 48:
		return []int{2, 4, 8, 16, 24}
	case vcpus <= 80:
		return []int{4, 8, 16, 24}
	default:
		return []int{8, 16, 24}
	}
}

func localSSDCountsC2(vcpus int) []int {
	// C2: 1, 2, 4, 8 depending on size.
	switch {
	case vcpus <= 8:
		return []int{1, 2, 4, 8}
	case vcpus <= 16:
		return []int{2, 4, 8}
	case vcpus <= 30:
		return []int{4, 8}
	default:
		return []int{8}
	}
}

func localSSDCountsC2D(vcpus int) []int {
	// C2D: 1, 2, 4, 8 depending on size.
	// Reference: gcloud.go AllowedLocalSSDCount
	switch {
	case vcpus <= 16:
		return []int{1, 2, 4, 8}
	case vcpus <= 32:
		return []int{2, 4, 8}
	case vcpus <= 56:
		return []int{4, 8}
	default:
		return []int{8}
	}
}

func localSSDCountsM1(vcpus int) []int {
	// M1: varies by vCPU count.
	// Reference: gcloud.go AllowedLocalSSDCount
	// Note: gcloud.go only has 40 and 80, but larger sizes exist.
	switch vcpus {
	case 40:
		return []int{1, 2, 3, 4, 5}
	default:
		// 80+ vCPUs (including 80, 96, 160) support full range.
		return []int{1, 2, 3, 4, 5, 6, 7, 8}
	}
}

func localSSDCountsM3(vcpus int) []int {
	// M3: varies by vCPU count.
	// Reference: gcloud.go AllowedLocalSSDCount
	switch vcpus {
	case 32, 64:
		return []int{4, 8}
	case 128:
		return []int{8}
	default:
		return nil // unsupported vCPU count
	}
}

// Fixed local SSD counts for -lssd variants.
// These return a single fixed count (not a list of options) based on vCPU count.
// Reference: https://docs.cloud.google.com/compute/docs/disks/local-ssd#lssd-suffix

func lssdCountC3(vcpus int) int {
	// C3-lssd: fixed count based on vCPU.
	// Reference: gcloud.go AllowedLocalSSDCount
	switch vcpus {
	case 4:
		return 1
	case 8:
		return 2
	case 22:
		return 4
	case 44:
		return 8
	case 88:
		return 16
	case 176:
		return 32
	default:
		return 0 // unsupported vCPU count
	}
}

func lssdCountC3D(vcpus int) int {
	// C3D-lssd: fixed count based on vCPU.
	// Reference: gcloud.go AllowedLocalSSDCount
	switch vcpus {
	case 8, 16:
		return 1
	case 30:
		return 2
	case 60:
		return 4
	case 90:
		return 8
	case 180:
		return 16
	case 360:
		return 32
	default:
		return 0 // unsupported vCPU count
	}
}

func lssdCountC4(vcpus int) int {
	// C4-lssd: fixed count based on vCPU.
	// Reference: gcloud.go AllowedLocalSSDCount
	switch vcpus {
	case 4, 8:
		return 1
	case 16:
		return 2
	case 24:
		return 4
	case 32:
		return 5
	case 48:
		return 8
	case 96:
		return 16
	case 144:
		return 24
	case 192:
		return 32
	case 288:
		return 48
	default:
		return 0 // unsupported vCPU count
	}
}

func lssdCountC4A(vcpus int) int {
	// C4A-lssd: fixed count based on vCPU.
	// Reference: gcloud.go AllowedLocalSSDCount
	// Note: C4A has different SSD counts than C4!
	switch vcpus {
	case 4:
		return 1
	case 8:
		return 2
	case 16:
		return 4
	case 32:
		return 6
	case 48:
		return 10
	case 64:
		return 14
	case 72:
		return 16
	default:
		return 0 // unsupported vCPU count
	}
}

func lssdCountC4D(vcpus int) int {
	// C4D-lssd: fixed count based on vCPU.
	// Reference: gcloud.go AllowedLocalSSDCount
	// Note: C4D has different SSD counts than C4!
	switch vcpus {
	case 8, 16:
		return 1
	case 32:
		return 2
	case 48:
		return 4
	case 64:
		return 6
	case 96:
		return 8
	case 192:
		return 16
	case 384:
		return 32
	default:
		return 0 // unsupported vCPU count
	}
}

// Accelerator-optimized machine handlers.

func getA2MachineInfo(variant string, vcpus int) (MachineInfo, error) {
	// A2 machines have specific configurations based on GPU count.
	// A2 Standard: 1-16 A100 40GB GPUs.
	// A2 Ultra: 1-8 A100 80GB GPUs.
	info := MachineInfo{
		CPUCores:     vcpus,
		Architecture: ArchAMD64,
		StorageTypes: storageAccelerator,
		CPUPlatforms: platformsA2,
	}

	// Memory is roughly 85 GiB per 12 vCPUs for standard.
	switch variant {
	case "highgpu", "megagpu":
		// a2-highgpu-{1,2,4,8}g, a2-megagpu-16g
		info.MemoryGiB = int(float64(vcpus) * 7.083) // ~85 GiB per 12 vCPUs
	case "ultragpu":
		// a2-ultragpu-{1,2,4,8}g
		info.MemoryGiB = int(float64(vcpus) * 14.167) // ~170 GiB per 12 vCPUs
	default:
		info.MemoryGiB = vcpus * 7
	}

	info.AllowedLocalSSDCount = []int{1, 2, 4, 8}

	return info, nil
}

func getA3MachineInfo(variant string, vcpus int) (MachineInfo, error) {
	// A3 machines: fixed configurations with H100/H200 GPUs.
	info := MachineInfo{
		CPUCores:     vcpus,
		Architecture: ArchAMD64,
		StorageTypes: []string{"pd-balanced", "pd-ssd", "hyperdisk-balanced", "hyperdisk-ml"},
	}

	switch variant {
	case "highgpu":
		// a3-highgpu-8g: 208 vCPUs, 1872 GiB, 8x H100.
		info.CPUCores = 208
		info.MemoryGiB = 1872
		info.CPUPlatforms = platformsA3Sapphire
	case "megagpu":
		// a3-megagpu-8g: 208 vCPUs, 1872 GiB, 8x H100.
		info.CPUCores = 208
		info.MemoryGiB = 1872
		info.CPUPlatforms = platformsA3Sapphire
	case "ultragpu":
		// a3-ultragpu-8g: 224 vCPUs, 2952 GiB, 8x H200.
		info.CPUCores = 224
		info.MemoryGiB = 2952
		info.CPUPlatforms = platformsA3Emerald
	default:
		info.MemoryGiB = vcpus * 9
		info.CPUPlatforms = platformsA3Sapphire
	}

	return info, nil
}

func getG2MachineInfo(variant string, vcpus int) (MachineInfo, error) {
	// G2 machines: NVIDIA L4 GPUs.
	// Reference: gcloud.go AllowedLocalSSDCount
	info := MachineInfo{
		CPUCores:     vcpus,
		Architecture: ArchAMD64,
		StorageTypes: []string{"pd-balanced", "pd-ssd", "hyperdisk-balanced", "hyperdisk-ml", "hyperdisk-throughput"},
		CPUPlatforms: platformsG2,
	}

	// G2 has 16 GiB per 4 vCPUs (4 GiB/vCPU) for standard.
	info.MemoryGiB = vcpus * 4

	// G2 has fixed (not variable) SSD counts per vCPU configuration.
	switch vcpus {
	case 4, 8, 12, 16, 32:
		info.AllowedLocalSSDCount = []int{1}
	case 24:
		info.AllowedLocalSSDCount = []int{2}
	case 48:
		info.AllowedLocalSSDCount = []int{4}
	case 96:
		info.AllowedLocalSSDCount = []int{8}
	}

	return info, nil
}
