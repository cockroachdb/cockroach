// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// AWSMachineType selects a machine type given the desired number of CPUs, memory per CPU,
// support for locally-attached SSDs and CPU architecture. It returns a compatible machine type and its architecture.
//
// When MemPerCPU is Standard, the memory per CPU ratio is 4 GB. For High, it is 8 GB.
// For Auto, it's 4 GB up to and including 16 CPUs, then 2 GB. Low is not supported.
//
// N.B. in some cases, the selected architecture and machine type may be different from the requested one. E.g.,
// graviton3 with >= 24xlarge (96 vCPUs) isn't available, so we fall back to (c|m|r)6i.24xlarge.
// N.B. cpus is expected to be an even number; validation is deferred to a specific cloud provider.
//
// At the time of writing, the intel machines are all third-generation Xeon, "Ice Lake" which are isomorphic to
// GCE's n2-(standard|highmem|custom) _with_ --minimum-cpu-platform="Intel Ice Lake" (roachprod's default).
func AWSMachineType(
	cpus int, mem MemPerCPU, shouldSupportLocalSSD bool, arch vm.CPUArch,
) (string, vm.CPUArch) {
	family := "m6i" // 4 GB RAM per CPU
	selectedArch := vm.ArchAMD64

	if arch == vm.ArchFIPS {
		// N.B. FIPS is available in any AMD64 machine configuration.
		selectedArch = vm.ArchFIPS
	} else if arch == vm.ArchARM64 {
		family = "m7g" // 4 GB RAM per CPU (graviton3)
		selectedArch = vm.ArchARM64
	}

	switch mem {
	case Auto:
		if cpus > 16 {
			family = "c6i" // 2 GB RAM per CPU

			if arch == vm.ArchARM64 {
				family = "c7g" // 2 GB RAM per CPU (graviton3)
				selectedArch = vm.ArchARM64
			}
		}
	case Standard:
		// nothing to do, family is already configured as per above
	case High:
		family = "r6i" // 8 GB RAM per CPU
		// N.B. graviton3 doesn't support x8 memory multiplier, so we fall back.
		if arch == vm.ArchARM64 {
			selectedArch = vm.ArchAMD64
		}
	case Low:
		panic("low memory per CPU not available for AWS")
	}

	var size string
	switch {
	case cpus <= 2:
		size = "large"
	case cpus <= 4:
		size = "xlarge"
	case cpus <= 8:
		size = "2xlarge"
	case cpus <= 16:
		size = "4xlarge"
	case cpus <= 32:
		size = "8xlarge"
	case cpus <= 48:
		size = "12xlarge"
	case cpus <= 64:
		size = "16xlarge"
	case cpus <= 96:
		size = "24xlarge"
	default:
		panic(fmt.Sprintf("no aws machine type with %d cpus", cpus))
	}
	// There is no m7g.24xlarge (or c7g.24xlarge), fall back to (c|m|r)6i.24xlarge.
	if selectedArch == vm.ArchARM64 && size == "24xlarge" {
		switch mem {
		case Auto:
			family = "c6i"
		case Standard:
			family = "m6i"
		case High:
			family = "r6i"
		}
		selectedArch = vm.ArchAMD64
	}
	if shouldSupportLocalSSD {
		// All of the above instance families can be modified to support local SSDs by appending "d".
		family += "d"
	}

	return fmt.Sprintf("%s.%s", family, size), selectedArch
}

// GCEMachineType selects a machine type given the desired number of CPUs, memory per CPU, and CPU architecture.
// It returns a compatible machine type and its architecture.
//
// When MemPerCPU is Standard, the memory per CPU ratio is 4 GB. For High, it is 8 GB.
// For Auto, it's 4 GB up to and including 16 CPUs, then 2 GB. Low is 1 GB.
//
// N.B. in some cases, the selected architecture and machine type may be different from the requested one. E.g.,
// single CPU machines are not available, so we fall back to dual CPU machines.
// N.B. cpus is expected to be an even number; validation is deferred to a specific cloud provider.
//
// At the time of writing, the intel machines are all third-generation xeon, "Ice Lake" assuming
// --minimum-cpu-platform="Intel Ice Lake" (roachprod's default). This is isomorphic to AWS's m6i or c6i.
// The only exception is low memory machines (n2-highcpu-xxx), which aren't available in AWS.
func GCEMachineType(cpus int, mem MemPerCPU, arch vm.CPUArch) (string, vm.CPUArch) {
	series := "n2"
	selectedArch := vm.ArchAMD64

	if arch == vm.ArchFIPS {
		// N.B. FIPS is available in any AMD64 machine configuration.
		selectedArch = vm.ArchFIPS
	} else if arch == vm.ArchARM64 {
		selectedArch = vm.ArchARM64
		series = "t2a" // Ampere Altra
	}
	var kind string
	switch mem {
	case Auto:
		if cpus > 16 {
			// We'll use 2GB RAM per CPU for custom machines.
			kind = "custom"
			if arch == vm.ArchARM64 {
				// T2A doesn't support custom, fall back to n2.
				series = "n2"
				selectedArch = vm.ArchAMD64
			}
		} else {
			kind = "standard"
		}
	case Standard:
		kind = "standard" // 4 GB RAM per CPU
	case High:
		kind = "highmem" // 8 GB RAM per CPU
		if arch == vm.ArchARM64 {
			// T2A doesn't support highmem, fall back to n2.
			series = "n2"
			selectedArch = vm.ArchAMD64
		}
	case Low:
		kind = "highcpu" // 1 GB RAM per CPU
		if arch == vm.ArchARM64 {
			// T2A doesn't support highcpu, fall back to n2.
			series = "n2"
			selectedArch = vm.ArchAMD64
		}
	}
	// T2A doesn't support cpus > 48, fall back to n2.
	if selectedArch == vm.ArchARM64 && cpus > 48 {
		series = "n2"
		selectedArch = vm.ArchAMD64
	}
	// N.B. n2 does not support single CPU machines.
	if series == "n2" && cpus == 1 {
		cpus = 2
	}
	if kind == "custom" {
		// We use 2GB RAM per CPU for custom machines.
		return fmt.Sprintf("%s-custom-%d-%d", series, cpus, 2048*cpus), selectedArch
	}
	return fmt.Sprintf("%s-%s-%d", series, kind, cpus), selectedArch
}

// AzureMachineType selects a machine type given the desired number of CPUs and
// memory per CPU ratio.
func AzureMachineType(cpus int, mem MemPerCPU) string {
	if mem != Auto && mem != Standard {
		panic(fmt.Sprintf("custom memory per CPU not implemented for Azure, memory ratio requested: %d", mem))
	}
	switch {
	case cpus <= 2:
		return "Standard_D2_v3"
	case cpus <= 4:
		return "Standard_D4_v3"
	case cpus <= 8:
		return "Standard_D8_v3"
	case cpus <= 16:
		return "Standard_D16_v3"
	case cpus <= 36:
		return "Standard_D32_v3"
	case cpus <= 48:
		return "Standard_D48_v3"
	case cpus <= 64:
		return "Standard_D64_v3"
	default:
		panic(fmt.Sprintf("no azure machine type with %d cpus", cpus))
	}
}
