// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/errors"
)

// SelectAWSMachineType selects a machine type given the desired number of CPUs,
// memory per CPU, support for locally-attached SSDs and CPU architecture. It
// returns a compatible machine type and its architecture.
//
// When MemPerCPU is Standard, the memory per CPU ratio is 4 GB. For High, it is 8 GB.
// For Auto, it's 4 GB up to and including 16 CPUs, then 2 GB. Low is not supported.
//
// N.B. in some cases, the selected architecture and machine type may be _different_ from the requested one. E.g.,
// graviton3 with >= 24xlarge (96 vCPUs) isn't available, so we fall back to (c|m|r)6i.24xlarge.
// To keep parity with GCE, we fall back to AMD Milan (c6a.24xlarge) if cpus > 80. However, this family doesn't support
// local SSDs.
//
// N.B. cpus is expected to be an even number; validation is deferred to a specific cloud provider.
//
// N.B. if mem is Auto, and cpus > 80, we fall back to AMD Milan (c6a.24xlarge).
//
// At the time of writing, the intel machines are all third-generation Xeon, "Ice Lake" which are isomorphic to
// GCE's n2-(standard|highmem|custom) _with_ --minimum-cpu-platform="Intel Ice Lake" (roachprod's default).
//
// // See ExampleSelectAWSMachineType for an exhaustive list of selected machine types.
func SelectAWSMachineType(
	cpus int, mem MemPerCPU, shouldSupportLocalSSD bool, arch vm.CPUArch,
) (string, vm.CPUArch, error) {
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
		return "", "", errors.New("low memory per CPU not available for AWS")
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
		// N.B. some machines can go up to 192 vCPUs, but we never exceed 96 in tests.
		size = "24xlarge"
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
	if cpus > 80 && family == "c6i" {
		// N.B. to keep parity with GCE, we use AMD Milan instead of Intel Ice Lake, keeping same 2GB RAM per CPU ratio.
		family = "c6a"
	}
	if shouldSupportLocalSSD && family != "c6a" {
		// All of the above instance families _except_ "c6a" can be modified to support local SSDs by appending "d".
		family += "d"
	}
	return fmt.Sprintf("%s.%s", family, size), selectedArch, nil
}

// SelectGCEMachineType selects a machine type given the desired number of CPUs,
// memory per CPU, and CPU architecture.  It returns a compatible machine type
// and its architecture.
//
// When MemPerCPU is Standard, the memory per CPU ratio is 4 GB. For High, it is 8 GB.
// For Auto (default), it's 4 GB up to and including 16 CPUs, then 2 GB. Low is 1 GB.
//
// N.B. in some cases, the selected architecture and machine type may be different from the requested one. E.g.,
// single CPU machines are not available, so we fall back to dual CPU machines.
// N.B. cpus is expected to be an even number; validation is deferred to a specific cloud provider.
//
// N.B. if mem is Auto, and cpus > 80, we fall back to AMD Milan (n2d-custom-xxx).
//
// At the time of writing, the intel machines are all third-generation xeon, "Ice Lake" assuming
// --minimum-cpu-platform="Intel Ice Lake" (roachprod's default). This is isomorphic to AWS's m6i or c6i.
// Similarly, the AMD machines are all third-generation EPYC, "Milan".
// Low memory machines are n2-highcpu-xxx; currently unavailable in AWS or Azure.
//
// See ExampleSelectGCEMachineType for an exhaustive list of selected machine types.
func SelectGCEMachineType(cpus int, mem MemPerCPU, arch vm.CPUArch) (string, vm.CPUArch) {
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
		if cpus > 80 && series == "n2" {
			// N.B. n2 doesn't support custom instances with > 80 vCPUs. So, the best we can do is to go with n2d, which
			// unfortunately brings AMD Milan into the mix.
			series = "n2d"
		}
		return fmt.Sprintf("%s-custom-%d-%d", series, cpus, 2048*cpus), selectedArch
	}
	return fmt.Sprintf("%s-%s-%d", series, kind, cpus), selectedArch
}

// SelectAzureMachineType selects a machine type given the desired number of CPUs,
// memory per CPU, support for locally-attached SSDs and CPU architecture. It
// returns a compatible machine type and its architecture.
//
// N.B. We use exclusively v5 Azure series _with_ Temp Storage (i.e., locally attached SSDs). Persistent SSDs are
// optional and can be attached to any machine type via `ClusterSpec.VolumeSize`.
//
// When MemPerCPU is Standard, the memory per CPU ratio is 4 GB. For High, it is 8 GB.
// For Auto, it's 4 GB up to and including 16 CPUs, then 2 GB. Low is not supported.
//
// N.B. in some cases, the selected architecture and machine type may be different from the requested one. E.g.,
// Ampere Altra with >= 96 vCPUs isn't available, so we fall back to a corresponding Intel machine.
// N.B. cpus is expected to be an even number; validation is deferred to a specific cloud provider.
//
// See ExampleSelectAzureMachineType for an exhaustive list of selected machine types.
// TODO: Add Ebsv5 machine type to leverage NVMe
func SelectAzureMachineType(cpus int, mem MemPerCPU, arch vm.CPUArch) (string, vm.CPUArch, error) {
	series := "Ddsv5" // 4 GB RAM per CPU
	selectedArch := vm.ArchAMD64

	if arch == vm.ArchFIPS {
		// N.B. FIPS is available in any AMD64 machine configuration.
		selectedArch = vm.ArchFIPS
	} else if arch == vm.ArchARM64 {
		series = "Dpdsv5" // 4 GB RAM per CPU (Ampere Ultra)
		selectedArch = vm.ArchARM64
	}

	switch mem {
	case Auto:
		if cpus > 16 {
			series = "Dldsv5" // 2 GB RAM per CPU

			if arch == vm.ArchARM64 {
				series = "Dpldsv5" // 2 GB RAM per CPU (Ampere Ultra)
			}
		}
	case Standard:
		// nothing to do, family is already configured as per above
	case High:
		series = "Edsv5" // 8 GB RAM per CPU
		if arch == vm.ArchARM64 {
			series = "Epdsv5" // 8 GB RAM per CPU (Ampere Ultra)
		}
	case Low:
		return "", "", errors.New("low memory per CPU not available for Azure")
	}

	// Ampere Ultra doesn't support high count of cpus, fall back to Intel with the corresponding RAM per CPU ratio.
	if selectedArch == vm.ArchARM64 {
		if series == "Dpdsv5" && cpus > 64 {
			series = "Ddsv5"
			selectedArch = vm.ArchAMD64
		} else if series == "Dpldsv5" && cpus > 64 {
			series = "Dldsv5"
			selectedArch = vm.ArchAMD64
		} else if series == "Epdsv5" && cpus > 32 {
			series = "Edsv5"
			selectedArch = vm.ArchAMD64
		}
	}
	// N.B. single CPU machines are not supported.
	if cpus == 1 {
		cpus = 2
	}

	switch series {
	case "Ddsv5":
		return fmt.Sprintf("Standard_D%dds_v5", cpus), selectedArch, nil
	case "Dpdsv5":
		return fmt.Sprintf("Standard_D%dpds_v5", cpus), selectedArch, nil
	case "Dldsv5":
		return fmt.Sprintf("Standard_D%dlds_v5", cpus), selectedArch, nil
	case "Dpldsv5":
		return fmt.Sprintf("Standard_D%dplds_v5", cpus), selectedArch, nil
	case "Edsv5":
		return fmt.Sprintf("Standard_E%dds_v5", cpus), selectedArch, nil
	case "Epdsv5":
		return fmt.Sprintf("Standard_E%dpds_v5", cpus), selectedArch, nil
	default:
		return "", selectedArch, errors.Newf("invalid azure machine series %q", series)
	}
}
