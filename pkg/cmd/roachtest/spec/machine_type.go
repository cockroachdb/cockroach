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

// AWSMachineType selects a machine type given the desired number of CPUs and
// memory per CPU ratio. Also returns the architecture of the selected machine type.
func AWSMachineType(cpus int, mem MemPerCPU, arch vm.CPUArch) (string, vm.CPUArch) {
	// TODO(erikgrinaker): These have significantly less RAM than
	// their GCE counterparts. Consider harmonizing them.
	family := "c6id" // 2 GB RAM per CPU
	selectedArch := vm.ArchAMD64
	if arch == vm.ArchFIPS {
		selectedArch = vm.ArchFIPS
	} else if arch == vm.ArchARM64 {
		family = "c7g" // 2 GB RAM per CPU (graviton3)
		selectedArch = vm.ArchARM64
	}

	if mem == High {
		family = "m6i" // 4 GB RAM per CPU
		if arch == vm.ArchARM64 {
			family = "m7g" // 4 GB RAM per CPU (graviton3)
		}
	} else if mem == Low {
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

	// There is no m7g.24xlarge, fall back to m6i.24xlarge.
	if family == "m7g" && size == "24xlarge" {
		family = "m6i"
		selectedArch = vm.ArchAMD64
	}
	// There is no c7g.24xlarge, fall back to c6id.24xlarge.
	if family == "c7g" && size == "24xlarge" {
		family = "c6id"
		selectedArch = vm.ArchAMD64
	}

	return fmt.Sprintf("%s.%s", family, size), selectedArch
}

// GCEMachineType selects a machine type given the desired number of CPUs and
// memory per CPU ratio. Also returns the architecture of the selected machine type.
func GCEMachineType(cpus int, mem MemPerCPU, arch vm.CPUArch) (string, vm.CPUArch) {
	// TODO(peter): This is awkward: at or below 16 cpus, use n2-standard so that
	// the machines have a decent amount of RAM. We could use custom machine
	// configurations, but the rules for the amount of RAM per CPU need to be
	// determined (you can't request any arbitrary amount of RAM).
	series := "n2"
	selectedArch := vm.ArchAMD64
	if arch == vm.ArchFIPS {
		selectedArch = vm.ArchFIPS
	}
	var kind string
	switch mem {
	case Auto:
		if cpus > 16 {
			kind = "highcpu"
		} else {
			kind = "standard"
		}
	case Standard:
		kind = "standard" // 3.75 GB RAM per CPU
	case High:
		kind = "highmem" // 6.5 GB RAM per CPU
	case Low:
		kind = "highcpu" // 0.9 GB RAM per CPU
	}
	if arch == vm.ArchARM64 && mem == Auto && cpus <= 48 {
		series = "t2a"
		kind = "standard"
		selectedArch = vm.ArchARM64
	}
	// N.B. n2 family does not support single CPU machines.
	if series == "n2" && cpus == 1 {
		cpus = 2
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
