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

import "fmt"

// AWSMachineType selects a machine type given the desired number of CPUs and
// memory per CPU ratio.
func AWSMachineType(cpus int, mem MemPerCPU) string {
	// TODO(erikgrinaker): These have significantly less RAM than
	// their GCE counterparts. Consider harmonizing them.
	family := "c5d" // 2 GB RAM per CPU
	if mem == High {
		family = "m5d" // 4 GB RAM per CPU
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
	case cpus <= 36:
		size = "9xlarge"
	case cpus <= 72:
		size = "18xlarge"
	case cpus <= 96:
		size = "24xlarge"
	default:
		panic(fmt.Sprintf("no aws machine type with %d cpus", cpus))
	}

	// There is no c5d.24xlarge.
	if family == "c5d" && size == "24xlarge" {
		family = "m5d"
	}

	return fmt.Sprintf("%s.%s", family, size)
}

// GCEMachineType selects a machine type given the desired number of CPUs and
// memory per CPU ratio.
func GCEMachineType(cpus int, mem MemPerCPU) string {
	// TODO(peter): This is awkward: at or below 16 cpus, use n1-standard so that
	// the machines have a decent amount of RAM. We could use custom machine
	// configurations, but the rules for the amount of RAM per CPU need to be
	// determined (you can't request any arbitrary amount of RAM).
	series := "n1"
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
	return fmt.Sprintf("%s-%s-%d", series, kind, cpus)
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
