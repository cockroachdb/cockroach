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

// AWSMachineType selects a machine type given the desired number of CPUs.
func AWSMachineType(cpus int) string {
	switch {
	case cpus <= 2:
		return "c5d.large"
	case cpus <= 4:
		return "c5d.xlarge"
	case cpus <= 8:
		return "c5d.2xlarge"
	case cpus <= 16:
		return "c5d.4xlarge"
	case cpus <= 36:
		return "c5d.9xlarge"
	case cpus <= 72:
		return "c5d.18xlarge"
	case cpus <= 96:
		// There is no c5d.24xlarge.
		return "m5d.24xlarge"
	default:
		panic(fmt.Sprintf("no aws machine type with %d cpus", cpus))
	}
}

// GCEMachineType selects a machine type given the desired number of CPUs.
func GCEMachineType(cpus int) string {
	// TODO(peter): This is awkward: below 16 cpus, use n1-standard so that the
	// machines have a decent amount of RAM. We could use customer machine
	// configurations, but the rules for the amount of RAM per CPU need to be
	// determined (you can't request any arbitrary amount of RAM).
	if cpus < 16 {
		return fmt.Sprintf("n1-standard-%d", cpus)
	}
	return fmt.Sprintf("n1-highcpu-%d", cpus)
}

// AzureMachineType selects a machine type given the desired number of CPUs.
func AzureMachineType(cpus int) string {
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
