// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

func buildClusterCreateOpts(
	numNodes int, createVMOpts vm.CreateOpts, providerOptsContainer vm.ProviderOptionsContainer,
) ([]*cloud.ClusterCreateOpts, error) {
	gceProviderOpts, ok := providerOptsContainer[gce.ProviderName].(*gce.ProviderOpts)
	if !ok || len(gceProviderOpts.MachineTypeSpecs) == 0 {
		// Non-GCE provider or no machine type specs; use defaults as-is.
		return []*cloud.ClusterCreateOpts{{
			Nodes:                 numNodes,
			CreateOpts:            createVMOpts,
			ProviderOptsContainer: providerOptsContainer,
		}}, nil
	}

	machineTypeSpecs, err := gce.ParseMachineTypeSpecs(
		gceMachineTypeSpecsForCreate(createVMOpts, gceProviderOpts), numNodes,
	)
	if err != nil {
		return nil, err
	}

	if len(machineTypeSpecs) > 1 && !providerEnabled(createVMOpts.VMProviders, gce.ProviderName) {
		return nil, errors.Newf("--%s-machine-type requires gce in --clouds", gce.ProviderName)
	}

	opts := make([]*cloud.ClusterCreateOpts, 0, len(machineTypeSpecs))
	for _, spec := range machineTypeSpecs {
		providerOptsCopy := maps.Clone(providerOptsContainer)
		gceOpts := *gceProviderOpts
		gceOpts.MachineType = spec.MachineType
		gceOpts.MachineTypeSpecs = nil
		providerOptsCopy.SetProviderOpts(gce.ProviderName, &gceOpts)

		opts = append(opts, &cloud.ClusterCreateOpts{
			Nodes:                 spec.Count,
			CreateOpts:            createVMOpts,
			ProviderOptsContainer: providerOptsCopy,
		})
	}

	return opts, nil
}

func gceMachineTypeSpecsForCreate(
	createVMOpts vm.CreateOpts, gceProviderOpts *gce.ProviderOpts,
) []string {
	// The GCE machine type flag defaults to an AMD64 machine. When ARM64 is
	// selected and the resolved machine type is still the GCE default, rewrite it
	// to the appropriate ARM64 default: c4a-standard-4, or c4a-standard-4-lssd
	// when local SSDs are requested.
	if len(gceProviderOpts.MachineTypeSpecs) != 1 ||
		gceProviderOpts.MachineTypeSpecs[0] != gce.DefaultMachineType ||
		vm.ParseArch(createVMOpts.Arch) != vm.ArchARM64 {
		return gceProviderOpts.MachineTypeSpecs
	}
	if createVMOpts.SSDOpts.UseLocalSSD {
		return []string{gce.DefaultARM64LSSDMachineType}
	}
	return []string{gce.DefaultARM64MachineType}
}

func providerEnabled(providers []string, name string) bool {
	for _, provider := range providers {
		if provider == name {
			return true
		}
	}
	return false
}
