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
		gceProviderOpts.MachineTypeSpecs, numNodes,
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

func providerEnabled(providers []string, name string) bool {
	for _, provider := range providers {
		if provider == name {
			return true
		}
	}
	return false
}
