// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
)

func buildClusterCreateOpts(
	cmd *cobra.Command,
	numNodes int,
	createVMOpts vm.CreateOpts,
	providerOptsContainer vm.ProviderOptionsContainer,
) ([]*cloud.ClusterCreateOpts, error) {
	gceMachineTypeFlag := gce.ProviderName + "-machine-type"
	if !cmd.Flags().Changed(gceMachineTypeFlag) {
		return []*cloud.ClusterCreateOpts{{
			Nodes:                 numNodes,
			CreateOpts:            createVMOpts,
			ProviderOptsContainer: providerOptsContainer,
		}}, nil
	}

	if !providerEnabled(createVMOpts.VMProviders, gce.ProviderName) {
		return nil, errors.Newf("--%s requires gce in --clouds", gceMachineTypeFlag)
	}

	gceProviderOpts, ok := providerOptsContainer[gce.ProviderName].(*gce.ProviderOpts)
	if !ok {
		return nil, errors.New("gce provider options missing")
	}

	machineTypeSpecs, err := parseGceMachineTypeSpecs(gceProviderOpts.MachineTypeSpecs, numNodes)
	if err != nil {
		return nil, err
	}

	opts := make([]*cloud.ClusterCreateOpts, 0, len(machineTypeSpecs))
	for _, spec := range machineTypeSpecs {
		providerOptsCopy := maps.Clone(providerOptsContainer)
		gceOpts := *gceProviderOpts
		gceOpts.MachineType = spec.MachineType
		gceOpts.MachineTypeSpecs = nil
		providerOptsCopy.SetProviderOpts(gce.ProviderName, &gceOpts)

		opts = append(opts, &cloud.ClusterCreateOpts{
			Nodes:                 spec.Nodes,
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
