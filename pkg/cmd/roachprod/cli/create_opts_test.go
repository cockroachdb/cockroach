// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

func TestBuildClusterCreateOptsGCEARM64Defaults(t *testing.T) {
	testCases := []struct {
		name                string
		arch                vm.CPUArch
		useLocalSSD         bool
		machineTypeSpecs    []string
		expectedMachineType string
	}{
		{
			name:                "arm64 with local SSD defaults to C4A lssd",
			arch:                vm.ArchARM64,
			useLocalSSD:         true,
			machineTypeSpecs:    []string{gce.DefaultMachineType},
			expectedMachineType: gce.DefaultARM64LSSDMachineType,
		},
		{
			name:                "arm64 without local SSD defaults to C4A",
			arch:                vm.ArchARM64,
			useLocalSSD:         false,
			machineTypeSpecs:    []string{gce.DefaultMachineType},
			expectedMachineType: gce.DefaultARM64MachineType,
		},
		{
			name:                "explicit ARM64 machine type is preserved",
			arch:                vm.ArchARM64,
			useLocalSSD:         true,
			machineTypeSpecs:    []string{"t2a-standard-4"},
			expectedMachineType: "t2a-standard-4",
		},
		{
			name:                "amd64 keeps GCE default",
			arch:                vm.ArchAMD64,
			useLocalSSD:         true,
			machineTypeSpecs:    []string{gce.DefaultMachineType},
			expectedMachineType: gce.DefaultMachineType,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			createVMOpts := vm.DefaultCreateOpts()
			createVMOpts.Arch = string(tc.arch)
			createVMOpts.VMProviders = []string{gce.ProviderName}
			createVMOpts.SSDOpts.UseLocalSSD = tc.useLocalSSD

			providerOpts := gce.DefaultProviderOpts()
			providerOpts.MachineTypeSpecs = tc.machineTypeSpecs
			providerOptsContainer := vm.CreateProviderOptionsContainer()
			providerOptsContainer.SetProviderOpts(gce.ProviderName, providerOpts)

			opts, err := buildClusterCreateOpts(4, createVMOpts, providerOptsContainer)
			require.NoError(t, err)
			require.Len(t, opts, 1)

			gceOpts, ok := opts[0].ProviderOptsContainer[gce.ProviderName].(*gce.ProviderOpts)
			require.True(t, ok)
			require.Equal(t, tc.expectedMachineType, gceOpts.MachineType)
		})
	}
}
