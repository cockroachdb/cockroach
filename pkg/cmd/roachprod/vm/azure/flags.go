// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2019-07-01/compute"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/vm"
	"github.com/spf13/pflag"
)

type providerOpts struct {
	locations        []string
	machineType      string
	operationTimeout time.Duration
	syncDelete       bool
	vnetName         string
}

var defaultLocations = []string{
	"eastus2",
	"westus",
	"westeurope",
}

// ConfigureCreateFlags implements vm.ProviderFlags.
func (o *providerOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.DurationVar(&o.operationTimeout, ProviderName+"-timeout", 10*time.Minute,
		"The maximum amount of time for an Azure API operation to take")
	flags.BoolVar(&o.syncDelete, ProviderName+"-sync-delete", false,
		"Wait for deletions to finish before returning")
	flags.StringVar(&o.machineType, ProviderName+"-machine-type",
		string(compute.VirtualMachineSizeTypesStandardD4V3),
		"Machine type (see https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)")
	flags.StringSliceVar(&o.locations, ProviderName+"-locations", nil,
		fmt.Sprintf("Locations for cluster (see `az account list-locations`) (default\n[%s])",
			strings.Join(defaultLocations, ",")))
	flags.StringVar(&o.vnetName, ProviderName+"-vnet-name", "common",
		"The name of the VNet to use")
}

// ConfigureClusterFlags implements vm.ProviderFlags and is a no-op.
func (o *providerOpts) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}
