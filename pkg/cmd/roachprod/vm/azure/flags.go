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
	zone             string
	networkDiskType  string
	networkDiskSize  int32
	ultraDiskIOPS    int64
	diskCaching      string
}

var defaultLocations = []string{
	"eastus2",
	"westus",
	"westeurope",
}

var defaultZone = "1"

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
	flags.StringVar(&o.zone, ProviderName+"-availability-zone", "", "Availability Zone to create VMs in")
	flags.StringVar(&o.networkDiskType, ProviderName+"-network-disk-type", "premium-disk",
		"type of network disk [premium-disk, ultra-disk]. only used if local-ssd is false")
	flags.Int32Var(&o.networkDiskSize, ProviderName+"-volume-size", 500,
		"Size in GB of network disk volume, only used if local-ssd=false")
	flags.Int64Var(&o.ultraDiskIOPS, ProviderName+"-ultra-disk-iops", 5000,
		"Number of IOPS provisioned for ultra disk, only used if network-disk-type=ultra-disk")
	flags.StringVar(&o.diskCaching, ProviderName+"-disk-caching", "none",
		"Disk caching behavior for attached storage.  Valid values are: none, read-only, read-write.  Not applicable to Ultra disks.")
}

// ConfigureClusterFlags implements vm.ProviderFlags and is a no-op.
func (o *providerOpts) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}
