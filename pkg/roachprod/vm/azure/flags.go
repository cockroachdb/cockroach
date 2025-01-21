// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/spf13/pflag"
)

// ProviderOpts provides user-configurable, azure-specific create options.
type ProviderOpts struct {
	// N.B. Azure splits up the region (location) and availability zone, but
	// to keep things consistent with other providers, we treat zone to mean
	// both and split it up later.
	Zones           []string
	MachineType     string
	VnetName        string
	NetworkDiskType string
	NetworkDiskSize int32
	UltraDiskIOPS   int64
	DiskCaching     string
}

// These default locations support availability zones. At the time of
// this comment, `westus` did not and `westus2` is consistently out of
// capacity.
var DefaultZones = []string{
	"eastus-1",
	"canadacentral-1",
	"westus3-1",
}

// DefaultProviderOpts returns a new azure.ProviderOpts with default values set.
func DefaultProviderOpts() *ProviderOpts {
	return &ProviderOpts{
		Zones:           nil,
		MachineType:     string(armcompute.VirtualMachineSizeTypesStandardD4V3),
		VnetName:        "common",
		NetworkDiskType: "premium-disk",
		NetworkDiskSize: 500,
		UltraDiskIOPS:   5000,
		DiskCaching:     "none",
	}
}

// CreateProviderOpts returns a new azure.ProviderOpts with default values set.
func (p *Provider) CreateProviderOpts() vm.ProviderOpts {
	return DefaultProviderOpts()
}

// ConfigureProviderFlags implements vm.ProviderFlags and is a no-op.
func (p *Provider) ConfigureProviderFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

// ConfigureClusterCleanupFlags is part of ProviderOpts.
func (o *Provider) ConfigureClusterCleanupFlags(flags *pflag.FlagSet) {
	flags.StringSliceVar(&providerInstance.SubscriptionNames, ProviderName+"-subscription-names", []string{},
		"Azure subscription names as a comma-separated string")
}

// ConfigureCreateFlags implements vm.ProviderFlags.
func (o *ProviderOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.DurationVar(&providerInstance.OperationTimeout, ProviderName+"-timeout", providerInstance.OperationTimeout,
		"The maximum amount of time for an Azure API operation to take")
	flags.BoolVar(&providerInstance.SyncDelete, ProviderName+"-sync-delete", providerInstance.SyncDelete,
		"Wait for deletions to finish before returning")
	flags.StringVar(&o.MachineType, ProviderName+"-machine-type",
		string(armcompute.VirtualMachineSizeTypesStandardD4V3),
		"Machine type (see https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)")
	flags.StringSliceVar(&o.Zones, ProviderName+"-zones", nil,
		fmt.Sprintf("Zones for cluster, where a zone is a location (see `az account list-locations`)\n"+
			"and availability zone seperated by a dash. If zones are formatted as Location-AZ:N where N is an integer,\n"+
			"the zone will be repeated N times. If > 1 zone specified, nodes will be geo-distributed\n"+
			"regardless of geo (default [%s])",
			strings.Join(DefaultZones, ",")))
	flags.StringVar(&o.VnetName, ProviderName+"-vnet-name", "common",
		"The name of the VNet to use")
	flags.StringVar(&o.NetworkDiskType, ProviderName+"-network-disk-type", "premium-disk",
		"type of network disk [premium-disk, ultra-disk]. only used if local-ssd is false")
	flags.Int32Var(&o.NetworkDiskSize, ProviderName+"-volume-size", 500,
		"Size in GB of network disk volume, only used if local-ssd=false")
	flags.Int64Var(&o.UltraDiskIOPS, ProviderName+"-ultra-disk-iops", 5000,
		"Number of IOPS provisioned for ultra disk, only used if network-disk-type=ultra-disk")
	flags.StringVar(&o.DiskCaching, ProviderName+"-disk-caching", "none",
		"Disk caching behavior for attached storage.  Valid values are: none, read-only, read-write.  Not applicable to Ultra disks.")
}
