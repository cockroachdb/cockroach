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
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/spf13/pflag"
)

// ProviderOpts provides user-configurable, azure-specific create options.
type ProviderOpts struct {
	Locations        []string
	MachineType      string
	OperationTimeout time.Duration
	SyncDelete       bool
	VnetName         string
	Zone             string
	NetworkDiskType  string
	NetworkDiskSize  int32
	UltraDiskIOPS    int64
	DiskCaching      string
}

var defaultLocations = []string{
	"eastus2",
	"westus",
	"westeurope",
}

var defaultZone = "1"

// DefaultProviderOpts returns a new azure.ProviderOpts with default values set.
func DefaultProviderOpts() ProviderOpts {
	return ProviderOpts{
		Locations:        nil,
		MachineType:      string(compute.VirtualMachineSizeTypesStandardD4V3),
		OperationTimeout: 10 * time.Minute,
		SyncDelete:       false,
		VnetName:         "common",
		Zone:             "",
		NetworkDiskType:  "premium-disk",
		NetworkDiskSize:  500,
		UltraDiskIOPS:    5000,
		DiskCaching:      "none",
	}
}

// ConfigureCreateFlags implements vm.ProviderFlags.
func (o *ProviderOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {
	flags.DurationVar(&o.OperationTimeout, ProviderName+"-timeout", 10*time.Minute,
		"The maximum amount of time for an Azure API operation to take")
	flags.BoolVar(&o.SyncDelete, ProviderName+"-sync-delete", false,
		"Wait for deletions to finish before returning")
	flags.StringVar(&o.MachineType, ProviderName+"-machine-type",
		string(compute.VirtualMachineSizeTypesStandardD4V3),
		"Machine type (see https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/)")
	flags.StringSliceVar(&o.Locations, ProviderName+"-locations", nil,
		fmt.Sprintf("Locations for cluster (see `az account list-locations`) (default\n[%s])",
			strings.Join(defaultLocations, ",")))
	flags.StringVar(&o.VnetName, ProviderName+"-vnet-name", "common",
		"The name of the VNet to use")
	flags.StringVar(&o.Zone, ProviderName+"-availability-zone", "", "Availability Zone to create VMs in")
	flags.StringVar(&o.NetworkDiskType, ProviderName+"-network-disk-type", "premium-disk",
		"type of network disk [premium-disk, ultra-disk]. only used if local-ssd is false")
	flags.Int32Var(&o.NetworkDiskSize, ProviderName+"-volume-size", 500,
		"Size in GB of network disk volume, only used if local-ssd=false")
	flags.Int64Var(&o.UltraDiskIOPS, ProviderName+"-ultra-disk-iops", 5000,
		"Number of IOPS provisioned for ultra disk, only used if network-disk-type=ultra-disk")
	flags.StringVar(&o.DiskCaching, ProviderName+"-disk-caching", "none",
		"Disk caching behavior for attached storage.  Valid values are: none, read-only, read-write.  Not applicable to Ultra disks.")
}

// ConfigureClusterFlags implements vm.ProviderFlags and is a no-op.
func (o *ProviderOpts) ConfigureClusterFlags(*pflag.FlagSet, vm.MultipleProjectsOption) {
}

// ConfigureProviderOpts implements vm.ProviderFlags.
// Usage: create a new struct with default values using DefaultProviderOpts()
// and update its values then pass it to ConfigureProviderOpts().
func (o *ProviderOpts) ConfigureProviderOpts(updatedOpts interface{}) {
	// cast interface to ProviderOpts before assisgning
	*o = updatedOpts.(ProviderOpts)
}
