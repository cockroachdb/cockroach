// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/spf13/pflag"
)

// ProviderOpts provides user-configurable, IBM-specific create options.
type ProviderOpts struct {
	// MachineType is the IBM instance profile to use.
	MachineType string

	// RemoteUserName is the name of the remote user to SSH as.
	// IBM doesn't support overriding the remote user name, so this defaults
	// to "ubuntu" for Ubuntu images.
	RemoteUserName string

	// DefaultVolume is the default data volume to be attached.
	DefaultVolume IbmVolume

	// AttachedVolumes is a list of additional volumes to be attached.
	AttachedVolumes IbmVolumeList

	// UseMultipleDisks indicates whether disks will be mounted in their distinct
	// mount points or aggregated as RAID0 in a single mount point.
	UseMultipleDisks bool

	// Use specified ImageAMI when provisioning.
	// Overrides config.json AMI.
	ImageAMI string

	// CreateZones stores the list of zones for used cluster creation.
	// When > 1 zone specified, geo is automatically used, otherwise, geo depends
	// on the geo flag being set. If no zones specified, defaultCreateZones are
	// used. See defaultCreateZones.
	CreateZones []string

	// TerminateOnMigration defines wether or not the instance will be migrated
	// or terminated in case of a host failure or maintenance event. The default
	// is to migrate the instance, which is the same as setting this to false.
	TerminateOnMigration bool
}

// Volume represents a volume to be attached to an IBM instance.
type IbmVolume struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	IOPS       int    `json:"IOPS"`
}

type IbmVolumeList []*IbmVolume

// Set implements flag Value interface.
func (vl *IbmVolumeList) Set(s string) error {
	v := &IbmVolume{}
	err := json.Unmarshal([]byte(s), &v)
	if err != nil {
		return err
	}
	*vl = append(*vl, v)
	return nil
}

// Type implements flag Value interface.
func (vl *IbmVolumeList) Type() string {
	return "JSON"
}

// String Implements flag Value interface.
func (vl *IbmVolumeList) String() string {
	return "IBMVolumeList"
}

// DefaultProviderOpts returns the default IBM provider options.
func DefaultProviderOpts() *ProviderOpts {
	return &ProviderOpts{
		MachineType:          defaultMachineType,
		ImageAMI:             defaultImageAMI,
		RemoteUserName:       defaultRemoteUser,
		DefaultVolume:        IbmVolume{VolumeType: defaultVolumeType, VolumeSize: defaultVolumeSize},
		AttachedVolumes:      IbmVolumeList{},
		TerminateOnMigration: false,
	}
}

// ConfigureCreateFlags is part of the vm.ProviderOpts interface.
func (o *ProviderOpts) ConfigureCreateFlags(flags *pflag.FlagSet) {

	// cz2-8x16 is a 4 core/16Gb instance, equivalent to a GCE n2-standard-4
	flags.StringVar(
		&o.MachineType,
		ProviderName+"-machine-type",
		o.MachineType,
		"s390x instance profile (see https://cloud.ibm.com/docs/vpc?topic=vpc-vs-profiles)",
	)

	// Default data volume (network disk)
	flags.StringVar(
		&o.DefaultVolume.VolumeType,
		ProviderName+"-volume-type",
		o.DefaultVolume.VolumeType,
		"Type of the default data volume",
	)
	flags.IntVar(
		&o.DefaultVolume.VolumeSize,
		ProviderName+"-volume-size",
		o.DefaultVolume.VolumeSize,
		"Size in GB of the default data volume",
	)
	flags.IntVar(&o.DefaultVolume.IOPS,
		ProviderName+"-volume-iops",
		o.DefaultVolume.IOPS,
		"Custom number of IOPS to provision for the default data volume",
	)

	// Additional attached volumes
	flags.VarP(
		&o.AttachedVolumes,
		ProviderName+"-attached-volume",
		"",
		`Additional volume to attach, repeated for extra volumes.
Specified as JSON: '{ "VolumeType": "general-purpose", "VolumeSize": 1000, "IOPS": 15000 }'
- VolumeType: 'general-purpose' (3 IOPS/GB), '5iops-tier' (5 IOPS/GB), '10iops-tier' (10 IOPS/GB), 'custom' (specify IOPS)
- VolumeSize: Size in GB of the volume
- IOPS: IOPS for the volume`,
	)

	flags.StringSliceVar(
		&o.CreateZones,
		ProviderName+"-zones",
		o.CreateZones,
		fmt.Sprintf(
			`IBM availability zones to use for cluster creation.
If zones are formatted as AZ:N where N is an integer, the zone will be repeated N times.
If > 1 zone specified, the cluster will be spread out evenly by zone regardless of geo.
(default [%s])`,
			strings.Join(DefaultZones(true), ","),
		),
	)

	flags.BoolVar(
		&o.UseMultipleDisks,
		ProviderName+"-enable-multiple-stores",
		false,
		`Enable the use of multiple stores by creating one store directory per disk. Default is to raid0 stripe all disks.
See repeating --`+ProviderName+`-attached-volume for adding extra volumes.`,
	)

}

// ConfigureProviderFlags is part of the vm.Provider interface.
func (p *Provider) ConfigureProviderFlags(flags *pflag.FlagSet, _ vm.MultipleProjectsOption) {}

// ConfigureClusterCleanupFlags is part of the vm.Provider interface.
func (p *Provider) ConfigureClusterCleanupFlags(flags *pflag.FlagSet) {
	flags.StringSliceVar(
		&providerInstance.GCAccounts,
		ProviderName+"-accounts",
		[]string{},
		`List of accounts to GC. Accounts are expected to match API keys in the environment with the format:
IBM_<account>_APIKEY`,
	)
}

// CreateProviderOpts is part of the vm.Provider interface.
func (p *Provider) CreateProviderOpts() vm.ProviderOpts {
	return DefaultProviderOpts()
}
