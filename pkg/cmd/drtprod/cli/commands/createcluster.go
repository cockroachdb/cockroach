// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// createOptions are populated based on the cluster name
type createOptions struct {
	// Zones if configured will override the default zones for the cluster
	Zones           []string
	gceProviderOpts *gce.ProviderOpts
	createVMOpts    vm.CreateOpts
	numberOfNodes   int
}

// GetCreateDrtClusterCmd returns the command for creating drt clusters
func GetCreateDrtClusterCmd(ctx context.Context) *cobra.Command {
	// options created with the defaults
	options := &createOptions{
		gceProviderOpts: gce.DefaultProviderOpts(),
		createVMOpts:    vm.DefaultCreateOpts(),
	}
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("create <%s> [flags]", helpers.GetAllClusterNamesAsCSV()),
		Short: "create the drt cluster",
		Long: fmt.Sprintf(`Create a drt prod cluster.
The following clusters are supported: %s
`, helpers.GetAllClusterNamesAsCSV()),
		Args: cobra.ExactArgs(1),
		Run: helpers.Wrap(func(cmd *cobra.Command, args []string) (retErr error) {
			return create(ctx, args, options)
		}),
	}
	configureFlags(ctx, cmd.Flags(), options)
	return cmd
}

// configureFlags configures the flags specific to the create command
func configureFlags(_ context.Context, flags *pflag.FlagSet, options *createOptions) {
	flags.StringSliceVarP(&options.Zones, "zones", "", []string{},
		"override the default zones for the cluster")
}

func create(ctx context.Context, args []string, options *createOptions) (retErr error) {
	clusterName := args[0]
	if err := helpers.ValidateClusterName(clusterName); err != nil {
		return err
	}
	switch clusterName {
	case helpers.DrtLarge:
		updateOptionsForDrtLarge(ctx, clusterName, options)
	case helpers.DrtChaos:
		updateOptionsForDrtChaos(ctx, clusterName, options)
	case helpers.DrtUA1, helpers.DrtUA2:
		updateOptionsForDrtUA(ctx, clusterName, options)
	case helpers.DrtTest:
		updateOptionsForDrtTest(ctx, clusterName, options)
	}
	if len(options.Zones) > 0 {
		// override the default configured zones
		options.gceProviderOpts.Zones = options.Zones
	}
	err := roachprod.Create(ctx, config.Logger, "drt", options.numberOfNodes, options.createVMOpts,
		vm.ProviderOptionsContainer{
			gce.ProviderName: options.gceProviderOpts,
		},
	)
	if err != nil {
		return err
	}
	return ConfigureDns(ctx, clusterName)
}

// updateOptionsForDrtLarge updates the options with the values for drt large
func updateOptionsForDrtLarge(_ context.Context, clusterName string, options *createOptions) {
	options.createVMOpts.ClusterName = clusterName
	options.createVMOpts.Lifetime = 365 * 24 * time.Hour // 1 year
	options.createVMOpts.VMProviders = []string{gce.ProviderName}
	options.createVMOpts.OsVolumeSize = 100
	options.createVMOpts.SSDOpts.UseLocalSSD = true
	options.gceProviderOpts.UseSpot = true
	options.gceProviderOpts.MachineType = "n2-standard-16"
	options.gceProviderOpts.Zones = []string{
		"northamerica-northeast2-a:2", "northamerica-northeast2-b:2", "northamerica-northeast2-c:1",
		"us-east5-a:2", "us-east5-b:2", "us-east5-c:1",
		"northamerica-northeast1-a:2", "northamerica-northeast1-b:2", "northamerica-northeast1-c:1",
	}
	options.gceProviderOpts.SSDCount = 16
	options.gceProviderOpts.UseMultipleDisks = true
	options.gceProviderOpts.Managed = true

	options.numberOfNodes = 15
}

// updateOptionsForDrtChaos updates the options with the values for drt chaos
func updateOptionsForDrtChaos(_ context.Context, clusterName string, options *createOptions) {
	options.createVMOpts.ClusterName = clusterName
	options.createVMOpts.Lifetime = 365 * 24 * time.Hour // 1 year
	options.createVMOpts.VMProviders = []string{gce.ProviderName}
	options.createVMOpts.SSDOpts.UseLocalSSD = true
	options.gceProviderOpts.MachineType = "n2-standard-16"
	options.gceProviderOpts.Zones = []string{"us-east1-d", "us-east1-b", "us-east1-c"}
	options.gceProviderOpts.SSDCount = 4
	options.gceProviderOpts.UseMultipleDisks = true
	options.gceProviderOpts.Managed = true
	options.gceProviderOpts.Image = "ubuntu-2204-jammy-v20240319"

	options.numberOfNodes = 6
}

// updateOptionsForDrtUA updates the options with the values for drt ua clusters
func updateOptionsForDrtUA(_ context.Context, clusterName string, options *createOptions) {
	options.createVMOpts.ClusterName = clusterName
	options.createVMOpts.Lifetime = 365 * 24 * time.Hour // 1 year
	options.createVMOpts.VMProviders = []string{gce.ProviderName}
	options.createVMOpts.SSDOpts.UseLocalSSD = true
	options.gceProviderOpts.MachineType = "n2-standard-16"
	options.gceProviderOpts.Zones = []string{"us-east1-c"}
	options.gceProviderOpts.SSDCount = 8
	options.gceProviderOpts.Image = "ubuntu-2204-jammy-v20240319"

	options.numberOfNodes = 5
}

// updateOptionsForDrtTest is just for testing this code
func updateOptionsForDrtTest(_ context.Context, clusterName string, options *createOptions) {
	options.createVMOpts.ClusterName = clusterName
	options.createVMOpts.Lifetime = 24 * time.Hour // 1 year
	options.createVMOpts.VMProviders = []string{gce.ProviderName}
	options.createVMOpts.SSDOpts.UseLocalSSD = true
	options.gceProviderOpts.UseSpot = true
	options.gceProviderOpts.MachineType = "n2-standard-2"
	options.gceProviderOpts.Zones = []string{"us-east1-c"}
	options.gceProviderOpts.SSDCount = 2

	options.numberOfNodes = 2
}
