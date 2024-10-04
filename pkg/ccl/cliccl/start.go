// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/cliccl/cliflagsccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflagcfg"
	"github.com/spf13/cobra"
)

// This does not define a `start` command, only modifications to the existing command
// in `pkg/cli/start.go`.

var storeEncryptionSpecs baseccl.StoreEncryptionSpecList

func init() {
	for _, cmd := range cli.StartCmds {
		cliflagcfg.VarFlag(cmd.Flags(), &storeEncryptionSpecs, cliflagsccl.EnterpriseEncryption)

		// Add a new pre-run command to match encryption specs to store specs.
		cli.AddPersistentPreRunE(cmd, func(cmd *cobra.Command, _ []string) error {
			return populateStoreSpecsEncryption()
		})
	}
}

// populateStoreSpecsEncryption is a PreRun hook that matches store encryption specs with the
// parsed stores and populates some fields in the StoreSpec.
func populateStoreSpecsEncryption() error {
	return baseccl.PopulateStoreSpecWithEncryption(cli.GetServerCfgStores(), storeEncryptionSpecs)
}
