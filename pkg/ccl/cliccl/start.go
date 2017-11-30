// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/spf13/cobra"
)

// This does not define a `start` command, only modifications to the existing command
// in `pkg/cli/start.go`.

var storeEncryptionSpecs baseccl.StoreEncryptionSpecList

const (
	// TODO(mberhault): use pkg/cli helpers to format flag descriptions.
	encryptionFlagDesc = `
        *** Valid enterprise licenses only ***

        TODO(mberhault): fill this.

        Valid fields:
        * path: must match the path of one of the stores
        * key: path to the current key file
        * old-key: path to the previous key file
        * rotation-period: amount of time after which data keys should be rotated
`
)

func init() {
	cli.StartCmd.Flags().VarP(&storeEncryptionSpecs, "enterprise-encryption", "", encryptionFlagDesc)

	// Add a new pre-run command to match encryption specs to store specs.
	cli.AddPersistentPreRunE(cli.StartCmd, func(cmd *cobra.Command, _ []string) error {
		return populateStoreSpecsEncryption()
	})
}

// populateStoreSpecsEncryption is a PreRun hook that matches store encryption specs with the
// parsed stores and populates some fields in the StoreSpec.
func populateStoreSpecsEncryption() error {
	return baseccl.PopulateStoreSpecWithEncryption(cli.GetServerCfgStores(), storeEncryptionSpecs)
}
