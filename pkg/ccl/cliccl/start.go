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
	// Add additional flags to the existing start command.
	// This relies upon init-order between packages.
	cli.StartCmd.Flags().VarP(&storeEncryptionSpecs, "enterprise-encryption", "", encryptionFlagDesc)
	cli.ExtraStartPreRunHook = matchStoreEncryptionSpecs
}

// matchStoreEncryptionSpecs is a PreRun hook that matches store encryption specs with the
// parsed stores.
func matchStoreEncryptionSpecs() error {
	return baseccl.MatchStoreAndEncryptionSpecs(cli.ServerCfg.Stores, storeEncryptionSpecs)
}
