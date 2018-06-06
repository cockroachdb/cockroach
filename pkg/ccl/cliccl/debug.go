// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/cliccl/cliflagsccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

// This does not define new commands, only adds the encryption flag to debug commands in
// `pkg/cli/debug.go` and registers a callback to generate encryption options.

func init() {
	for _, cmd := range cli.DebugCmdsForRocksDB {
		// storeEncryptionSpecs is in start.go.
		cli.VarFlag(cmd.Flags(), &storeEncryptionSpecs, cliflagsccl.EnterpriseEncryption)
	}

	cli.PopulateRocksDBConfigHook = fillEncryptionOptionsForStore
}

// fillEncryptionOptionsForStore fills the RocksDBConfig fields
// based on the --enterprise-encryption flag value.
func fillEncryptionOptionsForStore(cfg *engine.RocksDBConfig) error {
	opts, err := baseccl.EncryptionOptionsForStore(cfg.Dir, storeEncryptionSpecs)
	if err != nil {
		return err
	}

	if opts != nil {
		cfg.ExtraOptions = opts
		cfg.UseFileRegistry = true
	}
	return nil
}
