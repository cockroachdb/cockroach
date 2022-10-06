// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	// The following import is required for the hook that populates
	// NewEncryptedEnvFunc in `pkg/storage`.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestDecrypt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir := t.TempDir()

	// Generate a new encryption key to use.
	keyPath := filepath.Join(dir, "aes.key")
	err := cli.GenEncryptionKeyCmd.RunE(nil, []string{keyPath})
	require.NoError(t, err)

	// Spin up a new encrypted store.
	encSpecStr := fmt.Sprintf("path=%s,key=%s,old-key=plain", dir, keyPath)
	encSpec, err := baseccl.NewStoreEncryptionSpec(encSpecStr)
	require.NoError(t, err)
	encOpts, err := encSpec.ToEncryptionOptions()
	require.NoError(t, err)
	p, err := storage.Open(ctx, storage.Filesystem(dir), storage.EncryptionAtRest(encOpts))
	require.NoError(t, err)

	// Find a manifest file to check.
	files, err := p.List(dir)
	require.NoError(t, err)
	var manifestPath string
	for _, basename := range files {
		if strings.HasPrefix(basename, "MANIFEST-") {
			manifestPath = filepath.Join(dir, basename)
			break
		}
	}
	// Should have found a manifest file.
	require.NotEmpty(t, manifestPath)

	// Close the DB.
	p.Close()

	// Pluck the `pebble manifest dump` command out of the debug command.
	dumpCmd := getTool(cli.DebugPebbleCmd, []string{"pebble", "manifest", "dump"})
	require.NotNil(t, dumpCmd)

	dumpManifest := func(cmd *cobra.Command, path string) string {
		var b bytes.Buffer
		dumpCmd.SetOut(&b)
		dumpCmd.SetErr(&b)
		dumpCmd.Run(cmd, []string{path})
		return b.String()
	}
	out := dumpManifest(dumpCmd, manifestPath)
	// Check for the presence of the comparator line in the manifest dump, as a
	// litmus test for the manifest file being readable. This line should only
	// appear once the file has been decrypted.
	const checkStr = "comparer:     cockroach_comparator"
	require.NotContains(t, out, checkStr)

	// Decrypt the manifest file.
	outPath := filepath.Join(dir, "manifest.plain")
	decryptCmd := getTool(cli.DebugCmd, []string{"debug", "encryption-decrypt"})
	require.NotNil(t, decryptCmd)
	err = decryptCmd.Flags().Set("enterprise-encryption", encSpecStr)
	require.NoError(t, err)
	err = decryptCmd.RunE(decryptCmd, []string{dir, manifestPath, outPath})
	require.NoError(t, err)

	// Check that the decrypted manifest file can now be read.
	out = dumpManifest(dumpCmd, outPath)
	require.Contains(t, out, checkStr)
}

// getTool traverses the given cobra.Command recursively, searching for a tool
// matching the given command.
func getTool(cmd *cobra.Command, want []string) *cobra.Command {
	// Base cases.
	if cmd.Name() != want[0] {
		return nil
	}
	if len(want) == 1 {
		return cmd
	}
	// Recursive case.
	for _, subCmd := range cmd.Commands() {
		found := getTool(subCmd, want[1:])
		if found != nil {
			return found
		}
	}
	return nil
}
