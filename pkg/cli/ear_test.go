// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
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
	err := genEncryptionKeyCmd.RunE(nil, []string{keyPath})
	require.NoError(t, err)

	// Spin up a new encrypted store.
	encSpecStr := fmt.Sprintf("path=%s,key=%s,old-key=plain", dir, keyPath)
	encSpec, err := baseccl.NewStoreEncryptionSpec(encSpecStr)
	require.NoError(t, err)
	encOpts, err := encSpec.ToEncryptionOptions()
	require.NoError(t, err)

	p, err := storage.NewPebble(ctx, storage.PebbleConfig{
		StorageConfig: base.StorageConfig{
			Dir:               dir,
			UseFileRegistry:   true,
			EncryptionOptions: encOpts,
		},
		Opts: &pebble.Options{
			FS: vfs.Default,
		},
	})
	require.NoError(t, err)
	defer p.Close()

	// Write some data and flush the memtable.
	err = p.PutUnversioned([]byte("foo"), []byte("bar"))
	require.NoError(t, err)
	require.NoError(t, p.Flush())

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

	// Pluck the `pebble manifest dump` command out of the debug command.
	dumpCmd := getPebbleTool(DebugPebbleCmd, []string{"pebble", "manifest", "dump"})
	require.NotNil(t, dumpCmd)

	// Check that the encrypted manifest file cannot be read.
	dumpManifest := func(path string) string {
		var b bytes.Buffer
		dumpCmd.SetOut(&b)
		dumpCmd.SetErr(&b)
		dumpCmd.Run(dumpCmd, []string{path})
		return b.String()
	}
	out := dumpManifest(manifestPath)
	// Check for the addition of the table that was flushed.
	require.NotContains(t, out, "added:")

	// Decrypt the manifest file.
	outPath := filepath.Join(dir, "manifest.plain")
	err = earDecryptCmd.RunE(earDecryptCmd, []string{encSpecStr, manifestPath, outPath})
	require.NoError(t, err)

	// Check that the decrypted manifest file can now be read.
	out = dumpManifest(outPath)
	require.Contains(t, out, "added:")
}

// getPebbleTool traverses the given cobra.Command recursively, searching for a
// Pebble tool matching the given command.
func getPebbleTool(cmd *cobra.Command, want []string) *cobra.Command {
	// Base cases.
	if cmd.Name() != want[0] {
		return nil
	}
	if len(want) == 1 {
		return cmd
	}
	// Recursive case.
	for _, subCmd := range cmd.Commands() {
		found := getPebbleTool(subCmd, want[1:])
		if found != nil {
			return found
		}
	}
	return nil
}
