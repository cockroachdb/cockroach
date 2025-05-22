// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tpcc

import (
	"flag"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var (
	storeDirFlag = flag.String(
		"store-dir", "", "name of the directory on disk to use for the loaded TPCC state",
	)
	generateStoreDirFlag = flag.Bool("generate-store-dir", false,
		"if store-dir is set, remove any exist data and regenerate the data")
)

func maybeGenerateStoreDir(b testing.TB) (_ vfs.FS, storeDir string, cleanup func()) {
	storeDir = *storeDirFlag
	cleanup = func() {}

	if storeDir != "" {
		if !*generateStoreDirFlag {
			fi, err := os.Stat(storeDir)
			require.NoError(b, err, "consider --generate-store-dir")
			require.True(b, fi.IsDir(), "consider --generate-store-dir")
			return vfs.Default, *storeDirFlag, cleanup
		}
		require.NoError(b, os.RemoveAll(storeDir))
		require.NoError(b, os.MkdirAll(storeDir, 0777))
	} else {
		storeDir, cleanup = testutils.TempDir(b)
		defer func() {
			if b.Failed() {
				cleanup()
			}
		}()
	}

	cmd, output := generateStoreDir.withEnv(storeDirEnvVar, storeDir).exec()
	if err := cmd.Run(); err != nil {
		b.Fatalf("failed to generate store dir: %s\n%s", err, output.String())
	}
	return vfs.Default, storeDir, cleanup
}
