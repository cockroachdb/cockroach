// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

var (
	storeDirFlag = flag.String(
		"store-dir", "", "name of the directory on disk to use for the loaded TPCC state",
	)
	generateStoreDir = flag.Bool("generate-store-dir", false,
		"if store-dir is set, remove any exist data and regenerate the data")
)

func maybeGenerateStoreDir(b testing.TB) (_ vfs.FS, storeDir string, cleanup func()) {
	ctx := context.Background()
	storeDir = *storeDirFlag
	cleanup = func() {}

	if storeDir != "" {
		if !*generateStoreDir {
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
	defer log.Scope(b).Close(b)
	tc := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{{Path: storeDir}},
		},
	})
	defer tc.Stopper().Stop(ctx)

	db := tc.ServerConn(0)
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(b, "CREATE DATABASE "+databaseName)
	tdb.Exec(b, "SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = true")
	tdb.Exec(b, "USE "+databaseName)
	tpcc, err := workload.Get("tpcc")
	require.NoError(b, err)
	gen := tpcc.New().(interface {
		workload.Flagser
		workload.Hookser
		workload.Generator
	})
	require.NoError(b, gen.Flags().Parse([]string{
		"--db=" + databaseName,
	}))
	require.NoError(b, gen.Hooks().Validate())
	{
		var l workloadsql.InsertsDataLoader
		_, err := workloadsql.Setup(ctx, db, gen, l)
		require.NoError(b, err)
	}
	tdb.Exec(b, "SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = false")
	tc.Stopper().Stop(ctx)
	return vfs.Default, storeDir, cleanup
}

func cloneInMemEngine(
	b testing.TB, srcFS vfs.FS, srcPath string,
) (_ base.TestServerArgs, cleanup func()) {
	spec := base.StoreSpec{InMemory: true, StickyInMemoryEngineID: "1"}

	newReg := server.NewStickyInMemEnginesRegistry(server.ReplaceEngines)
	eng, err := newReg.GetOrCreateStickyInMemEngine(context.Background(), &server.Config{}, spec)
	require.NoError(b, err)
	eng.Close()

	newFS, err := newReg.GetUnderlyingFS(spec)
	require.NoError(b, err)
	cloned, err := vfs.Clone(srcFS, newFS, srcPath, "/")
	require.NoError(b, err)
	require.True(b, cloned)
	return base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{spec},
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				StickyEngineRegistry: newReg,
			},
		},
	}, newReg.CloseAllStickyInMemEngines
}

func cloneOnDiskEngine(
	b testing.TB, srcFS vfs.FS, srcPath string,
) (_ base.TestServerArgs, cleanup func()) {
	tempDir, cleanup := testutils.TempDir(b)
	cloned, err := vfs.Clone(srcFS, vfs.Default, srcPath, tempDir)
	require.NoError(b, err)
	require.True(b, cloned)
	return base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{Path: tempDir}},
	}, cleanup
}
