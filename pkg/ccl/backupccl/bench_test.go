// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/stretchr/testify/require"
)

func BenchmarkDatabaseBackup(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(b, multiNode,
		0 /* numAccounts */, InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	loadURI := "nodelocal://0/load"
	if _, err := sampledataccl.ToBackup(b, bankData, dir, "load"); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, fmt.Sprintf(`RESTORE data.* FROM '%s'`, loadURI))

	// TODO(dan): Ideally, this would split and rebalance the ranges in a more
	// controlled way. A previous version of this code did it manually with
	// `SPLIT AT` and TestCluster's TransferRangeLease, but it seemed to still
	// be doing work after returning, which threw off the timing and the results
	// of the benchmark. DistSQL is working on improving this infrastructure, so
	// use what they build.

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, localFoo)).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
}

func BenchmarkDatabaseRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, sqlDB, dir, cleanup := backupRestoreTestSetup(b, multiNode,
		0 /* numAccounts*/, InitManualReplication)
	defer cleanup()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	if _, err := sampledataccl.ToBackup(b, bankData, dir, "foo"); err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	sqlDB.Exec(b, `RESTORE data.* FROM 'nodelocal://0/foo'`)
	b.StopTimer()
}

func BenchmarkEmptyIncrementalBackup(b *testing.B) {
	const numStatements = 100000

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(b, multiNode,
		0 /* numAccounts */, InitManualReplication)
	defer cleanupFn()

	restoreURI := localFoo + "/restore"
	fullURI := localFoo + "/full"

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, dir, "foo/restore")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreURI)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullURI).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		incrementalDir := localFoo + fmt.Sprintf("/incremental%d", i)
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`, incrementalDir, fullURI)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}

func BenchmarkDatabaseFullBackup(b *testing.B) {
	const numStatements = 100000

	_, sqlDB, dir, cleanupFn := backupRestoreTestSetup(b, multiNode,
		0 /* numAccounts */, InitManualReplication)
	defer cleanupFn()

	restoreURI := localFoo + "/restore"
	fullURI := localFoo + "/full"

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, dir, "foo/restore")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreURI)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullURI).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		backupDir := localFoo + fmt.Sprintf("/backup%d", i)
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1`, backupDir)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}

func BenchmarkIteratorMemory(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()

	numAccounts := 1000
	tc, _, _, cleanupFn := backupRestoreTestSetup(b, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	for _, testCase := range []struct {
		testName string
	}{
		{
			testName: "gcp",
		},
		{
			testName: "aws",
		},
	} {
		b.Run(testCase.testName, func(b *testing.B) {
			sst := envutil.EnvOrDefaultString("SST_PATH", "unknown")
			encKey := envutil.EnvOrDefaultString("ENCRYPTION_KEY", "invalid")

			uri, err := url.Parse(sst)
			if err != nil {
				b.Fatal(err)
			}

			storeURI := fmt.Sprintf("%s://%s/%s?AUTH=implicit", uri.Scheme, uri.Host, path.Dir(uri.Path))
			dataFile := path.Base(uri.Path)

			store, err := cloud.ExternalStorageFromURI(
				ctx,
				storeURI,
				base.ExternalIODirConfig{},
				tc.Servers[0].ClusterSettings(),
				blobs.TestEmptyBlobClientFactory,
				username.RootUserName(),
				tc.Servers[0].InternalDB().(isql.DB),
				nil, /* limiters */
				cloud.NilMetrics,
			)

			require.NoError(b, err)

			for _, encrypted := range []bool{true, false} {
				for _, iterCount := range []int{1, 10, 100, 1000} {
					for _, fileCount := range []int{10, 100, 1000, 10000} {
						b.Run(fmt.Sprintf("fileCount=%d/iterCount=%d/enc=%t", fileCount, iterCount, encrypted), func(b *testing.B) {
							b.ResetTimer()

							fileStores := make([]storageccl.StoreFile, fileCount)

							for i := 0; i < fileCount; i++ {
								fileStores[i].Store = store
								fileStores[i].FilePath = dataFile
							}

							execCfg := tc.Server(0).ExecutorConfig().(sql.ExecutorConfig)
							iterOpts := storage.IterOptions{
								RangeKeyMaskingBelow: execCfg.Clock.Now(),
								KeyTypes:             storage.IterKeyTypePointsAndRanges,
								LowerBound:           keys.LocalMax,
								UpperBound:           keys.MaxKey,
							}

							iters := make([]storage.SimpleMVCCIterator, iterCount)
							cleanup := func() {
								for _, iter := range iters {
									if iter != nil {
										iter.Close()
									}
								}
							}
							defer cleanup()

							var enc *kvpb.FileEncryptionOptions
							if encrypted {
								enc = &kvpb.FileEncryptionOptions{
									Key: []byte(encKey),
								}
							}

							for j := 0; j < iterCount; j++ {
								iter, err := storageccl.ExternalSSTReader(ctx, fileStores, enc, iterOpts)
								require.NoError(b, err)

								iters[j] = iter
								iter.SeekGE(storage.MVCCKey{})
							}

							b.StopTimer()
						})
					}
				}
			}
		})
	}
}
