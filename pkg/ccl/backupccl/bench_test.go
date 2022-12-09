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
	"io"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// BenchmarkIteratorMemory benchmarks the memory used for creating an SST
// iterator using ExternalSSTReader. It is meant to be used benchmark iterators
// for files on cloud providers. The test constructs an SST and writes it out to
// the cloud storage. It then repeatedly calls ExternalSSTReader to create
// iterators over multiple instances of the same SST.
//
// For example, to run on AWS:
//
//	dev bench ./pkg/ccl/backupccl/ --filter \
//	BenchmarkIteratorMemory/fileCount=100$/iterCount=10$/rows=200000$/enc=false \
//	--bench-mem --test_env=COCKROACH_SST_DIR=<dir> \
//	--test_env=AWS_ACCESS_KEY_ID=<ak> --test_env=AWS_SECRET_ACCESS_KEY=<sk>
func BenchmarkIteratorMemory(b *testing.B) {
	defer leaktest.AfterTest(b)()
	ctx := context.Background()

	numAccounts := 1000
	tc, _, _, cleanupFn := backupRestoreTestSetup(b, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	now := tc.Server(0).Clock().Now()

	makeWriter := func(
		store cloud.ExternalStorage,
		filename string,
		enc *jobspb.BackupEncryptionOptions) (io.WriteCloser, error) {
		w, err := store.Writer(ctx, filename)
		if err != nil {
			return nil, err
		}

		if enc != nil {
			key, err := backupencryption.GetEncryptionKey(ctx, enc, nil)
			if err != nil {
				return nil, err
			}
			encW, err := storageccl.EncryptingWriter(w, key)
			if err != nil {
				return nil, err
			}
			w = encW
		}
		return w, nil
	}

	getRandomPayload := func(buf []byte) {
		const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		const lettersLen = uint32(len(letters))
		for i := 0; i < len(buf); i++ {
			buf[i] = letters[rand.Uint32()%lettersLen]
		}
	}

	writeSST := func(w io.WriteCloser, store cloud.ExternalStorage, payloadSize int, numKeys int) {
		sst := storage.MakeBackupSSTWriter(ctx, store.Settings(), w)

		buf := make([]byte, payloadSize)
		key := storage.MVCCKey{Timestamp: now}
		for i := 0; i < numKeys; i++ {
			getRandomPayload(buf)
			key.Key = []byte(fmt.Sprintf("id-%09d", i))
			require.NoError(b, sst.Put(key, buf))
		}

		sst.Close()
	}

	sstDir := envutil.EnvOrDefaultString("COCKROACH_SST_DIR", "")
	require.NotEmpty(b, sstDir, "COCKROACH_SST_DIR must be set")
	storeURI := fmt.Sprintf("%s?AUTH=implicit", sstDir)

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

	for _, rows := range []int{1_000, 2_000, 10_000, 20_000, 50_000, 100_000, 200_000} {
		for _, encrypted := range []bool{true, false} {
			for _, iterCount := range []int{1, 10, 100, 1000} {
				for _, fileCount := range []int{10, 100, 1000, 10000} {
					b.Run(fmt.Sprintf("fileCount=%d/iterCount=%d/rows=%d/enc=%t", fileCount, iterCount, rows, encrypted), func(b *testing.B) {
						var enc *jobspb.BackupEncryptionOptions
						var encSuffix string
						if encrypted {
							const passphrase = "testing-encr-key"
							enc = &jobspb.BackupEncryptionOptions{
								Key:  []byte(passphrase),
								Mode: jobspb.EncryptionMode_Passphrase,
							}
							encSuffix = "-enc"
						}
						filename := fmt.Sprintf("test-%d-%d%s", now.WallTime, rows, encSuffix)
						w, err := makeWriter(store, filename, enc)
						require.NoError(b, err)

						writeSST(w, store, 100, rows)
						require.NoError(b, w.Close())

						sz, err := store.Size(ctx, filename)
						require.NoError(b, err)

						log.Infof(ctx, "Benchmarking using file of size %s", humanizeutil.IBytes(sz))
						fileStores := make([]storageccl.StoreFile, fileCount)
						for i := 0; i < fileCount; i++ {
							fileStores[i].Store = store
							fileStores[i].FilePath = filename
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

						var encOpts *kvpb.FileEncryptionOptions
						if enc != nil {
							key, err := backupencryption.GetEncryptionKey(ctx, enc, nil)
							require.NoError(b, err)
							encOpts = &kvpb.FileEncryptionOptions{Key: key}
						}

						b.ResetTimer()

						for j := 0; j < iterCount; j++ {
							iter, err := storageccl.ExternalSSTReader(ctx, fileStores, encOpts, iterOpts)
							require.NoError(b, err)

							iters[j] = iter
							iter.SeekGE(storage.MVCCKey{})
						}

						b.StopTimer()
					})
				}
			}
		}
	}
}
