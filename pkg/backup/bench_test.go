// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backupencryption"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// BenchmarkIteratorMemory benchmarks the memory used for creating an SST
// iterator using ExternalSSTReader. It is meant to be used benchmark iterators
// for files on cloud providers. The test constructs an SST and writes it out to
// the cloud storage. It then repeatedly calls ExternalSSTReader to create
// iterators over multiple instances of the same SST.
//
// To run:
//
//	dev bench ./pkg/backup/ --filter \
//	BenchmarkIteratorMemory/fileCount=100$/iterCount=10$/rows=200000$/enc=false \
//	--bench-mem --test_env=COCKROACH_S3_SST_DIR=<s3_dir> \
//	--test_env=COCKROACH_GCS_SST_DIR=<s3_dir> \
//	--test_env=AWS_ACCESS_KEY_ID=<ak> --test_env=AWS_SECRET_ACCESS_KEY=<sk> \
//	--test_env=GOOGLE_APPLICATION_CREDENTIALS=<json>
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
		sst := storage.MakeTransportSSTWriter(ctx, store.Settings(), w)

		buf := make([]byte, payloadSize)
		key := storage.MVCCKey{Timestamp: now}
		for i := 0; i < numKeys; i++ {
			getRandomPayload(buf)
			key.Key = []byte(fmt.Sprintf("id-%09d", i))
			require.NoError(b, sst.Put(key, buf))
		}

		sst.Close()
	}

	for _, testCase := range []struct {
		storeType string
		envVar    string
	}{
		{
			"s3",
			"COCKROACH_S3_SST_DIR",
		},
		{
			"gcs",
			"COCKROACH_GCS_SST_DIR",
		},
	} {
		sstDir := envutil.EnvOrDefaultString(testCase.envVar, "")
		if sstDir == "" {
			skip.IgnoreLintf(b, "Environment variable %s not set", testCase.envVar)
		}
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

		b.ReportAllocs()

		for _, rows := range []int{1_000, 10_000, 50_000, 100_000, 200_000} {
			for _, encrypted := range []bool{true, false} {
				for _, iterCount := range []int{1, 10, 100} {
					for _, fileCount := range []int{10, 100, 1000} {
						b.Run(fmt.Sprintf("fileCount=%d/iterCount=%d/rows=%d/store=%s/enc=%t",
							fileCount, iterCount, rows, testCase.storeType, encrypted), func(b *testing.B) {
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
}
