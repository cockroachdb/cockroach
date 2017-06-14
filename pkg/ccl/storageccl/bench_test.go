// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl_test

import (
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/dataccl"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
)

func BenchmarkAddSSTable(b *testing.B) {
	defer storage.TestingSetDisableSnapshotClearRange(true)()
	tempDir, dirCleanupFn := testutils.TempDir(b)
	defer dirCleanupFn()

	for _, numEntries := range []int{100, 1000, 10000, 100000} {
		bankData := dataccl.Bank(numEntries, dataccl.CfgDefault, dataccl.CfgDefault)
		backupDir := filepath.Join(tempDir, strconv.Itoa(numEntries))
		backup, err := dataccl.ToBackup(b, bankData, backupDir)
		if err != nil {
			b.Fatalf("%+v", err)
		}

		b.Run(strconv.Itoa(numEntries), func(b *testing.B) {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)
			kvDB := tc.Server(0).KVClient().(*client.DB)

			id := sqlbase.ID(keys.MaxReservedDescID + 1)

			var totalLen int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				sst, err := engine.MakeRocksDBSstFileWriter()
				if err != nil {
					b.Fatalf("%+v", err)
				}

				id++
				backup.ResetKeyValueIteration()
				kvs, span, err := backup.NextKeyValues(numEntries, id)
				if err != nil {
					b.Fatalf("%+v", err)
				}
				for _, kv := range kvs {
					if err := sst.Add(kv); err != nil {
						b.Fatalf("%+v", err)
					}
				}
				data, err := sst.Finish()
				if err != nil {
					b.Fatalf("%+v", err)
				}
				sst.Close()
				totalLen += int64(len(data))
				b.StartTimer()

				if err := kvDB.ExperimentalAddSSTable(ctx, span.Key, span.EndKey, data); err != nil {
					b.Fatalf("%+v", err)
				}
			}
			b.StopTimer()
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	tempDir, dirCleanupFn := testutils.TempDir(b)
	defer dirCleanupFn()

	for _, numEntries := range []int{100, 1000, 10000, 100000} {
		bankData := dataccl.Bank(numEntries, dataccl.CfgDefault, dataccl.CfgDefault)
		backupDir := filepath.Join(tempDir, strconv.Itoa(numEntries))
		backup, err := dataccl.ToBackup(b, bankData, backupDir)
		if err != nil {
			b.Fatalf("%+v", err)
		}

		b.Run(strconv.Itoa(numEntries), func(b *testing.B) {
			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)
			kvDB := tc.Server(0).KVClient().(*client.DB)

			id := sqlbase.ID(keys.MaxReservedDescID + 1)
			var batch engine.RocksDBBatchBuilder

			var totalLen int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				id++
				backup.ResetKeyValueIteration()
				kvs, span, err := backup.NextKeyValues(numEntries, id)
				if err != nil {
					b.Fatalf("%+v", err)
				}
				for _, kv := range kvs {
					batch.Put(kv.Key, kv.Value)
				}
				repr := batch.Finish()
				totalLen += int64(len(repr))
				b.StartTimer()

				if err := kvDB.WriteBatch(ctx, span.Key, span.EndKey, repr); err != nil {
					b.Fatalf("%+v", err)
				}
			}
			b.StopTimer()
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}
