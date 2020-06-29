// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
)

func BenchmarkAddSSTable(b *testing.B) {
	tempDir, dirCleanupFn := testutils.TempDir(b)
	defer dirCleanupFn()

	for _, numEntries := range []int{100, 1000, 10000, 300000} {
		b.Run(fmt.Sprintf("numEntries=%d", numEntries), func(b *testing.B) {
			bankData := bank.FromRows(numEntries).Tables()[0]
			backupDir := filepath.Join(tempDir, strconv.Itoa(numEntries))
			backup, err := sampledataccl.ToBackup(b, bankData, backupDir)
			if err != nil {
				b.Fatalf("%+v", err)
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)
			kvDB := tc.Server(0).DB()

			id := sqlbase.ID(keys.MinUserDescID)

			var totalLen int64
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sstFile := &storage.MemFile{}
				sst := storage.MakeBackupSSTWriter(sstFile)

				id++
				backup.ResetKeyValueIteration()
				kvs, span, err := backup.NextKeyValues(numEntries, id)
				if err != nil {
					b.Fatalf("%+v", err)
				}
				for _, kv := range kvs {
					if err := sst.Put(kv.Key, kv.Value); err != nil {
						b.Fatalf("%+v", err)
					}
				}
				if err := sst.Finish(); err != nil {
					b.Fatalf("%+v", err)
				}
				sst.Close()
				data := sstFile.Data()
				totalLen += int64(len(data))

				b.StartTimer()
				if err := kvDB.AddSSTable(
					ctx, span.Key, span.EndKey, data, false /* disallowShadowing */, nil /* stats */, false, /* ingestAsWrites */
				); err != nil {
					b.Fatalf("%+v", err)
				}
				b.StopTimer()
			}
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}

func BenchmarkWriteBatch(b *testing.B) {
	tempDir, dirCleanupFn := testutils.TempDir(b)
	defer dirCleanupFn()

	for _, numEntries := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("numEntries=%d", numEntries), func(b *testing.B) {
			bankData := bank.FromRows(numEntries).Tables()[0]
			backupDir := filepath.Join(tempDir, strconv.Itoa(numEntries))
			backup, err := sampledataccl.ToBackup(b, bankData, backupDir)
			if err != nil {
				b.Fatalf("%+v", err)
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, base.TestClusterArgs{})
			defer tc.Stopper().Stop(ctx)
			kvDB := tc.Server(0).DB()

			id := sqlbase.ID(keys.MinUserDescID)
			var batch storage.RocksDBBatchBuilder

			var totalLen int64
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
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
				b.StopTimer()
			}
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}

func BenchmarkImport(b *testing.B) {
	tempDir, dirCleanupFn := testutils.TempDir(b)
	defer dirCleanupFn()

	args := base.TestClusterArgs{}
	args.ServerArgs.ExternalIODir = tempDir

	for _, numEntries := range []int{1, 100, 10000, 300000} {
		b.Run(fmt.Sprintf("numEntries=%d", numEntries), func(b *testing.B) {
			bankData := bank.FromRows(numEntries).Tables()[0]
			subdir := strconv.Itoa(numEntries)
			backupDir := filepath.Join(tempDir, subdir)
			backup, err := sampledataccl.ToBackup(b, bankData, backupDir)
			if err != nil {
				b.Fatalf("%+v", err)
			}
			storage, err := cloudimpl.ExternalStorageConfFromURI(`nodelocal://0/`+subdir,
				security.RootUser)
			if err != nil {
				b.Fatalf("%+v", err)
			}

			ctx := context.Background()
			tc := testcluster.StartTestCluster(b, 3, args)
			defer tc.Stopper().Stop(ctx)
			kvDB := tc.Server(0).DB()

			id := sqlbase.ID(keys.MinUserDescID)

			var totalLen int64
			b.StopTimer()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				id++
				var rekeys []roachpb.ImportRequest_TableRekey
				var oldStartKey roachpb.Key
				{
					// TODO(dan): The following should probably make it into
					// dataccl.Backup somehow.
					tableDesc := backup.Desc.Descriptors[len(backup.Desc.Descriptors)-1].Table(hlc.Timestamp{})
					if tableDesc == nil || tableDesc.ParentID == keys.SystemDatabaseID {
						b.Fatalf("bad table descriptor: %+v", tableDesc)
					}
					oldStartKey = sqlbase.MakeIndexKeyPrefix(keys.SystemSQLCodec, tableDesc, tableDesc.PrimaryIndex.ID)
					newDesc := sqlbase.NewMutableCreatedTableDescriptor(*tableDesc)
					newDesc.ID = id
					newDescBytes, err := protoutil.Marshal(newDesc.DescriptorProto())
					if err != nil {
						panic(err)
					}
					rekeys = append(rekeys, roachpb.ImportRequest_TableRekey{
						OldID: uint32(tableDesc.ID), NewDesc: newDescBytes,
					})
				}
				newStartKey := keys.SystemSQLCodec.TablePrefix(uint32(id))

				b.StartTimer()
				var files []roachpb.ImportRequest_File
				for _, file := range backup.Desc.Files {
					files = append(files, roachpb.ImportRequest_File{Dir: storage, Path: file.Path})
				}
				req := &roachpb.ImportRequest{
					// Import is a point request because we don't want DistSender to split
					// it. Assume (but don't require) the entire post-rewrite span is on the
					// same range.
					RequestHeader: roachpb.RequestHeader{Key: newStartKey},
					DataSpan:      roachpb.Span{Key: oldStartKey, EndKey: oldStartKey.PrefixEnd()},
					Files:         files,
					Rekeys:        rekeys,
				}
				res, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
				if pErr != nil {
					b.Fatalf("%+v", pErr.GoError())
				}
				totalLen += res.(*roachpb.ImportResponse).Imported.DataSize
				b.StopTimer()
			}
			b.SetBytes(totalLen / int64(b.N))
		})
	}
}
