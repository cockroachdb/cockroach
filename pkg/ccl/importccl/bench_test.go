// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl_test

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/importccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/format"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/stretchr/testify/require"
)

type tableSSTable struct {
	meta    workload.Table
	span    roachpb.Span
	sstData []byte
}

func BenchmarkImportWorkload(b *testing.B) {
	b.Skip("#41932: broken due to adding keys out-of-order to an sstable")
	if testing.Short() {
		b.Skip("skipping long benchmark")
	}

	dir, cleanup := testutils.TempDir(b)
	defer cleanup()

	g := tpcc.FromWarehouses(1)

	ts := timeutil.Now()
	var tableSSTs []tableSSTable
	for i, table := range g.Tables() {
		tableID := sqlbase.ID(keys.MinUserDescID + 1 + i)
		sst, err := format.ToSSTable(table, tableID, ts)
		require.NoError(b, err)

		t := tableSSTable{
			meta:    table,
			span:    roachpb.Span{Key: keys.SystemSQLCodec.TablePrefix(uint32(tableID))},
			sstData: sst,
		}
		t.span.EndKey = t.span.Key.PrefixEnd()

		tableSSTs = append(tableSSTs, t)
	}

	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		b.Run(`WriteAndLink`, func(b *testing.B) {
			benchmarkWriteAndLink(b, dir, tableSSTs)
		})
		b.Run(`AddSStable`, func(b *testing.B) {
			benchmarkAddSSTable(b, dir, tableSSTs)
		})
	})
}

func benchmarkWriteAndLink(b *testing.B, dir string, tables []tableSSTable) {
	var bytes int64
	var paths []string
	for _, table := range tables {
		path := filepath.Join(dir, table.meta.Name+`.sst`)
		require.NoError(b, ioutil.WriteFile(path, table.sstData, 0644))
		bytes += int64(len(table.sstData))
		paths = append(paths, path)
	}
	b.SetBytes(bytes)

	ctx := context.Background()
	cache := storage.NewRocksDBCache(server.DefaultCacheSize)
	defer cache.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cfg := storage.RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Dir: filepath.Join(dir, `rocksdb`, timeutil.Now().String())}}
		db, err := storage.NewRocksDB(cfg, cache)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		for i, table := range tables {
			require.NoError(b, ioutil.WriteFile(paths[i], table.sstData, 0644))
		}
		require.NoError(b, db.IngestExternalFiles(ctx, paths))
		b.StopTimer()

		db.Close()
	}
	b.StopTimer()
}

func benchmarkAddSSTable(b *testing.B, dir string, tables []tableSSTable) {
	ctx := context.Background()

	var totalBytes int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		args := base.TestServerArgs{StoreSpecs: []base.StoreSpec{
			{InMemory: false, Path: filepath.Join(dir, "testserver", timeutil.Now().String())},
		}}
		s, _, kvDB := serverutils.StartServer(b, args)
		for _, t := range tables {
			if err := kvDB.AdminSplit(ctx, t.span.Key, hlc.Timestamp{} /* expirationTime */); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		for _, t := range tables {
			totalBytes += int64(len(t.sstData))
			require.NoError(b, kvDB.AddSSTable(
				ctx, t.span.Key, t.span.EndKey, t.sstData, true /* disallowShadowing */, nil /* stats */, false, /*ingestAsWrites */
			))
		}
		b.StopTimer()

		s.Stopper().Stop(ctx)
	}
	b.SetBytes(totalBytes / int64(b.N))
}

func BenchmarkConvertToKVs(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping long benchmark")
	}

	tpccGen := tpcc.FromWarehouses(1)
	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkConvertToKVs(b, tpccGen)
	})
}

func benchmarkConvertToKVs(b *testing.B, g workload.Generator) {
	ctx := context.Background()
	const tableID = sqlbase.ID(keys.MinUserDescID)
	ts := timeutil.Now()

	var bytes int64
	b.ResetTimer()
	for _, t := range g.Tables() {
		tableDesc, err := format.ToTableDescriptor(t, tableID, ts)
		if err != nil {
			b.Fatal(err)
		}

		kvCh := make(chan row.KVBatch)
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			defer close(kvCh)
			wc := importccl.NewWorkloadKVConverter(
				0, tableDesc, t.InitialRows, 0, t.InitialRows.NumBatches, kvCh)
			evalCtx := &tree.EvalContext{SessionData: &sessiondata.SessionData{}}
			return wc.Worker(ctx, evalCtx)
		})
		for kvBatch := range kvCh {
			for i := range kvBatch.KVs {
				kv := &kvBatch.KVs[i]
				bytes += int64(len(kv.Key) + len(kv.Value.RawBytes))
			}
		}
		if err := g.Wait(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.SetBytes(bytes)
}

func BenchmarkConvertToSSTable(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping long benchmark")
	}

	tpccGen := tpcc.FromWarehouses(1)
	b.Run(`tpcc/warehouses=1`, func(b *testing.B) {
		benchmarkConvertToSSTable(b, tpccGen)
	})
}

func benchmarkConvertToSSTable(b *testing.B, g workload.Generator) {
	const tableID = sqlbase.ID(keys.MinUserDescID)
	now := timeutil.Now()

	var totalBytes int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, table := range g.Tables() {
			sst, err := format.ToSSTable(table, tableID, now)
			if err != nil {
				b.Fatal(err)
			}
			totalBytes += int64(len(sst))
		}
	}
	b.StopTimer()
	b.SetBytes(totalBytes / int64(b.N))
}
