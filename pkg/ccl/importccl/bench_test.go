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
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/format"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
			span:    roachpb.Span{Key: keys.MakeTablePrefix(uint32(tableID))},
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
	cache := engine.NewRocksDBCache(server.DefaultCacheSize)
	defer cache.Release()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cfg := engine.RocksDBConfig{Dir: filepath.Join(dir, `rocksdb`, timeutil.Now().String())}
		db, err := engine.NewRocksDB(cfg, cache)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		for i, table := range tables {
			require.NoError(b, ioutil.WriteFile(paths[i], table.sstData, 0644))
		}
		const skipSeqNo, canModify = true, true
		require.NoError(b, db.IngestExternalFiles(ctx, paths, skipSeqNo, canModify))
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
			if err := kvDB.AdminSplit(ctx, t.span.Key, t.span.Key); err != nil {
				b.Fatal(err)
			}
		}

		b.StartTimer()
		for _, t := range tables {
			totalBytes += int64(len(t.sstData))
			require.NoError(b, kvDB.AddSSTable(ctx, t.span.Key, t.span.EndKey, t.sstData))
		}
		b.StopTimer()

		s.Stopper().Stop(ctx)
	}
	b.SetBytes(totalBytes / int64(b.N))
}
