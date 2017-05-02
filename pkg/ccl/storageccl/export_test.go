// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package storageccl

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestExport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(t, tc.Conns[0])
	kvDB := tc.Server(0).KVClient().(*client.DB)

	exportAndSlurp := func(start hlc.Timestamp) (hlc.Timestamp, []string, []engine.MVCCKeyValue) {
		req := &roachpb.ExportRequest{
			Span:      roachpb.Span{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
			StartTime: start,
			Storage: roachpb.ExportStorage{
				Provider:  roachpb.ExportStorageProvider_LocalFile,
				LocalFile: roachpb.ExportStorage_LocalFilePath{Path: dir},
			},
		}
		res, pErr := client.SendWrapped(ctx, kvDB.GetSender(), req)
		if pErr != nil {
			t.Fatalf("%+v", pErr)
		}
		ts := hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()

		var paths []string
		var kvs []engine.MVCCKeyValue
		ingestFunc := func(kv engine.MVCCKeyValue) (bool, error) {
			kvs = append(kvs, kv)
			return false, nil
		}
		for _, file := range res.(*roachpb.ExportResponse).Files {
			paths = append(paths, file.Path)

			readerTempDir, err := ioutil.TempDir(dir, "RocksDBSstFileReader")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.RemoveAll(readerTempDir); err != nil {
					t.Fatal(err)
				}
			}()

			sst, err := engine.MakeRocksDBSstFileReader(readerTempDir)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			defer sst.Close()
			if err := sst.AddFile(filepath.Join(dir, file.Path)); err != nil {
				t.Fatalf("%+v", err)
			}
			start, end := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}
			if err := sst.Iterate(start, end, ingestFunc); err != nil {
				t.Fatalf("%+v", err)
			}
		}

		return ts, paths, kvs
	}

	sqlDB.Exec(`CREATE DATABASE export`)
	sqlDB.Exec(`CREATE TABLE export.export (id INT PRIMARY KEY)`)
	sqlDB.Exec(`INSERT INTO export.export VALUES (1), (3)`)
	ts1, paths1, kvs1 := exportAndSlurp(hlc.Timestamp{})
	if expected := 1; len(paths1) != expected {
		t.Fatalf("expected %d files in export got %d", expected, len(paths1))
	}
	if expected := 2; len(kvs1) != expected {
		t.Fatalf("expected %d kvs in export got %d", expected, len(kvs1))
	}

	// If nothing has changed, nothing should be exported.
	ts2, paths2, _ := exportAndSlurp(ts1)
	if expected := 0; len(paths2) != expected {
		t.Fatalf("expected %d files in export got %d", expected, len(paths2))
	}

	sqlDB.Exec(`INSERT INTO export.export VALUES (2)`)
	ts3, _, kvs3 := exportAndSlurp(ts2)
	if expected := 1; len(kvs3) != expected {
		t.Fatalf("expected %d kvs in export got %d", expected, len(kvs3))
	}

	sqlDB.Exec(`DELETE FROM export.export WHERE id = 3`)
	_, _, kvs4 := exportAndSlurp(ts3)
	if expected := 1; len(kvs4) != expected {
		t.Fatalf("expected %d kvs in export got %d", expected, len(kvs4))
	}
	if len(kvs4[0].Value) != 0 {
		v := roachpb.Value{RawBytes: kvs4[0].Value}
		t.Fatalf("expected a deletion tombstone got %s", v.PrettyPrint())
	}

	sqlDB.Exec(`ALTER TABLE export.export SPLIT AT VALUES (2)`)
	_, paths5, kvs5 := exportAndSlurp(hlc.Timestamp{})
	if expected := 2; len(paths5) != expected {
		t.Fatalf("expected %d files in export got %d", expected, len(paths5))
	}
	if expected := 3; len(kvs5) != expected {
		t.Fatalf("expected %d kvs in export got %d", expected, len(kvs5))
	}
}

func TestExportGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).KVClient().(*client.DB)

	req := &roachpb.ExportRequest{
		Span:      roachpb.Span{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
		StartTime: hlc.Timestamp{WallTime: -1},
	}
	_, pErr := client.SendWrapped(ctx, kvDB.GetSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
}
