// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"bytes"
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

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

func TestExportCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	exportAndSlurpOne := func(
		t *testing.T, start hlc.Timestamp, mvccFilter roachpb.MVCCFilter,
	) ([]string, []engine.MVCCKeyValue) {
		req := &roachpb.ExportRequest{
			Span:      roachpb.Span{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
			StartTime: start,
			Storage: roachpb.ExportStorage{
				Provider:  roachpb.ExportStorageProvider_LocalFile,
				LocalFile: roachpb.ExportStorage_LocalFilePath{Path: "/foo"},
			},
			MVCCFilter: mvccFilter,
			ReturnSST:  true,
		}
		res, pErr := client.SendWrapped(ctx, kvDB.GetSender(), req)
		if pErr != nil {
			t.Fatalf("%+v", pErr)
		}

		var paths []string
		var kvs []engine.MVCCKeyValue
		ingestFunc := func(kv engine.MVCCKeyValue) (bool, error) {
			kvs = append(kvs, kv)
			return false, nil
		}
		for _, file := range res.(*roachpb.ExportResponse).Files {
			paths = append(paths, file.Path)

			sst := engine.MakeRocksDBSstFileReader()
			defer sst.Close()

			fileContents, err := ioutil.ReadFile(filepath.Join(dir, "foo", file.Path))
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if !bytes.Equal(fileContents, file.SST) {
				t.Fatal("Returned SST and exported SST don't match!")
			}
			if err := sst.IngestExternalFile(file.SST); err != nil {
				t.Fatalf("%+v", err)
			}
			start, end := engine.MVCCKey{Key: keys.MinKey}, engine.MVCCKey{Key: keys.MaxKey}
			if err := sst.Iterate(start, end, ingestFunc); err != nil {
				t.Fatalf("%+v", err)
			}
		}

		return paths, kvs
	}
	type ExportAndSlurpResult struct {
		end             hlc.Timestamp
		mvccLatestFiles []string
		mvccLatestKVs   []engine.MVCCKeyValue
		mvccAllFiles    []string
		mvccAllKVs      []engine.MVCCKeyValue
	}
	exportAndSlurp := func(t *testing.T, start hlc.Timestamp) ExportAndSlurpResult {
		var ret ExportAndSlurpResult
		ret.end = hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()
		ret.mvccLatestFiles, ret.mvccLatestKVs = exportAndSlurpOne(t, start, roachpb.MVCCFilter_Latest)
		ret.mvccAllFiles, ret.mvccAllKVs = exportAndSlurpOne(t, start, roachpb.MVCCFilter_All)
		return ret
	}

	expect := func(
		t *testing.T, res ExportAndSlurpResult,
		mvccLatestFilesLen int, mvccLatestKVsLen int, mvccAllFilesLen int, mvccAllKVsLen int,
	) {
		if len(res.mvccLatestFiles) != mvccLatestFilesLen {
			t.Errorf("expected %d files in latest export got %d", mvccLatestFilesLen, len(res.mvccLatestFiles))
		}
		if len(res.mvccLatestKVs) != mvccLatestKVsLen {
			t.Errorf("expected %d kvs in latest export got %d", mvccLatestKVsLen, len(res.mvccLatestKVs))
		}
		if len(res.mvccAllFiles) != mvccAllFilesLen {
			t.Errorf("expected %d files in all export got %d", mvccAllFilesLen, len(res.mvccAllFiles))
		}
		if len(res.mvccAllKVs) != mvccAllKVsLen {
			t.Errorf("expected %d kvs in all export got %d", mvccAllKVsLen, len(res.mvccAllKVs))
		}
	}

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE mvcclatest`)
	sqlDB.Exec(t, `CREATE TABLE mvcclatest.export (id INT PRIMARY KEY, value INT)`)

	var res1 ExportAndSlurpResult
	t.Run("ts1", func(t *testing.T) {
		// When run with MVCCFilter_Latest and a startTime of 0 (full backup of
		// only the latest values), Export special cases and skips keys that are
		// deleted before the export timestamp.
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (1, 1), (3, 3), (4, 4)`)
		sqlDB.Exec(t, `DELETE from mvcclatest.export WHERE id = 4`)
		res1 = exportAndSlurp(t, hlc.Timestamp{})
		expect(t, res1, 1, 2, 1, 4)
	})

	var res2 ExportAndSlurpResult
	t.Run("ts2", func(t *testing.T) {
		// If nothing has changed, nothing should be exported.
		res2 = exportAndSlurp(t, res1.end)
		expect(t, res2, 0, 0, 0, 0)
	})

	var res3 ExportAndSlurpResult
	t.Run("ts3", func(t *testing.T) {
		// MVCCFilter_All saves all values.
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (2, 2)`)
		sqlDB.Exec(t, `UPSERT INTO mvcclatest.export VALUES (2, 8)`)
		res3 = exportAndSlurp(t, res2.end)
		expect(t, res3, 1, 1, 1, 2)
	})

	var res4 ExportAndSlurpResult
	t.Run("ts4", func(t *testing.T) {
		sqlDB.Exec(t, `DELETE FROM mvcclatest.export WHERE id = 3`)
		res4 = exportAndSlurp(t, res3.end)
		expect(t, res4, 1, 1, 1, 1)
		if len(res4.mvccLatestKVs[0].Value) != 0 {
			v := roachpb.Value{RawBytes: res4.mvccLatestKVs[0].Value}
			t.Errorf("expected a deletion tombstone got %s", v.PrettyPrint())
		}
		if len(res4.mvccAllKVs[0].Value) != 0 {
			v := roachpb.Value{RawBytes: res4.mvccAllKVs[0].Value}
			t.Errorf("expected a deletion tombstone got %s", v.PrettyPrint())
		}
	})

	var res5 ExportAndSlurpResult
	t.Run("ts5", func(t *testing.T) {
		sqlDB.Exec(t, `ALTER TABLE mvcclatest.export SPLIT AT VALUES (2)`)
		res5 = exportAndSlurp(t, hlc.Timestamp{})
		expect(t, res5, 2, 2, 2, 7)
	})
}

func TestExportGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	req := &roachpb.ExportRequest{
		Span:      roachpb.Span{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
		StartTime: hlc.Timestamp{WallTime: -1},
	}
	_, pErr := client.SendWrapped(ctx, kvDB.GetSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
}
