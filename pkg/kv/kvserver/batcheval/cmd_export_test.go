// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestExportCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	export := func(
		t *testing.T, start hlc.Timestamp, mvccFilter roachpb.MVCCFilter, maxResponseSSTBytes int64,
	) (roachpb.Response, *roachpb.Error) {
		req := &roachpb.ExportRequest{
			RequestHeader: roachpb.RequestHeader{Key: bootstrap.TestingUserTableDataMin(), EndKey: keys.MaxKey},
			StartTime:     start,
			Storage: roachpb.ExternalStorage{
				Provider:  roachpb.ExternalStorageProvider_nodelocal,
				LocalFile: roachpb.ExternalStorage_LocalFilePath{Path: "/foo"},
			},
			MVCCFilter:     mvccFilter,
			ReturnSST:      true,
			TargetFileSize: batcheval.ExportRequestTargetFileSize.Get(&tc.Server(0).ClusterSettings().SV),
		}
		var h roachpb.Header
		h.TargetBytes = maxResponseSSTBytes
		return kv.SendWrappedWith(ctx, kvDB.NonTransactionalSender(), h, req)
	}

	exportAndSlurpOne := func(
		t *testing.T, start hlc.Timestamp, mvccFilter roachpb.MVCCFilter, maxResponseSSTBytes int64,
	) ([]string, []storage.MVCCKeyValue, roachpb.ResponseHeader) {
		res, pErr := export(t, start, mvccFilter, maxResponseSSTBytes)
		if pErr != nil {
			t.Fatalf("%+v", pErr)
		}

		var paths []string
		var kvs []storage.MVCCKeyValue
		for _, file := range res.(*roachpb.ExportResponse).Files {
			paths = append(paths, file.Path)

			sst, err := storage.NewMemSSTIterator(file.SST, false)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			defer sst.Close()

			sst.SeekGE(storage.MVCCKey{Key: keys.MinKey})
			for {
				if valid, err := sst.Valid(); !valid || err != nil {
					if err != nil {
						t.Fatalf("%+v", err)
					}
					break
				}
				newKv := storage.MVCCKeyValue{}
				newKv.Key.Key = append(newKv.Key.Key, sst.UnsafeKey().Key...)
				newKv.Key.Timestamp = sst.UnsafeKey().Timestamp
				newKv.Value = append(newKv.Value, sst.UnsafeValue()...)
				kvs = append(kvs, newKv)
				sst.Next()
			}
		}

		return paths, kvs, res.(*roachpb.ExportResponse).Header()
	}
	type ExportAndSlurpResult struct {
		end                      hlc.Timestamp
		mvccLatestFiles          []string
		mvccLatestKVs            []storage.MVCCKeyValue
		mvccAllFiles             []string
		mvccAllKVs               []storage.MVCCKeyValue
		mvccLatestResponseHeader roachpb.ResponseHeader
		mvccAllResponseHeader    roachpb.ResponseHeader
	}
	exportAndSlurp := func(t *testing.T, start hlc.Timestamp,
		maxResponseSSTBytes int64) ExportAndSlurpResult {
		var ret ExportAndSlurpResult
		ret.end = hlc.NewClock(hlc.UnixNano, time.Nanosecond).Now()
		ret.mvccLatestFiles, ret.mvccLatestKVs, ret.mvccLatestResponseHeader = exportAndSlurpOne(t,
			start, roachpb.MVCCFilter_Latest, maxResponseSSTBytes)
		ret.mvccAllFiles, ret.mvccAllKVs, ret.mvccAllResponseHeader = exportAndSlurpOne(t, start,
			roachpb.MVCCFilter_All, maxResponseSSTBytes)
		return ret
	}

	expect := func(
		t *testing.T, res ExportAndSlurpResult,
		mvccLatestFilesLen int, mvccLatestKVsLen int, mvccAllFilesLen int, mvccAllKVsLen int,
	) {
		t.Helper()
		require.Len(t, res.mvccLatestFiles, mvccLatestFilesLen, "unexpected files in latest export")
		require.Len(t, res.mvccLatestKVs, mvccLatestKVsLen, "unexpected kvs in latest export")
		require.Len(t, res.mvccAllFiles, mvccAllFilesLen, "unexpected files in all export")
		require.Len(t, res.mvccAllKVs, mvccAllKVsLen, "unexpected kvs in all export")
	}

	expectResponseHeader := func(
		t *testing.T, res ExportAndSlurpResult, mvccLatestResponseHeader roachpb.ResponseHeader,
		mvccAllResponseHeader roachpb.ResponseHeader) {
		t.Helper()
		requireResumeSpan := func(expect, actual *roachpb.Span, msgAndArgs ...interface{}) {
			t.Helper()
			if expect == nil {
				require.Nil(t, actual, msgAndArgs...)
			} else {
				require.NotNil(t, actual, msgAndArgs...)
				require.Equal(t, expect.String(), actual.String(), msgAndArgs...)
			}
		}
		require.Equal(t, mvccLatestResponseHeader.NumBytes, res.mvccLatestResponseHeader.NumBytes,
			"unexpected NumBytes in latest export")
		requireResumeSpan(mvccLatestResponseHeader.ResumeSpan, res.mvccLatestResponseHeader.ResumeSpan,
			"unexpected ResumeSpan in latest export")
		require.Equal(t, mvccLatestResponseHeader.ResumeReason, res.mvccLatestResponseHeader.ResumeReason,
			"unexpected ResumeReason in latest export")
		require.Equal(t, mvccAllResponseHeader.NumBytes, res.mvccAllResponseHeader.NumBytes,
			"unexpected NumBytes in all export")
		requireResumeSpan(mvccAllResponseHeader.ResumeSpan, res.mvccAllResponseHeader.ResumeSpan,
			"unexpected ResumeSpan in all export")
		require.Equal(t, mvccAllResponseHeader.ResumeReason, res.mvccAllResponseHeader.ResumeReason,
			"unexpected ResumeReason in latest export")
	}

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE mvcclatest`)
	sqlDB.Exec(t, `CREATE TABLE mvcclatest.export (id INT PRIMARY KEY, value INT)`)
	tableID := descpb.ID(sqlutils.QueryTableID(
		t, sqlDB.DB, "mvcclatest", "public", "export",
	))

	const (
		targetSizeSetting = "kv.bulk_sst.target_size"
		maxOverageSetting = "kv.bulk_sst.max_allowed_overage"
	)
	var (
		setSetting = func(t *testing.T, variable, val string) {
			sqlDB.Exec(t, "SET CLUSTER SETTING "+variable+" = "+val)
		}
		resetSetting = func(t *testing.T, variable string) {
			setSetting(t, variable, "DEFAULT")
		}
		setExportTargetSize = func(t *testing.T, val string) {
			setSetting(t, targetSizeSetting, val)
		}
		resetExportTargetSize = func(t *testing.T) {
			resetSetting(t, targetSizeSetting)
		}
		setMaxOverage = func(t *testing.T, val string) {
			setSetting(t, maxOverageSetting, val)
		}
		resetMaxOverage = func(t *testing.T) {
			resetSetting(t, maxOverageSetting)
		}
	)

	var res1 ExportAndSlurpResult
	var noTargetBytes int64
	t.Run("ts1", func(t *testing.T) {
		// When run with MVCCFilter_Latest and a startTime of 0 (full backup of
		// only the latest values), Export special cases and skips keys that are
		// deleted before the export timestamp.
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (1, 1), (3, 3), (4, 4)`)
		sqlDB.Exec(t, `DELETE from mvcclatest.export WHERE id = 4`)
		res1 = exportAndSlurp(t, hlc.Timestamp{}, noTargetBytes)
		expect(t, res1, 1, 2, 1, 4)
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		res1 = exportAndSlurp(t, hlc.Timestamp{}, noTargetBytes)
		expect(t, res1, 2, 2, 3, 4)
	})

	var res2 ExportAndSlurpResult
	t.Run("ts2", func(t *testing.T) {
		// If nothing has changed, nothing should be exported.
		res2 = exportAndSlurp(t, res1.end, noTargetBytes)
		expect(t, res2, 0, 0, 0, 0)
	})

	var res3 ExportAndSlurpResult
	t.Run("ts3", func(t *testing.T) {
		// MVCCFilter_All saves all values.
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (2, 2)`)
		sqlDB.Exec(t, `UPSERT INTO mvcclatest.export VALUES (2, 8)`)
		res3 = exportAndSlurp(t, res2.end, noTargetBytes)
		expect(t, res3, 1, 1, 1, 2)
	})

	var res4 ExportAndSlurpResult
	t.Run("ts4", func(t *testing.T) {
		sqlDB.Exec(t, `DELETE FROM mvcclatest.export WHERE id = 3`)
		res4 = exportAndSlurp(t, res3.end, noTargetBytes)
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
		res5 = exportAndSlurp(t, hlc.Timestamp{}, noTargetBytes)
		expect(t, res5, 2, 2, 2, 7)

		// Re-run the test with a 1b target size which will lead to more files.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		res5 = exportAndSlurp(t, hlc.Timestamp{}, noTargetBytes)
		expect(t, res5, 2, 2, 4, 7)
	})

	var res6 ExportAndSlurpResult
	t.Run("ts6", func(t *testing.T) {
		// Add 100 rows to the table.
		sqlDB.Exec(t, `WITH RECURSIVE
    t (id, value)
        AS (VALUES (1, 1) UNION ALL SELECT id + 1, value FROM t WHERE id < 100)
UPSERT
INTO
    mvcclatest.export
(SELECT id, value FROM t);`)

		// Run the test with the default target size which will lead to 2 files due
		// to the above split.
		res6 = exportAndSlurp(t, res5.end, noTargetBytes)
		expect(t, res6, 2, 100, 2, 100)

		// Re-run the test with a 1b target size which will lead to 100 files.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		res6 = exportAndSlurp(t, res5.end, noTargetBytes)
		expect(t, res6, 100, 100, 100, 100)

		// Set the MaxOverage to 1b and ensure that we get errors due to
		// the max overage being exceeded.
		defer resetMaxOverage(t)
		setMaxOverage(t, "'1b'")
		const expectedError = `export size \(11 bytes\) exceeds max size \(2 bytes\)`
		_, pErr := export(t, res5.end, roachpb.MVCCFilter_Latest, noTargetBytes)
		require.Regexp(t, expectedError, pErr)
		hints := errors.GetAllHints(pErr.GoError())
		require.Equal(t, 1, len(hints))
		const expectedHint = `consider increasing cluster setting "kv.bulk_sst.max_allowed_overage"`
		require.Regexp(t, expectedHint, hints[0])
		_, pErr = export(t, res5.end, roachpb.MVCCFilter_All, noTargetBytes)
		require.Regexp(t, expectedError, pErr)

		// Disable the TargetSize and ensure that we don't get any errors
		// to the max overage being exceeded.
		setExportTargetSize(t, "'0b'")
		res6 = exportAndSlurp(t, res5.end, noTargetBytes)
		expect(t, res6, 2, 100, 2, 100)
	})

	var res7 ExportAndSlurpResult
	t.Run("ts7", func(t *testing.T) {
		var maxResponseSSTBytes int64
		kvByteSize := int64(11)
		// Because of the above split, there are going to be two ExportRequests by
		// the DistSender. One for the first KV and the next one for the remaining
		// KVs.
		// This allows us to test both the TargetBytes limit within a single export
		// request and across subsequent requests.

		// No TargetSize and TargetBytes is greater than the size of a single KV.
		// The first ExportRequest should reduce the byte limit for the next
		// ExportRequest but since there is no TargetSize we should see all KVs
		// exported.
		maxResponseSSTBytes = kvByteSize + 1
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 2, 100, 2, 100)
		latestRespHeader := roachpb.ResponseHeader{
			NumBytes: maxResponseSSTBytes,
		}
		allRespHeader := roachpb.ResponseHeader{
			NumBytes: maxResponseSSTBytes,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)

		// No TargetSize and TargetBytes is equal to the size of a single KV. The
		// first ExportRequest will reduce the limit for the second request to zero
		// and so we should only see a single ExportRequest and an accurate
		// ResumeSpan.
		maxResponseSSTBytes = kvByteSize
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 1, 1, 1, 1)
		latestRespHeader = roachpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/2", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		allRespHeader = roachpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/2", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)

		// TargetSize to one KV and TargetBytes to two KVs. We should see one KV in
		// each ExportRequest SST.
		setExportTargetSize(t, "'11b'")
		maxResponseSSTBytes = 2 * kvByteSize
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 2, 2, 2, 2)
		latestRespHeader = roachpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/3/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		allRespHeader = roachpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/3/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)

		// TargetSize to one KV and TargetBytes to one less than the total KVs.
		setExportTargetSize(t, "'11b'")
		maxResponseSSTBytes = 99 * kvByteSize
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 99, 99, 99, 99)
		latestRespHeader = roachpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/100/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		allRespHeader = roachpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/100/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)

		// Target Size to one KV and TargetBytes to greater than all KVs. Checks if
		// final NumBytes is accurate.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'11b'")
		maxResponseSSTBytes = 101 * kvByteSize
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 100, 100, 100, 100)
		latestRespHeader = roachpb.ResponseHeader{
			NumBytes: 100 * kvByteSize,
		}
		allRespHeader = roachpb.ResponseHeader{
			NumBytes: 100 * kvByteSize,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)
	})
}

func TestExportGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeader{Key: bootstrap.TestingUserTableDataMin(), EndKey: keys.MaxKey},
		StartTime:     hlc.Timestamp{WallTime: -1},
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
}

// exportUsingGoIterator uses the legacy implementation of export, and is used
// as an oracle to check the correctness of pebbleExportToSst.
func exportUsingGoIterator(
	ctx context.Context,
	filter roachpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	startKey, endKey roachpb.Key,
	enableTimeBoundIteratorOptimization timeBoundOptimisation,
	reader storage.Reader,
) ([]byte, error) {
	memFile := &storage.MemFile{}
	sst := storage.MakeIngestionSSTWriter(
		ctx, cluster.MakeTestingClusterSettings(), memFile,
	)
	defer sst.Close()

	var skipTombstones bool
	var iterFn func(*storage.MVCCIncrementalIterator)
	switch filter {
	case roachpb.MVCCFilter_Latest:
		skipTombstones = true
		iterFn = (*storage.MVCCIncrementalIterator).NextKey
	case roachpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*storage.MVCCIncrementalIterator).Next
	default:
		return nil, nil
	}

	iter := storage.NewMVCCIncrementalIterator(reader, storage.MVCCIncrementalIterOptions{
		EndKey:                              endKey,
		EnableTimeBoundIteratorOptimization: bool(enableTimeBoundIteratorOptimization),
		StartTime:                           startTime,
		EndTime:                             endTime,
	})
	defer iter.Close()
	for iter.SeekGE(storage.MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a WriteIntentError. In which case, returning it will
			// cause this command to be retried.
			return nil, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}

		// Skip tombstone (len=0) records when startTime is zero
		// (non-incremental) and we're not exporting all versions.
		if skipTombstones && startTime.IsEmpty() && len(iter.UnsafeValue()) == 0 {
			continue
		}

		if err := sst.Put(iter.UnsafeKey(), iter.UnsafeValue()); err != nil {
			return nil, err
		}
	}
	if err := sst.Finish(); err != nil {
		return nil, err
	}

	if len(memFile.Data()) == 0 {
		// Let the defer Close the sstable.
		return nil, nil
	}

	return memFile.Data(), nil
}

func loadSST(t *testing.T, data []byte, start, end roachpb.Key) []storage.MVCCKeyValue {
	t.Helper()
	if len(data) == 0 {
		return nil
	}

	sst, err := storage.NewMemSSTIterator(data, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	var kvs []storage.MVCCKeyValue
	sst.SeekGE(storage.MVCCKey{Key: start})
	for {
		if valid, err := sst.Valid(); !valid || err != nil {
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if !sst.UnsafeKey().Less(storage.MVCCKey{Key: end}) {
			break
		}
		newKv := storage.MVCCKeyValue{}
		newKv.Key.Key = append(newKv.Key.Key, sst.UnsafeKey().Key...)
		newKv.Key.Timestamp = sst.UnsafeKey().Timestamp
		newKv.Value = append(newKv.Value, sst.UnsafeValue()...)
		kvs = append(kvs, newKv)
		sst.Next()
	}

	return kvs
}

type exportRevisions bool
type batchBoundaries bool
type timeBoundOptimisation bool

const (
	exportAll    exportRevisions = true
	exportLatest exportRevisions = false

	stopAtTimestamps batchBoundaries = true
	stopAtKeys       batchBoundaries = false

	optimizeTimeBounds     timeBoundOptimisation = true
	dontOptimizeTimeBounds timeBoundOptimisation = false
)

func assertEqualKVs(
	ctx context.Context,
	e storage.Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	exportAllRevisions exportRevisions,
	stopMidKey batchBoundaries,
	enableTimeBoundIteratorOptimization timeBoundOptimisation,
	targetSize uint64,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		var filter roachpb.MVCCFilter
		if exportAllRevisions {
			filter = roachpb.MVCCFilter_All
		} else {
			filter = roachpb.MVCCFilter_Latest
		}

		// Run the oracle which is a legacy implementation of pebbleExportToSst
		// backed by an MVCCIncrementalIterator.
		expected, err := exportUsingGoIterator(ctx, filter, startTime, endTime,
			startKey, endKey, enableTimeBoundIteratorOptimization, e)
		if err != nil {
			t.Fatalf("Oracle failed to export provided key range.")
		}

		// Run the actual code path used when exporting MVCCs to SSTs.
		var kvs []storage.MVCCKeyValue
		var resumeTs hlc.Timestamp
		for start := startKey; start != nil; {
			var sst []byte
			var summary roachpb.BulkOpSummary
			maxSize := uint64(0)
			prevStart := start
			prevTs := resumeTs
			sstFile := &storage.MemFile{}
			summary, start, resumeTs, err = e.ExportMVCCToSst(ctx, storage.ExportOptions{
				StartKey:           storage.MVCCKey{Key: start, Timestamp: resumeTs},
				EndKey:             endKey,
				StartTS:            startTime,
				EndTS:              endTime,
				ExportAllRevisions: bool(exportAllRevisions),
				TargetSize:         targetSize,
				MaxSize:            maxSize,
				StopMidKey:         bool(stopMidKey),
				UseTBI:             bool(enableTimeBoundIteratorOptimization),
			}, sstFile)
			require.NoError(t, err)
			sst = sstFile.Data()
			loaded := loadSST(t, sst, startKey, endKey)
			// Ensure that the pagination worked properly.
			if start != nil {
				dataSize := uint64(summary.DataSize)
				require.Truef(t, targetSize <= dataSize, "%d > %d",
					targetSize, summary.DataSize)
				// Now we want to ensure that if we remove the bytes due to the last
				// key that we are below the target size.
				firstKVofLastKey := sort.Search(len(loaded), func(i int) bool {
					return loaded[i].Key.Key.Equal(loaded[len(loaded)-1].Key.Key)
				})
				dataSizeWithoutLastKey := dataSize
				for _, kv := range loaded[firstKVofLastKey:] {
					dataSizeWithoutLastKey -= uint64(len(kv.Key.Key) + len(kv.Value))
				}
				require.Truef(t, targetSize > dataSizeWithoutLastKey, "%d <= %d", targetSize, dataSizeWithoutLastKey)
				// Ensure that maxSize leads to an error if exceeded.
				// Note that this uses a relatively non-sensical value of maxSize which
				// is equal to the targetSize.
				maxSize = targetSize
				dataSizeWhenExceeded := dataSize
				for i := len(loaded) - 1; i >= 0; i-- {
					kv := loaded[i]
					lessThisKey := dataSizeWhenExceeded - uint64(len(kv.Key.Key)+len(kv.Value))
					if lessThisKey >= maxSize {
						dataSizeWhenExceeded = lessThisKey
					} else {
						break
					}
				}
				// It might be the case that this key would lead to an SST of exactly
				// max size, in this case we overwrite max size to be less so that
				// we still generate an error.
				if dataSizeWhenExceeded == maxSize {
					maxSize--
				}
				_, _, _, err = e.ExportMVCCToSst(ctx, storage.ExportOptions{
					StartKey:           storage.MVCCKey{Key: prevStart, Timestamp: prevTs},
					EndKey:             endKey,
					StartTS:            startTime,
					EndTS:              endTime,
					ExportAllRevisions: bool(exportAllRevisions),
					TargetSize:         targetSize,
					MaxSize:            maxSize,
					StopMidKey:         false,
					UseTBI:             bool(enableTimeBoundIteratorOptimization),
				}, &storage.MemFile{})
				require.Regexp(t, fmt.Sprintf("export size \\(%d bytes\\) exceeds max size \\(%d bytes\\)",
					dataSizeWhenExceeded, maxSize), err)
			}
			kvs = append(kvs, loaded...)
		}

		// Compare the output of the current export MVCC to SST logic against the
		// legacy oracle output.
		expectedKVS := loadSST(t, expected, startKey, endKey)
		if len(kvs) != len(expectedKVS) {
			t.Fatalf("got %d kvs but expected %d:\n%v\n%v", len(kvs), len(expectedKVS), kvs, expectedKVS)
		}

		for i := range kvs {
			if !kvs[i].Key.Equal(expectedKVS[i].Key) {
				t.Fatalf("%d key: got %v but expected %v", i, kvs[i].Key, expectedKVS[i].Key)
			}
			if !bytes.Equal(kvs[i].Value, expectedKVS[i].Value) {
				t.Fatalf("%d value: got %x but expected %x", i, kvs[i].Value, expectedKVS[i].Value)
			}
		}
	}
}

func TestRandomKeyAndTimestampExport(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	mkEngine := func(t *testing.T) (e storage.Engine, cleanup func()) {
		dir, cleanupDir := testutils.TempDir(t)
		e, err := storage.Open(ctx,
			storage.Filesystem(dir),
			storage.CacheSize(0),
			storage.Settings(cluster.MakeTestingClusterSettings()))
		if err != nil {
			t.Fatal(err)
		}
		return e, func() {
			e.Close()
			cleanupDir()
		}
	}
	const keySize = 100
	const bytesPerValue = 300
	getNumKeys := func(t *testing.T, rnd *rand.Rand, targetSize uint64) (numKeys int) {
		const (
			targetPages = 10
			minNumKeys  = 2 // need > 1 keys for random key test
			maxNumKeys  = 5000
		)
		numKeys = maxNumKeys
		if targetSize > 0 {
			numKeys = rnd.Intn(int(targetSize)*targetPages*2) / bytesPerValue
		}
		if numKeys > maxNumKeys {
			numKeys = maxNumKeys
		} else if numKeys < minNumKeys {
			numKeys = minNumKeys
		}
		return numKeys
	}
	mkData := func(
		t *testing.T, e storage.Engine, rnd *rand.Rand, numKeys int,
	) ([]roachpb.Key, []hlc.Timestamp) {
		// Store generated keys and timestamps.
		var keys []roachpb.Key
		var timestamps []hlc.Timestamp

		var curWallTime = 0
		var curLogical = 0

		batch := e.NewBatch()
		for i := 0; i < numKeys; i++ {
			// Ensure walltime and logical are monotonically increasing.
			curWallTime = randutil.RandIntInRange(rnd, 0, math.MaxInt64-1)
			curLogical = randutil.RandIntInRange(rnd, 0, math.MaxInt32-1)
			ts := hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
			timestamps = append(timestamps, ts)

			// Make keys unique and ensure they are monotonically increasing.
			key := roachpb.Key(randutil.RandBytes(rnd, keySize))
			key = append([]byte(fmt.Sprintf("#%d", i)), key...)
			keys = append(keys, key)

			averageValueSize := bytesPerValue - keySize
			valueSize := randutil.RandIntInRange(rnd, averageValueSize-100, averageValueSize+100)
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, valueSize))
			value.InitChecksum(key)
			if err := storage.MVCCPut(ctx, batch, nil, key, ts, value, nil); err != nil {
				t.Fatal(err)
			}

			// Randomly decide whether to add a newer version of the same key to test
			// MVCC_Filter_All.
			if randutil.RandIntInRange(rnd, 0, math.MaxInt64)%2 == 0 {
				curWallTime++
				ts = hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
				value = roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, 200))
				value.InitChecksum(key)
				if err := storage.MVCCPut(ctx, batch, nil, key, ts, value, nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := batch.Commit(true); err != nil {
			t.Fatal(err)
		}
		batch.Close()

		sort.Slice(timestamps, func(i, j int) bool { return timestamps[i].Less(timestamps[j]) })
		return keys, timestamps
	}

	localMax := keys.LocalMax
	testWithTargetSize := func(t *testing.T, targetSize uint64) {
		e, cleanup := mkEngine(t)
		defer cleanup()
		rnd, _ := randutil.NewTestRand()
		numKeys := getNumKeys(t, rnd, targetSize)
		keys, timestamps := mkData(t, e, rnd, numKeys)
		var (
			keyMin = localMax
			keyMax = roachpb.KeyMax

			tsMin = hlc.Timestamp{WallTime: 0, Logical: 0}
			tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
		)

		keyUpperBound := randutil.RandIntInRange(rnd, 1, numKeys)
		keyLowerBound := rnd.Intn(keyUpperBound)
		tsUpperBound := randutil.RandIntInRange(rnd, 1, numKeys)
		tsLowerBound := rnd.Intn(tsUpperBound)

		for _, s := range []struct {
			name   string
			keyMin roachpb.Key
			keyMax roachpb.Key
			tsMin  hlc.Timestamp
			tsMax  hlc.Timestamp
		}{
			{"ts (0-∞]", keyMin, keyMax, tsMin, tsMax},
			{"kv [randLower, randUpper)", keys[keyLowerBound], keys[keyUpperBound], tsMin, tsMax},
			{"kv (randLowerTime, randUpperTime]", keyMin, keyMax, timestamps[tsLowerBound], timestamps[tsUpperBound]},
		} {
			t.Run(fmt.Sprintf("%s, latest, nontimebound", s.name),
				assertEqualKVs(ctx, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportLatest, stopAtKeys, dontOptimizeTimeBounds, targetSize))
			t.Run(fmt.Sprintf("%s, all, nontimebound", s.name),
				assertEqualKVs(ctx, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtKeys, dontOptimizeTimeBounds, targetSize))
			t.Run(fmt.Sprintf("%s, all, split rows, nontimebound", s.name),
				assertEqualKVs(ctx, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtTimestamps, dontOptimizeTimeBounds, targetSize))
			t.Run(fmt.Sprintf("%s, latest, timebound", s.name),
				assertEqualKVs(ctx, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportLatest, stopAtKeys, optimizeTimeBounds, targetSize))
			t.Run(fmt.Sprintf("%s, all, timebound", s.name),
				assertEqualKVs(ctx, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtKeys, optimizeTimeBounds, targetSize))
			t.Run(fmt.Sprintf("%s, all, split rows, timebound", s.name),
				assertEqualKVs(ctx, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtTimestamps, optimizeTimeBounds, targetSize))
		}
	}
	// Exercise min to max time and key ranges.
	for _, targetSize := range []uint64{
		0 /* unlimited */, 1 << 10, 1 << 16, 1 << 20,
	} {
		t.Run(fmt.Sprintf("targetSize=%d", targetSize), func(t *testing.T) {
			testWithTargetSize(t, targetSize)
		})
	}
}
