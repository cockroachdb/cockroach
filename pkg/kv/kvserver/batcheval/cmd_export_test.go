// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestExportCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(109429),
		ExternalIODir:     dir,
	})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	export := func(
		t *testing.T, start hlc.Timestamp, mvccFilter kvpb.MVCCFilter, maxResponseSSTBytes int64,
	) (kvpb.Response, *kvpb.Error) {
		startKey := ts.Codec().TablePrefix(bootstrap.TestingUserDescID(0))
		endKey := ts.Codec().TenantSpan().EndKey
		req := &kvpb.ExportRequest{
			RequestHeader:  kvpb.RequestHeader{Key: startKey, EndKey: endKey},
			StartTime:      start,
			MVCCFilter:     mvccFilter,
			TargetFileSize: batcheval.ExportRequestTargetFileSize.Get(&ts.ClusterSettings().SV),
		}
		var h kvpb.Header
		h.TargetBytes = maxResponseSSTBytes
		return kv.SendWrappedWith(ctx, kvDB.NonTransactionalSender(), h, req)
	}

	exportAndSlurpOne := func(
		t *testing.T, start hlc.Timestamp, mvccFilter kvpb.MVCCFilter, maxResponseSSTBytes int64,
	) (int, []storage.MVCCKeyValue, kvpb.ResponseHeader) {
		res, pErr := export(t, start, mvccFilter, maxResponseSSTBytes)
		if pErr != nil {
			t.Fatalf("%+v", pErr)
		}

		var files int
		var kvs []storage.MVCCKeyValue
		for _, file := range res.(*kvpb.ExportResponse).Files {
			files++
			iterOpts := storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsOnly,
				LowerBound: keys.LocalMax,
				UpperBound: keys.MaxKey,
			}
			sst, err := storage.NewMemSSTIterator(file.SST, true, iterOpts)
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
				v, err := sst.UnsafeValue()
				require.NoError(t, err)
				newKv.Value = append(newKv.Value, v...)
				kvs = append(kvs, newKv)
				sst.Next()
			}
		}

		return files, kvs, res.(*kvpb.ExportResponse).Header()
	}
	type ExportAndSlurpResult struct {
		end                      hlc.Timestamp
		mvccLatestFiles          int
		mvccLatestKVs            []storage.MVCCKeyValue
		mvccAllFiles             int
		mvccAllKVs               []storage.MVCCKeyValue
		mvccLatestResponseHeader kvpb.ResponseHeader
		mvccAllResponseHeader    kvpb.ResponseHeader
	}
	exportAndSlurp := func(t *testing.T, start hlc.Timestamp,
		maxResponseSSTBytes int64) ExportAndSlurpResult {
		var ret ExportAndSlurpResult
		ret.end = hlc.NewClockForTesting(nil).Now()
		ret.mvccLatestFiles, ret.mvccLatestKVs, ret.mvccLatestResponseHeader = exportAndSlurpOne(t,
			start, kvpb.MVCCFilter_Latest, maxResponseSSTBytes)
		ret.mvccAllFiles, ret.mvccAllKVs, ret.mvccAllResponseHeader = exportAndSlurpOne(t, start,
			kvpb.MVCCFilter_All, maxResponseSSTBytes)
		return ret
	}

	expect := func(
		t *testing.T, res ExportAndSlurpResult,
		mvccLatestFilesLen int, mvccLatestKVsLen int, mvccAllFilesLen int, mvccAllKVsLen int,
	) {
		t.Helper()
		require.Equal(t, res.mvccLatestFiles, mvccLatestFilesLen, "unexpected files in latest export")
		require.Len(t, res.mvccLatestKVs, mvccLatestKVsLen, "unexpected kvs in latest export")
		require.Equal(t, res.mvccAllFiles, mvccAllFilesLen, "unexpected files in all export")
		require.Len(t, res.mvccAllKVs, mvccAllKVsLen, "unexpected kvs in all export")
	}

	expectResponseHeader := func(
		t *testing.T, res ExportAndSlurpResult, mvccLatestResponseHeader kvpb.ResponseHeader,
		mvccAllResponseHeader kvpb.ResponseHeader) {
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

	sqlDB := sqlutils.MakeSQLRunner(db)
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
		_, pErr := export(t, res5.end, kvpb.MVCCFilter_Latest, noTargetBytes)
		require.Regexp(t, expectedError, pErr)
		hints := errors.GetAllHints(pErr.GoError())
		require.Equal(t, 1, len(hints))
		const expectedHint = `consider increasing cluster setting "kv.bulk_sst.max_allowed_overage"`
		require.Regexp(t, expectedHint, hints[0])
		_, pErr = export(t, res5.end, kvpb.MVCCFilter_All, noTargetBytes)
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
		latestRespHeader := kvpb.ResponseHeader{
			NumBytes: maxResponseSSTBytes,
		}
		allRespHeader := kvpb.ResponseHeader{
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
		latestRespHeader = kvpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/2", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: kvpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		allRespHeader = kvpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/2", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: kvpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)

		// TargetSize to one KV and TargetBytes to two KVs. We should see one KV in
		// each ExportRequest SST.
		setExportTargetSize(t, "'11b'")
		maxResponseSSTBytes = 2 * kvByteSize
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 2, 2, 2, 2)
		latestRespHeader = kvpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/3/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: kvpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		allRespHeader = kvpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/3/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: kvpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)

		// TargetSize to one KV and TargetBytes to one less than the total KVs.
		setExportTargetSize(t, "'11b'")
		maxResponseSSTBytes = 99 * kvByteSize
		res7 = exportAndSlurp(t, res5.end, maxResponseSSTBytes)
		expect(t, res7, 99, 99, 99, 99)
		latestRespHeader = kvpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/100/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: kvpb.RESUME_BYTE_LIMIT,
			NumBytes:     maxResponseSSTBytes,
		}
		allRespHeader = kvpb.ResponseHeader{
			ResumeSpan: &roachpb.Span{
				Key:    []byte(fmt.Sprintf("/Table/%d/1/100/0", tableID)),
				EndKey: []byte("/Max"),
			},
			ResumeReason: kvpb.RESUME_BYTE_LIMIT,
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
		latestRespHeader = kvpb.ResponseHeader{
			NumBytes: 100 * kvByteSize,
		}
		allRespHeader = kvpb.ResponseHeader{
			NumBytes: 100 * kvByteSize,
		}
		expectResponseHeader(t, res7, latestRespHeader, allRespHeader)
	})
}

func TestExportRequestWithCPULimitResumeSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test runs into overload issues when run with the {race, deadlock}
	// detector using remote execution.
	skip.UnderRace(t)
	skip.UnderDeadlock(t)

	ctx := context.Background()
	rng, _ := randutil.NewTestRand()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			UseDatabase: "test",
			Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
					for _, ru := range request.Requests {
						if _, ok := ru.GetInner().(*kvpb.ExportRequest); ok {
							h := admission.ElasticCPUWorkHandleFromContext(ctx)
							if h == nil {
								t.Fatalf("expected context to have CPU work handle")
							}
							h.TestingOverrideOverLimit(func() (bool, time.Duration) {
								if rng.Float32() > 0.5 {
									return true, 0
								}
								return false, 0
							})
						}
					}
					return nil
				},
			}}}})
	defer tc.Stopper().Stop(context.Background())

	srv := tc.Server(0)

	s := srv.ApplicationLayer()
	sqlDB := tc.Conns[0]
	kvDB := s.DB()

	db := sqlutils.MakeSQLRunner(sqlDB)
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	const (
		initRows = 1000
		splits   = 100
	)
	db.Exec(t, "CREATE DATABASE IF NOT EXISTS test")
	db.Exec(t, "CREATE TABLE test (k PRIMARY KEY) AS SELECT generate_series(1, $1)", initRows)
	db.Exec(t, "ALTER TABLE test SPLIT AT (select i*10 from generate_series(1, $1) as i)", initRows/splits)
	db.Exec(t, "ALTER TABLE test SCATTER")

	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, execCfg.Codec, "test", "test")
	span := desc.TableSpan(execCfg.Codec)

	req := &kvpb.ExportRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    span.Key,
			EndKey: span.EndKey},
	}
	header := kvpb.Header{
		ReturnElasticCPUResumeSpans: true,
	}
	_, err := kv.SendWrappedWith(ctx, kvDB.NonTransactionalSender(), header, req)
	require.NoError(t, err.GoError())
}

func TestExportGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()

	startKey := bootstrap.TestingUserTableDataMin(ts.Codec())
	endKey := ts.Codec().TenantEndKey()

	req := &kvpb.ExportRequest{
		RequestHeader: kvpb.RequestHeader{Key: startKey, EndKey: endKey},
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
	filter kvpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	startKey, endKey roachpb.Key,
	reader storage.Reader,
) ([]byte, error) {
	memFile := &storage.MemObject{}
	sst := storage.MakeIngestionSSTWriter(
		ctx, cluster.MakeTestingClusterSettings(), memFile,
	)
	defer sst.Close()

	var skipTombstones bool
	var iterFn func(*storage.MVCCIncrementalIterator)
	switch filter {
	case kvpb.MVCCFilter_Latest:
		skipTombstones = true
		iterFn = (*storage.MVCCIncrementalIterator).NextKey
	case kvpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*storage.MVCCIncrementalIterator).Next
	default:
		return nil, nil
	}

	iter, err := storage.NewMVCCIncrementalIterator(ctx, reader, storage.MVCCIncrementalIterOptions{
		EndKey:    endKey,
		StartTime: startTime,
		EndTime:   endTime,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.SeekGE(storage.MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
		ok, err := iter.Valid()
		if err != nil {
			// The error may be a LockConflictError. In which case, returning it will
			// cause this command to be retried.
			return nil, err
		}
		if !ok || iter.UnsafeKey().Key.Compare(endKey) >= 0 {
			break
		}

		// Skip tombstone (len=0) records when startTime is zero
		// (non-incremental) and we're not exporting all versions.
		v, err := iter.UnsafeValue()
		if err != nil {
			return nil, err
		}
		if skipTombstones && startTime.IsEmpty() && len(v) == 0 {
			continue
		}

		if err := sst.Put(iter.UnsafeKey(), v); err != nil {
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

	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: start,
		UpperBound: end,
	}
	sst, err := storage.NewMemSSTIterator(data, true, iterOpts)
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
		v, err := sst.UnsafeValue()
		if err != nil {
			t.Fatal(err)
		}
		newKv.Value = append(newKv.Value, v...)
		kvs = append(kvs, newKv)
		sst.Next()
	}

	return kvs
}

type exportRevisions bool
type batchBoundaries bool

const (
	exportAll    exportRevisions = true
	exportLatest exportRevisions = false

	stopAtTimestamps batchBoundaries = true
	stopAtKeys       batchBoundaries = false
)

func assertEqualKVs(
	ctx context.Context,
	st *cluster.Settings,
	e storage.Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	exportAllRevisions exportRevisions,
	stopMidKey batchBoundaries,
	targetSize uint64,
) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		var filter kvpb.MVCCFilter
		if exportAllRevisions {
			filter = kvpb.MVCCFilter_All
		} else {
			filter = kvpb.MVCCFilter_Latest
		}

		// Run the oracle which is a legacy implementation of pebbleExportToSst
		// backed by an MVCCIncrementalIterator.
		expected, err := exportUsingGoIterator(ctx, filter, startTime, endTime, startKey, endKey, e)
		if err != nil {
			t.Fatalf("Oracle failed to export provided key range.")
		}

		// Run the actual code path used when exporting MVCCs to SSTs.
		var kvs []storage.MVCCKeyValue
		start := storage.MVCCKey{Key: startKey}
		for start.Key != nil {
			var sst []byte
			maxSize := uint64(0)
			prevStart := start
			var sstFile bytes.Buffer
			summary, resumeInfo, err := storage.MVCCExportToSST(ctx, st, e, storage.MVCCExportOptions{
				StartKey:           start,
				EndKey:             endKey,
				StartTS:            startTime,
				EndTS:              endTime,
				ExportAllRevisions: bool(exportAllRevisions),
				TargetSize:         targetSize,
				MaxSize:            maxSize,
				StopMidKey:         bool(stopMidKey),
			}, &sstFile)
			require.NoError(t, err)
			start = resumeInfo.ResumeKey
			sst = sstFile.Bytes()
			loaded := loadSST(t, sst, startKey, endKey)
			// Ensure that the pagination worked properly.
			if start.Key != nil {
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
				_, _, err = storage.MVCCExportToSST(ctx, st, e, storage.MVCCExportOptions{
					StartKey:           prevStart,
					EndKey:             endKey,
					StartTS:            startTime,
					EndTS:              endTime,
					ExportAllRevisions: bool(exportAllRevisions),
					TargetSize:         targetSize,
					MaxSize:            maxSize,
					StopMidKey:         false,
				}, &bytes.Buffer{})
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
	defer log.Scope(t).Close(t)

	storage.DisableMetamorphicSimpleValueEncoding(t)

	ctx := context.Background()

	st := cluster.MakeTestingClusterSettings()
	mkEngine := func(t *testing.T) (e storage.Engine, cleanup func()) {
		dir, cleanupDir := testutils.TempDir(t)
		e, err := storage.Open(ctx, fs.MustInitPhysicalTestingEnv(dir), st, storage.CacheSize(0))
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

		numDigits := len(strconv.Itoa(numKeys))
		batch := e.NewBatch()
		for i := 0; i < numKeys; i++ {
			// Ensure walltime and logical are monotonically increasing.
			curWallTime := randutil.RandIntInRange(rnd, 0, math.MaxInt64-1)
			curLogical := randutil.RandIntInRange(rnd, 0, math.MaxInt32-1)
			ts := hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
			timestamps = append(timestamps, ts)

			// Make keys unique and ensure they are monotonically increasing.
			key := roachpb.Key(randutil.RandBytes(rnd, keySize))
			key = append([]byte(fmt.Sprintf("#%0"+strconv.Itoa(numDigits)+"d", i)), key...)
			keys = append(keys, key)

			averageValueSize := bytesPerValue - keySize
			valueSize := randutil.RandIntInRange(rnd, averageValueSize-100, averageValueSize+100)
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, valueSize))
			value.InitChecksum(key)
			if _, err := storage.MVCCPut(
				ctx, batch, key, ts, value, storage.MVCCWriteOptions{},
			); err != nil {
				t.Fatal(err)
			}

			// Randomly decide whether to add a newer version of the same key to test
			// MVCC_Filter_All.
			if randutil.RandIntInRange(rnd, 0, math.MaxInt64)%2 == 0 {
				curWallTime++
				ts = hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
				value = roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, 200))
				value.InitChecksum(key)
				if _, err := storage.MVCCPut(
					ctx, batch, key, ts, value, storage.MVCCWriteOptions{},
				); err != nil {
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
			t.Run(fmt.Sprintf("%s, latest", s.name),
				assertEqualKVs(ctx, st, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportLatest, stopAtKeys, targetSize))
			t.Run(fmt.Sprintf("%s, all", s.name),
				assertEqualKVs(ctx, st, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtKeys, targetSize))
			t.Run(fmt.Sprintf("%s, all, split rows", s.name),
				assertEqualKVs(ctx, st, e, s.keyMin, s.keyMax, s.tsMin, s.tsMax, exportAll, stopAtTimestamps, targetSize))
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
