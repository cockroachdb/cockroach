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
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
		t *testing.T, start hlc.Timestamp, mvccFilter roachpb.MVCCFilter,
	) (roachpb.Response, *roachpb.Error) {
		req := &roachpb.ExportRequest{
			RequestHeader: roachpb.RequestHeader{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
			StartTime:     start,
			Storage: roachpb.ExternalStorage{
				Provider:  roachpb.ExternalStorageProvider_LocalFile,
				LocalFile: roachpb.ExternalStorage_LocalFilePath{Path: "/foo"},
			},
			MVCCFilter:     mvccFilter,
			ReturnSST:      true,
			TargetFileSize: ExportRequestTargetFileSize.Get(&tc.Server(0).ClusterSettings().SV),
		}
		return kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	}

	exportAndSlurpOne := func(
		t *testing.T, start hlc.Timestamp, mvccFilter roachpb.MVCCFilter,
	) ([]string, []storage.MVCCKeyValue) {
		res, pErr := export(t, start, mvccFilter)
		if pErr != nil {
			t.Fatalf("%+v", pErr)
		}

		var paths []string
		var kvs []storage.MVCCKeyValue
		ingestFunc := func(kv storage.MVCCKeyValue) (bool, error) {
			kvs = append(kvs, kv)
			return false, nil
		}
		for _, file := range res.(*roachpb.ExportResponse).Files {
			paths = append(paths, file.Path)

			sst := storage.MakeRocksDBSstFileReader()
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
			if err := sst.Iterate(keys.MinKey, keys.MaxKey, ingestFunc); err != nil {
				t.Fatalf("%+v", err)
			}
		}

		return paths, kvs
	}
	type ExportAndSlurpResult struct {
		end             hlc.Timestamp
		mvccLatestFiles []string
		mvccLatestKVs   []storage.MVCCKeyValue
		mvccAllFiles    []string
		mvccAllKVs      []storage.MVCCKeyValue
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
	t.Run("ts1", func(t *testing.T) {
		// When run with MVCCFilter_Latest and a startTime of 0 (full backup of
		// only the latest values), Export special cases and skips keys that are
		// deleted before the export timestamp.
		sqlDB.Exec(t, `INSERT INTO mvcclatest.export VALUES (1, 1), (3, 3), (4, 4)`)
		sqlDB.Exec(t, `DELETE from mvcclatest.export WHERE id = 4`)
		res1 = exportAndSlurp(t, hlc.Timestamp{})
		expect(t, res1, 1, 2, 1, 4)
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		res1 = exportAndSlurp(t, hlc.Timestamp{})
		expect(t, res1, 2, 2, 3, 4)
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

		// Re-run the test with a 1b target size which will lead to more files.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		res5 = exportAndSlurp(t, hlc.Timestamp{})
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
		res6 = exportAndSlurp(t, res5.end)
		expect(t, res6, 2, 100, 2, 100)

		// Re-run the test with a 1b target size which will lead to 100 files.
		defer resetExportTargetSize(t)
		setExportTargetSize(t, "'1b'")
		res6 = exportAndSlurp(t, res5.end)
		expect(t, res6, 100, 100, 100, 100)

		// Set the MaxOverage to 1b and ensure that we get errors due to
		// the max overage being exceeded.
		defer resetMaxOverage(t)
		setMaxOverage(t, "'1b'")
		const expectedError = `export size \(11 bytes\) exceeds max size \(2 bytes\)`
		_, pErr := export(t, res5.end, roachpb.MVCCFilter_Latest)
		require.Regexp(t, expectedError, pErr)
		_, pErr = export(t, res5.end, roachpb.MVCCFilter_All)
		require.Regexp(t, expectedError, pErr)

		// Disable the TargetSize and ensure that we don't get any errors
		// to the max overage being exceeded.
		setExportTargetSize(t, "'0b'")
		res6 = exportAndSlurp(t, res5.end)
		expect(t, res6, 2, 100, 2, 100)

	})
}

func TestExportGCThreshold(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	kvDB := tc.Server(0).DB()

	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeader{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
		StartTime:     hlc.Timestamp{WallTime: -1},
	}
	_, pErr := kv.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
}

// exportUsingGoIterator uses the legacy implementation of export, and is used
// as an oracle to check the correctness of the new C++ implementation.
func exportUsingGoIterator(
	filter roachpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	startKey, endKey roachpb.Key,
	enableTimeBoundIteratorOptimization bool,
	reader storage.Reader,
) ([]byte, error) {
	sst, err := storage.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, nil //nolint:returnerrcheck
	}
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

	io := storage.IterOptions{
		UpperBound: endKey,
	}
	if enableTimeBoundIteratorOptimization {
		io.MaxTimestampHint = endTime
		io.MinTimestampHint = startTime.Next()
	}
	iter := storage.NewMVCCIncrementalIterator(reader, storage.MVCCIncrementalIterOptions{
		IterOptions: io,
		StartTime:   startTime,
		EndTime:     endTime,
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

	if sst.DataSize() == 0 {
		// Let the defer Close the sstable.
		return nil, nil
	}

	sstContents, err := sst.Finish()
	if err != nil {
		return nil, err
	}

	return sstContents, nil
}

func loadSST(t *testing.T, data []byte, start, end roachpb.Key) []storage.MVCCKeyValue {
	t.Helper()
	if len(data) == 0 {
		return nil
	}

	sst := storage.MakeRocksDBSstFileReader()
	defer sst.Close()

	if err := sst.IngestExternalFile(data); err != nil {
		t.Fatal(err)
	}

	var kvs []storage.MVCCKeyValue
	if err := sst.Iterate(start, end, func(kv storage.MVCCKeyValue) (bool, error) {
		kvs = append(kvs, kv)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	return kvs
}

func assertEqualKVs(
	ctx context.Context,
	e storage.Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	exportAllRevisions bool,
	enableTimeBoundIteratorOptimization bool,
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

		// Run oracle (go implementation of the IncrementalIterator).
		expected, err := exportUsingGoIterator(filter, startTime, endTime,
			startKey, endKey, enableTimeBoundIteratorOptimization, e)
		if err != nil {
			t.Fatalf("Oracle failed to export provided key range.")
		}

		// Run new C++ implementation of IncrementalIterator.
		io := storage.IterOptions{
			UpperBound: endKey,
		}
		if enableTimeBoundIteratorOptimization {
			io.MaxTimestampHint = endTime
			io.MinTimestampHint = startTime.Next()
		}
		var kvs []storage.MVCCKeyValue
		for start := startKey; start != nil; {
			var sst []byte
			var summary roachpb.BulkOpSummary
			maxSize := uint64(0)
			prevStart := start
			sst, summary, start, err = e.ExportToSst(start, endKey, startTime, endTime,
				exportAllRevisions, targetSize, maxSize, io)
			require.NoError(t, err)
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
				_, _, _, err = e.ExportToSst(prevStart, endKey, startTime, endTime,
					exportAllRevisions, targetSize, maxSize, io)
				require.Regexp(t, fmt.Sprintf("export size \\(%d bytes\\) exceeds max size \\(%d bytes\\)",
					dataSizeWhenExceeded, maxSize), err)
			}
			kvs = append(kvs, loaded...)
		}

		// Compare new C++ implementation against the oracle.
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
		e, err := storage.NewDefaultEngine(
			0,
			base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			})
		if err != nil {
			t.Fatal(err)
		}
		return e, func() {
			e.Close()
			cleanupDir()
		}
	}
	getNumKeys := func(t *testing.T, rnd *rand.Rand, targetSize uint64) (numKeys int) {
		const (
			targetPages   = 10
			bytesPerValue = 300
			minNumKeys    = 2 // need > 1 keys for random key test
			maxNumKeys    = 5000
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
			key := roachpb.Key(randutil.RandBytes(rnd, 100))
			key = append([]byte(fmt.Sprintf("#%d", i)), key...)
			keys = append(keys, key)

			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, 200))
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

		sort.Slice(timestamps, func(i, j int) bool {
			return (timestamps[i].WallTime < timestamps[j].WallTime) ||
				(timestamps[i].WallTime == timestamps[j].WallTime &&
					timestamps[i].Logical < timestamps[j].Logical)
		})
		return keys, timestamps
	}

	testWithTargetSize := func(t *testing.T, targetSize uint64) {
		e, cleanup := mkEngine(t)
		defer cleanup()
		rnd, _ := randutil.NewPseudoRand()
		numKeys := getNumKeys(t, rnd, targetSize)
		keys, timestamps := mkData(t, e, rnd, numKeys)
		var (
			keyMin = roachpb.KeyMin
			keyMax = roachpb.KeyMax

			tsMin = hlc.Timestamp{WallTime: 0, Logical: 0}
			tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
		)

		t.Run("ts (0-∞], latest, nontimebound", assertEqualKVs(ctx, e, keyMin, keyMax, tsMin, tsMax, false, false, targetSize))
		t.Run("ts (0-∞], all, nontimebound", assertEqualKVs(ctx, e, keyMin, keyMax, tsMin, tsMax, true, false, targetSize))
		t.Run("ts (0-∞], latest, timebound", assertEqualKVs(ctx, e, keyMin, keyMax, tsMin, tsMax, false, true, targetSize))
		t.Run("ts (0-∞], all, timebound", assertEqualKVs(ctx, e, keyMin, keyMax, tsMin, tsMax, true, true, targetSize))

		upperBound := randutil.RandIntInRange(rnd, 1, numKeys)
		lowerBound := rnd.Intn(upperBound)

		// Exercise random key ranges.
		t.Run("kv [randLower, randUpper), latest, nontimebound", assertEqualKVs(ctx, e, keys[lowerBound], keys[upperBound], tsMin, tsMax, false, false, targetSize))
		t.Run("kv [randLower, randUpper), all, nontimebound", assertEqualKVs(ctx, e, keys[lowerBound], keys[upperBound], tsMin, tsMax, true, false, targetSize))
		t.Run("kv [randLower, randUpper), latest, timebound", assertEqualKVs(ctx, e, keys[lowerBound], keys[upperBound], tsMin, tsMax, false, true, targetSize))
		t.Run("kv [randLower, randUpper), all, timebound", assertEqualKVs(ctx, e, keys[lowerBound], keys[upperBound], tsMin, tsMax, true, true, targetSize))

		upperBound = randutil.RandIntInRange(rnd, 1, numKeys)
		lowerBound = rnd.Intn(upperBound)

		// Exercise random timestamps.
		t.Run("kv (randLowerTime, randUpperTime], latest, nontimebound", assertEqualKVs(ctx, e, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], false, false, targetSize))
		t.Run("kv (randLowerTime, randUpperTime], all, nontimebound", assertEqualKVs(ctx, e, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], true, false, targetSize))
		t.Run("kv (randLowerTime, randUpperTime], latest, timebound", assertEqualKVs(ctx, e, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], false, true, targetSize))
		t.Run("kv (randLowerTime, randUpperTime], all, timebound", assertEqualKVs(ctx, e, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], true, true, targetSize))
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
