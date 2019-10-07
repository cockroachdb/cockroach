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
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
			RequestHeader: roachpb.RequestHeader{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
			StartTime:     start,
			Storage: roachpb.ExternalStorage{
				Provider:  roachpb.ExternalStorageProvider_LocalFile,
				LocalFile: roachpb.ExternalStorage_LocalFilePath{Path: "/foo"},
			},
			MVCCFilter: mvccFilter,
			ReturnSST:  true,
		}
		res, pErr := client.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
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
		RequestHeader: roachpb.RequestHeader{Key: keys.UserTableDataMin, EndKey: keys.MaxKey},
		StartTime:     hlc.Timestamp{WallTime: -1},
	}
	_, pErr := client.SendWrapped(ctx, kvDB.NonTransactionalSender(), req)
	if !testutils.IsPError(pErr, "must be after replica GC threshold") {
		t.Fatalf(`expected "must be after replica GC threshold" error got: %+v`, pErr)
	}
}

// exportUsingGoIterator uses the legacy implementation of export, and is used
// as ana oracle to check the correctness of the new C++ implementation.
func exportUsingGoIterator(
	filter roachpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	startKey, endKey roachpb.Key,
	enableTimeBoundIteratorOptimization bool,
	batch engine.Reader,
) ([]byte, error) {
	sst, err := engine.MakeRocksDBSstFileWriter()
	if err != nil {
		return nil, nil
	}
	defer sst.Close()

	var skipTombstones bool
	var iterFn func(*MVCCIncrementalIterator)
	switch filter {
	case roachpb.MVCCFilter_Latest:
		skipTombstones = true
		iterFn = (*MVCCIncrementalIterator).NextKey
	case roachpb.MVCCFilter_All:
		skipTombstones = false
		iterFn = (*MVCCIncrementalIterator).Next
	default:
		return nil, nil
	}

	iter := NewMVCCIncrementalIterator(batch, IterOptions{
		StartTime:                           startTime,
		EndTime:                             endTime,
		UpperBound:                          endKey,
		EnableTimeBoundIteratorOptimization: enableTimeBoundIteratorOptimization,
	})
	defer iter.Close()
	for iter.Seek(engine.MakeMVCCMetadataKey(startKey)); ; iterFn(iter) {
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

	if sst.DataSize == 0 {
		// Let the defer Close the sstable.
		return nil, nil
	}

	sstContents, err := sst.Finish()
	if err != nil {
		return nil, err
	}

	return sstContents, nil
}

func loadSST(t *testing.T, data []byte, start, end roachpb.Key) []engine.MVCCKeyValue {
	t.Helper()
	if len(data) == 0 {
		return nil
	}

	sst := engine.MakeRocksDBSstFileReader()
	defer sst.Close()

	if err := sst.IngestExternalFile(data); err != nil {
		t.Fatal(err)
	}

	var kvs []engine.MVCCKeyValue
	if err := sst.Iterate(engine.MVCCKey{Key: start}, engine.MVCCKey{Key: end}, func(kv engine.MVCCKeyValue) (bool, error) {
		kvs = append(kvs, kv)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	return kvs
}

func assertEqualKVs(
	ctx context.Context,
	e engine.Engine,
	startKey, endKey roachpb.Key,
	startTime, endTime hlc.Timestamp,
	exportAllRevisions bool,
	enableTimeBoundIteratorOptimization bool,
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
		start := engine.MVCCKey{Key: startKey, Timestamp: startTime}
		end := engine.MVCCKey{Key: endKey, Timestamp: endTime}
		io := engine.IterOptions{
			UpperBound: endKey,
		}
		if enableTimeBoundIteratorOptimization {
			io.MaxTimestampHint = endTime
			io.MinTimestampHint = startTime
		}
		sst, _, err := engine.ExportToSst(ctx, e, start, end, exportAllRevisions, io)
		if err != nil {
			t.Fatal(err)
		}

		// Compare new C++ implementation against the oracle.
		expectedKVS := loadSST(t, expected, startKey, endKey)
		kvs := loadSST(t, sst, startKey, endKey)

		if len(kvs) != len(expectedKVS) {
			t.Fatalf("got %d kvs (%+v) but expected %d (%+v)", len(kvs), kvs, len(expected), expected)
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
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	rocksdb, err := engine.NewRocksDB(
		engine.RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		engine.RocksDBCache{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rocksdb.Close()

	rnd, _ := randutil.NewPseudoRand()

	var (
		keyMin = roachpb.KeyMin
		keyMax = roachpb.KeyMax

		tsMin = hlc.Timestamp{WallTime: 0, Logical: 0}
		tsMax = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 0}
	)

	// Store generated keys and timestamps.
	var keys []roachpb.Key
	var timestamps []hlc.Timestamp

	// Set to > 1 to prevent random key test from panicking.
	var numKeys = 5000
	var curWallTime = 0
	var curLogical = 0

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
		if err := engine.MVCCPut(ctx, rocksdb, nil, key, ts, value, nil); err != nil {
			t.Fatal(err)
		}

		// Randomly decide whether to add a newer version of the same key to test
		// MVCC_Filter_All.
		if randutil.RandIntInRange(rnd, 0, math.MaxInt64)%2 == 0 {
			curWallTime++
			ts = hlc.Timestamp{WallTime: int64(curWallTime), Logical: int32(curLogical)}
			value = roachpb.MakeValueFromBytes(randutil.RandBytes(rnd, 200))
			value.InitChecksum(key)
			if err := engine.MVCCPut(ctx, rocksdb, nil, key, ts, value, nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	sort.Slice(timestamps, func(i, j int) bool {
		return (timestamps[i].WallTime < timestamps[j].WallTime) ||
			(timestamps[i].WallTime == timestamps[j].WallTime &&
				timestamps[i].Logical < timestamps[j].Logical)
	})

	// Exercise min to max time and key ranges.
	t.Run("ts (0-∞], latest, nontimebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, tsMin, tsMax, false, false))
	t.Run("ts (0-∞], all, nontimebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, tsMin, tsMax, true, false))
	t.Run("ts (0-∞], latest, timebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, tsMin, tsMax, false, true))
	t.Run("ts (0-∞], all, timebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, tsMin, tsMax, true, true))

	upperBound := randutil.RandIntInRange(rnd, 1, numKeys)
	lowerBound := rnd.Intn(upperBound)

	// Exercise random key ranges.
	t.Run("kv [randLower, randUpper), latest, nontimebound", assertEqualKVs(ctx, rocksdb, keys[lowerBound], keys[upperBound], tsMin, tsMax, false, false))
	t.Run("kv [randLower, randUpper), all, nontimebound", assertEqualKVs(ctx, rocksdb, keys[lowerBound], keys[upperBound], tsMin, tsMax, true, false))
	t.Run("kv [randLower, randUpper), latest, timebound", assertEqualKVs(ctx, rocksdb, keys[lowerBound], keys[upperBound], tsMin, tsMax, false, true))
	t.Run("kv [randLower, randUpper), all, timebound", assertEqualKVs(ctx, rocksdb, keys[lowerBound], keys[upperBound], tsMin, tsMax, true, true))

	upperBound = randutil.RandIntInRange(rnd, 1, numKeys)
	lowerBound = rnd.Intn(upperBound)

	// Exercise random timestamps.
	t.Run("kv (randLowerTime, randUpperTime], latest, nontimebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], false, false))
	t.Run("kv (randLowerTime, randUpperTime], all, nontimebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], true, false))
	t.Run("kv (randLowerTime, randUpperTime], latest, timebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], false, true))
	t.Run("kv (randLowerTime, randUpperTime], all, timebound", assertEqualKVs(ctx, rocksdb, keyMin, keyMax, timestamps[lowerBound], timestamps[upperBound], true, true))
}
