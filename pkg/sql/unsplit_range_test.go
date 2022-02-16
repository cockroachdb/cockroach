// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"bytes"
	"context"
	gosql "database/sql"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/gcjob"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// getMidKeyInSpan returns median of existing keys in the span{key, endKey}
func getMidKeyInSpan(t *testing.T, kvDB *kv.DB, key, endKey interface{}) roachpb.Key {
	t.Helper()
	if kvs, err := kvDB.Scan(context.Background(), key, endKey, 0); err == nil && len(kvs) > 0 {
		return kvs[len(kvs)/2].Key
	} else if err != nil {
		t.Fatal(err)
	} else if len(kvs) == 0 {
		t.Fatal("expected at least one key value pairs, but got zero")
	}

	return nil
}

// hasManuallySplitRangesInSpan checks whether there is any range has sticky bit
// in tablespan.
func hasManuallySplitRangesInSpan(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span,
) bool {
	metaStartKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.Key))
	metaEndKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.EndKey))
	ranges, err := kvDB.Scan(ctx, metaStartKey, metaEndKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	require.NotEmpty(t, ranges)
	for _, r := range ranges {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			t.Fatal(err)
		}
		if !desc.GetStickyBit().IsEmpty() {
			return true
		}
	}
	return false
}

// hasManuallySplitRangesOnIndex checks whether there is any range of an index
// has stick bit.
func hasManuallySplitRangesOnIndex(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span, indexID descpb.IndexID,
) bool {
	ranges, err := kvclient.ScanMetaKVs(ctx, kvDB.NewTxn(ctx, "drop index unsplit test"), tableSpan)
	if err != nil {
		t.Fatal("Failed to scan ranges for table")
	}

	var desc roachpb.RangeDescriptor
	for i := range ranges {
		if err := ranges[i].ValueProto(&desc); err != nil {
			t.Fatal(err)
		}
		_, _, foundIndexID, err := keys.SystemSQLCodec.DecodeIndexPrefix(roachpb.Key(desc.StartKey))
		if err != nil {
			continue
		}
		if indexID == descpb.IndexID(foundIndexID) && !desc.GetStickyBit().IsEmpty() {
			return true
		}
	}

	return false
}

// splitLastRangeInSpan split the last range into two ranges,
// and check the new range has sticky bit set.
func splitLastRangeInSpan(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span,
) roachpb.Key {
	metaStartKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.Key))
	metaEndKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.EndKey))
	ranges, err := kvDB.Scan(ctx, metaStartKey, metaEndKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	lastRange := ranges[len(ranges)-1]
	var lastRangeDesc roachpb.RangeDescriptor
	if err := lastRange.ValueProto(&lastRangeDesc); err != nil {
		t.Fatal(err)
	}
	splitKey := getMidKeyInSpan(t, kvDB, lastRangeDesc.StartKey, lastRangeDesc.EndKey)
	splitKey, err = keys.EnsureSafeSplitKey(splitKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := kvDB.AdminSplit(ctx, splitKey, hlc.MaxTimestamp); err != nil {
		t.Fatal(err)
	}

	return splitKey
}

// rangeIsManuallySplit check if there is a range starts with `startKey` and is
// manually split
func rangeIsManuallySplit(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span, startKey roachpb.Key,
) bool {
	metaStartKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.Key))
	metaEndKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.EndKey))
	ranges, err := kvDB.Scan(ctx, metaStartKey, metaEndKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	require.NotEmpty(t, ranges)
	for _, r := range ranges {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(desc.StartKey.AsRawKey(), startKey) && !desc.GetStickyBit().IsEmpty() {
			return true
		}
	}
	return false
}

// Test that manually split ranges get unsplit when dropping a
// table/database/index or truncating a table. It verifies that the logic is
// working on both the old (before version `UnsplitRangesInAsyncGCJobs`) and new
// (from versiom `UnsplitRangesInAsyncGCJobs`) pathes.
// TODO(Chengxiong): remove test for test cases with binary version
// "clusterversion.UnsplitRangesInAsyncGCJobs - 1" and update this comment in
// 22.2.
func TestUnsplitRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// To prepare each subtest, we create a table "test1" with "numRows" rows of
	// records and a secondary index "foo". The last range in the table span is
	// manually split to create a split range on the primary key index. We keep
	// the split point as a local variable "splitKey" in each test. Note that
	// there is already a split range on index "foo", so no need to create a split
	// by hand.
	//
	// For each testcase we execute a statement represented by the testcase's
	// query string to either drop the table/database/index or truncate the table.
	// The Unsplit logic is then triggered through either the new or old code
	// path, determined by the "binaryVersion" assigned. For the new code path,
	// ranges are not unsplit until the gc job is kicked off. So we wait until it
	// succeeds.
	//
	// In the end, expected results are verified based on the testcase's settings.
	// We first verify GC job succeeded by checking the number of keys remained.
	// Then we checked if there are still split ranges start with key "splitKey"
	// and on the table in overall.
	type testCase struct {
		name          string
		query         string
		binaryVersion clusterversion.Key
		// allKeyCntAfterGC is the expected keys count of the whole table.
		// For example, we expect it to be 0 when dropping a table because all data
		// should be gone, while 2*numRows is expected if only index "foo" is being
		// dropped because other keys stay.
		allKeyCntAfterGC int
		// hasSplitOnTableAfterGC indicates whether there's any split range expected
		// after gc job has finished.
		// For example, we expect it to be false when dropping a database because
		// all ranges are unsplit, but expect true when dropping index "foo" since
		// other ranges are preserved.
		hasSplitOnTableAfterGC bool
		// hasSplitOnKeyAfterGC indicates there's any split range expected to start
		// with key "splitKey" after gc job hash finished.
		// For example, we expect it to be false when truncating a table because all
		// ranges including the one with start key "splitKey" are unsplit, but
		// expect false when dropping index "foo" since the range is not touched.
		hasSplitOnKeyAfterGC bool
		// gcSucceedFunc is called within testutils.SucceedsSoon() to make sure gc
		// worked. Different statements need has different success condition.
		gcSucceedFunc func(kvDB *kv.DB, sqlDB *gosql.DB, tableDesc catalog.TableDescriptor, indexSpan roachpb.Span) error
	}

	const numRows = 2*row.TableTruncateChunkSize + 1
	const numKeys = 3 * numRows
	const tableName string = "test1"

	tableDropSucceed := func(kvDB *kv.DB, sqlDB *gosql.DB, tableDesc catalog.TableDescriptor, indexSpan roachpb.Span) error {
		if err := descExists(sqlDB, false, tableDesc.GetID()); err != nil {
			return err
		}
		return zoneExists(sqlDB, nil, tableDesc.GetID())
	}

	tableTruncateSucceed := func(kvDB *kv.DB, sqlDB *gosql.DB, tableDesc catalog.TableDescriptor, indexSpan roachpb.Span) error {
		tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
		if kvs, err := kvDB.Scan(context.Background(), tableSpan.Key, tableSpan.EndKey, 0); err != nil {
			return err
		} else if len(kvs) != 0 {
			return errors.New("table not truncated")
		}
		return nil
	}

	indexDropSucceed := func(kvDB *kv.DB, sqlDB *gosql.DB, tableDesc catalog.TableDescriptor, indexSpan roachpb.Span) error {
		if kvs, err := kvDB.Scan(context.Background(), indexSpan.Key, indexSpan.EndKey, 0); err != nil {
			return err
		} else if len(kvs) != 0 {
			return errors.New("index not dropped")
		}
		return nil
	}

	testCases := []testCase{
		{
			name:                   "drop-table-unsplit-sync",
			query:                  "DROP TABLE t.test1",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs - 1,
			allKeyCntAfterGC:       0,
			hasSplitOnTableAfterGC: false,
			hasSplitOnKeyAfterGC:   false,
			gcSucceedFunc:          tableDropSucceed,
		},
		{
			name:                   "drop-table-unsplit-async",
			query:                  "DROP TABLE t.test1",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs,
			allKeyCntAfterGC:       0,
			hasSplitOnTableAfterGC: false,
			hasSplitOnKeyAfterGC:   false,
			gcSucceedFunc:          tableDropSucceed,
		},
		{
			name:                   "drop-database-unsplit-sync",
			query:                  "DROP DATABASE t",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs - 1,
			allKeyCntAfterGC:       0,
			hasSplitOnTableAfterGC: false,
			hasSplitOnKeyAfterGC:   false,
			gcSucceedFunc:          tableDropSucceed,
		},
		{
			name:                   "drop-database-unsplit-async",
			query:                  "DROP DATABASE t",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs,
			allKeyCntAfterGC:       0,
			hasSplitOnTableAfterGC: false,
			hasSplitOnKeyAfterGC:   false,
			gcSucceedFunc:          tableDropSucceed,
		},
		{
			name:                   "truncate-table-unsplit-sync",
			query:                  "TRUNCATE TABLE t.test1",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs - 1,
			allKeyCntAfterGC:       0,
			hasSplitOnTableAfterGC: true, // It's true since we copy split points.
			hasSplitOnKeyAfterGC:   false,
			gcSucceedFunc:          tableTruncateSucceed,
		},
		{
			name:                   "truncate-table-unsplit-async",
			query:                  "TRUNCATE TABLE t.test1",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs,
			allKeyCntAfterGC:       0,
			hasSplitOnTableAfterGC: true, // It's true since we copy split points.
			hasSplitOnKeyAfterGC:   false,
			gcSucceedFunc:          tableTruncateSucceed,
		},
		{
			name:                   "drop-index-unsplit-sync",
			query:                  "DROP INDEX t.test1@foo",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs - 1,
			allKeyCntAfterGC:       numRows * 2,
			hasSplitOnTableAfterGC: true, // It's true since we only unsplit ranges of index foo
			hasSplitOnKeyAfterGC:   true,
			gcSucceedFunc:          indexDropSucceed,
		},
		{
			name:                   "drop-index-unsplit-async",
			query:                  "DROP INDEX t.test1@foo",
			binaryVersion:          clusterversion.UnsplitRangesInAsyncGCJobs,
			allKeyCntAfterGC:       numRows * 2,
			hasSplitOnTableAfterGC: true, // It's true since we only unsplit ranges of index foo
			hasSplitOnKeyAfterGC:   true,
			gcSucceedFunc:          indexDropSucceed,
		},
	}

	ctx := context.Background()
	run := func(t *testing.T, tc testCase) {
		params, _ := tests.CreateTestServerParams()
		// Override binary version to be older.
		params.Knobs.Server = &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			BinaryVersionOverride:          clusterversion.ByKey(tc.binaryVersion),
		}
		params.Knobs.JobsTestingKnobs = jobs.NewTestingKnobsWithShortIntervals()

		defer gcjob.SetSmallMaxGCIntervalForTest()()

		s, sqlDB, kvDB := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(context.Background())

		// Speed up how long it takes for the zone config changes to propagate.
		sqltestutils.SetShortRangeFeedIntervals(t, sqlDB)

		// Disable strict GC TTL enforcement because we're going to shove a zero-value
		// TTL into the system with AddImmediateGCZoneConfig.
		defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

		require.NoError(t, tests.CreateKVTable(sqlDB, tableName, numRows))

		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName)
		tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
		tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

		idx, err := tableDesc.FindIndexWithName("foo")
		if err != nil {
			t.Fatal(err)
		}
		indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, idx.GetID())
		tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

		// Split the last range.
		splitKey := splitLastRangeInSpan(ctx, t, kvDB, tableSpan)
		// Verify there are manually split ranges.
		require.True(t, rangeIsManuallySplit(ctx, t, kvDB, tableSpan, splitKey))
		require.True(t, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))
		require.True(t, hasManuallySplitRangesOnIndex(ctx, t, kvDB, tableSpan, idx.GetID()))

		if _, err := sqlDB.Exec(tc.query); err != nil {
			t.Fatal(err)
		}
		// Push a new zone config for a few tables with TTL=0 so the data
		// is deleted immediately.
		if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
			t.Fatal(err)
		}

		// Check GC worked!
		testutils.SucceedsSoon(t, func() error {
			return tc.gcSucceedFunc(kvDB, sqlDB, tableDesc, indexSpan)
		})
		tests.CheckKeyCount(t, kvDB, tableSpan, tc.allKeyCntAfterGC)
		// There should be always zero keys left since dropping index/table/database or
		// truncating table all remove index "foo".
		tests.CheckKeyCount(t, kvDB, indexSpan, 0 /*numKeys*/)

		require.Equal(t, tc.hasSplitOnKeyAfterGC, rangeIsManuallySplit(ctx, t, kvDB, tableSpan, splitKey))
		require.Equal(t, tc.hasSplitOnTableAfterGC, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))
		// This should be always false since dropping index/table/database or
		// truncating table all remove index "foo".
		require.False(t, hasManuallySplitRangesOnIndex(ctx, t, kvDB, tableSpan, idx.GetID()))
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { run(t, tc) })
	}
}
