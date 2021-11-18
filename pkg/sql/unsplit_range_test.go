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
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

// countManuallySplitRangesInSpan returns a count of ranges have sticky bit in a
// tablespan.
func countManuallySplitRangesInSpan(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span,
) int {
	metaStartKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.Key))
	metaEndKey := keys.RangeMetaKey(keys.MustAddr(tableSpan.EndKey))
	ranges, err := kvDB.Scan(ctx, metaStartKey, metaEndKey, 0)
	if err != nil {
		t.Fatal(err)
	}
	require.NotEmpty(t, ranges)
	count := 0
	for _, r := range ranges {
		var desc roachpb.RangeDescriptor
		if err := r.ValueProto(&desc); err != nil {
			t.Fatal(err)
		}
		if !desc.GetStickyBit().IsEmpty() {
			count++
		}
	}
	return count
}

// hasManuallySplitRangesInSpan checks whether there is any range has sticky bit
// in tablespan.
func hasManuallySplitRangesInSpan(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span,
) bool {
	return countManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan) > 0
}

// countManuallySplitRangesOnIndex returns a count of ranges of have sticky bit
// of an index.
func countManuallySplitRangesOnIndex(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span, indexID descpb.IndexID,
) int {
	ranges, err := kvclient.ScanMetaKVs(ctx, kvDB.NewTxn(ctx, "drop index unsplit test"), tableSpan)
	if err != nil {
		t.Fatal("Failed to scan ranges for table")
	}
	count := 0
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
			count++
		}
	}

	return count
}

// hasManuallySplitRangesOnIndex checks whether there is any range of an index
// has stick bit.
func hasManuallySplitRangesOnIndex(
	ctx context.Context, t *testing.T, kvDB *kv.DB, tableSpan roachpb.Span, indexID descpb.IndexID,
) bool {
	return countManuallySplitRangesOnIndex(ctx, t, kvDB, tableSpan, indexID) > 0
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
		if bytes.Compare(desc.StartKey.AsRawKey(), startKey) == 0 && !desc.GetStickyBit().IsEmpty() {
			return true
		}
	}
	return false
}

// TODO(Chengxiong): remove test for old version and update this comment.
// Test that manually split ranges get unsplit when dropping a table/database.
// It verifies that the logic is working on both the old (before version
// `UnsplitRangesInAsyncGCJobs`) and new (from versiom
// `UnsplitRangesInAsyncGCJobs`) pathes.
func TestRangesUnsplitWhenTableDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		query         string
		binaryVersion clusterversion.Key
	}{
		{
			name:          "drop-table-unsplit-sync",
			query:         "DROP TABLE t.test1",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs - 1,
		},
		{
			name:          "drop-table-unsplit-async",
			query:         "DROP TABLE t.test1",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs,
		},
		{
			name:          "drop-database-unsplit-sync",
			query:         "DROP DATABASE t",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs - 1,
		},
		{
			name:          "drop-database-unsplit-async",
			query:         "DROP DATABASE t",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defer leaktest.AfterTest(t)()
			defer log.Scope(t).Close(t)
			params, _ := tests.CreateTestServerParams()
			// Override binary version to be older.
			params.Knobs.Server = &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: 1,
				BinaryVersionOverride:          clusterversion.ByKey(testCase.binaryVersion),
			}

			defer gcjob.SetSmallMaxGCIntervalForTest()()

			s, sqlDB, kvDB := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(context.Background())
			ctx := context.Background()

			// Disable strict GC TTL enforcement because we're going to shove a zero-value
			// TTL into the system with AddImmediateGCZoneConfig.
			defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

			const numRows = 2*row.TableTruncateChunkSize + 1
			const numKeys = 3 * numRows
			const tableName string = "test1"
			require.NoError(t, tests.CreateKVTable(sqlDB, tableName, numRows))

			tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName)
			tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
			tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

			// Assert that all ranges are not manually split.
			require.False(t, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))
			// Split the last range.
			splitLastRangeInSpan(ctx, t, kvDB, tableSpan)
			// Verify there are manually split ranges.
			require.True(t, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))

			if _, err := sqlDB.Exec(testCase.query); err != nil {
				t.Fatal(err)
			}
			// Push a new zone config for a few tables with TTL=0 so the data
			// is deleted immediately.
			if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
				t.Fatal(err)
			}

			// Check GC worked!
			testutils.SucceedsSoon(t, func() error {
				if err := descExists(sqlDB, false, tableDesc.GetID()); err != nil {
					return err
				}
				return zoneExists(sqlDB, nil, tableDesc.GetID())
			})
			tests.CheckKeyCount(t, kvDB, tableSpan, 0)

			// Verify there is no manually split ranges.
			require.False(t, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))
		})
	}
}

// TODO(Chengxiong): remove test for old version and update this comment.
// Test that manually split ranges get unsplit when truncating a table.
// It verifies that the logic is working on both the old (before version
// `UnsplitRangesInAsyncGCJobs`) and new (from versiom
// `UnsplitRangesInAsyncGCJobs`) pathes.
func TestRangesUnsplitWhenTableTruncated(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		query         string
		binaryVersion clusterversion.Key
	}{
		{
			name:          "truncate-table-unsplit-sync",
			query:         "TRUNCATE TABLE t.test1",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs - 1,
		},
		{
			name:          "truncate-table-unsplit-async",
			query:         "TRUNCATE TABLE t.test1",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defer leaktest.AfterTest(t)()
			defer log.Scope(t).Close(t)
			params, _ := tests.CreateTestServerParams()
			// Override binary version to be older.
			params.Knobs.Server = &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: 1,
				BinaryVersionOverride:          clusterversion.ByKey(testCase.binaryVersion),
			}

			defer gcjob.SetSmallMaxGCIntervalForTest()()

			s, sqlDB, kvDB := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(context.Background())
			ctx := context.Background()

			// Disable strict GC TTL enforcement because we're going to shove a zero-value
			// TTL into the system with AddImmediateGCZoneConfig.
			defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

			const numRows = 2*row.TableTruncateChunkSize + 1
			const numKeys = 3 * numRows
			const tableName string = "test1"
			require.NoError(t, tests.CreateKVTable(sqlDB, tableName, numRows))

			tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName)
			tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
			tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)
			// Assert that all ranges are not manually split.
			require.Equal(t, 0, countManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))

			// Split the last range.
			splitKey := splitLastRangeInSpan(ctx, t, kvDB, tableSpan)
			// Verify the range start with the split key is manually split.
			require.True(t, rangeIsManuallySplit(ctx, t, kvDB, tableSpan, splitKey))
			require.Equal(t, 1, countManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))

			if _, err := sqlDB.Exec(testCase.query); err != nil {
				t.Fatal(err)
			}
			// Push a new zone config for a few tables with TTL=0 so the data
			// is deleted immediately.
			if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
				t.Fatal(err)
			}

			// Check GC worked!
			testutils.SucceedsSoon(t, func() error {
				if kvs, err := kvDB.Scan(context.Background(), tableSpan.Key, tableSpan.EndKey, 0); err != nil {
					return err
				} else if len(kvs) != 0 {
					return errors.New("table not truncated")
				}
				return nil
			})
			tests.CheckKeyCount(t, kvDB, tableSpan, 0)

			// Verify there are still manually split ranges, but range start with the
			// split key is not manually split anymore.
			require.False(t, rangeIsManuallySplit(ctx, t, kvDB, tableSpan, splitKey))
			require.Equal(t, 1, countManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))
		})
	}
}

// TODO(Chengxiong): remove test for old version and update this comment.
// Test that manually split ranges get unsplit when dropping an index.
// It verifies that the logic is working on both the old (before version
// `UnsplitRangesInAsyncGCJobs`) and new (from versiom
// `UnsplitRangesInAsyncGCJobs`) pathes.
func TestRangesUnsplitWhenIndexDropped(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name          string
		query         string
		binaryVersion clusterversion.Key
	}{
		{
			name:          "truncate-table-unsplit-sync",
			query:         "DROP INDEX t.test1@foo",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs - 1,
		},
		{
			name:          "truncate-table-unsplit-async",
			query:         "DROP INDEX t.test1@foo",
			binaryVersion: clusterversion.UnsplitRangesInAsyncGCJobs,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defer leaktest.AfterTest(t)()
			defer log.Scope(t).Close(t)
			params, _ := tests.CreateTestServerParams()
			// Override binary version to be older.
			params.Knobs.Server = &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: 1,
				BinaryVersionOverride:          clusterversion.ByKey(testCase.binaryVersion),
			}

			defer gcjob.SetSmallMaxGCIntervalForTest()()

			s, sqlDB, kvDB := serverutils.StartServer(t, params)
			defer s.Stopper().Stop(context.Background())
			ctx := context.Background()

			// Disable strict GC TTL enforcement because we're going to shove a zero-value
			// TTL into the system with AddImmediateGCZoneConfig.
			defer sqltestutils.DisableGCTTLStrictEnforcement(t, sqlDB)()

			const numRows = 2*row.TableTruncateChunkSize + 1
			const numKeys = 3 * numRows
			const tableName string = "test1"
			require.NoError(t, tests.CreateKVTable(sqlDB, tableName, numRows))

			tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", tableName)
			tableSpan := tableDesc.TableSpan(keys.SystemSQLCodec)
			tests.CheckKeyCount(t, kvDB, tableSpan, numKeys)

			idx, err := tableDesc.FindIndexWithName("foo")
			if err != nil {
				t.Fatal(err)
			}
			indexSpan := tableDesc.IndexSpan(keys.SystemSQLCodec, idx.GetID())
			tests.CheckKeyCount(t, kvDB, indexSpan, numRows)

			// Split the last range of primary index.
			splitLastRangeInSpan(ctx, t, kvDB, tableSpan)
			require.True(t, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))

			// Split Index at the median row.
			if _, err := sqlDB.Exec(fmt.Sprintf("ALTER INDEX t.test1@foo SPLIT AT VALUES (%v)", numRows/2)); err != nil {
				t.Fatal(err)
			}
			require.True(t, hasManuallySplitRangesOnIndex(ctx, t, kvDB, tableSpan, idx.GetID()))

			if _, err := sqlDB.Exec(testCase.query); err != nil {
				t.Fatal(err)
			}
			// Push a new zone config for a few tables with TTL=0 so the data
			// is deleted immediately.
			if _, err := sqltestutils.AddImmediateGCZoneConfig(sqlDB, tableDesc.GetID()); err != nil {
				t.Fatal(err)
			}

			// Check GC worked!
			testutils.SucceedsSoon(t, func() error {
				if kvs, err := kvDB.Scan(context.Background(), indexSpan.Key, indexSpan.EndKey, 0); err != nil {
					return err
				} else if len(kvs) != 0 {
					return errors.New("table not truncated")
				}
				return nil
			})
			tests.CheckKeyCount(t, kvDB, tableSpan, numRows*2)
			tests.CheckKeyCount(t, kvDB, indexSpan, 0)

			// Verify there are still manually split ranges on the table, but ranges
			// of the dropped index are unsplit.
			require.True(t, hasManuallySplitRangesInSpan(ctx, t, kvDB, tableSpan))
			require.False(t, hasManuallySplitRangesOnIndex(ctx, t, kvDB, tableSpan, idx.GetID()))
		})
	}
}
