// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestIndexInterface(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This server is only used to turn a CREATE TABLE statement into a
	// catalog.TableDescriptor.
	s, conn, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	if _, err := conn.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatalf("%+v", err)
	}
	sqlutils.MakeSQLRunner(conn).Exec(t, `
		SET experimental_enable_hash_sharded_indexes = on;
		CREATE TABLE d.t (
			c1 INT,
			c2 INT,
			c3 INT,
			c4 VARCHAR,
			c5 VARCHAR,
			c6 JSONB,
			c7 GEOGRAPHY(GEOMETRY,4326) NULL,
			CONSTRAINT pk PRIMARY KEY (c1 ASC, c2 ASC, c3 ASC),
			INDEX s1 (c4 DESC, c5 DESC),
			INVERTED INDEX s2 (c6),
			INDEX s3 (c2, c3) STORING (c5, c6),
			INDEX s4 (c5) USING HASH WITH BUCKET_COUNT=8,
			UNIQUE INDEX s5 (c1, c4) WHERE c4 = 'x',
			INVERTED INDEX s6 (c7) WITH (s2_level_mod=2)
		);
	`)

	indexNames := []string{"pk", "s1", "s2", "s3", "s4", "s5", "s6"}
	indexColumns := [][]string{
		{"c1", "c2", "c3"},
		{"c4", "c5"},
		{"c6"},
		{"c2", "c3"},
		{"crdb_internal_c5_shard_8", "c5"},
		{"c1", "c4"},
		{"c7"},
	}
	extraColumnsAsPkColOrdinals := [][]int{
		{},
		{0, 1, 2},
		{0, 1, 2},
		{0},
		{0, 1, 2},
		{1, 2},
		{0, 1, 2},
	}

	immutable := catalogkv.TestingGetImmutableTableDescriptor(db, keys.SystemSQLCodec, "d", "t")
	require.NotNil(t, immutable)
	var tableI catalog.TableDescriptor = immutable
	require.NotNil(t, tableI)

	// Find indexes by name, check that names are correct and that indexes
	// are in the correct order.
	indexes := make([]catalog.Index, len(indexNames))
	for i, name := range indexNames {
		idx, err := tableI.FindIndexWithName(name)
		require.NoError(t, err)
		require.Equal(t, name, idx.GetName())
		require.Equal(t, i, idx.Ordinal())
		indexes[i] = idx
	}

	pk := indexes[0]
	s1 := indexes[1]
	s2 := indexes[2]
	s3 := indexes[3]
	s4 := indexes[4]
	s5 := indexes[5]
	s6 := indexes[6]

	// Check that GetPrimaryIndex returns the primary index.
	require.Equal(t, pk, tableI.GetPrimaryIndex())
	require.Equal(t, pk.GetID(), tableI.GetPrimaryIndexID())
	require.True(t, pk.Primary())
	require.True(t, pk.Public())
	require.Equal(t, descpb.PrimaryIndexEncoding, pk.GetEncodingType())

	// Check that ActiveIndexes returns the same indexes in the same order.
	require.Equal(t, indexes, tableI.ActiveIndexes())

	// Check that PublicNonPrimaryIndexes returns the same indexes sans primary.
	require.Equal(t, indexes[1:], tableI.PublicNonPrimaryIndexes())
	for _, idx := range tableI.PublicNonPrimaryIndexes() {
		require.False(t, idx.Primary())
		require.True(t, idx.Public())
		require.Equal(t, descpb.SecondaryIndexEncoding, idx.GetEncodingType())
	}

	// Check that ForEachActiveIndex visits indexes in the same order as well.
	{
		expectedOrdinal := 0
		err := catalog.ForEachActiveIndex(tableI, func(idx catalog.Index) error {
			if idx.Ordinal() != expectedOrdinal {
				return fmt.Errorf("expected ordinal %d for index %s, instead got %d",
					expectedOrdinal, idx.GetName(), idx.Ordinal())
			}
			expectedOrdinal++
			return nil
		})
		require.NoError(t, err)
	}

	// Check various catalog.ForEach* and catalog.Find* functions.
	for _, s := range []struct {
		name               string
		expectedIndexNames []string
		sliceFunc          func(catalog.TableDescriptor) []catalog.Index
		forEachFunc        func(catalog.TableDescriptor, func(catalog.Index) error) error
		findFunc           func(catalog.TableDescriptor, func(catalog.Index) bool) catalog.Index
	}{
		{
			"ActiveIndex",
			indexNames,
			catalog.TableDescriptor.ActiveIndexes,
			catalog.ForEachActiveIndex,
			catalog.FindActiveIndex,
		},
		{
			"NonDropIndex",
			indexNames,
			catalog.TableDescriptor.NonDropIndexes,
			catalog.ForEachNonDropIndex,
			catalog.FindNonDropIndex,
		},
		{
			"PartialIndex",
			[]string{"s5"},
			catalog.TableDescriptor.PartialIndexes,
			catalog.ForEachPartialIndex,
			catalog.FindPartialIndex,
		},
		{
			"PublicNonPrimaryIndex",
			indexNames[1:],
			catalog.TableDescriptor.PublicNonPrimaryIndexes,
			catalog.ForEachPublicNonPrimaryIndex,
			catalog.FindPublicNonPrimaryIndex,
		},
		{
			"WritableNonPrimaryIndex",
			indexNames[1:],
			catalog.TableDescriptor.WritableNonPrimaryIndexes,
			catalog.ForEachWritableNonPrimaryIndex,
			catalog.FindWritableNonPrimaryIndex,
		},
		{
			"DeletableNonPrimaryIndex",
			indexNames[1:],
			catalog.TableDescriptor.DeletableNonPrimaryIndexes,
			catalog.ForEachDeletableNonPrimaryIndex,
			catalog.FindDeletableNonPrimaryIndex,
		},
		{
			"DeleteOnlyNonPrimaryIndex",
			[]string{},
			catalog.TableDescriptor.DeleteOnlyNonPrimaryIndexes,
			catalog.ForEachDeleteOnlyNonPrimaryIndex,
			catalog.FindDeleteOnlyNonPrimaryIndex,
		},
	} {
		expected := s.sliceFunc(tableI)
		var actual []catalog.Index
		err := s.forEachFunc(tableI, func(index catalog.Index) error {
			actual = append(actual, index)
			return nil
		})
		require.NoErrorf(t, err, "Unexpected error from ForEach%s.", s.name)
		require.Equalf(t, expected, actual, "Unexpected results from ForEach%s.", s.name)
		actualNames := make([]string, len(actual))
		for i, idx := range actual {
			actualNames[i] = idx.GetName()
		}
		require.Equalf(t, s.expectedIndexNames, actualNames, "Unexpected results from %ses.", s.name)
		for _, expectedIndex := range expected {
			foundIndex := s.findFunc(tableI, func(index catalog.Index) bool {
				return index == expectedIndex
			})
			require.Equalf(t, foundIndex, expectedIndex, "Unexpected results from Find%s.", s.name)
		}
	}

	// Check that finding indexes by ID is correct.
	for _, idx := range indexes {
		found, err := tableI.FindIndexWithID(idx.GetID())
		require.NoError(t, err)
		require.Equalf(t, idx.GetID(), found.GetID(),
			"mismatched IDs for index '%s'", idx.GetName())
	}

	// Check index metadata.
	for _, idx := range indexes {
		require.False(t, idx.WriteAndDeleteOnly())
		require.False(t, idx.DeleteOnly())
		require.False(t, idx.Adding())
		require.False(t, idx.Dropped())
	}

	errMsgFmt := "Unexpected %s result for index '%s'."

	// Check index methods on features not tested here.
	for _, idx := range indexes {
		require.False(t, idx.IsInterleaved(),
			errMsgFmt, "IsInterleaved", idx.GetName())
		require.False(t, idx.IsDisabled(),
			errMsgFmt, "IsDisabled", idx.GetName())
		require.False(t, idx.IsCreatedExplicitly(),
			errMsgFmt, "IsCreatedExplicitly", idx.GetName())
		require.Equal(t, descpb.IndexDescriptorVersion(0x2), idx.GetVersion(),
			errMsgFmt, "GetVersion", idx.GetName())
		require.Equal(t, descpb.PartitioningDescriptor{}, idx.GetPartitioning(),
			errMsgFmt, "GetPartitioning", idx.GetName())
		require.Equal(t, []string(nil), idx.PartitionNames(),
			errMsgFmt, "PartitionNames", idx.GetName())
		require.Equal(t, 0, idx.NumInterleaveAncestors(),
			errMsgFmt, "NumInterleaveAncestors", idx.GetName())
		require.Equal(t, 0, idx.NumInterleavedBy(),
			errMsgFmt, "NumInterleavedBy", idx.GetName())
		require.False(t, idx.HasOldStoredColumns(),
			errMsgFmt, "HasOldStoredColumns", idx.GetName())
		require.Equalf(t, 0, idx.NumCompositeColumns(),
			errMsgFmt, "NumCompositeColumns", idx.GetName())
	}

	// Check particular index features.
	require.Equal(t, "c4 = 'x':::STRING", s5.GetPredicate())
	require.Equal(t, "crdb_internal_c5_shard_8", s4.GetShardColumnName())
	require.Equal(t, int32(2), s6.GetGeoConfig().S2Geography.S2Config.LevelMod)
	for _, idx := range indexes {
		require.Equalf(t, idx == s5, idx.IsPartial(),
			errMsgFmt, "IsPartial", idx.GetName())
		require.Equal(t, idx == s5, idx.GetPredicate() != "",
			errMsgFmt, "GetPredicate", idx.GetName())
		require.Equal(t, idx == s5 || idx == pk, idx.IsUnique(),
			errMsgFmt, "IsUnique", idx.GetName())
		require.Equal(t, idx == s2 || idx == s6, idx.GetType() == descpb.IndexDescriptor_INVERTED,
			errMsgFmt, "GetType", idx.GetName())
		require.Equal(t, idx == s4, idx.IsSharded(),
			errMsgFmt, "IsSharded", idx.GetName())
		require.Equal(t, idx == s6, !(&geoindex.Config{}).Equal(idx.GetGeoConfig()),
			errMsgFmt, "GetGeoConfig", idx.GetName())
		require.Equal(t, idx == s4, idx.GetShardColumnName() != "",
			errMsgFmt, "GetShardColumnName", idx.GetName())
		require.Equal(t, idx == s4, !(&descpb.ShardedDescriptor{}).Equal(idx.GetSharded()),
			errMsgFmt, "GetSharded", idx.GetName())
		require.Equalf(t, idx != s3, idx.NumStoredColumns() == 0,
			errMsgFmt, "NumStoredColumns", idx.GetName())
	}

	// Check index columns.
	for i, idx := range indexes {
		expectedColNames := indexColumns[i]
		actualColNames := make([]string, idx.NumColumns())
		for j := range actualColNames {
			actualColNames[j] = idx.GetColumnName(j)
			require.Equalf(t, idx == s1, idx.GetColumnDirection(j) == descpb.IndexDescriptor_DESC,
				"mismatched column directions for index '%s'", idx.GetName())
			require.True(t, idx.ContainsColumnID(idx.GetColumnID(j)),
				"column ID resolution failure for column '%s' in index '%s'", idx.GetColumnName(j), idx.GetName())
		}
		require.Equalf(t, expectedColNames, actualColNames,
			"mismatched columns for index '%s'", idx.GetName())
	}

	// Check index extra columns.
	for i, idx := range indexes {
		expectedExtraColIDs := make([]descpb.ColumnID, len(extraColumnsAsPkColOrdinals[i]))
		for j, pkColOrdinal := range extraColumnsAsPkColOrdinals[i] {
			expectedExtraColIDs[j] = pk.GetColumnID(pkColOrdinal)
		}
		actualExtraColIDs := make([]descpb.ColumnID, idx.NumExtraColumns())
		for j := range actualExtraColIDs {
			actualExtraColIDs[j] = idx.GetExtraColumnID(j)
		}
		require.Equalf(t, expectedExtraColIDs, actualExtraColIDs,
			"mismatched extra columns for index '%s'", idx.GetName())
	}

	// Check particular index column features.
	require.Equal(t, "c6", s2.InvertedColumnName())
	require.Equal(t, s2.GetColumnID(0), s2.InvertedColumnID())
	require.Equal(t, "c7", s6.InvertedColumnName())
	require.Equal(t, s6.GetColumnID(0), s6.InvertedColumnID())
	require.Equal(t, 2, s3.NumStoredColumns())
	require.Equal(t, "c5", s3.GetStoredColumnName(0))
	require.Equal(t, "c6", s3.GetStoredColumnName(1))
}
