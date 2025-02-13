// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIndexInterface(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This server is only used to turn a CREATE TABLE statement into a
	// catalog.TableDescriptor.
	srv, conn, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	s := srv.ApplicationLayer()
	if _, err := conn.Exec(`CREATE DATABASE d`); err != nil {
		t.Fatalf("%+v", err)
	}
	sqlutils.MakeSQLRunner(conn).Exec(t, `
		CREATE TABLE d.t (
			c1 INT,
			c2 INT,
			c3 INT,
			c4 VARCHAR,
			c5 VARCHAR,
			c6 JSONB,
			c7 GEOGRAPHY(GEOMETRY,4326) NULL,
			c8 VECTOR(3),
			CONSTRAINT pk PRIMARY KEY (c1 ASC, c2 ASC, c3 ASC),
			INDEX s1 (c4 DESC, c5 DESC),
			INVERTED INDEX s2 (c6),
			INDEX s3 (c2, c3) STORING (c5, c6),
			INDEX s4 (c5) USING HASH WITH (bucket_count=8),
			UNIQUE INDEX s5 (c1, c4) WHERE c4 = 'x',
			INVERTED INDEX s6 (c7) WITH (s2_level_mod=2),
			VECTOR INDEX s7 (c8)
		);
	`)

	indexNames := []string{"pk", "s1", "s2", "s3", "s4", "s5", "s6", "s7"}
	indexColumns := [][]string{
		{"c1", "c2", "c3"},
		{"c4", "c5"},
		{"c6"},
		{"c2", "c3"},
		{"crdb_internal_c5_shard_8", "c5"},
		{"c1", "c4"},
		{"c7"},
		{"c8"},
	}
	extraColumnsAsPkColOrdinals := [][]int{
		{},
		{0, 1, 2},
		{0, 1, 2},
		{0},
		{0, 1, 2},
		{1, 2},
		{0, 1, 2},
		{0, 1, 2},
	}

	immutable := desctestutils.TestingGetPublicTableDescriptor(db, s.Codec(), "d", "t")
	require.NotNil(t, immutable)
	var tableI = immutable
	require.NotNil(t, tableI)

	// Find indexes by name, check that names are correct and that indexes
	// are in the correct order.
	indexes := make([]catalog.Index, len(indexNames))
	for i, name := range indexNames {
		idx, err := catalog.MustFindIndexByName(tableI, name)
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
	s7 := indexes[7]

	// Check that GetPrimaryIndex returns the primary index.
	require.Equal(t, pk, tableI.GetPrimaryIndex())
	require.Equal(t, pk.GetID(), tableI.GetPrimaryIndexID())
	require.True(t, pk.Primary())
	require.True(t, pk.Public())
	require.Equal(t, catenumpb.PrimaryIndexEncoding, pk.GetEncodingType())
	require.Equal(t, descpb.PrimaryIndexWithStoredColumnsVersion, pk.GetVersion())

	// Check that ActiveIndexes returns the same indexes in the same order.
	require.Equal(t, indexes, tableI.ActiveIndexes())

	// Check that PublicNonPrimaryIndexes returns the same indexes sans primary.
	require.Equal(t, indexes[1:], tableI.PublicNonPrimaryIndexes())
	for _, idx := range tableI.PublicNonPrimaryIndexes() {
		require.False(t, idx.Primary())
		require.True(t, idx.Public())
		require.Equal(t, catenumpb.SecondaryIndexEncoding, idx.GetEncodingType())
		require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, idx.GetVersion())
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
		{"VectorIndex",
			[]string{"s7"},
			catalog.TableDescriptor.VectorIndexes,
			catalog.ForEachVectorIndex,
			catalog.FindVectorIndex,
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
			"NonPrimaryIndex",
			indexNames[1:],
			catalog.TableDescriptor.NonPrimaryIndexes,
			catalog.ForEachNonPrimaryIndex,
			catalog.FindNonPrimaryIndex,
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
		found, err := catalog.MustFindIndexByID(tableI, idx.GetID())
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
		require.False(t, idx.IsDisabled(),
			errMsgFmt, "IsDisabled", idx.GetName())
		require.False(t, idx.IsCreatedExplicitly(),
			errMsgFmt, "IsCreatedExplicitly", idx.GetName())
		require.False(t, idx.HasOldStoredColumns(),
			errMsgFmt, "HasOldStoredColumns", idx.GetName())
		require.Equalf(t, 0, idx.NumCompositeColumns(),
			errMsgFmt, "NumCompositeColumns", idx.GetName())
	}

	// Check particular index features.
	require.Equal(t, "c4 = 'x':::STRING", s5.GetPredicate())
	require.Equal(t, "crdb_internal_c5_shard_8", s4.GetShardColumnName())
	require.Equal(t, int32(2), s6.GetGeoConfig().S2Geography.S2Config.LevelMod)
	require.Equal(t, int32(3), s7.GetVecConfig().Dims)
	for _, idx := range indexes {
		require.Equalf(t, idx == s5, idx.IsPartial(),
			errMsgFmt, "IsPartial", idx.GetName())
		require.Equal(t, idx == s5, idx.GetPredicate() != "",
			errMsgFmt, "GetPredicate", idx.GetName())
		require.Equal(t, idx == s5 || idx == pk, idx.IsUnique(),
			errMsgFmt, "IsUnique", idx.GetName())
		require.Equal(t, idx == s2 || idx == s6, idx.GetType() == idxtype.INVERTED,
			errMsgFmt, "GetType", idx.GetName())
		require.Equal(t, idx == s4, idx.IsSharded(),
			errMsgFmt, "IsSharded", idx.GetName())
		require.Equal(t, idx == s6, !(&geopb.Config{}).Equal(idx.GetGeoConfig()),
			errMsgFmt, "GetGeoConfig", idx.GetName())
		require.Equal(t, idx == s4, idx.GetShardColumnName() != "",
			errMsgFmt, "GetShardColumnName", idx.GetName())
		require.Equal(t, idx == s4, !(&catpb.ShardedDescriptor{}).Equal(idx.GetSharded()),
			errMsgFmt, "GetSharded", idx.GetName())
		require.Equalf(t, idx != s3, idx.NumSecondaryStoredColumns() == 0,
			errMsgFmt, "NumSecondaryStoredColumns", idx.GetName())
		vecConfig := idx.GetVecConfig()
		require.Equal(t, idx == s7, !(&vecpb.Config{}).Equal(&vecConfig),
			errMsgFmt, "GetVecConfig", idx.GetName())
	}

	// Check index columns.
	for i, idx := range indexes {
		expectedColNames := indexColumns[i]
		actualColNames := make([]string, idx.NumKeyColumns())
		colIDs := idx.CollectKeyColumnIDs()
		colIDs.UnionWith(idx.CollectSecondaryStoredColumnIDs())
		colIDs.UnionWith(idx.CollectKeySuffixColumnIDs())
		for j := range actualColNames {
			actualColNames[j] = idx.GetKeyColumnName(j)
			require.Equalf(t, idx == s1, idx.GetKeyColumnDirection(j) == catenumpb.IndexColumn_DESC,
				"mismatched column directions for index '%s'", idx.GetName())
			require.True(t, colIDs.Contains(idx.GetKeyColumnID(j)),
				"column ID resolution failure for column '%s' in index '%s'", idx.GetKeyColumnName(j), idx.GetName())
		}
		require.Equalf(t, expectedColNames, actualColNames,
			"mismatched columns for index '%s'", idx.GetName())
	}

	// Check index extra columns.
	for i, idx := range indexes {
		expectedExtraColIDs := make([]descpb.ColumnID, len(extraColumnsAsPkColOrdinals[i]))
		for j, pkColOrdinal := range extraColumnsAsPkColOrdinals[i] {
			expectedExtraColIDs[j] = pk.GetKeyColumnID(pkColOrdinal)
		}
		actualExtraColIDs := make([]descpb.ColumnID, idx.NumKeySuffixColumns())
		for j := range actualExtraColIDs {
			actualExtraColIDs[j] = idx.GetKeySuffixColumnID(j)
		}
		require.Equalf(t, expectedExtraColIDs, actualExtraColIDs,
			"mismatched extra columns for index '%s'", idx.GetName())
	}

	// Check particular index column features.
	require.Equal(t, "c6", s2.InvertedColumnName())
	require.Equal(t, s2.GetKeyColumnID(0), s2.InvertedColumnID())
	require.Equal(t, "c7", s6.InvertedColumnName())
	require.Equal(t, s6.GetKeyColumnID(0), s6.InvertedColumnID())
	require.Equal(t, 2, s3.NumSecondaryStoredColumns())
	require.Equal(t, "c5", s3.GetStoredColumnName(0))
	require.Equal(t, "c6", s3.GetStoredColumnName(1))
	require.Equal(t, s7.GetKeyColumnID(0), s7.VectorColumnID())
	require.Equal(t, "c8", s7.VectorColumnName())
}

// TestIndexStrictColumnIDs tests that the index format version value
// descpb.StrictIndexColumnIDGuaranteesVersion can prevent issues stemming from
// redundant column IDs in descpb.IndexDescriptor.
func TestIndexStrictColumnIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create a regular table with a secondary index.
	srv, conn, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	codec := srv.ApplicationLayer().Codec()
	_, err := conn.Exec(`
		CREATE DATABASE d;
		CREATE TABLE d.t (
			c1 INT,
			c2 INT,
			CONSTRAINT pk PRIMARY KEY (c1 ASC),
			INDEX sec (c2 ASC)
		);
	`)
	require.NoError(t, err)

	// Mess with the table descriptor to add redundant columns in the secondary
	// index while still passing validation.
	mut := desctestutils.TestingGetMutableExistingTableDescriptor(db, codec, "d", "t")
	idx := &mut.Indexes[0]
	id := idx.KeyColumnIDs[0]
	name := idx.KeyColumnNames[0]
	idx.Version = descpb.SecondaryIndexFamilyFormatVersion
	idx.StoreColumnIDs = append([]descpb.ColumnID{}, id, id, id, id)
	idx.StoreColumnNames = append([]string{}, name, name, name, name)
	idx.KeySuffixColumnIDs = append([]descpb.ColumnID{}, id, id, id, id)
	mut.Version++
	require.NoError(t, validate.Self(clusterversion.TestingClusterVersion, mut))

	// Store the corrupted table descriptor.
	err = db.Put(
		ctx,
		catalogkeys.MakeDescMetadataKey(codec, mut.GetID()),
		mut.DescriptorProto(),
	)
	require.NoError(t, err)

	// Add a new row.
	_, err = conn.Exec(`
		SET tracing = on,kv;
		INSERT INTO d.t VALUES (0, 0);
		SET tracing = off;
	`)
	require.NoError(t, err)

	// Retrieve KV trace and check for redundant values.
	rows, err := conn.Query(`SELECT message FROM [SHOW KV TRACE FOR SESSION] WHERE message LIKE 'CPut%/Table/%/2% ->%'`)
	require.NoError(t, err)
	defer rows.Close()
	require.True(t, rows.Next())
	var msg string
	err = rows.Scan(&msg)
	require.NoError(t, err)
	var tenantPrefix string
	if srv.TenantController().StartedDefaultTestTenant() {
		tenantPrefix = codec.TenantPrefix().String()
	}
	expected := fmt.Sprintf(`CPut %s/Table/%d/2/0/0/0/0/0/0 -> /BYTES/0x2300030003000300`, tenantPrefix, mut.GetID())
	require.Equal(t, expected, msg)

	// Test that with the strict guarantees, this table descriptor would have been
	// considered invalid.
	idx.Version = descpb.StrictIndexColumnIDGuaranteesVersion
	expected = fmt.Sprintf(`relation "t" (%d): index "sec" has duplicates in KeySuffixColumnIDs: [2 2 2 2]`, mut.GetID())
	require.EqualError(t, validate.Self(clusterversion.TestingClusterVersion, mut), expected)
}

// TestLatestIndexDescriptorVersionValues tests the correct behavior of the
// LatestIndexDescVersion version. The values it returns should reflect those
// used when creating indexes.
//
// TODO(postamar): bump versions to LatestIndexDescriptorVersion in 22.2
// This is not possible until then because of a limitation in 21.2 which
// affects mixed-21.2-22.1-version clusters (issue #78426).
func TestLatestIndexDescriptorVersionValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Create a test cluster that will be used to create all kinds of indexes.
	// We make it hang while finalizing an ALTER PRIMARY KEY to cover the edge
	// case of primary-index-encoded indexes in mutations.
	swapNotification := make(chan struct{})
	waitBeforeContinuing := make(chan struct{})
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforePrimaryKeySwap: func() {
					swapNotification <- struct{}{}
					<-waitBeforeContinuing
				},
			},
		},
	}
	srv, sqlDB, kvDB := serverutils.StartServer(t, args)
	defer srv.Stopper().Stop(ctx)
	codec := srv.ApplicationLayer().Codec()
	tdb := sqlutils.MakeSQLRunner(sqlDB)

	// Test relies on legacy schema changer testing knobs.
	tdb.Exec(t, "SET use_declarative_schema_changer = 'off'")
	// Populate the test cluster with all manner of indexes and index mutations.
	tdb.Exec(t, "CREATE SEQUENCE s")
	tdb.Exec(t, "CREATE MATERIALIZED VIEW v AS SELECT 1 AS e, 2 AS f")
	tdb.Exec(t, "CREATE INDEX vsec ON v (f)")
	tdb.Exec(t, "CREATE TABLE t (a INT NOT NULL PRIMARY KEY, b INT, c INT, d INT NOT NULL, INDEX tsec (b), UNIQUE (c))")
	// Wait for schema changes to complete.
	q := fmt.Sprintf(
		`SELECT count(*) FROM [SHOW JOBS] WHERE job_type = '%s' AND status <> 'succeeded'`,
		jobspb.TypeSchemaChange,
	)
	tdb.CheckQueryResultsRetry(t, q, [][]string{{"0"}})
	// Hang on ALTER PRIMARY KEY finalization.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tdb.Exec(t, "ALTER TABLE t ALTER PRIMARY KEY USING COLUMNS (d)")
		wg.Done()
	}()
	<-swapNotification

	test := func(desc catalog.TableDescriptor) {
		require.Equal(t, catenumpb.PrimaryIndexEncoding, desc.GetPrimaryIndex().GetEncodingType())
		require.Equal(t, descpb.PrimaryIndexWithStoredColumnsVersion, desc.GetPrimaryIndex().GetVersion())
		for _, index := range desc.PublicNonPrimaryIndexes() {
			require.Equal(t, catenumpb.SecondaryIndexEncoding, index.GetEncodingType())
		}
		nonPrimaries := desc.DeletableNonPrimaryIndexes()

		switch desc.GetName() {
		case "t":
			require.Equal(t, 10, len(nonPrimaries))
			for _, np := range nonPrimaries {
				switch np.GetName() {
				case "tsec":
					require.True(t, np.Public())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_c_key":
					require.True(t, np.Public())
					require.True(t, np.IsUnique())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_a_key":
					require.True(t, np.IsMutation())
					require.Equal(t, catenumpb.SecondaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "new_primary_key":
					require.True(t, np.IsMutation())
					require.Equal(t, catenumpb.PrimaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "tsec_rewrite_for_primary_key_change":
					require.True(t, np.IsMutation())
					require.Equal(t, catenumpb.SecondaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_c_key_rewrite_for_primary_key_change":
					require.True(t, np.IsMutation())
					require.True(t, np.IsUnique())
					require.Equal(t, catenumpb.SecondaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_a_crdb_internal_dpe_key":
					// Temporary index for new index based on old primary index (t_a_key)
					require.True(t, np.IsMutation())
					require.Equal(t, catenumpb.SecondaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_b_crdb_internal_dpe_idx":
					// Temporary index for tsec_rewrite_for_primary_key_change
					require.True(t, np.IsMutation())
					require.Equal(t, catenumpb.SecondaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_c_crdb_internal_dpe_key":
					// Temporary index for t_c_key_rewrite_for_primary_key_change
					require.True(t, np.IsMutation())
					require.True(t, np.IsUnique())
					require.Equal(t, catenumpb.SecondaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				case "t_d_crdb_internal_dpe_key":
					// Temporary index for new_primary_key
					require.True(t, np.IsMutation())
					require.Equal(t, catenumpb.PrimaryIndexEncoding, np.GetEncodingType())
					require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())

				default:
					t.Fatalf("unexpected index or index mutation %q", np.GetName())
				}
			}
		case "s":
			require.Empty(t, nonPrimaries)

		case "v":
			require.Equal(t, 1, len(nonPrimaries))
			np := nonPrimaries[0]
			require.False(t, np.IsMutation())
			require.Equal(t, "vsec", np.GetName())
			require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, np.GetVersion())
		}
	}

	// We bypass the usual descriptor retrieval mechanisms because we don't want
	// RunPostDeserializationChanges to run and potentially overwrite index
	// descriptor versions.
	rows := tdb.QueryStr(t, `
		SELECT encode(descriptor, 'hex')
		FROM system.descriptor
		WHERE id IN ('t'::REGCLASS::INT, 's'::REGCLASS::INT, 'v'::REGCLASS::INT)`)
	require.NotEmpty(t, rows)
	for _, row := range rows {
		require.NotEmpty(t, row)
		bytes, err := hex.DecodeString(row[0])
		require.NoError(t, err)
		b, err := descbuilder.FromBytesAndMVCCTimestamp(bytes, hlc.Timestamp{WallTime: 1})
		require.NoError(t, err)
		require.NotNil(t, b)
		require.Equal(t, catalog.Table, b.DescriptorType())
		desc := b.BuildImmutable().(catalog.TableDescriptor)
		test(desc)
	}

	// Test again but with RunPostDeserializationChanges.
	for _, name := range []string{`t`, `s`, `v`} {
		desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "defaultdb", name)
		test(desc)
	}

	// Resume pending statement execution.
	waitBeforeContinuing <- struct{}{}
	wg.Wait()
}

// TestSecKeyLatestIndexDescriptorVersion tests that the
// TableDescriptorBuilder correctly promotes primary and secondary index
// versions. This test also verifies that the store column slices of the primary
// index are correctly populated.
//
// TODO(postamar): promote to LatestIndexDescriptorVersion in 22.2
// This is not possible until then because of a limitation in 21.2 which
// affects mixed-21.2-22.1-version clusters (issue #78426).
func TestSecKeyLatestIndexDescriptorVersion(t *testing.T) {
	oldDesc := descpb.TableDescriptor{
		ID:            2,
		ParentID:      1,
		Name:          "foo",
		FormatVersion: descpb.InterleavedFormatVersion,
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "c1"},
			{ID: 2, Name: "c2"},
			{ID: 3, Name: "c3"},
		},
		Families: []descpb.ColumnFamilyDescriptor{
			{ID: 0, Name: "fam_0", ColumnIDs: []descpb.ColumnID{1, 2, 3}, ColumnNames: []string{"c1", "c2", "c3"}},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID: 1, Name: "foo_pkey", KeyColumnIDs: []descpb.ColumnID{1}, KeyColumnNames: []string{"c1"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			EncodingType:        catenumpb.PrimaryIndexEncoding,
		},
		Indexes: []descpb.IndexDescriptor{
			{ID: 2, Name: "sec", KeyColumnIDs: []descpb.ColumnID{2},
				KeyColumnNames:      []string{"c2"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				StoreColumnNames:    []string{"c2"},
				StoreColumnIDs:      []descpb.ColumnID{2},
				Version:             descpb.EmptyArraysInInvertedIndexesVersion,
				EncodingType:        catenumpb.SecondaryIndexEncoding,
			},
		},
		Mutations: []descpb.DescriptorMutation{
			{
				Descriptor_: &descpb.DescriptorMutation_Index{
					Index: &descpb.IndexDescriptor{
						ID:                  3,
						Name:                "new_idx",
						Unique:              true,
						KeyColumnIDs:        []descpb.ColumnID{3},
						KeyColumnNames:      []string{"c3"},
						KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
						Version:             descpb.EmptyArraysInInvertedIndexesVersion,
						EncodingType:        catenumpb.SecondaryIndexEncoding,
						ConstraintID:        1,
					},
				},
				Direction: descpb.DescriptorMutation_ADD,
				State:     descpb.DescriptorMutation_DELETE_ONLY,
			},
		},
	}

	b := tabledesc.NewBuilder(&oldDesc)
	require.NoError(t, b.RunPostDeserializationChanges())
	newDesc := b.BuildExistingMutable().(*tabledesc.Mutable)

	// The primary index should have its version bumped and its stored column
	// slices properly set.
	require.Equal(t, descpb.PrimaryIndexWithStoredColumnsVersion, newDesc.PrimaryIndex.Version)
	require.Equal(t, []string{"c2", "c3"}, newDesc.PrimaryIndex.StoreColumnNames)
	require.Equal(t, []descpb.ColumnID{2, 3}, newDesc.PrimaryIndex.StoreColumnIDs)
	// The public secondary index should not have its version bumped, because its
	// key and store column sets intersect.
	require.Equal(t, descpb.EmptyArraysInInvertedIndexesVersion, newDesc.Indexes[0].Version)
	// The index mutation should have its version bumped.
	require.Equal(t, descpb.StrictIndexColumnIDGuaranteesVersion, newDesc.Mutations[0].GetIndex().Version)
}
