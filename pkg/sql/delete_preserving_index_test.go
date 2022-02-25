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
	"context"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/rowencpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This tests the delete-preserving index encoding for SQL writes on an index
// mutation by pausing the backfill process and while running SQL transactions.
// The same transactions are ran twice: once on an index with the normal
// encoding, once on an index using the delete-preserving encoding. After the
// transactions, the key value revision log for the delete-preserving encoding
// index is compared against the normal index to make sure the entries match.
func TestDeletePreservingIndexEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	params, _ := tests.CreateTestServerParams()
	mergeFinished := make(chan struct{})
	completeSchemaChange := make(chan struct{})
	errorChan := make(chan error, 1)

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunAfterTempIndexMerge: func() {
				mergeFinished <- struct{}{}
				<-completeSchemaChange
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	_, err := sqlDB.Exec(`
SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer = 'off';
SET use_declarative_schema_changer = 'off';
`)
	require.NoError(t, err)
	defer server.Stopper().Stop(context.Background())

	getRevisionsForTest := func(setupSQL, schemaChangeSQL, dataSQL string, deletePreservingEncoding bool) ([]kvclient.VersionedValues, []byte, error) {
		if _, err := sqlDB.Exec(setupSQL); err != nil {
			t.Fatal(err)
		}

		// Start the schema change but pause right before the backfill.
		var finishedSchemaChange sync.WaitGroup
		finishedSchemaChange.Add(1)
		go func() {
			_, err := sqlDB.Exec(schemaChangeSQL)

			errorChan <- err

			finishedSchemaChange.Done()
		}()
		<-mergeFinished

		// Find the descriptor for the temporary index mutation.
		codec := keys.SystemSQLCodec
		tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, codec, "d", "t")
		var index *descpb.IndexDescriptor
		for _, i := range tableDesc.Mutations {
			if i.GetIndex() != nil && i.GetIndex().UseDeletePreservingEncoding == deletePreservingEncoding {
				index = i.GetIndex()
				break
			}
		}
		if index == nil {
			return nil, nil, errors.Newf("Could not find index mutation")
		}

		// Make some transactions.
		now := kvDB.Clock().Now()
		if _, err := sqlDB.Exec(dataSQL); err != nil {
			return nil, nil, err
		}
		end := kvDB.Clock().Now()

		// Grab the revision histories for the index.
		prefix := rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, tableDesc.GetID(), index.ID)
		prefixEnd := append(prefix, []byte("\xff")...)

		revisions, err := kvclient.GetAllRevisions(context.Background(), kvDB, prefix, prefixEnd, now, end)
		if err != nil {
			return nil, nil, err
		}

		completeSchemaChange <- struct{}{}
		finishedSchemaChange.Wait()
		if err := <-errorChan; err != nil {
			t.Logf("Schema change with delete_preserving=%v encountered an error: %s, continuing...", deletePreservingEncoding, err)
		}

		return revisions, prefix, nil
	}

	resetTestData := func() error {
		if _, err := sqlDB.Exec(`DROP DATABASE IF EXISTS d;`); err != nil {
			return err
		}

		return nil
	}

	testCases := []struct {
		name            string
		setupSQL        string
		schemaChangeSQL string
		dataSQL         string
	}{
		{"secondary_index_encoding_test",
			`CREATE DATABASE d;
					CREATE TABLE d.t (
						k INT NOT NULL PRIMARY KEY,
						a INT NOT NULL,
						b INT
					);`,
			`CREATE INDEX ON d.t (a) STORING (b);`,
			`INSERT INTO d.t (k, a, b) VALUES (1234, 101, 10001), (1235, 102, 10002), (1236, 103, 10003);
		DELETE FROM d.t WHERE k = 1;
		UPDATE d.t SET b = 10004 WHERE k = 2;`,
		},
		{"primary_encoding_test",
			`CREATE DATABASE d;
					CREATE TABLE d.t (
						k INT NOT NULL PRIMARY KEY,
						a INT NOT NULL,
						b INT
					);`,
			`ALTER TABLE d.t ALTER PRIMARY KEY USING COLUMNS (k, a);`,
			`INSERT INTO d.t (k, a, b) VALUES (1234, 101, 10001), (1235, 102, 10002), (1236, 103, 10003);
		DELETE FROM d.t WHERE k = 1;
		UPDATE d.t SET b = 10004 WHERE k = 2;`,
		},
		{"unique_index_test",
			`CREATE DATABASE d;
					CREATE TABLE d.t (
						k INT NOT NULL PRIMARY KEY,
						a INT NOT NULL,
						b INT
					);`,
			`CREATE UNIQUE INDEX ON d.t (a);`,
			`INSERT INTO d.t (k, a, b) VALUES (1234, 101, 10001), (1235, 102, 10002), (1236, 103, 10003);
		DELETE FROM d.t WHERE k = 1234;
	INSERT INTO d.t (k, a, b) VALUES (1237, 101, 10004);`,
		},
		{"primary_encoding_test_same_key",
			`CREATE DATABASE d;
					CREATE TABLE d.t (
						k INT NOT NULL PRIMARY KEY,
						a INT NOT NULL,
						b INT
					);`,
			`ALTER TABLE d.t ALTER PRIMARY KEY USING COLUMNS (k, a);`,
			`INSERT INTO d.t (k, a, b) VALUES (1234, 101, 10001), (1235, 102, 10002), (1236, 103, 10003);
		DELETE FROM d.t WHERE k = 1234;
		INSERT INTO d.t (k, a, b) VALUES (1234, 104, 10004);`,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if err := resetTestData(); err != nil {
				t.Fatalf("error while resetting test data %s", err)
			}
			delEncRevisions, delEncPrefix, err := getRevisionsForTest(test.setupSQL, test.schemaChangeSQL, test.dataSQL, true)
			if err != nil {
				t.Fatalf("error while getting delete encoding revisions %s", err)
			}
			if err := resetTestData(); err != nil {
				t.Fatalf("error while resetting test data %s", err)
			}
			defaultRevisions, defaultPrefix, err := getRevisionsForTest(test.setupSQL, test.schemaChangeSQL, test.dataSQL, false)
			if err != nil {
				t.Fatalf("error while getting default revisions %s", err)
			}

			err = compareRevisionHistories(defaultRevisions, len(defaultPrefix), delEncRevisions, len(delEncPrefix))
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestDeletePreservingIndexEncodingUsesNormalDeletesInDeleteOnly
// tests that deletes from an index in DELETE_ONLY uses actual deletes
// and not preserved deletes.
func TestDeletePreservingIndexEncodingUsesNormalDeletesInDeleteOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())

	setupSQL := `
CREATE DATABASE t;
CREATE TABLE t.test (a INT PRIMARY KEY, b INT);
CREATE UNIQUE INDEX test_index_to_mutate ON t.test (b);
`
	_, err := sqlDB.Exec(setupSQL)
	require.NoError(t, err)
	codec := server.ExecutorConfig().(sql.ExecutorConfig).Codec
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, codec, "t", "test")

	// Move index to DELETE_ONLY. The following delete should not
	// be preserved even though the index sees the delete.
	err = mutateIndexByName(kvDB, codec, tableDesc, "test_index_to_mutate", func(idx *descpb.IndexDescriptor) error {
		idx.UseDeletePreservingEncoding = true
		return nil
	}, descpb.DescriptorMutation_DELETE_ONLY)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`INSERT INTO t.test VALUES (1, 1)`)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`DELETE FROM t.test WHERE a = 1`)
	require.NoError(t, err)

	// Move index to DELETE_AND_WRITE_ONLY. The following inserts
	// are seen by the index and deletes should be preserved.
	err = mutateIndexByName(kvDB, codec, tableDesc, "test_index_to_mutate", nil, descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)
	require.NoError(t, err)

	_, err = sqlDB.Exec(`INSERT INTO t.test VALUES (2, 2), (3, 3)`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`DELETE FROM t.test WHERE a = 2`)
	require.NoError(t, err)

	idx, err := tableDesc.FindIndexWithName("test_index_to_mutate")
	require.NoError(t, err)

	span := tableDesc.IndexSpan(codec, idx.GetID())
	kvs, err := kvDB.Scan(context.Background(), span.Key, span.EndKey, 0)
	require.NoError(t, err)

	// We should have 2 values, 1 deleted value and 1 normal.
	require.Len(t, kvs, 2)
	wrappedValue, err := rowenc.DecodeWrapper(kvs[0].Value)
	require.NoError(t, err)
	require.True(t, wrappedValue.Deleted)
	wrappedValue, err = rowenc.DecodeWrapper(kvs[1].Value)
	require.NoError(t, err)
	require.False(t, wrappedValue.Deleted)
}

// TestDeletePreservingIndexEncodingWithEmptyValues is a regression
// test for a panic when attempting to encode a KV with an empty body.
func TestDeletePreservingIndexEncodingWithEmptyValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer lease.TestingDisableTableLeases()()

	params, _ := tests.CreateTestServerParams()
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.Background())
	setupSQL := `
CREATE DATABASE t;
CREATE TABLE t.test (
        x INT PRIMARY KEY, y INT NOT NULL, z INT, a INT,
        FAMILY (x), FAMILY (y), FAMILY (a), FAMILY (z)
);
CREATE UNIQUE INDEX test_index_to_mutate ON t.test (y) STORING (z, a);
`
	_, err := sqlDB.Exec(setupSQL)
	require.NoError(t, err)
	codec := server.ExecutorConfig().(sql.ExecutorConfig).Codec
	tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, codec, "t", "test")
	err = mutateIndexByName(kvDB, codec, tableDesc, "test_index_to_mutate", func(idx *descpb.IndexDescriptor) error {
		// Here, we make this index look like the temporary
		// index for a new primary index during the
		// MVCC-compatible index backfilling process.
		idx.UseDeletePreservingEncoding = true
		idx.EncodingType = descpb.PrimaryIndexEncoding
		idx.StoreColumnNames = []string{"x", "z", "a"}
		idx.StoreColumnIDs = []catid.ColumnID{0x1, 0x3, 0x4}
		idx.KeySuffixColumnIDs = nil
		return nil
	}, descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO t.test VALUES (1, 1, 1, 1); DELETE FROM t.test WHERE x = 1;`)
	require.NoError(t, err)
}

func mutateIndexByName(
	kvDB *kv.DB,
	codec keys.SQLCodec,
	tableDesc *tabledesc.Mutable,
	index string,
	fn func(*descpb.IndexDescriptor) error,
	state descpb.DescriptorMutation_State,
) error {
	idx, err := tableDesc.FindIndexWithName(index)
	if err != nil {
		return err
	}
	idxCopy := *idx.IndexDesc()
	if fn != nil {
		if err := fn(&idxCopy); err != nil {
			return err
		}
	}

	m := descpb.DescriptorMutation{}
	m.Descriptor_ = &descpb.DescriptorMutation_Index{Index: &idxCopy}
	m.Direction = descpb.DescriptorMutation_ADD
	m.State = state
	if !idx.IsMutation() {
		tableDesc.RemovePublicNonPrimaryIndex(idx.Ordinal())
		tableDesc.Mutations = append(tableDesc.Mutations, m)
	} else {
		var ord int
		for _, m := range tableDesc.AllMutations() {
			if idx := m.AsIndex(); idx != nil && idx.GetID() == idxCopy.ID {
				ord = m.MutationOrdinal()
				break
			}
		}
		tableDesc.Mutations[ord] = m
	}
	tableDesc.Version++
	return kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(codec, tableDesc.ID),
		tableDesc.DescriptorProto(),
	)
}

type WrappedVersionedValues struct {
	Key    roachpb.Key
	Values []rowencpb.IndexValueWrapper
}

func compareRevisionHistories(
	expectedHistory []kvclient.VersionedValues,
	expectedPrefixLength int,
	deletePreservingHistory []kvclient.VersionedValues,
	deletePreservingPrefixLength int,
) error {
	decodedExpected, err := decodeVersionedValues(expectedHistory, false)
	if err != nil {
		return errors.Wrap(err, "error while decoding revision history")
	}

	decodedDeletePreserving, err := decodeVersionedValues(deletePreservingHistory, true)
	if err != nil {
		return errors.Wrap(err, "error while decoding revision history for delete-preserving encoding")
	}

	return compareVersionedValueWrappers(decodedExpected, expectedPrefixLength, decodedDeletePreserving, deletePreservingPrefixLength)
}

func decodeVersionedValues(
	revisions []kvclient.VersionedValues, deletePreserving bool,
) ([]WrappedVersionedValues, error) {
	wrappedVersionedValues := make([]WrappedVersionedValues, len(revisions))

	for i, revision := range revisions {
		wrappedValues := make([]rowencpb.IndexValueWrapper, len(revision.Values))

		for j, value := range revision.Values {
			var wrappedValue *rowencpb.IndexValueWrapper
			var err error

			if deletePreserving {
				wrappedValue, err = rowenc.DecodeWrapper(&value)
				if err != nil {
					return nil, err
				}
			} else {
				if len(value.RawBytes) == 0 {
					wrappedValue = &rowencpb.IndexValueWrapper{
						Value:   nil,
						Deleted: true,
					}
				} else {
					wrappedValue = &rowencpb.IndexValueWrapper{
						Value:   value.TagAndDataBytes(),
						Deleted: false,
					}

				}
			}

			wrappedValues[j] = *wrappedValue
		}

		wrappedVersionedValues[i].Key = revision.Key
		wrappedVersionedValues[i].Values = wrappedValues
	}

	return wrappedVersionedValues, nil
}

func compareVersionedValueWrappers(
	expected []WrappedVersionedValues,
	expectedPrefixLength int,
	actual []WrappedVersionedValues,
	actualPrefixLength int,
) error {
	if len(expected) != len(actual) {
		return errors.Newf("expected %d values, got %d", len(expected), len(actual))
	}

	for idx := range expected {
		expectedVersions := &expected[idx]
		actualVersions := &actual[idx]

		if !reflect.DeepEqual(expectedVersions.Key[expectedPrefixLength:], actualVersions.Key[actualPrefixLength:]) {
			return errors.Newf("at index %d, expected key %s after index %d to equal %s after index %d",
				idx, actualVersions.Key, actualPrefixLength, expectedVersions.Key, expectedPrefixLength)
		}

		if len(expectedVersions.Values) != len(actualVersions.Values) {
			return errors.Newf("expected %d values for key %s, got %d", len(expected), expectedVersions.Key,
				len(actual))
		}

		for versionIdx := range expectedVersions.Values {
			if !reflect.DeepEqual(expectedVersions.Values[versionIdx], actualVersions.Values[versionIdx]) {
				return errors.Newf("expected value %v for key %s entry %d, got %v",
					expectedVersions.Values[versionIdx], expectedVersions.Key, versionIdx, actualVersions.Values[versionIdx])
			}
		}
	}

	return nil
}

// This test tests that the schema changer is able to merge entries from a
// delete-preserving index into a regular index.
func TestMergeProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	defer lease.TestingDisableTableLeases()()

	params, _ := tests.CreateTestServerParams()

	type TestCase struct {
		name                   string
		setupSQL               string
		srcIndex               string
		dstIndex               string
		dstDataSQL             string
		srcDataSQL             string
		dstDataSQL2            string
		dstContentsBeforeMerge [][]string
		dstContentsAfterMerge  [][]string
	}

	testCases := []TestCase{
		{
			name: "unique index",
			setupSQL: `CREATE DATABASE d;
   CREATE TABLE d.t (k INT PRIMARY KEY, a INT, b INT,
		UNIQUE INDEX idx (b),
		UNIQUE INDEX idx_temp (b)
);`,
			srcIndex: "idx_temp",
			dstIndex: "idx",
			// Populate dstIndex with some 1000/1, 2000/2 entries. Populate srcIndex
			// with 3000/3, 4000/4 entries, and a delete for the 2000/2 entry.
			dstDataSQL: `INSERT INTO d.t (k, a, b) VALUES (1, 100, 1000), (2, 200, 2000)`,
			srcDataSQL: `INSERT INTO d.t (k, a, b) VALUES (3, 300, 3000), (4, 400, 4000);
   								 DELETE FROM d.t WHERE k = 2`,
			// Insert another row for the 2000/2 entry just so that there's a primary
			// index row for the index to return when we read it.
			dstDataSQL2: `INSERT INTO d.t (k, a, b) VALUES (2, 201, 2000)`,
			dstContentsBeforeMerge: [][]string{
				{"1", "1000"},
				{"2", "2000"},
			},
			// After merge dstIndex has 1000/1, 3000/3, 4000/4 entries. Its 2000/2 entry was
			// deleted by the srcIndex delete.
			dstContentsAfterMerge: [][]string{
				{"1", "1000"},
				{"3", "3000"},
				{"4", "4000"},
			},
		},
		{
			name: "unique index noop delete",
			setupSQL: `
				CREATE DATABASE d;
				CREATE TABLE d.t (k INT PRIMARY KEY, a INT, b INT,
					UNIQUE INDEX idx (b),
					UNIQUE INDEX idx_temp (b)
  			);`,
			srcIndex: "idx_temp",
			dstIndex: "idx",
			// Populate dstIndex with some 1000/1, 2000/2 entries. Populate srcIndex
			// with 3000/3, 4000/4 entries, and a delete for a nonexistent key.
			dstDataSQL: `INSERT INTO d.t (k, a, b) VALUES (1, 100, 1000), (2, 200, 2000)`,
			srcDataSQL: `INSERT INTO d.t (k, a, b) VALUES (3, 300, 3000), (4, 400, 4000);
   								 DELETE FROM d.t WHERE k = 5`,
			dstContentsBeforeMerge: [][]string{
				{"1", "1000"},
				{"2", "2000"},
			},
			// After merge dstIndex has entries from both indexes.
			dstContentsAfterMerge: [][]string{
				{"1", "1000"},
				{"2", "2000"},
				{"3", "3000"},
				{"4", "4000"},
			},
		},
		{
			name: "index with overriding values",
			setupSQL: `
				CREATE DATABASE d;
   			CREATE TABLE d.t (k INT PRIMARY KEY, a INT, b INT,
					UNIQUE INDEX idx (b),
					UNIQUE INDEX idx_temp (b)
				);`,
			srcIndex:   "idx_temp",
			dstIndex:   "idx",
			dstDataSQL: `INSERT INTO d.t (k, a, b) VALUES (1, 100, 1000), (2, 200, 2000)`,
			srcDataSQL: `INSERT INTO d.t (k, a, b) VALUES (3, 300, 1000), (4, 400, 2000)`,
			dstContentsBeforeMerge: [][]string{
				{"1", "1000"},
				{"2", "2000"},
			},
			// After merge dstIndex should have same entries as srcIndex.
			dstContentsAfterMerge: [][]string{
				{"3", "1000"},
				{"4", "2000"},
			},
		},
	}

	run := func(t *testing.T, test TestCase) {
		server, tdb, kvDB := serverutils.StartServer(t, params)
		defer server.Stopper().Stop(context.Background())

		// Run the initial setupSQL.
		if _, err := tdb.Exec(test.setupSQL); err != nil {
			t.Fatal(err)
		}

		codec := keys.SystemSQLCodec
		tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, codec, "d", "t")
		settings := server.ClusterSettings()
		execCfg := server.ExecutorConfig().(sql.ExecutorConfig)
		evalCtx := tree.EvalContext{Settings: settings}
		//mm := mon.NewMonitor("MemoryMonitor", mon.MemoryResource, nil, nil, 0, math.MaxInt64, settings)
		mm := mon.NewUnlimitedMonitor(ctx, "MemoryMonitor", mon.MemoryResource, nil, nil, math.MaxInt64, settings)
		flowCtx := execinfra.FlowCtx{Cfg: &execinfra.ServerConfig{DB: kvDB,
			Settings:          settings,
			Codec:             codec,
			BackfillerMonitor: mm,
		},
			EvalCtx: &evalCtx}

		im, err := backfill.NewIndexBackfillMerger(ctx, &flowCtx, execinfrapb.IndexBackfillMergerSpec{}, nil)
		if err != nil {
			t.Fatal(err)
		}

		// Here want to have different entries for the two indices, so we manipulate
		// the index to DELETE_ONLY when we don't want to write to it, and
		// DELETE_AND_WRITE_ONLY when we write to it.
		setUseDeletePreservingEncoding := func(b bool) func(*descpb.IndexDescriptor) error {
			return func(idx *descpb.IndexDescriptor) error {
				idx.UseDeletePreservingEncoding = b
				return nil
			}
		}

		err = mutateIndexByName(kvDB, codec, tableDesc, test.dstIndex, nil, descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)
		require.NoError(t, err)
		err = mutateIndexByName(kvDB, codec, tableDesc, test.srcIndex, setUseDeletePreservingEncoding(true), descpb.DescriptorMutation_DELETE_ONLY)
		require.NoError(t, err)

		if _, err := tdb.Exec(test.dstDataSQL); err != nil {
			t.Fatal(err)
		}

		err = mutateIndexByName(kvDB, codec, tableDesc, test.dstIndex, nil, descpb.DescriptorMutation_DELETE_ONLY)
		require.NoError(t, err)
		err = mutateIndexByName(kvDB, codec, tableDesc, test.srcIndex, nil, descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)
		require.NoError(t, err)

		if _, err := tdb.Exec(test.srcDataSQL); err != nil {
			t.Fatal(err)
		}

		err = mutateIndexByName(kvDB, codec, tableDesc, test.dstIndex, nil, descpb.DescriptorMutation_DELETE_AND_WRITE_ONLY)
		require.NoError(t, err)
		err = mutateIndexByName(kvDB, codec, tableDesc, test.srcIndex, nil, descpb.DescriptorMutation_DELETE_ONLY)
		require.NoError(t, err)

		if _, err := tdb.Exec(test.dstDataSQL2); err != nil {
			t.Fatal(err)
		}

		tableDesc = desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, codec, "d", "t")

		dstIndex, err := tableDesc.FindIndexWithName(test.dstIndex)
		if err != nil {
			t.Fatal(err)
		}

		srcIndex, err := tableDesc.FindIndexWithName(test.srcIndex)
		if err != nil {
			t.Fatal(err)
		}

		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			mut, err := descriptors.GetMutableTableByID(ctx, txn, tableDesc.GetID(), tree.ObjectLookupFlags{})
			if err != nil {
				return err
			}

			require.Equal(t, test.dstContentsBeforeMerge,
				datumSliceToStrMatrix(fetchIndex(ctx, t, txn, mut, test.dstIndex)))

			return nil
		}))

		sp := tableDesc.IndexSpan(codec, srcIndex.GetID())
		_, err = im.Merge(context.Background(), codec, tableDesc, srcIndex.GetID(), dstIndex.GetID(), sp.Key, sp.EndKey)
		if err != nil {
			t.Fatal(err)
		}

		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection) error {
			mut, err := descriptors.GetMutableTableByID(ctx, txn, tableDesc.GetID(), tree.ObjectLookupFlags{})
			if err != nil {
				return err
			}

			require.Equal(t, test.dstContentsAfterMerge,
				datumSliceToStrMatrix(fetchIndex(ctx, t, txn, mut, test.dstIndex)))
			return nil
		}))
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			run(t, test)
		})
	}
}

// fetchIndex fetches the contents of an a table index returning the results
// as datums. The datums will correspond to each of the columns stored in the
// index, ordered by column ID.
func fetchIndex(
	ctx context.Context, t *testing.T, txn *kv.Txn, table *tabledesc.Mutable, indexName string,
) []tree.Datums {
	t.Helper()
	var fetcher row.Fetcher
	var alloc tree.DatumAlloc

	mm := mon.MakeStandaloneBudget(1 << 30)
	idx, err := table.FindIndexWithName(indexName)
	require.NoError(t, err)
	colIdxMap := catalog.ColumnIDToOrdinalMap(table.PublicColumns())
	var valsNeeded util.FastIntSet
	{
		colIDsNeeded := idx.CollectKeyColumnIDs()
		if idx.Primary() {
			for _, column := range table.PublicColumns() {
				if !column.IsVirtual() {
					colIDsNeeded.Add(column.GetID())
				}
			}
		} else {
			colIDsNeeded.UnionWith(idx.CollectSecondaryStoredColumnIDs())
			colIDsNeeded.UnionWith(idx.CollectKeySuffixColumnIDs())
		}

		colIDsNeeded.ForEach(func(colID descpb.ColumnID) {
			valsNeeded.Add(colIdxMap.GetDefault(colID))
		})
	}
	var columns []descpb.ColumnID
	for i, col := range table.PublicColumns() {
		if valsNeeded.Contains(i) {
			columns = append(columns, col.GetID())
		}
	}

	var spec descpb.IndexFetchSpec
	require.NoError(t, rowenc.InitIndexFetchSpec(&spec, keys.SystemSQLCodec, table, idx, columns))

	spans := []roachpb.Span{table.IndexSpan(keys.SystemSQLCodec, idx.GetID())}
	const reverse = false
	require.NoError(t, fetcher.Init(
		ctx,
		reverse,
		descpb.ScanLockingStrength_FOR_NONE,
		descpb.ScanLockingWaitPolicy_BLOCK,
		0,
		&alloc,
		mm.Monitor(),
		&spec,
	))

	require.NoError(t, fetcher.StartScan(
		ctx, txn, spans, rowinfra.NoBytesLimit, 0, true, false, /* forceProductionBatchSize */
	))
	var rows []tree.Datums
	for {
		datums, err := fetcher.NextRowDecoded(ctx)
		require.NoError(t, err)
		if datums == nil {
			break
		}
		// Copy the datums out as the slice is reused internally.
		row := append(tree.Datums(nil), datums...)
		rows = append(rows, row)
	}
	return rows
}

func datumSliceToStrMatrix(rows []tree.Datums) [][]string {
	res := make([][]string, len(rows))
	for i, row := range rows {
		rowStrs := make([]string, len(row))
		for j, d := range row {
			rowStrs[j] = d.String()
		}
		res[i] = rowStrs
	}
	return res
}
