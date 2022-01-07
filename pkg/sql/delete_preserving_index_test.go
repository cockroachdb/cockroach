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
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/rowencpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/startupmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
	startBackfill := make(chan bool)
	atBackfillStage := make(chan bool)
	errorChan := make(chan error, 1)

	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			RunBeforeIndexBackfill: func() {
				// Wait until we get a signal to begin backfill.
				atBackfillStage <- true
				<-startBackfill
			},
		},
		// Disable backfill migrations, we still need the jobs table migration.
		StartupMigrationManager: &startupmigrations.MigrationManagerTestingKnobs{
			DisableBackfillMigrations: true,
		},
	}

	server, sqlDB, kvDB := serverutils.StartServer(t, params)
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

		<-atBackfillStage
		// Find the descriptors for the indices.
		codec := keys.SystemSQLCodec
		tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(kvDB, codec, "d", "t")
		var index *descpb.IndexDescriptor
		var ord int
		for idx, i := range tableDesc.Mutations {
			if i.GetIndex() != nil {
				index = i.GetIndex()
				ord = idx
			}
		}

		if index == nil {
			return nil, nil, errors.Newf("Could not find index mutation")
		}

		if deletePreservingEncoding {
			// Mutate index descriptor to use the delete-preserving encoding.
			index.UseDeletePreservingEncoding = true
			tableDesc.Mutations[ord].Descriptor_ = &descpb.DescriptorMutation_Index{Index: index}

			if err := kvDB.Put(
				context.Background(),
				catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
				tableDesc.DescriptorProto(),
			); err != nil {
				return nil, nil, err
			}
		}

		// Make some transactions.
		now := kvDB.Clock().Now()
		if _, err := sqlDB.Exec(dataSQL); err != nil {
			return nil, nil, err
		}
		end := kvDB.Clock().Now()

		startBackfill <- true
		finishedSchemaChange.Wait()
		if err := <-errorChan; err != nil {
			t.Fatalf("Schema change encountered an error: %s", err)
		}

		// Grab the revision histories for both indices.
		prefix := rowenc.MakeIndexKeyPrefix(keys.SystemSQLCodec, tableDesc.GetID(), index.ID)
		prefixEnd := append(prefix, []byte("\xff")...)

		revisions, err := kvclient.GetAllRevisions(context.Background(), kvDB, prefix, prefixEnd, now, end)
		if err != nil {
			return nil, nil, err
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
