// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestValidateUniqueConstraints tests that the builtin function
// crdb_internal.validate_unique_constraints() can find unique constraint
// violations.
func TestValidateUniqueConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	r := sqlutils.MakeSQLRunner(db)

	// Create two tables and a view.
	_, err := db.Exec(`
CREATE DATABASE test; USE test;
SET experimental_enable_unique_without_index_constraints = true;
SET experimental_enable_implicit_column_partitioning = true;
CREATE TABLE t (k INT PRIMARY KEY, v INT UNIQUE WITHOUT INDEX);
CREATE VIEW v AS SELECT k, v FROM t;
CREATE TABLE u (
  k INT PRIMARY KEY,
  v INT UNIQUE,
  partition_by INT
) PARTITION ALL BY LIST (partition_by) (
  PARTITION one VALUES IN (1),
  PARTITION two VALUES IN (2)
);
`)
	require.NoError(t, err)

	// insertValuesT inserts values into table t, bypassing the SQL layer.
	// This will allow us to insert data that violates unique constraints.
	insertValuesT := func(values []tree.Datum) {
		// Get the table descriptor and primary index of t.
		tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
		primaryIndex := tableDesc.GetPrimaryIndex()

		var colIDtoRowIndex catalog.TableColMap
		colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
		colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

		// Construct the primary index entry to insert.
		primaryIndexEntry, err := rowenc.EncodePrimaryIndex(
			keys.SystemSQLCodec, tableDesc, primaryIndex, colIDtoRowIndex, values, true, /* includeEmpty */
		)
		require.NoError(t, err)
		require.Equal(t, 1, len(primaryIndexEntry))

		// Insert the entry into the index.
		err = kvDB.Put(context.Background(), primaryIndexEntry[0].Key, &primaryIndexEntry[0].Value)
		require.NoError(t, err)
	}

	// insertValuesU inserts values into table u, bypassing the SQL layer.
	// This will allow us to insert data that violates unique constraints.
	insertValuesU := func(values []tree.Datum) {
		// Get the table descriptor and indexes of u.
		tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "u")
		primaryIndex := tableDesc.GetPrimaryIndex()
		secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

		var colIDtoRowIndex catalog.TableColMap
		colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
		colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
		colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)

		// Construct the primary and secondary index entries to insert.
		primaryIndexEntry, err := rowenc.EncodePrimaryIndex(
			keys.SystemSQLCodec, tableDesc, primaryIndex, colIDtoRowIndex, values, true, /* includeEmpty */
		)
		require.NoError(t, err)
		require.Equal(t, 1, len(primaryIndexEntry))
		secondaryIndexEntry, err := rowenc.EncodeSecondaryIndex(
			keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)
		require.NoError(t, err)
		require.Equal(t, 1, len(secondaryIndexEntry))

		// Insert the entries into the indexes.
		err = kvDB.Put(context.Background(), primaryIndexEntry[0].Key, &primaryIndexEntry[0].Value)
		require.NoError(t, err)
		err = kvDB.Put(context.Background(), secondaryIndexEntry[0].Key, &secondaryIndexEntry[0].Value)
		require.NoError(t, err)
	}

	t.Run("validate_unique_without_index", func(t *testing.T) {
		// Insert a couple of rows into table t.
		_, err := db.Exec(`INSERT INTO test.t VALUES (10, 10), (20, 20);`)
		require.NoError(t, err)

		// Verify that we get no validation error.
		r.Exec(t, `SELECT crdb_internal.validate_unique_constraints()`)

		// Insert a row into table t that has a duplicate value for v.
		values := []tree.Datum{tree.NewDInt(30), tree.NewDInt(10)}
		insertValuesT(values)
		r.CheckQueryResults(
			t, `SELECT * FROM test.t;`, [][]string{{"10", "10"}, {"20", "20"}, {"30", "10"}},
		)

		// Verify that we get a validation error.
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.validate_unique_constraints()`)

		// Clean up.
		_, err = db.Exec(`TRUNCATE test.t;`)
		require.NoError(t, err)
	})

	t.Run("validate_implicitly_partitioned_primary_index", func(t *testing.T) {
		// Insert a couple of rows into table u.
		_, err := db.Exec(`INSERT INTO test.u VALUES (10, 10, 1), (20, 20, 1);`)
		require.NoError(t, err)

		// Verify that we get no validation error.
		r.Exec(t, `SELECT crdb_internal.validate_unique_constraints()`)

		// Insert a row into table u that has a duplicate value for k.
		values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(30), tree.NewDInt(2)}
		insertValuesU(values)
		r.CheckQueryResults(
			t, `SELECT * FROM test.u;`, [][]string{{"10", "10", "1"}, {"20", "20", "1"}, {"10", "30", "2"}},
		)

		// Verify that we get an error.
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.validate_unique_constraints()`)

		// Clean up.
		_, err = db.Exec(`TRUNCATE test.u;`)
		require.NoError(t, err)
	})

	t.Run("validate_implicitly_partitioned_secondary_index", func(t *testing.T) {
		// Insert a couple of rows into table u.
		_, err := db.Exec(`INSERT INTO test.u VALUES (10, 10, 1), (20, 20, 1);`)
		require.NoError(t, err)

		// Verify that we get no validation error.
		r.Exec(t, `SELECT crdb_internal.validate_unique_constraints()`)

		// Insert a row into table u that has a duplicate value for v.
		values := []tree.Datum{tree.NewDInt(30), tree.NewDInt(10), tree.NewDInt(2)}
		insertValuesU(values)
		r.CheckQueryResults(
			t, `SELECT * FROM test.u;`, [][]string{{"10", "10", "1"}, {"20", "20", "1"}, {"30", "10", "2"}},
		)

		// Verify that we get an error.
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.validate_unique_constraints()`)

		// Clean up.
		_, err = db.Exec(`TRUNCATE test.u;`)
		require.NoError(t, err)
	})

}
