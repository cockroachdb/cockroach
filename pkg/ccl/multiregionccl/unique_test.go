// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestValidateUniqueConstraints tests that the builtin functions
// crdb_internal.revalidate_unique_constraint* can find unique constraint
// violations.
func TestValidateUniqueConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test fails when run within a tenant. More investigation is
	// required. Tracked with #76378.
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{DefaultTestTenant: base.TODOTestTenantDisabled})
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
		tableDesc := desctestutils.TestingGetTableDescriptor(
			kvDB, keys.SystemSQLCodec, "test", "public", "t",
		)
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
		tableDesc := desctestutils.TestingGetTableDescriptor(
			kvDB, keys.SystemSQLCodec, "test", "public", "u",
		)
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
			context.Background(), keys.SystemSQLCodec, tableDesc, secondaryIndex,
			colIDtoRowIndex, values, true, /* includeEmpty */
		)
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
		_, err := db.Exec(`INSERT INTO test.t VALUES (10, 10), (20, 20)`)
		require.NoError(t, err)

		// Verify that we get no validation error.
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_all_tables()`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('t')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('u')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 't_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 'unique_v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_v_key')`)

		// Insert a row into table t that has a duplicate value for v.
		values := []tree.Datum{tree.NewDInt(30), tree.NewDInt(10)}
		insertValuesT(values)
		r.CheckQueryResults(
			t, `SELECT * FROM test.t`, [][]string{{"10", "10"}, {"20", "20"}, {"30", "10"}},
		)

		// Verify that we get a validation error for table t, constraint unique_v.
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraints_in_all_tables()`)
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraints_in_table('t')`)
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraint('t', 'unique_v')`)

		// The other tables should not err.
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('u')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 't_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_v_key')`)

		// Clean up.
		_, err = db.Exec(`TRUNCATE test.t`)
		require.NoError(t, err)
	})

	t.Run("validate_implicitly_partitioned_primary_index", func(t *testing.T) {
		// Insert a couple of rows into table u.
		_, err := db.Exec(`INSERT INTO test.u VALUES (10, 10, 1), (20, 20, 1)`)
		require.NoError(t, err)

		// Verify that we get no validation error.
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_all_tables()`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('t')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('u')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 't_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 'unique_v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_v_key')`)

		// Insert a row into table u that has a duplicate value for k.
		values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(30), tree.NewDInt(2)}
		insertValuesU(values)
		r.CheckQueryResults(
			t, `SELECT * FROM test.u`, [][]string{{"10", "10", "1"}, {"20", "20", "1"}, {"10", "30", "2"}},
		)

		// Verify that we get an error for table u, constraint u_pkey.
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraints_in_all_tables()`)
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraints_in_table('u')`)
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraint('u', 'u_pkey')`)

		// The other tables should not err.
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('t')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 't_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 'unique_v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_v_key')`)

		// Clean up.
		_, err = db.Exec(`TRUNCATE test.u`)
		require.NoError(t, err)
	})

	t.Run("validate_implicitly_partitioned_secondary_index", func(t *testing.T) {
		// Insert a couple of rows into table u.
		_, err := db.Exec(`INSERT INTO test.u VALUES (100, 100, 1), (200, 200, 1)`)
		require.NoError(t, err)

		// Verify that we get no validation error.
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_all_tables()`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('t')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('u')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 't_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 'unique_v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_v_key')`)

		// Insert a row into table u that has a duplicate value for v.
		values := []tree.Datum{tree.NewDInt(300), tree.NewDInt(100), tree.NewDInt(2)}
		insertValuesU(values)
		r.CheckQueryResults(
			t, `SELECT * FROM test.u`, [][]string{{"100", "100", "1"}, {"200", "200", "1"}, {"300", "100", "2"}},
		)

		// Verify that we get an error for table u, constraint u_v_key.
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraints_in_all_tables()`)
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraints_in_table('u')`)
		r.ExpectErr(t, `failed to validate unique constraint`,
			`SELECT crdb_internal.revalidate_unique_constraint('u', 'u_v_key')`)

		// The other tables should not err.
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('t')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraints_in_table('v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 't_pkey')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('t', 'unique_v')`)
		r.Exec(t, `SELECT crdb_internal.revalidate_unique_constraint('u', 'u_pkey')`)

		// Clean up.
		_, err = db.Exec(`TRUNCATE test.u`)
		require.NoError(t, err)
	})

}
