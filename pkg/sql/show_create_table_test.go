// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestShowCreateTableWithConstraintInvalidated(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	s0 := s.ApplicationLayer()

	tdb := sqlutils.MakeSQLRunner(conn)
	tdb.Exec(t, `CREATE DATABASE db`)
	tdb.Exec(t, `USE db`)
	tdb.Exec(t, `CREATE SCHEMA schema`)
	tdb.Exec(t, `CREATE TABLE db.schema.table(x INT, y INT, INDEX(y) USING HASH)`)

	ief := s0.InternalDB().(descs.DB)
	require.NoError(
		t,
		ief.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			tn := tree.MakeTableNameWithSchema("db", "schema", "table")
			_, mut, err := descs.PrefixAndMutableTable(ctx, txn.Descriptors().MutableByName(txn.KV()), &tn)
			require.NoError(t, err)
			require.NotNil(t, mut)

			// Check the show create table res before we invalidate the constraint.
			// The check constraint from the hash shared index should not appear.
			rows, err := txn.QueryRowEx(
				ctx,
				"show-create-table-before-invalidate-constraint",
				txn.KV(),
				sessiondata.NoSessionDataOverride,
				`SHOW CREATE TABLE db.schema.table`,
			)

			require.NoError(t, err)
			require.Equal(
				t,
				`e'CREATE TABLE schema."table" (`+
					`\n\tx INT8 NULL,`+
					`\n\ty INT8 NULL,`+
					`\n\tcrdb_internal_y_shard_16 INT8 NOT VISIBLE NOT NULL AS (`+
					`mod(fnv32(md5(crdb_internal.datums_to_bytes(y))), 16:::INT8)) VIRTUAL,`+
					`\n\trowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),`+
					`\n\tCONSTRAINT table_pkey PRIMARY KEY (rowid ASC),`+
					`\n\tINDEX table_y_idx (y ASC) USING HASH WITH (bucket_count=16)`+
					`\n)'`,
				fmt.Sprintf("%v", rows[1].String()),
			)

			// Change the check constraint from the hash shared index to unvalidated.
			for _, c := range mut.CheckConstraints() {
				if c.GetName() == "check_crdb_internal_y_shard_16" {
					c.CheckDesc().Validity = descpb.ConstraintValidity_Unvalidated
				}
			}

			require.NoError(t, txn.Descriptors().WriteDesc(
				ctx, true /* kvTrace */, mut, txn.KV(),
			))

			// Check the show create table res after we invalidate the constraint.
			// The constraint should appear now.
			rows, err = txn.QueryRowEx(
				ctx,
				"show-create-table-after-invalidate-constraint",
				txn.KV(),
				sessiondata.NoSessionDataOverride,
				`SHOW CREATE TABLE db.schema.table`,
			)

			require.NoError(t, err)
			require.Equal(
				t,
				`e'CREATE TABLE schema."table" (`+
					`\n\tx INT8 NULL,`+
					`\n\ty INT8 NULL,`+
					`\n\tcrdb_internal_y_shard_16 INT8 NOT VISIBLE NOT NULL AS (mod(fnv32(md5(crdb_internal.datums_to_bytes(y))), 16:::INT8)) VIRTUAL,`+
					`\n\trowid INT8 NOT VISIBLE NOT NULL DEFAULT unique_rowid(),`+
					`\n\tCONSTRAINT table_pkey PRIMARY KEY (rowid ASC),`+
					`\n\tINDEX table_y_idx (y ASC) USING HASH WITH (bucket_count=16),`+
					`\n\tCONSTRAINT check_crdb_internal_y_shard_16 CHECK (`+
					`crdb_internal_y_shard_16 IN (0:::INT8, 1:::INT8, 2:::INT8, 3:::INT8, `+
					`4:::INT8, 5:::INT8, 6:::INT8, 7:::INT8, 8:::INT8, 9:::INT8, 10:::INT8, `+
					`11:::INT8, 12:::INT8, 13:::INT8, 14:::INT8, 15:::INT8)) NOT VALID`+
					`\n)'`, fmt.Sprintf("%v", rows[1].String()))
			return nil
		}))

}
