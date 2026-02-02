// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwriter

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func newTxnWriter(t *testing.T, s serverutils.ApplicationLayerInterface) TransactionWriter {
	writer, err := NewTransactionWriter(
		context.Background(),
		s.InternalDB().(isql.DB),
		s.LeaseManager().(*lease.Manager),
		s.ClusterSettings(),
	)
	require.NoError(t, err)
	return writer
}

func TestTransactionWriter_Smoketest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			id INT PRIMARY KEY,
			name STRING
		)
	`)

	writer := newTxnWriter(t, s)
	defer writer.Close(ctx)

	sqlDB.Exec(t, `
		INSERT INTO test_table (id, name) VALUES (1, 'old-value'), (2, 'delete-me')
	`)

	var descID descpb.ID
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_table'`).Scan(&descID)

	originTime := s.Clock().Now()

	batch := []ldrdecoder.Transaction{
		{
			Timestamp: s.Clock().Now(),
			WriteSet: []ldrdecoder.DecodedRow{{
				Row:              tree.Datums{tree.NewDInt(1), tree.NewDString("new-value")},
				PrevRow:          tree.Datums{tree.NewDInt(1), tree.NewDString("old-value")},
				PrevRowTimestamp: originTime,
				IsDelete:         false,
				TableID:          descID,
			}, {
				Row:              tree.Datums{tree.NewDInt(2), tree.NewDString("delete-me")},
				PrevRow:          tree.Datums{tree.NewDInt(2), tree.NewDString("delete-me")},
				PrevRowTimestamp: originTime,
				IsDelete:         true,
				TableID:          descID,
			}, {
				Row:              tree.Datums{tree.NewDInt(3), tree.NewDString("inserted-value")},
				PrevRow:          nil,
				PrevRowTimestamp: originTime,
				IsDelete:         false,
				TableID:          descID,
			}},
		},
	}

	results, err := writer.ApplyBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 3, results[0].AppliedRows)

	sqlDB.CheckQueryResults(t, `SELECT id, name, crdb_internal_origin_timestamp IS NULL as is_remote FROM test_table ORDER BY id`, [][]string{
		{"1", "new-value", "false"},
		{"3", "inserted-value", "false"},
	})
}

func TestTransactionWriter_UniqueConstraintUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create table with UUID primary key and unique int column
	sqlDB.Exec(t, `
		CREATE TABLE test_table (
			uuid UUID PRIMARY KEY,
			unique_value INT UNIQUE
		)
	`)

	writer := newTxnWriter(t, s)
	defer writer.Close(ctx)

	var descID descpb.ID
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_table'`).Scan(&descID)

	// Generate two random UUIDs for the test
	var uuid1, uuid2 string
	sqlDB.QueryRow(t, `SELECT gen_random_uuid()::STRING`).Scan(&uuid1)
	sqlDB.QueryRow(t, `SELECT gen_random_uuid()::STRING`).Scan(&uuid2)

	uuid1Datum, err := tree.ParseDUuidFromString(uuid1)
	require.NoError(t, err)
	uuid2Datum, err := tree.ParseDUuidFromString(uuid2)
	require.NoError(t, err)

	txn1Time := s.Clock().Now()
	txn2Time := s.Clock().Now()

	// First transaction: insert a row with unique_value = 1337
	// Second transaction: delete that row and insert a different row with unique_value = 1337
	// The random UUIDs ensure non-deterministic primary key order
	batch := []ldrdecoder.Transaction{
		{
			Timestamp: txn1Time,
			WriteSet: []ldrdecoder.DecodedRow{{
				Row:              tree.Datums{uuid1Datum, tree.NewDInt(1337)},
				PrevRow:          nil,
				PrevRowTimestamp: txn1Time,
				IsDelete:         false,
				TableID:          descID,
			}},
		},
		{
			Timestamp: txn2Time,
			WriteSet: []ldrdecoder.DecodedRow{
				{
					PrevRow:          tree.Datums{uuid1Datum, tree.NewDInt(1337)},
					Row:              tree.Datums{uuid1Datum, tree.DNull},
					PrevRowTimestamp: txn1Time,
					IsDelete:         true,
					TableID:          descID,
				},
				{
					PrevRow:  nil,
					Row:      tree.Datums{uuid2Datum, tree.NewDInt(1337)},
					IsDelete: false,
					TableID:  descID,
				},
			},
		},
	}

	results, err := writer.ApplyBatch(ctx, batch)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, 1, results[0].AppliedRows)
	require.Equal(t, 2, results[1].AppliedRows)

	// Verify the final state: only the row with uuid2 should exist
	sqlDB.CheckQueryResults(t, `SELECT unique_value FROM test_table ORDER BY unique_value`, [][]string{
		{"1337"},
	})

	// Verify we can query by unique_value and get the second UUID
	var resultUUID string
	sqlDB.QueryRow(t, `SELECT uuid::STRING FROM test_table WHERE unique_value = 1337`).Scan(&resultUUID)
	require.Equal(t, uuid2, resultUUID)
}

func TestTransactionWriter(t *testing.T) {
	// Create a simple parent/child schema for testing.
	// Create a single batch to test.
	// Randomly make values stale or not stale.
	// Write the batch.
	// Verify the results.

	// TODO test lww is working for insert/update/delete
	// TODO test unique index conflcit
	// TODO test foreign key insert conflict
	// TODO test foreign key delete conflict
}
