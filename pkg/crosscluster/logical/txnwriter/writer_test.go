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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

	batch := []ldrdecoder.Transaction{
		{
			Timestamp: s.Clock().Now(),
			WriteSet: []ldrdecoder.DecodedRow{{
				Row:      tree.Datums{tree.NewDInt(1), tree.NewDString("new-value")},
				PrevRow:  tree.Datums{tree.NewDInt(1), tree.NewDString("old-value")},
				IsDelete: false,
				TableID:  descID,
			}, {
				Row:      tree.Datums{tree.NewDInt(2), tree.NewDString("delete-me")},
				PrevRow:  tree.Datums{tree.NewDInt(2), tree.NewDString("delete-me")},
				IsDelete: true,
				TableID:  descID,
			}, {
				Row:      tree.Datums{tree.NewDInt(3), tree.NewDString("inserted-value")},
				PrevRow:  nil,
				IsDelete: false,
				TableID:  descID,
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

	uuid1Datum := &tree.DUuid{UUID: uuid.NewV4()}
	uuid2Datum := &tree.DUuid{UUID: uuid.NewV4()}

	// First transaction: insert a row with unique_value = 1337 Second
	// transaction: delete that row and insert a different row with unique_value
	// = 1337 The random UUIDs ensure non-deterministic primary key order
	batch := []ldrdecoder.Transaction{
		{
			Timestamp: s.Clock().Now(),
			WriteSet: []ldrdecoder.DecodedRow{{
				Row:      tree.Datums{uuid1Datum, tree.NewDInt(1337)},
				PrevRow:  nil,
				IsDelete: false,
				TableID:  descID,
			}},
		},
		{
			Timestamp: s.Clock().Now(),
			WriteSet: []ldrdecoder.DecodedRow{
				{
					PrevRow:  tree.Datums{uuid1Datum, tree.NewDInt(1337)},
					Row:      tree.Datums{uuid1Datum, tree.DNull},
					IsDelete: true,
					TableID:  descID,
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
	require.Equal(t, uuid2Datum.UUID.String(), resultUUID)
}

func TestTransactionWriter_Dlq(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	writer := newTxnWriter(t, s)
	defer writer.Close(ctx)

	lookupDescID := func(t *testing.T, tableName string) descpb.ID {
		var id descpb.ID
		sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = $1`, tableName).Scan(&id)
		return id
	}

	applyInsert := func(t *testing.T, tableID descpb.ID, row tree.Datums) hlc.Timestamp {
		ts := s.Clock().Now()
		_, err := writer.ApplyBatch(ctx, []ldrdecoder.Transaction{{
			Timestamp: ts,
			WriteSet: []ldrdecoder.DecodedRow{{
				Row: row, PrevRow: nil, TableID: tableID,
			}},
		}})
		require.NoError(t, err)
		return ts
	}

	tests := []struct {
		name          string
		setup         func(t *testing.T)
		buildWriteSet func(t *testing.T) []ldrdecoder.DecodedRow
		errContains   string
	}{
		{
			name: "fk insert",
			setup: func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE fk_ins_parent (id INT PRIMARY KEY)`)
				sqlDB.Exec(t, `CREATE TABLE fk_ins_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_ins_parent(id)
				)`)
			},
			buildWriteSet: func(t *testing.T) []ldrdecoder.DecodedRow {
				return []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(1), tree.NewDInt(999)},
					PrevRow: nil,
					TableID: lookupDescID(t, "fk_ins_child"),
				}}
			},
			errContains: "foreign key",
		},
		{
			name: "fk update",
			setup: func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE fk_upd_parent (id INT PRIMARY KEY)`)
				sqlDB.Exec(t, `INSERT INTO fk_upd_parent VALUES (1)`)
				sqlDB.Exec(t, `CREATE TABLE fk_upd_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_upd_parent(id)
				)`)
				// Insert a valid child row so we can update it.
				childID := lookupDescID(t, "fk_upd_child")
				applyInsert(t, childID, tree.Datums{tree.NewDInt(1), tree.NewDInt(1)})
			},
			buildWriteSet: func(t *testing.T) []ldrdecoder.DecodedRow {
				childID := lookupDescID(t, "fk_upd_child")
				return []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(1), tree.NewDInt(999)},
					PrevRow: tree.Datums{tree.NewDInt(1), tree.NewDInt(1)},
					TableID: childID,
				}}
			},
			errContains: "foreign key",
		},
		{
			name: "fk delete",
			setup: func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE fk_del_parent (id INT PRIMARY KEY)`)
				sqlDB.Exec(t, `CREATE TABLE fk_del_child (
					id INT PRIMARY KEY,
					parent_id INT REFERENCES fk_del_parent(id)
				)`)
				// Insert a parent row, then a child row referencing it.
				sqlDB.Exec(t, `INSERT INTO fk_del_parent VALUES (1)`)
				sqlDB.Exec(t, `INSERT INTO fk_del_child VALUES (1, 1)`)
			},
			buildWriteSet: func(t *testing.T) []ldrdecoder.DecodedRow {
				// Delete the parent row while the child still references it.
				parentID := lookupDescID(t, "fk_del_parent")
				return []ldrdecoder.DecodedRow{{
					Row:      tree.Datums{tree.NewDInt(1)},
					PrevRow:  tree.Datums{tree.NewDInt(1)},
					IsDelete: true,
					TableID:  parentID,
				}}
			},
			errContains: "foreign key",
		},
		{
			name: "unique insert",
			setup: func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE uniq_ins (
					id INT PRIMARY KEY,
					val INT UNIQUE
				)`)
				// Insert a row that the test will conflict with.
				applyInsert(t, lookupDescID(t, "uniq_ins"),
					tree.Datums{tree.NewDInt(1), tree.NewDInt(42)})
			},
			buildWriteSet: func(t *testing.T) []ldrdecoder.DecodedRow {
				return []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(2), tree.NewDInt(42)},
					PrevRow: nil,
					TableID: lookupDescID(t, "uniq_ins"),
				}}
			},
			errContains: "duplicate key",
		},
		{
			name: "unique update",
			setup: func(t *testing.T) {
				sqlDB.Exec(t, `CREATE TABLE uniq_upd (
					id INT PRIMARY KEY,
					val INT UNIQUE
				)`)
				tableID := lookupDescID(t, "uniq_upd")
				applyInsert(t, tableID, tree.Datums{tree.NewDInt(1), tree.NewDInt(42)})
				applyInsert(t, tableID, tree.Datums{tree.NewDInt(2), tree.NewDInt(99)})
			},
			buildWriteSet: func(t *testing.T) []ldrdecoder.DecodedRow {
				// Update row 2 to have the same unique value as row 1.
				return []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(2), tree.NewDInt(42)},
					PrevRow: tree.Datums{tree.NewDInt(2), tree.NewDInt(99)},
					TableID: lookupDescID(t, "uniq_upd"),
				}}
			},
			errContains: "duplicate key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)
			batch := []ldrdecoder.Transaction{{
				Timestamp: s.Clock().Now(),
				WriteSet:  tc.buildWriteSet(t),
			}}
			results, err := writer.ApplyBatch(ctx, batch)

			// NOTE: When a transaction is eligible for the DLQ, it's not an error, it
			// is a field set on the transaction apply results.
			require.NoError(t, err)

			require.Len(t, results, 1)
			require.ErrorContains(t, results[0].DlqReason, tc.errContains)
		})
	}
}
