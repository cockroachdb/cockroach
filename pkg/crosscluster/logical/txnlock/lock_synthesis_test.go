// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestLockSynthesisNoConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec := s.Codec()

	runner := sqlutils.MakeSQLRunner(conn)

	// Create two tables with no unique constraints
	runner.Exec(t, `
		CREATE TABLE table1 (
			id INT PRIMARY KEY,
			data STRING
		)
	`)
	runner.Exec(t, `
		CREATE TABLE table2 (
			id INT PRIMARY KEY,
			value INT
		)
	`)

	// Get table descriptors
	table1Desc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "defaultdb", "public", "table1")
	table2Desc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "defaultdb", "public", "table2")

	table1ID := table1Desc.GetID()
	table2ID := table2Desc.GetID()

	// Create lock synthesizer
	ls, err := NewLockSynthesizer(
		ctx,
		s.LeaseManager().(*lease.Manager),
		s.Clock(),
		[]ldrdecoder.TableMapping{
			{DestID: table1ID},
			{DestID: table2ID},
		},
	)
	require.NoError(t, err)

	// Create test rows - simple inserts/updates with no dependencies
	rows := []ldrdecoder.DecodedRow{
		{
			TableID:  table1ID,
			Row:      tree.Datums{tree.NewDInt(1), tree.NewDString("data1")},
			PrevRow:  nil,
			IsDelete: false,
		},
		{
			TableID:  table1ID,
			Row:      tree.Datums{tree.NewDInt(2), tree.NewDString("data2")},
			PrevRow:  nil,
			IsDelete: false,
		},
		{
			TableID:  table2ID,
			Row:      tree.Datums{tree.NewDInt(1), tree.NewDInt(100)},
			PrevRow:  nil,
			IsDelete: false,
		},
	}

	// Derive locks
	lockSet, err := ls.DeriveLocks(rows)
	require.NoError(t, err)

	// Verify we got locks - at least one per table since there are no unique constraints
	require.Len(t, lockSet.Locks, 3)

	// Verify sorted rows - with no dependencies, order should be preserved
	require.Len(t, lockSet.SortedRows, 3)
}

func TestLockSynthesisUniqueConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	codec := s.Codec()

	runner := sqlutils.MakeSQLRunner(conn)

	t.Run("delete_then_insert_unique_constraint", func(t *testing.T) {
		// Create a table with a unique constraint
		runner.Exec(t, `
			CREATE TABLE users (
				id INT PRIMARY KEY,
				email STRING UNIQUE
			)
		`)

		// Get table descriptor
		tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "defaultdb", "public", "users")
		tableID := tableDesc.GetID()

		// Create lock synthesizer
		ls, err := NewLockSynthesizer(
			ctx,
			s.LeaseManager().(*lease.Manager),
			s.Clock(),
			[]ldrdecoder.TableMapping{{DestID: tableID}},
		)
		require.NoError(t, err)

		// Test case: Delete a row with email 'user@example.com' and insert a new row with the same email
		// This creates a dependency: delete must happen before insert
		rows := []ldrdecoder.DecodedRow{
			// Insert with email 'user@example.com'
			{
				TableID:  tableID,
				Row:      tree.Datums{tree.NewDInt(2), tree.NewDString("user@example.com")},
				PrevRow:  tree.Datums{tree.DNull, tree.DNull},
				IsDelete: false,
			},
			// Delete with email 'user@example.com'
			// For deletes, Row contains PK values with NULLs for non-PK columns
			{
				TableID:  tableID,
				Row:      tree.Datums{tree.NewDInt(1), tree.DNull},
				PrevRow:  tree.Datums{tree.NewDInt(1), tree.NewDString("user@example.com")},
				IsDelete: true,
			},
		}

		// Derive locks
		lockSet, err := ls.DeriveLocks(rows)
		require.NoError(t, err)

		// Verify we got locks for both primary keys and the unique constraint
		require.Greater(t, len(lockSet.Locks), 2, "Expected locks for primary keys and unique email")

		// Verify the sorted order: delete should come before insert
		require.Len(t, lockSet.SortedRows, 2)
		require.True(t, lockSet.SortedRows[0].IsDelete, "Delete should be sorted first")
		require.False(t, lockSet.SortedRows[1].IsDelete, "Insert should be sorted second")
	})

	t.Run("update_cycle_detection", func(t *testing.T) {
		// Create a table with a unique constraint
		runner.Exec(t, `
			CREATE TABLE accounts (
				id INT PRIMARY KEY,
				username STRING UNIQUE
			)
		`)

		// Get table descriptor
		tableDesc := desctestutils.TestingGetTableDescriptor(kvDB, codec, "defaultdb", "public", "accounts")
		tableID := tableDesc.GetID()

		// Create lock synthesizer
		ls, err := NewLockSynthesizer(
			ctx,
			s.LeaseManager().(*lease.Manager),
			s.Clock(),
			[]ldrdecoder.TableMapping{{DestID: tableID}},
		)
		require.NoError(t, err)

		// Test case: Two updates that create a cycle
		// Update 1: username 'alice' -> 'bob'
		// Update 2: username 'bob' -> 'alice'
		// This creates a cycle that should be detected
		rows := []ldrdecoder.DecodedRow{
			{
				TableID:  tableID,
				Row:      tree.Datums{tree.NewDInt(1), tree.NewDString("bob")},
				PrevRow:  tree.Datums{tree.NewDInt(1), tree.NewDString("alice")},
				IsDelete: false,
			},
			{
				TableID:  tableID,
				Row:      tree.Datums{tree.NewDInt(2), tree.NewDString("alice")},
				PrevRow:  tree.Datums{tree.NewDInt(2), tree.NewDString("bob")},
				IsDelete: false,
			},
		}

		// Derive locks - this should detect a cycle
		_, err = ls.DeriveLocks(rows)
		require.ErrorIs(t, err, ApplyCycle, "Expected cycle detection error")
	})
}
