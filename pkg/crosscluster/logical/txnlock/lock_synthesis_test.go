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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// plainRow models a row with schema (id INT PRIMARY KEY, data STRING).
type plainRow struct {
	id   int
	data string
}

func (r plainRow) toDatums() tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(r.id)), tree.NewDString(r.data)}
}

// ucRow models a row with schema (id INT PRIMARY KEY, email STRING UNIQUE).
// An empty email maps to DNull.
type ucRow struct {
	id    int
	email string
}

func (r ucRow) toDatums() tree.Datums {
	var emailDatum tree.Datum
	if r.email == "" {
		emailDatum = tree.DNull
	} else {
		emailDatum = tree.NewDString(r.email)
	}
	return tree.Datums{tree.NewDInt(tree.DInt(r.id)), emailDatum}
}

// overlappingLocks counts lock hashes shared between two LockSets.
func overlappingLocks(a, b LockSet) int {
	set := make(map[uint64]struct{}, len(a.Locks))
	for _, l := range a.Locks {
		set[l.Hash] = struct{}{}
	}
	count := 0
	for _, l := range b.Locks {
		if _, ok := set[l.Hash]; ok {
			count++
		}
	}
	return count
}

// requireSortOrder verifies that sorted rows match the expected permutation
// of the input rows. expectedOrder[i] is the index in the input slice that
// should appear at position i in the sorted output.
func requireSortOrder(
	t *testing.T,
	input []ldrdecoder.DecodedRow,
	sorted []ldrdecoder.DecodedRow,
	expectedOrder []int,
	label string,
) {
	t.Helper()
	require.Len(t, sorted, len(expectedOrder), "%s: wrong sorted length", label)
	for i, wantIdx := range expectedOrder {
		want := input[wantIdx]
		got := sorted[i]
		require.Equal(t, want.TableID, got.TableID,
			"%s: position %d: table ID mismatch", label, i)
		// Compare PK value (first datum).
		require.Equal(t, want.Row[0], got.Row[0],
			"%s: position %d: PK mismatch", label, i)
	}
}

// txnCase holds a batch of rows and the expected DeriveLocks outcome.
// A zero-value txnCase (nil rows) means "no transaction".
type txnCase struct {
	rows      []ldrdecoder.DecodedRow
	lockCount int
	order     []int // expected permutation of input row indices
	err       error // nil means success
}

type lockSynthesisTestCase struct {
	name         string
	txn1, txn2   txnCase
	overlapCount int
}

func TestDeriveLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, `CREATE TABLE plain_table (id INT PRIMARY KEY, data STRING)`)
	runner.Exec(t, `CREATE TABLE uc_table (id INT PRIMARY KEY, email STRING UNIQUE)`)

	codec := s.Codec()
	plainID := desctestutils.TestingGetTableDescriptor(
		kvDB, codec, "defaultdb", "public", "plain_table",
	).GetID()
	ucID := desctestutils.TestingGetTableDescriptor(
		kvDB, codec, "defaultdb", "public", "uc_table",
	).GetID()

	ls, err := NewLockSynthesizer(
		ctx,
		&eval.Context{},
		s.LeaseManager().(*lease.Manager),
		s.Clock(),
		[]ldrdecoder.TableMapping{
			{DestID: plainID},
			{DestID: ucID},
		},
	)
	require.NoError(t, err)

	testCases := []lockSynthesisTestCase{
		{
			// Two txns insert into a plain table with disjoint PKs. Each row
			// produces one PK lock. No overlap.
			name: "pk_only_no_overlap",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: plainID, Row: plainRow{1, "a"}.toDatums()},
					{TableID: plainID, Row: plainRow{2, "b"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0, 1},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: plainID, Row: plainRow{3, "c"}.toDatums()},
					{TableID: plainID, Row: plainRow{4, "d"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0, 1},
			},
		},
		{
			// Both txns touch the same PK. Each gets 1 lock, with 1 overlap.
			name: "pk_only_with_overlap",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: plainID, Row: plainRow{1, "a"}.toDatums()},
				},
				lockCount: 1,
				order:     []int{0},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: plainID, Row: plainRow{1, "x"}.toDatums()},
				},
				lockCount: 1,
				order:     []int{0},
			},
			overlapCount: 1,
		},
		{
			// Inserts with different unique values produce PK + UC locks each.
			// No overlap because emails differ.
			name: "unique_no_overlap",
			txn1: txnCase{
				// Insert (nil PrevRow): PK + UC = 2 locks.
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{1, "a@test.com"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{2, "b@test.com"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0},
			},
		},
		{
			// Both txns insert a row with the same unique email value.
			// PKs differ, so 1 overlap from the shared UC lock.
			name: "unique_with_overlap",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{1, "shared@test.com"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{2, "shared@test.com"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0},
			},
			overlapCount: 1,
		},
		{
			// Insert with email "x" is given before delete whose PrevRow has
			// the same email. The sort must reorder: delete first, insert
			// second.
			name: "delete_before_insert",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					// Insert row id=2 with email "user@example.com".
					{
						TableID: ucID,
						Row:     ucRow{2, "user@example.com"}.toDatums(),
					},
					// Delete row id=1 whose PrevRow had email "user@example.com".
					{
						TableID:  ucID,
						Row:      ucRow{1, ""}.toDatums(),
						PrevRow:  ucRow{1, "user@example.com"}.toDatums(),
						IsDelete: true,
					},
				},
				// Insert: PK + UC(new) = 2; delete: PK + UC(prev) = 2.
				// UC("user@example.com") shared, so deduplicated: 3 total.
				lockCount: 3,
				order:     []int{1, 0}, // delete before insert
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{3, "other@example.com"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0},
			},
		},
		{
			// Two updates that swap unique values: alice→bob and bob→alice.
			// This creates a cycle that cannot be resolved.
			name: "cycle_detection",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{
						TableID: ucID,
						Row:     ucRow{1, "bob"}.toDatums(),
						PrevRow: ucRow{1, "alice"}.toDatums(),
					},
					{
						TableID: ucID,
						Row:     ucRow{2, "alice"}.toDatums(),
						PrevRow: ucRow{2, "bob"}.toDatums(),
					},
				},
				err: ErrApplyCycle,
			},
		},
		{
			// NULL unique values should not produce UC locks, only PK locks.
			name: "null_unique_values",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{1, ""}.toDatums()},
					{TableID: ucID, Row: ucRow{2, ""}.toDatums()},
				},
				lockCount: 2, // PK-only
				order:     []int{0, 1},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{3, ""}.toDatums()},
				},
				lockCount: 1, // PK-only
				order:     []int{0},
			},
		},
		{
			// Two tables with the same PK value and email value. Because
			// they belong to different tables, locks use different mixins
			// and should not overlap.
			name: "cross_table_no_overlap",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: plainID, Row: plainRow{1, "same"}.toDatums()},
				},
				lockCount: 1, // PK only
				order:     []int{0},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{1, "same"}.toDatums()},
				},
				lockCount: 2, // PK + UC
				order:     []int{0},
			},
		},
		{
			// Three-row dependency chain via unique values that must be
			// reordered. Row 0 takes the value freed by row 1, and row 1
			// takes the value freed by row 2, so the sort order is {2, 1, 0}.
			name: "dependency_chain",
			txn1: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{
						TableID: ucID,
						Row:     ucRow{1, "val-b"}.toDatums(),
						PrevRow: ucRow{1, "val-a"}.toDatums(),
					},
					{
						TableID: ucID,
						Row:     ucRow{2, "val-c"}.toDatums(),
						PrevRow: ucRow{2, "val-b"}.toDatums(),
					},
					{
						TableID: ucID,
						Row:     ucRow{3, "val-d"}.toDatums(),
						PrevRow: ucRow{3, "val-c"}.toDatums(),
					},
				},
				// 3 PKs + 4 UCs (val-a, val-b, val-c, val-d) = 7.
				lockCount: 7,
				order:     []int{2, 1, 0},
			},
			txn2: txnCase{
				rows: []ldrdecoder.DecodedRow{
					{TableID: ucID, Row: ucRow{10, "unrelated"}.toDatums()},
				},
				lockCount: 2,
				order:     []int{0},
			},
		},
	}

	checkTxn := func(t *testing.T, tc txnCase, label string) LockSet {
		t.Helper()
		result, err := ls.DeriveLocks(ctx, tc.rows)
		if tc.err != nil {
			require.ErrorIs(t, err, tc.err)
			return LockSet{}
		}
		require.NoError(t, err)
		require.Len(t, result.Locks, tc.lockCount, "%s lock count", label)
		requireSortOrder(t, tc.rows, result.SortedRows, tc.order, label)
		return result
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls1 := checkTxn(t, tc.txn1, "txn1")

			if tc.txn2.rows == nil {
				return
			}
			ls2 := checkTxn(t, tc.txn2, "txn2")

			if tc.txn1.err == nil && tc.txn2.err == nil {
				require.Equal(t, tc.overlapCount,
					overlappingLocks(ls1, ls2), "overlap count")
			}
		})
	}
}
