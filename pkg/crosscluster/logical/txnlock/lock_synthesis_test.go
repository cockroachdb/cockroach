// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// overlappingLocks counts lock hashes shared between two LockSets.
func overlappingLocks(a, b LockSet) int {
	set := make(map[LockHash]struct{}, len(a.Locks))
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
		wantPK := want.Row[0]
		gotPK := got.Row[0]
		if want.IsDelete {
			wantPK = want.PrevRow[0]
		}
		if got.IsDelete {
			gotPK = got.PrevRow[0]
		}
		require.Equal(t, wantPK, gotPK,
			"%s: position %d: PK mismatch", label, i)
	}
}

// txnCase holds a batch of events and the expected DeriveLocks outcome.
// A zero-value txnCase (nil events) means "no transaction".
type txnCase struct {
	events    []streampb.StreamEvent_KV
	lockCount int
	order     []int // expected permutation of input row indices
	err       error // nil means success
}

type lockSynthesisTestCase struct {
	name         string
	txn1, txn2   txnCase
	overlapCount int
}

// plainRow builds datums for schema (id INT PRIMARY KEY, data STRING).
func plainRow(id int, data string) tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(id)), tree.NewDString(data)}
}

// ucRow builds datums for schema (id INT PRIMARY KEY, email STRING UNIQUE).
// An empty email maps to DNull.
func ucRow(id int, email string) tree.Datums {
	var emailDatum tree.Datum
	if email == "" {
		emailDatum = tree.DNull
	} else {
		emailDatum = tree.NewDString(email)
	}
	return tree.Datums{tree.NewDInt(tree.DInt(id)), emailDatum}
}

// compositeRow builds datums for schema:
//
//	(pk1 INT, pk2 INT, a INT, b INT, PRIMARY KEY (pk1, pk2), UNIQUE (a, b))
//
// Negative values for a or b map to DNull.
func compositeRow(pk1, pk2, a, b int) tree.Datums {
	var aDatum, bDatum tree.Datum
	if a < 0 {
		aDatum = tree.DNull
	} else {
		aDatum = tree.NewDInt(tree.DInt(a))
	}
	if b < 0 {
		bDatum = tree.DNull
	} else {
		bDatum = tree.NewDInt(tree.DInt(b))
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(pk1)),
		tree.NewDInt(tree.DInt(pk2)),
		aDatum,
		bDatum,
	}
}

func TestDeriveLocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, `CREATE TABLE plain_table (id INT PRIMARY KEY, data STRING)`)
	runner.Exec(t, `CREATE TABLE uc_table (id INT PRIMARY KEY, email STRING UNIQUE)`)
	// TODO(164507): make one of these columns a stored computed column.
	runner.Exec(t, `CREATE TABLE composite_table (pk1 INT, pk2 INT, a INT, b INT, PRIMARY KEY (pk1, pk2), UNIQUE (a, b))`)

	plainDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "plain_table",
	)
	ucDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "uc_table",
	)
	compositeDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "composite_table",
	)

	plainEB := ldrdecoder.NewTestEventBuilder(t, plainDesc.TableDesc())
	ucEB := ldrdecoder.NewTestEventBuilder(t, ucDesc.TableDesc())
	compositeEB := ldrdecoder.NewTestEventBuilder(t, compositeDesc.TableDesc())
	txnTime := s.Clock().Now()

	decoder, err := ldrdecoder.NewTxnDecoder(
		ctx, s.InternalDB().(descs.DB), s.ClusterSettings(),
		[]ldrdecoder.TableMapping{
			{SourceDescriptor: plainDesc, DestID: plainDesc.GetID()},
			{SourceDescriptor: ucDesc, DestID: ucDesc.GetID()},
			{SourceDescriptor: compositeDesc, DestID: compositeDesc.GetID()},
		},
	)
	require.NoError(t, err)

	ls, err := NewLockSynthesizer(
		ctx,
		&eval.Context{},
		s.LeaseManager().(*lease.Manager),
		s.Clock(),
		[]ldrdecoder.TableMapping{
			{DestID: plainDesc.GetID()},
			{DestID: ucDesc.GetID()},
			{DestID: compositeDesc.GetID()},
		},
	)
	require.NoError(t, err)

	testCases := []lockSynthesisTestCase{
		{
			// Two txns insert into a plain table with disjoint PKs. Each row
			// produces one PK lock. No overlap.
			name: "pk_only_no_overlap",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "a")),
					plainEB.InsertEvent(txnTime, plainRow(2, "b")),
				},
				lockCount: 2,
				order:     []int{0, 1},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(3, "c")),
					plainEB.InsertEvent(txnTime, plainRow(4, "d")),
				},
				lockCount: 2,
				order:     []int{0, 1},
			},
		},
		{
			// Both txns touch the same PK. Each gets 1 lock, with 1 overlap.
			name: "pk_only_with_overlap",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "a")),
				},
				lockCount: 1,
				order:     []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "x")),
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
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(1, "a@test.com")),
				},
				lockCount: 2,
				order:     []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(2, "b@test.com")),
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
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(1, "shared@test.com")),
				},
				lockCount: 2,
				order:     []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(2, "shared@test.com")),
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
				events: []streampb.StreamEvent_KV{
					// Insert row id=2 with email "user@example.com".
					ucEB.InsertEvent(txnTime, ucRow(2, "user@example.com")),
					// Delete row id=1 whose PrevRow had email "user@example.com".
					ucEB.DeleteEvent(txnTime, ucRow(1, "user@example.com")),
				},
				// Insert: PK + UC(new) = 2; delete: PK + UC(prev) = 2.
				// UC("user@example.com") shared, so deduplicated: 3 total.
				lockCount: 3,
				order:     []int{1, 0}, // delete before insert
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(3, "other@example.com")),
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
				events: []streampb.StreamEvent_KV{
					ucEB.UpdateEvent(txnTime,
						ucRow(1, "bob"),
						ucRow(1, "alice")),
					ucEB.UpdateEvent(txnTime,
						ucRow(2, "alice"),
						ucRow(2, "bob")),
				},
				err: ErrApplyCycle,
			},
		},
		{
			// NULL unique values should not produce UC locks, only PK locks.
			name: "null_unique_values",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(1, "")),
					ucEB.InsertEvent(txnTime, ucRow(2, "")),
				},
				lockCount: 2, // PK-only
				order:     []int{0, 1},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(3, "")),
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
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "same")),
				},
				lockCount: 1, // PK only
				order:     []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(1, "same")),
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
				events: []streampb.StreamEvent_KV{
					ucEB.UpdateEvent(txnTime,
						ucRow(1, "val-b"),
						ucRow(1, "val-a")),
					ucEB.UpdateEvent(txnTime,
						ucRow(2, "val-c"),
						ucRow(2, "val-b")),
					ucEB.UpdateEvent(txnTime,
						ucRow(3, "val-d"),
						ucRow(3, "val-c")),
				},
				// 3 PKs + 4 UCs (val-a, val-b, val-c, val-d) = 7.
				lockCount: 7,
				order:     []int{2, 1, 0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(10, "unrelated")),
				},
				lockCount: 2,
				order:     []int{0},
			},
		},
		{
			// One txn inserts a row, the other updates the same composite
			// PK. Both produce a PK lock on (1,2), so they overlap.
			name: "composite_pk_insert_and_update",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.InsertEvent(txnTime,
						compositeRow(1, 2, 10, 20)),
				},
				// PK + UC(10,20) = 2.
				lockCount: 2,
				order:     []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.UpdateEvent(txnTime,
						compositeRow(1, 2, 10, 20),
						compositeRow(1, 2, 10, 20)),
				},
				// PK only, UC unchanged.
				lockCount: 1,
				order:     []int{0},
			},
			overlapCount: 1, // shared PK(1,2)
		},
		{
			// One txn updates a row's unique columns, the other deletes
			// the same composite PK. PK locks overlap.
			name: "composite_pk_update_and_delete",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.UpdateEvent(txnTime,
						compositeRow(1, 2, 30, 40),
						compositeRow(1, 2, 10, 20)),
				},
				// PK + UC(new: 30,40) + UC(old: 10,20) = 3.
				lockCount: 3,
				order:     []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.DeleteEvent(txnTime,
						compositeRow(1, 2, 30, 40)),
				},
				// PK + UC(prev: 30,40) = 2.
				lockCount: 2,
				order:     []int{0},
			},
			overlapCount: 2, // shared PK(1,2), UC(30, 40)
		},
		{
			// Unique key transfer on composite table: row 0 takes the
			// unique value (10,20) freed by row 1's update away from it.
			// The sort must place row 1 before row 0.
			name: "composite_unique_key_transfer",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.UpdateEvent(txnTime,
						compositeRow(1, 1, 10, 20),
						compositeRow(1, 1, -1, -1)),
					compositeEB.UpdateEvent(txnTime,
						compositeRow(2, 2, 30, 40),
						compositeRow(2, 2, 10, 20)),
				},
				// PK(1, 1) + UC(10, 20) + PK(2, 2) + UC(30, 40) = 4
				lockCount: 4,
				order:     []int{1, 0}, // row 1 frees (10,20), row 0 takes it
			},
		},
	}

	decodeTxn := func(
		t *testing.T, events []streampb.StreamEvent_KV,
	) []ldrdecoder.DecodedRow {
		t.Helper()
		txn, err := decoder.DecodeTxn(ctx, events)
		require.NoError(t, err)
		return txn.WriteSet
	}

	checkTxn := func(
		t *testing.T, tc txnCase, label string,
	) LockSet {
		t.Helper()
		rows := decodeTxn(t, tc.events)
		result, err := ls.DeriveLocks(ctx, rows)
		if tc.err != nil {
			require.ErrorIs(t, err, tc.err)
			return LockSet{}
		}
		require.NoError(t, err)
		require.Len(t, result.Locks, tc.lockCount, "%s lock count", label)
		requireSortOrder(t, rows, result.SortedRows, tc.order, label)
		return result
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls1 := checkTxn(t, tc.txn1, "txn1")

			if tc.txn2.events == nil {
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
