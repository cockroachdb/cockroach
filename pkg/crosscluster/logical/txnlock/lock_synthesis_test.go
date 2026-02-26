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

// overlappingLocks counts lock hashes shared between two LockSets,
// separated by whether the overlap involves a write lock (at least one
// side is write) or is read-only (both sides are read).
func overlappingLocks(a, b LockSet) (writeOverlaps, readOverlaps int) {
	set := make(map[LockHash]bool, len(a.Locks))
	for _, l := range a.Locks {
		set[l.Hash] = l.Read
	}
	for _, l := range b.Locks {
		aRead, ok := set[l.Hash]
		if !ok {
			continue
		}
		if aRead && l.Read {
			readOverlaps++
		} else {
			writeOverlaps++
		}
	}
	return writeOverlaps, readOverlaps
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
	events         []streampb.StreamEvent_KV
	writeLockCount int
	readLockCount  int
	order          []int // expected permutation of input row indices
	err            error // nil means success
}

type lockSynthesisTestCase struct {
	name              string
	txn1, txn2        txnCase
	writeOverlapCount int
	readOverlapCount  int
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

// fkParentRow builds datums for schema
// (id INT PRIMARY KEY, ref_col INT UNIQUE, val INT).
func fkParentRow(id, refCol, val int) tree.Datums {
	return tree.Datums{
		tree.NewDInt(tree.DInt(id)),
		tree.NewDInt(tree.DInt(refCol)),
		tree.NewDInt(tree.DInt(val)),
	}
}

// fkChildRow builds datums for schema
//
//	(id INT PRIMARY KEY,
//	 parent_id INT REFERENCES fk_parent(id),
//	 parent_ref INT REFERENCES fk_parent(ref_col),
//	 data INT)
//
// Negative values for parent_id or parent_ref map to DNull.
func fkChildRow(id, parentID, parentRef, data int) tree.Datums {
	var pidDatum, refDatum tree.Datum
	if parentID < 0 {
		pidDatum = tree.DNull
	} else {
		pidDatum = tree.NewDInt(tree.DInt(parentID))
	}
	if parentRef < 0 {
		refDatum = tree.DNull
	} else {
		refDatum = tree.NewDInt(tree.DInt(parentRef))
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(id)),
		pidDatum,
		refDatum,
		tree.NewDInt(tree.DInt(data)),
	}
}

// selfRefRow builds datums for schema
// (id INT PRIMARY KEY, parent_id INT REFERENCES self_ref(id), data INT).
// A negative parent_id maps to DNull.
func selfRefRow(id, parentID, data int) tree.Datums {
	var pidDatum tree.Datum
	if parentID < 0 {
		pidDatum = tree.DNull
	} else {
		pidDatum = tree.NewDInt(tree.DInt(parentID))
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(id)),
		pidDatum,
		tree.NewDInt(tree.DInt(data)),
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
	runner.Exec(t, `CREATE TABLE fk_parent (id INT PRIMARY KEY, ref_col INT UNIQUE, val INT)`)
	runner.Exec(t, `
		CREATE TABLE fk_child (
			id INT PRIMARY KEY,
			parent_id INT REFERENCES fk_parent(id),
			parent_ref INT REFERENCES fk_parent(ref_col),
			data INT
		)`)
	runner.Exec(t, `
		CREATE TABLE fk_self_ref (
			id INT PRIMARY KEY,
			parent_id INT REFERENCES fk_self_ref(id),
			data INT
		)`)
	plainDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "plain_table",
	)
	ucDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "uc_table",
	)
	compositeDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "composite_table",
	)
	parentDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "fk_parent",
	)
	childDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "fk_child",
	)
	selfRefDesc := cdctest.GetHydratedTableDescriptor(
		t, s.ExecutorConfig(), "fk_self_ref",
	)

	plainEB := ldrdecoder.NewTestEventBuilder(t, plainDesc.TableDesc())
	ucEB := ldrdecoder.NewTestEventBuilder(t, ucDesc.TableDesc())
	compositeEB := ldrdecoder.NewTestEventBuilder(t, compositeDesc.TableDesc())
	parentEB := ldrdecoder.NewTestEventBuilder(t, parentDesc.TableDesc())
	childEB := ldrdecoder.NewTestEventBuilder(t, childDesc.TableDesc())
	selfRefEB := ldrdecoder.NewTestEventBuilder(t, selfRefDesc.TableDesc())
	txnTime := s.Clock().Now()

	decoder, err := ldrdecoder.NewTxnDecoder(
		ctx, s.InternalDB().(descs.DB), s.ClusterSettings(),
		[]ldrdecoder.TableMapping{
			{SourceDescriptor: plainDesc, DestID: plainDesc.GetID()},
			{SourceDescriptor: ucDesc, DestID: ucDesc.GetID()},
			{SourceDescriptor: compositeDesc, DestID: compositeDesc.GetID()},
			{SourceDescriptor: parentDesc, DestID: parentDesc.GetID()},
			{SourceDescriptor: childDesc, DestID: childDesc.GetID()},
			{SourceDescriptor: selfRefDesc, DestID: selfRefDesc.GetID()},
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
			{DestID: parentDesc.GetID()},
			{DestID: childDesc.GetID()},
			{DestID: selfRefDesc.GetID()},
		},
	)
	require.NoError(t, err)

	testCases := []lockSynthesisTestCase{
		{
			// Two txns insert into a plain table with disjoint PKs. Each row
			// produces one PK write lock. No overlap.
			name: "pk_only_no_overlap",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "a")),
					plainEB.InsertEvent(txnTime, plainRow(2, "b")),
				},
				writeLockCount: 2,
				order:          []int{0, 1},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(3, "c")),
					plainEB.InsertEvent(txnTime, plainRow(4, "d")),
				},
				writeLockCount: 2,
				order:          []int{0, 1},
			},
		},
		{
			// Both txns touch the same PK. Each gets 1 write lock, with
			// 1 overlap.
			name: "pk_only_with_overlap",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "a")),
				},
				writeLockCount: 1,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					plainEB.InsertEvent(txnTime, plainRow(1, "x")),
				},
				writeLockCount: 1,
				order:          []int{0},
			},
			writeOverlapCount: 1, // PK
		},
		{
			// Inserts with different unique values produce PK + UC locks each.
			// No overlap because emails differ.
			name: "unique_no_overlap",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(1, "a@test.com")),
				},
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(2, "b@test.com")),
				},
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
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
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(2, "shared@test.com")),
				},
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
			},
			writeOverlapCount: 1, // UC(shared@test.com)
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
				writeLockCount: 3,
				order:          []int{1, 0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(3, "other@example.com")),
				},
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
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
				writeLockCount: 2, // PK-only
				order:          []int{0, 1},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(3, "")),
				},
				writeLockCount: 1, // PK-only
				order:          []int{0},
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
				writeLockCount: 1, // PK only
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(1, "same")),
				},
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
			},
		},
		{
			// Three-row dependency chain via unique values that must be
			// reordered. Row 0 takes the value freed by row 1, and row 1
			// takes the value freed by row 2, so the sort order is {2, 1, 0}.
			// Shared UC hashes (val-b, val-c) are upgraded to write.
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
				// 3 PK + UC(val-a,val-b,val-c,val-d) = 7 write.
				writeLockCount: 7,
				order:          []int{2, 1, 0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					ucEB.InsertEvent(txnTime, ucRow(10, "unrelated")),
				},
				writeLockCount: 2, // PK + UC(new)
				order:          []int{0},
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
				writeLockCount: 2, // PK + UC(new: 10,20)
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.UpdateEvent(txnTime,
						compositeRow(1, 2, 10, 20),
						compositeRow(1, 2, 10, 20)),
				},
				writeLockCount: 1, // PK only, UC unchanged
				order:          []int{0},
			},
			writeOverlapCount: 1, // PK(1,2)
		},
		{
			// One txn updates a row's unique columns, the other deletes the
			// same composite PK. PK and UC(30,40) locks overlap.
			name: "composite_pk_update_and_delete",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.UpdateEvent(txnTime,
						compositeRow(1, 2, 30, 40),
						compositeRow(1, 2, 10, 20)),
				},
				// PK + UC(old: 10,20) + UC(new: 30,40) = 3 write.
				writeLockCount: 3,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					compositeEB.DeleteEvent(txnTime,
						compositeRow(1, 2, 30, 40)),
				},
				writeLockCount: 2, // PK + UC(prev: 30,40)
				order:          []int{0},
			},
			writeOverlapCount: 2, // PK(1,2) + UC(30,40)
		},
		{
			// Unique key transfer on composite table: row 0 takes the unique
			// value (10,20) freed by row 1's update. The sort must place
			// row 1 before row 0. UC(10,20) is deduplicated across rows:
			// read(row0) && write(row1) → write.
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
				// 2 PK + UC(10,20) + UC(30,40) = 4 write.
				writeLockCount: 4,
				order:          []int{1, 0},
			},
		},
		{
			// Inserting children that reference different parents should
			// not conflict, since they don't compete for the same FK slot.
			name: "fk_insert_children_different_parents",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 10, 100, 0)),
				},
				// PK + FK(read,parent_id=10) + FK(read,parent_ref=100).
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(2, 20, 200, 0)),
				},
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
		},
		{
			// Inserting two children that reference the same parent should
			// conflict, since both depend on that parent row existing.
			name: "fk_insert_children_same_parent",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 10, 100, 0)),
				},
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(2, 20, 100, 0)),
				},
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
			readOverlapCount: 1, // FK(parent_ref=100)
		},
		{
			// A child insert that references a parent's unique column must
			// conflict with a concurrent parent update that changes that
			// column away from the referenced value.
			name: "fk_insert_child_while_parent_changes_ref",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 10, 10, 0)),
				},
				// PK + FK(read,parent_id=10) + FK(read,parent_ref=10).
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.UpdateEvent(txnTime,
						fkParentRow(2, 20, 0),
						fkParentRow(2, 10, 0)),
				},
				// PK + UC(write,10) + UC(write,20) + FK(write,10,ref) + FK(write,20,ref)
				writeLockCount: 5,
				order:          []int{0},
			},
			// Overlap on FK(parent_ref=10)
			writeOverlapCount: 1,
		},
		{
			// Deleting a child that references a parent must conflict with
			// deleting that parent, since the child delete must be applied
			// first to avoid violating the FK constraint.
			name: "fk_delete_child_and_parent",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.DeleteEvent(txnTime, fkChildRow(1, 10, 100, 0)),
				},
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.DeleteEvent(txnTime, fkParentRow(10, 50, 0)),
				},
				writeLockCount: 4,
				order:          []int{0},
			},
			writeOverlapCount: 1,
		},
		{
			// Updating a child row without changing any FK columns should
			// not produce FK locks, since the parent relationship is unchanged.
			name: "fk_update_child_unchanged_fk_cols",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 10, 100, 1),
						fkChildRow(1, 10, 100, 0)),
				},
				writeLockCount: 1, // PK only
				order:          []int{0},
			},
		},
		{
			// Updating a child's FK column to reference a different parent
			// should produce locks on both the old and new parent values.
			name: "fk_update_child_changed_fk_col",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 10, 200, 0),
						fkChildRow(1, 10, 100, 0)),
				},
				// PK + FK(read,100) + FK(read,200). parent_id=10 unchanged.
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
		},
		{
			// Inserting a child with NULL FK columns does not reference any
			// parent, so no FK locks should be produced.
			name: "fk_insert_child_null_fk_cols",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, -1, -1, 0)),
				},
				writeLockCount: 1, // PK only
				order:          []int{0},
			},
		},
		{
			// Updating a parent's referenced column (ref_col) must produce
			// FK locks, since children may depend on both the old and new
			// values.
			name: "fk_update_parent_referenced_col",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.UpdateEvent(txnTime,
						fkParentRow(1, 20, 0),
						fkParentRow(1, 10, 0)),
				},
				// PK + UC(write,10) + UC(write,20) + FK(write,10,ref_col) +
				// FK(write,20,ref_col) = 5 write.
				// Inbound FK on id=1: unchanged → skipped.
				writeLockCount: 5,
				order:          []int{0},
			},
		},
		{
			// Updating a parent column that no child references should not
			// produce any FK or UC locks.
			name: "fk_update_parent_unreferenced_col",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.UpdateEvent(txnTime,
						fkParentRow(1, 10, 1),
						fkParentRow(1, 10, 0)),
				},
				writeLockCount: 1, // PK only
				order:          []int{0},
			},
		},
		{
			name: "fk_child_starts_referencing_value_parent_is_changing",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					// Child starts referencing ref_col=10.
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 10, 10, 0),
						fkChildRow(1, 10, 20, 0)),
					// Parent changes ref_col from 5 to 10.
					parentEB.UpdateEvent(txnTime,
						fkParentRow(2, 10, 0),
						fkParentRow(2, 5, 0)),
				},
				writeLockCount: 6,
				readLockCount:  1,
				order:          []int{1, 0},
			},
		},
		{
			// Child and parent updates that touch completely disjoint FK
			// values should not require reordering, since neither depends
			// on a value the other is changing.
			name: "fk_child_and_parent_disjoint_values",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 10, 20, 0),
						fkChildRow(1, 10, 10, 0)),
					parentEB.UpdateEvent(txnTime,
						fkParentRow(2, 40, 0),
						fkParentRow(2, 30, 0)),
				},
				writeLockCount: 6,
				readLockCount:  2,
				order:          []int{0, 1},
			},
		},
		{
			// Inserting a child that references a parent's ref_col while
			// the parent is updating that ref_col to a new value in the
			// same txn. The parent update must be applied first so the
			// referenced value still exists when the child is inserted.
			name: "fk_insert_child_while_parent_updates_ref",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 5, 20, 0)),
					parentEB.UpdateEvent(txnTime,
						fkParentRow(2, 20, 0),
						fkParentRow(2, 10, 0)),
				},
				writeLockCount: 6,
				readLockCount:  1,
				order:          []int{1, 0},
			},
		},
		{
			// Deleting a child and its parent in the same txn. The child
			// must be deleted before the parent to avoid violating the FK
			// constraint.
			name: "fk_delete_child_then_parent_same_txn",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.DeleteEvent(txnTime, fkParentRow(10, 50, 0)),
					childEB.DeleteEvent(txnTime, fkChildRow(1, 10, 50, 0)),
				},
				writeLockCount: 5,
				readLockCount:  0,
				order:          []int{1, 0},
			},
		},
		{
			// Inserting a new parent and a child that references it in the
			// same txn. The parent must be inserted before the child so the
			// FK reference is valid.
			name: "fk_insert_parent_then_child_same_txn",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 10, 50, 0)),
					parentEB.InsertEvent(txnTime, fkParentRow(10, 50, 0)),
				},
				writeLockCount: 5,
				order:          []int{1, 0},
			},
		},
		{
			// Self-referential: inserting a parent row and a child row
			// that references it in the same txn. The parent must be
			// inserted first.
			name: "fk_self_ref_insert_parent_then_child",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					selfRefEB.InsertEvent(txnTime, selfRefRow(2, 1, 0)),
					selfRefEB.InsertEvent(txnTime, selfRefRow(1, -1, 0)),
				},
				writeLockCount: 4,
				readLockCount:  0,
				order:          []int{1, 0},
			},
		},
		{
			// Self-referential: deleting a child row and the parent row it
			// references in the same txn. The child must be deleted first.
			name: "fk_self_ref_delete_child_then_parent",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					selfRefEB.DeleteEvent(txnTime, selfRefRow(1, -1, 0)),
					selfRefEB.DeleteEvent(txnTime, selfRefRow(2, 1, 0)),
				},
				writeLockCount: 4,
				readLockCount:  0,
				order:          []int{1, 0},
			},
		},
		{
			// Self-referential FK: updating a row's parent_id should only
			// produce outbound FK locks (on the referenced parent rows),
			// not inbound locks on the row's own PK since it is unchanged.
			name: "fk_self_ref_update_parent_id",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					selfRefEB.UpdateEvent(txnTime,
						selfRefRow(1, 10, 0),
						selfRefRow(1, 5, 0)),
				},
				// PK = 1 write; FK(read,5) + FK(read,10) = 2 read.
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
			},
		},
		{
			// We should delete the id=10 row only after we've updated the id=1 row that references it.
			name: "fk_self_ref_delete_parent_id",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					selfRefEB.DeleteEvent(txnTime, selfRefRow(10, -1, 0)),
					selfRefEB.UpdateEvent(txnTime,
						selfRefRow(1, 5, 0),
						selfRefRow(1, 10, 0)),
				},
				// 3 write locks = 2 PK + FK(10); 1 read lock = FK(5).
				writeLockCount: 3,
				readLockCount:  1,
				order:          []int{1, 0},
			},
		},
		{
			name: "fk_update_null_to_non_null_insert",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 1, 1, 1),
						fkChildRow(1, -1, -1, 0)),
					parentEB.InsertEvent(txnTime,
						fkParentRow(1, 1, 1)),
				},
				// 2 PKs + UC(1) + 2 FKs that are changing
				writeLockCount: 5,
				order:          []int{1, 0},
			},
		},
		{
			name: "fk_update_non_null_to_null_delete",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.DeleteEvent(txnTime,
						fkParentRow(1, 1, 1)),
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, -1, -1, 0),
						fkChildRow(1, 1, 1, 1)),
				},
				// 2 PKs + 2 FKs that are changing + 1 UC delete
				writeLockCount: 5,
				order:          []int{1, 0},
			},
		},
		{
			name: "fk_swap_parents",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.DeleteEvent(txnTime, fkParentRow(10, 100, 0)),
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 20, -1, 0),
						fkChildRow(1, 10, -1, 0)),
					parentEB.InsertEvent(txnTime, fkParentRow(20, 200, 0)),
				},

				// Delete: PK + UC delete + 2 FK parent = 4
				// Update: PK = 1 (FK reads upgraded to write via collision)
				// Insert: PK + UC insert + 2 FK parent = 4
				writeLockCount: 9,
				order:          []int{2, 1, 0},
			},
		},

		{
			// Ensure we don't have spurious dependencies when deleting two unrelated
			// rows with fk dependencies.
			name: "fk_delete_null_fk_child_and_parent_same_txn",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.DeleteEvent(txnTime, fkParentRow(10, 50, 0)),
					childEB.DeleteEvent(txnTime, fkChildRow(1, -1, -1, 0)),
				},
				// parent delete: PK + UC(50) + FK(id=10) + FK(ref_col=50) = 4
				// child delete: PK
				writeLockCount: 5,
				order:          []int{0, 1},
			},
		},
		{
			name: "fk_insert_parent_update_child_to_new_parent",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.UpdateEvent(txnTime,
						fkChildRow(1, 10, -1, 0),
						fkChildRow(1, 5, -1, 0)),
					parentEB.InsertEvent(txnTime, fkParentRow(10, 50, 0)),
				},
				// child: PK
				// parent: PK + UC(50) + FK(id=10) + FK(ref_col=50) = 4
				writeLockCount: 5,
				// child: FK(parent_id=5)
				readLockCount: 1,
				order:         []int{1, 0},
			},
		},
		{
			// Cross-txn: Txn1 inserts a parent, Txn2 inserts a child
			// referencing that parent. The child's FK read lock on
			// parent_id overlaps with the parent's PK write lock.
			name: "fk_cross_txn_parent_insert_child_insert",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.InsertEvent(txnTime, fkParentRow(10, 50, 0)),
				},
				// PK + UC(50) + FK(id=10) + FK(ref_col=50) = 4
				writeLockCount: 4,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 10, -1, 0)),
				},
				// PK
				writeLockCount: 1,
				// FK(parent_id=10)
				readLockCount: 1,
				order:         []int{0},
			},
			// child FK read lock on parent_id=10 overlaps with
			// parent's inbound FK write lock on id=10.
			writeOverlapCount: 1,
		},
		{
			// Txn1 deletes a parent, Txn2 inserts a child
			// referencing that parent. The child's FK read lock
			// overlaps with the parent delete's write lock.
			name: "fk_cross_txn_parent_delete_child_insert",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.DeleteEvent(txnTime, fkParentRow(10, 50, 0)),
				},
				// PK + UC + 2 FK
				writeLockCount: 4,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.InsertEvent(txnTime, fkChildRow(1, 10, -1, 0)),
				},
				// PK = 1 write
				writeLockCount: 1,
				// FK(parent_id=10)
				readLockCount: 1,
				order:         []int{0},
			},
			// child FK read lock on parent_id=10 overlaps with
			// parent's inbound FK write lock on id=10.
			writeOverlapCount: 1,
		},
		{
			// We always emit pk locks, but we don't need to do the same for fk locks on
			// pk, i.e why we use separate FK locks instead of letting them overlap with
			// the UC locks. Test we don't have spurious dependencies in this case.
			name: "fk_cross_txn_no_overlap",
			txn1: txnCase{
				events: []streampb.StreamEvent_KV{
					parentEB.UpdateEvent(txnTime,
						fkParentRow(10, 50, 1),
						fkParentRow(10, 50, 0)),
				},
				writeLockCount: 1,
				order:          []int{0},
			},
			txn2: txnCase{
				events: []streampb.StreamEvent_KV{
					childEB.DeleteEvent(txnTime, fkChildRow(1, 10, 50, 0)),
				},
				writeLockCount: 1,
				readLockCount:  2,
				order:          []int{0},
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

		var readCount, writeCount int
		for _, l := range result.Locks {
			if l.Read {
				readCount++
			} else {
				writeCount++
			}
		}
		require.Equal(t, tc.writeLockCount, writeCount, "%s write lock count", label)
		require.Equal(t, tc.readLockCount, readCount, "%s read lock count", label)
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
				writeOverlaps, readOverlaps := overlappingLocks(ls1, ls2)
				require.Equal(t, tc.writeOverlapCount, writeOverlaps, "write overlap count")
				require.Equal(t, tc.readOverlapCount, readOverlaps, "read overlap count")
			}
		})
	}
}
