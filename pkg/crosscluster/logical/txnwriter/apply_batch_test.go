// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwriter

import (
	"context"
	"fmt"
	"slices"
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

func TestTransactionWriter_ApplyBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE parent (id INT PRIMARY KEY, payload STRING)`)
	sqlDB.Exec(t, `CREATE TABLE child (
		id INT PRIMARY KEY,
		parent_id INT REFERENCES parent(id),
		unique_val INT UNIQUE,
		payload STRING
	)`)

	writer := newTxnWriter(t, s)
	defer writer.Close(ctx)

	var parentID, childID descpb.ID
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'parent'`).Scan(&parentID)
	sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'child'`).Scan(&childID)

	type applyTestCase struct {
		name string
		// setup creates the local state for the test case. All setup functions
		// run after preSetupTS is recorded, so any rows created during setup
		// have timestamps newer than preSetupTS.
		setup func(t *testing.T, baseID int)
		// buildTxn constructs the transaction to apply. preSetupTS is a clock
		// reading taken before any setup ran — use it for LWW loser timestamps.
		// Use s.Clock().Now() for LWW winner or normal timestamps.
		buildTxn func(t *testing.T, baseID int, preSetupTS hlc.Timestamp) ldrdecoder.Transaction
		validate func(t *testing.T, baseID int, result ApplyResult)
	}

	happyPathCases := []applyTestCase{{
		name:  "insert",
		setup: func(t *testing.T, baseID int) {},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("inserted")},
					PrevRow: nil,
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"inserted"}},
			)
		},
	}, {
		name: "update",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'before')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("after")},
					PrevRow: tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("before")},
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"after"}},
			)
		},
	}, {
		name: "delete",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'doomed')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.DNull},
					PrevRow:  tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("doomed")},
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT count(*) FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"0"}},
			)
		},
	}, {
		name: "update_insert_and_delete",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'update-me'), (%d, 'delete-me')`,
				baseID+1, baseID+2))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("updated")},
						PrevRow: tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("update-me")},
						TableID: parentID,
					},
					{
						Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 2)), tree.DNull},
						PrevRow:  tree.Datums{tree.NewDInt(tree.DInt(baseID + 2)), tree.NewDString("delete-me")},
						IsDelete: true,
						TableID:  parentID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 3)), tree.NewDString("inserted")},
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 3, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT id, payload FROM parent WHERE id IN (%d, %d, %d) ORDER BY id`,
					baseID+1, baseID+2, baseID+3),
				[][]string{
					{fmt.Sprintf("%d", baseID+1), "updated"},
					{fmt.Sprintf("%d", baseID+3), "inserted"},
				},
			)
		},
	}}

	lwwLosers := []applyTestCase{{
		name: "insert_lww_loser",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'existing')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, preSetupTS hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: preSetupTS,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("stale-insert")},
						PrevRow: nil,
						TableID: parentID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 2)), tree.NewDString("new-insert")},
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.GreaterOrEqual(t, result.AppliedRows, 1)
			// Original row unchanged.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"existing"}},
			)
			// Second insert succeeded.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+2),
				[][]string{{"new-insert"}},
			)
		},
	}, {
		name: "update_lww_loser",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'local')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, preSetupTS hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: preSetupTS,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("loser")},
						PrevRow: tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("stale")},
						TableID: parentID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 2)), tree.NewDString("new-insert")},
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			// Original row unchanged.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"local"}},
			)
			// Second insert succeeded.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+2),
				[][]string{{"new-insert"}},
			)
		},
	}, {
		name: "delete_lww_loser",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'local')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, preSetupTS hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: preSetupTS,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.DNull},
						PrevRow:  tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("stale")},
						IsDelete: true,
						TableID:  parentID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 2)), tree.NewDString("new-insert")},
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			// Original row unchanged.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"local"}},
			)
			// Second insert succeeded.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+2),
				[][]string{{"new-insert"}},
			)
		},
	}}

	lwwWinners := []applyTestCase{{
		name: "update_lww_winner",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'old')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("winner")},
					PrevRow: tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("stale")},
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"winner"}},
			)
		},
	}, {
		name: "delete_lww_winner",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'old')`, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.DNull},
					PrevRow:  tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("stale")},
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT count(*) FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"0"}},
			)
		},
	}, {
		name: "refresh_converts_update_to_insert",
		setup: func(t *testing.T, baseID int) {
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:     tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("recovered")},
					PrevRow: tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("phantom")},
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"recovered"}},
			)
		},
	}, {
		name: "refresh_converts_delete_to_tombstone_noop",
		setup: func(t *testing.T, baseID int) {
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.DNull},
					PrevRow:  tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("phantom")},
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 1, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			// TODO(jeffswenson): check the resulting origin time of the tombstone to
			// ensure we updated the tombstone value. Right now this "works" because the
			// writer skips the tombstone update case.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT count(*) FROM parent WHERE id = %d`, baseID+1),
				[][]string{{"0"}},
			)
		},
	}}

	fkTestCases := []applyTestCase{{
		name: "insert_parent_and_child",
		setup: func(t *testing.T, baseID int) {
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDString("parent-row"),
					},
					TableID: parentID,
				}, {
					Row: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDString("child-row"),
					},
					TableID: childID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 2, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload from parent WHERE id = %d`, baseID+1),
				[][]string{{"parent-row"}})
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT payload FROM child WHERE id = %d`, baseID+2),
				[][]string{{"child-row"}})
		},
	}, {
		name:  "dlq_fk_insert",
		setup: func(t *testing.T, baseID int) {},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDInt(tree.DInt(baseID + 99)), // non-existent parent
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDString("orphan"),
					},
					PrevRow: nil,
					TableID: childID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.ErrorContains(t, result.DlqReason, "foreign key")
		},
	}, {
		name: "dlq_fk_update",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'p')`, baseID+1))
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO child (id, parent_id, unique_val, payload) VALUES (%d, %d, %d, 'child')`,
				baseID+1, baseID+1, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDInt(tree.DInt(baseID + 99)), // non-existent parent
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDString("child"),
					},
					PrevRow: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDInt(tree.DInt(baseID + 1)),
						tree.NewDString("child"),
					},
					TableID: childID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.ErrorContains(t, result.DlqReason, "foreign key")
		},
	}, {
		name: "dlq_fk_delete",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'p')`, baseID+1))
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO child (id, parent_id, unique_val, payload) VALUES (%d, %d, %d, 'child')`,
				baseID+1, baseID+1, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.DNull},
					PrevRow:  tree.Datums{tree.NewDInt(tree.DInt(baseID + 1)), tree.NewDString("p")},
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.ErrorContains(t, result.DlqReason, "foreign key")
		},
	},
	}

	uniqueIndexTestCases := []applyTestCase{{
		name: "unique_transfer",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'p1'), (%d, 'p2')`,
				baseID+1, baseID+2))
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO child (id, parent_id, unique_val, payload) VALUES (%d, %d, %d, 'old-child')`,
				baseID+1, baseID+1, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{
					// Delete must precede insert so the unique constraint is freed.
					{
						Row: tree.Datums{
							tree.NewDInt(tree.DInt(baseID + 1)),
							tree.DNull,
							tree.DNull,
							tree.DNull,
						},
						PrevRow: tree.Datums{
							tree.NewDInt(tree.DInt(baseID + 1)),
							tree.NewDInt(tree.DInt(baseID + 1)),
							tree.NewDInt(tree.DInt(baseID + 1)),
							tree.NewDString("old-child"),
						},
						IsDelete: true,
						TableID:  childID,
					},
					{
						Row: tree.Datums{
							tree.NewDInt(tree.DInt(baseID + 2)),
							tree.NewDInt(tree.DInt(baseID + 2)),
							tree.NewDInt(tree.DInt(baseID + 1)), // same unique_val
							tree.NewDString("new-child"),
						},
						PrevRow: nil,
						TableID: childID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Nil(t, result.DlqReason)
			require.Equal(t, 2, result.AppliedRows)
			require.Equal(t, 0, result.LwwLoserRows)
			// Old child gone, new child has the unique_val.
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT count(*) FROM child WHERE id = %d`, baseID+1),
				[][]string{{"0"}},
			)
			sqlDB.CheckQueryResults(t,
				fmt.Sprintf(`SELECT unique_val, payload FROM child WHERE id = %d`, baseID+2),
				[][]string{{fmt.Sprintf("%d", baseID+1), "new-child"}},
			)
		},
	}, {
		name: "dlq_unique_insert",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'p1'), (%d, 'p2')`,
				baseID+1, baseID+2))
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO child (id, parent_id, unique_val, payload) VALUES (%d, %d, %d, 'existing')`,
				baseID+1, baseID+1, baseID+1))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 1)), // duplicate unique_val
						tree.NewDString("duplicate"),
					},
					PrevRow: nil,
					TableID: childID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.ErrorContains(t, result.DlqReason, "duplicate key")
		},
	}, {
		name: "dlq_unique_update",
		setup: func(t *testing.T, baseID int) {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO parent (id, payload) VALUES (%d, 'p1'), (%d, 'p2')`,
				baseID+1, baseID+2))
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO child (id, parent_id, unique_val, payload) VALUES (%d, %d, %d, 'child1'), (%d, %d, %d, 'child2')`,
				baseID+1, baseID+1, baseID+1,
				baseID+2, baseID+2, baseID+2))
		},
		buildTxn: func(t *testing.T, baseID int, _ hlc.Timestamp) ldrdecoder.Transaction {
			return ldrdecoder.Transaction{
				Timestamp: s.Clock().Now(),
				WriteSet: []ldrdecoder.DecodedRow{{
					Row: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 1)), // duplicate unique_val
						tree.NewDString("child2"),
					},
					PrevRow: tree.Datums{
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDInt(tree.DInt(baseID + 2)),
						tree.NewDString("child2"),
					},
					TableID: childID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.ErrorContains(t, result.DlqReason, "duplicate key")
		},
	}}

	cases := slices.Concat(
		happyPathCases,
		lwwLosers,
		lwwWinners,
		fkTestCases,
		uniqueIndexTestCases,
	)

	// "individual" runs each case independently with its own setup, build,
	// apply, and validate cycle.
	t.Run("individual", func(t *testing.T) {
		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				baseID := (i + 1) * 1000
				preSetupTS := s.Clock().Now()
				tc.setup(t, baseID)
				txn := tc.buildTxn(t, baseID, preSetupTS)
				results, err := writer.ApplyBatch(ctx, []ldrdecoder.Transaction{txn})
				require.NoError(t, err)
				tc.validate(t, baseID, results[0])
			})
		}
	})

	// "single_batch" runs all setups, builds all transactions, then applies
	// them in a single ApplyBatch call.
	t.Run("single_batch", func(t *testing.T) {
		preSetupTS := s.Clock().Now()
		baseIDs := make([]int, len(cases))
		for i := range cases {
			baseIDs[i] = 100000 + (i+1)*1000
			cases[i].setup(t, baseIDs[i])
		}

		txns := make([]ldrdecoder.Transaction, len(cases))
		for i := range cases {
			txns[i] = cases[i].buildTxn(t, baseIDs[i], preSetupTS)
		}

		results, err := writer.ApplyBatch(ctx, txns)
		require.NoError(t, err)
		require.Len(t, results, len(cases))

		for i, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				tc.validate(t, baseIDs[i], results[i])
			})
		}
	})
}
