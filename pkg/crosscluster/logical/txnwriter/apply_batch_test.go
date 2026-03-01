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

	parentRow := func(id int, payload string) tree.Datums {
		return tree.Datums{tree.NewDInt(tree.DInt(id)), tree.NewDString(payload)}
	}
	childRow := func(id, parentFK int, payload string) tree.Datums {
		return tree.Datums{
			tree.NewDInt(tree.DInt(id)),
			tree.NewDInt(tree.DInt(parentFK)),
			tree.DNull,
			tree.NewDString(payload),
		}
	}
	childRowWithUnique := func(id, parentFK, uniqueVal int, payload string) tree.Datums {
		return tree.Datums{
			tree.NewDInt(tree.DInt(id)),
			tree.NewDInt(tree.DInt(parentFK)),
			tree.NewDInt(tree.DInt(uniqueVal)),
			tree.NewDString(payload),
		}
	}

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
					Row:     parentRow(baseID+1, "inserted"),
					PrevRow: nil,
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
					Row:     parentRow(baseID+1, "after"),
					PrevRow: parentRow(baseID+1, "before"),
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
					PrevRow:  parentRow(baseID+1, "doomed"),
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
						Row:     parentRow(baseID+1, "updated"),
						PrevRow: parentRow(baseID+1, "update-me"),
						TableID: parentID,
					},
					{
						Row:      tree.Datums{tree.NewDInt(tree.DInt(baseID + 2)), tree.DNull},
						PrevRow:  parentRow(baseID+2, "delete-me"),
						IsDelete: true,
						TableID:  parentID,
					},
					{
						Row:     parentRow(baseID+3, "inserted"),
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 3}, result)
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
						Row:     parentRow(baseID+1, "stale-insert"),
						PrevRow: nil,
						TableID: parentID,
					},
					{
						Row:     parentRow(baseID+2, "new-insert"),
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1, LwwLoserRows: 1}, result)
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
						Row:     parentRow(baseID+1, "loser"),
						PrevRow: parentRow(baseID+1, "stale"),
						TableID: parentID,
					},
					{
						Row:     parentRow(baseID+2, "new-insert"),
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1, LwwLoserRows: 1}, result)
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
						PrevRow:  parentRow(baseID+1, "stale"),
						IsDelete: true,
						TableID:  parentID,
					},
					{
						Row:     parentRow(baseID+2, "new-insert"),
						PrevRow: nil,
						TableID: parentID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1, LwwLoserRows: 1}, result)
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
					Row:     parentRow(baseID+1, "winner"),
					PrevRow: parentRow(baseID+1, "stale"),
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
					PrevRow:  parentRow(baseID+1, "stale"),
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
					Row:     parentRow(baseID+1, "recovered"),
					PrevRow: parentRow(baseID+1, "phantom"),
					TableID: parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
					PrevRow:  parentRow(baseID+1, "phantom"),
					IsDelete: true,
					TableID:  parentID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 1}, result)
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
					Row:     parentRow(baseID+1, "parent-row"),
					TableID: parentID,
				}, {
					Row:     childRow(baseID+2, baseID+1, "child-row"),
					TableID: childID,
				}},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 2}, result)
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
					Row:     childRow(baseID+1, baseID+99, "orphan"), // non-existent parent
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
					Row:     childRow(baseID+1, baseID+99, "child"), // non-existent parent
					PrevRow: childRow(baseID+1, baseID+1, "child"),
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
					PrevRow:  parentRow(baseID+1, "p"),
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
						PrevRow:  childRow(baseID+1, baseID+1, "old-child"),
						IsDelete: true,
						TableID:  childID,
					},
					{
						Row:     childRowWithUnique(baseID+2, baseID+2, baseID+1, "new-child"), // same unique_val
						PrevRow: nil,
						TableID: childID,
					},
				},
			}
		},
		validate: func(t *testing.T, baseID int, result ApplyResult) {
			require.Equal(t, ApplyResult{AppliedRows: 2}, result)
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
					Row:     childRowWithUnique(baseID+2, baseID+2, baseID+1, "duplicate"), // duplicate unique_val
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
					Row:     childRowWithUnique(baseID+2, baseID+2, baseID+1, "child2"), // duplicate unique_val
					PrevRow: childRow(baseID+2, baseID+2, "child2"),
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

	validateApplyResult := func(t *testing.T, txn ldrdecoder.Transaction, result ApplyResult) {
		t.Helper()
		if result.DlqReason != nil {
			require.Equal(t, 0, result.AppliedRows, "applied rows should be zero on DLQ")
			require.Equal(t, 0, result.LwwLoserRows, "lww loser rows should be zero on DLQ")
		} else {
			require.Equal(t, len(txn.WriteSet),
				result.LwwLoserRows+result.AppliedRows,
				"lww losers + applied rows should equal batch size")
		}
	}

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
				validateApplyResult(t, txn, results[0])
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
				validateApplyResult(t, txns[i], results[i])
				tc.validate(t, baseIDs[i], results[i])
			})
		}
	})
}
