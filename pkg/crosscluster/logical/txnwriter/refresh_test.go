// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwriter

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestTransactionWriter_Refresh tests the refresh method directly, verifying
// that it reads local state and filters LWW losers without applying any rows.
func TestTransactionWriter_Refresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	writer := newTxnWriter(t, s)
	defer writer.Close(ctx)
	tw := writer.(*transactionWriter)

	// setupTable creates a table with (id INT PRIMARY KEY, name STRING), inserts
	// the given rows using SQL, and returns the table's descriptor ID along with
	// two timestamps: tsStart is before the rows are written, and tsEnd is after.
	// Test cases use these to construct batches that are older (LWW losers) or
	// newer (LWW winners) than the setup state.
	setupTable := func(
		t *testing.T, tableName string, insertRows []string,
	) (descID descpb.ID, tsStart, tsEnd hlc.Timestamp) {
		sqlDB.Exec(t, fmt.Sprintf(
			`CREATE TABLE %s (id INT PRIMARY KEY, name STRING)`, tableName,
		))
		sqlDB.QueryRow(t,
			`SELECT id FROM system.namespace WHERE name = $1`, tableName,
		).Scan(&descID)

		require.NoError(t, tw.initTable(ctx, descID))

		tsStart = s.Clock().Now()
		for _, row := range insertRows {
			sqlDB.Exec(t, fmt.Sprintf(
				`INSERT INTO %s (id, name) VALUES (%s)`, tableName, row,
			))
		}
		tsEnd = s.Clock().Now()
		return descID, tsStart, tsEnd
	}

	tests := []struct {
		name string
		// insertRows are SQL value lists inserted into the table before refresh
		// is called (e.g. "1, 'foo'").
		insertRows []string
		// buildBatch constructs the incoming transactions. tsStart is before any
		// setup rows were written; tsEnd is after. The function can use tsStart
		// to create LWW losers (older than local) or tsEnd to create LWW winners
		// (newer than local). PrevRows should be deliberately wrong to exercise
		// the refresh PrevRow correction logic.
		buildBatch func(descID descpb.ID, tsStart, tsEnd hlc.Timestamp) []ldrdecoder.Transaction
		// wantLwwLosers is the expected LwwLoserRows in the result.
		wantLwwLosers int
		// wantRefreshedRows is the expected number of rows in the refreshed
		// transaction's WriteSet (original rows minus losers).
		wantRefreshedRows int
	}{{
		name:       "partial_loser",
		insertRows: []string{"1, 'existing'"},
		buildBatch: func(descID descpb.ID, tsStart, _ hlc.Timestamp) []ldrdecoder.Transaction {
			return []ldrdecoder.Transaction{{
				Timestamp: tsStart,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						// Stale update to existing row — LWW loser.
						Row:     tree.Datums{tree.NewDInt(1), tree.NewDString("stale")},
						PrevRow: tree.Datums{tree.NewDInt(1), tree.NewDString("wrong")},
						TableID: descID,
					},
					{
						// Insert of new row — should survive refresh.
						Row:     tree.Datums{tree.NewDInt(2), tree.NewDString("new")},
						PrevRow: nil,
						TableID: descID,
					},
				},
			}}
		},
		wantLwwLosers:     1,
		wantRefreshedRows: 1,
	}, {
		name:       "all_losers",
		insertRows: []string{"1, 'row1'", "2, 'row2'"},
		buildBatch: func(descID descpb.ID, tsStart, _ hlc.Timestamp) []ldrdecoder.Transaction {
			return []ldrdecoder.Transaction{{
				Timestamp: tsStart,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:     tree.Datums{tree.NewDInt(1), tree.NewDString("stale1")},
						PrevRow: tree.Datums{tree.NewDInt(1), tree.NewDString("wrong")},
						TableID: descID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(2), tree.NewDString("stale2")},
						PrevRow: tree.Datums{tree.NewDInt(2), tree.NewDString("wrong")},
						TableID: descID,
					},
				},
			}}
		},
		wantLwwLosers:     2,
		wantRefreshedRows: 0,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tableName := fmt.Sprintf("refresh_%s", tc.name)
			descID, tsStart, tsEnd := setupTable(t, tableName, tc.insertRows)

			batch := tc.buildBatch(descID, tsStart, tsEnd)
			results := make([]ApplyResult, len(batch))

			var refreshed []ldrdecoder.Transaction
			err := tw.session.Txn(ctx, func(ctx context.Context) error {
				var err error
				refreshed, err = tw.refresh(ctx, batch, results)
				return err
			})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.wantLwwLosers, results[0].LwwLoserRows)
			require.Len(t, refreshed, 1)
			require.Len(t, refreshed[0].WriteSet, tc.wantRefreshedRows)
		})
	}
}
