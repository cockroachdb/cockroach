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

func TestTransactionWriter_Refresh(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, db, _ := serverutils.StartSlimServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	writer := newTxnWriter(t, s)
	defer writer.Close(ctx)
	tw := writer.(*transactionWriter)

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
		name              string
		insertRows        []string
		buildBatch        func(descID descpb.ID, tsStart, tsEnd hlc.Timestamp) []ldrdecoder.Transaction
		wantLwwLosers     int
		wantRefreshedRows int
		checkPrevRows     func(t *testing.T, refreshed []ldrdecoder.Transaction)
	}{{
		name:       "partial_loser",
		insertRows: []string{"1, 'existing'"},
		buildBatch: func(descID descpb.ID, tsStart, _ hlc.Timestamp) []ldrdecoder.Transaction {
			return []ldrdecoder.Transaction{{
				Timestamp: tsStart,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:     tree.Datums{tree.NewDInt(1), tree.NewDString("stale")},
						PrevRow: tree.Datums{tree.NewDInt(1), tree.NewDString("wrong")},
						TableID: descID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(2), tree.NewDString("new")},
						PrevRow: nil,
						TableID: descID,
					},
				},
			}}
		},
		wantLwwLosers:     1,
		wantRefreshedRows: 1,
		checkPrevRows: func(t *testing.T, refreshed []ldrdecoder.Transaction) {
			require.Nil(t, refreshed[0].WriteSet[0].PrevRow)
		},
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
		checkPrevRows: func(t *testing.T, refreshed []ldrdecoder.Transaction) {
			require.Empty(t, refreshed[0].WriteSet)
		},
	}, {
		name:       "winner_corrects_prev_row",
		insertRows: []string{"1, 'local_val'"},
		buildBatch: func(descID descpb.ID, _, tsEnd hlc.Timestamp) []ldrdecoder.Transaction {
			return []ldrdecoder.Transaction{{
				Timestamp: tsEnd,
				WriteSet: []ldrdecoder.DecodedRow{
					{
						Row:     tree.Datums{tree.NewDInt(1), tree.NewDString("updated")},
						PrevRow: tree.Datums{tree.NewDInt(1), tree.NewDString("wrong")},
						TableID: descID,
					},
					{
						Row:     tree.Datums{tree.NewDInt(2), tree.NewDString("brand_new")},
						PrevRow: tree.Datums{tree.NewDInt(2), tree.NewDString("ghost")},
						TableID: descID,
					},
				},
			}}
		},
		wantLwwLosers:     0,
		wantRefreshedRows: 2,
		checkPrevRows: func(t *testing.T, refreshed []ldrdecoder.Transaction) {
			ws := refreshed[0].WriteSet
			require.NotNil(t, ws[0].PrevRow)
			require.Equal(t, tree.NewDInt(1), ws[0].PrevRow[0])
			require.Equal(t, tree.NewDString("local_val"), ws[0].PrevRow[1])
			require.Nil(t, ws[1].PrevRow)
		},
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
			tc.checkPrevRows(t, refreshed)
		})
	}
}
