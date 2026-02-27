// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwriter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// refresh reads local state for all rows in the batch and builds new
// transactions with LWW losers filtered out. A row is an LWW loser if the
// local row's timestamp is >= the incoming transaction's timestamp. Note: we
// can only filter rows that lose to live rows. If a row loses to a tombstone,
// we depend on a tryApply's Insert observing a cput error to surface the LWW
// loser.
//
// The returned transactions preserve the original WriteSet order so
// that dependent operations (e.g., delete-before-insert for unique key
// transfer) remain correctly ordered.
//
// Note: refresh only really works if the write sets are non-overlapping. If
// two transactions write to the same row, we will set the prev value to be
// value in the local database, when it should be whatever value is written by
// the predecessor transaction in the batch.
func (tw *transactionWriter) refresh(
	ctx context.Context, transactions []ldrdecoder.Transaction, results []ApplyResult,
) ([]ldrdecoder.Transaction, error) {
	type rowIndex struct {
		txn int
		row int
	}
	type tableToRefresh struct {
		rows    []tree.Datums
		indexes []rowIndex
	}

	tables := make(map[descpb.ID]tableToRefresh)
	for txnIdx, transaction := range transactions {
		for rowIdx, row := range transaction.WriteSet {
			table := tables[row.TableID]
			table.rows = append(table.rows, row.Row)
			table.indexes = append(table.indexes, rowIndex{txn: txnIdx, row: rowIdx})
			tables[row.TableID] = table
		}
	}

	// Read the current values in the local database.
	priorRows := make(map[rowIndex]sqlwriter.PriorRow)
	for tableID, table := range tables {
		readResult, err := tw.tableReaders[tableID].ReadRows(ctx, table.rows)
		if err != nil {
			return nil, err
		}
		for i, index := range table.indexes {
			if pr, ok := readResult[i]; ok {
				priorRows[index] = pr
			}
		}
	}

	// Build new transaction to apply preserving original WriteSet order with LWW
	// losers removed.
	refreshed := make([]ldrdecoder.Transaction, len(transactions))
	for txnIdx, transaction := range transactions {
		var writeSet []ldrdecoder.DecodedRow
		for rowIdx, row := range transaction.WriteSet {
			idx := rowIndex{txn: txnIdx, row: rowIdx}
			pr, rowExists := priorRows[idx]

			if rowExists && !pr.LogicalTimestamp.Less(transaction.Timestamp) {
				results[txnIdx].LwwLoserRows++
				continue
			}

			refreshedRow := row
			if rowExists {
				refreshedRow.PrevRow = pr.Row
			} else {
				refreshedRow.PrevRow = nil
			}
			writeSet = append(writeSet, refreshedRow)
		}
		refreshed[txnIdx] = ldrdecoder.Transaction{
			Timestamp: transaction.Timestamp,
			WriteSet:  writeSet,
		}
	}

	return refreshed, nil
}
