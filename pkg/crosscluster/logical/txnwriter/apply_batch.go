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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func (tw *transactionWriter) ApplyBatch(
	ctx context.Context, transactions []ldrdecoder.Transaction,
) ([]ApplyResult, error) {
	for _, transaction := range transactions {
		for _, row := range transaction.WriteSet {
			err := tw.initTable(ctx, row.TableID)
			if err != nil {
				return nil, err
			}
		}
	}

	results := make([]ApplyResult, len(transactions))

	err := tw.session.Txn(ctx, func(ctx context.Context) error {
		return tw.tryApply(ctx, transactions, results)
	})
	if err == nil {
		return results, nil
	}
	if !errors.Is(err, sqlwriter.ErrStalePreviousValue) {
		return nil, err
	}

	err = tw.session.Txn(ctx, func(ctx context.Context) error {
		err := tw.refresh(ctx, transactions)
		if err != nil {
			return err
		}
		return tw.tryApply(ctx, transactions, results)
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (tw *transactionWriter) refresh(
	ctx context.Context, transactions []ldrdecoder.Transaction,
) error {
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

	for tableID, table := range tables {
		priorRows, err := tw.tableReaders[tableID].ReadRows(ctx, table.rows)
		if err != nil {
			return err
		}
		for _, index := range table.indexes {
			priorRow, ok := priorRows[index.row]
			if ok {
				row := &(transactions[index.txn].WriteSet[index.row])
				row.PrevRow = priorRow.Row
				row.PrevRowTimestamp = priorRow.LogicalTimestamp
			} else {
				// NOTE: the fact we don't observe tombstones here means we need to
				// depend on insert/delete cputs implementing lww correctly.
				transactions[index.txn].WriteSet[index.row].PrevRow = nil
				transactions[index.txn].WriteSet[index.row].PrevRowTimestamp = hlc.Timestamp{}
			}
		}
	}

	return nil
}

func (tw *transactionWriter) tryApply(
	ctx context.Context, txn []ldrdecoder.Transaction, results []ApplyResult,
) error {
	for i, transaction := range txn {
		err := tw.session.Savepoint(ctx, func(ctx context.Context) error {
			var err error
			results[i], err = tw.tryApplyTransaction(ctx, transaction)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (tw *transactionWriter) tryApplyTransaction(
	ctx context.Context, transaction ldrdecoder.Transaction,
) (ApplyResult, error) {
	for _, row := range transaction.WriteSet {
		tableWriter := tw.tableWriters[row.TableID]
		switch {
		case row.IsDeleteRow():
			err := tableWriter.DeleteRow(ctx, transaction.Timestamp, row.PrevRow)
			if err != nil {
				return ApplyResult{}, err
			}
		case row.IsTombstoneUpdate():
			// TODO(jeffswenson): handle the tombstone update case. For ordered mode,
			// this case is only needed for racing updates.
		case row.IsInsertRow():
			err := tableWriter.InsertRow(ctx, transaction.Timestamp, row.Row)
			if err != nil {
				return ApplyResult{}, err
			}
		case row.IsUpdateRow():
			err := tableWriter.UpdateRow(
				ctx,
				transaction.Timestamp,
				row.PrevRow,
				row.Row,
			)
			if err != nil {
				return ApplyResult{}, err
			}
		default:
			return ApplyResult{}, errors.AssertionFailedf("unhandled row case: %v", row)
		}
	}
	result := ApplyResult{
		// TODO(jeffswenson): detect dlq reasons
		// TODO(jeffswenson): count lww losers
		AppliedRows: len(transaction.WriteSet),
	}
	return result, nil
}
