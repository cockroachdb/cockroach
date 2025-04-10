// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type sqlRowReader struct {
	selectStatement statements.Statement[tree.Statement]
	// keyColumnIndices is the index of the datums that are part of the primary key.
	keyColumnIndices []int
	columns          []columnSchema
}

// priorRow is a row returned by the SQL reader. It contains the rows local
// value.
type priorRow struct {
	// The row is the local value of the row. It is in the correct order to use
	// with the crud insert/update/delete statements that are condition based on
	// the previous row values.
	row tree.Datums
	// logicalTimestamp is the origin timestamp if it exists or the mvcc
	// timestamp if the row was generated localy.
	logicalTimestamp hlc.Timestamp
	// isLocal is true if the row was generated locally. This implies the logical
	// timestamp is the rows mvcc timestamp.
	isLocal bool
}

func newSQLRowReader(table catalog.TableDescriptor) (*sqlRowReader, error) {
	cols := getPhysicalColumnsSchema(table)
	keyColumns := make([]int, 0, len(cols))
	for i, col := range cols {
		if col.isPrimaryKey {
			keyColumns = append(keyColumns, i)
		}
	}

	selectStatement, err := newBulkSelectStatement(table)
	if err != nil {
		return nil, err
	}

	return &sqlRowReader{
		selectStatement:  selectStatement,
		keyColumnIndices: keyColumns,
		columns:          cols,
	}, nil
}

// ReadRows reads the rows from the table using the provided transaction. A row
// will only be present in the result set if it exists. The index of the row in
// the input is the key to the output map.
//
// E.g. result[i] and rows[i] are the same row.
func (r *sqlRowReader) ReadRows(
	ctx context.Context, txn isql.Txn, rows []tree.Datums,
) (map[int]priorRow, error) {
	// TODO(jeffswenson): optimize allocations. It may require a change to the
	// API. For now, this probably isn't a performance bottleneck because:
	// 1. Many of the allocations are one per batch instead of one per row.
	// 2. This query is only used if one of the replicated row's prior value does
	// not match the local value.

	if len(rows) == 0 {
		return nil, nil
	}

	params := make([]any, 0, len(r.keyColumnIndices))
	for _, index := range r.keyColumnIndices {
		array := tree.NewDArray(r.columns[index].column.GetType())
		for _, row := range rows {
			if err := array.Append(row[index]); err != nil {
				return nil, err
			}
		}
		params = append(params, array)
	}

	// Execute the query using QueryBufferedEx which returns all rows at once.
	// This is okay since we already know the batch is small enough to fit in
	// memory.
	rows, err := txn.QueryBufferedEx(ctx, "replication-read-refresh", txn.KV(),
		sessiondata.NoSessionDataOverride,
		r.selectStatement.SQL,
		params...,
	)
	if err != nil {
		return nil, err
	}

	result := make(map[int]priorRow, len(rows))
	for _, row := range rows {
		// The extra columns are:
		// 0. The row index (used to match input rows to refreshed rows).
		// 1. The origin timestamp.
		// 2. The mvcc timestamp.
		const prefixColumns = 3
		if len(row) != len(r.columns)+prefixColumns {
			return nil, errors.AssertionFailedf("expected %d columns, got %d", len(r.columns)+3, len(row))
		}

		rowIndex, ok := tree.AsDInt(row[0])
		if !ok {
			return nil, errors.AssertionFailedf("expected column 0 to be the row index")
		}

		isLocal := false
		timestamp := row[1]
		if timestamp == tree.DNull {
			timestamp = row[2]
			isLocal = true
		}

		decimal, ok := tree.AsDDecimal(timestamp)
		if !ok {
			return nil, errors.AssertionFailedf("expected column 1 or 2 to be origin timestamp")
		}

		logicalTimestamp, err := hlc.DecimalToHLC(&decimal.Decimal)
		if err != nil {
			return nil, err
		}

		result[int(rowIndex)-1] = priorRow{
			row:              row[prefixColumns:],
			logicalTimestamp: logicalTimestamp,
			isLocal:          isLocal,
		}
	}

	return result, nil
}
