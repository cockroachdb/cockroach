// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type RowReader interface {
	// ReadRows reads the rows from the table using the provided transaction. A row
	// will only be present in the result set if it exists. The index of the row in
	// the input is the key to the output map.
	//
	// E.g. result[i] and rows[i] are the same row.
	ReadRows(ctx context.Context, rows []tree.Datums) (map[int]PriorRow, error)
}

// PriorRow is a row returned by the SQL reader. It contains the rows local
// value.
type PriorRow struct {
	// The Row is the local value of the Row. It is in the correct order to use
	// with the crud insert/update/delete statements that are condition based on
	// the previous Row values.
	Row tree.Datums
	// LogicalTimestamp is the origin timestamp if it exists or the mvcc
	// timestamp if the row was generated localy.
	LogicalTimestamp hlc.Timestamp
	// IsLocal is true if the row was generated locally. This implies the logical
	// timestamp is the rows mvcc timestamp.
	IsLocal bool
}

func NewRowReader(
	ctx context.Context, table catalog.TableDescriptor, session isql.Session,
) (RowReader, error) {
	hasArrayPrimaryKey := false
	for _, col := range GetColumnSchema(table) {
		if col.IsPrimaryKey && col.ColumnType.Family() == types.ArrayFamily {
			hasArrayPrimaryKey = true
			break
		}
	}
	if hasArrayPrimaryKey {
		// TODO(#32552): delete point row reader once CockroachDB supports nested
		// array types. We can't use the bulk reader because it passes all of the
		// primary key values in an array, which results in an array of arrays when
		// a primary key column is an array.
		return newPointRowReader(ctx, table, session)
	}
	return newBulkRowReader(ctx, table, session)
}

type bulkRowReader struct {
	session isql.Session

	selectStatement isql.PreparedStatement

	// keyColumnIndices is the index of the datums that are part of the primary key.
	keyColumnIndices []int
	columns          []ColumnSchema
}

func newBulkRowReader(
	ctx context.Context, table catalog.TableDescriptor, session isql.Session,
) (*bulkRowReader, error) {
	cols := GetColumnSchema(table)
	keyColumns := make([]int, 0, len(cols))
	for i, col := range cols {
		if col.IsPrimaryKey {
			keyColumns = append(keyColumns, i)
		}
	}

	selectStatementRaw, types, err := newBulkSelectStatement(table)
	if err != nil {
		return nil, err
	}
	selectStatement, err := session.Prepare(ctx, fmt.Sprintf("replication-read-refresh-%d", table.GetID()), selectStatementRaw, types)
	if err != nil {
		return nil, err
	}

	return &bulkRowReader{
		session:          session,
		selectStatement:  selectStatement,
		keyColumnIndices: keyColumns,
		columns:          cols,
	}, nil
}

func (r *bulkRowReader) ReadRows(
	ctx context.Context, rows []tree.Datums,
) (map[int]PriorRow, error) {
	// TODO(jeffswenson): optimize allocations. It may require a change to the
	// API. For now, this probably isn't a performance bottleneck because:
	// 1. Many of the allocations are one per batch instead of one per row.
	// 2. This query is only used if one of the replicated row's prior value does
	// not match the local value.

	if len(rows) == 0 {
		return nil, nil
	}

	params := make([]tree.Datum, 0, len(r.keyColumnIndices))
	for _, index := range r.keyColumnIndices {
		array := tree.NewDArray(r.columns[index].ColumnType)
		for _, row := range rows {
			if err := array.Append(row[index]); err != nil {
				return nil, err
			}
		}
		params = append(params, array)
	}

	// Execute the query using QueryPrepared which returns all rows at once.
	// This is okay since we already know the batch is small enough to fit in
	// memory.
	rows, err := r.session.QueryPrepared(ctx, r.selectStatement, params)
	if err != nil {
		return nil, err
	}

	result := make(map[int]PriorRow, len(rows))
	for _, row := range rows {
		// The extra columns are:
		// 0. The row index (used to match input rows to refreshed rows).
		// 1. The origin timestamp.
		// 2. The mvcc timestamp.
		const prefixColumns = 3
		if len(row) != len(r.columns)+prefixColumns {
			return nil, errors.AssertionFailedf("expected %d columns, got %d", len(r.columns)+3, len(row))
		}

		rowIndex, ok := row[0].(*tree.DInt)
		if !ok {
			return nil, errors.AssertionFailedf("expected column 0 to be the row index")
		}

		isLocal := false
		timestamp := row[1]
		if timestamp == tree.DNull {
			timestamp = row[2]
			isLocal = true
		}

		decimal, ok := timestamp.(*tree.DDecimal)
		if !ok {
			return nil, errors.AssertionFailedf("expected column 1 or 2 to be origin timestamp")
		}

		logicalTimestamp, err := hlc.DecimalToHLC(&decimal.Decimal)
		if err != nil {
			return nil, err
		}

		result[int(*rowIndex)-1] = PriorRow{
			Row:              row[prefixColumns:],
			LogicalTimestamp: logicalTimestamp,
			IsLocal:          isLocal,
		}
	}

	return result, nil
}

type pointReadRowReader struct {
	session isql.Session

	selectStatement isql.PreparedStatement

	// keyColumnIndices is the index of the datums that are part of the primary key.
	keyColumnIndices []int
	columns          []ColumnSchema
}

func newPointRowReader(
	ctx context.Context, table catalog.TableDescriptor, session isql.Session,
) (*pointReadRowReader, error) {
	cols := GetColumnSchema(table)
	keyColumns := make([]int, 0, len(cols))
	for i, col := range cols {
		if col.IsPrimaryKey {
			keyColumns = append(keyColumns, i)
		}
	}

	selectStatementRaw, types, err := newPointSelectStatement(table)
	if err != nil {
		return nil, err
	}
	selectStatement, err := session.Prepare(ctx, fmt.Sprintf("replication-read-point-%d", table.GetID()), selectStatementRaw, types)
	if err != nil {
		return nil, err
	}

	return &pointReadRowReader{
		session:          session,
		selectStatement:  selectStatement,
		keyColumnIndices: keyColumns,
		columns:          cols,
	}, nil
}

func (p *pointReadRowReader) ReadRows(
	ctx context.Context, rows []tree.Datums,
) (map[int]PriorRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	result := make(map[int]PriorRow, len(rows))

	for i, row := range rows {
		params := make([]tree.Datum, 0, len(p.keyColumnIndices))
		for _, keyIndex := range p.keyColumnIndices {
			params = append(params, row[keyIndex])
		}

		queryRows, err := p.session.QueryPrepared(ctx, p.selectStatement, params)
		if err != nil {
			return nil, err
		}

		if len(queryRows) > 1 {
			return nil, errors.AssertionFailedf("expected at most 1 row, got %d", len(queryRows))
		}
		if len(queryRows) == 0 {
			continue
		}

		resultRow := queryRows[0]
		// The columns are:
		// 0. The origin timestamp.
		// 1. The mvcc timestamp.
		// 2+. The table columns.
		const prefixColumns = 2
		if len(resultRow) != len(p.columns)+prefixColumns {
			return nil, errors.AssertionFailedf("expected %d columns, got %d", len(p.columns)+prefixColumns, len(resultRow))
		}

		isLocal := false
		timestamp := resultRow[0]
		if timestamp == tree.DNull {
			timestamp = resultRow[1]
			isLocal = true
		}

		decimal, ok := timestamp.(*tree.DDecimal)
		if !ok {
			return nil, errors.AssertionFailedf("expected column 0 or 1 to be origin timestamp")
		}

		logicalTimestamp, err := hlc.DecimalToHLC(&decimal.Decimal)
		if err != nil {
			return nil, err
		}

		result[i] = PriorRow{
			Row:              resultRow[prefixColumns:],
			LogicalTimestamp: logicalTimestamp,
			IsLocal:          isLocal,
		}
	}

	return result, nil
}
