// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ttljob

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/errors"
)

// selectQueryBuilder is responsible for maintaining state around the
// SELECT portion of the TTL job.
type selectQueryBuilder struct {
	tableID         descpb.ID
	pkColumns       []string
	selectOpName    string
	startPK, endPK  tree.Datums
	selectBatchSize int64
	aost            time.Time
	ttlExpr         catpb.Expression

	// isFirst is true if we have not invoked a query using the builder yet.
	isFirst bool
	// cachedQuery is the cached query, which stays the same from the second
	// iteration onwards.
	cachedQuery string
	// cachedArgs keeps a cache of args to use in the run query.
	// The cache is of form [cutoff, <endFilterClause...>, <startFilterClause..>].
	cachedArgs []interface{}
	// pkColumnNamesSQL caches the column names of the PK.
	pkColumnNamesSQL string
	// endPKColumnNamesSQL caches the column names of the ending PK.
	endPKColumnNamesSQL string
}

func makeSelectQueryBuilder(
	tableID descpb.ID,
	cutoff time.Time,
	pkColumns []string,
	relationName string,
	startPK, endPK tree.Datums,
	aost time.Time,
	selectBatchSize int64,
	ttlExpr catpb.Expression,
) selectQueryBuilder {
	// We will have a maximum of 1 + len(pkColumns)*2 columns, where one
	// is reserved for AOST, and len(pkColumns) for both start and end key.
	cachedArgs := make([]interface{}, 0, 1+len(pkColumns)*2)
	cachedArgs = append(cachedArgs, cutoff)
	for _, d := range endPK {
		cachedArgs = append(cachedArgs, d)
	}
	for _, d := range startPK {
		cachedArgs = append(cachedArgs, d)
	}

	return selectQueryBuilder{
		tableID:         tableID,
		pkColumns:       pkColumns,
		selectOpName:    fmt.Sprintf("ttl select %s", relationName),
		startPK:         startPK,
		endPK:           endPK,
		aost:            aost,
		selectBatchSize: selectBatchSize,
		ttlExpr:         ttlExpr,

		cachedArgs:          cachedArgs,
		isFirst:             true,
		pkColumnNamesSQL:    makeColumnNamesSQL(pkColumns),
		endPKColumnNamesSQL: makeColumnNamesSQL(pkColumns[:len(endPK)]),
	}
}

func (b *selectQueryBuilder) buildQuery() string {
	// Generate the end key clause for SELECT, which always stays the same.
	// Start from $2 as $1 is for the now clause.
	// The end key of a range is exclusive, so use <.
	var endFilterClause string
	if len(b.endPK) > 0 {
		endFilterClause = fmt.Sprintf(" AND (%s) < (", b.endPKColumnNamesSQL)
		for i := range b.endPK {
			if i > 0 {
				endFilterClause += ", "
			}
			endFilterClause += fmt.Sprintf("$%d", i+2)
		}
		endFilterClause += ")"
	}

	var filterClause string
	if !b.isFirst {
		// After the first query, we always want (col1, ...) > (cursor_col_1, ...)
		filterClause = fmt.Sprintf(" AND (%s) > (", b.pkColumnNamesSQL)
		for i := range b.pkColumns {
			if i > 0 {
				filterClause += ", "
			}
			// We start from 2 if we don't have an endPK clause, but add len(b.endPK)
			// if there is.
			filterClause += fmt.Sprintf("$%d", 2+len(b.endPK)+i)
		}
		filterClause += ")"
	} else if len(b.startPK) > 0 {
		// For the the first query, we want (col1, ...) >= (cursor_col_1, ...)
		filterClause = fmt.Sprintf(" AND (%s) >= (", makeColumnNamesSQL(b.pkColumns[:len(b.startPK)]))
		for i := range b.startPK {
			if i > 0 {
				filterClause += ", "
			}
			// We start from 2 if we don't have an endPK clause, but add len(b.endPK)
			// if there is.
			filterClause += fmt.Sprintf("$%d", 2+len(b.endPK)+i)
		}
		filterClause += ")"
	}

	return fmt.Sprintf(
		`SELECT %[1]s FROM [%[2]d AS tbl_name]
AS OF SYSTEM TIME %[3]s
WHERE %[4]s <= $1%[5]s%[6]s
ORDER BY %[1]s
LIMIT %[7]d`,
		b.pkColumnNamesSQL,
		b.tableID,
		tree.MustMakeDTimestampTZ(b.aost, time.Microsecond),
		b.ttlExpr,
		filterClause,
		endFilterClause,
		b.selectBatchSize,
	)
}

func (b *selectQueryBuilder) nextQuery() (string, []interface{}) {
	if b.isFirst {
		q := b.buildQuery()
		b.isFirst = false
		return q, b.cachedArgs
	}
	// All subsequent query strings are the same.
	// Populate the cache once, and then maintain it for all subsequent calls.
	if b.cachedQuery == "" {
		b.cachedQuery = b.buildQuery()
	}
	return b.cachedQuery, b.cachedArgs
}

func (b *selectQueryBuilder) run(
	ctx context.Context, ie *sql.InternalExecutor,
) ([]tree.Datums, error) {
	q, args := b.nextQuery()

	// Use a nil txn so that the AOST clause is handled correctly. Currently,
	// the internal executor will treat a passed-in txn as an explicit txn, so
	// the AOST clause on the SELECT query would not be interpreted correctly.
	qosLevel := sessiondatapb.TTLLow
	ret, err := ie.QueryBufferedEx(
		ctx,
		b.selectOpName,
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.RootUserName(),
			QualityOfService: &qosLevel,
		},
		q,
		args...,
	)
	if err != nil {
		return nil, err
	}
	if err := b.moveCursor(ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *selectQueryBuilder) moveCursor(rows []tree.Datums) error {
	// Move the cursor forward.
	if len(rows) > 0 {
		lastRow := rows[len(rows)-1]
		b.cachedArgs = b.cachedArgs[:1+len(b.endPK)]
		if len(lastRow) != len(b.pkColumns) {
			return errors.AssertionFailedf("expected %d columns for last row, got %d", len(b.pkColumns), len(lastRow))
		}
		for _, d := range lastRow {
			b.cachedArgs = append(b.cachedArgs, d)
		}
	}
	return nil
}

// deleteQueryBuilder is responsible for maintaining state around the
// SELECT portion of the TTL job.
type deleteQueryBuilder struct {
	tableID         descpb.ID
	pkColumns       []string
	deleteBatchSize int64
	deleteOpName    string
	ttlExpr         catpb.Expression

	// cachedQuery is the cached query, which stays the same as long as we are
	// deleting up to deleteBatchSize elements.
	cachedQuery string
	// cachedArgs keeps a cache of args to use in the run query.
	// The cache is of form [cutoff, flattened PKs...].
	cachedArgs []interface{}
}

func makeDeleteQueryBuilder(
	tableID descpb.ID,
	cutoff time.Time,
	pkColumns []string,
	relationName string,
	deleteBatchSize int64,
	ttlExpr catpb.Expression,
) deleteQueryBuilder {
	cachedArgs := make([]interface{}, 0, 1+int64(len(pkColumns))*deleteBatchSize)
	cachedArgs = append(cachedArgs, cutoff)

	return deleteQueryBuilder{
		tableID:         tableID,
		pkColumns:       pkColumns,
		deleteBatchSize: deleteBatchSize,
		deleteOpName:    fmt.Sprintf("ttl delete %s", relationName),
		ttlExpr:         ttlExpr,
		cachedArgs:      cachedArgs,
	}
}

func (b *deleteQueryBuilder) buildQuery(numRows int) string {
	columnNamesSQL := makeColumnNamesSQL(b.pkColumns)
	var placeholderStr string
	for i := 0; i < numRows; i++ {
		if i > 0 {
			placeholderStr += ", "
		}
		placeholderStr += "("
		for j := 0; j < len(b.pkColumns); j++ {
			if j > 0 {
				placeholderStr += ", "
			}
			placeholderStr += fmt.Sprintf("$%d", 2+i*len(b.pkColumns)+j)
		}
		placeholderStr += ")"
	}

	return fmt.Sprintf(
		`DELETE FROM [%d AS tbl_name] WHERE %s <= $1 AND (%s) IN (%s)`,
		b.tableID,
		b.ttlExpr,
		columnNamesSQL,
		placeholderStr,
	)
}

func (b *deleteQueryBuilder) buildQueryAndArgs(rows []tree.Datums) (string, []interface{}) {
	var q string
	if int64(len(rows)) == b.deleteBatchSize {
		if b.cachedQuery == "" {
			b.cachedQuery = b.buildQuery(len(rows))
		}
		q = b.cachedQuery
	} else {
		q = b.buildQuery(len(rows))
	}
	deleteArgs := b.cachedArgs[:1]
	for _, row := range rows {
		for _, col := range row {
			deleteArgs = append(deleteArgs, col)
		}
	}
	return q, deleteArgs
}

func (b *deleteQueryBuilder) run(
	ctx context.Context, ie *sql.InternalExecutor, txn *kv.Txn, rows []tree.Datums,
) (int, error) {
	q, deleteArgs := b.buildQueryAndArgs(rows)
	qosLevel := sessiondatapb.TTLLow
	return ie.ExecEx(
		ctx,
		b.deleteOpName,
		txn,
		sessiondata.InternalExecutorOverride{
			User:             username.RootUserName(),
			QualityOfService: &qosLevel,
		},
		q,
		deleteArgs...,
	)
}

// makeColumnNamesSQL converts columns into an escape string
// for an order by clause, e.g.:
//   {"a", "b"} => a, b
//   {"escape-me", "b"} => "escape-me", b
func makeColumnNamesSQL(columns []string) string {
	var b bytes.Buffer
	for i, pkColumn := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		lexbase.EncodeRestrictedSQLIdent(&b, pkColumn, lexbase.EncNoFlags)
	}
	return b.String()
}
