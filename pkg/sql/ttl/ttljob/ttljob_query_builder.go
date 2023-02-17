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
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/errors"
)

// selectQueryBuilder is responsible for maintaining state around the
// SELECT portion of the TTL job.
type selectQueryBuilder struct {
	tableID         descpb.ID
	pkColumns       []string
	selectOpName    string
	spanToProcess   spanToProcess
	selectBatchSize int64
	aostDuration    time.Duration
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

type spanToProcess struct {
	startPK, endPK tree.Datums
}

func makeSelectQueryBuilder(
	tableID descpb.ID,
	cutoff time.Time,
	pkColumns []string,
	relationName string,
	spanToProcess spanToProcess,
	aostDuration time.Duration,
	selectBatchSize int64,
	ttlExpr catpb.Expression,
) selectQueryBuilder {
	// We will have a maximum of 1 + len(pkColumns)*2 columns, where one
	// is reserved for AOST, and len(pkColumns) for both start and end key.
	cachedArgs := make([]interface{}, 0, 1+len(pkColumns)*2)
	cachedArgs = append(cachedArgs, cutoff)
	endPK := spanToProcess.endPK
	for _, d := range endPK {
		cachedArgs = append(cachedArgs, d)
	}
	startPK := spanToProcess.startPK
	for _, d := range startPK {
		cachedArgs = append(cachedArgs, d)
	}

	return selectQueryBuilder{
		tableID:         tableID,
		pkColumns:       pkColumns,
		selectOpName:    fmt.Sprintf("ttl select %s", relationName),
		spanToProcess:   spanToProcess,
		aostDuration:    aostDuration,
		selectBatchSize: selectBatchSize,
		ttlExpr:         ttlExpr,

		cachedArgs:          cachedArgs,
		isFirst:             true,
		pkColumnNamesSQL:    ttlbase.MakeColumnNamesSQL(pkColumns),
		endPKColumnNamesSQL: ttlbase.MakeColumnNamesSQL(pkColumns[:len(endPK)]),
	}
}

func (b *selectQueryBuilder) buildQuery() string {
	// Generate the end key clause for SELECT, which always stays the same.
	// Start from $2 as $1 is for the now clause.
	// The end key of a span is exclusive, so use <.
	var endFilterClause string
	endPK := b.spanToProcess.endPK
	if len(endPK) > 0 {
		endFilterClause = fmt.Sprintf(" AND (%s) < (", b.endPKColumnNamesSQL)
		for i := range endPK {
			if i > 0 {
				endFilterClause += ", "
			}
			endFilterClause += fmt.Sprintf("$%d", i+2)
		}
		endFilterClause += ")"
	}

	startPK := b.spanToProcess.startPK
	var filterClause string
	if !b.isFirst {
		// After the first query, we always want (col1, ...) > (cursor_col_1, ...)
		filterClause = fmt.Sprintf("AND (%s) > (", b.pkColumnNamesSQL)
		for i := range b.pkColumns {
			if i > 0 {
				filterClause += ", "
			}
			// We start from 2 if we don't have an endPK clause, but add len(b.endPK)
			// if there is.
			filterClause += fmt.Sprintf("$%d", 2+len(endPK)+i)
		}
		filterClause += ")"
	} else if len(startPK) > 0 {
		// For the the first query, we want (col1, ...) >= (cursor_col_1, ...)
		filterClause = fmt.Sprintf("AND (%s) >= (", ttlbase.MakeColumnNamesSQL(b.pkColumns[:len(startPK)]))
		for i := range startPK {
			if i > 0 {
				filterClause += ", "
			}
			// We start from 2 if we don't have an endPK clause, but add len(b.endPK)
			// if there is.
			filterClause += fmt.Sprintf("$%d", 2+len(endPK)+i)
		}
		filterClause += ")"
	}

	return fmt.Sprintf(
		ttlbase.SelectTemplate,
		b.pkColumnNamesSQL,
		b.tableID,
		int64(b.aostDuration.Seconds()),
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

func (b *selectQueryBuilder) run(ctx context.Context, ie isql.Executor) ([]tree.Datums, error) {
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
		b.cachedArgs = b.cachedArgs[:1+len(b.spanToProcess.endPK)]
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
	columnNamesSQL := ttlbase.MakeColumnNamesSQL(b.pkColumns)
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
		ttlbase.DeleteTemplate,
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
	ctx context.Context, txn isql.Txn, rows []tree.Datums,
) (int64, error) {
	q, deleteArgs := b.buildQueryAndArgs(rows)
	qosLevel := sessiondatapb.TTLLow
	rowCount, err := txn.ExecEx(
		ctx,
		b.deleteOpName,
		txn.KV(),
		sessiondata.InternalExecutorOverride{
			User:             username.RootUserName(),
			QualityOfService: &qosLevel,
		},
		q,
		deleteArgs...,
	)
	return int64(rowCount), err
}
