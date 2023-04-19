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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
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
	relationName    string
	pkColNames      []string
	pkColDirs       []catenumpb.IndexColumn_Direction
	selectOpName    string
	queryBounds     ttlbase.QueryBounds
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
}

func makeSelectQueryBuilder(
	cutoff time.Time,
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	relationName string,
	queryBounds ttlbase.QueryBounds,
	aostDuration time.Duration,
	selectBatchSize int64,
	ttlExpr catpb.Expression,
) selectQueryBuilder {
	// We will have a maximum of 1 + len(pkColNames)*2 columns, where one
	// is reserved for AOST, and len(pkColNames) for both start and end key.
	cachedArgs := make([]interface{}, 0, 1+len(pkColNames)*2)
	cachedArgs = append(cachedArgs, cutoff)
	endPK := queryBounds.End
	for _, d := range endPK {
		cachedArgs = append(cachedArgs, d)
	}
	startPK := queryBounds.Start
	for _, d := range startPK {
		cachedArgs = append(cachedArgs, d)
	}

	return selectQueryBuilder{
		relationName:    relationName,
		pkColNames:      pkColNames,
		pkColDirs:       pkColDirs,
		selectOpName:    fmt.Sprintf("ttl select %s", relationName),
		queryBounds:     queryBounds,
		aostDuration:    aostDuration,
		selectBatchSize: selectBatchSize,
		ttlExpr:         ttlExpr,

		cachedArgs: cachedArgs,
		isFirst:    true,
	}
}

func (b *selectQueryBuilder) nextQuery() (string, []interface{}) {
	if b.isFirst {
		q := ttlbase.BuildSelectQuery(
			b.relationName,
			b.pkColNames,
			b.pkColDirs,
			b.aostDuration,
			b.ttlExpr,
			b.queryBounds,
			b.selectBatchSize,
			b.isFirst,
		)
		b.isFirst = false
		return q, b.cachedArgs
	}
	// All subsequent query strings are the same.
	// Populate the cache once, and then maintain it for all subsequent calls.
	if b.cachedQuery == "" {
		b.cachedQuery = ttlbase.BuildSelectQuery(
			b.relationName,
			b.pkColNames,
			b.pkColDirs,
			b.aostDuration,
			b.ttlExpr,
			b.queryBounds,
			b.selectBatchSize,
			b.isFirst,
		)
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
		b.cachedArgs = b.cachedArgs[:1+len(b.queryBounds.End)]
		if len(lastRow) != len(b.pkColNames) {
			return errors.AssertionFailedf("expected %d columns for last row, got %d", len(b.pkColNames), len(lastRow))
		}
		for _, d := range lastRow {
			b.cachedArgs = append(b.cachedArgs, d)
		}
		b.queryBounds.Start = lastRow
	}
	return nil
}

// deleteQueryBuilder is responsible for maintaining state around the
// SELECT portion of the TTL job.
type deleteQueryBuilder struct {
	relationName    string
	pkColNames      []string
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
	cutoff time.Time,
	pkColNames []string,
	relationName string,
	deleteBatchSize int64,
	ttlExpr catpb.Expression,
) deleteQueryBuilder {
	cachedArgs := make([]interface{}, 0, 1+int64(len(pkColNames))*deleteBatchSize)
	cachedArgs = append(cachedArgs, cutoff)

	return deleteQueryBuilder{
		relationName:    relationName,
		pkColNames:      pkColNames,
		deleteBatchSize: deleteBatchSize,
		deleteOpName:    fmt.Sprintf("ttl delete %s", relationName),
		ttlExpr:         ttlExpr,
		cachedArgs:      cachedArgs,
	}
}

func (b *deleteQueryBuilder) buildQueryAndArgs(rows []tree.Datums) (string, []interface{}) {
	var q string
	if int64(len(rows)) == b.deleteBatchSize {
		if b.cachedQuery == "" {
			b.cachedQuery = ttlbase.BuildDeleteQuery(
				b.relationName,
				b.pkColNames,
				b.ttlExpr,
				len(rows),
			)
		}
		q = b.cachedQuery
	} else {
		q = ttlbase.BuildDeleteQuery(
			b.relationName,
			b.pkColNames,
			b.ttlExpr,
			len(rows),
		)
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
