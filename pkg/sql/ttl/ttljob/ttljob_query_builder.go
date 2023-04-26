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

// QueryBounds stores the start and end bounds for the SELECT query that the
// SelectQueryBuilder will run.
type QueryBounds struct {
	// Start represent the lower bounds in the SELECT statement. After each
	// SelectQueryBuilder.Run, the start bounds increase to exclude the rows
	// selected in the previous SelectQueryBuilder.Run.
	//
	// For the first SELECT in a span, the start bounds are inclusive because the
	// start bounds are based on the first row >= Span.Key. That row must be
	// included in the first SELECT. For subsequent SELECTS, the start bounds
	// are exclusive to avoid re-selecting the last row from the previous SELECT.
	Start tree.Datums
	// End represents the upper bounds in the SELECT statement. The end bounds
	// never change between each SelectQueryBuilder.Run.
	//
	// For all SELECTS in a span, the end bounds are inclusive even though a
	// span's end key is exclusive because the end bounds are based on the first
	// row < Span.EndKey.
	End tree.Datums
}

// SelectQueryBuilder is responsible for maintaining state around the SELECT
// portion of the TTL job.
type SelectQueryBuilder struct {
	relationName    string
	pkColNames      []string
	pkColDirs       []catenumpb.IndexColumn_Direction
	selectOpName    string
	bounds          QueryBounds
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

func MakeSelectQueryBuilder(
	cutoff time.Time,
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	relationName string,
	bounds QueryBounds,
	aostDuration time.Duration,
	selectBatchSize int64,
	ttlExpr catpb.Expression,
) SelectQueryBuilder {
	numPkCols := len(pkColNames)
	if numPkCols == 0 {
		panic("pkColNames is empty")
	}
	if numPkCols != len(pkColDirs) {
		panic("different number of pkColNames and pkColDirs")
	}
	// We will have a maximum of 1 + len(pkColNames)*2 columns, where one
	// is reserved for AOST, and len(pkColNames) for both start and end key.
	cachedArgs := make([]interface{}, 0, 1+numPkCols*2)
	cachedArgs = append(cachedArgs, cutoff)
	endPK := bounds.End
	for _, d := range endPK {
		cachedArgs = append(cachedArgs, d)
	}
	startPK := bounds.Start
	for _, d := range startPK {
		cachedArgs = append(cachedArgs, d)
	}

	return SelectQueryBuilder{
		relationName:    relationName,
		pkColNames:      pkColNames,
		pkColDirs:       pkColDirs,
		selectOpName:    fmt.Sprintf("ttl select %s", relationName),
		bounds:          bounds,
		aostDuration:    aostDuration,
		selectBatchSize: selectBatchSize,
		ttlExpr:         ttlExpr,

		cachedArgs: cachedArgs,
		isFirst:    true,
	}
}

func (b *SelectQueryBuilder) buildQuery() string {
	return ttlbase.BuildSelectQuery(
		b.relationName,
		b.pkColNames,
		b.pkColDirs,
		b.aostDuration,
		b.ttlExpr,
		len(b.bounds.Start),
		len(b.bounds.End),
		b.selectBatchSize,
		b.isFirst,
	)
}

var qosLevel = sessiondatapb.TTLLow

func (b *SelectQueryBuilder) Run(
	ctx context.Context, ie isql.Executor,
) (_ []tree.Datums, hasNext bool, _ error) {
	var query string
	if b.isFirst {
		query = b.buildQuery()
		b.isFirst = false
	} else {
		if b.cachedQuery == "" {
			b.cachedQuery = b.buildQuery()
		}
		query = b.cachedQuery
	}

	// Use a nil txn so that the AOST clause is handled correctly. Currently,
	// the internal executor will treat a passed-in txn as an explicit txn, so
	// the AOST clause on the SELECT query would not be interpreted correctly.
	rows, err := ie.QueryBufferedEx(
		ctx,
		b.selectOpName,
		nil, /* txn */
		sessiondata.InternalExecutorOverride{
			User:             username.RootUserName(),
			QualityOfService: &qosLevel,
		},
		query,
		b.cachedArgs...,
	)
	if err != nil {
		return nil, false, err
	}

	numRows := int64(len(rows))
	if numRows > 0 {
		// Move the cursor forward if SELECT returns rows.
		lastRow := rows[numRows-1]
		if len(lastRow) != len(b.pkColNames) {
			return nil, false, errors.AssertionFailedf("expected %d columns for last row, got %d", len(b.pkColNames), len(lastRow))
		}
		b.cachedArgs = b.cachedArgs[:len(b.cachedArgs)-len(b.bounds.Start)]
		for _, d := range lastRow {
			b.cachedArgs = append(b.cachedArgs, d)
		}
		b.bounds.Start = lastRow
	}

	return rows, numRows == b.selectBatchSize, nil
}

// DeleteQueryBuilder is responsible for maintaining state around the DELETE
// portion of the TTL job.
type DeleteQueryBuilder struct {
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

func MakeDeleteQueryBuilder(
	cutoff time.Time,
	pkColNames []string,
	relationName string,
	deleteBatchSize int64,
	ttlExpr catpb.Expression,
) DeleteQueryBuilder {
	if len(pkColNames) == 0 {
		panic("pkColNames is empty")
	}
	cachedArgs := make([]interface{}, 0, 1+int64(len(pkColNames))*deleteBatchSize)
	cachedArgs = append(cachedArgs, cutoff)

	return DeleteQueryBuilder{
		relationName:    relationName,
		pkColNames:      pkColNames,
		deleteBatchSize: deleteBatchSize,
		deleteOpName:    fmt.Sprintf("ttl delete %s", relationName),
		ttlExpr:         ttlExpr,
		cachedArgs:      cachedArgs,
	}
}

func (b *DeleteQueryBuilder) buildQuery(numRows int) string {
	return ttlbase.BuildDeleteQuery(
		b.relationName,
		b.pkColNames,
		b.ttlExpr,
		numRows,
	)
}

func (b *DeleteQueryBuilder) Run(
	ctx context.Context, txn isql.Txn, rows []tree.Datums,
) (int64, error) {
	numRows := len(rows)
	var query string
	if int64(numRows) == b.deleteBatchSize {
		if b.cachedQuery == "" {
			b.cachedQuery = b.buildQuery(numRows)
		}
		query = b.cachedQuery
	} else {
		query = b.buildQuery(numRows)
	}

	deleteArgs := b.cachedArgs[:1]
	for _, row := range rows {
		for _, col := range row {
			deleteArgs = append(deleteArgs, col)
		}
	}

	rowCount, err := txn.ExecEx(
		ctx,
		b.deleteOpName,
		txn.KV(),
		sessiondata.InternalExecutorOverride{
			User:             username.RootUserName(),
			QualityOfService: &qosLevel,
		},
		query,
		deleteArgs...,
	)
	return int64(rowCount), err
}
