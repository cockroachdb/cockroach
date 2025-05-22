// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

type SelectQueryParams struct {
	RelationName      string
	PKColNames        []string
	PKColDirs         []catenumpb.IndexColumn_Direction
	PKColTypes        []*types.T
	Bounds            QueryBounds
	AOSTDuration      time.Duration
	SelectBatchSize   int64
	TTLExpr           catpb.Expression
	SelectDuration    *aggmetric.Histogram
	SelectRateLimiter *quotapool.RateLimiter
}

type SelectQueryBuilder interface {
	// Run will perform the SELECT operation and return the rows.
	Run(ctx context.Context, ie isql.Executor) (_ []tree.Datums, hasNext bool, _ error)

	// BuildQuery will generate the SELECT query for the given builder.
	BuildQuery() string
}

// SelectQueryBuilder is responsible for maintaining state around the SELECT
// portion of the TTL job.
type selectQueryBuilder struct {
	SelectQueryParams
	selectOpName redact.RedactableString
	// isFirst is true if we have not invoked a query using the builder yet.
	isFirst bool
	// cachedQuery is the cached query, which stays the same from the second
	// iteration onwards.
	cachedQuery string
	// cachedArgs keeps a cache of args to use in the run query.
	// The cache is of form [cutoff, <endFilterClause...>, <startFilterClause..>].
	cachedArgs []interface{}
}

func MakeSelectQueryBuilder(params SelectQueryParams, cutoff time.Time) SelectQueryBuilder {
	numPkCols := len(params.PKColNames)
	if numPkCols == 0 {
		panic("PKColNames is empty")
	}
	if numPkCols != len(params.PKColDirs) {
		panic("different number of PKColNames and PKColDirs")
	}
	// We will have a maximum of 1 + len(PKColNames)*2 columns, where one
	// is reserved for AOST, and len(PKColNames) for both start and end key.
	cachedArgs := make([]interface{}, 0, 1+numPkCols*2)
	cachedArgs = append(cachedArgs, cutoff)
	for _, d := range params.Bounds.End {
		cachedArgs = append(cachedArgs, d)
	}
	for _, d := range params.Bounds.Start {
		cachedArgs = append(cachedArgs, d)
	}

	return &selectQueryBuilder{
		SelectQueryParams: params,
		selectOpName:      redact.Sprintf("ttl select %s", params.RelationName),
		cachedArgs:        cachedArgs,
		isFirst:           true,
	}
}

// BuildQuery implements the SelectQueryBuilder interface.
func (b *selectQueryBuilder) BuildQuery() string {
	return ttlbase.BuildSelectQuery(
		b.RelationName,
		b.PKColNames,
		b.PKColDirs,
		b.PKColTypes,
		b.AOSTDuration,
		b.TTLExpr,
		len(b.Bounds.Start),
		len(b.Bounds.End),
		b.SelectBatchSize,
		b.isFirst,
	)
}

func getInternalExecutorOverride(
	qosLevel sessiondatapb.QoSLevel,
) sessiondata.InternalExecutorOverride {
	return sessiondata.InternalExecutorOverride{
		User:                   username.NodeUserName(),
		QualityOfService:       &qosLevel,
		OptimizerUseHistograms: true,
	}
}

// Run implements the SelectQueryBuilder interface.
func (b *selectQueryBuilder) Run(
	ctx context.Context, ie isql.Executor,
) (_ []tree.Datums, hasNext bool, _ error) {
	var query string
	if b.isFirst {
		query = b.BuildQuery()
		b.isFirst = false
	} else {
		if b.cachedQuery == "" {
			b.cachedQuery = b.BuildQuery()
		}
		query = b.cachedQuery
	}
	// Convert any DEnum args to their logical representation to avoid the risk
	// of using the wrong version of the enum type descriptor.
	for i, arg := range b.cachedArgs {
		if enum, ok := arg.(*tree.DEnum); ok {
			b.cachedArgs[i] = enum.LogicalRep
		}
	}

	tokens, err := b.SelectRateLimiter.Acquire(ctx, b.SelectBatchSize)
	if err != nil {
		return nil, false, err
	}
	defer tokens.Consume()

	start := timeutil.Now()
	// Use a nil txn so that the AOST clause is handled correctly. Currently,
	// the internal executor will treat a passed-in txn as an explicit txn, so
	// the AOST clause on the SELECT query would not be interpreted correctly.
	rows, err := ie.QueryBufferedEx(
		ctx,
		b.selectOpName,
		nil, /* txn */
		getInternalExecutorOverride(sessiondatapb.BulkLowQoS),
		query,
		b.cachedArgs...,
	)
	if err != nil {
		return nil, false, err
	}
	b.SelectDuration.RecordValue(int64(timeutil.Since(start)))

	numRows := int64(len(rows))
	if numRows > 0 {
		// Move the cursor forward if SELECT returns rows.
		lastRow := rows[numRows-1]
		if len(lastRow) != len(b.PKColNames) {
			return nil, false, errors.AssertionFailedf("expected %d columns for last row, got %d", len(b.PKColNames), len(lastRow))
		}
		b.cachedArgs = b.cachedArgs[:len(b.cachedArgs)-len(b.Bounds.Start)]
		for _, d := range lastRow {
			b.cachedArgs = append(b.cachedArgs, d)
		}
		b.Bounds.Start = lastRow
	}

	return rows, numRows == b.SelectBatchSize, nil
}

type DeleteQueryParams struct {
	RelationName      string
	PKColNames        []string
	DeleteBatchSize   int64
	TTLExpr           catpb.Expression
	DeleteDuration    *aggmetric.Histogram
	DeleteRateLimiter *quotapool.RateLimiter
}

// DeleteQueryBuilder is responsible for maintaining state around the DELETE
// portion of the TTL job.
type DeleteQueryBuilder interface {
	// Run will perform the DELETE operation on the given rows.
	Run(ctx context.Context, txn isql.Txn, rows []tree.Datums) (int64, error)

	// BuildQuery generates the DELETE query for the given number of rows.
	BuildQuery(numRows int) string

	// GetBatchSize returns the batch size for the DELETE operation.
	GetBatchSize() int
}

type deleteQueryBuilder struct {
	DeleteQueryParams
	deleteOpName redact.RedactableString
	// cachedQuery is the cached query, which stays the same as long as we are
	// deleting up to DeleteBatchSize elements.
	cachedQuery string
	// cachedArgs keeps a cache of args to use in the run query.
	// The cache is of form [cutoff, flattened PKs...].
	cachedArgs []interface{}
}

func MakeDeleteQueryBuilder(params DeleteQueryParams, cutoff time.Time) DeleteQueryBuilder {
	if len(params.PKColNames) == 0 {
		panic("PKColNames is empty")
	}
	cachedArgs := make([]interface{}, 0, 1+int64(len(params.PKColNames))*params.DeleteBatchSize)
	cachedArgs = append(cachedArgs, cutoff)

	return &deleteQueryBuilder{
		DeleteQueryParams: params,
		deleteOpName:      redact.Sprintf("ttl delete %s", params.RelationName),
		cachedArgs:        cachedArgs,
	}
}

func (b *deleteQueryBuilder) BuildQuery(numRows int) string {
	return ttlbase.BuildDeleteQuery(
		b.RelationName,
		b.PKColNames,
		b.TTLExpr,
		numRows,
	)
}

// GetBatchSize implements the DeleteQueryBuilder interface.
func (b *deleteQueryBuilder) GetBatchSize() int {
	return int(b.DeleteBatchSize)
}

// Run implements the DeleteQueryBuilder interface.
func (b *deleteQueryBuilder) Run(
	ctx context.Context, txn isql.Txn, rows []tree.Datums,
) (int64, error) {
	numRows := len(rows)
	var query string
	if int64(numRows) == b.DeleteBatchSize {
		if b.cachedQuery == "" {
			b.cachedQuery = b.BuildQuery(numRows)
		}
		query = b.cachedQuery
	} else {
		query = b.BuildQuery(numRows)
	}

	deleteArgs := b.cachedArgs[:1]
	for _, row := range rows {
		for _, col := range row {
			// Convert any DEnum args to their logical representation to avoid the risk
			// of using the wrong version of the enum type descriptor.
			if enum, ok := col.(*tree.DEnum); ok {
				deleteArgs = append(deleteArgs, enum.LogicalRep)
			} else {
				deleteArgs = append(deleteArgs, col)
			}
		}
	}

	tokens, err := b.DeleteRateLimiter.Acquire(ctx, int64(numRows))
	if err != nil {
		return 0, err
	}
	defer tokens.Consume()

	start := timeutil.Now()
	rowCount, err := txn.ExecEx(
		ctx,
		b.deleteOpName,
		txn.KV(),
		getInternalExecutorOverride(sessiondatapb.BulkLowQoS),
		query,
		deleteArgs...,
	)
	if err != nil {
		return 0, err
	}
	b.DeleteDuration.RecordValue(int64(timeutil.Since(start)))
	return int64(rowCount), nil
}
