// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttljob"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/stretchr/testify/require"
)

const (
	relationName = "defaultdb.relation_name"
	ttlColName   = "expire_at"
)

var (
	cutoff   = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	expireAt = cutoff.AddDate(-1, 0, 0)
)

func genCreateTableStatement(
	pkColNames []string, pkColDirs []catenumpb.IndexColumn_Direction,
) string {
	numPKCols := len(pkColNames)
	colDefs := make([]string, 0, numPKCols+1)
	for i := range pkColNames {
		colDefs = append(colDefs, pkColNames[i]+" int")
	}
	colDefs = append(colDefs, ttlColName+" timestamptz")
	pkColDefs := make([]string, 0, numPKCols)
	for i := range pkColNames {
		var pkColDir catenumpb.IndexColumn_Direction
		if pkColDirs != nil {
			pkColDir = pkColDirs[i]
		}
		pkColDefs = append(pkColDefs, pkColNames[i]+" "+pkColDir.String())
	}
	return fmt.Sprintf(
		"CREATE TABLE %s (%s, PRIMARY KEY(%s))",
		relationName, strings.Join(colDefs, ", "), strings.Join(pkColDefs, ", "),
	)
}

func genInsertStatement(values []string) string {
	return fmt.Sprintf(
		"INSERT INTO %s VALUES %s",
		relationName, strings.Join(values, ", "),
	)
}

func TestSelectQueryBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	intsToDatums := func(ints ...int) tree.Datums {
		datums := make(tree.Datums, 0, len(ints))
		for _, i := range ints {
			datums = append(datums, tree.NewDInt(tree.DInt(i)))
		}
		return datums
	}

	testCases := []struct {
		desc      string
		pkColDirs []catenumpb.IndexColumn_Direction
		numRows   int
		bounds    ttljob.QueryBounds
		// [iteration][row][val]
		iterations [][][]int
	}{
		{
			desc: "ASC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			iterations: [][][]int{
				{
					{0},
					{1},
				},
				{},
			},
		},
		{
			desc: "DESC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			iterations: [][][]int{
				{
					{1},
					{0},
				},
				{},
			},
		},
		{
			desc: "ASC partial last result",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			numRows: 1,
			iterations: [][][]int{
				{
					{0},
				},
			},
		},
		{
			desc: "DESC partial last result",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			numRows: 1,
			iterations: [][][]int{
				{
					{0},
				},
			},
		},
		{
			desc: "ASC start bounds",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			bounds: ttljob.QueryBounds{
				Start: intsToDatums(1),
			},
			iterations: [][][]int{
				{
					{1},
				},
			},
		},
		{
			desc: "ASC end bounds",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			bounds: ttljob.QueryBounds{
				End: intsToDatums(0),
			},
			iterations: [][][]int{
				{
					{0},
				},
			},
		},
		{
			desc: "DESC start bounds",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			bounds: ttljob.QueryBounds{
				Start: intsToDatums(0),
			},
			iterations: [][][]int{
				{
					{0},
				},
			},
		},
		{
			desc: "DESC end bounds",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			bounds: ttljob.QueryBounds{
				End: intsToDatums(1),
			},
			iterations: [][][]int{
				{
					{1},
				},
			},
		},
		{
			desc: "ASC ASC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
			},
			iterations: [][][]int{
				{
					{0, 0},
					{0, 1},
				},
				{
					{1, 0},
					{1, 1},
				},
				{},
			},
		},
		{
			desc: "DESC DESC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_DESC,
			},
			iterations: [][][]int{
				{
					{1, 1},
					{1, 0},
				},
				{
					{0, 1},
					{0, 0},
				},
				{},
			},
		},
		{
			desc: "ASC DESC ASC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_ASC,
			},
			iterations: [][][]int{
				{
					{0, 1, 0},
					{0, 1, 1},
				},
				{
					{0, 0, 0},
					{0, 0, 1},
				},
				{
					{1, 1, 0},
					{1, 1, 1},
				},
				{
					{1, 0, 0},
					{1, 0, 1},
				},
				{},
			},
		},
		{
			desc: "DESC ASC DESC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_DESC,
			},
			iterations: [][][]int{
				{
					{1, 0, 1},
					{1, 0, 0},
				},
				{
					{1, 1, 1},
					{1, 1, 0},
				},
				{
					{0, 0, 1},
					{0, 0, 0},
				},
				{
					{0, 1, 1},
					{0, 1, 0},
				},
				{},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer srv.Stopper().Stop(ctx)

			ie := srv.ApplicationLayer().InternalExecutor().(*sql.InternalExecutor)

			// Generate PKColNames.
			pkColDirs := tc.pkColDirs
			numPKCols := len(pkColDirs)
			pkColNames := ttlbase.GenPKColNames(numPKCols)

			// Run CREATE TABLE statement.
			createTableStatement := genCreateTableStatement(pkColNames, pkColDirs)
			_, err := ie.Exec(ctx, "create ttl table", nil, createTableStatement)
			require.NoError(t, err)

			// Run INSERT statement.
			numRows := tc.numRows
			maxNumRows := 2 << (numPKCols - 1)
			if numRows == 0 {
				numRows = maxNumRows
			} else if numRows > maxNumRows {
				panic("numRows must be less than maxNumRows")
			}
			values := make([]string, 0, numRows)
			for i := 0; i < numRows; i++ {
				format := fmt.Sprintf("%%0%db", numPKCols)
				number := fmt.Sprintf(format, i)
				value := make([]string, 0, numPKCols+1)
				for j := 0; j < numPKCols; j++ {
					value = append(value, string(number[j]))
				}
				value = append(value, "'"+expireAt.Format(time.RFC3339)+"'")
				values = append(values, "("+strings.Join(value, ", ")+")")
			}
			insertStatement := genInsertStatement(values)
			_, err = ie.Exec(ctx, "insert ttl table", nil, insertStatement)
			require.NoError(t, err)

			// Setup SelectQueryBuilder.
			queryBuilder := ttljob.MakeSelectQueryBuilder(
				ttljob.SelectQueryParams{
					RelationName:    relationName,
					PKColNames:      pkColNames,
					PKColDirs:       pkColDirs,
					Bounds:          tc.bounds,
					AOSTDuration:    0,
					SelectBatchSize: 2,
					TTLExpr:         ttlColName,
					SelectDuration:  testHistogram(),
					SelectRateLimiter: quotapool.NewRateLimiter(
						"",
						quotapool.Inf(),
						math.MaxInt64,
					),
				},
				cutoff,
			)

			// Verify queryBuilder iterations.
			i := 0
			expectedIterations := tc.iterations
			actualIterations := make([][][]int, 0, len(expectedIterations))
			for ; ; i++ {
				const msg = "i=%d"
				result, hasNext, err := queryBuilder.Run(ctx, ie)
				require.NoErrorf(t, err, msg, i)
				actualIteration := make([][]int, 0, len(result))
				for _, datums := range result {
					row := make([]int, 0, len(datums))
					for _, datum := range datums {
						val := int(*datum.(*tree.DInt))
						row = append(row, val)
					}
					actualIteration = append(actualIteration, row)
				}
				actualIterations = append(actualIterations, actualIteration)
				require.Greaterf(t, len(expectedIterations), i, msg, i)
				require.Equalf(t, expectedIterations[i], actualIteration, msg, i)
				if !hasNext {
					break
				}
			}
			require.Len(t, expectedIterations, i+1)

			// Verify all selected rows are unique.
			for i := range actualIterations {
				for j := range actualIterations {
					if i != j {
						require.NotEqualf(t, actualIterations[i], actualIterations[j], "i=%d j=%d", i, j)
					}
				}
			}
		})
	}
}

func TestDeleteQueryBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc      string
		numPKCols int
		numRows   int
	}{
		{
			desc:      "1 PK col - 0 rows",
			numPKCols: 1,
			numRows:   0,
		},
		{
			desc:      "1 PK col - 1 row",
			numPKCols: 1,
			numRows:   1,
		},
		{
			desc:      "3 PK cols - 3 rows",
			numPKCols: 3,
			numRows:   3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
			defer srv.Stopper().Stop(ctx)
			s := srv.ApplicationLayer()

			ie := s.InternalExecutor().(*sql.InternalExecutor)
			db := s.InternalDB().(*sql.InternalDB)

			// Generate PKColNames.
			numPKCols := tc.numPKCols
			pkColNames := ttlbase.GenPKColNames(numPKCols)

			// Run CREATE TABLE statement.
			createTableStatement := genCreateTableStatement(pkColNames, nil)
			_, err := ie.Exec(ctx, "create ttl table", nil, createTableStatement)
			require.NoError(t, err)

			// Run INSERT statement.
			expectedNumRows := tc.numRows
			if expectedNumRows > 0 {
				values := make([]string, 0, expectedNumRows)
				for i := 0; i < expectedNumRows; i++ {
					value := make([]string, 0, numPKCols+1)
					for j := 0; j < numPKCols; j++ {
						value = append(value, strconv.Itoa(i))
					}
					value = append(value, "'"+expireAt.Format(time.RFC3339)+"'")
					values = append(values, "("+strings.Join(value, ", ")+")")
				}
				insertStatement := genInsertStatement(values)
				_, err = ie.Exec(ctx, "insert ttl table", nil, insertStatement)
				require.NoError(t, err)
			}

			// Setup DeleteQueryBuilder.
			queryBuilder := ttljob.MakeDeleteQueryBuilder(
				ttljob.DeleteQueryParams{
					RelationName:    relationName,
					PKColNames:      pkColNames,
					DeleteBatchSize: 2,
					TTLExpr:         ttlColName,
					DeleteDuration:  testHistogram(),
					DeleteRateLimiter: quotapool.NewRateLimiter(
						"",
						quotapool.Inf(),
						math.MaxInt64,
					),
				},
				cutoff,
			)

			// Verify rows are deleted.
			rows := make([]tree.Datums, 0, expectedNumRows)
			err = db.Txn(
				ctx,
				func(ctx context.Context, txn isql.Txn) error {
					actualNumRows, err := queryBuilder.Run(ctx, txn, rows)
					if err != nil {
						return err
					}
					require.Equal(t, int64(expectedNumRows), actualNumRows)
					return nil
				},
			)
			require.NoError(t, err)
		})
	}
}

func testHistogram() *aggmetric.Histogram {
	return aggmetric.MakeBuilder().Histogram(metric.HistogramOptions{
		SigFigs: 1,
	}).AddChild()
}

// BenchmarkTTLExpiration will benchmark the performance of queries that mimic
// the different kinds of TTL expiration expressions.
func BenchmarkTTLExpiration(b *testing.B) {
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(db)
	tdb.ExecMultiple(b,
		"create database db1",
		"create table db1.t1 (created_at timestamptz, expired_at timestamptz)",
	)
	// We are ingesting data. But we pick a timestamp so that the queries we do
	// below don't return any rows. We don't want the process of returning rows to
	// throw off the benchmark. We insert rows in batches to speed up the test
	// setup.
	const numRows = 250000
	const batchSize = 10000
	for i := 0; i < numRows; i += batchSize {
		tdb.Exec(b, fmt.Sprintf(`
        INSERT INTO db1.t1 (created_at, expired_at)
        SELECT now(), now() + interval '10 months' + generate_series(%d, %d) * interval '1 second'
    `, i, i+batchSize-1))
	}

	for _, tc := range []struct {
		desc  string
		query string
	}{
		{
			desc: "query=tz_conv",
			// This is the query that is very similar to what we only supported with
			// TTL expressions up until 23.2
			query: "select * from db1.t1 where (created_at AT TIME ZONE 'utc' + INTERVAL '6 months') AT TIME ZONE 'utc' > expired_at",
		},
		{
			desc: "query=interval_math",
			// In version 23.2, support was added for TTL expressions that use
			// TIMESTAMPTZ + INTERVAL. This improvement eliminates the need to
			// convert the time into a specific time zone, as was previously
			// required.
			query: "select * from db1.t1 where created_at + interval '6 months' > expired_at",
		},
	} {
		// Execute the query once outside the timings to prime any caches.
		tdb.Exec(b, tc.query)

		b.Run(tc.desc, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Execute the query on the table. The actual results are irrelevant, as there
				// shouldn't be any rows. We're only interested in measuring the execution time.
				tdb.Exec(b, tc.query)
			}
			b.StopTimer()
		})

	}
}
