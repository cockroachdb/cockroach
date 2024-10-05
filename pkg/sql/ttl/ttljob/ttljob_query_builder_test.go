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
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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
			testCluster := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
			defer testCluster.Stopper().Stop(ctx)

			testServer := testCluster.Server(0)
			ie := testServer.InternalExecutor().(*sql.InternalExecutor)

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

			testCluster := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
			defer testCluster.Stopper().Stop(ctx)

			testServer := testCluster.Server(0)
			ie := testServer.InternalExecutor().(*sql.InternalExecutor)
			db := testServer.InternalDB().(*sql.InternalDB)

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
