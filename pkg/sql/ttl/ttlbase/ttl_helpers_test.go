// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

const (
	relationName = "relation_name"
	ttlExpr      = "expire_at"
)

func TestBuildSelectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc                string
		pkColDirs           []catenumpb.IndexColumn_Direction
		numStartQueryBounds int
		numEndQueryBounds   int
		startIncl           bool
		expectedQuery       string
	}{
		{
			desc: "ASC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 1,
			numEndQueryBounds:   1,
			expectedQuery: `SELECT col0
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 > $3)
)
AND (
  (col0 <= $2)
)
ORDER BY col0 ASC
LIMIT 2`,
		},
		{
			desc: "DESC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 1,
			numEndQueryBounds:   1,
			expectedQuery: `SELECT col0
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 < $3)
)
AND (
  (col0 >= $2)
)
ORDER BY col0 DESC
LIMIT 2`,
		},
		{
			desc: "ASC empty",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 0,
			numEndQueryBounds:   0,
			expectedQuery: `SELECT col0
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
ORDER BY col0 ASC
LIMIT 2`,
		},
		{
			desc: "DESC empty",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 0,
			numEndQueryBounds:   0,
			startIncl:           true,
			expectedQuery: `SELECT col0
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
ORDER BY col0 DESC
LIMIT 2`,
		},
		{
			desc: "ASC startIncl",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 1,
			numEndQueryBounds:   1,
			startIncl:           true,
			expectedQuery: `SELECT col0
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 >= $3)
)
AND (
  (col0 <= $2)
)
ORDER BY col0 ASC
LIMIT 2`,
		},
		{
			desc: "DESC startIncl",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 1,
			numEndQueryBounds:   1,
			startIncl:           true,
			expectedQuery: `SELECT col0
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 <= $3)
)
AND (
  (col0 >= $2)
)
ORDER BY col0 DESC
LIMIT 2`,
		},
		{
			desc: "ASC ASC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 2,
			numEndQueryBounds:   2,
			expectedQuery: `SELECT col0, col1
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 > $4) OR
  (col0 = $4 AND col1 > $5)
)
AND (
  (col0 < $2) OR
  (col0 = $2 AND col1 <= $3)
)
ORDER BY col0 ASC, col1 ASC
LIMIT 2`,
		},
		{
			desc: "ASC ASC partial start",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 1,
			numEndQueryBounds:   2,
			expectedQuery: `SELECT col0, col1
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 > $4)
)
AND (
  (col0 < $2) OR
  (col0 = $2 AND col1 <= $3)
)
ORDER BY col0 ASC, col1 ASC
LIMIT 2`,
		},
		{
			desc: "ASC ASC partial end",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 2,
			numEndQueryBounds:   1,
			expectedQuery: `SELECT col0, col1
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 > $3) OR
  (col0 = $3 AND col1 > $4)
)
AND (
  (col0 <= $2)
)
ORDER BY col0 ASC, col1 ASC
LIMIT 2`,
		},
		{
			desc: "DESC DESC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 2,
			numEndQueryBounds:   2,
			expectedQuery: `SELECT col0, col1
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 < $4) OR
  (col0 = $4 AND col1 < $5)
)
AND (
  (col0 > $2) OR
  (col0 = $2 AND col1 >= $3)
)
ORDER BY col0 DESC, col1 DESC
LIMIT 2`,
		},
		{
			desc: "DESC DESC partial start",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 1,
			numEndQueryBounds:   2,
			expectedQuery: `SELECT col0, col1
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 < $4)
)
AND (
  (col0 > $2) OR
  (col0 = $2 AND col1 >= $3)
)
ORDER BY col0 DESC, col1 DESC
LIMIT 2`,
		},
		{
			desc: "DESC DESC partial end",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 2,
			numEndQueryBounds:   1,
			expectedQuery: `SELECT col0, col1
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 < $3) OR
  (col0 = $3 AND col1 < $4)
)
AND (
  (col0 >= $2)
)
ORDER BY col0 DESC, col1 DESC
LIMIT 2`,
		},
		{
			desc: "ASC DESC ASC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_ASC,
			},
			numStartQueryBounds: 3,
			numEndQueryBounds:   3,
			expectedQuery: `SELECT col0, col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 > $5) OR
  (col0 = $5 AND col1 < $6) OR
  (col0 = $5 AND col1 = $6 AND col2 > $7)
)
AND (
  (col0 < $2) OR
  (col0 = $2 AND col1 > $3) OR
  (col0 = $2 AND col1 = $3 AND col2 <= $4)
)
ORDER BY col0 ASC, col1 DESC, col2 ASC
LIMIT 2`,
		},
		{
			desc: "DESC ASC DESC",
			pkColDirs: []catenumpb.IndexColumn_Direction{
				catenumpb.IndexColumn_DESC,
				catenumpb.IndexColumn_ASC,
				catenumpb.IndexColumn_DESC,
			},
			numStartQueryBounds: 3,
			numEndQueryBounds:   3,
			expectedQuery: `SELECT col0, col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-30 seconds'
WHERE ((expire_at) <= $1)
AND (
  (col0 < $5) OR
  (col0 = $5 AND col1 > $6) OR
  (col0 = $5 AND col1 = $6 AND col2 < $7)
)
AND (
  (col0 > $2) OR
  (col0 = $2 AND col1 < $3) OR
  (col0 = $2 AND col1 = $3 AND col2 >= $4)
)
ORDER BY col0 DESC, col1 ASC, col2 DESC
LIMIT 2`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pkColDirs := tc.pkColDirs
			pkColNames := GenPKColNames(len(pkColDirs))
			actualQuery := BuildSelectQuery(
				relationName,
				pkColNames,
				pkColDirs,
				DefaultAOSTDuration,
				ttlExpr,
				tc.numStartQueryBounds,
				tc.numEndQueryBounds,
				2, /*limit*/
				tc.startIncl,
			)
			require.Equal(t, tc.expectedQuery, actualQuery)
		})
	}
}

func TestBuildDeleteQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc          string
		numPKCols     int
		numRows       int
		expectedQuery string
	}{
		{
			desc:      "1 PK col - 1 row",
			numPKCols: 1,
			numRows:   1,
			expectedQuery: `DELETE FROM relation_name
WHERE ((expire_at) <= $1)
AND (col0) IN (($2))`,
		},
		{
			desc:      "3 PK cols - 3 rows",
			numPKCols: 3,
			numRows:   3,
			expectedQuery: `DELETE FROM relation_name
WHERE ((expire_at) <= $1)
AND (col0, col1, col2) IN (($2, $3, $4), ($5, $6, $7), ($8, $9, $10))`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			pkColNames := GenPKColNames(tc.numPKCols)
			actualQuery := BuildDeleteQuery(
				relationName,
				pkColNames,
				ttlExpr,
				tc.numRows,
			)
			require.Equal(t, tc.expectedQuery, actualQuery)
		})
	}
}
