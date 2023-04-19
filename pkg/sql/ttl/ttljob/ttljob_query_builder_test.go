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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSelectQueryBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mockTime := time.Date(2000, 1, 1, 13, 30, 45, 0, time.UTC)
	mockDuration := -10 * time.Second

	type iteration struct {
		expectedQuery string
		expectedArgs  []interface{}
		rows          []tree.Datums
	}
	testCases := []struct {
		desc       string
		b          selectQueryBuilder
		iterations []iteration
	}{
		{
			desc: "middle range ASC",
			b: makeSelectQueryBuilder(
				mockTime,
				[]string{"col1", "col2"},
				[]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				"relation_name",
				ttlbase.QueryBounds{
					Start: tree.Datums{tree.NewDInt(100), tree.NewDInt(5)},
					End:   tree.Datums{tree.NewDInt(200), tree.NewDInt(15)},
				},
				mockDuration,
				2,
				catpb.TTLDefaultExpirationColumnName,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $4) OR
  (col1 = $4 AND col2 >= $5)
)
AND (
  (col1 < $2) OR
  (col1 = $2 AND col2 < $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(200), tree.NewDInt(15),
						tree.NewDInt(100), tree.NewDInt(5),
					},
					rows: []tree.Datums{
						{tree.NewDInt(100), tree.NewDInt(12)},
						{tree.NewDInt(105), tree.NewDInt(12)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $4) OR
  (col1 = $4 AND col2 > $5)
)
AND (
  (col1 < $2) OR
  (col1 = $2 AND col2 < $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(200), tree.NewDInt(15),
						tree.NewDInt(105), tree.NewDInt(12),
					},
					rows: []tree.Datums{
						{tree.NewDInt(112), tree.NewDInt(19)},
						{tree.NewDInt(180), tree.NewDInt(132)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $4) OR
  (col1 = $4 AND col2 > $5)
)
AND (
  (col1 < $2) OR
  (col1 = $2 AND col2 < $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(200), tree.NewDInt(15),
						tree.NewDInt(180), tree.NewDInt(132),
					},
					rows: []tree.Datums{},
				},
			},
		},
		{
			desc: "middle range DESC",
			b: makeSelectQueryBuilder(
				mockTime,
				[]string{"col1", "col2"},
				[]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_DESC, catenumpb.IndexColumn_DESC},
				"relation_name",
				ttlbase.QueryBounds{
					Start: tree.Datums{tree.NewDInt(200), tree.NewDInt(15)},
					End:   tree.Datums{tree.NewDInt(100), tree.NewDInt(5)},
				},
				mockDuration,
				2,
				catpb.TTLDefaultExpirationColumnName,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 < $4) OR
  (col1 = $4 AND col2 <= $5)
)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 DESC, col2 DESC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(100), tree.NewDInt(5),
						tree.NewDInt(200), tree.NewDInt(15),
					},
					rows: []tree.Datums{
						{tree.NewDInt(105), tree.NewDInt(12)},
						{tree.NewDInt(100), tree.NewDInt(12)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 < $4) OR
  (col1 = $4 AND col2 < $5)
)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 DESC, col2 DESC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(100), tree.NewDInt(5),
						tree.NewDInt(100), tree.NewDInt(12),
					},
					rows: []tree.Datums{
						{tree.NewDInt(180), tree.NewDInt(132)},
						{tree.NewDInt(112), tree.NewDInt(19)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 < $4) OR
  (col1 = $4 AND col2 < $5)
)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 DESC, col2 DESC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(100), tree.NewDInt(5),
						tree.NewDInt(112), tree.NewDInt(19),
					},
					rows: []tree.Datums{},
				},
			},
		},
		{
			desc: "only one range",
			b: makeSelectQueryBuilder(
				mockTime,
				[]string{"col1", "col2"},
				[]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				"relation_name",
				ttlbase.QueryBounds{},
				mockDuration,
				2,
				catpb.TTLDefaultExpirationColumnName,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
					},
					rows: []tree.Datums{
						{tree.NewDInt(100), tree.NewDInt(12)},
						{tree.NewDInt(105), tree.NewDInt(12)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(105), tree.NewDInt(12),
					},
					rows: []tree.Datums{
						{tree.NewDInt(112), tree.NewDInt(19)},
						{tree.NewDInt(180), tree.NewDInt(132)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(180), tree.NewDInt(132),
					},
					rows: []tree.Datums{},
				},
			},
		},
		{
			desc: "one range, but a partial startPK and endPK split",
			b: makeSelectQueryBuilder(
				mockTime,
				[]string{"col1", "col2"},
				[]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				"relation_name",
				ttlbase.QueryBounds{
					Start: tree.Datums{tree.NewDInt(100)},
					End:   tree.Datums{tree.NewDInt(181)},
				},
				mockDuration,
				2,
				catpb.TTLDefaultExpirationColumnName,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 >= $3)
)
AND (
  (col1 < $2)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(181),
						tree.NewDInt(100),
					},
					rows: []tree.Datums{
						{tree.NewDInt(100), tree.NewDInt(12)},
						{tree.NewDInt(105), tree.NewDInt(12)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $3) OR
  (col1 = $3 AND col2 > $4)
)
AND (
  (col1 < $2)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(181),
						tree.NewDInt(105), tree.NewDInt(12),
					},
					rows: []tree.Datums{
						{tree.NewDInt(112), tree.NewDInt(19)},
						{tree.NewDInt(180), tree.NewDInt(132)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $3) OR
  (col1 = $3 AND col2 > $4)
)
AND (
  (col1 < $2)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(181),
						tree.NewDInt(180), tree.NewDInt(132),
					},
					rows: []tree.Datums{},
				},
			},
		},
		{
			desc: "first range",
			b: makeSelectQueryBuilder(
				mockTime,
				[]string{"col1", "col2"},
				[]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				"relation_name",
				ttlbase.QueryBounds{
					End: tree.Datums{tree.NewDInt(200), tree.NewDInt(15)},
				},
				mockDuration,
				2,
				catpb.TTLDefaultExpirationColumnName,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 < $2) OR
  (col1 = $2 AND col2 < $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(200), tree.NewDInt(15),
					},
					rows: []tree.Datums{
						{tree.NewDInt(100), tree.NewDInt(12)},
						{tree.NewDInt(105), tree.NewDInt(12)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $4) OR
  (col1 = $4 AND col2 > $5)
)
AND (
  (col1 < $2) OR
  (col1 = $2 AND col2 < $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(200), tree.NewDInt(15),
						tree.NewDInt(105), tree.NewDInt(12),
					},
					rows: []tree.Datums{
						{tree.NewDInt(112), tree.NewDInt(19)},
						{tree.NewDInt(180), tree.NewDInt(132)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $4) OR
  (col1 = $4 AND col2 > $5)
)
AND (
  (col1 < $2) OR
  (col1 = $2 AND col2 < $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(200), tree.NewDInt(15),
						tree.NewDInt(180), tree.NewDInt(132),
					},
					rows: []tree.Datums{},
				},
			},
		},
		{
			desc: "last range",
			b: makeSelectQueryBuilder(
				mockTime,
				[]string{"col1", "col2"},
				[]catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				"relation_name",
				ttlbase.QueryBounds{
					Start: tree.Datums{tree.NewDInt(100), tree.NewDInt(5)},
				},
				mockDuration,
				2,
				catpb.TTLDefaultExpirationColumnName,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 >= $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(100), tree.NewDInt(5),
					},
					rows: []tree.Datums{
						{tree.NewDInt(100), tree.NewDInt(12)},
						{tree.NewDInt(105), tree.NewDInt(12)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(105), tree.NewDInt(12),
					},
					rows: []tree.Datums{
						{tree.NewDInt(112), tree.NewDInt(19)},
						{tree.NewDInt(180), tree.NewDInt(132)},
					},
				},
				{
					expectedQuery: `SELECT col1, col2
FROM relation_name
AS OF SYSTEM TIME INTERVAL '-10 seconds'
WHERE ((crdb_internal_expiration) <= $1)
AND (
  (col1 > $2) OR
  (col1 = $2 AND col2 > $3)
)
ORDER BY col1 ASC, col2 ASC
LIMIT 2`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(180), tree.NewDInt(132),
					},
					rows: []tree.Datums{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			for i, it := range tc.iterations {
				const msg = "iteration=%d"
				q, args := tc.b.nextQuery()
				require.Equalf(t, it.expectedQuery, q, msg, i)
				require.Equalf(t, it.expectedArgs, args, msg, i)
				require.NoErrorf(t, tc.b.moveCursor(it.rows), msg, i)
				if i >= 1 {
					require.NotEmptyf(t, tc.b.cachedQuery, msg, i)
				}
			}
		})
	}
}

func TestDeleteQueryBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mockTime := time.Date(2000, 1, 1, 13, 30, 45, 0, time.UTC)

	type iteration struct {
		rows []tree.Datums

		expectedQuery string
		expectedArgs  []interface{}
	}
	testCases := []struct {
		desc       string
		b          deleteQueryBuilder
		iterations []iteration
	}{
		{
			desc: "single delete less than batch size",
			b:    makeDeleteQueryBuilder(mockTime, []string{"col1", "col2"}, "relation_name", 3, catpb.TTLDefaultExpirationColumnName),
			iterations: []iteration{
				{
					rows: []tree.Datums{
						{tree.NewDInt(10), tree.NewDInt(15)},
						{tree.NewDInt(12), tree.NewDInt(16)},
					},
					expectedQuery: `DELETE FROM relation_name
WHERE (crdb_internal_expiration) <= $1
AND (col1, col2) IN (($2, $3), ($4, $5))`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(10), tree.NewDInt(15),
						tree.NewDInt(12), tree.NewDInt(16),
					},
				},
			},
		},
		{
			desc: "multiple deletes",
			b:    makeDeleteQueryBuilder(mockTime, []string{"col1", "col2"}, "relation_name", 3, catpb.TTLDefaultExpirationColumnName),
			iterations: []iteration{
				{
					rows: []tree.Datums{
						{tree.NewDInt(10), tree.NewDInt(15)},
						{tree.NewDInt(12), tree.NewDInt(16)},
						{tree.NewDInt(12), tree.NewDInt(18)},
					},
					expectedQuery: `DELETE FROM relation_name
WHERE (crdb_internal_expiration) <= $1
AND (col1, col2) IN (($2, $3), ($4, $5), ($6, $7))`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(10), tree.NewDInt(15),
						tree.NewDInt(12), tree.NewDInt(16),
						tree.NewDInt(12), tree.NewDInt(18),
					},
				},
				{
					rows: []tree.Datums{
						{tree.NewDInt(110), tree.NewDInt(115)},
						{tree.NewDInt(112), tree.NewDInt(116)},
						{tree.NewDInt(112), tree.NewDInt(118)},
					},
					expectedQuery: `DELETE FROM relation_name
WHERE (crdb_internal_expiration) <= $1
AND (col1, col2) IN (($2, $3), ($4, $5), ($6, $7))`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(110), tree.NewDInt(115),
						tree.NewDInt(112), tree.NewDInt(116),
						tree.NewDInt(112), tree.NewDInt(118),
					},
				},
				{
					rows: []tree.Datums{
						{tree.NewDInt(1210), tree.NewDInt(1215)},
					},
					expectedQuery: `DELETE FROM relation_name
WHERE (crdb_internal_expiration) <= $1
AND (col1, col2) IN (($2, $3))`,
					expectedArgs: []interface{}{
						mockTime,
						tree.NewDInt(1210), tree.NewDInt(1215),
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			for _, it := range tc.iterations {
				q, args := tc.b.buildQueryAndArgs(it.rows)
				require.Equal(t, it.expectedQuery, q)
				require.Equal(t, it.expectedArgs, args)
			}
		})
	}
}
