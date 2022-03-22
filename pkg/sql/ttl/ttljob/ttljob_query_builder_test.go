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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSelectQueryBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mockTime := time.Date(2000, 1, 1, 13, 30, 45, 0, time.UTC)
	mockTimestampTZ, err := tree.MakeDTimestampTZ(mockTime, time.Microsecond)
	require.NoError(t, err)

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
			desc: "middle range",
			b: makeSelectQueryBuilder(
				1,
				mockTime,
				[]string{"col1", "col2"},
				tree.Datums{tree.NewDInt(100), tree.NewDInt(5)},
				tree.Datums{tree.NewDInt(200), tree.NewDInt(15)},
				*mockTimestampTZ,
				2,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) >= ($4, $5) AND (col1, col2) < ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($4, $5) AND (col1, col2) < ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($4, $5) AND (col1, col2) < ($2, $3)
ORDER BY col1, col2
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
			desc: "only one range",
			b: makeSelectQueryBuilder(
				1,
				mockTime,
				[]string{"col1", "col2"},
				nil,
				nil,
				*mockTimestampTZ,
				2,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($2, $3)
ORDER BY col1, col2
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
			desc: "first range",
			b: makeSelectQueryBuilder(
				1,
				mockTime,
				[]string{"col1", "col2"},
				nil,
				tree.Datums{tree.NewDInt(200), tree.NewDInt(15)},
				*mockTimestampTZ,
				2,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) < ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($4, $5) AND (col1, col2) < ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($4, $5) AND (col1, col2) < ($2, $3)
ORDER BY col1, col2
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
				1,
				mockTime,
				[]string{"col1", "col2"},
				tree.Datums{tree.NewDInt(100), tree.NewDInt(5)},
				nil,
				*mockTimestampTZ,
				2,
			),
			iterations: []iteration{
				{
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) >= ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($2, $3)
ORDER BY col1, col2
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
					expectedQuery: `SELECT col1, col2 FROM [1 AS tbl_name]
AS OF SYSTEM TIME '2000-01-01 13:30:45+00:00'
WHERE crdb_internal_expiration <= $1 AND (col1, col2) > ($2, $3)
ORDER BY col1, col2
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
				q, args := tc.b.nextQuery()
				require.Equal(t, it.expectedQuery, q)
				require.Equal(t, it.expectedArgs, args)
				require.NoError(t, tc.b.moveCursor(it.rows))
				if i >= 1 {
					require.NotEmpty(t, tc.b.cachedQuery)
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
			b:    makeDeleteQueryBuilder(1, mockTime, []string{"col1", "col2"}, 3),
			iterations: []iteration{
				{
					rows: []tree.Datums{
						{tree.NewDInt(10), tree.NewDInt(15)},
						{tree.NewDInt(12), tree.NewDInt(16)},
					},
					expectedQuery: `DELETE FROM [1 AS tbl_name] WHERE crdb_internal_expiration <= $1 AND (col1, col2) IN (($2, $3), ($4, $5))`,
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
			b:    makeDeleteQueryBuilder(1, mockTime, []string{"col1", "col2"}, 3),
			iterations: []iteration{
				{
					rows: []tree.Datums{
						{tree.NewDInt(10), tree.NewDInt(15)},
						{tree.NewDInt(12), tree.NewDInt(16)},
						{tree.NewDInt(12), tree.NewDInt(18)},
					},
					expectedQuery: `DELETE FROM [1 AS tbl_name] WHERE crdb_internal_expiration <= $1 AND (col1, col2) IN (($2, $3), ($4, $5), ($6, $7))`,
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
					expectedQuery: `DELETE FROM [1 AS tbl_name] WHERE crdb_internal_expiration <= $1 AND (col1, col2) IN (($2, $3), ($4, $5), ($6, $7))`,
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
					expectedQuery: `DELETE FROM [1 AS tbl_name] WHERE crdb_internal_expiration <= $1 AND (col1, col2) IN (($2, $3))`,
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

func TestMakeColumnNamesSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		cols     []string
		expected string
	}{
		{[]string{"a"}, "a"},
		{[]string{"index"}, `"index"`},
		{[]string{"a", "b"}, "a, b"},
		{[]string{"escape-me", "index", "c"}, `"escape-me", "index", c`},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			require.Equal(t, tc.expected, makeColumnNamesSQL(tc.cols))
		})
	}
}
