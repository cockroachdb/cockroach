// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecargs"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecjoin"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexectestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var (
	floats = []float64{0.314, 3.14, 31.4, 314}
	decs   []apd.Decimal
)

func init() {
	// Set up the apd.Decimal values used in tests.
	decs = make([]apd.Decimal, len(floats))
	for i, f := range floats {
		_, err := decs[i].SetFloat64(f)
		if err != nil {
			colexecerror.InternalError(errors.AssertionFailedf("%v", err))
		}
	}
}

func getHJTestCases() []*joinTestCase {
	hjTestCases := []*joinTestCase{
		{
			description: "0",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			leftTuples: colexectestutils.Tuples{
				{0},
				{1},
				{2},
				{3},
			},
			rightTuples: colexectestutils.Tuples{
				{-1},
				{1},
				{3},
				{5},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          descpb.FullOuterJoin,
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{nil, -1},
				{1, 1},
				{3, 3},
				{nil, 5},
				{0, nil},
				{2, nil},
			},
		},
		{
			description: "1",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test an empty build table.
			leftTuples: colexectestutils.Tuples{},
			rightTuples: colexectestutils.Tuples{
				{-1},
				{1},
				{3},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:         descpb.FullOuterJoin,
			leftEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{nil, -1},
				{nil, 1},
				{nil, 3},
			},
		},
		{
			description: "2",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			leftTuples: colexectestutils.Tuples{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			rightTuples: colexectestutils.Tuples{
				{1},
				{3},
				{5},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          descpb.LeftOuterJoin,
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{1, 1},
				{3, 3},
				{0, nil},
				{2, nil},
				{4, nil},
			},
		},
		{
			description: "3",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test right outer join.
			leftTuples: colexectestutils.Tuples{
				{0},
				{1},
			},
			rightTuples: colexectestutils.Tuples{
				{1},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          descpb.RightOuterJoin,
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{1, 1},
				{nil, 2},
			},
		},
		{
			description: "4",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test right outer join with non-distinct left build table with an
			// unmatched row from the right followed by a matched one. This is a
			// regression test for #39303 in order to check that probeRowUnmatched
			// is updated correctly in case of non-distinct build table.
			leftTuples: colexectestutils.Tuples{
				{0},
				{0},
				{2},
			},
			rightTuples: colexectestutils.Tuples{
				{1},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          descpb.RightOuterJoin,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{nil, 1},
				{2, 2},
			},
		},
		{
			description: "5",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test null handling only on probe column.
			leftTuples: colexectestutils.Tuples{
				{0},
			},
			rightTuples: colexectestutils.Tuples{
				{nil},
				{0},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{0},
			},
		},
		{
			description: "6",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test null handling only on build column.
			leftTuples: colexectestutils.Tuples{
				{nil},
				{nil},
				{1},
				{0},
			},
			rightTuples: colexectestutils.Tuples{
				{1},
				{0},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},
			// Note that although right equality columns are key, we want to test
			// null handling on the build column, so we "lie" here.
			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{1},
				{0},
			},
		},
		{
			description: "7",
			leftTypes:   []*types.T{types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int},

			// Test null handling in output columns.
			leftTuples: colexectestutils.Tuples{
				{1, nil},
				{2, nil},
				{3, 1},
				{4, 2},
			},
			rightTuples: colexectestutils.Tuples{
				{1, 2},
				{2, nil},
				{3, nil},
				{4, 4},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{nil, 2},
				{nil, nil},
				{1, nil},
				{2, 4},
			},
		},
		{
			description: "8",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test null handling in hash join key column.
			leftTuples: colexectestutils.Tuples{
				{1},
				{3},
				{nil},
				{2},
			},
			rightTuples: colexectestutils.Tuples{
				{2},
				{nil},
				{3},
				{nil},
				{1},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{2},
				{3},
				{1},
			},
		},
		{
			// Test handling of multiple column non-distinct equality keys.
			description: "9",
			leftTypes:   []*types.T{types.Int, types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int, types.Int},

			leftTuples: colexectestutils.Tuples{
				{0, 0, 1},
				{0, 0, 2},
				{1, 0, 3},
				{1, 1, 4},
				{1, 1, 5},
				{0, 0, 6},
			},
			rightTuples: colexectestutils.Tuples{
				{1, 0, 7},
				{0, 0, 8},
				{0, 0, 9},
				{0, 1, 10},
			},

			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 1},
			leftOutCols:  []uint32{2},
			rightOutCols: []uint32{2},

			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{3, 7},
				{6, 8},
				{1, 8},
				{2, 8},
				{6, 9},
				{1, 9},
				{2, 9},
			},
		},
		{
			// Test handling of duplicate equality keys that map to same buckets.
			description: "10",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			leftTuples: colexectestutils.Tuples{
				{0},
				{coldata.BatchSize()},
				{coldata.BatchSize()},
				{coldata.BatchSize()},
				{0},
				{coldata.BatchSize() * 2},
				{1},
				{1},
				{coldata.BatchSize() + 1},
			},
			rightTuples: colexectestutils.Tuples{
				{coldata.BatchSize()},
				{coldata.BatchSize() * 2},
				{coldata.BatchSize() * 3},
				{0},
				{1},
				{coldata.BatchSize() + 1},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			// Note that although right equality columns are key, we want to test
			// handling of collisions, so we "lie" here.
			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{coldata.BatchSize(), coldata.BatchSize()},
				{coldata.BatchSize(), coldata.BatchSize()},
				{coldata.BatchSize(), coldata.BatchSize()},
				{coldata.BatchSize() * 2, coldata.BatchSize() * 2},
				{0, 0},
				{0, 0},
				{1, 1},
				{1, 1},
				{coldata.BatchSize() + 1, coldata.BatchSize() + 1},
			},
		},
		{
			// Test handling of duplicate equality keys.
			description: "11",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			leftTuples: colexectestutils.Tuples{
				{0},
				{0},
				{1},
				{1},
				{1},
				{2},
			},
			rightTuples: colexectestutils.Tuples{
				{1},
				{0},
				{2},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{1},
				{1},
				{1},
				{0},
				{0},
				{2},
				{2},
			},
		},
		{
			// Test handling of various output column types.
			description: "12",
			leftTypes:   []*types.T{types.Bool, types.Int, types.Bytes, types.Int},
			rightTypes:  []*types.T{types.Int, types.Float, types.Int4},

			leftTuples: colexectestutils.Tuples{
				{false, 5, "a", 10},
				{true, 3, "b", 30},
				{false, 2, "foo", 20},
				{false, 6, "bar", 50},
			},
			rightTuples: colexectestutils.Tuples{
				{1, 1.1, int32(1)},
				{2, 2.2, int32(2)},
				{3, 3.3, int32(4)},
				{4, 4.4, int32(8)},
				{5, 5.5, int32(16)},
			},

			leftEqCols:   []uint32{1},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{1, 2},
			rightOutCols: []uint32{0, 2},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{2, "foo", 2, int32(2)},
				{3, "b", 3, int32(4)},
				{5, "a", 5, int32(16)},
			},
		},
		{
			description: "13",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Reverse engineering hash table hash heuristic to find key values that
			// hash to the same bucket.
			leftTuples: colexectestutils.Tuples{
				{0},
				{coldata.BatchSize()},
				{coldata.BatchSize() * 2},
				{coldata.BatchSize() * 3},
			},
			rightTuples: colexectestutils.Tuples{
				{0},
				{coldata.BatchSize()},
				{coldata.BatchSize() * 3},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{0},
				{coldata.BatchSize()},
				{coldata.BatchSize() * 3},
			},
		},
		{
			description: "14",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			// Test a N:1 inner join where the right side key has duplicate values.
			leftTuples: colexectestutils.Tuples{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			rightTuples: colexectestutils.Tuples{
				{1},
				{1},
				{1},
				{2},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{1, 1},
				{1, 1},
				{1, 1},
				{2, 2},
				{2, 2},
			},
		},
		{
			description: "15",
			leftTypes:   []*types.T{types.Int, types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int, types.Int},

			// Test inner join on multiple equality columns.
			leftTuples: colexectestutils.Tuples{
				{0, 0, 10},
				{0, 1, 20},
				{0, 2, 30},
				{1, 1, 40},
				{1, 2, 50},
				{2, 0, 60},
				{2, 1, 70},
			},
			rightTuples: colexectestutils.Tuples{
				{0, 100, 2},
				{1, 200, 1},
				{2, 300, 0},
				{2, 400, 1},
			},

			leftEqCols:   []uint32{0, 1},
			rightEqCols:  []uint32{0, 2},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{1},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{0, 2, 30, 100},
				{1, 1, 40, 200},
				{2, 0, 60, 300},
				{2, 1, 70, 400},
			},
		},
		{
			description: "16",
			leftTypes:   []*types.T{types.Int, types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int},

			// Test multiple column with values that hash to the same bucket.
			leftTuples: colexectestutils.Tuples{
				{10, 0, 0},
				{20, 0, coldata.BatchSize()},
				{40, coldata.BatchSize(), 0},
				{50, coldata.BatchSize(), coldata.BatchSize()},
				{60, coldata.BatchSize() * 2, 0},
				{70, coldata.BatchSize() * 2, coldata.BatchSize()},
			},
			rightTuples: colexectestutils.Tuples{
				{0, coldata.BatchSize()},
				{coldata.BatchSize() * 2, coldata.BatchSize()},
				{0, 0},
				{0, coldata.BatchSize() * 2},
			},

			leftEqCols:   []uint32{1, 2},
			rightEqCols:  []uint32{0, 1},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{20, 0, coldata.BatchSize()},
				{70, coldata.BatchSize() * 2, coldata.BatchSize()},
				{10, 0, 0},
			},
		},
		{
			description: "17",
			leftTypes:   []*types.T{types.Bytes, types.Bool, types.Int2, types.Int4, types.Int, types.Bytes},
			rightTypes:  []*types.T{types.Int, types.Int4, types.Int2, types.Bool, types.Bytes},

			// Test multiple equality columns of different types.
			leftTuples: colexectestutils.Tuples{
				{"foo", false, int16(100), int32(1000), int64(10000), "aaa"},
				{"foo", true, 100, 1000, 10000, "bbb"},
				{"foo1", false, 100, 1000, 10000, "ccc"},
				{"foo", false, 200, 1000, 10000, "ddd"},
				{"foo", false, 100, 2000, 10000, "eee"},
				{"bar", true, 300, 3000, 30000, "fff"},
			},
			rightTuples: colexectestutils.Tuples{
				{int64(10000), int32(1000), int16(100), false, "foo1"},
				{10000, 1000, 100, false, "foo"},
				{30000, 3000, 300, true, "bar"},
				{10000, 1000, 200, false, "foo"},
				{30000, 3000, 300, false, "bar"},
				{10000, 1000, 100, false, "random"},
			},

			leftEqCols:   []uint32{0, 1, 2, 3, 4},
			rightEqCols:  []uint32{4, 3, 2, 1, 0},
			leftOutCols:  []uint32{5},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{"ccc"},
				{"aaa"},
				{"fff"},
				{"ddd"},
			},
		},
		{
			description: "18",
			leftTypes:   []*types.T{types.Float},
			rightTypes:  []*types.T{types.Float},

			// Test equality columns of type float.
			leftTuples: colexectestutils.Tuples{
				{33.333},
				{44.4444},
				{55.55555},
				{44.4444},
			},
			rightTuples: colexectestutils.Tuples{
				{44.4444},
				{55.55555},
				{33.333},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{55.55555},
				{44.4444},
				{44.4444},
				{33.333},
			},
		},
		{
			description: "19",
			leftTypes:   []*types.T{types.Int, types.Int, types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int, types.Int, types.Int},

			// Test use right side as build table.
			leftTuples: colexectestutils.Tuples{
				{2, 4, 8, 16},
				{3, 3, 2, 2},
				{3, 7, 2, 1},
				{5, 4, 3, 2},
			},
			rightTuples: colexectestutils.Tuples{
				{1, 3, 5, 7},
				{1, 1, 1, 1},
				{1, 2, 3, 4},
			},

			leftEqCols:   []uint32{2, 0},
			rightEqCols:  []uint32{1, 2},
			leftOutCols:  []uint32{0, 1, 2, 3},
			rightOutCols: []uint32{0, 1, 2, 3},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{3, 3, 2, 2, 1, 2, 3, 4},
				{3, 7, 2, 1, 1, 2, 3, 4},
				{5, 4, 3, 2, 1, 3, 5, 7},
			},
		},
		{
			description: "20",
			leftTypes:   []*types.T{types.Decimal},
			rightTypes:  []*types.T{types.Decimal},

			// Test types.Decimal type as equality column.
			leftTuples: colexectestutils.Tuples{
				{decs[0]},
				{decs[1]},
				{decs[2]},
			},
			rightTuples: colexectestutils.Tuples{
				{decs[2]},
				{decs[3]},
				{decs[0]},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{},
			rightOutCols: []uint32{0},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expected: colexectestutils.Tuples{
				{decs[2]},
				{decs[0]},
			},
		},
		{
			description: "21",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			joinType: descpb.LeftSemiJoin,

			leftTuples: colexectestutils.Tuples{
				{0},
				{0},
				{1},
				{2},
			},
			rightTuples: colexectestutils.Tuples{
				{0},
				{0},
				{1},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{0},
				{0},
				{1},
			},
		},
		{
			description: "22",
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},

			joinType: descpb.LeftAntiJoin,

			leftTuples: colexectestutils.Tuples{
				{0},
				{0},
				{1},
				{2},
			},
			rightTuples: colexectestutils.Tuples{
				{0},
				{0},
				{1},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expected: colexectestutils.Tuples{
				{2},
			},
		},
		{
			description: "23",
			leftTypes:   []*types.T{types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int},

			// Test ON expression.
			leftTuples: colexectestutils.Tuples{
				{1, nil},
				{2, nil},
				{3, 1},
				{4, 2},
			},
			rightTuples: colexectestutils.Tuples{
				{1, 2},
				{2, nil},
				{3, nil},
				{4, 4},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			onExpr: execinfrapb.Expression{Expr: "@1 + @3 > 2 AND @1 + @3 < 8"},
			expected: colexectestutils.Tuples{
				{nil, nil},
				{1, nil},
			},
		},
		{
			description: "24",
			leftTypes:   []*types.T{types.Int, types.Int},
			rightTypes:  []*types.T{types.Int, types.Int},

			// Test ON expression.
			leftTuples: colexectestutils.Tuples{
				{1, nil},
				{2, nil},
				{3, 1},
				{4, 2},
			},
			rightTuples: colexectestutils.Tuples{
				{1, 2},
				{2, nil},
				{3, nil},
				{4, 4},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{1},
			rightOutCols: []uint32{1},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			onExpr: execinfrapb.Expression{Expr: "@1 + @3 + @4 < 100"},
			expected: colexectestutils.Tuples{
				{nil, 2},
				{2, 4},
			},
		},
		{
			description: "25",
			joinType:    descpb.IntersectAllJoin,
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},
			leftTuples:  colexectestutils.Tuples{{1}, {1}, {2}, {2}, {2}, {3}, {3}},
			rightTuples: colexectestutils.Tuples{{1}, {2}, {3}, {3}, {3}},
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			leftOutCols: []uint32{0},
			expected:    colexectestutils.Tuples{{1}, {2}, {3}, {3}},
		},
		{
			description: "26",
			joinType:    descpb.ExceptAllJoin,
			leftTypes:   []*types.T{types.Int},
			rightTypes:  []*types.T{types.Int},
			leftTuples:  colexectestutils.Tuples{{1}, {1}, {2}, {2}, {2}, {3}, {3}},
			rightTuples: colexectestutils.Tuples{{1}, {2}, {3}, {3}, {3}},
			leftEqCols:  []uint32{0},
			rightEqCols: []uint32{0},
			leftOutCols: []uint32{0},
			expected:    colexectestutils.Tuples{{1}, {2}, {2}},
		},
	}
	return withMirrors(hjTestCases)
}

// createSpecForHashJoiner creates a hash join processor spec based on a test
// case.
func createSpecForHashJoiner(tc *joinTestCase) *execinfrapb.ProcessorSpec {
	hjSpec := &execinfrapb.HashJoinerSpec{
		LeftEqColumns:        tc.leftEqCols,
		RightEqColumns:       tc.rightEqCols,
		LeftEqColumnsAreKey:  tc.leftEqColsAreKey,
		RightEqColumnsAreKey: tc.rightEqColsAreKey,
		OnExpr:               tc.onExpr,
		Type:                 tc.joinType,
	}
	projection := make([]uint32, 0, len(tc.leftOutCols)+len(tc.rightOutCols))
	projection = append(projection, tc.leftOutCols...)
	rColOffset := uint32(len(tc.leftTypes))
	if !tc.joinType.ShouldIncludeLeftColsInOutput() {
		rColOffset = 0
	}
	for _, outCol := range tc.rightOutCols {
		projection = append(projection, rColOffset+outCol)
	}
	resultTypes := make([]*types.T, 0, len(projection))
	for _, i := range projection {
		if int(i) < len(tc.leftTypes) {
			resultTypes = append(resultTypes, tc.leftTypes[i])
		} else {
			resultTypes = append(resultTypes, tc.rightTypes[i-rColOffset])
		}
	}
	return &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{
			{ColumnTypes: tc.leftTypes},
			{ColumnTypes: tc.rightTypes},
		},
		Core: execinfrapb.ProcessorCoreUnion{
			HashJoiner: hjSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: projection,
		},
		ResultTypes: resultTypes,
	}
}

// runHashJoinTestCase is a helper function that runs a single test case
// against a hash join operator (either in-memory or disk-backed one) which is
// created by the provided constructor.
func runHashJoinTestCase(
	t *testing.T,
	tc *joinTestCase,
	hjOpConstructor func(sources []colexecop.Operator) (colexecop.Operator, error),
) {
	tc.init()
	inputs := []colexectestutils.Tuples{tc.leftTuples, tc.rightTuples}
	typs := [][]*types.T{tc.leftTypes, tc.rightTypes}
	var runner colexectestutils.TestRunner
	if tc.skipAllNullsInjection {
		// We're omitting all nulls injection test. See comments for each such
		// test case.
		runner = colexectestutils.RunTestsWithoutAllNullsInjection
	} else {
		runner = colexectestutils.RunTestsWithTyps
	}
	log.Infof(context.Background(), "%s", tc.description)
	runner(t, testAllocator, inputs, typs, tc.expected, colexectestutils.UnorderedVerifier, hjOpConstructor)
}

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: st},
	}

	for _, tcs := range [][]*joinTestCase{getHJTestCases(), getMJTestCases()} {
		for _, tc := range tcs {
			for _, tc := range tc.mutateTypes() {
				runHashJoinTestCase(t, tc, func(sources []colexecop.Operator) (colexecop.Operator, error) {
					spec := createSpecForHashJoiner(tc)
					args := &colexecargs.NewColOperatorArgs{
						Spec:                spec,
						Inputs:              colexectestutils.MakeInputs(sources),
						StreamingMemAccount: testMemAcc,
					}
					args.TestingKnobs.UseStreamingMemAccountForBuffering = true
					args.TestingKnobs.DiskSpillingDisabled = true
					result, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
					if err != nil {
						return nil, err
					}
					return result.Root, nil
				})
			}
		}
	}
}

func BenchmarkHashJoiner(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	nCols := 4
	sourceTypes := make([]*types.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = types.Int
	}

	batch := testAllocator.NewMemBatchWithMaxCapacity(sourceTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < coldata.BatchSize(); i++ {
			col[i] = int64(i)
		}
	}

	batch.SetLength(coldata.BatchSize())

	for _, hasNulls := range []bool{false, true} {
		b.Run(fmt.Sprintf("nulls=%v", hasNulls), func(b *testing.B) {

			if hasNulls {
				for colIdx := 0; colIdx < nCols; colIdx++ {
					vec := batch.ColVec(colIdx)
					vec.Nulls().SetNull(0)
				}
			} else {
				for colIdx := 0; colIdx < nCols; colIdx++ {
					vec := batch.ColVec(colIdx)
					vec.Nulls().UnsetNulls()
				}
			}

			for _, fullOuter := range []bool{false, true} {
				b.Run(fmt.Sprintf("fullOuter=%v", fullOuter), func(b *testing.B) {
					for _, rightDistinct := range []bool{true, false} {
						b.Run(fmt.Sprintf("distinct=%v", rightDistinct), func(b *testing.B) {
							for _, nBatches := range []int{1 << 1, 1 << 8, 1 << 12} {
								b.Run(fmt.Sprintf("rows=%d", nBatches*coldata.BatchSize()), func(b *testing.B) {
									// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
									// batch) * nCols (number of columns / row) * 2 (number of sources).
									b.SetBytes(int64(8 * nBatches * coldata.BatchSize() * nCols * 2))
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										leftSource := colexecop.NewRepeatableBatchSource(testAllocator, batch, sourceTypes)
										rightSource := colexectestutils.NewFiniteBatchSource(testAllocator, batch, sourceTypes, nBatches)
										joinType := descpb.InnerJoin
										if fullOuter {
											joinType = descpb.FullOuterJoin
										}
										hjSpec := colexecjoin.MakeHashJoinerSpec(
											joinType,
											[]uint32{0, 1}, []uint32{2, 3},
											sourceTypes, sourceTypes,
											rightDistinct,
										)
										hj := colexecjoin.NewHashJoiner(
											testAllocator, testAllocator, hjSpec,
											leftSource, rightSource,
											colexecjoin.HashJoinerInitialNumBuckets, execinfra.DefaultMemoryLimit,
										)
										hj.Init(ctx)

										for i := 0; i < nBatches; i++ {
											// Technically, the non-distinct hash join will produce much more
											// than nBatches of output.
											hj.Next()
										}
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

// TestHashJoinerProjection tests that planning of hash joiner correctly
// handles the "post-joiner" projection. The test uses different types with a
// projection in which output columns from both sides are intertwined so that
// if the projection is not handled correctly, the interface conversion panic
// would occur.
func TestHashJoinerProjection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg: &execinfra.ServerConfig{
			Settings: st,
		},
	}

	leftTypes := []*types.T{types.Bool, types.Int, types.Bytes}
	rightTypes := []*types.T{types.Int, types.Float, types.Decimal}
	leftTuples := colexectestutils.Tuples{{false, 1, "foo"}}
	rightTuples := colexectestutils.Tuples{{1, 1.1, decs[1]}}

	spec := &execinfrapb.ProcessorSpec{
		Core: execinfrapb.ProcessorCoreUnion{
			HashJoiner: &execinfrapb.HashJoinerSpec{
				LeftEqColumns:        []uint32{1},
				RightEqColumns:       []uint32{0},
				LeftEqColumnsAreKey:  true,
				RightEqColumnsAreKey: true,
			},
		},
		Input: []execinfrapb.InputSyncSpec{
			{ColumnTypes: leftTypes},
			{ColumnTypes: rightTypes},
		},
		Post: execinfrapb.PostProcessSpec{
			Projection: true,
			// The "core" of the test - we ask for a projection in which the columns
			// from the left and from the right are intertwined.
			OutputColumns: []uint32{3, 1, 0, 5, 4, 2},
		},
		ResultTypes: []*types.T{types.Int, types.Int, types.Bool, types.Decimal, types.Float, types.Bytes},
	}

	leftSource := colexectestutils.NewOpTestInput(testAllocator, 1, leftTuples, leftTypes)
	rightSource := colexectestutils.NewOpTestInput(testAllocator, 1, rightTuples, rightTypes)
	args := &colexecargs.NewColOperatorArgs{
		Spec:                spec,
		Inputs:              []colexecargs.OpWithMetaInfo{{Root: leftSource}, {Root: rightSource}},
		StreamingMemAccount: testMemAcc,
	}
	args.TestingKnobs.UseStreamingMemAccountForBuffering = true
	args.TestingKnobs.DiskSpillingDisabled = true
	hjOp, err := colexecargs.TestNewColOperator(ctx, flowCtx, args)
	require.NoError(t, err)
	hjOp.Root.Init(ctx)
	for b := hjOp.Root.Next(); b.Length() > 0; b = hjOp.Root.Next() {
		// The output types should be {Int64, Int64, Bool, Decimal, Float64, Bytes}
		// and we check this explicitly.
		b.ColVec(0).Int64()
		b.ColVec(1).Int64()
		b.ColVec(2).Bool()
		b.ColVec(3).Decimal()
		b.ColVec(4).Float64()
		b.ColVec(5).Bytes()
	}
}
