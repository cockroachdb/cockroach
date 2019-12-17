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
	"runtime"
	"testing"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

type hjTestCase struct {
	leftTypes  []coltypes.T
	rightTypes []coltypes.T

	leftTuples  tuples
	rightTuples tuples

	leftEqCols  []uint32
	rightEqCols []uint32

	leftOutCols  []uint32
	rightOutCols []uint32

	// The default joinType is sqlbase.JoinType_INNER if this value is not set.
	joinType sqlbase.JoinType

	leftEqColsAreKey  bool
	rightEqColsAreKey bool

	expectedTuples tuples

	onExpr execinfrapb.Expression
}

var (
	floats = []float64{0.314, 3.14, 31.4, 314}
	decs   []apd.Decimal
	tcs    []hjTestCase
)

func init() {
	// Set up the apd.Decimal values used in tests.
	decs = make([]apd.Decimal, len(floats))
	for i, f := range floats {
		_, err := decs[i].SetFloat64(f)
		if err != nil {
			execerror.VectorizedInternalPanic(fmt.Sprintf("%v", err))
		}
	}

	tcs = []hjTestCase{
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			leftTuples: tuples{
				{0},
				{1},
				{2},
				{3},
			},
			rightTuples: tuples{
				{-1},
				{1},
				{3},
				{5},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          sqlbase.JoinType_FULL_OUTER,
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{nil, -1},
				{1, 1},
				{3, 3},
				{nil, 5},
				{0, nil},
				{2, nil},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test an empty build table.
			leftTuples: tuples{},
			rightTuples: tuples{
				{-1},
				{1},
				{3},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:         sqlbase.JoinType_FULL_OUTER,
			leftEqColsAreKey: true,

			expectedTuples: tuples{
				{nil, -1},
				{nil, 1},
				{nil, 3},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			leftTuples: tuples{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			rightTuples: tuples{
				{1},
				{3},
				{5},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          sqlbase.JoinType_LEFT_OUTER,
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{1, 1},
				{3, 3},
				{0, nil},
				{2, nil},
				{4, nil},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test right outer join.
			leftTuples: tuples{
				{0},
				{1},
			},
			rightTuples: tuples{
				{1},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          sqlbase.JoinType_RIGHT_OUTER,
			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{1, 1},
				{nil, 2},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test right outer join with non-distinct left build table with an
			// unmatched row from the right followed by a matched one. This is a
			// regression test for #39303 in order to check that probeRowUnmatched
			// is updated correctly in case of non-distinct build table.
			leftTuples: tuples{
				{0},
				{0},
				{2},
			},
			rightTuples: tuples{
				{1},
				{2},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},

			joinType:          sqlbase.JoinType_RIGHT_OUTER,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{nil, 1},
				{2, 2},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test null handling only on probe column.
			leftTuples: tuples{
				{0},
			},
			rightTuples: tuples{
				{nil},
				{0},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: false,

			expectedTuples: tuples{
				{0},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test null handling only on build column.
			leftTuples: tuples{
				{nil},
				{nil},
				{1},
				{0},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{1},
				{0},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64},

			// Test null handling in output columns.
			leftTuples: tuples{
				{1, nil},
				{2, nil},
				{3, 1},
				{4, 2},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{nil, 2},
				{nil, nil},
				{1, nil},
				{2, 4},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test null handling in hash join key column.
			leftTuples: tuples{
				{1},
				{3},
				{nil},
				{2},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{2},
				{3},
				{1},
			},
		},
		{
			// Test handling of multiple column non-distinct equality keys.
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},

			leftTuples: tuples{
				{0, 0, 1},
				{0, 0, 2},
				{1, 0, 3},
				{1, 1, 4},
				{1, 1, 5},
				{0, 0, 6},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
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
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			leftTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize},
				{hashTableBucketSize},
				{0},
				{hashTableBucketSize * 2},
				{1},
				{1},
				{hashTableBucketSize + 1},
			},
			rightTuples: tuples{
				{hashTableBucketSize},
				{hashTableBucketSize * 2},
				{hashTableBucketSize * 3},
				{0},
				{1},
				{hashTableBucketSize + 1},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{0},
			// Note that although right equality columns are key, we want to test
			// handling of collisions, so we "lie" here.
			leftEqColsAreKey:  false,
			rightEqColsAreKey: false,

			expectedTuples: tuples{
				{hashTableBucketSize, hashTableBucketSize},
				{hashTableBucketSize, hashTableBucketSize},
				{hashTableBucketSize, hashTableBucketSize},
				{hashTableBucketSize * 2, hashTableBucketSize * 2},
				{0, 0},
				{0, 0},
				{1, 1},
				{1, 1},
				{hashTableBucketSize + 1, hashTableBucketSize + 1},
			},
		},
		{
			// Test handling of duplicate equality keys.
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			leftTuples: tuples{
				{0},
				{0},
				{1},
				{1},
				{1},
				{2},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
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
			// Test handling of various output column coltypes.
			leftTypes:  []coltypes.T{coltypes.Bool, coltypes.Int64, coltypes.Bytes, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Float64, coltypes.Int32},

			leftTuples: tuples{
				{false, 5, "a", 10},
				{true, 3, "b", 30},
				{false, 2, "foo", 20},
				{false, 6, "bar", 50},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{2, "foo", 2, int32(2)},
				{3, "b", 3, int32(4)},
				{5, "a", 5, int32(16)},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Reverse engineering hash table hash heuristic to find key values that
			// hash to the same bucket.
			leftTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 2},
				{hashTableBucketSize * 3},
			},
			rightTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 3},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{0},
				{hashTableBucketSize},
				{hashTableBucketSize * 3},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			// Test a N:1 inner join where the right side key has duplicate values.
			leftTuples: tuples{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{1, 1},
				{1, 1},
				{1, 1},
				{2, 2},
				{2, 2},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},

			// Test inner join on multiple equality columns.
			leftTuples: tuples{
				{0, 0, 10},
				{0, 1, 20},
				{0, 2, 30},
				{1, 1, 40},
				{1, 2, 50},
				{2, 0, 60},
				{2, 1, 70},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{0, 2, 30, 100},
				{1, 1, 40, 200},
				{2, 0, 60, 300},
				{2, 1, 70, 400},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64},

			// Test multiple column with values that hash to the same bucket.
			leftTuples: tuples{
				{10, 0, 0},
				{20, 0, hashTableBucketSize},
				{40, hashTableBucketSize, 0},
				{50, hashTableBucketSize, hashTableBucketSize},
				{60, hashTableBucketSize * 2, 0},
				{70, hashTableBucketSize * 2, hashTableBucketSize},
			},
			rightTuples: tuples{
				{0, hashTableBucketSize},
				{hashTableBucketSize * 2, hashTableBucketSize},
				{0, 0},
				{0, hashTableBucketSize * 2},
			},

			leftEqCols:   []uint32{1, 2},
			rightEqCols:  []uint32{0, 1},
			leftOutCols:  []uint32{0, 1, 2},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{20, 0, hashTableBucketSize},
				{70, hashTableBucketSize * 2, hashTableBucketSize},
				{10, 0, 0},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Bytes, coltypes.Bool, coltypes.Int16, coltypes.Int32, coltypes.Int64, coltypes.Bytes},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int32, coltypes.Int16, coltypes.Bool, coltypes.Bytes},

			// Test multiple equality columns of different coltypes.
			leftTuples: tuples{
				{"foo", false, int16(100), int32(1000), int64(10000), "aaa"},
				{"foo", true, 100, 1000, 10000, "bbb"},
				{"foo1", false, 100, 1000, 10000, "ccc"},
				{"foo", false, 200, 1000, 10000, "ddd"},
				{"foo", false, 100, 2000, 10000, "eee"},
				{"bar", true, 300, 3000, 30000, "fff"},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{"ccc"},
				{"aaa"},
				{"fff"},
				{"ddd"},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Float64},
			rightTypes: []coltypes.T{coltypes.Float64},

			// Test equality columns of type float.
			leftTuples: tuples{
				{float64(33.333)},
				{float64(44.4444)},
				{float64(55.55555)},
				{float64(44.4444)},
			},
			rightTuples: tuples{
				{float64(44.4444)},
				{float64(55.55555)},
				{float64(33.333)},
			},

			leftEqCols:   []uint32{0},
			rightEqCols:  []uint32{0},
			leftOutCols:  []uint32{0},
			rightOutCols: []uint32{},

			leftEqColsAreKey:  true,
			rightEqColsAreKey: true,

			expectedTuples: tuples{
				{float64(55.55555)},
				{float64(44.4444)},
				{float64(44.4444)},
				{float64(33.333)},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64, coltypes.Int64, coltypes.Int64},

			// Test use right side as build table.
			leftTuples: tuples{
				{2, 4, 8, 16},
				{3, 3, 2, 2},
				{3, 7, 2, 1},
				{5, 4, 3, 2},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{3, 3, 2, 2, 1, 2, 3, 4},
				{3, 7, 2, 1, 1, 2, 3, 4},
				{5, 4, 3, 2, 1, 3, 5, 7},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Decimal},
			rightTypes: []coltypes.T{coltypes.Decimal},

			// Test coltypes.Decimal type as equality column.
			leftTuples: tuples{
				{decs[0]},
				{decs[1]},
				{decs[2]},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{decs[2]},
				{decs[0]},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64},

			joinType: sqlbase.JoinType_LEFT_SEMI,

			leftTuples: tuples{
				{0},
				{0},
				{1},
				{2},
			},
			rightTuples: tuples{
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

			expectedTuples: tuples{
				{0},
				{0},
				{1},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64},

			// Test ON expression.
			leftTuples: tuples{
				{1, nil},
				{2, nil},
				{3, 1},
				{4, 2},
			},
			rightTuples: tuples{
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
			expectedTuples: tuples{
				{nil, nil},
				{1, nil},
			},
		},
		{
			leftTypes:  []coltypes.T{coltypes.Int64, coltypes.Int64},
			rightTypes: []coltypes.T{coltypes.Int64, coltypes.Int64},

			// Test ON expression.
			leftTuples: tuples{
				{1, nil},
				{2, nil},
				{3, 1},
				{4, 2},
			},
			rightTuples: tuples{
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
			expectedTuples: tuples{
				{nil, 2},
				{2, 4},
			},
		},
	}
}

func createSpecForHashJoiner(tc hjTestCase) *execinfrapb.ProcessorSpec {
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
	for _, outCol := range tc.rightOutCols {
		projection = append(projection, rColOffset+outCol)
	}
	return &execinfrapb.ProcessorSpec{
		Input: []execinfrapb.InputSyncSpec{
			{ColumnTypes: typeconv.ToColumnTypes(tc.leftTypes)},
			{ColumnTypes: typeconv.ToColumnTypes(tc.rightTypes)},
		},
		Core: execinfrapb.ProcessorCoreUnion{
			HashJoiner: hjSpec,
		},
		Post: execinfrapb.PostProcessSpec{
			Projection:    true,
			OutputColumns: projection,
		},
	}
}

func TestHashJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	flowCtx := &execinfra.FlowCtx{
		EvalCtx: &evalCtx,
		Cfg:     &execinfra.ServerConfig{Settings: st},
	}

	for _, outputBatchSize := range []uint16{1, 17, coldata.BatchSize()} {
		for _, tc := range tcs {
			inputs := []tuples{tc.leftTuples, tc.rightTuples}
			typs := [][]coltypes.T{tc.leftTypes, tc.rightTypes}
			runTestsWithTyps(t, inputs, typs, tc.expectedTuples, unorderedVerifier, func(sources []Operator) (Operator, error) {
				spec := createSpecForHashJoiner(tc)
				args := NewColOperatorArgs{
					Spec:                               spec,
					Inputs:                             sources,
					StreamingMemAccount:                testMemAcc,
					UseStreamingMemAccountForBuffering: true,
				}
				result, err := NewColOperator(ctx, flowCtx, args)
				if err != nil {
					return nil, err
				}
				if hj, ok := result.Op.(*hashJoinEqOp); ok {
					hj.outputBatchSize = outputBatchSize
				}
				return result.Op, nil
			})
		}
	}
}

func TestHashJoinerOutputsOnlyRequestedColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, tc := range tcs {
		leftSource := newOpTestInput(1, tc.leftTuples, tc.leftTypes)
		rightSource := newOpTestInput(1, tc.rightTuples, tc.rightTypes)
		hjOp, err := NewEqHashJoinerOp(
			testAllocator,
			leftSource, rightSource,
			tc.leftEqCols, tc.rightEqCols,
			tc.leftOutCols, tc.rightOutCols,
			tc.leftTypes, tc.rightTypes,
			tc.rightEqColsAreKey, tc.leftEqColsAreKey || tc.rightEqColsAreKey,
			tc.joinType)
		require.NoError(t, err)
		hjOp.Init()
		for {
			b := hjOp.Next(context.Background())
			if b.Length() == 0 {
				break
			}
			require.Equal(t, len(tc.leftOutCols)+(len(tc.rightOutCols)), b.Width())
		}
	}
}

func BenchmarkHashJoiner(b *testing.B) {
	ctx := context.Background()
	nCols := 4
	sourceTypes := make([]coltypes.T, nCols)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		sourceTypes[colIdx] = coltypes.Int64
	}

	batch := testAllocator.NewMemBatch(sourceTypes)

	for colIdx := 0; colIdx < nCols; colIdx++ {
		col := batch.ColVec(colIdx).Int64()
		for i := 0; i < int(coldata.BatchSize()); i++ {
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
					for _, buildDistinct := range []bool{true, false} {
						b.Run(fmt.Sprintf("distinct=%v", buildDistinct), func(b *testing.B) {
							for _, nBatches := range []int{1 << 1, 1 << 8, 1 << 12} {
								b.Run(fmt.Sprintf("rows=%d", nBatches*int(coldata.BatchSize())), func(b *testing.B) {
									// 8 (bytes / int64) * nBatches (number of batches) * col.BatchSize() (rows /
									// batch) * nCols (number of columns / row) * 2 (number of sources).
									b.SetBytes(int64(8 * nBatches * int(coldata.BatchSize()) * nCols * 2))
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										leftSource := newFiniteBatchSource(batch, nBatches)
										rightSource := NewRepeatableBatchSource(batch)

										spec := hashJoinerSpec{
											left: hashJoinerSourceSpec{
												eqCols:      []uint32{0, 2},
												outCols:     []uint32{0, 1},
												sourceTypes: sourceTypes,
												source:      leftSource,
												outer:       fullOuter,
											},

											right: hashJoinerSourceSpec{
												eqCols:      []uint32{1, 3},
												outCols:     []uint32{2, 3},
												sourceTypes: sourceTypes,
												source:      rightSource,
												outer:       fullOuter,
											},

											buildDistinct: buildDistinct,
										}

										hj := &hashJoinEqOp{
											allocator:       testAllocator,
											spec:            spec,
											outputBatchSize: coldata.BatchSize(),
										}

										hj.Init()

										for i := 0; i < nBatches; i++ {
											// Technically, the non-distinct hash join will produce much more
											// than nBatches of output.
											hj.Next(ctx)
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

// TestHashingDoesNotAllocate ensures that our use of the noescape hack to make
// sure hashing with unsafe.Pointer doesn't allocate still works correctly.
func TestHashingDoesNotAllocate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var sum uintptr
	foundAllocations := 0
	for i := 0; i < 10; i++ {
		// Sometimes, Go allocates somewhere else. To make this test not flaky,
		// let's just make sure that at least one of the rounds of this loop doesn't
		// allocate at all.
		s := &runtime.MemStats{}
		runtime.ReadMemStats(s)
		numAlloc := s.TotalAlloc
		i := 10
		x := memhash64(noescape(unsafe.Pointer(&i)), 0)
		runtime.ReadMemStats(s)

		if numAlloc != s.TotalAlloc {
			foundAllocations++
		}
		sum += x
	}
	if foundAllocations == 10 {
		// Uhoh, we allocated every single time. This probably means we regressed,
		// and our hash function allocates.
		t.Fatalf("memhash64(noescape(&i)) allocated at least once")
	}
	t.Log(sum)
}

// TestHashJoinerProjection tests that planning of hash joiner correctly
// handles the "post-joiner" projection. The test uses different types with a
// projection in which output columns from both sides are intertwined so that
// if the projection is not handled correctly, the interface conversion panic
// would occur.
func TestHashJoinerProjection(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

	leftTypes := []types.T{*types.Bool, *types.Int, *types.Bytes}
	leftColTypes := []coltypes.T{coltypes.Bool, coltypes.Int64, coltypes.Bytes}
	rightTypes := []types.T{*types.Int, *types.Float, *types.Decimal}
	rightColTypes := []coltypes.T{coltypes.Int64, coltypes.Float64, coltypes.Decimal}
	leftTuples := tuples{{false, 1, "foo"}}
	rightTuples := tuples{{1, 1.1, decs[1]}}

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
	}

	leftSource := newOpTestInput(1, leftTuples, leftColTypes)
	rightSource := newOpTestInput(1, rightTuples, rightColTypes)
	args := NewColOperatorArgs{
		Spec:                               spec,
		Inputs:                             []Operator{leftSource, rightSource},
		StreamingMemAccount:                testMemAcc,
		UseStreamingMemAccountForBuffering: true,
	}
	hjOp, err := NewColOperator(ctx, flowCtx, args)
	require.NoError(t, err)
	hjOp.Op.Init()
	for {
		b := hjOp.Op.Next(ctx)
		// The output types should be {Int64, Int64, Bool, Decimal, Float64, Bytes}
		// and we check this explicitly.
		b.ColVec(0).Int64()
		b.ColVec(1).Int64()
		b.ColVec(2).Bool()
		b.ColVec(3).Decimal()
		b.ColVec(4).Float64()
		b.ColVec(5).Bytes()
		if b.Length() == 0 {
			break
		}
	}
}
