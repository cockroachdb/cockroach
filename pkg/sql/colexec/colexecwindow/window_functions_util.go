// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecwindow

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

// WindowArgs extracts common arguments to window operators.
type WindowArgs struct {
	EvalCtx         *tree.EvalContext
	MainAllocator   *colmem.Allocator
	BufferAllocator *colmem.Allocator
	MemoryLimit     int64
	QueueCfg        colcontainer.DiskQueueCfg
	FdSemaphore     semaphore.Semaphore
	DiskAcc         *mon.BoundAccount
	Input           colexecop.Operator
	InputTypes      []*types.T
	OutputColIdx    int
	PartitionColIdx int
	PeersColIdx     int
}

// windowFnMaxNumArgs is a mapping from the window function to the maximum
// number of arguments it takes.
var windowFnMaxNumArgs = map[execinfrapb.WindowerSpec_WindowFunc]int{
	execinfrapb.WindowerSpec_ROW_NUMBER:   0,
	execinfrapb.WindowerSpec_RANK:         0,
	execinfrapb.WindowerSpec_DENSE_RANK:   0,
	execinfrapb.WindowerSpec_PERCENT_RANK: 0,
	execinfrapb.WindowerSpec_CUME_DIST:    0,
	execinfrapb.WindowerSpec_NTILE:        1,
	execinfrapb.WindowerSpec_LAG:          3,
	execinfrapb.WindowerSpec_LEAD:         3,
	execinfrapb.WindowerSpec_FIRST_VALUE:  1,
	execinfrapb.WindowerSpec_LAST_VALUE:   1,
	execinfrapb.WindowerSpec_NTH_VALUE:    2,
}

// WindowFnNeedsPeersInfo returns whether a window function pays attention to
// the concept of "peers" during its computation ("peers" are tuples within the
// same partition - from PARTITION BY clause - that are not distinct on the
// columns in ORDER BY clause). For most window functions, the result of
// computation should be the same for "peers", so most window functions do need
// this information.
func WindowFnNeedsPeersInfo(windowFn *execinfrapb.WindowerSpec_WindowFn) bool {
	if windowFn.Func.WindowFunc != nil {
		switch *windowFn.Func.WindowFunc {
		case
			execinfrapb.WindowerSpec_ROW_NUMBER,
			execinfrapb.WindowerSpec_NTILE,
			execinfrapb.WindowerSpec_LAG,
			execinfrapb.WindowerSpec_LEAD:
			// Functions that ignore the concept of "peers."
			return false
		case
			execinfrapb.WindowerSpec_RANK,
			execinfrapb.WindowerSpec_DENSE_RANK,
			execinfrapb.WindowerSpec_PERCENT_RANK,
			execinfrapb.WindowerSpec_CUME_DIST:
			return true
		case
			execinfrapb.WindowerSpec_FIRST_VALUE,
			execinfrapb.WindowerSpec_LAST_VALUE,
			execinfrapb.WindowerSpec_NTH_VALUE:
			if len(windowFn.Ordering.Columns) == 0 {
				return false
			}
			return windowFrameNeedsPeersInfo(windowFn.Frame, &windowFn.Ordering)
		default:
			colexecerror.InternalError(errors.AssertionFailedf("window function %s is not supported", windowFn.String()))
			// This code is unreachable, but the compiler cannot infer that.
			return false
		}
	}
	// Aggregate window functions need the peer groups column only if the frame
	// requires it.
	return windowFrameNeedsPeersInfo(windowFn.Frame, &windowFn.Ordering)
}

// windowFrameNeedsPeersInfo returns whether a window function will need access
// to a peer group column.
func windowFrameNeedsPeersInfo(
	windowFrame *execinfrapb.WindowerSpec_Frame, ordering *execinfrapb.Ordering,
) bool {
	if len(ordering.Columns) == 0 {
		return false
	}
	switch windowFrame.Mode {
	case
		execinfrapb.WindowerSpec_Frame_GROUPS,
		execinfrapb.WindowerSpec_Frame_RANGE:
		if windowFrame.Bounds.Start.BoundType != execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING ||
			windowFrame.Bounds.End.BoundType != execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING {
			return true
		}
	}
	if windowFrame.Exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP ||
		windowFrame.Exclusion == execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES {
		return true
	}
	return false
}

// WindowFnArgNeedsCast returns true if the given argument type requires a cast,
// as well as the expected type (if the cast is needed). If the cast is not
// needed, the provided type is returned.
func WindowFnArgNeedsCast(
	windowFn execinfrapb.WindowerSpec_Func, provided *types.T, idx int,
) (needsCast bool, expectedType *types.T) {
	if windowFn.WindowFunc != nil {
		switch *windowFn.WindowFunc {
		case execinfrapb.WindowerSpec_NTILE:
			// NTile expects a single int64 argument.
			if idx != 0 {
				colexecerror.InternalError(errors.AssertionFailedf("ntile expects exactly one argument"))
			}
			return !types.Int.Identical(provided), types.Int
		case
			execinfrapb.WindowerSpec_LAG,
			execinfrapb.WindowerSpec_LEAD:
			if idx == 0 || idx == 2 {
				// The first and third arguments can have any type. No casting necessary.
				return false, provided
			}
			if idx == 1 {
				// The second argument is an integer offset that must be an int64.
				return !types.Int.Identical(provided), types.Int
			}
			colexecerror.InternalError(errors.AssertionFailedf("lag and lead expect between one and three arguments"))
		case
			execinfrapb.WindowerSpec_FIRST_VALUE,
			execinfrapb.WindowerSpec_LAST_VALUE:
			if idx > 0 {
				colexecerror.InternalError(errors.AssertionFailedf("first_value and last_value expect exactly one argument"))
			}
			// These window functions can take any argument type.
			return false, provided
		case execinfrapb.WindowerSpec_NTH_VALUE:
			// The first argument can be any type, but the second must be an integer.
			if idx > 1 {
				colexecerror.InternalError(errors.AssertionFailedf("nth_value expects exactly two arguments"))
			}
			if idx == 0 {
				return false, provided
			}
			return !types.Int.Identical(provided), types.Int
		case
			execinfrapb.WindowerSpec_ROW_NUMBER,
			execinfrapb.WindowerSpec_RANK,
			execinfrapb.WindowerSpec_DENSE_RANK,
			execinfrapb.WindowerSpec_PERCENT_RANK,
			execinfrapb.WindowerSpec_CUME_DIST:
			colexecerror.InternalError(errors.AssertionFailedf("window function %s does not expect an argument", windowFn.WindowFunc.String()))
			// This code is unreachable, but the compiler cannot infer that.
			return false, nil
		}
		colexecerror.InternalError(errors.AssertionFailedf("unknown window function: %v", windowFn.WindowFunc))
	}
	// Aggregate functions do not require casts.
	return false, provided
}

// NormalizeWindowFrame returns a frame that is identical to the given one
// except that the default values have been explicitly set (where before they
// may have been nil).
func NormalizeWindowFrame(frame *execinfrapb.WindowerSpec_Frame) *execinfrapb.WindowerSpec_Frame {
	if frame == nil {
		// The default window frame:
		//   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO ROWS
		return &execinfrapb.WindowerSpec_Frame{
			Mode: execinfrapb.WindowerSpec_Frame_RANGE,
			Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
				Start: execinfrapb.WindowerSpec_Frame_Bound{
					BoundType: execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING,
				},
				End: &execinfrapb.WindowerSpec_Frame_Bound{
					BoundType: execinfrapb.WindowerSpec_Frame_CURRENT_ROW,
				},
			},
			Exclusion: execinfrapb.WindowerSpec_Frame_NO_EXCLUSION,
		}
	}
	if frame.Bounds.End == nil {
		// The default end bound is CURRENT ROW.
		return &execinfrapb.WindowerSpec_Frame{
			Mode: frame.Mode,
			Bounds: execinfrapb.WindowerSpec_Frame_Bounds{
				Start: frame.Bounds.Start,
				End: &execinfrapb.WindowerSpec_Frame_Bound{
					BoundType: execinfrapb.WindowerSpec_Frame_CURRENT_ROW,
				},
			},
			Exclusion: frame.Exclusion,
		}
	}
	return frame
}

// EncodeWindowFrameOffset returns the given datum offset encoded as bytes, for
// use in testing window functions in RANGE mode with offsets.
func EncodeWindowFrameOffset(t *testing.T, offset tree.Datum) []byte {
	var encoded, scratch []byte
	encoded, err := rowenc.EncodeTableValue(
		encoded, descpb.ColumnID(encoding.NoColumnID), offset, scratch)
	require.NoError(t, err)
	return encoded
}

// MakeRandWindowFrameRangeOffset returns a valid offset of the given type for
// use in testing window functions in RANGE mode with offsets.
func MakeRandWindowFrameRangeOffset(t *testing.T, rng *rand.Rand, typ *types.T) tree.Datum {
	isNegative := func(val tree.Datum) bool {
		switch datumTyp := val.(type) {
		case *tree.DInt:
			return int64(*datumTyp) < 0
		case *tree.DFloat:
			return float64(*datumTyp) < 0
		case *tree.DDecimal:
			return datumTyp.Negative
		case *tree.DInterval:
			return false
		default:
			t.Errorf("unexpected error: %v", errors.AssertionFailedf("unsupported datum: %v", datumTyp))
			return false
		}
	}

	for {
		val := randgen.RandDatumSimple(rng, typ)
		if isNegative(val) {
			// Offsets must be non-null and non-negative.
			continue
		}
		return val
	}
}

// GetOffsetTypeFromOrderColType returns the correct offset type for the given
// order column type for a window frame in RANGE mode with offsets. For numeric
// columns, the order and offset types are the same. For datetime columns,
// offsets are of type interval. GetOffsetTypeFromOrderColType is intended for
// use in testing window functions.
func GetOffsetTypeFromOrderColType(t *testing.T, orderColType *types.T) *types.T {
	if !types.IsAdditiveType(orderColType) {
		t.Errorf("unexpected order column type: %v", orderColType)
	}
	if types.IsDateTimeType(orderColType) {
		return types.Interval
	}
	return orderColType
}

// isWindowFnLinear returns whether the vectorized engine has an implementation
// of the given window function that scales linearly when the default window
// frame is used.
func isWindowFnLinear(fn execinfrapb.WindowerSpec_Func) bool {
	if fn.WindowFunc != nil {
		return true
	}
	switch *fn.AggregateFunc {
	case
		execinfrapb.Count,
		execinfrapb.CountRows,
		execinfrapb.Sum,
		execinfrapb.SumInt,
		execinfrapb.Avg,
		execinfrapb.Min,
		execinfrapb.Max:
		return true
	default:
		return false
	}
}

// WindowFrameCanShrink returns true if a sliding window aggregate function over
// the given frame may need to call Remove, which is the case when the frame for
// a given row may not include all rows that were part of the previous frame.
func WindowFrameCanShrink(
	frame *execinfrapb.WindowerSpec_Frame, ordering *execinfrapb.Ordering,
) bool {
	if frame.Exclusion != execinfrapb.WindowerSpec_Frame_NO_EXCLUSION {
		return true
	}
	if frame.Bounds.Start.BoundType == execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING {
		return false
	}
	if len(ordering.Columns) == 0 {
		// All rows are part of the same peer group.
		if frame.Bounds.Start.BoundType == execinfrapb.WindowerSpec_Frame_CURRENT_ROW &&
			(frame.Mode == execinfrapb.WindowerSpec_Frame_RANGE ||
				frame.Mode == execinfrapb.WindowerSpec_Frame_GROUPS) {
			return false
		}
		if frame.Mode == execinfrapb.WindowerSpec_Frame_GROUPS &&
			frame.Bounds.Start.BoundType == execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING &&
			frame.Bounds.Start.IntOffset >= 1 {
			return false
		}
	}
	return true
}
