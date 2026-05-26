// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecwindow

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/marusama/semaphore"
)

// WindowArgs extracts common arguments to window operators.
type WindowArgs struct {
	EvalCtx         *eval.Context
	MainAllocator   *colmem.Allocator
	BufferAllocator *colmem.Allocator
	MemoryLimit     int64
	QueueCfg        colcontainer.DiskQueueCfg
	FdSemaphore     semaphore.Semaphore
	DiskAcc         *mon.BoundAccount
	DiskQueueMemAcc *mon.BoundAccount
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

// WindowFnArgCasts returns a list of types to which the given window function
// arguments should be cast. For arguments that do not require a cast, the
// corresponding types in the list are nil.
func WindowFnArgCasts(windowFn execinfrapb.WindowerSpec_Func, argTypes []*types.T) []*types.T {
	castTo := make([]*types.T, len(argTypes))

	// Only window functions require casts.
	if windowFn.WindowFunc != nil {
		switch *windowFn.WindowFunc {
		case execinfrapb.WindowerSpec_NTILE:
			// NTile expects a single int64 argument.
			if len(argTypes) != 1 {
				colexecerror.InternalError(
					errors.AssertionFailedf("ntile expects exactly one argument"))
			}
			if !argTypes[0].Identical(types.Int) {
				castTo[0] = types.Int
			}
		case
			execinfrapb.WindowerSpec_LAG,
			execinfrapb.WindowerSpec_LEAD:
			if len(argTypes) < 1 || len(argTypes) > 3 {
				colexecerror.InternalError(
					errors.AssertionFailedf("lead and lag expect between one and three arguments"))
			}
			if len(argTypes) >= 2 && !argTypes[1].Identical(types.Int) {
				// The second argument is an integer offset that must be an int64.
				castTo[1] = types.Int
			}
			if len(argTypes) == 3 && !argTypes[0].Identical(argTypes[2]) {
				// The third argument must have the same type as the first argument.
				castTo[2] = argTypes[0]
			}
		case
			execinfrapb.WindowerSpec_FIRST_VALUE,
			execinfrapb.WindowerSpec_LAST_VALUE:
			if len(argTypes) != 1 {
				colexecerror.InternalError(errors.AssertionFailedf("first_value and last_value expect exactly one argument"))
			}
			// Any argument type is allowed, so no casts necessary.
		case execinfrapb.WindowerSpec_NTH_VALUE:
			// The first argument can be any type, but the second must be an integer.
			if len(argTypes) != 2 {
				colexecerror.InternalError(errors.AssertionFailedf("nth_value expects exactly two arguments"))
			}
			if !argTypes[1].Identical(types.Int) {
				castTo[1] = types.Int
			}
		case
			execinfrapb.WindowerSpec_ROW_NUMBER,
			execinfrapb.WindowerSpec_RANK,
			execinfrapb.WindowerSpec_DENSE_RANK,
			execinfrapb.WindowerSpec_PERCENT_RANK,
			execinfrapb.WindowerSpec_CUME_DIST:
			// No arguments expected.
		default:
			colexecerror.InternalError(errors.AssertionFailedf("unknown window function: %v", windowFn.WindowFunc))
		}
	}
	return castTo
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
