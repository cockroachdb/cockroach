// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra/execagg"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

func createWindowFnSpec(
	ctx context.Context,
	planCtx *PlanningCtx,
	plan *PhysicalPlan,
	funcInProgress *windowFuncHolder,
	ordCols []execinfrapb.Ordering_Column,
) (execinfrapb.WindowerSpec_WindowFn, *types.T, error) {
	for _, argIdx := range funcInProgress.argsIdxs {
		if argIdx >= uint32(len(plan.GetResultTypes())) {
			return execinfrapb.WindowerSpec_WindowFn{}, nil, errors.Errorf("ColIdx out of range (%d)", argIdx)
		}
	}
	// Figure out which built-in to compute.
	funcSpec, err := rowexec.CreateWindowerSpecFunc(funcInProgress.expr.Func.String())
	if err != nil {
		return execinfrapb.WindowerSpec_WindowFn{}, nil, err
	}
	argTypes := make([]*types.T, len(funcInProgress.argsIdxs))
	for i, argIdx := range funcInProgress.argsIdxs {
		argTypes[i] = plan.GetResultTypes()[argIdx]
	}
	_, outputType, err := execagg.GetWindowFunctionInfo(funcSpec, argTypes...)
	if err != nil {
		return execinfrapb.WindowerSpec_WindowFn{}, outputType, err
	}
	funcInProgressSpec := execinfrapb.WindowerSpec_WindowFn{
		Func:         funcSpec,
		ArgsIdxs:     funcInProgress.argsIdxs,
		Ordering:     execinfrapb.Ordering{Columns: ordCols},
		FilterColIdx: int32(funcInProgress.filterColIdx),
		OutputColIdx: uint32(funcInProgress.outputColIdx),
	}
	if funcInProgress.frame != nil {
		// funcInProgress has a custom window frame.
		frameSpec := &execinfrapb.WindowerSpec_Frame{}
		if err := initFrameFromAST(ctx, frameSpec, funcInProgress.frame, planCtx.EvalContext()); err != nil {
			return execinfrapb.WindowerSpec_WindowFn{}, outputType, err
		}
		funcInProgressSpec.Frame = frameSpec
	}

	return funcInProgressSpec, outputType, nil
}

// initFrameFromAST initializes the spec based on tree.WindowFrame. It will
// evaluate offset expressions if present in the frame.
func initFrameFromAST(
	ctx context.Context,
	spec *execinfrapb.WindowerSpec_Frame,
	f *tree.WindowFrame,
	evalCtx *eval.Context,
) error {
	if err := initModeFromAST(&spec.Mode, f.Mode); err != nil {
		return err
	}
	if err := initExclusionFromAST(&spec.Exclusion, f.Exclusion); err != nil {
		return err
	}
	return initBoundsFromAST(ctx, &spec.Bounds, f.Bounds, f.Mode, evalCtx)
}

func initModeFromAST(
	spec *execinfrapb.WindowerSpec_Frame_Mode, w treewindow.WindowFrameMode,
) error {
	switch w {
	case treewindow.RANGE:
		*spec = execinfrapb.WindowerSpec_Frame_RANGE
	case treewindow.ROWS:
		*spec = execinfrapb.WindowerSpec_Frame_ROWS
	case treewindow.GROUPS:
		*spec = execinfrapb.WindowerSpec_Frame_GROUPS
	default:
		return errors.AssertionFailedf("unexpected WindowFrameMode")
	}
	return nil
}

func initExclusionFromAST(
	spec *execinfrapb.WindowerSpec_Frame_Exclusion, e treewindow.WindowFrameExclusion,
) error {
	switch e {
	case treewindow.NoExclusion:
		*spec = execinfrapb.WindowerSpec_Frame_NO_EXCLUSION
	case treewindow.ExcludeCurrentRow:
		*spec = execinfrapb.WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
	case treewindow.ExcludeGroup:
		*spec = execinfrapb.WindowerSpec_Frame_EXCLUDE_GROUP
	case treewindow.ExcludeTies:
		*spec = execinfrapb.WindowerSpec_Frame_EXCLUDE_TIES
	default:
		return errors.AssertionFailedf("unexpected WindowerFrameExclusion")
	}
	return nil
}

func initBoundTypeFromAST(
	spec *execinfrapb.WindowerSpec_Frame_BoundType, bt treewindow.WindowFrameBoundType,
) error {
	switch bt {
	case treewindow.UnboundedPreceding:
		*spec = execinfrapb.WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case treewindow.OffsetPreceding:
		*spec = execinfrapb.WindowerSpec_Frame_OFFSET_PRECEDING
	case treewindow.CurrentRow:
		*spec = execinfrapb.WindowerSpec_Frame_CURRENT_ROW
	case treewindow.OffsetFollowing:
		*spec = execinfrapb.WindowerSpec_Frame_OFFSET_FOLLOWING
	case treewindow.UnboundedFollowing:
		*spec = execinfrapb.WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	default:
		return errors.AssertionFailedf("unexpected WindowFrameBoundType")
	}
	return nil
}

// If offset exprs are present, we evaluate them and save the encoded results
// in the spec.
func initBoundsFromAST(
	ctx context.Context,
	spec *execinfrapb.WindowerSpec_Frame_Bounds,
	b tree.WindowFrameBounds,
	m treewindow.WindowFrameMode,
	evalCtx *eval.Context,
) error {
	if b.StartBound == nil {
		return errors.Errorf("unexpected: Start Bound is nil")
	}
	spec.Start = execinfrapb.WindowerSpec_Frame_Bound{}
	if err := initBoundTypeFromAST(&spec.Start.BoundType, b.StartBound.BoundType); err != nil {
		return err
	}
	if b.StartBound.HasOffset() {
		typedStartOffset := b.StartBound.OffsetExpr.(tree.TypedExpr)
		dStartOffset, err := eval.Expr(ctx, evalCtx, typedStartOffset)
		if err != nil {
			return err
		}
		if dStartOffset == tree.DNull {
			return pgerror.Newf(pgcode.NullValueNotAllowed, "frame starting offset must not be null")
		}
		switch m {
		case treewindow.ROWS:
			startOffset := int64(tree.MustBeDInt(dStartOffset))
			if startOffset < 0 {
				return pgerror.Newf(pgcode.InvalidWindowFrameOffset, "frame starting offset must not be negative")
			}
			spec.Start.IntOffset = uint64(startOffset)
		case treewindow.RANGE:
			if neg, err := isNegative(ctx, evalCtx, dStartOffset); err != nil {
				return err
			} else if neg {
				return pgerror.Newf(pgcode.InvalidWindowFrameOffset, "invalid preceding or following size in window function")
			}
			typ := dStartOffset.ResolvedType()
			spec.Start.OffsetType = execinfrapb.DatumInfo{Encoding: catenumpb.DatumEncoding_VALUE, Type: typ}
			var buf []byte
			var a tree.DatumAlloc
			datum := rowenc.DatumToEncDatum(typ, dStartOffset)
			buf, err = datum.Encode(typ, &a, catenumpb.DatumEncoding_VALUE, buf)
			if err != nil {
				return err
			}
			spec.Start.TypedOffset = buf
		case treewindow.GROUPS:
			startOffset := int64(tree.MustBeDInt(dStartOffset))
			if startOffset < 0 {
				return pgerror.Newf(pgcode.InvalidWindowFrameOffset, "frame starting offset must not be negative")
			}
			spec.Start.IntOffset = uint64(startOffset)
		}
	}

	if b.EndBound != nil {
		spec.End = &execinfrapb.WindowerSpec_Frame_Bound{}
		if err := initBoundTypeFromAST(&spec.End.BoundType, b.EndBound.BoundType); err != nil {
			return err
		}
		if b.EndBound.HasOffset() {
			typedEndOffset := b.EndBound.OffsetExpr.(tree.TypedExpr)
			dEndOffset, err := eval.Expr(ctx, evalCtx, typedEndOffset)
			if err != nil {
				return err
			}
			if dEndOffset == tree.DNull {
				return pgerror.Newf(pgcode.NullValueNotAllowed, "frame ending offset must not be null")
			}
			switch m {
			case treewindow.ROWS:
				endOffset := int64(tree.MustBeDInt(dEndOffset))
				if endOffset < 0 {
					return pgerror.Newf(pgcode.InvalidWindowFrameOffset, "frame ending offset must not be negative")
				}
				spec.End.IntOffset = uint64(endOffset)
			case treewindow.RANGE:
				if neg, err := isNegative(ctx, evalCtx, dEndOffset); err != nil {
					return err
				} else if neg {
					return pgerror.Newf(pgcode.InvalidWindowFrameOffset, "invalid preceding or following size in window function")
				}
				typ := dEndOffset.ResolvedType()
				spec.End.OffsetType = execinfrapb.DatumInfo{Encoding: catenumpb.DatumEncoding_VALUE, Type: typ}
				var buf []byte
				var a tree.DatumAlloc
				datum := rowenc.DatumToEncDatum(typ, dEndOffset)
				buf, err = datum.Encode(typ, &a, catenumpb.DatumEncoding_VALUE, buf)
				if err != nil {
					return err
				}
				spec.End.TypedOffset = buf
			case treewindow.GROUPS:
				endOffset := int64(tree.MustBeDInt(dEndOffset))
				if endOffset < 0 {
					return pgerror.Newf(pgcode.InvalidWindowFrameOffset, "frame ending offset must not be negative")
				}
				spec.End.IntOffset = uint64(endOffset)
			}
		}
	}

	return nil
}

// isNegative returns whether offset is negative.
func isNegative(ctx context.Context, evalCtx *eval.Context, offset tree.Datum) (bool, error) {
	switch o := offset.(type) {
	case *tree.DInt:
		return *o < 0, nil
	case *tree.DDecimal:
		return o.Negative, nil
	case *tree.DFloat:
		return *o < 0, nil
	case *tree.DInterval:
		cmp, err := o.Compare(ctx, evalCtx, &tree.DInterval{Duration: duration.Duration{}})
		return cmp < 0, err
	default:
		panic("unexpected offset type")
	}
}
