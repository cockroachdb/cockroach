// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/pkg/errors"
)

// Equals returns true if two aggregation specifiers are identical (and thus
// will always yield the same result).
func (a AggregatorSpec_Aggregation) Equals(b AggregatorSpec_Aggregation) bool {
	if a.Func != b.Func || a.Distinct != b.Distinct {
		return false
	}
	if a.FilterColIdx == nil {
		if b.FilterColIdx != nil {
			return false
		}
	} else {
		if b.FilterColIdx == nil || *a.FilterColIdx != *b.FilterColIdx {
			return false
		}
	}
	if len(a.ColIdx) != len(b.ColIdx) {
		return false
	}
	for i, c := range a.ColIdx {
		if c != b.ColIdx[i] {
			return false
		}
	}
	return true
}

func (spec *WindowerSpec_Frame_Mode) initFromAST(w tree.WindowFrameMode) {
	switch w {
	case tree.RANGE:
		*spec = WindowerSpec_Frame_RANGE
	case tree.ROWS:
		*spec = WindowerSpec_Frame_ROWS
	case tree.GROUPS:
		*spec = WindowerSpec_Frame_GROUPS
	default:
		panic("unexpected WindowFrameMode")
	}
}

func (spec *WindowerSpec_Frame_BoundType) initFromAST(bt tree.WindowFrameBoundType) {
	switch bt {
	case tree.UnboundedPreceding:
		*spec = WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case tree.OffsetPreceding:
		*spec = WindowerSpec_Frame_OFFSET_PRECEDING
	case tree.CurrentRow:
		*spec = WindowerSpec_Frame_CURRENT_ROW
	case tree.OffsetFollowing:
		*spec = WindowerSpec_Frame_OFFSET_FOLLOWING
	case tree.UnboundedFollowing:
		*spec = WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	default:
		panic("unexpected WindowFrameBoundType")
	}
}

// If offset exprs are present, we evaluate them and save the encoded results
// in the spec.
func (spec *WindowerSpec_Frame_Bounds) initFromAST(
	b tree.WindowFrameBounds, m tree.WindowFrameMode, evalCtx *tree.EvalContext,
) error {
	if b.StartBound == nil {
		return errors.Errorf("unexpected: Start Bound is nil")
	}
	spec.Start = WindowerSpec_Frame_Bound{}
	spec.Start.BoundType.initFromAST(b.StartBound.BoundType)
	if b.StartBound.HasOffset() {
		typedStartOffset := b.StartBound.OffsetExpr.(tree.TypedExpr)
		dStartOffset, err := typedStartOffset.Eval(evalCtx)
		if err != nil {
			return err
		}
		if dStartOffset == tree.DNull {
			return pgerror.NewErrorf(pgerror.CodeNullValueNotAllowedError, "frame starting offset must not be null")
		}
		switch m {
		case tree.ROWS:
			startOffset := int(tree.MustBeDInt(dStartOffset))
			if startOffset < 0 {
				return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame starting offset must not be negative")
			}
			spec.Start.IntOffset = uint32(startOffset)
		case tree.RANGE:
			if isNegative(evalCtx, dStartOffset) {
				return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "invalid preceding or following size in window function")
			}
			typ, err := sqlbase.DatumTypeToColumnType(dStartOffset.ResolvedType())
			if err != nil {
				return err
			}
			spec.Start.OffsetType = DatumInfo{Encoding: sqlbase.DatumEncoding_VALUE, Type: typ}
			var buf []byte
			var a sqlbase.DatumAlloc
			datum := sqlbase.DatumToEncDatum(typ, dStartOffset)
			buf, err = datum.Encode(&typ, &a, sqlbase.DatumEncoding_VALUE, buf)
			if err != nil {
				return err
			}
			spec.Start.TypedOffset = buf
		case tree.GROUPS:
			startOffset := int(tree.MustBeDInt(dStartOffset))
			if startOffset < 0 {
				return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame starting offset must not be negative")
			}
			spec.Start.IntOffset = uint32(startOffset)
		}
	}

	if b.EndBound != nil {
		spec.End = &WindowerSpec_Frame_Bound{}
		spec.End.BoundType.initFromAST(b.EndBound.BoundType)
		if b.EndBound.HasOffset() {
			typedEndOffset := b.EndBound.OffsetExpr.(tree.TypedExpr)
			dEndOffset, err := typedEndOffset.Eval(evalCtx)
			if err != nil {
				return err
			}
			if dEndOffset == tree.DNull {
				return pgerror.NewErrorf(pgerror.CodeNullValueNotAllowedError, "frame ending offset must not be null")
			}
			switch m {
			case tree.ROWS:
				endOffset := int(tree.MustBeDInt(dEndOffset))
				if endOffset < 0 {
					return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame ending offset must not be negative")
				}
				spec.End.IntOffset = uint32(endOffset)
			case tree.RANGE:
				if isNegative(evalCtx, dEndOffset) {
					return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "invalid preceding or following size in window function")
				}
				typ, err := sqlbase.DatumTypeToColumnType(dEndOffset.ResolvedType())
				if err != nil {
					return err
				}
				spec.End.OffsetType = DatumInfo{Encoding: sqlbase.DatumEncoding_VALUE, Type: typ}
				var buf []byte
				var a sqlbase.DatumAlloc
				datum := sqlbase.DatumToEncDatum(typ, dEndOffset)
				buf, err = datum.Encode(&typ, &a, sqlbase.DatumEncoding_VALUE, buf)
				if err != nil {
					return err
				}
				spec.End.TypedOffset = buf
			case tree.GROUPS:
				endOffset := int(tree.MustBeDInt(dEndOffset))
				if endOffset < 0 {
					return pgerror.NewErrorf(pgerror.CodeInvalidWindowFrameOffsetError, "frame ending offset must not be negative")
				}
				spec.End.IntOffset = uint32(endOffset)
			}
		}
	}

	return nil
}

// isNegative returns whether offset is negative.
func isNegative(evalCtx *tree.EvalContext, offset tree.Datum) bool {
	switch o := offset.(type) {
	case *tree.DInt:
		return *o < 0
	case *tree.DDecimal:
		return o.Negative
	case *tree.DFloat:
		return *o < 0
	case *tree.DInterval:
		return o.Compare(evalCtx, &tree.DInterval{Duration: duration.Duration{}}) < 0
	default:
		panic("unexpected offset type")
	}
}

// InitFromAST initializes the spec based on tree.WindowFrame. It will evaluate
// offset expressions if present in the frame.
func (spec *WindowerSpec_Frame) InitFromAST(f *tree.WindowFrame, evalCtx *tree.EvalContext) error {
	spec.Mode.initFromAST(f.Mode)
	return spec.Bounds.initFromAST(f.Bounds, f.Mode, evalCtx)
}

func (spec WindowerSpec_Frame_Mode) convertToAST() tree.WindowFrameMode {
	switch spec {
	case WindowerSpec_Frame_RANGE:
		return tree.RANGE
	case WindowerSpec_Frame_ROWS:
		return tree.ROWS
	case WindowerSpec_Frame_GROUPS:
		return tree.GROUPS
	default:
		panic("unexpected WindowerSpec_Frame_Mode")
	}
}

func (spec WindowerSpec_Frame_BoundType) convertToAST() tree.WindowFrameBoundType {
	switch spec {
	case WindowerSpec_Frame_UNBOUNDED_PRECEDING:
		return tree.UnboundedPreceding
	case WindowerSpec_Frame_OFFSET_PRECEDING:
		return tree.OffsetPreceding
	case WindowerSpec_Frame_CURRENT_ROW:
		return tree.CurrentRow
	case WindowerSpec_Frame_OFFSET_FOLLOWING:
		return tree.OffsetFollowing
	case WindowerSpec_Frame_UNBOUNDED_FOLLOWING:
		return tree.UnboundedFollowing
	default:
		panic("unexpected WindowerSpec_Frame_BoundType")
	}
}

// convertToAST produces tree.WindowFrameBounds based on
// WindowerSpec_Frame_Bounds. Note that it might not be fully equivalent to
// original - if offsetExprs were present in original tree.WindowFrameBounds,
// they are not included.
func (spec WindowerSpec_Frame_Bounds) convertToAST() tree.WindowFrameBounds {
	bounds := tree.WindowFrameBounds{StartBound: &tree.WindowFrameBound{
		BoundType: spec.Start.BoundType.convertToAST(),
	}}
	if spec.End != nil {
		bounds.EndBound = &tree.WindowFrameBound{BoundType: spec.End.BoundType.convertToAST()}
	}
	return bounds
}

// ConvertToAST produces a tree.WindowFrame given a WindoweSpec_Frame.
func (spec *WindowerSpec_Frame) ConvertToAST() *tree.WindowFrame {
	return &tree.WindowFrame{Mode: spec.Mode.convertToAST(), Bounds: spec.Bounds.convertToAST()}
}
