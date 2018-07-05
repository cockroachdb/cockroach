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

package distsqlplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

func mapToSpecMode(m tree.WindowFrameMode) distsqlrun.WindowerSpec_Frame_Mode {
	switch m {
	case tree.RANGE:
		return distsqlrun.WindowerSpec_Frame_RANGE
	case tree.ROWS:
		return distsqlrun.WindowerSpec_Frame_ROWS
	default:
		panic("unexpected WindowFrameMode")
	}
}

func mapToSpecBoundType(bt tree.WindowFrameBoundType) distsqlrun.WindowerSpec_Frame_BoundType {
	switch bt {
	case tree.UnboundedPreceding:
		return distsqlrun.WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case tree.ValuePreceding:
		return distsqlrun.WindowerSpec_Frame_VALUE_PRECEDING
	case tree.CurrentRow:
		return distsqlrun.WindowerSpec_Frame_CURRENT_ROW
	case tree.ValueFollowing:
		return distsqlrun.WindowerSpec_Frame_VALUE_FOLLOWING
	case tree.UnboundedFollowing:
		return distsqlrun.WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	default:
		panic("unexpected WindowFrameBoundType")
	}
}

// If offset exprs are present, we evaluate them and save the encoded results
// in the spec.
func convertToSpecBounds(
	b tree.WindowFrameBounds, evalCtx *tree.EvalContext,
) (distsqlrun.WindowerSpec_Frame_Bounds, error) {
	bounds := distsqlrun.WindowerSpec_Frame_Bounds{}
	if b.StartBound == nil {
		return bounds, errors.Errorf("unexpected: Start Bound is nil")
	}
	var a sqlbase.DatumAlloc
	bounds.Start = distsqlrun.WindowerSpec_Frame_Bound{
		BoundType: mapToSpecBoundType(b.StartBound.BoundType),
	}
	if b.StartBound.OffsetExpr != nil {
		typedStartOffset := b.StartBound.OffsetExpr.(tree.TypedExpr)
		dStartOffset, err := typedStartOffset.Eval(evalCtx)
		if err != nil {
			return bounds, err
		}
		if dStartOffset == tree.DNull {
			return bounds, pgerror.NewErrorf(pgerror.CodeNullValueNotAllowedError, "frame starting offset must not be null")
		}
		startOffset := int(tree.MustBeDInt(dStartOffset))
		if startOffset < 0 {
			return bounds, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "frame starting offset must not be negative")
		}
		colTyp, err := sqlbase.DatumTypeToColumnType(types.Int)
		if err != nil {
			return bounds, err
		}
		encOffset := sqlbase.DatumToEncDatum(colTyp, dStartOffset)
		var buf []byte
		buf, err = encOffset.Encode(&colTyp, &a, sqlbase.DatumEncoding_VALUE, buf)
		if err != nil {
			return bounds, err
		}
		bounds.Start.Offset = buf
		bounds.Start.OffsetType = &distsqlrun.DatumInfo{
			Encoding: sqlbase.DatumEncoding_VALUE,
			Type:     colTyp,
		}
	}

	if b.EndBound != nil {
		bounds.End = &distsqlrun.WindowerSpec_Frame_Bound{BoundType: mapToSpecBoundType(b.EndBound.BoundType)}
		if b.EndBound.OffsetExpr != nil {
			typedEndOffset := b.EndBound.OffsetExpr.(tree.TypedExpr)
			dEndOffset, err := typedEndOffset.Eval(evalCtx)
			if err != nil {
				return bounds, err
			}
			if dEndOffset == tree.DNull {
				return bounds, pgerror.NewErrorf(pgerror.CodeNullValueNotAllowedError, "frame ending offset must not be null")
			}
			endOffset := int(tree.MustBeDInt(dEndOffset))
			if endOffset < 0 {
				return bounds, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "frame ending offset must not be negative")
			}
			colTyp, err := sqlbase.DatumTypeToColumnType(types.Int)
			if err != nil {
				return bounds, err
			}
			encOffset := sqlbase.DatumToEncDatum(colTyp, dEndOffset)
			var buf []byte
			buf, err = encOffset.Encode(&colTyp, &a, sqlbase.DatumEncoding_VALUE, buf)
			if err != nil {
				return bounds, err
			}
			bounds.End.Offset = buf
			bounds.End.OffsetType = &distsqlrun.DatumInfo{
				Encoding: sqlbase.DatumEncoding_VALUE,
				Type:     colTyp,
			}
		}
	}

	return bounds, nil
}

// ConvertToSpec produces spec based on WindowFrame. It also evaluates offset
// expressions if present in the frame.
func ConvertToSpec(
	f *tree.WindowFrame, evalCtx *tree.EvalContext,
) (distsqlrun.WindowerSpec_Frame, error) {
	frame := distsqlrun.WindowerSpec_Frame{Mode: mapToSpecMode(f.Mode)}
	bounds, err := convertToSpecBounds(f.Bounds, evalCtx)
	frame.Bounds = bounds
	return frame, err
}
