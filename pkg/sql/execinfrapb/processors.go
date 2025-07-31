// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	context "context"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/errors"
)

// GetAggregateFuncIdx converts the aggregate function name to the enum value
// with the same string representation.
func GetAggregateFuncIdx(funcName string) (int32, error) {
	var ub upperBuffer //gcassert:noescape
	funcStr, ok := ub.ToUpper(funcName)
	if !ok {
		return 0, errors.Errorf("unknown aggregate %s", funcName)
	}
	funcIdx, ok := AggregatorSpec_Func_value[funcStr]
	if !ok {
		return 0, errors.Errorf("unknown aggregate %s", funcName)
	}
	return funcIdx, nil
}

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

// IsScalar returns whether the aggregate function is in scalar context.
func (spec *AggregatorSpec) IsScalar() bool {
	switch spec.Type {
	case AggregatorSpec_SCALAR:
		return true
	case AggregatorSpec_NON_SCALAR:
		return false
	default:
		// This case exists for backward compatibility.
		return (len(spec.GroupCols) == 0)
	}
}

// IsRowCount returns true if the aggregator spec is scalar and has a single
// COUNT_ROWS aggregation with no FILTER or DISTINCT.
func (spec *AggregatorSpec) IsRowCount() bool {
	return len(spec.Aggregations) == 1 &&
		spec.Aggregations[0].FilterColIdx == nil &&
		spec.Aggregations[0].Func == CountRows &&
		!spec.Aggregations[0].Distinct &&
		spec.IsScalar()
}

// GetWindowFuncIdx converts the window function name to the enum value with
// the same string representation.
func GetWindowFuncIdx(funcName string) (int32, error) {
	var ub upperBuffer //gcassert:noescape
	funcStr, ok := ub.ToUpper(funcName)
	if !ok {
		return 0, errors.Errorf("unknown window function %s", funcName)
	}
	funcIdx, ok := WindowerSpec_WindowFunc_value[funcStr]
	if !ok {
		return 0, errors.Errorf("unknown window function %s", funcName)
	}
	return funcIdx, nil
}

func (spec *WindowerSpec_Frame_Mode) initFromAST(w treewindow.WindowFrameMode) error {
	switch w {
	case treewindow.RANGE:
		*spec = WindowerSpec_Frame_RANGE
	case treewindow.ROWS:
		*spec = WindowerSpec_Frame_ROWS
	case treewindow.GROUPS:
		*spec = WindowerSpec_Frame_GROUPS
	default:
		return errors.AssertionFailedf("unexpected WindowFrameMode")
	}
	return nil
}

func (spec *WindowerSpec_Frame_BoundType) initFromAST(bt treewindow.WindowFrameBoundType) error {
	switch bt {
	case treewindow.UnboundedPreceding:
		*spec = WindowerSpec_Frame_UNBOUNDED_PRECEDING
	case treewindow.OffsetPreceding:
		*spec = WindowerSpec_Frame_OFFSET_PRECEDING
	case treewindow.CurrentRow:
		*spec = WindowerSpec_Frame_CURRENT_ROW
	case treewindow.OffsetFollowing:
		*spec = WindowerSpec_Frame_OFFSET_FOLLOWING
	case treewindow.UnboundedFollowing:
		*spec = WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	default:
		return errors.AssertionFailedf("unexpected WindowFrameBoundType")
	}
	return nil
}

func (spec *WindowerSpec_Frame_Exclusion) initFromAST(e treewindow.WindowFrameExclusion) error {
	switch e {
	case treewindow.NoExclusion:
		*spec = WindowerSpec_Frame_NO_EXCLUSION
	case treewindow.ExcludeCurrentRow:
		*spec = WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
	case treewindow.ExcludeGroup:
		*spec = WindowerSpec_Frame_EXCLUDE_GROUP
	case treewindow.ExcludeTies:
		*spec = WindowerSpec_Frame_EXCLUDE_TIES
	default:
		return errors.AssertionFailedf("unexpected WindowerFrameExclusion")
	}
	return nil
}

// If offset exprs are present, we evaluate them and save the encoded results
// in the spec.
func (spec *WindowerSpec_Frame_Bounds) initFromAST(
	ctx context.Context,
	b tree.WindowFrameBounds,
	m treewindow.WindowFrameMode,
	evalCtx *eval.Context,
) error {
	if b.StartBound == nil {
		return errors.Errorf("unexpected: Start Bound is nil")
	}
	spec.Start = WindowerSpec_Frame_Bound{}
	if err := spec.Start.BoundType.initFromAST(b.StartBound.BoundType); err != nil {
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
			spec.Start.OffsetType = DatumInfo{Encoding: catenumpb.DatumEncoding_VALUE, Type: typ}
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
		spec.End = &WindowerSpec_Frame_Bound{}
		if err := spec.End.BoundType.initFromAST(b.EndBound.BoundType); err != nil {
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
				spec.End.OffsetType = DatumInfo{Encoding: catenumpb.DatumEncoding_VALUE, Type: typ}
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

// InitFromAST initializes the spec based on tree.WindowFrame. It will evaluate
// offset expressions if present in the frame.
func (spec *WindowerSpec_Frame) InitFromAST(
	ctx context.Context, f *tree.WindowFrame, evalCtx *eval.Context,
) error {
	if err := spec.Mode.initFromAST(f.Mode); err != nil {
		return err
	}
	if err := spec.Exclusion.initFromAST(f.Exclusion); err != nil {
		return err
	}
	return spec.Bounds.initFromAST(ctx, f.Bounds, f.Mode, evalCtx)
}

func (spec WindowerSpec_Frame_Mode) convertToAST() (treewindow.WindowFrameMode, error) {
	switch spec {
	case WindowerSpec_Frame_RANGE:
		return treewindow.RANGE, nil
	case WindowerSpec_Frame_ROWS:
		return treewindow.ROWS, nil
	case WindowerSpec_Frame_GROUPS:
		return treewindow.GROUPS, nil
	default:
		return treewindow.WindowFrameMode(0), errors.AssertionFailedf("unexpected WindowerSpec_Frame_Mode")
	}
}

func (spec WindowerSpec_Frame_BoundType) convertToAST() (treewindow.WindowFrameBoundType, error) {
	switch spec {
	case WindowerSpec_Frame_UNBOUNDED_PRECEDING:
		return treewindow.UnboundedPreceding, nil
	case WindowerSpec_Frame_OFFSET_PRECEDING:
		return treewindow.OffsetPreceding, nil
	case WindowerSpec_Frame_CURRENT_ROW:
		return treewindow.CurrentRow, nil
	case WindowerSpec_Frame_OFFSET_FOLLOWING:
		return treewindow.OffsetFollowing, nil
	case WindowerSpec_Frame_UNBOUNDED_FOLLOWING:
		return treewindow.UnboundedFollowing, nil
	default:
		return treewindow.WindowFrameBoundType(0), errors.AssertionFailedf("unexpected WindowerSpec_Frame_BoundType")
	}
}

func (spec WindowerSpec_Frame_Exclusion) convertToAST() (treewindow.WindowFrameExclusion, error) {
	switch spec {
	case WindowerSpec_Frame_NO_EXCLUSION:
		return treewindow.NoExclusion, nil
	case WindowerSpec_Frame_EXCLUDE_CURRENT_ROW:
		return treewindow.ExcludeCurrentRow, nil
	case WindowerSpec_Frame_EXCLUDE_GROUP:
		return treewindow.ExcludeGroup, nil
	case WindowerSpec_Frame_EXCLUDE_TIES:
		return treewindow.ExcludeTies, nil
	default:
		return treewindow.WindowFrameExclusion(0), errors.AssertionFailedf("unexpected WindowerSpec_Frame_Exclusion")
	}
}

// convertToAST produces tree.WindowFrameBounds based on
// WindowerSpec_Frame_Bounds. Note that it might not be fully equivalent to
// original - if offsetExprs were present in original tree.WindowFrameBounds,
// they are not included.
func (spec WindowerSpec_Frame_Bounds) convertToAST() (tree.WindowFrameBounds, error) {
	bounds := tree.WindowFrameBounds{}
	startBoundType, err := spec.Start.BoundType.convertToAST()
	if err != nil {
		return bounds, err
	}
	bounds.StartBound = &tree.WindowFrameBound{
		BoundType: startBoundType,
	}

	if spec.End != nil {
		endBoundType, err := spec.End.BoundType.convertToAST()
		if err != nil {
			return bounds, err
		}
		bounds.EndBound = &tree.WindowFrameBound{BoundType: endBoundType}
	}
	return bounds, nil
}

// ConvertToAST produces a tree.WindowFrame given a WindoweSpec_Frame.
func (spec *WindowerSpec_Frame) ConvertToAST() (*tree.WindowFrame, error) {
	mode, err := spec.Mode.convertToAST()
	if err != nil {
		return nil, err
	}
	bounds, err := spec.Bounds.convertToAST()
	if err != nil {
		return nil, err
	}
	exclusion, err := spec.Exclusion.convertToAST()
	if err != nil {
		return nil, err
	}
	return &tree.WindowFrame{
		Mode:      mode,
		Bounds:    bounds,
		Exclusion: exclusion,
	}, nil
}

// IsIndexJoin returns true if spec defines an index join (as opposed to a
// lookup join).
func (spec *JoinReaderSpec) IsIndexJoin() bool {
	return len(spec.LookupColumns) == 0 && spec.LookupExpr.Empty()
}

// init performs some sanity checks for the invariants required by the
// upperBuffer type.
func init() {
	isAllASCII := func(s string) bool {
		for i := range s {
			if s[i] >= utf8.RuneSelf {
				return false
			}
		}
		return true
	}
	// Check that aggregate function names are not longer than upperBufferSize
	// and that they do not have non-ASCII characters. If these invariants
	// change in the future, upperBufferSize or ToUpper will need to be
	// adjusted.
	for funcStr := range AggregatorSpec_Func_value {
		if len(funcStr) > upperBufferSize {
			panic(errors.AssertionFailedf(
				"aggregate function name length cannot exceed length %d: %q",
				upperBufferSize, funcStr,
			))
		}
		if !isAllASCII(funcStr) {
			panic(errors.AssertionFailedf(
				"aggregate function name cannot contain non-ASCII characters: %q", funcStr,
			))
		}
	}

	// Perform the same check for window function names.
	for funcStr := range WindowerSpec_WindowFunc_value {
		if len(funcStr) > upperBufferSize {
			panic(errors.AssertionFailedf(
				"window function name length cannot exceed length %d: %q",
				upperBufferSize, funcStr,
			))
		}
		if !isAllASCII(funcStr) {
			panic(errors.AssertionFailedf(
				"window function name cannot contain non-ASCII characters: %q", funcStr,
			))
		}
	}
}

const (
	// upperBufferSize is large enough to accommodate the longest aggregate or
	// window function name.
	upperBufferSize = 31
)

// upperBuffer is a helper struct for creating a temporary upper-cased string
// without performing a heap allocation. See ToUpper.
type upperBuffer struct {
	buf  [upperBufferSize]byte
	used bool
}

// ToUpper returns a string where every lowercase ASCII character in "s" has
// been converted to an uppercase character. If the length of "s" is greater
// than upperBufferSize, ok=false is returned. If ToUpper returns ok=true, all
// future invocations on the same upperBuffer will return ok=false.
func (ub *upperBuffer) ToUpper(s string) (_ string, ok bool) {
	if ub.used {
		// Don't allow the buffer to be reused.
		return "", false
	}
	if len(s) > upperBufferSize {
		// The init function guarantees that no aggregate or window function has
		// a name longer than upperBufferSize bytes. We can return ok=false here
		// because upper-casing is pointless - the lookups in
		// GetAggregateFuncIdx and GetWindowFuncIdx would fail anyway.
		return "", false
	}
	ub.used = true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		ub.buf[i] = c
	}
	// Convert the buffer to a string without allocating.
	return unsafe.String(noescape(&ub.buf[0]), len(s)), true
}

// noescape hides a pointer from escape analysis.  noescape is the identity
// function but escape analysis doesn't think the output depends on the input.
// noescape is inlined and currently compiles down to zero instructions.
// USE CAREFULLY!
//
// This was copied from the strings package.
//
//go:nosplit
//go:nocheckptr
func noescape(p *byte) *byte {
	x := uintptr(unsafe.Pointer(p))
	//lint:ignore SA4016 x ^ 0 is a no-op that fools escape analysis.
	return (*byte)(unsafe.Pointer(x ^ 0)) // nolint:unsafeptr
}
