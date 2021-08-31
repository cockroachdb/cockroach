// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// {{/*
// +build execgen_template
//
// This file is the execgen template for select_in.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"sort"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ apd.Context
	_ duration.Duration
	_ = coldataext.CompareDatum
	_ json.JSON
)

// Remove unused warnings.
var (
	_ = colexecerror.InternalError
)

// {{/*

type _GOTYPESLICE interface{}
type _GOTYPE interface{}
type _TYPE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

func _COMPARE(_, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// Enum used to represent comparison results.
type comparisonResult int

const (
	siTrue comparisonResult = iota
	siFalse
	siNull
)

func GetInProjectionOperator(
	allocator *colmem.Allocator,
	t *types.T,
	input colexecop.Operator,
	colIdx int,
	resultIdx int,
	datumTuple *tree.DTuple,
	negate bool,
) (colexecop.Operator, error) {
	input = colexecutils.NewVectorTypeEnforcer(allocator, input, types.Bool, resultIdx)
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			obj := &projectInOp_TYPE{
				OneInputHelper: colexecop.MakeOneInputHelper(input),
				allocator:      allocator,
				colIdx:         colIdx,
				outputIdx:      resultIdx,
				negate:         negate,
			}
			obj.filterRow, obj.hasNulls = fillDatumRow_TYPE(t, datumTuple)
			return obj, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unhandled type: %s", t.Name())
}

func GetInOperator(
	t *types.T, input colexecop.Operator, colIdx int, datumTuple *tree.DTuple, negate bool,
) (colexecop.Operator, error) {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			obj := &selectInOp_TYPE{
				OneInputHelper: colexecop.MakeOneInputHelper(input),
				colIdx:         colIdx,
				negate:         negate,
			}
			obj.filterRow, obj.hasNulls = fillDatumRow_TYPE(t, datumTuple)
			return obj, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unhandled type: %s", t.Name())
}

// {{range .}}
// {{range .WidthOverloads}}

type selectInOp_TYPE struct {
	colexecop.OneInputHelper
	colIdx    int
	filterRow []_GOTYPE
	hasNulls  bool
	negate    bool
	sorted    bool
}

var _ colexecop.Operator = &selectInOp_TYPE{}

type projectInOp_TYPE struct {
	colexecop.OneInputHelper
	allocator *colmem.Allocator
	colIdx    int
	outputIdx int
	filterRow []_GOTYPE
	hasNulls  bool
	negate    bool
	sorted    bool
}

var _ colexecop.Operator = &projectInOp_TYPE{}

func fillDatumRow_TYPE(t *types.T, datumTuple *tree.DTuple) ([]_GOTYPE, bool) {
	conv := colconv.GetDatumToPhysicalFn(t)
	var result []_GOTYPE
	hasNulls := false
	for _, d := range datumTuple.D {
		if d == tree.DNull {
			hasNulls = true
		} else {
			convRaw := conv(d)
			converted := convRaw.(_GOTYPE)
			result = append(result, converted)
		}
	}
	return result, hasNulls
}

func sortDatumRow_TYPE(filterRow []_GOTYPE, targetCol _GOTYPESLICE) {
	less := func(i, j int) bool {
		var cmpResult int
		_COMPARE(cmpResult, filterRow[i], filterRow[j], targetCol, _)
		return cmpResult < 0
	}
	if !sort.SliceIsSorted(filterRow, less) {
		sort.Slice(filterRow, less)
	}
}

func cmpIn_TYPE(
	targetElem _GOTYPE, targetCol _GOTYPESLICE, filterRow []_GOTYPE, hasNulls bool,
) comparisonResult {
	// Filter row input was already sorted in sortDatumRow_TYPE, so we can
	// perform a binary search.
	lo := 0
	hi := len(filterRow)
	for lo < hi {
		i := (lo + hi) / 2
		var cmpResult int
		_COMPARE(cmpResult, targetElem, filterRow[i], targetCol, _)
		if cmpResult == 0 {
			return siTrue
		} else if cmpResult > 0 {
			lo = i + 1
		} else {
			hi = i
		}
	}

	if hasNulls {
		return siNull
	} else {
		return siFalse
	}
}

func (si *selectInOp_TYPE) Next() coldata.Batch {
	for {
		batch := si.Input.Next()
		if batch.Length() == 0 {
			return coldata.ZeroBatch
		}

		vec := batch.ColVec(si.colIdx)
		col := vec.TemplateType()
		var idx int
		n := batch.Length()

		// Sort si.filterRow once. We perform the sort here instead of in
		// fillDatumRow_TYPE because the compare overload requires the eval
		// context of a coldata.DatumVec target column.
		if !si.sorted {
			sortDatumRow_TYPE(si.filterRow, col)
			si.sorted = true
		}

		compVal := siTrue
		if si.negate {
			compVal = siFalse
		}

		if vec.MaybeHasNulls() {
			nulls := vec.Nulls()
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					v := col.Get(i)
					if !nulls.NullAt(i) && cmpIn_TYPE(v, col, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = i
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()
				_ = col.Get(n - 1)
				for i := 0; i < n; i++ {
					// {{if .Sliceable}}
					//gcassert:bce
					// {{end}}
					v := col.Get(i)
					if !nulls.NullAt(i) && cmpIn_TYPE(v, col, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = i
						idx++
					}
				}
			}
		} else {
			if sel := batch.Selection(); sel != nil {
				sel = sel[:n]
				for _, i := range sel {
					v := col.Get(i)
					if cmpIn_TYPE(v, col, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = i
						idx++
					}
				}
			} else {
				batch.SetSelection(true)
				sel := batch.Selection()
				_ = col.Get(n - 1)
				for i := 0; i < n; i++ {
					// {{if .Sliceable}}
					//gcassert:bce
					// {{end}}
					v := col.Get(i)
					if cmpIn_TYPE(v, col, si.filterRow, si.hasNulls) == compVal {
						sel[idx] = i
						idx++
					}
				}
			}
		}

		if idx > 0 {
			batch.SetLength(idx)
			return batch
		}
	}
}

func (pi *projectInOp_TYPE) Next() coldata.Batch {
	batch := pi.Input.Next()
	if batch.Length() == 0 {
		return coldata.ZeroBatch
	}

	vec := batch.ColVec(pi.colIdx)
	col := vec.TemplateType()

	// Sort pi.filterRow once. We perform the sort here instead of in
	// fillDatumRow_TYPE because the compare overload requires the eval context
	// of a coldata.DatumVec target column.
	if !pi.sorted {
		sortDatumRow_TYPE(pi.filterRow, col)
		pi.sorted = true
	}

	projVec := batch.ColVec(pi.outputIdx)
	projCol := projVec.Bool()
	projNulls := projVec.Nulls()
	if projVec.MaybeHasNulls() {
		// We need to make sure that there are no left over null values in the
		// output vector.
		projNulls.UnsetNulls()
	}

	n := batch.Length()

	cmpVal := siTrue
	if pi.negate {
		cmpVal = siFalse
	}

	if vec.MaybeHasNulls() {
		nulls := vec.Nulls()
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				if nulls.NullAt(i) {
					projNulls.SetNull(i)
				} else {
					v := col.Get(i)
					cmpRes := cmpIn_TYPE(v, col, pi.filterRow, pi.hasNulls)
					if cmpRes == siNull {
						projNulls.SetNull(i)
					} else {
						projCol[i] = cmpRes == cmpVal
					}
				}
			}
		} else {
			_ = col.Get(n - 1)
			for i := 0; i < n; i++ {
				if nulls.NullAt(i) {
					projNulls.SetNull(i)
				} else {
					// {{if .Sliceable}}
					//gcassert:bce
					// {{end}}
					v := col.Get(i)
					cmpRes := cmpIn_TYPE(v, col, pi.filterRow, pi.hasNulls)
					if cmpRes == siNull {
						projNulls.SetNull(i)
					} else {
						projCol[i] = cmpRes == cmpVal
					}
				}
			}
		}
	} else {
		if sel := batch.Selection(); sel != nil {
			sel = sel[:n]
			for _, i := range sel {
				v := col.Get(i)
				cmpRes := cmpIn_TYPE(v, col, pi.filterRow, pi.hasNulls)
				if cmpRes == siNull {
					projNulls.SetNull(i)
				} else {
					projCol[i] = cmpRes == cmpVal
				}
			}
		} else {
			_ = col.Get(n - 1)
			for i := 0; i < n; i++ {
				// {{if .Sliceable}}
				//gcassert:bce
				// {{end}}
				v := col.Get(i)
				cmpRes := cmpIn_TYPE(v, col, pi.filterRow, pi.hasNulls)
				if cmpRes == siNull {
					projNulls.SetNull(i)
				} else {
					projCol[i] = cmpRes == cmpVal
				}
			}
		}
	}
	return batch
}

// {{end}}
// {{end}}
