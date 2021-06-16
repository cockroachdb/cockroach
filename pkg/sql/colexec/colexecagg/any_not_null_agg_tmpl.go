// Copyright 2018 The Cockroach Authors.
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
// This file is the execgen template for any_not_null_agg.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexecagg

import (
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execgen"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
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
	_ tree.AggType
	_ apd.Context
	_ duration.Duration
	_ json.JSON
	_ colexecerror.StorageError
	_ coldataext.Datum
)

// {{/*

// Declarations to make the template compile properly.

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// */}}

func newAnyNotNull_AGGKINDAggAlloc(
	allocator *colmem.Allocator, t *types.T, allocSize int64,
) (aggregateFuncAlloc, error) {
	allocBase := aggAllocBase{allocator: allocator, allocSize: allocSize}
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &anyNotNull_TYPE_AGGKINDAggAlloc{aggAllocBase: allocBase}, nil
			// {{end}}
		}
		// {{end}}
	}
	return nil, errors.Errorf("unsupported any not null agg type %s", t.Name())
}

// {{range .}}
// {{range .WidthOverloads}}

// anyNotNull_TYPE_AGGKINDAgg implements the ANY_NOT_NULL aggregate, returning the
// first non-null value in the input column.
type anyNotNull_TYPE_AGGKINDAgg struct {
	// {{if eq "_AGGKIND" "Ordered"}}
	orderedAggregateFuncBase
	// {{else}}
	hashAggregateFuncBase
	// {{end}}
	col                         _GOTYPESLICE
	curAgg                      _GOTYPE
	foundNonNullForCurrentGroup bool
}

var _ AggregateFunc = &anyNotNull_TYPE_AGGKINDAgg{}

func (a *anyNotNull_TYPE_AGGKINDAgg) SetOutput(vec coldata.Vec) {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.SetOutput(vec)
	// {{else}}
	a.hashAggregateFuncBase.SetOutput(vec)
	// {{end}}
	a.col = vec.TemplateType()
}

func (a *anyNotNull_TYPE_AGGKINDAgg) Compute(
	vecs []coldata.Vec, inputIdxs []uint32, inputLen int, sel []int,
) {
	// {{if eq "_AGGKIND" "Hash"}}
	if a.foundNonNullForCurrentGroup {
		// We have already seen non-null for the current group, and since there
		// is at most a single group when performing hash aggregation, we can
		// finish computing.
		return
	}
	// {{end}}

	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	vec := vecs[inputIdxs[0]]
	col, nulls := vec.TemplateType(), vec.Nulls()
	a.allocator.PerformOperation([]coldata.Vec{a.vec}, func() {
		// {{if eq "_AGGKIND" "Ordered"}}
		// Capture groups and col to force bounds check to work. See
		// https://github.com/golang/go/issues/39756
		groups := a.groups
		col := col
		// {{/*
		// We don't need to check whether sel is non-nil when performing
		// hash aggregation because the hash aggregator always uses non-nil
		// sel to specify the tuples to be aggregated.
		// */}}
		if sel == nil {
			_ = groups[inputLen-1]
			_ = col.Get(inputLen - 1)
			if nulls.MaybeHasNulls() {
				for i := 0; i < inputLen; i++ {
					_FIND_ANY_NOT_NULL(a, groups, nulls, i, true, false)
				}
			} else {
				for i := 0; i < inputLen; i++ {
					_FIND_ANY_NOT_NULL(a, groups, nulls, i, false, false)
				}
			}
		} else
		// {{end}}
		{
			sel = sel[:inputLen]
			if nulls.MaybeHasNulls() {
				for _, i := range sel {
					_FIND_ANY_NOT_NULL(a, groups, nulls, i, true, true)
				}
			} else {
				for _, i := range sel {
					_FIND_ANY_NOT_NULL(a, groups, nulls, i, false, true)
				}
			}
		}
	},
	)
	execgen.SETVARIABLESIZE(newCurAggSize, a.curAgg)
	if newCurAggSize != oldCurAggSize {
		a.allocator.AdjustMemoryUsage(int64(newCurAggSize - oldCurAggSize))
	}
}

func (a *anyNotNull_TYPE_AGGKINDAgg) Flush(outputIdx int) {
	// If we haven't found any non-nulls for this group so far, the output for
	// this group should be null.
	// {{if eq "_AGGKIND" "Ordered"}}
	// Go around "argument overwritten before first use" linter error.
	_ = outputIdx
	outputIdx = a.curIdx
	a.curIdx++
	// {{end}}
	if !a.foundNonNullForCurrentGroup {
		a.nulls.SetNull(outputIdx)
	} else {
		a.col.Set(outputIdx, a.curAgg)
	}
	// {{if or (.IsBytesLike) (eq .VecMethod "Datum")}}
	// Release the reference to curAgg eagerly.
	execgen.SETVARIABLESIZE(oldCurAggSize, a.curAgg)
	a.allocator.AdjustMemoryUsage(-int64(oldCurAggSize))
	a.curAgg = nil
	// {{end}}
}

func (a *anyNotNull_TYPE_AGGKINDAgg) Reset() {
	// {{if eq "_AGGKIND" "Ordered"}}
	a.orderedAggregateFuncBase.Reset()
	// {{end}}
	a.foundNonNullForCurrentGroup = false
}

type anyNotNull_TYPE_AGGKINDAggAlloc struct {
	aggAllocBase
	aggFuncs []anyNotNull_TYPE_AGGKINDAgg
}

var _ aggregateFuncAlloc = &anyNotNull_TYPE_AGGKINDAggAlloc{}

const sizeOfAnyNotNull_TYPE_AGGKINDAgg = int64(unsafe.Sizeof(anyNotNull_TYPE_AGGKINDAgg{}))
const anyNotNull_TYPE_AGGKINDAggSliceOverhead = int64(unsafe.Sizeof([]anyNotNull_TYPE_AGGKINDAgg{}))

func (a *anyNotNull_TYPE_AGGKINDAggAlloc) newAggFunc() AggregateFunc {
	if len(a.aggFuncs) == 0 {
		a.allocator.AdjustMemoryUsage(anyNotNull_TYPE_AGGKINDAggSliceOverhead + sizeOfAnyNotNull_TYPE_AGGKINDAgg*a.allocSize)
		a.aggFuncs = make([]anyNotNull_TYPE_AGGKINDAgg, a.allocSize)
	}
	f := &a.aggFuncs[0]
	f.allocator = a.allocator
	a.aggFuncs = a.aggFuncs[1:]
	return f
}

// {{end}}
// {{end}}

// {{/*
// _FIND_ANY_NOT_NULL finds a non-null value for the group that contains the ith
// row. If a non-null value was already found, then it does nothing. If this is
// the first row of a new group, and no non-nulls have been found for the
// current group, then the output for the current group is set to null.
func _FIND_ANY_NOT_NULL(
	a *anyNotNull_TYPE_AGGKINDAgg,
	groups []bool,
	nulls *coldata.Nulls,
	i int,
	_HAS_NULLS bool,
	_HAS_SEL bool,
) { // */}}
	// {{define "findAnyNotNull" -}}

	// {{if eq "_AGGKIND" "Ordered"}}
	// {{if not .HasSel}}
	//gcassert:bce
	// {{end}}
	if groups[i] {
		if !a.isFirstGroup {
			// If this is a new group, check if any non-nulls have been found for the
			// current group.
			if !a.foundNonNullForCurrentGroup {
				a.nulls.SetNull(a.curIdx)
			} else {
				a.col.Set(a.curIdx, a.curAgg)
			}
			a.curIdx++
			a.foundNonNullForCurrentGroup = false
		}
		a.isFirstGroup = false
	}
	// {{end}}

	var isNull bool
	// {{if .HasNulls}}
	isNull = nulls.NullAt(i)
	// {{else}}
	isNull = false
	// {{end}}
	if !a.foundNonNullForCurrentGroup && !isNull {
		// If we haven't seen any non-nulls for the current group yet, and the
		// current value is non-null, then we can pick the current value to be
		// the output.
		// {{if and (.Sliceable) (not .HasSel)}}
		//gcassert:bce
		// {{end}}
		val := col.Get(i)
		// {{with .Global}}
		execgen.COPYVAL(a.curAgg, val)
		// {{end}}
		a.foundNonNullForCurrentGroup = true
		// {{if eq "_AGGKIND" "Hash"}}
		// We have already seen non-null for the current group, and since there
		// is at most a single group when performing hash aggregation, we can
		// finish computing.
		return
		// {{end}}
	}
	// {{end}}

	// {{/*
} // */}}
