// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// {{/*
//go:build execgen_template

//
// This file is the execgen template for sort.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/memsize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ = coldataext.CompareDatum
	_ tree.AggType
)

// {{/*

// Declarations to make the template compile properly.

// _GOTYPESLICE is the template variable.
type _GOTYPESLICE interface{}

// _CANONICAL_TYPE_FAMILY is the template variable.
const _CANONICAL_TYPE_FAMILY = types.UnknownFamily

// _TYPE_WIDTH is the template variable.
const _TYPE_WIDTH = 0

// _DIR_ENUM is the template variable.
const _DIR_ENUM = 0

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the second input < the third input.
func _ASSIGN_LT(_, _, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// {{range .}}
// {{$nulls := .Nulls}}
func newSingleSorter_WITH_NULLS(t *types.T, dir execinfrapb.Ordering_Column_Direction) colSorter {
	switch dir {
	// {{range .DirOverloads}}
	// {{$dir := .DirString}}
	case _DIR_ENUM:
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		// {{range .FamilyOverloads}}
		case _CANONICAL_TYPE_FAMILY:
			switch t.Width() {
			// {{range .WidthOverloads}}
			case _TYPE_WIDTH:
				return &sort_TYPE_DIR_HANDLES_NULLSOp{}
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unsupported type %s", t))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// {{end}}

// {{range .}}
// {{$nulls := .Nulls}}
// {{range .DirOverloads}}
// {{$dir := .DirString}}
// {{range .FamilyOverloads}}
// {{range .WidthOverloads}}

type sort_TYPE_DIR_HANDLES_NULLSOp struct {
	// {{if .CanAbbreviate}}
	allocator          *colmem.Allocator
	abbreviatedSortCol []uint64
	// {{end}}
	nulls         *coldata.Nulls
	cancelChecker colexecutils.CancelChecker
	sortCol       _GOTYPESLICE
	order         []int
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) init(
	ctx context.Context, allocator *colmem.Allocator, col *coldata.Vec, order []int,
) {
	s.sortCol = col.TemplateType()
	// {{if .CanAbbreviate}}
	s.allocator = allocator
	s.allocator.AdjustMemoryUsage(memsize.Uint64 * int64(s.sortCol.Len()))
	s.abbreviatedSortCol = s.sortCol.Abbreviated()
	// {{end}}
	s.nulls = col.Nulls()
	s.order = order
	s.cancelChecker.Init(ctx)
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) reset() {
	// {{if .CanAbbreviate}}
	s.allocator.AdjustMemoryUsage(0 - memsize.Uint64*int64(s.sortCol.Len()))
	s.allocator = nil
	s.abbreviatedSortCol = nil
	// {{end}}
	s.sortCol = nil
	s.nulls = nil
	s.order = nil
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) sort() {
	n := s.sortCol.Len()
	s.pdqsort(0, n, bits.Len(uint(n)))
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) sortPartitions(partitions []int) {
	order := s.order
	for i, partitionStart := range partitions {
		var partitionEnd int
		if i == len(partitions)-1 {
			partitionEnd = len(order)
		} else {
			partitionEnd = partitions[i+1]
		}
		s.order = order[partitionStart:partitionEnd]
		n := partitionEnd - partitionStart
		s.pdqsort(0, n, bits.Len(uint(n)))
	}
}

// {{/*
// TODO(yuzefovich): think through how we can inline more implementations of
// Less method - this has non-trivial performance improvements.
// */}}
// {{$isInt := or (or (eq .VecMethod "Int16") (eq .VecMethod "Int32")) (eq .VecMethod "Int64")}}
// {{if and ($isInt) (not $nulls)}}gcassert:inline{{end}}
func (s *sort_TYPE_DIR_HANDLES_NULLSOp) Less(i, j int) bool {
	// {{if $nulls}}
	n1 := s.nulls.MaybeHasNulls() && s.nulls.NullAt(s.order[i])
	n2 := s.nulls.MaybeHasNulls() && s.nulls.NullAt(s.order[j])
	// {{if eq $dir "Asc"}}
	// If ascending, nulls always sort first, so we encode that logic here.
	if n1 && n2 {
		return false
	} else if n1 {
		return true
	} else if n2 {
		return false
	}
	// {{else}}
	// If descending, nulls always sort last, so we encode that logic here.
	if n1 && n2 {
		return false
	} else if n1 {
		return false
	} else if n2 {
		return true
	}
	// {{end}}
	// {{end}}

	// {{if .CanAbbreviate}}
	// If the type can be abbreviated as a uint64, compare the abbreviated
	// values first. If they are not equal, we are done with the comparison. If
	// they are equal, we must fallback to a full comparison of the datums.
	abbr1 := s.abbreviatedSortCol[s.order[i]]
	abbr2 := s.abbreviatedSortCol[s.order[j]]
	if abbr1 != abbr2 {
		// {{if eq $dir "Asc"}}
		return abbr1 < abbr2
		// {{else}}
		return abbr1 > abbr2
		// {{end}}
	}
	// {{end}}

	var lt bool
	// We always indirect via the order vector.
	arg1 := s.sortCol.Get(s.order[i])
	arg2 := s.sortCol.Get(s.order[j])
	_ASSIGN_LT(lt, arg1, arg2, _, s.sortCol, s.sortCol)
	return lt
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) Swap(i, j int) {
	// We don't physically swap the column - we merely edit the order vector.
	s.order[i], s.order[j] = s.order[j], s.order[i]
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) Len() int {
	return len(s.order)
}

// {{end}}
// {{end}}
// {{end}}
// {{end}}
