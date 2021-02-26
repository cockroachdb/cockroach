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
// This file is the execgen template for sort.eg.go. It's formatted in a
// special way, so it's both valid Go and a valid text/template input. This
// permits editing this file with editor support.
//
// */}}

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/colexecutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// Workaround for bazel auto-generated code. goimports does not automatically
// pick up the right packages when run within the bazel sandbox.
var (
	_ coldataext.Datum
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

// _ISNULL is the template type variable for whether the sorter handles nulls
// or not. It will be replaced by the appropriate boolean.
const _ISNULL = false

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the second input < the third input.
func _ASSIGN_LT(_, _, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

func isSorterSupported(t *types.T, dir execinfrapb.Ordering_Column_Direction) bool {
	// {{range .}}
	// {{if .Nulls}}
	switch dir {
	// {{range .DirOverloads}}
	case _DIR_ENUM:
		switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
		// {{range .FamilyOverloads}}
		case _CANONICAL_TYPE_FAMILY:
			switch t.Width() {
			// {{range .WidthOverloads}}
			case _TYPE_WIDTH:
				return true
				// {{end}}
			}
			// {{end}}
		}
		// {{end}}
	}
	// {{end}}
	// {{end}}
	return false
}

func newSingleSorter(
	t *types.T, dir execinfrapb.Ordering_Column_Direction, hasNulls bool,
) colSorter {
	switch hasNulls {
	// {{range .}}
	// {{$nulls := .Nulls}}
	case _ISNULL:
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
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("isSorterSupported should have caught this"))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}

// {{range .}}
// {{$nulls := .Nulls}}
// {{range .DirOverloads}}
// {{$dir := .DirString}}
// {{range .FamilyOverloads}}
// {{range .WidthOverloads}}

type sort_TYPE_DIR_HANDLES_NULLSOp struct {
	sortCol       _GOTYPESLICE
	nulls         *coldata.Nulls
	order         []int
	cancelChecker colexecutils.CancelChecker
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) init(ctx context.Context, col coldata.Vec, order []int) {
	s.sortCol = col.TemplateType()
	s.nulls = col.Nulls()
	s.order = order
	s.cancelChecker.Init(ctx)
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) sort() {
	n := s.sortCol.Len()
	s.quickSort(0, n, maxDepth(n))
}

func (s *sort_TYPE_DIR_HANDLES_NULLSOp) sortPartitions(partitions []int) {
	if len(partitions) < 1 {
		colexecerror.InternalError(errors.AssertionFailedf("invalid partitions list %v", partitions))
	}
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
		s.quickSort(0, n, maxDepth(n))
	}
}

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
	// {{else if eq $dir "Desc"}}
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
