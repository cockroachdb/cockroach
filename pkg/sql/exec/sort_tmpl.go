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

package exec

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// {{/*

// Declarations to make the template compile properly.

// Dummy import to pull in "bytes" package.
var _ bytes.Buffer

// Dummy import to pull in "apd" package.
var _ apd.Decimal

// Dummy import to pull in "tree" package.
var _ tree.Datum

// _GOTYPE is the template Go type variable for this operator. It will be
// replaced by the Go type equivalent for each type in types.T, for example
// int64 for types.Int64.
type _GOTYPE interface{}

// _TYPES_T is the template type variable for types.T. It will be replaced by
// types.Foo for each type Foo in the types.T type.
const _TYPES_T = types.Unhandled

// _ASSIGN_LT is the template equality function for assigning the first input
// to the result of the second input < the third input.
func _ASSIGN_LT(_, _, _ string) bool {
	panic("")
}

// */}}

func newSingleSorter(t types.T, dir distsqlpb.Ordering_Column_Direction) (colSorter, error) {
	switch t {
	// {{range .}} {{/* for each type */}}
	case _TYPES_T:
		switch dir {
		// {{range .Overloads}} {{/* for each direction */}}
		case _DIR_ENUM:
			return &sort_TYPE_DIROp{}, nil
		// {{end}}
		default:
			return nil, errors.Errorf("unsupported sort dir %s", dir)
		}
	// {{end}}
	default:
		return nil, errors.Errorf("unsupported sort type %s", t)
	}
}

// {{range .}} {{/* for each type */}}
// {{range .Overloads}} {{/* for each direction */}}

type sort_TYPE_DIROp struct {
	sortCol       []_GOTYPE
	order         []uint64
	workingSpace  []uint64
	cancelChecker CancelChecker
}

func (s *sort_TYPE_DIROp) init(col coldata.Vec, order []uint64, workingSpace []uint64) {
	s.sortCol = col._TemplateType()
	s.order = order
	s.workingSpace = workingSpace
}

func (s *sort_TYPE_DIROp) sort(ctx context.Context) {
	n := len(s.sortCol)
	s.quickSort(ctx, 0, n, maxDepth(n))
}

func (s *sort_TYPE_DIROp) reorder() {
	// Initialize our index vector to the inverse of the order vector. This
	// creates what is known as a permutation. Position i in the permutation has
	// the output index for the value at position i in the original ordering of
	// the data we sorted. For example, if we were sorting the column [d,c,a,b],
	// the order vector would be [2,3,1,0], and the permutation would be
	// [3,2,0,1].
	index := s.workingSpace
	for idx, ord := range s.order {
		index[int(ord)] = uint64(idx)
	}
	// Once we have our permutation, we apply it to our value column by following
	// each cycle within the permutation until we reach the identity. This
	// algorithm takes just O(n) swaps to reorder the sortCol. It also returns
	// the index array to an ordinal list in the process.
	for i := range index {
		for index[i] != uint64(i) {
			s.sortCol[index[i]], s.sortCol[i] = s.sortCol[i], s.sortCol[index[i]]
			index[i], index[index[i]] = index[index[i]], index[i]
		}
	}
}

func (s *sort_TYPE_DIROp) sortPartitions(ctx context.Context, partitions []uint64) {
	if len(partitions) < 1 {
		panic(fmt.Sprintf("invalid partitions list %v", partitions))
	}
	order := s.order
	sortCol := s.sortCol
	for i, partitionStart := range partitions {
		var partitionEnd uint64
		if i == len(partitions)-1 {
			partitionEnd = uint64(len(order))
		} else {
			partitionEnd = partitions[i+1]
		}
		s.order = order[partitionStart:partitionEnd]
		s.sortCol = sortCol[partitionStart:partitionEnd]
		n := int(partitionEnd - partitionStart)
		s.quickSort(ctx, 0, n, maxDepth(n))
	}
}

func (s *sort_TYPE_DIROp) Less(i, j int) bool {
	var lt bool
	_ASSIGN_LT("lt", "s.sortCol[i]", "s.sortCol[j]")
	return lt
}

func (s *sort_TYPE_DIROp) Swap(i, j int) {
	// Swap needs to swap the values in the column being sorted, as otherwise
	// subsequent calls to Less would be incorrect.
	// We also store the swap order in s.order to swap all the other columns.
	s.sortCol[i], s.sortCol[j] = s.sortCol[j], s.sortCol[i]
	s.order[i], s.order[j] = s.order[j], s.order[i]
}

func (s *sort_TYPE_DIROp) Len() int {
	return len(s.order)
}

// {{end}}
// {{end}}
