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
// This file is the execgen template for vec_comparators.eg.go. It's formatted
// in a special way, so it's both valid Go and a valid text/template input.
// This permits editing this file with editor support.
//
// */}}

package colexec

import (
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/col/typeconv"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
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

// _COMPARE is the template equality function for assigning the first input
// to the result of comparing second and third inputs.
func _COMPARE(_, _, _, _, _ string) bool {
	colexecerror.InternalError(errors.AssertionFailedf(""))
}

// */}}

// vecComparator is a helper for the ordered synchronizer. It stores multiple
// column vectors of a single type and facilitates comparing values between
// them.
type vecComparator interface {
	// compare compares values from two vectors. vecIdx is the index of the vector
	// and valIdx is the index of the value in that vector to compare. Returns -1,
	// 0, or 1.
	compare(vecIdx1, vecIdx2 int, valIdx1, valIdx2 int) int

	// set sets the value of the vector at dstVecIdx at index dstValIdx to the value
	// at the vector at srcVecIdx at index srcValIdx.
	// NOTE: whenever set is used, the caller is responsible for updating the
	// memory accounts.
	set(srcVecIdx, dstVecIdx int, srcValIdx, dstValIdx int)

	// setVec updates the vector at idx.
	setVec(idx int, vec coldata.Vec)
}

// {{range .}}
// {{range .WidthOverloads}}
type _TYPEVecComparator struct {
	vecs  []_GOTYPESLICE
	nulls []*coldata.Nulls
}

func (c *_TYPEVecComparator) compare(vecIdx1, vecIdx2 int, valIdx1, valIdx2 int) int {
	n1 := c.nulls[vecIdx1].MaybeHasNulls() && c.nulls[vecIdx1].NullAt(valIdx1)
	n2 := c.nulls[vecIdx2].MaybeHasNulls() && c.nulls[vecIdx2].NullAt(valIdx2)
	if n1 && n2 {
		return 0
	} else if n1 {
		return -1
	} else if n2 {
		return 1
	}
	left := c.vecs[vecIdx1].Get(valIdx1)
	right := c.vecs[vecIdx2].Get(valIdx2)
	var cmp int
	_COMPARE(cmp, left, right, c.vecs[vecIdx1], c.vecs[vecIdx2])
	return cmp
}

func (c *_TYPEVecComparator) setVec(idx int, vec coldata.Vec) {
	c.vecs[idx] = vec._TYPE()
	c.nulls[idx] = vec.Nulls()
}

func (c *_TYPEVecComparator) set(srcVecIdx, dstVecIdx int, srcIdx, dstIdx int) {
	if c.nulls[srcVecIdx].MaybeHasNulls() && c.nulls[srcVecIdx].NullAt(srcIdx) {
		c.nulls[dstVecIdx].SetNull(dstIdx)
	} else {
		c.nulls[dstVecIdx].UnsetNull(dstIdx)
		// {{if .IsBytesLike}}
		// Since flat Bytes cannot be set at arbitrary indices (data needs to be
		// moved around), we use CopySlice to accept the performance hit.
		// Specifically, this is a performance hit because we are overwriting the
		// variable number of bytes in `dstVecIdx`, so we will have to either shift
		// the bytes after that element left or right, depending on how long the
		// source bytes slice is. Refer to the CopySlice comment for an example.
		c.vecs[dstVecIdx].CopySlice(c.vecs[srcVecIdx], dstIdx, srcIdx, srcIdx+1)
		// {{else}}
		v := c.vecs[srcVecIdx].Get(srcIdx)
		c.vecs[dstVecIdx].Set(dstIdx, v)
		// {{end}}
	}
}

// {{end}}
// {{end}}

func GetVecComparator(t *types.T, numVecs int) vecComparator {
	switch typeconv.TypeFamilyToCanonicalTypeFamily(t.Family()) {
	// {{range .}}
	case _CANONICAL_TYPE_FAMILY:
		switch t.Width() {
		// {{range .WidthOverloads}}
		case _TYPE_WIDTH:
			return &_TYPEVecComparator{
				vecs:  make([]_GOTYPESLICE, numVecs),
				nulls: make([]*coldata.Nulls, numVecs),
			}
			// {{end}}
		}
		// {{end}}
	}
	colexecerror.InternalError(errors.AssertionFailedf("unhandled type %s", t))
	// This code is unreachable, but the compiler cannot infer that.
	return nil
}
