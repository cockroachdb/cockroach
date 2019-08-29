// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execgen

import "github.com/cockroachdb/cockroach/pkg/sql/exec/execerror"

const nonTemplatePanic = "do not call from non-template code"

// Remove unused warnings.
var (
	_ = UNSAFEGET
	_ = GET
	_ = SET
	_ = SWAP
	_ = SLICE
	_ = COPYSLICE
	_ = APPENDSLICE
	_ = APPENDVAL
	_ = LEN
	_ = ZERO
	_ = RANGE
)

// UNSAFEGET is a template function. Use this if you are not keeping data around
// (including passing it to SET).
func UNSAFEGET(target, i interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// GET is a template function. The Bytes implementation of this function
// performs a copy. Use this if you need to keep values around past the
// lifecycle of a Batch.
func GET(target, i interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// SET is a template function.
func SET(target, i, new interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// SWAP is a template function.
func SWAP(target, i, j interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// SLICE is a template function.
func SLICE(target, start, end interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// COPYSLICE is a template function.
func COPYSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// APPENDSLICE is a template function.
func APPENDSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// APPENDVAL is a template function.
func APPENDVAL(target, v interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// LEN is a template function.
func LEN(target interface{}) interface{} {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return nil
}

// ZERO is a template function.
func ZERO(target interface{}) {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
}

// RANGE is a template function.
func RANGE(loopVariableIdent interface{}, target interface{}) bool {
	execerror.VectorizedInternalPanic(nonTemplatePanic)
	return false
}
