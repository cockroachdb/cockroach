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

const nonTemplatePanic = "do not call from non-template code"

// Remove unused warnings.
var (
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

// GET is a template function.
func GET(target, i interface{}) interface{} {
	panic(nonTemplatePanic)
}

// SET is a template function.
func SET(target, i, new interface{}) {
	panic(nonTemplatePanic)
}

// SWAP is a template function.
func SWAP(target, i, j interface{}) {
	panic(nonTemplatePanic)
}

// SLICE is a template function.
func SLICE(target, start, end interface{}) interface{} {
	panic(nonTemplatePanic)
}

// COPYSLICE is a template function.
func COPYSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	panic(nonTemplatePanic)
}

// APPENDSLICE is a template function.
func APPENDSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	panic(nonTemplatePanic)
}

// APPENDVAL is a template function.
func APPENDVAL(target, v interface{}) {
	panic(nonTemplatePanic)
}

// LEN is a template function.
func LEN(target interface{}) interface{} {
	panic(nonTemplatePanic)
}

// ZERO is a template function.
func ZERO(target interface{}) {
	panic(nonTemplatePanic)
}

// RANGE is a template function.
func RANGE(loopVariableIdent interface{}, target interface{}) bool {
	panic(nonTemplatePanic)
}
