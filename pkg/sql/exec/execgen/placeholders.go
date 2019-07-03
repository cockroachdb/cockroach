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

func GET(target, i interface{}) interface{} {
	panic(nonTemplatePanic)
}

func SET(target, i, new interface{}) {
	panic(nonTemplatePanic)
}

func SWAP(target, i, j interface{}) {
	panic(nonTemplatePanic)
}

func SLICE(target, start, end interface{}) interface{} {
	panic(nonTemplatePanic)
}

func COPYSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	panic(nonTemplatePanic)
}

func APPENDSLICE(target, src, destIdx, srcStartIdx, srcEndIdx interface{}) {
	panic(nonTemplatePanic)
}

func APPENDVAL(target, v interface{}) {
	panic(nonTemplatePanic)
}

func LEN(target interface{}) interface{} {
	panic(nonTemplatePanic)
}

func LOOP(target interface{}) bool {
	panic(nonTemplatePanic)
}
