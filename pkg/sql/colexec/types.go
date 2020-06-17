// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

type TypeInfo interface {
	UnsafeGet(target interface{}, i int) interface{}

	Slice(col interface{}, start int, end int) interface{}
}

type int16TypeInfo struct{}

func (t int16TypeInfo) UnsafeGet(col []int16, idx int) int16 {
	return col[idx]
}

func (t int16TypeInfo) CopyVal(v int16) int16 {
	return v
}

func (t int16TypeInfo) Ne(x int16, y int16) bool {
	return x != y
}

func (t int16TypeInfo) Eq(x int16, y int16) bool {
	return x == y
}

func (t int16TypeInfo) Lt(x int16, y int16) bool {
	return x < y
}

func (t int16TypeInfo) Le(x int16, y int16) bool {
	return x <= y
}

func (t int16TypeInfo) Gt(x int16, y int16) bool {
	return x < y
}

func (t int16TypeInfo) Ge(x int16, y int16) bool {
	return x >= y
}

func (t int16TypeInfo) Slice(col []int16, start int, end int) []int16 {
	return col[start:end]
}
