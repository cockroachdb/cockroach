// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

func nonZeroingMakeByteSlice(len int) []byte {
	ptr := mallocgc(uintptr(len), nil, false)
	return (*[MaxArrayLen]byte)(ptr)[:len:len]
}
