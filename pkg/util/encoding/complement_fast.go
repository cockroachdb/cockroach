// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build 386 || amd64

package encoding

import "unsafe"

// The idea for the fast ones complement is borrowed from fastXORBytes
// in the crypto standard library.
const wordSize = int(unsafe.Sizeof(uintptr(0)))

func onesComplement(b []byte) {
	n := len(b)
	w := n / wordSize
	if w > 0 {
		bw := *(*[]uintptr)(unsafe.Pointer(&b))
		for i := 0; i < w; i++ {
			bw[i] = ^bw[i]
		}
	}

	for i := w * wordSize; i < n; i++ {
		b[i] = ^b[i]
	}
}
