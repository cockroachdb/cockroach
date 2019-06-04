// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build 386 amd64

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
