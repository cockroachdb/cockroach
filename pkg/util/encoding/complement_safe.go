// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !386 && !amd64

package encoding

func onesComplement(b []byte) {
	for i := range b {
		b[i] = ^b[i]
	}
}
