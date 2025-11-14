// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaload

type SignedLoadValue struct {
	n int64
}

type SignedLoadVector [NumLoadDimensions]SignedLoadValue

func (v *SignedLoadVector) Add(o SignedLoadVector) {
	for i := 0; i < int(NumLoadDimensions); i++ {
		(*v)[i].n += o[i].n
	}
}

func (v *SignedLoadVector) Subtract(o SignedLoadVector) {
	for i := 0; i < int(NumLoadDimensions); i++ {
		(*v)[i].n -= o[i].n
	}
}
