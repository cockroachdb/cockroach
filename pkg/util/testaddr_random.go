// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build linux

package util

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func init() {
	r, _ := randutil.NewTestRand()
	// 127.255.255.255 is special (broadcast), so choose values less
	// than 255.
	a := r.Intn(255)
	b := r.Intn(255)
	c := r.Intn(255)
	IsolatedTestAddr = NewUnresolvedAddr("tcp", fmt.Sprintf("127.%d.%d.%d:0", a, b, c))
}
