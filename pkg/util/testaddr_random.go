// Copyright 2016 The Cockroach Authors.
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

// +build linux

package util

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func init() {
	r, _ := randutil.NewPseudoRand()
	// 127.255.255.255 is special (broadcast), so choose values less
	// than 255.
	a := r.Intn(255)
	b := r.Intn(255)
	c := r.Intn(255)
	IsolatedTestAddr = NewUnresolvedAddr("tcp", fmt.Sprintf("127.%d.%d.%d:0", a, b, c))
}
