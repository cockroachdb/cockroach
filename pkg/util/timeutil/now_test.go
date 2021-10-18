// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timeutil

import (
	"testing"
	"time"
)

func BenchmarkNow(b *testing.B) {
	type test struct {
		name    string
		nowFunc func() time.Time
	}
	for _, tc := range []test{
		{name: "stdlib-now",
			nowFunc: time.Now,
		},
		{
			name:    "timeutil-now",
			nowFunc: Now,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Now()
			}
		})
	}
}

func BenchmarkSince(b *testing.B) {
	type test struct {
		name    string
		nowFunc func() time.Time
	}
	for _, tc := range []test{
		{name: "stdlib-now",
			nowFunc: time.Now,
		},
		{
			name:    "timeutil-now",
			nowFunc: Now,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			start := tc.nowFunc()
			for i := 0; i < b.N; i++ {
				Since(start)
			}
		})
	}
}
