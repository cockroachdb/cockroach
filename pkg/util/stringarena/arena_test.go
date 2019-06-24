// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stringarena

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkStringArena(b *testing.B) {
	const count = 1024
	vals := make([][]byte, count)
	for i := range vals {
		vals[i] = []byte(fmt.Sprint(i))
	}

	b.Run("arena", func(b *testing.B) {
		a := Make(nil /* acc */)
		m := make([]string, count)

		for i := 0; i < b.N; i++ {
			j := i % count
			s, err := a.AllocBytes(context.Background(), vals[j])
			if err != nil {
				b.Fatal(err)
			}
			m[j] = s
		}
	})

	b.Run("noarena", func(b *testing.B) {
		m := make([]string, count)

		for i := 0; i < b.N; i++ {
			j := i % count
			m[j] = string(vals[j])
		}
	})
}
