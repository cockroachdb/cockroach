// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package stringarena

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkStringArena(b *testing.B) {
	vals := make([][]byte, 1000)
	for i := range vals {
		vals[i] = []byte(fmt.Sprint(i))
	}

	b.Run("arena", func(b *testing.B) {
		a := Make(nil /* acc */)
		m := make([]string, len(vals))

		for i, j := 0, 0; i < b.N; i++ {
			s, err := a.AllocBytes(context.Background(), vals[j])
			if err != nil {
				b.Fatal(err)
			}

			m[j] = s
			j++
			if j >= len(vals) {
				j = 0
			}

		}
	})

	b.Run("noarena", func(b *testing.B) {
		m := make([]string, len(vals))

		for i, j := 0, 0; i < b.N; i++ {
			m[j] = string(vals[j])
			j++
			if j >= len(vals) {
				j = 0
			}
		}
	})
}
