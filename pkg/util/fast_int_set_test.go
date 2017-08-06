// Copyright 2017 The Cockroach Authors.
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

package util

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestFastIntSet(t *testing.T) {
	for _, m := range []uint32{1, 8, 30, smallValCutoff, 2 * smallValCutoff, 4 * smallValCutoff} {
		t.Run(fmt.Sprintf("%d", m), func(t *testing.T) {
			in := make([]bool, m)
			forEachRes := make([]bool, m)

			var s FastIntSet
			for i := 0; i < 1000; i++ {
				v := uint32(rand.Intn(int(m)))
				if rand.Intn(2) == 0 {
					in[v] = true
					s.Add(v)
				} else {
					in[v] = false
					s.Remove(v)
				}
				empty := true
				for j := uint32(0); j < m; j++ {
					empty = empty && !in[j]
					if in[j] != s.Contains(j) {
						t.Fatalf("incorrect result for Contains(%d), expected %t", j, in[j])
					}
				}
				if empty != s.Empty() {
					t.Fatalf("incorrect result for Empty(), expected %t", empty)
				}
				// Test ForEach
				for j := range forEachRes {
					forEachRes[j] = false
				}
				s.ForEach(func(j uint32) {
					forEachRes[j] = true
				})
				for j := uint32(0); j < m; j++ {
					if in[j] != forEachRes[j] {
						t.Fatalf("incorrect ForEachResult for %d (%t, expected %t)", j, forEachRes[j], in[j])
					}
				}
				// Cross-check Ordered and Next().
				vals := make([]int, 0)
				for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
					vals = append(vals, int(i))
				}
				if o := s.Ordered(); !reflect.DeepEqual(vals, o) {
					t.Fatalf("set build with Next doesn't match Ordered: %v vs %v", vals, o)
				}
				s2 := s.Copy()
				if !s.Equals(s2) || !s2.Equals(s) {
					t.Fatalf("expected equality: %v, %v", s, s2)
				}
				if col, ok := s2.Next(0); ok {
					s2.Remove(col)
					if s.Equals(s2) || s2.Equals(s) {
						t.Fatalf("unexpected equality: %v, %v", s, s2)
					}
					s2.Add(col)
					if !s.Equals(s2) || !s2.Equals(s) {
						t.Fatalf("expected equality: %v, %v", s, s2)
					}
				}
			}
		})
	}
}
