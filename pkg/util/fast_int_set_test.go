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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestFastIntSet(t *testing.T) {
	for _, mVal := range []int{1, 8, 30, smallCutoff, 2 * smallCutoff, 4 * smallCutoff} {
		m := mVal
		t.Run(fmt.Sprintf("%d", m), func(t *testing.T) {
			t.Parallel()
			rng, _ := randutil.NewPseudoRand()
			in := make([]bool, m)
			forEachRes := make([]bool, m)

			var s FastIntSet
			for i := 0; i < 1000; i++ {
				v := rng.Intn(m)
				if rng.Intn(2) == 0 {
					in[v] = true
					s.Add(v)
				} else {
					in[v] = false
					s.Remove(v)
				}
				empty := true
				for j := 0; j < m; j++ {
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
				s.ForEach(func(j int) {
					forEachRes[j] = true
				})
				for j := 0; j < m; j++ {
					if in[j] != forEachRes[j] {
						t.Fatalf("incorrect ForEachResult for %d (%t, expected %t)", j, forEachRes[j], in[j])
					}
				}
				// Cross-check Ordered and Next().
				var vals []int
				for i, ok := s.Next(0); ok; i, ok = s.Next(i + 1) {
					vals = append(vals, i)
				}
				if o := s.Ordered(); !reflect.DeepEqual(vals, o) {
					t.Fatalf("set built with Next doesn't match Ordered: %v vs %v", vals, o)
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

func TestFastIntSetTwoSetOps(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	// genSet creates a set of numElem values in [minVal, minVal + valRange)
	// It also adds and then removes numRemoved elements.
	genSet := func(numElem, numRemoved, minVal, valRange int) (FastIntSet, map[int]bool) {
		var s FastIntSet
		vals := rng.Perm(valRange)[:numElem+numRemoved]
		used := make(map[int]bool, len(vals))
		for _, i := range vals {
			used[i] = true
		}
		for k := range used {
			s.Add(k)
		}
		p := rng.Perm(len(vals))
		for i := 0; i < numRemoved; i++ {
			k := vals[p[i]]
			s.Remove(k)
			delete(used, k)
		}
		return s, used
	}

	// returns true if a is a subset of b
	subset := func(a, b map[int]bool) bool {
		for k := range a {
			if !b[k] {
				return false
			}
		}
		return true
	}

	for _, minVal := range []int{-10, -1, 0, smallCutoff, 2 * smallCutoff} {
		for _, valRange := range []int{0, 20, 200} {
			for _, num1 := range []int{0, 1, 5, 10, 20} {
				for _, removed1 := range []int{0, 1, 3, 8} {
					s1, m1 := genSet(num1, removed1, minVal, num1+removed1+valRange)
					for _, num2 := range []int{0, 1, 5, 10, 20} {
						for _, removed2 := range []int{0, 1, 4, 10} {
							s2, m2 := genSet(num2, removed2, minVal, num2+removed2+valRange)

							subset1 := subset(m1, m2)
							if subset1 != s1.SubsetOf(s2) {
								t.Errorf("SubsetOf result incorrect: %s, %s", &s1, &s2)
							}
							subset2 := subset(m2, m1)
							if subset2 != s2.SubsetOf(s1) {
								t.Errorf("SubsetOf result incorrect: %s, %s", &s2, &s1)
							}
							eq := subset1 && subset2
							if eq != s1.Equals(s2) || eq != s2.Equals(s1) {
								t.Errorf("Equals result incorrect: %s, %s", &s1, &s2)
							}

							// Test union.

							u := s1.Copy()
							u.UnionWith(s2)
							// Verify all elements from m1 and m2 are in u.
							for _, m := range []map[int]bool{m1, m2} {
								for x := range m {
									if !u.Contains(x) {
										t.Errorf("incorrect union result %s union %s = %s", &s1, &s2, &u)
										break
									}
								}
							}
							// Verify all elements from u are in m2 or m1.
							for x, ok := u.Next(minVal); ok; x, ok = u.Next(x + 1) {
								if !(m1[x] || m2[x]) {
									t.Errorf("incorrect union result %s union %s = %s", &s1, &s2, &u)
									break
								}
							}

							// Test intersection.
							u = s1.Copy()
							u.IntersectionWith(s2)
							// Verify all elements from m1 and m2 are in u.
							for x := range m1 {
								if m2[x] && !u.Contains(x) {
									t.Errorf("incorrect intersection result %s union %s = %s  x=%d", &s1, &s2, &u, x)
									break
								}
							}
							// Verify all elements from u are in m2 and m1.
							for x, ok := u.Next(minVal); ok; x, ok = u.Next(x + 1) {
								if !(m1[x] && m2[x]) {
									t.Errorf("incorrect intersection result %s intersect %s = %s", &s1, &s2, &u)
									break
								}
							}
						}
					}
				}
			}
		}
	}
}
