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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestUnionFind(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	for _, n := range []int{2, 3, 4, 10, 50, 100, 1000} {
		group := make([]int, n)
		for i := range group {
			group[i] = i
		}
		uf := UnionFind{}
		for i := 0; i < 100*n; i++ {
			x := rng.Intn(n)
			y := rng.Intn(n)
			// 99% of the time we check if x and y are in the same group; 1% of
			// the time we union them.
			if rng.Intn(100) > 0 {
				gx := uf.Find(x)
				gy := uf.Find(y)
				if (gx == gy) != (group[x] == group[y]) {
					t.Fatalf(
						"Find(%d)=%d, Find(%d)=%d, groups %d, %d",
						x, gx, y, gy, group[x], group[y],
					)
				}
			} else {
				uf.Union(x, y)
				gx := group[x]
				gy := group[y]
				for i := range group {
					if group[i] == gx {
						group[i] = gy
					}
				}
			}
		}
	}
}

func TestUnionFindCopyEquals(t *testing.T) {
	// Verify equality of two set groups that are equivalent but were generated
	// with different operations.
	var x UnionFind
	x.Union(1, 2)
	x.Union(2, 3)
	x.Union(3, 4)
	x.Union(6, 7)

	var y UnionFind
	y.Union(3, 1)
	y.Union(2, 4)
	y.Union(1, 2)
	y.Union(6, 7)
	if !x.Equals(y) || !y.Equals(x) {
		t.Errorf("identical sets not equal")
	}

	y = x.Copy()
	if !x.Equals(y) || !y.Equals(x) {
		t.Errorf("identical sets not equal")
	}
	y.Union(1, 6)
	if x.Equals(y) || y.Equals(x) {
		t.Errorf("different sets equal")
	}
	x.Union(1, 6)
	if !x.Equals(y) || !y.Equals(x) {
		t.Errorf("identical sets not equal")
	}
}
