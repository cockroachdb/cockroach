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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package util

import (
	"math/rand"
	"testing"
)

func TestFastIntSet(t *testing.T) {
	const m = 2 * smallValCutoff
	in := make([]bool, m)

	var s FastIntSet
	for i := 0; i < 1000; i++ {
		v := uint32(rand.Intn(m))
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
	}
}
