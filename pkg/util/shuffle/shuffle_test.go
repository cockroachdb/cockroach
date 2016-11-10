// Copyright 2016 The Cockroach Authors.
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

package shuffle

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type testSlice []int

// testSlice implements shuffle.Interface.
func (ts testSlice) Len() int      { return len(ts) }
func (ts testSlice) Swap(i, j int) { ts[i], ts[j] = ts[j], ts[i] }

func TestShuffle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rand.Seed(0)

	verify := func(original, expected testSlice) {
		Shuffle(original)
		if !reflect.DeepEqual(original, expected) {
			t.Errorf("expected %v, got %v", expected, original)
		}
	}

	ts := testSlice{}
	verify(ts, testSlice{})
	verify(ts, testSlice{})

	ts = testSlice{1}
	verify(ts, testSlice{1})
	verify(ts, testSlice{1})

	ts = testSlice{1, 2}
	verify(ts, testSlice{2, 1})
	verify(ts, testSlice{1, 2})

	ts = testSlice{1, 2, 3}
	verify(ts, testSlice{1, 3, 2})
	verify(ts, testSlice{1, 2, 3})
	verify(ts, testSlice{1, 2, 3})
	verify(ts, testSlice{3, 1, 2})

	ts = testSlice{1, 2, 3, 4, 5}
	verify(ts, testSlice{2, 1, 3, 5, 4})
	verify(ts, testSlice{4, 2, 1, 5, 3})
	verify(ts, testSlice{1, 4, 2, 3, 5})
	verify(ts, testSlice{2, 5, 4, 1, 3})
	verify(ts, testSlice{4, 2, 3, 1, 5})
}

func TestReplicaSetRandPerm(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rand.Seed(0)

	verify := func(startIndex int, endIndex int, original, expected testSlice) {
		Between(original, startIndex, endIndex)
		if !reflect.DeepEqual(original, expected) {
			t.Errorf("expected order %v, got %v", expected, original)
		}
	}

	verify(2, 2, testSlice{1, 2, 3, 4, 5}, testSlice{1, 2, 3, 4, 5})
	verify(0, 0, testSlice{1, 2, 3, 4, 5}, testSlice{1, 2, 3, 4, 5})
	verify(5, 5, testSlice{1, 2, 3, 4, 5}, testSlice{1, 2, 3, 4, 5})
	verify(3, 4, testSlice{1, 2, 3, 4, 5}, testSlice{1, 2, 3, 5, 4})
	verify(0, 2, testSlice{1, 2, 3, 4, 5}, testSlice{2, 3, 1, 4, 5})
	verify(1, 3, testSlice{1, 2, 3, 4, 5}, testSlice{1, 3, 2, 4, 5})
	verify(0, 4, testSlice{1, 2, 3, 4, 5}, testSlice{2, 4, 1, 5, 3})

	verify(0, 0, testSlice{1}, testSlice{1})
}
