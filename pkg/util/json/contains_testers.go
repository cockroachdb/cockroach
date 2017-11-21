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

// This file contains (ha!) helpers to test @>.

package json

import (
	"math/rand"
)

type containsTester interface {
	// slowContains is a slower but simpler implementation of contains to check
	// against the substantially more complex actual implementation.
	slowContains(other JSON) bool

	// subdocument returns a JSON document which is contained by this one.
	subdocument(isRoot bool, rng *rand.Rand) JSON
}

func (j jsonNull) slowContains(other JSON) bool   { return j.Compare(other) == 0 }
func (j jsonTrue) slowContains(other JSON) bool   { return j.Compare(other) == 0 }
func (j jsonFalse) slowContains(other JSON) bool  { return j.Compare(other) == 0 }
func (j jsonNumber) slowContains(other JSON) bool { return j.Compare(other) == 0 }
func (j jsonString) slowContains(other JSON) bool { return j.Compare(other) == 0 }

func (j jsonArray) slowContains(other JSON) bool {
	if other.isScalar() {
		for i := 0; i < len(j); i++ {
			if j[i].Compare(other) == 0 {
				return true
			}
		}
	}

	if ary, ok := other.(jsonArray); ok {
		for i := 0; i < len(ary); i++ {
			found := false
			for k := 0; k < len(j); k++ {
				if j[k].jsonType() == ary[i].jsonType() && j[k].(containsTester).slowContains(ary[i]) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	}
	return false
}

func (j jsonObject) slowContains(other JSON) bool {
	if obj, ok := other.(jsonObject); ok {
		for i := 0; i < len(obj); i++ {
			leftVal := j.FetchValKey(string(obj[i].k))
			if leftVal == nil || !leftVal.(containsTester).slowContains(obj[i].v) {
				return false
			}
		}
		return true
	}
	return false
}

func (j jsonNull) subdocument(_ bool, _ *rand.Rand) JSON   { return j }
func (j jsonTrue) subdocument(_ bool, _ *rand.Rand) JSON   { return j }
func (j jsonFalse) subdocument(_ bool, _ *rand.Rand) JSON  { return j }
func (j jsonNumber) subdocument(_ bool, _ *rand.Rand) JSON { return j }
func (j jsonString) subdocument(_ bool, _ *rand.Rand) JSON { return j }

func (j jsonArray) subdocument(isRoot bool, rng *rand.Rand) JSON {
	// Root arrays contain their scalar elements.
	if isRoot && rng.Intn(5) == 0 {
		idx := rng.Intn(len(j))
		if j[idx].isScalar() {
			return j[idx]
		}
	}
	result := make(jsonArray, 0)
	i := 0
	for i < len(j) {
		if rng.Intn(2) == 0 {
			result = append(result, j[i].(containsTester).subdocument(false /* isRoot */, rng))
		}
		if rng.Intn(2) == 0 {
			i++
		}
	}
	// Shuffle the slice.
	for i := range result {
		j := rng.Intn(i + 1)
		result[i], result[j] = result[j], result[i]
	}

	return result
}

func (j jsonObject) subdocument(_ bool, rng *rand.Rand) JSON {
	result := make(jsonObject, 0)
	for _, e := range j {
		if rng.Intn(2) == 0 {
			result = append(result, jsonKeyValuePair{
				k: e.k,
				v: e.v.(containsTester).subdocument(false /* isRoot */, rng),
			})
		}
	}
	return result
}
