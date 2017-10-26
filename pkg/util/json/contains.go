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

package json

import (
	"sort"
)

// Contains returns true if a contains b. This implements the @>, <@ operators.
// See the Postgres docs for the expected semantics of Contains.
// https://www.postgresql.org/docs/10/static/datatype-json.html#JSON-CONTAINMENT
// The naive approach to doing array containment would be to do an O(n^2)
// nested loop through the arrays to check if one is contained in the
// other.  We're out of luck when the arrays contain other arrays or
// objects (there might actually be something fancy we can do, but there's nothing
// obvious).
// When the arrays contain scalars however, we can optimize this by
// pre-sorting both arrays and iterating through them in lockstep.
// To this end, we preprocess the JSON document to sort all of its arrays so
// that when we perform contains we can extract the scalars sorted, and then
// also the arrays and objects in separate arrays, so that we can do the fast
// thing for the subset of the arrays which are scalars.
func Contains(a, b JSON) bool {
	a = a.preprocessForContains()
	b = b.preprocessForContains()
	return a.contains(b)
}

func (j jsonNull) preprocessForContains() JSON   { return j }
func (j jsonFalse) preprocessForContains() JSON  { return j }
func (j jsonTrue) preprocessForContains() JSON   { return j }
func (j jsonNumber) preprocessForContains() JSON { return j }
func (j jsonString) preprocessForContains() JSON { return j }

// shallowLess is a less than implementation for JSON values which considers
// all objects equal, and all arrays equal (all arrays are still less than all
// objects, and both are greater than all scalars).
func shallowLess(a, b JSON) bool {
	switch a.(type) {
	case jsonArray:
		_, ok := b.(jsonObject)
		return ok
	case jsonObject:
		return false
	}
	switch b.(type) {
	case jsonArray, jsonObject:
		return true
	}
	return a.Compare(b) == -1
}

// preprocessJSONArrayForContains returns a new JSON document with all arrays
// having the following properties:
// * All scalars appear at the beginning of any arrays, and are sorted.
// * All the arrays come after all the objects.
// * All the objects come after all the arrays.
// The relative ordering of arrays with arrays and objects with objects is not guaranteed.
func (j jsonArray) preprocessForContains() JSON {
	sorted := make(jsonArray, len(j))
	copy(sorted, j)

	for i := range sorted {
		sorted[i] = sorted[i].preprocessForContains()
	}

	// Note: this does not make any guarantees about the ordering of objects with
	// objects or arrays with arrays.
	sort.Slice(sorted, func(i, j int) bool {
		return shallowLess(sorted[i], sorted[j])
	})

	return sorted
}

func (j jsonObject) preprocessForContains() JSON {
	preprocessed := make(jsonObject, len(j))
	copy(preprocessed, j)

	for i := range preprocessed {
		preprocessed[i].v = preprocessed[i].v.preprocessForContains()
	}

	return preprocessed
}

func (jsonNull) contains(other JSON) bool     { return other == NullJSONValue }
func (jsonFalse) contains(other JSON) bool    { return other == FalseJSONValue }
func (jsonTrue) contains(other JSON) bool     { return other == TrueJSONValue }
func (j jsonNumber) contains(other JSON) bool { return j.Compare(other) == 0 }
func (j jsonString) contains(other JSON) bool { return j.Compare(other) == 0 }

func partitionSortedJSONArray(j jsonArray) (scalars, arrays, objects []JSON) {
	// Since the slice is sorted, we just need to find the split points. This
	// works because objects always sort after everything and arrays always sort
	// after scalars.
	firstArrayIdx := len(j)
	firstObjIdx := len(j)
	for i := 0; i < len(j); i++ {
		if !j[i].isScalar() {
			firstArrayIdx = i
			break
		}
	}
	for i := firstArrayIdx; i < len(j); i++ {
		if _, ok := j[i].(jsonObject); ok {
			firstObjIdx = i
			break
		}
	}
	return j[:firstArrayIdx], j[firstArrayIdx:firstObjIdx], j[firstObjIdx:]
}

func (j jsonArray) contains(other JSON) bool {
	if other.isScalar() {
		found := sort.Search(len(j), func(i int) bool {
			return j[i].Compare(other) >= 0
		})
		return found < len(j) && j[found].Compare(other) == 0
	}

	if contained, ok := other.(jsonArray); ok {
		containerScalars, containerArrays, containerObjects := partitionSortedJSONArray(j)
		otherScalars, otherArrays, otherObjects := partitionSortedJSONArray(contained)

		// Both slices of scalars are sorted now via the preprocessing, so we can
		// step through them together via binary search.
		remainingScalars := containerScalars[:]
		for _, val := range otherScalars {
			found := sort.Search(len(remainingScalars), func(i int) bool {
				return remainingScalars[i].Compare(val) >= 0
			})
			if found == len(remainingScalars) || remainingScalars[found].Compare(val) != 0 {
				return false
			}
			remainingScalars = remainingScalars[found:]
		}

		// TODO(justin): there's possibly(?) something fancier we can do with the
		// objects and arrays, but for now just do the quadratic check.
		if !quadraticJSONArrayContains(containerObjects, otherObjects) ||
			!quadraticJSONArrayContains(containerArrays, otherArrays) {
			return false
		}
		return true
	}
	return false
}

// quadraticJSONArrayContains does an O(n^2) check to see if every value in
// `other` is contained within a value in `container`. `container` and `other`
// cannot contain scalars.
func quadraticJSONArrayContains(container, other jsonArray) bool {
	for _, otherVal := range other {
		found := false
		for _, containerVal := range container {
			if containerVal.contains(otherVal) {
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

func (j jsonObject) contains(other JSON) bool {
	if contained, ok := other.(jsonObject); ok {
		// We can iterate through the keys of `other` and scan through to find the
		// corresponding keys in `j` since they're both sorted.
		objIdx := 0
		for _, rightEntry := range contained {
			for objIdx < len(j) && j[objIdx].k < rightEntry.k {
				objIdx++
			}
			if objIdx >= len(j) ||
				j[objIdx].k != rightEntry.k ||
				!j[objIdx].v.contains(rightEntry.v) {
				return false
			}
			objIdx++
		}
		return true
	}
	return false
}
