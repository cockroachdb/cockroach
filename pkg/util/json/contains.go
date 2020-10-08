// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package json

import "sort"

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
func Contains(a, b JSON) (bool, error) {
	if a.Type() == ArrayJSONType && b.isScalar() {
		decoded, err := a.tryDecode()
		if err != nil {
			return false, err
		}
		ary := decoded.(jsonArray)
		return checkArrayContainsScalar(ary, b)
	}

	preA, err := a.preprocessForContains()
	if err != nil {
		return false, err
	}
	preB, err := b.preprocessForContains()
	if err != nil {
		return false, err
	}
	return preA.contains(preB)
}

// checkArrayContainsScalar performs a unique case of contains (and is
// described as such in the Postgres docs) - a top-level array contains a
// scalar which is an element of it.  This contradicts the general rule of
// contains that the contained object must have the same "shape" as the
// containing object.
func checkArrayContainsScalar(ary jsonArray, s JSON) (bool, error) {
	for _, j := range ary {
		cmp, err := j.Compare(s)
		if err != nil {
			return false, err
		}
		if cmp == 0 {
			return true, nil
		}
	}
	return false, nil
}

// containsable is an interface used internally for the implementation of @>.
type containsable interface {
	contains(other containsable) (bool, error)
}

// containsableScalar is a preprocessed JSON scalar. The JSON it holds will
// never be a JSON object or a JSON array.
type containsableScalar struct{ JSON }

// containsableArray is a preprocessed JSON array.
// * scalars will always be scalars and will always be sorted,
// * arrays will only contain containsableArrays,
// * objects will only contain containsableObjects
// (the last two are stored interfaces for reuse, though)
type containsableArray struct {
	scalars []containsableScalar
	arrays  []containsable
	objects []containsable
}

type containsableKeyValuePair struct {
	k jsonString
	v containsable
}

// containsableObject is a preprocessed JSON object.
// Same as a jsonObject, it is stored as a sorted-by-key list of key-value
// pairs.
type containsableObject []containsableKeyValuePair

func (j jsonNull) preprocessForContains() (containsable, error)   { return containsableScalar{j}, nil }
func (j jsonFalse) preprocessForContains() (containsable, error)  { return containsableScalar{j}, nil }
func (j jsonTrue) preprocessForContains() (containsable, error)   { return containsableScalar{j}, nil }
func (j jsonNumber) preprocessForContains() (containsable, error) { return containsableScalar{j}, nil }
func (j jsonString) preprocessForContains() (containsable, error) { return containsableScalar{j}, nil }

func (j jsonArray) preprocessForContains() (containsable, error) {
	result := containsableArray{}
	for _, e := range j {
		switch e.Type() {
		case ArrayJSONType:
			preprocessed, err := e.preprocessForContains()
			if err != nil {
				return nil, err
			}
			result.arrays = append(result.arrays, preprocessed)
		case ObjectJSONType:
			preprocessed, err := e.preprocessForContains()
			if err != nil {
				return nil, err
			}
			result.objects = append(result.objects, preprocessed)
		default:
			preprocessed, err := e.preprocessForContains()
			if err != nil {
				return nil, err
			}
			result.scalars = append(result.scalars, preprocessed.(containsableScalar))
		}
	}

	var err error
	sort.Slice(result.scalars, func(i, j int) bool {
		if err != nil {
			return false
		}
		var c int
		c, err = result.scalars[i].JSON.Compare(result.scalars[j].JSON)
		return c == -1
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (j jsonObject) preprocessForContains() (containsable, error) {
	preprocessed := make(containsableObject, len(j))

	for i := range preprocessed {
		preprocessed[i].k = j[i].k
		v, err := j[i].v.preprocessForContains()
		if err != nil {
			return nil, err
		}
		preprocessed[i].v = v
	}

	return preprocessed, nil
}

func (j containsableScalar) contains(other containsable) (bool, error) {
	if o, ok := other.(containsableScalar); ok {
		result, err := j.JSON.Compare(o.JSON)
		if err != nil {
			return false, err
		}
		return result == 0, nil
	}
	return false, nil
}

func (j containsableArray) contains(other containsable) (bool, error) {
	if contained, ok := other.(containsableArray); ok {
		// Since both slices of scalars are sorted via the preprocessing, we can
		// step through them together via binary search.
		remainingScalars := j.scalars[:]
		for _, val := range contained.scalars {
			var err error
			found := sort.Search(len(remainingScalars), func(i int) bool {
				if err != nil {
					return false
				}
				var result int
				result, err = remainingScalars[i].JSON.Compare(val.JSON)
				return result >= 0
			})

			if found == len(remainingScalars) {
				return false, nil
			}
			result, err := remainingScalars[found].JSON.Compare(val.JSON)
			if err != nil {
				return false, err
			}
			if result != 0 {
				return false, nil
			}
			remainingScalars = remainingScalars[found:]
		}

		// TODO(justin): there's possibly(?) something fancier we can do with the
		// objects and arrays, but for now just do the quadratic check.
		objectsMatch, err := quadraticJSONArrayContains(j.objects, contained.objects)
		if err != nil {
			return false, err
		}
		if !objectsMatch {
			return false, nil
		}

		arraysMatch, err := quadraticJSONArrayContains(j.arrays, contained.arrays)
		if err != nil {
			return false, err
		}
		if !arraysMatch {
			return false, nil
		}

		return true, nil
	}
	return false, nil
}

// quadraticJSONArrayContains does an O(n^2) check to see if every value in
// `other` is contained within a value in `container`. `container` and `other`
// should not contain scalars.
func quadraticJSONArrayContains(container, other []containsable) (bool, error) {
	for _, otherVal := range other {
		found := false
		for _, containerVal := range container {
			c, err := containerVal.contains(otherVal)
			if err != nil {
				return false, err
			}
			if c {
				found = true
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	return true, nil
}

func (j containsableObject) contains(other containsable) (bool, error) {
	if contained, ok := other.(containsableObject); ok {
		// We can iterate through the keys of `other` and scan through to find the
		// corresponding keys in `j` since they're both sorted.
		objIdx := 0
		for _, rightEntry := range contained {
			for objIdx < len(j) && j[objIdx].k < rightEntry.k {
				objIdx++
			}
			if objIdx >= len(j) ||
				j[objIdx].k != rightEntry.k {
				return false, nil
			}
			c, err := j[objIdx].v.contains(rightEntry.v)
			if err != nil {
				return false, err
			}
			if !c {
				return false, nil
			}
			objIdx++
		}
		return true, nil
	}
	return false, nil
}
