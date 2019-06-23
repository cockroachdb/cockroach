// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This file contains (ha!) helpers to test @>.

package json

import "math/rand"

type containsTester interface {
	// slowContains is a slower but simpler implementation of contains to check
	// against the substantially more complex actual implementation.
	slowContains(other JSON) bool

	// subdocument returns a JSON document which is contained by this one.
	subdocument(isRoot bool, rng *rand.Rand) JSON
}

func slowContains(a, b JSON) bool {
	// This is a unique case of contains (and is described as such in the
	// Postgres docs) - an array contains a scalar which is an element of it.
	// This contradicts the general rule of contains that the contained object
	// must have the same "shape" as the containing object.
	if a.Type() == ArrayJSONType {
		ary := a.MaybeDecode().(jsonArray)
		if b.isScalar() {
			for _, j := range ary {
				cmp, _ := j.Compare(b)
				if cmp == 0 {
					return true
				}
			}
			return false
		}
	}

	return a.(containsTester).slowContains(b)
}

func (j jsonNull) slowContains(other JSON) bool {
	c, _ := j.Compare(other)
	return c == 0
}
func (j jsonTrue) slowContains(other JSON) bool {
	c, _ := j.Compare(other)
	return c == 0
}
func (j jsonFalse) slowContains(other JSON) bool {
	c, _ := j.Compare(other)
	return c == 0
}
func (j jsonNumber) slowContains(other JSON) bool {
	c, _ := j.Compare(other)
	return c == 0
}
func (j jsonString) slowContains(other JSON) bool {
	c, _ := j.Compare(other)
	return c == 0
}

func (j jsonArray) slowContains(other JSON) bool {
	other = other.MaybeDecode()
	if ary, ok := other.(jsonArray); ok {
		for i := 0; i < len(ary); i++ {
			found := false
			for k := 0; k < len(j); k++ {
				if j[k].Type() == ary[i].Type() && j[k].(containsTester).slowContains(ary[i]) {
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
	other = other.MaybeDecode()
	if obj, ok := other.(jsonObject); ok {
		for i := 0; i < len(obj); i++ {
			leftVal, _ := j.FetchValKey(string(obj[i].k))
			if leftVal == nil || !leftVal.(containsTester).slowContains(obj[i].v) {
				return false
			}
		}
		return true
	}
	return false
}

func (j *jsonEncoded) slowContains(other JSON) bool {
	return j.mustDecode().(containsTester).slowContains(other)
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

func (j *jsonEncoded) subdocument(isRoot bool, rng *rand.Rand) JSON {
	return j.mustDecode().(containsTester).subdocument(isRoot, rng)
}
