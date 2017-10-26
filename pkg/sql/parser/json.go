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

package parser

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

var errTrailingCharacters = pgerror.NewError(pgerror.CodeInvalidTextRepresentationError, "trailing characters after JSON document")

// ParseDJSON takes a string of JSON and returns a JSON datum.
func ParseDJSON(s string) (Datum, error) {
	// This goes in two phases - first it parses the string into raw interface{}s
	// using the Go encoding/json package, then it transforms that into a DJSON.
	// This could be faster if we wrote a parser to go directly into the DJSON.
	var result interface{}
	decoder := json.NewDecoder(strings.NewReader(s))
	// We want arbitrary size/precision decimals, so we call UseNumber() to tell
	// the decoder to decode numbers into strings instead of float64s (which we
	// later parse using apd).
	decoder.UseNumber()
	err := decoder.Decode(&result)
	if err != nil {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidTextRepresentationError, "error decoding JSON: %s", err.Error())
	}
	if decoder.More() {
		return nil, errTrailingCharacters
	}
	datum, err := interpretJSON(result)
	if err != nil {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidTextRepresentationError, "error decoding JSON: %s", err.Error())
	}
	return &datum, nil
}

func interpretJSON(d interface{}) (DJSON, error) {
	switch v := d.(type) {
	case json.Number:
		// The JSON decoder has already verified that the string `v` represents a
		// valid JSON number, and the set of valid JSON numbers is a [proper] subset
		// of the set of valid DDecimal values.
		val, err := ParseDDecimalVal(string(v))
		if err != nil {
			return DJSON{}, err
		}
		return DJSON{
			typ:    numberJSONType,
			numVal: val,
		}, nil
	case string:
		return DJSON{
			typ:    stringJSONType,
			strVal: DString(v),
		}, nil
	case bool:
		if v {
			return DJSON{
				typ: trueJSONType,
			}, nil
		}
		return DJSON{
			typ: falseJSONType,
		}, nil
	case nil:
		return DJSON{
			typ: nullJSONType,
		}, nil
	case []interface{}:
		elems := make([]DJSON, len(v))
		for i := range v {
			var err error
			elems[i], err = interpretJSON(v[i])
			if err != nil {
				return DJSON{}, err
			}
		}
		return DJSON{
			typ:      arrayJSONType,
			arrayVal: elems,
		}, nil
	case map[string]interface{}:
		keys := make([]string, len(v))
		i := 0
		for k := range v {
			keys[i] = k
			i++
		}
		sort.Strings(keys)
		result := make([]jsonKeyValuePair, len(v))
		for i := range keys {
			v, err := interpretJSON(v[keys[i]])
			if err != nil {
				return DJSON{}, err
			}
			result[i] = jsonKeyValuePair{
				k: DString(keys[i]),
				v: v,
			}
		}
		return DJSON{
			typ:       objectJSONType,
			objectVal: result,
		}, nil
	}
	return DJSON{}, nil
}

func (obj DJSON) isScalar() bool {
	return obj.typ != objectJSONType && obj.typ != arrayJSONType
}

func (obj DJSON) contains(ctx *EvalContext, other DJSON) bool {
	if obj.isScalar() {
		return obj.Compare(ctx, &other) == 0
	}
	if obj.typ == arrayJSONType && other.isScalar() {
		for _, v := range obj.arrayVal {
			if v.Compare(ctx, &other) == 0 {
				return true
			}
		}
	}
	if obj.typ != other.typ {
		return false
	}
	if obj.typ == arrayJSONType {
		return jsonArrayContains(ctx, obj, other)
	} else if obj.typ == objectJSONType {
		return jsonObjContains(ctx, obj, other)
	}
	return false
}

func jsonArrayContains(ctx *EvalContext, container DJSON, other DJSON) bool {
	if container.typ != arrayJSONType || other.typ != arrayJSONType {
		panic("arguments to jsonArrayContains must be JSON arrays")
	}

	containerScalars, containerArrays, containerObjects := preprocessJSONArrayForContains(ctx, container.arrayVal)
	otherScalars, otherArrays, otherObjects := preprocessJSONArrayForContains(ctx, other.arrayVal)

	// Both slices of scalars are sorted now via the preprocessing, so we can
	// step through them together.
	i := 0
	for _, val := range otherScalars {
		for i < len(containerScalars) && containerScalars[i].Compare(ctx, &val) == -1 {
			i++
		}
		if i >= len(containerScalars) || containerScalars[i].Compare(ctx, &val) != 0 {
			return false
		}
	}

	// TODO(justin): there's possibly(?) something fancier we can do with the
	// objects and arrays, but for now just do the quadratic check.
	if !quadraticJSONArrayContains(ctx, containerObjects, otherObjects) {
		return false
	}
	if !quadraticJSONArrayContains(ctx, containerArrays, otherArrays) {
		return false
	}

	return true
}

// quadraticJSONArrayContains does an O(n^2) check to see if every value in
// `other` is contained within a value in `container`. `container` and `other`
// cannot contain scalars.
func quadraticJSONArrayContains(ctx *EvalContext, container, other []DJSON) bool {
	for _, otherVal := range other {
		found := false
		for _, containerVal := range container {
			if containerVal.contains(ctx, otherVal) {
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

// preprocessJSONArrayForContains takes a slice of DJSONs and returns three slices containing
// * the scalars in the given slice, sorted,
// * the arrays in the input slice,
// * and the objects in the input slice.
// The input array is unmodified.
// TODO(justin): this is slightly alloc-heavy - in theory we could re-use a
// slice for each call to this function, or if we wanted to get crazy we could
// permute the input array and then pass back up a way to permute it back to
// the way it was once the caller was done with it.
// TODO(justin): if a JSON document is given as a literal for the @> operator,
// theoretically during planning we could do this preprocessing and avoid doing it
// on every invocation of the operator.
func preprocessJSONArrayForContains(
	ctx *EvalContext, ary []DJSON,
) (scalars, arrays, objects []DJSON) {
	sorted := make([]DJSON, len(ary))
	copy(sorted, ary)
	// Note: we don't actually care about the ordering of arrays and
	// objects.
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Compare(ctx, &sorted[j]) == -1
	})

	// Now that the slice is sorted, we just need to find the split points. This
	// works because objects always sort after everything and arrays always sort
	// after scalars.
	firstArrayIdx := len(sorted)
	firstObjIdx := len(sorted)
	for i := 0; i < len(sorted); i++ {
		if !sorted[i].isScalar() {
			firstArrayIdx = i
			break
		}
	}
	for i := firstArrayIdx; i < len(sorted); i++ {
		if sorted[i].typ == objectJSONType {
			firstObjIdx = i
			break
		}
	}
	return sorted[:firstArrayIdx], sorted[firstArrayIdx:firstObjIdx], sorted[firstObjIdx:]
}

func jsonObjContains(ctx *EvalContext, container DJSON, other DJSON) bool {
	if container.typ != objectJSONType || other.typ != objectJSONType {
		panic("arguments to jsonObjContains must be JSON objects")
	}
	// We can iterate through the keys of `other` and scan through to find the
	// corresponding keys in `container` since they're both sorted.
	objIdx := 0
	for _, rightEntry := range other.objectVal {
		for objIdx < len(container.objectVal) && container.objectVal[objIdx].k < rightEntry.k {
			objIdx++
		}
		if objIdx >= len(container.objectVal) ||
			container.objectVal[objIdx].k != rightEntry.k ||
			!container.objectVal[objIdx].v.contains(ctx, rightEntry.v) {
			return false
		}
		objIdx++
	}
	return true
}
