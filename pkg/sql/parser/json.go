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
	"strconv"
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

// asText formats a DJSON as required by ->>, that is, strings are returned
// without quotes, and everything else is as normal.
func (obj DJSON) asText() DString {
	if obj.typ == stringJSONType {
		return obj.strVal
	}
	return DString(obj.String())
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

// fetchValKey implements the `->` operator for strings.
func (obj *DJSON) fetchValKey(key DString) Datum {
	if obj.typ != objectJSONType {
		return DNull
	}
	for i := range obj.objectVal {
		if obj.objectVal[i].k == key {
			return &obj.objectVal[i].v
		}
		if obj.objectVal[i].k > key {
			break
		}
	}
	return DNull
}

// fetchValIdx implements the `->` operator for ints.
func (obj *DJSON) fetchValIdx(idx int) Datum {
	if obj.typ != arrayJSONType {
		return DNull
	}
	if idx < 0 {
		idx = len(obj.arrayVal) + idx
	}
	if idx >= 0 && idx < len(obj.arrayVal) {
		return &obj.arrayVal[idx]
	}
	return DNull
}

func (obj *DJSON) fetchPath(path DArray) Datum {
	var next Datum
	for _, v := range path.Array {
		next = obj.fetchValKeyOrIdx(MustBeDString(v))
		if next == DNull {
			return DNull
		}
		obj = next.(*DJSON)
	}
	return obj
}

// fetchValKeyOrIdx is used for path access, if obj is an object, it tries to
// access the given field. If it's an array, it interprets the key as an int
// and tries to access the given index.
func (obj *DJSON) fetchValKeyOrIdx(key DString) Datum {
	switch obj.typ {
	case objectJSONType:
		return obj.fetchValKey(key)
	case arrayJSONType:
		idx, err := strconv.Atoi(string(key))
		if err != nil {
			return DNull
		}
		return obj.fetchValIdx(idx)
	default:
		return DNull
	}
}

var errCannotDeleteFromScalar = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "cannot delete from scalar")
var errCannotDeleteFromObject = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "cannot delete from object using integer index")

// removeKey implements the `-` operator for strings.
func (obj *DJSON) removeKey(key DString) (Datum, error) {
	switch obj.typ {
	case arrayJSONType:
		return obj, nil
	case objectJSONType:
		newVal := make([]jsonKeyValuePair, 0, len(obj.objectVal))
		for i := range obj.objectVal {
			if obj.objectVal[i].k != key {
				newVal = append(newVal, obj.objectVal[i])
			}
		}
		return &DJSON{typ: objectJSONType, objectVal: newVal}, nil
	default:
		return nil, errCannotDeleteFromScalar
	}
}

// removeIndex implements the `-` operator for ints.
func (obj *DJSON) removeIndex(idx DInt) (Datum, error) {
	switch obj.typ {
	case arrayJSONType:
		if idx < 0 {
			idx = DInt(len(obj.arrayVal) + int(idx))
		}
		if int(idx) < 0 || int(idx) >= len(obj.arrayVal) {
			return obj, nil
		}
		result := make([]DJSON, len(obj.arrayVal)-1)
		for i := 0; i < int(idx); i++ {
			result[i] = obj.arrayVal[i]
		}
		for i := int(idx) + 1; i < len(obj.arrayVal); i++ {
			result[i-1] = obj.arrayVal[i]
		}
		return &DJSON{typ: arrayJSONType, arrayVal: result}, nil
	case objectJSONType:
		return nil, errCannotDeleteFromObject
	default:
		return nil, errCannotDeleteFromScalar
	}
}

// existenceOperator implements the `?` operator.
func (obj *DJSON) existenceOperator(str string) bool {
	switch obj.typ {
	case objectJSONType:
		for i := 0; i < len(obj.objectVal); i++ {
			if string(obj.objectVal[i].k) == str {
				return true
			}
			// This is justified because we store keys in sorted order.
			if string(obj.objectVal[i].k) > str {
				return false
			}
		}
		return false
	case arrayJSONType:
		for i := 0; i < len(obj.arrayVal); i++ {
			if obj.arrayVal[i].typ == stringJSONType && string(obj.arrayVal[i].strVal) == str {
				return true
			}
		}
		return false
	default:
		return false
	}
}
