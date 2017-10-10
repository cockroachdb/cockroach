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
