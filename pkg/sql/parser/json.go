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
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

type jsonType int

// This enum defines the ordering of types.
const (
	_ jsonType = iota
	nullJSONType
	stringJSONType
	numberJSONType
	falseJSONType
	trueJSONType
	arrayJSONType
	objectJSONType
)

type jsonVal interface {
	compare(*EvalContext, jsonVal) int
	typeOrdinal() jsonType
	format(buf *bytes.Buffer)
	size() uintptr
}

type jsonTrue struct{}
type jsonFalse struct{}
type jsonNull struct{}

type jsonNumber DDecimal
type jsonString DString

type jsonArray []jsonVal

type jsonKeyValuePair struct {
	k DString
	v jsonVal
}
type jsonObject []jsonKeyValuePair

func (jsonNull) typeOrdinal() jsonType   { return nullJSONType }
func (jsonFalse) typeOrdinal() jsonType  { return falseJSONType }
func (jsonTrue) typeOrdinal() jsonType   { return trueJSONType }
func (jsonNumber) typeOrdinal() jsonType { return numberJSONType }
func (jsonString) typeOrdinal() jsonType { return stringJSONType }
func (jsonArray) typeOrdinal() jsonType  { return arrayJSONType }
func (jsonObject) typeOrdinal() jsonType { return objectJSONType }

func (j jsonNull) compare(_ *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	// This should never happen provided the orderings of JSON values don't
	// change.
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	return 0
}

func (j jsonFalse) compare(_ *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	return 0
}

func (j jsonTrue) compare(_ *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	return 0
}

func (j jsonNumber) compare(ctx *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	o := DDecimal(other.(jsonNumber))
	dec := DDecimal(j)
	return dec.Compare(ctx, &o)
}

func (j jsonString) compare(ctx *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	o := DString(other.(jsonString))
	str := DString(j)
	return str.Compare(ctx, &o)
}

func (j jsonArray) compare(ctx *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	o := other.(jsonArray)
	if len(j) < len(o) {
		return -1
	}
	if len(j) > len(o) {
		return 1
	}
	for i := 0; i < len(j); i++ {
		cmp := j[i].compare(ctx, o[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (j jsonObject) compare(ctx *EvalContext, other jsonVal) int {
	if other.typeOrdinal() > j.typeOrdinal() {
		return -1
	}
	if other.typeOrdinal() < j.typeOrdinal() {
		return 1
	}
	o := other.(jsonObject)
	if len(j) < len(o) {
		return -1
	}
	if len(j) > len(o) {
		return 1
	}
	for i := 0; i < len(j); i++ {
		cmpKey := j[i].k.Compare(ctx, &o[i].k)
		if cmpKey != 0 {
			return cmpKey
		}
		cmpVal := j[i].v.compare(ctx, o[i].v)
		if cmpVal != 0 {
			return cmpVal
		}
	}
	return 0
}

var errTrailingCharacters = pgerror.NewError(pgerror.CodeInvalidTextRepresentationError, "trailing characters after JSON document")

func (jsonNull) format(buf *bytes.Buffer) { buf.WriteString("null") }

func (jsonFalse) format(buf *bytes.Buffer) { buf.WriteString("false") }

func (jsonTrue) format(buf *bytes.Buffer) { buf.WriteString("true") }

func (j jsonNumber) format(buf *bytes.Buffer) {
	dec := DDecimal(j)
	dec.Format(buf, FmtSimple)
}

func (j jsonString) format(buf *bytes.Buffer) {
	encodeJSONString(buf, string(j), FmtSimple)
}

func (j jsonArray) format(buf *bytes.Buffer) {
	buf.WriteByte('[')
	for i := range j {
		if i != 0 {
			buf.WriteByte(',')
		}
		j[i].format(buf)
	}
	buf.WriteByte(']')
}

func (j jsonObject) format(buf *bytes.Buffer) {
	buf.WriteByte('{')
	for i := range j {
		if i != 0 {
			buf.WriteByte(',')
		}
		encodeJSONString(buf, string(j[i].k), FmtSimple)
		buf.WriteByte(':')
		j[i].v.format(buf)
	}
	buf.WriteByte('}')
}

func (jsonNull) size() uintptr { return 0 }

func (jsonFalse) size() uintptr { return 0 }

func (jsonTrue) size() uintptr { return 0 }

func (j jsonNumber) size() uintptr {
	dec := DDecimal(j)
	return dec.Size()
}

func (j jsonString) size() uintptr {
	str := DString(j)
	return str.Size()
}

func (j jsonArray) size() uintptr {
	valSize := uintptr(0)
	for i := range j {
		valSize += unsafe.Sizeof(j[i])
		valSize += j[i].size()
	}
	return valSize
}

func (j jsonObject) size() uintptr {
	valSize := uintptr(0)
	for i := range j {
		valSize += unsafe.Sizeof(j[i])
		valSize += j[i].k.Size()
		valSize += j[i].v.size()
	}
	return valSize
}

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
	j, err := interpretJSON(result)
	if err != nil {
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidTextRepresentationError, "error decoding JSON: %s", err.Error())
	}
	datum := DJSON{j}
	return &datum, nil
}

func interpretJSON(d interface{}) (jsonVal, error) {
	switch v := d.(type) {
	case json.Number:
		// The JSON decoder has already verified that the string `v` represents a
		// valid JSON number, and the set of valid JSON numbers is a [proper] subset
		// of the set of valid DDecimal values.
		val, err := ParseDDecimalVal(string(v))
		if err != nil {
			return nil, err
		}
		return jsonNumber(val), nil
	case string:
		return jsonString(v), nil
	case bool:
		if v {
			return jsonTrue{}, nil
		}
		return jsonFalse{}, nil
	case nil:
		return jsonNull{}, nil
	case []interface{}:
		elems := make([]jsonVal, len(v))
		for i := range v {
			var err error
			elems[i], err = interpretJSON(v[i])
			if err != nil {
				return nil, err
			}
		}
		return jsonArray(elems), nil
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
				return nil, err
			}
			result[i] = jsonKeyValuePair{
				k: DString(keys[i]),
				v: v,
			}
		}
		return jsonObject(result), nil
	}
	return nil, nil
}
