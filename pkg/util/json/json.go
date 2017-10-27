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
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"unicode"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/stringencoding"
)

type jsonType int

// This enum defines the ordering of types. It should not be reordered.
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

// JSON represents a JSON value.
type JSON interface {
	fmt.Stringer

	Compare(JSON) int
	jsonType() jsonType
	// Format writes out the JSON document to the specified buffer.
	Format(buf *bytes.Buffer)
	// Size returns the size of the JSON document in bytes.
	Size() uintptr
}

type jsonTrue struct{}

// TrueJSONValue is JSON `true`
var TrueJSONValue = jsonTrue{}

type jsonFalse struct{}

// FalseJSONValue is JSON `false`
var FalseJSONValue = jsonFalse{}

type jsonNull struct{}

// NullJSONValue is JSON `null`
var NullJSONValue = jsonNull{}

type jsonNumber apd.Decimal
type jsonString string

type jsonArray []JSON

type jsonKeyValuePair struct {
	k jsonString
	v JSON
}
type jsonObject []jsonKeyValuePair

func (jsonNull) jsonType() jsonType   { return nullJSONType }
func (jsonFalse) jsonType() jsonType  { return falseJSONType }
func (jsonTrue) jsonType() jsonType   { return trueJSONType }
func (jsonNumber) jsonType() jsonType { return numberJSONType }
func (jsonString) jsonType() jsonType { return stringJSONType }
func (jsonArray) jsonType() jsonType  { return arrayJSONType }
func (jsonObject) jsonType() jsonType { return objectJSONType }

func cmpJSONTypes(a jsonType, b jsonType) int {
	if b > a {
		return -1
	}
	if b < a {
		return 1
	}
	return 0
}

func (j jsonNull) Compare(other JSON) int  { return cmpJSONTypes(j.jsonType(), other.jsonType()) }
func (j jsonFalse) Compare(other JSON) int { return cmpJSONTypes(j.jsonType(), other.jsonType()) }
func (j jsonTrue) Compare(other JSON) int  { return cmpJSONTypes(j.jsonType(), other.jsonType()) }

func (j jsonNumber) Compare(other JSON) int {
	cmp := cmpJSONTypes(j.jsonType(), other.jsonType())
	if cmp != 0 {
		return cmp
	}
	dec := apd.Decimal(j)
	o := apd.Decimal(other.(jsonNumber))
	return dec.Cmp(&o)
}

func (j jsonString) Compare(other JSON) int {
	cmp := cmpJSONTypes(j.jsonType(), other.jsonType())
	if cmp != 0 {
		return cmp
	}
	o := other.(jsonString)
	if o > j {
		return -1
	}
	if o < j {
		return 1
	}
	return 0
}

func (j jsonArray) Compare(other JSON) int {
	cmp := cmpJSONTypes(j.jsonType(), other.jsonType())
	if cmp != 0 {
		return cmp
	}
	o := other.(jsonArray)
	if len(j) < len(o) {
		return -1
	}
	if len(j) > len(o) {
		return 1
	}
	for i := 0; i < len(j); i++ {
		cmp := j[i].Compare(o[i])
		if cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (j jsonObject) Compare(other JSON) int {
	cmp := cmpJSONTypes(j.jsonType(), other.jsonType())
	if cmp != 0 {
		return cmp
	}
	o := other.(jsonObject)
	if len(j) < len(o) {
		return -1
	}
	if len(j) > len(o) {
		return 1
	}
	for i := 0; i < len(j); i++ {
		cmpKey := j[i].k.Compare(o[i].k)
		if cmpKey != 0 {
			return cmpKey
		}
		cmpVal := j[i].v.Compare(o[i].v)
		if cmpVal != 0 {
			return cmpVal
		}
	}
	return 0
}

var errTrailingCharacters = pgerror.NewError(pgerror.CodeInvalidTextRepresentationError, "trailing characters after JSON document")

func (jsonNull) Format(buf *bytes.Buffer) { buf.WriteString("null") }

func (jsonFalse) Format(buf *bytes.Buffer) { buf.WriteString("false") }

func (jsonTrue) Format(buf *bytes.Buffer) { buf.WriteString("true") }

func (j jsonNumber) Format(buf *bytes.Buffer) {
	dec := apd.Decimal(j)
	buf.WriteString(dec.String())
}

func (j jsonString) Format(buf *bytes.Buffer) {
	encodeJSONString(buf, string(j))
}

func asString(j JSON) string {
	var buf bytes.Buffer
	j.Format(&buf)
	return buf.String()
}

func (j jsonNull) String() string   { return asString(j) }
func (j jsonTrue) String() string   { return asString(j) }
func (j jsonFalse) String() string  { return asString(j) }
func (j jsonString) String() string { return asString(j) }
func (j jsonNumber) String() string { return asString(j) }
func (j jsonArray) String() string  { return asString(j) }
func (j jsonObject) String() string { return asString(j) }

// encodeJSONString writes a string literal to buf as a JSON string.
// Very similar to encodeSQLStringInsideArray. Primary difference is that it is
// legal to directly print out unicode characters.
func encodeJSONString(buf *bytes.Buffer, in string) {
	buf.WriteByte('"')
	// Loop through each unicode code point.
	for i, r := range in {
		ch := byte(r)
		if unicode.IsPrint(r) && !stringencoding.NeedEscape(ch) && ch != '"' {
			// Character is printable doesn't need escaping - just print it out.
			buf.WriteRune(r)
		} else {
			stringencoding.EncodeEscapedChar(buf, in, r, ch, i, '"')
		}
	}

	buf.WriteByte('"')
}

func (j jsonArray) Format(buf *bytes.Buffer) {
	buf.WriteByte('[')
	for i := range j {
		if i != 0 {
			buf.WriteByte(',')
		}
		j[i].Format(buf)
	}
	buf.WriteByte(']')
}

func (j jsonObject) Format(buf *bytes.Buffer) {
	buf.WriteByte('{')
	for i := range j {
		if i != 0 {
			buf.WriteByte(',')
		}
		encodeJSONString(buf, string(j[i].k))
		buf.WriteByte(':')
		j[i].v.Format(buf)
	}
	buf.WriteByte('}')
}

func (jsonNull) Size() uintptr { return 0 }

func (jsonFalse) Size() uintptr { return 0 }

func (jsonTrue) Size() uintptr { return 0 }

func (j jsonNumber) Size() uintptr {
	intVal := j.Coeff
	return unsafe.Sizeof(j) + uintptr(cap(intVal.Bits()))*unsafe.Sizeof(big.Word(0))
}

func (j jsonString) Size() uintptr {
	return unsafe.Sizeof(j) + uintptr(len(j))
}

func (j jsonArray) Size() uintptr {
	valSize := uintptr(0)
	for i := range j {
		valSize += unsafe.Sizeof(j[i])
		valSize += j[i].Size()
	}
	return valSize
}

func (j jsonObject) Size() uintptr {
	valSize := uintptr(0)
	for i := range j {
		valSize += unsafe.Sizeof(j[i])
		valSize += j[i].k.Size()
		valSize += j[i].v.Size()
	}
	return valSize
}

// ParseJSON takes a string of JSON and returns a JSON value.
func ParseJSON(s string) (JSON, error) {
	// This goes in two phases - first it parses the string into raw interface{}s
	// using the Go encoding/json package, then it transforms that into a JSON.
	// This could be faster if we wrote a parser to go directly into the JSON.
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
	return MakeJSON(result)
}

// MakeJSON returns a JSON value given a Go-style representation of JSON.
// * JSON null is Go `nil`,
// * JSON true is Go `true`,
// * JSON false is Go `false`,
// * JSON numbers are json.Number | int | int64 | float64,
// * JSON string is a Go string,
// * JSON array is a Go []interface{},
// * JSON object is a Go map[string]interface{}.
func MakeJSON(d interface{}) (JSON, error) {
	switch v := d.(type) {
	case json.Number:
		// The JSON decoder has already verified that the string `v` represents a
		// valid JSON number, and the set of valid JSON numbers is a [proper] subset
		// of the set of valid apd.Decimal values.
		dec := apd.Decimal{}
		_, _, err := dec.SetString(string(v))
		return jsonNumber(dec), err
	case string:
		return jsonString(v), nil
	case bool:
		if v {
			return TrueJSONValue, nil
		}
		return FalseJSONValue, nil
	case nil:
		return NullJSONValue, nil
	case []interface{}:
		elems := make([]JSON, len(v))
		for i := range v {
			var err error
			elems[i], err = MakeJSON(v[i])
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
			v, err := MakeJSON(v[keys[i]])
			if err != nil {
				return nil, err
			}
			result[i] = jsonKeyValuePair{
				k: jsonString(keys[i]),
				v: v,
			}
		}
		return jsonObject(result), nil
		// The below are not used by ParseDJSON, but are provided for ease-of-use when
		// constructing Datums.
	case int:
		dec := apd.Decimal{}
		dec.SetCoefficient(int64(v))
		return jsonNumber(dec), nil
	case int64:
		dec := apd.Decimal{}
		dec.SetCoefficient(v)
		return jsonNumber(dec), nil
	case float64:
		dec := apd.Decimal{}
		_, err := dec.SetFloat64(v)
		if err != nil {
			return nil, err
		}
		return jsonNumber(dec), nil
	}
	return nil, pgerror.NewError("invalid value %s passed to MakeJSON", d.(fmt.Stringer).String())
}
