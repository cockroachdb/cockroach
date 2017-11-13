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
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// Type represents a JSON type.
type Type int

// This enum defines the ordering of types. It should not be reordered.
const (
	_ Type = iota
	NullJSONType
	StringJSONType
	NumberJSONType
	FalseJSONType
	TrueJSONType
	ArrayJSONType
	ObjectJSONType
)

// JSON represents a JSON value.
type JSON interface {
	fmt.Stringer

	Compare(JSON) (int, error)
	// Type returns the JSON type.
	Type() Type
	// Format writes out the JSON document to the specified buffer.
	Format(buf *bytes.Buffer)
	// Size returns the size of the JSON document in bytes.
	Size() uintptr

	// EncodeInvertedIndexKeys takes in a key prefix and returns a slice of inverted index keys,
	// one per path through the receiver.
	EncodeInvertedIndexKeys(b []byte) ([][]byte, error)

	// FetchValKey implements the `->` operator for strings, returning nil if the
	// key is not found.
	FetchValKey(key string) (JSON, error)

	// FetchValIdx implements the `->` operator for ints, returning nil if the
	// key is not found.
	FetchValIdx(idx int) (JSON, error)

	// FetchValKeyOrIdx is used for path access, if obj is an object, it tries to
	// access the given field. If it's an array, it interprets the key as an int
	// and tries to access the given index.
	FetchValKeyOrIdx(key string) (JSON, error)

	// RemoveKey implements the `-` operator for strings.
	RemoveKey(key string) (JSON, error)

	// RemoveIndex implements the `-` operator for ints.
	RemoveIndex(idx int) (JSON, error)

	// TypeAsText returns the type of JSON document as a string.
	TypeAsText() string

	// AsText returns the JSON document as a string, with quotes around strings removed, and null as nil.
	AsText() (*string, error)

	// Exists implements the `?` operator.
	Exists(string) (bool, error)

	// isScalar returns whether the JSON document is null, true, false, a string,
	// or a number.
	isScalar() bool

	// preprocessForContains converts a JSON document to an internal interface
	// which is used to efficiently implement the @> operator.
	preprocessForContains() (containsable, error)

	// encode appends the encoding of the JSON document to appendTo, returning
	// the result alongside the JEntry for the document. Note that some values
	// (true/false/null) are encoded with 0 bytes and are purely defined by their
	// JEntry.
	encode(appendTo []byte) (jEntry uint32, b []byte, err error)

	maybeDecode() JSON
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

// jsonObject represents a JSON object as a sorted-by-key list of key-value
// pairs, which are unique by key.
type jsonObject []jsonKeyValuePair

func (jsonNull) Type() Type   { return NullJSONType }
func (jsonFalse) Type() Type  { return FalseJSONType }
func (jsonTrue) Type() Type   { return TrueJSONType }
func (jsonNumber) Type() Type { return NumberJSONType }
func (jsonString) Type() Type { return StringJSONType }
func (jsonArray) Type() Type  { return ArrayJSONType }
func (jsonObject) Type() Type { return ObjectJSONType }

func (j jsonNull) maybeDecode() JSON   { return j }
func (j jsonFalse) maybeDecode() JSON  { return j }
func (j jsonTrue) maybeDecode() JSON   { return j }
func (j jsonNumber) maybeDecode() JSON { return j }
func (j jsonString) maybeDecode() JSON { return j }
func (j jsonArray) maybeDecode() JSON  { return j }
func (j jsonObject) maybeDecode() JSON { return j }

func cmpJSONTypes(a Type, b Type) int {
	if b > a {
		return -1
	}
	if b < a {
		return 1
	}
	return 0
}

func (j jsonNull) Compare(other JSON) (int, error)  { return cmpJSONTypes(j.Type(), other.Type()), nil }
func (j jsonFalse) Compare(other JSON) (int, error) { return cmpJSONTypes(j.Type(), other.Type()), nil }
func (j jsonTrue) Compare(other JSON) (int, error)  { return cmpJSONTypes(j.Type(), other.Type()), nil }

func (j jsonNumber) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	dec := apd.Decimal(j)
	o := apd.Decimal(other.(jsonNumber))
	return dec.Cmp(&o), nil
}

func (j jsonString) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	o := other.(jsonString)
	if o > j {
		return -1, nil
	}
	if o < j {
		return 1, nil
	}
	return 0, nil
}

func (j jsonArray) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	o := other.(jsonArray)
	if len(j) < len(o) {
		return -1, nil
	}
	if len(j) > len(o) {
		return 1, nil
	}
	for i := 0; i < len(j); i++ {
		cmp, err := j[i].Compare(o[i])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func (j jsonObject) Compare(other JSON) (int, error) {
	cmp := cmpJSONTypes(j.Type(), other.Type())
	if cmp != 0 {
		return cmp, nil
	}
	o := other.(jsonObject)
	if len(j) < len(o) {
		return -1, nil
	}
	if len(j) > len(o) {
		return 1, nil
	}
	for i := 0; i < len(j); i++ {
		cmpKey, err := j[i].k.Compare(o[i].k)
		if err != nil {
			return 0, err
		}
		if cmpKey != 0 {
			return cmpKey, nil
		}
		cmpVal, err := j[i].v.Compare(o[i].v)
		if err != nil {
			return 0, err
		}
		if cmpVal != 0 {
			return cmpVal, nil
		}
	}
	return 0, nil
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

const hexAlphabet = "0123456789abcdef"

// encodeJSONString writes a string literal to buf as a JSON string.
// Cribbed from https://github.com/golang/go/blob/7badae85f20f1bce4cc344f9202447618d45d414/src/encoding/json/encode.go.
func encodeJSONString(buf *bytes.Buffer, s string) {
	buf.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if safeSet[b] {
				i++
				continue
			}
			if start < i {
				buf.WriteString(s[start:i])
			}
			switch b {
			case '\\', '"':
				buf.WriteByte('\\')
				buf.WriteByte(b)
			case '\n':
				buf.WriteByte('\\')
				buf.WriteByte('n')
			case '\r':
				buf.WriteByte('\\')
				buf.WriteByte('r')
			case '\t':
				buf.WriteByte('\\')
				buf.WriteByte('t')
			default:
				// This encodes bytes < 0x20 except for \t, \n and \r.
				// If escapeHTML is set, it also escapes <, >, and &
				// because they can lead to security holes when
				// user-controlled strings are rendered into JSON
				// and served to some browsers.
				buf.WriteString(`\u00`)
				buf.WriteByte(hexAlphabet[b>>4])
				buf.WriteByte(hexAlphabet[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				buf.WriteString(s[start:i])
			}
			buf.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		buf.WriteString(s[start:])
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

func (j jsonNull) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	return [][]byte{encoding.EncodeNullAscending(b)}, nil
}
func (jsonTrue) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	return [][]byte{encoding.EncodeTrueAscending(b)}, nil
}
func (jsonFalse) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	return [][]byte{encoding.EncodeFalseAscending(b)}, nil
}
func (j jsonString) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	return [][]byte{encoding.EncodeStringAscending(b, string(j))}, nil
}
func (j jsonNumber) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	var dec = apd.Decimal(j)
	return [][]byte{encoding.EncodeDecimalAscending(b, &dec)}, nil
}
func (j jsonArray) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	var outKeys [][]byte

	for i := range j {
		children, err := j[i].EncodeInvertedIndexKeys(nil)
		if err != nil {
			return nil, err
		}
		for _, childBytes := range children {
			encodedKey := bytes.Join([][]byte{b, encoding.EncodeArrayAscending(nil), childBytes}, nil)
			outKeys = append(outKeys, encodedKey)
		}
	}

	return outKeys, nil
}

func (j jsonObject) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	var outKeys [][]byte
	for i := range j {
		children, err := j[i].v.EncodeInvertedIndexKeys(nil)
		if err != nil {
			return nil, err
		}
		for _, childBytes := range children {
			encodedKey := bytes.Join([][]byte{b,
				encoding.EncodeNotNullAscending(nil),
				encoding.EncodeStringAscending(nil, string(j[i].k)),
				childBytes}, nil)

			outKeys = append(outKeys, encodedKey)
		}
	}

	return outKeys, nil
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
		// The below are not used by ParseJSON, but are provided for ease-of-use when
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
	// If we get passed a JSON, just accept it. This is useful in cases like the
	// random JSON generator.
	if j, ok := d.(JSON); ok {
		return j, nil
	}
	return nil, pgerror.NewError("invalid value %s passed to MakeJSON", d.(fmt.Stringer).String())
}

// This value was determined through some rough experimental results as a good
// place to start doing binary search over a linear scan.
const bsearchCutoff = 20

func (j jsonObject) FetchValKey(key string) (JSON, error) {
	// For small objects, the overhead of binary search is significant and so
	// it's faster to just do a linear scan.
	if len(j) < bsearchCutoff {
		for i := range j {
			if string(j[i].k) == key {
				return j[i].v, nil
			}
			if string(j[i].k) > key {
				break
			}
		}
		return nil, nil
	}

	i := sort.Search(len(j), func(i int) bool { return string(j[i].k) >= key })
	if i < len(j) && string(j[i].k) == key {
		return j[i].v, nil
	}
	return nil, nil
}

func (jsonNull) FetchValKey(string) (JSON, error)   { return nil, nil }
func (jsonTrue) FetchValKey(string) (JSON, error)   { return nil, nil }
func (jsonFalse) FetchValKey(string) (JSON, error)  { return nil, nil }
func (jsonString) FetchValKey(string) (JSON, error) { return nil, nil }
func (jsonNumber) FetchValKey(string) (JSON, error) { return nil, nil }
func (jsonArray) FetchValKey(string) (JSON, error)  { return nil, nil }

func (j jsonArray) FetchValIdx(idx int) (JSON, error) {
	if idx < 0 {
		idx = len(j) + idx
	}
	if idx >= 0 && idx < len(j) {
		return j[idx], nil
	}
	return nil, nil
}

func (jsonNull) FetchValIdx(int) (JSON, error)   { return nil, nil }
func (jsonTrue) FetchValIdx(int) (JSON, error)   { return nil, nil }
func (jsonFalse) FetchValIdx(int) (JSON, error)  { return nil, nil }
func (jsonString) FetchValIdx(int) (JSON, error) { return nil, nil }
func (jsonNumber) FetchValIdx(int) (JSON, error) { return nil, nil }
func (jsonObject) FetchValIdx(int) (JSON, error) { return nil, nil }

// FetchPath implements the #> operator.
func FetchPath(j JSON, path []string) (JSON, error) {
	var next JSON
	var err error
	for _, v := range path {
		next, err = j.FetchValKeyOrIdx(v)
		if next == nil {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		j = next
	}
	return j, nil
}

func (j jsonObject) FetchValKeyOrIdx(key string) (JSON, error) {
	return j.FetchValKey(key)
}

func (j jsonArray) FetchValKeyOrIdx(key string) (JSON, error) {
	idx, err := strconv.Atoi(key)
	if err != nil {
		// We shouldn't return this error because it means we couldn't parse the
		// number, meaning it was a string and that just means we can't find the
		// value in an array.
		return nil, nil
	}
	return j.FetchValIdx(idx)
}

func (jsonNull) FetchValKeyOrIdx(string) (JSON, error)   { return nil, nil }
func (jsonTrue) FetchValKeyOrIdx(string) (JSON, error)   { return nil, nil }
func (jsonFalse) FetchValKeyOrIdx(string) (JSON, error)  { return nil, nil }
func (jsonString) FetchValKeyOrIdx(string) (JSON, error) { return nil, nil }
func (jsonNumber) FetchValKeyOrIdx(string) (JSON, error) { return nil, nil }

var errCannotDeleteFromScalar = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "cannot delete from scalar")
var errCannotDeleteFromObject = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "cannot delete from object using integer index")

func (j jsonArray) RemoveKey(key string) (JSON, error) {
	return j, nil
}

func (j jsonObject) RemoveKey(key string) (JSON, error) {
	newVal := make([]jsonKeyValuePair, 0, len(j))
	for i := range j {
		if string(j[i].k) != key {
			newVal = append(newVal, j[i])
		}
	}
	return jsonObject(newVal), nil
}

func (jsonNull) RemoveKey(string) (JSON, error)   { return nil, errCannotDeleteFromScalar }
func (jsonTrue) RemoveKey(string) (JSON, error)   { return nil, errCannotDeleteFromScalar }
func (jsonFalse) RemoveKey(string) (JSON, error)  { return nil, errCannotDeleteFromScalar }
func (jsonString) RemoveKey(string) (JSON, error) { return nil, errCannotDeleteFromScalar }
func (jsonNumber) RemoveKey(string) (JSON, error) { return nil, errCannotDeleteFromScalar }

func (j jsonArray) RemoveIndex(idx int) (JSON, error) {
	if idx < 0 {
		idx = len(j) + idx
	}
	if idx < 0 || idx >= len(j) {
		return j, nil
	}
	result := make(jsonArray, len(j)-1)
	for i := 0; i < idx; i++ {
		result[i] = j[i]
	}
	for i := idx + 1; i < len(j); i++ {
		result[i-1] = j[i]
	}
	return result, nil
}

func (j jsonObject) RemoveIndex(int) (JSON, error) {
	return nil, errCannotDeleteFromObject
}

func (jsonNull) RemoveIndex(int) (JSON, error)   { return nil, errCannotDeleteFromScalar }
func (jsonTrue) RemoveIndex(int) (JSON, error)   { return nil, errCannotDeleteFromScalar }
func (jsonFalse) RemoveIndex(int) (JSON, error)  { return nil, errCannotDeleteFromScalar }
func (jsonString) RemoveIndex(int) (JSON, error) { return nil, errCannotDeleteFromScalar }
func (jsonNumber) RemoveIndex(int) (JSON, error) { return nil, errCannotDeleteFromScalar }

func (j jsonString) TypeAsText() string { return "string" }
func (j jsonNull) TypeAsText() string   { return "null" }
func (j jsonTrue) TypeAsText() string   { return "true" }
func (j jsonFalse) TypeAsText() string  { return "false" }
func (j jsonNumber) TypeAsText() string { return "number" }
func (j jsonArray) TypeAsText() string  { return "array" }
func (j jsonObject) TypeAsText() string { return "object" }

func (j jsonString) AsText() (*string, error) {
	s := string(j)
	return &s, nil
}
func (j jsonNull) AsText() (*string, error) { return nil, nil }
func (j jsonTrue) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonFalse) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonNumber) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonArray) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}
func (j jsonObject) AsText() (*string, error) {
	s := j.String()
	return &s, nil
}

func (jsonNull) Exists(string) (bool, error)   { return false, nil }
func (jsonTrue) Exists(string) (bool, error)   { return false, nil }
func (jsonFalse) Exists(string) (bool, error)  { return false, nil }
func (jsonNumber) Exists(string) (bool, error) { return false, nil }
func (jsonString) Exists(string) (bool, error) { return false, nil }
func (j jsonArray) Exists(s string) (bool, error) {
	for i := 0; i < len(j); i++ {
		if elem, ok := j[i].(jsonString); ok && string(elem) == s {
			return true, nil
		}
	}
	return false, nil
}
func (j jsonObject) Exists(s string) (bool, error) {
	v, err := j.FetchValKey(s)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (jsonNull) isScalar() bool   { return true }
func (jsonFalse) isScalar() bool  { return true }
func (jsonTrue) isScalar() bool   { return true }
func (jsonNumber) isScalar() bool { return true }
func (jsonString) isScalar() bool { return true }
func (jsonArray) isScalar() bool  { return false }
func (jsonObject) isScalar() bool { return false }
