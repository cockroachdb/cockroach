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
	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// This file implements the format described in the JSONB encoding RFC.

const nullTag = 0x00000000
const stringTag = 0x10000000
const numberTag = 0x20000000
const falseTag = 0x30000000
const trueTag = 0x40000000
const containerTag = 0x50000000

const jEntryTypeMask = 0x70000000
const jEntryOffLenMask = 0x0FFFFFFF

const arrayContainerTag = 0x80000000
const objectContainerTag = 0x40000000
const scalarContainerTag = 0x20000000

const containerHeaderTypeMask = 0xE0000000
const containerHeaderLenMask = 0x1FFFFFFF

const maxByteLength = int(jEntryOffLenMask)

const containerHeaderLen = 4
const jEntryLen = 4

// checkLength ensures that an encoded value is not too long to fit into the
// JEntry header. This should never come up, since it would require a ~250MB
// JSON value, but check it just to be safe.
func checkLength(length int) error {
	if length > maxByteLength {
		return pgerror.NewError(pgerror.CodeInternalError, "JSON value too large")
	}
	return nil
}

// Note: the encoding of each of null, true, and false are the encoding of length 0.
// Their values are purely dictated by their type.
func (jsonNull) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	return nullTag, appendTo, nil
}

func (jsonTrue) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	return trueTag, appendTo, nil
}

func (jsonFalse) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	return falseTag, appendTo, nil
}

func (j jsonString) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	if err := checkLength(len(j)); err != nil {
		return 0, b, err
	}
	return stringTag | uint32(len(j)), append(appendTo, []byte(j)...), nil
}

func (j jsonNumber) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	decOffset := len(appendTo)
	dec := apd.Decimal(j)
	appendTo = encoding.EncodeUntaggedDecimalValue(appendTo, &dec)
	lengthInBytes := len(appendTo) - decOffset
	if err := checkLength(lengthInBytes); err != nil {
		return 0, b, err
	}
	return numberTag | uint32(lengthInBytes), appendTo, nil
}

func (j jsonArray) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	encodingStartPosition := len(appendTo)
	// Array container header.
	appendTo = encoding.EncodeUint32Ascending(appendTo, arrayContainerTag|uint32(len(j)))
	// Reserve space for the JEntries and store where they start so we can fill them in later.
	jEntryIdx := len(appendTo)
	for i := 0; i < len(j); i++ {
		appendTo = append(appendTo, 0, 0, 0, 0)
	}
	for i := 0; i < len(j); i++ {
		var nextJEntry uint32
		nextJEntry, appendTo, err = j[i].encode(appendTo)
		if err != nil {
			return 0, appendTo, err
		}
		appendTo = encoding.PutUint32Ascending(appendTo, nextJEntry, jEntryIdx+i*4)
	}
	lengthInBytes := len(appendTo) - encodingStartPosition
	if err := checkLength(lengthInBytes); err != nil {
		return 0, b, err
	}
	return containerTag | uint32(lengthInBytes), appendTo, nil
}

func (j jsonObject) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	encodingStartPosition := len(appendTo)
	// Object container header.
	appendTo = encoding.EncodeUint32Ascending(appendTo, objectContainerTag|uint32(len(j)))
	// Reserve space for the key and value JEntries and store where they start so
	// we can fill them in later.
	jEntryIdx := len(appendTo)
	for i := 0; i < len(j)*2; i++ {
		appendTo = append(appendTo, 0, 0, 0, 0)
	}
	// Encode all keys.
	for i := 0; i < len(j); i++ {
		var nextJEntry uint32
		nextJEntry, appendTo, err = j[i].k.encode(appendTo)
		if err != nil {
			return 0, appendTo, err
		}
		appendTo = encoding.PutUint32Ascending(appendTo, nextJEntry, jEntryIdx+i*4)
	}
	// Encode all values.
	for i := 0; i < len(j); i++ {
		var nextJEntry uint32
		nextJEntry, appendTo, err = j[i].v.encode(appendTo)
		if err != nil {
			return 0, appendTo, err
		}
		appendTo = encoding.PutUint32Ascending(appendTo, nextJEntry, jEntryIdx+(len(j)+i)*4)
	}
	lengthInBytes := len(appendTo) - encodingStartPosition
	if err := checkLength(lengthInBytes); err != nil {
		return 0, b, err
	}
	return containerTag | uint32(lengthInBytes), appendTo, nil
}

// EncodeJSON encodes a JSON value as a sequence of bytes.
func EncodeJSON(appendTo []byte, j JSON) ([]byte, error) {
	switch j.Type() {
	case ArrayJSONType, ObjectJSONType:
		// We just discard the JEntry in these cases.
		var err error
		_, appendTo, err = j.encode(appendTo)
		if err != nil {
			return appendTo, err
		}
		return appendTo, nil
	default: // j is a scalar, so we must construct a scalar container for it at the top level.
		// Scalar container header.
		appendTo = encoding.EncodeUint32Ascending(appendTo, scalarContainerTag)
		// Reserve space for scalar jEntry.
		jEntryIdx := len(appendTo)
		appendTo = encoding.EncodeUint32Ascending(appendTo, 0)
		var jEntry uint32
		var err error
		jEntry, appendTo, err = j.encode(appendTo)
		if err != nil {
			return appendTo, err
		}
		appendTo = encoding.PutUint32Ascending(appendTo, jEntry, jEntryIdx)
		return appendTo, nil
	}
}

// DecodeJSON decodes a value encoded with EncodeJSON.
func DecodeJSON(b []byte) ([]byte, JSON, error) {
	b, containerHeader, err := encoding.DecodeUint32Ascending(b)
	if err != nil {
		return b, nil, err
	}
	switch containerHeader & containerHeaderTypeMask {
	case scalarContainerTag:
		var jEntry uint32
		var err error
		b, jEntry, err = encoding.DecodeUint32Ascending(b)
		if err != nil {
			return b, nil, err
		}
		return decodeJSONValue(jEntry, b)
	case arrayContainerTag:
		return decodeJSONArray(containerHeader, b)
	case objectContainerTag:
		return decodeJSONObject(containerHeader, b)
	}
	return b, nil, pgerror.NewError(pgerror.CodeInternalError, "error decoding JSON value")
}

// FromEncoding returns a JSON value which is lazily decoded.
func FromEncoding(b []byte) (JSON, error) {
	return newEncodedFromRoot(b)
}

func decodeJSONArray(containerHeader uint32, b []byte) ([]byte, JSON, error) {
	length := containerHeader & containerHeaderLenMask
	b, jEntries, err := decodeJEntries(length, b)
	if err != nil {
		return b, nil, err
	}
	result := make(jsonArray, length)
	for i := uint32(0); i < length; i++ {
		var nextJSON JSON
		b, nextJSON, err = decodeJSONValue(jEntries[i], b)
		if err != nil {
			return b, nil, err
		}
		result[i] = nextJSON
	}
	return b, result, nil
}

func decodeJEntries(n uint32, b []byte) ([]byte, []uint32, error) {
	var err error
	jEntries := make([]uint32, n)
	for i := uint32(0); i < n; i++ {
		var nextJEntry uint32
		b, nextJEntry, err = encoding.DecodeUint32Ascending(b)
		if err != nil {
			return b, nil, err
		}
		jEntries[i] = nextJEntry
	}
	return b, jEntries, nil
}

func decodeJSONObject(containerHeader uint32, b []byte) ([]byte, JSON, error) {
	length := containerHeader & containerHeaderLenMask

	b, keyJEntries, err := decodeJEntries(length, b)
	if err != nil {
		return b, nil, err
	}

	b, valueJEntries, err := decodeJEntries(length, b)
	if err != nil {
		return b, nil, err
	}

	result := make(jsonObject, length)
	// Decode the keys.
	for i := uint32(0); i < length; i++ {
		var nextJSON JSON
		b, nextJSON, err = decodeJSONValue(keyJEntries[i], b)
		if err != nil {
			return b, nil, err
		}
		if key, ok := nextJSON.(jsonString); ok {
			result[i].k = key
		} else {
			return b, nil, pgerror.NewError(pgerror.CodeInternalError, "key encoded as non-string")
		}
	}

	// Decode the values.
	for i := uint32(0); i < length; i++ {
		var nextJSON JSON
		b, nextJSON, err = decodeJSONValue(valueJEntries[i], b)
		if err != nil {
			return b, nil, err
		}
		result[i].v = nextJSON
	}
	return b, result, nil
}

func decodeJSONNumber(b []byte) ([]byte, JSON, error) {
	b, d, err := encoding.DecodeUntaggedDecimalValue(b)
	if err != nil {
		return b, nil, err
	}
	return b, jsonNumber(d), nil
}

func decodeJSONValue(jEntry uint32, b []byte) ([]byte, JSON, error) {
	switch jEntry & jEntryTypeMask {
	case trueTag:
		return b, TrueJSONValue, nil
	case falseTag:
		return b, FalseJSONValue, nil
	case nullTag:
		return b, NullJSONValue, nil
	case stringTag:
		length := jEntry & jEntryOffLenMask
		return b[length:], jsonString(b[:length]), nil
	case numberTag:
		return decodeJSONNumber(b)
	case containerTag:
		return DecodeJSON(b)
	}
	return b, nil, pgerror.NewError(pgerror.CodeInternalError, "error decoding JSON value")
}
