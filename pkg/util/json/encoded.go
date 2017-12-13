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
	"fmt"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type jsonEncoded struct {
	// containerLength is only set if this is an object or an array.
	containerLen int
	typ          Type
	// value contains the encoding of this JSON value. In the case of
	// arrays and objects, value contains the container header, but it never
	// contains a scalar container header.
	value []byte

	// TODO(justin): for simplicity right now we use a mutex, we could be using
	// an atomic CAS though.
	mu struct {
		syncutil.RWMutex

		cachedDecoded JSON
	}
}

// alreadyDecoded returns a decoded JSON value if this jsonEncoded has already
// been decoded, otherwise it returns nil. This allows us to fast-path certain
// operations if we've already done the work of decoding an object.
func (j *jsonEncoded) alreadyDecoded() JSON {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if j.mu.cachedDecoded != nil {
		return j.mu.cachedDecoded
	}
	return nil
}

func (j *jsonEncoded) Type() Type {
	return j.typ
}

// newEncodedFromRoot returns a jsonEncoded from a fully-encoded JSON document.
func newEncodedFromRoot(v []byte) (*jsonEncoded, error) {
	v, typ, err := jsonTypeFromRootBuffer(v)
	if err != nil {
		return nil, err
	}

	containerLen := -1
	if typ == ArrayJSONType || typ == ObjectJSONType {
		containerHeader, err := getUint32At(v, 0)
		if err != nil {
			return nil, err
		}
		containerLen = int(containerHeader & containerHeaderLenMask)
	}

	return &jsonEncoded{
		typ:          typ,
		containerLen: containerLen,
		value:        v,
	}, nil
}

func jsonTypeFromRootBuffer(v []byte) ([]byte, Type, error) {
	// Root buffers always have a container header.
	containerHeader, err := getUint32At(v, 0)
	if err != nil {
		return v, 0, err
	}
	typeTag := containerHeader & containerHeaderTypeMask
	switch typeTag {
	case arrayContainerTag:
		return v, ArrayJSONType, nil
	case objectContainerTag:
		return v, ObjectJSONType, nil
	case scalarContainerTag:
		jEntry, err := getUint32At(v, containerHeaderLen)
		if err != nil {
			return v, 0, err
		}
		switch jEntry & jEntryTypeMask {
		case nullTag:
			return v[containerHeaderLen+jEntryLen:], NullJSONType, nil
		case trueTag:
			return v[containerHeaderLen+jEntryLen:], TrueJSONType, nil
		case falseTag:
			return v[containerHeaderLen+jEntryLen:], FalseJSONType, nil
		case numberTag:
			return v[containerHeaderLen+jEntryLen:], NumberJSONType, nil
		case stringTag:
			return v[containerHeaderLen+jEntryLen:], StringJSONType, nil
		}
	}
	return nil, 0, pgerror.NewErrorf(pgerror.CodeInternalError, "unknown type %d", typeTag)
}

func newEncoded(jEntry uint32, v []byte) (JSON, error) {
	var typ Type
	var containerLen int
	switch jEntry & jEntryTypeMask {
	case stringTag:
		typ = StringJSONType
	case numberTag:
		typ = NumberJSONType
	case nullTag: // Don't bother with returning a jsonEncoded for the singleton types.
		return NullJSONValue, nil
	case falseTag:
		return FalseJSONValue, nil
	case trueTag:
		return TrueJSONValue, nil
	case containerTag:
		// Every container is prefixed with its uint32 container header.
		containerHeader, err := getUint32At(v, 0)
		if err != nil {
			return nil, err
		}
		switch containerHeader & containerHeaderTypeMask {
		case arrayContainerTag:
			typ = ArrayJSONType
		case objectContainerTag:
			typ = ObjectJSONType
		}
		containerLen = int(containerHeader & containerHeaderLenMask)
	}

	return &jsonEncoded{
		typ:          typ,
		containerLen: containerLen,
		value:        v,
	}, nil
}

func getUint32At(v []byte, idx int) (uint32, error) {
	if idx+4 > len(v) {
		return 0, pgerror.NewError(pgerror.CodeInternalError, "insufficient bytes to decode uint32 int value")
	}

	return uint32(v[idx])<<24 |
		uint32(v[idx+1])<<16 |
		uint32(v[idx+2])<<8 |
		uint32(v[idx+3]), nil
}

type encodedArrayIterator struct {
	curDataIdx int
	idx        int
	len        int
	data       []byte
}

func (e *encodedArrayIterator) nextEncoded() (nextJEntry uint32, next []byte, ok bool, err error) {
	if e.idx >= e.len {
		return 0, nil, false, nil
	}

	// Recall the layout of an encoded array:
	// [ container header ] [ all JEntries ] [ all values ]
	nextJEntry, err = getUint32At(e.data, containerHeaderLen+e.idx*jEntryLen)
	if err != nil {
		return 0, nil, false, err
	}
	nextLen := int(nextJEntry & jEntryOffLenMask)
	nextData := e.data[e.curDataIdx : e.curDataIdx+nextLen]
	e.idx++
	e.curDataIdx += nextLen
	return nextJEntry, nextData, true, nil
}

// iterArrayValues iterates through all the values of an encoded array without
// requiring decoding of all of them.
func (j *jsonEncoded) iterArrayValues() encodedArrayIterator {
	if j.typ != ArrayJSONType {
		panic("can only iterate through the array values of an array")
	}

	return encodedArrayIterator{
		curDataIdx: containerHeaderLen + j.containerLen*jEntryLen,
		len:        j.containerLen,
		idx:        0,
		data:       j.value,
	}
}

type encodedObjectIterator struct {
	curKeyIdx   int
	curValueIdx int
	idx         int
	len         int
	data        []byte
}

func (e *encodedObjectIterator) nextEncoded() (nextKey []byte, nextJEntry uint32, nextVal []byte, ok bool, err error) {
	if e.idx >= e.len {
		return nil, 0, nil, false, nil
	}

	// Recall the layout of an encoded object:
	// [ container header ] [ all key JEntries ] [ all value JEntries ] [ all key data ] [ all value data ].
	nextKeyJEntry, err := getUint32At(e.data, containerHeaderLen+e.idx*jEntryLen)
	if err != nil {
		return nil, 0, nil, false, err
	}
	nextKeyLen := int(nextKeyJEntry & jEntryOffLenMask)
	nextKeyData := e.data[e.curKeyIdx : e.curKeyIdx+nextKeyLen]

	nextValueJEntry, err := getUint32At(e.data, containerHeaderLen+(e.idx+e.len)*jEntryLen)
	if err != nil {
		return nil, 0, nil, false, err
	}
	nextValueLen := int(nextValueJEntry & jEntryOffLenMask)
	nextValueData := e.data[e.curValueIdx : e.curValueIdx+nextValueLen]

	e.idx++
	e.curKeyIdx += nextKeyLen
	e.curValueIdx += nextValueLen
	return nextKeyData, nextValueJEntry, nextValueData, true, nil
}

// iterObject iterates through all the keys and values of an encoded object
// without requiring decoding of all of them.
func (j *jsonEncoded) iterObject() (encodedObjectIterator, error) {
	if j.typ != ObjectJSONType {
		panic("can only iterate through the object values of an object")
	}

	curKeyIdx := containerHeaderLen + j.containerLen*jEntryLen*2
	curValueIdx := curKeyIdx

	// We have to seek to the start of the value data.
	for i := 0; i < j.containerLen; i++ {
		jEntry, err := getUint32At(j.value, containerHeaderLen+i*jEntryLen)
		if err != nil {
			return encodedObjectIterator{}, err
		}
		nextLen := int(jEntry & jEntryOffLenMask)
		curValueIdx += nextLen
	}

	return encodedObjectIterator{
		curKeyIdx:   curKeyIdx,
		curValueIdx: curValueIdx,
		len:         j.containerLen,
		idx:         0,
		data:        j.value,
	}, nil
}

func (j *jsonEncoded) IterObjectKey() (*ObjectKeyIterator, error) {
	dec, err := j.shallowDecode()
	if err != nil {
		return nil, err
	}
	return dec.IterObjectKey()
}

func (j *jsonEncoded) FetchValIdx(idx int) (JSON, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.FetchValIdx(idx)
	}

	if j.Type() == ArrayJSONType {
		if idx < 0 {
			idx = j.containerLen + idx
		}
		if idx < 0 || idx >= j.containerLen {
			return nil, nil
		}
		iter := j.iterArrayValues()
		resultJEntry, resultData, _, err := iter.nextEncoded()
		if err != nil {
			return nil, err
		}
		for i := 0; i < idx; i++ {
			resultJEntry, resultData, _, err = iter.nextEncoded()
			if err != nil {
				return nil, err
			}
		}
		return newEncoded(resultJEntry, resultData)
	}
	return nil, nil
}

// TODO(justin): this is quite slow as it requires a linear scan - implement
// the length/offset distinction in order to make this fast.
func (j *jsonEncoded) FetchValKey(key string) (JSON, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.FetchValKey(key)
	}

	if j.Type() == ObjectJSONType {
		iter, err := j.iterObject()
		if err != nil {
			return nil, err
		}
		for {
			nextKey, jEntry, nextValue, ok, err := iter.nextEncoded()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, nil
			}
			if string(nextKey) == key {
				next, err := newEncoded(jEntry, nextValue)
				if err != nil {
					return nil, err
				}
				return next, nil
			}
		}
	}
	return nil, nil
}

// shallowDecode decodes only the keys of an object, and doesn't decode any
// elements of an array. It can be used to save a decode-encode cycle for
// certain operations (say, key deletion).
func (j *jsonEncoded) shallowDecode() (JSON, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec, nil
	}

	switch j.typ {
	case NumberJSONType, StringJSONType, TrueJSONType, FalseJSONType, NullJSONType:
		return j.decode()
	case ArrayJSONType:
		iter := j.iterArrayValues()
		result := make(jsonArray, j.containerLen)
		for i := 0; i < j.containerLen; i++ {
			jEntry, next, _, err := iter.nextEncoded()
			if err != nil {
				return nil, err
			}
			result[i], err = newEncoded(jEntry, next)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case ObjectJSONType:
		iter, err := j.iterObject()
		if err != nil {
			return nil, err
		}
		result := make(jsonObject, j.containerLen)
		for i := 0; i < j.containerLen; i++ {
			nextKey, jEntry, nextValue, _, err := iter.nextEncoded()
			if err != nil {
				return nil, err
			}
			v, err := newEncoded(jEntry, nextValue)
			if err != nil {
				return nil, err
			}
			result[i] = jsonKeyValuePair{
				k: jsonString(nextKey),
				v: v,
			}
		}
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.mu.cachedDecoded == nil {
			j.mu.cachedDecoded = result
		}
		return result, nil
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unknown type %v", j.typ)
	}
}

func (j *jsonEncoded) mustDecode() JSON {
	decoded, err := j.decode()
	if err != nil {
		panic(fmt.Sprintf("invalid JSON data: %s, %v", err.Error(), j.value))
	}
	return decoded
}

// decode should be used in cases where you will definitely have to use the
// entire decoded JSON structure, like printing it out to a string.
func (j *jsonEncoded) decode() (JSON, error) {
	switch j.typ {
	case NumberJSONType:
		_, j, err := decodeJSONNumber(j.value)
		return j, err
	case StringJSONType:
		return jsonString(j.value), nil
	case TrueJSONType:
		return TrueJSONValue, nil
	case FalseJSONType:
		return FalseJSONValue, nil
	case NullJSONType:
		return NullJSONValue, nil
	}
	_, decoded, err := DecodeJSON(j.value)

	j.mu.Lock()
	defer j.mu.Unlock()
	j.mu.cachedDecoded = decoded

	return decoded, err
}

func (j *jsonEncoded) AsText() (*string, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.AsText()
	}

	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.AsText()
}

func (j *jsonEncoded) Compare(other JSON) (int, error) {
	// TODO(justin): this can be optimized in some cases. We don't necessarily
	// need to decode all of an array or every object key.
	dec, err := j.shallowDecode()
	if err != nil {
		return 0, err
	}
	return dec.Compare(other)
}

func (j *jsonEncoded) Exists(key string) (bool, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.Exists(key)
	}

	switch j.typ {
	case ObjectJSONType:
		v, err := j.FetchValKey(key)
		if err != nil {
			return false, err
		}
		return v != nil, nil
	case ArrayJSONType:
		iter := j.iterArrayValues()
		for {
			nextJEntry, data, ok, err := iter.nextEncoded()
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
			next, err := newEncoded(nextJEntry, data)
			if err != nil {
				return false, err
			}
			// This is a minor optimization - we know that newEncoded always returns a
			// jsonEncoded if it's decoding a string, and we can save actually
			// allocating that string for this check by not forcing a decode into a
			// jsonString.  This operates on two major assumptions:
			// 1. newEncoded returns a jsonEncoded (and not a jsonString) for string
			// types and
			// 2. the `value` field on such a jsonEncoded directly corresponds to the string.
			// This is tested sufficiently that if either of those assumptions is
			// broken it will be caught.
			if next.Type() == StringJSONType && string(next.(*jsonEncoded).value) == key {
				return true, nil
			}
		}
	}
	return false, nil
}

func (j *jsonEncoded) FetchValKeyOrIdx(key string) (JSON, error) {
	switch j.typ {
	case ObjectJSONType:
		return j.FetchValKey(key)
	case ArrayJSONType:
		idx, err := strconv.Atoi(key)
		if err != nil {
			// We shouldn't return this error because it means we couldn't parse the
			// number, meaning it was a string and that just means we can't find the
			// value in an array.
			return nil, nil
		}
		return j.FetchValIdx(idx)
	}
	return nil, nil
}

func (j *jsonEncoded) Format(buf *bytes.Buffer) {
	decoded, err := j.decode()
	if err != nil {
		fmt.Fprintf(buf, `<corrupt JSON data: %s>`, err.Error())
	} else {
		decoded.Format(buf)
	}
}

// RemoveIndex implements the JSON interface.
func (j *jsonEncoded) RemoveIndex(idx int) (JSON, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, err
	}
	return decoded.RemoveIndex(idx)
}

// RemoveKey implements the JSON interface.
func (j *jsonEncoded) RemoveKey(key string) (JSON, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, err
	}
	return decoded.RemoveKey(key)
}

// Size implements the JSON interface.
func (j *jsonEncoded) Size() uintptr {
	return unsafe.Sizeof(j) + uintptr(len(j.value))
}

func (j *jsonEncoded) String() string {
	var buf bytes.Buffer
	j.Format(&buf)
	return buf.String()
}

// isScalar implements the JSON interface.
func (j *jsonEncoded) isScalar() bool {
	return j.typ != ArrayJSONType && j.typ != ObjectJSONType
}

// EncodeInvertedIndexKeys implements the JSON interface.
func (j *jsonEncoded) EncodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	// TODO(justin): this could possibly be optimized.
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.EncodeInvertedIndexKeys(b)
}

// preprocessForContains implements the JSON interface.
func (j *jsonEncoded) preprocessForContains() (containsable, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.preprocessForContains()
	}

	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.preprocessForContains()
}

// jEntry implements the JSON interface.
func (j *jsonEncoded) jEntry() uint32 {
	var typeTag uint32
	switch j.typ {
	case NullJSONType:
		typeTag = nullTag
	case TrueJSONType:
		typeTag = trueTag
	case FalseJSONType:
		typeTag = falseTag
	case StringJSONType:
		typeTag = stringTag
	case NumberJSONType:
		typeTag = numberTag
	case ObjectJSONType, ArrayJSONType:
		typeTag = containerTag
	}
	byteLen := uint32(len(j.value))
	return typeTag | byteLen
}

// encode implements the JSON interface.
func (j *jsonEncoded) encode(appendTo []byte) (jEntry uint32, b []byte, err error) {
	return j.jEntry(), append(appendTo, j.value...), nil
}

// MaybeDecode implements the JSON interface.
func (j *jsonEncoded) MaybeDecode() JSON {
	return j.mustDecode()
}

// toGoRepr implements the JSON interface.
func (j *jsonEncoded) toGoRepr() (interface{}, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, err
	}
	return decoded.toGoRepr()
}
