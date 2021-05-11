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

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
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

const jsonEncodedSize = unsafe.Sizeof(jsonEncoded{})

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
		// Manually set the capacity of the new slice to its length, so we properly
		// report the memory size of this encoded json object. The original slice
		// capacity is very large, since it probably points to the backing byte
		// slice of a kv batch.
		value: v[:len(v):len(v)],
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
		entry, err := getUint32At(v, containerHeaderLen)
		if err != nil {
			return v, 0, err
		}
		switch entry & jEntryTypeMask {
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
	return nil, 0, errors.AssertionFailedf("unknown json type %d", errors.Safe(typeTag))
}

func newEncoded(e jEntry, v []byte) (JSON, error) {
	var typ Type
	var containerLen int
	switch e.typCode {
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
		return 0, errors.AssertionFailedf(
			"insufficient bytes to decode uint32 int value: %+v", v)
	}

	return uint32(v[idx])<<24 |
		uint32(v[idx+1])<<16 |
		uint32(v[idx+2])<<8 |
		uint32(v[idx+3]), nil
}

type encodedArrayIterator struct {
	curDataIdx int
	dataOffset int
	idx        int
	len        int
	data       []byte
}

func (e *encodedArrayIterator) nextEncoded() (nextJEntry jEntry, next []byte, ok bool, err error) {
	if e.idx >= e.len {
		return jEntry{}, nil, false, nil
	}

	// Recall the layout of an encoded array:
	// [ container header ] [ all JEntries ] [ all values ]
	nextJEntry, err = getJEntryAt(e.data, containerHeaderLen+e.idx*jEntryLen, e.curDataIdx)
	if err != nil {
		return jEntry{}, nil, false, err
	}
	nextLen := int(nextJEntry.length)
	nextData := e.data[e.dataOffset+e.curDataIdx : e.dataOffset+e.curDataIdx+nextLen]
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
		dataOffset: containerHeaderLen + j.containerLen*jEntryLen,
		curDataIdx: 0,
		len:        j.containerLen,
		idx:        0,
		data:       j.value,
	}
}

type encodedObjectIterator struct {
	curKeyIdx   int
	keyOffset   int
	curValueIdx int
	valueOffset int
	idx         int
	len         int
	data        []byte
}

func (e *encodedObjectIterator) nextEncoded() (
	nextKey []byte,
	nextJEntry jEntry,
	nextVal []byte,
	ok bool,
	err error,
) {
	if e.idx >= e.len {
		return nil, jEntry{}, nil, false, nil
	}

	// Recall the layout of an encoded object:
	// [ container header ] [ all key JEntries ] [ all value JEntries ] [ all key data ] [ all value data ].
	nextKeyJEntry, err := getJEntryAt(e.data, containerHeaderLen+e.idx*jEntryLen, e.curKeyIdx)
	if err != nil {
		return nil, jEntry{}, nil, false, err
	}
	nextKeyData := e.data[e.keyOffset+e.curKeyIdx : e.keyOffset+e.curKeyIdx+int(nextKeyJEntry.length)]

	offsetFromBeginningOfData := e.curValueIdx + e.valueOffset - e.keyOffset

	nextValueJEntry, err := getJEntryAt(e.data, containerHeaderLen+(e.idx+e.len)*jEntryLen, offsetFromBeginningOfData)
	if err != nil {
		return nil, jEntry{}, nil, false, err
	}
	nextValueData := e.data[e.valueOffset+e.curValueIdx : e.valueOffset+e.curValueIdx+int(nextValueJEntry.length)]

	e.idx++
	e.curKeyIdx += int(nextKeyJEntry.length)
	e.curValueIdx += int(nextValueJEntry.length)
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
		entry, err := getJEntryAt(j.value, containerHeaderLen+i*jEntryLen, curValueIdx-curKeyIdx)
		if err != nil {
			return encodedObjectIterator{}, err
		}
		curValueIdx += int(entry.length)
	}

	return encodedObjectIterator{
		curKeyIdx:   0,
		keyOffset:   curKeyIdx,
		curValueIdx: 0,
		valueOffset: curValueIdx,
		len:         j.containerLen,
		idx:         0,
		data:        j.value,
	}, nil
}

func (j *jsonEncoded) StripNulls() (JSON, bool, error) {
	dec, err := j.shallowDecode()
	if err != nil {
		return nil, false, err
	}
	return dec.StripNulls()
}

func (j *jsonEncoded) ObjectIter() (*ObjectIterator, error) {
	dec, err := j.shallowDecode()
	if err != nil {
		return nil, err
	}
	return dec.ObjectIter()
}

func (j *jsonEncoded) nthJEntry(n int, off int) (jEntry, error) {
	return getJEntryAt(j.value, containerHeaderLen+n*jEntryLen, off)
}

// objectGetDataRange returns the [begin, end) subslice of the object's data.
func (j *jsonEncoded) objectGetDataRange(begin, end int) []byte {
	dataStart := containerHeaderLen + j.containerLen*jEntryLen*2
	return j.value[dataStart+begin : dataStart+end]
}

// getNthEntryBounds returns the beginning, ending, and JEntry of the nth entry
// in the container. If the container is an object, the i-th entry is the i-th
// key, and the (i+length)-th entry is the i-th value.
func (j *jsonEncoded) getNthEntryBounds(n int) (begin, end int, entry jEntry, err error) {
	// First, we seek for the beginning of the current entry by stepping
	// backwards via beginningOfIdx.
	begin, err = j.beginningOfIdx(n)
	if err != nil {
		return 0, 0, jEntry{}, err
	}

	// Once we know where this entry starts, we can derive the end from its own
	// JEntry.
	entry, err = j.nthJEntry(n, begin)
	if err != nil {
		return 0, 0, jEntry{}, err
	}
	return begin, begin + int(entry.length), entry, nil
}

// objectGetNthDataRange returns the byte subslice and jEntry of the given nth entry.
// If the container is an object, the i-th entry is the i-th key, and the
// (i+length)-th entry is the i-th value.
func (j *jsonEncoded) objectGetNthDataRange(n int) ([]byte, jEntry, error) {
	begin, end, entry, err := j.getNthEntryBounds(n)
	if err != nil {
		return nil, jEntry{}, err
	}
	return j.objectGetDataRange(begin, end), entry, nil
}

// objectNthValue returns the nth value in the sorted-by-key representation of
// the object.
func (j *jsonEncoded) objectNthValue(n int) (JSON, error) {
	data, entry, err := j.objectGetNthDataRange(j.containerLen + n)
	if err != nil {
		return nil, err
	}
	return newEncoded(entry, data)
}

func parseJEntry(jEntry uint32) (isOff bool, offlen int) {
	return (jEntry & jEntryIsOffFlag) != 0, int(jEntry & jEntryOffLenMask)
}

// beginningOfIdx finds the offset to the beginning of the given entry.
func (j *jsonEncoded) beginningOfIdx(idx int) (int, error) {
	if idx == 0 {
		return 0, nil
	}

	offset := 0
	curIdx := idx - 1
	for curIdx >= 0 {
		// We need to manually extract the JEntry here because this is a case where
		// we logically care if it's an offset or a length.
		e, err := getUint32At(j.value, containerHeaderLen+curIdx*jEntryLen)
		if err != nil {
			return 0, err
		}
		isOff, offlen := parseJEntry(e)
		if isOff {
			return offlen + offset, nil
		}
		offset += offlen
		curIdx--
	}
	return offset, nil
}

func (j *jsonEncoded) arrayGetDataRange(begin, end int) []byte {
	dataStart := containerHeaderLen + j.containerLen*jEntryLen
	return j.value[dataStart+begin : dataStart+end]
}

func (j *jsonEncoded) FetchValIdx(idx int) (JSON, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.FetchValIdx(idx)
	}

	switch j.typ {
	case NumberJSONType, StringJSONType, TrueJSONType, FalseJSONType, NullJSONType:
		return fetchValIdxForScalar(j, idx), nil
	case ArrayJSONType:
		if idx < 0 {
			idx = j.containerLen + idx
		}
		if idx < 0 || idx >= j.containerLen {
			return nil, nil
		}

		// We need to find the bounds for a given index, but this is nontrivial,
		// since some headers store an offset and some store a length.

		begin, end, entry, err := j.getNthEntryBounds(idx)
		if err != nil {
			return nil, err
		}

		return newEncoded(entry, j.arrayGetDataRange(begin, end))
	case ObjectJSONType:
		return nil, nil
	default:
		return nil, errors.AssertionFailedf("unknown json type: %v", errors.Safe(j.typ))
	}
}

func (j *jsonEncoded) FetchValKey(key string) (JSON, error) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.FetchValKey(key)
	}

	if j.Type() == ObjectJSONType {
		// TODO(justin): This is not as absolutely efficient as it could be - every
		// lookup we have to seek to find the actual location of the key. We could
		// be caching the locations of all the intermediate keys that we have to
		// scan in order to get to this one, in case we need to look them up later,
		// or maybe there's something fancier we could do if we know the locations
		// of the offsets by strategically positioning our binary search guesses to
		// land on them.
		var searchErr error
		i := sort.Search(j.containerLen, func(idx int) bool {
			data, _, err := j.objectGetNthDataRange(idx)
			if err != nil {
				searchErr = err
				return false
			}
			return string(data) >= key
		})
		if searchErr != nil {
			return nil, searchErr
		}
		// The sort.Search API implies that we have to double-check if the key we
		// landed on is the one we were searching for in the first place.
		if i >= j.containerLen {
			return nil, nil
		}

		data, _, err := j.objectGetNthDataRange(i)
		if err != nil {
			return nil, err
		}

		if string(data) == key {
			return j.objectNthValue(i)
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
			entry, next, _, err := iter.nextEncoded()
			if err != nil {
				return nil, err
			}
			result[i], err = newEncoded(entry, next)
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
			nextKey, entry, nextValue, _, err := iter.nextEncoded()
			if err != nil {
				return nil, err
			}
			v, err := newEncoded(entry, nextValue)
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
		return nil, errors.AssertionFailedf("unknown json type: %v", errors.Safe(j.typ))
	}
}

func (j *jsonEncoded) mustDecode() JSON {
	decoded, err := j.shallowDecode()
	if err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "invalid JSON data: %v", j.value))
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

func (j *jsonEncoded) AsDecimal() (*apd.Decimal, bool) {
	if dec := j.alreadyDecoded(); dec != nil {
		return dec.AsDecimal()
	}

	decoded, err := j.decode()
	if err != nil {
		return nil, false
	}
	return decoded.AsDecimal()
}

func (j *jsonEncoded) Compare(other JSON) (int, error) {
	if other == nil {
		return -1, nil
	}
	if cmp := cmpJSONTypes(j.Type(), other.Type()); cmp != 0 {
		return cmp, nil
	}
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
	default:
		s, err := j.decode()
		if err != nil {
			return false, err
		}
		return s.Exists(key)
	}
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
			return nil, nil //nolint:returnerrcheck
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
func (j *jsonEncoded) RemoveIndex(idx int) (JSON, bool, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, false, err
	}
	return decoded.RemoveIndex(idx)
}

// Concat implements the JSON interface.
func (j *jsonEncoded) Concat(other JSON) (JSON, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, err
	}
	return decoded.Concat(other)
}

// RemoveString implements the JSON interface.
func (j *jsonEncoded) RemoveString(s string) (JSON, bool, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, false, err
	}
	return decoded.RemoveString(s)
}

func (j *jsonEncoded) RemovePath(path []string) (JSON, bool, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, false, err
	}
	return decoded.RemovePath(path)
}

func (j *jsonEncoded) doRemovePath(path []string) (JSON, bool, error) {
	decoded, err := j.shallowDecode()
	if err != nil {
		return nil, false, err
	}
	return decoded.doRemovePath(path)
}

// Size implements the JSON interface.
func (j *jsonEncoded) Size() uintptr {
	return jsonEncodedSize + uintptr(cap(j.value))
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

func (j *jsonEncoded) Len() int {
	if j.typ != ArrayJSONType && j.typ != ObjectJSONType {
		return 0
	}
	return j.containerLen
}

// EncodeInvertedIndexKeys implements the JSON interface.
func (j *jsonEncoded) encodeInvertedIndexKeys(b []byte) ([][]byte, error) {
	// TODO(justin): this could possibly be optimized.
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.encodeInvertedIndexKeys(b)
}

func (j *jsonEncoded) encodeContainingInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.encodeContainingInvertedIndexSpans(b, isRoot, isObjectValue)
}

func (j *jsonEncoded) encodeContainedInvertedIndexSpans(
	b []byte, isRoot, isObjectValue bool,
) (inverted.Expression, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.encodeContainedInvertedIndexSpans(b, isRoot, isObjectValue)
}

// numInvertedIndexEntries implements the JSON interface.
func (j *jsonEncoded) numInvertedIndexEntries() (int, error) {
	if j.isScalar() || j.containerLen == 0 {
		return 1, nil
	}
	decoded, err := j.decode()
	if err != nil {
		return 0, err
	}
	return decoded.numInvertedIndexEntries()
}

func (j *jsonEncoded) allPaths() ([]JSON, error) {
	decoded, err := j.decode()
	if err != nil {
		return nil, err
	}
	return decoded.allPaths()
}

// HasContainerLeaf implements the JSON interface.
func (j *jsonEncoded) HasContainerLeaf() (bool, error) {
	decoded, err := j.decode()
	if err != nil {
		return false, err
	}
	return decoded.HasContainerLeaf()
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
func (j *jsonEncoded) jEntry() jEntry {
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
	return jEntry{typeTag, byteLen}
}

// encode implements the JSON interface.
func (j *jsonEncoded) encode(appendTo []byte) (jEntry jEntry, b []byte, err error) {
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

// tryDecode implements the JSON interface.
func (j *jsonEncoded) tryDecode() (JSON, error) {
	return j.shallowDecode()
}
