// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package json

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

func (j *jsonEncoded) Format(buf *bytes.Buffer) {
	// We don't know exactly how big the JSON string will be when decoded and
	// formatted, but we can make a reasonable guess based on the size of the
	// encoded data. This can avoid some buffer reallocations in the common case.
	buf.Grow(len(j.value))
	preLen := buf.Len()
	err := j.format(buf)
	if err != nil {
		buf.Truncate(preLen)
		fmt.Fprintf(buf, `<corrupt JSON data: %s>`, err.Error())
	}
}

func (j *jsonEncoded) format(buf *bytes.Buffer) error {
	switch j.typ {
	case NumberJSONType:
		_, d, err := decodeJSONNumber(j.value)
		if err != nil {
			return err
		}
		d.Format(buf)
	case StringJSONType:
		s := unsafeJSONString(j.value)
		s.Format(buf)
	case TrueJSONType:
		TrueJSONValue.Format(buf)
	case FalseJSONType:
		FalseJSONValue.Format(buf)
	case NullJSONType:
		NullJSONValue.Format(buf)
	default:
		_, err := formatEncodedJSON(buf, j.value)
		if err != nil {
			return err
		}
	}
	return nil
}

func formatEncodedJSON(buf *bytes.Buffer, b []byte) ([]byte, error) {
	b, containerHeader, err := encoding.DecodeUint32Ascending(b)
	if err != nil {
		return nil, err
	}
	switch containerHeader & containerHeaderTypeMask {
	case scalarContainerTag:
		var entry jEntry
		var err error
		b, entry, err = decodeJEntry(b, 0)
		if err != nil {
			return nil, err
		}
		return formatEncodedJSONValue(buf, entry, b)
	case arrayContainerTag:
		return formatEncodedJSONArray(buf, containerHeader, b)
	case objectContainerTag:
		return formatEncodedJSONObject(buf, containerHeader, b)
	default:
		return nil, errors.AssertionFailedf(
			"error decoding JSON value, header: %x", errors.Safe(containerHeader))
	}
}

func formatEncodedJSONArray(buf *bytes.Buffer, containerHeader uint32, b []byte) ([]byte, error) {
	length := int(containerHeader & containerHeaderLenMask)

	// There are `length` JEntries in the array.
	entOff, valOff := uint32(0), uint32(0)
	entB, valB := b, b
	// Skip past the JEntries to find the first value, permitting parallel
	// iteration through the JEntries and values below.
	var err error
	for i := 0; i < length; i++ {
		// Decode and ignore.
		var ent jEntry
		valB, ent, err = decodeJEntry(valB, valOff)
		if err != nil {
			return nil, err
		}
		valOff += ent.length
	}

	buf.WriteByte('[')
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		var ent jEntry
		var err error
		entB, ent, err = decodeJEntry(entB, entOff)
		if err != nil {
			return nil, err
		}
		entOff += ent.length
		valB, err = formatEncodedJSONValue(buf, ent, valB)
		if err != nil {
			return nil, err
		}
	}
	buf.WriteByte(']')
	return valB, nil
}

func formatEncodedJSONObject(buf *bytes.Buffer, containerHeader uint32, b []byte) ([]byte, error) {
	length := int(containerHeader & containerHeaderLenMask)

	// There are `length` key JEntries and then `length` value JEntries in the
	// object.
	keyOff, valOff := uint32(0), uint32(0)
	keyEntB, valEntB := b, b
	// Skip past the key JEntries to find the first value JEntry, permitting
	// parallel iteration through the key JEntries, value JEntries, keys, and
	// values below.
	var err error
	for i := 0; i < length; i++ {
		// Decode (and ignore) key JEntry.
		var keyEnt jEntry
		valEntB, keyEnt, err = decodeJEntry(valEntB, valOff)
		if err != nil {
			return nil, err
		}
		if keyEnt.typCode != stringTag {
			return nil, errors.AssertionFailedf(
				"key encoded as non-string: %d", errors.Safe(keyEnt.typCode))
		}
		valOff += keyEnt.length
	}
	// Skip past the value JEntries to find the first key.
	valEntsSize := jEntrySize * length
	if len(valEntB) < valEntsSize {
		return nil, errors.AssertionFailedf("insufficient data to skip JSON object value JEntries")
	}
	keyB := valEntB[valEntsSize:]
	// Skip past the keys to find the first value.
	if len(keyB) < int(valOff) {
		return nil, errors.AssertionFailedf("insufficient data to skip JSON object keys")
	}
	valB := keyB[valOff:]

	// Iterate through the keys and values in parallel, formatting them.
	buf.WriteByte('{')
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		// Decode key JEntry i.
		var keyEnt jEntry
		keyEntB, keyEnt, err = decodeJEntry(keyEntB, keyOff)
		if err != nil {
			return nil, err
		}
		keyOff += keyEnt.length
		// Decode value JEntry i.
		var valEnt jEntry
		valEntB, valEnt, err = decodeJEntry(valEntB, valOff)
		if err != nil {
			return nil, err
		}
		valOff += valEnt.length
		// Decode and format key i.
		keyS := unsafeJSONString(keyB[:keyEnt.length])
		keyS.Format(buf)
		keyB = keyB[keyEnt.length:]
		buf.WriteString(": ")
		// Decode and format value i.
		valB, err = formatEncodedJSONValue(buf, valEnt, valB)
		if err != nil {
			return nil, err
		}
	}
	buf.WriteByte('}')
	return valB, nil
}

func formatEncodedJSONValue(buf *bytes.Buffer, e jEntry, b []byte) ([]byte, error) {
	switch e.typCode {
	case trueTag:
		TrueJSONValue.Format(buf)
	case falseTag:
		FalseJSONValue.Format(buf)
	case nullTag:
		NullJSONValue.Format(buf)
	case stringTag:
		s := unsafeJSONString(b[:e.length])
		s.Format(buf)
		b = b[e.length:]
	case numberTag:
		var d jsonNumber
		var err error
		b, d, err = decodeJSONNumber(b)
		if err != nil {
			return nil, err
		}
		d.Format(buf)
	case containerTag:
		return formatEncodedJSON(buf, b)
	default:
		return nil, errors.AssertionFailedf(
			"error decoding JSON value, unexpected type code: %d", errors.Safe(e.typCode))
	}
	return b, nil
}

func unsafeJSONString(b []byte) jsonString {
	return jsonString(encoding.UnsafeConvertBytesToString(b))
}
