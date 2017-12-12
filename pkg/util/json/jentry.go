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
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

const nullTag = 0x00000000
const stringTag = 0x10000000
const numberTag = 0x20000000
const falseTag = 0x30000000
const trueTag = 0x40000000
const containerTag = 0x50000000

const jEntryIsOffFlag = 0x80000000
const jEntryTypeMask = 0x70000000
const jEntryOffLenMask = 0x0FFFFFFF

// jEntry is a header for a particular JSON value. See the JSONB encoding RFC
// for an explanation of its purpose and format:
// https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20171005_jsonb_encoding.md
type jEntry struct {
	typCode uint32
	length  uint32
}

type encodingType int

const (
	lengthEncoding encodingType = iota
	offsetEncoding
)

// encodingMode specifies the context in which a JEntry is to be encoded.
type encodingMode struct {
	typ encodingType
	// offset is the offset in the current container which we will be encoding
	// this JEntry. Only relevant when typ == offsetEncoding.
	offset uint32
}

var lengthMode = encodingMode{typ: lengthEncoding}

func offsetEncode(offset uint32) encodingMode {
	return encodingMode{typ: offsetEncoding, offset: offset}
}

var nullJEntry = jEntry{nullTag, 0}
var trueJEntry = jEntry{trueTag, 0}
var falseJEntry = jEntry{falseTag, 0}

func makeStringJEntry(length int) jEntry {
	return jEntry{stringTag, uint32(length)}
}

func makeNumberJEntry(length int) jEntry {
	return jEntry{numberTag, uint32(length)}
}

func makeContainerJEntry(length int) jEntry {
	return jEntry{containerTag, uint32(length)}
}

// encoded returns the encoded form of the jEntry.
func (e jEntry) encoded(mode encodingMode) uint32 {
	switch mode.typ {
	case lengthEncoding:
		return e.typCode | e.length
	case offsetEncoding:
		return jEntryIsOffFlag | e.typCode | mode.offset
	}
	return 0
}

func getJEntryAt(b []byte, idx int, offset int) (jEntry, error) {
	enc, err := getUint32At(b, idx)
	if err != nil {
		return jEntry{}, err
	}
	length := enc & jEntryOffLenMask
	if (enc & jEntryIsOffFlag) != 0 {
		length -= uint32(offset)
	}
	return jEntry{
		length:  length,
		typCode: enc & jEntryTypeMask,
	}, nil
}

// decodeJEntry decodes a 4-byte JEntry from a buffer. The current offset is
// required because a JEntry can either encode a length or an offset, and while
// a length can be interpreted locally, the current decoding offset is required
// in order to interpret the encoded offset.
func decodeJEntry(b []byte, offset uint32) ([]byte, jEntry, error) {
	b, encoded, err := encoding.DecodeUint32Ascending(b)
	if err != nil {
		return b, jEntry{}, err
	}

	length := encoded & jEntryOffLenMask

	isOff := (encoded & jEntryIsOffFlag) != 0
	if isOff {
		length -= offset
	}

	return b, jEntry{
		typCode: encoded & jEntryTypeMask,
		length:  length,
	}, nil
}
