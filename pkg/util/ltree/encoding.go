// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// EncodeLTree encodes a ltree into a serialized representation
// that's suitable for on-disk storage.
//
// ltree:
//
//	uint16 number of labels
//	for each label:
//		bytes		label with escaped bytes
//
// This encoding is currently used for value-side encoding of LTREE columns.
func EncodeLTree(appendTo []byte, l LTree) ([]byte, error) {
	numLabels := uint16(len(l.Path))
	appendTo = encoding.EncodeUint16Ascending(appendTo, numLabels)
	for _, label := range l.Path {
		appendTo = encoding.EncodeBytesAscending(appendTo, []byte(label))
	}
	return appendTo, nil
}

// DecodeLTree decodes a ltree from the input byte slice.
// See: EncodeLTree
func DecodeLTree(b []byte) (LTree, error) {
	var l LTree
	var labels uint16
	b, labels, err := encoding.DecodeUint16Ascending(b)
	if err != nil {
		return LTree{}, err
	}

	for i := uint16(0); i < labels; i++ {
		var label []byte
		b, label, err = encoding.DecodeBytesAscending(b, label)
		if err != nil {
			return LTree{}, err
		}
		l.Path = append(l.Path, Label(label))
	}
	return l, nil
}

// EncodeLTreeBinary encodes a ltree into a serialized representation
// that's identical to Postgres's wire protocol representation.
//
// PG's binary encoding for ltree is a version byte and a dot-separated list of labels in their byte-form.
// ltree:
//
//		uint8 encoding version byte
//	  bytes byte-form of labels separated by dots
//
// See encodings.json for examples.
func EncodeLTreeBinary(appendTo []byte, l LTree) ([]byte, error) {
	// To match PG's binary encoding, we also use '1' as a version byte.
	appendTo = encoding.EncodeUint8Ascending(appendTo, 1)
	appendTo = encoding.EncodeBytesAscending(appendTo, []byte(l.String()))
	return appendTo, nil
}

// DecodeLTreeBinary decodes a ltree from the input byte slice.
// See: EncodeLTreeBinary
func DecodeLTreeBinary(b []byte) (LTree, error) {
	b, version, err := encoding.DecodeUint8Ascending(b)
	if err != nil {
		return LTree{}, err
	}
	if version != 1 {
		return LTree{}, pgerror.Newf(pgcode.Syntax, "unsupported ltree encoding version %d", version)
	}
	b, labelsBytes, err := encoding.DecodeBytesAscending(b, nil)
	return ParseLTree(string(labelsBytes))
}
