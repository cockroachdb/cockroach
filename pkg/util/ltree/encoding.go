// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import "github.com/cockroachdb/cockroach/pkg/util/encoding"

// EncodeLTree encodes LTREE for on-disk storage
func EncodeLTree(appendTo []byte, l LTree) ([]byte, error) {
	appendTo = encoding.EncodeUint16Ascending(appendTo, uint16(l.Len()))
	for _, label := range l.Path {
		appendTo = encoding.EncodeBytesAscending(appendTo, []byte(label))
	}
	return appendTo, nil
}

// DecodeLTree decodes LTREE from on-disk storage
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
// that's similar to Postgres's wire protocol representation.
//
// ltree:
//
//	uint16 number of labels
//	for each label:
//		/* length of label isn't included in this implementation, instead we use escaped bytes */
//		uint16	length of label
//		bytes		label
func EncodeLTreeBinary(appendTo []byte, l LTree) ([]byte, error) {
	appendTo = encoding.EncodeUint16Ascending(appendTo, uint16(l.Len()))
	for _, label := range l.Path {
		appendTo = encoding.EncodeBytesAscending(appendTo, []byte(label))
	}
	return appendTo, nil
}

// DecodeLTreeBinary decodes a ltree from the input byte slice which is
// formatted in Postgres binary protocol.
func DecodeLTreeBinary(b []byte) (LTree, error) {
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
