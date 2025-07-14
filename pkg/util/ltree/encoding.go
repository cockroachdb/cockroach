// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import "github.com/cockroachdb/cockroach/pkg/util/encoding"

// EncodeLTree encodes a ltree into a serialized representation
// that's suitable for on-disk storage.
// This encoding is currently used for value-side encoding of LTREE columns.
func EncodeLTree(appendTo []byte, l LTree) ([]byte, error) {
	appendTo = encoding.EncodeUntaggedBytesValue(appendTo, encoding.UnsafeConvertStringToBytes(l.String()))
	return appendTo, nil
}

// DecodeLTree decodes a ltree from the input byte slice.
// See: EncodeLTree
func DecodeLTree(b []byte) (LTree, error) {
	var l LTree
	_, pathBytes, err := encoding.DecodeUntaggedBytesValue(b)
	if err != nil {
		return l, err
	}
	l, err = ParseLTree(string(pathBytes))
	if err != nil {
		return l, err
	}
	return l, nil
}
