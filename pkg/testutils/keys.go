// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import "bytes"

// MakeKey makes a new key which is the concatenation of the given inputs, in
// order.
func MakeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}
