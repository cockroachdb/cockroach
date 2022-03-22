// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keysbase

// KeyMax is a maximum key value which sorts after all other keys.
var KeyMax = []byte{0xff, 0xff}

// PrefixEnd determines the end key given b as a prefix, that is the key that
// sorts precisely behind all keys starting with prefix: "1" is added to the
// final byte and the carry propagated. The special cases of nil and KeyMin
// always returns KeyMax.
func PrefixEnd(b []byte) []byte {
	if len(b) == 0 {
		return KeyMax
	}
	// Switched to "make and copy" pattern in #4963 for performance.
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	// This statement will only be reached if the key is already a maximal byte
	// string (i.e. already \xff...).
	return b
}
