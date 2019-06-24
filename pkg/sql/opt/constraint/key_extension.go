// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package constraint

// KeyExtension is an enumeration used when comparing keys. Key extensions
// specify whether each key is conceptually suffixed with a value that sorts
// before all values (ExtendLow) or with a value that sorts after all values
// (ExtendHigh). This allows span boundaries to be compared with one another.
// See the comment for the Key.Compare method for more details.
type KeyExtension bool

const (
	// ExtendLow specifies that the key is conceptually suffixed with a value
	// that sorts before all values for purposes of comparison.
	ExtendLow KeyExtension = false

	// ExtendHigh specifies that the key is conceptually suffixed with a value
	// that sorts after all values for purposes of comparison.
	ExtendHigh KeyExtension = true
)

// ToCmp converts from a key extension value to a comparison value. ExtendLow
// maps to -1 because it sorts before all other values. ExtendHigh maps to 1
// because it sorts after all other values.
func (e KeyExtension) ToCmp() int {
	// Map ExtendLow into -1 and ExtendHigh into +1.
	if e == ExtendLow {
		return -1
	}
	return 1
}
