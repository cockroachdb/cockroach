// Copyright 2018 The Cockroach Authors.
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

package constraint

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

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

// EmptyKey has zero values. If it's a start key, then it sorts before all
// other keys. If it's an end key, then it sorts after all other keys.
var EmptyKey = Key{}

// Key is a composite of N typed datum values that are ordered from most
// significant to least significant for purposes of sorting. The datum values
// correspond to a set of columns; it is the responsibility of the calling code
// to keep track of them.
type Key struct {
	// firstVal stores the first value in the key. Subsequent values are stored
	// in otherVals. Inlining the first value avoids an extra allocation in the
	// common case of a single value in the key.
	firstVal tree.Datum

	// otherVals is an ordered list of typed datum values. It only stores values
	// after the first, and is nil if there is <= 1 value.
	otherVals tree.Datums
}

// MakeKey constructs a simple one dimensional key having the given value. If
// val is nil, then MakeKey returns an empty key.
func MakeKey(val tree.Datum) Key {
	return Key{firstVal: val}
}

// MakeCompositeKey constructs an N-dimensional key from the given slice of
// values.
func MakeCompositeKey(vals ...tree.Datum) Key {
	switch len(vals) {
	case 0:
		return Key{}
	case 1:
		return Key{firstVal: vals[0]}
	}
	return Key{firstVal: vals[0], otherVals: vals[1:]}
}

// IsEmpty is true if this key has zero values.
func (k Key) IsEmpty() bool {
	return k.firstVal == nil
}

// Length returns the number of values in the key. If the count is zero, then
// this is an empty key.
func (k Key) Length() int {
	if k.IsEmpty() {
		return 0
	}
	return 1 + len(k.otherVals)
}

// Value returns the nth value of the key. If the index is out-of-range, then
// Value will panic.
func (k Key) Value(nth int) tree.Datum {
	if k.firstVal != nil && nth == 0 {
		return k.firstVal
	}
	return k.otherVals[nth-1]
}

// Compare returns an integer indicating the ordering of the two keys. The
// result will be 0 if the keys are equal, -1 if this key is less than the
// given key, or 1 if this key is greater.
//
// Comparisons between keys where one key is a prefix of the other have special
// handling which can be controlled via the key extension parameters. Key
// extensions specify whether each key is conceptually suffixed with negative
// or positive infinity for purposes of comparison. For example, if k is /1/2,
// then:
//   k (kExt = ExtendLow) : /1/2/Low
//   k (kExt = ExtendHigh): /1/2/High
//
// These extensions have no effect if one key is not a prefix of the other,
// since the comparison would already have concluded in previous values. But
// if comparison proceeds all the way to the end of one of the keys, then the
// extension determines which key is greater. This enables correct comparison
// of start and end keys in spans which may be either inclusive or exclusive.
// Here is the mapping:
//   [/1/2 - ...] (inclusive start key): ExtendLow : /1/2/Low
//   (/1/2 - ...] (exclusive start key): ExtendHigh: /1/2/High
//   [... - /1/2] (inclusive end key)  : ExtendHigh: /1/2/High
//   [... - /1/2) (exclusive end key)  : ExtendLow : /1/2/Low
func (k Key) Compare(evalCtx *tree.EvalContext, l Key, kExt, lExt KeyExtension) int {
	kLen := k.Length()
	lLen := l.Length()
	for i := 0; i < kLen && i < lLen; i++ {
		if cmp := k.Value(i).Compare(evalCtx, l.Value(i)); cmp != 0 {
			return cmp
		}
	}

	if kLen < lLen {
		// k matches a prefix of l:
		//   k = /1
		//   l = /1/2
		// Which of these is "smaller" depends on whether k is extended with
		// -inf or with +inf:
		//   k (ExtendLow)  = /1/Low  < /1/2  ->  k is smaller (-1)
		//   k (ExtendHigh) = /1/High > /1/2  ->  k is bigger  (1)
		return kExt.ToCmp()
	} else if kLen > lLen {
		// Inverse case of above.
		return -lExt.ToCmp()
	}

	// Equal keys:
	//   k (ExtendLow)  vs. l (ExtendLow)   ->  equal   (0)
	//   k (ExtendLow)  vs. l (ExtendHigh)  ->  smaller (-1)
	//   k (ExtendHigh) vs. l (ExtendLow)   ->  bigger  (1)
	//   k (ExtendHigh) vs. l (ExtendHigh)  ->  equal   (0)
	if kExt == lExt {
		return 0
	}
	return kExt.ToCmp()
}

// Concat creates a new composite key by extending this key's values with the
// values of the given key. The new key's length is len(k) + len(l).
func (k Key) Concat(l Key) Key {
	kLen := k.Length()
	lLen := l.Length()

	switch kLen {
	case 0:
		return l

	case 1:
		switch lLen {
		case 0:
			return k

		case 1:
			return Key{firstVal: k.firstVal, otherVals: tree.Datums{l.firstVal}}

		default:
			vals := make(tree.Datums, lLen)
			vals[0] = l.firstVal
			copy(vals[1:], l.otherVals)
			return Key{firstVal: k.firstVal, otherVals: vals}
		}

	default:
		switch lLen {
		case 0:
			return k

		case 1:
			vals := make(tree.Datums, kLen)
			copy(vals, k.otherVals)
			vals[len(k.otherVals)] = l.firstVal
			return Key{firstVal: k.firstVal, otherVals: vals}

		default:
			vals := make(tree.Datums, kLen+lLen-1)
			copy(vals, k.otherVals)
			vals[len(k.otherVals)] = l.firstVal
			copy(vals[len(k.otherVals)+1:], l.otherVals)
			return Key{firstVal: k.firstVal, otherVals: vals}
		}
	}
}

// String formats a key like this:
//  EmptyKey         : empty string
//  Key with 1 value : /2
//  Key with 2 values: /5/1
//  Key with 3 values: /3/6/4
func (k Key) String() string {
	var buf bytes.Buffer
	for i := 0; i < k.Length(); i++ {
		fmt.Fprintf(&buf, "/%s", k.Value(i))
	}
	return buf.String()
}

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
