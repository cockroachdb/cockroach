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
// specify whether each key is suffixed with an empty value or an infinite
// value for purposes of comparison. See the comment for the Key.Compare method
// for more details.
type KeyExtension bool

const (
	// ExtendEmpty specifies that the key should be suffixed with the empty
	// value for purposes of comparison.
	ExtendEmpty KeyExtension = false

	// ExtendInf specifies that the key should be suffixed with the infinite
	// value for purposes of comparison.
	ExtendInf KeyExtension = true
)

// EmptyKey has zero values. Depending on whether its a start or end key, and
// depending on whether the boundary is inclusive or exclusive, it may sort
// before all other keys, or it may sort after all other keys:
//   inclusive start = Ø
//   exclusive start = invalid (prevented by Span.Set)
//   inclusive end   = ∞
//   exclusive end   = Ø
//
// These rules derive from the key extension rules described in the comment for
// the Key.Compare method.
var EmptyKey = Key{}

// Key is a composite of N typed datum values that are ordered from most
// significant to least significant.
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
// extensions specify whether each key is suffixed with an empty value or an
// infinite value for purposes of comparison. For example, if k is /1/2, then:
//   k (kExt = ExtendEmpty): /1/2/Ø
//   k (kExt = ExtendInf)  : /1/2/∞
//
// These extensions have no effect if one key is not a prefix of the other,
// since the comparison would already have concluded in previous values. But
// if comparison proceeds all the way to the end of one of the keys, then the
// extension determines which key is greater. Any value except empty compares
// greater than the empty value, and any value except infinite compares less
// than the infinite value:
//   /5/Ø   = /5/Ø
//   /5/Ø   < /5/∞
//   /5/1/Ø > /5/Ø
//   /5/1/Ø < /5/∞
//   /5/1/∞ > /5/Ø
//   /5/1/∞ < /5/∞
//   /5/∞   > /5/Ø
//   /5/∞   = /5/∞
//
// Extensions are used to properly compare start and end keys in spans which
// may be either inclusive or exclusive. Here is the mapping:
//   [/1/2 - ...] (inclusive start key): ExtendEmpty: /1/2/Ø
//   (/1/2 - ...] (exclusive start key): ExtendInf  : /1/2/∞
//   [... - /1/2] (inclusive end key)  : ExtendInf  : /1/2/∞
//   [... - /1/2) (exclusive end key)  : ExtendEmpty: /1/2/Ø
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
		// empty or with inf:
		//   k (ExtendEmpty) = /1/Ø  ->  k is smaller (-1)
		//   k (ExtendInf)   = /1/∞  ->  k is bigger  (1)
		return kExt.ToCmp()
	} else if kLen > lLen {
		// Inverse case of above.
		return -lExt.ToCmp()
	}

	// Equal keys:
	//   k (ExtendEmpty) vs. l (ExtendEmpty)  ->  equal   (0)
	//   k (ExtendEmpty) vs. l (ExtendInf)    ->  smaller (-1)
	//   k (ExtendInf)   vs. l (ExtendEmpty)  ->  bigger  (1)
	//   k (ExtendInf)   vs. l (ExtendInf)    ->  equal   (0)
	if kExt == lExt {
		return 0
	}
	return kExt.ToCmp()
}

// Concat creates a new composite key by extending this key's values with the
// values of the given key. The new key's length is len(k) + len(l).
func (k Key) Concat(l Key) Key {
	switch k.Length() {
	case 0:
		return l

	case 1:
		switch l.Length() {
		case 0:
			return k

		case 1:
			return Key{firstVal: k.firstVal, otherVals: tree.Datums{l.firstVal}}

		default:
			vals := make(tree.Datums, 1+len(l.otherVals))
			vals[0] = l.firstVal
			copy(vals[1:], l.otherVals)
			return Key{firstVal: k.firstVal, otherVals: vals}
		}

	default:
		switch l.Length() {
		case 0:
			return k

		case 1:
			vals := make(tree.Datums, len(k.otherVals)+1)
			copy(vals, k.otherVals)
			vals[len(vals)-1] = l.firstVal
			return Key{firstVal: k.firstVal, otherVals: vals}

		default:
			vals := make(tree.Datums, len(k.otherVals)+1+len(l.otherVals))
			copy(vals, k.otherVals)
			vals[len(k.otherVals)] = l.firstVal
			copy(vals[len(k.otherVals)+1:], l.otherVals)
			return Key{firstVal: k.firstVal, otherVals: vals}
		}
	}
}

// String formats a key like this:
//  EmptyKey: empty string
//  1D key: /2
//  2D key: /5/1
//  3D key: /3/6/4
func (k Key) String() string {
	var buf bytes.Buffer
	for i := 0; i < k.Length(); i++ {
		fmt.Fprintf(&buf, "/%s", k.Value(i))
	}
	return buf.String()
}

// ToCmp converts from a key extension value to a comparison value. ExtendEmpty
// maps to -1 because the empty value sorts before all other values. ExtendInf
// maps to 1 because the infinite value sorts after all other values.
func (e KeyExtension) ToCmp() int {
	// Map ExtendEmpty (false) into -1 and ExtendInf (true) into 1.
	if e == ExtendEmpty {
		return -1
	}
	return 1
}
