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

// Concat creates a new composite key by extending this key's values with the
// values of the given key. The new key's length is len(k) + len(l).
func (k Key) Concat(l Key) Key {
	klen := k.Length()
	llen := l.Length()

	if klen == 0 {
		return l
	}
	if llen == 0 {
		return k
	}

	vals := make(tree.Datums, klen+llen-1)
	copy(vals, k.otherVals)
	vals[klen-1] = l.firstVal
	copy(vals[klen:], l.otherVals)
	return Key{firstVal: k.firstVal, otherVals: vals}
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

// KeyContext contains the necessary metadata for comparing Keys.
type KeyContext struct {
	Columns *Columns
	EvalCtx *tree.EvalContext
}

// MakeKeyContext initializes a KeyContext.
func MakeKeyContext(cols *Columns, evalCtx *tree.EvalContext) KeyContext {
	return KeyContext{Columns: cols, EvalCtx: evalCtx}
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
func (c KeyContext) Compare(k Key, kext KeyExtension, l Key, lext KeyExtension) int {
	klen := k.Length()
	llen := l.Length()
	for i := 0; i < klen && i < llen; i++ {
		if cmp := k.Value(i).Compare(c.EvalCtx, l.Value(i)); cmp != 0 {
			if c.Columns.Get(i).Descending() {
				cmp = -cmp
			}
			return cmp
		}
	}

	if klen < llen {
		// k matches a prefix of l:
		//   k = /1
		//   l = /1/2
		// Which of these is "smaller" depends on whether k is extended with
		// -inf or with +inf:
		//   k (ExtendLow)  = /1/Low  < /1/2  ->  k is smaller (-1)
		//   k (ExtendHigh) = /1/High > /1/2  ->  k is bigger  (1)
		return kext.ToCmp()
	} else if klen > llen {
		// Inverse case of above.
		return -lext.ToCmp()
	}

	// Equal keys:
	//   k (ExtendLow)  vs. l (ExtendLow)   ->  equal   (0)
	//   k (ExtendLow)  vs. l (ExtendHigh)  ->  smaller (-1)
	//   k (ExtendHigh) vs. l (ExtendLow)   ->  bigger  (1)
	//   k (ExtendHigh) vs. l (ExtendHigh)  ->  equal   (0)
	if kext == lext {
		return 0
	}
	return kext.ToCmp()
}
