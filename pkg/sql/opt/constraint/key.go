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
// Key is immutable; it cannot be changed once created.
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

// IsNull is true if this key is the NULL value.
func (k Key) IsNull() bool {
	return k.Length() == 1 && k.firstVal == tree.DNull
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
func (k Key) Compare(keyCtx *KeyContext, l Key, kext, lext KeyExtension) int {
	klen := k.Length()
	llen := l.Length()
	for i := 0; i < klen && i < llen; i++ {
		if cmp := keyCtx.Compare(i, k.Value(i), l.Value(i)); cmp != 0 {
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

// CutFront returns the key with the first numCols values removed.
// Example:
//   [/1/2 - /1/3].CutFront(1) = [/2 - /3]
func (k Key) CutFront(numCols int) Key {
	if numCols == 0 {
		return k
	}
	if len(k.otherVals) < numCols {
		return EmptyKey
	}
	return Key{
		firstVal:  k.otherVals[numCols-1],
		otherVals: k.otherVals[numCols:],
	}
}

// CutBack returns the key with the last numCols values removed.
// Example:
//   '/1/2'.CutBack(1) = '/1'
func (k Key) CutBack(numCols int) Key {
	if numCols == 0 {
		return k
	}
	if len(k.otherVals) < numCols {
		return EmptyKey
	}
	return Key{
		firstVal:  k.firstVal,
		otherVals: k.otherVals[:len(k.otherVals)-numCols],
	}
}

// IsNextKey returns true if:
//  - k and other have the same length;
//  - on all but the last column, k and other have the same values;
//  - on the last column, k has the datum that follows other's datum (for
//    types that support it).
// For example: /2.IsNextKey(/1) is true.
func (k Key) IsNextKey(keyCtx *KeyContext, other Key) bool {
	n := k.Length()
	if n != other.Length() {
		return false
	}
	// All the datums up to the last one must be equal.
	for i := 0; i < n-1; i++ {
		if keyCtx.Compare(i, k.Value(i), other.Value(i)) != 0 {
			return false
		}
	}

	next, ok := keyCtx.Next(n-1, other.Value(n-1))
	return ok && keyCtx.Compare(n-1, k.Value(n-1), next) == 0
}

// Next returns the next key; this only works for discrete types like integers.
// It is guaranteed that there are no  possible keys in the span
//   ( key, Next(keu) ).
//
// Examples:
//   Next(/1/2) = /1/3
//   Next(/1/false) = /1/true
//   Next(/1/true) returns !ok
//   Next(/'foo') = /'foo\x00'
//
// If a column is descending, the values on that column go backwards:
//   Next(/2) = /1
//
// The key cannot be empty.
func (k Key) Next(keyCtx *KeyContext) (_ Key, ok bool) {
	// TODO(radu): here we could do a better job: if we know the last value is the
	// maximum possible value, we could shorten the key; for example
	//   Next(/1/true) -> /2
	// This is a bit tricky to implement because of NULL values (e.g. on a
	// descending nullable column, "false" is not the minimum value, NULL is).
	col := k.Length() - 1
	nextVal, ok := keyCtx.Next(col, k.Value(col))
	if !ok {
		return Key{}, false
	}
	if col == 0 {
		return Key{firstVal: nextVal}, true
	}
	// Keep the key up to col, and replace the value for col with nextVal.
	vals := make([]tree.Datum, col)
	copy(vals[:col-1], k.otherVals)
	vals[col-1] = nextVal
	return Key{firstVal: k.firstVal, otherVals: vals}, true
}

// Prev returns the next key; this only works for discrete types like integers.
//
// Examples:
//   Prev(/1/2) = /1/1
//   Prev(/1/true) = /1/false
//   Prev(/1/false) returns !ok.
//   Prev(/'foo') returns !ok.
//
// If a column is descending, the values on that column go backwards:
//   Prev(/1) = /2
//
// If this is the minimum possible key, returns EmptyKey.
func (k Key) Prev(keyCtx *KeyContext) (_ Key, ok bool) {
	col := k.Length() - 1
	prevVal, ok := keyCtx.Prev(col, k.Value(col))
	if !ok {
		return Key{}, false
	}
	if col == 0 {
		return Key{firstVal: prevVal}, true
	}
	// Keep the key up to col, and replace the value for col with prevVal.
	vals := make([]tree.Datum, col)
	copy(vals[:col-1], k.otherVals)
	vals[col-1] = prevVal
	return Key{firstVal: k.firstVal, otherVals: vals}, true
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
	Columns Columns
	EvalCtx *tree.EvalContext
}

// MakeKeyContext initializes a KeyContext.
func MakeKeyContext(cols *Columns, evalCtx *tree.EvalContext) KeyContext {
	return KeyContext{Columns: *cols, EvalCtx: evalCtx}
}

// Compare two values for a given column.
// Returns 0 if the values are equal, -1 if a is less than b, or 1 if b is less
// than a.
func (c *KeyContext) Compare(colIdx int, a, b tree.Datum) int {
	// Fast path when the datums are the same.
	if a == b {
		return 0
	}
	cmp := a.Compare(c.EvalCtx, b)
	if c.Columns.Get(colIdx).Descending() {
		cmp = -cmp
	}
	return cmp
}

// Next returns the next value on a given column (for discrete types like
// integers). See Datum.Next/Prev.
func (c *KeyContext) Next(colIdx int, val tree.Datum) (_ tree.Datum, ok bool) {
	if c.Columns.Get(colIdx).Ascending() {
		if val.IsMax(c.EvalCtx) {
			return nil, false
		}
		return val.Next(c.EvalCtx)
	}
	if val.IsMin(c.EvalCtx) {
		return nil, false
	}
	return val.Prev(c.EvalCtx)
}

// Prev returns the previous value on a given column (for discrete types like
// integers). See Datum.Next/Prev.
func (c *KeyContext) Prev(colIdx int, val tree.Datum) (_ tree.Datum, ok bool) {
	if c.Columns.Get(colIdx).Ascending() {
		if val.IsMin(c.EvalCtx) {
			return nil, false
		}
		return val.Prev(c.EvalCtx)
	}
	if val.IsMax(c.EvalCtx) {
		return nil, false
	}
	return val.Next(c.EvalCtx)
}
