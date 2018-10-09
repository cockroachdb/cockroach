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

package memo

import (
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

const (
	// offset64 is the initial hash value, and is taken from fnv.go
	offset64 = 14695981039346656037

	// prime64 is a large-ish prime number used in hashing and taken from fnv.go.
	prime64 = 1099511628211
)

// internHash is a 64-bit hash value, computed using the FNV-1a algorithm.
type internHash uint64

// interner interns relational and scalar expressions, which means that multiple
// equivalent expressions are mapped to a single in-memory instance. If two
// expressions with the same values for all fields are interned, the intern
// method will return the first expression in both cases. Interned expressions
// can therefore be checked for equivalence by simple pointer comparison.
// Equivalence is defined more strictly than SQL equivalence; two values are
// equivalent only if their binary encoded representations are identical. For
// example, positive and negative float64 values *are not* equivalent, whereas
// NaN float values *are* equivalent.
//
// To use interner, first call the Init method to initialize storage. Call the
// appropriate Intern method to retrieve the canonical instance of that
// expression. Release references to other instances, including the expression
// passed to Intern.
//
// The interner computes a hash function for each expression operator type that
// enables quick determination of whether an expression has already been added
// to the interner. A hashXXX and isXXXEqual method must be added for every
// unique type of every field in every expression struct. Optgen generates
// Intern methods which use these methods to compute the hash and check whether
// an equivalent expression is already present in the cache. Because hashing is
// used, interned expressions must remain immutable after interning. That is,
// once set, they can never be modified again, else their hash value would
// become invalid.
//
// The non-cryptographic hash function is adapted from fnv.go in Golang's
// standard library. That in turn was taken from FNV-1a, described here:
//
//   https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function
//
// Each expression type follows the same interning pattern:
//
//   1. Compute an int64 hash value for the expression using FNV-1a.
//   2. Do a fast 64-bit Go map lookup to determine if an expression with the
//      same hash is already in the cache.
//   3. If so, then whether the existing expression is equivalent, since there
//      may be a hash value collision.
//   4. Expressions with colliding hash values are linked together in a list.
//      Rather than use an explicit linked list data structure, colliding
//      entries are rehashed using a randomly generated hash value that is
//      stored in the existing entry.
//
// This pattern enables very low overhead hashing of expressions - the
// allocation of a Go map with a fast 64-bit key, plus a couple of reusable
// scratch byte arrays.
type interner struct {
	bytes     []byte
	bytes2    []byte
	hash      internHash
	cache     map[internHash]internedItem
	item      interface{}
	collision internHash
}

// internedItem is the Go map value. In case of hash value collisions it
// functions as a linked list node; its collision field is a randomly generated
// re-hash value that "points" to the colliding node. That node in turn can
// point to yet another colliding node, and so on. Walking a collision list
// therefore consists of computing an initial hash based on the value of the
// node, and then following the list of collision hash values by indexing into
// the cache map repeatedly.
type internedItem struct {
	item      interface{}
	collision internHash
}

// Init creates the cache in preparation for calls to the Intern methods.
func (in *interner) Init() {
	in.cache = make(map[internHash]internedItem)
}

// Clear releases the cache. Future calls to Intern methods will fail unless
// Init is called again. Expressions interned before the call to Clear will not
// be connected to expressions interned after.
func (in *interner) Clear() {
	in.cache = nil
}

// Count returns the number of expressions that have been interned.
func (in *interner) Count() int {
	return len(in.cache)
}

var physPropsTypeHash = internHash(uint64(reflect.ValueOf((*props.Physical)(nil)).Pointer()))

// InternPhysicalProps interns a set of physical properties using the same
// pattern as that used by the expression intern methods, with one difference.
// This intern method does not force the incoming physical properties to escape
// to the heap. It does this by making a copy of the physical properties before
// adding them to the cache.
func (in *interner) InternPhysicalProps(val *props.Physical) *props.Physical {
	// Hash the props.Physical reflect type to distinguish it from other values.
	in.hash = offset64
	in.hash ^= physPropsTypeHash
	in.hash *= prime64
	in.hashPhysProps(val)

	for in.lookupItem() {
		// There's an existing item, so check for equality.
		if other, ok := in.item.(*props.Physical); ok {
			if in.isPhysPropsEqual(val, other) {
				return other
			}
		}

		// Values have same hash, but are not equal, so pick a new hash value.
		in.handleCollision()
	}

	// Shallow copy the props to prevent "val" from escaping.
	copy := *val
	in.cache[in.hash] = internedItem{item: &copy}
	return &copy
}

// lookupItem looks up an item in the cache using the hash field value. If the
// item exists, it's stored in the item field, along with any collision value,
// and lookupItem returns true. If the item is not in the cache, lookupItem
// returns false.
func (in *interner) lookupItem() bool {
	interned, ok := in.cache[in.hash]
	if !ok {
		return false
	}
	in.item = interned.item
	in.collision = interned.collision
	return true
}

// handleCollision is called when two non-equivalent items have the same hash
// value. Colliding items are re-hashed using a randomly generated hash value
// that is stored in the previous linked list entry.
func (in *interner) handleCollision() {
	// If at the end of the list of any existing collisions, create a new one.
	if in.collision == 0 {
		// Re-hash the item with a new random hash value.
		in.collision = internHash(rand.Uint64())
		if in.collision == 0 {
			// Reserve 0 to mean "no collision".
			in.collision = 1
		}

		// Link the previous hash entry to the new hash value.
		prev := in.cache[in.hash]
		prev.collision = in.collision
		in.cache[in.hash] = prev
	}

	// Set the next hash value to look up.
	in.hash = in.collision
	in.collision = 0
}

// ----------------------------------------------------------------------
//
// Hash functions
//   Each field in each item to be hashed must have a hash function that
//   corresponds to the type of that field. Each type mutates the hash field
//   of the interner using the FNV-1a hash algorithm.
//
// ----------------------------------------------------------------------

func (in *interner) hashBool(val bool) {
	i := 0
	if val {
		i = 1
	}
	in.hash ^= internHash(i)
	in.hash *= prime64
}

func (in *interner) hashInt(val int) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashUint64(val uint64) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashFloat64(val float64) {
	in.hash ^= internHash(math.Float64bits(val))
	in.hash *= prime64
}

func (in *interner) hashString(val string) {
	for _, c := range val {
		in.hash ^= internHash(c)
		in.hash *= prime64
	}
}

func (in *interner) hashBytes(val []byte) {
	for _, c := range val {
		in.hash ^= internHash(c)
		in.hash *= prime64
	}
}

func (in *interner) hashOperator(val opt.Operator) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashType(val reflect.Type) {
	in.hash ^= internHash(uint64(reflect.ValueOf(val).Pointer()))
	in.hash *= prime64
}

func (in *interner) hashDatum(val tree.Datum) {
	// Distinguish distinct values with the same representation (i.e. 1 can
	// be a Decimal or Int) using the reflect.Type of the value.
	in.hashType(reflect.TypeOf(val))

	// Special case some datum types that are simple to hash. For the others,
	// hash the key encoding or string representation.
	switch t := val.(type) {
	case *tree.DBool:
		in.hashBool(bool(*t))
	case *tree.DInt:
		in.hashUint64(uint64(*t))
	case *tree.DFloat:
		in.hashFloat64(float64(*t))
	case *tree.DString:
		in.hashString(string(*t))
	case *tree.DBytes:
		in.hashBytes([]byte(*t))
	case *tree.DDate:
		in.hashUint64(uint64(*t))
	case *tree.DTime:
		in.hashUint64(uint64(*t))
	case *tree.DJSON:
		in.hashString(t.String())
	case *tree.DTuple:
		for _, d := range t.D {
			in.hashDatum(d)
		}
		in.hashDatumType(t.ResolvedType())
	case *tree.DArray:
		for _, d := range t.Array {
			in.hashDatum(d)
		}
	default:
		in.hashBytes(in.encodeDatum(in.bytes[:0], val))
	}
}

func (in *interner) hashDatumType(val types.T) {
	in.hashString(val.String())
}

func (in *interner) hashColType(val coltypes.T) {
	buf := bytes.NewBuffer(in.bytes)
	val.Format(buf, lex.EncNoFlags)
	in.hashBytes(buf.Bytes())
}

func (in *interner) hashTypedExpr(val tree.TypedExpr) {
	in.hash ^= internHash(uint64(reflect.ValueOf(val).Pointer()))
	in.hash *= prime64
}

func (in *interner) hashColumnID(val opt.ColumnID) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashColSet(val opt.ColSet) {
	hash := in.hash
	for c, ok := val.Next(0); ok; c, ok = val.Next(c + 1) {
		hash ^= internHash(c)
		hash *= prime64
	}
	in.hash = hash
}

func (in *interner) hashColList(val opt.ColList) {
	hash := in.hash
	for _, id := range val {
		hash ^= internHash(id)
		hash *= prime64
	}
	in.hash = hash
}

func (in *interner) hashOrdering(val opt.Ordering) {
	hash := in.hash
	for _, id := range val {
		hash ^= internHash(id)
		hash *= prime64
	}
	in.hash = hash
}

func (in *interner) hashOrderingChoice(val props.OrderingChoice) {
	in.hashColSet(val.Optional)

	for i := range val.Columns {
		choice := &val.Columns[i]
		in.hashColSet(choice.Group)
		in.hashBool(choice.Descending)
	}
}

func (in *interner) hashTableID(val opt.TableID) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashConstraint(val *constraint.Constraint) {
	in.hash ^= internHash(uintptr(unsafe.Pointer(val)))
	in.hash *= prime64
}

func (in *interner) hashScanLimit(val ScanLimit) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashScanFlags(val ScanFlags) {
	in.hashBool(val.NoIndexJoin)
	in.hashBool(val.ForceIndex)
	in.hash ^= internHash(val.Index)
	in.hash *= prime64
}

func (in *interner) hashSubquery(val *tree.Subquery) {
	in.hash ^= internHash(uintptr(unsafe.Pointer(val)))
	in.hash *= prime64
}

func (in *interner) hashExplainOptions(val tree.ExplainOptions) {
	in.hashColSet(val.Flags)
	in.hash ^= internHash(val.Mode)
	in.hash *= prime64
}

func (in *interner) hashShowTraceType(val tree.ShowTraceType) {
	in.hashString(string(val))
}

func (in *interner) hashFuncProps(val *tree.FunctionProperties) {
	in.hash ^= internHash(uintptr(unsafe.Pointer(val)))
	in.hash *= prime64
}

func (in *interner) hashFuncOverload(val *tree.Overload) {
	in.hash ^= internHash(uintptr(unsafe.Pointer(val)))
	in.hash *= prime64
}

func (in *interner) hashTupleOrdinal(val TupleOrdinal) {
	in.hash ^= internHash(val)
	in.hash *= prime64
}

func (in *interner) hashPhysProps(val *props.Physical) {
	for i := range val.Presentation {
		col := &val.Presentation[i]
		in.hashString(col.Label)
		in.hashColumnID(col.ID)
	}
	in.hashOrderingChoice(val.Ordering)
}

func (in *interner) hashRelExpr(val RelExpr) {
	in.hash ^= internHash(uint64(reflect.ValueOf(val).Pointer()))
	in.hash *= prime64
}

func (in *interner) hashScalarExpr(val opt.ScalarExpr) {
	in.hash ^= internHash(uint64(reflect.ValueOf(val).Pointer()))
	in.hash *= prime64
}

func (in *interner) hashScalarListExpr(val ScalarListExpr) {
	for i := range val {
		in.hashScalarExpr(val[i])
	}
}

func (in *interner) hashFiltersExpr(val FiltersExpr) {
	for i := range val {
		in.hashScalarExpr(val[i].Condition)
	}
}

func (in *interner) hashProjectionsExpr(val ProjectionsExpr) {
	for i := range val {
		item := &val[i]
		in.hashColumnID(item.Col)
		in.hashScalarExpr(item.Element)
	}
}

func (in *interner) hashAggregationsExpr(val AggregationsExpr) {
	for i := range val {
		item := &val[i]
		in.hashColumnID(item.Col)
		in.hashScalarExpr(item.Agg)
	}
}

// ----------------------------------------------------------------------
//
// Equality functions
//   Each field in each item to be hashed must have an equality function that
//   corresponds to the type of that field. An equality function returns true
//   if the two values are equivalent. If all fields in two items are
//   equivalent, then the items are considered equivalent, and only one is
//   retained in the cache.
//
// ----------------------------------------------------------------------

func (in *interner) isBoolEqual(l, r bool) bool {
	return l == r
}

func (in *interner) isIntEqual(l, r int) bool {
	return l == r
}

func (in *interner) isFloat64Equal(l, r float64) bool {
	return math.Float64bits(l) == math.Float64bits(r)
}

func (in *interner) isStringEqual(l, r string) bool {
	return bytes.Equal([]byte(l), []byte(r))
}

func (in *interner) isBytesEqual(l, r []byte) bool {
	return bytes.Equal(l, r)
}

func (in *interner) isTypeEqual(l, r reflect.Type) bool {
	return l == r
}

func (in *interner) isOperatorEqual(l, r opt.Operator) bool {
	return l == r
}

func (in *interner) isDatumTypeEqual(l, r types.T) bool {
	return l.String() == r.String()
}

func (in *interner) isColTypeEqual(l, r coltypes.T) bool {
	lbuf := bytes.NewBuffer(in.bytes)
	l.Format(lbuf, lex.EncNoFlags)
	rbuf := bytes.NewBuffer(in.bytes2)
	r.Format(rbuf, lex.EncNoFlags)
	return bytes.Equal(lbuf.Bytes(), rbuf.Bytes())
}

func (in *interner) isDatumEqual(l, r tree.Datum) bool {
	switch lt := l.(type) {
	case *tree.DBool:
		if rt, ok := r.(*tree.DBool); ok {
			return *lt == *rt
		}
	case *tree.DInt:
		if rt, ok := r.(*tree.DInt); ok {
			return *lt == *rt
		}
	case *tree.DFloat:
		if rt, ok := r.(*tree.DFloat); ok {
			return in.isFloat64Equal(float64(*lt), float64(*rt))
		}
	case *tree.DString:
		if rt, ok := r.(*tree.DString); ok {
			return in.isStringEqual(string(*lt), string(*rt))
		}
	case *tree.DBytes:
		if rt, ok := r.(*tree.DBytes); ok {
			return bytes.Equal([]byte(*lt), []byte(*rt))
		}
	case *tree.DDate:
		if rt, ok := r.(*tree.DDate); ok {
			return uint64(*lt) == uint64(*rt)
		}
	case *tree.DTime:
		if rt, ok := r.(*tree.DTime); ok {
			return uint64(*lt) == uint64(*rt)
		}
	case *tree.DJSON:
		if rt, ok := r.(*tree.DJSON); ok {
			return in.isStringEqual(lt.String(), rt.String())
		}
	case *tree.DTuple:
		if rt, ok := r.(*tree.DTuple); ok {
			if len(lt.D) != len(rt.D) {
				return false
			}
			for i := range lt.D {
				if !in.isDatumEqual(lt.D[i], rt.D[i]) {
					return false
				}
			}
			return in.isDatumTypeEqual(l.ResolvedType(), r.ResolvedType())
		}
	case *tree.DArray:
		if rt, ok := r.(*tree.DArray); ok {
			if len(lt.Array) != len(rt.Array) {
				return false
			}
			for i := range lt.Array {
				if !in.isDatumEqual(lt.Array[i], rt.Array[i]) {
					return false
				}
			}
			return true
		}
	default:
		lb := in.encodeDatum(in.bytes[:0], l)
		rb := in.encodeDatum(in.bytes2[:0], r)
		return bytes.Equal(lb, rb)
	}

	return false
}

func (in *interner) isTypedExprEqual(l, r tree.TypedExpr) bool {
	return l == r
}

func (in *interner) isColumnIDEqual(l, r opt.ColumnID) bool {
	return l == r
}

func (in *interner) isColSetEqual(l, r opt.ColSet) bool {
	return l.Equals(r)
}

func (in *interner) isColListEqual(l, r opt.ColList) bool {
	return l.Equals(r)
}

func (in *interner) isOrderingEqual(l, r opt.Ordering) bool {
	return l.Equals(r)
}

func (in *interner) isOrderingChoiceEqual(l, r props.OrderingChoice) bool {
	return l.Equals(&r)
}

func (in *interner) isTableIDEqual(l, r opt.TableID) bool {
	return l == r
}

func (in *interner) isConstraintEqual(l, r *constraint.Constraint) bool {
	return l == r
}

func (in *interner) isScanLimitEqual(l, r ScanLimit) bool {
	return l == r
}

func (in *interner) isScanFlagsEqual(l, r ScanFlags) bool {
	return l == r
}

func (in *interner) isSubqueryEqual(l, r *tree.Subquery) bool {
	return l == r
}

func (in *interner) isExplainOptionsEqual(l, r tree.ExplainOptions) bool {
	return l.Mode == r.Mode && l.Flags.Equals(r.Flags)
}

func (in *interner) isShowTraceTypeEqual(l, r tree.ShowTraceType) bool {
	return bytes.Equal([]byte(l), []byte(r))
}

func (in *interner) isFuncPropsEqual(l, r *tree.FunctionProperties) bool {
	return l == r
}

func (in *interner) isFuncOverloadEqual(l, r *tree.Overload) bool {
	return l == r
}

func (in *interner) isTupleOrdinalEqual(l, r TupleOrdinal) bool {
	return l == r
}

func (in *interner) isPhysPropsEqual(l, r *props.Physical) bool {
	return l.Equals(r)
}

func (in *interner) isRelExprEqual(l, r RelExpr) bool {
	return l == r
}

func (in *interner) isScalarExprEqual(l, r opt.ScalarExpr) bool {
	return l == r
}

func (in *interner) isScalarListExprEqual(l, r ScalarListExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i] != r[i] {
			return false
		}
	}
	return true
}

func (in *interner) isFiltersExprEqual(l, r FiltersExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Condition != r[i].Condition {
			return false
		}
	}
	return true
}

func (in *interner) isProjectionsExprEqual(l, r ProjectionsExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Col != r[i].Col || l[i].Element != r[i].Element {
			return false
		}
	}
	return true
}

func (in *interner) isAggregationsExprEqual(l, r AggregationsExpr) bool {
	if len(l) != len(r) {
		return false
	}
	for i := range l {
		if l[i].Col != r[i].Col || l[i].Agg != r[i].Agg {
			return false
		}
	}
	return true
}

// encodeDatum turns the given datum into an encoded string of bytes. If two
// datums are equivalent, then their encoded bytes will be identical.
// Conversely, if two datums are not equivalent, then their encoded bytes will
// differ.
func (in *interner) encodeDatum(b []byte, val tree.Datum) []byte {
	// Fast path: encode the datum using table key encoding. This does not always
	// work, because the encoding does not uniquely represent some values which
	// should not be considered equivalent by the interner (e.g. decimal values
	// 1.0 and 1.00).
	if !sqlbase.DatumTypeHasCompositeKeyEncoding(val.ResolvedType()) {
		var err error
		b, err = sqlbase.EncodeTableKey(b, val, encoding.Ascending)
		if err == nil {
			return b
		}
	}

	// Fall back on a string representation which can be used to check for
	// equivalence.
	buf := bytes.NewBuffer(b)
	ctx := tree.MakeFmtCtx(buf, tree.FmtCheckEquivalence)
	val.Format(&ctx)
	return buf.Bytes()
}
