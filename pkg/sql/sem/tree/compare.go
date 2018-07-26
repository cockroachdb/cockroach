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

package tree

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/apd"
)

// This file implements comparisons between tuples.
//
// This implements the comparators for three separate relations.
//
// - a total ordering for the purpose of sorting and searching
//   values in indexes. In that relation, NULL sorts at the
//   same location as itself and before other values.
//
//   Functions: TotalOrderLess(), TotalOrderCompare().
//
// - the logical SQL scalar partial ordering, where non-NULL
//   values can be compared to each others but NULL comparisons
//   themselves produce a NULL result.
//
//   Function: ScalarCompare()
//
// - the IS [NOT] DISTINCT relation, in which every value can
//   be compared to every other, NULLs are distinct from every
//   non-NULL value but not distinct from each other.
//
//   Function: Distinct().
//
// Due to the way the SQL language semantics are constructed, it is
// the case Distinct() returns true if and only if
// TotalOrderCompare() returns nonzero.  However, one should be
// careful when using this methods to properly convey *intent* to the
// reader of the code:
//
// - the functions related to the total order for sorting should only
//   be used in contexts that are about sorting values.
//
// - Distinct() and ScalarCompare() should be used everywhere else.
//
// Besides, separating Distinct() from TotalOrderCompare() enables
// later performance optimizations of the former by specializing the
// code. This is currently done for e.g. EncDatums.

// TotalOrderLess returns true if and only if a sorts before b. NULLs
// are considered to sort first.
func TotalOrderLess(ctx *EvalContext, a, b Datum) bool {
	return doCompare(ctx, cmpFlagOrderedNulls, a, b) < 0
}

// TotalOrderCompare returns -1 if a sorts before b, +1 if a sorts
// after b, and 0 if a and be are considered equal for the purpose of
// sorting.
// This function is only suitable for index span computations and
// should not be used to test equality. Consider Distinct()
// and ScalarCompare() instead.
func TotalOrderCompare(ctx *EvalContext, a, b Datum) int {
	// We can safely cast to int because doCompare never returns
	// cmpObservedNulls if cmpFlagOrderedNulls is set.
	return int(doCompare(ctx, cmpFlagOrderedNulls, a, b))
}

// Distinct returns true if and only if a and b are distinct
// from each other. NULLs are considered to not be distinct from each
// other but are distinct from every other value.
func Distinct(ctx *EvalContext, a, b Datum) bool {
	return doCompare(ctx, cmpFlagOrderedNulls, a, b) != cmpResultEqual
}

// Distinct checks to see if two slices of datums are distinct
// from each other. Any change in value is considered distinct,
// however, a NULL value is NOT considered disctinct from another NULL
// value.
func (d Datums) Distinct(evalCtx *EvalContext, other Datums) bool {
	if len(d) != len(other) {
		return true
	}
	for i, val := range d {
		if Distinct(evalCtx, val, other[i]) {
			return true
		}
	}
	return false
}

// ScalarCompare returns a SQL value for the given comparison.
//
// It properly returns NULL if the comparison is not "Distinct" and
// requires comparing NULL values against anything else.
//
// The operator op and two operands must have already undergone
// normalization via foldComparisonExpr: the only supported operators
// here are EQ, LT, LE and IsNotDistinctFrom.
func ScalarCompare(ctx *EvalContext, a, b Datum, op ComparisonOperator) Datum {
	if op == IsNotDistinctFrom {
		return MakeDBool(DBool(!Distinct(ctx, a, b)))
	}

	var fl internalCmpFlags
	if op == EQ {
		fl = cmpFlagEQ
	}
	res := doCompare(ctx, fl, a, b)
	if res == cmpObservedNulls {
		return DNull
	}

	cmp := int(res)

	switch op {
	case EQ:
		return MakeDBool(cmp == 0)
	case LT:
		return MakeDBool(cmp < 0)
	case LE:
		return MakeDBool(cmp <= 0)
	default:
		panic(fmt.Sprintf("unexpected ComparisonOperator in boolFromCmp: %v", op))
	}
}

type internalCmpResult int

const (
	cmpResultLower   internalCmpResult = -1 // must be -1 for TotalOrderCompare() and TotalOrderLess()
	cmpResultEqual   internalCmpResult = 0  // must be zero for TotalOrderCompare()
	cmpResultGreater internalCmpResult = 1  // must be 1 for TotalOrderCompare()
	cmpObservedNulls internalCmpResult = 2  // can be anything else than -1, 0, or 1
)

// doCompare is the main function.
func doCompare(ctx *EvalContext, flags internalCmpFlags, a, b Datum) internalCmpResult {
	if a == DNull || b == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			if b != DNull {
				return cmpResultLower
			}
			if a != DNull {
				return cmpResultGreater
			}
			return cmpResultEqual
		}
		return cmpObservedNulls
	}
	b = UnwrapDatum(ctx, b)
	return a.internalCompare(ctx, flags, b)
}

type internalCmpFlags int

const (
	// cmpFlagOrderedNulls indicate that a NULL value
	// can be compared to other values.
	cmpFlagOrderedNulls internalCmpFlags = 1 << iota
	// cmpFlagEQ indicates that the comparison is done on behalf of a
	// SQL equal (=) operator.  This causes special semantics for tuple
	// and array comparisons.
	cmpFlagEQ
)

func (f internalCmpFlags) isSet(fl internalCmpFlags) bool {
	return 0 != f&fl
}

// internalCompare implements the Datum interface.
func (d *DBool) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DBool)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if !*d && *v {
		return cmpResultLower
	}
	if *d && !*v {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DInt) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	var v DInt
	switch t := other.(type) {
	case *DInt:
		v = *t
	case *DFloat, *DDecimal:
		// We don't need to call doCompare() here because we know both
		// sides are non-NULL and not OID wrappers.
		// Also since they are non-NULL the value is never cmpObservedNulls so
		// flipping the sign is always correct.
		return -t.internalCompare(ctx, flags, d)
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return cmpResultLower
	}
	if *d > v {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DFloat) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	var v DFloat
	switch t := other.(type) {
	case *DFloat:
		v = *t
	case *DInt:
		v = DFloat(MustBeDInt(t))
	case *DDecimal:
		// We don't need to call doCompare() here because we know both
		// sides are non-NULL and not OID wrappers.
		// Also since they are non-NULL the value is never cmpObservedNulls so
		// flipping the sign is always correct.
		return -t.internalCompare(ctx, flags, d)
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return cmpResultLower
	}
	if *d > v {
		return cmpResultGreater
	}
	// NaN sorts before non-NaN (#10109).
	if *d == v {
		return cmpResultEqual
	}
	if math.IsNaN(float64(*d)) {
		if math.IsNaN(float64(v)) {
			return cmpResultEqual
		}
		return cmpResultLower
	}
	return cmpResultGreater
}

// internalCompare implements the Datum interface.
func (d *DDecimal) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v := ctx.getTmpDec()
	switch t := other.(type) {
	case *DDecimal:
		v = &t.Decimal
	case *DInt:
		v.SetCoefficient(int64(*t)).SetExponent(0)
	case *DFloat:
		if _, err := v.SetFloat64(float64(*t)); err != nil {
			panic(err)
		}
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	// NaNs sort first in SQL.
	if dn, vn := d.Form == apd.NaN, v.Form == apd.NaN; dn && !vn {
		return cmpResultLower
	} else if !dn && vn {
		return cmpResultGreater
	} else if dn && vn {
		return cmpResultEqual
	}
	return internalCmpResult(d.Cmp(v))
}

// internalCompare implements the Datum interface.
func (d *DString) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DString)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < *v {
		return cmpResultLower
	}
	if *d > *v {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DCollatedString) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DCollatedString)
	if !ok || d.Locale != v.Locale {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return internalCmpResult(bytes.Compare(d.Key, v.Key))
}

// internalCompare implements the Datum interface.
func (d *DBytes) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DBytes)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < *v {
		return cmpResultLower
	}
	if *d > *v {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DUuid) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DUuid)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return internalCmpResult(bytes.Compare(d.GetBytes(), v.GetBytes()))
}

// internalCompare implements the Datum interface.
func (d *DIPAddr) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DIPAddr)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}

	return internalCmpResult(d.IPAddr.Compare(&v.IPAddr))
}

// internalCompare implements the Datum interface.
func (d *DDate) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	var v DDate
	switch t := other.(type) {
	case *DDate:
		v = *t
	case *DTimestamp, *DTimestampTZ:
		return internalCmpResult(compareTimestamps(ctx, d, other))
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return cmpResultLower
	}
	if v < *d {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DTime) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	return internalCmpResult(compareTimestamps(ctx, d, other))
}

// internalCompare implements the Datum interface.
func (d *DTimeTZ) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	return internalCmpResult(compareTimestamps(ctx, d, other))
}

// internalCompare implements the Datum interface.
func (d *DTimestamp) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	return internalCmpResult(compareTimestamps(ctx, d, other))
}

// internalCompare implements the Datum interface.
func (d *DTimestampTZ) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	return internalCmpResult(compareTimestamps(ctx, d, other))
}

// internalCompare implements the Datum interface.
func (d *DInterval) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DInterval)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return internalCmpResult(d.Duration.Compare(v.Duration))
}

// internalCompare implements the Datum interface.
func (d *DJSON) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DJSON)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	// No avenue for us to pass up this error here at the moment, but Compare
	// only errors for invalid encoded data.
	// TODO(justin): modify internalCompare to allow passing up errors.
	c, err := d.JSON.Compare(v.JSON)
	if err != nil {
		panic(err)
	}
	// Note: in contrast to tuples and arrays, "nulls" in JSON objects
	// do not mean "unknown" and are values of their own. So they can
	// always be compared to each other and we do not need to consider
	// the compare flags here.
	return internalCmpResult(c)
}

// internalCompare implements the Datum interface.
func (d *DTuple) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DTuple)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	n := len(d.D)
	if n > len(v.D) {
		n = len(v.D)
	}
	sawNull := false
	for i := 0; i < n; i++ {
		// TODO(knz/radu): PostgreSQL does this differently. As of pg 10,
		// values *inside* tuples are always compared using total order,
		// and NULLs do not cause the comparison to yield "unknown". We
		// are deciding here to ignore pg's behavior and instead do "the
		// right thing" and properly determine whether the comparison is
		// unknown. If/when it becomes clear that pg's behavior is more
		// desirable, the following code can be changed to:
		//   if c := TotalOrderCompare(ctx, d.D[i], v.D[i]); c != 0 {
		//      return c
		//   }
		c := doCompare(ctx, flags, d.D[i], v.D[i])
		switch c {
		case cmpResultEqual:
			continue
		case cmpObservedNulls:
			// One of the operands was NULL.
			// If either Datum is NULL and the op is EQ, we continue the
			// comparison and the result is only NULL if the other (non-NULL)
			// elements are equal. This is because NULL is thought of as "unknown",
			// so a NULL equality comparison does not prevent the equality from
			// being proven false, but does prevent it from being proven true.
			if flags.isSet(cmpFlagEQ) {
				sawNull = true
				continue
			}
			fallthrough
		default:
			return c
		}
	}
	if sawNull {
		// The op is EQ and all non-NULL elements are equal, but we saw at least
		// one NULL element. Since NULL comparisons are treated as unknown, the
		// result of the comparison becomes unknown (NULL).
		return cmpObservedNulls
	}
	if len(d.D) < len(v.D) {
		return cmpResultLower
	}
	if len(d.D) > len(v.D) {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// search searches the tuple for the target Datum, returning
// - cmpResultEqual of the datum was found and no NULL participated;
// - != cmpResultEqual, != cmpObserverNull if the datum was not found and no NULL participated;
// - cmpObservedNull if a NULL participated in the comparison.
func (d *DTuple) search(ctx *EvalContext, target Datum) internalCmpResult {
	if len(d.D) == 0 {
		return cmpResultLower
	}
	if d.sorted && !containsNull(target) {
		// If d.sorted is set, the tuple is guaranteed to not contain
		// NULLs in sub-expressions. We also just asserted that target
		// cannot contain NULLs either. So a binary search is well-defined
		// and we can use it.
		i := sort.Search(len(d.D), func(i int) bool {
			return doCompare(ctx, 0, d.D[i], target) >= 0
		})
		if i >= len(d.D) {
			// The binary search failed.
			return cmpResultLower
		}
		return doCompare(ctx, 0, d.D[i], target)
	}

	sawNull := false
	for _, val := range d.D {
		if res := doCompare(ctx, cmpFlagEQ, val, target); res == cmpObservedNulls {
			// If a NULL is encountered, we continue the comparison and
			// the result is only NULL if the lhs is not found in the
			// rhs tuple. This is because NULL is thought of as
			// "unknown".
			sawNull = true
		} else if res == cmpResultEqual {
			return cmpResultEqual
		}
	}
	if sawNull {
		// The value was not found directly in the rhs tuple, but
		// a NULL was observed to participate in the comparison.
		// In that case, we don't really know: return "unknown".
		return cmpObservedNulls
	}
	return cmpResultLower
}

// internalCompare implements the Datum interface.
func (d dNull) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	panic("programming error: doCompare() is responsible for this case")
}

// internalCompare implements the Datum interface.
func (d *DArray) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DArray)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	n := d.Len()
	if n > v.Len() {
		n = v.Len()
	}
	sawNull := false
	for i := 0; i < n; i++ {
		// TODO(knz/radu): PostgreSQL does this differently. As of pg 10,
		// values *inside* arrays are always compared using total order,
		// and NULLs do not cause the comparison to yield "unknown". We
		// are deciding here to ignore pg's behavior and instead do "the
		// right thing" and properly determine whether the comparison is
		// unknown. If/when it becomes clear that pg's behavior is more
		// desirable, the following code can be changed to:
		//   if c := TotalOrderCompare(ctx, d.Array[i], v.Array[i]); c != 0 {
		//      return c
		//   }
		c := doCompare(ctx, flags, d.Array[i], v.Array[i])
		switch c {
		case cmpResultEqual:
			continue
		case cmpObservedNulls:
			// One of the operands was NULL.
			// If either Datum is NULL and the op is EQ, we continue the
			// comparison and the result is only NULL if the other (non-NULL)
			// elements are equal. This is because NULL is thought of as "unknown",
			// so a NULL equality comparison does not prevent the equality from
			// being proven false, but does prevent it from being proven true.
			if flags.isSet(cmpFlagEQ) {
				sawNull = true
				continue
			}
			fallthrough
		default:
			return c
		}
	}
	if sawNull {
		// The op is EQ and all non-NULL elements are equal, but we saw at least
		// one NULL element. Since NULL comparisons are treated as unknown, the
		// result of the comparison becomes unknown (NULL).
		return cmpObservedNulls
	}
	if d.Len() < v.Len() {
		return cmpResultLower
	}
	if d.Len() > v.Len() {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DOid) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	v, ok := other.(*DOid)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if d.DInt < v.DInt {
		return cmpResultLower
	}
	if d.DInt > v.DInt {
		return cmpResultGreater
	}
	return cmpResultEqual
}

// internalCompare implements the Datum interface.
func (d *DOidWrapper) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	return doCompare(ctx, flags, d.Wrapped, other)
}

// internalCompare implements the Datum interface.
func (d *Placeholder) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) internalCmpResult {
	return doCompare(ctx, flags, d.mustGetValue(ctx), other)
}
