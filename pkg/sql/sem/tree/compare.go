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
	return doCompare(ctx, cmpFlagOrderedNulls, a, b)
}

// Distinct returns true if and only if a and b are distinct
// from each other. NULLs are considered to not be distinct from each
// other but are distinct from every other value.
func Distinct(ctx *EvalContext, a, b Datum) bool {
	return doCompare(ctx, cmpFlagOrderedNulls, a, b) != 0
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
	cmp := doCompare(ctx, fl, a, b)
	if cmp == -2 {
		return DNull
	}

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

// doCompare is the main function.
func doCompare(ctx *EvalContext, flags internalCmpFlags, a, b Datum) int {
	// TODO(knz): this should not use internalCompare any more.
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
func (d *DBool) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DBool)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if !*d && *v {
		return -1
	}
	if *d && !*v {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DInt) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	var v DInt
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DInt:
		v = *t
	case *DFloat, *DDecimal:
		return -t.internalCompare(ctx, flags, d)
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return -1
	}
	if *d > v {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DFloat) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	var v DFloat
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DFloat:
		v = *t
	case *DInt:
		v = DFloat(MustBeDInt(t))
	case *DDecimal:
		return -t.internalCompare(ctx, flags, d)
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return -1
	}
	if *d > v {
		return 1
	}
	// NaN sorts before non-NaN (#10109).
	if *d == v {
		return 0
	}
	if math.IsNaN(float64(*d)) {
		if math.IsNaN(float64(v)) {
			return 0
		}
		return -1
	}
	return 1
}

// internalCompare implements the Datum interface.
func (d *DDecimal) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v := ctx.getTmpDec()
	switch t := UnwrapDatum(ctx, other).(type) {
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
		return -1
	} else if !dn && vn {
		return 1
	} else if dn && vn {
		return 0
	}
	return d.Cmp(v)
}

// internalCompare implements the Datum interface.
func (d *DString) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DString)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DCollatedString) internalCompare(
	ctx *EvalContext, flags internalCmpFlags, other Datum,
) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DCollatedString)
	if !ok || d.Locale != v.Locale {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return bytes.Compare(d.Key, v.Key)
}

// internalCompare implements the Datum interface.
func (d *DBytes) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DBytes)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < *v {
		return -1
	}
	if *d > *v {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DUuid) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DUuid)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return bytes.Compare(d.GetBytes(), v.GetBytes())
}

// internalCompare implements the Datum interface.
func (d *DIPAddr) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DIPAddr)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}

	return d.IPAddr.Compare(&v.IPAddr)
}

// internalCompare implements the Datum interface.
func (d *DDate) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	var v DDate
	switch t := UnwrapDatum(ctx, other).(type) {
	case *DDate:
		v = *t
	case *DTimestamp, *DTimestampTZ:
		return compareTimestamps(ctx, d, other)
	default:
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if *d < v {
		return -1
	}
	if v < *d {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DTime) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	return compareTimestamps(ctx, d, other)
}

// internalCompare implements the Datum interface.
func (d *DTimeTZ) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	return compareTimestamps(ctx, d, other)
}

// internalCompare implements the Datum interface.
func (d *DTimestamp) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	return compareTimestamps(ctx, d, other)
}

// internalCompare implements the Datum interface.
func (d *DTimestampTZ) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	return compareTimestamps(ctx, d, other)
}

// internalCompare implements the Datum interface.
func (d *DInterval) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DInterval)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	return d.Duration.Compare(v.Duration)
}

// internalCompare implements the Datum interface.
func (d *DJSON) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DJSON)
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
	return c
}

// internalCompare implements the Datum interface.
func (d *DTuple) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DTuple)
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
		c := d.D[i].internalCompare(ctx, flags, v.D[i])
		switch c {
		case 0:
			continue
		case -2:
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
		return -2
	}
	if len(d.D) < len(v.D) {
		return -1
	}
	if len(d.D) > len(v.D) {
		return 1
	}
	return 0
}

// search searches the tuple for the target Datum, returning
// - 0 of the datum was found and no NULL participated;
// - +1 or -1 if the datum was not found and no NULL participated;
// - -2 if a NULL participated in the comparison.
func (d *DTuple) search(ctx *EvalContext, target Datum) (res int) {
	if len(d.D) == 0 {
		return -1
	}
	if d.sorted && !containsNull(target) {
		// If d.sorted is set, the tuple is guaranteed to not contain
		// NULLs in sub-expressions. We also just asserted that target
		// cannot contain NULLs either. So a binary search is well-defined
		// and we can use it.
		i := sort.Search(len(d.D), func(i int) bool {
			return d.D[i].internalCompare(ctx, 0, target) >= 0
		})
		if i >= len(d.D) {
			// The binary search failed.
			return -1
		}
		return d.D[i].internalCompare(ctx, 0, target)
	}

	sawNull := false
	for _, val := range d.D {
		if res := val.internalCompare(ctx, cmpFlagEQ, target); res == -2 {
			// If a NULL is encountered, we continue the comparison and
			// the result is only NULL if the lhs is not found in the
			// rhs tuple. This is because NULL is thought of as
			// "unknown".
			sawNull = true
		} else if res == 0 {
			return 0
		}
	}
	if sawNull {
		// The value was not found directly in the rhs tuple, but
		// a NULL was observed to participate in the comparison.
		// In that case, we don't really know: return "unknown".
		return -2
	}
	return -1
}

// internalCompare implements the Datum interface.
func (d dNull) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if flags.isSet(cmpFlagOrderedNulls) {
		// NULL is less than any non-NULL value.
		if other == DNull {
			return 0
		}
		return -1
	}
	return -2
}

// internalCompare implements the Datum interface.
func (d *DArray) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DArray)
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
		c := d.Array[i].internalCompare(ctx, flags, v.Array[i])
		switch c {
		case 0:
			continue
		case -2:
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
		return -2
	}
	if d.Len() < v.Len() {
		return -1
	}
	if d.Len() > v.Len() {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DOid) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	v, ok := UnwrapDatum(ctx, other).(*DOid)
	if !ok {
		panic(makeUnsupportedComparisonMessage(d, other))
	}
	if d.DInt < v.DInt {
		return -1
	}
	if d.DInt > v.DInt {
		return 1
	}
	return 0
}

// internalCompare implements the Datum interface.
func (d *DOidWrapper) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	if other == DNull {
		if flags.isSet(cmpFlagOrderedNulls) {
			// NULL is less than any non-NULL value.
			return 1
		}
		return -2
	}
	if v, ok := other.(*DOidWrapper); ok {
		return d.Wrapped.internalCompare(ctx, flags, v.Wrapped)
	}
	return d.Wrapped.internalCompare(ctx, flags, other)
}

// internalCompare implements the Datum interface.
func (d *Placeholder) internalCompare(ctx *EvalContext, flags internalCmpFlags, other Datum) int {
	return d.mustGetValue(ctx).internalCompare(ctx, flags, other)
}
