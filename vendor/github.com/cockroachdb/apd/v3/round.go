// Copyright 2016 The Cockroach Authors.
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

package apd

// Round sets d to rounded x, rounded to the precision specified by c. If c
// has zero precision, no rounding will occur. If c has no Rounding specified,
// RoundHalfUp is used.
func (c *Context) Round(d, x *Decimal) (Condition, error) {
	return c.goError(c.round(d, x))
}

//gcassert:inline
func (c *Context) round(d, x *Decimal) Condition {
	return c.Rounding.Round(c, d, x, true /* disableIfPrecisionZero */)
}

// Rounder specifies the behavior of rounding.
type Rounder string

// ShouldAddOne returns true if 1 should be added to the absolute value
// of a number being rounded. result is the result to which the 1 would
// be added. neg is true if the number is negative. half is -1 if the
// discarded digits are < 0.5, 0 if = 0.5, or 1 if > 0.5.
func (r Rounder) ShouldAddOne(result *BigInt, neg bool, half int) bool {
	// NOTE: this is written using a switch statement instead of some
	// other form of dynamic dispatch to assist Go's escape analysis.
	switch r {
	case RoundDown:
		return roundDown(result, neg, half)
	case RoundHalfUp:
		return roundHalfUp(result, neg, half)
	case RoundHalfEven:
		return roundHalfEven(result, neg, half)
	case RoundCeiling:
		return roundCeiling(result, neg, half)
	case RoundFloor:
		return roundFloor(result, neg, half)
	case RoundHalfDown:
		return roundHalfDown(result, neg, half)
	case RoundUp:
		return roundUp(result, neg, half)
	case Round05Up:
		return round05Up(result, neg, half)
	default:
		return roundHalfUp(result, neg, half)
	}
}

// Round sets d to rounded x.
func (r Rounder) Round(c *Context, d, x *Decimal, disableIfPrecisionZero bool) Condition {
	d.Set(x)
	nd := x.NumDigits()
	xs := x.Sign()
	var res Condition

	if disableIfPrecisionZero && c.Precision == 0 {
		// Rounding has been disabled.
		return d.setExponent(c, nd, res, int64(d.Exponent))
	}

	// adj is the adjusted exponent: exponent + clength - 1
	if adj := int64(x.Exponent) + nd - 1; xs != 0 && adj < int64(c.MinExponent) {
		// Subnormal is defined before rounding.
		res |= Subnormal
		// setExponent here to prevent double-rounded subnormals.
		res |= d.setExponent(c, nd, res, int64(d.Exponent))
		return res
	}

	diff := nd - int64(c.Precision)
	if diff > 0 {
		if diff > MaxExponent {
			return SystemOverflow | Overflow
		}
		if diff < MinExponent {
			return SystemUnderflow | Underflow
		}
		res |= Rounded
		var y, m BigInt
		e := tableExp10(diff, &y)
		y.QuoRem(&d.Coeff, e, &m)
		if m.Sign() != 0 {
			res |= Inexact
			var discard Decimal
			discard.Coeff.Set(&m)
			discard.Exponent = int32(-diff)
			if r.ShouldAddOne(&y, x.Negative, discard.Cmp(decimalHalf)) {
				roundAddOne(&y, &diff)
			}
		}
		d.Coeff.Set(&y)
		// The coefficient changed, so recompute num digits in setExponent.
		nd = unknownNumDigits
	} else {
		diff = 0
	}
	res |= d.setExponent(c, nd, res, int64(d.Exponent), diff)
	return res
}

// roundAddOne adds 1 to abs(b).
func roundAddOne(b *BigInt, diff *int64) {
	if b.Sign() < 0 {
		panic("unexpected negative")
	}
	nd := NumDigits(b)
	b.Add(b, bigOne)
	nd2 := NumDigits(b)
	if nd2 > nd {
		b.Quo(b, bigTen)
		*diff++
	}
}

// roundings is a set containing all available Rounders.
var roundings = map[Rounder]struct{}{
	RoundDown:     {},
	RoundHalfUp:   {},
	RoundHalfEven: {},
	RoundCeiling:  {},
	RoundFloor:    {},
	RoundHalfDown: {},
	RoundUp:       {},
	Round05Up:     {},
}

const (
	// RoundDown rounds toward 0; truncate.
	RoundDown Rounder = "down"
	// RoundHalfUp rounds up if the digits are >= 0.5.
	RoundHalfUp Rounder = "half_up"
	// RoundHalfEven rounds up if the digits are > 0.5. If the digits are equal
	// to 0.5, it rounds up if the previous digit is odd, always producing an
	// even digit.
	RoundHalfEven Rounder = "half_even"
	// RoundCeiling towards +Inf: rounds up if digits are > 0 and the number
	// is positive.
	RoundCeiling Rounder = "ceiling"
	// RoundFloor towards -Inf: rounds up if digits are > 0 and the number
	// is negative.
	RoundFloor Rounder = "floor"
	// RoundHalfDown rounds up if the digits are > 0.5.
	RoundHalfDown Rounder = "half_down"
	// RoundUp rounds away from 0.
	RoundUp Rounder = "up"
	// Round05Up rounds zero or five away from 0; same as round-up, except that
	// rounding up only occurs if the digit to be rounded up is 0 or 5.
	Round05Up Rounder = "05up"
)

func roundDown(result *BigInt, neg bool, half int) bool {
	return false
}

func roundUp(result *BigInt, neg bool, half int) bool {
	return true
}

func round05Up(result *BigInt, neg bool, half int) bool {
	var z BigInt
	z.Rem(result, bigFive)
	if z.Sign() == 0 {
		return true
	}
	z.Rem(result, bigTen)
	return z.Sign() == 0
}

func roundHalfUp(result *BigInt, neg bool, half int) bool {
	return half >= 0
}

func roundHalfEven(result *BigInt, neg bool, half int) bool {
	if half > 0 {
		return true
	}
	if half < 0 {
		return false
	}
	return result.Bit(0) == 1
}

func roundHalfDown(result *BigInt, neg bool, half int) bool {
	return half > 0
}

func roundFloor(result *BigInt, neg bool, half int) bool {
	return neg
}

func roundCeiling(result *BigInt, neg bool, half int) bool {
	return !neg
}
