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

package duration

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/arith"
)

const (
	daysInMonth   = 30
	nanosInDay    = 24 * int64(time.Hour) // Try as I might, couldn't do this without the cast.
	nanosInMonth  = daysInMonth * nanosInDay
	nanosInSecond = 1000 * 1000 * 1000

	// Used in overflow calculations.
	maxYearsInDuration = math.MaxInt64 / nanosInMonth
	minYearsInDuration = math.MinInt64 / nanosInMonth
)

var (
	bigDaysInMonth  = big.NewInt(daysInMonth)
	bigNanosInDay   = big.NewInt(nanosInDay)
	bigNanosInMonth = big.NewInt(nanosInMonth)
)

// ErrEncodeOverflow is returned by Encode when the sortNanos returned would
// have overflowed or underflowed.
var ErrEncodeOverflow = errors.New("overflow during Encode")

// A Duration represents a length of time.
//
// A duration of "1 month" cannot be represented as a fixed number of
// nanoseconds because the length of months vary. The same is true for days
// because of leap seconds. Given a begin or end time to anchor a duration, the
// nanosecond count can be calculated, but it's useful to represent durations
// such as "1 year 3 months" without an anchor. Duration allows this.
//
// For the purposes of Compare and Encode, 1 month is considered equivalent to
// 30 days and 1 day is equivalent to 24 * 60 * 60 * 1E9 nanoseconds.
//
// TODO(dan): Until the overflow and underflow handling is fixed, this is only
// useful for durations of < 292 years.
type Duration struct {
	Months int64
	Days   int64
	Nanos  int64
}

// Compare returns an integer representing the relative length of two Durations.
// The result will be 0 if d==x, -1 if d < x, and +1 if d > x.
func (d Duration) Compare(x Duration) int {
	normD := d.normalize()
	normX := x.normalize()
	if normD.Months < normX.Months {
		return -1
	} else if normD.Months > normX.Months {
		return 1
	} else if normD.Days < normX.Days {
		return -1
	} else if normD.Days > normX.Days {
		return 1
	} else if normD.Nanos < normX.Nanos {
		return -1
	} else if normD.Nanos > normX.Nanos {
		return 1
	}
	return 0
}

// FromInt64 converts an int64 number of seconds to a
// duration. Inverse conversion of AsInt64.
func FromInt64(x int64) Duration {
	days := x / (nanosInDay / nanosInSecond)
	seconds := x % (nanosInDay / nanosInSecond)
	d := Duration{Days: days, Nanos: seconds * nanosInSecond}
	return d.normalize()
}

// FromFloat64 converts a float64 number of seconds to a
// duration. Inverse conversion of AsFloat64.
func FromFloat64(x float64) Duration {
	months := int64(x / float64(nanosInMonth/nanosInSecond))
	secDays := math.Mod(x, float64(nanosInMonth/nanosInSecond))
	days := int64(secDays / float64(nanosInDay/nanosInSecond))
	secsRem := math.Mod(secDays, float64(nanosInDay/nanosInSecond))
	d := Duration{Months: months, Days: days, Nanos: int64(secsRem * 1e9)}
	return d.normalize()
}

// FromBigInt converts a big.Int number of nanoseconds to a
// duration. Inverse conversion of AsBigInt. Boolean false
// if the result overflows.
func FromBigInt(src *big.Int) (Duration, bool) {
	var rem big.Int
	var monthsDec big.Int
	monthsDec.QuoRem(src, bigNanosInMonth, &rem)
	if !monthsDec.IsInt64() {
		return Duration{}, false
	}

	var daysDec big.Int
	var nanosRem big.Int
	daysDec.QuoRem(&rem, bigNanosInDay, &nanosRem)
	// Note: we do not need to check for overflow of daysDec because any
	// excess bits were spilled into months above already.

	d := Duration{Months: monthsDec.Int64(), Days: daysDec.Int64(), Nanos: nanosRem.Int64()}
	return d.normalize(), true
}

// AsInt64 converts a duration to an int64 number of seconds.
// The conversion may overflow, in which case the boolean return
// value is false.
func (d Duration) AsInt64() (int64, bool) {
	mSecs, ok := arith.MulHalfPositiveWithOverflow(d.Months, nanosInMonth/nanosInSecond)
	if !ok {
		return 0, ok
	}
	dSecs, ok := arith.MulHalfPositiveWithOverflow(d.Days, nanosInDay/nanosInSecond)
	if !ok {
		return 0, ok
	}
	if dSecs, ok = arith.AddWithOverflow(mSecs, dSecs); !ok {
		return 0, ok
	}
	return arith.AddWithOverflow(dSecs, d.Nanos/nanosInSecond)
}

// AsFloat64 converts a duration to a float64 number of seconds.
func (d Duration) AsFloat64() float64 {
	mSecs := float64(d.Months) * float64(nanosInMonth/nanosInSecond)
	dSecs := float64(d.Days) * float64(nanosInDay/nanosInSecond)
	return float64(d.Nanos)/float64(nanosInSecond) + mSecs + dSecs
}

// AsBigInt converts a duration to a big.Int with the number of
// nanoseconds.
func (d Duration) AsBigInt(dst *big.Int) {
	dst.SetInt64(d.Months)
	dst.Mul(dst, bigDaysInMonth)
	dst.Add(dst, big.NewInt(d.Days))
	dst.Mul(dst, bigNanosInDay)
	dst.Add(dst, big.NewInt(d.Nanos))
}

// Format emits a string representation of a Duration to a Buffer.
func (d Duration) Format(buf *bytes.Buffer) {
	if d.Nanos == 0 && d.Days == 0 && d.Months == 0 {
		buf.WriteString("0s")
		return
	}

	if absGE(d.Months, 11) {
		fmt.Fprintf(buf, "%dy", d.Months/12)
		d.Months %= 12
	}
	if d.Months != 0 {
		fmt.Fprintf(buf, "%dmon", d.Months)
	}
	if d.Days != 0 {
		fmt.Fprintf(buf, "%dd", d.Days)
	}

	// The following comparisons are careful to preserve the sign in
	// case the value is MinInt64, and thus cannot be made positive lest
	// an overflow occur.
	if absGE(d.Nanos, time.Hour.Nanoseconds()) {
		fmt.Fprintf(buf, "%dh", d.Nanos/time.Hour.Nanoseconds())
		d.Nanos %= time.Hour.Nanoseconds()
	}
	if absGE(d.Nanos, time.Minute.Nanoseconds()) {
		fmt.Fprintf(buf, "%dm", d.Nanos/time.Minute.Nanoseconds())
		d.Nanos %= time.Minute.Nanoseconds()
	}
	if absGE(d.Nanos, time.Second.Nanoseconds()) {
		fmt.Fprintf(buf, "%ds", d.Nanos/time.Second.Nanoseconds())
		d.Nanos %= time.Second.Nanoseconds()
	}
	if absGE(d.Nanos, time.Millisecond.Nanoseconds()) {
		fmt.Fprintf(buf, "%dms", d.Nanos/time.Millisecond.Nanoseconds())
		d.Nanos %= time.Millisecond.Nanoseconds()
	}
	if absGE(d.Nanos, time.Microsecond.Nanoseconds()) {
		fmt.Fprintf(buf, "%dµs", d.Nanos/time.Microsecond.Nanoseconds())
		d.Nanos %= time.Microsecond.Nanoseconds()
	}
	if d.Nanos != 0 {
		fmt.Fprintf(buf, "%dns", d.Nanos)
	}
}

// absGE returns whether x is greater than or equal to y in magnitude.
// y is always positive, x may be negative.
func absGE(x, y int64) bool {
	if x < 0 {
		return x <= -y
	}
	return x >= y
}

// String returns a string representation of a Duration.
func (d Duration) String() string {
	var buf bytes.Buffer
	d.Format(&buf)
	return buf.String()
}

// Encode returns three integers such that the original Duration is recoverable
// (using Decode) and the first int will approximately sort a collection of
// encoded Durations.
func (d Duration) Encode() (sortNanos int64, months int64, days int64, err error) {
	// The number of whole years equivalent to any value of Duration always fits
	// in an int64. Use this to compute a conservative estimate of overflow.
	//
	// TODO(dan): Compute overflow exactly, then document that EncodeBigInt can be
	// used in overflow cases.
	years := d.Months/12 + d.Days/daysInMonth/12 + d.Nanos/nanosInMonth/12
	if years > maxYearsInDuration || years < minYearsInDuration {
		return 0, 0, 0, ErrEncodeOverflow
	}

	totalNanos := d.Months*nanosInMonth + d.Days*nanosInDay + d.Nanos
	return totalNanos, d.Months, d.Days, nil
}

// EncodeBigInt is the same as Encode, except that it always returns
// successfully and is slower.
func (d Duration) EncodeBigInt() (sortNanos *big.Int, months int64, days int64) {
	bigMonths := big.NewInt(d.Months)
	bigMonths.Mul(bigMonths, big.NewInt(nanosInMonth))
	bigDays := big.NewInt(d.Days)
	bigDays.Mul(bigDays, big.NewInt(nanosInDay))
	totalNanos := big.NewInt(d.Nanos)
	totalNanos.Add(totalNanos, bigMonths).Add(totalNanos, bigDays)
	return totalNanos, d.Months, d.Days
}

// Decode reverses the three integers returned from Encode and produces an equal
// Duration to the original.
func Decode(sortNanos int64, months int64, days int64) (Duration, error) {
	nanos := sortNanos - months*nanosInMonth - days*nanosInDay
	// TODO(dan): Handle underflow, then document that DecodeBigInt can be used
	// in underflow cases.
	return Duration{Months: months, Days: days, Nanos: nanos}, nil
}

// TODO(dan): Write DecodeBigInt.

// AdditionMode controls date normalization behaviors in Add().
type AdditionMode bool

const (
	// AdditionModeCompatible applies a date-normalization strategy
	// which produces results which are more compatible with
	// PostgreSQL in which adding a month "rounds down" to the last
	// day of a month instead of producing a value in the following
	// month.
	//
	// See PostgreSQL 10.5: src/backend/utils/adt/timestamp.c timestamp_pl_interval().
	AdditionModeCompatible AdditionMode = false
	// AdditionModeLegacy delegates to time.Time.AddDate() for
	// performing Time+Duration math.
	AdditionModeLegacy AdditionMode = true
)

func (m AdditionMode) String() string {
	switch m {
	case AdditionModeLegacy:
		return "legacy"
	default:
		return "compatible"
	}
}

// Context is used to prevent a package-dependency cycle via tree.EvalContext.
type Context interface {
	GetAdditionMode() AdditionMode
}

// GetAdditionMode allows an AdditionMode to be used as its own context.
func (m AdditionMode) GetAdditionMode() AdditionMode {
	return m
}

// Add returns the time t+d, using a configurable mode.
func Add(ctx Context, t time.Time, d Duration) time.Time {
	var mode AdditionMode
	if ctx != nil {
		mode = ctx.GetAdditionMode()
	}

	// We can fast-path if the duration is always a fixed amount of time,
	// or if the day number that we're starting from can never result
	// in normalization.
	if mode == AdditionModeLegacy || d.Months == 0 || t.Day() <= 28 {
		return t.AddDate(0, int(d.Months), int(d.Days)).Add(time.Duration(d.Nanos))
	}

	// Adjustments for 1-based math.
	expectedMonth := time.Month((int(t.Month())-1+int(d.Months))%12 + 1)

	// Use AddDate() to get a rough value.  This might overshoot the
	// end of the expected month by multiple days.  We could iteratively
	// subtract a day until we jump a month backwards, but that's
	// at least twice as slow as computing the correct value ourselves.
	res := t.AddDate(0 /* years */, int(d.Months), 0 /* days */)

	// Unpack fields as little as possible.
	year, month, _ := res.Date()
	hour, min, sec := res.Clock()

	if month != expectedMonth {
		// Pro-tip: Count across your knuckles and the divots between
		// them, wrapping around when you hit July. Knuckle == 31 days.
		var lastDayOfMonth int
		switch expectedMonth {
		case time.February:
			// Leap year if divisible by 4, but not centuries unless also divisible by 400.
			// Adjust the earth's orbital parameters?
			if year%4 == 0 && (year%100 != 0 || year%400 == 0) {
				lastDayOfMonth = 29
			} else {
				lastDayOfMonth = 28
			}
		case time.January, time.March, time.May, time.July, time.August, time.October, time.December:
			lastDayOfMonth = 31
		default:
			lastDayOfMonth = 30
		}

		res = time.Date(
			year, expectedMonth, lastDayOfMonth,
			hour, min, sec,
			res.Nanosecond(), res.Location())
	}

	return res.AddDate(0, 0, int(d.Days)).Add(time.Duration(d.Nanos))
}

// Add returns a Duration representing a time length of d+x.
func (d Duration) Add(x Duration) Duration {
	return Duration{d.Months + x.Months, d.Days + x.Days, d.Nanos + x.Nanos}
}

// Sub returns a Duration representing a time length of d-x.
func (d Duration) Sub(x Duration) Duration {
	return Duration{d.Months - x.Months, d.Days - x.Days, d.Nanos - x.Nanos}
}

// Mul returns a Duration representing a time length of d*x.
func (d Duration) Mul(x int64) Duration {
	return Duration{d.Months * x, d.Days * x, d.Nanos * x}
}

// Div returns a Duration representing a time length of d/x.
func (d Duration) Div(x int64) Duration {
	return Duration{d.Months / x, d.Days / x, d.Nanos / x}
}

// MulFloat returns a Duration representing a time length of d*x.
func (d Duration) MulFloat(x float64) Duration {
	return Duration{
		int64(float64(d.Months) * x),
		int64(float64(d.Days) * x),
		int64(float64(d.Nanos) * x),
	}
}

// DivFloat returns a Duration representing a time length of d/x.
func (d Duration) DivFloat(x float64) Duration {
	return Duration{
		int64(float64(d.Months) / x),
		int64(float64(d.Days) / x),
		int64(float64(d.Nanos) / x),
	}
}

// normalized returns a new Duration transformed using the equivalence rules.
// Each quantity of days greater than the threshold is moved into months,
// likewise for nanos. Integer overflow is avoided by partial transformation.
func (d Duration) normalize() Duration {
	if d.Days > 0 {
		d = d.shiftPosDaysToMonths()
	} else if d.Days < 0 {
		d = d.shiftNegDaysToMonths()
	}
	// After shifting days into months, there are two cases:
	// - Months did not hit MaxInt64 or MinInt64, in which case Days is now in
	//   (-30,30). We shift nanos, then days one more time in case the nano shift
	//   made a full month.
	// - Months did hit MaxInt64 or MinInt64, in which case there can be no more
	//   months. We only need to shift nanos.
	if d.Nanos > 0 {
		d = d.shiftPosNanosToDays()
		d = d.shiftPosDaysToMonths()
	} else if d.Nanos < 0 {
		d = d.shiftNegNanosToDays()
		d = d.shiftNegDaysToMonths()
	}
	return d
}

func (d Duration) shiftPosDaysToMonths() Duration {
	var maxMonths = int64(math.MaxInt64)
	if d.Months > 0 {
		// If d.Months < 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		maxMonths = math.MaxInt64 - d.Months
	}
	monthsFromDays := int64Min(d.Days/daysInMonth, maxMonths)
	d.Months += monthsFromDays
	d.Days -= monthsFromDays * daysInMonth
	return d
}

func (d Duration) shiftPosNanosToDays() Duration {
	var maxDays = int64(math.MaxInt64)
	if d.Days > 0 {
		// If d.Days < 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		maxDays = math.MaxInt64 - d.Days
	}
	daysFromNanos := int64Min(d.Nanos/nanosInDay, maxDays)
	d.Days += daysFromNanos
	d.Nanos -= daysFromNanos * nanosInDay
	return d
}

func (d Duration) shiftNegDaysToMonths() Duration {
	var minMonths = int64(math.MinInt64)
	if d.Months < 0 {
		// If d.Months > 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		minMonths = math.MinInt64 - d.Months
	}
	monthsFromDays := int64Max(d.Days/daysInMonth, minMonths)
	d.Months += monthsFromDays
	d.Days -= monthsFromDays * daysInMonth
	return d
}

func (d Duration) shiftNegNanosToDays() Duration {
	var minDays = int64(math.MinInt64)
	if d.Days < 0 {
		// If d.Days > 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		minDays = math.MinInt64 - d.Days
	}
	daysFromNanos := int64Max(d.Nanos/nanosInDay, minDays)
	d.Days += daysFromNanos
	d.Nanos -= daysFromNanos * nanosInDay
	return d
}

func int64Max(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func int64Min(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

const (
	minTimeDuration time.Duration = -1 << 63
	maxTimeDuration time.Duration = 1<<63 - 1
)

// DiffMicros computes the microsecond difference between two time values. The reason
// this function is necessary even though time.Sub(time) exists is that time.Duration
// can only hold values up to ~290 years, because it stores duration at the nanosecond
// resolution. This function should be used if a difference of more than 290 years is
// possible between time values, and a microsecond resolution is acceptable.
func DiffMicros(t1, t2 time.Time) int64 {
	micros := int64(0)
	nanos := time.Duration(0)
	for {
		// time.Sub(time) can overflow for durations larger than ~290 years, so
		// we need to perform this diff iteratively. If this method overflows,
		// it will return either minTimeDuration or maxTimeDuration.
		d := t1.Sub(t2)
		overflow := d == minTimeDuration || d == maxTimeDuration
		if d == minTimeDuration {
			// We use -maxTimeDuration here because -minTimeDuration would overflow.
			d = -maxTimeDuration
		}
		micros += int64(d / time.Microsecond)
		nanos += d % time.Microsecond
		if !overflow {
			break
		}
		t1 = t1.Add(-d)
	}
	micros += int64(nanos / time.Microsecond)
	nanoRem := nanos % time.Microsecond
	if nanoRem >= time.Microsecond/2 {
		micros++
	} else if nanoRem <= -time.Microsecond/2 {
		micros--
	}
	return micros
}

// AddMicros adds the microsecond delta to the provided time value. The reason
// this function is necessary even though time.Add(duration) exists is that time.Duration
// can only hold values up to ~290 years, because it stores duration at the nanosecond
// resolution. This function makes it possible to add more than 290 years to a time.Time,
// at the tradeoff of working on a microsecond resolution.
func AddMicros(t time.Time, d int64) time.Time {
	negMult := time.Duration(1)
	if d < 0 {
		negMult = -1
		d = -d
	}
	const maxMicroDur = int64(maxTimeDuration / time.Microsecond)
	for d > maxMicroDur {
		const maxWholeNanoDur = time.Duration(maxMicroDur) * time.Microsecond
		t = t.Add(negMult * maxWholeNanoDur)
		d -= maxMicroDur
	}
	return t.Add(negMult * time.Duration(d) * time.Microsecond)
}

// Truncate returns a new duration obtained from the first argument
// by discarding the portions at finer resolution than that given by the
// second argument.
// Example: Truncate(time.Second+1, time.Second) == time.Second.
func Truncate(d time.Duration, r time.Duration) time.Duration {
	if r == 0 {
		panic("zero passed as resolution")
	}
	return d - (d % r)
}
