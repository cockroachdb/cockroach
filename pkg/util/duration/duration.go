// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package duration

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/errors"
)

const (
	// MicrosPerMilli is the amount of microseconds in a millisecond.
	MicrosPerMilli = 1000
	// MillisPerSec is the amount of seconds in a millisecond.
	MillisPerSec = 1000
	// SecsPerMinute is the amount of seconds in a minute.
	SecsPerMinute = 60
	// SecsPerHour is the amount of seconds in an hour.
	SecsPerHour = 3600
	// SecsPerDay is the amount of seconds in a day.
	SecsPerDay = 86400
	// DaysPerMonth is the assumed amount of days in a month.
	// is always evaluated to 30, as it is in postgres.
	DaysPerMonth = 30
	// DaysPerYear is the number of days in a year.
	// It is assumed to include a quarter day to account for the leap year.
	// Matches DAYS_PER_YEAR in postgres.
	DaysPerYear = 365.25
	// MonthsPerYear is the amount of months in the year.
	MonthsPerYear = 12
)

const (
	nanosInDay    = 24 * int64(time.Hour) // Try as I might, couldn't do this without the cast.
	nanosInMonth  = DaysPerMonth * nanosInDay
	nanosInSecond = 1000 * 1000 * 1000
	nanosInMicro  = 1000

	// Used in overflow calculations.
	maxYearsInDuration = math.MaxInt64 / nanosInMonth
	minYearsInDuration = math.MinInt64 / nanosInMonth
)

var (
	bigDaysInMonth  = big.NewInt(DaysPerMonth)
	bigNanosInDay   = big.NewInt(nanosInDay)
	bigNanosInMonth = big.NewInt(nanosInMonth)
)

// errEncodeOverflow is returned by Encode when the sortNanos returned would
// have overflowed or underflowed.
var errEncodeOverflow = pgerror.WithCandidateCode(errors.New("overflow during Encode"), pgcode.IntervalFieldOverflow)

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
// Although the Nanos field is a number of nanoseconds, all operations
// round to the nearest microsecond. Any setting of this field should avoid
// setting with precision below microseconds. The only exceptions are the
// encode/decode operations.
//
// TODO(dan): Until the overflow and underflow handling is fixed, this is only
// useful for durations of < 292 years.
type Duration struct {
	Months int64
	Days   int64
	// nanos is an unexported field so that it cannot be misused by other
	// packages. It should almost always be rounded to the nearest microsecond.
	nanos int64
}

// MakeDuration returns a Duration rounded to the nearest microsecond.
func MakeDuration(nanos, days, months int64) Duration {
	return Duration{
		Months: months,
		Days:   days,
		nanos:  rounded(nanos),
	}
}

// DecodeDuration returns a Duration without rounding nanos.
func DecodeDuration(months, days, nanos int64) Duration {
	return Duration{
		Months: months,
		Days:   days,
		nanos:  nanos,
	}
}

// Nanos returns the nanos of d.
func (d Duration) Nanos() int64 {
	return d.nanos
}

// SetNanos rounds and sets nanos.
func (d *Duration) SetNanos(nanos int64) {
	d.nanos = rounded(nanos)
}

// round rounds nanos to the nearest microsecond.
func (d Duration) round() Duration {
	d.nanos = rounded(d.nanos)
	return d
}

// rounded returns nanos rounded to the nearest microsecond.
func (d Duration) rounded() int64 {
	return rounded(d.nanos)
}

// rounded returns nanos rounded to the nearest microsecond.
func rounded(nanos int64) int64 {
	dur := time.Duration(nanos) * time.Nanosecond
	v := dur.Round(time.Microsecond).Nanoseconds()
	// Near the boundaries of int64 will return the argument unchanged. Check
	// for those cases and truncate instead of round so that we never have nanos.
	if m := v % nanosInMicro; m != 0 {
		v -= m
	}
	return v
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
	} else if normD.nanos < normX.nanos {
		return -1
	} else if normD.nanos > normX.nanos {
		return 1
	}
	return 0
}

// FromInt64 converts an int64 number of seconds to a
// duration. Inverse conversion of AsInt64.
func FromInt64(x int64) Duration {
	days := x / (nanosInDay / nanosInSecond)
	seconds := x % (nanosInDay / nanosInSecond)
	d := Duration{Days: days, nanos: seconds * nanosInSecond}
	return d.normalize()
}

// FromFloat64 converts a float64 number of seconds to a duration. Inverse
// conversion of AsFloat64.
func FromFloat64(x float64) Duration {
	months := int64(x / float64(nanosInMonth/nanosInSecond))
	secDays := math.Mod(x, float64(nanosInMonth/nanosInSecond))
	days := int64(secDays / float64(nanosInDay/nanosInSecond))
	secsRem := math.Mod(secDays, float64(nanosInDay/nanosInSecond))
	d := Duration{Months: months, Days: days, nanos: int64(secsRem * 1e9)}
	return d.normalize().round()
}

// FromBigInt converts a big.Int number of nanoseconds to a duration. Inverse
// conversion of AsBigInt. Boolean false if the result overflows.
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

	d := Duration{Months: monthsDec.Int64(), Days: daysDec.Int64(), nanos: nanosRem.Int64()}
	return d.normalize().round(), true
}

// AsInt64 converts a duration to an int64 number of seconds.
// The conversion may overflow, in which case the boolean return
// value is false.
func (d Duration) AsInt64() (int64, bool) {
	numYears := d.Months / 12
	numMonthsInYear := d.Months % 12
	// To do overflow detection with years, we have to convert to float in order
	// to maintain accuracy, as DaysPerYear is a floating point number.
	ySecs := float64(numYears*SecsPerDay) * DaysPerYear
	// Since float has a higher range than int, fail if the number of seconds in the year
	// value is greater than what an int can handle.
	if ySecs > float64(math.MaxInt64) || ySecs < float64(math.MinInt64) {
		return 0, false
	}
	mSecs, ok := arith.MulHalfPositiveWithOverflow(numMonthsInYear, nanosInMonth/nanosInSecond)
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
	if dSecs, ok = arith.AddWithOverflow(dSecs, int64(ySecs)); !ok {
		return 0, ok
	}
	return arith.AddWithOverflow(dSecs, d.nanos/nanosInSecond)
}

// AsFloat64 converts a duration to a float64 number of seconds.
func (d Duration) AsFloat64() float64 {
	numYears := d.Months / 12
	numMonthsInYear := d.Months % 12
	return (float64(d.Nanos()) / float64(time.Second)) +
		float64(d.Days*SecsPerDay) +
		float64(numYears*SecsPerDay)*DaysPerYear +
		float64(numMonthsInYear*DaysPerMonth*SecsPerDay)
}

// AsBigInt converts a duration to a big.Int with the number of nanoseconds.
func (d Duration) AsBigInt(dst *big.Int) {
	dst.SetInt64(d.Months)
	dst.Mul(dst, bigDaysInMonth)
	dst.Add(dst, big.NewInt(d.Days))
	dst.Mul(dst, bigNanosInDay)
	// Uses rounded instead of nanos here to remove any on-disk nanos.
	dst.Add(dst, big.NewInt(d.rounded()))
}

const (
	hourNanos   = uint64(time.Hour / time.Nanosecond)
	minuteNanos = uint64(time.Minute / time.Nanosecond)
	secondNanos = uint64(time.Second / time.Nanosecond)
)

// Format emits a string representation of a Duration to a Buffer truncated to microseconds.
func (d Duration) Format(buf *bytes.Buffer) {
	if d.nanos == 0 && d.Days == 0 && d.Months == 0 {
		buf.WriteString("00:00:00")
		return
	}

	wrote := false
	// Defining both arguments in the signature gives a 10% speedup.
	wrotePrev := func(wrote bool, buf *bytes.Buffer) bool {
		if wrote {
			buf.WriteString(" ")
		}
		return true
	}

	negDays := d.Months < 0 || d.Days < 0
	if absGE(d.Months, 12) {
		years := d.Months / 12
		wrote = wrotePrev(wrote, buf)
		fmt.Fprintf(buf, "%d year%s", years, isPlural(years))
		d.Months %= 12
	}
	if d.Months != 0 {
		wrote = wrotePrev(wrote, buf)
		fmt.Fprintf(buf, "%d mon%s", d.Months, isPlural(d.Months))
	}
	if d.Days != 0 {
		wrote = wrotePrev(wrote, buf)
		fmt.Fprintf(buf, "%d day%s", d.Days, isPlural(d.Days))
	}

	if d.nanos == 0 {
		return
	}

	wrotePrev(wrote, buf)

	if d.nanos/nanosInMicro < 0 {
		buf.WriteString("-")
	} else if negDays {
		buf.WriteString("+")
	}

	// Extract abs(d.nanos). See https://play.golang.org/p/U3_gNMpyUew.
	var nanos uint64
	if d.nanos >= 0 {
		nanos = uint64(d.nanos)
	} else {
		nanos = uint64(-d.nanos)
	}

	hn := nanos / hourNanos
	nanos %= hourNanos
	mn := nanos / minuteNanos
	nanos %= minuteNanos
	sn := nanos / secondNanos
	nanos %= secondNanos
	fmt.Fprintf(buf, "%02d:%02d:%02d", hn, mn, sn)

	micros := nanos / nanosInMicro
	if micros != 0 {
		s := fmt.Sprintf(".%06d", micros)
		buf.WriteString(strings.TrimRight(s, "0"))
	}
}

func isPlural(i int64) string {
	if i == 1 {
		return ""
	}
	return "s"
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

// StringNanos returns a string representation of a Duration including
// its hidden nanoseconds value. To be used only by the encoding/decoding
// packages for pretty printing of on-disk values.
func (d Duration) StringNanos() string {
	var buf bytes.Buffer
	d.Format(&buf)
	nanos := d.nanos % nanosInMicro
	if nanos != 0 {
		fmt.Fprintf(&buf, "%+dns", nanos)
	}
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
	years := d.Months/12 + d.Days/DaysPerMonth/12 + d.nanos/nanosInMonth/12
	if years > maxYearsInDuration || years < minYearsInDuration {
		return 0, 0, 0, errEncodeOverflow
	}

	totalNanos := d.Months*nanosInMonth + d.Days*nanosInDay + d.nanos
	return totalNanos, d.Months, d.Days, nil
}

// EncodeBigInt is the same as Encode, except that it always returns
// successfully and is slower.
func (d Duration) EncodeBigInt() (sortNanos *big.Int, months int64, days int64) {
	bigMonths := big.NewInt(d.Months)
	bigMonths.Mul(bigMonths, big.NewInt(nanosInMonth))
	bigDays := big.NewInt(d.Days)
	bigDays.Mul(bigDays, big.NewInt(nanosInDay))
	totalNanos := big.NewInt(d.nanos)
	totalNanos.Add(totalNanos, bigMonths).Add(totalNanos, bigDays)
	return totalNanos, d.Months, d.Days
}

// Decode reverses the three integers returned from Encode and produces an equal
// Duration to the original.
func Decode(sortNanos int64, months int64, days int64) (Duration, error) {
	nanos := sortNanos - months*nanosInMonth - days*nanosInDay
	// TODO(dan): Handle underflow, then document that DecodeBigInt can be used
	// in underflow cases.
	return Duration{Months: months, Days: days, nanos: nanos}, nil
}

// TODO(dan): Write DecodeBigInt.

// Add returns the time t+d, using a configurable mode.
func Add(t time.Time, d Duration) time.Time {
	// We can fast-path if the duration is always a fixed amount of time,
	// or if the day number that we're starting from can never result
	// in normalization.
	if d.Months == 0 || t.Day() <= 28 {
		return t.AddDate(0, int(d.Months), int(d.Days)).Add(time.Duration(d.nanos))
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

	return res.AddDate(0, 0, int(d.Days)).Add(time.Duration(d.nanos))
}

// Add returns a Duration representing a time length of d+x.
func (d Duration) Add(x Duration) Duration {
	return MakeDuration(d.nanos+x.nanos, d.Days+x.Days, d.Months+x.Months)
}

// Sub returns a Duration representing a time length of d-x.
func (d Duration) Sub(x Duration) Duration {
	return MakeDuration(d.nanos-x.nanos, d.Days-x.Days, d.Months-x.Months)
}

// Mul returns a Duration representing a time length of d*x.
func (d Duration) Mul(x int64) Duration {
	return MakeDuration(d.nanos*x, d.Days*x, d.Months*x)
}

// Div returns a Duration representing a time length of d/x.
func (d Duration) Div(x int64) Duration {
	return d.DivFloat(float64(x))
}

// MulFloat returns a Duration representing a time length of d*x.
func (d Duration) MulFloat(x float64) Duration {
	monthInt, monthFrac := math.Modf(float64(d.Months) * x)
	dayInt, dayFrac := math.Modf((float64(d.Days) * x) + (monthFrac * DaysPerMonth))

	return MakeDuration(
		int64((float64(d.nanos)*x)+(dayFrac*float64(nanosInDay))),
		int64(dayInt),
		int64(monthInt),
	)
}

// DivFloat returns a Duration representing a time length of d/x.
func (d Duration) DivFloat(x float64) Duration {
	monthInt, monthFrac := math.Modf(float64(d.Months) / x)
	dayInt, dayFrac := math.Modf((float64(d.Days) / x) + (monthFrac * DaysPerMonth))

	return MakeDuration(
		int64((float64(d.nanos)/x)+(dayFrac*float64(nanosInDay))),
		int64(dayInt),
		int64(monthInt),
	)
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
	if d.nanos > 0 {
		d = d.shiftPosNanosToDays()
		d = d.shiftPosDaysToMonths()
	} else if d.nanos < 0 {
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
	monthsFromDays := int64Min(d.Days/DaysPerMonth, maxMonths)
	d.Months += monthsFromDays
	d.Days -= monthsFromDays * DaysPerMonth
	return d
}

func (d Duration) shiftPosNanosToDays() Duration {
	var maxDays = int64(math.MaxInt64)
	if d.Days > 0 {
		// If d.Days < 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		maxDays = math.MaxInt64 - d.Days
	}
	daysFromNanos := int64Min(d.nanos/nanosInDay, maxDays)
	d.Days += daysFromNanos
	d.nanos -= daysFromNanos * nanosInDay
	return d
}

func (d Duration) shiftNegDaysToMonths() Duration {
	var minMonths = int64(math.MinInt64)
	if d.Months < 0 {
		// If d.Months > 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		minMonths = math.MinInt64 - d.Months
	}
	monthsFromDays := int64Max(d.Days/DaysPerMonth, minMonths)
	d.Months += monthsFromDays
	d.Days -= monthsFromDays * DaysPerMonth
	return d
}

func (d Duration) shiftNegNanosToDays() Duration {
	var minDays = int64(math.MinInt64)
	if d.Days < 0 {
		// If d.Days > 0, then this would overflow, but because of the exchange
		// rate, we can never transfer more than math.MaxInt64 anyway.
		minDays = math.MinInt64 - d.Days
	}
	daysFromNanos := int64Max(d.nanos/nanosInDay, minDays)
	d.Days += daysFromNanos
	d.nanos -= daysFromNanos * nanosInDay
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
		panic(errors.AssertionFailedf("zero passed as resolution"))
	}
	return d - (d % r)
}
