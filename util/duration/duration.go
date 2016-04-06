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
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package duration

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
)

const (
	daysInMonth  = 30
	nanosInDay   = 24 * int64(time.Hour) // Try as I might, couldn't do this without the cast.
	nanosInMonth = daysInMonth * nanosInDay

	// Used in overflow calculations.
	maxYearsInDuration = math.MaxInt64 / nanosInMonth
	minYearsInDuration = math.MinInt64 / nanosInMonth
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

// String returns a string representation of a Duration.
func (d Duration) String() string {
	return fmt.Sprintf("%dm%dd%s", d.Months, d.Days, time.Duration(d.Nanos)*time.Nanosecond)
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

// Add returns the time t+d.
func Add(t time.Time, d Duration) time.Time {
	// TODO(dan): Overflow handling.
	return t.AddDate(0, int(d.Months), int(d.Days)).Add(time.Duration(d.Nanos) * time.Nanosecond)
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
	var maxMonths int64 = math.MaxInt64
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
	var maxDays int64 = math.MaxInt64
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
	var minMonths int64 = math.MinInt64
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
	var minDays int64 = math.MinInt64
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
