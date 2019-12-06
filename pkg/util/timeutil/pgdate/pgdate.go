// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pgdate contains parsing functions and types for dates and times
// in a manner that is compatible with PostgreSQL.
//
// The implementation here is inspired by the following
// https://github.com/postgres/postgres/blob/REL_10_5/src/backend/utils/adt/datetime.c
//
// The general approach is to take each input string and break it into
// contiguous runs of alphanumeric characters.  Then, we attempt to
// interpret the input in order to populate a collection of date/time
// fields.  We track which fields have been set in order to be able
// to apply various parsing heuristics to map the input chunks into
// date/time fields.
package pgdate

import (
	"bytes"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	unixEpochToPGEpoch = 10957      // Number of days from 1970-01-01 to 2000-01-01.
	lowDays            = -2451545   // 4714-11-24 BC
	highDays           = 2145031948 // 5874897-12-31
)

var (
	// LowDate is the lowest non-infinite Date.
	LowDate = Date{days: lowDays}
	// HighDate is the highest non-infinite Date.
	HighDate = Date{days: highDays}
	// PosInfDate is the positive infinity Date.
	PosInfDate = Date{days: math.MaxInt32}
	// NegInfDate is the negative infinity date.
	NegInfDate = Date{days: math.MinInt32}
)

// Date is a postgres-compatible date implementation. It stores the
// number of days from the postgres epoch (2000-01-01). Its properties
// are unexported so that it cannot be misused by external packages. This
// package takes care to prevent silent problems like overflow that can
// occur when adding days or converting to and from a time.Time.
type Date struct {
	days int32

	// orig is the original on-disk number of days. It is only set when
	// creating a Date from an on-disk encoding and will be used instead
	// of days when encoding for on-disk. This is required so that we
	// can roundtrip to the same on-disk value, which is necessary for
	// deleting old index entries. This value should never be copied
	// to another Date, but always cleared and forgotten on any mutation.
	hasOrig bool
	orig    int64
}

// MakeCompatibleDateFromDisk creates a Date from the number of days
// since the Unix epoch. If it is out of range of LowDate and HighDate,
// positive or negative infinity dates are returned. This function should
// be used only by the on-disk decoder to maintain backward
// compatibility. It retains the origin days argument which is returned
// by UnixEpochDaysWithOrig.
func MakeCompatibleDateFromDisk(unixDays int64) Date {
	date := Date{
		hasOrig: true,
		orig:    unixDays,
	}
	if pgDays, ok := arith.SubWithOverflow(unixDays, unixEpochToPGEpoch); !ok {
		// We overflowed converting from Unix to PG days.
		date.days = math.MinInt32
	} else if pgDays > highDays {
		date.days = math.MaxInt32
	} else if pgDays < lowDays {
		date.days = math.MinInt32
	} else {
		date.days = int32(pgDays)
	}
	return date
}

// MakeDateFromTime creates a Date from the specified time. The
// timezone-relative date is used.
func MakeDateFromTime(t time.Time) (Date, error) {
	sec := t.Unix()
	_, offset := t.Zone()
	sec += int64(offset)

	days := sec / secondsPerDay
	if sec < 0 && sec%secondsPerDay != 0 {
		// If days are negative AND not divisible by secondsPerDay,
		// we need to round down.
		// e.g. for 1969-12-30 01:00, the division will round to -1
		// but we want -2.
		days--
	}
	d, err := MakeDateFromUnixEpoch(days)
	return d, err
}

var errDateOutOfRange = pgerror.WithCandidateCode(errors.New("date is out of range"), pgcode.DatetimeFieldOverflow)

// MakeDateFromUnixEpoch creates a Date from the number of days since the
// Unix epoch.
func MakeDateFromUnixEpoch(days int64) (Date, error) {
	days, ok := arith.SubWithOverflow(days, unixEpochToPGEpoch)
	if !ok || days <= math.MinInt32 || days >= math.MaxInt32 {
		return Date{}, errors.WithStack(errDateOutOfRange)
	}
	return MakeDateFromPGEpoch(int32(days))
}

// MakeDateFromPGEpoch creates a Date from the number of days since
// 2000-01-01. MaxInt32 or MinInt32 represent the positive and negative
// infinity dates.
func MakeDateFromPGEpoch(days int32) (Date, error) {
	if days == math.MaxInt32 {
		return PosInfDate, nil
	}
	if days == math.MinInt32 {
		return NegInfDate, nil
	}
	if days < lowDays || days > highDays {
		return Date{}, errors.WithStack(errDateOutOfRange)
	}
	return Date{days: days}, nil
}

// ToTime returns d as a time.Time. Non finite dates return an error.
func (d Date) ToTime() (time.Time, error) {
	if d.days == math.MinInt32 || d.days == math.MaxInt32 {
		return time.Time{}, pgerror.WithCandidateCode(
			errors.Newf("%s out of range for timestamp", d), pgcode.DatetimeFieldOverflow)
	}
	return timeutil.Unix(d.UnixEpochDays()*secondsPerDay, 0), nil
}

const secondsPerDay = 24 * 60 * 60

// Format formats d as a string.
func (d Date) Format(buf *bytes.Buffer) {
	switch d.days {
	case math.MinInt32:
		buf.WriteString("-infinity")
	case math.MaxInt32:
		buf.WriteString("infinity")
	default:
		// ToTime only returns an error on infinity, which was already checked above.
		t, _ := d.ToTime()
		year, month, day := t.Date()
		bc := year <= 0
		if bc {
			year = -year + 1
		}
		fmt.Fprintf(buf, "%04d-%02d-%02d", year, month, day)
		if bc {
			buf.WriteString(" BC")
		}
	}
}

func (d Date) String() string {
	var buf bytes.Buffer
	d.Format(&buf)
	return buf.String()
}

// IsFinite returns whether d is finite.
func (d Date) IsFinite() bool {
	return d.days != math.MinInt32 && d.days != math.MaxInt32
}

// PGEpochDays returns the number of days since 2001-01-01. This value can
// also be MinInt32 or MaxInt32, indicating negative or positive infinity.
func (d Date) PGEpochDays() int32 {
	return d.days
}

// UnixEpochDays returns the number of days since the Unix epoch. Infinite
// dates are converted to MinInt64 or MaxInt64.
func (d Date) UnixEpochDays() int64 {
	if d.days == math.MinInt32 {
		return math.MinInt64
	}
	if d.days == math.MaxInt32 {
		return math.MaxInt64
	}
	// Converting to an int64 makes overflow impossible.
	return int64(d.days) + unixEpochToPGEpoch
}

// UnixEpochDaysWithOrig returns the original on-disk representation if
// present, otherwise UnixEpochDays().
func (d Date) UnixEpochDaysWithOrig() int64 {
	if d.hasOrig {
		return d.orig
	}
	return d.UnixEpochDays()
}

// Compare compares two dates.
func (d Date) Compare(other Date) int {
	if d.days < other.days {
		return -1
	}
	if d.days > other.days {
		return 1
	}
	return 0
}

// AddDays adds days to d with overflow and bounds checking.
func (d Date) AddDays(days int64) (Date, error) {
	if !d.IsFinite() {
		return d, nil
	}
	n, ok := arith.Add32to64WithOverflow(d.days, days)
	if !ok {
		return Date{}, pgerror.WithCandidateCode(
			errors.Newf("%s + %d is out of range", d, errors.Safe(days)),
			pgcode.DatetimeFieldOverflow)
	}
	return MakeDateFromPGEpoch(n)
}

// SubDays subtracts days from d with overflow and bounds checking.
func (d Date) SubDays(days int64) (Date, error) {
	if !d.IsFinite() {
		return d, nil
	}
	n, ok := arith.Sub32to64WithOverflow(d.days, days)
	if !ok {
		return Date{}, pgerror.WithCandidateCode(
			errors.Newf("%s - %d is out of range", d, errors.Safe(days)),
			pgcode.DatetimeFieldOverflow)
	}
	return MakeDateFromPGEpoch(n)
}
