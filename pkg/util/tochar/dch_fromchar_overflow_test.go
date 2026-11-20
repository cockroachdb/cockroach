// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/stretchr/testify/require"
)

// TestCharToDateFieldOverflow checks that digit fields wider than the
// 64-bit range are rejected instead of wrapping back into range
// (cockroachdb/cockroach#176244). A 2^32 + k or 2^63 + k field was already
// rejected; only 2^64 + k wrapped, because fromCharParseIntLen accumulated
// digits in a Go int and checked the int32 bound only afterwards.
func TestCharToDateFieldOverflow(t *testing.T) {
	c := NewFormatCache(1000)

	overflow := func(t *testing.T, input, format string) {
		t.Helper()
		_, _, _, err := CharToDate(input, c, format)
		require.Error(t, err)
		require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))
	}

	// The issue's repro: 2^64 + 15 wraps to day 15.
	overflow(t, "2023-03-18446744073709551631", "YYYY-MM-DD")
	// 2^64 - 1 wraps to day -1 as int64, then to 15 after the sign flip.
	overflow(t, "2023-03-18446744073709551615", "YYYY-MM-DD")
	// 2^65 + 15: wider than 64 bits entirely.
	overflow(t, "2023-03-36893488147419103247", "YYYY-MM-DD")
	// Wrap on the month and year fields.
	overflow(t, "2023-18446744073709551628-15", "YYYY-MM-DD")
	overflow(t, "18446744073709551639-03-15", "YYYY-MM-DD")
	// Day-of-year field.
	overflow(t, "2023 18446744073709551690", "YYYY DDD")
	// Already rejected before the fix; kept as regression guards.
	overflow(t, "2023-03-4294967311", "YYYY-MM-DD")
	overflow(t, "2023-03-9223372036854775823", "YYYY-MM-DD")
	// int32 boundary: both just inside and just outside the bound are out
	// of range for a day field.
	overflow(t, "2023-03-2147483647", "YYYY-MM-DD")
	overflow(t, "2023-03-2147483648", "YYYY-MM-DD")
	// A huge negative field must also be an overflow, not a wrapped value.
	overflow(t, "2023-03--18446744073709551631", "YYYY-MM-DD")

	// Leading zeros must not trip the overflow check: the value is 15.
	year, month, day, err := CharToDate("2023-03-00000000000000000000000015", c, "YYYY-MM-DD")
	require.NoError(t, err)
	require.Equal(t, 2023, year)
	require.Equal(t, time.March, month)
	require.Equal(t, 15, day)

	// Ordinary values still parse.
	year, month, day, err = CharToDate("2023-03-15", c, "YYYY-MM-DD")
	require.NoError(t, err)
	require.Equal(t, 2023, year)
	require.Equal(t, time.March, month)
	require.Equal(t, 15, day)
}

// TestCharToTimestampFieldOverflow is the to_timestamp half of #176244:
// an overflowing hour field must be rejected, not wrapped.
func TestCharToTimestampFieldOverflow(t *testing.T) {
	c := NewFormatCache(1000)

	_, err := CharToTimestamp("2023-03-15 18446744073709551631:00:00", c, "YYYY-MM-DD HH24:MI:SS", time.UTC)
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	// The issue's hour/minute/second repros: 2^64 + 10 / +30 / +20.
	_, err = CharToTimestamp("2023-03-15 18446744073709551626:30:00", c, "YYYY-MM-DD HH24:MI:SS", time.UTC)
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	_, err = CharToTimestamp("2023-03-15 10:18446744073709551646:00", c, "YYYY-MM-DD HH24:MI:SS", time.UTC)
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	_, err = CharToTimestamp("2023-03-15 10:30:18446744073709551636", c, "YYYY-MM-DD HH24:MI:SS", time.UTC)
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	_, err = CharToTimestamp("2023-03-15 10:00:00", c, "YYYY-MM-DD HH24:MI:SS", time.UTC)
	require.NoError(t, err)
}

// TestCharToDateYYYYYOverflow covers the Y,YYY branch, whose millennia
// accumulate with no length limit and can wrap the same way.
func TestCharToDateYYYYYOverflow(t *testing.T) {
	c := NewFormatCache(1000)

	_, _, _, err := CharToDate("18446744073709551615,023-03-15", c, "Y,YYY-MM-DD")
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	_, _, _, err = CharToDate("99999999999999999999,023-03-15", c, "Y,YYY-MM-DD")
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	// The issue's Y,YYY repros: the millennia fit in int64, but
	// millennia*1000 + years overflows it.
	_, _, _, err = CharToDate("18446744073709553,639", c, "Y,YYY")
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	_, _, _, err = CharToDate("18446744073709552,023", c, "Y,YYY")
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	// The combined value is also bounded to the int32 range, like every
	// other numeric field: 2147483,647 is the largest valid Y,YYY year.
	_, _, _, err = CharToDate("2147483,648", c, "Y,YYY")
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	_, _, _, err = CharToDate("2147484,000", c, "Y,YYY")
	require.Error(t, err)
	require.Equal(t, pgcode.DatetimeFieldOverflow, pgerror.GetPGCode(err))

	year, month, day, err := CharToDate("2147483,647-01-01", c, "Y,YYY-MM-DD")
	require.NoError(t, err)
	require.Equal(t, 2147483647, year)
	require.Equal(t, time.January, month)
	require.Equal(t, 1, day)

	year, month, day, err = CharToDate("2,023-03-15", c, "Y,YYY-MM-DD")
	require.NoError(t, err)
	require.Equal(t, 2023, year)
	require.Equal(t, time.March, month)
	require.Equal(t, 15, day)
}
