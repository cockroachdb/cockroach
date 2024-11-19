// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package humanizeutil

import (
	"fmt"
	"time"

	"github.com/cockroachdb/redact"
)

// Duration formats a duration in a user-friendly way. The result is not exact
// and the granularity is no smaller than microseconds.
//
// Examples:
//
//	0              ->  "0µs"
//	123456ns       ->  "123µs"
//	12345678ns     ->  "12ms"
//	12345678912ns  ->  "1.2s"
func Duration(val time.Duration) redact.SafeString {
	val = val.Round(time.Microsecond)
	if val == 0 {
		return "0µs"
	}

	// Everything under 1ms will show up as µs.
	if val < time.Millisecond {
		return redact.SafeString(val.String())
	}
	// Everything in-between 1ms and 1s will show up as ms.
	if val < time.Second {
		return redact.SafeString(val.Round(time.Millisecond).String())
	}
	// Everything in-between 1s and 1m will show up as seconds with one decimal.
	if val < time.Minute {
		return redact.SafeString(val.Round(100 * time.Millisecond).String())
	}

	// Everything larger is rounded to the nearest second.
	return redact.SafeString(val.Round(time.Second).String())
}

// LongDuration formats a duration that is expected to be on the order of
// minutes / hours / days in a user-friendly way. The result is not exact and
// the granularity is no smaller than seconds.
//
// Examples:
//   - 0 seconds
//   - 1 second
//   - 3 minutes
//   - 1 hour
//   - 5 days
//   - 1000 days
func LongDuration(val time.Duration) redact.SafeString {
	var round time.Duration
	var unit string

	switch {
	case val < time.Minute:
		round = time.Second
		unit = "second"

	case val < time.Hour:
		round = time.Minute
		unit = "minute"

	case val < 24*time.Hour:
		round = time.Hour
		unit = "hour"

	default:
		round = 24 * time.Hour
		unit = "day"
	}

	n := int64(val.Round(round) / round)
	s := ""
	if n != 1 {
		s = "s"
	}
	return redact.SafeString(fmt.Sprintf("%d %s%s", n, unit, s))
}
