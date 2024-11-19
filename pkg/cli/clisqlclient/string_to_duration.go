// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlclient

import (
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
)

// stringToDuration converts a server-side interval value returned by
// SHOW LAST QUERY STATISTICS. We use a custom parser here to avoid
// depending on package `tree` or `duration`, which makes the SQL
// shell executable significantly larger.
//
// Note: this parser only supports the 'postgres' encoding for
// IntervalStyle. This code breaks if the server-side
// IntervalStyle is set to another value e.g. 'iso_8601'.
// See: https://github.com/cockroachdb/cockroach/issues/67618
func stringToDuration(s string) (time.Duration, error) {
	m := intervalRe.FindStringSubmatch(s)
	if m == nil {
		return 0, errors.Newf("invalid format: %q", s)
	}
	th, e1 := strconv.Atoi(m[1])
	tm, e2 := strconv.Atoi(m[2])
	ts, e3 := strconv.Atoi(m[3])
	us := m[4] + "000000"[:6-len(m[4])]
	tus, e4 := strconv.Atoi(us)
	return (time.Duration(th)*time.Hour +
			time.Duration(tm)*time.Minute +
			time.Duration(ts)*time.Second +
			time.Duration(tus)*time.Microsecond),
		errors.CombineErrors(e1,
			errors.CombineErrors(e2,
				errors.CombineErrors(e3, e4)))
}

// intervalRe indicates how to parse the interval value.
// The format is HHHH:MM:SS[.ffffff]
//
// Note: we do not need to support a day prefix, because SHOW LAST
// QUERY STATISTICS always reports intervals computed from a number
// of seconds, and these never contain a "days" components.
//
// For example, a query that ran for 3 days will have its interval
// displayed as 72:00:00, not "3 days 00:00:00".
var intervalRe = regexp.MustCompile(`^(\d{2,}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$`)
