// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlclient

import (
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
)

// stringToDuration converts a server-side interval value returned by
// SHOW LAST QUERY STATISTICS. We use a custom parser here to avoid
// depending on package `tree`, which makes the SQL shell executable
// significantly larger.
func stringToDuration(s string) (time.Duration, error) {
	// The format is HH:MM:SS[.ffffff]
	if len(s) < 8 {
		return 0, errors.Newf("value too short: %q", s)
	}
	if s[2] != ':' || s[5] != ':' || (len(s) > 8 && s[8] != '.') {
		return 0, errors.Newf("invalid format: %q", s)
	}
	th, e1 := strconv.Atoi(s[0:2])
	tm, e2 := strconv.Atoi(s[3:5])
	ts, e3 := strconv.Atoi(s[6:8])
	var tus int
	var e4 error
	if len(s) > 8 {
		us := s[9:]
		if len(us) > 6 {
			return 0, errors.Newf("too many microsecond decimals: %q", s)
		}
		// Zero-pad on the left so that there are 6 decimals in total.
		us = us + "000000"[:6-len(us)]
		tus, e4 = strconv.Atoi(us)
	}
	return (time.Duration(th)*time.Hour +
			time.Duration(tm)*time.Minute +
			time.Duration(ts)*time.Second +
			time.Duration(tus)*time.Microsecond),
		errors.CombineErrors(e1,
			errors.CombineErrors(e2,
				errors.CombineErrors(e3, e4)))
}
