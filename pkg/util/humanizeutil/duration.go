// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package humanizeutil

import "time"

// Duration formats a duration in a user-friendly way. The result is not exact
// and the granularity is no smaller than microseconds.
//
// Examples:
//   0              ->  "0µs"
//   123456ns       ->  "123µs"
//   12345678ns     ->  "12ms"
//   12345678912ns  ->  "1.2s"
//
func Duration(val time.Duration) string {
	val = val.Round(time.Microsecond)
	if val == 0 {
		return "0µs"
	}

	// Everything under 1ms will show up as µs.
	if val < time.Millisecond {
		return val.String()
	}
	// Everything in-between 1ms and 1s will show up as ms.
	if val < time.Second {
		return val.Round(time.Millisecond).String()
	}
	// Everything in-between 1s and 1m will show up as seconds with one decimal.
	if val < time.Minute {
		return val.Round(100 * time.Millisecond).String()
	}

	// Everything larger is rounded to the nearest second.
	return val.Round(time.Second).String()
}
