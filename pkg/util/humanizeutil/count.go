// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package humanizeutil

import (
	"math"

	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// Count formats a unitless integer value like a row count. It uses separating
// commas for large values (e.g. "1,000,000").
func Count(val uint64) redact.SafeString {
	if val > math.MaxInt64 {
		val = math.MaxInt64
	}
	return redact.SafeString(humanize.Comma(int64(val)))
}

func Countf(val float64) redact.SafeString {
	if val > math.MaxInt64 {
		val = math.MaxInt64
	}
	return redact.SafeString(humanize.Commaf(val))
}
