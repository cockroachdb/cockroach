// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package humanizeutil

import (
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// Count formats a unitless integer value like a row count. It uses separating
// commas for large values (e.g. "1,000,000").
func Count(val uint64) redact.SafeString {
	return redact.SafeString(humanize.Comma(int64(val)))
}
