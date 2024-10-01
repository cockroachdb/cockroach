// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import "github.com/cockroachdb/redact"

// Pluralize returns a single character 's' unless n == 1.
func Pluralize(n int64) redact.SafeString {
	if n == 1 {
		return ""
	}
	return "s"
}
