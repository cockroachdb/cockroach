// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

// Pluralize returns a single character 's' unless n == 1.
func Pluralize(n int64) string {
	if n == 1 {
		return ""
	}
	return "s"
}
