// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import "github.com/cockroachdb/redact"

// Pluralize returns a single character 's' unless n == 1.
func Pluralize(n int64) redact.SafeString {
	if n == 1 {
		return ""
	}
	return "s"
}
