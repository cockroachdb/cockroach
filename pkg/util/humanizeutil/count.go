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

import "github.com/dustin/go-humanize"

// Count formats a unitless integer value like a row count. It uses separating
// commas for large values (e.g. "1,000,000").
func Count(val uint64) string {
	return humanize.Comma(int64(val))
}
