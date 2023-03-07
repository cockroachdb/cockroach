// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigbounds

import "fmt"

type intI interface{ int32 | int64 }

func formatRange[T intI](start, end T) string {
	if end <= start {
		return fmt.Sprintf("%d", start)
	}
	return fmt.Sprintf("[%d, %d]", start, end)
}

func checkRange[T intI](c *T, start, end T) bool {
	return *c >= start &&
		((end > start && *c <= end) || *c == start)
}

func clampRange[T intI](c *T, start, end T) bool {
	if end < start {
		end = start
	}
	switch {
	case *c < start:
		*c = start
		return true
	case *c > end:
		*c = end
		return true
	default:
		return false
	}
}
