// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"bytes"
	"fmt"
)

// String returns a list representation of elements. Sequential runs of positive
// numbers are shown as ranges. For example, for the set {0, 1, 2, 5, 6, 10},
// the output is "(0-2,5,6,10)".
func (s FastIntSet) String() string {
	var buf bytes.Buffer
	buf.WriteByte('(')
	appendRange := func(start, end int) {
		if buf.Len() > 1 {
			buf.WriteByte(',')
		}
		if start == end {
			fmt.Fprintf(&buf, "%d", start)
		} else if start+1 == end {
			fmt.Fprintf(&buf, "%d,%d", start, end)
		} else {
			fmt.Fprintf(&buf, "%d-%d", start, end)
		}
	}
	rangeStart, rangeEnd := -1, -1
	s.ForEach(func(i int) {
		if i < 0 {
			appendRange(i, i)
			return
		}
		if rangeStart != -1 && rangeEnd == i-1 {
			rangeEnd = i
		} else {
			if rangeStart != -1 {
				appendRange(rangeStart, rangeEnd)
			}
			rangeStart, rangeEnd = i, i
		}
	})
	if rangeStart != -1 {
		appendRange(rangeStart, rangeEnd)
	}
	buf.WriteByte(')')
	return buf.String()
}
