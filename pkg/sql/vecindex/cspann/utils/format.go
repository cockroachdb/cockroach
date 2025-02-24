// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package utils

import (
	"bytes"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// FormatFloat formats each number to the specified number of decimal places,
// removing unnecessary zeros. Don't print negative zero, since this causes
// diffs when running on Linux vs. Mac.
func FormatFloat(value float32, prec int) string {
	s := strconv.FormatFloat(float64(value), 'f', prec, 32)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	if s == "-0" {
		return "0"
	}
	return s
}

// WriteVector formats the given vector like "(1, 2, ..., 9, 10)" and writes it
// to the given buffer, using the specified precision.
func WriteVector(buf *bytes.Buffer, vec vector.T, prec int) {
	buf.WriteString("(")
	if len(vec) > 4 {
		// Show first 2 numbers, '...', and last 2 numbers.
		buf.WriteString(FormatFloat(vec[0], prec))
		buf.WriteString(", ")
		buf.WriteString(FormatFloat(vec[1], prec))
		buf.WriteString(", ..., ")
		buf.WriteString(FormatFloat(vec[len(vec)-2], prec))
		buf.WriteString(", ")
		buf.WriteString(FormatFloat(vec[len(vec)-1], prec))
	} else {
		// Show all numbers if there are 4 or fewer.
		for i, val := range vec {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(FormatFloat(val, prec))
		}
	}
	buf.WriteString(")")
}
