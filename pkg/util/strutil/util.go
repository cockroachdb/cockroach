// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package strutil

import (
	"fmt"
	"strconv"
	"strings"
)

// AppendInt appends the decimal form of x to b and returns the result.
// If the decimal form is shorter than width, the result is padded with leading 0's.
// If the decimal is longer than width, returns formatted decimal without
// any truncation.
func AppendInt(b []byte, x int, width int) []byte {
	if x < 0 {
		width--
		x = -x
		b = append(b, '-')
	}

	var scratch [16]byte
	xb := strconv.AppendInt(scratch[:0], int64(x), 10)

	// Add 0-padding.
	for w := len(xb); w < width; w++ {
		b = append(b, '0')
	}
	return append(b, xb...)
}

// JoinIDs joins a slice of any ids into a comma separated string. Each ID could
// be prefixed with a string (e.g. n1, n2, n3 to represent nodes).
func JoinIDs[T ~int | ~int32 | ~int64](prefix string, ids []T) string {
	idNames := make([]string, 0, len(ids))
	for _, id := range ids {
		idNames = append(idNames, fmt.Sprintf("%s%d", prefix, id))
	}
	return strings.Join(idNames, ", ")
}
