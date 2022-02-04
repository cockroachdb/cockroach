// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
)

// UnsortedMatricesDiff returns a diff of the two input string matrices. The
// inputs do not have to be in the same sorted order to be compared as equal.
// If the input matrices are identical, the result is the empty string.
func UnsortedMatricesDiff(rowMatrix1, rowMatrix2 [][]string) string {
	var rows1 []string
	for _, row := range rowMatrix1 {
		rows1 = append(rows1, strings.Join(row[:], ","))
	}
	var rows2 []string
	for _, row := range rowMatrix2 {
		rows2 = append(rows2, strings.Join(row[:], ","))
	}
	sort.Strings(rows1)
	sort.Strings(rows2)
	return cmp.Diff(rows1, rows2)
}
