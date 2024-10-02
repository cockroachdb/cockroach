// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttlbase

import "fmt"

// GenPKColNames generates column names col0, col1, col2, etc for tests.
func GenPKColNames(numPKCols int) []string {
	names := make([]string, 0, numPKCols)
	for i := 0; i < numPKCols; i++ {
		names = append(names, fmt.Sprintf("col%d", i))
	}
	return names
}
