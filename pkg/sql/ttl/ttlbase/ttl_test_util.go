// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
