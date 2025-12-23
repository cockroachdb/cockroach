// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

// inspectCheckRowCount defines an inspectCheck that counts rows in addition to
// its primary validation.
type inspectCheckRowCount interface {
	inspectCheck

	// Rows returns the number of rows counted by the check.
	RowCount() uint64
}
