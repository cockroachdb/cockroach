// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package protectedts

import "testing"

// TestProtectedTimestamps exists mostly to defeat the unused linter.
func TestProtectedTimestamps(t *testing.T) {
	var (
		_ Provider
		_ Cache
		_ Storage
		_ = EmptyCache(nil)
		_ = ErrNotExists
		_ = ErrExists
		_ = PollInterval
		_ = MaxBytes
		_ = MaxSpans
	)
}
