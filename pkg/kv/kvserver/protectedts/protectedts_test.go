// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protectedts

import "testing"

// TestProtectedTimestamps exists mostly to defeat the unused linter.
func TestProtectedTimestamps(t *testing.T) {
	var (
		_ Provider
		_ Cache
		_ Verifier
		_ Storage
		_ = EmptyCache(nil)
		_ = ErrNotExists
		_ = ErrExists
		_ = PollInterval
		_ = MaxBytes
		_ = MaxSpans
	)
}
