// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaintegration

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// TestingKnobs is used for testing purposes to tune the behavior of mma.
type TestingKnobs struct {
	// OverrideIsInConflictWithMMA is used to override the value returned by
	// isInConflictWithMMA.
	OverrideIsInConflictWithMMA func(cand roachpb.StoreID) bool
}
