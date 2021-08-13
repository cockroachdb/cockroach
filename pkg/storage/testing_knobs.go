// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

// TestingKnobs can be passed when instantiating a storage engine. Settings here
// are used to change behavior in tests.
type TestingKnobs struct {
	// DisableSeparatedIntents disables the writing of separated intents. Only
	// used in tests to check handling of interleaved intents.
	DisableSeparatedIntents bool
}
