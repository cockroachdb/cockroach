// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import "github.com/cockroachdb/cockroach/pkg/sql/clusterunique"

// TestingKnobs provides hooks and testingKnobs for unit tests.
type TestingKnobs struct {
	// OnSessionClear is a callback that is triggered when the locking
	// registry clears a session entry.
	OnSessionClear func(sessionID clusterunique.ID)
}

// ModuleTestingKnobs implements base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
