// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idxusage

import (
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TestingKnobs is the testing knobs that provides callbacks that unit tests
// can hook into.
type TestingKnobs struct {
	// OnIndexUsageStatsProcessedCallback is invoked whenever a index usage event
	// is processed.
	OnIndexUsageStatsProcessedCallback func(key roachpb.IndexUsageKey)
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}
