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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

// CreateIndexStatsIngestedCallbackForTest creates a pair of callbacks and
// notification channel for unit tests. The callback is injected into
// IndexUsageStats struct through testing knobs and the channel can be used by
// WaitForStatsIngestionForTest.
func CreateIndexStatsIngestedCallbackForTest() (
	func(key roachpb.IndexUsageKey),
	chan roachpb.IndexUsageKey,
) {
	// Create a buffered channel so the callback is non-blocking.
	notify := make(chan roachpb.IndexUsageKey, 100)

	cb := func(key roachpb.IndexUsageKey) {
		notify <- key
	}

	return cb, notify
}

// WaitForIndexStatsIngestionForTest waits for a map of expected events
// to occur for expectedEventCnt number of times from the event channel.
// If expected events did not occur, an error is returned.
func WaitForIndexStatsIngestionForTest(
	notify chan roachpb.IndexUsageKey,
	expectedKeys map[roachpb.IndexUsageKey]struct{},
	expectedEventCnt int,
	timeout time.Duration,
) error {
	var timer timeutil.Timer
	eventCnt := 0

	timer.Reset(timeout)

	for eventCnt < expectedEventCnt {
		select {
		case key := <-notify:
			if _, ok := expectedKeys[key]; ok {
				eventCnt++
			}
			continue
		case <-timer.C:
			timer.Read = true
			return errors.New("failed to wait for index usage stats ingestion")
		}
	}

	return nil
}
