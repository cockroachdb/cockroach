// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package telemetry_test

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/stretchr/testify/require"
)

// TestGetCounterDoesNotRace ensures that concurrent calls to GetCounter for
// the same feature always returns the same pointer. Even when this race was
// possible this test would fail on every run but would fail rapidly under
// stressrace.
func TestGetCounterDoesNotRace(t *testing.T) {
	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	counters := make([]telemetry.Counter, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			counters[i] = telemetry.GetCounter("test.foo")
			wg.Done()
		}(i)
	}
	wg.Wait()
	counterSet := make(map[telemetry.Counter]struct{})
	for _, c := range counters {
		counterSet[c] = struct{}{}
	}
	require.Len(t, counterSet, 1)
}
