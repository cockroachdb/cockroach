// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

type testMonitorImpl struct {
	monitor interface {
		test.Monitor
		WaitForNodeDeath() error
	}
	t test.Test
}

// newTestMonitor is a function that creates a new test monitor. It can be used for testing
// purposes to replace the default monitor with a custom implementation.
var newTestMonitor = func(ctx context.Context, t test.Test, c *clusterImpl) *testMonitorImpl {
	return &testMonitorImpl{
		monitor: newMonitor(ctx, t, c, true /* expectExactProcessDeath */),
		t:       t,
	}
}

func (m *testMonitorImpl) start() {
	go func() {
		err := m.monitor.WaitForNodeDeath()
		if err != nil {
			m.t.Errorf("test monitor: %v", err)
		}
	}()
}
