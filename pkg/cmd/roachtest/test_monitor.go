// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

var _ test.Monitor = &testMonitorImpl{}

type testMonitorImpl struct {
	*monitorImpl
}

func newTestMonitor(ctx context.Context, t test.Test, c *clusterImpl) *testMonitorImpl {
	return &testMonitorImpl{
		monitorImpl: newMonitor(ctx, t, c, true /* expectExactProcessDeath */),
	}
}

func (m *testMonitorImpl) start() {
	go func() {
		err := m.WaitForNodeDeath()
		if err != nil {
			m.t.Fatal(err)
		}
	}()
}
