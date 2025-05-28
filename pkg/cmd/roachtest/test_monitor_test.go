// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type stubTestMonitorError struct{}

func (s *stubTestMonitorError) ExpectProcessDead(
	nodes option.NodeListOption, opts ...option.OptionFunc,
) {
}

func (s *stubTestMonitorError) ExpectProcessAlive(
	nodes option.NodeListOption, opts ...option.OptionFunc,
) {
}
func (s *stubTestMonitorError) AvailableNodes(virtualClusterName string) option.NodeListOption {
	return option.NodeListOption{}
}

func (s *stubTestMonitorError) WaitForNodeDeath() error {
	return errors.New("test error")
}

func TestGlobalMonitorError(t *testing.T) {
	newTestMonitor = func(_ context.Context, t test.Test, c *clusterImpl) *testMonitorImpl {
		return &testMonitorImpl{
			monitor: &stubTestMonitorError{},
			t:       t,
		}
	}

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	cr := newClusterRegistry()
	runner := newUnitTestRunner(cr, stopper)

	var buf syncedBuffer
	copt := defaultClusterOpt()
	lopt := defaultLoggingOpt(&buf)

	mockTest := registry.TestSpec{
		Name:             `mock test`,
		Owner:            OwnerUnitTest,
		Cluster:          spec.MakeClusterSpec(0),
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		CockroachBinary:  registry.StandardCockroach,
		Monitor:          true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Minute):
				return
			}
		},
	}
	err := runner.Run(ctx, []registry.TestSpec{mockTest}, 1, /* count */
		defaultParallelism, copt, testOpts{}, lopt)
	require.Error(t, err)
}
