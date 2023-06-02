// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMakeTestRegistry(t *testing.T) {
	testutils.RunTrueAndFalse(t, "preferSSD", func(t *testing.T, preferSSD bool) {
		r := makeTestRegistry(spec.AWS, "foo", "zone123", preferSSD, false)
		require.Equal(t, preferSSD, r.preferSSD)
		require.Equal(t, "zone123", r.zones)
		require.Equal(t, "foo", r.instanceType)
		require.Equal(t, spec.AWS, r.cloud)

		s := r.MakeClusterSpec(100, spec.Geo(), spec.Zones("zone99"), spec.CPU(12),
			spec.PreferLocalSSD(true))
		require.EqualValues(t, 100, s.NodeCount)
		require.Equal(t, "foo", s.InstanceType)
		require.True(t, s.Geo)
		require.Equal(t, "zone99", s.Zones)
		require.EqualValues(t, 12, s.CPUs)
		require.True(t, s.PreferLocalSSD)

		s = r.MakeClusterSpec(100, spec.CPU(4), spec.TerminateOnMigration())
		require.EqualValues(t, 100, s.NodeCount)
		require.Equal(t, "foo", s.InstanceType)
		require.EqualValues(t, 4, s.CPUs)
		require.True(t, s.TerminateOnMigration)

		s = r.MakeClusterSpec(10, spec.CPU(16), spec.Arch(vm.ArchARM64))
		require.EqualValues(t, 10, s.NodeCount)
		require.Equal(t, "foo", s.InstanceType)
		require.EqualValues(t, 16, s.CPUs)
		require.EqualValues(t, vm.ArchARM64, s.Arch)
	})
}

// TestPrometheusMetricParser tests that the registry.PromSub()
// helper properly converts a string into a metric name that Prometheus can read.
func TestPrometheusMetricParser(t *testing.T) {
	r := makeTestRegistry(spec.AWS, "foo", "zone123", true, false)
	f := r.PromFactory()

	rawName := "restore/nodes=4/duration"
	promName := registry.PromSub(rawName)
	require.Equal(t, "restore_nodes_4_duration", promName)
	f.NewGauge(prometheus.GaugeOpts{Namespace: prometheusNameSpace, Name: promName})
}
