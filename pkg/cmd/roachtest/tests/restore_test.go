// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
)

type mockRegistry struct {
	testNames []string
}

func (m *mockRegistry) MakeClusterSpec(_ int, _ ...spec.Option) spec.ClusterSpec {
	return spec.ClusterSpec{}
}

func (m *mockRegistry) Add(spec registry.TestSpec) {
	if m.testNames == nil {
		m.testNames = make([]string, 0)
	}
	m.testNames = append(m.testNames, spec.Name)
}

func (m *mockRegistry) AddOperation(spec registry.OperationSpec) {
	// No-op.
}

func (m *mockRegistry) PromFactory() promauto.Factory {
	return promauto.With(nil)
}

func (m *mockRegistry) Cloud() string {
	return "mock"
}

var _ registry.Registry = &mockRegistry{}

func TestRestoreRegisteredNames(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := &mockRegistry{}
	registerRestore(r)
	registerRestoreNodeShutdown(r)
	expectedRestoreTests := []string{
		"restore/nodeShutdown/coordinator",
		"restore/nodeShutdown/worker",
		"restore/pause/tpcc-5k/gce/nodes=4/cpus=8",
		"restore/tpcc-10/gce/nodes=4/cpus=8",
		"restore/tpcc-300k/gce/nodes=15/cpus=16",
		"restore/tpcc-30k/aws/nodes=4/cpus=8",
		"restore/tpcc-30k/aws/nodes=9/cpus=8/zones=us-east-2b,us-west-2b,eu-west-1b",
		"restore/tpcc-30k/gce/fullOnly/nodes=10/cpus=8",
		"restore/tpcc-30k/gce/nodes=4/cpus=16",
		"restore/tpcc-30k/gce/nodes=4/cpus=8",
		"restore/tpcc-30k/gce/nodes=4/cpus=8/lowmem",
		"restore/tpcc-30k/gce/nodes=8/cpus=8",
		"restore/tpcc-5k/gce/fullOnly/nodes=4/cpus=8",
		"restore/tpcc-5k/gce/nodes=4/cpus=8",
	}
	slices.Sort(r.testNames)
	require.Equal(t, expectedRestoreTests, r.testNames)
}
