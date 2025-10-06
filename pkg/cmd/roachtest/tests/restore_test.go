// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"sort"
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
		"restore/pause/tpce/15GB/aws/nodes=4/cpus=8",
		"restore/tpce/15GB/aws/nodes=4/cpus=8",
		"restore/tpce/32TB/aws/inc-count=400/nodes=15/cpus=16",
		"restore/tpce/32TB/aws/nodes=15/cpus=16",
		"restore/tpce/32TB/gce/inc-count=400/nodes=15/cpus=16",
		"restore/tpce/400GB/aws/inc-count=48/nodes=4/cpus=8",
		"restore/tpce/400GB/aws/nodes=4/cpus=16",
		"restore/tpce/400GB/aws/nodes=4/cpus=8",
		"restore/tpce/400GB/aws/nodes=8/cpus=8",
		"restore/tpce/400GB/aws/nodes=9/cpus=8/zones=us-east-2b,us-west-2b,eu-west-1b",
		"restore/tpce/400GB/gce/nodes=4/cpus=8",
		"restore/tpce/400GB/gce/nodes=4/cpus=8/lowmem",
		"restore/tpce/8TB/aws/nodes=10/cpus=8",
	}
	sort.Slice(r.testNames, func(i, j int) bool {
		return r.testNames[i] < r.testNames[j]
	})
	require.Equal(t, expectedRestoreTests, r.testNames)
}
