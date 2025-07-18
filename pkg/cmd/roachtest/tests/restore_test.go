// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

func (m *mockRegistry) AddMultiCluster(spec registry.TestSpec) {
	// No-op for mock registry.
	m.Add(spec)
}

func (m *mockRegistry) MakeNamedClusterSpec(name string, nodeCount int, opts ...spec.Option) registry.NamedClusterSpec {
	return registry.NamedClusterSpec{
		Name: name,
		Spec: spec.MakeClusterSpec(nodeCount, opts...),
	}
}

func (m *mockRegistry) Cloud() string {
	return "mock"
}

var _ registry.Registry = &mockRegistry{}
