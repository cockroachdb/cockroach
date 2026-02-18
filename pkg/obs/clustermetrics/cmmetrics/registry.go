// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmmetrics

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// Registry wraps a metric.Registry for cluster metrics. It is exposed
// externally only as a metric.RegistryReader via NewRegistryReader,
// preventing callers outside the cluster metrics packages from mutating
// the registry directly; only the cmreader's registrySyncer may add or
// remove metrics.
type Registry struct {
	*metric.Registry
}

// NewRegistry creates a new Registry.
func NewRegistry() *Registry {
	return &Registry{metric.NewRegistry()}
}

// NewRegistryReader creates a new cluster metric registry exposed as a
// read-only metric.RegistryReader.
func NewRegistryReader() metric.RegistryReader {
	return NewRegistry()
}
