// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// registry wraps a metric.Registry for cluster metrics. It is exposed
// externally only as a metric.RegistryReader via NewRegistryReader,
// preventing callers outside the cmreader package from mutating
// the registry directly; only the registrySyncer may add or remove
// metrics.
type registry struct {
	*metric.Registry
}

func newRegistry() *registry {
	return &registry{metric.NewRegistry()}
}

// NewRegistryReader creates a new cluster metric registry exposed as a
// read-only metric.RegistryReader.
func NewRegistryReader() metric.RegistryReader {
	return newRegistry()
}
