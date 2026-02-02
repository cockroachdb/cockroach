// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import (
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// Registry is a thin wrapper around metric.Registry to implement
// metric.RegistryReader. It is used to record metrics via the
// status.MetricsRecorder struct while removing access to write to the
// underlying metric.Registry. Metrics can only be registered to this registry
// from within this package, protecting it from unintended usage.
type Registry struct {
	registry *metric.Registry
}

var _ metric.RegistryReader = &Registry{}

func (c *Registry) AddLabel(name string, value interface{}) {
	c.registry.AddLabel(name, value)
}

func (c *Registry) Each(f func(name string, val interface{})) {
	c.registry.Each(f)
}

func (c *Registry) Select(metrics map[string]struct{}, f func(name string, val interface{})) {
	c.Select(metrics, f)
}

func (c *Registry) GetLabels() []*io_prometheus_client.LabelPair {
	return c.registry.GetLabels()
}

func (c *Registry) WriteMetricsMetadata(dest map[string]metric.Metadata) {
	c.registry.WriteMetricsMetadata(dest)
}

func NewCMReaderRegistry() *Registry {
	return &Registry{registry: metric.NewRegistry()}
}
