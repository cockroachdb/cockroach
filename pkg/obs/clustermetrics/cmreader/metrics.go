// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmreader

import "github.com/cockroachdb/cockroach/pkg/util/metric"

// registry embeds a metric.Registry, but is package private to prevent
// access to the Registry's methods that would allow mutation. This struct
// is exposed only via the metric.RegistryReader interface.
type registry struct {
	*metric.Registry
}

func NewRegistryReader() metric.RegistryReader {
	return &registry{metric.NewRegistry()}
}
