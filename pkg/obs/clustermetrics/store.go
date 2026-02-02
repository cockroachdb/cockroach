// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clustermetrics

import "context"

// MetricStore abstracts the storage for cluster metrics.
type MetricStore interface {
	// Write writes the given metrics to storage. The store can use GetName()
	// and Get() on each metric to retrieve the name and value.
	Write(ctx context.Context, metrics []Metric) error
	// Get retrieves all stored metrics.
	Get(ctx context.Context) ([]Metric, error)
}
