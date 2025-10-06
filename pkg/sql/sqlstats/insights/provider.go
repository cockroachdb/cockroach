// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Provider offers access to the insights subsystem.
type Provider struct {
	store           *LockingStore
	ingester        *ConcurrentBufferIngester
	anomalyDetector *AnomalyDetector
}

// Start launches the background tasks necessary for processing insights.
func (p *Provider) Start(ctx context.Context, stopper *stop.Stopper) {
	p.ingester.Start(ctx, stopper)
}

// Writer returns an object that observes statement and transaction executions.
func (p *Provider) Writer() *ConcurrentBufferIngester {
	return p.ingester
}

// Store returns an object that offers read access to any detected insights.
func (p *Provider) Store() *LockingStore {
	return p.store
}

// Anomalies returns an object that offers read access to latency information,
// such as percentiles.
func (p *Provider) Anomalies() *AnomalyDetector {
	return p.anomalyDetector
}
