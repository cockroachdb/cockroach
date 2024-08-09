// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package insights

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
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

// ActiveSessions returns the IDs of all sessions that are currently being observed.
// Used for testing.
func (p *Provider) ActiveSessions() []clusterunique.ID {
	return p.ingester.registry.getTrackedSessions()
}
