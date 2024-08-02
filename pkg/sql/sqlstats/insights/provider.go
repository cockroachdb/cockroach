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
	store           *lockingStore
	ingester        *concurrentBufferIngester
	anomalyDetector *anomalyDetector
}

// Start launches the background tasks necessary for processing insights.
func (p *Provider) Start(ctx context.Context, stopper *stop.Stopper) {
	p.ingester.Start(ctx, stopper)
}

// Writer returns an object that observes statement and transaction executions.
// Pass true for internal when called by the internal executor.
func (p *Provider) Writer(internal bool) Writer {
	// We ignore statements and transactions run by the internal executor.
	if internal {
		return nullWriterInstance
	}
	return p.ingester
}

// Reader returns an object that offers read access to any detected insights.
func (p *Provider) Reader() Reader {
	return p.store
}

// LatencyInformation returns an object that offers read access to latency information,
// such as percentiles.
func (p *Provider) LatencyInformation() LatencyInformation {
	return p.anomalyDetector
}

type nullWriter struct{}

func (n *nullWriter) ObserveStatement(_ clusterunique.ID, _ *Statement) {
}

func (n *nullWriter) ObserveTransaction(_ clusterunique.ID, _ *Transaction) {
}

func (n *nullWriter) Clear() {
}

var nullWriterInstance Writer = &nullWriter{}
