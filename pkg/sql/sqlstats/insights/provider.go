// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type defaultProvider struct {
	store           *lockingStore
	ingester        *concurrentBufferIngester
	anomalyDetector *anomalyDetector
}

var _ Provider = &defaultProvider{}

func (p *defaultProvider) Start(ctx context.Context, stopper *stop.Stopper) {
	p.ingester.Start(ctx, stopper)
}

func (p *defaultProvider) Writer(internal bool) Writer {
	// We ignore statements and transactions run by the internal executor.
	if internal {
		return nullWriterInstance
	}
	return p.ingester
}

func (p *defaultProvider) Reader() Reader {
	return p.store
}

func (p *defaultProvider) LatencyInformation() LatencyInformation {
	return p.anomalyDetector
}

type nullWriter struct{}

func (n *nullWriter) ObserveStatement(_ clusterunique.ID, _ *Statement) {
}

func (n *nullWriter) ObserveTransaction(_ clusterunique.ID, _ *Transaction) {
}

var nullWriterInstance Writer = &nullWriter{}
