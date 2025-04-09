// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
)

// Provider offers access to the insights subsystem.
type Provider struct {
	store           *LockingStore
	registry        *lockingRegistry
	anomalyDetector *AnomalyDetector
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

// ObserveTransaction implements sslocal.SQLStatsSink
func (p *Provider) ObserveTransaction(
	_ctx context.Context,
	transactionStats *sqlstats.RecordedTxnStats,
	statements []*sqlstats.RecordedStmtStats,
) {
	p.registry.observeTransaction(transactionStats, statements)
}
