// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

var applierStatsPollInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"logical_replication.txn_applier.stats_polling_interval",
	"how often the txn-mode applier polls and calculates metrics.",
	30*time.Second,
	settings.PositiveDuration,
)

// startApplierStatsPoller periodically polls to calculate applier metrics.
func (a *Applier) startApplierStatsPoller(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		a.updateInFlightTxnMetrics()
	}
}

// updateInFlightTxnMetrics computes and updates in flight transaction metrics.
func (a *Applier) updateInFlightTxnMetrics() {
	txnWait, horizonWait, ready := a.computeInFlightTxnStats()

	a.metrics.TxnApplierTxnWaitTxns.Update(txnWait)
	a.metrics.TxnApplierHorizonWaitTxns.Update(horizonWait)
	a.metrics.TxnApplierReadyTxns.Update(ready)

	if a.metricsLabel != "" {
		labels := map[string]string{"label": a.metricsLabel}
		a.metrics.LabeledTxnApplierTxnWaitTxns.Update(labels, txnWait)
		a.metrics.LabeledTxnApplierHorizonWaitTxns.Update(labels, horizonWait)
		a.metrics.LabeledTxnApplierReadyTxns.Update(labels, ready)
	}
}

// computeInFlightTxnStats returns the count of in-flight transactions in each
// state of the applier.
// N.B. the states are mutually exclusive, e.g. a transaction is only waiting
// on the horizon once all of its dependencies are resolved.
func (a *Applier) computeInFlightTxnStats() (txnWait, horizonWait, ready int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	globalFrontier := a.getGlobalFrontierLocked()
	for _, txn := range a.mu.transactions {
		switch {
		case txn.remainingDeps > 0:
			txnWait++
		case !txn.EventHorizon.LessEq(globalFrontier):
			horizonWait++
		}
	}
	ready = int64(len(a.mu.transactions)) - txnWait - horizonWait
	return txnWait, horizonWait, ready
}
