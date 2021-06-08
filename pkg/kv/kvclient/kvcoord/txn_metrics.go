// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts          *metric.Counter
	Commits         *metric.Counter
	Commits1PC      *metric.Counter // Commits which finished in a single phase
	ParallelCommits *metric.Counter // Commits which entered the STAGING state
	CommitWaits     *metric.Counter // Commits that waited for linearizability

	RefreshSuccess                *metric.Counter
	RefreshFail                   *metric.Counter
	RefreshFailWithCondensedSpans *metric.Counter
	RefreshMemoryLimitExceeded    *metric.Counter
	RefreshAutoRetries            *metric.Counter

	Durations *metric.Histogram

	TxnsWithCondensedIntents      *metric.Counter
	TxnsWithCondensedIntentsGauge *metric.Gauge

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram

	// Counts of restart types.
	RestartsWriteTooOld           telemetry.CounterWithMetric
	RestartsWriteTooOldMulti      telemetry.CounterWithMetric
	RestartsSerializable          telemetry.CounterWithMetric
	RestartsAsyncWriteFailure     telemetry.CounterWithMetric
	RestartsReadWithinUncertainty telemetry.CounterWithMetric
	RestartsTxnAborted            telemetry.CounterWithMetric
	RestartsTxnPush               telemetry.CounterWithMetric
	RestartsUnknown               telemetry.CounterWithMetric

	// End transaction failure counters.
	RollbacksFailed      *metric.Counter
	AsyncRollbacksFailed *metric.Counter
}

var (
	metaAbortsRates = metric.Metadata{
		Name:        "txn.aborts",
		Help:        "Number of aborted KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommitsRates = metric.Metadata{
		Name:        "txn.commits",
		Help:        "Number of committed KV transactions (including 1PC)",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommits1PCRates = metric.Metadata{
		Name:        "txn.commits1PC",
		Help:        "Number of KV transaction one-phase commit attempts",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaParallelCommitsRates = metric.Metadata{
		Name:        "txn.parallelcommits",
		Help:        "Number of KV transaction parallel commit attempts",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommitWaitCount = metric.Metadata{
		Name: "txn.commit_waits",
		Help: "Number of KV transactions that had to commit-wait on commit " +
			"in order to ensure linearizability. This generally happens to " +
			"transactions writing to global ranges.",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshSuccess = metric.Metadata{
		Name: "txn.refresh.success",
		Help: "Number of successful transaction refreshes. A refresh may be " +
			"preemptive or reactive. A reactive refresh is performed after a " +
			"request throws an error because a refresh is needed for it to " +
			"succeed. In these cases, the request will be re-issued as an " +
			"auto-retry (see txn.refresh.auto_retries) after the refresh succeeds.",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshFail = metric.Metadata{
		Name:        "txn.refresh.fail",
		Help:        "Number of failed transaction refreshes",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshFailWithCondensedSpans = metric.Metadata{
		Name: "txn.refresh.fail_with_condensed_spans",
		Help: "Number of failed refreshes for transactions whose read " +
			"tracking lost fidelity because of condensing. Such a failure " +
			"could be a false conflict. Failures counted here are also counted " +
			"in txn.refresh.fail, and the respective transactions are also counted in " +
			"txn.refresh.memory_limit_exceeded.",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshMemoryLimitExceeded = metric.Metadata{
		Name: "txn.refresh.memory_limit_exceeded",
		Help: "Number of transaction which exceed the refresh span bytes limit, causing " +
			"their read spans to be condensed",
		Measurement: "Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRefreshAutoRetries = metric.Metadata{
		Name:        "txn.refresh.auto_retries",
		Help:        "Number of request retries after successful refreshes",
		Measurement: "Retries",
		Unit:        metric.Unit_COUNT,
	}
	metaDurationsHistograms = metric.Metadata{
		Name:        "txn.durations",
		Help:        "KV transaction durations",
		Measurement: "KV Txn Duration",
		Unit:        metric.Unit_NANOSECONDS,
	}
	metaTxnsWithCondensedIntentSpans = metric.Metadata{
		Name: "txn.condensed_intent_spans",
		Help: "KV transactions that have exceeded their intent tracking " +
			"memory budget (kv.transaction.max_intents_bytes). See also " +
			"txn.condensed_intent_spans_gauge for a gauge of such transactions currently running.",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaTxnsWithCondensedIntentSpansGauge = metric.Metadata{
		Name: "txn.condensed_intent_spans_gauge",
		Help: "KV transactions currently running that have exceeded their intent tracking " +
			"memory budget (kv.transaction.max_intents_bytes). See also txn.condensed_intent_spans " +
			"for a perpetual counter/rate.",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsHistogram = metric.Metadata{
		Name:        "txn.restarts",
		Help:        "Number of restarted KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	// There are two ways we can get "write too old" restarts. In both cases, a
	// WriteTooOldError is generated in the MVCC layer. This is intercepted on
	// the way out by the Store, which performs a single retry at a pushed
	// timestamp. If the retry succeeds, the immediate operation succeeds but
	// the WriteTooOld flag is set on the Transaction, which causes EndTxn to
	// return a/ TransactionRetryError with RETRY_WRITE_TOO_OLD. These are
	// captured as txn.restarts.writetooold.
	//
	// If the Store's retried operation generates a second WriteTooOldError
	// (indicating a conflict with a third transaction with a higher timestamp
	// than the one that caused the first WriteTooOldError), the store doesn't
	// retry again, and the WriteTooOldError will be returned up the stack to be
	// retried at this level. These are captured as
	// txn.restarts.writetoooldmulti. This path is inefficient, and if it turns
	// out to be common we may want to do something about it.
	metaRestartsWriteTooOld = metric.Metadata{
		Name:        "txn.restarts.writetooold",
		Help:        "Number of restarts due to a concurrent writer committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsWriteTooOldMulti = metric.Metadata{
		Name:        "txn.restarts.writetoooldmulti",
		Help:        "Number of restarts due to multiple concurrent writers committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsSerializable = metric.Metadata{
		Name:        "txn.restarts.serializable",
		Help:        "Number of restarts due to a forwarded commit timestamp and isolation=SERIALIZABLE",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsPossibleReplay = metric.Metadata{
		Name:        "txn.restarts.possiblereplay",
		Help:        "Number of restarts due to possible replays of command batches at the storage layer",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsAsyncWriteFailure = metric.Metadata{
		Name:        "txn.restarts.asyncwritefailure",
		Help:        "Number of restarts due to async consensus writes that failed to leave intents",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsReadWithinUncertainty = metric.Metadata{
		Name:        "txn.restarts.readwithinuncertainty",
		Help:        "Number of restarts due to reading a new value within the uncertainty interval",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsTxnAborted = metric.Metadata{
		Name:        "txn.restarts.txnaborted",
		Help:        "Number of restarts due to an abort by a concurrent transaction (usually due to deadlock)",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	// TransactionPushErrors at this level are unusual. They are
	// normally handled at the Store level with the txnwait and
	// contention queues. However, they can reach this level and be
	// retried in tests that disable the store-level retries, and
	// there may be edge cases that allow them to reach this point in
	// production.
	metaRestartsTxnPush = metric.Metadata{
		Name:        "txn.restarts.txnpush",
		Help:        "Number of restarts due to a transaction push failure",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsUnknown = metric.Metadata{
		Name:        "txn.restarts.unknown",
		Help:        "Number of restarts due to a unknown reasons",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRollbacksFailed = metric.Metadata{
		Name:        "txn.rollbacks.failed",
		Help:        "Number of KV transaction that failed to send final abort",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaAsyncRollbacksFailed = metric.Metadata{
		Name:        "txn.rollbacks.async.failed",
		Help:        "Number of KV transaction that failed to send abort asynchronously which is not always retried",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:                        metric.NewCounter(metaAbortsRates),
		Commits:                       metric.NewCounter(metaCommitsRates),
		Commits1PC:                    metric.NewCounter(metaCommits1PCRates),
		ParallelCommits:               metric.NewCounter(metaParallelCommitsRates),
		CommitWaits:                   metric.NewCounter(metaCommitWaitCount),
		RefreshSuccess:                metric.NewCounter(metaRefreshSuccess),
		RefreshFail:                   metric.NewCounter(metaRefreshFail),
		RefreshFailWithCondensedSpans: metric.NewCounter(metaRefreshFailWithCondensedSpans),
		RefreshMemoryLimitExceeded:    metric.NewCounter(metaRefreshMemoryLimitExceeded),
		RefreshAutoRetries:            metric.NewCounter(metaRefreshAutoRetries),
		Durations:                     metric.NewLatency(metaDurationsHistograms, histogramWindow),
		TxnsWithCondensedIntents:      metric.NewCounter(metaTxnsWithCondensedIntentSpans),
		TxnsWithCondensedIntentsGauge: metric.NewGauge(metaTxnsWithCondensedIntentSpansGauge),
		Restarts:                      metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
		RestartsWriteTooOld:           telemetry.NewCounterWithMetric(metaRestartsWriteTooOld),
		RestartsWriteTooOldMulti:      telemetry.NewCounterWithMetric(metaRestartsWriteTooOldMulti),
		RestartsSerializable:          telemetry.NewCounterWithMetric(metaRestartsSerializable),
		RestartsAsyncWriteFailure:     telemetry.NewCounterWithMetric(metaRestartsAsyncWriteFailure),
		RestartsReadWithinUncertainty: telemetry.NewCounterWithMetric(metaRestartsReadWithinUncertainty),
		RestartsTxnAborted:            telemetry.NewCounterWithMetric(metaRestartsTxnAborted),
		RestartsTxnPush:               telemetry.NewCounterWithMetric(metaRestartsTxnPush),
		RestartsUnknown:               telemetry.NewCounterWithMetric(metaRestartsUnknown),
		RollbacksFailed:               metric.NewCounter(metaRollbacksFailed),
		AsyncRollbacksFailed:          metric.NewCounter(metaAsyncRollbacksFailed),
	}
}
