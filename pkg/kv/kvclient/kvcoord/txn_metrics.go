// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts                    *metric.Counter
	Commits                   *metric.Counter
	Commits1PC                *metric.Counter // Commits which finished in a single phase
	CommitsReadOnly           *metric.Counter // Commits which finished without acquiring locks
	ParallelCommits           *metric.Counter // Commits which entered the STAGING state
	ParallelCommitAutoRetries *metric.Counter // Commits which were retried after entering the STAGING state
	CommitWaits               *metric.Counter // Commits that waited for linearizability
	Prepares                  *metric.Counter

	ClientRefreshSuccess                *metric.Counter
	ClientRefreshFail                   *metric.Counter
	ClientRefreshFailWithCondensedSpans *metric.Counter
	ClientRefreshMemoryLimitExceeded    *metric.Counter
	ClientRefreshAutoRetries            *metric.Counter
	ServerRefreshSuccess                *metric.Counter

	Durations metric.IHistogram

	TxnsWithCondensedIntents            *metric.Counter
	TxnsWithCondensedIntentsGauge       *metric.Gauge
	TxnsRejectedByLockSpanBudget        *metric.Counter
	TxnsRejectedByCountLimit            *metric.Counter
	TxnsResponseOverCountLimit          *metric.Counter
	TxnsInFlightLocksOverTrackingBudget *metric.Counter

	// Restarts is the number of times we had to restart the transaction.
	Restarts metric.IHistogram

	// Counts of restart types.
	RestartsWriteTooOld            telemetry.CounterWithMetric
	RestartsSerializable           telemetry.CounterWithMetric
	RestartsAsyncWriteFailure      telemetry.CounterWithMetric
	RestartsCommitDeadlineExceeded telemetry.CounterWithMetric
	RestartsReadWithinUncertainty  telemetry.CounterWithMetric
	RestartsTxnAborted             telemetry.CounterWithMetric
	RestartsTxnPush                telemetry.CounterWithMetric
	RestartsUnknown                telemetry.CounterWithMetric

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
		Help:        "Number of KV transaction one-phase commits",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaCommitsReadOnly = metric.Metadata{
		Name:        "txn.commits_read_only",
		Help:        "Number of read only KV transaction commits",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaParallelCommitsRates = metric.Metadata{
		Name:        "txn.parallelcommits",
		Help:        "Number of KV transaction parallel commits",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaParallelCommitAutoRetries = metric.Metadata{
		Name:        "txn.parallelcommits.auto_retries",
		Help:        "Number of commit tries after successful failed parallel commit attempts",
		Measurement: "Retries",
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
	metaPreparesRates = metric.Metadata{
		Name:        "txn.prepares",
		Help:        "Number of prepared KV transactions",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaClientRefreshSuccess = metric.Metadata{
		Name: "txn.refresh.success",
		Help: "Number of successful client-side transaction refreshes. A refresh may be " +
			"preemptive or reactive. A reactive refresh is performed after a " +
			"request throws an error because a refresh is needed for it to " +
			"succeed. In these cases, the request will be re-issued as an " +
			"auto-retry (see txn.refresh.auto_retries) after the refresh succeeds.",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaClientRefreshFail = metric.Metadata{
		Name:        "txn.refresh.fail",
		Help:        "Number of failed client-side transaction refreshes",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaClientRefreshFailWithCondensedSpans = metric.Metadata{
		Name: "txn.refresh.fail_with_condensed_spans",
		Help: "Number of failed client-side refreshes for transactions whose read " +
			"tracking lost fidelity because of condensing. Such a failure " +
			"could be a false conflict. Failures counted here are also counted " +
			"in txn.refresh.fail, and the respective transactions are also counted in " +
			"txn.refresh.memory_limit_exceeded.",
		Measurement: "Refreshes",
		Unit:        metric.Unit_COUNT,
	}
	metaClientRefreshMemoryLimitExceeded = metric.Metadata{
		Name: "txn.refresh.memory_limit_exceeded",
		Help: "Number of transaction which exceed the refresh span bytes limit, causing " +
			"their read spans to be condensed",
		Measurement: "Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaClientRefreshAutoRetries = metric.Metadata{
		Name:        "txn.refresh.auto_retries",
		Help:        "Number of request retries after successful client-side refreshes",
		Measurement: "Retries",
		Unit:        metric.Unit_COUNT,
	}
	metaServerRefreshSuccess = metric.Metadata{
		Name:        "txn.refresh.success_server_side",
		Help:        "Number of successful server-side transaction refreshes",
		Measurement: "Refreshes",
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
	metaTxnsRejectedByLockSpanBudget = metric.Metadata{
		Name: "txn.condensed_intent_spans_rejected",
		Help: "KV transactions that have been aborted because they exceeded their intent tracking " +
			"memory budget (kv.transaction.max_intents_bytes). " +
			"Rejection is caused by kv.transaction.reject_over_max_intents_budget.",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaTxnsRejectedByCountLimit = metric.Metadata{
		Name:        "txn.count_limit_rejected",
		Help:        "KV transactions that have been aborted because they exceeded the max number of writes and locking reads allowed",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaTxnsResponseOverCountLimit = metric.Metadata{
		Name:        "txn.count_limit_on_response",
		Help:        "KV transactions that have exceeded the count limit on a response",
		Measurement: "KV Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaTxnsInflightLocksOverTrackingBudget = metric.Metadata{
		Name: "txn.inflight_locks_over_tracking_budget",
		Help: "KV transactions whose in-flight writes and locking reads have exceeded " +
			"the intent tracking memory budget (kv.transaction.max_intents_bytes).",
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
	metaRestartsWriteTooOld = metric.Metadata{
		Name:        "txn.restarts.writetooold",
		Help:        "Number of restarts due to a concurrent writer committing first",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsSerializable = metric.Metadata{
		Name:        "txn.restarts.serializable",
		Help:        "Number of restarts due to a forwarded commit timestamp and isolation=SERIALIZABLE",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsAsyncWriteFailure = metric.Metadata{
		Name:        "txn.restarts.asyncwritefailure",
		Help:        "Number of restarts due to async consensus writes that failed to leave intents",
		Measurement: "Restarted Transactions",
		Unit:        metric.Unit_COUNT,
	}
	metaRestartsCommitDeadlineExceeded = metric.Metadata{
		Name:        "txn.restarts.commitdeadlineexceeded",
		Help:        "Number of restarts due to a transaction exceeding its deadline",
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
		Aborts:                              metric.NewCounter(metaAbortsRates),
		Commits:                             metric.NewCounter(metaCommitsRates),
		Commits1PC:                          metric.NewCounter(metaCommits1PCRates),
		CommitsReadOnly:                     metric.NewCounter(metaCommitsReadOnly),
		ParallelCommits:                     metric.NewCounter(metaParallelCommitsRates),
		ParallelCommitAutoRetries:           metric.NewCounter(metaParallelCommitAutoRetries),
		CommitWaits:                         metric.NewCounter(metaCommitWaitCount),
		Prepares:                            metric.NewCounter(metaPreparesRates),
		ClientRefreshSuccess:                metric.NewCounter(metaClientRefreshSuccess),
		ClientRefreshFail:                   metric.NewCounter(metaClientRefreshFail),
		ClientRefreshFailWithCondensedSpans: metric.NewCounter(metaClientRefreshFailWithCondensedSpans),
		ClientRefreshMemoryLimitExceeded:    metric.NewCounter(metaClientRefreshMemoryLimitExceeded),
		ClientRefreshAutoRetries:            metric.NewCounter(metaClientRefreshAutoRetries),
		ServerRefreshSuccess:                metric.NewCounter(metaServerRefreshSuccess),
		Durations: metric.NewHistogram(metric.HistogramOptions{
			Mode:         metric.HistogramModePreferHdrLatency,
			Metadata:     metaDurationsHistograms,
			Duration:     histogramWindow,
			BucketConfig: metric.IOLatencyBuckets,
		}),
		TxnsWithCondensedIntents:            metric.NewCounter(metaTxnsWithCondensedIntentSpans),
		TxnsWithCondensedIntentsGauge:       metric.NewGauge(metaTxnsWithCondensedIntentSpansGauge),
		TxnsRejectedByLockSpanBudget:        metric.NewCounter(metaTxnsRejectedByLockSpanBudget),
		TxnsRejectedByCountLimit:            metric.NewCounter(metaTxnsRejectedByCountLimit),
		TxnsResponseOverCountLimit:          metric.NewCounter(metaTxnsResponseOverCountLimit),
		TxnsInFlightLocksOverTrackingBudget: metric.NewCounter(metaTxnsInflightLocksOverTrackingBudget),
		Restarts: metric.NewHistogram(metric.HistogramOptions{
			Metadata:     metaRestartsHistogram,
			Duration:     histogramWindow,
			MaxVal:       100,
			SigFigs:      3,
			BucketConfig: metric.Count1KBuckets,
		}),
		RestartsWriteTooOld:            telemetry.NewCounterWithMetric(metaRestartsWriteTooOld),
		RestartsSerializable:           telemetry.NewCounterWithMetric(metaRestartsSerializable),
		RestartsAsyncWriteFailure:      telemetry.NewCounterWithMetric(metaRestartsAsyncWriteFailure),
		RestartsCommitDeadlineExceeded: telemetry.NewCounterWithMetric(metaRestartsCommitDeadlineExceeded),
		RestartsReadWithinUncertainty:  telemetry.NewCounterWithMetric(metaRestartsReadWithinUncertainty),
		RestartsTxnAborted:             telemetry.NewCounterWithMetric(metaRestartsTxnAborted),
		RestartsTxnPush:                telemetry.NewCounterWithMetric(metaRestartsTxnPush),
		RestartsUnknown:                telemetry.NewCounterWithMetric(metaRestartsUnknown),
		RollbacksFailed:                metric.NewCounter(metaRollbacksFailed),
		AsyncRollbacksFailed:           metric.NewCounter(metaAsyncRollbacksFailed),
	}
}
