// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sqlstats is a subsystem that is responsible for tracking the
// statistics of statements and transactions.

package ssmemstorage

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type SQLStatsAtomicCounters struct {
	st *cluster.Settings

	// uniqueStmtFingerprintLimit is the limit on number of unique statement
	// fingerprints we can store in memory.
	UniqueStmtFingerprintLimit *settings.IntSetting

	// uniqueTxnFingerprintLimit is the limit on number of unique transaction
	// fingerprints we can store in memory.
	UniqueTxnFingerprintLimit *settings.IntSetting

	// uniqueStmtFingerprintCount is the number of unique statement fingerprints
	// we are storing in memory.
	uniqueStmtFingerprintCount int64

	// uniqueTxnFingerprintCount is the number of unique transaction fingerprints
	// we are storing in memory.
	uniqueTxnFingerprintCount int64

	// discardUniqueStmtFingerprintCount is the number of unique statement
	// fingerprints that are discard because of memory limitations.
	discardUniqueStmtFingerprintCount int64

	// discardUniqueTxnFingerprintCount is the number of unique transaction
	// fingerprints that are discard because of memory limitations.
	discardUniqueTxnFingerprintCount int64

	mu struct {
		syncutil.Mutex

		// lastDiscardLogMessageSent is the last time a log message was sent for
		// statistics being discarded because of memory pressure.
		lastDiscardLogMessageSent time.Time
	}
}

// DiscardedStatsLogInterval specifies the interval between log emissions for discarded
// statement and transaction statistics due to reaching the SQL statistics memory limit.
var DiscardedStatsLogInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.metrics.discarded_stats_log.interval",
	"interval between log emissions for discarded statistics due to SQL statistics memory limit",
	1*time.Minute,
	settings.NonNegativeDuration,
	settings.WithVisibility(settings.Reserved))

func NewSQLStatsAtomicCounters(
	st *cluster.Settings,
	uniqueStmtFingerprintLimit *settings.IntSetting,
	uniqueTxnFingerprintLimit *settings.IntSetting,
) *SQLStatsAtomicCounters {
	return &SQLStatsAtomicCounters{
		st:                         st,
		UniqueStmtFingerprintLimit: uniqueStmtFingerprintLimit,
		UniqueTxnFingerprintLimit:  uniqueTxnFingerprintLimit,
	}
}

// maybeLogDiscardMessage logs a warning if statement or transaction
// fingerprints were discarded because of memory limits and enough time passed
// since the last time the warning was logged. This is necessary to avoid
// flooding the log with warnings once the limit is hit.
func (s *SQLStatsAtomicCounters) maybeLogDiscardMessage(ctx context.Context) {
	discardSmtCnt := atomic.LoadInt64(&s.discardUniqueStmtFingerprintCount)
	discardTxnCnt := atomic.LoadInt64(&s.discardUniqueTxnFingerprintCount)
	if discardSmtCnt == 0 && discardTxnCnt == 0 {
		return
	}

	// Get the config values before the lock to reduce time in the lock.
	discardLogInterval := DiscardedStatsLogInterval.Get(&s.st.SV)
	stmtLimit := s.UniqueStmtFingerprintLimit.Get(&s.st.SV)
	txnLimit := s.UniqueTxnFingerprintLimit.Get(&s.st.SV)
	s.mu.Lock()
	defer s.mu.Unlock()
	timeNow := timeutil.Now()

	// Not enough time has passed since the last log message was sent.
	if timeNow.Sub(s.mu.lastDiscardLogMessageSent) < discardLogInterval {
		return
	}

	// The discard counts might be slightly off because it's possible that the
	// count changed after the initial load and before the log message is sent.
	// The count being slightly off won't impact users looking at the message. It
	// also avoids holding a lock on the counts which would block requests until
	// the log is sent.
	log.Warningf(ctx, "statistics discarded due to memory limit. transaction discard count: %d with limit: %d, statement discard count: %d with limit: %d, logged at interval: %s, last logged: %s",
		discardTxnCnt, stmtLimit, discardSmtCnt, txnLimit, discardLogInterval, s.mu.lastDiscardLogMessageSent)
	s.mu.lastDiscardLogMessageSent = timeNow

	// Reset the discard count back to 0 since the value was logged
	atomic.StoreInt64(&s.discardUniqueStmtFingerprintCount, int64(0))
	atomic.StoreInt64(&s.discardUniqueTxnFingerprintCount, int64(0))
}

// tryAddStmtFingerprint attempts to add 1 to the server level count for
// statement level fingerprints and returns false if it is being throttled.
func (s *SQLStatsAtomicCounters) tryAddStmtFingerprint() (ok bool) {
	limit := s.UniqueStmtFingerprintLimit.Get(&s.st.SV)

	// We check if we have reached the limit of unique fingerprints we can
	// store.
	incrementedFingerprintCount :=
		atomic.AddInt64(&s.uniqueStmtFingerprintCount, int64(1) /* delts */)

	// Abort if we have exceeded limit of unique statement fingerprints.
	if incrementedFingerprintCount < limit {
		return true
	}

	atomic.AddInt64(&s.discardUniqueStmtFingerprintCount, int64(1))
	atomic.AddInt64(&s.uniqueStmtFingerprintCount, -int64(1) /* delts */)
	return false
}

// tryAddTxnFingerprint attempts to add 1 to the server level count for
// transaction level fingerprints and returns false if it is being throttled.
func (s *SQLStatsAtomicCounters) tryAddTxnFingerprint() (ok bool) {
	limit := s.UniqueTxnFingerprintLimit.Get(&s.st.SV)

	// We check if we have reached the limit of unique fingerprints we can
	// store.
	incrementedFingerprintCount :=
		atomic.AddInt64(&s.uniqueTxnFingerprintCount, int64(1) /* delts */)

	if incrementedFingerprintCount < limit {
		return true
	}

	atomic.AddInt64(&s.discardUniqueTxnFingerprintCount, int64(1))
	atomic.AddInt64(&s.uniqueTxnFingerprintCount, -int64(1) /* delts */)
	return false
}

// freeByCnt decrements the statement and transaction count by the value
// passed in. This is used in scenarios where an entire container which is
// per an app name is being cleaned up.
func (s *SQLStatsAtomicCounters) freeByCnt(
	uniqueStmtFingerprintCount, uniqueTxnFingerprintCount int64,
) {
	atomic.AddInt64(&s.uniqueStmtFingerprintCount, -uniqueStmtFingerprintCount)
	atomic.AddInt64(&s.uniqueTxnFingerprintCount, -uniqueTxnFingerprintCount)
}

// GetTotalFingerprintCount returns total number of unique statement and
// transaction fingerprints stored in the current SQLStats.
func (s *SQLStatsAtomicCounters) GetTotalFingerprintCount() int64 {
	return atomic.LoadInt64(&s.uniqueStmtFingerprintCount) + atomic.LoadInt64(&s.uniqueTxnFingerprintCount)
}

// GetStatementCount returns the number of unique statement fingerprints stored
// in the current SQLStats.
func (s *SQLStatsAtomicCounters) GetStatementCount() int64 {
	return atomic.LoadInt64(&s.uniqueStmtFingerprintCount)
}
