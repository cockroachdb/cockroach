// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

type stmtResponseList []serverpb.StatementsResponse_CollectedStatementStatistics

var _ sort.Interface = stmtResponseList{}

// Len implements the sort.Interface interface.
func (s stmtResponseList) Len() int {
	return len(s)
}

// Less implements the sort.Interface interface.
func (s stmtResponseList) Less(i, j int) bool {
	return s[i].Key.KeyData.App < s[j].Key.KeyData.App
}

// Swap implements the sort.Interface interface.
func (s stmtResponseList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type txnResponseList []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics

var _ sort.Interface = txnResponseList{}

// Len implements the sort.Interface interface.
func (t txnResponseList) Len() int {
	return len(t)
}

// Less implements the sort.Interface interface.
func (t txnResponseList) Less(i, j int) bool {
	return t[i].StatsData.App < t[j].StatsData.App
}

// Swap implements the sort.Interface interface.
func (t txnResponseList) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// NewTempSQLStatsFromExistingStmtStats returns an instance of SQLStats populated
// from the provided slice of appstatspb.CollectedStatementStatistics.
//
// This constructor returns a variant of SQLStats which is used to aggregate
// RPC-fanout results. This means that, unliked the regular SQLStats, whose
// lifetime is same as the sql.Server, the lifetime of this variant is only as
// long as the duration of the RPC request itself. This is why it bypasses the
// existing memory accounting infrastructure and fingerprint cluster limit.
func NewTempSQLStatsFromExistingStmtStats(
	statistics []serverpb.StatementsResponse_CollectedStatementStatistics,
) (*SQLStats, error) {
	var err error
	s := &SQLStats{}
	s.mu.apps = make(map[string]*ssmemstorage.Container)

	for i := range statistics {
		appName := statistics[i].Key.KeyData.App
		container, ok := s.mu.apps[appName]
		if !ok {
			container = ssmemstorage.New(
				nil, /* st */
				nil, /* uniqueStmtFingerprintLimit */
				nil, /* uniqueTxnFingerprintLimit */
				nil, /* uniqueStmtFingerprintCount */
				nil, /* uniqueTxnFingerprintCount */
				nil, /* mon */
				appName,
				nil, /* knobs */
				nil, /* insights */
				nil, /*latencyInformation */
			)
			s.mu.apps[appName] = container
		}

		if err = container.InsertExistingStmtStats(statistics[i]); err != nil {
			return nil, err
		}

	}

	return s, nil
}

// NewTempSQLStatsFromExistingTxnStats returns an instance of SQLStats populated
// from the provided slice of CollectedTransactionStatistics.
//
// This constructor returns a variant of SQLStats which is used to aggregate
// RPC-fanout results. This means that, unliked the regular SQLStats, whose
// lifetime is same as the sql.Server, the lifetime of this variant is only as
// long as the duration of the RPC request itself. This is why it bypasses the
// existing memory accounting infrastructure and fingerprint cluster limit.
func NewTempSQLStatsFromExistingTxnStats(
	statistics []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics,
) (*SQLStats, error) {
	sort.Sort(txnResponseList(statistics))

	var err error
	s := &SQLStats{}
	s.mu.apps = make(map[string]*ssmemstorage.Container)

	for i := range statistics {
		appName := statistics[i].StatsData.App
		container, ok := s.mu.apps[appName]
		if !ok {
			container = ssmemstorage.New(
				nil, /* st */
				nil, /* uniqueStmtFingerprintLimit */
				nil, /* uniqueTxnFingerprintLimit */
				nil, /* uniqueStmtFingerprintCount */
				nil, /* uniqueTxnFingerprintCount */
				nil, /* mon */
				appName,
				nil, /* knobs */
				nil, /* insights */
				nil, /* latencyInformation */
			)
			s.mu.apps[appName] = container
		}

		if err = container.InsertExistingTxnStats(statistics[i]); err != nil {
			return nil, err
		}

	}

	return s, nil
}
