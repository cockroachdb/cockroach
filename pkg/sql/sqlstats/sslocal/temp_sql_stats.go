// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	sort.Sort(stmtResponseList(statistics))

	var err error
	s := &SQLStats{}
	s.mu.apps = make(map[string]*ssmemstorage.Container)

	for len(statistics) > 0 {
		appName := statistics[0].Key.KeyData.App
		var container *ssmemstorage.Container

		container, statistics, err =
			ssmemstorage.NewTempContainerFromExistingStmtStats(statistics)
		if err != nil {
			return nil, err
		}

		s.mu.apps[appName] = container
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

	for len(statistics) > 0 {
		appName := statistics[0].StatsData.App
		var container *ssmemstorage.Container

		container, statistics, err =
			ssmemstorage.NewTempContainerFromExistingTxnStats(statistics)
		if err != nil {
			return nil, err
		}

		s.mu.apps[appName] = container
	}

	return s, nil
}
