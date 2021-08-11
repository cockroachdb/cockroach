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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

// NewTempSQLStatsFromExistingData returns an instance of TempSQLStats populated
// from the provided slice of roachpb.CollectedStatementStatistics.
//
// This constructor returns a variant of SQLStats which is used to aggregate
// RPC-fanout results. This means that, unliked the regular SQLStats, whose
// lifetime is same as the sql.Server, the lifetime of this variant is only as
// long as the duration of the RPC request itself. This is why:
// * it bypasses the existing memory accounting infrastructure and
//   fingerprint cluster limit.
// * it bypasses the mutex locking when it's inserting new data since
//   it SHOULD NOT be used concurrently by multiple goroutines.
func NewTempSQLStatsFromExistingData(
	statistics []serverpb.StatementsResponse_CollectedStatementStatistics,
) (*SQLStats, error) {
	s := &SQLStats{}

	s.mu.apps = make(map[string]*ssmemstorage.Container)

	for i := range statistics {
		container := s.unsafeGetStatsContainerForAppName(statistics[i].Key.KeyData.App)
		if err := container.UnsafeInsertStmtStats(
			statistics[i].ID,
			&statistics[i].Key.KeyData,
			&statistics[i].Stats); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (s *SQLStats) unsafeGetStatsContainerForAppName(appName string) *ssmemstorage.Container {
	if a, ok := s.mu.apps[appName]; ok {
		return a
	}

	a := ssmemstorage.New(
		nil, /* st */
		nil, /* uniqueStmtFingerprintLimit */
		nil, /* uniqueTxnFingerprintLimit */
		nil, /* uniqueStmtFingerprintCount */
		nil, /* uniqueTxnFingerprintCount */
		nil, /* mon */
		appName,
	)
	s.mu.apps[appName] = a
	return a
}
