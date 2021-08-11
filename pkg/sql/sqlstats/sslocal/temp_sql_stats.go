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
	"strings"

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
	return strings.Compare(s[i].Key.KeyData.App, s[j].Key.KeyData.App) == -1
}

// Swap implements the sort.Interface interface.
func (s stmtResponseList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// NewTempSQLStatsFromExistingData returns an instance of TempSQLStats populated
// from the provided slice of roachpb.CollectedStatementStatistics.
//
// This constructor returns a variant of SQLStats which is used to aggregate
// RPC-fanout results. This means that, unliked the regular SQLStats, whose
// lifetime is same as the sql.Server, the lifetime of this variant is only as
// long as the duration of the RPC request itself. This is why it bypasses the
// existing memory accounting infrastructure and fingerprint cluster limit.
func NewTempSQLStatsFromExistingData(
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
			ssmemstorage.NewTempContainerFromExistingData(statistics)
		if err != nil {
			return nil, err
		}

		s.mu.apps[appName] = container
	}

	return s, nil
}
