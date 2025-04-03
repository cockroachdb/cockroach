// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ssremote

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// SQLStats is a remote implementation of the sqlstats.SSDrainer interface.
type SQLStats struct {
	statusServer serverpb.SQLStatusServer
}

var _ sqlstats.SSDrainer = &SQLStats{}

func New(statusServer serverpb.SQLStatusServer) *SQLStats {
	return &SQLStats{statusServer: statusServer}
}

// DrainStats drains the stats from the cluster using the DrainSqlStats RPC.
// See statusServer.DrainSqlStats for more details.
func (ss *SQLStats) DrainStats(
	ctx context.Context,
) (
	[]*appstatspb.CollectedStatementStatistics,
	[]*appstatspb.CollectedTransactionStatistics,
	int64,
) {
	resp, err := ss.statusServer.DrainSqlStats(ctx, &serverpb.DrainSqlStatsRequest{})
	if err != nil {
		log.Warningf(ctx, "Error calling statusServer.DrainSqlStats. err=%s", err.Error())
		return nil, nil, 0
	}
	return resp.Statements, resp.Transactions, resp.FingerprintCount
}

// Reset Resets in-memory stats for cluster.
func (ss *SQLStats) Reset(ctx context.Context) error {
	_, err := ss.statusServer.ResetSQLStats(ctx, &serverpb.ResetSQLStatsRequest{ResetPersistedStats: false})
	return err
}
