// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
)

// Controller implements the SQL Stats subsystem control plane. This exposes
// administrative interfaces that can be consumed by other parts of the database
// (e.g. status server, builtins) to control the behavior of the SQL Stats
// subsystem.
type Controller struct {
	*sslocal.Controller
	db isql.DB
	st *cluster.Settings
}

// NewController returns a new instance of sqlstats.Controller.
func NewController(
	sqlStats *PersistedSQLStats, status serverpb.SQLStatusServer, db isql.DB,
) *Controller {
	return &Controller{
		Controller: sslocal.NewController(sqlStats.SQLStats, status),
		db:         db,
		st:         sqlStats.cfg.Settings,
	}
}

// CreateSQLStatsCompactionSchedule implements the tree.SQLStatsController
// interface.
func (s *Controller) CreateSQLStatsCompactionSchedule(ctx context.Context) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := CreateSQLStatsCompactionScheduleIfNotYetExist(ctx, txn, s.st)
		return err
	})
}

// ResetClusterSQLStats implements the tree.SQLStatsController interface. This
// method resets both the cluster-wide in-memory stats (via RPC fanout) and
// persisted stats (via TRUNCATE SQL statement)
func (s *Controller) ResetClusterSQLStats(ctx context.Context) error {
	if err := s.Controller.ResetClusterSQLStats(ctx); err != nil {
		return err
	}

	resetSysTableStats := func(tableName string) (err error) {
		ex := s.db.Executor()
		_, err = ex.ExecEx(
			ctx,
			"reset-sql-stats",
			nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			"TRUNCATE "+tableName)
		return err
	}
	if err := resetSysTableStats("system.statement_statistics"); err != nil {
		return err
	}

	return resetSysTableStats("system.transaction_statistics")
}
