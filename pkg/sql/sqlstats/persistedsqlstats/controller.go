// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
)

// Controller implements the SQL Stats subsystem control plane. This exposes
// administrative interfaces that can be consumed by other parts of the database
// (e.g. status server, builtins) to control the behavior of the SQL Stats
// subsystem.
type Controller struct {
	*sslocal.Controller
	db        isql.DB
	st        *cluster.Settings
	clusterID func() uuid.UUID
}

// NewController returns a new instance of sqlstats.Controller.
func NewController(
	sqlStats *PersistedSQLStats, status serverpb.SQLStatusServer, db isql.DB,
) *Controller {
	return &Controller{
		Controller: sslocal.NewController(sqlStats.SQLStats, status),
		db:         db,
		st:         sqlStats.cfg.Settings,
		clusterID:  sqlStats.cfg.ClusterID,
	}
}

// CreateSQLStatsCompactionSchedule implements the tree.SQLStatsController
// interface.
func (s *Controller) CreateSQLStatsCompactionSchedule(ctx context.Context) error {
	return s.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := CreateSQLStatsCompactionScheduleIfNotYetExist(ctx, txn, s.st, s.clusterID())
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

	if err := s.resetSysTableStats(ctx, "system.statement_statistics"); err != nil {
		return err
	}

	if err := s.resetSysTableStats(ctx, "system.transaction_statistics"); err != nil {
		return err
	}

	return s.ResetActivityTables(ctx)
}

// ResetActivityTables implements the tree.SQLStatsController interface. This
// method resets the {statement|transaction}_activity system tables.
func (s *Controller) ResetActivityTables(ctx context.Context) error {
	if err := s.resetSysTableStats(ctx, "system.statement_activity"); err != nil {
		return err
	}

	return s.resetSysTableStats(ctx, "system.transaction_activity")
}

// ResetInsightsTables implements the tree.SQLStatsController interface. This
// method reset the {statement|transaction}_execution_insights tables.
func (s *Controller) ResetInsightsTables(ctx context.Context) error {
	if err := s.resetSysTableStats(ctx, "system.statement_execution_insights"); err != nil {
		return err
	}

	return s.resetSysTableStats(ctx, "system.transaction_execution_insights")
}

func (s *Controller) resetSysTableStats(ctx context.Context, tableName string) (err error) {
	ex := s.db.Executor()
	_, err = ex.ExecEx(
		ctx,
		redact.Sprintf("reset-%s", tableName),
		nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"TRUNCATE "+tableName)
	return err
}
