// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduledjobs

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

// JobSchedulerEnv is an environment for running scheduled jobs.
// This environment facilitates dependency injection mechanism for tests.
type JobSchedulerEnv interface {
	// ScheduledJobsTableName returns the name of the scheduled_jobs table.
	ScheduledJobsTableName() string
	// SystemJobsTableName returns the name of the system jobs table.
	SystemJobsTableName() string
	// Now returns current time.
	Now() time.Time
	// NowExpr returns expression representing current time when
	// used in the database queries.
	NowExpr() string
	// IsExecutorEnabled returns true if the scheduled jobs for the
	// specified executor type are allowed to run.
	IsExecutorEnabled(name string) bool
}

// JobExecutionConfig encapsulates external components needed for scheduled job execution.
type JobExecutionConfig struct {
	Settings *cluster.Settings
	DB       isql.DB
	// TestingKnobs is *jobs.TestingKnobs; however we cannot depend
	// on jobs package due to circular dependencies.
	TestingKnobs base.ModuleTestingKnobs
	// PlanHookMaker is responsible for creating sql.NewInternalPlanner. It returns an
	// *sql.planner as an interface{} due to package dependency cycles. It should
	// be cast to that type in the sql package when it is used. Returns a cleanup
	// function that must be called once the caller is done with the planner.
	// This is the same mechanism used in jobs.Registry.
	PlanHookMaker func(ctx context.Context, opName redact.SafeString, tnx *kv.Txn, user username.SQLUsername) (interface{}, func())
	// ShouldRunScheduler, if set, returns true if the job scheduler should run
	// schedules.  This callback should be re-checked periodically.
	ShouldRunScheduler func(ctx context.Context, ts hlc.ClockTimestamp) (bool, error)
}

// production JobSchedulerEnv implementation.
type prodJobSchedulerEnvImpl struct{}

// ProdJobSchedulerEnv is a JobSchedulerEnv implementation suitable for production.
var ProdJobSchedulerEnv JobSchedulerEnv = &prodJobSchedulerEnvImpl{}

func (e *prodJobSchedulerEnvImpl) ScheduledJobsTableName() string {
	return "system.scheduled_jobs"
}

func (e *prodJobSchedulerEnvImpl) SystemJobsTableName() string {
	return "system.jobs"
}

func (e *prodJobSchedulerEnvImpl) Now() time.Time {
	return timeutil.Now()
}

func (e *prodJobSchedulerEnvImpl) NowExpr() string {
	return "current_timestamp()"
}

func (e *prodJobSchedulerEnvImpl) IsExecutorEnabled(name string) bool {
	return true
}

// ScheduleControllerEnv is an environment for controlling (DROP, PAUSE)
// scheduled jobs.
type ScheduleControllerEnv interface {
	PTSProvider() protectedts.Storage
}

// ProdScheduleControllerEnvImpl is the production implementation of
// ScheduleControllerEnv.
type ProdScheduleControllerEnvImpl struct {
	pts protectedts.Storage
}

// MakeProdScheduleControllerEnv returns a ProdScheduleControllerEnvImpl
// instance.
func MakeProdScheduleControllerEnv(pts protectedts.Storage) *ProdScheduleControllerEnvImpl {
	return &ProdScheduleControllerEnvImpl{pts: pts}
}

// PTSProvider implements the ScheduleControllerEnv interface.
func (c *ProdScheduleControllerEnvImpl) PTSProvider() protectedts.Storage {
	return c.pts
}
