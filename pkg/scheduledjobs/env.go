// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledjobs

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
}

// JobExecutionConfig encapsulates external components needed for scheduled job execution.
type JobExecutionConfig struct {
	Settings         *cluster.Settings
	InternalExecutor sqlutil.InternalExecutor
	DB               *kv.DB
	// TestingKnobs is *jobs.TestingKnobs; however we cannot depend
	// on jobs package due to circular dependencies.
	TestingKnobs base.ModuleTestingKnobs
	// PlanHookMaker is responsible for creating sql.NewInternalPlanner. It returns an
	// *sql.planner as an interface{} due to package dependency cycles. It should
	// be cast to that type in the sql package when it is used. Returns a cleanup
	// function that must be called once the caller is done with the planner.
	// This is the same mechanism used in jobs.Registry.
	PlanHookMaker func(opName string, tnx *kv.Txn, user security.SQLUsername) (interface{}, func())
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
