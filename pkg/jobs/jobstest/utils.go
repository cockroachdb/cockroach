// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobstest

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// EnvTablesType tells JobSchedulerTestEnv whether to use the system tables,
// or to use test tables.
type EnvTablesType bool

// UseTestTables instructs JobSchedulerTestEnv to use test tables.
const UseTestTables EnvTablesType = false

// UseSystemTables instructs JobSchedulerTestEnv to use system tables.
const UseSystemTables EnvTablesType = true

// NewJobSchedulerTestEnv creates JobSchedulerTestEnv and initializes environments
// current time to initial time.
func NewJobSchedulerTestEnv(whichTables EnvTablesType, t time.Time) *JobSchedulerTestEnv {
	var env *JobSchedulerTestEnv
	if whichTables == UseTestTables {
		env = &JobSchedulerTestEnv{
			scheduledJobsTableName: "defaultdb.scheduled_jobs",
			jobsTableName:          "defaultdb.system_jobs",
		}
	} else {
		env = &JobSchedulerTestEnv{
			scheduledJobsTableName: "system.scheduled_jobs",
			jobsTableName:          "system.jobs",
		}
	}
	env.mu.now = t
	return env
}

// JobSchedulerTestEnv is a job scheduler environment with an added ability to
// manipulate time.
type JobSchedulerTestEnv struct {
	scheduledJobsTableName string
	jobsTableName          string
	mu                     struct {
		syncutil.Mutex
		now time.Time
	}
}

var _ scheduledjobs.JobSchedulerEnv = &JobSchedulerTestEnv{}

// ScheduledJobsTableName implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) ScheduledJobsTableName() string {
	return e.scheduledJobsTableName
}

// SystemJobsTableName implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) SystemJobsTableName() string {
	return e.jobsTableName
}

// Now implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) Now() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.mu.now
}

// AdvanceTime implements JobSchedulerTestEnv
func (e *JobSchedulerTestEnv) AdvanceTime(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.now = e.mu.now.Add(d)
}

// SetTime implements JobSchedulerTestEnv
func (e *JobSchedulerTestEnv) SetTime(t time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mu.now = t
}

const timestampTZLayout = "2006-01-02 15:04:05.000000"

// NowExpr implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) NowExpr() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return fmt.Sprintf("TIMESTAMPTZ '%s'", e.mu.now.Format(timestampTZLayout))
}

// GetScheduledJobsTableSchema returns schema for the scheduled jobs table.
func GetScheduledJobsTableSchema(env scheduledjobs.JobSchedulerEnv) string {
	if env.ScheduledJobsTableName() == "system.jobs" {
		return systemschema.ScheduledJobsTableSchema
	}
	return strings.Replace(systemschema.ScheduledJobsTableSchema,
		"system.scheduled_jobs", env.ScheduledJobsTableName(), 1)
}

// GetJobsTableSchema returns schema for the jobs table.
func GetJobsTableSchema(env scheduledjobs.JobSchedulerEnv) string {
	if env.SystemJobsTableName() == "system.jobs" {
		return systemschema.JobsTableSchema
	}
	return strings.Replace(systemschema.JobsTableSchema,
		"system.jobs", env.SystemJobsTableName(), 1)
}
