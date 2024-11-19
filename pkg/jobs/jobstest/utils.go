// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobstest

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// EnvTablesType tells JobSchedulerTestEnv whether to use the system tables,
// or to use test tables. System tables such as system.jobs may be affected
// by the system in the background, while test tables are completely isolated.
type EnvTablesType bool

// UseTestTables instructs JobSchedulerTestEnv to use test tables.
const UseTestTables EnvTablesType = false

// UseSystemTables instructs JobSchedulerTestEnv to use system tables.
const UseSystemTables EnvTablesType = true

// NewJobSchedulerTestEnv creates JobSchedulerTestEnv and initializes environments
// current time to initial time.
func NewJobSchedulerTestEnv(
	whichTables EnvTablesType, t time.Time, allowedExecutors ...tree.ScheduledJobExecutorType,
) *JobSchedulerTestEnv {
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
	if len(allowedExecutors) > 0 {
		env.allowedExecutors = make(map[string]struct{}, len(allowedExecutors))
		for _, e := range allowedExecutors {
			env.allowedExecutors[e.InternalName()] = struct{}{}
		}
	}

	return env
}

// JobSchedulerTestEnv is a job scheduler environment with an added ability to
// manipulate time.
type JobSchedulerTestEnv struct {
	scheduledJobsTableName string
	jobsTableName          string
	allowedExecutors       map[string]struct{}
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

// IsExecutorEnabled implements scheduledjobs.JobSchedulerEnv
func (e *JobSchedulerTestEnv) IsExecutorEnabled(name string) bool {
	enabled := e.allowedExecutors == nil
	if !enabled {
		_, enabled = e.allowedExecutors[name]
	}
	return enabled
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

// DummyClusterID is used while instantiating dummy schedules
var DummyClusterID = uuid.UUID{1}

// DummyClusterVersion is used while instantiating dummy schedules
var DummyClusterVersion = clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()}

// AddDummyScheduleDetails augments passed in details with a dummy clusterID and CreationClusterVersion.
func AddDummyScheduleDetails(details jobspb.ScheduleDetails) jobspb.ScheduleDetails {
	dummyDetails := details
	dummyDetails.ClusterID = DummyClusterID
	dummyDetails.CreationClusterVersion = DummyClusterVersion
	return dummyDetails
}
