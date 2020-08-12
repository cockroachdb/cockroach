// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// TestingKnobs are base.ModuleTestingKnobs for testing jobs related infra.
type TestingKnobs struct {
	// SchedulerDaemonInitialScanDelay overrides the initial scan delay.
	SchedulerDaemonInitialScanDelay func() time.Duration

	// SchedulerDaemonScanDelay overrides the delay between successive scans.
	SchedulerDaemonScanDelay func() time.Duration

	// JobSchedulerEnv overrides the environment to use for scheduled jobs.
	JobSchedulerEnv scheduledjobs.JobSchedulerEnv

	// TakeOverJobScheduling is a function which replaces  the normal job scheduler
	// daemon logic.
	// This function will be passed in a function which the caller
	// may invoke directly, bypassing normal job scheduler daemon logic.
	TakeOverJobsScheduling func(func(ctx context.Context, maxSchedules int64, txn *kv.Txn) error)

	// CaptureJobExecutionConfig is a callback invoked with a job execution config
	// which will be used when executing job schedules.
	// The reason this callback exists is due to a circular dependency issues that exists
	// if trying to initialize this config outside of sql.Server -- namely, we cannot
	// initialize PlanHookMaker outside of sql package.
	CaptureJobExecutionConfig func(config *scheduledjobs.JobExecutionConfig)

	// OverrideAsOfClause is a function which has a chance of modifying
	// tree.AsOfClause.
	OverrideAsOfClause func(clause *tree.AsOfClause)
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
