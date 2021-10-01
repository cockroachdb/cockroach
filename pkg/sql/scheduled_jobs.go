// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
)

// CreateScheduledJobEnv initializes a JobScheduledEnv given the ExecutorConfig.
func CreateScheduledJobEnv(execCfg *ExecutorConfig) scheduledjobs.JobSchedulerEnv {
	env := scheduledjobs.ProdJobSchedulerEnv
	// TODO(XXX): big no no
	if execCfg != nil {
		if knobs, ok := execCfg.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
			if knobs.JobSchedulerEnv != nil {
				env = knobs.JobSchedulerEnv
			}
		}
	}
	return env
}
