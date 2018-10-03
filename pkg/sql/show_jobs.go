// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ShowJobs returns all the jobs.
// Privileges: None.
func (p *planner) ShowJobs(ctx context.Context, n *tree.ShowJobs) (planNode, error) {
	// The query intends to present:
	// - first all the running jobs sorted in order of start time,
	// - then all completed jobs sorted in order of completion time.
	// The "ORDER BY" clause below exploits the fact that all
	// running jobs have finished = NULL.
	return p.delegateQuery(ctx, "SHOW JOBS",
		`SELECT job_id, job_type, description, user_name, status, running_status, created,
            started, finished, modified, fraction_completed, error, coordinator_id
       FROM crdb_internal.jobs
   ORDER BY COALESCE(finished, now()) DESC, started DESC`,
		nil, nil)
}
