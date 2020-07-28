// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateJobControl(stmt *tree.ControlJobsForSchedules) (tree.Statement, error) {
	// TODO(yevgeniy): it is very unfornate that we have to use the IN() clause
	// in order to select matching jobs.
	// It would be better if the job control statement had a better (planNode) implementation,
	// so that we can rely on the optimizer to select relevant nodes.
	return parse(fmt.Sprintf(`
%s JOBS
SELECT id FROM system.jobs
WHERE jobs.created_by_type = '%s' AND jobs.created_by_id IN (%s)
`, tree.JobCommandToStatement[stmt.Command], jobs.CreatedByScheduledJobs, stmt.Schedules))
}
