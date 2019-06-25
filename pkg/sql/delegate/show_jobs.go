// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateShowJobs(n *tree.ShowJobs) (tree.Statement, error) {
	var typePredicate string
	if n.Automatic {
		typePredicate = fmt.Sprintf("job_type = '%s'", jobspb.TypeAutoCreateStats)
	} else {
		typePredicate = fmt.Sprintf(
			"(job_type != '%s' OR job_type IS NULL)", jobspb.TypeAutoCreateStats,
		)
	}

	// The query intends to present:
	// - first all the running jobs sorted in order of start time,
	// - then all completed jobs sorted in order of completion time.
	// The "ORDER BY" clause below exploits the fact that all
	// running jobs have finished = NULL.
	return parse(fmt.Sprintf(
		`SELECT job_id, job_type, description, statement, user_name, status, running_status, created,
            started, finished, modified, fraction_completed, error, coordinator_id
		FROM crdb_internal.jobs
		WHERE %s
		AND (finished IS NULL OR finished > now() - '12h':::interval)
		ORDER BY COALESCE(finished, now()) DESC, started DESC`, typePredicate,
	))
}
