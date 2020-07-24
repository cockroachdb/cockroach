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

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (d *delegator) delegateControlSchedules(ctl *tree.ControlSchedules) (tree.Statement, error) {
	sqlStmt := fmt.Sprintf(`
WITH matches AS (
  SELECT * FROM system.scheduled_jobs WHERE schedule_id IN (%s)
) %s SCHEDULES FROM matches
`, ctl.Schedules.String(), ctl.Command)

	return parse(sqlStmt)
}
