// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

// gcTables drops the table data and descriptor of tables that have an expired
// deadline and updates the job details to mark the work it did.
// The job progress is updated in place, but needs to be persisted to the job.
func gcTables(
	ctx context.Context, execCfg *sql.ExecutorConfig, progress *jobspb.WaitingForGCProgress,
) error {
	for _, droppedTable := range progress.Tables {
		if droppedTable.Status != jobspb.WaitingForGCProgress_DELETING {
			// Table is not ready to be dropped, or has already been dropped.
			continue
		}

		table, err := getTableDescriptor(ctx, execCfg, droppedTable.ID)
		if err != nil {
			return err
		}

		if !table.Dropped() {
			// We shouldn't drop this table yet.
			continue
		}

		// First, delete all the table data.
		if err := sql.TruncateTable(ctx, execCfg.DB, execCfg.DistSender, table); err != nil {
			return err
		}

		// Then, delete the table descriptor.
		if err := dropTableDesc(ctx, execCfg.DB, table); err != nil {
			return err
		}

		// Update the details payload to indicate that the table was dropped.
		if err := markTableGCed(table.ID, progress); err != nil {
			return err
		}
	}
	return nil
}
