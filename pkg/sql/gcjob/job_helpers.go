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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// markTableGCed updates the job payload details to indicate that the specified
// table was GC'd.
func markTableGCed(tableID sqlbase.ID, progress *jobspb.WaitingForGCProgress) error {
	for i := range progress.Tables {
		tableProgress := &progress.Tables[i]
		if tableProgress.ID == tableID {
			tableProgress.Status = jobspb.WaitingForGCProgress_DELETED
		}
	}
	return nil
}

// markIndexGCed marks the set of indexes as GC'd.
func markIndexGCed(garbageCollectedIndexID sqlbase.IndexID, progress *jobspb.WaitingForGCProgress) {
	// Update the job details to remove the dropped indexes.
	for i := range progress.Indexes {
		indexToUpdate := &progress.Indexes[i]
		if indexToUpdate.IndexID == garbageCollectedIndexID {
			indexToUpdate.Status = jobspb.WaitingForGCProgress_DELETED
		}
	}
}

// initDetailsAndProgress sets up the job progress if not already populated and
// validates that the job details is properly formatted.
func initDetailsAndProgress(
	ctx context.Context, execCfg *sql.ExecutorConfig, jobID int64,
) (*jobspb.WaitingForGCDetails, *jobspb.WaitingForGCProgress, error) {
	var details jobspb.WaitingForGCDetails
	var progress *jobspb.WaitingForGCProgress
	var job *jobs.Job
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		job, err = execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		details = job.Details().(jobspb.WaitingForGCDetails)
		jobProgress := job.Progress()
		progress = jobProgress.GetWaitingForGC()
		return nil
	}); err != nil {
		return nil, nil, err
	}
	if err := validateDetails(&details); err != nil {
		return nil, nil, err
	}
	if err := initializeProgress(ctx, execCfg, jobID, &details, progress); err != nil {
		return nil, nil, err
	}
	return &details, progress, nil
}

// initializeProgress converts the details provided into a progress payload that
// will be updated as the elements that need to be GC'd get processed.
func initializeProgress(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID int64,
	details *jobspb.WaitingForGCDetails,
	progress *jobspb.WaitingForGCProgress,
) error {
	if len(progress.Tables) != len(details.Tables) || len(progress.Indexes) != len(details.Indexes) {
		for _, table := range details.Tables {
			progress.Tables = append(progress.Tables, jobspb.WaitingForGCProgress_TableProgress{ID: table.ID})
		}
		for _, index := range details.Indexes {
			progress.Indexes = append(progress.Indexes, jobspb.WaitingForGCProgress_IndexProgress{IndexID: index.IndexID})
		}

		// Write out new progress.
		if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
			if err != nil {
				return err
			}
			return job.SetProgress(ctx, *progress)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Check if we are done GC'ing everything.
func isDoneGC(progress *jobspb.WaitingForGCProgress) bool {
	for _, index := range progress.Indexes {
		if index.Status != jobspb.WaitingForGCProgress_DELETED {
			return false
		}
	}
	for _, table := range progress.Tables {
		if table.Status != jobspb.WaitingForGCProgress_DELETED {
			return false
		}
	}

	return true
}

// getAllTablesWaitingForGC returns a slice with all of the table IDs which have
// note yet been scheduled to be GC'd or have not yet been GC'd. This is used to
// determine which tables' statuses need to be updated.
func getAllTablesWaitingForGC(
	details *jobspb.WaitingForGCDetails, progress *jobspb.WaitingForGCProgress,
) []sqlbase.ID {
	allRemainingTableIDs := make([]sqlbase.ID, 0, len(progress.Tables))
	if len(details.Indexes) > 0 {
		allRemainingTableIDs = append(allRemainingTableIDs, details.ParentID)
	}
	for _, table := range progress.Tables {
		if table.Status == jobspb.WaitingForGCProgress_WAITING_FOR_GC {
			allRemainingTableIDs = append(allRemainingTableIDs, table.ID)
		}
	}

	return allRemainingTableIDs
}

// The job details should resemble one of the following:
//
// 1. Index deletions: One or more deletions of an index on a table.
//      details.Indexes -> the indexes to GC. These indexes must be
//      non-interleaved.
//      details.ParentID -> the table with the indexes.
//
// 2. Table deletions: The deletion of a single table.
//      details.Tables -> the tables to be deleted.
//
// 3. Database deletions: The deletion of a database and therefore all its tables.
//      details.Tables -> the IDs of the tables to GC.
//      details.ParentID -> the ID of the database to drop.
func validateDetails(details *jobspb.WaitingForGCDetails) error {
	if len(details.Indexes) > 0 {
		if details.ParentID == sqlbase.InvalidID {
			return errors.Errorf("must provide a parentID when dropping an index")
		}
	}
	return nil
}

// persistProgress sets the current state of the progress back on the job.
func persistProgress(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID int64,
	progress *jobspb.WaitingForGCProgress,
) {
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		return job.SetProgress(ctx, *progress)
	}); err != nil {
		log.Warningf(ctx, "failed to update job's progress payload err: %+v", err)
	}
}

// getDropTimes returns the data stored in details as a map for convenience.
func getDropTimes(
	details *jobspb.WaitingForGCDetails,
) (map[sqlbase.ID]int64, map[sqlbase.IndexID]int64) {
	tableDropTimes := make(map[sqlbase.ID]int64)
	for _, table := range details.Tables {
		tableDropTimes[table.ID] = table.DropTime
	}
	indexDropTimes := make(map[sqlbase.IndexID]int64)
	for _, index := range details.Indexes {
		indexDropTimes[index.IndexID] = index.DropTime
	}
	return tableDropTimes, indexDropTimes
}
