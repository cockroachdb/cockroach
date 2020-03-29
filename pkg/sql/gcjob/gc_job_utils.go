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
func markTableGCed(
	ctx context.Context, tableID sqlbase.ID, progress *jobspb.SchemaChangeGCProgress,
) {
	for i := range progress.Tables {
		tableProgress := &progress.Tables[i]
		if tableProgress.ID == tableID {
			tableProgress.Status = jobspb.SchemaChangeGCProgress_DELETED
			if log.V(2) {
				log.Infof(ctx, "determined table %d is GC'd", tableID)
			}
		}
	}
}

// markIndexGCed marks the index as GC'd.
func markIndexGCed(
	ctx context.Context,
	garbageCollectedIndexID sqlbase.IndexID,
	progress *jobspb.SchemaChangeGCProgress,
) {
	// Update the job details to remove the dropped indexes.
	for i := range progress.Indexes {
		indexToUpdate := &progress.Indexes[i]
		if indexToUpdate.IndexID == garbageCollectedIndexID {
			indexToUpdate.Status = jobspb.SchemaChangeGCProgress_DELETED
			log.Infof(ctx, "marked index %d as GC'd", garbageCollectedIndexID)
		}
	}
}

// initDetailsAndProgress sets up the job progress if not already populated and
// validates that the job details is properly formatted.
func initDetailsAndProgress(
	ctx context.Context, execCfg *sql.ExecutorConfig, jobID int64,
) (*jobspb.SchemaChangeGCDetails, *jobspb.SchemaChangeGCProgress, error) {
	var details jobspb.SchemaChangeGCDetails
	var progress *jobspb.SchemaChangeGCProgress
	var job *jobs.Job
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		job, err = execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		details = job.Details().(jobspb.SchemaChangeGCDetails)
		jobProgress := job.Progress()
		progress = jobProgress.GetSchemaChangeGC()
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
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	if len(progress.Tables) != len(details.Tables) || len(progress.Indexes) != len(details.Indexes) {
		for _, table := range details.Tables {
			progress.Tables = append(progress.Tables, jobspb.SchemaChangeGCProgress_TableProgress{ID: table.ID})
		}
		for _, index := range details.Indexes {
			progress.Indexes = append(progress.Indexes, jobspb.SchemaChangeGCProgress_IndexProgress{IndexID: index.IndexID})
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
func isDoneGC(progress *jobspb.SchemaChangeGCProgress) bool {
	for _, index := range progress.Indexes {
		if index.Status != jobspb.SchemaChangeGCProgress_DELETED {
			return false
		}
	}
	for _, table := range progress.Tables {
		if table.Status != jobspb.SchemaChangeGCProgress_DELETED {
			return false
		}
	}

	return true
}

// getAllTablesWaitingForGC returns a slice with all of the table IDs which have
// note yet been been GC'd. This is used to determine which tables' statuses
// need to be updated.
func getAllTablesWaitingForGC(
	details *jobspb.SchemaChangeGCDetails, progress *jobspb.SchemaChangeGCProgress,
) []sqlbase.ID {
	allRemainingTableIDs := make([]sqlbase.ID, 0, len(progress.Tables))
	if len(details.Indexes) > 0 {
		allRemainingTableIDs = append(allRemainingTableIDs, details.ParentID)
	}
	for _, table := range progress.Tables {
		if table.Status != jobspb.SchemaChangeGCProgress_DELETED {
			allRemainingTableIDs = append(allRemainingTableIDs, table.ID)
		}
	}

	return allRemainingTableIDs
}

// validateDetails ensures that the job details payload follows the structure
// described in the comment for SchemaChangeGCDetails.
func validateDetails(details *jobspb.SchemaChangeGCDetails) error {
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
	progress *jobspb.SchemaChangeGCProgress,
) {
	if err := execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		if err := job.SetProgress(ctx, *progress); err != nil {
			return err
		}
		log.Infof(ctx, "updated progress payload: %+v", progress)
		return nil
	}); err != nil {
		log.Warningf(ctx, "failed to update job's progress payload err: %+v", err)
	}
}

// getDropTimes returns the data stored in details as a map for convenience.
func getDropTimes(
	details *jobspb.SchemaChangeGCDetails,
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
