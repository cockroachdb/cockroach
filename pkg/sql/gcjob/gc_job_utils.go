// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// markTableGCed updates the job payload details to indicate that the specified
// table was GC'd.
func markTableGCed(
	ctx context.Context,
	tableID descpb.ID,
	progress *jobspb.SchemaChangeGCProgress,
	status jobspb.SchemaChangeGCProgress_Status,
) {
	for i := range progress.Tables {
		tableProgress := &progress.Tables[i]
		if tableProgress.ID == tableID {
			tableProgress.Status = status
			if log.V(2) {
				log.Infof(ctx, "determined table %d is GC'd", tableID)
			}
		}
	}
}

// markIndexGCed marks the index as GC'd.
func markIndexGCed(
	ctx context.Context,
	garbageCollectedIndexID descpb.IndexID,
	progress *jobspb.SchemaChangeGCProgress,
	nextStatus jobspb.SchemaChangeGCProgress_Status,
) {
	// Update the job details to remove the dropped indexes.
	for i := range progress.Indexes {
		indexToUpdate := &progress.Indexes[i]
		if indexToUpdate.IndexID == garbageCollectedIndexID {
			indexToUpdate.Status = nextStatus
			log.Infof(ctx, "marked index %d as GC'd", garbageCollectedIndexID)
		}
	}
}

// initDetailsAndProgress sets up the job progress if not already populated and
// validates that the job details is properly formatted.
func initDetailsAndProgress(
	ctx context.Context, execCfg *sql.ExecutorConfig, jobID jobspb.JobID,
) (*jobspb.SchemaChangeGCDetails, *jobspb.SchemaChangeGCProgress, error) {
	var details jobspb.SchemaChangeGCDetails
	var progress *jobspb.SchemaChangeGCProgress
	var job *jobs.Job
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
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
	jobID jobspb.JobID,
	details *jobspb.SchemaChangeGCDetails,
	progress *jobspb.SchemaChangeGCProgress,
) error {
	var update bool
	if details.Tenant != nil && progress.Tenant == nil {
		progress.Tenant = &jobspb.SchemaChangeGCProgress_TenantProgress{
			Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR,
		}
		update = true
	} else if len(progress.Tables) != len(details.Tables) || len(progress.Indexes) != len(details.Indexes) {
		update = true
		for _, table := range details.Tables {
			progress.Tables = append(progress.Tables, jobspb.SchemaChangeGCProgress_TableProgress{
				ID:     table.ID,
				Status: jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR,
			})
		}
		for _, index := range details.Indexes {
			progress.Indexes = append(progress.Indexes, jobspb.SchemaChangeGCProgress_IndexProgress{
				IndexID: index.IndexID,
				Status:  jobspb.SchemaChangeGCProgress_WAITING_FOR_CLEAR,
			})
		}
	}

	if update {
		if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
			if err != nil {
				return err
			}
			return job.WithTxn(txn).SetProgress(ctx, *progress)
		}); err != nil {
			return err
		}
	}
	return nil
}

// Check if we are done GC'ing everything.
func isDoneGC(progress *jobspb.SchemaChangeGCProgress) bool {
	for _, index := range progress.Indexes {
		if index.Status != jobspb.SchemaChangeGCProgress_CLEARED {
			return false
		}
	}
	for _, table := range progress.Tables {
		if table.Status != jobspb.SchemaChangeGCProgress_CLEARED {
			return false
		}
	}
	if progress.Tenant != nil && progress.Tenant.Status != jobspb.SchemaChangeGCProgress_CLEARED {
		return false
	}

	return true
}

// runningStatusGC generates a RunningStatus string which always remains under
// a certain size, given any progress struct.
func runningStatusGC(progress *jobspb.SchemaChangeGCProgress) jobs.RunningStatus {
	var anyWaitingForMVCCGC bool
	maybeSetAnyDeletedOrWaitingForMVCCGC := func(status jobspb.SchemaChangeGCProgress_Status) {
		switch status {
		case jobspb.SchemaChangeGCProgress_WAITING_FOR_MVCC_GC:
			anyWaitingForMVCCGC = true
		}
	}
	tableIDs := make([]string, 0, len(progress.Tables))
	indexIDs := make([]string, 0, len(progress.Indexes))
	for _, table := range progress.Tables {
		maybeSetAnyDeletedOrWaitingForMVCCGC(table.Status)
		if table.Status == jobspb.SchemaChangeGCProgress_CLEARING {
			tableIDs = append(tableIDs, strconv.Itoa(int(table.ID)))
		}
	}
	for _, index := range progress.Indexes {
		maybeSetAnyDeletedOrWaitingForMVCCGC(index.Status)
		if index.Status == jobspb.SchemaChangeGCProgress_CLEARING {
			indexIDs = append(indexIDs, strconv.Itoa(int(index.IndexID)))
		}
	}

	var b strings.Builder
	b.WriteString("performing garbage collection on")
	var flag bool
	if progress.Tenant != nil && progress.Tenant.Status == jobspb.SchemaChangeGCProgress_CLEARING {
		b.WriteString(" tenant")
		flag = true
	}

	for _, s := range []struct {
		ids      []string
		singular string
		plural   string
	}{
		{tableIDs, "table", "tables"},
		{indexIDs, "index", "indexes"},
	} {
		if len(s.ids) == 0 {
			continue
		}
		if flag {
			b.WriteRune(';')
		}
		b.WriteRune(' ')
		switch len(s.ids) {
		case 1:
			// one id, e.g. "table 123"
			b.WriteString(s.singular)
			b.WriteRune(' ')
			b.WriteString(s.ids[0])
		case 2, 3, 4, 5:
			// a few ids, e.g. "tables 123, 456, 789"
			b.WriteString(s.plural)
			b.WriteRune(' ')
			b.WriteString(strings.Join(s.ids, ", "))
		default:
			// too many ids to print, e.g. "25 tables"
			b.WriteString(strconv.Itoa(len(s.ids)))
			b.WriteRune(' ')
			b.WriteString(s.plural)
		}
		flag = true
	}

	switch {
	// `flag` not set implies we're not GCing anything.
	case !flag && anyWaitingForMVCCGC:
		return sql.RunningStatusWaitingForMVCCGC
	case !flag:
		return sql.RunningStatusWaitingGC // legacy status
	default:
		return jobs.RunningStatus(b.String())
	}
}

// getAllTablesWaitingForGC returns a slice with all of the table IDs which have
// note yet been been GC'd. This is used to determine which tables' statuses
// need to be updated.
func getAllTablesWaitingForGC(
	details *jobspb.SchemaChangeGCDetails, progress *jobspb.SchemaChangeGCProgress,
) []descpb.ID {
	allRemainingTableIDs := make([]descpb.ID, 0, len(progress.Tables))
	if len(details.Indexes) > 0 {
		allRemainingTableIDs = append(allRemainingTableIDs, details.ParentID)
	}
	for _, table := range progress.Tables {
		if table.Status != jobspb.SchemaChangeGCProgress_CLEARED {
			allRemainingTableIDs = append(allRemainingTableIDs, table.ID)
		}
	}

	return allRemainingTableIDs
}

// validateDetails ensures that the job details payload follows the structure
// described in the comment for SchemaChangeGCDetails.
func validateDetails(details *jobspb.SchemaChangeGCDetails) error {
	if details.Tenant != nil &&
		(len(details.Tables) > 0 || len(details.Indexes) > 0) {
		return errors.AssertionFailedf(
			"Either field Tenant is set or any of Tables or Indexes: %+v", *details,
		)
	}
	if len(details.Indexes) > 0 {
		if details.ParentID == descpb.InvalidID {
			return errors.Errorf("must provide a parentID when dropping an index")
		}
	}
	return nil
}

// persistProgress sets the current state of the progress and running status
// back on the job.
func persistProgress(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	jobID jobspb.JobID,
	progress *jobspb.SchemaChangeGCProgress,
	runningStatus jobs.RunningStatus,
) {
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		job, err := execCfg.JobRegistry.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		if err := job.WithTxn(txn).SetProgress(ctx, *progress); err != nil {
			return err
		}
		log.Infof(ctx, "updated progress payload: %+v", progress)
		err = job.WithTxn(txn).RunningStatus(ctx, func(_ context.Context, _ jobspb.Details) (jobs.RunningStatus, error) {
			return runningStatus, nil
		})
		if err != nil {
			return err
		}
		log.Infof(ctx, "updated running status: %+v", runningStatus)
		return nil
	}); err != nil {
		log.Warningf(ctx, "failed to update job's progress payload or running status err: %+v", err)
	}
}

// getDropTimes returns the data stored in details as a map for convenience.
func getDropTimes(
	details *jobspb.SchemaChangeGCDetails,
) (map[descpb.ID]int64, map[descpb.IndexID]int64) {
	tableDropTimes := make(map[descpb.ID]int64)
	for _, table := range details.Tables {
		tableDropTimes[table.ID] = table.DropTime
	}
	indexDropTimes := make(map[descpb.IndexID]int64)
	for _, index := range details.Indexes {
		indexDropTimes[index.IndexID] = index.DropTime
	}
	return tableDropTimes, indexDropTimes
}
