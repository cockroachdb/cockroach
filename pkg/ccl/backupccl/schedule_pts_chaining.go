// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// maybeUpdateSchedulePTSRecord is responsible for chaining of protected ts
// records across scheduled backups. For a detailed outline of the scheme refer
// to the comment above
// ScheduledBackupExecutionArgs.ChainProtectedTimestampRecords.
//
// maybeUpdateSchedulePTSRecord writes/updates a pts record
// on the schedule to protect all data after the current backups EndTime. This
// EndTime could be out of the GC window by the time the job has reached here.
// In some situations, we rely on the pts record written by the backup during
// planning (also protecting data after EndTime) to ensure that we can protect
// and verify the new record. Thus, we must ensure that this method is called
// before we release the jobs' pts record below.
func maybeUpdateSchedulePTSRecord(
	ctx context.Context,
	exec *sql.ExecutorConfig,
	backupDetails jobspb.BackupDetails,
	id jobspb.JobID,
) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := exec.DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	return exec.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// We cannot rely on b.job containing created_by_id because on job
		// resumption the registry does not populate the resumers' CreatedByInfo.
		datums, err := exec.InternalExecutor.QueryRowEx(
			ctx,
			"lookup-schedule-info",
			txn,
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			fmt.Sprintf(
				"SELECT created_by_id FROM %s WHERE id=$1 AND created_by_type=$2",
				env.SystemJobsTableName()),
			id, jobs.CreatedByScheduledJobs)

		if err != nil {
			return errors.Wrap(err, "schedule info lookup")
		}
		if datums == nil {
			// Not a scheduled backup.
			return nil
		}

		scheduleID := int64(tree.MustBeDInt(datums[0]))
		_, args, err := getScheduledBackupExecutionArgsFromSchedule(ctx, env, txn,
			exec.InternalExecutor, scheduleID)
		if err != nil {
			return errors.Wrap(err, "load scheduled job")
		}

		// Check if the schedule is configured to chain protected timestamp records.
		if !args.ChainProtectedTimestampRecords {
			return nil
		}

		// If SchedulePTSChainingRecord was not set during backup planning, we do
		// not need to perform any chaining.
		if backupDetails.SchedulePTSChainingRecord == nil {
			return nil
		}

		switch args.BackupType {
		case ScheduledBackupExecutionArgs_INCREMENTAL:
			if backupDetails.SchedulePTSChainingRecord.Action != jobspb.SchedulePTSChainingRecord_UPDATE {
				return errors.AssertionFailedf("incremental backup has unexpected chaining action %d on"+
					" backup job details", backupDetails.SchedulePTSChainingRecord.Action)
			}
			if err := manageIncrementalBackupPTSChaining(ctx,
				backupDetails.SchedulePTSChainingRecord.ProtectedTimestampRecord,
				backupDetails.EndTime, exec, txn, scheduleID); err != nil {
				return errors.Wrap(err, "failed to manage chaining of pts record during a inc backup")
			}
		case ScheduledBackupExecutionArgs_FULL:
			if backupDetails.SchedulePTSChainingRecord.Action != jobspb.SchedulePTSChainingRecord_RELEASE {
				return errors.AssertionFailedf("full backup has unexpected chaining action %d on"+
					" backup job details", backupDetails.SchedulePTSChainingRecord.Action)
			}
			if err := manageFullBackupPTSChaining(ctx, env, txn, backupDetails, exec, args); err != nil {
				return errors.Wrap(err, "failed to manage chaining of pts record during a full backup")
			}
		}
		return nil
	})
}

// manageFullBackupPTSChaining is invoked on successful completion of a
// scheduled full backup. It is responsible for:
// - Releasing the pts record that was stored on the incremental schedule when
//   the full backup was planned.
// - Writing a new pts record protecting all data after the full backups' EndTime
//   and store this on the incremental schedule.
func manageFullBackupPTSChaining(
	ctx context.Context,
	env scheduledjobs.JobSchedulerEnv,
	txn *kv.Txn,
	backupDetails jobspb.BackupDetails,
	exec *sql.ExecutorConfig,
	args *ScheduledBackupExecutionArgs,
) error {
	// Let's resolve the dependent incremental schedule as the first step. If the
	// schedule has been dropped then we can avoid doing unnecessary work.
	incSj, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(ctx, env, txn,
		exec.InternalExecutor, args.DependentScheduleID)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			log.Warningf(ctx, "could not find dependent schedule with id %d",
				args.DependentScheduleID)
			return nil
		}
		return err
	}

	// Resolve the target that needs to be protected on this execution of the
	// scheduled backup.
	targetToProtect, deprecatedSpansToProtect, err := getTargetProtectedByBackup(ctx, backupDetails, txn, exec)
	if err != nil {
		return errors.Wrap(err, "getting spans to protect")
	}

	// Records written by the backup schedule should be ignored when making GC
	// decisions on any table that has been marked as `exclude_data_from_backup`.
	// This ensures that the schedule does not holdup GC on that table span for
	// the duration of execution.
	if targetToProtect != nil {
		targetToProtect.IgnoreIfExcludedFromBackup = true
	}

	// Protect the target after the EndTime of the current backup. We do not need
	// to verify this new record as we have a record written by the backup during
	// planning, already protecting this target after EndTime.
	//
	// Since this record will be stored on the incremental schedule, we use the
	// inc schedule ID as the records' Meta. This ensures that even if the full
	// schedule is dropped, the reconciliation job will not release the pts
	// record stored on the inc schedule, and the chaining will continue.
	ptsRecord, err := protectTimestampRecordForSchedule(ctx, targetToProtect, deprecatedSpansToProtect,
		backupDetails.EndTime, incSj.ScheduleID(), exec, txn)
	if err != nil {
		return errors.Wrap(err, "protect and verify pts record for schedule")
	}

	// Attempt to release the pts record that was written on the incremental
	// schedule when the full backup was being planned.
	if err := releaseProtectedTimestamp(ctx, txn, exec.ProtectedTimestampProvider,
		backupDetails.SchedulePTSChainingRecord.ProtectedTimestampRecord); err != nil {
		return errors.Wrap(err, "release pts record for schedule")
	}

	// Update the incremental schedule with the new pts record.
	incArgs.ProtectedTimestampRecord = &ptsRecord
	any, err := pbtypes.MarshalAny(incArgs)
	if err != nil {
		return err
	}
	incSj.SetExecutionDetails(incSj.ExecutorType(), jobspb.ExecutionArguments{Args: any})
	return incSj.Update(ctx, exec.InternalExecutor, txn)
}

// manageIncrementalBackupPTSChaining is invoked on successful completion of an
// incremental backup. It is responsible for updating the pts record on the
// incremental schedule to protect after the EndTime of the current backup.
func manageIncrementalBackupPTSChaining(
	ctx context.Context,
	ptsRecordID *uuid.UUID,
	tsToProtect hlc.Timestamp,
	exec *sql.ExecutorConfig,
	txn *kv.Txn,
	scheduleID int64,
) error {
	if ptsRecordID == nil {
		return errors.Newf("unexpected nil pts record id on incremental schedule %d", scheduleID)
	}
	err := exec.ProtectedTimestampProvider.UpdateTimestamp(ctx, txn, *ptsRecordID,
		tsToProtect)
	// If we cannot find the pts record to update it is possible that a
	// concurrent full backup has released the record, and written a new record
	// on the incremental schedule. This should only happen in the case of an
	// "overhang" incremental backup.
	// In such a scenario it is okay to do nothing since the next incremental on
	// the new full backup will rely on the pts record written by the full
	// backup.
	if err != nil && errors.Is(err, protectedts.ErrNotExists) {
		log.Warningf(ctx, "failed to update timestamp record %d since it does not exist", ptsRecordID)
		return nil //nolint:returnerrcheck
	}
	return err
}

func getTargetProtectedByBackup(
	ctx context.Context, backupDetails jobspb.BackupDetails, txn *kv.Txn, exec *sql.ExecutorConfig,
) (target *ptpb.Target, deprecatedSpans []roachpb.Span, err error) {
	if backupDetails.ProtectedTimestampRecord == nil {
		return nil, nil, nil
	}

	ptsRecord, err := exec.ProtectedTimestampProvider.GetRecord(ctx, txn,
		*backupDetails.ProtectedTimestampRecord)
	if err != nil {
		return nil, nil, err
	}

	return ptsRecord.Target, ptsRecord.DeprecatedSpans, nil
}

func protectTimestampRecordForSchedule(
	ctx context.Context,
	targetToProtect *ptpb.Target,
	deprecatedSpansToProtect roachpb.Spans,
	tsToProtect hlc.Timestamp,
	scheduleID int64,
	exec *sql.ExecutorConfig,
	txn *kv.Txn,
) (uuid.UUID, error) {
	protectedtsID := uuid.MakeV4()
	rec := jobsprotectedts.MakeRecord(protectedtsID, scheduleID, tsToProtect, deprecatedSpansToProtect,
		jobsprotectedts.Schedules, targetToProtect)
	return protectedtsID, exec.ProtectedTimestampProvider.Protect(ctx, txn, rec)
}
