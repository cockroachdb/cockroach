// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
)

// A backup is responsible for reading KVs in a target key span and writing them
// to an external storage sink. It is the backup's responsibility to ensure that
// the KVs it has to read as of a certain timestamp have not been
// garbage collected when it goes to read them. To achieve this, the backup,
// prior to execution writes a protected timestamp (pts) record on its target
// schema object, and releases this record upon successful completion.
//
// 1) In the case of a full backup, the pts record will protect data from GC,
// starting at the time the as of which we want to back data up.
//
// 2) In the case of an incremental backup, the pts record will protect data
// from GC, starting at the time at which the previous backup in the chain
// backed data up. This allows the incremental to read all revisions between the
// previous backup and itself.
//
// Previously, (2) meant that the gap between two backups in a chain had to be
// less than the GC TTL of the target schema object. If this was not the case,
// then the data the pts record needs to protect may have already been garbage
// collected, causing the incremental backup to fail.
//
// To decouple scheduled backups from GC TTL we make the backup schedules (full
// and incremental) responsible for managing pts records as described below.
// This is in addition to the job managed pts records explained above.
//
// 1. On successful completion of the first full backup, the full backup schedule
//    will write a pts record to protect all target data as of the time the full
//    backup backed up data. This will be done before the job managed pts record
//    is released, thereby guaranteeing that the data will still be live.
//    The full schedule will then store a reference of this record on the dependent
//    incremental schedule.
//
// 2. On successful completion of every incremental backup adding to the backup
//    chain, the incremental schedule will pull up the PTS record stored on it to
//    protect the target data as of the time the incremental backup covered.
//
// 3. On successful completion of subsequent full backups we will do two things:
//
//  a) Release the pts record stored on the incremental schedule corresponding to the
//     now old chain of backups.
//
//  b) Repeat 1)
//
// You should see that this chaining schema, along with the job managed pts
// records means that we are never trying to protect data at a timestamp that
// might have fallen out of the GC window, without a previously written pts
// record protecting it from GC.

// maybeUpdateSchedulePTSRecord is responsible for managing the schedule owned
// protected timestamp record based on the stage we are in in the scheme
// described above.
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

	return exec.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// We cannot rely on b.job containing created_by_id because on job
		// resumption the registry does not populate the resumers' CreatedByInfo.
		datums, err := txn.QueryRowEx(
			ctx,
			"lookup-schedule-info",
			txn.KV(),
			sessiondata.NodeUserSessionDataOverride,
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

		schedules := jobs.ScheduledJobTxn(txn)
		scheduleID := jobspb.ScheduleID(tree.MustBeDInt(datums[0]))
		sj, args, err := getScheduledBackupExecutionArgsFromSchedule(
			ctx, env, schedules, scheduleID,
		)
		if err != nil {
			return errors.Wrap(err, "load scheduled job")
		}

		// Check if the schedule is configured to chain protected timestamp records.
		if !args.ChainProtectedTimestampRecords {
			return nil
		}

		// Update the full schedule to disable chaining since there is no associated
		// inc schedule. It could have been dropped before we got here.
		if args.BackupType == backuppb.ScheduledBackupExecutionArgs_FULL && args.DependentScheduleID == 0 {
			args.ChainProtectedTimestampRecords = false
			any, err := pbtypes.MarshalAny(args)
			if err != nil {
				return err
			}
			sj.SetExecutionDetails(sj.ExecutorType(), jobspb.ExecutionArguments{Args: any})
			return jobs.ScheduledJobTxn(txn).Update(ctx, sj)
		}

		if backupDetails.SchedulePTSChainingRecord == nil {
			return errors.AssertionFailedf(
				"scheduled backup is chaining protected timestamp records but no chaining action was specified")
		}
		pts := exec.ProtectedTimestampProvider.WithTxn(txn)
		switch args.BackupType {
		case backuppb.ScheduledBackupExecutionArgs_INCREMENTAL:
			if backupDetails.SchedulePTSChainingRecord.Action != jobspb.SchedulePTSChainingRecord_UPDATE {
				return errors.AssertionFailedf("incremental backup has unexpected chaining action %d on"+
					" backup job details", backupDetails.SchedulePTSChainingRecord.Action)
			}
			if err := manageIncrementalBackupPTSChaining(ctx, pts,
				backupDetails.SchedulePTSChainingRecord.ProtectedTimestampRecord,
				backupDetails.EndTime, scheduleID); err != nil {
				return errors.Wrap(err, "failed to manage chaining of pts record during a inc backup")
			}
		case backuppb.ScheduledBackupExecutionArgs_FULL:
			if backupDetails.SchedulePTSChainingRecord.Action != jobspb.SchedulePTSChainingRecord_RELEASE {
				return errors.AssertionFailedf("full backup has unexpected chaining action %d on"+
					" backup job details", backupDetails.SchedulePTSChainingRecord.Action)
			}
			if err := manageFullBackupPTSChaining(ctx, pts, schedules, env, backupDetails, args, scheduleID); err != nil {
				return errors.Wrap(err, "failed to manage chaining of pts record during a full backup")
			}
		}
		return nil
	})
}

// manageFullBackupPTSChaining implements is responsible for managing the
// schedule owned protected timestamp record on completion of a full backup.
func manageFullBackupPTSChaining(
	ctx context.Context,
	pts protectedts.Storage,
	schedules jobs.ScheduledJobStorage,
	env scheduledjobs.JobSchedulerEnv,
	backupDetails jobspb.BackupDetails,
	fullScheduleArgs *backuppb.ScheduledBackupExecutionArgs,
	scheduleID jobspb.ScheduleID,
) error {
	// Let's resolve the dependent incremental schedule as the first step. If the
	// schedule has been dropped then we can avoid doing unnecessary work.
	incSj, incArgs, err := getScheduledBackupExecutionArgsFromSchedule(
		ctx, env, schedules, fullScheduleArgs.DependentScheduleID,
	)
	if err != nil {
		if jobs.HasScheduledJobNotFoundError(err) {
			log.Warningf(ctx, "could not find dependent schedule with id %d",
				fullScheduleArgs.DependentScheduleID)
			return nil
		}
		return err
	}

	// Resolve the target that needs to be protected on this execution of the
	// scheduled backup.
	targetToProtect, deprecatedSpansToProtect, err := getTargetProtectedByBackup(ctx, pts, backupDetails)
	if err != nil {
		return errors.Wrap(err, "getting target to protect")
	}

	// Records written by the backup schedule should be ignored when making GC
	// decisions on any table that has been marked as `exclude_data_from_backup`.
	// This ensures that the schedule does not holdup GC on that table span for
	// the duration of execution.
	if targetToProtect != nil {
		targetToProtect.IgnoreIfExcludedFromBackup = true
	}

	// Protect the target after the EndTime of the current backup. Data in the
	// target will not have been GC'ed because we have a protected timestamp
	// record written during backup planning, already protecting this target after
	// EndTime.
	//
	// Since this record will be stored on the incremental schedule, we use the
	// inc schedule ID as the records' Meta. This ensures that even if the full
	// schedule is dropped, the reconciliation job will not release the pts
	// record stored on the inc schedule, and the chaining will continue.
	log.Infof(ctx, "schedule %d is writing a protected timestamp record at %s",
		scheduleID, backupDetails.EndTime.String())
	ptsRecord, err := protectTimestampRecordForSchedule(
		ctx, pts, targetToProtect, deprecatedSpansToProtect,
		backupDetails.EndTime, incSj.ScheduleID(),
	)
	if err != nil {
		return errors.Wrap(err, "protect pts record for schedule")
	}

	// When the full backup was being planned, if there was a protected timestamp
	// record on the incremental schedule then we should try and release it. This
	// should be the case for every scheduled full backup after the first
	//
	// This is because, with the completion of this scheduled full backup we will
	// be starting a new backup chain, and therefore the record associated with
	// the old chain should be released.
	//
	// NB: Since this logic runs after we have written the scheduled full backup
	// manifest to external storage, we are guaranteed that no new incremental
	// backups will build on top of the old chain, and rely on the record we are
	// about to release. Already running incremental backup jobs would have
	// written their own pts record during planning, and should complete
	// successfully.
	log.Infof(ctx, "schedule %d is releasing a protected timestamp record held by the previous chain", scheduleID)
	if err := releaseProtectedTimestamp(
		ctx, pts,
		backupDetails.SchedulePTSChainingRecord.ProtectedTimestampRecord,
	); err != nil {
		return errors.Wrap(err, "release pts record for schedule")
	}

	// Update the incremental schedule with the new pts record.
	incArgs.ProtectedTimestampRecord = &ptsRecord
	any, err := pbtypes.MarshalAny(incArgs)
	if err != nil {
		return err
	}
	incSj.SetExecutionDetails(incSj.ExecutorType(), jobspb.ExecutionArguments{Args: any})
	return schedules.Update(ctx, incSj)
}

// manageFullBackupPTSChaining implements is responsible for managing the
// schedule owned protected timestamp record on completion of an incremental
// backup.
func manageIncrementalBackupPTSChaining(
	ctx context.Context,
	pts protectedts.Storage,
	ptsRecordID *uuid.UUID,
	tsToProtect hlc.Timestamp,
	scheduleID jobspb.ScheduleID,
) error {
	if ptsRecordID == nil {
		return errors.AssertionFailedf("unexpected nil pts record id on incremental schedule %d", scheduleID)
	}
	log.Infof(ctx, "schedule %d is updating a protected timestamp record to %s", scheduleID, tsToProtect.String())
	err := pts.UpdateTimestamp(ctx, *ptsRecordID, tsToProtect)
	// If we cannot find the pts record to update it is possible that a concurrent
	// full backup has released the record, and written a new record on the
	// incremental schedule. This should only happen if this is an "overhang"
	// incremental backup i.e. an incremental that has completed after a
	// concurrent full backup has started a new chain.
	//
	// In such a scenario it is okay to do nothing since the next incremental on
	// the new full backup will rely on the pts record written by the full backup.
	if err != nil && errors.Is(err, protectedts.ErrNotExists) {
		log.Warningf(ctx, "failed to update timestamp record %d since it does not exist", ptsRecordID)
		return nil //nolint:returnerrcheck
	}
	return err
}

func getTargetProtectedByBackup(
	ctx context.Context, pts protectedts.Storage, backupDetails jobspb.BackupDetails,
) (target *ptpb.Target, deprecatedSpans []roachpb.Span, err error) {
	if backupDetails.ProtectedTimestampRecord == nil {
		return nil, nil, nil
	}

	ptsRecord, err := pts.GetRecord(ctx, *backupDetails.ProtectedTimestampRecord)
	if err != nil {
		return nil, nil, err
	}

	return ptsRecord.Target, ptsRecord.DeprecatedSpans, nil
}

func protectTimestampRecordForSchedule(
	ctx context.Context,
	pts protectedts.Storage,
	targetToProtect *ptpb.Target,
	deprecatedSpansToProtect roachpb.Spans,
	tsToProtect hlc.Timestamp,
	scheduleID jobspb.ScheduleID,
) (uuid.UUID, error) {
	protectedtsID := uuid.MakeV4()
	return protectedtsID, pts.Protect(ctx, jobsprotectedts.MakeRecord(
		protectedtsID, int64(scheduleID), tsToProtect, deprecatedSpansToProtect,
		jobsprotectedts.Schedules, targetToProtect,
	))
}
