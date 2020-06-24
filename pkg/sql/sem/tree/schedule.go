// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import "fmt"

// The name of the scheduled job executor responsible for execution of scheduled
// backups.
const ScheduledBackupExecutorTypeName = "backup-executor"

// ScheduleErrorHandling mirrors enum defined in jobspb.ScheduleDetails_ErrorHandlingBehavior
// This enum is redefined here to avoid dependency loop.
type ScheduleErrorHandling int32

const (
	// ScheduleFailureDefault uses default mechanism for failure handling.
	ScheduleFailureDefault ScheduleErrorHandling = iota
	// ScheduleFailureRetry attempts to execute scheduled job again, retrying based on crontab.
	ScheduleFailureRetry
	// ScheduleFailureRetrySoon retries execution after some delay.
	ScheduleFailureRetrySoon
	// ScheduleFailurePause pauses this schedule.
	ScheduleFailurePause
)

// ScheduleWaitBehavior mirrors enum defined in jobspb.ScheduleDetails_WaitBehavior
// This enum is redefined here to avoid dependency loop.
type ScheduleWaitBehavior int32

const (
	// ScheduleWaitDefault uses default waiting behavior.
	ScheduleWaitDefault ScheduleWaitBehavior = iota
	// ScheduleWait waits for the previous run to complete.
	ScheduleWait
	// ScheduleDoNotWait starts the next run even if the previous one has not completed.
	ScheduleDoNotWait
	// ScheduleSkip skips current run and retries the next one based on the schedule.
	ScheduleSkipRun
)

// ScheduleOptions is container of configuration options generic to any schedule.
type ScheduleOptions struct {
	ScheduleName string
	ExecutorType string
	FirstRun     Expr
	Recurrence   *string
	OnError      ScheduleErrorHandling
	WaitBehavior ScheduleWaitBehavior
}

// BackupScheduleOptions contains generic ScheduleOptions plus options specific to backup.
type BackupScheduleOptions struct {
	ScheduleOptions
	ChangeCapturePeriod Expr
}

var _ NodeFormatter = &ScheduleOptions{}
var _ NodeFormatter = &BackupScheduleOptions{}

// ScheduledBackup represents scheduled backup job.
type ScheduledBackup struct {
	*BackupScheduleOptions
	*Backup
}

var _ Statement = &ScheduledBackup{}

// Format implements the NodeFormatter interface.
func (opts *ScheduleOptions) Format(ctx *FmtCtx) {
	if opts.FirstRun != nil {
		ctx.WriteString(" STARTING ")
		opts.FirstRun.Format(ctx)
	}

	fmt.Fprintf(ctx, " RECURRING %q", opts.Recurrence)
	fmt.Fprintf(ctx, " ON EXECUTION FAILURE %q", opts.OnError)
	fmt.Fprintf(ctx, " ON PREVIOUS RUNNING %q", opts.WaitBehavior)
}

// Format implements the NodeFormatter interface.
func (opts *BackupScheduleOptions) Format(ctx *FmtCtx) {
	if opts.ChangeCapturePeriod != nil {
		ctx.WriteString(" APPENDING CHANGES EVERY ")
		opts.ChangeCapturePeriod.Format(ctx)
	}
}

// Format implements the NodeFormatter interface.
func (node *ScheduledBackup) Format(ctx *FmtCtx) {
	fmt.Fprintf(ctx, "CREATE SCHEDULE %q FOR ", node.ScheduleName)
	node.Backup.Format(ctx)
	node.BackupScheduleOptions.Format(ctx)
}

// String() implements Stringer interface.
func (e ScheduleErrorHandling) String() string {
	switch e {
	case ScheduleFailureRetrySoon:
		return "retry soon"
	case ScheduleFailurePause:
		return "pause schedule"
	default:
		return "retry next scheduled"
	}
}

// String() implements Stringer interface.
func (w ScheduleWaitBehavior) String() string {
	switch w {
	case ScheduleSkipRun:
		return "skip run"
	case ScheduleDoNotWait:
		return "start anyway"
	default:
		return "wait"
	}
}
