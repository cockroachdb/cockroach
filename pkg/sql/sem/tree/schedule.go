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

// FullBackupClause describes the frequency of full backups.
type FullBackupClause struct {
	AlwaysFull bool
	Recurrence Expr
}

// ScheduleLabelSpec describes the labeling specification for a scheduled job.
type ScheduleLabelSpec struct {
	IfNotExists bool
	Label       Expr
}

// ScheduledBackup represents scheduled backup job.
type ScheduledBackup struct {
	ScheduleLabelSpec ScheduleLabelSpec
	Recurrence        Expr
	FullBackup        *FullBackupClause /* nil implies choose default */
	Targets           *TargetList       /* nil implies tree.AllDescriptors coverage */
	To                StringOrPlaceholderOptList
	BackupOptions     BackupOptions
	ScheduleOptions   KVOptions
}

var _ Statement = &ScheduledBackup{}

// Format implements the NodeFormatter interface.
func (node *ScheduledBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEDULE")

	if node.ScheduleLabelSpec.IfNotExists {
		ctx.WriteString(" IF NOT EXISTS")
	}
	if node.ScheduleLabelSpec.Label != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.ScheduleLabelSpec.Label)
	}

	ctx.WriteString(" FOR BACKUP")
	if node.Targets != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.Targets)
	}

	ctx.WriteString(" INTO ")
	ctx.FormatNode(&node.To)

	if !node.BackupOptions.IsDefault() {
		ctx.WriteString(" WITH ")
		ctx.FormatNode(&node.BackupOptions)
	}

	ctx.WriteString(" RECURRING ")
	if node.Recurrence == nil {
		ctx.WriteString("NEVER")
	} else {
		ctx.FormatNode(node.Recurrence)
	}

	if node.FullBackup != nil {

		if node.FullBackup.Recurrence != nil {
			ctx.WriteString(" FULL BACKUP ")
			ctx.FormatNode(node.FullBackup.Recurrence)
		} else if node.FullBackup.AlwaysFull {
			ctx.WriteString(" FULL BACKUP ALWAYS")
		}
	}

	if node.ScheduleOptions != nil {
		ctx.WriteString(" WITH SCHEDULE OPTIONS ")
		ctx.FormatNode(&node.ScheduleOptions)
	}
}

// Coverage return the coverage (all vs requested).
func (node ScheduledBackup) Coverage() DescriptorCoverage {
	if node.Targets == nil {
		return AllDescriptors
	}
	return RequestedDescriptors
}
