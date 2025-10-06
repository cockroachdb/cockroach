// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// FullBackupClause describes the frequency of full backups.
type FullBackupClause struct {
	AlwaysFull bool
	Recurrence Expr
}

// LabelSpec describes the labeling specification for an object.
type LabelSpec struct {
	IfNotExists bool
	Label       Expr
}

// Format implements the NodeFormatter interface.
func (l *LabelSpec) Format(ctx *FmtCtx) {
	if l.IfNotExists {
		ctx.WriteString(" IF NOT EXISTS")
	}
	if l.Label != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(l.Label)
	}
}

var _ NodeFormatter = &LabelSpec{}

// ScheduledBackup represents scheduled backup job.
type ScheduledBackup struct {
	ScheduleLabelSpec LabelSpec
	Recurrence        Expr
	FullBackup        *FullBackupClause /* nil implies choose default */
	Targets           *BackupTargetList /* nil implies tree.AllDescriptors coverage */
	To                StringOrPlaceholderOptList
	BackupOptions     BackupOptions
	ScheduleOptions   KVOptions
}

var _ Statement = &ScheduledBackup{}

// Format implements the NodeFormatter interface.
func (node *ScheduledBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEDULE")

	ctx.FormatNode(&node.ScheduleLabelSpec)
	ctx.WriteString(" FOR BACKUP")
	if node.Targets != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.Targets)
	}

	ctx.WriteString(" INTO ")
	ctx.FormatURIs(node.To)

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

// ScheduledChangefeed represents scheduled changefeed job.
type ScheduledChangefeed struct {
	*CreateChangefeed
	ScheduleLabelSpec LabelSpec
	Recurrence        Expr
	ScheduleOptions   KVOptions
}

// Format implements the NodeFormatter interface.
func (node *ScheduledChangefeed) Format(ctx *FmtCtx) {
	ctx.WriteString("CREATE SCHEDULE")

	if node.ScheduleLabelSpec.IfNotExists {
		ctx.WriteString(" IF NOT EXISTS")
	}

	if node.ScheduleLabelSpec.Label != nil {
		ctx.WriteString(" ")
		ctx.FormatNode(node.ScheduleLabelSpec.Label)
	}

	ctx.WriteString(" FOR CHANGEFEED")

	if node.Select == nil {
		ctx.WriteString(" ")
		ctx.FormatNode(&node.Targets)
	}

	ctx.WriteString(" INTO ")
	ctx.FormatNode(node.SinkURI)

	if node.Options != nil {
		ctx.WriteString(" WITH OPTIONS (")
		ctx.FormatNode(&node.Options)
		ctx.WriteString(" )")
	}

	if node.Select != nil {
		ctx.WriteString(" AS ")
		ctx.FormatNode(node.Select)
	}

	ctx.WriteString(" RECURRING ")
	ctx.FormatNode(node.Recurrence)

	if node.ScheduleOptions != nil {
		ctx.WriteString(" WITH SCHEDULE OPTIONS ")
		ctx.FormatNode(&node.ScheduleOptions)
	}
}
