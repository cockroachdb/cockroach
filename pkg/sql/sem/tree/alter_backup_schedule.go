// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "strconv"

// AlterBackupSchedule represents an ALTER BACKUP SCHEDULE statement.
type AlterBackupSchedule struct {
	ScheduleID uint64
	Cmds       AlterBackupScheduleCmds
}

var _ Statement = &AlterBackupSchedule{}

// Format implements the NodeFormatter interface.
func (node *AlterBackupSchedule) Format(ctx *FmtCtx) {
	ctx.WriteString(`ALTER BACKUP SCHEDULE `)
	if ctx.HasFlags(FmtHideConstants) || ctx.HasFlags(FmtAnonymize) {
		ctx.WriteString("123")
	} else {
		ctx.WriteString(strconv.FormatUint(node.ScheduleID, 10))
	}
	ctx.WriteByte(' ')
	ctx.FormatNode(&node.Cmds)
}

// AlterBackupScheduleCmds represents a list of changefeed alterations
type AlterBackupScheduleCmds []AlterBackupScheduleCmd

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(n)
	}
}

// AlterBackupScheduleCmd represents a changefeed modification operation.
type AlterBackupScheduleCmd interface {
	NodeFormatter
	// Placeholder function to ensure that only desired types
	// (AlterBackupSchedule*) conform to the AlterBackupScheduleCmd interface.
	alterBackupScheduleCmd()
}

func (*AlterBackupScheduleSetLabel) alterBackupScheduleCmd()          {}
func (*AlterBackupScheduleSetInto) alterBackupScheduleCmd()           {}
func (*AlterBackupScheduleSetWith) alterBackupScheduleCmd()           {}
func (*AlterBackupScheduleSetRecurring) alterBackupScheduleCmd()      {}
func (*AlterBackupScheduleSetFullBackup) alterBackupScheduleCmd()     {}
func (*AlterBackupScheduleSetScheduleOption) alterBackupScheduleCmd() {}
func (*AlterBackupScheduleNextRun) alterBackupScheduleCmd()           {}

var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetLabel{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetInto{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetWith{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetRecurring{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetFullBackup{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleSetScheduleOption{}
var _ AlterBackupScheduleCmd = &AlterBackupScheduleNextRun{}

// AlterBackupScheduleSetLabel represents an ADD <label> command
type AlterBackupScheduleSetLabel struct {
	Label Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetLabel) Format(ctx *FmtCtx) {
	ctx.WriteString("SET LABEL ")
	ctx.FormatNode(node.Label)
}

// AlterBackupScheduleSetInto represents a SET <destinations> command
type AlterBackupScheduleSetInto struct {
	Into StringOrPlaceholderOptList
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetInto) Format(ctx *FmtCtx) {
	ctx.WriteString("SET INTO ")
	ctx.FormatURIs(node.Into)
}

// AlterBackupScheduleSetWith represents an SET <options> command
type AlterBackupScheduleSetWith struct {
	With *BackupOptions
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetWith) Format(ctx *FmtCtx) {
	ctx.WriteString("SET WITH ")
	ctx.FormatNode(node.With)
}

// AlterBackupScheduleSetRecurring represents an SET RECURRING <recurrence> command
type AlterBackupScheduleSetRecurring struct {
	Recurrence Expr
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetRecurring) Format(ctx *FmtCtx) {
	ctx.WriteString("SET RECURRING ")
	if node.Recurrence == nil {
		ctx.WriteString("NEVER")
	} else {
		ctx.FormatNode(node.Recurrence)
	}
}

// AlterBackupScheduleSetFullBackup represents an SET FULL BACKUP <recurrence> command
type AlterBackupScheduleSetFullBackup struct {
	FullBackup FullBackupClause
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetFullBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("SET FULL BACKUP ")
	if node.FullBackup.AlwaysFull {
		ctx.WriteString("ALWAYS")
	} else {
		ctx.FormatNode(node.FullBackup.Recurrence)
	}
}

// AlterBackupScheduleSetScheduleOption represents an SET SCHEDULE OPTION <kv_options> command
type AlterBackupScheduleSetScheduleOption struct {
	Option KVOption
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleSetScheduleOption) Format(ctx *FmtCtx) {
	ctx.WriteString("SET SCHEDULE OPTION ")

	// KVOption Key values never contain PII and should be distinguished
	// for feature tracking purposes.
	o := node.Option
	ctx.WithFlags(ctx.flags&^FmtMarkRedactionNode, func() {
		ctx.FormatNode(&o.Key)
	})
	if o.Value != nil {
		ctx.WriteString(` = `)
		ctx.FormatNode(o.Value)
	}
}

// AlterBackupScheduleRunNow represents a RUN NOW command.
type AlterBackupScheduleNextRun struct {
	Full bool
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupScheduleNextRun) Format(ctx *FmtCtx) {
	if node.Full {
		ctx.WriteString("EXECUTE FULL IMMEDIATELY")
	} else {
		ctx.WriteString("EXECUTE IMMEDIATELY")
	}
}
