// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

// AlterBackup represents an ALTER BACKUP statement.
type AlterBackup struct {
	// Backup contains the locations for the backup we seek to add new keys to.
	Backup Expr
	Subdir Expr
	Cmds   AlterBackupCmds
}

var _ Statement = &AlterBackup{}

// Format implements the NodeFormatter interface.
func (node *AlterBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER BACKUP ")

	if node.Subdir != nil {
		ctx.FormatNode(node.Subdir)
		ctx.WriteString(" IN ")
	}

	ctx.FormatNode(node.Backup)
	ctx.FormatNode(&node.Cmds)
}

// AlterBackupCmds is an array of type AlterBackupCmd
type AlterBackupCmds []AlterBackupCmd

// Format implements the NodeFormatter interface.
func (node *AlterBackupCmds) Format(ctx *FmtCtx) {
	for i, n := range *node {
		if i > 0 {
			ctx.WriteString(" ")
		}
		ctx.FormatNode(n)
	}
}

// AlterBackupCmd represents a backup modification operation.
type AlterBackupCmd interface {
	NodeFormatter
	alterBackupCmd()
}

func (node *AlterBackupKMS) alterBackupCmd() {}

var _ AlterBackupCmd = &AlterBackupKMS{}

// AlterBackupKMS represents a possible alter_backup_cmd option.
type AlterBackupKMS struct {
	KMSInfo BackupKMS
}

// Format implements the NodeFormatter interface.
func (node *AlterBackupKMS) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD NEW_KMS=")
	ctx.FormatNode(&node.KMSInfo.NewKMSURI)

	ctx.WriteString(" WITH OLD_KMS=")
	ctx.FormatNode(&node.KMSInfo.OldKMSURI)
}

// BackupKMS represents possible options used when altering a backup KMS
type BackupKMS struct {
	NewKMSURI StringOrPlaceholderOptList
	OldKMSURI StringOrPlaceholderOptList
}
