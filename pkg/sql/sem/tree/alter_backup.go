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

// AlterBackup represents an ALTER BACKUP statement
type AlterBackup struct {
	// Backup contains the locations for the backup we seek to add new keys to.
	Backup Expr
	Subdir Expr
	Cmd    AlterBackupCmd
}

// Format implements the NodeFormatter interface.
func (node *AlterBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER BACKUP ")

	if node.Subdir != nil {
		ctx.FormatNode(node.Subdir)
		ctx.WriteString(" IN ")
	}

	ctx.FormatNode(node.Backup)
}

var _ Statement = &AlterBackup{}

type AlterBackupCmd interface {
	NodeFormatter
	alterBackupCmd()
}

var _ AlterBackupCmd = &AlterBackupKMS{}

type AlterBackupKMS struct {
	NewKMSURI StringOrPlaceholderOptList
	OldKMSURI StringOrPlaceholderOptList
}

func (node *AlterBackupKMS) alterBackupCmd() {}

func (node *AlterBackupKMS) Format(ctx *FmtCtx) {
	ctx.WriteString(" ADD NEW_KMS=")
	ctx.FormatNode(&node.NewKMSURI)

	ctx.WriteString(" WITH OLD_KMS=")
	ctx.FormatNode(&node.OldKMSURI)
}
