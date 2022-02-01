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
	// Backup contains the locations for the backup we seek to add new keys to
	Backup    []StringOrPlaceholderOptList
	Subdir    Expr
	OldKMSURI StringOrPlaceholderOptList
	NewKMSURI StringOrPlaceholderOptList
}

var _ Statement = &AlterBackup{}

// Format implements the NodeFormatter interface.
func (node *AlterBackup) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER BACKUP ")

	if node.Subdir != nil {
		ctx.FormatNode(node.Subdir)
		ctx.WriteString(" IN ")
	}
	for i := range node.Backup {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Backup[i])
	}

	ctx.WriteString(" ADD NEW_KMS=")
	ctx.FormatNode(&node.NewKMSURI)

	ctx.WriteString(" WITH OLD_KMS=")
	ctx.FormatNode(&node.OldKMSURI)
}
