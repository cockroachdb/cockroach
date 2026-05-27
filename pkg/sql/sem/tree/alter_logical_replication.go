// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// AlterLogicalReplicationStream represents an ALTER LOGICAL REPLICATION STREAM
// statement. The statement supports a list of subcommands applied to the job
// identified by JobID.
type AlterLogicalReplicationStream struct {
	JobID Expr
	Cmds  AlterLogicalReplicationCmds
}

// AlterLogicalReplicationCmds is the list of subcommands attached to an
// AlterLogicalReplicationStream statement.
type AlterLogicalReplicationCmds []AlterLogicalReplicationCmd

// AlterLogicalReplicationCmd is the interface implemented by each subcommand of
// an ALTER LOGICAL REPLICATION STREAM statement.
type AlterLogicalReplicationCmd interface {
	NodeFormatter
	alterLogicalReplicationCmd()
}

// SkipConflictCmd represents the SKIP subcommand, which advances the replicated
// time on a paused transactional LDR job.
type SkipConflictCmd struct{}

func (*SkipConflictCmd) alterLogicalReplicationCmd() {}

var _ Statement = &AlterLogicalReplicationStream{}
var _ AlterLogicalReplicationCmd = &SkipConflictCmd{}

// Format implements the NodeFormatter interface.
func (node *AlterLogicalReplicationStream) Format(ctx *FmtCtx) {
	ctx.WriteString("ALTER LOGICAL REPLICATION STREAM ")
	ctx.FormatNode(node.JobID)
	for _, cmd := range node.Cmds {
		ctx.WriteString(" ")
		ctx.FormatNode(cmd)
	}
}

// Format implements the NodeFormatter interface.
func (node *SkipConflictCmd) Format(ctx *FmtCtx) {
	ctx.WriteString("SKIP")
}

// StatementReturnType implements the Statement interface.
func (*AlterLogicalReplicationStream) StatementReturnType() StatementReturnType {
	return Ack
}

// StatementType implements the Statement interface.
func (*AlterLogicalReplicationStream) StatementType() StatementType { return TypeDML }

// StatementTag returns a short string identifying the type of statement.
func (*AlterLogicalReplicationStream) StatementTag() string {
	return `ALTER LOGICAL REPLICATION STREAM`
}

func (*AlterLogicalReplicationStream) planHookStatement() {}

// String implements the fmt.Stringer interface.
func (node *AlterLogicalReplicationStream) String() string {
	return AsString(node)
}
