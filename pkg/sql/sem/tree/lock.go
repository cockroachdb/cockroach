// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// LockMode represents the lock strength in a LOCK TABLE statement.
type LockMode int

const (
	LockModeAccessShare LockMode = iota + 1
	LockModeRowShare
	LockModeRowExclusive
	LockModeShareUpdateExclusive
	LockModeShare
	LockModeShareRowExclusive
	LockModeExclusive
	LockModeAccessExclusive
)

func (m LockMode) String() string {
	switch m {
	case LockModeAccessShare:
		return "ACCESS SHARE"
	case LockModeRowShare:
		return "ROW SHARE"
	case LockModeRowExclusive:
		return "ROW EXCLUSIVE"
	case LockModeShareUpdateExclusive:
		return "SHARE UPDATE EXCLUSIVE"
	case LockModeShare:
		return "SHARE"
	case LockModeShareRowExclusive:
		return "SHARE ROW EXCLUSIVE"
	case LockModeExclusive:
		return "EXCLUSIVE"
	case LockModeAccessExclusive:
		return "ACCESS EXCLUSIVE"
	default:
		return "UNKNOWN"
	}
}

// LockTable represents a LOCK TABLE statement.
type LockTable struct {
	Tables TableNames
	Mode   LockMode
	NoWait bool
}

var _ Statement = &LockTable{}

// Format implements the NodeFormatter interface.
func (node *LockTable) Format(ctx *FmtCtx) {
	ctx.WriteString("LOCK TABLE ")
	for i := range node.Tables {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&node.Tables[i])
	}
	ctx.WriteString(" IN ")
	ctx.WriteString(node.Mode.String())
	ctx.WriteString(" MODE")
	if node.NoWait {
		ctx.WriteString(" NOWAIT")
	}
}

// String implements the Statement interface.
func (node *LockTable) String() string {
	return AsString(node)
}

// StatementReturnType implements the Statement interface.
func (*LockTable) StatementReturnType() StatementReturnType { return Ack }

// StatementType implements the Statement interface.
func (*LockTable) StatementType() StatementType { return TypeTCL }

// StatementTag returns a short string identifying the type of statement.
func (*LockTable) StatementTag() string { return "LOCK TABLE" }
