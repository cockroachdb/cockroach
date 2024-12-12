// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/redact"
)

// IsolationLevel holds the isolation level for a transaction.
type IsolationLevel int

var _ redact.SafeValue = IsolationLevel(0)

// SafeValue makes Kind a redact.SafeValue.
func (k IsolationLevel) SafeValue() {}

// IsolationLevel values
const (
	UnspecifiedIsolation IsolationLevel = iota
	ReadUncommittedIsolation
	ReadCommittedIsolation
	RepeatableReadIsolation
	SnapshotIsolation
	SerializableIsolation
)

var isolationLevelNames = [...]string{
	UnspecifiedIsolation:     "UNSPECIFIED",
	ReadUncommittedIsolation: "READ UNCOMMITTED",
	ReadCommittedIsolation:   "READ COMMITTED",
	RepeatableReadIsolation:  "REPEATABLE READ",
	SnapshotIsolation:        "SNAPSHOT",
	SerializableIsolation:    "SERIALIZABLE",
}

// IsolationLevelMap is a map from string isolation level name to isolation
// level, in the lowercase format that set isolation_level supports.
var IsolationLevelMap = map[string]IsolationLevel{
	"read uncommitted": ReadUncommittedIsolation,
	"read committed":   ReadCommittedIsolation,
	"repeatable read":  RepeatableReadIsolation,
	"snapshot":         SnapshotIsolation,
	"serializable":     SerializableIsolation,
}

func (i IsolationLevel) String() string {
	if i < 0 || i > IsolationLevel(len(isolationLevelNames)-1) {
		return fmt.Sprintf("IsolationLevel(%d)", i)
	}
	return isolationLevelNames[i]
}

// ToKVIsoLevel converts an IsolationLevel to its isolation.Level equivalent.
func (i IsolationLevel) ToKVIsoLevel() isolation.Level {
	switch i {
	case ReadUncommittedIsolation, ReadCommittedIsolation:
		return isolation.ReadCommitted
	case RepeatableReadIsolation, SnapshotIsolation:
		return isolation.Snapshot
	case SerializableIsolation:
		return isolation.Serializable
	default:
		panic(fmt.Sprintf("unknown isolation level: %s", i))
	}
}

// FromKVIsoLevel converts an isolation.Level to its SQL semantic equivalent.
func FromKVIsoLevel(level isolation.Level) IsolationLevel {
	switch level {
	case isolation.ReadCommitted:
		return ReadCommittedIsolation
	case isolation.Snapshot:
		return RepeatableReadIsolation
	case isolation.Serializable:
		return SerializableIsolation
	default:
		panic(fmt.Sprintf("unknown isolation level: %s", level))
	}
}

// UpgradeToEnabledLevel upgrades the isolation level to the weakest enabled
// isolation level that is stronger than or equal to the input level.
func (i IsolationLevel) UpgradeToEnabledLevel(
	allowReadCommitted, allowRepeatableRead, hasLicense bool,
) (_ IsolationLevel, upgraded, upgradedDueToLicense bool) {
	switch i {
	case ReadUncommittedIsolation:
		// READ UNCOMMITTED is mapped to READ COMMITTED. PostgreSQL also does
		// this: https://www.postgresql.org/docs/current/transaction-iso.html.
		upgraded = true
		fallthrough
	case ReadCommittedIsolation:
		// READ COMMITTED is only allowed if the cluster setting is enabled and the
		// cluster has a license. Otherwise, it is mapped to a stronger isolation
		// level (REPEATABLE READ if enabled, SERIALIZABLE otherwise).
		if allowReadCommitted && hasLicense {
			return ReadCommittedIsolation, upgraded, upgradedDueToLicense
		}
		upgraded = true
		if allowReadCommitted && !hasLicense {
			upgradedDueToLicense = true
		}
		fallthrough
	case RepeatableReadIsolation, SnapshotIsolation:
		// REPEATABLE READ and SNAPSHOT are considered aliases. The isolation levels
		// are only allowed if the cluster setting is enabled and the cluster has a
		// license. Otherwise, they are mapped to SERIALIZABLE.
		if allowRepeatableRead && hasLicense {
			return RepeatableReadIsolation, upgraded, upgradedDueToLicense
		}
		upgraded = true
		if allowRepeatableRead && !hasLicense {
			upgradedDueToLicense = true
		}
		fallthrough
	case SerializableIsolation:
		return SerializableIsolation, upgraded, upgradedDueToLicense
	default:
		panic(fmt.Sprintf("unknown isolation level: %s", i))
	}
}

// UserPriority holds the user priority for a transaction.
type UserPriority int

// UserPriority values
const (
	UnspecifiedUserPriority UserPriority = iota
	Low
	Normal
	High
)

var userPriorityNames = [...]string{
	UnspecifiedUserPriority: "UNSPECIFIED",
	Low:                     "LOW",
	Normal:                  "NORMAL",
	High:                    "HIGH",
}

func (up UserPriority) String() string {
	if up < 0 || up > UserPriority(len(userPriorityNames)-1) {
		return fmt.Sprintf("UserPriority(%d)", up)
	}
	return userPriorityNames[up]
}

// UserPriorityFromString converts a string into a UserPriority.
func UserPriorityFromString(val string) (_ UserPriority, ok bool) {
	switch strings.ToUpper(val) {
	case "LOW":
		return Low, true
	case "NORMAL":
		return Normal, true
	case "HIGH":
		return High, true
	default:
		return 0, false
	}
}

// ReadWriteMode holds the read write mode for a transaction.
type ReadWriteMode int

// ReadWriteMode values
const (
	UnspecifiedReadWriteMode ReadWriteMode = iota
	ReadOnly
	ReadWrite
)

var readWriteModeNames = [...]string{
	UnspecifiedReadWriteMode: "UNSPECIFIED",
	ReadOnly:                 "ONLY",
	ReadWrite:                "WRITE",
}

func (ro ReadWriteMode) String() string {
	if ro < 0 || ro > ReadWriteMode(len(readWriteModeNames)-1) {
		return fmt.Sprintf("ReadWriteMode(%d)", ro)
	}
	return readWriteModeNames[ro]
}

// DeferrableMode holds the deferrable mode for a transaction.
type DeferrableMode int

// DeferrableMode values.
const (
	UnspecifiedDeferrableMode DeferrableMode = iota
	Deferrable
	NotDeferrable
)

var deferrableModeNames = [...]string{
	UnspecifiedDeferrableMode: "UNSPECIFIED",
	Deferrable:                "DEFERRABLE",
	NotDeferrable:             "NOT DEFERRABLE",
}

func (d DeferrableMode) String() string {
	if d < 0 || d > DeferrableMode(len(deferrableModeNames)-1) {
		return fmt.Sprintf("DeferrableMode(%d)", d)
	}
	return deferrableModeNames[d]
}

// TransactionModes holds the transaction modes for a transaction.
type TransactionModes struct {
	Isolation     IsolationLevel
	UserPriority  UserPriority
	ReadWriteMode ReadWriteMode
	AsOf          AsOfClause
	Deferrable    DeferrableMode
}

// Format implements the NodeFormatter interface.
func (node *TransactionModes) Format(ctx *FmtCtx) {
	var sep string
	if node.Isolation != UnspecifiedIsolation {
		ctx.Printf(" ISOLATION LEVEL %s", node.Isolation)
		sep = ","
	}
	if node.UserPriority != UnspecifiedUserPriority {
		ctx.Printf("%s PRIORITY %s", sep, node.UserPriority)
		sep = ","
	}
	if node.ReadWriteMode != UnspecifiedReadWriteMode {
		ctx.Printf("%s READ %s", sep, node.ReadWriteMode)
		sep = ","
	}
	if node.AsOf.Expr != nil {
		ctx.WriteString(sep)
		ctx.WriteString(" ")
		ctx.FormatNode(&node.AsOf)
		sep = ","
	}
	if node.Deferrable != UnspecifiedDeferrableMode {
		ctx.Printf("%s %s", sep, node.Deferrable)
	}
}

var (
	errIsolationLevelSpecifiedMultipleTimes = pgerror.New(pgcode.Syntax, "isolation level specified multiple times")
	errUserPrioritySpecifiedMultipleTimes   = pgerror.New(pgcode.Syntax, "user priority specified multiple times")
	errReadModeSpecifiedMultipleTimes       = pgerror.New(pgcode.Syntax, "read mode specified multiple times")
	errAsOfSpecifiedMultipleTimes           = pgerror.New(pgcode.Syntax, "AS OF SYSTEM TIME specified multiple times")
	errDeferrableSpecifiedMultipleTimes     = pgerror.New(pgcode.Syntax, "deferrable mode specified multiple times")

	// ErrAsOfSpecifiedWithReadWrite is returned when a statement attempts to set
	// a historical query to READ WRITE which conflicts with its implied READ ONLY
	// mode.
	ErrAsOfSpecifiedWithReadWrite = pgerror.New(pgcode.Syntax, "AS OF SYSTEM TIME specified with READ WRITE mode")
)

// Merge groups two sets of transaction modes together.
// Used in the parser.
func (node *TransactionModes) Merge(other TransactionModes) error {
	if other.Isolation != UnspecifiedIsolation {
		if node.Isolation != UnspecifiedIsolation {
			return errIsolationLevelSpecifiedMultipleTimes
		}
		node.Isolation = other.Isolation
	}
	if other.UserPriority != UnspecifiedUserPriority {
		if node.UserPriority != UnspecifiedUserPriority {
			return errUserPrioritySpecifiedMultipleTimes
		}
		node.UserPriority = other.UserPriority
	}
	if other.AsOf.Expr != nil {
		if node.AsOf.Expr != nil {
			return errAsOfSpecifiedMultipleTimes
		}
		node.AsOf.Expr = other.AsOf.Expr
	}
	if other.ReadWriteMode != UnspecifiedReadWriteMode {
		if node.ReadWriteMode != UnspecifiedReadWriteMode {
			return errReadModeSpecifiedMultipleTimes
		}
		node.ReadWriteMode = other.ReadWriteMode
	}
	if node.ReadWriteMode != UnspecifiedReadWriteMode &&
		node.ReadWriteMode != ReadOnly &&
		node.AsOf.Expr != nil {
		return ErrAsOfSpecifiedWithReadWrite
	}
	if other.Deferrable != UnspecifiedDeferrableMode {
		if node.Deferrable != UnspecifiedDeferrableMode {
			return errDeferrableSpecifiedMultipleTimes
		}
		node.Deferrable = other.Deferrable
	}
	return nil
}

// BeginTransaction represents a BEGIN statement
type BeginTransaction struct {
	// FormatWithStart says whether this statement must be formatted with
	// "START" rather than "BEGIN". This is needed if this statement is in a
	// BEGIN ATOMIC block of a procedure or function.
	FormatWithStart bool
	Modes           TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *BeginTransaction) Format(ctx *FmtCtx) {
	if node.FormatWithStart {
		ctx.WriteString("START TRANSACTION")
	} else {
		ctx.WriteString("BEGIN TRANSACTION")
	}
	ctx.FormatNode(&node.Modes)
}

// CommitTransaction represents a COMMIT statement.
type CommitTransaction struct{}

// Format implements the NodeFormatter interface.
func (node *CommitTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMIT TRANSACTION")
}

// RollbackTransaction represents a ROLLBACK statement.
type RollbackTransaction struct{}

// Format implements the NodeFormatter interface.
func (node *RollbackTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("ROLLBACK TRANSACTION")
}

// Savepoint represents a SAVEPOINT <name> statement.
type Savepoint struct {
	Name Name
}

// Format implements the NodeFormatter interface.
func (node *Savepoint) Format(ctx *FmtCtx) {
	ctx.WriteString("SAVEPOINT ")
	ctx.FormatNode(&node.Name)
}

// ReleaseSavepoint represents a RELEASE SAVEPOINT <name> statement.
type ReleaseSavepoint struct {
	Savepoint Name
}

// Format implements the NodeFormatter interface.
func (node *ReleaseSavepoint) Format(ctx *FmtCtx) {
	ctx.WriteString("RELEASE SAVEPOINT ")
	ctx.FormatNode(&node.Savepoint)
}

// RollbackToSavepoint represents a ROLLBACK TO SAVEPOINT <name> statement.
type RollbackToSavepoint struct {
	Savepoint Name
}

// Format implements the NodeFormatter interface.
func (node *RollbackToSavepoint) Format(ctx *FmtCtx) {
	ctx.WriteString("ROLLBACK TRANSACTION TO SAVEPOINT ")
	ctx.FormatNode(&node.Savepoint)
}

// PrepareTransaction represents a PREPARE TRANSACTION <transaction-id>
// statement, used for the first phase of a two-phase commit.
type PrepareTransaction struct {
	Transaction *StrVal
}

// Format implements the NodeFormatter interface.
func (node *PrepareTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("PREPARE TRANSACTION ")
	ctx.FormatNode(node.Transaction)
}

// CommitPrepared represents a COMMIT PREPARED <transaction-id> statement, used
// for the second phase of a two-phase commit.
type CommitPrepared struct {
	Transaction *StrVal
}

// Format implements the NodeFormatter interface.
func (node *CommitPrepared) Format(ctx *FmtCtx) {
	ctx.WriteString("COMMIT PREPARED ")
	ctx.FormatNode(node.Transaction)
}

// RollbackPrepared represents a ROLLBACK PREPARED <transaction-id> statement,
// used for the second phase of a two-phase rollback.
type RollbackPrepared struct {
	Transaction *StrVal
}

// Format implements the NodeFormatter interface.
func (node *RollbackPrepared) Format(ctx *FmtCtx) {
	ctx.WriteString("ROLLBACK PREPARED ")
	ctx.FormatNode(node.Transaction)
}
