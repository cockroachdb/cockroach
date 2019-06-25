// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// IsolationLevel holds the isolation level for a transaction.
type IsolationLevel int

// IsolationLevel values
const (
	UnspecifiedIsolation IsolationLevel = iota
	SerializableIsolation
)

var isolationLevelNames = [...]string{
	UnspecifiedIsolation:  "UNSPECIFIED",
	SerializableIsolation: "SERIALIZABLE",
}

// IsolationLevelMap is a map from string isolation level name to isolation
// level, in the lowercase format that set isolation_level supports.
var IsolationLevelMap = map[string]IsolationLevel{
	"serializable": SerializableIsolation,
}

func (i IsolationLevel) String() string {
	if i < 0 || i > IsolationLevel(len(isolationLevelNames)-1) {
		return fmt.Sprintf("IsolationLevel(%d)", i)
	}
	return isolationLevelNames[i]
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

// TransactionModes holds the transaction modes for a transaction.
type TransactionModes struct {
	Isolation     IsolationLevel
	UserPriority  UserPriority
	ReadWriteMode ReadWriteMode
	AsOf          AsOfClause
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
	}
	if node.AsOf.Expr != nil {
		ctx.WriteString(sep)
		ctx.WriteString(" ")
		node.AsOf.Format(ctx)
	}
}

var (
	errIsolationLevelSpecifiedMultipleTimes = pgerror.New(pgcode.Syntax, "isolation level specified multiple times")
	errUserPrioritySpecifiedMultipleTimes   = pgerror.New(pgcode.Syntax, "user priority specified multiple times")
	errReadModeSpecifiedMultipleTimes       = pgerror.New(pgcode.Syntax, "read mode specified multiple times")
	errAsOfSpecifiedMultipleTimes           = pgerror.New(pgcode.Syntax, "AS OF SYSTEM TIME specified multiple times")

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
	return nil
}

// BeginTransaction represents a BEGIN statement
type BeginTransaction struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *BeginTransaction) Format(ctx *FmtCtx) {
	ctx.WriteString("BEGIN TRANSACTION")
	node.Modes.Format(ctx)
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
	node.Name.Format(ctx)
}

// ReleaseSavepoint represents a RELEASE SAVEPOINT <name> statement.
type ReleaseSavepoint struct {
	Savepoint Name
}

// Format implements the NodeFormatter interface.
func (node *ReleaseSavepoint) Format(ctx *FmtCtx) {
	ctx.WriteString("RELEASE SAVEPOINT ")
	node.Savepoint.Format(ctx)
}

// RollbackToSavepoint represents a ROLLBACK TO SAVEPOINT <name> statement.
type RollbackToSavepoint struct {
	Savepoint Name
}

// Format implements the NodeFormatter interface.
func (node *RollbackToSavepoint) Format(ctx *FmtCtx) {
	ctx.WriteString("ROLLBACK TRANSACTION TO SAVEPOINT ")
	node.Savepoint.Format(ctx)
}
