// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)
// Author: Andrei Matei (andreimatei1@gmail.com)
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sqlbase

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/util/caller"
)

// SrcCtx contains contextual information about the source of an error.
type SrcCtx struct {
	File     string
	Line     int
	Function string
}

// MakeSrcCtx creates a SrcCtx value with contextual information about the
// caller at the requested depth.
func MakeSrcCtx(depth int) SrcCtx {
	f, l, fun := caller.Lookup(depth + 1)
	return SrcCtx{File: f, Line: l, Function: fun}
}

// ErrorWithPGCode represents errors that carries an error code to the user.
// pgwire recognizes this interfaces and extracts the code.
type ErrorWithPGCode interface {
	error
	Code() string
	SrcContext() SrcCtx
}

var _ ErrorWithPGCode = &ErrNonNullViolation{}
var _ ErrorWithPGCode = &ErrUniquenessConstraintViolation{}
var _ ErrorWithPGCode = &ErrTransactionAborted{}
var _ ErrorWithPGCode = &ErrTransactionCommitted{}
var _ ErrorWithPGCode = &ErrUndefinedDatabase{}
var _ ErrorWithPGCode = &ErrUndefinedTable{}
var _ ErrorWithPGCode = &ErrRetry{}

const (
	txnAbortedMsg = "current transaction is aborted, commands ignored " +
		"until end of transaction block"
	txnCommittedMsg = "current transaction is committed, commands ignored " +
		"until end of transaction block"
	txnRetryMsgPrefix = "restart transaction:"
)

// NewRetryError creates a ErrRetry.
func NewRetryError(cause error) error {
	return &ErrRetry{ctx: MakeSrcCtx(1), msg: fmt.Sprintf("%s %v", txnRetryMsgPrefix, cause)}
}

// ErrRetry means that the transaction can be retried. It signals to the user
// that the SQL txn entered the RESTART_WAIT state after a serialization error,
// and that a ROLLBACK TO SAVEPOINT COCKROACH_RESTART statement should be issued.
type ErrRetry struct {
	ctx SrcCtx
	msg string
}

func (e *ErrRetry) Error() string {
	return e.msg
}

// Code implements the ErrorWithPGCode interface.
func (*ErrRetry) Code() string {
	return pgerror.CodeSerializationFailureError
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrRetry) SrcContext() SrcCtx {
	return e.ctx
}

// NewTransactionAbortedError creates a new ErrTransactionAborted.
func NewTransactionAbortedError(customMsg string) error {
	return &ErrTransactionAborted{ctx: MakeSrcCtx(1), CustomMsg: customMsg}
}

// ErrTransactionAborted represents an error for trying to run a command in the
// context of transaction that's already aborted.
type ErrTransactionAborted struct {
	ctx       SrcCtx
	CustomMsg string
}

func (e *ErrTransactionAborted) Error() string {
	msg := txnAbortedMsg
	if e.CustomMsg != "" {
		msg += "; " + e.CustomMsg
	}
	return msg
}

// Code implements the ErrorWithPGCode interface.
func (*ErrTransactionAborted) Code() string {
	return pgerror.CodeInFailedSQLTransactionError
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrTransactionAborted) SrcContext() SrcCtx {
	return e.ctx
}

// NewTransactionCommittedError creates a new ErrTransactionCommitted.
func NewTransactionCommittedError() error {
	return &ErrTransactionCommitted{ctx: MakeSrcCtx(1)}
}

// ErrTransactionCommitted signals that the SQL txn is in the COMMIT_WAIT state
// and that only a COMMIT statement will be accepted.
type ErrTransactionCommitted struct {
	ctx SrcCtx
}

func (*ErrTransactionCommitted) Error() string {
	return txnCommittedMsg
}

// Code implements the ErrorWithPGCode interface.
func (*ErrTransactionCommitted) Code() string {
	return pgerror.CodeInvalidTransactionStateError
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrTransactionCommitted) SrcContext() SrcCtx {
	return e.ctx
}

// NewNonNullViolationError creates a new ErrNonNullViolation.
func NewNonNullViolationError(columnName string) error {
	return &ErrNonNullViolation{ctx: MakeSrcCtx(1), columnName: columnName}
}

// ErrNonNullViolation represents a violation of a non-NULL constraint.
type ErrNonNullViolation struct {
	ctx        SrcCtx
	columnName string
}

func (e *ErrNonNullViolation) Error() string {
	return fmt.Sprintf("null value in column %q violates not-null constraint", e.columnName)
}

// Code implements the ErrorWithPGCode interface.
func (*ErrNonNullViolation) Code() string {
	return pgerror.CodeNotNullViolationError
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrNonNullViolation) SrcContext() SrcCtx {
	return e.ctx
}

// NewUniquenessConstraintViolationError creates a new
// ErrUniquenessConstrainViolation.
func NewUniquenessConstraintViolationError(
	index *IndexDescriptor, vals []parser.Datum,
) error {
	return &ErrUniquenessConstraintViolation{
		ctx:   MakeSrcCtx(1),
		index: index,
		vals:  vals,
	}
}

// ErrUniquenessConstraintViolation represents a violation of a UNIQUE constraint.
type ErrUniquenessConstraintViolation struct {
	ctx   SrcCtx
	index *IndexDescriptor
	vals  []parser.Datum
}

// Code implements the ErrorWithPGCode interface.
func (*ErrUniquenessConstraintViolation) Code() string {
	return pgerror.CodeUniqueViolationError
}

func (e *ErrUniquenessConstraintViolation) Error() string {
	valStrs := make([]string, 0, len(e.vals))
	for _, val := range e.vals {
		valStrs = append(valStrs, val.String())
	}

	return fmt.Sprintf("duplicate key value (%s)=(%s) violates unique constraint %q",
		strings.Join(e.index.ColumnNames, ","),
		strings.Join(valStrs, ","),
		e.index.Name)
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrUniquenessConstraintViolation) SrcContext() SrcCtx {
	return e.ctx
}

// NewUndefinedTableError creates a new ErrUndefinedTable.
func NewUndefinedTableError(name string) error {
	return &ErrUndefinedTable{ctx: MakeSrcCtx(1), name: name}
}

// ErrUndefinedTable represents a missing database table.
type ErrUndefinedTable struct {
	ctx  SrcCtx
	name string
}

func (e *ErrUndefinedTable) Error() string {
	return fmt.Sprintf("table %q does not exist", e.name)
}

// Code implements the ErrorWithPGCode interface.
func (*ErrUndefinedTable) Code() string {
	return pgerror.CodeUndefinedTableError
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrUndefinedTable) SrcContext() SrcCtx {
	return e.ctx
}

// NewUndefinedDatabaseError creates a new ErrUndefinedDatabase.
func NewUndefinedDatabaseError(name string) error {
	return &ErrUndefinedDatabase{ctx: MakeSrcCtx(1), name: name}
}

// ErrUndefinedDatabase represents a missing database error.
type ErrUndefinedDatabase struct {
	ctx  SrcCtx
	name string
}

func (e *ErrUndefinedDatabase) Error() string {
	return fmt.Sprintf("database %q does not exist", e.name)
}

// Code implements the ErrorWithPGCode interface.
func (*ErrUndefinedDatabase) Code() string {
	// Postgres will return an UndefinedTable error on queries that go to a "relation"
	// that does not exist (a query to a non-existent table or database), but will
	// return an InvalidCatalogName error when connecting to a database that does
	// not exist. We've chosen to return this code for all cases where the error cause
	// is a missing database.
	return pgerror.CodeInvalidCatalogNameError
}

// SrcContext implements the ErrorWithPGCode interface.
func (e *ErrUndefinedDatabase) SrcContext() SrcCtx {
	return e.ctx
}

// IsIntegrityConstraintError returns true if the error is some kind of SQL
// constraint violation.
func IsIntegrityConstraintError(err error) bool {
	switch err.(type) {
	case *ErrNonNullViolation, *ErrUniquenessConstraintViolation:
		return true
	default:
		return false
	}
}
