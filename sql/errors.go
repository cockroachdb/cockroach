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

package sql

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/encoding"
)

// Cockroach error extensions:
const (
	// CodeRetriableError signals to the user that the SQL txn entered the
	// RESTART_WAIT state and that a RESTART statement should be issued.
	CodeRetriableError string = "CR000"
	// CodeTransactionCommittedError signals that the SQL txn is in the
	// COMMIT_WAIT state and a COMMIT statement should be issued.
	CodeTransactionCommittedError string = "CR001"
)

// SrcCtx contains contextual information about the source of an error.
type SrcCtx struct {
	File     string
	Line     int
	Function string
}

func makeSrcCtx(depth int) SrcCtx {
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

var _ ErrorWithPGCode = &errNonNullViolation{}
var _ ErrorWithPGCode = &errUniquenessConstraintViolation{}
var _ ErrorWithPGCode = &errTransactionAborted{}
var _ ErrorWithPGCode = &errTransactionCommitted{}
var _ ErrorWithPGCode = &errUndefinedDatabase{}
var _ ErrorWithPGCode = &errUndefinedTable{}
var _ ErrorWithPGCode = &errRetry{}

const (
	txnAbortedMsg = "current transaction is aborted, commands ignored " +
		"until end of transaction block"
	txnCommittedMsg = "current transaction is committed, commands ignored " +
		"until end of transaction block"
	txnRetryMsgPrefix = "restart transaction:"
)

func newRetryError(msg string) error {
	return &errRetry{ctx: makeSrcCtx(1), msg: msg}
}

// errRetry means that the transaction can be retried.
type errRetry struct {
	ctx SrcCtx
	msg string
}

func (e *errRetry) Error() string {
	return e.msg
}

func (*errRetry) Code() string {
	return CodeRetriableError
}

func (e *errRetry) SrcContext() SrcCtx {
	return e.ctx
}

func newTransactionAbortedError(customMsg string) error {
	return &errTransactionAborted{ctx: makeSrcCtx(1), CustomMsg: customMsg}
}

type errTransactionAborted struct {
	ctx       SrcCtx
	CustomMsg string
}

func (e *errTransactionAborted) Error() string {
	msg := txnAbortedMsg
	if e.CustomMsg != "" {
		msg += "; " + e.CustomMsg
	}
	return msg
}

func (*errTransactionAborted) Code() string {
	return pgerror.CodeInFailedSQLTransactionError
}

func (e *errTransactionAborted) SrcContext() SrcCtx {
	return e.ctx
}

func newTransactionCommittedError() error {
	return &errTransactionCommitted{ctx: makeSrcCtx(1)}
}

type errTransactionCommitted struct {
	ctx SrcCtx
}

func (*errTransactionCommitted) Error() string {
	return txnCommittedMsg
}

func (*errTransactionCommitted) Code() string {
	return CodeTransactionCommittedError
}

func (e *errTransactionCommitted) SrcContext() SrcCtx {
	return e.ctx
}

func newNonNullViolationError(columnName string) error {
	return &errNonNullViolation{ctx: makeSrcCtx(1), columnName: columnName}
}

type errNonNullViolation struct {
	ctx        SrcCtx
	columnName string
}

func (e *errNonNullViolation) Error() string {
	return fmt.Sprintf("null value in column %q violates not-null constraint", e.columnName)
}

func (*errNonNullViolation) Code() string {
	return pgerror.CodeNotNullViolationError
}

func (e *errNonNullViolation) SrcContext() SrcCtx {
	return e.ctx
}

func newUniquenessConstraintViolationError(
	index *sqlbase.IndexDescriptor, vals []parser.Datum,
) error {
	return &errUniquenessConstraintViolation{
		ctx:   makeSrcCtx(1),
		index: index,
		vals:  vals,
	}
}

type errUniquenessConstraintViolation struct {
	ctx   SrcCtx
	index *sqlbase.IndexDescriptor
	vals  []parser.Datum
}

func (*errUniquenessConstraintViolation) Code() string {
	return pgerror.CodeUniqueViolationError
}

func (e *errUniquenessConstraintViolation) Error() string {
	valStrs := make([]string, 0, len(e.vals))
	for _, val := range e.vals {
		valStrs = append(valStrs, val.String())
	}

	return fmt.Sprintf("duplicate key value (%s)=(%s) violates unique constraint %q",
		strings.Join(e.index.ColumnNames, ","),
		strings.Join(valStrs, ","),
		e.index.Name)
}

func (e *errUniquenessConstraintViolation) SrcContext() SrcCtx {
	return e.ctx
}

func newUndefinedTableError(name string) error {
	return &errUndefinedTable{ctx: makeSrcCtx(1), name: name}
}

type errUndefinedTable struct {
	ctx  SrcCtx
	name string
}

func (e *errUndefinedTable) Error() string {
	return fmt.Sprintf("table %q does not exist", e.name)
}

func (*errUndefinedTable) Code() string {
	return pgerror.CodeUndefinedTableError
}

func (e *errUndefinedTable) SrcContext() SrcCtx {
	return e.ctx
}

func newUndefinedDatabaseError(name string) error {
	return &errUndefinedDatabase{ctx: makeSrcCtx(1), name: name}
}

type errUndefinedDatabase struct {
	ctx  SrcCtx
	name string
}

func (e *errUndefinedDatabase) Error() string {
	return fmt.Sprintf("database %q does not exist", e.name)
}

func (*errUndefinedDatabase) Code() string {
	// Postgres will return an UndefinedTable error on queries that go to a "relation"
	// that does not exist (a query to a non-existent table or database), but will
	// return an InvalidCatalogName error when connecting to a database that does
	// not exist. We've chosen to return this code for all cases where the error cause
	// is a missing database.
	return pgerror.CodeInvalidCatalogNameError
}

func (e *errUndefinedDatabase) SrcContext() SrcCtx {
	return e.ctx
}

func isIntegrityConstraintError(err error) bool {
	switch err.(type) {
	case *errNonNullViolation, *errUniquenessConstraintViolation:
		return true
	default:
		return false
	}
}

func convertBatchError(tableDesc *sqlbase.TableDescriptor, b *client.Batch) error {
	origPErr := b.MustPErr()
	if origPErr.Index == nil {
		return origPErr.GoError()
	}
	index := origPErr.Index.Index
	if index >= int32(len(b.Results)) {
		panic(fmt.Sprintf("index %d outside of results: %+v", index, b.Results))
	}
	result := b.Results[index]
	var alloc sqlbase.DatumAlloc
	if _, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok {
		for _, row := range result.Rows {
			indexID, key, err := sqlbase.DecodeIndexKeyPrefix(tableDesc, row.Key)
			if err != nil {
				return err
			}
			index, err := tableDesc.FindIndexByID(indexID)
			if err != nil {
				return err
			}
			valTypes, err := sqlbase.MakeKeyVals(tableDesc, index.ColumnIDs)
			if err != nil {
				return err
			}
			dirs := make([]encoding.Direction, 0, len(index.ColumnIDs))
			for _, dir := range index.ColumnDirections {
				convertedDir, err := dir.ToEncodingDirection()
				if err != nil {
					return err
				}
				dirs = append(dirs, convertedDir)
			}
			vals := make([]parser.Datum, len(valTypes))
			if _, err := sqlbase.DecodeKeyVals(&alloc, valTypes, vals, dirs, key); err != nil {
				return err
			}

			return newUniquenessConstraintViolationError(index, vals)
		}
	}
	return origPErr.GoError()
}

// convertToErrWithPGCode recognizes errs that should have SQL error codes to be
// reported to the client and converts err to them. If this doesn't apply, err
// is returned.
// Note that this returns a new proto, and no fields from the original one are
// copied. So only use it in contexts where the only thing that matters in the
// response is the error detail.
// TODO(andrei): convertBatchError() above seems to serve similar purposes, but
// it's called from more specialized contexts. Consider unifying the two.
func convertToErrWithPGCode(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*roachpb.RetryableTxnError); ok {
		return newRetryError(fmt.Sprintf("%s %v", txnRetryMsgPrefix, err))
	}
	return err
}
