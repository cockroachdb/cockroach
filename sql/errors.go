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
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util/encoding"
)

const (
	// PG error codes from:
	// http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html

	// CodeNonNullViolationError represents violation of a non-null constraint.
	CodeNonNullViolationError string = "23502"
	// CodeUniquenessConstraintViolationError represents violations of uniqueness
	// constraints.
	CodeUniquenessConstraintViolationError string = "23505"
	// CodeTransactionAbortedError signals that the user tried to execute a
	// statement in the context of a SQL txn that's already aborted.
	CodeTransactionAbortedError string = "25P02"
	// CodeInternalError represents all internal cockroach errors, plus acts
	// as a catch-all for random errors for which we haven't implemented the
	// appropriate error code.
	CodeInternalError string = "XX000"

	// Cockroach extensions:

	// CodeRetriableError signals to the user that the SQL txn entered the
	// RESTART_WAIT state and that a RESTART statement should be issued.
	CodeRetriableError string = "CR000"
	// CodeTransactionCommittedError signals that the SQL txn is in the
	// COMMIT_WAIT state and a COMMIT statement should be issued.
	CodeTransactionCommittedError string = "CR001"
)

// ErrorWithPGCode represents errors that carries an error code to the user.
// pgwire recognizes this interfaces and extracts the code.
type ErrorWithPGCode interface {
	error
	Code() string
}

var _ ErrorWithPGCode = &errNonNullViolation{}
var _ ErrorWithPGCode = &errUniquenessConstraintViolation{}
var _ ErrorWithPGCode = &errTransactionAborted{}
var _ ErrorWithPGCode = &errTransactionCommitted{}
var _ ErrorWithPGCode = &errRetry{}

const (
	txnAbortedMsg = "current transaction is aborted, commands ignored " +
		"until end of transaction block"
	txnCommittedMsg = "current transaction is committed, commands ignored " +
		"until end of transaction block"
	txnRetryMsgPrefix = "restart transaction:"
)

// errRetry means that the transaction can be retried.
type errRetry struct {
	msg string
}

func (e *errRetry) Error() string {
	return e.msg
}

func (*errRetry) Code() string {
	return CodeRetriableError
}

type errTransactionAborted struct {
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
	return CodeTransactionAbortedError
}

type errTransactionCommitted struct{}

func (e *errTransactionCommitted) Error() string {
	return txnCommittedMsg
}

func (*errTransactionCommitted) Code() string {
	return CodeTransactionCommittedError
}

type errNonNullViolation struct {
	column string
}

func (e *errNonNullViolation) Error() string {
	return fmt.Sprintf("column %s contains null values", e.column)
}

func (*errNonNullViolation) Code() string {
	return CodeNonNullViolationError
}

type errUniquenessConstraintViolation struct {
	index *sqlbase.IndexDescriptor
	vals  []parser.Datum
}

func (*errUniquenessConstraintViolation) Code() string {
	return CodeUniquenessConstraintViolationError
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

func isIntegrityConstraintError(err error) bool {
	switch err.(type) {
	case *errNonNullViolation, *errUniquenessConstraintViolation:
		return true
	default:
		return false
	}
}

func convertBatchError(tableDesc *sqlbase.TableDescriptor, b client.Batch, origPErr *roachpb.Error) error {
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

			return &errUniquenessConstraintViolation{index: index, vals: vals}
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
		return &errRetry{msg: txnRetryMsgPrefix + " " + err.Error()}
	}
	return err
}
