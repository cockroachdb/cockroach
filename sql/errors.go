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
	"github.com/cockroachdb/cockroach/util/encoding"
)

const (
	// PG error codes from:
	// http://www.postgresql.org/docs/9.5/static/errcodes-appendix.html

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

// errorWithPGCode represents errors that carries an error code to the user.
type errorWithPGCode interface {
	error
	Code() string
}

var _ errorWithPGCode = &errUniquenessConstraintViolation{}
var _ errorWithPGCode = &errTransactionAborted{}
var _ errorWithPGCode = &errTransactionCommitted{}
var _ errorWithPGCode = &errRetry{}

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

type errUniquenessConstraintViolation struct {
	index *IndexDescriptor
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

func convertBatchError(tableDesc *TableDescriptor, b client.Batch, origPErr *roachpb.Error) *roachpb.Error {
	if origPErr.Index == nil {
		return origPErr
	}
	index := origPErr.Index.Index
	if index >= int32(len(b.Results)) {
		panic(fmt.Sprintf("index %d outside of results: %+v", index, b.Results))
	}
	result := b.Results[index]
	if _, ok := origPErr.GetDetail().(*roachpb.ConditionFailedError); ok {
		for _, row := range result.Rows {
			indexID, key, err := decodeIndexKeyPrefix(tableDesc, row.Key)
			if err != nil {
				return roachpb.NewError(err)
			}
			index, err := tableDesc.FindIndexByID(indexID)
			if err != nil {
				return roachpb.NewError(err)
			}
			valTypes, err := makeKeyVals(tableDesc, index.ColumnIDs)
			if err != nil {
				return roachpb.NewError(err)
			}
			dirs := make([]encoding.Direction, 0, len(index.ColumnIDs))
			for _, dir := range index.ColumnDirections {
				convertedDir, err := dir.toEncodingDirection()
				if err != nil {
					return roachpb.NewError(err)
				}
				dirs = append(dirs, convertedDir)
			}
			vals := make([]parser.Datum, len(valTypes))
			if _, err := decodeKeyVals(valTypes, vals, dirs, key); err != nil {
				return roachpb.NewError(err)
			}

			return sqlErrToPErr(&errUniquenessConstraintViolation{index: index, vals: vals})
		}
	}
	return origPErr
}

// convertToErrWithPGCode recognizes pErrs that should have SQL error codes to be
// reported to the client and converts pErr to them. If this doesn't apply, pErr
// is returned.
// Note that this returns a new proto, and no fields from the original one are
// copied. So only use it in contexts where the only thing that matters in the
// response is the error detail.
// TODO(andrei): convertBatchError() above seems to serve similar purposes, but
// it's called from more specialized contexts. Consider unifying the two.
func convertToErrWithPGCode(pErr *roachpb.Error) *roachpb.Error {
	if pErr == nil {
		return nil
	}
	if pErr.TransactionRestart != roachpb.TransactionRestart_NONE {
		return sqlErrToPErr(&errRetry{msg: txnRetryMsgPrefix + " " + pErr.Message})
	}
	return pErr
}

// sqlErrToPErr takes a sqlError and produces a pErr with a suitable detail.
func sqlErrToPErr(e errorWithPGCode) *roachpb.Error {
	var detail roachpb.ErrorWithPGCode
	detail.ErrorCode = e.Code()
	detail.Message = e.Error()
	return roachpb.NewError(&detail)
}
