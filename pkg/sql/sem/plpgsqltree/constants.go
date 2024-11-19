// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

import "github.com/cockroachdb/errors"

// GetDiagnosticsKind represents the type of error diagnostic
// item in stmt_getdiag.
type GetDiagnosticsKind int

const (
	// GetDiagnosticsRowCount returns the number of rows processed by the recent
	// SQL command.
	GetDiagnosticsRowCount GetDiagnosticsKind = iota
	// GetDiagnosticsContext returns text describing the current call stack.
	GetDiagnosticsContext
	// GetDiagnosticsErrorContext returns text describing the exception's
	// callstack.
	GetDiagnosticsErrorContext
	// GetDiagnosticsErrorDetail returns the exceptions detail message.
	GetDiagnosticsErrorDetail
	// GetDiagnosticsErrorHint returns the exceptions hint message.
	GetDiagnosticsErrorHint
	// GetDiagnosticsReturnedSQLState returns the SQLSTATE error code related to
	// the exception.
	GetDiagnosticsReturnedSQLState
	// GetDiagnosticsColumnName returns the column name related to the exception.
	GetDiagnosticsColumnName
	// GetDiagnosticsConstraintName returns the constraint name related to
	// the exception.
	GetDiagnosticsConstraintName
	// GetDiagnosticsDatatypeName returns the data type name related to the
	// exception.
	GetDiagnosticsDatatypeName
	// GetDiagnosticsMessageText returns the exceptions primary message.
	GetDiagnosticsMessageText
	// GetDiagnosticsTableName returns the name of the table related to the
	// exception.
	GetDiagnosticsTableName
	// GetDiagnosticsSchemaName returns the name of the schema related to the
	// exception.
	GetDiagnosticsSchemaName
)

// String implements the fmt.Stringer interface.
func (k GetDiagnosticsKind) String() string {
	switch k {
	case GetDiagnosticsRowCount:
		return "ROW_COUNT"
	case GetDiagnosticsContext:
		return "PG_CONTEXT"
	case GetDiagnosticsErrorContext:
		return "PG_EXCEPTION_CONTEXT"
	case GetDiagnosticsErrorDetail:
		return "PG_EXCEPTION_DETAIL"
	case GetDiagnosticsErrorHint:
		return "PG_EXCEPTION_HINT"
	case GetDiagnosticsReturnedSQLState:
		return "RETURNED_SQLSTATE"
	case GetDiagnosticsColumnName:
		return "COLUMN_NAME"
	case GetDiagnosticsConstraintName:
		return "CONSTRAINT_NAME"
	case GetDiagnosticsDatatypeName:
		return "PG_DATATYPE_NAME"
	case GetDiagnosticsMessageText:
		return "MESSAGE_TEXT"
	case GetDiagnosticsTableName:
		return "TABLE_NAME"
	case GetDiagnosticsSchemaName:
		return "SCHEMA_NAME"
	}
	panic(errors.AssertionFailedf("unknown diagnostics kind"))
}
