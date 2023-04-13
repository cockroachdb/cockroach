// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plpgsqltree

import "github.com/cockroachdb/errors"

// PLpgSQLRaiseOptionType represents the severity of the error in
// a raise statement.
type PLpgSQLRaiseOptionType int

// PLpgSQLGetDiagKind represents the type of error diagnostic
// item in stmt_getdiag.
type PLpgSQLGetDiagKind int

// PlpgsqlGetdiagRowCount is an option for diagnostic items that can be
// present in stmt_getdiag.
const (
	// PlpgsqlGetdiagRowCount returns the number of rows processed by the recent
	// SQL command.
	PlpgsqlGetdiagRowCount PLpgSQLGetDiagKind = iota
	// PlpgsqlGetdiagContext returns text describing the current call stack.
	PlpgsqlGetdiagContext
	// PlpgsqlGetdiagErrorContext returns text describing the exception's callstack.
	PlpgsqlGetdiagErrorContext
	// PlpgsqlGetdiagErrorDetail returns the exceptions detail message.
	PlpgsqlGetdiagErrorDetail
	// PlpgsqlGetdiagErrorHint returns the exceptions hint message.
	PlpgsqlGetdiagErrorHint
	// PlpgsqlGetdiagReturnedSqlstate returns the SQLSTATE error code related to
	// the exception.
	PlpgsqlGetdiagReturnedSqlstate
	// PlpgsqlGetdiagColumnName returns the column name related to the exception.
	PlpgsqlGetdiagColumnName
	// PlpgsqlGetdiagConstraintName returns the constraint name related to
	// the exception.
	PlpgsqlGetdiagConstraintName
	// PlpgsqlGetdiagDatatypeName returns the data type name related to
	// the exception.
	PlpgsqlGetdiagDatatypeName
	// PlpgsqlGetdiagMessageText returns the exceptions primary message.
	PlpgsqlGetdiagMessageText
	// PlpgsqlGetdiagTableName returns the name of the table related to
	// the exception.
	PlpgsqlGetdiagTableName
	// PlpgsqlGetdiagSchemaName returns the name of the schema related to
	//	// the exception.
	PlpgsqlGetdiagSchemaName
)

// String implements the fmt.Stringer interface.
func (k PLpgSQLGetDiagKind) String() string {
	switch k {
	case PlpgsqlGetdiagRowCount:
		return "ROW_COUNT"
	case PlpgsqlGetdiagContext:
		return "PG_CONTEXT"
	case PlpgsqlGetdiagErrorContext:
		return "PG_EXCEPTION_CONTEXT"
	case PlpgsqlGetdiagErrorDetail:
		return "PG_EXCEPTION_DETAIL"
	case PlpgsqlGetdiagErrorHint:
		return "PG_EXCEPTION_HINT"
	case PlpgsqlGetdiagReturnedSqlstate:
		return "RETURNED_SQLSTATE"
	case PlpgsqlGetdiagColumnName:
		return "COLUMN_NAME"
	case PlpgsqlGetdiagConstraintName:
		return "CONSTRAINT_NAME"
	case PlpgsqlGetdiagDatatypeName:
		return "PG_DATATYPE_NAME"
	case PlpgsqlGetdiagMessageText:
		return "MESSAGE_TEXT"
	case PlpgsqlGetdiagTableName:
		return "TABLE_NAME"
	case PlpgsqlGetdiagSchemaName:
		return "SCHEMA_NAME"
	}
	panic(errors.AssertionFailedf("no Annotations unknown getDiagnistics kind"))

}

// PLpgSQLFetchDirection represents the direction clause passed into a
// fetch statement.
type PLpgSQLFetchDirection int

// PLpgSQLCursorOpt represents a cursor option, which describes
// how a cursor will behave.
type PLpgSQLCursorOpt uint32

const (
	// PLpgSQLCursorOptNone
	PLpgSQLCursorOptNone PLpgSQLCursorOpt = iota
	// PLpgSQLCursorOptBinary describes cursors that return data in binary form.
	PLpgSQLCursorOptBinary
	// PLpgSQLCursorOptScroll describes cursors that can retrieve rows in
	// non-sequential fashion.
	PLpgSQLCursorOptScroll
	// PLpgSQLCursorOptNoScroll describes cursors that can not retrieve rows in
	// non-sequential fashion.
	PLpgSQLCursorOptNoScroll
	// PLpgSQLCursorOptInsensitive describes cursors that can't see changes to
	// done to data in same txn.
	PLpgSQLCursorOptInsensitive
	// PLpgSQLCursorOptAsensitive describes cursors that may be able to see
	// changes to done to data in same txn.
	PLpgSQLCursorOptAsensitive
	// PLpgSQLCursorOptHold describes cursors that can be used after a txn that it
	// was created in commits.
	PLpgSQLCursorOptHold
	// PLpgSQLCursorOptFastPlan describes cursors that can not be used after a txn
	// that it was created in commits.
	PLpgSQLCursorOptFastPlan
	// PLpgSQLCursorOptGenericPlan describes cursors that uses a generic plan.
	PLpgSQLCursorOptGenericPlan
	// PLpgSQLCursorOptCustomPlan describes cursors that uses a custom plan.
	PLpgSQLCursorOptCustomPlan
	// PLpgSQLCursorOptParallelOK describes cursors that allows parallel queries.
	PLpgSQLCursorOptParallelOK
)

// String implements the fmt.Stringer interface.
func (o PLpgSQLCursorOpt) String() string {
	switch o {
	case PLpgSQLCursorOptNoScroll:
		return "NO SCROLL"
	case PLpgSQLCursorOptScroll:
		return "SCROLL"
	case PLpgSQLCursorOptFastPlan:
		return ""
	// TODO(jane): implement string representation for other opts.
	default:
		return "NOT_IMPLEMENTED_OPT"
	}
}

// Mask returns the bitmask for a given cursor option.
func (o PLpgSQLCursorOpt) Mask() uint32 {
	return 1 << o
}

// IsSetIn returns true if this cursor option is set in the supplied bitfield.
func (o PLpgSQLCursorOpt) IsSetIn(bits uint32) bool {
	return bits&o.Mask() != 0
}

type plpgSQLCursorOptList []PLpgSQLCursorOpt

// ToBitField returns the bitfield representation of a list of cursor options.
func (ol plpgSQLCursorOptList) ToBitField() uint32 {
	var ret uint32
	for _, o := range ol {
		ret |= o.Mask()
	}
	return ret
}

// OptListFromBitField returns a list of cursor option to be printed.
func OptListFromBitField(m uint32) plpgSQLCursorOptList {
	ret := plpgSQLCursorOptList{}
	opts := []PLpgSQLCursorOpt{
		PLpgSQLCursorOptBinary,
		PLpgSQLCursorOptScroll,
		PLpgSQLCursorOptNoScroll,
		PLpgSQLCursorOptInsensitive,
		PLpgSQLCursorOptAsensitive,
		PLpgSQLCursorOptHold,
		PLpgSQLCursorOptFastPlan,
		PLpgSQLCursorOptGenericPlan,
		PLpgSQLCursorOptCustomPlan,
		PLpgSQLCursorOptParallelOK,
	}
	for _, opt := range opts {
		if opt.IsSetIn(m) {
			ret = append(ret, opt)
		}
	}
	return ret
}
