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

import (
	"github.com/cockroachdb/errors"
	"math"
)

type DatumType int

type PLpgSQLStatementType int

type PLpgSQLRaiseOptionType int

type PLpgSQLGetDiagKind int

const (
	PlpgsqlGetdiagRowCount PLpgSQLGetDiagKind = iota
	PlpgsqlGetdiagContext
	PlpgsqlGetdiagErrorContext
	PlpgsqlGetdiagErrorDetail
	PlpgsqlGetdiagErrorHint
	PlpgsqlGetdiagReturnedSqlstate
	PlpgsqlGetdiagColumnName
	PlpgsqlGetdiagConstraintName
	PlpgsqlGetdiagDatatypeName
	PlpgsqlGetdiagMessageText
	PlpgsqlGetdiagTableName
	PlpgsqlGetdiagSchemaName
)

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

type PLpgSQLFetchDirection int

const (
	PLpgSQLFetchForward PLpgSQLFetchDirection = iota
	PLpgSQLFetchBackward
	PLpgSQLFetchAbsolute
	PLpgSQLFetchRelative
)

func (k PLpgSQLFetchDirection) String() string {
	switch k {
	case PLpgSQLFetchForward:
		return "FORWARD"
	case PLpgSQLFetchBackward:
		return "BACKWARD"
	case PLpgSQLFetchAbsolute:
		return "ABSOLUTE"
	case PLpgSQLFetchRelative:
		return "RELATIVE"
	default:
		panic(errors.AssertionFailedf("not known fetch direction"))
	}
}

// TODO(janexing): better naming and comments.
type PLpgSQLFetchHowMany int

const (
	PLpgSQLFetchAll  PLpgSQLFetchHowMany = math.MaxInt64
	PlpgSQLFetchLast PLpgSQLFetchHowMany = -1
)

type PLpgSQLPromiseType int

type PLpgSQLCursorOpt uint32

const (
	PLpgSQLCursorOptNone PLpgSQLCursorOpt = iota
	PLpgSQLCursorOptBinary
	PLpgSQLCursorOptScroll
	PLpgSQLCursorOptNoScroll
	PLpgSQLCursorOptInsensitive
	PLpgSQLCursorOptAsensitive
	PLpgSQLCursorOptHold
	PLpgSQLCursorOptFastPlan
	PLpgSQLCursorOptGenericPlan
	PLpgSQLCursorOptCustomPlan
	PLpgSQLCursorOptParallelOK
)

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

type PLpgSQLCursorOptList []PLpgSQLCursorOpt

// ToBitField returns the bitfield representation of a list of cursor options.
func (ol PLpgSQLCursorOptList) ToBitField() uint32 {
	var ret uint32
	for _, o := range ol {
		ret |= o.Mask()
	}
	return ret
}

func OptListFromBitField(m uint32) PLpgSQLCursorOptList {
	ret := PLpgSQLCursorOptList{}
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
