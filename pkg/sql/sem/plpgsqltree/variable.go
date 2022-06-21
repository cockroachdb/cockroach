// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type PLpgSQLDatum interface {
	plpgsqldatum()
}

type PLpgSQLDatumImpl struct {
	DatumType DatumType
	// TODO (Chengxiong) this is probably not useful for us since we don't need to
	// track index but can use pointer instead?
	DatumNo int
}

func (d *PLpgSQLDatumImpl) plpgsqldatum() {}

type PLpgSQLVariable interface {
	PLpgSQLDatum
	pgpgsqlvariable()
}

// TODO we need a mpping from name/id/index to variable
type PLpgSQLVariableImpl struct {
	PLpgSQLDatumImpl

	// TODO (Chengxiong) the id of the variable in the scope it's defined
	// I added these :)
	ID    int
	Scope *VariableScope

	Name string
	// TODO (Chengxiong) figure out how this is set.
	LineNo       int
	IsConstant   bool
	NotNull      bool
	DefaultValue PLpgSQLExpr
}

func (v *PLpgSQLVariableImpl) pgpgsqlvariable() {}

type PLpgSQLScalarVar struct {
	PLpgSQLVariableImpl
	DataType *types.T

	/*
	 * Variables declared as CURSOR FOR <query> are mostly like ordinary
	 * scalar variables of type refcursor, but they have these additional
	 * properties:
	 */
	CursorExplicitExpr   PLpgSQLExpr
	CursorExplicitArgRow int
	CursorOptions        int

	/* Fields below here can change at runtime */

	Value     tree.Datum
	IsNull    bool
	FreeValue bool

	/*
	 * The promise field records which "promised" value to assign if the
	 * promise must be honored.  If it's a normal variable, or the promise has
	 * been fulfilled, this is PLPGSQL_PROMISE_NONE.
	 */
	promise PLpgSQLPromiseType
}

type PLpgSQLRowVar struct {
	PLpgSQLVariableImpl
	FieldNames []string
	Vars       []PLpgSQLScalarVar

	/*
	 * rowtupdesc is only set up if we might need to convert the row into a
	 * composite datum, which currently only happens for OUT parameters.
	 * Otherwise it is NULL.
	 */
	// TODO (Chengxiong) figure this out
	// TupleDesc	rowtupdesc;
}

// TODO We don't need to worry about this yet since we don't support user
// defined type yet.
type PLpgSQLRecordVar struct {
	PLpgSQLVariableImpl
	DataType *types.T
	Fields   []PLpgSQLRecordField

	// TODO expanded record header?
}

type PLpgSQLRecordField struct {
	PLpgSQLDatumImpl

	FieldName    string
	ParentRecord *PLpgSQLRecordVar
	NextField    *PLpgSQLRecordField // TODO we probably don't need this

	// TODO record tuple desc id?
	// TODO Expanded Record Field Info?
}

// TODO look at PLpgSQL_variable / PLpgSQL_var / PLpgSQL_row / PLpgSQL_rec / PLpgSQL_recfield

type PLpgSQLEnv struct {
	VarScopeStack []VariableScope
}

// Scope contains all the variables defined in the DECLARE section of current statement block.
type VariableScope struct {
	Variables    []*PLpgSQLVariable
	VarNameToIdx map[string]int // mapping from variable
}
