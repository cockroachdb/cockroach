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

type DatumType int

const (
	PlpgsqlDtypeVar DatumType = iota
	plpgsqlDtypeRow
	plpgsqlDtypeRec
	plpgsqlDtypeRecfield
	plpgsqlDtypePromise
)

type PLpgSQLStatementType int

const (
	PlpgsqlStmtBlock PLpgSQLStatementType = iota
	PlpgsqlStmtAssign
	PlpgsqlStmtIf
	PlpgsqlStmtCase
	PlpgsqlStmtLoop
	PlpgsqlStmtWhile
	PlpgsqlStmtFori
	PlpgsqlStmtFors
	PlpgsqlStmtForc
	PlpgsqlStmtForeachA
	PlpgsqlStmtExit
	PlpgsqlStmtReturn
	PlpgsqlStmtReturnNext
	PlpgsqlStmtReturnQuery
	PlpgsqlStmtRaise
	PlpgsqlStmtAssert
	PlpgsqlStmtExecSql
	PlpgsqlStmtDynExecute
	PlpgsqlStmtDynFors
	PlpgsqlStmtGetDiag
	PlpgsqlStmtOpen
	PlpgsqlStmtFetch
	PlpgsqlStmtClose
	PlpgsqlStmtPerform
	PlpgsqlStmtCall
	PlpgsqlStmtCommit
	PlpgsqlStmtRollback
)

type PLpgSQLRaiseOptionType int

const (
	PlpgsqlRaiseoptionErrcode PLpgSQLRaiseOptionType = iota
	PlpgsqlRaiseoptionMessage
	PlpgsqlRaiseoptionDetail
	PlpgsqlRaiseoptionHint
	PlpgsqlRaiseoptionColumn
	PlpgsqlRaiseoptionConstraint
	PlpgsqlRaiseoptionDatatype
	PlpgsqlRaiseoptionTable
	PlpgsqlRaiseoptionSchema
)

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

type PLpgSQLFetchDirection int

const (
	PLpgSQLFetchForward PLpgSQLFetchDirection = iota
	// TODO add more, but maybe we have existing enum for this already.
)

type PLpgSQLPromiseType int

const (
	PlpgsqlPromiseNone = iota /* not a promise, or promise satisfied */
	PlpgsqlPromiseTgName
	PlpgsqlPromiseTgWhen
	PlpgsqlPromiseTgLevel
	PlpgsqlPromiseTgOp
	PlpgsqlPromiseTgRelid
	PlpgsqlPromiseTgTableName
	PlpgsqlPromiseTgTableSchema
	PlpgsqlPromiseTgNargs
	PlpgsqlPromiseTgArgv
	PlpgsqlPromiseTgEvent
	PlpgsqlPromiseTgTag
)
