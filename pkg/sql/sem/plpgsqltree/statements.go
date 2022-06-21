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

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// TODO not sure if there's anything else needed for expression
type PLpgSQLExpr = tree.Expr

type PLpgSQLStatement interface {
	GetType() PLpgSQLStatementType
	GetLineNo() int
	GetStmtID() uint
	plpgsqlStmt()
}

type PLpgSQLStatementImpl struct {
	CmdType PLpgSQLStatementType
	// TODO (Chengxiong) figure out how to get line number from scanner.
	LineNo int
	/*
	 * Unique statement ID in this function (starting at 1; 0 is invalid/not
	 * set).  This can be used by a profiler as the index for an array of
	 * per-statement metrics.
	 */
	// TODO (Chengxiong) figure out how to get statement id from parser.
	StmtID uint
}

func (s *PLpgSQLStatementImpl) GetType() PLpgSQLStatementType {
	return s.CmdType
}

func (s *PLpgSQLStatementImpl) GetLineNo() int {
	return s.LineNo
}

func (s *PLpgSQLStatementImpl) GetStmtID() uint {
	return s.StmtID
}

func (s *PLpgSQLStatementImpl) plpgsqlStmt() {}

// pl_block
type PLpgSQLStmtBlock struct {
	PLpgSQLStatementImpl
	Label      string
	Body       []PLpgSQLStatement
	InitVars   []PLpgSQLVariable
	Exceptions *PLpgSQLExceptionBlock
	Scope      VariableScope
}

// stmt_assign
type PLpgSQLStmtAssign struct {
	PLpgSQLStatement
	Var   PLpgSQLVariable
	Value PLpgSQLExpr
}

// stmt_if
type PLpgSQLStmtIf struct {
	PLpgSQLStatementImpl
	Condition  PLpgSQLExpr
	ThenBody   []PLpgSQLStatement
	ElseIfList []*PLpgSQLStmtIfElseIfArm
	ElseBody   []PLpgSQLStatement
}

type PLpgSQLStmtIfElseIfArm struct {
	LineNo    int
	Condition PLpgSQLExpr
	Stmts     []PLpgSQLStatement
}

// stmt_case
type PLpgSQLStmtCase struct {
	PLpgSQLStatementImpl
	TestExpr     PLpgSQLExpr
	Var          PLpgSQLVariable
	CaseWhenList []PLpgSQLStmtCaseWhenArm
	HaveElse     bool
	ElseStmts    []PLpgSQLStatement
}

type PLpgSQLStmtCaseWhenArm struct {
	LineNo int
	Expr   PLpgSQLExpr
	Stmts  []PLpgSQLStatement
}

// stmt_loop
type PLpgSQLStmtSimpleLoop struct {
	PLpgSQLStatementImpl
	Label string
	Body  []PLpgSQLStatement
}

// stmt_while
type PLpgSQLStmtWhileLoop struct {
	PLpgSQLStatementImpl
	Label     string
	Condition PLpgSQLExpr
	Body      []PLpgSQLStatement
}

// stmt_for
type PLpgSQLStmtForIntLoop struct {
	PLpgSQLStatementImpl
	Label   string
	Var     PLpgSQLVariable
	Lower   PLpgSQLExpr
	Upper   PLpgSQLExpr
	Step    PLpgSQLExpr
	Reverse int
	Body    []PLpgSQLStatement
}

type PLpgSQLStmtForQueryLoop struct {
	PLpgSQLStatementImpl
	Label string
	Var   PLpgSQLVariable
	Body  []PLpgSQLStatement
}

type PLpgSQLStmtForQuerySelectLoop struct {
	PLpgSQLStmtForQueryLoop
	Query PLpgSQLExpr
}

type PLpgSQLStmtForQueryCursorLoop struct {
	PLpgSQLStmtForQueryLoop
	CurVar   int // TODO is this CursorVariable?
	ArgQuery PLpgSQLExpr
}

type PLpgSQLStmtForDynamicLoop struct {
	PLpgSQLStmtForQueryLoop
	Query  PLpgSQLExpr
	Params []PLpgSQLExpr
}

// stmt_foreach_a
type PLpgSQLStmtForEachALoop struct {
	PLpgSQLStatementImpl
	Label string
	Var   *PLpgSQLVariable
	Slice int // TODO not sure what this is
	Expr  PLpgSQLExpr
	Body  []PLpgSQLStatement
}

// stmt_exit
type PLpgSQLStmtExit struct {
	PLpgSQLStatementImpl
	IsExit    bool
	Label     string
	Condition PLpgSQLExpr
}

// stmt_return
type PLpgSQLStmtReturn struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	RetVar PLpgSQLVariable
}

type PLpgSQLStmtReturnNext struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	RetVar PLpgSQLVariable
}

type PLpgSQLStmtReturnQuery struct {
	PLpgSQLStatementImpl
	Query        PLpgSQLExpr
	DynamicQuery PLpgSQLExpr
	Params       []PLpgSQLExpr
}

// stmt_raise
type PLpgSQLStmtRaise struct {
	PLpgSQLStatementImpl
	LogLevel int
	CodeName string
	Message  string
	Params   []PLpgSQLExpr
	Options  []PLpgSQLStmtRaiseOption
}

type PLpgSQLStmtRaiseOption struct {
	OptType PLpgSQLRaiseOptionType
	Expr    PLpgSQLExpr
}

// stmt_assert
type PLpgSQLStmtAssert struct {
	PLpgSQLStatementImpl
	Condition PLpgSQLExpr
	Message   PLpgSQLExpr
}

// stmt_execsql
type PLpgSQLStmtExecSql struct {
	PLpgSQLStatementImpl
	SqlStmt            PLpgSQLExpr
	IsModifyingStmt    bool            // is the stmt insert/update/delete/merge?
	IsModifyingStmtSet bool            // is the stmt valid yet?
	Into               bool            // INTO provided?
	Strict             bool            // INTO STRICT flag
	Target             PLpgSQLVariable // INTO target (record variable or row variable)
}

// stmt_dynexecute
type PLpgSQLStmtDynamicExecute struct {
	PLpgSQLStatementImpl
	Query  PLpgSQLExpr
	Into   bool
	Strict bool
	Target PLpgSQLVariable
	Params []PLpgSQLExpr
}

// stmt_perform
type PLpgSQLStmtPerform struct {
	PLpgSQLStatementImpl
	Expr PLpgSQLExpr
}

// stmt_call
type PLpgSQLStmtCall struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	IsCall bool
	Target PLpgSQLVariable
}

// stmt_getdiag
type PLpgSQLStmtGetDiag struct {
	PLpgSQLStatementImpl
	IsStacked bool
	DiagItems []PLpgSQLStmtGetDiagItem // TODO what is this?
}

type PLpgSQLStmtGetDiagItem struct {
	Kind   PLpgSQLGetDiagKind
	Target int // where to assign it?
}

// stmt_open
type PLpgSQLStmtOpen struct {
	PLpgSQLStatementImpl
	CurVar        int // TODO this could just a PLpgSQLVariable
	CursorOptions int
	ArgQuery      PLpgSQLExpr
	Query         PLpgSQLExpr
	DynamicQuery  PLpgSQLExpr
	Params        []PLpgSQLExpr
}

// stmt_fetch
// stmt_move (where IsMove = true)
type PLpgSQLStmtFetch struct {
	PLpgSQLStatementImpl
	Target           PLpgSQLVariable
	CurVar           int // TODO this could just a PLpgSQLVariable
	Direction        PLpgSQLFetchDirection
	HowMany          int64
	Expr             PLpgSQLExpr
	IsMove           bool
	ReturnsMultiRows bool
}

// stmt_close
type PLpgSQLStmtClose struct {
	PLpgSQLStatementImpl
	CurVar int // TODO this could just a PLpgSQLVariable
}

// TODO stmt_null ? it's only a `NULL`

// stmt_commit
type PLpgSQLStmtCommit struct {
	PLpgSQLStatementImpl
	Chain bool
}

// stmt_rollback
type PLpgSQLStmtRollback struct {
	PLpgSQLStatementImpl
	Chain bool
}
