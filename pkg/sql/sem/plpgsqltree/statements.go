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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// PLpgSQLExpr is a placeholder for the sql query and expression representation
// in pl/pgsql world.
// TODO(chengxiong): at the moment of this implementation, we treat all
// queries/expressions as pure strings.
// We could later potentially introduce sql parser into pl/pgsql parser to:
// 1. validate sql queries/expressions
// 2. parse sql queries/expressions into real expressions.
// which means that `string` here can be `tree.Expr`.
type PLpgSQLExpr = string

type PLpgSQLStatement interface {
	tree.NodeFormatter
	GetLineNo() int
	GetStmtID() uint
	plpgsqlStmt()
}

type PLpgSQLStatements []PLpgSQLStatement

func (p PLpgSQLStatements) Format(ctx *tree.FmtCtx) {
	for _, stmt := range p {
		stmt.Format(ctx)
	}
}

type PLpgSQLStatementImpl struct {
	// TODO(Chengxiong): figure out how to get line number from scanner.
	LineNo int
	/*
	 * Unique statement ID in this function (starting at 1; 0 is invalid/not
	 * set).  This can be used by a profiler as the index for an array of
	 * per-statement metrics.
	 */
	// TODO(Chengxiong): figure out how to get statement id from parser.
	StmtID uint
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
	Body       PLpgSQLStatements
	InitVars   []PLpgSQLVariable
	Exceptions *PLpgSQLExceptionBlock
}

func (s *PLpgSQLStmtBlock) Format(ctx *tree.FmtCtx) {
	if s.InitVars != nil {
		ctx.WriteString("DECLARE\n")
	}
	// TODO: Make sure the child statement is pretty printed correctly
	ctx.WriteString("BEGIN\n")
	s.Body.Format(ctx)
	ctx.WriteString("END\n")
	ctx.WriteString("<NOT DONE YET>")
}

// stmt_assign
type PLpgSQLStmtAssign struct {
	PLpgSQLStatement
	Var   PLpgSQLVariable
	Value PLpgSQLExpr
}

func (s *PLpgSQLStmtAssign) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("ASSIGN %s := %s\n", s.Var, s.Value))
}

// stmt_if
type PLpgSQLStmtIf struct {
	PLpgSQLStatementImpl
	Condition  PLpgSQLExpr
	ThenBody   PLpgSQLStatements
	ElseIfList []*PLpgSQLStmtIfElseIfArm
	ElseBody   PLpgSQLStatements
}

func (s *PLpgSQLStmtIf) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("IF %s THEN\n", s.Condition))
	s.ThenBody.Format(ctx)
	for _, elsifStmt := range s.ElseIfList {
		elsifStmt.Format(ctx)
	}
	if len(s.ElseBody) > 0 {
		ctx.WriteString("ELSE\n")
	}
	s.ElseBody.Format(ctx)
	ctx.WriteString("END IF\n")
	ctx.WriteString("<NOT DONE YET>")
}

type PLpgSQLStmtIfElseIfArm struct {
	PLpgSQLStatementImpl
	LineNo    int
	Condition PLpgSQLExpr
	Stmts     PLpgSQLStatements
}

func (s *PLpgSQLStmtIfElseIfArm) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("ELSIF %s THEN\n", s.Condition))
	s.Stmts.Format(ctx)
}

// stmt_case
type PLpgSQLStmtCase struct {
	PLpgSQLStatementImpl
	TestExpr     PLpgSQLExpr
	Var          PLpgSQLVariable
	CaseWhenList []*PLpgSQLStmtCaseWhenArm
	HaveElse     bool
	ElseStmts    PLpgSQLStatements
}

func (s *PLpgSQLStmtCase) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CASE")
	if len(s.TestExpr) > 0 {
		ctx.WriteString(fmt.Sprintf(" %s", s.TestExpr))
	}
	ctx.WriteString("\n")
	// TODO: Strip spaces before each comma of the when list
	for _, when := range s.CaseWhenList {
		when.Format(ctx)
	}
	if s.HaveElse {
		ctx.WriteString("ELSE\n")
		s.ElseStmts.Format(ctx)
	}
	ctx.WriteString("END CASE\n")
	ctx.WriteString("<NOT DONE YET>")

}

type PLpgSQLStmtCaseWhenArm struct {
	LineNo int
	Expr   PLpgSQLExpr
	Stmts  PLpgSQLStatements
}

func (s *PLpgSQLStmtCaseWhenArm) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("WHEN %s THEN\n", s.Expr))
	s.Stmts.Format(ctx)
}

// stmt_loop
type PLpgSQLStmtSimpleLoop struct {
	PLpgSQLStatementImpl
	Label string
	Body  PLpgSQLStatements
}

func (s *PLpgSQLStmtSimpleLoop) Format(ctx *tree.FmtCtx) {
}

// stmt_while
type PLpgSQLStmtWhileLoop struct {
	PLpgSQLStatementImpl
	Label     string
	Condition PLpgSQLExpr
	Body      PLpgSQLStatements
}

func (s *PLpgSQLStmtWhileLoop) Format(ctx *tree.FmtCtx) {
}

type PLpgSQLStmtForLoop interface {
	PLpgSQLStatement
	SetBody(stmts PLpgSQLStatements)
	SetForVariable(variable PLpgSQLVariable)
}

// PLpgSQLStmtForIntLoop
type PLpgSQLStmtForIntLoop struct {
	PLpgSQLStatementImpl
	Label   string
	Var     PLpgSQLVariable
	Lower   PLpgSQLExpr
	Upper   PLpgSQLExpr
	Step    PLpgSQLExpr
	Reverse bool
	Body    PLpgSQLStatements
}

func (s *PLpgSQLStmtForIntLoop) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("FOR ")
	ctx.WriteString(s.Var)
	ctx.WriteString(" IN ")
	if s.Reverse {
		ctx.WriteString("REVERSE ")
	}
	ctx.WriteString(s.Lower)
	ctx.WriteString(" .. ")
	ctx.WriteString(s.Upper)
	if len(s.Step) > 0 {
		ctx.WriteString(" BY ")
		ctx.WriteString(s.Step)
	}
	ctx.WriteString(" LOOP\n")
	s.Body.Format(ctx)
	ctx.WriteString("END LOOP\n")
}

func (s *PLpgSQLStmtForIntLoop) SetBody(stmts PLpgSQLStatements) {
	s.Body = stmts
}

func (s *PLpgSQLStmtForIntLoop) SetForVariable(variable PLpgSQLVariable) {
	s.Var = variable
}

type PLpgSQLStmtForQueryLoop struct {
	PLpgSQLStatementImpl
	Label string
	Var   PLpgSQLVariable
	Body  PLpgSQLStatements
}

func (s *PLpgSQLStmtForQueryLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForQueryLoop) SetBody(stmts PLpgSQLStatements) {
	s.Body = stmts
}

func (s *PLpgSQLStmtForQueryLoop) SetForVariable(variable PLpgSQLVariable) {
	s.Var = variable
}

type PLpgSQLStmtForQuerySelectLoop struct {
	PLpgSQLStmtForQueryLoop
	Query PLpgSQLExpr
}

func (s *PLpgSQLStmtForQuerySelectLoop) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("FOR ")
	ctx.WriteString(s.Var)
	ctx.WriteString(" IN ")
	ctx.WriteString(s.Query)
	ctx.WriteString(" LOOP\n")
	s.Body.Format(ctx)
	ctx.WriteString("END LOOP\n")
}

type PLpgSQLStmtForQueryCursorLoop struct {
	PLpgSQLStmtForQueryLoop
	CurVar   PLpgSQLVariable
	ArgQuery PLpgSQLExpr
}

func (s *PLpgSQLStmtForQueryCursorLoop) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("FOR ")
	ctx.WriteString(s.Var)
	ctx.WriteString(" IN ")
	ctx.WriteString(s.CurVar)
	ctx.WriteString(" LOOP\n")
	s.Body.Format(ctx)
	ctx.WriteString("END LOOP\n")
}

type PLpgSQLStmtForDynamicLoop struct {
	PLpgSQLStmtForQueryLoop
	Query  PLpgSQLExpr
	Params []PLpgSQLExpr
}

func (s *PLpgSQLStmtForDynamicLoop) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("FOR ")
	ctx.WriteString(s.Var)
	ctx.WriteString(" IN EXECUTE ")
	ctx.WriteString(s.Query)
	if len(s.Params) > 0 {
		ctx.WriteString(" USING ")
		for i, param := range s.Params {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.WriteString(param)
		}
	}
	ctx.WriteString(" LOOP\n")
	s.Body.Format(ctx)
	ctx.WriteString("END LOOP\n")
}

// stmt_foreach_a
type PLpgSQLStmtForEachALoop struct {
	PLpgSQLStatementImpl
	Label string
	Var   PLpgSQLVariable
	Slice int // TODO not sure what this is
	Expr  PLpgSQLExpr
	Body  PLpgSQLStatements
}

func (s *PLpgSQLStmtForEachALoop) Format(ctx *tree.FmtCtx) {
}

// stmt_exit
type PLpgSQLStmtExit struct {
	PLpgSQLStatementImpl
	IsExit    bool
	Label     string
	Condition PLpgSQLExpr
}

func (s *PLpgSQLStmtExit) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty print the exit label
	ctx.WriteString("EXIT\n")
	ctx.WriteString("<NOT DONE YET>")

}

// stmt_return
type PLpgSQLStmtReturn struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	RetVar PLpgSQLVariable
}

func (s *PLpgSQLStmtReturn) Format(ctx *tree.FmtCtx) {
}

type PLpgSQLStmtReturnNext struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	RetVar PLpgSQLVariable
}

func (s *PLpgSQLStmtReturnNext) Format(ctx *tree.FmtCtx) {
}

type PLpgSQLStmtReturnQuery struct {
	PLpgSQLStatementImpl
	Query        PLpgSQLExpr
	DynamicQuery PLpgSQLExpr
	Params       []PLpgSQLExpr
}

func (s *PLpgSQLStmtReturnQuery) Format(ctx *tree.FmtCtx) {
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

func (s *PLpgSQLStmtRaise) Format(ctx *tree.FmtCtx) {
}

type PLpgSQLStmtRaiseOption struct {
	OptType PLpgSQLRaiseOptionType
	Expr    PLpgSQLExpr
}

func (s *PLpgSQLStmtRaiseOption) Format(ctx *tree.FmtCtx) {
}

// stmt_assert
type PLpgSQLStmtAssert struct {
	PLpgSQLStatementImpl
	Condition PLpgSQLExpr
	Message   PLpgSQLExpr
}

func (s *PLpgSQLStmtAssert) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty print the assert condition and message
	ctx.WriteString("ASSERT\n")
	ctx.WriteString("<NOT DONE YET>\n")
}

// stmt_execsql
type PLpgSQLStmtExecSql struct {
	PLpgSQLStatementImpl
	SqlStmt PLpgSQLExpr
	Into    bool // INTO provided?
	Strict  bool // INTO STRICT flag
}

func (s *PLpgSQLStmtExecSql) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty print the sql statement
	ctx.WriteString("EXECUTE bare sql query")
	if s.Into {
		ctx.WriteString(" WITH INTO")
	}
	if s.Strict {
		ctx.WriteString(" STRICT")
	}
	ctx.WriteString("\n")
	ctx.WriteString("<NOT DONE YET>")
}

// stmt_dynexecute
// TODO(chengxiong): query should be a better expression type.
type PLpgSQLStmtDynamicExecute struct {
	PLpgSQLStatementImpl
	Query  PLpgSQLExpr
	Into   bool
	Strict bool
	Target PLpgSQLVariable
	Params []PLpgSQLExpr
}

func (s *PLpgSQLStmtDynamicExecute) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty print the original command
	ctx.WriteString("EXECUTE a dynamic command")
	if s.Into {
		ctx.WriteString(" WITH INTO")
		if s.Strict {
			ctx.WriteString(" STRICT")
		}
	}
	if s.Params != nil {
		ctx.WriteString(" WITH USING")
	}
	ctx.WriteString("\n")
	ctx.WriteString("<NOT DONE YET>")
}

// stmt_perform
type PLpgSQLStmtPerform struct {
	PLpgSQLStatementImpl
	Expr PLpgSQLExpr
}

func (s *PLpgSQLStmtPerform) Format(ctx *tree.FmtCtx) {
}

// stmt_call
type PLpgSQLStmtCall struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	IsCall bool
	Target PLpgSQLVariable
}

func (s *PLpgSQLStmtCall) Format(ctx *tree.FmtCtx) {
	// TODO: Correct the pretty printing and keep func call
	if s.IsCall {
		ctx.WriteString("CALL a function/procedure\n")
	} else {
		ctx.WriteString("DO a code block\n")
	}
	ctx.WriteString("<NOT DONE YET>\n")

}

// stmt_getdiag
type PLpgSQLStmtGetDiag struct {
	PLpgSQLStatementImpl
	IsStacked bool
	DiagItems PLpgSQLStmtGetDiagItemList // TODO: what is this?
}

func (s *PLpgSQLStmtGetDiag) Format(ctx *tree.FmtCtx) {
	if s.IsStacked {
		ctx.WriteString("GET STACKED DIAGNOSTICS ")
	} else {
		ctx.WriteString("GET DIAGNOSTICS ")
	}
	for idx, i := range s.DiagItems {
		i.Format(ctx)
		if idx != len(s.DiagItems)-1 {
			ctx.WriteString(" ")
		}
	}
	ctx.WriteString("\n")
}

type PLpgSQLStmtGetDiagItem struct {
	Kind PLpgSQLGetDiagKind
	// TODO(jane): TargetName is temporary -- should be removed and use Target.
	TargetName string
	Target     int // where to assign it?
}

func (s *PLpgSQLStmtGetDiagItem) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%s := %s", s.TargetName, s.Kind.String()))
}

type PLpgSQLStmtGetDiagItemList []*PLpgSQLStmtGetDiagItem

// stmt_open
type PLpgSQLStmtOpen struct {
	PLpgSQLStatementImpl
	CursorOptions    uint32
	CurVar           PLpgSQLVariable
	WithExplicitExpr bool
	ArgQuery         PLpgSQLExpr
	Query            PLpgSQLExpr
	DynamicQuery     PLpgSQLExpr
	Params           []PLpgSQLExpr
}

func (s *PLpgSQLStmtOpen) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(
		fmt.Sprintf(
			"OPEN %s ",
			s.CurVar,
		))

	opts := OptListFromBitField(s.CursorOptions)
	for _, opt := range opts {
		if opt.String() != "" {
			ctx.WriteString(fmt.Sprintf("%s ", opt.String()))
		}
	}
	if !s.WithExplicitExpr {
		ctx.WriteString("FOR ")
		if s.DynamicQuery != "" {
			// TODO: Make sure placeholders are properly printed
			ctx.WriteString(fmt.Sprintf("EXECUTE %s ", s.DynamicQuery))
			if len(s.Params) != 0 {
				// TODO: Dont print instances of multiple params with brackets `[...]`
				ctx.WriteString(fmt.Sprintf("USING %s", s.Params))
			}
		} else {
			ctx.WriteString(s.Query)
		}
	} else {
		ctx.WriteString(s.ArgQuery)
	}
	ctx.WriteString("\n")
}

type PLpgSQLStmtFetch struct {
	PLpgSQLStatementImpl
	Targets   []string
	CurVar    PLpgSQLVariable
	Direction PLpgSQLFetchDirection
	HowMany   PLpgSQLFetchHowMany
	// Expr is integer-valued expression. It is meant for the number of results to
	// be returned.
	Expr PLpgSQLExpr
	// IsMove is set true if this is a MOVE statement.
	IsMove           bool
	ReturnsMultiRows bool
}

func (s *PLpgSQLStmtFetch) Format(ctx *tree.FmtCtx) {
	if !s.IsMove {
		ctx.WriteString("FETCH ")
	} else {
		ctx.WriteString("MOVE ")
	}

	if s.HowMany == PLpgSQLFetchAll {
		ctx.WriteString("ALL ")
	} else {
		switch s.Direction {
		case PLpgSQLFetchAbsolute:
			if s.HowMany == PlpgSQLFetchLast {
				ctx.WriteString("LAST ")
			} else if s.Expr != "" {
				ctx.WriteString(fmt.Sprintf("ABSOLUTE %s FROM ", s.Expr))
			} else {
				ctx.WriteString("FIRST ")
			}
		case PLpgSQLFetchRelative:
			ctx.WriteString(fmt.Sprintf("RELATIVE %s FROM ", s.Expr))
		default:
			ctx.WriteString(fmt.Sprintf("%s ", s.Direction.String()))
		}
	}

	ctx.WriteString(s.CurVar)

	if !s.IsMove {
		ctx.WriteString(" INTO ")
		for idx, target := range s.Targets {
			ctx.WriteString(target)
			if idx != len(s.Targets)-1 {
				ctx.WriteString(", ")
			}
		}
	}

	ctx.WriteString("\n")
}

// stmt_close
type PLpgSQLStmtClose struct {
	PLpgSQLStatementImpl
	CurVar int // TODO: this could just a PLpgSQLVariable
}

func (s *PLpgSQLStmtClose) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty- Print the cursor identifier
	ctx.WriteString("CLOSE a cursor\n")
	ctx.WriteString("<NOT DONE YET>")

}

// stmt_commit
type PLpgSQLStmtCommit struct {
	PLpgSQLStatementImpl
	Chain bool
}

func (s *PLpgSQLStmtCommit) Format(ctx *tree.FmtCtx) {
}

// stmt_rollback
type PLpgSQLStmtRollback struct {
	PLpgSQLStatementImpl
	Chain bool
}

func (s *PLpgSQLStmtRollback) Format(ctx *tree.FmtCtx) {
}

// stmt_null
type PLpgSQLStmtNull struct {
	PLpgSQLStatementImpl
}

func (s *PLpgSQLStmtNull) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("NULL\n")
}
