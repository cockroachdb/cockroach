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

// TODO: not sure if there's anything else needed for expression
type PLpgSQLExpr = tree.Expr

type PLpgSQLStatement interface {
	tree.NodeFormatter
	GetLineNo() int
	GetStmtID() uint
	plpgsqlStmt()
	WalkStmt(PLpgSQLStmtVisitor)
}

type TaggedPLpgSQLStatement interface {
	PlpgSQLStatementTag() string
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
// TODO: not done yet.
type PLpgSQLStmtBlock struct {
	PLpgSQLStatementImpl
	Label      string
	Decls      []PLpgSQLDecl
	Body       []PLpgSQLStatement
	Exceptions *PLpgSQLExceptionBlock
	Scope      VariableScope
}

func (s *PLpgSQLStmtBlock) Format(ctx *tree.FmtCtx) {
	if s.Decls != nil {
		ctx.WriteString("DECLARE\n")
		for _, dec := range s.Decls {
			dec.Format(ctx)
		}
	}
	// TODO: Make sure the child statement is pretty printed correctly
	ctx.WriteString("BEGIN\n")
	for _, childStmt := range s.Body {
		childStmt.Format(ctx)
	}
	ctx.WriteString("END\n")
}

func (s *PLpgSQLStmtBlock) PlpgSQLStatementTag() string {
	return "stmt_block"
}

func (s *PLpgSQLStmtBlock) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// decl_stmt
type PLpgSQLDecl struct {
	PLpgSQLStatementImpl
	Var      PLpgSQLVariable
	Constant bool
	Typ      tree.ResolvableTypeReference
	Collate  string
	NotNull  bool
	Expr     PLpgSQLExpr
}

func (s *PLpgSQLDecl) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(string(s.Var))
	if s.Constant {
		ctx.WriteString(" CONSTANT")
	}
	ctx.WriteString(fmt.Sprintf(" %s", s.Typ.SQLString()))
	if s.Collate != "" {
		ctx.WriteString(fmt.Sprintf(" %s", s.Collate))
	}
	if s.NotNull {
		ctx.WriteString(" NOT NULL")
	}
	if s.Expr != nil {
		ctx.WriteString(" := ")
		s.Expr.Format(ctx)
	}
	ctx.WriteString(";\n")
}

func (s *PLpgSQLDecl) PlpgSQLStatementTag() string {
	return "decl_stmt"
}

func (s *PLpgSQLDecl) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_assign
type PLpgSQLStmtAssign struct {
	PLpgSQLStatement
	Var   PLpgSQLVariable
	Value PLpgSQLExpr
}

func (s *PLpgSQLStmtAssign) PlpgSQLStatementTag() string {
	return "stmt_assign"
}

func (s *PLpgSQLStmtAssign) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%s := %s;\n", s.Var, s.Value))
}

func (s *PLpgSQLStmtAssign) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_if
// TODO: not done yet.
type PLpgSQLStmtIf struct {
	PLpgSQLStatementImpl
	Condition  PLpgSQLExpr
	ThenBody   []PLpgSQLStatement
	ElseIfList []*PLpgSQLStmtIfElseIfArm
	ElseBody   []PLpgSQLStatement
}

func (s *PLpgSQLStmtIf) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("IF ")
	s.Condition.Format(ctx)
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.ThenBody {
		// TODO: Pretty Print with spaces not tabs
		ctx.WriteString("\t")
		stmt.Format(ctx)
	}
	for _, elsifStmt := range s.ElseIfList {
		elsifStmt.Format(ctx)
	}
	for i, elseStmt := range s.ElseBody {
		if i == 0 {
			ctx.WriteString("ELSE\n")
		}
		ctx.WriteString("\t")
		elseStmt.Format(ctx)
	}
	ctx.WriteString("END IF;\n")
}

func (s *PLpgSQLStmtIf) PlpgSQLStatementTag() string {
	return "stmt_if"
}

func (s *PLpgSQLStmtIf) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)

	for _, thenStmt := range s.ThenBody {
		thenStmt.WalkStmt(visitor)
	}

	for _, elseIf := range s.ElseIfList {
		elseIf.WalkStmt(visitor)
	}

	for _, elseStmt := range s.ElseBody {
		elseStmt.WalkStmt(visitor)
	}

}

type PLpgSQLStmtIfElseIfArm struct {
	PLpgSQLStatementImpl
	LineNo    int
	Condition PLpgSQLExpr
	Stmts     []PLpgSQLStatement
}

func (s *PLpgSQLStmtIfElseIfArm) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("ELSIF ")
	s.Condition.Format(ctx)
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.Stmts {
		ctx.WriteString("\t")
		stmt.Format(ctx)
	}
}

func (s *PLpgSQLStmtIfElseIfArm) PlpgSQLStatementTag() string {
	return "stmt_if_else_if"
}

func (s *PLpgSQLStmtIfElseIfArm) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)

	for _, stmt := range s.Stmts {
		stmt.WalkStmt(visitor)
	}
}

// stmt_case
// TODO: not done yet.
type PLpgSQLStmtCase struct {
	PLpgSQLStatementImpl
	// TODO: Change to PLpgSQLExpr
	TestExpr     string
	Var          PLpgSQLVariable
	CaseWhenList []*PLpgSQLStmtCaseWhenArm
	HaveElse     bool
	ElseStmts    []PLpgSQLStatement
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
		for _, stmt := range s.ElseStmts {
			ctx.WriteString("  ")
			stmt.Format(ctx)
		}
	}
	ctx.WriteString("END CASE\n")
}

func (s *PLpgSQLStmtCase) PlpgSQLStatementTag() string {
	return "stmt_case"
}

func (s *PLpgSQLStmtCase) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)

	for _, when := range s.CaseWhenList {
		when.WalkStmt(visitor)
	}

	if s.HaveElse {
		for _, stmt := range s.ElseStmts {
			stmt.WalkStmt(visitor)
		}
	}
}

type PLpgSQLStmtCaseWhenArm struct {
	PLpgSQLStatementImpl
	// TODO: Change to PLpgSQLExpr
	Expr  string
	Stmts []PLpgSQLStatement
}

func (s *PLpgSQLStmtCaseWhenArm) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("WHEN %s THEN\n", s.Expr))
	for i, stmt := range s.Stmts {
		ctx.WriteString("  ")
		stmt.Format(ctx)
		if i != len(s.Stmts)-1 {
			ctx.WriteString("\n")
		}
	}
}

func (s *PLpgSQLStmtCaseWhenArm) PlpgSQLStatementTag() string {
	return "stmt_when"
}

func (s *PLpgSQLStmtCaseWhenArm) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)

	for _, stmt := range s.Stmts {
		stmt.WalkStmt(visitor)
	}
}

// stmt_loop
type PLpgSQLStmtSimpleLoop struct {
	PLpgSQLStatementImpl
	Label string
	Body  []PLpgSQLStatement
}

func (s *PLpgSQLStmtSimpleLoop) PlpgSQLStatementTag() string {
	return "stmt_simple_loop"
}

func (s *PLpgSQLStmtSimpleLoop) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("LOOP\n")
	for _, stmt := range s.Body {
		stmt.Format(ctx)
	}
	ctx.WriteString("END LOOP")
	if s.Label != "" {
		ctx.WriteString(fmt.Sprintf(" %s", s.Label))
	}
	ctx.WriteString(";\n")
}

func (s *PLpgSQLStmtSimpleLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// stmt_while
type PLpgSQLStmtWhileLoop struct {
	PLpgSQLStatementImpl
	Label     string
	Condition PLpgSQLExpr
	Body      []PLpgSQLStatement
}

func (s *PLpgSQLStmtWhileLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtWhileLoop) PlpgSQLStatementTag() string {
	return "stmt_while"
}

func (s *PLpgSQLStmtWhileLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
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

func (s *PLpgSQLStmtForIntLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForIntLoop) PlpgSQLStatementTag() string {
	return "stmt_for_int_loop"
}

func (s *PLpgSQLStmtForIntLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

type PLpgSQLStmtForQueryLoop struct {
	PLpgSQLStatementImpl
	Label string
	Var   PLpgSQLVariable
	Body  []PLpgSQLStatement
}

func (s *PLpgSQLStmtForQueryLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForQueryLoop) PlpgSQLStatementTag() string {
	return "stmt_for_query_loop"
}

func (s *PLpgSQLStmtForQueryLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

type PLpgSQLStmtForQuerySelectLoop struct {
	PLpgSQLStmtForQueryLoop
	Query PLpgSQLExpr
}

func (s *PLpgSQLStmtForQuerySelectLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForQuerySelectLoop) PlpgSQLStatementTag() string {
	return "stmt_query_select_loop"
}

func (s *PLpgSQLStmtForQuerySelectLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	s.PLpgSQLStmtForQueryLoop.WalkStmt(visitor)
}

type PLpgSQLStmtForQueryCursorLoop struct {
	PLpgSQLStmtForQueryLoop
	CurVar   int // TODO: is this CursorVariable?
	ArgQuery PLpgSQLExpr
}

func (s *PLpgSQLStmtForQueryCursorLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForQueryCursorLoop) PlpgSQLStatementTag() string {
	return "stmt_for_query_cursor_loop"
}

func (s *PLpgSQLStmtForQueryCursorLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	s.PLpgSQLStmtForQueryLoop.WalkStmt(visitor)
}

type PLpgSQLStmtForDynamicLoop struct {
	PLpgSQLStmtForQueryLoop
	Query  PLpgSQLExpr
	Params []PLpgSQLExpr
}

func (s *PLpgSQLStmtForDynamicLoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForDynamicLoop) PlpgSQLStatementTag() string {
	return "stmt_for_dyn_loop"
}

func (s *PLpgSQLStmtForDynamicLoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
	s.PLpgSQLStmtForQueryLoop.WalkStmt(visitor)
}

// stmt_foreach_a
type PLpgSQLStmtForEachALoop struct {
	PLpgSQLStatementImpl
	Label string
	Var   *PLpgSQLVariable
	Slice int // TODO: not sure what this is
	Expr  PLpgSQLExpr
	Body  []PLpgSQLStatement
}

func (s *PLpgSQLStmtForEachALoop) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtForEachALoop) PlpgSQLStatementTag() string {
	return "stmt_for_each_a"
}

func (s *PLpgSQLStmtForEachALoop) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)

	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// stmt_exit
type PLpgSQLStmtExit struct {
	PLpgSQLStatementImpl
	Label     string
	Condition PLpgSQLExpr
}

func (s *PLpgSQLStmtExit) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("EXIT")
	if s.Label != "" {
		ctx.WriteString(fmt.Sprintf(" %s", s.Label))
	}
	if s.Condition != nil {
		ctx.WriteString(" WHEN ")
		s.Condition.Format(ctx)
	}
	ctx.WriteString(";\n")

}

func (s *PLpgSQLStmtExit) PlpgSQLStatementTag() string {
	return "stmt_exit"
}

func (s *PLpgSQLStmtExit) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_continue
type PLpgSQLStmtContinue struct {
	PLpgSQLStatementImpl
	Label     string
	Condition PLpgSQLExpr
}

func (s *PLpgSQLStmtContinue) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CONTINUE")
	if s.Label != "" {
		ctx.WriteString(fmt.Sprintf(" %s", s.Label))
	}
	if s.Condition != nil {
		ctx.WriteString(" WHEN ")
		s.Condition.Format(ctx)
	}
	ctx.WriteString(";\n")
}

func (s *PLpgSQLStmtContinue) PlpgSQLStatementTag() string {
	return "stmt_continue"
}

func (s *PLpgSQLStmtContinue) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_return
type PLpgSQLStmtReturn struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	RetVar PLpgSQLVariable
}

func (s *PLpgSQLStmtReturn) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("RETURN ")
	if s.Expr == nil {
		s.RetVar.Format(ctx)
	} else {
		s.Expr.Format(ctx)
	}
	ctx.WriteString(";\n")
}

func (s *PLpgSQLStmtReturn) PlpgSQLStatementTag() string {
	return "stmt_return"
}

func (s *PLpgSQLStmtReturn) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

type PLpgSQLStmtReturnNext struct {
	PLpgSQLStatementImpl
	Expr   PLpgSQLExpr
	RetVar PLpgSQLVariable
}

func (s *PLpgSQLStmtReturnNext) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtReturnNext) PlpgSQLStatementTag() string {
	return "stmt_return_next"
}

func (s *PLpgSQLStmtReturnNext) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

type PLpgSQLStmtReturnQuery struct {
	PLpgSQLStatementImpl
	Query        PLpgSQLExpr
	DynamicQuery PLpgSQLExpr
	Params       []PLpgSQLExpr
}

func (s *PLpgSQLStmtReturnQuery) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtReturnQuery) PlpgSQLStatementTag() string {
	return "stmt_return_query"
}

func (s *PLpgSQLStmtReturnQuery) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
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

func (s *PLpgSQLStmtRaise) PlpgSQLStatementTag() string {
	return "stmt_raise"
}

func (s *PLpgSQLStmtRaise) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_assert
// TODO: not done yet.
type PLpgSQLStmtAssert struct {
	PLpgSQLStatementImpl
	Condition PLpgSQLExpr
	Message   PLpgSQLExpr
}

func (s *PLpgSQLStmtAssert) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty print the assert condition and message
	ctx.WriteString("ASSERT\n")
}

func (s *PLpgSQLStmtAssert) PlpgSQLStatementTag() string {
	return "stmt_assert"
}

func (s *PLpgSQLStmtAssert) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_execsql
// TODO: not done yet.
type PLpgSQLStmtExecSql struct {
	PLpgSQLStatementImpl
	SqlStmt string
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
}

func (s *PLpgSQLStmtExecSql) PlpgSQLStatementTag() string {
	return "stmt_exec_sql"
}

func (s *PLpgSQLStmtExecSql) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_dynexecute
// TODO: not done yet.
// TODO(chengxiong): query should be a better expression type.
type PLpgSQLStmtDynamicExecute struct {
	PLpgSQLStatementImpl
	Query  string
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
}

func (s *PLpgSQLStmtDynamicExecute) PlpgSQLStatementTag() string {
	return "stmt_dyn_exec"
}

func (s *PLpgSQLStmtDynamicExecute) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_perform
type PLpgSQLStmtPerform struct {
	PLpgSQLStatementImpl
	Expr PLpgSQLExpr
}

func (s *PLpgSQLStmtPerform) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtPerform) PlpgSQLStatementTag() string {
	return "stmt_perform"
}

func (s *PLpgSQLStmtPerform) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_call
// TODO: not done yet.
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
}

func (s *PLpgSQLStmtCall) PlpgSQLStatementTag() string {
	return "stmt_call"
}

func (s *PLpgSQLStmtCall) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
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

func (s *PLpgSQLStmtGetDiag) PlpgSQLStatementTag() string {
	return "stmt_get_diag"
}

func (s *PLpgSQLStmtGetDiag) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_open
type PLpgSQLStmtOpen struct {
	PLpgSQLStatementImpl
	CurVar        int // TODO: this could just a PLpgSQLVariable
	CursorOptions uint32
	// TODO(jane): This is temporary and we should remove it and use CurVar.
	CursorName       string
	WithExplicitExpr bool
	// TODO(jane): Should be PLpgSQLExpr
	ArgQuery string
	// TODO(jane): Should be PLpgSQLExpr
	Query string
	// TODO(jane): Should be PLpgSQLExpr
	DynamicQuery string
	// TODO(jane): Should be []PLpgSQLExpr
	Params []string
}

func (s *PLpgSQLStmtOpen) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(
		fmt.Sprintf(
			"OPEN %s ",
			s.CursorName,
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

func (s *PLpgSQLStmtOpen) PlpgSQLStatementTag() string {
	return "stmt_open"
}

func (s *PLpgSQLStmtOpen) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_fetch
// stmt_move (where IsMove = true)
type PLpgSQLStmtFetch struct {
	PLpgSQLStatementImpl
	Target           PLpgSQLVariable
	CurVar           int // TODO: this could just a PLpgSQLVariable
	Direction        PLpgSQLFetchDirection
	HowMany          int64
	Expr             PLpgSQLExpr
	IsMove           bool
	ReturnsMultiRows bool
}

func (s *PLpgSQLStmtFetch) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtFetch) PlpgSQLStatementTag() string {
	if s.IsMove {
		return "stmt_move"
	}
	return "stmt_fetch"
}

func (s *PLpgSQLStmtFetch) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_close
// TODO: not done yet.
type PLpgSQLStmtClose struct {
	PLpgSQLStatementImpl
	CurVar int // TODO: this could just a PLpgSQLVariable
}

func (s *PLpgSQLStmtClose) Format(ctx *tree.FmtCtx) {
	// TODO: Pretty- Print the cursor identifier
	ctx.WriteString("CLOSE a cursor\n")

}

func (s *PLpgSQLStmtClose) PlpgSQLStatementTag() string {
	return "stmt_close"
}

func (s *PLpgSQLStmtClose) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_commit
type PLpgSQLStmtCommit struct {
	PLpgSQLStatementImpl
	Chain bool
}

func (s *PLpgSQLStmtCommit) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtCommit) PlpgSQLStatementTag() string {
	return "stmt_commit"
}

func (s *PLpgSQLStmtCommit) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_rollback
type PLpgSQLStmtRollback struct {
	PLpgSQLStatementImpl
	Chain bool
}

func (s *PLpgSQLStmtRollback) Format(ctx *tree.FmtCtx) {
}

func (s *PLpgSQLStmtRollback) PlpgSQLStatementTag() string {
	return "stmt_rollback"
}

func (s *PLpgSQLStmtRollback) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}

// stmt_null
type PLpgSQLStmtNull struct {
	PLpgSQLStatementImpl
}

func (s *PLpgSQLStmtNull) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("NULL\n")
}

func (s *PLpgSQLStmtNull) PlpgSQLStatementTag() string {
	return "stmt_null"
}

func (s *PLpgSQLStmtNull) WalkStmt(visitor PLpgSQLStmtVisitor) {
	visitor.Visit(s)
}
