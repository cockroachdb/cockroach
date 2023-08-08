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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type Expr = tree.Expr

type Statement interface {
	tree.NodeFormatter
	GetLineNo() int
	GetStmtID() uint
	plpgsqlStmt()
	WalkStmt(StatementVisitor)
}

type TaggedStatement interface {
	PlpgSQLStatementTag() string
}

type StatementImpl struct {
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

func (s *StatementImpl) GetLineNo() int {
	return s.LineNo
}

func (s *StatementImpl) GetStmtID() uint {
	return s.StmtID
}

func (s *StatementImpl) plpgsqlStmt() {}

// pl_block
type Block struct {
	StatementImpl
	Label      string
	Decls      []Declaration
	Body       []Statement
	Exceptions []Exception
}

// TODO(drewk): format Label and Exceptions fields.
func (s *Block) Format(ctx *tree.FmtCtx) {
	if s.Decls != nil {
		ctx.WriteString("DECLARE\n")
		for _, dec := range s.Decls {
			dec.Format(ctx)
		}
	}
	// TODO(drewk): Make sure the child statement is pretty printed correctly
	// with indents.
	ctx.WriteString("BEGIN\n")
	for _, childStmt := range s.Body {
		childStmt.Format(ctx)
	}
	if s.Exceptions != nil {
		ctx.WriteString("EXCEPTION\n")
		for _, e := range s.Exceptions {
			e.Format(ctx)
		}
	}
	ctx.WriteString("END\n")
}

func (s *Block) PlpgSQLStatementTag() string {
	return "stmt_block"
}

func (s *Block) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// decl_stmt
type Declaration struct {
	StatementImpl
	Var      Variable
	Constant bool
	Typ      tree.ResolvableTypeReference
	Collate  string
	NotNull  bool
	Expr     Expr
}

func (s *Declaration) Format(ctx *tree.FmtCtx) {
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

func (s *Declaration) PlpgSQLStatementTag() string {
	return "decl_stmt"
}

func (s *Declaration) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_assign
type Assignment struct {
	Statement
	Var   Variable
	Value Expr
}

func (s *Assignment) PlpgSQLStatementTag() string {
	return "stmt_assign"
}

func (s *Assignment) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%s := %s;\n", s.Var, s.Value))
}

func (s *Assignment) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_if
type If struct {
	StatementImpl
	Condition  Expr
	ThenBody   []Statement
	ElseIfList []ElseIf
	ElseBody   []Statement
}

func (s *If) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("IF ")
	s.Condition.Format(ctx)
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.ThenBody {
		// TODO(drewk): Pretty Print with spaces, not tabs.
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

func (s *If) PlpgSQLStatementTag() string {
	return "stmt_if"
}

func (s *If) WalkStmt(visitor StatementVisitor) {
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

type ElseIf struct {
	StatementImpl
	Condition Expr
	Stmts     []Statement
}

func (s *ElseIf) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("ELSIF ")
	s.Condition.Format(ctx)
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.Stmts {
		ctx.WriteString("\t")
		stmt.Format(ctx)
	}
}

func (s *ElseIf) PlpgSQLStatementTag() string {
	return "stmt_if_else_if"
}

func (s *ElseIf) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)

	for _, stmt := range s.Stmts {
		stmt.WalkStmt(visitor)
	}
}

// stmt_case
type Case struct {
	StatementImpl
	// TODO(drewk): Change to Expr
	TestExpr     string
	Var          Variable
	CaseWhenList []*CaseWhen
	HaveElse     bool
	ElseStmts    []Statement
}

// TODO(drewk): fix the whitespace/newline formatting for CASE (see the
// stmt_case test file).
func (s *Case) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CASE")
	if len(s.TestExpr) > 0 {
		ctx.WriteString(fmt.Sprintf(" %s", s.TestExpr))
	}
	ctx.WriteString("\n")
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

func (s *Case) PlpgSQLStatementTag() string {
	return "stmt_case"
}

func (s *Case) WalkStmt(visitor StatementVisitor) {
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

type CaseWhen struct {
	StatementImpl
	// TODO(drewk): Change to Expr
	Expr  string
	Stmts []Statement
}

func (s *CaseWhen) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("WHEN %s THEN\n", s.Expr))
	for i, stmt := range s.Stmts {
		ctx.WriteString("  ")
		stmt.Format(ctx)
		if i != len(s.Stmts)-1 {
			ctx.WriteString("\n")
		}
	}
}

func (s *CaseWhen) PlpgSQLStatementTag() string {
	return "stmt_when"
}

func (s *CaseWhen) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)

	for _, stmt := range s.Stmts {
		stmt.WalkStmt(visitor)
	}
}

// stmt_loop
type Loop struct {
	StatementImpl
	Label string
	Body  []Statement
}

func (s *Loop) PlpgSQLStatementTag() string {
	return "stmt_simple_loop"
}

func (s *Loop) Format(ctx *tree.FmtCtx) {
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

func (s *Loop) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// stmt_while
type While struct {
	StatementImpl
	Label     string
	Condition Expr
	Body      []Statement
}

func (s *While) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("WHILE ")
	s.Condition.Format(ctx)
	ctx.WriteString(" LOOP\n")
	for _, stmt := range s.Body {
		stmt.Format(ctx)
	}
	ctx.WriteString("END LOOP")
	if s.Label != "" {
		ctx.WriteString(fmt.Sprintf(" %s", s.Label))
	}
	ctx.WriteString(";\n")
}

func (s *While) PlpgSQLStatementTag() string {
	return "stmt_while"
}

func (s *While) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// stmt_for
type ForInt struct {
	StatementImpl
	Label   string
	Var     Variable
	Lower   Expr
	Upper   Expr
	Step    Expr
	Reverse int
	Body    []Statement
}

func (s *ForInt) Format(ctx *tree.FmtCtx) {
}

func (s *ForInt) PlpgSQLStatementTag() string {
	return "stmt_for_int_loop"
}

func (s *ForInt) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

type ForQuery struct {
	StatementImpl
	Label string
	Var   Variable
	Body  []Statement
}

func (s *ForQuery) Format(ctx *tree.FmtCtx) {
}

func (s *ForQuery) PlpgSQLStatementTag() string {
	return "stmt_for_query_loop"
}

func (s *ForQuery) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

type ForSelect struct {
	ForQuery
	Query Expr
}

func (s *ForSelect) Format(ctx *tree.FmtCtx) {
}

func (s *ForSelect) PlpgSQLStatementTag() string {
	return "stmt_query_select_loop"
}

func (s *ForSelect) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	s.ForQuery.WalkStmt(visitor)
}

type ForCursor struct {
	ForQuery
	CurVar   int // TODO(drewk): is this CursorVariable?
	ArgQuery Expr
}

func (s *ForCursor) Format(ctx *tree.FmtCtx) {
}

func (s *ForCursor) PlpgSQLStatementTag() string {
	return "stmt_for_query_cursor_loop"
}

func (s *ForCursor) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	s.ForQuery.WalkStmt(visitor)
}

type ForDynamic struct {
	ForQuery
	Query  Expr
	Params []Expr
}

func (s *ForDynamic) Format(ctx *tree.FmtCtx) {
}

func (s *ForDynamic) PlpgSQLStatementTag() string {
	return "stmt_for_dyn_loop"
}

func (s *ForDynamic) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
	s.ForQuery.WalkStmt(visitor)
}

// stmt_foreach_a
type ForEachArray struct {
	StatementImpl
	Label string
	Var   *Variable
	Slice int // TODO(drewk): not sure what this is
	Expr  Expr
	Body  []Statement
}

func (s *ForEachArray) Format(ctx *tree.FmtCtx) {
}

func (s *ForEachArray) PlpgSQLStatementTag() string {
	return "stmt_for_each_a"
}

func (s *ForEachArray) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)

	for _, stmt := range s.Body {
		stmt.WalkStmt(visitor)
	}
}

// stmt_exit
type Exit struct {
	StatementImpl
	Label     string
	Condition Expr
}

func (s *Exit) Format(ctx *tree.FmtCtx) {
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

func (s *Exit) PlpgSQLStatementTag() string {
	return "stmt_exit"
}

func (s *Exit) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_continue
type Continue struct {
	StatementImpl
	Label     string
	Condition Expr
}

func (s *Continue) Format(ctx *tree.FmtCtx) {
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

func (s *Continue) PlpgSQLStatementTag() string {
	return "stmt_continue"
}

func (s *Continue) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_return
type Return struct {
	StatementImpl
	Expr   Expr
	RetVar Variable
}

func (s *Return) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("RETURN ")
	if s.Expr == nil {
		s.RetVar.Format(ctx)
	} else {
		s.Expr.Format(ctx)
	}
	ctx.WriteString(";\n")
}

func (s *Return) PlpgSQLStatementTag() string {
	return "stmt_return"
}

func (s *Return) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

type ReturnNext struct {
	StatementImpl
	Expr   Expr
	RetVar Variable
}

func (s *ReturnNext) Format(ctx *tree.FmtCtx) {
}

func (s *ReturnNext) PlpgSQLStatementTag() string {
	return "stmt_return_next"
}

func (s *ReturnNext) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

type ReturnQuery struct {
	StatementImpl
	Query        Expr
	DynamicQuery Expr
	Params       []Expr
}

func (s *ReturnQuery) Format(ctx *tree.FmtCtx) {
}

func (s *ReturnQuery) PlpgSQLStatementTag() string {
	return "stmt_return_query"
}

func (s *ReturnQuery) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_raise
type Raise struct {
	StatementImpl
	LogLevel string
	Code     string
	CodeName string
	Message  string
	Params   []Expr
	Options  []RaiseOption
}

func (s *Raise) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("RAISE")
	if s.LogLevel != "" {
		ctx.WriteString(" ")
		ctx.WriteString(s.LogLevel)
	}
	if s.Code != "" {
		ctx.WriteString(fmt.Sprintf(" SQLSTATE '%s'", s.Code))
	}
	if s.CodeName != "" {
		ctx.WriteString(fmt.Sprintf(" %s", s.CodeName))
	}
	if s.Message != "" {
		ctx.WriteString(fmt.Sprintf(" '%s'", s.Message))
		for i := range s.Params {
			ctx.WriteString(", ")
			s.Params[i].Format(ctx)
		}
	}
	for i := range s.Options {
		if i == 0 {
			ctx.WriteString("\nUSING ")
		} else {
			ctx.WriteString(",\n")
		}
		s.Options[i].Format(ctx)
	}
	ctx.WriteString(";\n")
}

type RaiseOption struct {
	OptType string
	Expr    Expr
}

func (s *RaiseOption) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%s = ", strings.ToUpper(s.OptType)))
	s.Expr.Format(ctx)
}

func (s *Raise) PlpgSQLStatementTag() string {
	return "stmt_raise"
}

func (s *Raise) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_assert
type Assert struct {
	StatementImpl
	Condition Expr
	Message   Expr
}

func (s *Assert) Format(ctx *tree.FmtCtx) {
	// TODO(drewk): Pretty print the assert condition and message
	ctx.WriteString("ASSERT\n")
}

func (s *Assert) PlpgSQLStatementTag() string {
	return "stmt_assert"
}

func (s *Assert) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_execsql
type Execute struct {
	StatementImpl
	SqlStmt tree.Statement
	Strict  bool // INTO STRICT flag
	Target  []Variable
}

func (s *Execute) Format(ctx *tree.FmtCtx) {
	s.SqlStmt.Format(ctx)
	if s.Target != nil {
		ctx.WriteString(" INTO ")
		if s.Strict {
			ctx.WriteString("STRICT ")
		}
		for i := range s.Target {
			if i > 0 {
				ctx.WriteString(", ")
			}
			s.Target[i].Format(ctx)
		}
	}
	ctx.WriteString(";\n")
}

func (s *Execute) PlpgSQLStatementTag() string {
	return "stmt_exec_sql"
}

func (s *Execute) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_dynexecute
// TODO(chengxiong): query should be a better expression type.
type DynamicExecute struct {
	StatementImpl
	Query  string
	Into   bool
	Strict bool
	Target Variable
	Params []Expr
}

func (s *DynamicExecute) Format(ctx *tree.FmtCtx) {
	// TODO(drewk): Pretty print the original command
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

func (s *DynamicExecute) PlpgSQLStatementTag() string {
	return "stmt_dyn_exec"
}

func (s *DynamicExecute) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_perform
type Perform struct {
	StatementImpl
	Expr Expr
}

func (s *Perform) Format(ctx *tree.FmtCtx) {
}

func (s *Perform) PlpgSQLStatementTag() string {
	return "stmt_perform"
}

func (s *Perform) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_call
type Call struct {
	StatementImpl
	Expr   Expr
	IsCall bool
	Target Variable
}

func (s *Call) Format(ctx *tree.FmtCtx) {
	// TODO(drewk): Correct the Call field and print the Expr and Target.
	if s.IsCall {
		ctx.WriteString("CALL a function/procedure\n")
	} else {
		ctx.WriteString("DO a code block\n")
	}
}

func (s *Call) PlpgSQLStatementTag() string {
	return "stmt_call"
}

func (s *Call) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_getdiag
type GetDiagnostics struct {
	StatementImpl
	IsStacked bool
	DiagItems GetDiagnosticsItemList // TODO(drewk): what is this?
}

func (s *GetDiagnostics) Format(ctx *tree.FmtCtx) {
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

type GetDiagnosticsItem struct {
	Kind GetDiagnosticsKind
	// TODO(jane): TargetName is temporary -- should be removed and use Target.
	TargetName string
	Target     int // where to assign it?
}

func (s *GetDiagnosticsItem) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%s := %s", s.TargetName, s.Kind.String()))
}

type GetDiagnosticsItemList []*GetDiagnosticsItem

func (s *GetDiagnostics) PlpgSQLStatementTag() string {
	return "stmt_get_diag"
}

func (s *GetDiagnostics) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_open
type Open struct {
	StatementImpl
	CurVar        int // TODO(drewk): this could just a Variable
	CursorOptions uint32
	// TODO(jane): This is temporary and we should remove it and use CurVar.
	CursorName       string
	WithExplicitExpr bool
	// TODO(jane): Should be Expr
	ArgQuery string
	// TODO(jane): Should be Expr
	Query string
	// TODO(jane): Should be Expr
	DynamicQuery string
	// TODO(jane): Should be []Expr
	Params []string
}

func (s *Open) Format(ctx *tree.FmtCtx) {
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
			// TODO(drewk): Make sure placeholders are properly printed
			ctx.WriteString(fmt.Sprintf("EXECUTE %s ", s.DynamicQuery))
			if len(s.Params) != 0 {
				// TODO(drewk): Dont print instances of multiple params with brackets `[...]`
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

func (s *Open) PlpgSQLStatementTag() string {
	return "stmt_open"
}

func (s *Open) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_fetch
// stmt_move (where IsMove = true)
type Fetch struct {
	StatementImpl
	Target           Variable
	CurVar           int // TODO(drewk): this could just a Variable
	Direction        FetchDirection
	HowMany          int64
	Expr             Expr
	IsMove           bool
	ReturnsMultiRows bool
}

func (s *Fetch) Format(ctx *tree.FmtCtx) {
}

func (s *Fetch) PlpgSQLStatementTag() string {
	if s.IsMove {
		return "stmt_move"
	}
	return "stmt_fetch"
}

func (s *Fetch) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_close
type Close struct {
	StatementImpl
	CurVar int // TODO(drewk): this could just a Variable
}

func (s *Close) Format(ctx *tree.FmtCtx) {
	// TODO(drewk): Pretty- Print the cursor identifier
	ctx.WriteString("CLOSE a cursor\n")

}

func (s *Close) PlpgSQLStatementTag() string {
	return "stmt_close"
}

func (s *Close) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_commit
type Commit struct {
	StatementImpl
	Chain bool
}

func (s *Commit) Format(ctx *tree.FmtCtx) {
}

func (s *Commit) PlpgSQLStatementTag() string {
	return "stmt_commit"
}

func (s *Commit) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_rollback
type Rollback struct {
	StatementImpl
	Chain bool
}

func (s *Rollback) Format(ctx *tree.FmtCtx) {
}

func (s *Rollback) PlpgSQLStatementTag() string {
	return "stmt_rollback"
}

func (s *Rollback) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}

// stmt_null
type Null struct {
	StatementImpl
}

func (s *Null) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("NULL\n")
}

func (s *Null) PlpgSQLStatementTag() string {
	return "stmt_null"
}

func (s *Null) WalkStmt(visitor StatementVisitor) {
	visitor.Visit(s)
}
