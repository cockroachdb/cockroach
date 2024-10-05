// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plpgsqltree

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type Expr = tree.Expr

type Statement interface {
	tree.NodeFormatter
	GetLineNo() int
	GetStmtID() uint
	plpgsqlStmt()
	WalkStmt(StatementVisitor) (newStmt Statement, changed bool)
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
	Decls      []Statement
	Body       []Statement
	Exceptions []Exception
}

func (s *Block) CopyNode() *Block {
	copyNode := *s
	copyNode.Decls = append([]Statement(nil), copyNode.Decls...)
	copyNode.Body = append([]Statement(nil), copyNode.Body...)
	copyNode.Exceptions = append([]Exception(nil), copyNode.Exceptions...)
	return &copyNode
}

// TODO(drewk): format Label and Exceptions fields.
func (s *Block) Format(ctx *tree.FmtCtx) {
	if s.Decls != nil {
		ctx.WriteString("DECLARE\n")
		for _, dec := range s.Decls {
			ctx.FormatNode(dec)
		}
	}
	// TODO(drewk): Make sure the child statement is pretty printed correctly
	// with indents.
	ctx.WriteString("BEGIN\n")
	for _, childStmt := range s.Body {
		ctx.FormatNode(childStmt)
	}
	if s.Exceptions != nil {
		ctx.WriteString("EXCEPTION\n")
		for _, e := range s.Exceptions {
			ctx.FormatNode(&e)
		}
	}
	ctx.WriteString("END\n")
}

func (s *Block) PlpgSQLStatementTag() string {
	return "stmt_block"
}

func (s *Block) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	for i, stmt := range s.Decls {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*Block).Decls[i] = ns
		}
	}
	for i, stmt := range s.Body {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*Block).Body[i] = ns
		}
	}
	for i, stmt := range s.Exceptions {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*Block).Exceptions[i] = *(ns.(*Exception))
		}
	}
	return newStmt, changed
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

func (s *Declaration) CopyNode() *Declaration {
	copyNode := *s
	return &copyNode
}

func (s *Declaration) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(string(s.Var))
	if s.Constant {
		ctx.WriteString(" CONSTANT")
	}
	ctx.WriteString(" ")
	ctx.FormatTypeReference(s.Typ)
	if s.Collate != "" {
		ctx.WriteString(" COLLATE ")
		ctx.FormatNameP(&s.Collate)
	}
	if s.NotNull {
		ctx.WriteString(" NOT NULL")
	}
	if s.Expr != nil {
		ctx.WriteString(" := ")
		ctx.FormatNode(s.Expr)
	}
	ctx.WriteString(";\n")
}

func (s *Declaration) PlpgSQLStatementTag() string {
	return "decl_stmt"
}

func (s *Declaration) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

type CursorDeclaration struct {
	StatementImpl
	Name   Variable
	Scroll tree.CursorScrollOption
	Query  tree.Statement
}

func (s *CursorDeclaration) CopyNode() *CursorDeclaration {
	copyNode := *s
	return &copyNode
}

func (s *CursorDeclaration) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(string(s.Name))
	switch s.Scroll {
	case tree.Scroll:
		ctx.WriteString(" SCROLL")
	case tree.NoScroll:
		ctx.WriteString(" NO SCROLL")
	}
	ctx.WriteString(" CURSOR FOR ")
	ctx.FormatNode(s.Query)
	ctx.WriteString(";\n")
}

func (s *CursorDeclaration) PlpgSQLStatementTag() string {
	return "decl_cursor_stmt"
}

func (s *CursorDeclaration) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_assign
type Assignment struct {
	Statement
	Var   Variable
	Value Expr
}

func (s *Assignment) CopyNode() *Assignment {
	copyNode := *s
	return &copyNode
}

func (s *Assignment) PlpgSQLStatementTag() string {
	return "stmt_assign"
}

func (s *Assignment) Format(ctx *tree.FmtCtx) {
	ctx.FormatNode(&s.Var)
	ctx.WriteString(" := ")
	ctx.FormatNode(s.Value)
	ctx.WriteString(";\n")
}

func (s *Assignment) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_if
type If struct {
	StatementImpl
	Condition  Expr
	ThenBody   []Statement
	ElseIfList []ElseIf
	ElseBody   []Statement
}

func (s *If) CopyNode() *If {
	copyNode := *s
	copyNode.ThenBody = append([]Statement(nil), copyNode.ThenBody...)
	copyNode.ElseBody = append([]Statement(nil), copyNode.ElseBody...)
	copyNode.ElseIfList = make([]ElseIf, len(s.ElseIfList))
	for i, ei := range s.ElseIfList {
		copyNode.ElseIfList[i] = ei
		copyNode.ElseIfList[i].Stmts = append([]Statement(nil), copyNode.ElseIfList[i].Stmts...)
	}
	return &copyNode
}

func (s *If) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("IF ")
	ctx.FormatNode(s.Condition)
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.ThenBody {
		// TODO(drewk): Pretty Print with spaces, not tabs.
		ctx.WriteString("\t")
		ctx.FormatNode(stmt)
	}
	for _, elsifStmt := range s.ElseIfList {
		ctx.FormatNode(&elsifStmt)
	}
	for i, elseStmt := range s.ElseBody {
		if i == 0 {
			ctx.WriteString("ELSE\n")
		}
		ctx.WriteString("\t")
		ctx.FormatNode(elseStmt)
	}
	ctx.WriteString("END IF;\n")
}

func (s *If) PlpgSQLStatementTag() string {
	return "stmt_if"
}

func (s *If) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)

	for i, thenStmt := range s.ThenBody {
		ns, ch := thenStmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*If).ThenBody[i] = ns
		}
	}

	for i, elseIf := range s.ElseIfList {
		ns, ch := elseIf.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*If).ElseIfList[i] = *ns.(*ElseIf)
		}
	}

	for i, elseStmt := range s.ElseBody {
		ns, ch := elseStmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*If).ElseBody[i] = ns
		}
	}

	return newStmt, changed
}

type ElseIf struct {
	StatementImpl
	Condition Expr
	Stmts     []Statement
}

func (s *ElseIf) CopyNode() *ElseIf {
	copyNode := *s
	copyNode.Stmts = append([]Statement(nil), copyNode.Stmts...)
	return &copyNode
}

func (s *ElseIf) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("ELSIF ")
	ctx.FormatNode(s.Condition)
	ctx.WriteString(" THEN\n")
	for _, stmt := range s.Stmts {
		ctx.WriteString("\t")
		ctx.FormatNode(stmt)
	}
}

func (s *ElseIf) PlpgSQLStatementTag() string {
	return "stmt_if_else_if"
}

func (s *ElseIf) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)

	for i, stmt := range s.Stmts {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*ElseIf).Stmts[i] = ns
		}
	}
	return newStmt, changed
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

func (s *Case) CopyNode() *Case {
	copyNode := *s
	copyNode.ElseStmts = append([]Statement(nil), copyNode.ElseStmts...)
	copyNode.CaseWhenList = make([]*CaseWhen, len(s.CaseWhenList))
	caseWhens := make([]CaseWhen, len(s.CaseWhenList))
	for i, cw := range s.CaseWhenList {
		caseWhens[i] = *cw
		copyNode.CaseWhenList[i] = &caseWhens[i]
	}
	return &copyNode
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
		ctx.FormatNode(when)
	}
	if s.HaveElse {
		ctx.WriteString("ELSE\n")
		for _, stmt := range s.ElseStmts {
			ctx.WriteString("  ")
			ctx.FormatNode(stmt)
		}
	}
	ctx.WriteString("END CASE\n")
}

func (s *Case) PlpgSQLStatementTag() string {
	return "stmt_case"
}

func (s *Case) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)

	for i, when := range s.CaseWhenList {
		ns, ch := when.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*Case).CaseWhenList[i] = ns.(*CaseWhen)
		}
	}

	if s.HaveElse {
		for i, stmt := range s.ElseStmts {
			ns, ch := stmt.WalkStmt(visitor)
			if ch {
				changed = true
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Case).ElseStmts[i] = ns
			}
		}
	}
	return newStmt, changed
}

type CaseWhen struct {
	StatementImpl
	// TODO(drewk): Change to Expr
	Expr  string
	Stmts []Statement
}

func (s *CaseWhen) CopyNode() *CaseWhen {
	copyNode := *s
	copyNode.Stmts = append([]Statement(nil), copyNode.Stmts...)
	return &copyNode
}

func (s *CaseWhen) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("WHEN %s THEN\n", s.Expr))
	for i, stmt := range s.Stmts {
		ctx.WriteString("  ")
		ctx.FormatNode(stmt)
		if i != len(s.Stmts)-1 {
			ctx.WriteString("\n")
		}
	}
}

func (s *CaseWhen) PlpgSQLStatementTag() string {
	return "stmt_when"
}

func (s *CaseWhen) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)

	for i, stmt := range s.Stmts {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*CaseWhen).Stmts[i] = ns
		}
	}
	return newStmt, changed
}

// stmt_loop
type Loop struct {
	StatementImpl
	Label string
	Body  []Statement
}

func (s *Loop) CopyNode() *Loop {
	copyNode := *s
	copyNode.Body = append([]Statement(nil), copyNode.Body...)
	return &copyNode
}

func (s *Loop) PlpgSQLStatementTag() string {
	return "stmt_simple_loop"
}

func (s *Loop) Format(ctx *tree.FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("<<")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(">>\n")
	}
	ctx.WriteString("LOOP\n")
	for _, stmt := range s.Body {
		ctx.FormatNode(stmt)
	}
	ctx.WriteString("END LOOP")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	ctx.WriteString(";\n")
}

func (s *Loop) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	for i, stmt := range s.Body {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*Loop).Body[i] = ns
		}
	}
	return newStmt, changed
}

// stmt_while
type While struct {
	StatementImpl
	Label     string
	Condition Expr
	Body      []Statement
}

func (s *While) CopyNode() *While {
	copyNode := *s
	copyNode.Body = append([]Statement(nil), copyNode.Body...)
	return &copyNode
}

func (s *While) Format(ctx *tree.FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("<<")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(">>\n")
	}
	ctx.WriteString("WHILE ")
	ctx.FormatNode(s.Condition)
	ctx.WriteString(" LOOP\n")
	for _, stmt := range s.Body {
		ctx.FormatNode(stmt)
	}
	ctx.WriteString("END LOOP")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	ctx.WriteString(";\n")
}

func (s *While) PlpgSQLStatementTag() string {
	return "stmt_while"
}

func (s *While) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	for i, stmt := range s.Body {
		ns, ch := stmt.WalkStmt(visitor)
		if ch {
			changed = true
			if newStmt == s {
				newStmt = s.CopyNode()
			}
			newStmt.(*While).Body[i] = ns
		}
	}
	return newStmt, changed
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

func (s *ForInt) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *ForQuery) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *ForSelect) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *ForCursor) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *ForDynamic) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *ForEachArray) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
}

// stmt_exit
type Exit struct {
	StatementImpl
	Label     string
	Condition Expr
}

func (s *Exit) CopyNode() *Exit {
	copyNode := *s
	return &copyNode
}

func (s *Exit) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("EXIT")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	if s.Condition != nil {
		ctx.WriteString(" WHEN ")
		ctx.FormatNode(s.Condition)
	}
	ctx.WriteString(";\n")

}

func (s *Exit) PlpgSQLStatementTag() string {
	return "stmt_exit"
}

func (s *Exit) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_continue
type Continue struct {
	StatementImpl
	Label     string
	Condition Expr
}

func (s *Continue) CopyNode() *Continue {
	copyNode := *s
	return &copyNode
}

func (s *Continue) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CONTINUE")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	if s.Condition != nil {
		ctx.WriteString(" WHEN ")
		ctx.FormatNode(s.Condition)
	}
	ctx.WriteString(";\n")
}

func (s *Continue) PlpgSQLStatementTag() string {
	return "stmt_continue"
}

func (s *Continue) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_return
type Return struct {
	StatementImpl
	Expr   Expr
	RetVar Variable
}

func (s *Return) CopyNode() *Return {
	copyNode := *s
	return &copyNode
}

func (s *Return) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("RETURN ")
	if s.Expr == nil {
		ctx.FormatNode(&s.RetVar)
	} else {
		ctx.FormatNode(s.Expr)
	}
	ctx.WriteString(";\n")
}

func (s *Return) PlpgSQLStatementTag() string {
	return "stmt_return"
}

func (s *Return) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
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

func (s *ReturnNext) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *ReturnQuery) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
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

func (s *Raise) CopyNode() *Raise {
	copyNode := *s
	copyNode.Params = append([]Expr(nil), s.Params...)
	copyNode.Options = append([]RaiseOption(nil), s.Options...)
	return &copyNode
}

func (s *Raise) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("RAISE")
	if s.LogLevel != "" {
		ctx.WriteString(" ")
		ctx.WriteString(s.LogLevel)
	}
	if s.Code != "" {
		ctx.WriteString(" SQLSTATE ")
		formatStringQuotes(ctx, s.Code)
	}
	if s.CodeName != "" {
		ctx.WriteString(" ")
		formatString(ctx, s.CodeName)
	}
	if s.Message != "" {
		ctx.WriteString(" ")
		formatStringQuotes(ctx, s.Message)
		for i := range s.Params {
			ctx.WriteString(", ")
			ctx.FormatNode(s.Params[i])
		}
	}
	for i := range s.Options {
		if i == 0 {
			ctx.WriteString("\nUSING ")
		} else {
			ctx.WriteString(",\n")
		}
		ctx.FormatNode(&s.Options[i])
	}
	ctx.WriteString(";\n")
}

type RaiseOption struct {
	OptType string
	Expr    Expr
}

func (s *RaiseOption) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(fmt.Sprintf("%s = ", strings.ToUpper(s.OptType)))
	ctx.FormatNode(s.Expr)
}

func (s *Raise) PlpgSQLStatementTag() string {
	return "stmt_raise"
}

func (s *Raise) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_assert
type Assert struct {
	StatementImpl
	Condition Expr
	Message   Expr
}

func (s *Assert) CopyNode() *Assert {
	copyNode := *s
	return &copyNode
}

func (s *Assert) Format(ctx *tree.FmtCtx) {
	// TODO(drewk): Pretty print the assert condition and message
	ctx.WriteString("ASSERT\n")
}

func (s *Assert) PlpgSQLStatementTag() string {
	return "stmt_assert"
}

func (s *Assert) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_execsql
type Execute struct {
	StatementImpl
	SqlStmt tree.Statement
	Strict  bool // INTO STRICT flag
	Target  []Variable
}

func (s *Execute) CopyNode() *Execute {
	copyNode := *s
	copyNode.Target = append([]Variable(nil), copyNode.Target...)
	return &copyNode
}

func (s *Execute) Format(ctx *tree.FmtCtx) {
	ctx.FormatNode(s.SqlStmt)
	if s.Target != nil {
		ctx.WriteString(" INTO ")
		if s.Strict {
			ctx.WriteString("STRICT ")
		}
		for i := range s.Target {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(&s.Target[i])
		}
	}
	ctx.WriteString(";\n")
}

func (s *Execute) PlpgSQLStatementTag() string {
	return "stmt_exec_sql"
}

func (s *Execute) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
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

func (s *DynamicExecute) CopyNode() *DynamicExecute {
	copyNode := *s
	copyNode.Params = append([]Expr(nil), s.Params...)
	return &copyNode
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

func (s *DynamicExecute) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
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

func (s *Perform) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
}

// stmt_call
type Call struct {
	StatementImpl
	Expr   Expr
	IsCall bool
	Target Variable
}

func (s *Call) CopyNode() *Call {
	copyNode := *s
	return &copyNode
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

func (s *Call) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
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
		ctx.FormatNode(i)
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

func (s *GetDiagnostics) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_open
type Open struct {
	StatementImpl
	CurVar Variable
	Scroll tree.CursorScrollOption
	Query  tree.Statement
}

func (s *Open) CopyNode() *Open {
	copyNode := *s
	return &copyNode
}

func (s *Open) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("OPEN ")
	ctx.FormatNode(&s.CurVar)
	switch s.Scroll {
	case tree.Scroll:
		ctx.WriteString(" SCROLL")
	case tree.NoScroll:
		ctx.WriteString(" NO SCROLL")
	}
	if s.Query != nil {
		ctx.WriteString(" FOR ")
		ctx.FormatNode(s.Query)
	}
	ctx.WriteString(";\n")
}

func (s *Open) PlpgSQLStatementTag() string {
	return "stmt_open"
}

func (s *Open) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_fetch
// stmt_move (where IsMove = true)
type Fetch struct {
	StatementImpl
	Cursor tree.CursorStmt
	Target []Variable
	IsMove bool
}

func (s *Fetch) Format(ctx *tree.FmtCtx) {
	if s.IsMove {
		ctx.WriteString("MOVE ")
	} else {
		ctx.WriteString("FETCH ")
	}
	if dir := s.Cursor.FetchType.String(); dir != "" {
		ctx.WriteString(dir)
		ctx.WriteString(" ")
	}
	if s.Cursor.FetchType.HasCount() {
		ctx.WriteString(strconv.Itoa(int(s.Cursor.Count)))
		ctx.WriteString(" ")
	}
	ctx.WriteString("FROM ")
	ctx.FormatName(string(s.Cursor.Name))
	if s.Target != nil {
		ctx.WriteString(" INTO ")
		for i := range s.Target {
			if i > 0 {
				ctx.WriteString(", ")
			}
			ctx.FormatNode(&s.Target[i])
		}
	}
	ctx.WriteString(";\n")
}

func (s *Fetch) PlpgSQLStatementTag() string {
	if s.IsMove {
		return "stmt_move"
	}
	return "stmt_fetch"
}

func (s *Fetch) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_close
type Close struct {
	StatementImpl
	CurVar Variable
}

func (s *Close) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CLOSE ")
	ctx.FormatNode(&s.CurVar)
	ctx.WriteString(";\n")
}

func (s *Close) PlpgSQLStatementTag() string {
	return "stmt_close"
}

func (s *Close) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
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

func (s *Commit) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
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

func (s *Rollback) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// stmt_null
type Null struct {
	StatementImpl
}

func (s *Null) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("NULL;\n")
}

func (s *Null) PlpgSQLStatementTag() string {
	return "stmt_null"
}

func (s *Null) WalkStmt(visitor StatementVisitor) (newStmt Statement, changed bool) {
	newStmt, changed = visitor.Visit(s)
	return newStmt, changed
}

// formatString is a helper function that prints "_" if FmtHideConstants is set,
// and otherwise prints the given string.
func formatString(ctx *tree.FmtCtx, str string) {
	if ctx.HasFlags(tree.FmtHideConstants) {
		ctx.WriteString("_")
	} else {
		ctx.WriteString(str)
	}
}

// formatStringQuotes is similar to formatString, but surrounds the output with
// single quotes.
func formatStringQuotes(ctx *tree.FmtCtx, str string) {
	ctx.WriteString("'")
	formatString(ctx, str)
	ctx.WriteString("'")
}
