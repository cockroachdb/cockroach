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
	WalkStmt(StatementVisitor) Statement
	PlpgSQLStatementTag() string
}

type StatementImpl struct {
	// TODO(drewk): figure out how to get line number from scanner.
	LineNo int
	/*
	 * Unique statement ID in this function (starting at 1; 0 is invalid/not
	 * set).  This can be used by a profiler as the index for an array of
	 * per-statement metrics.
	 */
	// TODO(drewk): figure out how to get statement id from parser.
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

func (s *Block) Format(ctx *tree.FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("<<")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(">>\n")
	}
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
	ctx.WriteString("END")
	if s.Label != "" {
		ctx.WriteString(" ")
		ctx.FormatNameP(&s.Label)
	}
	ctx.WriteString(";\n")
}

func (s *Block) PlpgSQLStatementTag() string {
	return "stmt_block"
}

func (s *Block) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)
	if recurse {
		for i, decl := range s.Decls {
			newDecl := decl.WalkStmt(visitor)
			if newDecl != decl {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Block).Decls[i] = newDecl
			}
		}
		for i, bodyStmt := range s.Body {
			newBodyStmt := bodyStmt.WalkStmt(visitor)
			if newBodyStmt != bodyStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Block).Body[i] = newBodyStmt
			}
		}
		for i := range s.Exceptions {
			exception := &s.Exceptions[i]
			newException := exception.WalkStmt(visitor)
			if newException != exception {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Block).Exceptions[i] = *(newException.(*Exception))
			}
		}
	}
	return newStmt
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
	ctx.FormatNode(&s.Var)
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

func (s *Declaration) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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
	ctx.FormatNode(&s.Name)
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

func (s *CursorDeclaration) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
}

// stmt_assign
type Assignment struct {
	StatementImpl
	Var   Variable
	Value Expr

	// Indirection is the optional name of a field in a composite variable. For
	// example, in the assignment "foo.bar := 1", Indirection would be "bar".
	Indirection tree.Name
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
	if s.Indirection != "" {
		ctx.WriteByte('.')
		ctx.FormatNode(&s.Indirection)
	}
	ctx.WriteString(" := ")
	ctx.FormatNode(s.Value)
	ctx.WriteString(";\n")
}

func (s *Assignment) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *If) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, thenStmt := range s.ThenBody {
			newThenStmt := thenStmt.WalkStmt(visitor)
			if newThenStmt != thenStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*If).ThenBody[i] = newThenStmt
			}
		}
		for i := range s.ElseIfList {
			elseIf := &s.ElseIfList[i]
			newElseIf := elseIf.WalkStmt(visitor)
			if newElseIf != elseIf {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*If).ElseIfList[i] = *newElseIf.(*ElseIf)
			}
		}
		for i, elseStmt := range s.ElseBody {
			newElseStmt := elseStmt.WalkStmt(visitor)
			if newElseStmt != elseStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*If).ElseBody[i] = newElseStmt
			}
		}
	}
	return newStmt
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

func (s *ElseIf) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, bodyStmt := range s.Stmts {
			newBodyStmt := bodyStmt.WalkStmt(visitor)
			if newBodyStmt != bodyStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*ElseIf).Stmts[i] = newBodyStmt
			}
		}
	}
	return newStmt
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

func (s *Case) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, when := range s.CaseWhenList {
			newWhen := when.WalkStmt(visitor)
			if newWhen != when {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Case).CaseWhenList[i] = newWhen.(*CaseWhen)
			}
		}
		if s.HaveElse {
			for i, elseStmt := range s.ElseStmts {
				newElseStmt := elseStmt.WalkStmt(visitor)
				if newElseStmt != elseStmt {
					if newStmt == s {
						newStmt = s.CopyNode()
					}
					newStmt.(*Case).ElseStmts[i] = newElseStmt
				}
			}
		}
	}
	return newStmt
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

func (s *CaseWhen) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, bodyStmt := range s.Stmts {
			newBodyStmt := bodyStmt.WalkStmt(visitor)
			if newBodyStmt != bodyStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*CaseWhen).Stmts[i] = newBodyStmt
			}
		}
	}
	return newStmt
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

func (s *Loop) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, bodyStmt := range s.Body {
			newBodyStmt := bodyStmt.WalkStmt(visitor)
			if newBodyStmt != bodyStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*Loop).Body[i] = newBodyStmt
			}
		}
	}
	return newStmt
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

func (s *While) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, bodyStmt := range s.Body {
			newBodyStmt := bodyStmt.WalkStmt(visitor)
			if newBodyStmt != bodyStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*While).Body[i] = newBodyStmt
			}
		}
	}
	return newStmt
}

// ForLoopControl is an interface covering the loop control structures for the
// integer range, query, and cursor FOR loops.
type ForLoopControl interface {
	isForLoopControl()
	Format(ctx *tree.FmtCtx)
}

type IntForLoopControl struct {
	Reverse bool
	Lower   Expr
	Upper   Expr
	Step    Expr
}

var _ ForLoopControl = &IntForLoopControl{}

func (c *IntForLoopControl) isForLoopControl() {}

func (c *IntForLoopControl) Format(ctx *tree.FmtCtx) {
	if c.Reverse {
		ctx.WriteString("REVERSE ")
	}
	ctx.FormatNode(c.Lower)
	ctx.WriteString("..")
	ctx.FormatNode(c.Upper)
	if c.Step != nil {
		ctx.WriteString(" BY ")
		ctx.FormatNode(c.Step)
	}
}

// stmt_for
type ForLoop struct {
	StatementImpl
	Label   string
	Target  []Variable
	Control ForLoopControl
	Body    []Statement
}

func (s *ForLoop) CopyNode() *ForLoop {
	copyNode := *s
	copyNode.Body = append([]Statement(nil), copyNode.Body...)
	return &copyNode
}

func (s *ForLoop) Format(ctx *tree.FmtCtx) {
	if s.Label != "" {
		ctx.WriteString("<<")
		ctx.FormatNameP(&s.Label)
		ctx.WriteString(">>\n")
	}
	ctx.WriteString("FOR ")
	for i, target := range s.Target {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatName(string(target))
	}
	ctx.WriteString(" IN ")
	ctx.FormatNode(s.Control)
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

func (s *ForLoop) PlpgSQLStatementTag() string {
	switch s.Control.(type) {
	case *IntForLoopControl:
		return "stmt_for_int_loop"
	}
	return "stmt_for_unknown"
}

func (s *ForLoop) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, recurse := visitor.Visit(s)

	if recurse {
		for i, bodyStmt := range s.Body {
			newBodyStmt := bodyStmt.WalkStmt(visitor)
			if newBodyStmt != bodyStmt {
				if newStmt == s {
					newStmt = s.CopyNode()
				}
				newStmt.(*ForLoop).Body[i] = newBodyStmt
			}
		}
	}
	return newStmt
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

func (s *ForEachArray) WalkStmt(visitor StatementVisitor) Statement {
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

func (s *Exit) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Continue) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
}

// stmt_return
type Return struct {
	StatementImpl
	Expr Expr
}

func (s *Return) CopyNode() *Return {
	copyNode := *s
	return &copyNode
}

func (s *Return) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("RETURN")
	if s.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(s.Expr)
	}
	ctx.WriteString(";\n")
}

func (s *Return) PlpgSQLStatementTag() string {
	return "stmt_return"
}

func (s *Return) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *ReturnNext) WalkStmt(visitor StatementVisitor) Statement {
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

func (s *ReturnQuery) WalkStmt(visitor StatementVisitor) Statement {
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
		ctx.WriteString(strings.ToUpper(s.LogLevel))
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

func (s *Raise) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Assert) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Execute) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *DynamicExecute) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Perform) WalkStmt(visitor StatementVisitor) Statement {
	panic(unimplemented.New("plpgsql visitor", "Unimplemented PLpgSQL visitor pattern"))
}

// stmt_call
type Call struct {
	StatementImpl
	Proc *tree.FuncExpr
}

func (s *Call) CopyNode() *Call {
	copyNode := *s
	return &copyNode
}

func (s *Call) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("CALL ")
	ctx.FormatNode(s.Proc)
	ctx.WriteString(";\n")
}

func (s *Call) PlpgSQLStatementTag() string {
	return "stmt_call"
}

func (s *Call) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
}

// stmt_do
type DoBlock struct {
	StatementImpl

	// Block is the code block that defines the logic of the DO statement.
	Block *Block
}

var _ Statement = (*DoBlock)(nil)
var _ tree.DoBlockBody = (*DoBlock)(nil)

func (s *DoBlock) Format(ctx *tree.FmtCtx) {
	ctx.WriteString("DO ")

	// Format the body of the DO block separately so that FormatStringDollarQuotes
	// can examine the resulting string and determine how to quote the block.
	bodyCtx := ctx.Clone()
	bodyCtx.FormatNode(s.Block)
	bodyStr := "\n" + bodyCtx.CloseAndGetString()

	// Avoid replacing the entire formatted string with '_' if any redaction flags
	// are set. They will have already been applied when the body was formatted.
	ctx.WithoutConstantRedaction(func() {
		ctx.FormatStringDollarQuotes(bodyStr)
	})
	ctx.WriteString(";\n")
}

func (s *DoBlock) IsDoBlockBody() {}

func (s *DoBlock) VisitBody(v tree.Visitor) tree.DoBlockBody {
	plVisitor := SQLStmtVisitor{Visitor: v}
	newBlock := Walk(&plVisitor, s.Block)
	if newBlock != s.Block {
		return &DoBlock{Block: newBlock.(*Block)}
	}
	return s
}

func (s *DoBlock) CopyNode() *DoBlock {
	copyNode := *s
	copyNode.Block = s.Block.CopyNode()
	return &copyNode
}

func (s *DoBlock) PlpgSQLStatementTag() string {
	return "stmt_do"
}

func (s *DoBlock) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	newBlock := s.Block.WalkStmt(visitor)
	if newBlock != s.Block {
		if newStmt == s {
			newStmt = s.CopyNode()
		}
		newStmt.(*DoBlock).Block = newBlock.(*Block)
	}
	return newStmt
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
	// TODO(drewk): TargetName is temporary -- should be removed and use Target.
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

func (s *GetDiagnostics) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Open) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Fetch) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Close) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
}

// stmt_commit and stmt_rollback
type TransactionControl struct {
	StatementImpl
	Rollback bool
	Chain    bool
}

func (s *TransactionControl) Format(ctx *tree.FmtCtx) {
	if s.Rollback {
		ctx.WriteString("ROLLBACK")
	} else {
		ctx.WriteString("COMMIT")
	}
	if s.Chain {
		ctx.WriteString(" AND CHAIN")
	}
	ctx.WriteString(";\n")
}

func (s *TransactionControl) PlpgSQLStatementTag() string {
	if s.Rollback {
		return "stmt_rollback"
	}
	return "stmt_commit"
}

func (s *TransactionControl) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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

func (s *Null) WalkStmt(visitor StatementVisitor) Statement {
	newStmt, _ := visitor.Visit(s)
	return newStmt
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
