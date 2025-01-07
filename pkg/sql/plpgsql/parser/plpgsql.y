%{
package parser

import (
  "strings"

  "github.com/cockroachdb/cockroach/pkg/build"
  "github.com/cockroachdb/cockroach/pkg/sql/parser"
  "github.com/cockroachdb/cockroach/pkg/sql/scanner"
  "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
  "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
  "github.com/cockroachdb/cockroach/pkg/sql/types"
  "github.com/cockroachdb/errors"
  "github.com/cockroachdb/redact"
)
%}

%{
func setErr(plpgsqllex plpgsqlLexer, err error) int {
    plpgsqllex.(*lexer).setErr(err)
    return 1
}

func setErrNoDetails(plpgsqllex plpgsqlLexer, err error) int {
    plpgsqllex.(*lexer).setErrNoDetails(err)
    return 1
}

func unimplemented(plpgsqllex plpgsqlLexer, feature string) int {
    plpgsqllex.(*lexer).Unimplemented(feature)
    return 1
}

//functions to cast plpgsqlSymType/sqlSymUnion to other types.
var _ scanner.ScanSymType = &plpgsqlSymType{}

func (s *plpgsqlSymType) ID() int32 {
  return s.id
}

func (s *plpgsqlSymType) SetID(id int32) {
  s.id = id
}

func (s *plpgsqlSymType) Pos() int32 {
  return s.pos
}

func (s *plpgsqlSymType) SetPos(pos int32) {
  s.pos = pos
}

func (s *plpgsqlSymType) Str() string {
  return s.str
}

func (s *plpgsqlSymType) SetStr(str string) {
  s.str = str
}

func (s *plpgsqlSymType) UnionVal() interface{} {
  return s.union.val
}

func (s *plpgsqlSymType) SetUnionVal(val interface{}) {
  s.union.val = val
}

func (s *plpgsqlSymType) plpgsqlScanSymType() {}

type plpgsqlSymUnion struct {
    val interface{}
}

func (u *plpgsqlSymUnion) block() *plpgsqltree.Block {
    return u.val.(*plpgsqltree.Block)
}

func (u *plpgsqlSymUnion) caseWhen() *plpgsqltree.CaseWhen {
    return u.val.(*plpgsqltree.CaseWhen)
}

func (u *plpgsqlSymUnion) caseWhens() []*plpgsqltree.CaseWhen {
    return u.val.([]*plpgsqltree.CaseWhen)
}

func (u *plpgsqlSymUnion) statement() plpgsqltree.Statement {
    return u.val.(plpgsqltree.Statement)
}

func (u *plpgsqlSymUnion) statements() []plpgsqltree.Statement {
    return u.val.([]plpgsqltree.Statement)
}

func (u *plpgsqlSymUnion) int32() int32 {
    return u.val.(int32)
}

func (u *plpgsqlSymUnion) uint32() uint32 {
    return u.val.(uint32)
}

func (u *plpgsqlSymUnion) bool() bool {
    return u.val.(bool)
}

func (u *plpgsqlSymUnion) numVal() *tree.NumVal {
    return u.val.(*tree.NumVal)
}

func (u *plpgsqlSymUnion) typ() tree.ResolvableTypeReference {
    return u.val.(tree.ResolvableTypeReference)
}

func (u *plpgsqlSymUnion) getDiagnosticsKind() plpgsqltree.GetDiagnosticsKind {
    return u.val.(plpgsqltree.GetDiagnosticsKind)
}

func (u *plpgsqlSymUnion) getDiagnosticsItem() *plpgsqltree.GetDiagnosticsItem {
    return u.val.(*plpgsqltree.GetDiagnosticsItem)
}

func (u *plpgsqlSymUnion) getDiagnosticsItemList() plpgsqltree.GetDiagnosticsItemList {
    return u.val.(plpgsqltree.GetDiagnosticsItemList)
}

func (u *plpgsqlSymUnion) elseIf() []plpgsqltree.ElseIf {
    return u.val.([]plpgsqltree.ElseIf)
}

func (u *plpgsqlSymUnion) open() *plpgsqltree.Open {
    return u.val.(*plpgsqltree.Open)
}

func (u *plpgsqlSymUnion) expr() plpgsqltree.Expr {
    if u.val == nil {
        return nil
    }
    return u.val.(plpgsqltree.Expr)
}

func (u *plpgsqlSymUnion) exprs() []plpgsqltree.Expr {
    return u.val.([]plpgsqltree.Expr)
}

func (u *plpgsqlSymUnion) raiseOption() *plpgsqltree.RaiseOption {
    return u.val.(*plpgsqltree.RaiseOption)
}

func (u *plpgsqlSymUnion) raiseOptions() []plpgsqltree.RaiseOption {
    return u.val.([]plpgsqltree.RaiseOption)
}


func (u *plpgsqlSymUnion) exception() *plpgsqltree.Exception {
    return u.val.(*plpgsqltree.Exception)
}

func (u *plpgsqlSymUnion) exceptions() []plpgsqltree.Exception {
    return u.val.([]plpgsqltree.Exception)
}

func (u *plpgsqlSymUnion) condition() *plpgsqltree.Condition {
    return u.val.(*plpgsqltree.Condition)
}

func (u *plpgsqlSymUnion) conditions() []plpgsqltree.Condition {
    return u.val.([]plpgsqltree.Condition)
}

func (u *plpgsqlSymUnion) cursorScrollOption() tree.CursorScrollOption {
    return u.val.(tree.CursorScrollOption)
}

func (u *plpgsqlSymUnion) sqlStatement() tree.Statement {
    return u.val.(tree.Statement)
}

func (u *plpgsqlSymUnion) variables() []plpgsqltree.Variable {
		return u.val.([]plpgsqltree.Variable)
}

func (u *plpgsqlSymUnion) forLoopControl() plpgsqltree.ForLoopControl {
		return u.val.(plpgsqltree.ForLoopControl)
}

func (u *plpgsqlSymUnion) doBlockOptions() tree.DoBlockOptions {
    return u.val.(tree.DoBlockOptions)
}

func (u *plpgsqlSymUnion) doBlockOption() tree.DoBlockOption {
    return u.val.(tree.DoBlockOption)
}

%}
/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str> IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
%token <*tree.NumVal>	ICONST PARAM
%token <str> TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token <str> LESS_EQUALS GREATER_EQUALS NOT_EQUALS

/*
 * Other tokens recognized by plpgsql's lexer interface layer (pl_scanner.c).
 */
%token <str> LESS_LESS GREATER_GREATER

/*
 * Keyword tokens.  Some of these are reserved and some are not;
 * see pl_scanner.c for info.  Be sure unreserved keywords are listed
 * in the "unreserved_keyword" production below.
 */
%token <str>  ABSOLUTE
%token <str>  ALIAS
%token <str>  ALL
%token <str>  AND
%token <str>  ARRAY
%token <str>  ASSERT
%token <str>  BACKWARD
%token <str>  BEGIN
%token <str>  BY
%token <str>  CALL
%token <str>  CASE
%token <str>  CHAIN
%token <str>  CLOSE
%token <str>  COLLATE
%token <str>  COLUMN
%token <str>  COLUMN_NAME
%token <str>  COMMIT
%token <str>  CONSTANT
%token <str>  CONSTRAINT
%token <str>  CONSTRAINT_NAME
%token <str>  CONTINUE
%token <str>  CURRENT
%token <str>  CURSOR
%token <str>  DATATYPE
%token <str>  DEBUG
%token <str>  DECLARE
%token <str>  DEFAULT
%token <str>  DETAIL
%token <str>  DIAGNOSTICS
%token <str>  DO
%token <str>  DUMP
%token <str>  ELSE
%token <str>  ELSIF
%token <str>  END
%token <str>  END_CASE
%token <str>  END_IF
%token <str>  ERRCODE
%token <str>  ERROR
%token <str>  EXCEPTION
%token <str>  EXECUTE
%token <str>  EXIT
%token <str>  FETCH
%token <str>  FIRST
%token <str>  FOR
%token <str>  FOREACH
%token <str>  FORWARD
%token <str>  FROM
%token <str>  GET
%token <str>  HINT
%token <str>  IF
%token <str>  IMPORT
%token <str>  IN
%token <str>  INFO
%token <str>  INSERT
%token <str>  INTO
%token <str>  IS
%token <str>  LANGUAGE
%token <str>  LAST
%token <str>  LOG
%token <str>  LOOP
%token <str>  MERGE
%token <str>  MESSAGE
%token <str>  MESSAGE_TEXT
%token <str>  MOVE
%token <str>  NEXT
%token <str>  NO
%token <str>  NO_SCROLL
%token <str>  NOT
%token <str>  NOTICE
%token <str>  NULL
%token <str>  OPEN
%token <str>  OPTION
%token <str>  OR
%token <str>  PERFORM
%token <str>  PG_CONTEXT
%token <str>  PG_DATATYPE_NAME
%token <str>  PG_EXCEPTION_CONTEXT
%token <str>  PG_EXCEPTION_DETAIL
%token <str>  PG_EXCEPTION_HINT
%token <str>  PRINT_STRICT_PARAMS
%token <str>  PRIOR
%token <str>  QUERY
%token <str>  RAISE
%token <str>  RELATIVE
%token <str>  RETURN
%token <str>  RETURN_NEXT
%token <str>  RETURN_QUERY
%token <str>  RETURNED_SQLSTATE
%token <str>  REVERSE
%token <str>  ROLLBACK
%token <str>  ROW_COUNT
%token <str>  ROWTYPE
%token <str>  SCHEMA
%token <str>  SCHEMA_NAME
%token <str>  SCROLL
%token <str>  SLICE
%token <str>  SQLSTATE
%token <str>  STACKED
%token <str>  STRICT
%token <str>  TABLE
%token <str>  TABLE_NAME
%token <str>  THEN
%token <str>  TO
%token <str>  TYPE
%token <str>  UPSERT
%token <str>  USE_COLUMN
%token <str>  USE_VARIABLE
%token <str>  USING
%token <str>  VARIABLE_CONFLICT
%token <str>  WARNING
%token <str>  WHEN
%token <str>  WHILE


%union {
  id    int32
  pos   int32
  str   string
  union plpgsqlSymUnion
}

%type <str> decl_varname decl_defkey
%type <bool>	decl_const decl_notnull
%type <plpgsqltree.Expr>	decl_defval decl_cursor_query
%type <tree.ResolvableTypeReference>	decl_datatype
%type <str>		decl_collate

%type <str>	expr_until_semi expr_until_paren stmt_until_semi return_expr
%type <str>	expr_until_then expr_until_loop opt_expr_until_when
%type <plpgsqltree.Expr>	opt_exitcond

%type <[]plpgsqltree.Variable> for_target
%type <*tree.NumVal> foreach_slice
%type <plpgsqltree.ForLoopControl> for_control

%type <str> any_identifier opt_block_label opt_loop_label opt_label query_options
%type <str> opt_error_level option_type

%type <tree.DoBlockOptions> do_stmt_opt_list
%type <tree.DoBlockOption> do_stmt_opt_item

%type <[]plpgsqltree.Statement> proc_sect
%type <[]plpgsqltree.ElseIf> stmt_elsifs
%type <[]plpgsqltree.Statement> stmt_else loop_body
%type <plpgsqltree.Statement>  pl_block
%type <plpgsqltree.Statement>	proc_stmt
%type <plpgsqltree.Statement>	stmt_assign stmt_if stmt_loop stmt_while stmt_exit stmt_continue
%type <plpgsqltree.Statement>	stmt_return stmt_raise stmt_assert stmt_execsql
%type <plpgsqltree.Statement>	stmt_dynexecute stmt_for stmt_perform stmt_call stmt_do stmt_getdiag
%type <plpgsqltree.Statement>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <plpgsqltree.Statement>	stmt_commit stmt_rollback
%type <plpgsqltree.Statement>	stmt_case stmt_foreach_a

%type <plpgsqltree.Statement> decl_statement
%type <[]plpgsqltree.Statement> decl_sect opt_decl_stmts decl_stmts

%type <[]plpgsqltree.Exception> exception_sect proc_exceptions
%type <*plpgsqltree.Exception>	proc_exception
%type <[]plpgsqltree.Condition> proc_conditions
%type <*plpgsqltree.Condition> proc_condition

%type <*plpgsqltree.CaseWhen>	case_when
%type <[]*plpgsqltree.CaseWhen>	case_when_list
%type <[]plpgsqltree.Statement> opt_case_else

%type <bool>	getdiag_area_opt
%type <plpgsqltree.GetDiagnosticsItemList>	getdiag_list
%type <*plpgsqltree.GetDiagnosticsItem> getdiag_list_item
%type <int32> getdiag_item

%type <*plpgsqltree.RaiseOption> option_expr
%type <[]plpgsqltree.RaiseOption> option_exprs opt_option_exprs
%type <plpgsqltree.Expr> format_expr
%type <[]plpgsqltree.Expr> opt_format_exprs format_exprs

%type <tree.CursorScrollOption>	opt_scrollable

%type <bool>	opt_transaction_chain

%type <str>	unreserved_keyword
%%

pl_function:
  pl_block opt_semi
  {
    plpgsqllex.(*lexer).SetStmt($1.statement())
  }

opt_semi:
| ';'
;

pl_block: opt_block_label decl_sect BEGIN proc_sect exception_sect END opt_label
  {
    blockLabel, blockEndLabel := $1, $7
    if err := checkLoopLabels(blockLabel, blockEndLabel); err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.Block{
      Label: $1,
      Decls: $2.statements(),
      Body: $4.statements(),
      Exceptions: $5.exceptions(),
    }
  }
;

decl_sect: DECLARE opt_decl_stmts
  {
    $$.val = $2.statements()
  }
| /* EMPTY */
  {
    // Use a nil slice to indicate DECLARE was not used.
    $$.val = []plpgsqltree.Statement(nil)
  }
;

opt_decl_stmts: decl_stmts
  {
    $$.val = $1.statements()
  }
| /* EMPTY */
  {
    $$.val = []plpgsqltree.Statement{}
  }
;

opt_declare: DECLARE {}
| {}
;

decl_stmts: decl_stmts opt_declare decl_statement
  {
    decs := $1.statements()
    dec := $3.statement()
    $$.val = append(decs, dec)
  }
| decl_statement
  {
    dec := $1.statement()
    $$.val = []plpgsqltree.Statement{dec}
	}
;

decl_statement: decl_varname decl_const decl_datatype decl_collate decl_notnull decl_defval
  {
    $$.val = &plpgsqltree.Declaration{
      Var: plpgsqltree.Variable($1),
      Constant: $2.bool(),
      Typ: $3.typ(),
      Collate: $4,
      NotNull: $5.bool(),
      Expr: $6.expr(),
    }
  }
| decl_varname ALIAS FOR decl_aliasitem ';'
  {
    return unimplemented(plpgsqllex, "alias for")
  }
| decl_varname opt_scrollable CURSOR decl_cursor_args decl_is_for decl_cursor_query
  {
    $$.val = &plpgsqltree.CursorDeclaration{
      Name: plpgsqltree.Variable($1),
      Scroll: $2.cursorScrollOption(),
      Query: $6.sqlStatement(),
    }
  }
;

opt_scrollable:
  {
    $$.val = tree.UnspecifiedScroll
  }
| NO_SCROLL SCROLL
  {
    $$.val = tree.NoScroll
  }
| SCROLL
  {
    $$.val = tree.Scroll
  }
;

decl_cursor_query: stmt_until_semi ';'
  {
    stmts, err := parser.Parse($1)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    if len(stmts) != 1 {
      return setErr(plpgsqllex, errors.New("expected exactly one SQL statement for cursor"))
    }
    $$.val = stmts[0].AST
  }
;

decl_cursor_args: '('
  {
    return unimplemented(plpgsqllex, "cursor arguments")
  }
| /* EMPTY */
  {
  }
;

decl_cursor_arglist: decl_cursor_arg
  {
  }
| decl_cursor_arglist ',' decl_cursor_arg
  {
  }
;

decl_cursor_arg: decl_varname decl_datatype
  {
  }
;

decl_is_for:
  IS   /* Oracle */
| FOR  /* SQL standard */

decl_aliasitem: IDENT
  {
  }
| unreserved_keyword
  {
  }
;

decl_varname: IDENT
| unreserved_keyword
;

decl_const:
  {
    $$.val = false
  }
| CONSTANT
  {
    $$.val = true
  }
;

decl_datatype:
  {
    // Read until reaching one of the tokens that can follow a declaration
    // data type.
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(
      ';', COLLATE, NOT, '=', COLON_EQUALS, DECLARE,
    )
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    // This is an inlined version of GetTypeFromValidSQLSyntax which doesn't
    // return an assertion failure.
    castExpr, err := plpgsqllex.(*lexer).ParseExpr("1::" + sqlStr)
    if err != nil {
      return setErr(plpgsqllex, errors.New("unable to parse type of variable declaration"))
    }
    switch t := castExpr.(type) {
    case *tree.CollateExpr:
      $$.val = types.MakeCollatedString(types.String, t.Locale)
    case *tree.CastExpr:
      $$.val = t.Type
    default:
      err := errors.New("unable to parse type of variable declaration")
      if strings.Contains(sqlStr, "%") {
        err = errors.WithIssueLink(errors.WithHint(err,
          "you may have attempted to use %TYPE or %ROWTYPE syntax, which is unsupported.",
        ), errors.IssueLink{IssueURL: build.MakeIssueURL(114676)})
      }
      return setErr(plpgsqllex, err)
    }
  }
;

decl_collate:
  {
    $$ = ""
  }
| COLLATE IDENT
  {
    $$ = $2
  }
| COLLATE unreserved_keyword
  {
    $$ = $2
  }
;

decl_notnull:
  {
    $$.val = false
  }
| NOT NULL
  {
    $$.val = true
  }
;

decl_defval: ';'
  {
    $$.val = (plpgsqltree.Expr)(nil)
  }
| decl_defkey ';'
  {
    expr, err := plpgsqllex.(*lexer).ParseExpr($1)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = expr
  }
;

decl_defkey: assign_operator
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
| DEFAULT
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

/*
 * Ada-based PL/SQL uses := for assignment and variable defaults, while
 * the SQL standard uses equals for these cases and for GET
 * DIAGNOSTICS, so we support both.  FOR and OPEN only support :=.
 */
assign_operator: '='
| COLON_EQUALS
;

proc_sect:
  {
    $$.val = []plpgsqltree.Statement{}
  }
| proc_sect proc_stmt
  {
    stmts := $1.statements()
    stmts = append(stmts, $2.statement())
    $$.val = stmts
  }
;

proc_stmt:pl_block ';'
  {
    $$.val = $1.block()
  }
| stmt_assign
  {
    $$.val = $1.statement()
  }
| stmt_if
  {
    $$.val = $1.statement()
  }
| stmt_case
  {
    $$.val = $1.statement()
  }
| stmt_loop
  {
    $$.val = $1.statement()
  }
| stmt_while
  {
    $$.val = $1.statement()
  }
| stmt_for
  { }
| stmt_foreach_a
  { }
| stmt_exit
  {
    $$.val = $1.statement()
  }
| stmt_continue
  {
    $$.val = $1.statement()
  }
| stmt_return
  {
    $$.val = $1.statement()
  }
| stmt_raise
  {
    $$.val = $1.statement()
  }
| stmt_assert
  {
    $$.val = $1.statement()
  }
| stmt_execsql
  {
    $$.val = $1.statement()
  }
| stmt_dynexecute
  {
    $$.val = $1.statement()
  }
| stmt_perform
  { }
| stmt_call
  {
    $$.val = $1.statement()
  }
| stmt_do
  {
    $$.val = $1.statement()
  }
| stmt_getdiag
  { }
| stmt_open
  {
    $$.val = $1.statement()
  }
| stmt_fetch
  {
    $$.val = $1.statement()
  }
| stmt_move
  {
    $$.val = $1.statement()
  }
| stmt_close
  {
    $$.val = $1.statement()
  }
| stmt_null
  {
    $$.val = $1.statement()
  }
| stmt_commit
  {
    $$.val = $1.statement()
  }
| stmt_rollback
  {
    $$.val = $1.statement()
  }
;

stmt_perform: PERFORM stmt_until_semi ';'
  {
    return unimplemented(plpgsqllex, "perform")
  }
;

stmt_call: CALL expr_until_semi ';'
  {
    expr, err := plpgsqllex.(*lexer).ParseExpr($2)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    proc, ok := expr.(*tree.FuncExpr)
    if !ok {
      return setErr(plpgsqllex,
        errors.New("CALL statement target must be a stored procedure"),
      )
    }
    // Set the InCall flag to true to indicate that this is a CALL statement.
    proc.InCall = true
    $$.val = &plpgsqltree.Call{Proc: proc}
  }
;

stmt_do:
  DO do_stmt_opt_list ';'
  {
    doBlock, err := makeDoStmt($2.doBlockOptions())
    if err != nil {
      return setErrNoDetails(plpgsqllex, err)
    }
    $$.val = doBlock
  }
;

do_stmt_opt_list:
  do_stmt_opt_item
  {
    $$.val = tree.DoBlockOptions{$1.doBlockOption()}
  }
| do_stmt_opt_list do_stmt_opt_item
  {
    $$.val = append($1.doBlockOptions(), $2.doBlockOption())
  }
;

do_stmt_opt_item:
  SCONST
  {
    $$.val = tree.RoutineBodyStr($1)
  }
| LANGUAGE any_identifier
  {
    lang, err := tree.AsRoutineLanguage($2)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = lang
  }
;

stmt_assign: IDENT assign_operator expr_until_semi ';'
  {
    expr, err := plpgsqllex.(*lexer).ParseExpr($3)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.Assignment{
      Var: plpgsqltree.Variable($1),
      Value: expr,
    }
  }
| IDENT '.' IDENT assign_operator expr_until_semi ';'
  {
    // TODO(#91779, #122322): allow arbitrary nesting of indirection.
    expr, err := plpgsqllex.(*lexer).ParseExpr($5)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.Assignment{
      Var: plpgsqltree.Variable($1),
      Value: expr,
      Indirection: tree.Name($3),
    }
  }
;

stmt_getdiag: GET getdiag_area_opt DIAGNOSTICS getdiag_list ';'
  {
  $$.val = &plpgsqltree.GetDiagnostics{
    IsStacked: $2.bool(),
    DiagItems: $4.getDiagnosticsItemList(),
  }
  // TODO(drewk): Check information items are valid for area option.
  }
;

getdiag_area_opt:
  {
    $$.val = false
  }
| CURRENT
  {
    $$.val = false
  }
| STACKED
  {
    $$.val = true
  }
;

getdiag_list: getdiag_list ',' getdiag_list_item
  {
    $$.val = append($1.getDiagnosticsItemList(), $3.getDiagnosticsItem())
  }
| getdiag_list_item
  {
    $$.val = plpgsqltree.GetDiagnosticsItemList{$1.getDiagnosticsItem()}
  }
;

getdiag_list_item: IDENT assign_operator getdiag_item
  {
    $$.val = &plpgsqltree.GetDiagnosticsItem{
      Kind : $3.getDiagnosticsKind(),
      TargetName: $1,
      // TODO(drewk): set the target from $1.
    }
  }
;

getdiag_item: unreserved_keyword {
  switch $1 {
    case "row_count":
      $$.val = plpgsqltree.GetDiagnosticsRowCount;
    case "pg_context":
      $$.val = plpgsqltree.GetDiagnosticsContext;
    case "pg_exception_detail":
      $$.val = plpgsqltree.GetDiagnosticsErrorDetail;
    case "pg_exception_hint":
      $$.val = plpgsqltree.GetDiagnosticsErrorHint;
    case "pg_exception_context":
      $$.val = plpgsqltree.GetDiagnosticsErrorContext;
    case "column_name":
      $$.val = plpgsqltree.GetDiagnosticsColumnName;
    case "constraint_name":
      $$.val = plpgsqltree.GetDiagnosticsConstraintName;
    case "pg_datatype_name":
      $$.val = plpgsqltree.GetDiagnosticsDatatypeName;
    case "message_text":
      $$.val = plpgsqltree.GetDiagnosticsMessageText;
    case "table_name":
      $$.val = plpgsqltree.GetDiagnosticsTableName;
    case "schema_name":
      $$.val = plpgsqltree.GetDiagnosticsSchemaName;
    case "returned_sqlstate":
      $$.val = plpgsqltree.GetDiagnosticsReturnedSQLState;
    default:
      // TODO(drewk): Should this use an unimplemented error instead?
      return setErr(plpgsqllex, errors.Newf("unrecognized GET DIAGNOSTICS item: %s", redact.Safe($1)))
  }
}
;

getdiag_target:
// TODO(drewk): remove ident.
IDENT
  {
  }
;

stmt_if: IF expr_until_then THEN proc_sect stmt_elsifs stmt_else END_IF IF ';'
  {
    cond, err := plpgsqllex.(*lexer).ParseExpr($2)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.If{
      Condition: cond,
      ThenBody: $4.statements(),
      ElseIfList: $5.elseIf(),
      ElseBody: $6.statements(),
    }
  }
;

stmt_elsifs:
  {
    $$.val = []plpgsqltree.ElseIf{};
  }
| stmt_elsifs ELSIF expr_until_then THEN proc_sect
  {
    cond, err := plpgsqllex.(*lexer).ParseExpr($3)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    newStmt := plpgsqltree.ElseIf{
      Condition: cond,
      Stmts: $5.statements(),
    }
    $$.val = append($1.elseIf() , newStmt)
  }
;

stmt_else:
  {
    $$.val = []plpgsqltree.Statement{};
  }
| ELSE proc_sect
  {
    $$.val = $2.statements();
  }
;

stmt_case: CASE opt_expr_until_when case_when_list opt_case_else END_CASE CASE ';'
  {
    expr := &plpgsqltree.Case {
      TestExpr: $2,
      CaseWhenList: $3.caseWhens(),
    }
    if $4.val != nil {
       expr.HaveElse = true
       expr.ElseStmts = $4.statements()
    }
    $$.val = expr
  }
;

opt_expr_until_when:
  {
    if plpgsqllex.(*lexer).Peek().id != WHEN {
      sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(WHEN)
      if err != nil {
        return setErr(plpgsqllex, err)
      }
      $$ = sqlStr
    } else {
      $$ = ""
    }
  }
;

case_when_list: case_when_list case_when
  {
    stmts := $1.caseWhens()
    stmts = append(stmts, $2.caseWhen())
    $$.val = stmts
  }
| case_when
  {
    stmts := []*plpgsqltree.CaseWhen{}
    stmts = append(stmts, $1.caseWhen())
    $$.val = stmts
  }
;

case_when: WHEN expr_until_then THEN proc_sect
  {
     expr := &plpgsqltree.CaseWhen{
       Expr: $2,
       Stmts: $4.statements(),
     }
     $$.val = expr
  }
;

opt_case_else:
  {
    $$.val = nil
  }
| ELSE proc_sect
  {
    $$.val = $2.statements()
  }
;

stmt_loop: opt_loop_label LOOP loop_body opt_label ';'
  {
    loopLabel, loopEndLabel := $1, $4
    if err := checkLoopLabels(loopLabel, loopEndLabel); err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.Loop{
      Label: loopLabel,
      Body: $3.statements(),
    }
  }
;

stmt_while: opt_loop_label WHILE expr_until_loop LOOP loop_body opt_label ';'
  {
    loopLabel, loopEndLabel := $1, $6
    if err := checkLoopLabels(loopLabel, loopEndLabel); err != nil {
      return setErr(plpgsqllex, err)
    }
    cond, err := plpgsqllex.(*lexer).ParseExpr($3)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.While{
      Label: loopLabel,
      Condition: cond,
      Body: $5.statements(),
    }
  }
;

stmt_for: opt_loop_label FOR for_target IN for_control loop_body opt_label ';'
  {
    loopLabel, loopEndLabel := $1, $7
    if err := checkLoopLabels(loopLabel, loopEndLabel); err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.ForLoop{
			Label: loopLabel,
			Target: $3.variables(),
			Control: $5.forLoopControl(),
			Body: $6.statements(),
		}
  }
;

for_control:
  {
	  foundToken, err := plpgsqllex.(*lexer).findFirstOccurrence(DOT_DOT, LOOP)
	  if err != nil {
	    return setErr(plpgsqllex, err)
	  }
	  switch foundToken {
	  case DOT_DOT:
	    // This is an iteration over an integer range.
	    forLoopControl, err := plpgsqllex.(*lexer).ReadIntegerForLoopControl()
	    if err != nil {
	      return setErr(plpgsqllex, err)
	    }
	    $$.val = forLoopControl
	  case LOOP:
	    return unimplemented(plpgsqllex, "for loop over query or cursor")
	  default:
	    return setErr(plpgsqllex, errors.New("unterminated FOR loop definition"))
	  }
  }
;

for_target:
  {
    target, err := plpgsqllex.(*lexer).ReadTarget()
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = target
  }
;

stmt_foreach_a: opt_loop_label FOREACH
  {
    return unimplemented(plpgsqllex, "for each loop")
  }
;

foreach_slice:
  {
  }
| SLICE ICONST
  {
  }
;

stmt_exit: EXIT opt_label opt_exitcond
  {
    $$.val = &plpgsqltree.Exit{
      Label: $2,
      Condition: $3.expr(),
    }
  }
;

stmt_continue: CONTINUE opt_label opt_exitcond
  {
    $$.val = &plpgsqltree.Continue{
      Label: $2,
      Condition: $3.expr(),
    }
  }
;

stmt_return: RETURN return_expr ';'
  {
    var expr plpgsqltree.Expr
    if $2 != "" {
      var err error
      expr, err = plpgsqllex.(*lexer).ParseExpr($2)
      if err != nil {
        return setErr(plpgsqllex, err)
      }
    }
    $$.val = &plpgsqltree.Return{Expr: expr}
  }
| RETURN_NEXT NEXT
  {
    return unimplemented(plpgsqllex, "return next")
  }
| RETURN_QUERY QUERY
 {
   return unimplemented (plpgsqllex, "return query")
 }
;

return_expr:
  {
    sqlStr, err := plpgsqllex.(*lexer).ReadReturnExpr()
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

query_options:
  {
    _, terminator, err := plpgsqllex.(*lexer).ReadSqlExpr(EXECUTE, ';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    if terminator == EXECUTE {
      return unimplemented (plpgsqllex, "return dynamic sql query")
    }
    _, _, err = plpgsqllex.(*lexer).ReadSqlExpr(';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
  }
;

stmt_raise:
  RAISE ';'
  {
    return unimplemented(plpgsqllex, "empty RAISE statement")
  }
| RAISE opt_error_level SCONST opt_format_exprs opt_option_exprs ';'
  {
    $$.val = &plpgsqltree.Raise{
      LogLevel: $2,
      Message: $3,
      Params: $4.exprs(),
      Options: $5.raiseOptions(),
    }
  }
| RAISE opt_error_level IDENT opt_option_exprs ';'
  {
    $$.val = &plpgsqltree.Raise{
      LogLevel: $2,
      CodeName: $3,
      Options: $4.raiseOptions(),
    }
  }
| RAISE opt_error_level SQLSTATE SCONST opt_option_exprs ';'
  {
    $$.val = &plpgsqltree.Raise{
      LogLevel: $2,
      Code: $4,
      Options: $5.raiseOptions(),
    }
  }
| RAISE opt_error_level USING option_exprs ';'
  {
    $$.val = &plpgsqltree.Raise{
      LogLevel: $2,
      Options: $4.raiseOptions(),
    }
  }
;

opt_error_level:
  DEBUG
| LOG
| INFO
| NOTICE
| WARNING
| EXCEPTION
| /* EMPTY */
  {
    $$ = ""
  }
;

opt_option_exprs:
  USING option_exprs
  {
    $$.val = $2.raiseOptions()
  }
| /* EMPTY */
  {
    $$.val = []plpgsqltree.RaiseOption{}
  }
;

option_exprs:
  option_exprs ',' option_expr
  {
    option := $3.raiseOption()
    $$.val = append($1.raiseOptions(), *option)
  }
| option_expr
  {
    option := $1.raiseOption()
    $$.val = []plpgsqltree.RaiseOption{*option}
  }
;

option_expr:
  option_type assign_operator
  {
    // Read until reaching one of the tokens that can follow a raise option.
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(',', ';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    optionExpr, err := plpgsqllex.(*lexer).ParseExpr(sqlStr)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = &plpgsqltree.RaiseOption{
      OptType: $1,
      Expr: optionExpr,
    }
  }
;

option_type:
  MESSAGE
| DETAIL
| HINT
| ERRCODE
| COLUMN
| CONSTRAINT
| DATATYPE
| TABLE
| SCHEMA
;

opt_format_exprs:
  format_exprs
  {
    $$.val = $1.exprs()
  }
 | /* EMPTY */
  {
    $$.val = []plpgsqltree.Expr{}
  }
;

format_exprs:
  format_expr
  {
    $$.val = []plpgsqltree.Expr{$1.expr()}
  }
| format_exprs format_expr
  {
    $$.val = append($1.exprs(), $2.expr())
  }
;

format_expr: ','
  {
    // Read until reaching a token that can follow a raise format parameter.
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(',', ';', USING)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    param, err := plpgsqllex.(*lexer).ParseExpr(sqlStr)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = param
  }
;

stmt_assert: ASSERT assert_cond ';'
  {
    $$.val = &plpgsqltree.Assert{}
  }
;

assert_cond:
  {
    _, terminator, err := plpgsqllex.(*lexer).ReadSqlExpr(',', ';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    if terminator == ',' {
      _, _, err = plpgsqllex.(*lexer).ReadSqlExpr(';')
      if err != nil {
        return setErr(plpgsqllex, err)
      }
    }
  }
;

loop_body: proc_sect END LOOP
  {
    $$.val = $1.statements()
  }
;

stmt_execsql: stmt_execsql_start
  {
    stmt, err := plpgsqllex.(*lexer).MakeExecSqlStmt()
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = stmt
  }
;

stmt_execsql_start:
  IMPORT
| INSERT
| UPSERT
| MERGE
| IDENT
;

stmt_dynexecute: EXECUTE
  {
    stmt, err := plpgsqllex.(*lexer).MakeDynamicExecuteStmt()
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = stmt
  }
;

stmt_open: OPEN IDENT ';'
  {
    $$.val = &plpgsqltree.Open{CurVar: plpgsqltree.Variable($2)}
  }
| OPEN IDENT opt_scrollable FOR EXECUTE 
  {
    return unimplemented(plpgsqllex, "cursor for execute")
  }
| OPEN IDENT opt_scrollable FOR stmt_until_semi ';'
  {
    stmts, err := parser.Parse($5)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    if len(stmts) != 1 {
      return setErr(plpgsqllex, errors.New("expected exactly one SQL statement for cursor"))
    }
    $$.val = &plpgsqltree.Open{
      CurVar: plpgsqltree.Variable($2),
      Scroll: $3.cursorScrollOption(),
      Query: stmts[0].AST,
    }
  }
;

stmt_fetch: FETCH
  {
    fetch, err := plpgsqllex.(*lexer).MakeFetchOrMoveStmt(false)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = fetch
  }
;

stmt_move: MOVE
  {
    move, err := plpgsqllex.(*lexer).MakeFetchOrMoveStmt(true)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = move
  }
;

stmt_close: CLOSE IDENT ';'
  {
    $$.val = &plpgsqltree.Close{CurVar: plpgsqltree.Variable($2)}
  }
;

stmt_null: NULL ';'
  {
    $$.val = &plpgsqltree.Null{};
  }
;

stmt_commit: COMMIT opt_transaction_chain ';'
  {
    $$.val = &plpgsqltree.TransactionControl{Chain: $2.bool()}
  }
;

stmt_rollback: ROLLBACK opt_transaction_chain ';'
  {
    $$.val = &plpgsqltree.TransactionControl{Chain: $2.bool(), Rollback: true}
  }
;

opt_transaction_chain:
AND CHAIN
  {
    $$.val = true
  }
| AND NO CHAIN
  {
    $$.val = false
  }
| /* EMPTY */
  {
    $$.val = false
  }

exception_sect: /* EMPTY */
  {
    $$.val = []plpgsqltree.Exception(nil)
  }
| EXCEPTION proc_exceptions
  {
    $$.val = $2.exceptions()
  }
;

proc_exceptions: proc_exceptions proc_exception
  {
    e := $2.exception()
    $$.val = append($1.exceptions(), *e)
  }
| proc_exception
  {
    e := $1.exception()
    $$.val = []plpgsqltree.Exception{*e}
  }
;

proc_exception: WHEN proc_conditions THEN proc_sect
  {
    $$.val = &plpgsqltree.Exception{
      Conditions: $2.conditions(),
      Action: $4.statements(),
    }
  }
;

proc_conditions: proc_conditions OR proc_condition
  {
    c := $3.condition()
    $$.val = append($1.conditions(), *c)
  }
| proc_condition
  {
    c := $1.condition()
    $$.val = []plpgsqltree.Condition{*c}
  }
;

proc_condition: any_identifier
  {
    $$.val = &plpgsqltree.Condition{SqlErrName: $1}
  }
| SQLSTATE SCONST
  {
    $$.val = &plpgsqltree.Condition{SqlErrState: $2}
  }
;

expr_until_semi:
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

stmt_until_semi:
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlStatement(';')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

expr_until_then:
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(THEN)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

expr_until_loop:
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(LOOP)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

expr_until_paren :
  {
    sqlStr, _, err := plpgsqllex.(*lexer).ReadSqlExpr(')')
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$ = sqlStr
  }
;

opt_block_label	:
  {
    $$ = ""
  }
| LESS_LESS any_identifier GREATER_GREATER
  {
    $$ = $2
  }
;

opt_loop_label:
  {
    $$ = ""
  }
| LESS_LESS any_identifier GREATER_GREATER
  {
    $$ = $2
  }
;

opt_label:
  {
    $$ = ""
  }
| any_identifier
  {
    $$ = $1
  }
;

opt_exitcond: ';'
  { }
| WHEN expr_until_semi ';'
  {
    expr, err := plpgsqllex.(*lexer).ParseExpr($2)
    if err != nil {
      return setErr(plpgsqllex, err)
    }
    $$.val = expr
  }
;

any_identifier:
  IDENT
| unreserved_keyword
;

unreserved_keyword:
  ABSOLUTE
| ALIAS
| AND
| ARRAY
| ASSERT
| BACKWARD
| CALL
| CHAIN
| CLOSE
| COLLATE
| COLUMN
| COLUMN_NAME
| COMMIT
| CONSTANT
| CONSTRAINT
| CONSTRAINT_NAME
| CONTINUE
| CURRENT
| CURSOR
| DATATYPE
| DEBUG
| DEFAULT
| DETAIL
| DIAGNOSTICS
| DO
| DUMP
| ELSIF
| ERRCODE
| ERROR
| EXCEPTION
| EXIT
| FETCH
| FIRST
| FORWARD
| GET
| HINT
| IMPORT
| INFO
| INSERT
| IS
| LANGUAGE
| LAST
| LOG
| MERGE
| MESSAGE
| MESSAGE_TEXT
| MOVE
| NEXT
| NO
| NO_SCROLL
| NOTICE
| OPEN
| OPTION
| PERFORM
| PG_CONTEXT
| PG_DATATYPE_NAME
| PG_EXCEPTION_CONTEXT
| PG_EXCEPTION_DETAIL
| PG_EXCEPTION_HINT
| PRINT_STRICT_PARAMS
| PRIOR
| QUERY
| RAISE
| RELATIVE
| RETURN
| RETURN_NEXT
| RETURN_QUERY
| RETURNED_SQLSTATE
| REVERSE
| ROLLBACK
| ROW_COUNT
| ROWTYPE
| SCHEMA
| SCHEMA_NAME
| SCROLL
| SLICE
| SQLSTATE
| STACKED
| TABLE
| TABLE_NAME
| TYPE
| UPSERT
| USE_COLUMN
| USE_VARIABLE
| VARIABLE_CONFLICT
| WARNING

reserved_keyword:
  ALL
| BEGIN
| BY
| CASE
| DECLARE
| ELSE
| END
| END_CASE
| END_IF
| EXECUTE
| FOR
| FOREACH
| FROM
| IF
| IN
| INTO
| LOOP
| NOT
| NULL
| OR
| STRICT
| THEN
| TO
| USING
| WHEN
| WHILE

%%
