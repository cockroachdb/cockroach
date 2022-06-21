%{
package parser

import (
  "github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
  "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
  "github.com/cockroachdb/cockroach/pkg/sql/types"
  "github.com/lib/pq/oid"
)
%}

%{
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

func (u *plpgsqlSymUnion) plpgsqlStmtBlock() *plpgsqltree.PLpgSQLStmtBlock {
    return u.val.(*plpgsqltree.PLpgSQLStmtBlock)
}

func (u *plpgsqlSymUnion) plpgsqlStatement() plpgsqltree.PLpgSQLStatement {
    return u.val.(plpgsqltree.PLpgSQLStatement)
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
// TODO (Chengxiong) figure out why these 3 tokens are needed
%token <plWord>		T_WORD		/* unrecognized simple identifier */
%token <plCWord>		T_CWORD		/* unrecognized composite identifier */
%token <plWDatum>		T_DATUM		/* a VAR, ROW, REC, or RECFIELD variable */
%token <str> LESS_LESS GREATER_GREATER

/*
 * Keyword tokens.  Some of these are reserved and some are not;
 * see pl_scanner.c for info.  Be sure unreserved keywords are listed
 * in the "unreserved_keyword" production below.
 */
%token <str>	ABSOLUTE
%token <str>	ALIAS
%token <str>	ALL
%token <str>	AND
%token <str>	ARRAY
%token <str>	ASSERT
%token <str>	BACKWARD
%token <str>	BEGIN
%token <str>	BY
%token <str>	CALL
%token <str>	CASE
%token <str>	CHAIN
%token <str>	CLOSE
%token <str>	COLLATE
%token <str>	COLUMN
%token <str>	COLUMN_NAME
%token <str>	COMMIT
%token <str>	CONSTANT
%token <str>	CONSTRAINT
%token <str>	CONSTRAINT_NAME
%token <str>	CONTINUE
%token <str>	CURRENT
%token <str>	CURSOR
%token <str>	DATATYPE
%token <str>	DEBUG
%token <str>	DECLARE
%token <str>	DEFAULT
%token <str>	DETAIL
%token <str>	DIAGNOSTICS
%token <str>	DO
%token <str>	DUMP
%token <str>	ELSE
%token <str>	ELSIF
%token <str>	END
%token <str>	ERRCODE
%token <str>	ERROR
%token <str>	EXCEPTION
%token <str>	EXECUTE
%token <str>	EXIT
%token <str>	FETCH
%token <str>	FIRST
%token <str>	FOR
%token <str>	FOREACH
%token <str>	FORWARD
%token <str>	FROM
%token <str>	GET
%token <str>	HINT
%token <str>	IF
%token <str>	IMPORT
%token <str>	IN
%token <str>	INFO
%token <str>	INSERT
%token <str>	INTO
%token <str>	IS
%token <str>	LAST
%token <str>	LOG
%token <str>	LOOP
%token <str>	MERGE
%token <str>	MESSAGE
%token <str>	MESSAGE_TEXT
%token <str>	MOVE
%token <str>	NEXT
%token <str>	NO
%token <str>	NOT
%token <str>	NOTICE
%token <str>	NULL
%token <str>	OPEN
%token <str>	OPTION
%token <str>	OR
%token <str>	PERFORM
%token <str>	PG_CONTEXT
%token <str>	PG_DATATYPE_NAME
%token <str>	PG_EXCEPTION_CONTEXT
%token <str>	PG_EXCEPTION_DETAIL
%token <str>	PG_EXCEPTION_HINT
%token <str>	PRINT_STRICT_PARAMS
%token <str>	PRIOR
%token <str>	QUERY
%token <str>	RAISE
%token <str>	RELATIVE
%token <str>	RETURN
%token <str>  RETURN_NEXT
%token <str>  RETURN_QUERY
%token <str>	RETURNED_SQLSTATE
%token <str>	REVERSE
%token <str>	ROLLBACK
%token <str>	ROW_COUNT
%token <str>	ROWTYPE
%token <str>	SCHEMA
%token <str>	SCHEMA_NAME
%token <str>	SCROLL
%token <str>	SLICE
%token <str>	SQLSTATE
%token <str>	STACKED
%token <str>	STRICT
%token <str>	TABLE
%token <str>	TABLE_NAME
%token <str>	THEN
%token <str>	TO
%token <str>	TYPE
%token <str>	USE_COLUMN
%token <str>	USE_VARIABLE
%token <str>	USING
%token <str>	VARIABLE_CONFLICT
%token <str>	WARNING
%token <str>	WHEN
%token <str>	WHILE

%union {
  id    int32
  pos   int32
  str   string
  union plpgsqlSymUnion
}

%type <declareHeader> decl_sect
%type <varName> decl_varname
%type <boolean>	decl_const decl_notnull exit_type
%type <plpgsqltree.PLpgSQLExpr>	decl_defval decl_cursor_query
%type <*types.T>	decl_datatype
%type <oid.OID>		decl_collate
%type <plpgsqltree.PLpgSQLDatum>	decl_cursor_args

// TODO figure these two out.
//%type <list>	decl_cursor_arglist // TODO a list of what?
//%type <nsitem>	decl_aliasitem // TODO what is nsitem? looks like namespace item, not sure if we need it.

%type <plpgsqltree.PLpgSQLExpr>	expr_until_semi
%type <plpgsqltree.PLpgSQLExpr>	expr_until_then expr_until_loop opt_expr_until_when
%type <plpgsqltree.PLpgSQLExpr>	opt_exitcond

%type <plpgsqltree.PLpgSQLScalarVar>		cursor_variable
%type <plpgsqltree.PLpgSQLDatum>	decl_cursor_arg
%type <forvariable>	for_variable
%type <*tree.NumVal>	foreach_slice
%type <plpgsqltree.PLpgSQLStatement>	for_control

%type <str>		any_identifier opt_block_label opt_loop_label opt_label
%type <str>		option_value

//%type <list>	proc_sect stmt_elsifs stmt_else // TODO is this a list of statement?
%type <loopBody>	loop_body
%type <*plpgsqltree.PLpgSQLStmtBlock> pl_block
%type <plpgsqltree.PLpgSQLStatement>	proc_stmt
%type <plpgsqltree.PLpgSQLStatement>	stmt_assign stmt_if stmt_loop stmt_while stmt_exit
%type <plpgsqltree.PLpgSQLStatement>	stmt_return stmt_raise stmt_assert stmt_execsql
%type <plpgsqltree.PLpgSQLStatement>	stmt_dynexecute stmt_for stmt_perform stmt_call stmt_getdiag
%type <plpgsqltree.PLpgSQLStatement>	stmt_open stmt_fetch stmt_move stmt_close stmt_null
%type <plpgsqltree.PLpgSQLStatement>	stmt_commit stmt_rollback
%type <plpgsqltree.PLpgSQLStatement>	stmt_case stmt_foreach_a

//%type <list>	proc_exceptions // TODO is this a list of exeception arms?
%type <*plpgsqltree.PLpgSQLExceptionBlock> exception_sect
%type <*plpgsqltree.PLpgSQLException>	proc_exception
%type <*plpgsqltree.PLpgSQLCondition>	proc_conditions proc_condition

%type <*plpgsqltree.PLpgSQLStmtCaseWhenArm>	case_when
//%type <list>	case_when_list opt_case_else // TODO is this a list of case when arms?

%type <bool>	getdiag_area_opt
//%type <list>	getdiag_list // TODO don't know what this is
//%type <diagitem> getdiag_list_item // TODO don't know what this is
//%type <plpgsqltree.PLpgSQLDatum>	getdiag_target // TODO don't know what this is
//%type <*tree.NumVal>	getdiag_item // TODO don't know what this is

%type <*tree.NumVal>	opt_scrollable
%type <*plpgsqltree.PLpgSQLStmtFetch>	opt_fetch_direction

%type <*tree.NumVal>	opt_transaction_chain

%type <str>	unreserved_keyword


%%

pl_function:
  // TODO we need to set the final AST in this block. To achieve that, the lexer
  // need to have a statement field to catch that.
  pl_block opt_semi
  {

  }

option_value : T_WORD
				{
				}
			 | unreserved_keyword
				{
				}

opt_semi		:
				| ';'
				;

pl_block		: decl_sect BEGIN proc_sect exception_sect END opt_label
          // TODO we need to create a new scope for each block
          // 1. create a scope and push to stack before parsing statements
          // 2. pop the scope after all statements are parsed.
					{
					  stmtBlock := &plpgsqltree.PLpgSQLStmtBlock{}
					  $$.val = stmtBlock
					}
				;


decl_sect		: opt_block_label
					{
					}
				| opt_block_label decl_start
					{
					}
				| opt_block_label decl_start decl_stmts
					{
					}
				;

decl_start		: DECLARE
					{
					}
				;

decl_stmts		: decl_stmts decl_stmt
				| decl_stmt
				;

decl_stmt		: decl_statement
				| DECLARE
					{
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
					}
				;

decl_statement	: decl_varname decl_const decl_datatype decl_collate decl_notnull decl_defval
					{
					}
				| decl_varname ALIAS FOR decl_aliasitem ';'
					{
					}
				| decl_varname opt_scrollable CURSOR
					{ }
				  decl_cursor_args decl_is_for decl_cursor_query
					{
					}
				;

opt_scrollable :
					{
					}
				| NO SCROLL
					{
					}
				| SCROLL
					{
					}
				;

decl_cursor_query :
					{
					}
				;

decl_cursor_args :
					{
					}
				| '(' decl_cursor_arglist ')'
					{
					}
				;

decl_cursor_arglist : decl_cursor_arg
					{
					}
				| decl_cursor_arglist ',' decl_cursor_arg
					{
					}
				;

decl_cursor_arg : decl_varname decl_datatype
					{
					}
				;

decl_is_for		:	IS |		/* Oracle */
					FOR;		/* SQL standard */

decl_aliasitem	: T_WORD
					{
					}
				| unreserved_keyword
					{
					}
				| T_CWORD
					{
					}
				;

decl_varname	: T_WORD
					{
					}
				| unreserved_keyword
					{
					}
				;

decl_const		:
					{ }
				| CONSTANT
					{ }
				;

decl_datatype	:
					{
					}
				;

decl_collate	:
					{ }
				| COLLATE T_WORD
					{
					}
				| COLLATE unreserved_keyword
					{
					}
				| COLLATE T_CWORD
					{
					}
				;

decl_notnull	:
					{ }
				| NOT NULL
					{ }
				;

decl_defval		: ';'
					{ }
				| decl_defkey
					{
					}
				;

decl_defkey		: assign_operator
				| DEFAULT
				;

/*
 * Ada-based PL/SQL uses := for assignment and variable defaults, while
 * the SQL standard uses equals for these cases and for GET
 * DIAGNOSTICS, so we support both.  FOR and OPEN only support :=.
 */
assign_operator	: '='
				| COLON_EQUALS
				;

proc_sect		:
					{ }
				| proc_sect proc_stmt
					{
					}
				;

proc_stmt:
  pl_block ';'
	{
	  $$.val = $1.plpgsqlStmtBlock()
	}
				| stmt_assign
						{ }
				| stmt_if
						{ }
				| stmt_case
						{ }
				| stmt_loop
						{ }
				| stmt_while
						{ }
				| stmt_for
						{ }
				| stmt_foreach_a
						{ }
				| stmt_exit
						{ }
				| stmt_return
						{ }
				| stmt_raise
						{ }
				| stmt_assert
						{ }
				| stmt_execsql
						{ }
				| stmt_dynexecute
						{ }
				| stmt_perform
						{ }
				| stmt_call
						{ }
				| stmt_getdiag
						{ }
				| stmt_open
						{ }
				| stmt_fetch
						{ }
				| stmt_move
						{ }
				| stmt_close
						{ }
				| stmt_null
						{ }
				| stmt_commit
						{ }
				| stmt_rollback
						{ }
				;

stmt_perform	: PERFORM
					{
					}
				;

stmt_call		: CALL
					{
					}
				| DO
					{
					}
				;

stmt_assign		: T_DATUM
					{
					}
				;

stmt_getdiag	: GET getdiag_area_opt DIAGNOSTICS getdiag_list ';'
					{
					}
				;

getdiag_area_opt :
					{
					}
				| CURRENT
					{
					}
				| STACKED
					{
					}
				;

getdiag_list : getdiag_list ',' getdiag_list_item
					{
					}
				| getdiag_list_item
					{
					}
				;

getdiag_list_item : getdiag_target assign_operator getdiag_item
					{
					}
				;

getdiag_item :
					{
					}
				;

getdiag_target	: T_DATUM
					{
					}
				| T_WORD
					{
					}
				| T_CWORD
					{
					}
				;

stmt_if			: IF expr_until_then proc_sect stmt_elsifs stmt_else END IF ';'
					{
					}
				;

stmt_elsifs		:
					{
					}
				| stmt_elsifs ELSIF expr_until_then proc_sect
					{
					}
				;

stmt_else		:
					{
					}
				| ELSE proc_sect
					{
					}
				;

stmt_case		: CASE opt_expr_until_when case_when_list opt_case_else END CASE ';'
					{
					}
				;

opt_expr_until_when	:
					{
					}
				;

case_when_list	: case_when_list case_when
					{
					}
				| case_when
					{
					}
				;

case_when		: WHEN expr_until_then proc_sect
					{
					}
				;

opt_case_else	:
					{
					}
				| ELSE proc_sect
					{
					}
				;

stmt_loop		: opt_loop_label LOOP loop_body
					{
					}
				;

stmt_while		: opt_loop_label WHILE expr_until_loop loop_body
					{
					}
				;

stmt_for		: opt_loop_label FOR for_control loop_body
					{
					}
				;

for_control		: for_variable IN
          // TODO need to parse the sql expression here.
					{
					}
				;

/*
 * Processing the for_variable is tricky because we don't yet know if the
 * FOR is an integer FOR loop or a loop over query results.  In the former
 * case, the variable is just a name that we must instantiate as a loop
 * local variable, regardless of any other definition it might have.
 * Therefore, we always save the actual identifier into $$.name where it
 * can be used for that case.  We also save the outer-variable definition,
 * if any, because that's what we need for the loop-over-query case.  Note
 * that we must NOT apply check_assignable() or any other semantic check
 * until we know what's what.
 *
 * However, if we see a comma-separated list of names, we know that it
 * can't be an integer FOR loop and so it's OK to check the variables
 * immediately.  In particular, for T_WORD followed by comma, we should
 * complain that the name is not known rather than say it's a syntax error.
 * Note that the non-error result of this case sets *both* $$.scalar and
 * $$.row; see the for_control production.
 */
for_variable	: T_DATUM
					{
					}
				| T_WORD
					{
					}
				| T_CWORD
					{
					}
				;

stmt_foreach_a	: opt_loop_label FOREACH for_variable foreach_slice IN ARRAY expr_until_loop loop_body
					{
					}
				;

foreach_slice	:
					{
					}
				| SLICE ICONST
					{
					}
				;

stmt_exit		: exit_type opt_label opt_exitcond
					{
					}
				;

exit_type		: EXIT
					{
					}
				| CONTINUE
					{
					}
				;

stmt_return:
  // TODO handle variable names
  // 1. verify if the first token is a variable (this means that we need to track variable scope during parsing)
  // 2. if yes, check next token is ';'
  // 3. if no, expecting a sql expression "read_sql_expression"
  //    we can just read until a ';', then do the sql expression validation during compile time.
  RETURN
  {

  }
| RETURN_NEXT NEXT
  {}
| RETURN_QUERY QUERY
  {}

stmt_raise		: RAISE
					{
					}
				;

stmt_assert		: ASSERT
					{
					}
				;

loop_body		: proc_sect END LOOP opt_label ';'
					{
					}
				;

/*
 * T_WORD+T_CWORD match any initial identifier that is not a known plpgsql
 * variable.  (The composite case is probably a syntax error, but we'll let
 * the core parser decide that.)  Normally, we should assume that such a
 * word is a SQL statement keyword that isn't also a plpgsql keyword.
 * However, if the next token is assignment or '[' or '.', it can't be a valid
 * SQL statement, and what we're probably looking at is an intended variable
 * assignment.  Give an appropriate complaint for that, instead of letting
 * the core parser throw an unhelpful "syntax error".
 */
stmt_execsql	: IMPORT
					{
					}
				| INSERT
					{
					}
				| MERGE
					{
					}
				| T_WORD
					{
					}
				| T_CWORD
					{
					}
				;

stmt_dynexecute : EXECUTE
					{
					}
				;


stmt_open		: OPEN cursor_variable
					{
					}
				;

stmt_fetch		: FETCH opt_fetch_direction cursor_variable INTO
					{
					}
				;

stmt_move		: MOVE opt_fetch_direction cursor_variable ';'
					{
					}
				;

opt_fetch_direction	:
					{
					}
				;

stmt_close		: CLOSE cursor_variable ';'
					{
					}
				;

stmt_null		: NULL ';'
					{
					}
				;

stmt_commit		: COMMIT opt_transaction_chain ';'
					{
					}
				;

stmt_rollback	: ROLLBACK opt_transaction_chain ';'
					{
					}
				;

opt_transaction_chain:
			AND CHAIN			{ }
			| AND NO CHAIN	{ }
			| /* EMPTY */			{ }
				;


cursor_variable	: T_DATUM
					{
					}
				| T_WORD
					{
					}
				| T_CWORD
					{
					}
				;

exception_sect	:
					{ }
				| EXCEPTION
					{
					}
					proc_exceptions
					{
					}
				;

proc_exceptions	: proc_exceptions proc_exception
						{
						}
				| proc_exception
						{
						}
				;

proc_exception	: WHEN proc_conditions THEN proc_sect
					{
					}
				;

proc_conditions	: proc_conditions OR proc_condition
						{
						}
				| proc_condition
						{
						}
				;

proc_condition	: any_identifier
						{
						}
				;

expr_until_semi :
					{ }
				;

expr_until_then :
					{ }
				;

expr_until_loop :
					{ }
				;

opt_block_label	:
					{
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
					}
				;

opt_loop_label	:
					{
					}
				| LESS_LESS any_identifier GREATER_GREATER
					{
					}
				;

opt_label	:
					{
					}
				| any_identifier
					{
					}
				;

opt_exitcond	: ';'
					{ }
				| WHEN expr_until_semi
					{ }
				;

/*
 * need to allow DATUM because scanner will have tried to resolve as variable
 */
any_identifier:
  T_WORD
  {
  }
| unreserved_keyword
  {
  }
| T_DATUM
  {
  }

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
| LAST
| LOG
| MERGE
| MESSAGE
| MESSAGE_TEXT
| MOVE
| NEXT
| NO
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
