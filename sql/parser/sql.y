// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California

%{
package parser
import "github.com/cockroachdb/cockroach/sql/privilege"

func unimplemented() {
  panic("TODO(pmattis): unimplemented")
}
%}

%{
type sqlBoolVal struct {
	boolVal	bool
}
type sqlStrs struct {
	strs	[]string
}
type sqlQname struct {
	qname	*QualifiedName
}
type sqlQnames struct {
	qnames	QualifiedNames
}
type sqlIndirectElem struct {
	indirectElem	IndirectionElem
}
type sqlIndirect struct {
	indirect	Indirection
}
type sqlStmt struct {
	stmt	Statement
}
type sqlStmts struct {
	stmts	[]Statement
}
type sqlSelectStmt struct {
	selectStmt	SelectStatement
}
type sqlColDef struct {
	colDef	*ColumnTableDef
}
type sqlConstraintDef struct {
	constraintDef	ConstraintTableDef
}
type sqlTblDef struct {
	tblDef	TableDef
}
type sqlTblDefs struct {
	tblDefs	[]TableDef
}
type sqlColQual struct {
	colQual	ColumnQualification
}
type sqlColQuals struct {
	colQuals	[]ColumnQualification
}
type sqlColType struct {
	colType	ColumnType
}
type sqlColTypes struct {
	colTypes	[]ColumnType
}
type sqlExpr struct {
	expr	Expr
}
type sqlExprs struct {
	exprs	Exprs
}
type sqlSelExpr struct {
	selExpr	SelectExpr
}
type sqlSelExprs struct {
	selExprs	SelectExprs
}
type sqlAliasClause struct {
	aliasClause	AliasClause
}
type sqlTblExpr struct {
	tblExpr	TableExpr
}
type sqlTblExprs struct {
	tblExprs	TableExprs
}
type sqlJoinCond struct {
	joinCond	JoinCond
}
type sqlWhen struct {
	when	*When
}
type sqlWhens struct {
	whens	[]*When
}
type sqlUpdateExpr struct {
	updateExpr	*UpdateExpr
}
type sqlUpdateExprs struct {
	updateExprs	[]*UpdateExpr
}
type sqlLimit struct {
	limit	*Limit
}
type sqlTargetList struct {
	targetList	TargetList
}
type sqlTargetListPtr struct {
	targetListPtr	*TargetList
}
type sqlPrivilegeType struct {
	privilegeType	privilege.Kind
}
type sqlPrivilegeList struct {
	privilegeList	privilege.List
}
type sqlOrderBy struct {
	orderBy	OrderBy
}
type sqlOrders struct {
	orders	[]*Order
}
type sqlOrder struct {
	order	*Order
}
type sqlGroupBy struct {
	groupBy	GroupBy
}
type sqlDir struct {
	dir	Direction
}
type sqlAlterTableCmd struct {
	alterTableCmd	AlterTableCmd
}
type sqlAlterTableCmds struct {
	alterTableCmds	AlterTableCmds
}
type sqlIsoLevel struct {
	isoLevel	IsolationLevel
}
type sqlUserPriority struct {
	userPriority	UserPriority
}
type sqlIdxElem struct {
	idxElem	IndexElem
}
type sqlIdxElems struct {
	idxElems	IndexElemList
}

type sqlSymUnion interface {
	sqlSymUnion()
}

func (*sqlBoolVal) sqlSymUnion() {}
func (*sqlStrs) sqlSymUnion() {}
func (*sqlQname) sqlSymUnion() {}
func (*sqlQnames) sqlSymUnion() {}
func (*sqlIndirectElem) sqlSymUnion() {}
func (*sqlIndirect) sqlSymUnion() {}
func (*sqlStmt) sqlSymUnion() {}
func (*sqlStmts) sqlSymUnion() {}
func (*sqlSelectStmt) sqlSymUnion() {}
func (*sqlColDef) sqlSymUnion() {}
func (*sqlConstraintDef) sqlSymUnion() {}
func (*sqlTblDef) sqlSymUnion() {}
func (*sqlTblDefs) sqlSymUnion() {}
func (*sqlColQual) sqlSymUnion() {}
func (*sqlColQuals) sqlSymUnion() {}
func (*sqlColType) sqlSymUnion() {}
func (*sqlColTypes) sqlSymUnion() {}
func (*sqlExpr) sqlSymUnion() {}
func (*sqlExprs) sqlSymUnion() {}
func (*sqlSelExpr) sqlSymUnion() {}
func (*sqlSelExprs) sqlSymUnion() {}
func (*sqlAliasClause) sqlSymUnion() {}
func (*sqlTblExpr) sqlSymUnion() {}
func (*sqlTblExprs) sqlSymUnion() {}
func (*sqlJoinCond) sqlSymUnion() {}
func (*sqlWhen) sqlSymUnion() {}
func (*sqlWhens) sqlSymUnion() {}
func (*sqlUpdateExpr) sqlSymUnion() {}
func (*sqlUpdateExprs) sqlSymUnion() {}
func (*sqlLimit) sqlSymUnion() {}
func (*sqlTargetList) sqlSymUnion() {}
func (*sqlTargetListPtr) sqlSymUnion() {}
func (*sqlPrivilegeType) sqlSymUnion() {}
func (*sqlPrivilegeList) sqlSymUnion() {}
func (*sqlOrderBy) sqlSymUnion() {}
func (*sqlOrders) sqlSymUnion() {}
func (*sqlOrder) sqlSymUnion() {}
func (*sqlGroupBy) sqlSymUnion() {}
func (*sqlDir) sqlSymUnion() {}
func (*sqlAlterTableCmd) sqlSymUnion() {}
func (*sqlAlterTableCmds) sqlSymUnion() {}
func (*sqlIsoLevel) sqlSymUnion() {}
func (*sqlUserPriority) sqlSymUnion() {}
func (*sqlIdxElem) sqlSymUnion() {}
func (*sqlIdxElems) sqlSymUnion() {}
%}

%union {
  id             int
  pos            int
  empty          struct{}
  ival           IntVal
  str            string
  union          sqlSymUnion
}

%type <union> /* <sqlStmts> */ stmt_block
%type <union> /* <sqlStmts> */ stmt_list
%type <union> /* <sqlStmt> */ stmt

%type <union> /* <sqlStmt> */ alter_table_stmt
%type <union> /* <sqlStmt> */ create_stmt
%type <union> /* <sqlStmt> */ create_database_stmt
%type <union> /* <sqlStmt> */ create_index_stmt
%type <union> /* <sqlStmt> */ create_table_stmt
%type <union> /* <sqlStmt> */ delete_stmt
%type <union> /* <sqlStmt> */ drop_stmt
%type <union> /* <sqlStmt> */ explain_stmt
%type <union> /* <sqlStmt> */ explainable_stmt
%type <union> /* <sqlStmt> */ grant_stmt
%type <union> /* <sqlStmt> */ insert_stmt
%type <union> /* <sqlStmt> */ preparable_stmt
%type <union> /* <sqlStmt> */ rename_stmt
%type <union> /* <sqlStmt> */ revoke_stmt
%type <union> /* <sqlSelectStmt> */ select_stmt
%type <union> /* <sqlStmt> */ set_stmt
%type <union> /* <sqlStmt> */ show_stmt
%type <union> /* <sqlStmt> */ transaction_stmt
%type <union> /* <sqlStmt> */ truncate_stmt
%type <union> /* <sqlStmt> */ update_stmt

%type <union> /* <sqlSelectStmt> */ select_no_parens select_with_parens select_clause
%type <union> /* <sqlSelectStmt> */ simple_select values_clause

%type <empty> alter_column_default alter_using
%type <union> /* <sqlDir> */ opt_asc_desc

%type <union> /* <sqlAlterTableCmd> */ alter_table_cmd
%type <union> /* <sqlAlterTableCmds> */ alter_table_cmds

%type <empty> opt_collate_clause

%type <empty> opt_drop_behavior

%type <union> /* <sqlIsoLevel> */ transaction_iso_level
%type <union> /* <sqlUserPriority> */  transaction_user_priority

%type <str>   name opt_name

// %type <empty> subquery_op
%type <union> /* <sqlQname> */ func_name
%type <empty> opt_collate

%type <union> /* <sqlQname> */ qualified_name
%type <union> /* <sqlQname> */ indirect_name_or_glob
%type <union> /* <sqlQname> */ insert_target

// %type <empty> math_op

%type <union> /* <sqlIsoLevel> */ iso_level
%type <union> /* <sqlUserPriority> */ user_priority
%type <empty> opt_encoding

%type <union> /* <sqlTblDefs> */ opt_table_elem_list table_elem_list
%type <empty> opt_all_clause
%type <union> /* <sqlBoolVal> */ distinct_clause
%type <union> /* <sqlStrs> */ opt_column_list
%type <union> /* <sqlOrderBy> */ sort_clause opt_sort_clause
%type <union> /* <sqlOrders> */ sortby_list
%type <union> /* <sqlIdxElems> */ index_params
%type <union> /* <sqlStrs> */ name_list opt_name_list
%type <empty> opt_array_bounds
%type <union> /* <sqlTblExprs> */ from_clause from_list
%type <union> /* <sqlQnames> */ qualified_name_list
%type <union> /* <sqlQnames> */ indirect_name_or_glob_list
%type <union> /* <sqlQname> */ any_name
%type <union> /* <sqlQnames> */ any_name_list
%type <union> /* <sqlExprs> */ expr_list
%type <union> /* <sqlIndirect> */ attrs
%type <union> /* <sqlSelExprs> */ target_list opt_target_list
%type <union> /* <sqlUpdateExprs> */ set_clause_list
%type <union> /* <sqlUpdateExpr> */ set_clause multiple_set_clause
%type <union> /* <sqlIndirect> */ indirection
%type <union> /* <sqlExprs> */ ctext_expr_list ctext_row
%type <union> /* <sqlGroupBy> */ group_clause
%type <union> /* <sqlLimit> */ select_limit
%type <union> /* <sqlQnames> */ relation_expr_list

%type <union> /* <sqlBoolVal> */ all_or_distinct
%type <empty> join_outer
%type <union> /* <sqlJoinCond> */ join_qual
%type <str> join_type

%type <union> /* <sqlExprs> */ extract_list
%type <union> /* <sqlExprs> */ overlay_list
%type <union> /* <sqlExprs> */ position_list
%type <union> /* <sqlExprs> */ substr_list
%type <union> /* <sqlExprs> */ trim_list
%type <empty> opt_interval interval_second
%type <union> /* <sqlExpr> */ overlay_placing

%type <union> /* <sqlBoolVal> */ opt_unique opt_column

%type <empty> opt_set_data

%type <union> /* <sqlLimit> */ limit_clause offset_clause
%type <union> /* <sqlExpr> */  select_limit_value
// %type <empty> opt_select_fetch_first_value
%type <empty> row_or_rows
// %type <empty> first_or_next

%type <union> /* <sqlStmt> */  insert_rest
%type <empty> opt_conf_expr
%type <empty> opt_on_conflict

%type <union> /* <sqlStmt> */  generic_set set_rest set_rest_more transaction_mode_list opt_transaction_mode_list

%type <union> /* <sqlStrs> */ opt_storing
%type <union> /* <sqlColDef> */ column_def
%type <union> /* <sqlTblDef> */ table_elem
%type <union> /* <sqlExpr> */  where_clause
%type <union> /* <sqlIndirectElem> */ glob_indirection
%type <union> /* <sqlIndirectElem> */ name_indirection
%type <union> /* <sqlIndirectElem> */ indirection_elem
%type <union> /* <sqlExpr> */  a_expr b_expr c_expr a_expr_const
%type <union> /* <sqlExpr> */  substr_from substr_for
%type <union> /* <sqlExpr> */  in_expr
%type <union> /* <sqlExpr> */  having_clause
%type <union> /* <sqlExpr> */  array_expr
%type <union> /* <sqlColTypes> */ type_list
%type <union> /* <sqlExprs> */ array_expr_list
%type <union> /* <sqlExpr> */  row explicit_row implicit_row
%type <union> /* <sqlExpr> */  case_expr case_arg case_default
%type <union> /* <sqlWhen> */  when_clause
%type <union> /* <sqlWhens> */ when_clause_list
// %type <empty> sub_type
%type <union> /* <sqlExpr> */ ctext_expr
%type <union> /* <sqlExpr> */ numeric_only
%type <union> /* <sqlAliasClause> */ alias_clause opt_alias_clause
%type <union> /* <sqlOrder> */ sortby
%type <union> /* <sqlIdxElem> */ index_elem
%type <union> /* <sqlTblExpr> */ table_ref
%type <union> /* <sqlTblExpr> */ joined_table
%type <union> /* <sqlQname> */ relation_expr
%type <union> /* <sqlTblExpr> */ relation_expr_opt_alias
%type <union> /* <sqlSelExpr> */ target_elem
%type <union> /* <sqlUpdateExpr> */ single_set_clause

%type <str> explain_option_name
%type <union> /* <sqlStrs> */ explain_option_list

%type <union> /* <sqlColType> */ typename simple_typename const_typename
%type <union> /* <sqlColType> */ numeric opt_numeric_modifiers
%type <ival> opt_float
%type <union> /* <sqlColType> */ character const_character
%type <union> /* <sqlColType> */ character_with_length character_without_length
%type <union> /* <sqlColType> */ const_datetime const_interval
%type <union> /* <sqlColType> */ bit const_bit bit_with_length bit_without_length
%type <union> /* <sqlColType> */ character_base
%type <str> extract_arg
%type <empty> opt_varying

%type <ival>  signed_iconst
%type <union> /* <sqlExpr> */  opt_boolean_or_string
%type <union> /* <sqlExprs> */ var_list
%type <union> /* <sqlQname> */ opt_from_var_name_clause var_name
%type <str>   col_label type_function_name
%type <str>   non_reserved_word
%type <union> /* <sqlExpr> */  non_reserved_word_or_sconst
%type <union> /* <sqlExpr> */  var_value
%type <union> /* <sqlExpr> */  zone_value

%type <str>   unreserved_keyword type_func_name_keyword
%type <str>   col_name_keyword reserved_keyword

%type <union> /* <sqlConstraintDef> */ table_constraint constraint_elem
%type <union> /* <sqlTblDef> */ index_def
%type <union> /* <sqlColQuals> */ col_qual_list
%type <union> /* <sqlColQual> */ col_qualification col_qualification_elem
%type <empty> key_actions key_delete key_match key_update key_action

%type <union> /* <sqlExpr> */  func_application func_expr_common_subexpr
%type <union> /* <sqlExpr> */  func_expr func_expr_windowless
%type <empty> common_table_expr
%type <empty> with_clause opt_with_clause
%type <empty> cte_list

%type <empty> within_group_clause
%type <empty> filter_clause
%type <empty> window_clause window_definition_list opt_partition_clause
%type <empty> window_definition over_clause window_specification
%type <empty> opt_frame_clause frame_extent frame_bound
%type <empty> opt_existing_window_name

%type <union> /* <sqlTargetList> */    privilege_target
%type <union> /* <sqlTargetListPtr> */ on_privilege_target_clause
%type <union> /* <sqlStrs> */          grantee_list for_grantee_clause
%type <union> /* <sqlPrivilegeList> */ privileges privilege_list
%type <union> /* <sqlPrivilegeType> */ privilege

// Non-keyword token types. These are hard-wired into the "flex" lexer. They
// must be listed first so that their numeric codes do not depend on the set of
// keywords. PL/pgsql depends on this so that it can share the same lexer. If
// you add/change tokens here, fix PL/pgsql to match!
//
// DOT_DOT is unused in the core SQL grammar, and so will always provoke parse
// errors. It is needed by PL/pgsql.
%token <str>   IDENT FCONST SCONST BCONST
%token <ival>  ICONST
%token <str>   PARAM
%token <str>   TYPECAST DOT_DOT
%token <str>   LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%token <str>   ERROR

// If you want to make any keyword changes, update the keyword table in
// src/include/parser/kwlist.h and add new keywords to the appropriate one of
// the reserved-or-not-so-reserved keyword lists, below; search this file for
// "Keyword category lists".

// Ordinary key words in alphabetical order.
%token <str>   ACTION ADD
%token <str>   ALL ALTER ANALYSE ANALYZE AND ANY ARRAY AS ASC
%token <str>   ASYMMETRIC AT

%token <str>   BEGIN BETWEEN BIGINT BIT
%token <str>   BLOB BOOL BOOLEAN BOTH BY BYTEA BYTES

%token <str>   CASCADE CASE CAST CHAR
%token <str>   CHARACTER CHECK
%token <str>   COALESCE COLLATE COLLATION COLUMN COLUMNS COMMIT
%token <str>   COMMITTED CONCAT CONFLICT CONSTRAINT
%token <str>   COVERING CREATE
%token <str>   CROSS CUBE CURRENT CURRENT_CATALOG CURRENT_DATE
%token <str>   CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP
%token <str>   CURRENT_USER CYCLE

%token <str>   DATA DATABASE DATABASES DATE DAY DEC DECIMAL DEFAULT
%token <str>   DEFERRABLE DELETE DESC
%token <str>   DISTINCT DO DOUBLE DROP

%token <str>   ELSE END ESCAPE EXCEPT
%token <str>   EXISTS EXPLAIN EXTRACT

%token <str>   FALSE FETCH FILTER FIRST FLOAT FOLLOWING FOR
%token <str>   FOREIGN FROM FULL

%token <str>   GRANT GRANTS GREATEST GROUP GROUPING

%token <str>   HAVING HIGH HOUR

%token <str>   IF IFNULL IN
%token <str>   INDEX INITIALLY
%token <str>   INNER INSERT INT INT64 INTEGER
%token <str>   INTERSECT INTERVAL INTO IS ISOLATION

%token <str>   JOIN

%token <str>   KEY

%token <str>   LATERAL
%token <str>   LEADING LEAST LEFT LEVEL LIKE LIMIT LOCAL
%token <str>   LOCALTIME LOCALTIMESTAMP LOW LSHIFT

%token <str>   MATCH MINUTE MONTH

%token <str>   NAME NAMES NATURAL NEXT NO NORMAL
%token <str>   NOT NOTHING NULL NULLIF
%token <str>   NULLS NUMERIC

%token <str>   OF OFF OFFSET ON ONLY OR
%token <str>   ORDER ORDINALITY OUT OUTER OVER OVERLAPS OVERLAY

%token <str>   PARTIAL PARTITION PLACING POSITION
%token <str>   PRECEDING PRECISION PRIMARY PRIORITY

%token <str>   RANGE READ REAL RECURSIVE REF REFERENCES
%token <str>   RENAME REPEATABLE
%token <str>   RESTRICT RETURNING REVOKE RIGHT ROLLBACK ROLLUP
%token <str>   ROW ROWS RSHIFT

%token <str>   SEARCH SECOND SELECT
%token <str>   SERIALIZABLE SESSION SESSION_USER SET SHOW
%token <str>   SIMILAR SIMPLE SMALLINT SNAPSHOT SOME SQL
%token <str>   START STRICT STRING STORING SUBSTRING
%token <str>   SYMMETRIC

%token <str>   TABLE TABLES TEXT THEN
%token <str>   TIME TIMESTAMP TO TRAILING TRANSACTION TREAT TRIM TRUE
%token <str>   TRUNCATE TYPE

%token <str>   UNBOUNDED UNCOMMITTED UNION UNIQUE UNKNOWN
%token <str>   UPDATE USER USING

%token <str>   VALID VALIDATE VALUE VALUES VARCHAR VARIADIC VARYING

%token <str>   WHEN WHERE WINDOW WITH WITHIN WITHOUT

%token <str>   YEAR

%token <str>   ZONE

// The grammar thinks these are keywords, but they are not in the kwlist.h list
// and so can never be entered directly. The filter in parser.c creates these
// tokens when required (based on looking one token ahead).
//
// NOT_LA exists so that productions such as NOT LIKE can be given the same
// precedence as LIKE; otherwise they'd effectively have the same precedence as
// NOT, at least with respect to their left-hand subexpression. WITH_LA is
// needed to make the grammar LALR(1).
%token     NOT_LA WITH_LA

// Precedence: lowest to highest
%nonassoc  SET                 // see relation_expr_opt_alias
%left      UNION EXCEPT
%left      INTERSECT
%left      OR
%left      AND
%right     NOT
%nonassoc  IS                  // IS sets precedence for IS NULL, etc
%nonassoc  '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc  BETWEEN IN LIKE SIMILAR NOT_LA
%nonassoc  ESCAPE              // ESCAPE must be just above LIKE/SIMILAR
%nonassoc  OVERLAPS
%left      POSTFIXOP           // dummy for postfix OP rules
// To support target_elem without AS, we must give IDENT an explicit priority
// between POSTFIXOP and OP. We can safely assign the same priority to various
// unreserved keywords as needed to resolve ambiguities (this can't have any
// bad effects since obviously the keywords will still behave the same as if
// they weren't keywords). We need to do this for PARTITION, RANGE, ROWS to
// support opt_existing_window_name; and for RANGE, ROWS so that they can
// follow a_expr without creating postfix-operator problems; and for NULL so
// that it can follow b_expr in col_qual_list without creating postfix-operator
// problems.
//
// To support CUBE and ROLLUP in GROUP BY without reserving them, we give them
// an explicit priority lower than '(', so that a rule with CUBE '(' will shift
// rather than reducing a conflicting rule that takes CUBE as a function name.
// Using the same precedence as IDENT seems right for the reasons given above.
//
// The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING are
// even messier: since UNBOUNDED is an unreserved keyword (per spec!), there is
// no principled way to distinguish these from the productions a_expr
// PRECEDING/FOLLOWING. We hack this up by giving UNBOUNDED slightly lower
// precedence than PRECEDING and FOLLOWING. At present this doesn't appear to
// cause UNBOUNDED to be treated differently from other unreserved keywords
// anywhere else in the grammar, but it's definitely risky. We can blame any
// funny behavior of UNBOUNDED on the SQL standard, though.
%nonassoc  UNBOUNDED         // ideally should have same precedence as IDENT
%nonassoc  IDENT NULL PARTITION RANGE ROWS PRECEDING FOLLOWING CUBE ROLLUP
%left      CONCAT       // multi-character ops
%left      '|'
%left      '^' '#'
%left      '&'
%left      LSHIFT RSHIFT
%left      '+' '-'
%left      '*' '/' '%'
// Unary Operators
%left      AT                // sets precedence for AT TIME ZONE
%left      COLLATE
%right     UMINUS
%left      '~'
%left      '[' ']'
%left      '(' ')'
%left      TYPECAST
%left      '.'
// These might seem to be low-precedence, but actually they are not part
// of the arithmetic hierarchy at all in their use as JOIN operators.
// We make them high-precedence to support their use as function names.
// They wouldn't be given a precedence at all, were it not that we need
// left-associativity among the JOIN rules themselves.
%left      JOIN CROSS LEFT FULL RIGHT INNER NATURAL

%%

stmt_block:
  stmt_list
  {
    sqllex.(*scanner).stmts = $1.(*sqlStmts).stmts
  }

stmt_list:
  stmt_list ';' stmt
  {
    if $3.(*sqlStmt).stmt != nil {
      $1.(*sqlStmts).stmts = append($1.(*sqlStmts).stmts, $3.(*sqlStmt).stmt)

      $$ = $1
    }
  }
| stmt
  {
    if $1.(*sqlStmt).stmt != nil {
      $$ = &sqlStmts{[]Statement{$1.(*sqlStmt).stmt}}
    } else {
      $$ = &sqlStmts{nil}
    }
  }

stmt:
  alter_table_stmt
| create_stmt
| delete_stmt
| drop_stmt
| explain_stmt
| grant_stmt
| insert_stmt
| rename_stmt
| revoke_stmt
| select_stmt
  {
    $$ = &sqlStmt{$1.(*sqlSelectStmt).selectStmt}
  }
| set_stmt
| show_stmt
| transaction_stmt
| truncate_stmt
| update_stmt
| /* EMPTY */
  {
    $$ = &sqlStmt{nil}
  }

alter_table_stmt:
  ALTER TABLE relation_expr alter_table_cmds
  {
    $$ = &sqlStmt{&AlterTable{Table: $3.(*sqlQname).qname, IfExists: false, Cmds: $4.(*sqlAlterTableCmds).alterTableCmds}}
  }
| ALTER TABLE IF EXISTS relation_expr alter_table_cmds
  {
    $$ = &sqlStmt{&AlterTable{Table: $5.(*sqlQname).qname, IfExists: true, Cmds: $6.(*sqlAlterTableCmds).alterTableCmds}}
  }

alter_table_cmds:
  alter_table_cmd
  {
    $$ = &sqlAlterTableCmds{AlterTableCmds{$1.(*sqlAlterTableCmd).alterTableCmd}}
  }
| alter_table_cmds ',' alter_table_cmd
  {
    $1.(*sqlAlterTableCmds).alterTableCmds = append($1.(*sqlAlterTableCmds).alterTableCmds, $3.(*sqlAlterTableCmd).alterTableCmd)
    $$ = $1
  }

alter_table_cmd:
  // ALTER TABLE <name> ADD <coldef>
  ADD column_def
  {
    $$ = &sqlAlterTableCmd{&AlterTableAddColumn{columnKeyword: false, IfNotExists: false, ColumnDef: $2.(*sqlColDef).colDef}}
  }
  // ALTER TABLE <name> ADD IF NOT EXISTS <coldef>
| ADD IF NOT EXISTS column_def
  {
    $$ = &sqlAlterTableCmd{&AlterTableAddColumn{columnKeyword: false, IfNotExists: true, ColumnDef: $5.(*sqlColDef).colDef}}
  }
  // ALTER TABLE <name> ADD COLUMN <coldef>
| ADD COLUMN column_def
  {
    $$ = &sqlAlterTableCmd{&AlterTableAddColumn{columnKeyword: true, IfNotExists: false, ColumnDef: $3.(*sqlColDef).colDef}}
  }
  // ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef>
| ADD COLUMN IF NOT EXISTS column_def
  {
    $$ = &sqlAlterTableCmd{&AlterTableAddColumn{columnKeyword: true, IfNotExists: true, ColumnDef: $6.(*sqlColDef).colDef}}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT}
| ALTER opt_column name alter_column_default { unimplemented() }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL
| ALTER opt_column name DROP NOT NULL { unimplemented() }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL
| ALTER opt_column name SET NOT NULL { unimplemented() }
  // ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE]
| DROP opt_column IF EXISTS name opt_drop_behavior
  {
    $$ = &sqlAlterTableCmd{&AlterTableDropColumn{columnKeyword: $2.(*sqlBoolVal).boolVal, IfExists: true, Column: $5}}
  }
  // ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE]
| DROP opt_column name opt_drop_behavior
  {
    $$ = &sqlAlterTableCmd{&AlterTableDropColumn{columnKeyword: $2.(*sqlBoolVal).boolVal, IfExists: false, Column: $3}}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
  //     [ USING <expression> ]
| ALTER opt_column name opt_set_data TYPE typename opt_collate_clause alter_using {}
  // ALTER TABLE <name> ADD CONSTRAINT ...
| ADD table_constraint
  {
    $$ = &sqlAlterTableCmd{&AlterTableAddConstraint{ConstraintDef: $2.(*sqlConstraintDef).constraintDef}}
  }
  // ALTER TABLE <name> ALTER CONSTRAINT ...
| ALTER CONSTRAINT name { unimplemented() }
  // ALTER TABLE <name> VALIDATE CONSTRAINT ...
| VALIDATE CONSTRAINT name { unimplemented() }
  // ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT IF EXISTS name opt_drop_behavior
  {
    $$ = &sqlAlterTableCmd{&AlterTableDropConstraint{IfExists: true, Constraint: $5}}
  }
  // ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT name opt_drop_behavior
  {
    $$ = &sqlAlterTableCmd{&AlterTableDropConstraint{IfExists: false, Constraint: $3}}
  }

alter_column_default:
  SET DEFAULT a_expr { unimplemented() }
| DROP DEFAULT { unimplemented() }

opt_drop_behavior:
  CASCADE { unimplemented() }
| RESTRICT { unimplemented() }
| /* EMPTY */ {}

opt_collate_clause:
  COLLATE any_name { unimplemented() }
| /* EMPTY */ {}

alter_using:
  USING a_expr { unimplemented() }
| /* EMPTY */ {}

// CREATE [DATABASE|INDEX|TABLE|TABLE AS]
create_stmt:
  create_database_stmt
| create_index_stmt
| create_table_stmt

// DELETE FROM query
delete_stmt:
  opt_with_clause DELETE FROM relation_expr_opt_alias where_clause
  {
    $$ = &sqlStmt{&Delete{Table: $4.(*sqlTblExpr).tblExpr, Where: newWhere(astWhere, $5.(*sqlExpr).expr)}}
  }

// DROP itemtype [ IF EXISTS ] itemname [, itemname ...] [ RESTRICT | CASCADE ]
drop_stmt:
  DROP DATABASE name
  {
    $$ = &sqlStmt{&DropDatabase{Name: Name($3), IfExists: false}}
  }
| DROP DATABASE IF EXISTS name
  {
    $$ = &sqlStmt{&DropDatabase{Name: Name($5), IfExists: true}}
  }
| DROP INDEX qualified_name_list opt_drop_behavior
  {
    $$ = &sqlStmt{&DropIndex{Names: $3.(*sqlQnames).qnames, IfExists: false}}
  }
| DROP INDEX IF EXISTS qualified_name_list opt_drop_behavior
  {
    $$ = &sqlStmt{&DropIndex{Names: $5.(*sqlQnames).qnames, IfExists: true}}
  }
| DROP TABLE any_name_list
  {
    $$ = &sqlStmt{&DropTable{Names: $3.(*sqlQnames).qnames, IfExists: false}}
  }
| DROP TABLE IF EXISTS any_name_list
  {
    $$ = &sqlStmt{&DropTable{Names: $5.(*sqlQnames).qnames, IfExists: true}}
  }

any_name_list:
  any_name
  {
    $$ = &sqlQnames{QualifiedNames{$1.(*sqlQname).qname}}
  }
| any_name_list ',' any_name
  {
    $1.(*sqlQnames).qnames = append($1.(*sqlQnames).qnames, $3.(*sqlQname).qname)
    $$ = $1
  }

any_name:
  name
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1)}}
  }
| name attrs
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1), Indirect: $2.(*sqlIndirect).indirect}}
  }

attrs:
  '.' col_label
  {
    $$ = &sqlIndirect{Indirection{NameIndirection($2)}}
  }
| attrs '.' col_label
  {
    $1.(*sqlIndirect).indirect = append($1.(*sqlIndirect).indirect, NameIndirection($3))
    $$ = $1
  }

// EXPLAIN (options) query
explain_stmt:
  EXPLAIN explainable_stmt
  {
    $$ = &sqlStmt{&Explain{Statement: $2.(*sqlStmt).stmt}}
  }
| EXPLAIN '(' explain_option_list ')' explainable_stmt
  {
    $$ = &sqlStmt{&Explain{Options: $3.(*sqlStrs).strs, Statement: $5.(*sqlStmt).stmt}}
  }

explainable_stmt:
  select_stmt
  {
    $$ = &sqlStmt{$1.(*sqlSelectStmt).selectStmt}
  }
| insert_stmt
| update_stmt
| delete_stmt

explain_option_list:
  explain_option_name
  {
    $$ = &sqlStrs{[]string{$1}}
  }
| explain_option_list ',' explain_option_name
  {
    $1.(*sqlStrs).strs = append($1.(*sqlStrs).strs, $3)
    $$ = $1
  }

explain_option_name:
  non_reserved_word

// GRANT privileges ON privilege_target TO grantee_list
grant_stmt:
  GRANT privileges ON privilege_target TO grantee_list
  {
    $$ = &sqlStmt{&Grant{Privileges: $2.(*sqlPrivilegeList).privilegeList, Grantees: NameList($6.(*sqlStrs).strs), Targets: $4.(*sqlTargetList).targetList}}
  }

// REVOKE privileges ON privilege_target FROM grantee_list
revoke_stmt:
  REVOKE privileges ON privilege_target FROM grantee_list
  {
    $$ = &sqlStmt{&Revoke{Privileges: $2.(*sqlPrivilegeList).privilegeList, Grantees: NameList($6.(*sqlStrs).strs), Targets: $4.(*sqlTargetList).targetList}}
  }


privilege_target:
  indirect_name_or_glob_list
  {
    // Unless "DATABASE" is specified, "TABLE" is the default.
    $$ = &sqlTargetList{TargetList{Tables: QualifiedNames($1.(*sqlQnames).qnames)}}
  }
| TABLE indirect_name_or_glob_list
  {
    $$ = &sqlTargetList{TargetList{Tables: QualifiedNames($2.(*sqlQnames).qnames)}}
  }
|  DATABASE name_list
  {
    $$ = &sqlTargetList{TargetList{Databases: NameList($2.(*sqlStrs).strs)}}
  }

// ALL is always by itself.
privileges:
  ALL
  {
    $$ = &sqlPrivilegeList{privilege.List{privilege.ALL}}
  }
| privilege_list { }

privilege_list:
  privilege
  {
    $$ = &sqlPrivilegeList{privilege.List{$1.(*sqlPrivilegeType).privilegeType}}
  }
| privilege_list ',' privilege
  {
    $1.(*sqlPrivilegeList).privilegeList = append($1.(*sqlPrivilegeList).privilegeList, $3.(*sqlPrivilegeType).privilegeType)
    $$ = $1
  }

// This list must match the list of privileges in sql/privilege/privilege.go.
privilege:
  CREATE
  {
    $$ = &sqlPrivilegeType{privilege.CREATE}
  }
| DROP
  {
    $$ = &sqlPrivilegeType{privilege.DROP}
  }
| GRANT
  {
    $$ = &sqlPrivilegeType{privilege.GRANT}
  }
| SELECT
  {
    $$ = &sqlPrivilegeType{privilege.SELECT}
  }
| INSERT
  {
    $$ = &sqlPrivilegeType{privilege.INSERT}
  }
| DELETE
  {
    $$ = &sqlPrivilegeType{privilege.DELETE}
  }
| UPDATE
  {
    $$ = &sqlPrivilegeType{privilege.UPDATE}
  }

// TODO(marc): this should not be 'name', but should instead be a
// type just for usernames.
grantee_list:
  name
  {
    $$ = &sqlStrs{[]string{$1}}
  }
| grantee_list ',' name
  {
    $1.(*sqlStrs).strs = append($1.(*sqlStrs).strs, $3)
    $$ = $1
  }

// SET name TO 'var_value'
// SET TIME ZONE 'var_value'
set_stmt:
  SET set_rest
  {
    $$ = $2
  }
| SET LOCAL set_rest
  {
    $$ = $3
  }
| SET SESSION set_rest
  {
    $$ = $3
  }

set_rest:
  TRANSACTION transaction_mode_list
  {
    $$ = $2
  }
| set_rest_more

transaction_mode_list:
  transaction_iso_level
  {
    $$ = &sqlStmt{&SetTransaction{Isolation: $1.(*sqlIsoLevel).isoLevel, UserPriority: UnspecifiedUserPriority}}
  }
| transaction_user_priority
  {
    $$ = &sqlStmt{&SetTransaction{Isolation: UnspecifiedIsolation, UserPriority: $1.(*sqlUserPriority).userPriority}}
  }
| transaction_iso_level ',' transaction_user_priority
  {
    $$ = &sqlStmt{&SetTransaction{Isolation: $1.(*sqlIsoLevel).isoLevel, UserPriority: $3.(*sqlUserPriority).userPriority}}
  }
| transaction_user_priority ',' transaction_iso_level
  {
    $$ = &sqlStmt{&SetTransaction{Isolation: $3.(*sqlIsoLevel).isoLevel, UserPriority: $1.(*sqlUserPriority).userPriority}}
  }


transaction_user_priority:
  PRIORITY user_priority
  {
    $$ = $2
  }

generic_set:
  var_name TO var_list
  {
    $$ = &sqlStmt{&Set{Name: $1.(*sqlQname).qname, Values: $3.(*sqlExprs).exprs}}
  }
| var_name '=' var_list
  {
    $$ = &sqlStmt{&Set{Name: $1.(*sqlQname).qname, Values: $3.(*sqlExprs).exprs}}
  }
| var_name TO DEFAULT
  {
    $$ = &sqlStmt{&Set{Name: $1.(*sqlQname).qname}}
  }
| var_name '=' DEFAULT
  {
    $$ = &sqlStmt{&Set{Name: $1.(*sqlQname).qname}}
  }

set_rest_more:
  // Generic SET syntaxes:
  generic_set
| var_name FROM CURRENT { unimplemented() }
  // Special syntaxes mandated by SQL standard:
| TIME ZONE zone_value
  {
    $$ = &sqlStmt{&SetTimeZone{Value: $3.(*sqlExpr).expr}}
  }
| NAMES opt_encoding { unimplemented() }

var_name:
  any_name

var_list:
  var_value
  {
    $$ = &sqlExprs{[]Expr{$1.(*sqlExpr).expr}}
  }
| var_list ',' var_value
  {
    $1.(*sqlExprs).exprs = append($1.(*sqlExprs).exprs, $3.(*sqlExpr).expr)
    $$ = $1
  }

var_value:
  opt_boolean_or_string
| numeric_only
| PARAM
  {
    $$ = &sqlExpr{ValArg{name: $1}}
  }

iso_level:
  READ UNCOMMITTED
  {
    $$ = &sqlIsoLevel{SnapshotIsolation}
  }
| READ COMMITTED
  {
    $$ = &sqlIsoLevel{SnapshotIsolation}
  }
| SNAPSHOT
  {
    $$ = &sqlIsoLevel{SnapshotIsolation}
  }
| REPEATABLE READ
  {
    $$ = &sqlIsoLevel{SerializableIsolation}
  }
| SERIALIZABLE
  {
    $$ = &sqlIsoLevel{SerializableIsolation}
  }

user_priority:
  LOW
  {
    $$ = &sqlUserPriority{Low}
  }
| NORMAL
  {
    $$ = &sqlUserPriority{Normal}
  }
| HIGH
  {
    $$ = &sqlUserPriority{High}
  }

opt_boolean_or_string:
  TRUE
  {
    $$ = &sqlExpr{DBool(true)}
  }
| FALSE
  {
    $$ = &sqlExpr{DBool(false)}
  }
| ON
  {
    $$ = &sqlExpr{DString($1)}
  }
  // OFF is also accepted as a boolean value, but is handled by the
  // non_reserved_word rule. The action for booleans and strings is the same,
  // so we don't need to distinguish them here.
| non_reserved_word_or_sconst

// Timezone values can be:
// - a string such as 'pst8pdt'
// - an identifier such as "pst8pdt"
// - an integer or floating point number
// - a time interval per SQL99
zone_value:
  SCONST
  {
    $$ = &sqlExpr{DString($1)}
  }
| IDENT
  {
    $$ = &sqlExpr{DString($1)}
  }
| const_interval SCONST opt_interval
  {
    expr := &CastExpr{Expr: DString($2), Type: $1.(*sqlColType).colType}
    var ctx EvalContext
    d, err := expr.Eval(ctx)
    if err != nil {
      sqllex.Error("cannot evaluate to an interval type")
      return 1
    }
    if _, ok := d.(DInterval); !ok {
      panic("not an interval type")
    }
    $$ = &sqlExpr{d}
  }
| numeric_only
| DEFAULT
  {
    $$ = &sqlExpr{DString($1)}
  }
| LOCAL
  {
    $$ = &sqlExpr{DString($1)}
  }

opt_encoding:
  SCONST { unimplemented() }
| DEFAULT { unimplemented() }
| /* EMPTY */ {}

non_reserved_word_or_sconst:
  non_reserved_word
  {
    $$ = &sqlExpr{DString($1)}
  }
| SCONST
  {
    $$ = &sqlExpr{DString($1)}
  }

show_stmt:
  SHOW IDENT
  {
    $$ = &sqlStmt{&Show{Name: $2}}
  }
| SHOW DATABASE
  {
    $$ = &sqlStmt{&Show{Name: $2}}
  }
| SHOW COLUMNS FROM var_name
  {
    $$ = &sqlStmt{&ShowColumns{Table: $4.(*sqlQname).qname}}
  }
| SHOW DATABASES
  {
    $$ = &sqlStmt{&ShowDatabases{}}
  }
| SHOW GRANTS on_privilege_target_clause for_grantee_clause
  {
    $$ = &sqlStmt{&ShowGrants{Targets: $3.(*sqlTargetListPtr).targetListPtr, Grantees: $4.(*sqlStrs).strs}}
  }
| SHOW INDEX FROM var_name
  {
    $$ = &sqlStmt{&ShowIndex{Table: $4.(*sqlQname).qname}}
  }
| SHOW TABLES opt_from_var_name_clause
  {
    $$ = &sqlStmt{&ShowTables{Name: $3.(*sqlQname).qname}}
  }
| SHOW TIME ZONE
  {
    $$ = &sqlStmt{&Show{Name: "TIME ZONE"}}
  }
| SHOW TRANSACTION ISOLATION LEVEL
  {
    $$ = &sqlStmt{&Show{Name: "TRANSACTION ISOLATION LEVEL"}}
  }
| SHOW TRANSACTION PRIORITY
  {
    $$ = &sqlStmt{&Show{Name: "TRANSACTION PRIORITY"}}
  }
| SHOW ALL
  {
    $$ = &sqlStmt{nil}
  }

opt_from_var_name_clause:
  FROM var_name
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlQname{nil}
  }

on_privilege_target_clause:
  ON privilege_target
  {
    tmp := $2.(*sqlTargetList).targetList
    $$ = &sqlTargetListPtr{&tmp}
  }
| /* EMPTY */
  {
    $$ = &sqlTargetListPtr{nil}
  }

for_grantee_clause:
  FOR grantee_list
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlStrs{nil}
  }

// CREATE TABLE relname
create_table_stmt:
  CREATE TABLE any_name '(' opt_table_elem_list ')'
  {
    $$ = &sqlStmt{&CreateTable{Table: $3.(*sqlQname).qname, IfNotExists: false, Defs: $5.(*sqlTblDefs).tblDefs}}
  }
| CREATE TABLE IF NOT EXISTS any_name '(' opt_table_elem_list ')'
  {
    $$ = &sqlStmt{&CreateTable{Table: $6.(*sqlQname).qname, IfNotExists: true, Defs: $8.(*sqlTblDefs).tblDefs}}
  }

opt_table_elem_list:
  table_elem_list
| /* EMPTY */
  {
    $$ = &sqlTblDefs{nil}
  }

table_elem_list:
  table_elem
  {
    $$ = &sqlTblDefs{TableDefs{$1.(*sqlTblDef).tblDef}}
  }
| table_elem_list ',' table_elem
  {
    $1.(*sqlTblDefs).tblDefs = append($1.(*sqlTblDefs).tblDefs, $3.(*sqlTblDef).tblDef)
    $$ = $1
  }

table_elem:
  column_def
  {
    $$ = &sqlTblDef{$1.(*sqlColDef).colDef}
  }
| index_def
| table_constraint
  {
    $$ = &sqlTblDef{$1.(*sqlConstraintDef).constraintDef}
  }

column_def:
  name typename col_qual_list
  {
    $$ = &sqlColDef{newColumnTableDef(Name($1), $2.(*sqlColType).colType, $3.(*sqlColQuals).colQuals)}
  }

col_qual_list:
  col_qual_list col_qualification
  {
    $1.(*sqlColQuals).colQuals = append($1.(*sqlColQuals).colQuals, $2.(*sqlColQual).colQual)
    $$ = $1
  }
| /* EMPTY */
  {
    $$ = &sqlColQuals{nil}
  }

col_qualification:
  CONSTRAINT name col_qualification_elem
  {
    $$ = $3
  }
| col_qualification_elem
| COLLATE any_name { unimplemented() }

// DEFAULT NULL is already the default for Postgres. But define it here and
// carry it forward into the system to make it explicit.
// - thomas 1998-09-13
//
// WITH NULL and NULL are not SQL-standard syntax elements, so leave them
// out. Use DEFAULT NULL to explicitly indicate that a column may have that
// value. WITH NULL leads to shift/reduce conflicts with WITH TIME ZONE anyway.
// - thomas 1999-01-08
//
// DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
// conflict on NOT (since NOT might start a subsequent NOT NULL constraint, or
// be part of a_expr NOT LIKE or similar constructs).
col_qualification_elem:
  NOT NULL
  {
    $$ = &sqlColQual{NotNullConstraint{}}
  }
| NULL
  {
    $$ = &sqlColQual{NullConstraint{}}
  }
| UNIQUE
  {
    $$ = &sqlColQual{UniqueConstraint{}}
  }
| PRIMARY KEY
  {
    $$ = &sqlColQual{PrimaryKeyConstraint{}}
  }
| CHECK '(' a_expr ')' { unimplemented() }
| DEFAULT b_expr
  {
    if ContainsVars($2.(*sqlExpr).expr) {
      sqllex.Error("default expression contains a variable")
      return 1
    }
    if containsSubquery($2.(*sqlExpr).expr) {
      sqllex.Error("default expression contains a subquery")
      return 1
    }
    $$ = &sqlColQual{&ColumnDefault{Expr: $2.(*sqlExpr).expr}}
  }
| REFERENCES qualified_name opt_column_list key_match key_actions { unimplemented() }

index_def:
  INDEX opt_name '(' index_params ')' opt_storing
  {
    $$ = &sqlTblDef{&IndexTableDef{
      Name:    Name($2),
      Columns: $4.(*sqlIdxElems).idxElems,
      Storing: $6.(*sqlStrs).strs,
    }}
  }
| UNIQUE INDEX opt_name '(' index_params ')' opt_storing
  {
    $$ = &sqlTblDef{&UniqueConstraintTableDef{
	  IndexTableDef: IndexTableDef {
	    Name:    Name($3),
	    Columns: $5.(*sqlIdxElems).idxElems,
	    Storing: $7.(*sqlStrs).strs,
	  },
	}}
  }

// constraint_elem specifies constraint syntax which is not embedded into a
// column definition. col_qualification_elem specifies the embedded form.
// - thomas 1997-12-03
table_constraint:
  CONSTRAINT name constraint_elem
  {
    $$ = $3
    $$.(*sqlConstraintDef).constraintDef.setName(Name($2))
  }
| constraint_elem
  {
    $$ = $1
  }

constraint_elem:
  CHECK '(' a_expr ')' { unimplemented() }
| UNIQUE '(' name_list ')' opt_storing
  {
    $$ = &sqlConstraintDef{&UniqueConstraintTableDef{
      IndexTableDef: IndexTableDef{
        Columns: NameListToIndexElems($3.(*sqlStrs).strs),
        Storing: $5.(*sqlStrs).strs,
      },
    }}
  }
| PRIMARY KEY '(' name_list ')'
  {
    $$ = &sqlConstraintDef{&UniqueConstraintTableDef{
      IndexTableDef: IndexTableDef{
        Columns: NameListToIndexElems($4.(*sqlStrs).strs),
      },
      PrimaryKey:    true,
    }}
  }
| FOREIGN KEY '(' name_list ')' REFERENCES qualified_name
    opt_column_list key_match key_actions { unimplemented() }

storing:
  COVERING
| STORING

// TODO(pmattis): It would be nice to support a syntax like STORING
// ALL or STORING (*). The syntax addition is straightforward, but we
// need to be careful with the rest of the implementation. In
// particular, columns stored at indexes are currently encoded in such
// a way that adding a new column would require rewriting the existing
// index values. We will need to change the storage format so that it
// is a list of <columnID, value> pairs which will allow both adding
// and dropping columns without rewriting indexes that are storing the
// adjusted column.
opt_storing:
  storing '(' name_list ')'
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = &sqlStrs{nil}
  }

opt_column_list:
  '(' name_list ')'
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlStrs{nil}
  }

key_match:
  MATCH FULL { unimplemented() }
| MATCH PARTIAL { unimplemented() }
| MATCH SIMPLE { unimplemented() }
| /* EMPTY */ {}

// We combine the update and delete actions into one value temporarily for
// simplicity of parsing, and then break them down again in the calling
// production. update is in the left 8 bits, delete in the right. Note that
// NOACTION is the default.
key_actions:
  key_update { unimplemented() }
| key_delete { unimplemented() }
| key_update key_delete { unimplemented() }
| key_delete key_update { unimplemented() }
| /* EMPTY */ {}

key_update:
  ON UPDATE key_action { unimplemented() }

key_delete:
  ON DELETE key_action { unimplemented() }

key_action:
  NO ACTION { unimplemented() }
| RESTRICT { unimplemented() }
| CASCADE { unimplemented() }
| SET NULL { unimplemented() }
| SET DEFAULT { unimplemented() }

numeric_only:
  FCONST
  {
    $$ = &sqlExpr{NumVal($1)}
  }
| '-' FCONST
  {
    $$ = &sqlExpr{NumVal("-" + $2)}
  }
| signed_iconst
  {
    $$ = &sqlExpr{DInt($1.Val)}
  }

// TRUNCATE table relname1, relname2, ...
truncate_stmt:
  TRUNCATE opt_table relation_expr_list opt_drop_behavior
  {
    $$ = &sqlStmt{&Truncate{Tables: $3.(*sqlQnames).qnames}}
  }

// CREATE INDEX
create_index_stmt:
  CREATE opt_unique INDEX opt_name ON qualified_name '(' index_params ')' opt_storing
  {
    $$ = &sqlStmt{&CreateIndex{
      Name:    Name($4),
      Table:   $6.(*sqlQname).qname,
      Unique:  $2.(*sqlBoolVal).boolVal,
      Columns: $8.(*sqlIdxElems).idxElems,
      Storing: $10.(*sqlStrs).strs,
    }}
  }
| CREATE opt_unique INDEX IF NOT EXISTS name ON qualified_name '(' index_params ')' opt_storing
  {
    $$ = &sqlStmt{&CreateIndex{
      Name:        Name($7),
      Table:       $9.(*sqlQname).qname,
      Unique:      $2.(*sqlBoolVal).boolVal,
      IfNotExists: true,
      Columns:     $11.(*sqlIdxElems).idxElems,
      Storing:     $13.(*sqlStrs).strs,
    }}
  }

opt_unique:
  UNIQUE
  {
    $$ = &sqlBoolVal{true}
  }
| /* EMPTY */
  {
    $$ = &sqlBoolVal{false}
  }

index_params:
  index_elem
  {
    $$ = &sqlIdxElems{IndexElemList{$1.(*sqlIdxElem).idxElem}}
  }
| index_params ',' index_elem
  {
    $1.(*sqlIdxElems).idxElems = append($1.(*sqlIdxElems).idxElems, $3.(*sqlIdxElem).idxElem)
    $$ = $1
  }

// Index attributes can be either simple column references, or arbitrary
// expressions in parens. For backwards-compatibility reasons, we allow an
// expression that's just a function call to be written without parens.
index_elem:
  name opt_collate opt_asc_desc
  {
    $$ = &sqlIdxElem{IndexElem{Column: Name($1), Direction: $3.(*sqlDir).dir}}
  }
| func_expr_windowless opt_collate opt_asc_desc { unimplemented() }
| '(' a_expr ')' opt_collate opt_asc_desc { unimplemented() }

opt_collate:
  COLLATE any_name { unimplemented() }
| /* EMPTY */ {}

opt_asc_desc:
  ASC
  {
    $$ = &sqlDir{Ascending}
  }
| DESC
  {
    $$ = &sqlDir{Descending}
  }
| /* EMPTY */
  {
    $$ = &sqlDir{DefaultDirection}
  }

// ALTER THING name RENAME TO newname
rename_stmt:
  ALTER DATABASE name RENAME TO name
  {
    $$ = &sqlStmt{&RenameDatabase{Name: Name($3), NewName: Name($6)}}
  }
| ALTER TABLE relation_expr RENAME TO qualified_name
  {
    $$ = &sqlStmt{&RenameTable{Name: $3.(*sqlQname).qname, NewName: $6.(*sqlQname).qname, IfExists: false}}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME TO qualified_name
  {
    $$ = &sqlStmt{&RenameTable{Name: $5.(*sqlQname).qname, NewName: $8.(*sqlQname).qname, IfExists: true}}
  }
| ALTER INDEX qualified_name RENAME TO name
  {
    $$ = &sqlStmt{&RenameIndex{Name: $3.(*sqlQname).qname, NewName: Name($6), IfExists: false}}
  }
| ALTER INDEX IF EXISTS qualified_name RENAME TO name
  {
    $$ = &sqlStmt{&RenameIndex{Name: $5.(*sqlQname).qname, NewName: Name($8), IfExists: true}}
  }
| ALTER TABLE relation_expr RENAME opt_column name TO name
  {
    $$ = &sqlStmt{&RenameColumn{Table: $3.(*sqlQname).qname, Name: Name($6), NewName: Name($8), IfExists: false}}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name
  {
    $$ = &sqlStmt{&RenameColumn{Table: $5.(*sqlQname).qname, Name: Name($8), NewName: Name($10), IfExists: true}}
  }
| ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
  {
    $$ = &sqlStmt{nil}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME CONSTRAINT name TO name
  {
    $$ = &sqlStmt{nil}
  }

opt_column:
  COLUMN
  {
    $$ = &sqlBoolVal{true}
  }
| /* EMPTY */
  {
    $$ = &sqlBoolVal{false}
  }

opt_set_data:
  SET DATA {}
| /* EMPTY */ {}

// BEGIN / START / COMMIT / END / ROLLBACK / ...
transaction_stmt:
  BEGIN opt_transaction opt_transaction_mode_list
  {
    $$ = $3
  }
| START TRANSACTION opt_transaction_mode_list
  {
    $$ = $3
  }
| COMMIT opt_transaction
  {
    $$ = &sqlStmt{&CommitTransaction{}}
  }
| END opt_transaction
  {
    $$ = &sqlStmt{&CommitTransaction{}}
  }
| ROLLBACK opt_transaction
  {
    $$ = &sqlStmt{&RollbackTransaction{}}
  }

opt_transaction:
  TRANSACTION {}
| /* EMPTY */ {}

opt_transaction_mode_list:
  transaction_iso_level
  {
    $$ = &sqlStmt{&BeginTransaction{Isolation: $1.(*sqlIsoLevel).isoLevel, UserPriority: UnspecifiedUserPriority}}
  }
| transaction_user_priority
  {
    $$ = &sqlStmt{&BeginTransaction{Isolation: UnspecifiedIsolation, UserPriority: $1.(*sqlUserPriority).userPriority}}
  }
| transaction_iso_level ',' transaction_user_priority
  {
    $$ = &sqlStmt{&BeginTransaction{Isolation: $1.(*sqlIsoLevel).isoLevel, UserPriority: $3.(*sqlUserPriority).userPriority}}
  }
| transaction_user_priority ',' transaction_iso_level
  {
    $$ = &sqlStmt{&BeginTransaction{Isolation: $3.(*sqlIsoLevel).isoLevel, UserPriority: $1.(*sqlUserPriority).userPriority}}
  }
| /* EMPTY */
  {
    $$ = &sqlStmt{&BeginTransaction{Isolation: UnspecifiedIsolation, UserPriority: UnspecifiedUserPriority}}
  }

transaction_iso_level:
  ISOLATION LEVEL iso_level
  {
    $$ = $3
  }

create_database_stmt:
  CREATE DATABASE name
  {
    $$ = &sqlStmt{&CreateDatabase{Name: Name($3)}}
  }
| CREATE DATABASE IF NOT EXISTS name
  {
    $$ = &sqlStmt{&CreateDatabase{IfNotExists: true, Name: Name($6)}}
  }

insert_stmt:
  opt_with_clause INSERT INTO insert_target insert_rest opt_on_conflict
  {
    $$ = $5
    $$.(*sqlStmt).stmt.(*Insert).Table = $4.(*sqlQname).qname
  }

// Can't easily make AS optional here, because VALUES in insert_rest would have
// a shift/reduce conflict with VALUES as an optional alias. We could easily
// allow unreserved_keywords as optional aliases, but that'd be an odd
// divergence from other places. So just require AS for now.
insert_target:
  qualified_name
| qualified_name AS name
  // TODO(pmattis): Support alias.

insert_rest:
  select_stmt
  {
    $$ = &sqlStmt{&Insert{Rows: $1.(*sqlSelectStmt).selectStmt}}
  }
| '(' qualified_name_list ')' select_stmt
  {
    $$ = &sqlStmt{&Insert{Columns: $2.(*sqlQnames).qnames, Rows: $4.(*sqlSelectStmt).selectStmt}}
  }
| DEFAULT VALUES
  {
    $$ = &sqlStmt{&Insert{}}
  }

// TODO(andrei): If this is ever supported, `opt_conf_expr` needs to use something different
// than `index_params`.
opt_on_conflict:
  ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list where_clause { unimplemented() }
| ON CONFLICT opt_conf_expr DO NOTHING { unimplemented() }
| /* EMPTY */ {}

opt_conf_expr:
  '(' index_params ')' where_clause { unimplemented() }
| ON CONSTRAINT name { unimplemented() }
| /* EMPTY */ {}

update_stmt:
  opt_with_clause UPDATE relation_expr_opt_alias SET set_clause_list from_clause where_clause
  {
    $$ = &sqlStmt{&Update{Table: $3.(*sqlTblExpr).tblExpr, Exprs: $5.(*sqlUpdateExprs).updateExprs, Where: newWhere(astWhere, $7.(*sqlExpr).expr)}}
  }

set_clause_list:
  set_clause
  {
    $$ = &sqlUpdateExprs{UpdateExprs{$1.(*sqlUpdateExpr).updateExpr}}
  }
| set_clause_list ',' set_clause
  {
    $1.(*sqlUpdateExprs).updateExprs = append($1.(*sqlUpdateExprs).updateExprs, $3.(*sqlUpdateExpr).updateExpr)
    $$ = $1
  }

set_clause:
  single_set_clause
| multiple_set_clause

single_set_clause:
  qualified_name '=' ctext_expr
  {
    $$ = &sqlUpdateExpr{&UpdateExpr{Names: QualifiedNames{$1.(*sqlQname).qname}, Expr: $3.(*sqlExpr).expr}}
  }

// Ideally, we'd accept any row-valued a_expr as RHS of a multiple_set_clause.
// However, per SQL spec the row-constructor case must allow DEFAULT as a row
// member, and it's pretty unclear how to do that (unless perhaps we allow
// DEFAULT in any a_expr and let parse analysis sort it out later?). For the
// moment, the planner/executor only support a subquery as a multiassignment
// source anyhow, so we need only accept ctext_row and subqueries here.
multiple_set_clause:
  '(' qualified_name_list ')' '=' ctext_row
  {
    $$ = &sqlUpdateExpr{&UpdateExpr{Tuple: true, Names: $2.(*sqlQnames).qnames, Expr: Tuple($5.(*sqlExprs).exprs)}}
  }
| '(' qualified_name_list ')' '=' select_with_parens
  {
    $$ = &sqlUpdateExpr{&UpdateExpr{Tuple: true, Names: $2.(*sqlQnames).qnames, Expr: &Subquery{Select: $5.(*sqlSelectStmt).selectStmt}}}
  }

// A complete SELECT statement looks like this.
//
// The rule returns either a single select_stmt node or a tree of them,
// representing a set-operation tree.
//
// There is an ambiguity when a sub-SELECT is within an a_expr and there are
// excess parentheses: do the parentheses belong to the sub-SELECT or to the
// surrounding a_expr?  We don't really care, but bison wants to know. To
// resolve the ambiguity, we are careful to define the grammar so that the
// decision is staved off as long as possible: as long as we can keep absorbing
// parentheses into the sub-SELECT, we will do so, and only when it's no longer
// possible to do that will we decide that parens belong to the expression. For
// example, in "SELECT (((SELECT 2)) + 3)" the extra parentheses are treated as
// part of the sub-select. The necessity of doing it that way is shown by
// "SELECT (((SELECT 2)) UNION SELECT 2)". Had we parsed "((SELECT 2))" as an
// a_expr, it'd be too late to go back to the SELECT viewpoint when we see the
// UNION.
//
// This approach is implemented by defining a nonterminal select_with_parens,
// which represents a SELECT with at least one outer layer of parentheses, and
// being careful to use select_with_parens, never '(' select_stmt ')', in the
// expression grammar. We will then have shift-reduce conflicts which we can
// resolve in favor of always treating '(' <select> ')' as a
// select_with_parens. To resolve the conflicts, the productions that conflict
// with the select_with_parens productions are manually given precedences lower
// than the precedence of ')', thereby ensuring that we shift ')' (and then
// reduce to select_with_parens) rather than trying to reduce the inner
// <select> nonterminal to something else. We use UMINUS precedence for this,
// which is a fairly arbitrary choice.
//
// To be able to define select_with_parens itself without ambiguity, we need a
// nonterminal select_no_parens that represents a SELECT structure with no
// outermost parentheses. This is a little bit tedious, but it works.
//
// In non-expression contexts, we use select_stmt which can represent a SELECT
// with or without outer parentheses.

select_stmt:
  select_no_parens %prec UMINUS
| select_with_parens %prec UMINUS

select_with_parens:
  '(' select_no_parens ')'
  {
    $$ = &sqlSelectStmt{&ParenSelect{Select: $2.(*sqlSelectStmt).selectStmt}}
  }
| '(' select_with_parens ')'
  {
    $$ = &sqlSelectStmt{&ParenSelect{Select: $2.(*sqlSelectStmt).selectStmt}}
  }

// This rule parses the equivalent of the standard's <query expression>. The
// duplicative productions are annoying, but hard to get rid of without
// creating shift/reduce conflicts.
//
//      The locking clause (FOR UPDATE etc) may be before or after
//      LIMIT/OFFSET. In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE We
//      now support both orderings, but prefer LIMIT/OFFSET before the locking
//      clause.
//      - 2002-08-28 bjm
select_no_parens:
  simple_select
| select_clause sort_clause
  {
    $$ = $1
    if s, ok := $$.(*sqlSelectStmt).selectStmt.(*Select); ok {
      s.OrderBy = $2.(*sqlOrderBy).orderBy
    }
  }
| select_clause opt_sort_clause select_limit
  {
    $$ = $1
    if s, ok := $$.(*sqlSelectStmt).selectStmt.(*Select); ok {
      s.OrderBy = $2.(*sqlOrderBy).orderBy
      s.Limit = $3.(*sqlLimit).limit
    }
  }
| with_clause select_clause
  {
    $$ = $2
  }
| with_clause select_clause sort_clause
  {
    $$ = $2
    if s, ok := $$.(*sqlSelectStmt).selectStmt.(*Select); ok {
      s.OrderBy = $3.(*sqlOrderBy).orderBy
    }
  }
| with_clause select_clause opt_sort_clause select_limit
  {
    $$ = $2
    if s, ok := $$.(*sqlSelectStmt).selectStmt.(*Select); ok {
      s.OrderBy = $3.(*sqlOrderBy).orderBy
      s.Limit = $4.(*sqlLimit).limit
    }
  }

select_clause:
  simple_select
| select_with_parens

// This rule parses SELECT statements that can appear within set operations,
// including UNION, INTERSECT and EXCEPT. '(' and ')' can be used to specify
// the ordering of the set operations. Without '(' and ')' we want the
// operations to be ordered per the precedence specs at the head of this file.
//
// As with select_no_parens, simple_select cannot have outer parentheses, but
// can have parenthesized subclauses.
//
// Note that sort clauses cannot be included at this level --- SQL requires
//       SELECT foo UNION SELECT bar ORDER BY baz
// to be parsed as
//       (SELECT foo UNION SELECT bar) ORDER BY baz
// not
//       SELECT foo UNION (SELECT bar ORDER BY baz)
//
// Likewise for WITH, FOR UPDATE and LIMIT. Therefore, those clauses are
// described as part of the select_no_parens production, not simple_select.
// This does not limit functionality, because you can reintroduce these clauses
// inside parentheses.
//
// NOTE: only the leftmost component select_stmt should have INTO. However,
// this is not checked by the grammar; parse analysis must check it.
simple_select:
  SELECT opt_all_clause opt_target_list from_clause where_clause group_clause having_clause window_clause
  {
    $$ = &sqlSelectStmt{&Select{
      Exprs:   $3.(*sqlSelExprs).selExprs,
      From:    $4.(*sqlTblExprs).tblExprs,
      Where:   newWhere(astWhere, $5.(*sqlExpr).expr),
      GroupBy: $6.(*sqlGroupBy).groupBy,
      Having:  newWhere(astHaving, $7.(*sqlExpr).expr),
    }}
  }
| SELECT distinct_clause target_list from_clause where_clause group_clause having_clause window_clause
  {
    $$ = &sqlSelectStmt{&Select{
      Distinct: $2.(*sqlBoolVal).boolVal,
      Exprs:    $3.(*sqlSelExprs).selExprs,
      From:     $4.(*sqlTblExprs).tblExprs,
      Where:    newWhere(astWhere, $5.(*sqlExpr).expr),
      GroupBy:  $6.(*sqlGroupBy).groupBy,
      Having:   newWhere(astHaving, $7.(*sqlExpr).expr),
    }}
  }
| values_clause
| TABLE relation_expr
  {
    $$ = &sqlSelectStmt{&Select{
      Exprs:       SelectExprs{starSelectExpr()},
      From:        TableExprs{&AliasedTableExpr{Expr: $2.(*sqlQname).qname}},
      tableSelect: true,
    }}
  }
| select_clause UNION all_or_distinct select_clause
  {
    $$ = &sqlSelectStmt{&Union{
      Type:  astUnion,
      Left:  $1.(*sqlSelectStmt).selectStmt,
      Right: $4.(*sqlSelectStmt).selectStmt,
      All:   $3.(*sqlBoolVal).boolVal,
    }}
  }
| select_clause INTERSECT all_or_distinct select_clause
  {
    $$ = &sqlSelectStmt{&Union{
      Type:  astIntersect,
      Left:  $1.(*sqlSelectStmt).selectStmt,
      Right: $4.(*sqlSelectStmt).selectStmt,
      All:   $3.(*sqlBoolVal).boolVal,
    }}
  }
| select_clause EXCEPT all_or_distinct select_clause
  {
    $$ = &sqlSelectStmt{&Union{
      Type:  astExcept,
      Left:  $1.(*sqlSelectStmt).selectStmt,
      Right: $4.(*sqlSelectStmt).selectStmt,
      All:   $3.(*sqlBoolVal).boolVal,
    }}
  }

// SQL standard WITH clause looks like:
//
// WITH [ RECURSIVE ] <query name> [ (<column>,...) ]
//        AS (query) [ SEARCH or CYCLE clause ]
//
// We don't currently support the SEARCH or CYCLE clause.
//
// Recognizing WITH_LA here allows a CTE to be named TIME or ORDINALITY.
with_clause:
WITH cte_list { unimplemented() }
| WITH_LA cte_list { unimplemented() }
| WITH RECURSIVE cte_list { unimplemented() }

cte_list:
  common_table_expr { unimplemented() }
| cte_list ',' common_table_expr { unimplemented() }

common_table_expr:
  name opt_name_list AS '(' preparable_stmt ')' { unimplemented() }

preparable_stmt:
  select_stmt
  {
    $$ = &sqlStmt{$1.(*sqlSelectStmt).selectStmt}
  }
| insert_stmt
| update_stmt
| delete_stmt

opt_with_clause:
  with_clause { unimplemented() }
| /* EMPTY */ {}

opt_table:
  TABLE {}
| /* EMPTY */ {}

all_or_distinct:
  ALL
  {
    $$ = &sqlBoolVal{true}
  }
| DISTINCT
  {
    $$ = &sqlBoolVal{false}
  }
| /* EMPTY */
  {
    $$ = &sqlBoolVal{false}
  }

distinct_clause:
  DISTINCT
  {
    $$ = &sqlBoolVal{true}
  }

opt_all_clause:
  ALL {}
| /* EMPTY */ {}

opt_sort_clause:
  sort_clause
  {
    $$ = $1
  }
| /* EMPTY */
  {
    $$ = &sqlOrderBy{nil}
  }

sort_clause:
  ORDER BY sortby_list
  {
    $$ = &sqlOrderBy{OrderBy($3.(*sqlOrders).orders)}
  }

sortby_list:
  sortby
  {
    $$ = &sqlOrders{[]*Order{$1.(*sqlOrder).order}}
  }
| sortby_list ',' sortby
  {
    $1.(*sqlOrders).orders = append($1.(*sqlOrders).orders, $3.(*sqlOrder).order)
    $$ = $1
  }

sortby:
  a_expr opt_asc_desc
  {
    $$ = &sqlOrder{&Order{Expr: $1.(*sqlExpr).expr, Direction: $2.(*sqlDir).dir}}
  }
// TODO(pmattis): Support ordering using arbitrary math ops?
// | a_expr USING math_op {}

select_limit:
  limit_clause offset_clause
  {
    if $1 == nil {
      $$ = $2
    } else {
      $$ = $1
      $$.(*sqlLimit).limit.Offset = $2.(*sqlLimit).limit.Offset
    }
  }
| offset_clause limit_clause
  {
    $$ = $1
    if $2.(*sqlLimit).limit != nil {
      $$.(*sqlLimit).limit.Count = $2.(*sqlLimit).limit.Count
    }
  }
| limit_clause
| offset_clause

limit_clause:
  LIMIT select_limit_value
  {
    if $2.(*sqlExpr).expr == nil {
      $$ = &sqlLimit{nil}
    } else {
      $$ = &sqlLimit{&Limit{Count: $2.(*sqlExpr).expr}}
    }
  }
// SQL:2008 syntax
// TODO(pmattis): Should we support this?
// | FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY { unimplemented() }

offset_clause:
  OFFSET a_expr
  {
    $$ = &sqlLimit{&Limit{Offset: $2.(*sqlExpr).expr}}
  }
  // SQL:2008 syntax
  // The trailing ROW/ROWS in this case prevent the full expression
  // syntax. c_expr is the best we can do.
| OFFSET c_expr row_or_rows
  {
    $$ = &sqlLimit{&Limit{Offset: $2.(*sqlExpr).expr}}
  }

select_limit_value:
  a_expr
| ALL
  {
    $$ = &sqlExpr{nil}
  }

// Allowing full expressions without parentheses causes various parsing
// problems with the trailing ROW/ROWS key words. SQL only calls for constants,
// so we allow the rest only with parentheses. If omitted, default to 1.
// opt_select_fetch_first_value:
//   signed_iconst { unimplemented() }
// | '(' a_expr ')' { unimplemented() }
// | /* EMPTY */ {}

// noise words
row_or_rows:
  ROW {}
| ROWS {}

// first_or_next:
//   FIRST { unimplemented() }
// | NEXT { unimplemented() }

// This syntax for group_clause tries to follow the spec quite closely.
// However, the spec allows only column references, not expressions,
// which introduces an ambiguity between implicit row constructors
// (a,b) and lists of column references.
//
// We handle this by using the a_expr production for what the spec calls
// <ordinary grouping set>, which in the spec represents either one column
// reference or a parenthesized list of column references. Then, we check the
// top node of the a_expr to see if it's an implicit RowExpr, and if so, just
// grab and use the list, discarding the node. (this is done in parse analysis,
// not here)
//
// (we abuse the row_format field of RowExpr to distinguish implicit and
// explicit row constructors; it's debatable if anyone sanely wants to use them
// in a group clause, but if they have a reason to, we make it possible.)
//
// Each item in the group_clause list is either an expression tree or a
// GroupingSet node of some type.
group_clause:
  GROUP BY expr_list
  {
    $$ = &sqlGroupBy{GroupBy($3.(*sqlExprs).exprs)}
  }
| /* EMPTY */
  {
    $$ = &sqlGroupBy{nil}
  }

having_clause:
  HAVING a_expr
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlExpr{nil}
  }

values_clause:
  VALUES ctext_row
  {
    $$ = &sqlSelectStmt{Values{Tuple($2.(*sqlExprs).exprs)}}
  }
| values_clause ',' ctext_row
  {
    $1.(*sqlSelectStmt).selectStmt = append($1.(*sqlSelectStmt).selectStmt.(Values), Tuple($3.(*sqlExprs).exprs))
    $$ = $1
  }

// clauses common to all optimizable statements:
//  from_clause   - allow list of both JOIN expressions and table names
//  where_clause  - qualifications for joins or restrictions

from_clause:
  FROM from_list
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlTblExprs{nil}
  }

from_list:
  table_ref
  {
    $$ = &sqlTblExprs{TableExprs{$1.(*sqlTblExpr).tblExpr}}
  }
| from_list ',' table_ref
  {
    $1.(*sqlTblExprs).tblExprs = append($1.(*sqlTblExprs).tblExprs, $3.(*sqlTblExpr).tblExpr)
    $$ = $1
  }

// table_ref is where an alias clause can be attached.
table_ref:
  relation_expr opt_alias_clause
  {
    $$ = &sqlTblExpr{&AliasedTableExpr{Expr: $1.(*sqlQname).qname, As: $2.(*sqlAliasClause).aliasClause}}
  }
| select_with_parens opt_alias_clause
  {
    $$ = &sqlTblExpr{&AliasedTableExpr{Expr: &Subquery{Select: $1.(*sqlSelectStmt).selectStmt}, As: $2.(*sqlAliasClause).aliasClause}}
  }
| joined_table
| '(' joined_table ')' alias_clause { unimplemented() }

// It may seem silly to separate joined_table from table_ref, but there is
// method in SQL's madness: if you don't do it this way you get reduce- reduce
// conflicts, because it's not clear to the parser generator whether to expect
// alias_clause after ')' or not. For the same reason we must treat 'JOIN' and
// 'join_type JOIN' separately, rather than allowing join_type to expand to
// empty; if we try it, the parser generator can't figure out when to reduce an
// empty join_type right after table_ref.
//
// Note that a CROSS JOIN is the same as an unqualified INNER JOIN, and an
// INNER JOIN/ON has the same shape but a qualification expression to limit
// membership. A NATURAL JOIN implicitly matches column names between tables
// and the shape is determined by which columns are in common. We'll collect
// columns during the later transformations.

joined_table:
  '(' joined_table ')'
  {
    $$ = &sqlTblExpr{&ParenTableExpr{Expr: $2.(*sqlTblExpr).tblExpr}}
  }
| table_ref CROSS JOIN table_ref
  {
    $$ = &sqlTblExpr{&JoinTableExpr{Join: astCrossJoin, Left: $1.(*sqlTblExpr).tblExpr, Right: $4.(*sqlTblExpr).tblExpr}}
  }
| table_ref join_type JOIN table_ref join_qual
  {
    $$ = &sqlTblExpr{&JoinTableExpr{Join: $2, Left: $1.(*sqlTblExpr).tblExpr, Right: $4.(*sqlTblExpr).tblExpr, Cond: $5.(*sqlJoinCond).joinCond}}
  }
| table_ref JOIN table_ref join_qual
  {
    $$ = &sqlTblExpr{&JoinTableExpr{Join: astJoin, Left: $1.(*sqlTblExpr).tblExpr, Right: $3.(*sqlTblExpr).tblExpr, Cond: $4.(*sqlJoinCond).joinCond}}
  }
| table_ref NATURAL join_type JOIN table_ref
  {
    $$ = &sqlTblExpr{&JoinTableExpr{Join: astNaturalJoin, Left: $1.(*sqlTblExpr).tblExpr, Right: $5.(*sqlTblExpr).tblExpr}}
  }
| table_ref NATURAL JOIN table_ref
  {
    $$ = &sqlTblExpr{&JoinTableExpr{Join: astNaturalJoin, Left: $1.(*sqlTblExpr).tblExpr, Right: $4.(*sqlTblExpr).tblExpr}}
  }

alias_clause:
  AS name '(' name_list ')'
  {
    $$ = &sqlAliasClause{AliasClause{Alias: Name($2), Cols: NameList($4.(*sqlStrs).strs)}}
  }
| AS name
  {
    $$ = &sqlAliasClause{AliasClause{Alias: Name($2)}}
  }
| name '(' name_list ')'
  {
    $$ = &sqlAliasClause{AliasClause{Alias: Name($1), Cols: NameList($3.(*sqlStrs).strs)}}
  }
| name
  {
    $$ = &sqlAliasClause{AliasClause{Alias: Name($1)}}
  }

opt_alias_clause:
  alias_clause
| /* EMPTY */
  {
    $$ = &sqlAliasClause{AliasClause{}}
  }

join_type:
  FULL join_outer
  {
    $$ = astFullJoin
  }
| LEFT join_outer
  {
    $$ = astLeftJoin
  }
| RIGHT join_outer
  {
    $$ = astRightJoin
  }
| INNER
  {
    $$ = astInnerJoin
  }

// OUTER is just noise...
join_outer:
  OUTER {}
| /* EMPTY */ {}

// JOIN qualification clauses
// Possibilities are:
//      USING ( column list ) allows only unqualified column names,
//          which must match between tables.
//      ON expr allows more general qualifications.
//
// We return USING as a List node, while an ON-expr will not be a List.
join_qual:
  USING '(' name_list ')'
  {
    $$ = &sqlJoinCond{&UsingJoinCond{Cols: NameList($3.(*sqlStrs).strs)}}
  }
| ON a_expr
  {
    $$ = &sqlJoinCond{&OnJoinCond{Expr: $2.(*sqlExpr).expr}}
  }

relation_expr:
  qualified_name
  {
    $$ = $1
  }
| qualified_name '*'
  {
    $$ = $1
  }
| ONLY qualified_name
  {
    $$ = $2
  }
| ONLY '(' qualified_name ')'
  {
    $$ = $3
  }

relation_expr_list:
  relation_expr
  {
    $$ = &sqlQnames{QualifiedNames{$1.(*sqlQname).qname}}
  }
| relation_expr_list ',' relation_expr
  {
    $1.(*sqlQnames).qnames = append($1.(*sqlQnames).qnames, $3.(*sqlQname).qname)
    $$ = $1
  }

// Given "UPDATE foo set set ...", we have to decide without looking any
// further ahead whether the first "set" is an alias or the UPDATE's SET
// keyword. Since "set" is allowed as a column name both interpretations are
// feasible. We resolve the shift/reduce conflict by giving the first
// relation_expr_opt_alias production a higher precedence than the SET token
// has, causing the parser to prefer to reduce, in effect assuming that the SET
// is not an alias.
relation_expr_opt_alias:
  relation_expr %prec UMINUS
  {
    $$ = &sqlTblExpr{&AliasedTableExpr{Expr: $1.(*sqlQname).qname}}
  }
| relation_expr name
  {
    $$ = &sqlTblExpr{&AliasedTableExpr{Expr: $1.(*sqlQname).qname, As: AliasClause{Alias: Name($2)}}}
  }
| relation_expr AS name
  {
    $$ = &sqlTblExpr{&AliasedTableExpr{Expr: $1.(*sqlQname).qname, As: AliasClause{Alias: Name($3)}}}
  }

where_clause:
  WHERE a_expr
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlExpr{nil}
  }

// Type syntax
//   SQL introduces a large amount of type-specific syntax.
//   Define individual clauses to handle these cases, and use
//   the generic case to handle regular type-extensible Postgres syntax.
//   - thomas 1997-10-10

typename:
  simple_typename opt_array_bounds
  {
    $$ = $1
  }
  // SQL standard syntax, currently only one-dimensional
| simple_typename ARRAY '[' ICONST ']' { unimplemented() }
| simple_typename ARRAY { unimplemented() }

opt_array_bounds:
  opt_array_bounds '[' ']' { unimplemented() }
| opt_array_bounds '[' ICONST ']' { unimplemented() }
| /* EMPTY */ {}

simple_typename:
  numeric
| bit
| character
| const_datetime
| const_interval opt_interval // TODO(pmattis): Support opt_interval?
| const_interval '(' ICONST ')' { unimplemented() }
| BLOB
  {
    $$ = &sqlColType{&BytesType{Name: "BLOB"}}
  }
| BYTES
  {
    $$ = &sqlColType{&BytesType{Name: "BYTES"}}
  }
| BYTEA
  {
    $$ = &sqlColType{&BytesType{Name: "BYTEA"}}
  }
| TEXT
  {
    $$ = &sqlColType{&StringType{Name: "TEXT"}}
  }
| STRING
  {
    $$ = &sqlColType{&StringType{Name: "STRING"}}
  }

// We have a separate const_typename to allow defaulting fixed-length types
// such as CHAR() and BIT() to an unspecified length. SQL9x requires that these
// default to a length of one, but this makes no sense for constructs like CHAR
// 'hi' and BIT '0101', where there is an obvious better choice to make. Note
// that const_interval is not included here since it must be pushed up higher
// in the rules to accommodate the postfix options (e.g. INTERVAL '1'
// YEAR). Likewise, we have to handle the generic-type-name case in
// a_expr_const to avoid premature reduce/reduce conflicts against function
// names.
const_typename:
  numeric
| const_bit
| const_character
| const_datetime

opt_numeric_modifiers:
  '(' ICONST ')'
  {
    $$ = &sqlColType{&DecimalType{Prec: int($2.Val)}}
  }
| '(' ICONST ',' ICONST ')'
  {
    $$ = &sqlColType{&DecimalType{Prec: int($2.Val), Scale: int($4.Val)}}
  }
| /* EMPTY */
  {
    $$ = &sqlColType{&DecimalType{}}
  }

// SQL numeric data types
numeric:
  INT
  {
    $$ = &sqlColType{&IntType{Name: "INT"}}
  }
| INT64
  {
    $$ = &sqlColType{&IntType{Name: "INT64"}}
  }
| INTEGER
  {
    $$ = &sqlColType{&IntType{Name: "INTEGER"}}
  }
| SMALLINT
  {
    $$ = &sqlColType{&IntType{Name: "SMALLINT"}}
  }
| BIGINT
  {
    $$ = &sqlColType{&IntType{Name: "BIGINT"}}
  }
| REAL
  {
    $$ = &sqlColType{&FloatType{Name: "REAL"}}
  }
| FLOAT opt_float
  {
    $$ = &sqlColType{&FloatType{Name: "FLOAT", Prec: int($2.Val)}}
  }
| DOUBLE PRECISION
  {
    $$ = &sqlColType{&FloatType{Name: "DOUBLE PRECISION"}}
  }
| DECIMAL opt_numeric_modifiers
  {
    $$ = $2
    $$.(*sqlColType).colType.(*DecimalType).Name = "DECIMAL"
  }
| DEC opt_numeric_modifiers
  {
    $$ = $2
    $$.(*sqlColType).colType.(*DecimalType).Name = "DEC"
  }
| NUMERIC opt_numeric_modifiers
  {
    $$ = $2
    $$.(*sqlColType).colType.(*DecimalType).Name = "NUMERIC"
  }
| BOOLEAN
  {
    $$ = &sqlColType{&BoolType{Name: "BOOLEAN"}}
  }
| BOOL
  {
    $$ = &sqlColType{&BoolType{Name: "BOOL"}}
  }

opt_float:
  '(' ICONST ')'
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = IntVal{}
  }

// SQL bit-field data types
// The following implements BIT() and BIT VARYING().
bit:
  bit_with_length
| bit_without_length

// const_bit is like bit except "BIT" defaults to unspecified length.
// See notes for const_character, which addresses same issue for "CHAR".
const_bit:
  bit_with_length
| bit_without_length

bit_with_length:
  BIT opt_varying '(' ICONST ')'
  {
    $$ = &sqlColType{&IntType{Name: "BIT", N: int($4.Val)}}
  }

bit_without_length:
  BIT opt_varying
  {
    $$ = &sqlColType{&IntType{Name: "BIT"}}
  }

// SQL character data types
// The following implements CHAR() and VARCHAR().
character:
  character_with_length
| character_without_length

const_character:
  character_with_length
| character_without_length

character_with_length:
  character_base '(' ICONST ')'
  {
    $$ = $1
    $$.(*sqlColType).colType.(*StringType).N = int($3.Val)
  }

character_without_length:
  character_base
  {
    $$ = $1
  }

character_base:
  CHARACTER opt_varying
  {
    $$ = &sqlColType{&StringType{Name: "CHAR"}}
  }
| CHAR opt_varying
  {
    $$ = &sqlColType{&StringType{Name: "CHAR"}}
  }
| VARCHAR
  {
    $$ = &sqlColType{&StringType{Name: "VARCHAR"}}
  }

opt_varying:
  VARYING {}
| /* EMPTY */ {}

// SQL date/time types
const_datetime:
  DATE
  {
    $$ = &sqlColType{&DateType{}}
  }
| TIMESTAMP
  {
    $$ = &sqlColType{&TimestampType{}}
  }

const_interval:
  INTERVAL {
    $$ = &sqlColType{&IntervalType{}}
  }

opt_interval:
  YEAR { unimplemented() }
| MONTH { unimplemented() }
| DAY { unimplemented() }
| HOUR { unimplemented() }
| MINUTE { unimplemented() }
| interval_second { unimplemented() }
| YEAR TO MONTH { unimplemented() }
| DAY TO HOUR { unimplemented() }
| DAY TO MINUTE { unimplemented() }
| DAY TO interval_second { unimplemented() }
| HOUR TO MINUTE { unimplemented() }
| HOUR TO interval_second { unimplemented() }
| MINUTE TO interval_second { unimplemented() }
| /* EMPTY */ {}

interval_second:
  SECOND { unimplemented() }
| SECOND '(' ICONST ')' { unimplemented() }

// General expressions. This is the heart of the expression syntax.
//
// We have two expression types: a_expr is the unrestricted kind, and b_expr is
// a subset that must be used in some places to avoid shift/reduce conflicts.
// For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr" because that
// use of AND conflicts with AND as a boolean operator. So, b_expr is used in
// BETWEEN and we remove boolean keywords from b_expr.
//
// Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
// always be used by surrounding it with parens.
//
// c_expr is all the productions that are common to a_expr and b_expr; it's
// factored out just to eliminate redundant coding.
//
// Be careful of productions involving more than one terminal token. By
// default, bison will assign such productions the precedence of their last
// terminal, but in nearly all cases you want it to be the precedence of the
// first terminal instead; otherwise you will not get the behavior you expect!
// So we use %prec annotations freely to set precedences.
a_expr:
  c_expr
| a_expr TYPECAST typename
  {
    $$ = &sqlExpr{&CastExpr{Expr: $1.(*sqlExpr).expr, Type: $3.(*sqlColType).colType}}
  }
| a_expr COLLATE any_name { unimplemented() }
| a_expr AT TIME ZONE a_expr %prec AT { unimplemented() }
  // These operators must be called out explicitly in order to make use of
  // bison's automatic operator-precedence handling. All other operator names
  // are handled by the generic productions using "OP", below; and all those
  // operators will have the same precedence.
  //
  // If you add more explicitly-known operators, be sure to add them also to
  // b_expr and to the math_op list below.
| '+' a_expr %prec UMINUS
  {
    $$ = &sqlExpr{&UnaryExpr{Operator: UnaryPlus, Expr: $2.(*sqlExpr).expr}}
  }
| '-' a_expr %prec UMINUS
  {
    $$ = &sqlExpr{&UnaryExpr{Operator: UnaryMinus, Expr: $2.(*sqlExpr).expr}}
  }
| '~' a_expr %prec UMINUS
  {
    $$ = &sqlExpr{&UnaryExpr{Operator: UnaryComplement, Expr: $2.(*sqlExpr).expr}}
  }
| a_expr '+' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Plus, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '-' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Minus, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '*' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Mult, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '/' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Div, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '%' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Mod, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '^' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitxor, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '#' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitxor, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '&' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitand, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '|' a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitor, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '<' a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: LT, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '>' a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: GT, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr '=' a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: EQ, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr CONCAT a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Concat, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr LSHIFT a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: LShift, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr RSHIFT a_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: RShift, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr LESS_EQUALS a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: LE, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr GREATER_EQUALS a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: GE, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr NOT_EQUALS a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: NE, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr AND a_expr
  {
    $$ = &sqlExpr{&AndExpr{Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr OR a_expr
  {
    $$ = &sqlExpr{&OrExpr{Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| NOT a_expr
  {
    $$ = &sqlExpr{&NotExpr{Expr: $2.(*sqlExpr).expr}}
  }
| NOT_LA a_expr %prec NOT
  {
    $$ = &sqlExpr{&NotExpr{Expr: $2.(*sqlExpr).expr}}
  }
| a_expr LIKE a_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: Like, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr NOT_LA LIKE a_expr %prec NOT_LA
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: NotLike, Left: $1.(*sqlExpr).expr, Right: $4.(*sqlExpr).expr}}
  }
| a_expr SIMILAR TO a_expr %prec SIMILAR
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: SimilarTo, Left: $1.(*sqlExpr).expr, Right: $4.(*sqlExpr).expr}}
  }
| a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: NotSimilarTo, Left: $1.(*sqlExpr).expr, Right: $5.(*sqlExpr).expr}}
  }
| a_expr IS NULL %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: Is, Left: $1.(*sqlExpr).expr, Right: DNull}}
  }
| a_expr IS NOT NULL %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsNot, Left: $1.(*sqlExpr).expr, Right: DNull}}
  }
| row OVERLAPS row { unimplemented() }
| a_expr IS TRUE %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: Is, Left: $1.(*sqlExpr).expr, Right: DBool(true)}}
  }
| a_expr IS NOT TRUE %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsNot, Left: $1.(*sqlExpr).expr, Right: DBool(true)}}
  }
| a_expr IS FALSE %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: Is, Left: $1.(*sqlExpr).expr, Right: DBool(false)}}
  }
| a_expr IS NOT FALSE %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsNot, Left: $1.(*sqlExpr).expr, Right: DBool(false)}}
  }
| a_expr IS UNKNOWN %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: Is, Left: $1.(*sqlExpr).expr, Right: DNull}}
  }
| a_expr IS NOT UNKNOWN %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsNot, Left: $1.(*sqlExpr).expr, Right: DNull}}
  }
| a_expr IS DISTINCT FROM a_expr %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsDistinctFrom, Left: $1.(*sqlExpr).expr, Right: $5.(*sqlExpr).expr}}
  }
| a_expr IS NOT DISTINCT FROM a_expr %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsNotDistinctFrom, Left: $1.(*sqlExpr).expr, Right: $6.(*sqlExpr).expr}}
  }
| a_expr IS OF '(' type_list ')' %prec IS
  {
    $$ = &sqlExpr{&IsOfTypeExpr{Expr: $1.(*sqlExpr).expr, Types: $5.(*sqlColTypes).colTypes}}
  }
| a_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$ = &sqlExpr{&IsOfTypeExpr{Not: true, Expr: $1.(*sqlExpr).expr, Types: $6.(*sqlColTypes).colTypes}}
  }
| a_expr BETWEEN opt_asymmetric b_expr AND a_expr %prec BETWEEN
  {
    $$ = &sqlExpr{&RangeCond{Left: $1.(*sqlExpr).expr, From: $4.(*sqlExpr).expr, To: $6.(*sqlExpr).expr}}
  }
| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
  {
    $$ = &sqlExpr{&RangeCond{Not: true, Left: $1.(*sqlExpr).expr, From: $5.(*sqlExpr).expr, To: $7.(*sqlExpr).expr}}
  }
| a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
  {
    $$ = &sqlExpr{&RangeCond{Left: $1.(*sqlExpr).expr, From: $4.(*sqlExpr).expr, To: $6.(*sqlExpr).expr}}
  }
| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
  {
    $$ = &sqlExpr{&RangeCond{Not: true, Left: $1.(*sqlExpr).expr, From: $5.(*sqlExpr).expr, To: $7.(*sqlExpr).expr}}
  }
| a_expr IN in_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: In, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| a_expr NOT_LA IN in_expr %prec NOT_LA
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: NotIn, Left: $1.(*sqlExpr).expr, Right: $4.(*sqlExpr).expr}}
  }
// | a_expr subquery_op sub_type select_with_parens %prec CONCAT { unimplemented() }
// | a_expr subquery_op sub_type '(' a_expr ')' %prec CONCAT { unimplemented() }
// | UNIQUE select_with_parens { unimplemented() }

// Restricted expressions
//
// b_expr is a subset of the complete expression syntax defined by a_expr.
//
// Presently, AND, NOT, IS, and IN are the a_expr keywords that would cause
// trouble in the places where b_expr is used. For simplicity, we just
// eliminate all the boolean-keyword-operator productions from b_expr.
b_expr:
  c_expr
| b_expr TYPECAST typename
  {
    $$ = &sqlExpr{&CastExpr{Expr: $1.(*sqlExpr).expr, Type: $3.(*sqlColType).colType}}
  }
| '+' b_expr %prec UMINUS
  {
    $$ = &sqlExpr{&UnaryExpr{Operator: UnaryPlus, Expr: $2.(*sqlExpr).expr}}
  }
| '-' b_expr %prec UMINUS
  {
    $$ = &sqlExpr{&UnaryExpr{Operator: UnaryMinus, Expr: $2.(*sqlExpr).expr}}
  }
| '~' b_expr %prec UMINUS
  {
    $$ = &sqlExpr{&UnaryExpr{Operator: UnaryComplement, Expr: $2.(*sqlExpr).expr}}
  }
| b_expr '+' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Plus, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '-' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Minus, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '*' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Mult, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '/' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Div, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '%' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Mod, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '^' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitxor, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '#' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitxor, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '&' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitand, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '|' b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Bitor, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '<' b_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: LT, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '>' b_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: GT, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr '=' b_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: EQ, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr CONCAT b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: Concat, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr LSHIFT b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: LShift, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr RSHIFT b_expr
  {
    $$ = &sqlExpr{&BinaryExpr{Operator: RShift, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr LESS_EQUALS b_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: LE, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr GREATER_EQUALS b_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: GE, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr NOT_EQUALS b_expr
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: NE, Left: $1.(*sqlExpr).expr, Right: $3.(*sqlExpr).expr}}
  }
| b_expr IS DISTINCT FROM b_expr %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsDistinctFrom, Left: $1.(*sqlExpr).expr, Right: $5.(*sqlExpr).expr}}
  }
| b_expr IS NOT DISTINCT FROM b_expr %prec IS
  {
    $$ = &sqlExpr{&ComparisonExpr{Operator: IsNotDistinctFrom, Left: $1.(*sqlExpr).expr, Right: $6.(*sqlExpr).expr}}
  }
| b_expr IS OF '(' type_list ')' %prec IS
  {
    $$ = &sqlExpr{&IsOfTypeExpr{Expr: $1.(*sqlExpr).expr, Types: $5.(*sqlColTypes).colTypes}}
  }
| b_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$ = &sqlExpr{&IsOfTypeExpr{Not: true, Expr: $1.(*sqlExpr).expr, Types: $6.(*sqlColTypes).colTypes}}
  }

// Productions that can be used in both a_expr and b_expr.
//
// Note: productions that refer recursively to a_expr or b_expr mostly cannot
// appear here. However, it's OK to refer to a_exprs that occur inside
// parentheses, such as function arguments; that cannot introduce ambiguity to
// the b_expr syntax.
c_expr:
  qualified_name
  {
    $$ = &sqlExpr{$1.(*sqlQname).qname}
  }
| a_expr_const
| PARAM
  {
    $$ = &sqlExpr{ValArg{name: $1}}
  }
| '(' a_expr ')'
  {
    $$ = &sqlExpr{&ParenExpr{Expr: $2.(*sqlExpr).expr}}
  }
| case_expr
| func_expr
| select_with_parens %prec UMINUS
  {
    $$ = &sqlExpr{&Subquery{Select: $1.(*sqlSelectStmt).selectStmt}}
  }
| select_with_parens indirection
  {
    $$ = &sqlExpr{&Subquery{Select: $1.(*sqlSelectStmt).selectStmt}}
  }
| EXISTS select_with_parens
  {
    $$ = &sqlExpr{&ExistsExpr{Subquery: &Subquery{Select: $2.(*sqlSelectStmt).selectStmt}}}
  }
// TODO(pmattis): Support this notation?
// | ARRAY select_with_parens { unimplemented() }
| ARRAY array_expr
  {
    $$ = $2
  }
| explicit_row
  {
    $$ = $1
  }
| implicit_row
  {
    $$ = $1
  }
// TODO(pmattis): Support this notation?
// | GROUPING '(' expr_list ')' { unimplemented() }

func_application:
  func_name '(' ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: $1.(*sqlQname).qname}}
  }
| func_name '(' expr_list opt_sort_clause ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: $1.(*sqlQname).qname, Exprs: $3.(*sqlExprs).exprs}}
  }
| func_name '(' VARIADIC a_expr opt_sort_clause ')' { unimplemented() }
| func_name '(' expr_list ',' VARIADIC a_expr opt_sort_clause ')' { unimplemented() }
| func_name '(' ALL expr_list opt_sort_clause ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: $1.(*sqlQname).qname, Type: All, Exprs: $4.(*sqlExprs).exprs}}
  }
| func_name '(' DISTINCT expr_list opt_sort_clause ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: $1.(*sqlQname).qname, Type: Distinct, Exprs: $4.(*sqlExprs).exprs}}
  }
| func_name '(' '*' ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: $1.(*sqlQname).qname, Exprs: Exprs{StarExpr()}}}
  }

// func_expr and its cousin func_expr_windowless are split out from c_expr just
// so that we have classifications for "everything that is a function call or
// looks like one". This isn't very important, but it saves us having to
// document which variants are legal in places like "FROM function()" or the
// backwards-compatible functional-index syntax for CREATE INDEX. (Note that
// many of the special SQL functions wouldn't actually make any sense as
// functional index entries, but we ignore that consideration here.)
func_expr:
  func_application within_group_clause filter_clause over_clause
  {
    $$ = $1
  }
| func_expr_common_subexpr
  {
    $$ = $1
  }

// As func_expr but does not accept WINDOW functions directly (but they can
// still be contained in arguments for functions etc). Use this when window
// expressions are not allowed, where needed to disambiguate the grammar
// (e.g. in CREATE INDEX).
func_expr_windowless:
  func_application { unimplemented() }
| func_expr_common_subexpr { unimplemented() }

// Special expressions that are considered to be functions.
func_expr_common_subexpr:
  COLLATION FOR '(' a_expr ')' { unimplemented() }
| CURRENT_DATE
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}}}
  }
| CURRENT_DATE '(' ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}}}
  }
| CURRENT_TIMESTAMP
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}}}
  }
| CURRENT_TIMESTAMP '(' ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}}}
  }
| CURRENT_ROLE { unimplemented() }
| CURRENT_USER { unimplemented() }
| SESSION_USER { unimplemented() }
| USER { unimplemented() }
| CAST '(' a_expr AS typename ')'
  {
    $$ = &sqlExpr{&CastExpr{Expr: $3.(*sqlExpr).expr, Type: $5.(*sqlColType).colType}}
  }
| EXTRACT '(' extract_list ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}, Exprs: $3.(*sqlExprs).exprs}}
  }
| OVERLAY '(' overlay_list ')'
  {
    $$ = &sqlExpr{&OverlayExpr{FuncExpr{Name: &QualifiedName{Base: Name($1)}, Exprs: $3.(*sqlExprs).exprs}}}
  }
| POSITION '(' position_list ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: "STRPOS"}, Exprs: $3.(*sqlExprs).exprs}}
  }
| SUBSTRING '(' substr_list ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}, Exprs: $3.(*sqlExprs).exprs}}
  }
| TREAT '(' a_expr AS typename ')' { unimplemented() }
| TRIM '(' BOTH trim_list ')'
  {
     $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: "BTRIM"}, Exprs: $4.(*sqlExprs).exprs}}
  }
| TRIM '(' LEADING trim_list ')'
  {
     $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: "LTRIM"}, Exprs: $4.(*sqlExprs).exprs}}
  }
| TRIM '(' TRAILING trim_list ')'
  {
     $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: "RTRIM"}, Exprs: $4.(*sqlExprs).exprs}}
  }
| TRIM '(' trim_list ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: "BTRIM"}, Exprs: $3.(*sqlExprs).exprs}}
  }
| IF '(' a_expr ',' a_expr ',' a_expr ')'
  {
    $$ = &sqlExpr{&IfExpr{Cond: $3.(*sqlExpr).expr, True: $5.(*sqlExpr).expr, Else: $7.(*sqlExpr).expr}}
  }
| NULLIF '(' a_expr ',' a_expr ')'
  {
    $$ = &sqlExpr{&NullIfExpr{Expr1: $3.(*sqlExpr).expr, Expr2: $5.(*sqlExpr).expr}}
  }
| IFNULL '(' a_expr ',' a_expr ')'
  {
    $$ = &sqlExpr{&CoalesceExpr{Name: "IFNULL", Exprs: Exprs{$3.(*sqlExpr).expr, $5.(*sqlExpr).expr}}}
  }
| COALESCE '(' expr_list ')'
  {
    $$ = &sqlExpr{&CoalesceExpr{Name: "COALESCE", Exprs: $3.(*sqlExprs).exprs}}
  }
| GREATEST '(' expr_list ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}, Exprs: $3.(*sqlExprs).exprs}}
  }
| LEAST '(' expr_list ')'
  {
    $$ = &sqlExpr{&FuncExpr{Name: &QualifiedName{Base: Name($1)}, Exprs: $3.(*sqlExprs).exprs}}
  }

// Aggregate decoration clauses
within_group_clause:
WITHIN GROUP '(' sort_clause ')' { unimplemented() }
| /* EMPTY */ {}

filter_clause:
  FILTER '(' WHERE a_expr ')' { unimplemented() }
| /* EMPTY */ {}

// Window Definitions
window_clause:
  WINDOW window_definition_list { unimplemented() }
| /* EMPTY */ {}

window_definition_list:
  window_definition { unimplemented() }
| window_definition_list ',' window_definition { unimplemented() }

window_definition:
  name AS window_specification { unimplemented() }

over_clause:
  OVER window_specification { unimplemented() }
| OVER name { unimplemented() }
| /* EMPTY */ {}

window_specification:
  '(' opt_existing_window_name opt_partition_clause
    opt_sort_clause opt_frame_clause ')' { unimplemented() }

// If we see PARTITION, RANGE, or ROWS as the first token after the '(' of a
// window_specification, we want the assumption to be that there is no
// existing_window_name; but those keywords are unreserved and so could be
// names. We fix this by making them have the same precedence as IDENT and
// giving the empty production here a slightly higher precedence, so that the
// shift/reduce conflict is resolved in favor of reducing the rule. These
// keywords are thus precluded from being an existing_window_name but are not
// reserved for any other purpose.
opt_existing_window_name:
  name { unimplemented() }
| /* EMPTY */ %prec CONCAT {}

opt_partition_clause:
  PARTITION BY expr_list { unimplemented() }
| /* EMPTY */ {}

// For frame clauses, we return a WindowDef, but only some fields are used:
// frameOptions, startOffset, and endOffset.
//
// This is only a subset of the full SQL:2008 frame_clause grammar. We don't
// support <window frame exclusion> yet.
opt_frame_clause:
  RANGE frame_extent { unimplemented() }
| ROWS frame_extent { unimplemented() }
| /* EMPTY */ {}

frame_extent:
  frame_bound { unimplemented() }
| BETWEEN frame_bound AND frame_bound { unimplemented() }

// This is used for both frame start and frame end, with output set up on the
// assumption it's frame start; the frame_extent productions must reject
// invalid cases.
frame_bound:
  UNBOUNDED PRECEDING { unimplemented() }
| UNBOUNDED FOLLOWING { unimplemented() }
| CURRENT ROW { unimplemented() }
| a_expr PRECEDING { unimplemented() }
| a_expr FOLLOWING { unimplemented() }

// Supporting nonterminals for expressions.

// Explicit row production.
//
// SQL99 allows an optional ROW keyword, so we can now do single-element rows
// without conflicting with the parenthesized a_expr production. Without the
// ROW keyword, there must be more than one a_expr inside the parens.
row:
  ROW '(' expr_list ')'
  {
    $$ = &sqlExpr{Row($3.(*sqlExprs).exprs)}
  }
| ROW '(' ')'
  {
    $$ = &sqlExpr{Row(nil)}
  }
| '(' expr_list ',' a_expr ')'
  {
    $$ = &sqlExpr{Tuple(append($2.(*sqlExprs).exprs, $4.(*sqlExpr).expr))}
  }

explicit_row:
  ROW '(' expr_list ')'
  {
    $$ = &sqlExpr{Row($3.(*sqlExprs).exprs)}
  }
| ROW '(' ')'
  {
    $$ = &sqlExpr{Row(nil)}
  }

implicit_row:
  '(' expr_list ',' a_expr ')'
  {
    $$ = &sqlExpr{Tuple(append($2.(*sqlExprs).exprs, $4.(*sqlExpr).expr))}
  }

// sub_type:
//   ANY { unimplemented() }
// | SOME { unimplemented() }
// | ALL { unimplemented() }

// math_op:
//   '+' { unimplemented() }
// | '-' { unimplemented() }
// | '*' { unimplemented() }
// | '/' { unimplemented() }
// | '%' { unimplemented() }
// | '&' { unimplemented() }
// | '|' { unimplemented() }
// | '^' { unimplemented() }
// | '#' { unimplemented() }
// | '<' { unimplemented() }
// | '>' { unimplemented() }
// | '=' { unimplemented() }
// | CONCAT { unimplemented() }
// | LESS_EQUALS { unimplemented() }
// | GREATER_EQUALS { unimplemented() }
// | NOT_EQUALS { unimplemented() }

// subquery_op:
//   math_op { unimplemented() }
// | LIKE { unimplemented() }
// | NOT_LA LIKE { unimplemented() }
  // cannot put SIMILAR TO here, because SIMILAR TO is a hack.
  // the regular expression is preprocessed by a function (similar_escape),
  // and the ~ operator for posix regular expressions is used.
  //        x SIMILAR TO y     ->    x ~ similar_escape(y)
  // this transformation is made on the fly by the parser upwards.
  // however the SubLink structure which handles any/some/all stuff
  // is not ready for such a thing.

expr_list:
  a_expr
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr}}
  }
| expr_list ',' a_expr
  {
    $1.(*sqlExprs).exprs = append($1.(*sqlExprs).exprs, $3.(*sqlExpr).expr)
    $$ = $1
  }

type_list:
  typename
  {
    $$ = &sqlColTypes{[]ColumnType{$1.(*sqlColType).colType}}
  }
| type_list ',' typename
  {
    $1.(*sqlColTypes).colTypes = append($1.(*sqlColTypes).colTypes, $3.(*sqlColType).colType)
    $$ = $1
  }

array_expr:
  '[' expr_list ']'
  {
    $$ = &sqlExpr{Array($2.(*sqlExprs).exprs)}
  }
| '[' array_expr_list ']'
  {
    $$ = &sqlExpr{Array($2.(*sqlExprs).exprs)}
  }
| '[' ']'
  {
    $$ = &sqlExpr{Array(nil)}
  }

array_expr_list:
  array_expr
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr}}
  }
| array_expr_list ',' array_expr
  {
    $1.(*sqlExprs).exprs = append($1.(*sqlExprs).exprs, $3.(*sqlExpr).expr)
    $$ = $1
  }

extract_list:
  extract_arg FROM a_expr
  {
    $$ = &sqlExprs{Exprs{DString($1), $3.(*sqlExpr).expr}}
  }

// TODO(vivek): Narrow down to just IDENT once the other
// terms are not keywords.
extract_arg:
  IDENT
| YEAR
| MONTH
| DAY
| HOUR
| MINUTE
| SECOND

// OVERLAY() arguments
// SQL99 defines the OVERLAY() function:
//   - overlay(text placing text from int for int)
//   - overlay(text placing text from int)
// and similarly for binary strings
overlay_list:
  a_expr overlay_placing substr_from substr_for
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr, $2.(*sqlExpr).expr, $3.(*sqlExpr).expr, $4.(*sqlExpr).expr}}
  }
| a_expr overlay_placing substr_from
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr, $2.(*sqlExpr).expr, $3.(*sqlExpr).expr}}
  }

overlay_placing:
  PLACING a_expr
  {
    $$ = $2
  }

// position_list uses b_expr not a_expr to avoid conflict with general IN
position_list:
  b_expr IN b_expr
  {
    $$ = &sqlExprs{Exprs{$3.(*sqlExpr).expr, $1.(*sqlExpr).expr}}
  }
| /* EMPTY */
  {
    $$ = &sqlExprs{nil}
  }

// SUBSTRING() arguments
// SQL9x defines a specific syntax for arguments to SUBSTRING():
//   - substring(text from int for int)
//   - substring(text from int) get entire string from starting point "int"
//   - substring(text for int) get first "int" characters of string
//   - substring(text from pattern) get entire string matching pattern
//   - substring(text from pattern for escape) same with specified escape char
// We also want to support generic substring functions which accept
// the usual generic list of arguments. So we will accept both styles
// here, and convert the SQL9x style to the generic list for further
// processing. - thomas 2000-11-28
substr_list:
  a_expr substr_from substr_for
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr, $2.(*sqlExpr).expr, $3.(*sqlExpr).expr}}
  }
| a_expr substr_for substr_from
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr, $3.(*sqlExpr).expr, $2.(*sqlExpr).expr}}
  }
| a_expr substr_from
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr, $2.(*sqlExpr).expr}}
  }
| a_expr substr_for
  {
    $$ = &sqlExprs{Exprs{$1.(*sqlExpr).expr, DInt(1), $2.(*sqlExpr).expr}}
  }
| expr_list
  {
    $$ = $1
  }
| /* EMPTY */
  {
    $$ = &sqlExprs{nil}
  }

substr_from:
  FROM a_expr
  {
    $$ = $2
  }

substr_for:
  FOR a_expr
  {
    $$ = $2
  }

trim_list:
  a_expr FROM expr_list
  {
    $3.(*sqlExprs).exprs = append($3.(*sqlExprs).exprs, $1.(*sqlExpr).expr)
    $$ = $3
  }
| FROM expr_list
  {
    $$ = $2
  }
| expr_list
  {
    $$ = $1
  }

in_expr:
  select_with_parens
  {
    $$ = &sqlExpr{&Subquery{Select: $1.(*sqlSelectStmt).selectStmt}}
  }
| '(' expr_list ')'
  {
    $$ = &sqlExpr{Tuple($2.(*sqlExprs).exprs)}
  }

// Define SQL-style CASE clause.
// - Full specification
//      CASE WHEN a = b THEN c ... ELSE d END
// - Implicit argument
//      CASE a WHEN b THEN c ... ELSE d END
case_expr:
  CASE case_arg when_clause_list case_default END
  {
    $$ = &sqlExpr{&CaseExpr{Expr: $2.(*sqlExpr).expr, Whens: $3.(*sqlWhens).whens, Else: $4.(*sqlExpr).expr}}
  }

when_clause_list:
  // There must be at least one
  when_clause
  {
    $$ = &sqlWhens{[]*When{$1.(*sqlWhen).when}}
  }
| when_clause_list when_clause
  {
    $1.(*sqlWhens).whens = append($1.(*sqlWhens).whens, $2.(*sqlWhen).when)
    $$ = $1
  }

when_clause:
  WHEN a_expr THEN a_expr
  {
    $$ = &sqlWhen{&When{Cond: $2.(*sqlExpr).expr, Val: $4.(*sqlExpr).expr}}
  }

case_default:
  ELSE a_expr
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = &sqlExpr{nil}
  }

case_arg:
  a_expr
| /* EMPTY */
  {
    $$ = &sqlExpr{nil}
  }

indirection_elem:
  name_indirection
  {
    $$ = $1
  }
| glob_indirection
  {
    $$ = $1
  }
| '@' col_label
  {
    $$ = &sqlIndirectElem{IndexIndirection($2)}
  }
| '[' a_expr ']'
  {
    $$ = &sqlIndirectElem{&ArrayIndirection{Begin: $2.(*sqlExpr).expr}}
  }
| '[' a_expr ':' a_expr ']'
  {
    $$ = &sqlIndirectElem{&ArrayIndirection{Begin: $2.(*sqlExpr).expr, End: $4.(*sqlExpr).expr}}
  }

name_indirection:
  '.' col_label
  {
    $$ = &sqlIndirectElem{NameIndirection($2)}
  }

glob_indirection:
  '.' '*'
  {
    $$ = &sqlIndirectElem{qualifiedStar}
  }

indirection:
  indirection_elem
  {
    $$ = &sqlIndirect{Indirection{$1.(*sqlIndirectElem).indirectElem}}
  }
| indirection indirection_elem
  {
    $1.(*sqlIndirect).indirect = append($1.(*sqlIndirect).indirect, $2.(*sqlIndirectElem).indirectElem)
    $$ = $1
  }

opt_asymmetric:
  ASYMMETRIC {}
| /* EMPTY */ {}

// The SQL spec defines "contextually typed value expressions" and
// "contextually typed row value constructors", which for our purposes are the
// same as "a_expr" and "row" except that DEFAULT can appear at the top level.

ctext_expr:
  a_expr
| DEFAULT
  {
    $$ = &sqlExpr{DefaultVal{}}
  }

ctext_expr_list:
  ctext_expr
  {
    $$ = &sqlExprs{[]Expr{$1.(*sqlExpr).expr}}
  }
| ctext_expr_list ',' ctext_expr
  {
    $1.(*sqlExprs).exprs = append($1.(*sqlExprs).exprs, $3.(*sqlExpr).expr)
    $$ = $1
  }

// We should allow ROW '(' ctext_expr_list ')' too, but that seems to require
// making VALUES a fully reserved word, which will probably break more apps
// than allowing the noise-word is worth.
ctext_row:
  '(' ctext_expr_list ')'
  {
    $$ = $2
  }

// target list for SELECT
opt_target_list:
  target_list
| /* EMPTY */
  {
    $$ = &sqlSelExprs{nil}
  }

target_list:
  target_elem
  {
    $$ = &sqlSelExprs{SelectExprs{$1.(*sqlSelExpr).selExpr}}
  }
| target_list ',' target_elem
  {
    $1.(*sqlSelExprs).selExprs = append($1.(*sqlSelExprs).selExprs, $3.(*sqlSelExpr).selExpr)
    $$ = $1
  }

target_elem:
  a_expr AS col_label
  {
    $$ = &sqlSelExpr{SelectExpr{Expr: $1.(*sqlExpr).expr, As: Name($3)}}
  }
  // We support omitting AS only for column labels that aren't any known
  // keyword. There is an ambiguity against postfix operators: is "a ! b" an
  // infix expression, or a postfix expression and a column label?  We prefer
  // to resolve this as an infix expression, which we accomplish by assigning
  // IDENT a precedence higher than POSTFIXOP.
| a_expr IDENT
  {
    $$ = &sqlSelExpr{SelectExpr{Expr: $1.(*sqlExpr).expr, As: Name($2)}}
  }
| a_expr
  {
    $$ = &sqlSelExpr{SelectExpr{Expr: $1.(*sqlExpr).expr}}
  }
| '*'
  {
    $$ = &sqlSelExpr{starSelectExpr()}
  }

// Names and constants.

qualified_name_list:
  qualified_name
  {
    $$ = &sqlQnames{QualifiedNames{$1.(*sqlQname).qname}}
  }
| qualified_name_list ',' qualified_name
  {
    $1.(*sqlQnames).qnames = append($1.(*sqlQnames).qnames, $3.(*sqlQname).qname)
    $$ = $1
  }

indirect_name_or_glob_list:
  indirect_name_or_glob
  {
    $$ = &sqlQnames{QualifiedNames{$1.(*sqlQname).qname}}
  }
| indirect_name_or_glob_list ',' indirect_name_or_glob
  {
    $1.(*sqlQnames).qnames = append($1.(*sqlQnames).qnames, $3.(*sqlQname).qname)
    $$ = $1
  }

// The production for a qualified relation name has to exactly match the
// production for a qualified func_name, because in a FROM clause we cannot
// tell which we are parsing until we see what comes after it ('(' for a
// func_name, something else for a relation). Therefore we allow 'indirection'
// which may contain subscripts, and reject that case in the C code.
qualified_name:
  name
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1)}}
  }
| name indirection
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1), Indirect: $2.(*sqlIndirect).indirect}}
  }

// indirect_name_or_glob is a subset of `qualified_name` accepting only:
// <database> / <table>
// <database>.<table>
// <database>.*
// *
indirect_name_or_glob:
  name
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1)}}
  }
| name name_indirection
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1), Indirect: Indirection{$2.(*sqlIndirectElem).indirectElem}}}
  }
| name glob_indirection
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1), Indirect: Indirection{$2.(*sqlIndirectElem).indirectElem}}}
  }
| '*'
  {
    $$ = &sqlQname{&QualifiedName{Indirect: Indirection{unqualifiedStar}}}
  }

name_list:
  name
  {
    $$ = &sqlStrs{[]string{$1}}
  }
| name_list ',' name
  {
    $1.(*sqlStrs).strs = append($1.(*sqlStrs).strs, $3)
    $$ = $1
  }

opt_name_list:
  '(' name_list ')'
  {
    $$ = $2
  }
| /* EMPTY */ {}

// The production for a qualified func_name has to exactly match the production
// for a qualified name, because we cannot tell which we are parsing until
// we see what comes after it ('(' or SCONST for a func_name, anything else for
// a name). Therefore we allow 'indirection' which may contain
// subscripts, and reject that case in the C code. (If we ever implement
// SQL99-like methods, such syntax may actually become legal!)
func_name:
  type_function_name
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1)}}
  }
| name indirection
  {
    $$ = &sqlQname{&QualifiedName{Base: Name($1), Indirect: $2.(*sqlIndirect).indirect}}
  }

// Constants
a_expr_const:
  ICONST
  {
    $$ = &sqlExpr{&IntVal{Val: $1.Val, Str: $1.Str}}
  }
| FCONST
  {
    $$ = &sqlExpr{NumVal($1)}
  }
| SCONST
  {
    $$ = &sqlExpr{DString($1)}
  }
| BCONST
  {
    $$ = &sqlExpr{DBytes($1)}
  }
| func_name '(' expr_list opt_sort_clause ')' SCONST { unimplemented() }
| const_typename SCONST
  {
    $$ = &sqlExpr{&CastExpr{Expr: DString($2), Type: $1.(*sqlColType).colType}}
  }
| const_interval SCONST opt_interval
  {
    $$ = &sqlExpr{&CastExpr{Expr: DString($2), Type: $1.(*sqlColType).colType}}
  }
| const_interval '(' ICONST ')' SCONST
  {
    $$ = &sqlExpr{&CastExpr{Expr: DString($5), Type: $1.(*sqlColType).colType}}
  }
| TRUE
  {
    $$ = &sqlExpr{DBool(true)}
  }
| FALSE
  {
    $$ = &sqlExpr{DBool(false)}
  }
| NULL
  {
    $$ = &sqlExpr{DNull}
  }

signed_iconst:
  ICONST
| '+' ICONST
  {
    $$ = $2
  }
| '-' ICONST
  {
    $$ = IntVal{Val: -$2.Val, Str: "-" + $2.Str}
  }

// Name classification hierarchy.
//
// IDENT is the lexeme returned by the lexer for identifiers that match no
// known keyword. In most cases, we can accept certain keywords as names, not
// only IDENTs. We prefer to accept as many such keywords as possible to
// minimize the impact of "reserved words" on programmers. So, we divide names
// into several possible classes. The classification is chosen in part to make
// keywords acceptable as names wherever possible.

// General name --- names that can be column, table, etc names.
name:
  IDENT
| unreserved_keyword
| col_name_keyword

opt_name:
  name
| /* EMPTY */
  {
    $$ = ""
  }

// Type/function identifier --- names that can be type or function names.
type_function_name:
  IDENT
| unreserved_keyword
| type_func_name_keyword

// Any not-fully-reserved word --- these names can be, eg, variable names.
non_reserved_word:
  IDENT
| unreserved_keyword
| col_name_keyword
| type_func_name_keyword

// Column label --- allowed labels in "AS" clauses. This presently includes
// *all* Postgres keywords.
col_label:
  IDENT
| unreserved_keyword
| col_name_keyword
| type_func_name_keyword
| reserved_keyword

// Keyword category lists. Generally, every keyword present in the Postgres
// grammar should appear in exactly one of these lists.
//
// Put a new keyword into the first list that it can go into without causing
// shift or reduce conflicts. The earlier lists define "less reserved"
// categories of keywords.
//
// Make sure that each keyword's category in kwlist.h matches where it is
// listed here. (Someday we may be able to generate these lists and kwlist.h's
// table from a common master list.)
//
// "Unreserved" keywords --- available for use as any kind of name.
unreserved_keyword:
  ACTION
| ADD
| ALTER
| AT
| BEGIN
| BLOB
| BY
| CASCADE
| COLUMNS
| COMMIT
| COMMITTED
| CONFLICT
| COVERING
| CUBE
| CURRENT
| CYCLE
| DATA
| DATABASE
| DATABASES
| DAY
| DELETE
| DOUBLE
| DROP
| EXPLAIN
| FILTER
| FIRST
| FOLLOWING
| GRANTS
| HIGH
| HOUR
| INSERT
| ISOLATION
| KEY
| LEVEL
| LOCAL
| LOW
| MATCH
| MINUTE
| MONTH
| NAME
| NAMES
| NEXT
| NO
| NORMAL
| NOTHING
| NULLS
| OF
| OFF
| ORDINALITY
| OVER
| PARTIAL
| PARTITION
| PRECEDING
| PRIORITY
| RANGE
| READ
| RECURSIVE
| REF
| RENAME
| REPEATABLE
| RESTRICT
| REVOKE
| ROLLBACK
| ROLLUP
| ROWS
| SEARCH
| SECOND
| SERIALIZABLE
| SESSION
| SET
| SHOW
| SIMPLE
| SNAPSHOT
| SQL
| START
| STORING
| STRICT
| TABLES
| TEXT
| TRANSACTION
| TRUNCATE
| TYPE
| UNBOUNDED
| UNCOMMITTED
| UNKNOWN
| UPDATE
| VALID
| VALIDATE
| VALUE
| VARYING
| WITHIN
| WITHOUT
| YEAR
| ZONE

// Column identifier --- keywords that can be column, table, etc names.
//
// Many of these keywords will in fact be recognized as type or function names
// too; but they have special productions for the purpose, and so can't be
// treated as "generic" type or function names.
//
// The type names appearing here are not usable as function names because they
// can be followed by '(' in typename productions, which looks too much like a
// function call for an LR(1) parser.
col_name_keyword:
  BETWEEN
| BIGINT
| BIT
| BOOL
| BOOLEAN
| BYTEA
| BYTES
| CHAR
| CHARACTER
| COALESCE
| DATE
| DEC
| DECIMAL
| EXISTS
| EXTRACT
| FLOAT
| GREATEST
| GROUPING
| IF
| IFNULL
| INT
| INT64
| INTEGER
| INTERVAL
| LEAST
| NULLIF
| NUMERIC
| OUT
| OVERLAY
| POSITION
| PRECISION
| REAL
| ROW
| SMALLINT
| STRING
| SUBSTRING
| TIME
| TIMESTAMP
| TREAT
| TRIM
| VALUES
| VARCHAR

// Type/function identifier --- keywords that can be type or function names.
//
// Most of these are keywords that are used as operators in expressions; in
// general such keywords can't be column names because they would be ambiguous
// with variables, but they are unambiguous as function identifiers.
//
// Do not include POSITION, SUBSTRING, etc here since they have explicit
// productions in a_expr to support the goofy SQL9x argument syntax.
// - thomas 2000-11-28
type_func_name_keyword:
  COLLATION
| CROSS
| FULL
| INNER
| IS
| JOIN
| LEFT
| LIKE
| NATURAL
| OUTER
| OVERLAPS
| RIGHT
| SIMILAR

// Reserved keyword --- these keywords are usable only as a col_label.
//
// Keywords appear here if they could not be distinguished from variable, type,
// or function names in some contexts. Don't put things here unless forced to.
reserved_keyword:
  ALL
| ANALYSE
| ANALYZE
| AND
| ANY
| ARRAY
| AS
| ASC
| ASYMMETRIC
| BOTH
| CASE
| CAST
| CHECK
| COLLATE
| COLUMN
| CONSTRAINT
| CREATE
| CURRENT_CATALOG
| CURRENT_DATE
| CURRENT_ROLE
| CURRENT_TIME
| CURRENT_TIMESTAMP
| CURRENT_USER
| DEFAULT
| DEFERRABLE
| DESC
| DISTINCT
| DO
| ELSE
| END
| EXCEPT
| FALSE
| FETCH
| FOR
| FOREIGN
| FROM
| GRANT
| GROUP
| HAVING
| IN
| INDEX
| INITIALLY
| INTERSECT
| INTO
| LATERAL
| LEADING
| LIMIT
| LOCALTIME
| LOCALTIMESTAMP
| NOT
| NULL
| OFFSET
| ON
| ONLY
| OR
| ORDER
| PLACING
| PRIMARY
| REFERENCES
| RETURNING
| SELECT
| SESSION_USER
| SOME
| SYMMETRIC
| TABLE
| THEN
| TO
| TRAILING
| TRUE
| UNION
| UNIQUE
| USER
| USING
| VARIADIC
| WHEN
| WHERE
| WINDOW
| WITH

%%
