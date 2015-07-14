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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
// Portions Copyright (c) 1994, Regents of the University of California

%{
package parser2

import "strings"
%}

%union {
  id             int
  pos            int
  empty          struct{}
  ival           int
  str            string
  strs           []string
  qname          QualifiedName
  qnames         []QualifiedName
  stmt           Statement
  stmts          []Statement
  tableDef       TableDef
  tableDefs      []TableDef
  colConstraint  ColumnConstraint
  colConstraints []ColumnConstraint
  colType        ColumnType
  expr           Expr
  exprs          Exprs
  selExpr        SelectExpr
  selExprs       SelectExprs
  tblExpr        TableExpr
  tblExprs       TableExprs
  joinCond       JoinCond
  when           *When
  whens          []*When
  updateExpr     *UpdateExpr
  updateExprs    []*UpdateExpr
  limit          *Limit
}

%type <stmts> stmt_block
%type <stmts> stmt_list
%type <stmt> stmt

%type <stmt> alter_stmt
%type <stmt> alter_database_stmt
%type <stmt> alter_index_stmt
%type <stmt> alter_table_stmt
// %type <stmt> alter_view_stmt
%type <stmt> create_stmt
%type <stmt> create_database_stmt
%type <stmt> create_index_stmt
// %type <stmt> create_schema_stmt
%type <stmt> create_table_stmt
%type <stmt> create_table_as_stmt
// %type <stmt> create_view_stmt
%type <stmt> delete_stmt
%type <stmt> drop_stmt
%type <stmt> explain_stmt
%type <stmt> explainable_stmt
%type <stmt> insert_stmt
%type <stmt> preparable_stmt
%type <stmt> rename_stmt
%type <stmt> select_stmt
%type <stmt> set_stmt
%type <stmt> show_stmt
%type <stmt> transaction_stmt
%type <stmt> truncate_stmt
%type <stmt> update_stmt

%type <stmt> select_no_parens select_with_parens select_clause
%type <stmt> simple_select values_clause

%type <empty> alter_column_default alter_using
%type <empty> opt_asc_desc opt_nulls_order

%type <empty> alter_table_cmd opt_collate_clause
%type <empty> alter_table_cmds

%type <empty> opt_drop_behavior

%type <empty> createdb_opt_list createdb_opt_items transaction_mode_list
%type <empty> createdb_opt_item transaction_mode_item

%type <empty> opt_with_data
%type <empty> opt_nowait_or_skip

// %type <empty> opt_schema_name
// %type <empty> opt_schema_elem_list
// %type <empty> opt_schema_elem

%type <str>   database_name attr_name
%type <empty> access_method_clause access_method
%type <str>   name
%type <empty> index_name opt_index_name

%type <empty> func_name subquery_op
%type <empty> opt_class
%type <empty> opt_collate

%type <qname> qualified_name
%type <qname> insert_target

%type <empty> math_op

%type <empty> iso_level opt_encoding

%type <tableDefs> opt_table_elem_list table_elem_list 
%type <empty> opt_inherit
%type <empty> opt_typed_table_elem_list typed_table_elem_list
%type <empty> reloptions opt_reloptions
%type <empty> opt_with distinct_clause opt_all_clause
%type <strs> opt_column_list column_list
%type <empty> sort_clause opt_sort_clause sortby_list index_params
%type <strs> name_list opt_name_list
%type <empty> opt_array_bounds
%type <tblExprs> from_clause from_list
%type <empty> qualified_name_list
%type <qname> any_name
%type <qnames> any_name_list
%type <empty> any_operator
%type <exprs> expr_list
%type <qname> attrs
%type <selExprs> target_list opt_target_list
%type <selExprs> insert_column_list
%type <empty> set_target_list
%type <updateExprs> set_clause_list
%type <updateExpr> set_clause multiple_set_clause
%type <strs>  indirection opt_indirection
%type <exprs> ctext_expr_list ctext_row
%type <empty> reloption_list group_clause
%type <limit> select_limit opt_select_limit
%type <empty> transaction_mode_list_or_empty
%type <empty> table_func_elem_list
%type <empty> returning_clause
%type <qnames> relation_expr_list

%type <empty> group_by_list
%type <empty> group_by_item empty_grouping_set

%type <empty> create_as_target

%type <empty> func_type

%type <empty> opt_restart_seqs
%type <empty> opt_temp
%type <empty> on_commit_option

%type <empty> for_locking_strength
%type <empty> for_locking_item
%type <empty> for_locking_clause opt_for_locking_clause for_locking_items
%type <empty> locked_rels_list
%type <empty> all_or_distinct

%type <empty> join_outer
%type <joinCond> join_qual
%type <str> join_type

%type <empty> extract_list overlay_list position_list
%type <empty> substr_list trim_list
%type <empty> opt_interval interval_second
%type <empty> overlay_placing substr_from substr_for

%type <empty> opt_unique opt_concurrently

%type <empty> opt_column opt_set_data
%type <empty> drop_type

%type <limit> limit_clause offset_clause 
%type <expr>  select_limit_value select_offset_value select_offset_value2 
%type <empty> opt_select_fetch_first_value
%type <empty> row_or_rows first_or_next

%type <stmt>  insert_rest
%type <empty> opt_conf_expr
%type <empty> opt_on_conflict

%type <stmt>  generic_set set_rest set_rest_more
%type <empty> set_reset_clause

%type <tableDef> table_elem column_def constraint_elem
%type <empty> typed_table_elem table_func_elem
%type <empty> column_options
%type <empty> reloption_elem
%type <empty> def_arg
%type <str>   column_elem
%type <expr>  where_clause
%type <str>   indirection_elem
%type <expr>  a_expr b_expr c_expr a_expr_const
%type <expr>  columnref
%type <expr>  in_expr
%type <expr>  having_clause
%type <empty> func_table array_expr
%type <empty> exclusion_where_clause
%type <empty> rowsfrom_item rowsfrom_list opt_col_def_list
%type <empty> opt_ordinality
%type <empty> exclusion_constraint_list exclusion_constraint_elem
%type <empty> func_arg_list
%type <empty> func_arg_expr
%type <empty> row explicit_row implicit_row type_list array_expr_list
%type <expr>  case_expr case_arg case_default
%type <when>  when_clause
%type <whens> when_clause_list
%type <empty> sub_type
%type <expr> ctext_expr
%type <expr> numeric_only
%type <str> alias_clause opt_alias_clause
%type <empty> func_alias_clause
%type <empty> sortby
%type <empty> index_elem
%type <tblExpr> table_ref
%type <tblExpr> joined_table
%type <qname> relation_expr
%type <tblExpr> relation_expr_opt_alias
%type <selExpr> target_elem insert_column_item
%type <updateExpr> single_set_clause
%type <qname> set_target

%type <empty> explain_option_name
%type <empty> explain_option_arg
%type <empty> explain_option_elem
%type <empty> explain_option_list

%type <colType> typename simple_typename const_typename
%type <colType> numeric opt_numeric_modifiers
%type <ival> opt_float
%type <colType> character const_character
%type <colType> character_with_length character_without_length
%type <colType> const_datetime const_interval
%type <colType> bit const_bit bit_with_length bit_without_length
%type <colType> character_base
%type <empty> extract_arg
%type <empty> opt_charset
%type <empty> opt_varying opt_timezone opt_no_inherit

%type <ival>  iconst signed_iconst
%type <str>   sconst
%type <expr>  opt_boolean_or_string
%type <exprs> var_list
%type <strs>  var_name
%type <str>   col_id
%type <str>   col_label type_function_name param_name
%type <str>   non_reserved_word
%type <expr>  non_reserved_word_or_sconst
%type <empty> createdb_opt_name
%type <expr>  var_value
%type <empty> zone_value
// %type <empty> role_spec

%type <str>   unreserved_keyword type_func_name_keyword
%type <str>   col_name_keyword reserved_keyword

%type <tableDef> table_constraint table_like_clause
%type <empty> table_like_option_list table_like_option
%type <colConstraints> col_qual_list
%type <colConstraint> col_constraint col_constraint_elem
%type <empty> key_actions key_delete key_match key_update key_action
%type <empty> existing_index

// %type <empty> opt_check_option

%type <empty> func_application func_expr_common_subexpr
%type <empty> func_expr func_expr_windowless
%type <empty> common_table_expr
%type <empty> with_clause opt_with_clause
%type <empty> cte_list

%type <empty> within_group_clause
%type <empty> filter_clause
%type <empty> window_clause window_definition_list opt_partition_clause
%type <empty> window_definition over_clause window_specification
%type <empty> opt_frame_clause frame_extent frame_bound
%type <empty> opt_existing_window_name

// Non-keyword token types. These are hard-wired into the "flex" lexer. They
// must be listed first so that their numeric codes do not depend on the set of
// keywords. PL/pgsql depends on this so that it can share the same lexer. If
// you add/change tokens here, fix PL/pgsql to match!
// 
// DOT_DOT is unused in the core SQL grammar, and so will always provoke parse
// errors. It is needed by PL/pgsql.
%token <str>   IDENT FCONST SCONST BCONST XCONST
%token <ival>  ICONST PARAM
%token <str>   TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token <str>   LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%token <str>   ERROR

// If you want to make any keyword changes, update the keyword table in
// src/include/parser/kwlist.h and add new keywords to the appropriate one of
// the reserved-or-not-so-reserved keyword lists, below; search this file for
// "Keyword category lists".

// Ordinary key words in alphabetical order.
%token <str>   ABORT ABSOLUTE ACCESS ACTION ADD ADMIN AFTER
%token <str>   AGGREGATE ALL ALSO ALTER ALWAYS ANALYSE ANALYZE AND ANY ARRAY AS ASC
%token <str>   ASSERTION ASSIGNMENT ASYMMETRIC AT ATTRIBUTE AUTHORIZATION

%token <str>   BACKWARD BEFORE BEGIN BETWEEN BIGINT BINARY BIT
%token <str>   BLOB BOOLEAN BOTH BY

%token <str>   CACHE CALLED CASCADE CASCADED CASE CAST CATALOG CHAIN CHAR
%token <str>   CHARACTER CHARACTERISTICS CHECK CHECKPOINT CLASS CLOSE
%token <str>   CLUSTER COALESCE COLLATE COLLATION COLUMN COLUMNS COMMENT COMMENTS COMMIT
%token <str>   COMMITTED CONCAT CONCURRENTLY CONFIGURATION CONFLICT CONNECTION CONSTRAINT
%token <str>   CONSTRAINTS CONTENT CONTINUE CONVERSION COPY COST CREATE
%token <str>   CROSS CSV CUBE CURRENT CURRENT_CATALOG CURRENT_DATE
%token <str>   CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME CURRENT_TIMESTAMP
%token <str>   CURRENT_USER CURSOR CYCLE

%token <str>   DATA DATABASE DATABASES DATE DAY DEALLOCATE DEC DECIMAL DECLARE DEFAULT DEFAULTS
%token <str>   DEFERRABLE DEFERRED DEFINER DELETE DELIMITER DELIMITERS DESC
%token <str>   DICTIONARY DISABLE DISCARD DISTINCT DO DOCUMENT DOMAIN DOUBLE DROP

%token <str>   EACH ELSE ENABLE ENCODING ENCRYPTED END ENUM ESCAPE EVENT EXCEPT
%token <str>   EXCLUDE EXCLUDING EXCLUSIVE EXECUTE EXISTS EXPLAIN EXTENSION EXTERNAL EXTRACT

%token <str>   FALSE FAMILY FETCH FILTER FIRST FLOAT FOLLOWING FOR
%token <str>   FORCE FOREIGN FORWARD FREEZE FROM FULL FUNCTION FUNCTIONS

%token <str>   GLOBAL GRANT GRANTED GREATEST GROUP GROUPING

%token <str>   HANDLER HAVING HEADER HOLD HOUR

%token <str>   IDENTITY IF IMMEDIATE IMMUTABLE IMPLICIT IMPORT IN
%token <str>   INCLUDING INCREMENT INDEX INDEXES INHERIT INHERITS INITIALLY INLINE
%token <str>   INNER INOUT INPUT INSENSITIVE INSERT INSTEAD INT INTEGER
%token <str>   INTERSECT INTERVAL INTO INVOKER IS ISOLATION

%token <str>   JOIN

%token <str>   KEY

%token <str>   LABEL LANGUAGE LARGE LAST LATERAL
%token <str>   LEADING LEAKPROOF LEAST LEFT LEVEL LIKE LIMIT LISTEN LOAD LOCAL
%token <str>   LOCALTIME LOCALTIMESTAMP LOCATION LOCK LOCKED LOGGED

%token <str>   MAPPING MATCH MATERIALIZED MAXVALUE MINUTE MINVALUE MODE MONTH MOVE

%token <str>   NAME NAMES NATIONAL NATURAL NCHAR NEXT NO NONE
%token <str>   NOT NOTHING NOTIFY NOWAIT NULL NULLIF
%token <str>   NULLS NUMERIC

%token <str>   OBJECT OF OFF OFFSET OIDS ON ONLY OPTION OPTIONS OR
%token <str>   ORDER ORDINALITY OUT OUTER OVER OVERLAPS OVERLAY OWNED OWNER

%token <str>   PARSER PARTIAL PARTITION PASSING PASSWORD PLACING PLANS POLICY POSITION
%token <str>   PRECEDING PRECISION PRESERVE PREPARE PREPARED PRIMARY
%token <str>   PRIOR PRIVILEGES PROCEDURAL PROCEDURE PROGRAM

%token <str>   QUOTE

%token <str>   RANGE READ REAL REASSIGN RECHECK RECURSIVE REF REFERENCES REFRESH REINDEX
%token <str>   RELATIVE RELEASE RENAME REPEATABLE REPLACE REPLICA RESET RESTART
%token <str>   RESTRICT RETURNING RETURNS REVOKE RIGHT ROLE ROLLBACK ROLLUP
%token <str>   ROW ROWS RULE

%token <str>   SAVEPOINT SCHEMA SCROLL SEARCH SECOND SECURITY SELECT SEQUENCE SEQUENCES
%token <str>   SERIALIZABLE SERVER SESSION SESSION_USER SET SETS SETOF SHARE SHOW
%token <str>   SIMILAR SIMPLE SKIP SMALLINT SNAPSHOT SOME SQL STABLE STANDALONE START
%token <str>   STATEMENT STATISTICS STDIN STDOUT STORAGE STRICT STRIP SUBSTRING
%token <str>   SYMMETRIC SYSID SYSTEM

%token <str>   TABLE TABLES TABLESAMPLE TABLESPACE TEMP TEMPLATE TEMPORARY TEXT THEN
%token <str>   TIME TIMESTAMP TO TRAILING TRANSACTION TRANSFORM TREAT TRIGGER TRIM TRUE
%token <str>   TRUNCATE TRUSTED TYPE TYPES

%token <str>   UNBOUNDED UNCOMMITTED UNENCRYPTED UNION UNIQUE UNKNOWN UNLISTEN UNLOGGED
%token <str>   UNTIL UPDATE USER USING

%token <str>   VACUUM VALID VALIDATE VALIDATOR VALUE VALUES VARCHAR VARIADIC VARYING
%token <str>   VERBOSE VERSION VIEW VIEWS VOLATILE

%token <str>   WHEN WHERE WHITESPACE WINDOW WITH WITHIN WITHOUT WORK WRAPPER WRITE

%token <str>   YEAR YES

%token <str>   ZONE

// The grammar thinks these are keywords, but they are not in the kwlist.h list
// and so can never be entered directly. The filter in parser.c creates these
// tokens when required (based on looking one token ahead).
// 
// NOT_LA exists so that productions such as NOT LIKE can be given the same
// precedence as LIKE; otherwise they'd effectively have the same precedence as
// NOT, at least with respect to their left-hand subexpression. NULLS_LA and
// WITH_LA are needed to make the grammar LALR(1).
%token     NOT_LA NULLS_LA WITH_LA

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
%left      '+' '-'
%left      '*' '/' '%'
%left      '&' '|' '^' '#'
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
    sqllex.(*scanner).stmts = $1
  }

stmt_list:
  stmt_list ';' stmt
  {
    if $3 != nil {
      $$ = append($1, $3)
    }
  }
| stmt
  {
    if $1 != nil {
      $$ = []Statement{$1}
    } else {
      $$ = nil
    }
  }

stmt:
  alter_stmt
| create_stmt
| delete_stmt
| drop_stmt
| explain_stmt
| insert_stmt
| rename_stmt
| select_stmt
| set_stmt
| show_stmt
| transaction_stmt
| truncate_stmt
| update_stmt
| /* EMPTY */
  {
    $$ = nil
  }

// ALTER [DATABASE|INDEX|TABLE|VIEW]
alter_stmt:
  alter_database_stmt
| alter_index_stmt
| alter_table_stmt
  // | alter_view_stmt

alter_database_stmt:
  ALTER DATABASE database_name WITH createdb_opt_list
  {
    $$ = nil
  }
| ALTER DATABASE database_name createdb_opt_list
  {
    $$ = nil
  }
| ALTER DATABASE database_name set_reset_clause
  {
    $$ = nil
  }

alter_index_stmt:
  ALTER INDEX qualified_name alter_table_cmds
  {
    $$ = nil
  }
| ALTER INDEX IF EXISTS qualified_name alter_table_cmds
  {
    $$ = nil
  }

alter_table_stmt:
  ALTER TABLE relation_expr alter_table_cmds
  {
    $$ = nil
  }
| ALTER TABLE IF EXISTS relation_expr alter_table_cmds
  {
    $$ = nil
  }
// | ALTER TABLE relation_expr SET SCHEMA name
//   {
//     $$ = nil
//   }
// | ALTER TABLE IF EXISTS relation_expr SET SCHEMA name
//   {
//     $$ = nil
//   }

// alter_view_stmt:
//   ALTER VIEW qualified_name alter_table_cmds
//   {
//     $$ = nil
//   }
// | ALTER VIEW IF EXISTS qualified_name alter_table_cmds
//   {
//     $$ = nil
//   }
// | ALTER VIEW qualified_name SET SCHEMA name
//   {
//     $$ = nil
//   }
// | ALTER VIEW IF EXISTS qualified_name SET SCHEMA name
//   {
//     $$ = nil
//   }

alter_table_cmds:
  alter_table_cmd
| alter_table_cmds ',' alter_table_cmd

alter_table_cmd:
  // ALTER TABLE <name> ADD <coldef>
  ADD column_def {}
  // ALTER TABLE <name> ADD COLUMN <coldef>
| ADD COLUMN column_def {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT}
| ALTER opt_column col_id alter_column_default {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL
| ALTER opt_column col_id DROP NOT NULL {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL
| ALTER opt_column col_id SET NOT NULL {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS <signed_iconst>
| ALTER opt_column col_id SET STATISTICS signed_iconst {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] )
| ALTER opt_column col_id SET reloptions {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] )
| ALTER opt_column col_id RESET reloptions {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode>
| ALTER opt_column col_id SET STORAGE col_id {}
  // ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE]
| DROP opt_column IF EXISTS col_id opt_drop_behavior {}
  // ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE]
| DROP opt_column col_id opt_drop_behavior {}
  // ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
  //     [ USING <expression> ]
| ALTER opt_column col_id opt_set_data TYPE typename opt_collate_clause alter_using {}
  // ALTER TABLE <name> ADD CONSTRAINT ...
| ADD table_constraint {}
  // ALTER TABLE <name> ALTER CONSTRAINT ...
| ALTER CONSTRAINT name {}
  // ALTER TABLE <name> VALIDATE CONSTRAINT ...
| VALIDATE CONSTRAINT name {}
  // ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT IF EXISTS name opt_drop_behavior {}
  // ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT name opt_drop_behavior {}
  // ALTER TABLE <name> SET LOGGED 
| SET LOGGED {}
  // ALTER TABLE <name> SET UNLOGGED 
| SET UNLOGGED {}
  // ALTER TABLE <name> INHERIT <parent>
| INHERIT qualified_name {}
  // ALTER TABLE <name> NO INHERIT <parent>
| NO INHERIT qualified_name {}
  // ALTER TABLE <name> OF <type_name>
| OF any_name {}
  // ALTER TABLE <name> NOT OF
| NOT OF {}
  // ALTER TABLE <name> SET (...)
| SET reloptions {}
  // ALTER TABLE <name> RESET (...)
| RESET reloptions {}

alter_column_default:
  SET DEFAULT a_expr {}
| DROP DEFAULT {}

opt_drop_behavior:
  CASCADE {}
| RESTRICT {}
| /* EMPTY */ {}

opt_collate_clause:
  COLLATE any_name {}
| /* EMPTY */ {}

alter_using:
  USING a_expr {}
| /* EMPTY */ {}

reloptions:
  '(' reloption_list ')'
  {
    $$ = $2
  }

opt_reloptions:
  WITH reloptions {}
| /* EMPTY */ {}

reloption_list:
  reloption_elem {}
| reloption_list ',' reloption_elem {}

// This should match def_elem and also allow qualified names.
reloption_elem:
  col_label '=' def_arg {}
| col_label {}
| col_label '.' col_label '=' def_arg {}
| col_label '.' col_label {}

// CREATE [DATABASE|INDEX|SCHEMA|TABLE|TABLE AS|VIEW]
create_stmt:
  create_database_stmt
| create_index_stmt
// | create_schema_stmt
| create_table_stmt
| create_table_as_stmt
// | create_view_stmt

// DELETE FROM query
delete_stmt:
  opt_with_clause DELETE FROM relation_expr_opt_alias
    where_clause returning_clause
  {
    $$ = &Delete{Table: $4, Where: newWhere(astWhere, $5)}
  }

// DROP itemtype [ IF EXISTS ] itemname [, itemname ...] [ RESTRICT | CASCADE ]
drop_stmt:
  DROP drop_type IF EXISTS any_name_list opt_drop_behavior
  {
    $$ = nil
  }
| DROP drop_type any_name_list opt_drop_behavior
  {
    $$ = nil
  }
| DROP DATABASE database_name
  {
    $$ = &DropDatabase{Name: $3, IfExists: false}
  }
| DROP DATABASE IF EXISTS database_name
  {
    $$ = &DropDatabase{Name: $5, IfExists: true}
  }
| DROP TABLE any_name_list
  {
    $$ = &DropTable{Names: $3, IfExists: false}
  }
| DROP TABLE IF EXISTS any_name_list
  {
    $$ = &DropTable{Names: $5, IfExists: true}
  }

drop_type:
  INDEX {}
// | SCHEMA {}
// | VIEW {}

any_name_list:
  any_name
  {
    $$ = []QualifiedName{$1}
  }
| any_name_list ',' any_name
  {
    $$ = append($1, $3)
  }

any_name:
  col_id
  {
    $$ = QualifiedName{$1}
  }
| col_id attrs
  {
    $$ = append(QualifiedName{$1}, $2...)
  }

attrs:
  '.' attr_name
  {
    $$ = QualifiedName{$2}
  }
| attrs '.' attr_name
  {
    $$ = append($1, $3)
  }

// EXPLAIN [VERBOSE] query
// EXPLAIN (options) query
explain_stmt:
  EXPLAIN explainable_stmt
  {
    $$ = nil
  }
| EXPLAIN VERBOSE explainable_stmt
  {
    $$ = nil
  }
| EXPLAIN '(' explain_option_list ')' explainable_stmt
  {
    $$ = nil
  }

explainable_stmt:
  select_stmt
| insert_stmt
| update_stmt
| delete_stmt
| create_table_as_stmt

explain_option_list:
  explain_option_elem
| explain_option_list ',' explain_option_elem

explain_option_elem:
  explain_option_name explain_option_arg

explain_option_name:
  non_reserved_word
  {}

explain_option_arg:
  opt_boolean_or_string {}
| numeric_only {}
| /* EMPTY */ {}

// create_schema_stmt:
//   CREATE SCHEMA opt_schema_name AUTHORIZATION role_spec opt_schema_elem_list
//   {
//     $$ = nil
//   }
// | CREATE SCHEMA col_id opt_schema_elem_list
//   {
//     $$ = nil
//   }
// | CREATE SCHEMA IF NOT EXISTS opt_schema_name AUTHORIZATION role_spec opt_schema_elem_list
//   {
//     $$ = nil
//   }
// | CREATE SCHEMA IF NOT EXISTS col_id opt_schema_elem_list
//   {
//     $$ = nil
//   }
//
// opt_schema_name:
//   col_id {}
// | /* EMPTY */ {}
//
// opt_schema_elem_list:
//   opt_schema_elem_list opt_schema_elem {}
// | /* EMPTY */ {}
//
// // opt_schema_elem are the ones that can show up inside a CREATE SCHEMA
// // statement (in addition to by themselves).
// opt_schema_elem:
//   create_table_stmt {}
// | create_index_stmt {}
// | create_view_stmt {}

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
  TRANSACTION transaction_mode_list {}
| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list {}
| set_rest_more {}

generic_set:
  var_name TO var_list
  {
    $$ = &Set{Name: $1, Values: $3}
  }
| var_name '=' var_list
  {
    $$ = &Set{Name: $1, Values: $3}
  }
| var_name TO DEFAULT
  {
    $$ = &Set{Name: $1}
  }
| var_name '=' DEFAULT
  {
    $$ = &Set{Name: $1}
  }

set_rest_more:
  // Generic SET syntaxes:
  generic_set
| var_name FROM CURRENT {}
  // Special syntaxes mandated by SQL standard:
| TIME ZONE zone_value {}
| CATALOG sconst {}
// | SCHEMA sconst {}
| NAMES opt_encoding {}

var_name:
  col_id
  {
    $$ = []string{$1}
  }
| var_name '.' col_id
  {
    $$ = append($1, $3)
  }

var_list:
  var_value
  {
    $$ = []Expr{$1}
  }
| var_list ',' var_value
  {
    $$ = append($1, $3)
  }

var_value:
  opt_boolean_or_string
| numeric_only

iso_level:
  READ UNCOMMITTED {}
| READ COMMITTED {}
| REPEATABLE READ {}
| SERIALIZABLE {}

opt_boolean_or_string:
  TRUE
  {
    $$ = BoolVal(true)
  }
| FALSE
  {
    $$ = BoolVal(false)
  }
| ON
  {
    $$ = StrVal($1)
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
// col_id gives reduce/reduce errors against const_interval and LOCAL, so use
// IDENT (meaning we reject anything that is a key word).
zone_value:
  sconst {}
| IDENT {}
| const_interval sconst opt_interval {}
| const_interval '(' iconst ')' sconst {}
| numeric_only {}
| DEFAULT {}
| LOCAL {}

opt_encoding:
  sconst {}
| DEFAULT {}
| /* EMPTY */ {}

non_reserved_word_or_sconst:
  non_reserved_word
  {
    $$ = StrVal($1)
  }
| sconst
  {
    $$ = StrVal($1)
  }

set_reset_clause:
  SET set_rest {}

show_stmt:
  SHOW var_name
  {
    // It would be cleaner if we could have "SHOW DATABASES" and "SHOW
    // TABLES" rules, but unfortunately DATABASES and TABLES are
    // unreserved keywords and thus valid variable names and such
    // rules would cause reduce/reduce conflicts.
    if len($2) == 1 && strings.EqualFold($2[0], "DATABASES") {
      $$ = &ShowDatabases{}
    } else if len($2) == 1 && strings.EqualFold($2[0], "TABLES") {
      $$ = &ShowTables{}
    } else {
      $$ = nil
    }
  }
| SHOW TIME ZONE
  {
    $$ = nil
  }
| SHOW ALL
  {
    $$ = nil
  }
| SHOW TABLES FROM var_name
  {
    $$ = &ShowTables{Name: $4}
  }
| SHOW COLUMNS FROM var_name
  {
    $$ = &ShowColumns{Table: $4}
  }
| SHOW INDEX FROM var_name
  {
    $$ = &ShowIndex{Table: $4}
  }

// CREATE TABLE relname
create_table_stmt:
  CREATE opt_temp TABLE any_name '(' opt_table_elem_list ')'
    opt_inherit opt_with on_commit_option
  {
    $$ = &CreateTable{Table: $4, IfNotExists: false, Defs: $6}
  }
| CREATE opt_temp TABLE IF NOT EXISTS any_name '(' opt_table_elem_list ')'
    opt_inherit opt_with on_commit_option
  {
    $$ = &CreateTable{Table: $7, IfNotExists: true, Defs: $9}
  }
| CREATE opt_temp TABLE any_name OF any_name
    opt_typed_table_elem_list opt_with on_commit_option
  {
    $$ = nil
  }
| CREATE opt_temp TABLE IF NOT EXISTS any_name OF any_name
    opt_typed_table_elem_list opt_with on_commit_option
  {
    $$ = nil
  }

// Redundancy here is needed to avoid shift/reduce conflicts, since TEMP is not
// a reserved word.
// 
// NOTE: we accept both GLOBAL and LOCAL options. They currently do nothing,
// but future versions might consider GLOBAL to request SQL-spec-compliant temp
// table behavior, so warn about that. Since we have no modules the LOCAL
// keyword is really meaningless; furthermore, some other products implement
// LOCAL as meaning the same as our default temp table behavior, so we'll
// probably continue to treat LOCAL as a noise word.
opt_temp:
  TEMPORARY {}
| TEMP {}
| LOCAL TEMPORARY {}
| LOCAL TEMP {}
| GLOBAL TEMPORARY {}
| GLOBAL TEMP {}
| UNLOGGED {}
| /* EMPTY */ {}

opt_table_elem_list:
  table_elem_list
| /* EMPTY */
  {
    $$ = nil
  }

opt_typed_table_elem_list:
  '(' typed_table_elem_list ')'
  {
    $$ = $2
  }
| /* EMPTY */ {}

table_elem_list:
  table_elem
  {
    $$ = TableDefs{$1}
  }
| table_elem_list ',' table_elem
  {
    $$ = append($1, $3)
  }

typed_table_elem_list:
  typed_table_elem {}
| typed_table_elem_list ',' typed_table_elem {}

table_elem:
  column_def
| table_like_clause {}
| table_constraint

typed_table_elem:
  column_options {}
| table_constraint {}

column_def:
  col_id typename col_qual_list
  {
    $$ = newColumnTableDef($1, $2, $3)
  }

column_options:
  col_id WITH OPTIONS col_qual_list {}

col_qual_list:
  col_qual_list col_constraint
  {
    $$ = append($1, $2)
  }
| /* EMPTY */
  {
    $$ = nil
  }

col_constraint:
  CONSTRAINT name col_constraint_elem
  {
    // TODO(pmattis): Handle constraint name.
    $$ = $3
  }
| col_constraint_elem
| COLLATE any_name {}

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
col_constraint_elem:
  NOT NULL
  {
    $$ = NotNullConstraint{}
  }
| NULL
  {
    $$ = NullConstraint{}
  }
| UNIQUE
  {
    $$ = UniqueConstraint{}
  }
| PRIMARY KEY
  {
    $$ = PrimaryKeyConstraint{}
  }
| CHECK '(' a_expr ')' opt_no_inherit {}
| DEFAULT b_expr {}
| REFERENCES qualified_name opt_column_list key_match key_actions {}

table_like_clause:
  LIKE qualified_name table_like_option_list {}

table_like_option_list:
  table_like_option_list INCLUDING table_like_option {}
| table_like_option_list EXCLUDING table_like_option {}
| /* EMPTY */ {}

table_like_option:
  DEFAULTS {}
| CONSTRAINTS {}
| INDEXES {}
| STORAGE {}
| COMMENTS {}
| ALL {}

// constraint_elem specifies constraint syntax which is not embedded into a
// column definition. col_constraint_elem specifies the embedded form.
// - thomas 1997-12-03
table_constraint:
  CONSTRAINT name constraint_elem
  {
    $$ = $3
    if i, ok := $$.(*IndexTableDef); ok {
      i.Name = $2
    }
  }
| constraint_elem
  {
    $$ = $1
  }

constraint_elem:
  CHECK '(' a_expr ')' {}
| UNIQUE '(' column_list ')'
  {
    $$ = &IndexTableDef{Unique: true, Columns: $3}
  }
| UNIQUE existing_index {}
| INDEX '(' column_list ')'
  {
    $$ = &IndexTableDef{Columns: $3}
  }
| PRIMARY KEY '(' column_list ')'
  {
    $$ = &IndexTableDef{PrimaryKey: true, Columns: $4}
  }
| PRIMARY KEY existing_index {}
| EXCLUDE access_method_clause '(' exclusion_constraint_list ')'
    exclusion_where_clause {}
| FOREIGN KEY '(' column_list ')' REFERENCES qualified_name
    opt_column_list key_match key_actions {}

opt_no_inherit:
  NO INHERIT {}
| /* EMPTY */ {}

opt_column_list:
  '(' column_list ')'
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = nil
  }

column_list:
  column_elem
  {
    $$ = []string{$1}
  }
| column_list ',' column_elem
  {
    $$ = append($1, $3)
  }

column_elem:
  col_id

key_match:
  MATCH FULL {}
| MATCH PARTIAL {}
| MATCH SIMPLE {}
| /* EMPTY */ {}

exclusion_constraint_list:
  exclusion_constraint_elem {}
| exclusion_constraint_list ',' exclusion_constraint_elem {}

exclusion_constraint_elem:
  index_elem WITH any_operator {}

exclusion_where_clause:
  WHERE '(' a_expr ')' {}
| /* EMPTY */ {}

// We combine the update and delete actions into one value temporarily for
// simplicity of parsing, and then break them down again in the calling
// production. update is in the left 8 bits, delete in the right. Note that
// NOACTION is the default.
key_actions:
  key_update {}
| key_delete {}
| key_update key_delete {}
| key_delete key_update {}
| /* EMPTY */ {}

key_update:
  ON UPDATE key_action {}

key_delete:
  ON DELETE key_action {}

key_action:
  NO ACTION {}
| RESTRICT {}
| CASCADE {}
| SET NULL {}
| SET DEFAULT {}

opt_inherit:
  INHERITS '(' qualified_name_list ')' {}
| /* EMPTY */ {}

opt_with:
  WITH reloptions {}
| /* EMPTY */ {}

on_commit_option:
  ON COMMIT DROP {}
| ON COMMIT DELETE ROWS {}
| ON COMMIT PRESERVE ROWS {}
| /* EMPTY */ {}

existing_index:
  USING INDEX index_name {}

// CREATE TABLE relname AS select_stmt [ WITH [NO] DATA ]
create_table_as_stmt:
  CREATE opt_temp TABLE create_as_target AS select_stmt opt_with_data
  {
    $$ = nil
  }
| CREATE opt_temp TABLE IF NOT EXISTS create_as_target AS select_stmt opt_with_data
  {
    $$ = nil
  }

create_as_target:
  any_name opt_column_list opt_with on_commit_option {}

opt_with_data:
  WITH DATA {}
| WITH NO DATA {}
| /* EMPTY */ {}

numeric_only:
  FCONST
  {
    $$ = NumVal($1)
  }
| '-' FCONST
  {
    $$ = NumVal("-" + $2)
  }
| signed_iconst
  {
    $$ = IntVal($1)
  }

// Note: any simple identifier will be returned as a type name!
def_arg:
  func_type {}
| reserved_keyword {}
| math_op {}
| numeric_only {}
| sconst {}

// TRUNCATE table relname1, relname2, ...
truncate_stmt:
  TRUNCATE opt_table relation_expr_list opt_restart_seqs opt_drop_behavior
  {
    $$ = &Truncate{Tables: $3}
  }

opt_restart_seqs:
  CONTINUE IDENTITY {}
| RESTART IDENTITY {}
| /* EMPTY */ {}

// CREATE INDEX
create_index_stmt:
  CREATE opt_unique INDEX opt_concurrently opt_index_name
    ON qualified_name access_method_clause '(' index_params ')'
    opt_reloptions where_clause
  {
    $$ = nil
  }
| CREATE opt_unique INDEX opt_concurrently IF NOT EXISTS index_name
    ON qualified_name access_method_clause '(' index_params ')'
    opt_reloptions where_clause
  {
    $$ = nil
  }

opt_unique:
  UNIQUE {}
| /* EMPTY */ {}

opt_concurrently:
  CONCURRENTLY {}
| /* EMPTY */ {}

opt_index_name:
  index_name {}
| /* EMPTY */ {}

access_method_clause:
  USING access_method {}
| /* EMPTY */ {}

index_params:
  index_elem {}
| index_params ',' index_elem {}

// Index attributes can be either simple column references, or arbitrary
// expressions in parens. For backwards-compatibility reasons, we allow an
// expression that's just a function call to be written without parens.
index_elem:
  col_id opt_collate opt_class opt_asc_desc opt_nulls_order
  {}
| func_expr_windowless opt_collate opt_class opt_asc_desc opt_nulls_order
  {}
| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
  {}

opt_collate:
  COLLATE any_name {}
| /* EMPTY */ {}

opt_class:
  any_name {}
| USING any_name {}
| /* EMPTY */ {}

opt_asc_desc:
  ASC {}
| DESC {}
| /* EMPTY */ {}

opt_nulls_order:
  NULLS_LA FIRST {}
| NULLS_LA LAST {}
| /* EMPTY */ {}

// Ideally param_name should be col_id, but that causes too many conflicts.
param_name:
  type_function_name

// We would like to make the %TYPE productions here be col_id attrs etc, but
// that causes reduce/reduce conflicts. type_function_name is next best
// choice.
func_type:
  typename {}
| type_function_name attrs '%' TYPE {}
| SETOF type_function_name attrs '%' TYPE {}

any_operator:
  math_op {}
| col_id '.' any_operator {}

// ALTER THING name RENAME TO newname
rename_stmt:
  ALTER DATABASE database_name RENAME TO database_name
  {
    $$ = nil
  }
// | ALTER SCHEMA name RENAME TO name
//   {
//     $$ = nil
//   }
| ALTER TABLE relation_expr RENAME TO name
  {
    $$ = nil
  }
| ALTER TABLE IF EXISTS relation_expr RENAME TO name
  {
    $$ = nil
  }
// | ALTER VIEW qualified_name RENAME TO name
//   {
//     $$ = nil
//   }
// | ALTER VIEW IF EXISTS qualified_name RENAME TO name
//   {
//     $$ = nil
//   }
| ALTER INDEX qualified_name RENAME TO name
  {
    $$ = nil
  }
| ALTER INDEX IF EXISTS qualified_name RENAME TO name
  {
    $$ = nil
  }
| ALTER TABLE relation_expr RENAME opt_column name TO name
  {
    $$ = nil
  }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name
  {
    $$ = nil
  }
| ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
  {
    $$ = nil
  }
| ALTER TABLE IF EXISTS relation_expr RENAME CONSTRAINT name TO name
  {
    $$ = nil
  }

opt_column:
  COLUMN {}
| /* EMPTY */ {}

opt_set_data:
  SET DATA {}
| /* EMPTY */ {}

// BEGIN / COMMIT / ROLLBACK / ...
transaction_stmt:
  ABORT opt_transaction
  {
    $$ = nil
  }
| BEGIN opt_transaction transaction_mode_list_or_empty
  {
    $$ = nil
  }
| START TRANSACTION transaction_mode_list_or_empty
  {
    $$ = nil
  }
| COMMIT opt_transaction
  {
    $$ = nil
  }
| END opt_transaction
  {
    $$ = nil
  }
| ROLLBACK opt_transaction
  {
    $$ = nil
  }
| SAVEPOINT col_id
  {
    $$ = nil
  }
| RELEASE SAVEPOINT col_id
  {
    $$ = nil
  }
| RELEASE col_id
  {
    $$ = nil
  }
| ROLLBACK opt_transaction TO SAVEPOINT col_id
  {
    $$ = nil
  }
| ROLLBACK opt_transaction TO col_id
  {
    $$ = nil
  }
| PREPARE TRANSACTION sconst
  {
    $$ = nil
  }
| COMMIT PREPARED sconst
  {
    $$ = nil
  }
| ROLLBACK PREPARED sconst
  {
    $$ = nil
  }

opt_transaction:
  WORK {}
| TRANSACTION {}
| /* EMPTY */ {}

transaction_mode_item:
  ISOLATION LEVEL iso_level {}
| READ ONLY {}
| READ WRITE {}
| DEFERRABLE {}
| NOT DEFERRABLE {}

// Syntax with commas is SQL-spec, without commas is Postgres historical.
transaction_mode_list:
  transaction_mode_item {}
| transaction_mode_list ',' transaction_mode_item {}
| transaction_mode_list transaction_mode_item {}

transaction_mode_list_or_empty:
  transaction_mode_list {}
| /* EMPTY */ {}

// CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
//     AS <query> [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
// create_view_stmt:
//   CREATE opt_temp VIEW qualified_name opt_column_list opt_reloptions
//     AS select_stmt opt_check_option
//   {
//     $$ = nil
//   }
// | CREATE OR REPLACE opt_temp VIEW qualified_name opt_column_list opt_reloptions
//     AS select_stmt opt_check_option
//   {
//     $$ = nil
//   }
// | CREATE opt_temp RECURSIVE VIEW qualified_name '(' column_list ')' opt_reloptions
//     AS select_stmt opt_check_option
//   {
//     $$ = nil
//   }
// | CREATE OR REPLACE opt_temp RECURSIVE VIEW qualified_name '(' column_list ')' opt_reloptions
//     AS select_stmt opt_check_option
//   {
//     $$ = nil
//   }
//
// opt_check_option:
//   WITH CHECK OPTION {}
// | WITH CASCADED CHECK OPTION {}
// | WITH LOCAL CHECK OPTION {}
// | /* EMPTY */ {}

create_database_stmt:
  CREATE DATABASE database_name createdb_opt_with createdb_opt_list
  {
    // TODO(pmattis): Handle options.
    $$ = &CreateDatabase{Name: $3}
  }
| CREATE DATABASE IF NOT EXISTS database_name createdb_opt_with createdb_opt_list
  {
    // TODO(pmattis): Handle options.
    $$ = &CreateDatabase{IfNotExists: true, Name: $6}
  }

createdb_opt_with:
  WITH {}
| WITH_LA {}
| /* EMPTY */ {}

createdb_opt_list:
  createdb_opt_items {}
| /* EMPTY */ {}

createdb_opt_items:
  createdb_opt_item {}
| createdb_opt_items createdb_opt_item {}

createdb_opt_item:
  createdb_opt_name opt_equal signed_iconst {}
| createdb_opt_name opt_equal opt_boolean_or_string {}
| createdb_opt_name opt_equal DEFAULT {}

// Ideally we'd use col_id here, but that causes shift/reduce conflicts against
// the ALTER DATABASE SET/RESET syntaxes. Instead call out specific keywords
// we need, and allow IDENT so that database option names don't have to be
// parser keywords unless they are already keywords for other reasons.
// 
// XXX this coding technique is fragile since if someone makes a formerly
// non-keyword option name into a keyword and forgets to add it here, the
// option will silently break. Best defense is to provide a regression test
// exercising every such option, at least at the syntax level.
createdb_opt_name:
  IDENT {}
| CONNECTION LIMIT {}
| ENCODING {}
| LOCATION {}
| OWNER {}
| TEMPLATE {}

// Though the equals sign doesn't match other WITH options, pg_dump uses equals
// for backward compatibility, and it doesn't seem worth removing it.
opt_equal:
  '=' {}
| /* EMPTY */ {}

opt_name_list:
  '(' name_list ')'
  {
    $$ = $2
  }
| /* EMPTY */ {}

insert_stmt:
  opt_with_clause INSERT INTO insert_target insert_rest
    opt_on_conflict returning_clause
  {
    $$ = $5
    $$.(*Insert).Table = $4
  }

// Can't easily make AS optional here, because VALUES in insert_rest would have
// a shift/reduce conflict with VALUES as an optional alias. We could easily
// allow unreserved_keywords as optional aliases, but that'd be an odd
// divergence from other places. So just require AS for now.
insert_target:
  qualified_name
| qualified_name AS col_id
  // TODO(pmattis): Support alias.

insert_rest:
  select_stmt
  {
    $$ = &Insert{Rows: $1.(SelectStatement)}
  }
| '(' insert_column_list ')' select_stmt
  {
    $$ = &Insert{Columns: Columns($2), Rows: $4.(SelectStatement)}
  }
| DEFAULT VALUES
  {
    $$ = &Insert{}
  }

insert_column_list:
  insert_column_item
  {
    $$ = SelectExprs{$1}
  }
| insert_column_list ',' insert_column_item
  {
    $$ = append($1, $3)
  }

insert_column_item:
  col_id opt_indirection
  {
    $$ = &NonStarExpr{Expr: append(QualifiedName{$1}, $2...)}
  }

opt_on_conflict:
  ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list where_clause {}
| ON CONFLICT opt_conf_expr DO NOTHING {}
| /* EMPTY */ {}

opt_conf_expr:
  '(' index_params ')' where_clause {}
| ON CONSTRAINT name {}
| /* EMPTY */ {}

returning_clause:
  RETURNING target_list {}
| /* EMPTY */ {}

update_stmt:
  opt_with_clause UPDATE relation_expr_opt_alias
    SET set_clause_list
    from_clause
    where_clause
    returning_clause
  {
    $$ = &Update{Table: $3, Exprs: $5, Where: newWhere(astWhere, $7)}
  }

set_clause_list:
  set_clause
  {
    $$ = UpdateExprs{$1}
  }
| set_clause_list ',' set_clause
  {
    $$ = append($1, $3)
  }

set_clause:
  single_set_clause
| multiple_set_clause {}

single_set_clause:
  set_target '=' ctext_expr
  {
    $$ = &UpdateExpr{Name: $1, Expr: $3}
  }

// Ideally, we'd accept any row-valued a_expr as RHS of a multiple_set_clause.
// However, per SQL spec the row-constructor case must allow DEFAULT as a row
// member, and it's pretty unclear how to do that (unless perhaps we allow
// DEFAULT in any a_expr and let parse analysis sort it out later?). For the
// moment, the planner/executor only support a subquery as a multiassignment
// source anyhow, so we need only accept ctext_row and subqueries here.
multiple_set_clause:
  '(' set_target_list ')' '=' ctext_row {}
| '(' set_target_list ')' '=' select_with_parens {}

set_target:
  col_id opt_indirection
  {
    $$ = append(QualifiedName{$1}, $2...)
  }

set_target_list:
  set_target {}
| set_target_list ',' set_target {}

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
    $$ = $2
  }
| '(' select_with_parens ')'
  {
    $$ = $2
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
  }
| select_clause opt_sort_clause for_locking_clause opt_select_limit
  {
    $$ = $1
    if s, ok := $$.(*Select); ok {
      s.Limit = $4
    }
  }
| select_clause opt_sort_clause select_limit opt_for_locking_clause
  {
    $$ = $1
    if s, ok := $$.(*Select); ok {
      s.Limit = $3
    }
  }
| with_clause select_clause
  {
    $$ = $2
  }
| with_clause select_clause sort_clause
  {
    $$ = $2
  }
| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
  {
    $$ = $2
    if s, ok := $$.(*Select); ok {
      s.Limit = $5
    }
  }
| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
  {
    $$ = $2
    if s, ok := $$.(*Select); ok {
      s.Limit = $4
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
  SELECT opt_all_clause opt_target_list
    from_clause where_clause
    group_clause having_clause window_clause
  {
    $$ = &Select{
      Exprs:  $3,
      From:   $4,
      Where:  newWhere(astWhere, $5),
      Having: newWhere(astHaving, $7),
    }
  }
| SELECT distinct_clause target_list
    from_clause where_clause
    group_clause having_clause window_clause
  {
    // TODO(pmattis): Support DISTINCT ON?
    $$ = &Select{
      Distinct: astDistinct,
      Exprs:    $3,
      From:     $4,
      Where:    newWhere(astWhere, $5),
      Having:   newWhere(astHaving, $7),
    }
  }
| values_clause
| TABLE relation_expr {}
| select_clause UNION all_or_distinct select_clause
  {
    // TODO(pmattis): Support all/distinct
    $$ = &Union{
      Type:  astUnion,
      Left:  $1.(SelectStatement),
      Right: $4.(SelectStatement),
    }
  }
| select_clause INTERSECT all_or_distinct select_clause
  {
    // TODO(pmattis): Support all/distinct
    $$ = &Union{
      Type:  astIntersect,
      Left:  $1.(SelectStatement),
      Right: $4.(SelectStatement),
    }
  }
| select_clause EXCEPT all_or_distinct select_clause
  {
    // TODO(pmattis): Support all/distinct
    $$ = &Union{
      Type:  astExcept,
      Left:  $1.(SelectStatement),
      Right: $4.(SelectStatement),
    }
  }

// SQL standard WITH clause looks like:
// 
// WITH [ RECURSIVE ] <query name> [ (<column>,...) ]
//      	AS (query) [ SEARCH or CYCLE clause ]
// 
// We don't currently support the SEARCH or CYCLE clause.
// 
// Recognizing WITH_LA here allows a CTE to be named TIME or ORDINALITY.
with_clause:
  WITH cte_list {}
| WITH_LA cte_list {}
| WITH RECURSIVE cte_list {}

cte_list:
  common_table_expr {}
| cte_list ',' common_table_expr {}

common_table_expr:
  name opt_name_list AS '(' preparable_stmt ')' {}

preparable_stmt:
  select_stmt
| insert_stmt
| update_stmt
| delete_stmt

opt_with_clause:
  with_clause {}
| /* EMPTY */ {}

opt_table:
  TABLE {}
| /* EMPTY */ {}

all_or_distinct:
  ALL {}
| DISTINCT {}
| /* EMPTY */ {}

// We use (NIL) as a placeholder to indicate that all target expressions should
// be placed in the DISTINCT list during parsetree analysis.
distinct_clause:
  DISTINCT {}
| DISTINCT ON '(' expr_list ')' {}

opt_all_clause:
  ALL {}
| /* EMPTY */ {}

opt_sort_clause:
  sort_clause {}
| /* EMPTY */ {}

sort_clause:
  ORDER BY sortby_list {}

sortby_list:
  sortby {}
| sortby_list ',' sortby {}

sortby:
  a_expr USING math_op opt_nulls_order {}
| a_expr opt_asc_desc opt_nulls_order {}

select_limit:
  limit_clause offset_clause
  {
    if $1 == nil {
      $$ = $2
    } else {
      $$ = $1
      $$.Offset = $2.Offset
    }
  }
| offset_clause limit_clause
  {
    $$ = $1
    if $2 != nil {
      $$.Count = $2.Count
    }
  }
| limit_clause
| offset_clause

opt_select_limit:
  select_limit
| /* EMPTY */
  {
    $$ = nil
  }

limit_clause:
  LIMIT select_limit_value
  {
    if $2 == nil {
      $$ = nil
    } else {
      $$ = &Limit{Count: $2}
    }
  }
  // SQL:2008 syntax
| FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY {}

offset_clause:
  OFFSET select_offset_value
  {
    $$ = &Limit{Offset: $2}
  }
  // SQL:2008 syntax
| OFFSET select_offset_value2 row_or_rows
  {
    $$ = &Limit{Offset: $2}
  }

select_limit_value:
  a_expr
| ALL
  {
    $$ = nil
  }

select_offset_value:
  a_expr

// Allowing full expressions without parentheses causes various parsing
// problems with the trailing ROW/ROWS key words. SQL only calls for constants,
// so we allow the rest only with parentheses. If omitted, default to 1.
opt_select_fetch_first_value:
  signed_iconst {}
| '(' a_expr ')' {}
| /* EMPTY */ {}

// Again, the trailing ROW/ROWS in this case prevent the full expression
// syntax. c_expr is the best we can do.
select_offset_value2:
  c_expr

// noise words
row_or_rows:
  ROW {}
| ROWS {}

first_or_next:
  FIRST {}
| NEXT {}

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
  GROUP BY group_by_list {}
| /* EMPTY */ {}

group_by_list:
  group_by_item {}
| group_by_list ',' group_by_item {}

group_by_item:
  a_expr {}
| empty_grouping_set {}

empty_grouping_set:
  '(' ')' {}

having_clause:
  HAVING a_expr
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = nil
  }

for_locking_clause:
  for_locking_items {}
| FOR READ ONLY {}

opt_for_locking_clause:
  for_locking_clause {}
| /* EMPTY */ {}

for_locking_items:
  for_locking_item {}
| for_locking_items for_locking_item {}

for_locking_item:
  for_locking_strength locked_rels_list opt_nowait_or_skip {}

opt_nowait_or_skip:
 NOWAIT {}
| SKIP LOCKED {}
| /* EMPTY */ {}

for_locking_strength:
  FOR UPDATE {}
| FOR NO KEY UPDATE {}
| FOR SHARE {}
| FOR KEY SHARE {}

locked_rels_list:
  OF qualified_name_list {}
| /* EMPTY */ {}

values_clause:
  VALUES ctext_row
  {
    $$ = Values{Tuple($2)}
  }
| values_clause ',' ctext_row
  {
    $$ = append($1.(Values), Tuple($3))
  }

// clauses common to all optimizable statements:
// 	from_clause   - allow list of both JOIN expressions and table names
// 	where_clause  - qualifications for joins or restrictions

from_clause:
  FROM from_list
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = nil
  }

from_list:
  table_ref
  {
    $$ = TableExprs{$1}
  }
| from_list ',' table_ref
  {
    $$ = append($1, $3)
  }

// table_ref is where an alias clause can be attached.
table_ref:
  relation_expr opt_alias_clause
  {
    $$ = &AliasedTableExpr{Expr: $1, As: $2}
  }
| func_table func_alias_clause {}
| LATERAL func_table func_alias_clause {}
| select_with_parens opt_alias_clause
  {
    $$ = &AliasedTableExpr{Expr: &Subquery{Select: $1.(SelectStatement)}, As: $2}
  }
| LATERAL select_with_parens opt_alias_clause {}
| joined_table
| '(' joined_table ')' alias_clause {}

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
    $$ = &ParenTableExpr{Expr: $2}
  }
| table_ref CROSS JOIN table_ref
  {
    $$ = &JoinTableExpr{Join: astCrossJoin, Left: $1, Right: $4}
  }
| table_ref join_type JOIN table_ref join_qual
  {
    $$ = &JoinTableExpr{Join: $2, Left: $1, Right: $4, Cond: $5}
  }
| table_ref JOIN table_ref join_qual
  {
    $$ = &JoinTableExpr{Join: astJoin, Left: $1, Right: $3, Cond: $4}
  }
| table_ref NATURAL join_type JOIN table_ref
  {
    $$ = &JoinTableExpr{Join: astNaturalJoin, Left: $1, Right: $5}
  }
| table_ref NATURAL JOIN table_ref
  {
    $$ = &JoinTableExpr{Join: astNaturalJoin, Left: $1, Right: $4}
  }

alias_clause:
  AS col_id '(' name_list ')' {}
| AS col_id
  {
    $$ = $2
  }
| col_id '(' name_list ')' {}
| col_id
  {
    $$ = $1
  }

opt_alias_clause:
  alias_clause
| /* EMPTY */
  {
    $$ = ""
  }

// func_alias_clause can include both an Alias and a coldeflist, so we make it
// return a 2-element list that gets disassembled by calling production.
func_alias_clause:
  alias_clause {}
| AS '(' table_func_elem_list ')' {}
| AS col_id '(' table_func_elem_list ')' {}
| col_id '(' table_func_elem_list ')' {}
| /* EMPTY */ {}

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
    $$ = &UsingJoinCond{Cols: $3}
  }
| ON a_expr
  {
    $$ = &OnJoinCond{Expr: $2}
  }

relation_expr:
  qualified_name
  {
    $$ = $1
  }
| qualified_name '*'
  {
    $$ = append(QualifiedName($1), "*")
  }
| ONLY qualified_name
  {
    // TODO(pmattis): Support ONLY.
    $$ = $2
  }
| ONLY '(' qualified_name ')'
  {
    // TODO(pmattis): Support ONLY.
    $$ = $3
  }

relation_expr_list:
  relation_expr
  {
    $$ = []QualifiedName{$1}
  }
| relation_expr_list ',' relation_expr
  {
    $$ = append($1, $3)
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
    $$ = &AliasedTableExpr{Expr: $1}
  }
| relation_expr col_id
  {
    $$ = &AliasedTableExpr{Expr: $1, As: $2}
  }
| relation_expr AS col_id
  {
    $$ = &AliasedTableExpr{Expr: $1, As: $3}
  }

// func_table represents a function invocation in a FROM list. It can be a
// plain function call, like "foo(...)", or a ROWS FROM expression with one or
// more function calls, "ROWS FROM (foo(...), bar(...))", optionally with WITH
// ORDINALITY attached. In the ROWS FROM syntax, a column definition list can
// be given for each function, for example:
//     ROWS FROM (foo() AS (foo_res_a text, foo_res_b text),
//                bar() AS (bar_res_a text, bar_res_b text))
// It's also possible to attach a column definition list to the RangeFunction
// as a whole, but that's handled by the table_ref production.
func_table:
  func_expr_windowless opt_ordinality {}
| ROWS FROM '(' rowsfrom_list ')' opt_ordinality {}

rowsfrom_item:
  func_expr_windowless opt_col_def_list {}

rowsfrom_list:
  rowsfrom_item {}
| rowsfrom_list ',' rowsfrom_item {}

opt_col_def_list:
  AS '(' table_func_elem_list ')' {}
| /* EMPTY */ {}

opt_ordinality:
  WITH_LA ORDINALITY {}
| /* EMPTY */ {}

where_clause:
  WHERE a_expr
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = nil
  }

table_func_elem_list:
  table_func_elem {}
| table_func_elem_list ',' table_func_elem {}

table_func_elem:
  col_id typename opt_collate_clause {}

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
| SETOF simple_typename opt_array_bounds {}
  // SQL standard syntax, currently only one-dimensional
| simple_typename ARRAY '[' iconst ']' {}
| SETOF simple_typename ARRAY '[' iconst ']' {}
| simple_typename ARRAY {}
| SETOF simple_typename ARRAY {}

opt_array_bounds:
  opt_array_bounds '[' ']' {}
| opt_array_bounds '[' iconst ']' {}
| /* EMPTY */ {}

simple_typename:
  numeric
| bit
| character
| const_datetime
| const_interval opt_interval {}
| const_interval '(' iconst ')' {}
| BLOB
  {
    $$ = &BlobType{}
  }
| TEXT
  {
    $$ = &TextType{}
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
  '(' iconst ')'
  {
    $$ = &DecimalType{Prec: $2}
  }
| '(' iconst ',' iconst ')'
  {
    $$ = &DecimalType{Prec: $2, Scale: $4}
  }
| /* EMPTY */
  {
    $$ = &DecimalType{}
  }

// SQL numeric data types
numeric:
  INT
  {
    $$ = &IntType{Name: astInt}
  }
| INTEGER
  {
    $$ = &IntType{Name: astInteger}
  }
| SMALLINT
  {
    $$ = &IntType{Name: astSmallInt}
  }
| BIGINT
  {
    $$ = &IntType{Name: astBigInt}
  }
| REAL
  {
    $$ = &FloatType{Name: astReal}
  }
| FLOAT opt_float
  {
    $$ = &FloatType{Name: astFloat, Prec: $2}
  }
| DOUBLE PRECISION
  {
    $$ = &FloatType{Name: astDouble}
  }
| DECIMAL opt_numeric_modifiers
  {
    $$ = $2
    $$.(*DecimalType).Name = astDecimal
  }
| DEC opt_numeric_modifiers
  {
    $$ = $2
    $$.(*DecimalType).Name = astDecimal
  }
| NUMERIC opt_numeric_modifiers
  {
    $$ = $2
    $$.(*DecimalType).Name = astNumeric
  }
| BOOLEAN
  {
    $$ = &BoolType{}
  }

opt_float:
  '(' iconst ')'
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = 0
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
  BIT opt_varying '(' iconst ')'
  {
    $$ = &BitType{N: $4}
  }

bit_without_length:
  BIT opt_varying
  {
    $$ = &BitType{}
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
  character_base '(' iconst ')' opt_charset
  {
    $$ = $1
    $$.(*CharType).N = $3
  }

character_without_length:
  character_base opt_charset
  {
    $$ = $1
  }

character_base:
  CHARACTER opt_varying
  {
    $$ = &CharType{Name: astChar}
  }
| CHAR opt_varying
  {
    $$ = &CharType{Name: astChar}
  }
| VARCHAR
  {
    $$ = &CharType{Name: astVarChar}
  }

opt_varying:
  VARYING {}
| /* EMPTY */ {}

opt_charset:
  CHARACTER SET col_id {}
| /* EMPTY */ {}

// SQL date/time types
const_datetime:
  DATE
  {
    $$ = &DateType{}
  }
| TIMESTAMP '(' iconst ')' opt_timezone
  {
    $$ = &TimestampType{}
  }
| TIMESTAMP opt_timezone
  {
    $$ = &TimestampType{}
  }
| TIME '(' iconst ')' opt_timezone
  {
    $$ = &TimeType{}
  }
| TIME opt_timezone
  {
    $$ = &TimeType{}
  }

const_interval:
  INTERVAL {}

opt_timezone:
  WITH_LA TIME ZONE {}
| WITHOUT TIME ZONE {}
| /* EMPTY */ {}

opt_interval:
  YEAR {}
| MONTH {}
| DAY {}
| HOUR {}
| MINUTE {}
| interval_second {}
| YEAR TO MONTH {}
| DAY TO HOUR {}
| DAY TO MINUTE {}
| DAY TO interval_second {}
| HOUR TO MINUTE {}
| HOUR TO interval_second {}
| MINUTE TO interval_second {}
| /* EMPTY */ {}

interval_second:
  SECOND {}
| SECOND '(' iconst ')' {}

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
| a_expr TYPECAST typename {}
| a_expr COLLATE any_name {}
| a_expr AT TIME ZONE a_expr %prec AT {}
  // These operators must be called out explicitly in order to make use of
  // bison's automatic operator-precedence handling. All other operator names
  // are handled by the generic productions using "OP", below; and all those
  // operators will have the same precedence.
  // 
  // If you add more explicitly-known operators, be sure to add them also to
  // b_expr and to the math_op list below.
| '+' a_expr %prec UMINUS
  {
    $$ = &UnaryExpr{Operator: UnaryPlus, Expr: $2}
  }
| '-' a_expr %prec UMINUS
  {
    $$ = &UnaryExpr{Operator: UnaryMinus, Expr: $2}
  }
| '~' a_expr %prec UMINUS
  {
    $$ = &UnaryExpr{Operator: UnaryComplement, Expr: $2}
  }
| a_expr '+' a_expr
  {
    $$ = &BinaryExpr{Operator: Plus, Left: $1, Right: $3}
  }
| a_expr '-' a_expr
  {
    $$ = &BinaryExpr{Operator: Minus, Left: $1, Right: $3}
  }
| a_expr '*' a_expr
  {
    $$ = &BinaryExpr{Operator: Mult, Left: $1, Right: $3}
  }
| a_expr '/' a_expr
  {
    $$ = &BinaryExpr{Operator: Div, Left: $1, Right: $3}
  }
| a_expr '%' a_expr
  {
    $$ = &BinaryExpr{Operator: Mod, Left: $1, Right: $3}
  }
| a_expr '^' a_expr
  {
    $$ = &BinaryExpr{Operator: Exp, Left: $1, Right: $3}
  }
| a_expr '&' a_expr
  {
    $$ = &BinaryExpr{Operator: Bitand, Left: $1, Right: $3}
  }
| a_expr '|' a_expr
  {
    $$ = &BinaryExpr{Operator: Bitor, Left: $1, Right: $3}
  }
| a_expr '#' a_expr
  {
    $$ = &BinaryExpr{Operator: Bitxor, Left: $1, Right: $3}
  }
| a_expr '<' a_expr
  {
    $$ = &ComparisonExpr{Operator: LT, Left: $1, Right: $3}
  }
| a_expr '>' a_expr
  {
    $$ = &ComparisonExpr{Operator: GT, Left: $1, Right: $3}
  }
| a_expr '=' a_expr
  {
    $$ = &ComparisonExpr{Operator: EQ, Left: $1, Right: $3}
  }
| a_expr CONCAT a_expr
  {
    $$ = &BinaryExpr{Operator: Concat, Left: $1, Right: $3}
  }
| a_expr LESS_EQUALS a_expr
  {
    $$ = &ComparisonExpr{Operator: LE, Left: $1, Right: $3}
  }
| a_expr GREATER_EQUALS a_expr
  {
    $$ = &ComparisonExpr{Operator: GE, Left: $1, Right: $3}
  }
| a_expr NOT_EQUALS a_expr
  {
    $$ = &ComparisonExpr{Operator: NE, Left: $1, Right: $3}
  }
| a_expr AND a_expr
  {
    $$ = &AndExpr{Left: $1, Right: $3}
  }
| a_expr OR a_expr
  {
    $$ = &OrExpr{Left: $1, Right: $3}
  }
| NOT a_expr
  {
    $$ = &NotExpr{Expr: $2}
  }
| NOT_LA a_expr %prec NOT
  {
    $$ = &NotExpr{Expr: $2}
  }
| a_expr LIKE a_expr
  {
    $$ = &ComparisonExpr{Operator: Like, Left: $1, Right: $3}
  }
| a_expr LIKE a_expr ESCAPE a_expr %prec LIKE
  {
    $$ = &ComparisonExpr{Operator: Like, Left: $1, Right: $3}
  }
| a_expr NOT_LA LIKE a_expr %prec NOT_LA
  {
    $$ = &ComparisonExpr{Operator: NotLike, Left: $1, Right: $4}
  }
| a_expr NOT_LA LIKE a_expr ESCAPE a_expr %prec NOT_LA
  {
    $$ = &ComparisonExpr{Operator: NotLike, Left: $1, Right: $4}
  }
| a_expr SIMILAR TO a_expr %prec SIMILAR {}
| a_expr SIMILAR TO a_expr ESCAPE a_expr %prec SIMILAR {}
| a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA {}
| a_expr NOT_LA SIMILAR TO a_expr ESCAPE a_expr %prec NOT_LA {}
| a_expr IS NULL %prec IS
  {
    $$ = &NullCheck{Expr: $1}
  }
| a_expr IS NOT NULL %prec IS
  {
    $$ = &NullCheck{Not: true, Expr: $1}
  }
| row OVERLAPS row {}
| a_expr IS TRUE %prec IS {}
| a_expr IS NOT TRUE %prec IS {}
| a_expr IS FALSE %prec IS {}
| a_expr IS NOT FALSE %prec IS {}
| a_expr IS UNKNOWN %prec IS {}
| a_expr IS NOT UNKNOWN %prec IS {}
| a_expr IS DISTINCT FROM a_expr %prec IS {}
| a_expr IS NOT DISTINCT FROM a_expr %prec IS {}
| a_expr IS OF '(' type_list ')' %prec IS {}
| a_expr IS NOT OF '(' type_list ')' %prec IS {}
| a_expr BETWEEN opt_asymmetric b_expr AND a_expr %prec BETWEEN
  {
    $$ = &RangeCond{Left: $1, From: $4, To: $6}
  }
| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
  {
    $$ = &RangeCond{Not: true, Left: $1, From: $5, To: $7}
  }
| a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
  {
    $$ = &RangeCond{Left: $1, From: $4, To: $6}
  }
| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
  {
    $$ = &RangeCond{Not: true, Left: $1, From: $5, To: $7}
  }
| a_expr IN in_expr
  {
    $$ = &ComparisonExpr{Operator: In, Left: $1, Right: $3}
  }
| a_expr NOT_LA IN in_expr %prec NOT_LA
  {
    $$ = &ComparisonExpr{Operator: NotIn, Left: $1, Right: $4}
  }
| a_expr subquery_op sub_type select_with_parens %prec CONCAT {}
| a_expr subquery_op sub_type '(' a_expr ')' %prec CONCAT {}
| UNIQUE select_with_parens {}
| a_expr IS DOCUMENT %prec IS {}
| a_expr IS NOT DOCUMENT %prec IS {}

// Restricted expressions
// 
// b_expr is a subset of the complete expression syntax defined by a_expr.
// 
// Presently, AND, NOT, IS, and IN are the a_expr keywords that would cause
// trouble in the places where b_expr is used. For simplicity, we just
// eliminate all the boolean-keyword-operator productions from b_expr.
b_expr:
  c_expr
| b_expr TYPECAST typename {}
| '+' b_expr %prec UMINUS
  {
    $$ = &UnaryExpr{Operator: UnaryPlus, Expr: $2}
  }
| '-' b_expr %prec UMINUS
  {
    $$ = &UnaryExpr{Operator: UnaryMinus, Expr: $2}
  }
| '~' b_expr %prec UMINUS
  {
    $$ = &UnaryExpr{Operator: UnaryComplement, Expr: $2}
  }
| b_expr '+' b_expr
  {
    $$ = &BinaryExpr{Operator: Plus, Left: $1, Right: $3}
  }
| b_expr '-' b_expr
  {
    $$ = &BinaryExpr{Operator: Minus, Left: $1, Right: $3}
  }
| b_expr '*' b_expr
  {
    $$ = &BinaryExpr{Operator: Mult, Left: $1, Right: $3}
  }
| b_expr '/' b_expr
  {
    $$ = &BinaryExpr{Operator: Div, Left: $1, Right: $3}
  }
| b_expr '%' b_expr
  {
    $$ = &BinaryExpr{Operator: Mod, Left: $1, Right: $3}
  }
| b_expr '^' b_expr
  {
    $$ = &BinaryExpr{Operator: Exp, Left: $1, Right: $3}
  }
| b_expr '&' b_expr
  {
    $$ = &BinaryExpr{Operator: Bitand, Left: $1, Right: $3}
  }
| b_expr '|' b_expr
  {
    $$ = &BinaryExpr{Operator: Bitor, Left: $1, Right: $3}
  }
| b_expr '#' b_expr
  {
    $$ = &BinaryExpr{Operator: Bitxor, Left: $1, Right: $3}
  }
| b_expr '<' b_expr
  {
    $$ = &ComparisonExpr{Operator: LT, Left: $1, Right: $3}
  }
| b_expr '>' b_expr
  {
    $$ = &ComparisonExpr{Operator: GT, Left: $1, Right: $3}
  }
| b_expr '=' b_expr
  {
    $$ = &ComparisonExpr{Operator: EQ, Left: $1, Right: $3}
  }
| b_expr CONCAT b_expr
  {
    $$ = &BinaryExpr{Operator: Concat, Left: $1, Right: $3}
  }
| b_expr LESS_EQUALS b_expr
  {
    $$ = &ComparisonExpr{Operator: LE, Left: $1, Right: $3}
  }
| b_expr GREATER_EQUALS b_expr
  {
    $$ = &ComparisonExpr{Operator: GE, Left: $1, Right: $3}
  }
| b_expr NOT_EQUALS b_expr
  {
    $$ = &ComparisonExpr{Operator: NE, Left: $1, Right: $3}
  }
| b_expr IS DISTINCT FROM b_expr %prec IS {}
| b_expr IS NOT DISTINCT FROM b_expr %prec IS {}
| b_expr IS OF '(' type_list ')' %prec IS {}
| b_expr IS NOT OF '(' type_list ')' %prec IS {}
| b_expr IS DOCUMENT %prec IS {}
| b_expr IS NOT DOCUMENT %prec IS {}

// Productions that can be used in both a_expr and b_expr.
// 
// Note: productions that refer recursively to a_expr or b_expr mostly cannot
// appear here. However, it's OK to refer to a_exprs that occur inside
// parentheses, such as function arguments; that cannot introduce ambiguity to
// the b_expr syntax.
c_expr:
  columnref
| a_expr_const
| PARAM opt_indirection
  {
    $$ = ValArg($1)
  }
| '(' a_expr ')' opt_indirection
  {
    $$ = &ParenExpr{Expr: $2}
  }
| case_expr
| func_expr {}
| select_with_parens %prec UMINUS
  {
    $$ = &Subquery{Select: $1.(SelectStatement)}
  }
| select_with_parens indirection
  {
    $$ = &Subquery{Select: $1.(SelectStatement)}
  }
| EXISTS select_with_parens
  {
    $$ = &ExistsExpr{Subquery: &Subquery{Select: $2.(SelectStatement)}}
  }
| ARRAY select_with_parens {}
| ARRAY array_expr {}
| explicit_row {}
| implicit_row {}
| GROUPING '(' expr_list ')' {}

func_application:
  func_name '(' ')' {}
| func_name '(' func_arg_list opt_sort_clause ')' {}
| func_name '(' VARIADIC func_arg_expr opt_sort_clause ')' {}
| func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')' {}
| func_name '(' ALL func_arg_list opt_sort_clause ')' {}
| func_name '(' DISTINCT func_arg_list opt_sort_clause ')' {}
| func_name '(' '*' ')' {}

// func_expr and its cousin func_expr_windowless are split out from c_expr just
// so that we have classifications for "everything that is a function call or
// looks like one". This isn't very important, but it saves us having to
// document which variants are legal in places like "FROM function()" or the
// backwards-compatible functional-index syntax for CREATE INDEX. (Note that
// many of the special SQL functions wouldn't actually make any sense as
// functional index entries, but we ignore that consideration here.)
func_expr:
  func_application within_group_clause filter_clause over_clause {}
| func_expr_common_subexpr {}

// As func_expr but does not accept WINDOW functions directly (but they can
// still be contained in arguments for functions etc). Use this when window
// expressions are not allowed, where needed to disambiguate the grammar
// (e.g. in CREATE INDEX).
func_expr_windowless:
  func_application {}
| func_expr_common_subexpr {}

// Special expressions that are considered to be functions.
func_expr_common_subexpr:
  COLLATION FOR '(' a_expr ')' {}
| CURRENT_DATE {}
| CURRENT_TIME {}
| CURRENT_TIME '(' iconst ')' {}
| CURRENT_TIMESTAMP {}
| CURRENT_TIMESTAMP '(' iconst ')' {}
| LOCALTIME {}
| LOCALTIME '(' iconst ')' {}
| LOCALTIMESTAMP {}
| LOCALTIMESTAMP '(' iconst ')' {}
| CURRENT_ROLE {}
| CURRENT_USER {}
| SESSION_USER {}
| USER {}
| CURRENT_CATALOG {}
| CURRENT_SCHEMA {}
| CAST '(' a_expr AS typename ')' {}
| EXTRACT '(' extract_list ')' {}
| OVERLAY '(' overlay_list ')' {}
| POSITION '(' position_list ')' {}
| SUBSTRING '(' substr_list ')' {}
| TREAT '(' a_expr AS typename ')' {}
| TRIM '(' BOTH trim_list ')' {}
| TRIM '(' LEADING trim_list ')' {}
| TRIM '(' TRAILING trim_list ')' {}
| TRIM '(' trim_list ')' {}
| NULLIF '(' a_expr ',' a_expr ')' {}
| COALESCE '(' expr_list ')' {}
| GREATEST '(' expr_list ')' {}
| LEAST '(' expr_list ')' {}

// Aggregate decoration clauses
within_group_clause:
  WITHIN GROUP '(' sort_clause ')' {}
| /* EMPTY */ {}

filter_clause:
  FILTER '(' WHERE a_expr ')' {}
| /* EMPTY */ {}

// Window Definitions
window_clause:
  WINDOW window_definition_list {}
| /* EMPTY */ {}

window_definition_list:
  window_definition {}
| window_definition_list ',' window_definition {}

window_definition:
  col_id AS window_specification {}

over_clause:
  OVER window_specification {}
| OVER col_id {}
| /* EMPTY */ {}

window_specification:
  '(' opt_existing_window_name opt_partition_clause
    opt_sort_clause opt_frame_clause ')' {}

// If we see PARTITION, RANGE, or ROWS as the first token after the '(' of a
// window_specification, we want the assumption to be that there is no
// existing_window_name; but those keywords are unreserved and so could be
// col_ids. We fix this by making them have the same precedence as IDENT and
// giving the empty production here a slightly higher precedence, so that the
// shift/reduce conflict is resolved in favor of reducing the rule. These
// keywords are thus precluded from being an existing_window_name but are not
// reserved for any other purpose.
opt_existing_window_name:
  col_id {}
| /* EMPTY */ %prec CONCAT {}

opt_partition_clause:
  PARTITION BY expr_list {}
| /* EMPTY */ {}

// For frame clauses, we return a WindowDef, but only some fields are used:
// frameOptions, startOffset, and endOffset.
// 
// This is only a subset of the full SQL:2008 frame_clause grammar. We don't
// support <window frame exclusion> yet.
opt_frame_clause:
  RANGE frame_extent {}
| ROWS frame_extent {}
| /* EMPTY */ {}

frame_extent:
  frame_bound {}
| BETWEEN frame_bound AND frame_bound {}

// This is used for both frame start and frame end, with output set up on the
// assumption it's frame start; the frame_extent productions must reject
// invalid cases.
frame_bound:
  UNBOUNDED PRECEDING {}
| UNBOUNDED FOLLOWING {}
| CURRENT ROW {}
| a_expr PRECEDING {}
| a_expr FOLLOWING {}

// Supporting nonterminals for expressions.

// Explicit row production.
// 
// SQL99 allows an optional ROW keyword, so we can now do single-element rows
// without conflicting with the parenthesized a_expr production. Without the
// ROW keyword, there must be more than one a_expr inside the parens.
row:
  ROW '(' expr_list ')' {}
| ROW '(' ')' {}
| '(' expr_list ',' a_expr ')' {}

explicit_row:
  ROW '(' expr_list ')' {}
| ROW '(' ')' {}

implicit_row:
  '(' expr_list ',' a_expr ')' {}

sub_type:
  ANY {}
| SOME {}
| ALL {}

math_op:
  '+' {}
| '-' {}
| '*' {}
| '/' {}
| '%' {}
| '&' {}
| '|' {}
| '^' {}
| '#' {}
| '<' {}
| '>' {}
| '=' {}
| CONCAT {}
| LESS_EQUALS {}
| GREATER_EQUALS {}
| NOT_EQUALS {}

subquery_op:
  math_op {}
| LIKE {}
| NOT_LA LIKE {}
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
    $$ = Exprs{$1}
  }
| expr_list ',' a_expr
  {
    $$ = append($1, $3)
  }

// function arguments can have names
func_arg_list:
  func_arg_expr {}
| func_arg_list ',' func_arg_expr {}

func_arg_expr:
  a_expr {}
| param_name COLON_EQUALS a_expr {}
| param_name EQUALS_GREATER a_expr {}

type_list:
  typename {}
| type_list ',' typename {}

array_expr:
  '[' expr_list ']' {}
| '[' array_expr_list ']' {}
| '[' ']' {}

array_expr_list:
  array_expr {}
| array_expr_list ',' array_expr {}

extract_list:
  extract_arg FROM a_expr {}
| /* EMPTY */ {}

// Allow delimited string sconst in extract_arg as an SQL extension.
// - thomas 2001-04-12
extract_arg:
  IDENT {}
| YEAR {}
| MONTH {}
| DAY {}
| HOUR {}
| MINUTE {}
| SECOND {}
| sconst {}

// OVERLAY() arguments
// SQL99 defines the OVERLAY() function:
//   - overlay(text placing text from int for int)
//   - overlay(text placing text from int)
// and similarly for binary strings
overlay_list:
  a_expr overlay_placing substr_from substr_for {}
| a_expr overlay_placing substr_from {}

overlay_placing:
  PLACING a_expr {}

// position_list uses b_expr not a_expr to avoid conflict with general IN
position_list:
  b_expr IN b_expr {}
| /* EMPTY */ {}

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
  a_expr substr_from substr_for {}
| a_expr substr_for substr_from {}
| a_expr substr_from {}
| a_expr substr_for {}
| expr_list {}
| /* EMPTY */ {}

substr_from:
  FROM a_expr {}

substr_for:
  FOR a_expr {}

trim_list:
  a_expr FROM expr_list {}
| FROM expr_list {}
| expr_list {}

in_expr:
  select_with_parens
  {
    $$ = &Subquery{Select: $1.(SelectStatement)}
  }
| '(' expr_list ')'
  {
    $$ = Tuple($2)
  }

// Define SQL-style CASE clause.
// - Full specification
//      CASE WHEN a = b THEN c ... ELSE d END
// - Implicit argument
//      CASE a WHEN b THEN c ... ELSE d END
case_expr:
  CASE case_arg when_clause_list case_default END
  {
    $$ = &CaseExpr{Expr: $2, Whens: $3, Else: $4}
  }

when_clause_list:
  // There must be at least one
  when_clause
  {
    $$ = []*When{$1}
  }
| when_clause_list when_clause
  {
    $$ = append($1, $2)
  }

when_clause:
  WHEN a_expr THEN a_expr
  {
    $$ = &When{Cond: $2, Val: $4}
  }

case_default:
  ELSE a_expr
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = nil
  }

case_arg:
  a_expr
| /* EMPTY */
  {
    $$ = nil
  }

columnref:
  col_id
  {
    $$ = QualifiedName{$1}
  }
| col_id indirection
  {
    $$ = append(QualifiedName{$1}, $2...)
  }

indirection_elem:
  '.' attr_name
  {
    $$ = $2
  }
| '.' '*'
  {
    $$ = "*"
  }
| '[' a_expr ']'
  {
    $$ = ""
  }
| '[' a_expr ':' a_expr ']'
  {
    $$ = ""
  }

indirection:
  indirection_elem
  {
    $$ = []string{$1}
  }
| indirection indirection_elem
  {
    $$ = append($1, $2)
  }

opt_indirection:
  /* EMPTY */
  {
    $$ = nil
  }
| opt_indirection indirection_elem
  {
    $$ = append($1, $2)
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
    $$ = nil
  }

ctext_expr_list:
  ctext_expr
  {
    $$ = []Expr{$1}
  }
| ctext_expr_list ',' ctext_expr
  {
    $$ = append($1, $3)
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
    $$ = nil
  }

target_list:
  target_elem
  {
    $$ = SelectExprs{$1}
  }
| target_list ',' target_elem
  {
    $$ = append($1, $3)
  }

target_elem:
  a_expr AS col_label
  {
    $$ = &NonStarExpr{Expr: $1, As: $3}
  }
  // We support omitting AS only for column labels that aren't any known
  // keyword. There is an ambiguity against postfix operators: is "a ! b" an
  // infix expression, or a postfix expression and a column label?  We prefer
  // to resolve this as an infix expression, which we accomplish by assigning
  // IDENT a precedence higher than POSTFIXOP.
| a_expr IDENT
  {
    $$ = &NonStarExpr{Expr: $1, As: $2}
  }
| a_expr
  {
    $$ = &NonStarExpr{Expr: $1}
  }
| '*'
  {
    $$ = &StarExpr{}
  }

// Names and constants.

qualified_name_list:
  qualified_name {}
| qualified_name_list ',' qualified_name {}

// The production for a qualified relation name has to exactly match the
// production for a qualified func_name, because in a FROM clause we cannot
// tell which we are parsing until we see what comes after it ('(' for a
// func_name, something else for a relation). Therefore we allow 'indirection'
// which may contain subscripts, and reject that case in the C code.
qualified_name:
  col_id
  {
    $$ = []string{$1}
  }
| col_id indirection
  {
    $$ = append([]string{$1}, $2...)
  }

name_list:
  name
  {
    $$ = []string{$1}
  }
| name_list ',' name
  {
    $$ = append($1, $3)
  }

name:
  col_id

database_name:
  col_id

access_method:
  col_id {}

attr_name:
  col_label

index_name:
  col_id {}

// The production for a qualified func_name has to exactly match the production
// for a qualified columnref, because we cannot tell which we are parsing until
// we see what comes after it ('(' or sconst for a func_name, anything else for
// a columnref). Therefore we allow 'indirection' which may contain
// subscripts, and reject that case in the C code. (If we ever implement
// SQL99-like methods, such syntax may actually become legal!)
func_name:
  type_function_name {}
| col_id indirection {}

// Constants
a_expr_const:
  iconst
  {
    $$ = IntVal($1)
  }
| FCONST
  {
    $$ = NumVal($1)
  }
| sconst
  {
    // TODO(pmattis): string literal
    $$ = StrVal($1)
  }
| BCONST
  {
    // TODO(pmattis): bit literal.
    $$ = StrVal($1)
  }
| XCONST
  {
    // TODO(pmattis): hex literal.
    $$ = StrVal($1)
  }
| func_name sconst {}
| func_name '(' func_arg_list opt_sort_clause ')' sconst {}
| const_typename sconst {}
| const_interval sconst opt_interval {}
| const_interval '(' iconst ')' sconst {}
| TRUE
  {
    $$ = BoolVal(true)
  }
| FALSE
  {
    $$ = BoolVal(false)
  }
| NULL
  {
    $$ = NullVal{}
  }

iconst:
  ICONST

sconst:
  SCONST

signed_iconst:
  iconst
| '+' iconst
  {
    $$ = +$2
  }
| '-' iconst
  {
    $$ = -$2
  }

// role_spec:
//   non_reserved_word {}
// | CURRENT_USER {}
// | SESSION_USER {}

// Name classification hierarchy.
// 
// IDENT is the lexeme returned by the lexer for identifiers that match no
// known keyword. In most cases, we can accept certain keywords as names, not
// only IDENTs. We prefer to accept as many such keywords as possible to
// minimize the impact of "reserved words" on programmers. So, we divide names
// into several possible classes. The classification is chosen in part to make
// keywords acceptable as names wherever possible.

// Column identifier --- names that can be column, table, etc names.
col_id:
  IDENT
| unreserved_keyword
| col_name_keyword

// Type/function identifier --- names that can be type or function names.
type_function_name:
  IDENT
| unreserved_keyword
| type_func_name_keyword

// Any not-fully-reserved word --- these names can be, eg, role names.
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
  ABORT
| ABSOLUTE
| ACCESS
| ACTION
| ADD
| ADMIN
| AFTER
| AGGREGATE
| ALSO
| ALTER
| ALWAYS
| ASSERTION
| ASSIGNMENT
| AT
| ATTRIBUTE
| BACKWARD
| BEFORE
| BEGIN
| BY
| CACHE
| CALLED
| CASCADE
| CASCADED
| CATALOG
| CHAIN
| CHARACTERISTICS
| CHECKPOINT
| CLASS
| CLOSE
| CLUSTER
| COLUMNS
| COMMENT
| COMMENTS
| COMMIT
| COMMITTED
| CONFIGURATION
| CONFLICT
| CONNECTION
| CONSTRAINTS
| CONTENT
| CONTINUE
| CONVERSION
| COPY
| COST
| CSV
| CUBE
| CURRENT
| CURSOR
| CYCLE
| DATA
| DATABASE
| DATABASES
| DAY
| DEALLOCATE
| DECLARE
| DEFAULTS
| DEFERRED
| DEFINER
| DELETE
| DELIMITER
| DELIMITERS
| DICTIONARY
| DISABLE
| DISCARD
| DOCUMENT
| DOMAIN
| DOUBLE
| DROP
| EACH
| ENABLE
| ENCODING
| ENCRYPTED
| ENUM
| ESCAPE
| EVENT
| EXCLUDE
| EXCLUDING
| EXCLUSIVE
| EXECUTE
| EXPLAIN
| EXTENSION
| EXTERNAL
| FAMILY
| FILTER
| FIRST
| FOLLOWING
| FORCE
| FORWARD
| FUNCTION
| FUNCTIONS
| GLOBAL
| GRANTED
| HANDLER
| HEADER
| HOLD
| HOUR
| IDENTITY
| IF
| IMMEDIATE
| IMMUTABLE
| IMPLICIT
| IMPORT
| INCLUDING
| INCREMENT
| INDEX
| INDEXES
| INHERIT
| INHERITS
| INLINE
| INPUT
| INSENSITIVE
| INSERT
| INSTEAD
| INVOKER
| ISOLATION
| KEY
| LABEL
| LANGUAGE
| LARGE
| LAST
| LEAKPROOF
| LEVEL
| LISTEN
| LOAD
| LOCAL
| LOCATION
| LOCK
| LOCKED
| LOGGED
| MAPPING
| MATCH
| MATERIALIZED
| MAXVALUE
| MINUTE
| MINVALUE
| MODE
| MONTH
| MOVE
| NAME
| NAMES
| NEXT
| NO
| NOTHING
| NOTIFY
| NOWAIT
| NULLS
| OBJECT
| OF
| OFF
| OIDS
| OPTION
| OPTIONS
| ORDINALITY
| OVER
| OWNED
| OWNER
| PARSER
| PARTIAL
| PARTITION
| PASSING
| PASSWORD
| PLANS
| POLICY
| PRECEDING
| PREPARE
| PREPARED
| PRESERVE
| PRIOR
| PRIVILEGES
| PROCEDURAL
| PROCEDURE
| PROGRAM
| QUOTE
| RANGE
| READ
| REASSIGN
| RECHECK
| RECURSIVE
| REF
| REFRESH
| REINDEX
| RELATIVE
| RELEASE
| RENAME
| REPEATABLE
| REPLACE
| REPLICA
| RESET
| RESTART
| RESTRICT
| RETURNS
| REVOKE
| ROLE
| ROLLBACK
| ROLLUP
| ROWS
| RULE
| SAVEPOINT
| SCHEMA
| SCROLL
| SEARCH
| SECOND
| SECURITY
| SEQUENCE
| SEQUENCES
| SERIALIZABLE
| SERVER
| SESSION
| SET
| SETS
| SHARE
| SHOW
| SIMPLE
| SKIP
| SNAPSHOT
| SQL
| STABLE
| STANDALONE
| START
| STATEMENT
| STATISTICS
| STDIN
| STDOUT
| STORAGE
| STRICT
| STRIP
| SYSID
| SYSTEM
| TABLES
| TABLESPACE
| TEMP
| TEMPLATE
| TEMPORARY
| TEXT
| TRANSACTION
| TRANSFORM
| TRIGGER
| TRUNCATE
| TRUSTED
| TYPE
| TYPES
| UNBOUNDED
| UNCOMMITTED
| UNENCRYPTED
| UNKNOWN
| UNLISTEN
| UNLOGGED
| UNTIL
| UPDATE
| VACUUM
| VALID
| VALIDATE
| VALIDATOR
| VALUE
| VARYING
| VERSION
| VIEW
| VIEWS
| VOLATILE
| WHITESPACE
| WITHIN
| WITHOUT
| WORK
| WRAPPER
| WRITE
| YEAR
| YES
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
| BOOLEAN
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
| INOUT
| INT
| INTEGER
| INTERVAL
| LEAST
| NATIONAL
| NCHAR
| NONE
| NULLIF
| NUMERIC
| OUT
| OVERLAY
| POSITION
| PRECISION
| REAL
| ROW
| SETOF
| SMALLINT
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
  AUTHORIZATION
| BINARY
| COLLATION
| CONCURRENTLY
| CROSS
| CURRENT_SCHEMA
| FREEZE
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
| TABLESAMPLE
| VERBOSE

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
