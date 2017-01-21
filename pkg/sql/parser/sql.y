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

import (
    "fmt"

    "go/constant"
    "go/token"

    "github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// MaxUint is the maximum value of an uint.
const MaxUint = ^uint(0)
// MaxInt is the maximum value of an int.
const MaxInt = int(MaxUint >> 1)

func unimplemented(sqllex sqlLexer) int {
    sqllex.Error("unimplemented")
    return 1
}

func unimplementedWithIssue(sqllex sqlLexer, issue int) int {
    sqllex.Error(fmt.Sprintf("unimplemented "+
			     "(see issue https://github.com/cockroachdb/cockroach/issues/%d)",
			     issue))
    return 1
}
%}

%{
// sqlSymUnion represents a union of types, providing accessor methods
// to retrieve the underlying type stored in the union's empty interface.
// The purpose of the sqlSymUnion struct is to reduce the memory footprint of
// the sqlSymType because only one value (of a variety of types) is ever needed
// to be stored in the union field at a time.
//
// By using an empty interface, we lose the type checking previously provided
// by yacc and the Go compiler when dealing with union values. Instead, runtime
// type assertions must be relied upon in the methods below, and as such, the
// parser should be thoroughly tested whenever new syntax is added.
//
// It is important to note that when assigning values to sqlSymUnion.val, all
// nil values should be typed so that they are stored as nil instances in the
// empty interface, instead of setting the empty interface to nil. This means
// that:
//     $$ = []String(nil)
// should be used, instead of:
//     $$ = nil
// to assign a nil string slice to the union.
type sqlSymUnion struct {
    val interface{}
}

// The following accessor methods come in three forms, depending on the
// type of the value being accessed and whether a nil value is admissible
// for the corresponding grammar rule.
// - Values and pointers are directly type asserted from the empty
//   interface, regardless of whether a nil value is admissible or
//   not. A panic occurs if the type assertion is incorrect; no panic occurs
//   if a nil is not expected but present. (TODO(knz): split this category of
//   accessor in two; with one checking for unexpected nils.)
//   Examples: bool(), tableWithIdx().
//
// - Interfaces where a nil is admissible are handled differently
//   because a nil instance of an interface inserted into the empty interface
//   becomes a nil instance of the empty interface and therefore will fail a
//   direct type assertion. Instead, a guarded type assertion must be used,
//   which returns nil if the type assertion fails.
//   Examples: expr(), stmt().
//
// - Interfaces where a nil is not admissible are implemented as a direct
//   type assertion, which causes a panic to occur if an unexpected nil
//   is encountered.
//   Examples: namePart(), tblDef().
//
func (u *sqlSymUnion) numVal() *NumVal {
    return u.val.(*NumVal)
}
func (u *sqlSymUnion) strVal() *StrVal {
    if stmt, ok := u.val.(*StrVal); ok {
        return stmt
    }
    return nil
}
func (u *sqlSymUnion) bool() bool {
    return u.val.(bool)
}
func (u *sqlSymUnion) strPtr() *string {
    return u.val.(*string)
}
func (u *sqlSymUnion) strs() []string {
    return u.val.([]string)
}
func (u *sqlSymUnion) tableWithIdx() *TableNameWithIndex {
    return u.val.(*TableNameWithIndex)
}
func (u *sqlSymUnion) tableWithIdxList() TableNameWithIndexList {
    return u.val.(TableNameWithIndexList)
}
func (u *sqlSymUnion) namePart() NamePart {
    return u.val.(NamePart)
}
func (u *sqlSymUnion) nameList() NameList {
    return u.val.(NameList)
}
func (u *sqlSymUnion) unresolvedName() UnresolvedName {
    return u.val.(UnresolvedName)
}
func (u *sqlSymUnion) unresolvedNames() UnresolvedNames {
    return u.val.(UnresolvedNames)
}
func (u *sqlSymUnion) functionReference() FunctionReference {
    return u.val.(FunctionReference)
}
func (u *sqlSymUnion) resolvableFunctionReference() ResolvableFunctionReference {
    return ResolvableFunctionReference{u.val.(FunctionReference)}
}
func (u *sqlSymUnion) normalizableTableName() NormalizableTableName {
    return NormalizableTableName{u.val.(TableNameReference)}
}
func (u *sqlSymUnion) newNormalizableTableName() *NormalizableTableName {
    return &NormalizableTableName{u.val.(TableNameReference)}
}
func (u *sqlSymUnion) tablePatterns() TablePatterns {
    return u.val.(TablePatterns)
}
func (u *sqlSymUnion) tableNameReferences() TableNameReferences {
    return u.val.(TableNameReferences)
}
func (u *sqlSymUnion) indexHints() *IndexHints {
    return u.val.(*IndexHints)
}
func (u *sqlSymUnion) stmt() Statement {
    if stmt, ok := u.val.(Statement); ok {
        return stmt
    }
    return nil
}
func (u *sqlSymUnion) stmts() []Statement {
    return u.val.([]Statement)
}
func (u *sqlSymUnion) slct() *Select {
    return u.val.(*Select)
}
func (u *sqlSymUnion) selectStmt() SelectStatement {
    return u.val.(SelectStatement)
}
func (u *sqlSymUnion) colDef() *ColumnTableDef {
    return u.val.(*ColumnTableDef)
}
func (u *sqlSymUnion) constraintDef() ConstraintTableDef {
    return u.val.(ConstraintTableDef)
}
func (u *sqlSymUnion) tblDef() TableDef {
    return u.val.(TableDef)
}
func (u *sqlSymUnion) tblDefs() TableDefs {
    return u.val.(TableDefs)
}
func (u *sqlSymUnion) colQual() NamedColumnQualification {
    return u.val.(NamedColumnQualification)
}
func (u *sqlSymUnion) colQualElem() ColumnQualification {
    return u.val.(ColumnQualification)
}
func (u *sqlSymUnion) colQuals() []NamedColumnQualification {
    return u.val.([]NamedColumnQualification)
}
func (u *sqlSymUnion) colType() ColumnType {
    if colType, ok := u.val.(ColumnType); ok {
        return colType
    }
    return nil
}
func (u *sqlSymUnion) castTargetType() CastTargetType {
    return u.val.(CastTargetType)
}
func (u *sqlSymUnion) colTypes() []ColumnType {
    return u.val.([]ColumnType)
}
func (u *sqlSymUnion) expr() Expr {
    if expr, ok := u.val.(Expr); ok {
        return expr
    }
    return nil
}
func (u *sqlSymUnion) exprs() Exprs {
    return u.val.(Exprs)
}
func (u *sqlSymUnion) selExpr() SelectExpr {
    return u.val.(SelectExpr)
}
func (u *sqlSymUnion) selExprs() SelectExprs {
    return u.val.(SelectExprs)
}
func (u *sqlSymUnion) retExprs() ReturningExprs {
    return ReturningExprs(u.val.(SelectExprs))
}
func (u *sqlSymUnion) aliasClause() AliasClause {
    return u.val.(AliasClause)
}
func (u *sqlSymUnion) asOfClause() AsOfClause {
    return u.val.(AsOfClause)
}
func (u *sqlSymUnion) tblExpr() TableExpr {
    return u.val.(TableExpr)
}
func (u *sqlSymUnion) tblExprs() TableExprs {
    return u.val.(TableExprs)
}
func (u *sqlSymUnion) from() *From {
    return u.val.(*From)
}
func (u *sqlSymUnion) joinCond() JoinCond {
    return u.val.(JoinCond)
}
func (u *sqlSymUnion) when() *When {
    return u.val.(*When)
}
func (u *sqlSymUnion) whens() []*When {
    return u.val.([]*When)
}
func (u *sqlSymUnion) updateExpr() *UpdateExpr {
    return u.val.(*UpdateExpr)
}
func (u *sqlSymUnion) updateExprs() UpdateExprs {
    return u.val.(UpdateExprs)
}
func (u *sqlSymUnion) limit() *Limit {
    return u.val.(*Limit)
}
func (u *sqlSymUnion) targetList() TargetList {
    return u.val.(TargetList)
}
func (u *sqlSymUnion) targetListPtr() *TargetList {
    return u.val.(*TargetList)
}
func (u *sqlSymUnion) privilegeType() privilege.Kind {
    return u.val.(privilege.Kind)
}
func (u *sqlSymUnion) privilegeList() privilege.List {
    return u.val.(privilege.List)
}
func (u *sqlSymUnion) onConflict() *OnConflict {
    return u.val.(*OnConflict)
}
func (u *sqlSymUnion) orderBy() OrderBy {
    return u.val.(OrderBy)
}
func (u *sqlSymUnion) order() *Order {
    return u.val.(*Order)
}
func (u *sqlSymUnion) orders() []*Order {
    return u.val.([]*Order)
}
func (u *sqlSymUnion) groupBy() GroupBy {
    return u.val.(GroupBy)
}
func (u *sqlSymUnion) dir() Direction {
    return u.val.(Direction)
}
func (u *sqlSymUnion) alterTableCmd() AlterTableCmd {
    return u.val.(AlterTableCmd)
}
func (u *sqlSymUnion) alterTableCmds() AlterTableCmds {
    return u.val.(AlterTableCmds)
}
func (u *sqlSymUnion) isoLevel() IsolationLevel {
    return u.val.(IsolationLevel)
}
func (u *sqlSymUnion) userPriority() UserPriority {
    return u.val.(UserPriority)
}
func (u *sqlSymUnion) idxElem() IndexElem {
    return u.val.(IndexElem)
}
func (u *sqlSymUnion) idxElems() IndexElemList {
    return u.val.(IndexElemList)
}
func (u *sqlSymUnion) dropBehavior() DropBehavior {
    return u.val.(DropBehavior)
}
func (u *sqlSymUnion) validationBehavior() ValidationBehavior {
    return u.val.(ValidationBehavior)
}
func (u *sqlSymUnion) interleave() *InterleaveDef {
    return u.val.(*InterleaveDef)
}
func (u *sqlSymUnion) windowDef() *WindowDef {
    return u.val.(*WindowDef)
}
func (u *sqlSymUnion) window() Window {
    return u.val.(Window)
}
func (u *sqlSymUnion) op() operator {
    return u.val.(operator)
}
func (u *sqlSymUnion) cmpOp() ComparisonOperator {
    return u.val.(ComparisonOperator)
}
func (u *sqlSymUnion) durationField() durationField {
    return u.val.(durationField)
}
func (u *sqlSymUnion) kvOption() KVOption {
    return u.val.(KVOption)
}
func (u *sqlSymUnion) kvOptions() []KVOption {
    if colType, ok := u.val.([]KVOption); ok {
        return colType
    }
    return nil
}

%}

%union {
  id             int
  pos            int
  empty          struct{}
  str            string
  union          sqlSymUnion
}

%type <[]Statement> stmt_block
%type <[]Statement> stmt_list
%type <Statement> stmt

%type <Statement> alter_table_stmt
%type <Statement> backup_stmt
%type <Statement> copy_from_stmt
%type <Statement> create_stmt
%type <Statement> create_database_stmt
%type <Statement> create_index_stmt
%type <Statement> create_table_stmt
%type <Statement> create_table_as_stmt
%type <Statement> create_user_stmt
%type <Statement> create_view_stmt
%type <Statement> delete_stmt
%type <Statement> drop_stmt
%type <Statement> explain_stmt
%type <Statement> explainable_stmt
%type <Statement> help_stmt
%type <Statement> prepare_stmt
%type <Statement> preparable_stmt
%type <Statement> execute_stmt
%type <Statement> deallocate_stmt
%type <Statement> grant_stmt
%type <Statement> insert_stmt
%type <Statement> release_stmt
%type <Statement> rename_stmt
%type <Statement> revoke_stmt
%type <*Select> select_stmt
%type <Statement> savepoint_stmt
%type <Statement> set_stmt
%type <Statement> show_stmt
%type <Statement> split_stmt
%type <Statement> transaction_stmt
%type <Statement> truncate_stmt
%type <Statement> update_stmt

%type <[]string> opt_incremental
%type <KVOption> kv_option
%type <[]KVOption> kv_option_list opt_with_options
%type <str> opt_equal_value

%type <*Select> select_no_parens
%type <SelectStatement> select_clause select_with_parens simple_select values_clause

%type <empty> alter_using
%type <Expr> alter_column_default
%type <Direction> opt_asc_desc

%type <AlterTableCmd> alter_table_cmd
%type <AlterTableCmds> alter_table_cmds

%type <empty> opt_collate_clause

%type <DropBehavior> opt_drop_behavior
%type <DropBehavior> opt_interleave_drop_behavior

%type <ValidationBehavior> opt_validate_behavior

%type <str> opt_template_clause opt_encoding_clause opt_lc_collate_clause opt_lc_ctype_clause
%type <*string> opt_password

%type <IsolationLevel> transaction_iso_level
%type <UserPriority>  transaction_user_priority

%type <str>   name opt_name opt_name_parens opt_to_savepoint
%type <str>   savepoint_name

%type <operator> subquery_op
%type <FunctionReference> func_name
%type <empty> opt_collate

%type <UnresolvedName> qualified_name
%type <UnresolvedName> table_pattern
%type <TableExpr> insert_target

%type <*TableNameWithIndex> table_name_with_index
%type <TableNameWithIndexList> table_name_with_index_list

%type <operator> math_op

%type <IsolationLevel> iso_level
%type <UserPriority> user_priority
%type <empty> opt_encoding

%type <TableDefs> opt_table_elem_list table_elem_list
%type <*InterleaveDef> opt_interleave
%type <empty> opt_all_clause
%type <bool> distinct_clause
%type <NameList> opt_column_list
%type <OrderBy> sort_clause opt_sort_clause
%type <[]*Order> sortby_list
%type <IndexElemList> index_params
%type <NameList> name_list opt_name_list
%type <Exprs> opt_array_bounds
%type <*From> from_clause update_from_clause
%type <TableExprs> from_list
%type <UnresolvedNames> qualified_name_list
%type <TablePatterns> table_pattern_list
%type <UnresolvedName> any_name
%type <TableNameReferences> table_name_list
%type <Exprs> expr_list
%type <UnresolvedName> attrs
%type <SelectExprs> target_list
%type <UpdateExprs> set_clause_list
%type <*UpdateExpr> set_clause multiple_set_clause
%type <UnresolvedName> indirection opt_indirection
%type <UnresolvedName> qname_indirection
%type <NamePart> name_indirection_elem
%type <Exprs> ctext_expr_list ctext_row
%type <GroupBy> group_clause
%type <*Limit> select_limit
%type <TableNameReferences> relation_expr_list
%type <ReturningExprs> returning_clause

%type <bool> all_or_distinct
%type <empty> join_outer
%type <JoinCond> join_qual
%type <str> join_type

%type <Exprs> extract_list
%type <Exprs> overlay_list
%type <Exprs> position_list
%type <Exprs> substr_list
%type <Exprs> trim_list
%type <Exprs> execute_param_clause
%type <durationField> opt_interval interval_second
%type <Expr> overlay_placing

%type <bool> opt_unique opt_column

%type <empty> opt_set_data

%type <*Limit> limit_clause offset_clause
%type <Expr>  select_limit_value
// %type <empty> opt_select_fetch_first_value
%type <empty> row_or_rows
// %type <empty> first_or_next

%type <Statement>  insert_rest
%type <NameList> opt_conf_expr
%type <*OnConflict> on_conflict

%type <Statement>  generic_set set_rest set_rest_more transaction_mode_list opt_transaction_mode_list set_exprs_internal

%type <NameList> opt_storing
%type <*ColumnTableDef> column_def
%type <TableDef> table_elem
%type <Expr>  where_clause
%type <NamePart> glob_indirection
%type <NamePart> name_indirection
%type <NamePart> indirection_elem
%type <Expr> opt_slice_bound
%type <*IndexHints> opt_index_hints
%type <*IndexHints> index_hints_param
%type <*IndexHints> index_hints_param_list
%type <Expr>  a_expr b_expr c_expr a_expr_const d_expr
%type <Expr>  substr_from substr_for
%type <Expr>  in_expr
%type <Expr>  having_clause
%type <Expr>  array_expr
%type <[]ColumnType> type_list prep_type_clause
%type <Exprs> array_expr_list
%type <Expr>  row explicit_row implicit_row
%type <Expr>  case_expr case_arg case_default
%type <*When>  when_clause
%type <[]*When> when_clause_list
%type <ComparisonOperator> sub_type
%type <Expr> ctext_expr
%type <Expr> numeric_only
%type <AliasClause> alias_clause opt_alias_clause
%type <bool> opt_ordinality
%type <*Order> sortby
%type <IndexElem> index_elem
%type <TableExpr> table_ref
%type <TableExpr> joined_table
%type <UnresolvedName> relation_expr
%type <TableExpr> relation_expr_opt_alias
%type <SelectExpr> target_elem
%type <*UpdateExpr> single_set_clause
%type <AsOfClause> opt_as_of_clause

%type <str> explain_option_name
%type <[]string> explain_option_list

%type <ColumnType> typename simple_typename const_typename
%type <ColumnType> numeric opt_numeric_modifiers
%type <*NumVal> opt_float
%type <ColumnType> character const_character
%type <ColumnType> character_with_length character_without_length
%type <ColumnType> const_datetime const_interval
%type <ColumnType> bit const_bit bit_with_length bit_without_length
%type <ColumnType> character_base
%type <CastTargetType> postgres_oid
%type <CastTargetType> cast_target
%type <str> extract_arg
%type <empty> opt_varying

%type <*NumVal>  signed_iconst
%type <Expr>  opt_boolean_or_string
%type <Exprs> var_list
%type <UnresolvedName> var_name
%type <str>   unrestricted_name type_function_name
%type <str>   non_reserved_word
%type <str>   non_reserved_word_or_sconst
%type <Expr>  var_value
%type <Expr>  zone_value
%type <[]string> string_list

%type <str>   unreserved_keyword type_func_name_keyword
%type <str>   col_name_keyword reserved_keyword

%type <ConstraintTableDef> table_constraint constraint_elem
%type <TableDef> index_def
%type <TableDef> family_def
%type <[]NamedColumnQualification> col_qual_list
%type <NamedColumnQualification> col_qualification
%type <ColumnQualification> col_qualification_elem
%type <empty> key_actions key_delete key_match key_update key_action

%type <Expr>  func_application func_expr_common_subexpr
%type <Expr>  func_expr func_expr_windowless
%type <empty> common_table_expr
%type <empty> with_clause opt_with opt_with_clause
%type <empty> cte_list

%type <empty> within_group_clause
%type <Expr> filter_clause
%type <Exprs> opt_partition_clause
%type <Window> window_clause window_definition_list
%type <*WindowDef> window_definition over_clause window_specification
%type <str> opt_existing_window_name
%type <empty> opt_frame_clause frame_extent frame_bound

%type <TargetList>    targets
%type <*TargetList> on_privilege_target_clause
%type <NameList>       grantee_list for_grantee_clause
%type <privilege.List> privileges privilege_list
%type <privilege.Kind> privilege

// Non-keyword token types. These are hard-wired into the "flex" lexer. They
// must be listed first so that their numeric codes do not depend on the set of
// keywords. PL/pgsql depends on this so that it can share the same lexer. If
// you add/change tokens here, fix PL/pgsql to match!
//
// DOT_DOT is unused in the core SQL grammar, and so will always provoke parse
// errors. It is needed by PL/pgsql.
%token <str>   IDENT SCONST BCONST
%token <*NumVal> ICONST FCONST
%token <str>   PLACEHOLDER
%token <str>   TYPECAST TYPEANNOTATE DOT_DOT
%token <str>   LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%token <str>   NOT_REGMATCH REGIMATCH NOT_REGIMATCH
%token <str>   ERROR

// If you want to make any keyword changes, update the keyword table in
// src/include/parser/kwlist.h and add new keywords to the appropriate one of
// the reserved-or-not-so-reserved keyword lists, below; search this file for
// "Keyword category lists".

// Ordinary key words in alphabetical order.
%token <str>   ACTION ADD
%token <str>   ALL ALTER ANALYSE ANALYZE AND ANY ANNOTATE_TYPE ARRAY AS ASC
%token <str>   ASYMMETRIC AT

%token <str>   BACKUP BEGIN BETWEEN BIGINT BIGSERIAL BIT
%token <str>   BLOB BOOL BOOLEAN BOTH BY BYTEA BYTES

%token <str>   CASCADE CASE CAST CHAR
%token <str>   CHARACTER CHARACTERISTICS CHECK
%token <str>   COALESCE COLLATE COLLATION COLUMN COLUMNS COMMIT
%token <str>   COMMITTED CONCAT CONFLICT CONSTRAINT CONSTRAINTS
%token <str>   COPY COVERING CREATE
%token <str>   CROSS CUBE CURRENT CURRENT_CATALOG CURRENT_DATE
%token <str>   CURRENT_ROLE CURRENT_TIME CURRENT_TIMESTAMP
%token <str>   CURRENT_USER CYCLE

%token <str>   DATA DATABASE DATABASES DATE DAY DEC DECIMAL DEFAULT
%token <str>   DEALLOCATE DEFERRABLE DELETE DESC
%token <str>   DISTINCT DO DOUBLE DROP

%token <str>   ELSE ENCODING END ESCAPE EXCEPT
%token <str>   EXISTS EXECUTE EXPLAIN EXTRACT EXTRACT_DURATION

%token <str>   FALSE FAMILY FETCH FILTER FIRST FLOAT FLOORDIV FOLLOWING FOR
%token <str>   FORCE_INDEX FOREIGN FROM FULL

%token <str>   GRANT GRANTS GREATEST GROUP GROUPING

%token <str>   HAVING HELP HIGH HOUR

%token <str>   INCREMENTAL IF IFNULL ILIKE IN INTERLEAVE
%token <str>   INDEX INDEXES INITIALLY
%token <str>   INNER INSERT INT INT8 INT64 INTEGER
%token <str>   INTERSECT INTERVAL INTO IS ISOLATION

%token <str>   JOIN

%token <str>   KEY KEYS

%token <str>   LATERAL LC_CTYPE LC_COLLATE
%token <str>   LEADING LEAST LEFT LEVEL LIKE LIMIT LOCAL
%token <str>   LOCALTIME LOCALTIMESTAMP LOW LSHIFT

%token <str>   MATCH MINUTE MONTH

%token <str>   NAME NAMES NATURAL NEXT NO NO_INDEX_JOIN NORMAL
%token <str>   NOT NOTHING NULL NULLIF
%token <str>   NULLS NUMERIC

%token <str>   OF OFF OFFSET OID ON ONLY OPTIONS OR
%token <str>   ORDER ORDINALITY OUT OUTER OVER OVERLAPS OVERLAY

%token <str>   PARENT PARTIAL PARTITION PASSWORD PLACING POSITION
%token <str>   PRECEDING PRECISION PREPARE PRIMARY PRIORITY

%token <str>   RANGE READ REAL RECURSIVE REF REFERENCES
%token <str>   REGCLASS REGPROC REGPROCEDURE REGNAMESPACE REGTYPE
%token <str>   RENAME REPEATABLE
%token <str>   RELEASE RESTORE RESTRICT RETURNING REVOKE RIGHT ROLLBACK ROLLUP
%token <str>   ROW ROWS RSHIFT

%token <str>   STATUS SAVEPOINT SEARCH SECOND SELECT
%token <str>   SERIAL SERIALIZABLE SESSION SESSION_USER SET SHOW
%token <str>   SIMILAR SIMPLE SMALLINT SMALLSERIAL SNAPSHOT SOME SPLIT SQL
%token <str>   START STDIN STRICT STRING STORING SUBSTRING
%token <str>   SYMMETRIC SYSTEM

%token <str>   TABLE TABLES TEMPLATE TEXT THEN
%token <str>   TIME TIMESTAMP TIMESTAMPTZ TO TRAILING TRANSACTION TREAT TRIM TRUE
%token <str>   TRUNCATE TYPE

%token <str>   UNBOUNDED UNCOMMITTED UNION UNIQUE UNKNOWN
%token <str>   UPDATE UPSERT USER USERS USING

%token <str>   VALID VALIDATE VALUE VALUES VARCHAR VARIADIC VIEW VARYING

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
%token     NOT_LA WITH_LA AS_LA

// Precedence: lowest to highest
%nonassoc  VALUES              // see value_clause
%nonassoc  SET                 // see relation_expr_opt_alias
%left      UNION EXCEPT
%left      INTERSECT
%left      OR
%left      AND
%right     NOT
%nonassoc  IS                  // IS sets precedence for IS NULL, etc
%nonassoc  '<' '>' '=' LESS_EQUALS GREATER_EQUALS NOT_EQUALS
%nonassoc  BETWEEN IN LIKE ILIKE SIMILAR NOT_REGMATCH REGIMATCH NOT_REGIMATCH NOT_LA
%nonassoc  ESCAPE              // ESCAPE must be just above LIKE/ILIKE/SIMILAR
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
%left      '*' '/' FLOORDIV '%'
// Unary Operators
%left      AT                // sets precedence for AT TIME ZONE
%left      COLLATE
%right     UMINUS
%left      '~'
%left      '[' ']'
%left      '(' ')'
%left      TYPEANNOTATE
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
    sqllex.(*Scanner).stmts = $1.stmts()
  }

stmt_list:
  stmt_list ';' stmt
  {
    if $3.stmt() != nil {
      $$.val = append($1.stmts(), $3.stmt())
    }
  }
| stmt
  {
    if $1.stmt() != nil {
      $$.val = []Statement{$1.stmt()}
    } else {
      $$.val = []Statement(nil)
    }
  }

stmt:
  alter_table_stmt
| backup_stmt
| copy_from_stmt
| create_stmt
| delete_stmt
| drop_stmt
| explain_stmt
| help_stmt
| prepare_stmt
| execute_stmt
| deallocate_stmt
| grant_stmt
| insert_stmt
| rename_stmt
| revoke_stmt
| savepoint_stmt
| select_stmt
  {
    $$.val = $1.slct()
  }
| set_stmt
| show_stmt
| split_stmt
| transaction_stmt
| release_stmt
| truncate_stmt
| update_stmt
| /* EMPTY */
  {
    $$.val = Statement(nil)
  }

alter_table_stmt:
  ALTER TABLE relation_expr alter_table_cmds
  {
    $$.val = &AlterTable{Table: $3.normalizableTableName(), IfExists: false, Cmds: $4.alterTableCmds()}
  }
| ALTER TABLE IF EXISTS relation_expr alter_table_cmds
  {
    $$.val = &AlterTable{Table: $5.normalizableTableName(), IfExists: true, Cmds: $6.alterTableCmds()}
  }

alter_table_cmds:
  alter_table_cmd
  {
    $$.val = AlterTableCmds{$1.alterTableCmd()}
  }
| alter_table_cmds ',' alter_table_cmd
  {
    $$.val = append($1.alterTableCmds(), $3.alterTableCmd())
  }

alter_table_cmd:
  // ALTER TABLE <name> ADD <coldef>
  ADD column_def
  {
    $$.val = &AlterTableAddColumn{columnKeyword: false, IfNotExists: false, ColumnDef: $2.colDef()}
  }
  // ALTER TABLE <name> ADD IF NOT EXISTS <coldef>
| ADD IF NOT EXISTS column_def
  {
    $$.val = &AlterTableAddColumn{columnKeyword: false, IfNotExists: true, ColumnDef: $5.colDef()}
  }
  // ALTER TABLE <name> ADD COLUMN <coldef>
| ADD COLUMN column_def
  {
    $$.val = &AlterTableAddColumn{columnKeyword: true, IfNotExists: false, ColumnDef: $3.colDef()}
  }
  // ALTER TABLE <name> ADD COLUMN IF NOT EXISTS <coldef>
| ADD COLUMN IF NOT EXISTS column_def
  {
    $$.val = &AlterTableAddColumn{columnKeyword: true, IfNotExists: true, ColumnDef: $6.colDef()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT}
| ALTER opt_column name alter_column_default
  {
    $$.val = &AlterTableSetDefault{columnKeyword: $2.bool(), Column: Name($3), Default: $4.expr()}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL
| ALTER opt_column name DROP NOT NULL
  {
    $$.val = &AlterTableDropNotNull{columnKeyword: $2.bool(), Column: Name($3)}
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL
| ALTER opt_column name SET NOT NULL { return unimplemented(sqllex) }
  // ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE]
| DROP opt_column IF EXISTS name opt_drop_behavior
  {
    $$.val = &AlterTableDropColumn{
      columnKeyword: $2.bool(),
      IfExists: true,
      Column: Name($5),
      DropBehavior: $6.dropBehavior(),
    }
  }
  // ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE]
| DROP opt_column name opt_drop_behavior
  {
    $$.val = &AlterTableDropColumn{
      columnKeyword: $2.bool(),
      IfExists: false,
      Column: Name($3),
      DropBehavior: $4.dropBehavior(),
    }
  }
  // ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
  //     [ USING <expression> ]
| ALTER opt_column name opt_set_data TYPE typename opt_collate_clause alter_using { return unimplemented(sqllex) }
  // ALTER TABLE <name> ADD CONSTRAINT ...
| ADD table_constraint opt_validate_behavior
  {
    $$.val = &AlterTableAddConstraint{
      ConstraintDef: $2.constraintDef(),
      ValidationBehavior: $3.validationBehavior(),
    }
  }
  // ALTER TABLE <name> ALTER CONSTRAINT ...
| ALTER CONSTRAINT name { return unimplemented(sqllex) }
  // ALTER TABLE <name> VALIDATE CONSTRAINT ...
| VALIDATE CONSTRAINT name
  {
    $$.val = &AlterTableValidateConstraint{
      Constraint: Name($3),
    }
  }
  // ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT IF EXISTS name opt_drop_behavior
  {
    $$.val = &AlterTableDropConstraint{
      IfExists: true,
      Constraint: Name($5),
      DropBehavior: $6.dropBehavior(),
    }
  }
  // ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE]
| DROP CONSTRAINT name opt_drop_behavior
  {
    $$.val = &AlterTableDropConstraint{
      IfExists: false,
      Constraint: Name($3),
      DropBehavior: $4.dropBehavior(),
    }
  }

alter_column_default:
  SET DEFAULT a_expr
  {
    $$.val = $3.expr()
  }
| DROP DEFAULT
  {
    $$.val = nil
  }

opt_drop_behavior:
  CASCADE
  {
    $$.val = DropCascade
  }
| RESTRICT
  {
    $$.val = DropRestrict
  }
| /* EMPTY */
  {
    $$.val = DropDefault
  }

opt_validate_behavior:
  NOT VALID
  {
    $$.val = ValidationSkip
  }
| /* EMPTY */
  {
    $$.val = ValidationDefault
  }

opt_collate_clause:
  COLLATE any_name { return unimplementedWithIssue(sqllex, 2473) }
| /* EMPTY */ {}

alter_using:
  USING a_expr { return unimplemented(sqllex) }
| /* EMPTY */ {}

backup_stmt:
  BACKUP targets TO non_reserved_word_or_sconst opt_as_of_clause opt_incremental opt_with_options
  {
    /* SKIP DOC */
    $$.val = &Backup{Targets: $2.targetList(), To: $4, IncrementalFrom: $6.strs(), AsOf: $5.asOfClause(), Options: $7.kvOptions()}
  }
| RESTORE targets FROM string_list opt_as_of_clause opt_with_options
  {
    /* SKIP DOC */
    $$.val = &Restore{Targets: $2.targetList(), From: $4.strs(), AsOf: $5.asOfClause(), Options: $6.kvOptions()}
  }

string_list:
  non_reserved_word_or_sconst
  {
    $$.val = []string{$1}
  }
| string_list ',' non_reserved_word_or_sconst
  {
    $$.val = append($1.strs(), $3)
  }

opt_incremental:
  INCREMENTAL FROM string_list
  {
    $$.val = $3.strs()
  }
| /* EMPTY */
  {
    $$.val = []string(nil)
  }

opt_equal_value:
  '=' SCONST
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = ""
  }

kv_option:
  SCONST opt_equal_value
  {
    $$.val = KVOption{Key: $1, Value: $2}
  }

kv_option_list:
  kv_option
  {
    $$.val = []KVOption{$1.kvOption()}
  }
|  kv_option_list ',' kv_option
  {
    $$.val = append($1.kvOptions(), $3.kvOption())
  }

opt_with_options:
  WITH OPTIONS '(' kv_option_list ')'
  {
    $$.val = $4.kvOptions()
  }
| /* EMPTY */ {}

copy_from_stmt:
  COPY qualified_name FROM STDIN
  {
    $$.val = &CopyFrom{Table: $2.normalizableTableName(), Stdin: true}
  }
| COPY qualified_name '(' ')' FROM STDIN
  {
    $$.val = &CopyFrom{Table: $2.normalizableTableName(), Stdin: true}
  }
| COPY qualified_name '(' qualified_name_list ')' FROM STDIN
  {
    $$.val = &CopyFrom{Table: $2.normalizableTableName(), Columns: $4.unresolvedNames(), Stdin: true}
  }

// CREATE [DATABASE|INDEX|TABLE|TABLE AS|VIEW]
create_stmt:
  create_database_stmt
| create_index_stmt
| create_table_stmt
| create_table_as_stmt
| create_user_stmt
| create_view_stmt

// DELETE FROM query
delete_stmt:
  opt_with_clause DELETE FROM relation_expr_opt_alias where_clause returning_clause
  {
    $$.val = &Delete{Table: $4.tblExpr(), Where: newWhere(astWhere, $5.expr()), Returning: $6.retExprs()}
  }

// DROP itemtype [ IF EXISTS ] itemname [, itemname ...] [ RESTRICT | CASCADE ]
drop_stmt:
  DROP DATABASE name
  {
    $$.val = &DropDatabase{Name: Name($3), IfExists: false}
  }
| DROP DATABASE IF EXISTS name
  {
    $$.val = &DropDatabase{Name: Name($5), IfExists: true}
  }
| DROP INDEX table_name_with_index_list opt_drop_behavior
  {
    $$.val = &DropIndex{
      IndexList: $3.tableWithIdxList(),
      IfExists: false,
      DropBehavior: $4.dropBehavior(),
    }
  }
| DROP INDEX IF EXISTS table_name_with_index_list opt_drop_behavior
  {
    $$.val = &DropIndex{
      IndexList: $5.tableWithIdxList(),
      IfExists: true,
      DropBehavior: $6.dropBehavior(),
    }
  }
| DROP TABLE table_name_list opt_drop_behavior
  {
    $$.val = &DropTable{Names: $3.tableNameReferences(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP TABLE IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &DropTable{Names: $5.tableNameReferences(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }
| DROP VIEW table_name_list opt_drop_behavior
  {
    $$.val = &DropView{Names: $3.tableNameReferences(), IfExists: false, DropBehavior: $4.dropBehavior()}
  }
| DROP VIEW IF EXISTS table_name_list opt_drop_behavior
  {
    $$.val = &DropView{Names: $5.tableNameReferences(), IfExists: true, DropBehavior: $6.dropBehavior()}
  }

table_name_list:
  any_name
  {
    $$.val = TableNameReferences{$1.unresolvedName()}
  }
| table_name_list ',' any_name
  {
    $$.val = append($1.tableNameReferences(), $3.unresolvedName())
  }

any_name:
  name
  {
    $$.val = UnresolvedName{Name($1)}
  }
| name attrs
  {
    $$.val = append(UnresolvedName{Name($1)}, $2.unresolvedName()...)
  }

attrs:
  '.' unrestricted_name
  {
    $$.val = UnresolvedName{Name($2)}
  }
| attrs '.' unrestricted_name
  {
    $$.val = append($1.unresolvedName(), Name($3))
  }

// EXPLAIN (options) query
explain_stmt:
  EXPLAIN explainable_stmt
  {
    $$.val = &Explain{Statement: $2.stmt()}
  }
| EXPLAIN '(' explain_option_list ')' explainable_stmt
  {
    $$.val = &Explain{Options: $3.strs(), Statement: $5.stmt()}
  }

explainable_stmt:
  select_stmt
  {
    $$.val = $1.slct()
  }
| create_stmt
| drop_stmt
| alter_table_stmt
| insert_stmt
| update_stmt
| delete_stmt
| show_stmt
| help_stmt
| split_stmt
| explain_stmt { /* SKIP DOC */ }

explain_option_list:
  explain_option_name
  {
    $$.val = []string{$1}
  }
| explain_option_list ',' explain_option_name
  {
    $$.val = append($1.strs(), $3)
  }

explain_option_name:
  non_reserved_word

// PREPARE <plan_name> [(args, ...)] AS <query>
prepare_stmt:
  PREPARE name prep_type_clause AS preparable_stmt
  {
    $$.val = &Prepare{
      Name: Name($2),
      Types: $3.colTypes(),
      Statement: $5.stmt(),
    }
  }

prep_type_clause:
  '(' type_list ')'
  {
    $$.val = $2.colTypes();
  }
| /* EMPTY */
  {
    $$.val = []ColumnType(nil)
  }

preparable_stmt:
  select_stmt
  {
    $$.val = $1.slct()
  }
| insert_stmt
| update_stmt
| delete_stmt

execute_stmt:
  // EXECUTE <plan_name> [(params, ...)]
  EXECUTE name execute_param_clause
  {
    $$.val = &Execute{
      Name: Name($2),
      Params: $3.exprs(),
    }
  }
  // CREATE TABLE <name> AS EXECUTE <plan_name> [(params, ...)]
// | CREATE opt_temp TABLE create_as_target AS EXECUTE name execute_param_clause opt_with_data { return unimplemented(sqllex) }

execute_param_clause:
  '(' expr_list ')'
  {
    $$.val = $2.exprs()
  }
| /* EMPTY */
  {
    $$.val = Exprs(nil)
  }

// DEALLOCATE [PREPARE] <plan_name>
deallocate_stmt:
  DEALLOCATE name
  {
    $$.val = &Deallocate{
      Name: Name($2),
    }
  }
| DEALLOCATE PREPARE name
  {
    $$.val = &Deallocate{
      Name: Name($3),
    }
  }
| DEALLOCATE ALL
  {
    $$.val = &Deallocate{}
  }
| DEALLOCATE PREPARE ALL
  {
    $$.val = &Deallocate{}
  }

// GRANT privileges ON targets TO grantee_list
grant_stmt:
  GRANT privileges ON targets TO grantee_list
  {
    $$.val = &Grant{Privileges: $2.privilegeList(), Grantees: $6.nameList(), Targets: $4.targetList()}
  }

// REVOKE privileges ON targets FROM grantee_list
revoke_stmt:
  REVOKE privileges ON targets FROM grantee_list
  {
    $$.val = &Revoke{Privileges: $2.privilegeList(), Grantees: $6.nameList(), Targets: $4.targetList()}
  }


targets:
  table_pattern_list
  {
    $$.val = TargetList{Tables: $1.tablePatterns()}
  }
| TABLE table_pattern_list
  {
    $$.val = TargetList{Tables: $2.tablePatterns()}
  }
|  DATABASE name_list
  {
    $$.val = TargetList{Databases: $2.nameList()}
  }

// ALL is always by itself.
privileges:
  ALL
  {
    $$.val = privilege.List{privilege.ALL}
  }
  | privilege_list { }

privilege_list:
  privilege
  {
    $$.val = privilege.List{$1.privilegeType()}
  }
  | privilege_list ',' privilege
  {
    $$.val = append($1.privilegeList(), $3.privilegeType())
  }

// This list must match the list of privileges in sql/privilege/privilege.go.
privilege:
  CREATE
  {
    $$.val = privilege.CREATE
  }
| DROP
  {
    $$.val = privilege.DROP
  }
| GRANT
  {
    $$.val = privilege.GRANT
  }
| SELECT
  {
    $$.val = privilege.SELECT
  }
| INSERT
  {
    $$.val = privilege.INSERT
  }
| DELETE
  {
    $$.val = privilege.DELETE
  }
| UPDATE
  {
    $$.val = privilege.UPDATE
  }

// TODO(marc): this should not be 'name', but should instead be a
// type just for usernames.
grantee_list:
  name
  {
    $$.val = NameList{Name($1)}
  }
| grantee_list ',' name
  {
    $$.val = append($1.nameList(), Name($3))
  }

// SET name TO 'var_value'
// SET TIME ZONE 'var_value'
set_stmt:
  SET set_rest
  {
    $$.val = $2.stmt()
  }
| SET LOCAL set_rest { return unimplemented(sqllex) }
| SET SESSION CHARACTERISTICS AS TRANSACTION transaction_iso_level
  {
    $$.val = &SetDefaultIsolation{Isolation: $6.isoLevel()}
  }
| SET SESSION set_rest
  {
    $$.val = $3.stmt()
  }
| set_exprs_internal { /* SKIP DOC */ }

set_exprs_internal:
  /* SET ROW serves to accelerate parser.parseExprs().
     It cannot be used by clients. */
  SET ROW '(' expr_list ')'
  {
    $$.val = &Set{Values: $4.exprs()}
  }

set_rest:
  TRANSACTION transaction_mode_list
  {
    $$.val = $2.stmt()
  }
| set_rest_more

transaction_mode_list:
  transaction_iso_level
  {
    $$.val = &SetTransaction{Isolation: $1.isoLevel(), UserPriority: UnspecifiedUserPriority}
  }
| transaction_user_priority
  {
    $$.val = &SetTransaction{Isolation: UnspecifiedIsolation, UserPriority: $1.userPriority()}
  }
| transaction_iso_level ',' transaction_user_priority
  {
    $$.val = &SetTransaction{Isolation: $1.isoLevel(), UserPriority: $3.userPriority()}
  }
| transaction_user_priority ',' transaction_iso_level
  {
    $$.val = &SetTransaction{Isolation: $3.isoLevel(), UserPriority: $1.userPriority()}
  }


transaction_user_priority:
  PRIORITY user_priority
  {
    $$.val = $2.userPriority()
  }

generic_set:
  var_name TO var_list
  {
    $$.val = &Set{Name: $1.unresolvedName(), Values: $3.exprs()}
  }
| var_name '=' var_list
  {
    $$.val = &Set{Name: $1.unresolvedName(), Values: $3.exprs()}
  }
| var_name TO DEFAULT
  {
    $$.val = &Set{Name: $1.unresolvedName()}
  }
| var_name '=' DEFAULT
  {
    $$.val = &Set{Name: $1.unresolvedName()}
  }

set_rest_more:
  // Generic SET syntaxes:
  generic_set
| var_name FROM CURRENT { return unimplemented(sqllex) }
  // Special syntaxes mandated by SQL standard:
| TIME ZONE zone_value
  {
    $$.val = &SetTimeZone{Value: $3.expr()}
  }
| NAMES opt_encoding { return unimplemented(sqllex) }

var_name:
  any_name

var_list:
  var_value
  {
    $$.val = Exprs{$1.expr()}
  }
| var_list ',' var_value
  {
    $$.val = append($1.exprs(), $3.expr())
  }

var_value:
  opt_boolean_or_string
| numeric_only
| PLACEHOLDER
  {
    $$.val = NewPlaceholder($1)
  }

iso_level:
  READ UNCOMMITTED
  {
    $$.val = SnapshotIsolation
  }
| READ COMMITTED
  {
    $$.val = SnapshotIsolation
  }
| SNAPSHOT
  {
    $$.val = SnapshotIsolation
  }
| REPEATABLE READ
  {
    $$.val = SerializableIsolation
  }
| SERIALIZABLE
  {
    $$.val = SerializableIsolation
  }

user_priority:
  LOW
  {
    $$.val = Low
  }
| NORMAL
  {
    $$.val = Normal
  }
| HIGH
  {
    $$.val = High
  }

opt_boolean_or_string:
  TRUE
  {
    $$.val = MakeDBool(true)
  }
| FALSE
  {
    $$.val = MakeDBool(false)
  }
| ON
  {
    $$.val = &StrVal{s: $1}
  }
  // OFF is also accepted as a boolean value, but is handled by the
  // non_reserved_word rule. The action for booleans and strings is the same,
  // so we don't need to distinguish them here.
| non_reserved_word_or_sconst
  {
    $$.val = &StrVal{s: $1}
  }

// Timezone values can be:
// - a string such as 'pst8pdt'
// - an identifier such as "pst8pdt"
// - an integer or floating point number
// - a time interval per SQL99
zone_value:
  SCONST
  {
    $$.val = &StrVal{s: $1}
  }
| IDENT
  {
    $$.val = &StrVal{s: $1}
  }
| const_interval SCONST opt_interval
  {
    var err error
    var d Datum
    if $3.val == nil {
      d, err = ParseDInterval($2)
    } else {
      d, err = ParseDIntervalWithField($2, $3.durationField())
    }
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = d
  }
| numeric_only
| DEFAULT
  {
    $$.val = &StrVal{s: $1}
  }
| LOCAL
  {
    $$.val = &StrVal{s: $1}
  }

opt_encoding:
  SCONST { return unimplemented(sqllex) }
| DEFAULT { return unimplemented(sqllex) }
| /* EMPTY */ {}

non_reserved_word_or_sconst:
  non_reserved_word
| SCONST

show_stmt:
  SHOW IDENT
  {
    $$.val = &Show{Name: $2}
  }
| SHOW ALL
  {
    $$.val = &Show{Name: $2}
  }
| SHOW DATABASE
  {
    $$.val = &Show{Name: $2}
  }
| SHOW COLUMNS FROM var_name
  {
    $$.val = &ShowColumns{Table: $4.normalizableTableName()}
  }
| SHOW DATABASES
  {
    $$.val = &ShowDatabases{}
  }
| SHOW GRANTS on_privilege_target_clause for_grantee_clause
  {
    $$.val = &ShowGrants{Targets: $3.targetListPtr(), Grantees: $4.nameList()}
  }
| SHOW INDEX FROM var_name
  {
    $$.val = &ShowIndex{Table: $4.normalizableTableName()}
  }
| SHOW INDEXES FROM var_name
  {
    $$.val = &ShowIndex{Table: $4.normalizableTableName()}
  }
| SHOW CONSTRAINT FROM var_name
  {
    $$.val = &ShowConstraints{Table: $4.normalizableTableName()}
  }
| SHOW CONSTRAINTS FROM var_name
  {
    $$.val = &ShowConstraints{Table: $4.normalizableTableName()}
  }
| SHOW KEYS FROM var_name
  {
    $$.val = &ShowIndex{Table: $4.normalizableTableName()}
  }
| SHOW TABLES FROM name
  {
    $$.val = &ShowTables{Database: Name($4)}
  }
| SHOW TABLES
  {
    $$.val = &ShowTables{}
  }
| SHOW TIME ZONE
  {
    $$.val = &Show{Name: "TIME ZONE"}
  }
| SHOW TRANSACTION ISOLATION LEVEL
  {
    $$.val = &Show{Name: "TRANSACTION ISOLATION LEVEL"}
  }
| SHOW TRANSACTION PRIORITY
  {
    $$.val = &Show{Name: "TRANSACTION PRIORITY"}
  }
| SHOW TRANSACTION STATUS
  {
    $$.val = &ShowTransactionStatus{}
  }
| SHOW CREATE TABLE var_name
  {
    $$.val = &ShowCreateTable{Table: $4.normalizableTableName()}
  }
| SHOW CREATE VIEW var_name
  {
    $$.val = &ShowCreateView{View: $4.normalizableTableName()}
  }
| SHOW USERS
  {
    $$.val = &ShowUsers{}
  }

help_stmt:
  HELP unrestricted_name
  {
    $$.val = &Help{Name: Name($2)}
  }

on_privilege_target_clause:
  ON targets
  {
    tmp := $2.targetList()
    $$.val = &tmp
  }
| /* EMPTY */
  {
    $$.val = (*TargetList)(nil)
  }

for_grantee_clause:
  FOR grantee_list
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = NameList(nil)
  }

split_stmt:
  ALTER TABLE qualified_name SPLIT AT '(' expr_list ')'
  {
    $$.val = &Split{Table: $3.normalizableTableName(), Exprs: $7.exprs()}
  }
| ALTER INDEX table_name_with_index SPLIT AT '(' expr_list ')'
  {
    $$.val = &Split{Index: $3.tableWithIdx(), Exprs: $7.exprs()}
  }

// CREATE TABLE relname
create_table_stmt:
  CREATE TABLE any_name '(' opt_table_elem_list ')' opt_interleave
  {
    $$.val = &CreateTable{Table: $3.normalizableTableName(), IfNotExists: false, Interleave: $7.interleave(), Defs: $5.tblDefs(), AsSource: nil, AsColumnNames: nil}
  }
| CREATE TABLE IF NOT EXISTS any_name '(' opt_table_elem_list ')' opt_interleave
  {
    $$.val = &CreateTable{Table: $6.normalizableTableName(), IfNotExists: true, Interleave: $10.interleave(), Defs: $8.tblDefs(), AsSource: nil, AsColumnNames: nil}
  }

create_table_as_stmt:
  CREATE TABLE any_name opt_column_list AS select_stmt
  {
    $$.val = &CreateTable{Table: $3.normalizableTableName(), IfNotExists: false, Interleave: nil, Defs: nil, AsSource: $6.slct(), AsColumnNames: $4.nameList()}
  }
| CREATE TABLE IF NOT EXISTS any_name opt_column_list AS select_stmt
  {
    $$.val = &CreateTable{Table: $6.normalizableTableName(), IfNotExists: true, Interleave: nil, Defs: nil, AsSource: $9.slct(), AsColumnNames: $7.nameList()}
  }

opt_table_elem_list:
  table_elem_list
| /* EMPTY */
  {
    $$.val = TableDefs(nil)
  }

table_elem_list:
  table_elem
  {
    $$.val = TableDefs{$1.tblDef()}
  }
| table_elem_list ',' table_elem
  {
    $$.val = append($1.tblDefs(), $3.tblDef())
  }

table_elem:
  column_def
  {
    $$.val = $1.colDef()
  }
| index_def
| family_def
| table_constraint
  {
    $$.val = $1.constraintDef()
  }

opt_interleave:
  INTERLEAVE IN PARENT name '(' name_list ')' opt_interleave_drop_behavior
  {
    $$.val = &InterleaveDef{
               Parent: NormalizableTableName{UnresolvedName{Name($4)}},
               Fields: $6.nameList(),
               DropBehavior: $8.dropBehavior(),
    }
  }
| /* EMPTY */
  {
    $$.val = (*InterleaveDef)(nil)
  }

// TODO(dan): This can be removed in favor of opt_drop_behavior when #7854 is fixed.
opt_interleave_drop_behavior:
  CASCADE
  {
    /* SKIP DOC */
    $$.val = DropCascade
  }
| RESTRICT
  {
    /* SKIP DOC */
    $$.val = DropRestrict
  }
| /* EMPTY */
  {
    $$.val = DropDefault
  }

column_def:
  name typename col_qual_list
  {
    tableDef, err := newColumnTableDef(Name($1), $2.colType(), $3.colQuals())
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = tableDef
  }

col_qual_list:
  col_qual_list col_qualification
  {
    $$.val = append($1.colQuals(), $2.colQual())
  }
| /* EMPTY */
  {
    $$.val = []NamedColumnQualification(nil)
  }

col_qualification:
  CONSTRAINT name col_qualification_elem
  {
    $$.val = NamedColumnQualification{Name: Name($2), Qualification: $3.colQualElem()}
  }
| col_qualification_elem
  {
    $$.val = NamedColumnQualification{Qualification: $1.colQualElem()}
  }
| COLLATE any_name
  {
    $$.val = NamedColumnQualification{Qualification: ColumnCollation($2.unresolvedName().String())}
  }
| FAMILY name
  {
    $$.val = NamedColumnQualification{Qualification: &ColumnFamilyConstraint{Family: Name($2)}}
  }
| CREATE FAMILY opt_name
  {
    $$.val = NamedColumnQualification{Qualification: &ColumnFamilyConstraint{Family: Name($3), Create: true}}
  }
| CREATE IF NOT EXISTS FAMILY name
  {
    $$.val = NamedColumnQualification{Qualification: &ColumnFamilyConstraint{Family: Name($6), Create: true, IfNotExists: true}}
  }

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
    $$.val = NotNullConstraint{}
  }
| NULL
  {
    $$.val = NullConstraint{}
  }
| UNIQUE
  {
    $$.val = UniqueConstraint{}
  }
| PRIMARY KEY
  {
    $$.val = PrimaryKeyConstraint{}
  }
| CHECK '(' a_expr ')'
  {
    $$.val = &ColumnCheckConstraint{Expr: $3.expr()}
  }
| DEFAULT b_expr
  {
    $$.val = &ColumnDefault{Expr: $2.expr()}
  }
| REFERENCES qualified_name opt_name_parens key_match key_actions
 {
    $$.val = &ColumnFKConstraint{
      Table: $2.normalizableTableName(),
      Col: Name($3),
    }
 }

index_def:
  INDEX opt_name '(' index_params ')' opt_storing opt_interleave
  {
    $$.val = &IndexTableDef{
      Name:    Name($2),
      Columns: $4.idxElems(),
      Storing: $6.nameList(),
      Interleave: $7.interleave(),
    }
  }
| UNIQUE INDEX opt_name '(' index_params ')' opt_storing opt_interleave
  {
    $$.val = &UniqueConstraintTableDef{
      IndexTableDef: IndexTableDef {
        Name:    Name($3),
        Columns: $5.idxElems(),
        Storing: $7.nameList(),
        Interleave: $8.interleave(),
      },
    }
  }

family_def:
  FAMILY opt_name '(' name_list ')'
  {
    $$.val = &FamilyTableDef{
      Name: Name($2),
      Columns: $4.nameList(),
    }
  }

// constraint_elem specifies constraint syntax which is not embedded into a
// column definition. col_qualification_elem specifies the embedded form.
// - thomas 1997-12-03
table_constraint:
  CONSTRAINT name constraint_elem
  {
    $$.val = $3.constraintDef()
    $$.val.(ConstraintTableDef).setName(Name($2))
  }
| constraint_elem
  {
    $$.val = $1.constraintDef()
  }

constraint_elem:
  CHECK '(' a_expr ')'
  {
    $$.val = &CheckConstraintTableDef{
      Expr: $3.expr(),
    }
  }
| UNIQUE '(' name_list ')' opt_storing opt_interleave
  {
    $$.val = &UniqueConstraintTableDef{
      IndexTableDef: IndexTableDef{
        Columns: NameListToIndexElems($3.nameList()),
        Storing: $5.nameList(),
        Interleave: $6.interleave(),
      },
    }
  }
| PRIMARY KEY '(' name_list ')'
  {
    $$.val = &UniqueConstraintTableDef{
      IndexTableDef: IndexTableDef{
        Columns: NameListToIndexElems($4.nameList()),
      },
      PrimaryKey:    true,
    }
  }
| FOREIGN KEY '(' name_list ')' REFERENCES qualified_name
    opt_column_list key_match key_actions
  {
    $$.val = &ForeignKeyConstraintTableDef{
      Table: $7.normalizableTableName(),
      FromCols: $4.nameList(),
      ToCols: $8.nameList(),
    }
  }

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
    $$.val = $3.nameList()
  }
| /* EMPTY */
  {
    $$.val = NameList(nil)
  }

opt_column_list:
  '(' name_list ')'
  {
    $$.val = $2.nameList()
  }
| /* EMPTY */
  {
    $$.val = NameList(nil)
  }

key_match:
  MATCH FULL { return unimplemented(sqllex) }
| MATCH PARTIAL { return unimplemented(sqllex) }
| MATCH SIMPLE { return unimplemented(sqllex) }
| /* EMPTY */ {}

// We combine the update and delete actions into one value temporarily for
// simplicity of parsing, and then break them down again in the calling
// production. update is in the left 8 bits, delete in the right. Note that
// NOACTION is the default.
key_actions:
  key_update { return unimplemented(sqllex) }
| key_delete { return unimplemented(sqllex) }
| key_update key_delete { return unimplemented(sqllex) }
| key_delete key_update { return unimplemented(sqllex) }
| /* EMPTY */ {}

key_update:
  ON UPDATE key_action { return unimplemented(sqllex) }

key_delete:
  ON DELETE key_action { return unimplemented(sqllex) }

key_action:
  NO ACTION { return unimplemented(sqllex) }
| RESTRICT { return unimplemented(sqllex) }
| CASCADE { return unimplemented(sqllex) }
| SET NULL { return unimplemented(sqllex) }
| SET DEFAULT { return unimplemented(sqllex) }

numeric_only:
  FCONST
  {
    $$.val = $1.numVal()
  }
| '-' FCONST
  {
    $$.val = &NumVal{Value: constant.UnaryOp(token.SUB, $2.numVal().Value, 0)}
  }
| signed_iconst
  {
    $$.val = $1.numVal()
  }

// TRUNCATE table relname1, relname2, ...
truncate_stmt:
  TRUNCATE opt_table relation_expr_list opt_drop_behavior
  {
    $$.val = &Truncate{Tables: $3.tableNameReferences(), DropBehavior: $4.dropBehavior()}
  }

// CREATE USER
create_user_stmt:
  CREATE USER name opt_with opt_password
  {
    $$.val = &CreateUser{Name: Name($3), Password: $5.strPtr()}
  }

opt_password:
  PASSWORD SCONST
  {
    pwd := $2
    $$.val = &pwd
  }
| /* EMPTY */ {
    $$.val = (*string)(nil)
  }

// CREATE VIEW relname
create_view_stmt:
  CREATE VIEW any_name opt_column_list AS select_stmt
  {
    $$.val = &CreateView{
      Name: $3.normalizableTableName(),
      ColumnNames: $4.nameList(),
      AsSource: $6.slct(),
    }
  }

// TODO(a-robinson): CREATE OR REPLACE VIEW support (#2971).

// CREATE INDEX
create_index_stmt:
  CREATE opt_unique INDEX opt_name ON qualified_name '(' index_params ')' opt_storing opt_interleave
  {
    $$.val = &CreateIndex{
      Name:    Name($4),
      Table:   $6.normalizableTableName(),
      Unique:  $2.bool(),
      Columns: $8.idxElems(),
      Storing: $10.nameList(),
      Interleave: $11.interleave(),
    }
  }
| CREATE opt_unique INDEX IF NOT EXISTS name ON qualified_name '(' index_params ')' opt_storing opt_interleave
  {
    $$.val = &CreateIndex{
      Name:        Name($7),
      Table:       $9.normalizableTableName(),
      Unique:      $2.bool(),
      IfNotExists: true,
      Columns:     $11.idxElems(),
      Storing:     $13.nameList(),
      Interleave: $14.interleave(),
    }
  }

opt_unique:
  UNIQUE
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

index_params:
  index_elem
  {
    $$.val = IndexElemList{$1.idxElem()}
  }
| index_params ',' index_elem
  {
    $$.val = append($1.idxElems(), $3.idxElem())
  }

// Index attributes can be either simple column references, or arbitrary
// expressions in parens. For backwards-compatibility reasons, we allow an
// expression that's just a function call to be written without parens.
index_elem:
  name opt_collate opt_asc_desc
  {
    $$.val = IndexElem{Column: Name($1), Direction: $3.dir()}
  }
| func_expr_windowless opt_collate opt_asc_desc { return unimplemented(sqllex) }
| '(' a_expr ')' opt_collate opt_asc_desc { return unimplemented(sqllex) }

opt_collate:
  COLLATE any_name { return unimplemented(sqllex) }
| /* EMPTY */ {}

opt_asc_desc:
  ASC
  {
    $$.val = Ascending
  }
| DESC
  {
    $$.val = Descending
  }
| /* EMPTY */
  {
    $$.val = DefaultDirection
  }

// ALTER THING name RENAME TO newname
rename_stmt:
  ALTER DATABASE name RENAME TO name
  {
    $$.val = &RenameDatabase{Name: Name($3), NewName: Name($6)}
  }
| ALTER TABLE relation_expr RENAME TO qualified_name
  {
    $$.val = &RenameTable{Name: $3.normalizableTableName(), NewName: $6.normalizableTableName(), IfExists: false, IsView: false}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME TO qualified_name
  {
    $$.val = &RenameTable{Name: $5.normalizableTableName(), NewName: $8.normalizableTableName(), IfExists: true, IsView: false}
  }
| ALTER VIEW relation_expr RENAME TO qualified_name
  {
    $$.val = &RenameTable{Name: $3.normalizableTableName(), NewName: $6.normalizableTableName(), IfExists: false, IsView: true}
  }
| ALTER VIEW IF EXISTS relation_expr RENAME TO qualified_name
  {
    $$.val = &RenameTable{Name: $5.normalizableTableName(), NewName: $8.normalizableTableName(), IfExists: true, IsView: true}
  }
| ALTER INDEX table_name_with_index RENAME TO name
  {
    $$.val = &RenameIndex{Index: $3.tableWithIdx(), NewName: Name($6), IfExists: false}
  }
| ALTER INDEX IF EXISTS table_name_with_index RENAME TO name
  {
    $$.val = &RenameIndex{Index: $5.tableWithIdx(), NewName: Name($8), IfExists: true}
  }
| ALTER TABLE relation_expr RENAME opt_column name TO name
  {
    $$.val = &RenameColumn{Table: $3.normalizableTableName(), Name: Name($6), NewName: Name($8), IfExists: false}
  }
| ALTER TABLE IF EXISTS relation_expr RENAME opt_column name TO name
  {
    $$.val = &RenameColumn{Table: $5.normalizableTableName(), Name: Name($8), NewName: Name($10), IfExists: true}
  }
| ALTER TABLE relation_expr RENAME CONSTRAINT name TO name { return unimplemented(sqllex) }
| ALTER TABLE IF EXISTS relation_expr RENAME CONSTRAINT name TO name { return unimplemented(sqllex) }

opt_column:
  COLUMN
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

opt_set_data:
  SET DATA {}
| /* EMPTY */ {}

release_stmt:
 RELEASE savepoint_name
 {
  $$.val = &ReleaseSavepoint{Savepoint: $2}
 }

savepoint_stmt:
 SAVEPOINT savepoint_name
 {
  $$.val = &Savepoint{Name: $2}
 }

// BEGIN / START / COMMIT / END / ROLLBACK / ...
transaction_stmt:
  BEGIN opt_transaction opt_transaction_mode_list
  {
    $$.val = $3.stmt()
  }
| START TRANSACTION opt_transaction_mode_list
  {
    $$.val = $3.stmt()
  }
| COMMIT opt_transaction
  {
    $$.val = &CommitTransaction{}
  }
| END opt_transaction
  {
    $$.val = &CommitTransaction{}
  }
| ROLLBACK opt_to_savepoint
  {
    if $2 != "" {
      $$.val = &RollbackToSavepoint{Savepoint: $2}
    } else {
      $$.val = &RollbackTransaction{}
    }
  }

opt_transaction:
  TRANSACTION {}
| /* EMPTY */ {}

opt_to_savepoint:
  TRANSACTION
  {
    $$ = ""
  }
| TRANSACTION TO savepoint_name
  {
    $$ = $3
  }
| TO savepoint_name
  {
    $$ = $2
  }
| /* EMPTY */
  {
    $$ = ""
  }

savepoint_name:
  SAVEPOINT name
  {
    $$ = $2
  }
| name
  {
    $$ = $1
  }

opt_transaction_mode_list:
  transaction_iso_level
  {
    $$.val = &BeginTransaction{Isolation: $1.isoLevel(), UserPriority: UnspecifiedUserPriority}
  }
| transaction_user_priority
  {
    $$.val = &BeginTransaction{Isolation: UnspecifiedIsolation, UserPriority: $1.userPriority()}
  }
| transaction_iso_level ',' transaction_user_priority
  {
    $$.val = &BeginTransaction{Isolation: $1.isoLevel(), UserPriority: $3.userPriority()}
  }
| transaction_user_priority ',' transaction_iso_level
  {
    $$.val = &BeginTransaction{Isolation: $3.isoLevel(), UserPriority: $1.userPriority()}
  }
| /* EMPTY */
  {
    $$.val = &BeginTransaction{Isolation: UnspecifiedIsolation, UserPriority: UnspecifiedUserPriority}
  }

transaction_iso_level:
  ISOLATION LEVEL iso_level
  {
    $$.val = $3.isoLevel()
  }

create_database_stmt:
  CREATE DATABASE name opt_with opt_template_clause opt_encoding_clause opt_lc_collate_clause opt_lc_ctype_clause
  {
    $$.val = &CreateDatabase{
      Name: Name($3),
      Template: $5,
      Encoding: $6,
      Collate: $7,
      CType: $8,
    }
  }
| CREATE DATABASE IF NOT EXISTS name opt_with opt_template_clause opt_encoding_clause opt_lc_collate_clause opt_lc_ctype_clause
  {
    $$.val = &CreateDatabase{
      IfNotExists: true,
      Name: Name($6),
      Template: $8,
      Encoding: $9,
      Collate: $10,
      CType: $11,
    }
  }

opt_template_clause:
  TEMPLATE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_encoding_clause:
  ENCODING opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_lc_collate_clause:
  LC_COLLATE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_lc_ctype_clause:
  LC_CTYPE opt_equal non_reserved_word_or_sconst
  {
    $$ = $3
  }
| /* EMPTY */
  {
    $$ = ""
  }

opt_equal:
  '=' {}
| /* EMPTY */ {}

// TODO(dan): While RETURNING is not supported with UPSERT and ON CONFLICT
// (#6637), we do some gymnastics with the grammar to make the diagrams in the
// docs only show the supported combinations. This simplifies once #6637 is
// resolved.
insert_stmt:
  opt_with_clause INSERT INTO insert_target insert_rest returning_clause
  {
    $$.val = $5.stmt()
    $$.val.(*Insert).Table = $4.tblExpr()
    $$.val.(*Insert).Returning = $6.retExprs()
  }
| opt_with_clause INSERT INTO insert_target insert_rest on_conflict
  {
    $$.val = $5.stmt()
    $$.val.(*Insert).Table = $4.tblExpr()
    $$.val.(*Insert).OnConflict = $6.onConflict()
  }
| opt_with_clause INSERT INTO insert_target insert_rest on_conflict RETURNING target_list { return unimplementedWithIssue(sqllex, 6637) }
| opt_with_clause UPSERT INTO insert_target insert_rest
  {
    $$.val = $5.stmt()
    $$.val.(*Insert).Table = $4.tblExpr()
    $$.val.(*Insert).OnConflict = &OnConflict{}
  }
| opt_with_clause UPSERT INTO insert_target insert_rest RETURNING target_list { return unimplementedWithIssue(sqllex, 6637) }

// Can't easily make AS optional here, because VALUES in insert_rest would have
// a shift/reduce conflict with VALUES as an optional alias. We could easily
// allow unreserved_keywords as optional aliases, but that'd be an odd
// divergence from other places. So just require AS for now.
insert_target:
  qualified_name
  {
    $$.val = $1.newNormalizableTableName()
  }
| qualified_name AS name
  {
    $$.val = &AliasedTableExpr{Expr: $1.newNormalizableTableName(), As: AliasClause{Alias: Name($3)}}
  }

insert_rest:
  select_stmt
  {
    $$.val = &Insert{Rows: $1.slct()}
  }
| '(' qualified_name_list ')' select_stmt
  {
    $$.val = &Insert{Columns: $2.unresolvedNames(), Rows: $4.slct()}
  }
| DEFAULT VALUES
  {
    $$.val = &Insert{Rows: &Select{}}
  }

on_conflict:
  ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list where_clause
  {
    $$.val = &OnConflict{Columns: $3.nameList(), Exprs: $7.updateExprs(), Where: newWhere(astWhere, $8.expr())}
  }
| ON CONFLICT opt_conf_expr DO NOTHING
  {
    $$.val = &OnConflict{Columns: $3.nameList(), DoNothing: true}
  }

opt_conf_expr:
  '(' name_list ')' where_clause
  {
    // TODO(dan): Support the where_clause.
    $$.val = $2.nameList()
  }
| ON CONSTRAINT name { return unimplemented(sqllex) }
| /* EMPTY */
  {
    $$.val = NameList(nil)
  }

returning_clause:
  RETURNING target_list
  {
    $$.val = $2.selExprs()
  }
| /* EMPTY */
  {
    $$.val = SelectExprs(nil)
  }

update_stmt:
  opt_with_clause UPDATE relation_expr_opt_alias
    SET set_clause_list update_from_clause where_clause returning_clause
  {
    $$.val = &Update{Table: $3.tblExpr(), Exprs: $5.updateExprs(), Where: newWhere(astWhere, $7.expr()), Returning: $8.retExprs()}
  }

// Mark this as unimplemented until the normal from_clause is supported here.
update_from_clause:
  FROM from_list { return unimplementedWithIssue(sqllex, 7841) }
| /* EMPTY */ {}

set_clause_list:
  set_clause
  {
    $$.val = UpdateExprs{$1.updateExpr()}
  }
| set_clause_list ',' set_clause
  {
    $$.val = append($1.updateExprs(), $3.updateExpr())
  }

set_clause:
  single_set_clause
| multiple_set_clause

single_set_clause:
  qualified_name '=' ctext_expr
  {
    $$.val = &UpdateExpr{Names: UnresolvedNames{$1.unresolvedName()}, Expr: $3.expr()}
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
    $$.val = &UpdateExpr{Tuple: true, Names: $2.unresolvedNames(), Expr: &Tuple{Exprs: $5.exprs()}}
  }
| '(' qualified_name_list ')' '=' select_with_parens
  {
    $$.val = &UpdateExpr{Tuple: true, Names: $2.unresolvedNames(), Expr: &Subquery{Select: $5.selectStmt()}}
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
  {
    $$.val = &Select{Select: $1.selectStmt()}
  }

select_with_parens:
  '(' select_no_parens ')'
  {
    $$.val = &ParenSelect{Select: $2.slct()}
  }
| '(' select_with_parens ')'
  {
    $$.val = &ParenSelect{Select: &Select{Select: $2.selectStmt()}}
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
  {
    $$.val = &Select{Select: $1.selectStmt()}
  }
| select_clause sort_clause
  {
    $$.val = &Select{Select: $1.selectStmt(), OrderBy: $2.orderBy()}
  }
| select_clause opt_sort_clause select_limit
  {
    $$.val = &Select{Select: $1.selectStmt(), OrderBy: $2.orderBy(), Limit: $3.limit()}
  }
| with_clause select_clause
  {
    $$.val = &Select{Select: $2.selectStmt()}
  }
| with_clause select_clause sort_clause
  {
    $$.val = &Select{Select: $2.selectStmt(), OrderBy: $3.orderBy()}
  }
| with_clause select_clause opt_sort_clause select_limit
  {
    $$.val = &Select{Select: $2.selectStmt(), OrderBy: $3.orderBy(), Limit: $4.limit()}
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
  SELECT opt_all_clause target_list
    from_clause where_clause
    group_clause having_clause window_clause
  {
    $$.val = &SelectClause{
      Exprs:   $3.selExprs(),
      From:    $4.from(),
      Where:   newWhere(astWhere, $5.expr()),
      GroupBy: $6.groupBy(),
      Having:  newWhere(astHaving, $7.expr()),
      Window:  $8.window(),
    }
  }
| SELECT distinct_clause target_list
    from_clause where_clause
    group_clause having_clause window_clause
  {
    $$.val = &SelectClause{
      Distinct: $2.bool(),
      Exprs:    $3.selExprs(),
      From:     $4.from(),
      Where:    newWhere(astWhere, $5.expr()),
      GroupBy:  $6.groupBy(),
      Having:   newWhere(astHaving, $7.expr()),
      Window:   $8.window(),
    }
  }
| values_clause
| TABLE relation_expr
  {
    $$.val = &SelectClause{
      Exprs:       SelectExprs{starSelectExpr()},
      From:        &From{Tables: TableExprs{$2.newNormalizableTableName()}},
      tableSelect: true,
    }
  }
| select_clause UNION all_or_distinct select_clause
  {
    $$.val = &UnionClause{
      Type:  UnionOp,
      Left:  &Select{Select: $1.selectStmt()},
      Right: &Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }
| select_clause INTERSECT all_or_distinct select_clause
  {
    $$.val = &UnionClause{
      Type:  IntersectOp,
      Left:  &Select{Select: $1.selectStmt()},
      Right: &Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
  }
| select_clause EXCEPT all_or_distinct select_clause
  {
    $$.val = &UnionClause{
      Type:  ExceptOp,
      Left:  &Select{Select: $1.selectStmt()},
      Right: &Select{Select: $4.selectStmt()},
      All:   $3.bool(),
    }
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
WITH cte_list { return unimplemented(sqllex) }
| WITH_LA cte_list { return unimplemented(sqllex) }
| WITH RECURSIVE cte_list { return unimplemented(sqllex) }

cte_list:
  common_table_expr { return unimplemented(sqllex) }
| cte_list ',' common_table_expr { return unimplemented(sqllex) }

common_table_expr:
  name opt_name_list AS '(' preparable_stmt ')' { return unimplemented(sqllex) }

opt_with:
  WITH {}
| /* EMPTY */ {}

opt_with_clause:
  with_clause { return unimplemented(sqllex) }
| /* EMPTY */ {}

opt_table:
  TABLE {}
| /* EMPTY */ {}

all_or_distinct:
  ALL
  {
    $$.val = true
  }
| DISTINCT
  {
    $$.val = false
  }
| /* EMPTY */
  {
    $$.val = false
  }

distinct_clause:
  DISTINCT
  {
    $$.val = true
  }

opt_all_clause:
  ALL {}
| /* EMPTY */ {}

opt_sort_clause:
  sort_clause
  {
    $$.val = $1.orderBy()
  }
| /* EMPTY */
  {
    $$.val = OrderBy(nil)
  }

sort_clause:
  ORDER BY sortby_list
  {
    $$.val = OrderBy($3.orders())
  }

sortby_list:
  sortby
  {
    $$.val = []*Order{$1.order()}
  }
| sortby_list ',' sortby
  {
    $$.val = append($1.orders(), $3.order())
  }

sortby:
  a_expr opt_asc_desc
  {
    $$.val = &Order{Expr: $1.expr(), Direction: $2.dir()}
  }
// TODO(pmattis): Support ordering using arbitrary math ops?
// | a_expr USING math_op {}

select_limit:
  limit_clause offset_clause
  {
    if $1.limit() == nil {
      $$.val = $2.limit()
    } else {
      $$.val = $1.limit()
      $$.val.(*Limit).Offset = $2.limit().Offset
    }
  }
| offset_clause limit_clause
  {
    $$.val = $1.limit()
    if $2.limit() != nil {
      $$.val.(*Limit).Count = $2.limit().Count
    }
  }
| limit_clause
| offset_clause

limit_clause:
  LIMIT select_limit_value
  {
    if $2.expr() == nil {
      $$.val = (*Limit)(nil)
    } else {
      $$.val = &Limit{Count: $2.expr()}
    }
  }
// SQL:2008 syntax
// TODO(pmattis): Should we support this?
// | FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY { return unimplemented(sqllex) }

offset_clause:
  OFFSET a_expr
  {
    $$.val = &Limit{Offset: $2.expr()}
  }
  // SQL:2008 syntax
  // The trailing ROW/ROWS in this case prevent the full expression
  // syntax. c_expr is the best we can do.
| OFFSET c_expr row_or_rows
  {
    $$.val = &Limit{Offset: $2.expr()}
  }

select_limit_value:
  a_expr
| ALL
  {
    $$.val = Expr(nil)
  }

// Allowing full expressions without parentheses causes various parsing
// problems with the trailing ROW/ROWS key words. SQL only calls for constants,
// so we allow the rest only with parentheses. If omitted, default to 1.
// opt_select_fetch_first_value:
//   signed_iconst { return unimplemented(sqllex) }
// | '(' a_expr ')' { return unimplemented(sqllex) }
// | /* EMPTY */ {}

// noise words
row_or_rows:
  ROW {}
| ROWS {}

// first_or_next:
//   FIRST { return unimplemented(sqllex) }
// | NEXT { return unimplemented(sqllex) }

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
    $$.val = GroupBy($3.exprs())
  }
| /* EMPTY */
  {
    $$.val = GroupBy(nil)
  }

having_clause:
  HAVING a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = Expr(nil)
  }

// Given "VALUES (a, b)" in a table expression context, we have to
// decide without looking any further ahead whether VALUES is the
// values clause or a set-generating function. Since VALUES is allowed
// as a function name both interpretations are feasible. We resolve
// the shift/reduce conflict by giving the first values_clause
// production a higher precedence than the VALUES token has, causing
// the parser to prefer to reduce, in effect assuming that the VALUES
// is not a function name.
values_clause:
  VALUES ctext_row %prec UMINUS
  {
    $$.val = &ValuesClause{[]*Tuple{{Exprs: $2.exprs()}}}
  }
| values_clause ',' ctext_row
  {
    valNode := $1.selectStmt().(*ValuesClause)
    valNode.Tuples = append(valNode.Tuples, &Tuple{Exprs: $3.exprs()})
    $$.val = valNode
  }

// clauses common to all optimizable statements:
//  from_clause   - allow list of both JOIN expressions and table names
//  where_clause  - qualifications for joins or restrictions

from_clause:
  FROM from_list opt_as_of_clause
  {
    $$.val = &From{Tables: $2.tblExprs(), AsOf: $3.asOfClause()}
  }
| /* EMPTY */
  {
    $$.val = &From{}
  }

from_list:
  table_ref
  {
    $$.val = TableExprs{$1.tblExpr()}
  }
| from_list ',' table_ref
  {
    $$.val = append($1.tblExprs(), $3.tblExpr())
  }

index_hints_param:
  FORCE_INDEX '=' unrestricted_name
  {
     $$.val = &IndexHints{Index: Name($3)}
  }
|
  NO_INDEX_JOIN
  {
     $$.val = &IndexHints{NoIndexJoin: true}
  }

index_hints_param_list:
  index_hints_param
  {
    $$.val = $1.indexHints()
  }
|
  index_hints_param_list ',' index_hints_param
  {
    a := $1.indexHints()
    b := $3.indexHints()
    index := a.Index
    if index == "" {
       index = b.Index
    } else if b.Index != "" {
       sqllex.Error("FORCE_INDEX specified multiple times")
       return 1
    }
    if a.NoIndexJoin && b.NoIndexJoin {
       sqllex.Error("NO_INDEX_JOIN specified multiple times")
       return 1
    }
    noIndexJoin := a.NoIndexJoin || b.NoIndexJoin
    $$.val = &IndexHints{Index: index, NoIndexJoin: noIndexJoin}
  }

opt_index_hints:
  '@' unrestricted_name
  {
    $$.val = &IndexHints{Index: Name($2)}
  }
| '@' '{' index_hints_param_list '}'
  {
    $$.val = $3.indexHints()
  }
| /* EMPTY */
  {
    $$.val = (*IndexHints)(nil)
  }

// table_ref is where an alias clause can be attached.
table_ref:
  relation_expr opt_index_hints opt_ordinality opt_alias_clause
  {
    $$.val = &AliasedTableExpr{Expr: $1.newNormalizableTableName(), Hints: $2.indexHints(), Ordinality: $3.bool(), As: $4.aliasClause() }
  }
| qualified_name '(' expr_list ')' opt_ordinality opt_alias_clause
  {
    $$.val = &AliasedTableExpr{Expr: &FuncExpr{Func: $1.resolvableFunctionReference(), Exprs: $3.exprs()}, Ordinality: $5.bool(), As: $6.aliasClause() }
  }
| select_with_parens opt_ordinality opt_alias_clause
  {
    $$.val = &AliasedTableExpr{Expr: &Subquery{Select: $1.selectStmt()}, Ordinality: $2.bool(), As: $3.aliasClause() }
  }
| joined_table
  {
    $$.val = $1.tblExpr()
  }
| '(' joined_table ')' opt_ordinality alias_clause
  {
    $$.val = &AliasedTableExpr{Expr: $2.tblExpr(), Ordinality: $4.bool(), As: $5.aliasClause() }
  }

// The following syntax is a CockroachDB extension: SELECT ... FROM [ EXPLAIN .... ] WHERE ...
// EXPLAIN within square brackets can be used as a table expression (data source).
// We use square brackets for two reasons:
// - the grammar would be terribly ambiguous if we used simple
//   parentheses or no parentheses at all.
// - it carries visual semantic information, by marking the table
//   expression as radically different from the other things. This is
//   useful because the statement after EXPLAIN never runs, so the
//   entire bracketed EXPLAIN data source can be seen as a way to
//   "escape" the enclosed statement. And if a user does not know this
//   and encounters this syntax, they will know from the unusual
//   choice that something rather different is going on and may be
//   pushed by the unusual syntax to investigate further in the docs.

| '[' EXPLAIN  explainable_stmt ']' opt_ordinality opt_alias_clause
  {
    $$.val = &AliasedTableExpr{Expr: &Explain{ Statement: $3.stmt(), Enclosed: true }, Ordinality: $5.bool(), As: $6.aliasClause() }
  }
| '[' EXPLAIN '(' explain_option_list ')' explainable_stmt ']' opt_ordinality opt_alias_clause
  {
    $$.val = &AliasedTableExpr{Expr: &Explain{ Options: $4.strs(), Statement: $6.stmt(), Enclosed: true }, Ordinality: $8.bool(), As: $9.aliasClause() }
  }

opt_ordinality:
  WITH_LA ORDINALITY
  {
    $$.val = true
  }
| /* EMPTY */
  {
    $$.val = false
  }

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
    $$.val = &ParenTableExpr{Expr: $2.tblExpr()}
  }
| table_ref CROSS JOIN table_ref
  {
    $$.val = &JoinTableExpr{Join: astCrossJoin, Left: $1.tblExpr(), Right: $4.tblExpr()}
  }
| table_ref join_type JOIN table_ref join_qual
  {
    $$.val = &JoinTableExpr{Join: $2, Left: $1.tblExpr(), Right: $4.tblExpr(), Cond: $5.joinCond()}
  }
| table_ref JOIN table_ref join_qual
  {
    $$.val = &JoinTableExpr{Join: astJoin, Left: $1.tblExpr(), Right: $3.tblExpr(), Cond: $4.joinCond()}
  }
| table_ref NATURAL join_type JOIN table_ref
  {
    $$.val = &JoinTableExpr{Join: $3, Left: $1.tblExpr(), Right: $5.tblExpr(), Cond: NaturalJoinCond{}}
  }
| table_ref NATURAL JOIN table_ref
  {
    $$.val = &JoinTableExpr{Join: astJoin, Left: $1.tblExpr(), Right: $4.tblExpr(), Cond: NaturalJoinCond{}}
  }

alias_clause:
  AS name '(' name_list ')'
  {
    $$.val = AliasClause{Alias: Name($2), Cols: $4.nameList()}
  }
| AS name
  {
    $$.val = AliasClause{Alias: Name($2)}
  }
| name '(' name_list ')'
  {
    $$.val = AliasClause{Alias: Name($1), Cols: $3.nameList()}
  }
| name
  {
    $$.val = AliasClause{Alias: Name($1)}
  }

opt_alias_clause:
  alias_clause
| /* EMPTY */
  {
    $$.val = AliasClause{}
  }

opt_as_of_clause:
  AS_LA OF SYSTEM TIME a_expr_const
  {
    $$.val = AsOfClause{Expr: $5.expr()}
  }
| /* EMPTY */
  {
    $$.val = AsOfClause{}
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
    $$.val = &UsingJoinCond{Cols: $3.nameList()}
  }
| ON a_expr
  {
    $$.val = &OnJoinCond{Expr: $2.expr()}
  }

relation_expr:
  qualified_name
  {
    $$.val = $1.unresolvedName()
  }
| qualified_name '*'
  {
    $$.val = $1.unresolvedName()
  }
| ONLY qualified_name
  {
    $$.val = $2.unresolvedName()
  }
| ONLY '(' qualified_name ')'
  {
    $$.val = $3.unresolvedName()
  }

relation_expr_list:
  relation_expr
  {
    $$.val = TableNameReferences{$1.unresolvedName()}
  }
| relation_expr_list ',' relation_expr
  {
    $$.val = append($1.tableNameReferences(), $3.unresolvedName())
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
    $$.val = $1.newNormalizableTableName()
  }
| relation_expr name
  {
    $$.val = &AliasedTableExpr{Expr: $1.newNormalizableTableName(), As: AliasClause{Alias: Name($2)}}
  }
| relation_expr AS name
  {
    $$.val = &AliasedTableExpr{Expr: $1.newNormalizableTableName(), As: AliasClause{Alias: Name($3)}}
  }

where_clause:
  WHERE a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = Expr(nil)
  }

// Type syntax
//   SQL introduces a large amount of type-specific syntax.
//   Define individual clauses to handle these cases, and use
//   the generic case to handle regular type-extensible Postgres syntax.
//   - thomas 1997-10-10

typename:
  simple_typename opt_array_bounds
  {
    if exprs := $2.exprs(); exprs != nil {
      var err error
      $$.val, err = arrayOf($1.colType(), exprs)
      if err != nil {
        sqllex.Error(err.Error())
        return 1
      }
    } else {
      $$.val = $1.colType()
    }
  }
  // SQL standard syntax, currently only one-dimensional
| simple_typename ARRAY '[' ICONST ']' { return unimplementedWithIssue(sqllex, 2115) }
| simple_typename ARRAY { return unimplementedWithIssue(sqllex, 2115) }

cast_target:
  typename
  {
    $$.val = $1.colType()
  }
| postgres_oid
  {
    $$.val = $1.castTargetType()
  }

opt_array_bounds:
  opt_array_bounds '[' ']' { $$.val = Exprs{NewDInt(DInt(-1))} }
| opt_array_bounds '[' ICONST ']' { return unimplementedWithIssue(sqllex, 2115) }
| /* EMPTY */ { $$.val = Exprs(nil) }

simple_typename:
  numeric
| bit
| character
| const_datetime
| const_interval opt_interval // TODO(pmattis): Support opt_interval?
| const_interval '(' ICONST ')' { return unimplemented(sqllex) }
| BLOB
  {
    $$.val = bytesColTypeBlob
  }
| BYTES
  {
    $$.val = bytesColTypeBytes
  }
| BYTEA
  {
    $$.val = bytesColTypeBytea
  }
| TEXT
  {
    $$.val = stringColTypeText
  }
| NAME
  {
    $$.val = nameColTypeName
  }
| SERIAL
  {
    $$.val = intColTypeSerial
  }
| SMALLSERIAL
  {
    $$.val = intColTypeSmallSerial
  }
| BIGSERIAL
  {
    $$.val = intColTypeBigSerial
  }
| OID
  {
    $$.val = oidColTypeOid
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
    prec, err := $2.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = &DecimalColType{Prec: int(prec)}
  }
| '(' ICONST ',' ICONST ')'
  {
    prec, err := $2.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    scale, err := $4.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = &DecimalColType{Prec: int(prec), Scale: int(scale)}
  }
| /* EMPTY */
  {
    $$.val = nil
  }

// SQL numeric data types
numeric:
  INT
  {
    $$.val = intColTypeInt
  }
| INT8
  {
    $$.val = intColTypeInt8
  }
| INT64
  {
    $$.val = intColTypeInt64
  }
| INTEGER
  {
    $$.val = intColTypeInteger
  }
| SMALLINT
  {
    $$.val = intColTypeSmallInt
  }
| BIGINT
  {
    $$.val = intColTypeBigInt
  }
| REAL
  {
    $$.val = floatColTypeReal
  }
| FLOAT opt_float
  {
    prec, err := $2.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = newFloatColType(int(prec))
  }
| DOUBLE PRECISION
  {
    $$.val = floatColTypeDouble
  }
| DECIMAL opt_numeric_modifiers
  {
    $$.val = $2.colType()
    if $$.val == nil {
      $$.val = decimalColTypeDecimal
    } else {
      $$.val.(*DecimalColType).Name = "DECIMAL"
    }
  }
| DEC opt_numeric_modifiers
  {
    $$.val = $2.colType()
    if $$.val == nil {
      $$.val = decimalColTypeDec
    } else {
      $$.val.(*DecimalColType).Name = "DEC"
    }
  }
| NUMERIC opt_numeric_modifiers
  {
    $$.val = $2.colType()
    if $$.val == nil {
      $$.val = decimalColTypeNumeric
    } else {
      $$.val.(*DecimalColType).Name = "NUMERIC"
    }
  }
| BOOLEAN
  {
    $$.val = boolColTypeBoolean
  }
| BOOL
  {
    $$.val = boolColTypeBool
  }

// Postgres OID pseudo-types. See https://www.postgresql.org/docs/9.4/static/datatype-oid.html.
postgres_oid:
  REGPROC
  {
    $$.val = oidPseudoTypeRegProc
  }
| REGPROCEDURE
  {
    $$.val = oidPseudoTypeRegProc
  }
| REGCLASS
  {
    $$.val = oidPseudoTypeRegClass
  }
| REGTYPE
  {
    $$.val = oidPseudoTypeRegType
  }
| REGNAMESPACE
  {
    $$.val = oidPseudoTypeRegNamespace
  }

opt_float:
  '(' ICONST ')'
  {
    $$.val = $2.numVal()
  }
| /* EMPTY */
  {
    $$.val = &NumVal{Value: constant.MakeInt64(0)}
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
    n, err := $4.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    bit, err := newIntBitType(int(n))
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = bit
  }

bit_without_length:
  BIT opt_varying
  {
    $$.val = intColTypeBit
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
    n, err := $3.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    $$.val = $1.colType()
    if n != 0 {
      strType := &StringColType{N: int(n)}
      strType.Name = $$.val.(*StringColType).Name
      $$.val = strType
    }
  }

character_without_length:
  character_base
  {
    $$.val = $1.colType()
  }

character_base:
  CHARACTER opt_varying
  {
    $$.val = stringColTypeChar
  }
| CHAR opt_varying
  {
    $$.val = stringColTypeChar
  }
| VARCHAR
  {
    $$.val = stringColTypeVarChar
  }
| STRING
  {
    $$.val = stringColTypeString
  }

opt_varying:
  VARYING {}
| /* EMPTY */ {}

// SQL date/time types
const_datetime:
  DATE
  {
    $$.val = dateColTypeDate
  }
| TIMESTAMP
  {
    $$.val = timestampColTypeTimestamp
  }
| TIMESTAMP WITHOUT TIME ZONE
  {
    $$.val = timestampColTypeTimestamp
  }
| TIMESTAMPTZ
  {
    $$.val = timestampTzColTypeTimestampWithTZ
  }
| TIMESTAMP WITH_LA TIME ZONE
  {
    $$.val = timestampTzColTypeTimestampWithTZ
  }

const_interval:
  INTERVAL {
    $$.val = intervalColTypeInterval
  }

opt_interval:
  YEAR
  {
    $$.val = year
  }
| MONTH
  {
    $$.val = month
  }
| DAY
  {
    $$.val = day
  }
| HOUR
  {
    $$.val = hour
  }
| MINUTE
  {
    $$.val = minute
  }
| interval_second
  {
    $$.val = $1.durationField()
  }
// Like Postgres, we ignore the left duration field. See explanation:
// https://www.postgresql.org/message-id/20110510040219.GD5617%40tornado.gateway.2wire.net
| YEAR TO MONTH
  {
    $$.val = month
  }
| DAY TO HOUR
  {
    $$.val = hour
  }
| DAY TO MINUTE
  {
    $$.val = minute
  }
| DAY TO interval_second
  {
    $$.val = $3.durationField()
  }
| HOUR TO MINUTE
  {
    $$.val = minute
  }
| HOUR TO interval_second
  {
    $$.val = $3.durationField()
  }
| MINUTE TO interval_second
  {
    $$.val = $3.durationField()
  }
| /* EMPTY */ {}

interval_second:
  SECOND
  {
    $$.val = second
  }
| SECOND '(' ICONST ')' { return unimplemented(sqllex) }

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
| a_expr TYPECAST cast_target
  {
    $$.val = &CastExpr{Expr: $1.expr(), Type: $3.castTargetType(), syntaxMode: castShort}
  }
| a_expr TYPEANNOTATE typename
  {
    $$.val = &AnnotateTypeExpr{Expr: $1.expr(), Type: $3.colType(), syntaxMode: annotateShort}
  }
| a_expr COLLATE any_name
  {
    $$.val = &CollateExpr{Expr: $1.expr(), Locale: $3.unresolvedName().String()}
  }
| a_expr AT TIME ZONE a_expr %prec AT { return unimplemented(sqllex) }
  // These operators must be called out explicitly in order to make use of
  // bison's automatic operator-precedence handling. All other operator names
  // are handled by the generic productions using "OP", below; and all those
  // operators will have the same precedence.
  //
  // If you add more explicitly-known operators, be sure to add them also to
  // b_expr and to the math_op list below.
| '+' a_expr %prec UMINUS
  {
    $$.val = &UnaryExpr{Operator: UnaryPlus, Expr: $2.expr()}
  }
| '-' a_expr %prec UMINUS
  {
    $$.val = &UnaryExpr{Operator: UnaryMinus, Expr: $2.expr()}
  }
| '~' a_expr %prec UMINUS
  {
    $$.val = &UnaryExpr{Operator: UnaryComplement, Expr: $2.expr()}
  }
| a_expr '+' a_expr
  {
    $$.val = &BinaryExpr{Operator: Plus, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '-' a_expr
  {
    $$.val = &BinaryExpr{Operator: Minus, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '*' a_expr
  {
    $$.val = &BinaryExpr{Operator: Mult, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '/' a_expr
  {
    $$.val = &BinaryExpr{Operator: Div, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr FLOORDIV a_expr
  {
    $$.val = &BinaryExpr{Operator: FloorDiv, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '%' a_expr
  {
    $$.val = &BinaryExpr{Operator: Mod, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '^' a_expr
  {
    $$.val = &BinaryExpr{Operator: Bitxor, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '#' a_expr
  {
    $$.val = &BinaryExpr{Operator: Bitxor, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '&' a_expr
  {
    $$.val = &BinaryExpr{Operator: Bitand, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '|' a_expr
  {
    $$.val = &BinaryExpr{Operator: Bitor, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '<' a_expr
  {
    $$.val = &ComparisonExpr{Operator: LT, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '>' a_expr
  {
    $$.val = &ComparisonExpr{Operator: GT, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr '=' a_expr
  {
    $$.val = &ComparisonExpr{Operator: EQ, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr CONCAT a_expr
  {
    $$.val = &BinaryExpr{Operator: Concat, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LSHIFT a_expr
  {
    $$.val = &BinaryExpr{Operator: LShift, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr RSHIFT a_expr
  {
    $$.val = &BinaryExpr{Operator: RShift, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr LESS_EQUALS a_expr
  {
    $$.val = &ComparisonExpr{Operator: LE, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr GREATER_EQUALS a_expr
  {
    $$.val = &ComparisonExpr{Operator: GE, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_EQUALS a_expr
  {
    $$.val = &ComparisonExpr{Operator: NE, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr AND a_expr
  {
    $$.val = &AndExpr{Left: $1.expr(), Right: $3.expr()}
  }
| a_expr OR a_expr
  {
    $$.val = &OrExpr{Left: $1.expr(), Right: $3.expr()}
  }
| NOT a_expr
  {
    $$.val = &NotExpr{Expr: $2.expr()}
  }
| NOT_LA a_expr %prec NOT
  {
    $$.val = &NotExpr{Expr: $2.expr()}
  }
| a_expr LIKE a_expr
  {
    $$.val = &ComparisonExpr{Operator: Like, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_LA LIKE a_expr %prec NOT_LA
  {
    $$.val = &ComparisonExpr{Operator: NotLike, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr ILIKE a_expr
  {
    $$.val = &ComparisonExpr{Operator: ILike, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_LA ILIKE a_expr %prec NOT_LA
  {
    $$.val = &ComparisonExpr{Operator: NotILike, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr SIMILAR TO a_expr %prec SIMILAR
  {
    $$.val = &ComparisonExpr{Operator: SimilarTo, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr NOT_LA SIMILAR TO a_expr %prec NOT_LA
  {
    $$.val = &ComparisonExpr{Operator: NotSimilarTo, Left: $1.expr(), Right: $5.expr()}
  }
| a_expr '~' a_expr
  {
    $$.val = &ComparisonExpr{Operator: RegMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_REGMATCH a_expr
  {
    $$.val = &ComparisonExpr{Operator: NotRegMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr REGIMATCH a_expr
  {
    $$.val = &ComparisonExpr{Operator: RegIMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_REGIMATCH a_expr
  {
    $$.val = &ComparisonExpr{Operator: NotRegIMatch, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr IS NULL %prec IS
  {
    $$.val = &ComparisonExpr{Operator: Is, Left: $1.expr(), Right: DNull}
  }
| a_expr IS NOT NULL %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsNot, Left: $1.expr(), Right: DNull}
  }
| row OVERLAPS row { return unimplemented(sqllex) }
| a_expr IS TRUE %prec IS
  {
    $$.val = &ComparisonExpr{Operator: Is, Left: $1.expr(), Right: MakeDBool(true)}
  }
| a_expr IS NOT TRUE %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsNot, Left: $1.expr(), Right: MakeDBool(true)}
  }
| a_expr IS FALSE %prec IS
  {
    $$.val = &ComparisonExpr{Operator: Is, Left: $1.expr(), Right: MakeDBool(false)}
  }
| a_expr IS NOT FALSE %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsNot, Left: $1.expr(), Right: MakeDBool(false)}
  }
| a_expr IS UNKNOWN %prec IS
  {
    $$.val = &ComparisonExpr{Operator: Is, Left: $1.expr(), Right: DNull}
  }
| a_expr IS NOT UNKNOWN %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsNot, Left: $1.expr(), Right: DNull}
  }
| a_expr IS DISTINCT FROM a_expr %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsDistinctFrom, Left: $1.expr(), Right: $5.expr()}
  }
| a_expr IS NOT DISTINCT FROM a_expr %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsNotDistinctFrom, Left: $1.expr(), Right: $6.expr()}
  }
| a_expr IS OF '(' type_list ')' %prec IS
  {
    $$.val = &IsOfTypeExpr{Expr: $1.expr(), Types: $5.colTypes()}
  }
| a_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$.val = &IsOfTypeExpr{Not: true, Expr: $1.expr(), Types: $6.colTypes()}
  }
| a_expr BETWEEN opt_asymmetric b_expr AND a_expr %prec BETWEEN
  {
    $$.val = &RangeCond{Left: $1.expr(), From: $4.expr(), To: $6.expr()}
  }
| a_expr NOT_LA BETWEEN opt_asymmetric b_expr AND a_expr %prec NOT_LA
  {
    $$.val = &RangeCond{Not: true, Left: $1.expr(), From: $5.expr(), To: $7.expr()}
  }
| a_expr BETWEEN SYMMETRIC b_expr AND a_expr %prec BETWEEN
  {
    $$.val = &RangeCond{Left: $1.expr(), From: $4.expr(), To: $6.expr()}
  }
| a_expr NOT_LA BETWEEN SYMMETRIC b_expr AND a_expr %prec NOT_LA
  {
    $$.val = &RangeCond{Not: true, Left: $1.expr(), From: $5.expr(), To: $7.expr()}
  }
| a_expr IN in_expr
  {
    $$.val = &ComparisonExpr{Operator: In, Left: $1.expr(), Right: $3.expr()}
  }
| a_expr NOT_LA IN in_expr %prec NOT_LA
  {
    $$.val = &ComparisonExpr{Operator: NotIn, Left: $1.expr(), Right: $4.expr()}
  }
| a_expr subquery_op sub_type d_expr %prec CONCAT
  {
    op := $3.cmpOp()
    subOp := $2.op()
    subOpCmp, ok := subOp.(ComparisonOperator)
    if !ok {
      sqllex.Error(fmt.Sprintf("%s %s <array> is invalid because %q is not a boolean operator",
        subOp, op, subOp))
      return 1
    }
    $$.val = &ComparisonExpr{
      Operator: op,
      SubOperator: subOpCmp,
      Left: $1.expr(),
      Right: $4.expr(),
    }
  }
// | UNIQUE select_with_parens { return unimplemented(sqllex) }

// Restricted expressions
//
// b_expr is a subset of the complete expression syntax defined by a_expr.
//
// Presently, AND, NOT, IS, and IN are the a_expr keywords that would cause
// trouble in the places where b_expr is used. For simplicity, we just
// eliminate all the boolean-keyword-operator productions from b_expr.
b_expr:
  c_expr
| b_expr TYPECAST cast_target
  {
    $$.val = &CastExpr{Expr: $1.expr(), Type: $3.castTargetType(), syntaxMode: castShort}
  }
| b_expr TYPEANNOTATE typename
  {
    $$.val = &AnnotateTypeExpr{Expr: $1.expr(), Type: $3.colType(), syntaxMode: annotateShort}
  }
| '+' b_expr %prec UMINUS
  {
    $$.val = &UnaryExpr{Operator: UnaryPlus, Expr: $2.expr()}
  }
| '-' b_expr %prec UMINUS
  {
    $$.val = &UnaryExpr{Operator: UnaryMinus, Expr: $2.expr()}
  }
| '~' b_expr %prec UMINUS
  {
    $$.val = &UnaryExpr{Operator: UnaryComplement, Expr: $2.expr()}
  }
| b_expr '+' b_expr
  {
    $$.val = &BinaryExpr{Operator: Plus, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '-' b_expr
  {
    $$.val = &BinaryExpr{Operator: Minus, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '*' b_expr
  {
    $$.val = &BinaryExpr{Operator: Mult, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '/' b_expr
  {
    $$.val = &BinaryExpr{Operator: Div, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr FLOORDIV b_expr
  {
    $$.val = &BinaryExpr{Operator: FloorDiv, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '%' b_expr
  {
    $$.val = &BinaryExpr{Operator: Mod, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '^' b_expr
  {
    $$.val = &BinaryExpr{Operator: Bitxor, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '#' b_expr
  {
    $$.val = &BinaryExpr{Operator: Bitxor, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '&' b_expr
  {
    $$.val = &BinaryExpr{Operator: Bitand, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '|' b_expr
  {
    $$.val = &BinaryExpr{Operator: Bitor, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '<' b_expr
  {
    $$.val = &ComparisonExpr{Operator: LT, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '>' b_expr
  {
    $$.val = &ComparisonExpr{Operator: GT, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr '=' b_expr
  {
    $$.val = &ComparisonExpr{Operator: EQ, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr CONCAT b_expr
  {
    $$.val = &BinaryExpr{Operator: Concat, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr LSHIFT b_expr
  {
    $$.val = &BinaryExpr{Operator: LShift, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr RSHIFT b_expr
  {
    $$.val = &BinaryExpr{Operator: RShift, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr LESS_EQUALS b_expr
  {
    $$.val = &ComparisonExpr{Operator: LE, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr GREATER_EQUALS b_expr
  {
    $$.val = &ComparisonExpr{Operator: GE, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr NOT_EQUALS b_expr
  {
    $$.val = &ComparisonExpr{Operator: NE, Left: $1.expr(), Right: $3.expr()}
  }
| b_expr IS DISTINCT FROM b_expr %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsDistinctFrom, Left: $1.expr(), Right: $5.expr()}
  }
| b_expr IS NOT DISTINCT FROM b_expr %prec IS
  {
    $$.val = &ComparisonExpr{Operator: IsNotDistinctFrom, Left: $1.expr(), Right: $6.expr()}
  }
| b_expr IS OF '(' type_list ')' %prec IS
  {
    $$.val = &IsOfTypeExpr{Expr: $1.expr(), Types: $5.colTypes()}
  }
| b_expr IS NOT OF '(' type_list ')' %prec IS
  {
    $$.val = &IsOfTypeExpr{Not: true, Expr: $1.expr(), Types: $6.colTypes()}
  }

// Productions that can be used in both a_expr and b_expr.
//
// Note: productions that refer recursively to a_expr or b_expr mostly cannot
// appear here. However, it's OK to refer to a_exprs that occur inside
// parentheses, such as function arguments; that cannot introduce ambiguity to
// the b_expr syntax.
c_expr:
  d_expr
| d_expr indirection
  {
    $$.val = &IndirectionExpr{
      Expr: $1.expr(),
      Indirection: $2.unresolvedName(),
    }
  }
| case_expr
| EXISTS select_with_parens
  {
    $$.val = &ExistsExpr{Subquery: &Subquery{Select: $2.selectStmt()}}
  }

// Productions that can be followed by a postfix operator.
//
// Currently we support array indexing (see c_expr above), but these
// are also the expressions which later can be followed with `.xxx` when
// we support field subscripting syntax.
d_expr:
  qualified_name
  {
    $$.val = $1.unresolvedName()
  }
| a_expr_const
| '@' ICONST
  {
    colNum, err := $2.numVal().AsInt64()
    if err != nil {
      sqllex.Error(err.Error())
      return 1
    }
    if colNum < 1 || colNum > int64(MaxInt) {
      sqllex.Error(fmt.Sprintf("invalid column ordinal: @%d", colNum))
      return 1
    }
    $$.val = NewOrdinalReference(int(colNum-1))
  }
| PLACEHOLDER
  {
    $$.val = NewPlaceholder($1)
  }
| '(' a_expr ')'
  {
    $$.val = &ParenExpr{Expr: $2.expr()}
  }
| func_expr
| select_with_parens %prec UMINUS
  {
    $$.val = &Subquery{Select: $1.selectStmt()}
  }
| ARRAY select_with_parens
  {
    $$.val = &ArrayFlatten{Subquery: &Subquery{Select: $2.selectStmt()}}
  }
| ARRAY array_expr
  {
    $$.val = $2.expr()
  }
| explicit_row
  {
    $$.val = $1.expr()
  }
| implicit_row
  {
    $$.val = $1.expr()
  }
// TODO(pmattis): Support this notation?
// | GROUPING '(' expr_list ')' { return unimplemented(sqllex) }

func_application:
  func_name '(' ')'
  {
    $$.val = &FuncExpr{Func: $1.resolvableFunctionReference()}
  }
| func_name '(' expr_list opt_sort_clause ')'
  {
    $$.val = &FuncExpr{Func: $1.resolvableFunctionReference(), Exprs: $3.exprs()}
  }
| func_name '(' VARIADIC a_expr opt_sort_clause ')' { return unimplemented(sqllex) }
| func_name '(' expr_list ',' VARIADIC a_expr opt_sort_clause ')' { return unimplemented(sqllex) }
| func_name '(' ALL expr_list opt_sort_clause ')'
  {
    $$.val = &FuncExpr{Func: $1.resolvableFunctionReference(), Type: AllFuncType, Exprs: $4.exprs()}
  }
| func_name '(' DISTINCT expr_list opt_sort_clause ')'
  {
    $$.val = &FuncExpr{Func: $1.resolvableFunctionReference(), Type: DistinctFuncType, Exprs: $4.exprs()}
  }
| func_name '(' '*' ')'
  {
    $$.val = &FuncExpr{Func: $1.resolvableFunctionReference(), Exprs: Exprs{StarExpr()}}
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
    f := $1.expr().(*FuncExpr)
    f.Filter = $3.expr()
    f.WindowDef = $4.windowDef()
    $$.val = f
  }
| func_expr_common_subexpr
  {
    $$.val = $1.expr()
  }

// As func_expr but does not accept WINDOW functions directly (but they can
// still be contained in arguments for functions etc). Use this when window
// expressions are not allowed, where needed to disambiguate the grammar
// (e.g. in CREATE INDEX).
func_expr_windowless:
  func_application { return unimplemented(sqllex) }
| func_expr_common_subexpr { return unimplemented(sqllex) }

// Special expressions that are considered to be functions.
func_expr_common_subexpr:
  COLLATION FOR '(' a_expr ')' { return unimplemented(sqllex) }
| CURRENT_DATE
  {
    $$.val = &FuncExpr{Func: wrapFunction($1)}
  }
| CURRENT_DATE '(' ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1)}
  }
| CURRENT_TIMESTAMP
  {
    $$.val = &FuncExpr{Func: wrapFunction($1)}
  }
| CURRENT_TIMESTAMP '(' ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1)}
  }
| CURRENT_ROLE { return unimplemented(sqllex) }
| CURRENT_USER { return unimplemented(sqllex) }
| SESSION_USER { return unimplemented(sqllex) }
| USER { return unimplemented(sqllex) }
| CAST '(' a_expr AS cast_target ')'
  {
    $$.val = &CastExpr{Expr: $3.expr(), Type: $5.castTargetType(), syntaxMode: castExplicit}
  }
| ANNOTATE_TYPE '(' a_expr ',' typename ')'
  {
    $$.val = &AnnotateTypeExpr{Expr: $3.expr(), Type: $5.colType(), syntaxMode: annotateExplicit}
  }
| EXTRACT '(' extract_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1), Exprs: $3.exprs()}
  }
| EXTRACT_DURATION '(' extract_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1), Exprs: $3.exprs()}
  }
| OVERLAY '(' overlay_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1), Exprs: $3.exprs()}
  }
| POSITION '(' position_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction("STRPOS"), Exprs: $3.exprs()}
  }
| SUBSTRING '(' substr_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1), Exprs: $3.exprs()}
  }
| TREAT '(' a_expr AS typename ')' { return unimplemented(sqllex) }
| TRIM '(' BOTH trim_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction("BTRIM"), Exprs: $4.exprs()}
  }
| TRIM '(' LEADING trim_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction("LTRIM"), Exprs: $4.exprs()}
  }
| TRIM '(' TRAILING trim_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction("RTRIM"), Exprs: $4.exprs()}
  }
| TRIM '(' trim_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction("BTRIM"), Exprs: $3.exprs()}
  }
| IF '(' a_expr ',' a_expr ',' a_expr ')'
  {
    $$.val = &IfExpr{Cond: $3.expr(), True: $5.expr(), Else: $7.expr()}
  }
| NULLIF '(' a_expr ',' a_expr ')'
  {
    $$.val = &NullIfExpr{Expr1: $3.expr(), Expr2: $5.expr()}
  }
| IFNULL '(' a_expr ',' a_expr ')'
  {
    $$.val = &CoalesceExpr{Name: "IFNULL", Exprs: Exprs{$3.expr(), $5.expr()}}
  }
| COALESCE '(' expr_list ')'
  {
    $$.val = &CoalesceExpr{Name: "COALESCE", Exprs: $3.exprs()}
  }
| GREATEST '(' expr_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1), Exprs: $3.exprs()}
  }
| LEAST '(' expr_list ')'
  {
    $$.val = &FuncExpr{Func: wrapFunction($1), Exprs: $3.exprs()}
  }

// Aggregate decoration clauses
within_group_clause:
WITHIN GROUP '(' sort_clause ')' { return unimplemented(sqllex) }
| /* EMPTY */ {}

filter_clause:
  FILTER '(' WHERE a_expr ')'
  {
    $$.val = $4.expr()
  }
| /* EMPTY */
  {
    $$.val = Expr(nil)
  }

// Window Definitions
window_clause:
  WINDOW window_definition_list
  {
    $$.val = $2.window()
  }
| /* EMPTY */
  {
    $$.val = Window(nil)
  }

window_definition_list:
  window_definition
  {
    $$.val = Window{$1.windowDef()}
  }
| window_definition_list ',' window_definition
  {
    $$.val = append($1.window(), $3.windowDef())
  }

window_definition:
  name AS window_specification
  {
    n := $3.windowDef()
    n.Name = Name($1)
    $$.val = n
  }

over_clause:
  OVER window_specification
  {
    $$.val = $2.windowDef()
  }
| OVER name
  {
    $$.val = &WindowDef{Name: Name($2)}
  }
| /* EMPTY */
  {
    $$.val = (*WindowDef)(nil)
  }

window_specification:
  '(' opt_existing_window_name opt_partition_clause
    opt_sort_clause opt_frame_clause ')'
  {
    $$.val = &WindowDef{
      RefName: Name($2),
      Partitions: $3.exprs(),
      OrderBy: $4.orderBy(),
    }
  }

// If we see PARTITION, RANGE, or ROWS as the first token after the '(' of a
// window_specification, we want the assumption to be that there is no
// existing_window_name; but those keywords are unreserved and so could be
// names. We fix this by making them have the same precedence as IDENT and
// giving the empty production here a slightly higher precedence, so that the
// shift/reduce conflict is resolved in favor of reducing the rule. These
// keywords are thus precluded from being an existing_window_name but are not
// reserved for any other purpose.
opt_existing_window_name:
  name
| /* EMPTY */ %prec CONCAT
  {
    $$ = ""
  }

opt_partition_clause:
  PARTITION BY expr_list
  {
    $$.val = $3.exprs()
  }
| /* EMPTY */
  {
    $$.val = Exprs(nil)
  }

// For frame clauses, we return a WindowDef, but only some fields are used:
// frameOptions, startOffset, and endOffset.
//
// This is only a subset of the full SQL:2008 frame_clause grammar. We don't
// support <window frame exclusion> yet.
opt_frame_clause:
  RANGE frame_extent { return unimplemented(sqllex) }
| ROWS frame_extent { return unimplemented(sqllex) }
| /* EMPTY */ {}

frame_extent:
  frame_bound { return unimplemented(sqllex) }
| BETWEEN frame_bound AND frame_bound { return unimplemented(sqllex) }

// This is used for both frame start and frame end, with output set up on the
// assumption it's frame start; the frame_extent productions must reject
// invalid cases.
frame_bound:
  UNBOUNDED PRECEDING { return unimplemented(sqllex) }
| UNBOUNDED FOLLOWING { return unimplemented(sqllex) }
| CURRENT ROW { return unimplemented(sqllex) }
| a_expr PRECEDING { return unimplemented(sqllex) }
| a_expr FOLLOWING { return unimplemented(sqllex) }

// Supporting nonterminals for expressions.

// Explicit row production.
//
// SQL99 allows an optional ROW keyword, so we can now do single-element rows
// without conflicting with the parenthesized a_expr production. Without the
// ROW keyword, there must be more than one a_expr inside the parens.
row:
  ROW '(' expr_list ')'
  {
    $$.val = &Tuple{Exprs: $3.exprs(), row: true}
  }
| ROW '(' ')'
  {
    $$.val = &Tuple{Exprs: nil, row: true}
  }
| '(' expr_list ',' a_expr ')'
  {
    $$.val = &Tuple{Exprs: append($2.exprs(), $4.expr())}
  }

explicit_row:
  ROW '(' expr_list ')'
  {
    $$.val = &Tuple{Exprs: $3.exprs(), row: true}
  }
| ROW '(' ')'
  {
    $$.val = &Tuple{Exprs: nil, row: true}
  }

implicit_row:
  '(' expr_list ',' a_expr ')'
  {
    $$.val = &Tuple{Exprs: append($2.exprs(), $4.expr())}
  }

sub_type:
  ANY
  {
    $$.val = Any
  }
| SOME
  {
    $$.val = Some
  }
| ALL
  {
    $$.val = All
  }

math_op:
  '+' { $$.val = Plus  }
| '-' { $$.val = Minus }
| '*' { $$.val = Mult  }
| '/' { $$.val = Div   }
| FLOORDIV { $$.val = FloorDiv }
| '%' { $$.val = Mod    }
| '&' { $$.val = Bitand }
| '|' { $$.val = Bitor  }
| '^' { $$.val = Bitxor }
| '#' { $$.val = Bitxor }
| '<' { $$.val = LT }
| '>' { $$.val = GT }
| '=' { $$.val = EQ }
| LESS_EQUALS    { $$.val = LE }
| GREATER_EQUALS { $$.val = GE }
| NOT_EQUALS     { $$.val = NE }

subquery_op:
  math_op
| LIKE         { $$.val = Like     }
| NOT_LA LIKE  { $$.val = NotLike  }
| ILIKE        { $$.val = ILike    }
| NOT_LA ILIKE { $$.val = NotILike }
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
    $$.val = Exprs{$1.expr()}
  }
| expr_list ',' a_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

type_list:
  typename
  {
    $$.val = []ColumnType{$1.colType()}
  }
| type_list ',' typename
  {
    $$.val = append($1.colTypes(), $3.colType())
  }

array_expr:
  '[' expr_list ']'
  {
    $$.val = &Array{Exprs: $2.exprs()}
  }
| '[' array_expr_list ']'
  {
    $$.val = &Array{Exprs: $2.exprs()}
  }
| '[' ']'
  {
    $$.val = &Array{Exprs: nil}
  }

array_expr_list:
  array_expr
  {
    $$.val = Exprs{$1.expr()}
  }
| array_expr_list ',' array_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

extract_list:
  extract_arg FROM a_expr
  {
    $$.val = Exprs{&StrVal{s: $1}, $3.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
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
    $$.val = Exprs{$1.expr(), $2.expr(), $3.expr(), $4.expr()}
  }
| a_expr overlay_placing substr_from
  {
    $$.val = Exprs{$1.expr(), $2.expr(), $3.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

overlay_placing:
  PLACING a_expr
  {
    $$.val = $2.expr()
  }

// position_list uses b_expr not a_expr to avoid conflict with general IN
position_list:
  b_expr IN b_expr
  {
    $$.val = Exprs{$3.expr(), $1.expr()}
  }
| /* EMPTY */
  {
    $$.val = Exprs(nil)
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
    $$.val = Exprs{$1.expr(), $2.expr(), $3.expr()}
  }
| a_expr substr_for substr_from
  {
    $$.val = Exprs{$1.expr(), $3.expr(), $2.expr()}
  }
| a_expr substr_from
  {
    $$.val = Exprs{$1.expr(), $2.expr()}
  }
| a_expr substr_for
  {
    $$.val = Exprs{$1.expr(), NewDInt(1), $2.expr()}
  }
| expr_list
  {
    $$.val = $1.exprs()
  }
| /* EMPTY */
  {
    $$.val = Exprs(nil)
  }

substr_from:
  FROM a_expr
  {
    $$.val = $2.expr()
  }

substr_for:
  FOR a_expr
  {
    $$.val = $2.expr()
  }

trim_list:
  a_expr FROM expr_list
  {
    $$.val = append($3.exprs(), $1.expr())
  }
| FROM expr_list
  {
    $$.val = $2.exprs()
  }
| expr_list
  {
    $$.val = $1.exprs()
  }

in_expr:
  select_with_parens
  {
    $$.val = &Subquery{Select: $1.selectStmt()}
  }
| '(' expr_list ')'
  {
    $$.val = &Tuple{Exprs: $2.exprs()}
  }

// Define SQL-style CASE clause.
// - Full specification
//      CASE WHEN a = b THEN c ... ELSE d END
// - Implicit argument
//      CASE a WHEN b THEN c ... ELSE d END
case_expr:
  CASE case_arg when_clause_list case_default END
  {
    $$.val = &CaseExpr{Expr: $2.expr(), Whens: $3.whens(), Else: $4.expr()}
  }

when_clause_list:
  // There must be at least one
  when_clause
  {
    $$.val = []*When{$1.when()}
  }
| when_clause_list when_clause
  {
    $$.val = append($1.whens(), $2.when())
  }

when_clause:
  WHEN a_expr THEN a_expr
  {
    $$.val = &When{Cond: $2.expr(), Val: $4.expr()}
  }

case_default:
  ELSE a_expr
  {
    $$.val = $2.expr()
  }
| /* EMPTY */
  {
    $$.val = Expr(nil)
  }

case_arg:
  a_expr
| /* EMPTY */
  {
    $$.val = Expr(nil)
  }

indirection_elem:
  '[' a_expr ']'
  {
    $$.val = &ArraySubscript{Begin: $2.expr()}
  }
| '[' opt_slice_bound ':' opt_slice_bound ']'
  {
    $$.val = &ArraySubscript{Begin: $2.expr(), End: $4.expr(), Slice: true}
  }

opt_slice_bound:
  a_expr
| /*EMPTY*/
  {
    $$.val = Expr(nil)
  }

name_indirection:
  '.' unrestricted_name
  {
    $$.val = Name($2)
  }

glob_indirection:
  '.' '*'
  {
    $$.val = UnqualifiedStar{}
  }

name_indirection_elem:
  glob_indirection
  {
    $$.val = $1.namePart()
  }
| name_indirection
  {
    $$.val = $1.namePart()
  }

qname_indirection:
  name_indirection_elem
  {
    $$.val = UnresolvedName{$1.namePart()}
  }
| qname_indirection name_indirection_elem
  {
    $$.val = append($1.unresolvedName(), $2.namePart())
  }

indirection:
  indirection_elem
  {
    $$.val = UnresolvedName{$1.namePart()}
  }
| indirection indirection_elem
  {
    $$.val = append($1.unresolvedName(), $2.namePart())
  }

opt_indirection:
  /* EMPTY */
  {
    $$.val = UnresolvedName(nil)
  }
| opt_indirection indirection_elem
  {
    $$.val = append($1.unresolvedName(), $2.namePart())
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
    $$.val = DefaultVal{}
  }

ctext_expr_list:
  ctext_expr
  {
    $$.val = Exprs{$1.expr()}
  }
| ctext_expr_list ',' ctext_expr
  {
    $$.val = append($1.exprs(), $3.expr())
  }

// We should allow ROW '(' ctext_expr_list ')' too, but that seems to require
// making VALUES a fully reserved word, which will probably break more apps
// than allowing the noise-word is worth.
ctext_row:
  '(' ctext_expr_list ')'
  {
    $$.val = $2.exprs()
  }

target_list:
  target_elem
  {
    $$.val = SelectExprs{$1.selExpr()}
  }
| target_list ',' target_elem
  {
    $$.val = append($1.selExprs(), $3.selExpr())
  }

target_elem:
  a_expr AS unrestricted_name
  {
    $$.val = SelectExpr{Expr: $1.expr(), As: Name($3)}
  }
  // We support omitting AS only for column labels that aren't any known
  // keyword. There is an ambiguity against postfix operators: is "a ! b" an
  // infix expression, or a postfix expression and a column label?  We prefer
  // to resolve this as an infix expression, which we accomplish by assigning
  // IDENT a precedence higher than POSTFIXOP.
| a_expr IDENT
  {
    $$.val = SelectExpr{Expr: $1.expr(), As: Name($2)}
  }
| a_expr
  {
    $$.val = SelectExpr{Expr: $1.expr()}
  }
| '*'
  {
    $$.val = starSelectExpr()
  }

// Names and constants.

qualified_name_list:
  qualified_name
  {
    $$.val = UnresolvedNames{$1.unresolvedName()}
  }
| qualified_name_list ',' qualified_name
  {
    $$.val = append($1.unresolvedNames(), $3.unresolvedName())
  }

table_name_with_index_list:
  table_name_with_index
  {
    $$.val = TableNameWithIndexList{$1.tableWithIdx()}
  }
| table_name_with_index_list ',' table_name_with_index
  {
    $$.val = append($1.tableWithIdxList(), $3.tableWithIdx())
  }

table_pattern_list:
  table_pattern
  {
    $$.val = TablePatterns{$1.unresolvedName()}
  }
| table_pattern_list ',' table_pattern
  {
    $$.val = append($1.tablePatterns(), $3.unresolvedName())
  }

// The production for a qualified relation name has to exactly match the
// production for a qualified func_name, because in a FROM clause we cannot
// tell which we are parsing until we see what comes after it ('(' for a
// func_name, something else for a relation). Therefore we allow 'indirection'
// which may contain subscripts, and reject that case in the C code.
qualified_name:
  name
  {
    $$.val = UnresolvedName{Name($1)}
  }
| name qname_indirection
  {
    $$.val = append(UnresolvedName{Name($1)}, $2.unresolvedName()...)
  }

table_name_with_index:
  qualified_name '@' name
  {
    $$.val = &TableNameWithIndex{Table: $1.normalizableTableName(), Index: Name($3)}
  }
| qualified_name
  {
    $$.val = &TableNameWithIndex{Table: $1.normalizableTableName(), SearchTable: true}
  }

// table_pattern accepts:
// <database>.<table>
// <database>.*
// <table>
// *
table_pattern:
  name
  {
    $$.val = UnresolvedName{Name($1)}
  }
| '*'
  {
    $$.val = UnresolvedName{UnqualifiedStar{}}
  }
| name name_indirection
  {
    $$.val = UnresolvedName{Name($1), $2.namePart()}
  }
| name glob_indirection
  {
    $$.val = UnresolvedName{Name($1), $2.namePart()}
  }

name_list:
  name
  {
    $$.val = NameList{Name($1)}
  }
| name_list ',' name
  {
    $$.val = append($1.nameList(), Name($3))
  }

opt_name_list:
  '(' name_list ')'
  {
    $$.val = $2.nameList()
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
    $$.val = UnresolvedName{Name($1)}
  }
| name qname_indirection
  {
    $$.val = append(UnresolvedName{Name($1)}, $2.unresolvedName()...)
  }

// Constants
a_expr_const:
  ICONST
  {
    $$.val = $1.numVal()
  }
| FCONST
  {
    $$.val = $1.numVal()
  }
| SCONST
  {
    $$.val = &StrVal{s: $1}
  }
| BCONST
  {
    $$.val = &StrVal{s: $1, bytesEsc: true}
  }
| func_name '(' expr_list opt_sort_clause ')' SCONST { return unimplemented(sqllex) }
| const_typename SCONST
  {
    $$.val = &CastExpr{Expr: &StrVal{s: $2}, Type: $1.colType(), syntaxMode: castPrepend}
  }
| const_interval SCONST opt_interval
  {
    $$.val = &CastExpr{Expr: &StrVal{s: $2}, Type: $1.colType(), syntaxMode: castPrepend}
  }
| const_interval '(' ICONST ')' SCONST
  {
    $$.val = &CastExpr{Expr: &StrVal{s: $5}, Type: $1.colType(), syntaxMode: castPrepend}
  }
| TRUE
  {
    $$.val = MakeDBool(true)
  }
| FALSE
  {
    $$.val = MakeDBool(false)
  }
| NULL
  {
    $$.val = DNull
  }

signed_iconst:
  ICONST
| '+' ICONST
  {
    $$.val = $2.numVal()
  }
| '-' ICONST
  {
    $$.val = &NumVal{Value: constant.UnaryOp(token.SUB, $2.numVal().Value, 0)}
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

opt_name_parens:
  '(' name ')'
  {
    $$ = $2
  }
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

// Unrestricted name --- allowed labels in "AS" clauses. This presently
// includes *all* Postgres keywords.
unrestricted_name:
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
| BACKUP
| BEGIN
| BLOB
| BY
| CASCADE
| COLUMNS
| COMMIT
| COMMITTED
| CONFLICT
| CONSTRAINTS
| COPY
| COVERING
| CUBE
| CURRENT
| CYCLE
| DATA
| DATABASE
| DATABASES
| DAY
| DEALLOCATE
| DELETE
| DOUBLE
| DROP
| ENCODING
| EXECUTE
| EXPLAIN
| FILTER
| FIRST
| FOLLOWING
| FORCE_INDEX
| GRANTS
| HELP
| HIGH
| HOUR
| INCREMENTAL
| INDEXES
| INSERT
| INTERLEAVE
| ISOLATION
| KEY
| KEYS
| LC_COLLATE
| LC_CTYPE
| LEVEL
| LOCAL
| LOW
| MATCH
| MINUTE
| MONTH
| NAMES
| NEXT
| NO
| NORMAL
| NOTHING
| NO_INDEX_JOIN
| NULLS
| OF
| OFF
| OID
| OPTIONS
| ORDINALITY
| OVER
| PARENT
| PARTIAL
| PARTITION
| PASSWORD
| PRECEDING
| PREPARE
| PRIORITY
| RANGE
| READ
| RECURSIVE
| REF
| REGCLASS
| REGPROC
| REGPROCEDURE
| REGNAMESPACE
| REGTYPE
| RELEASE
| RENAME
| REPEATABLE
| RESTORE
| RESTRICT
| REVOKE
| ROLLBACK
| ROLLUP
| ROWS
| STATUS
| SAVEPOINT
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
| STDIN
| STORING
| STRICT
| SPLIT
| SYSTEM
| TABLES
| TEMPLATE
| TEXT
| TRANSACTION
| TRUNCATE
| TYPE
| UNBOUNDED
| UNCOMMITTED
| UNKNOWN
| UPDATE
| UPSERT
| USERS
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
  ANNOTATE_TYPE
| BETWEEN
| BIGINT
| BIGSERIAL
| BIT
| BOOL
| BOOLEAN
| BYTEA
| BYTES
| CHAR
| CHARACTER
| CHARACTERISTICS
| COALESCE
| DATE
| DEC
| DECIMAL
| EXISTS
| EXTRACT
| EXTRACT_DURATION
| FLOAT
| GREATEST
| GROUPING
| IF
| IFNULL
| INT
| INT8
| INT64
| INTEGER
| INTERVAL
| LEAST
| NAME
| NULLIF
| NUMERIC
| OUT
| OVERLAY
| POSITION
| PRECISION
| REAL
| ROW
| SERIAL
| SMALLINT
| SMALLSERIAL
| STRING
| SUBSTRING
| TIME
| TIMESTAMP
| TIMESTAMPTZ
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
| ILIKE
| IS
| JOIN
| LEFT
| LIKE
| NATURAL
| OUTER
| OVERLAPS
| RIGHT
| SIMILAR

// Reserved keyword --- these keywords are usable only as a unrestricted_name.
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
| FAMILY
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
| VIEW
| WHEN
| WHERE
| WINDOW
| WITH

%%
