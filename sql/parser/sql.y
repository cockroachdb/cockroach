// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

%{
package parser

import "strings"

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Tokenizer).ParseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
  yylex.(*Tokenizer).AllowComments = allow
}

func forceEOF(yylex interface{}) {
  yylex.(*Tokenizer).ForceEOF = true
}

%}

%union {
  empty       struct{}
  statement   Statement
  selStmt     SelectStatement
  byt         byte
  str         string
  str2        []string
  selectExprs SelectExprs
  selectExpr  SelectExpr
  columns     Columns
  colName     *ColName
  tableExprs  TableExprs
  tableExpr   TableExpr
  smTableExpr SimpleTableExpr
  tableName   *TableName
  indexHints  *IndexHints
  expr        Expr
  boolExpr    BoolExpr
  valExpr     ValExpr
  tuple       Tuple
  valExprs    ValExprs
  values      Values
  subquery    *Subquery
  caseExpr    *CaseExpr
  whens       []*When
  when        *When
  orderBy     OrderBy
  order       *Order
  limit       *Limit
  insRows     InsertRows
  updateExprs UpdateExprs
  updateExpr  *UpdateExpr
}

%token tokLexError
%token <empty> tokSelect tokInsert tokUpdate tokDelete tokFrom tokWhere tokGroup tokHaving tokOrder tokBy tokLimit tokOffset tokFor
%token <empty> tokAll tokDistinct tokAs tokExists tokIn tokIs tokLike tokBetween tokNull tokAsc tokDesc tokValues tokInto tokDuplicate tokKey tokDefault tokSet tokLock
%token <str> tokID tokString tokNumber tokValueArg tokComment
%token <empty> tokLE tokGE tokNE tokNullSafeEqual
%token <empty> '(' '=' '<' '>' '~'

%left <empty> tokUnion tokMinus tokExcept tokIntersect
%left <empty> ','
%left <empty> tokJoin tokStraightJoin tokLeft tokRight tokInner tokOuter tokCross tokNatural tokUse tokForce
%left <empty> tokOn tokUsing
%left <str> tokAnd tokOr
%right <str> tokNot
%left <empty> '&' '|' '^'
%left <empty> '+' '-'
%left <empty> '*' '/' '%'
%nonassoc <empty> '.'
%left <empty> tokUnary
%right <empty> tokCase, tokWhen, tokThen, tokElse
%left <empty> tokEnd

// DDL Tokens
%token <empty> tokCreate tokAlter tokDrop tokRename tokTruncate tokShow
%token <empty> tokDatabase tokTable tokTables tokIndex tokView tokColumns tokFull tokTo tokIgnore tokIf tokUnique

%start any_command

%type <statement> command
%type <selStmt> select_statement
%type <statement> insert_statement update_statement delete_statement set_statement use_statement show_statement
%type <statement> create_statement alter_statement rename_statement truncate_statement drop_statement
%type <str2> comment_opt comment_list
%type <str> union_op
%type <str> distinct_opt
%type <selectExprs> select_expression_list
%type <selectExpr> select_expression
%type <str> as_lower_opt as_opt
%type <expr> expression
%type <tableExprs> table_expression_list
%type <tableExpr> table_expression
%type <str> join_type
%type <smTableExpr> simple_table_expression
%type <tableName> dml_table_expression
%type <indexHints> index_hint_list
%type <str2> index_list
%type <boolExpr> where_expression_opt
%type <boolExpr> boolean_expression condition
%type <str> compare
%type <insRows> row_list
%type <valExpr> value value_expression
%type <tuple> tuple
%type <valExprs> value_expression_list
%type <values> tuple_list
%type <str> keyword_as_func
%type <subquery> subquery
%type <byt> unary_operator
%type <colName> column_name
%type <caseExpr> case_expression
%type <whens> when_expression_list
%type <when> when_expression
%type <valExpr> value_expression_opt else_expression_opt
%type <valExprs> group_by_opt
%type <boolExpr> having_opt
%type <orderBy> order_by_opt order_list
%type <order> order
%type <str> asc_desc_opt
%type <limit> limit_opt
%type <str> lock_opt
%type <columns> column_list_opt column_list
%type <updateExprs> on_dup_opt
%type <updateExprs> update_list
%type <updateExpr> update_expression
%type <empty> exists_opt not_exists_opt ignore_opt non_rename_operation to_opt constraint_opt using_opt
%type <str> sql_id
%type <empty> force_eof

%%

any_command:
  command
  {
    setParseTree(yylex, $1)
  }

command:
  select_statement
  {
    $$ = $1
  }
| insert_statement
| update_statement
| delete_statement
| set_statement
| use_statement
| show_statement
| create_statement
| alter_statement
| rename_statement
| truncate_statement
| drop_statement

select_statement:
  tokSelect comment_opt distinct_opt select_expression_list tokFrom table_expression_list where_expression_opt group_by_opt having_opt order_by_opt limit_opt lock_opt
  {
    $$ = &Select{Comments: Comments($2), Distinct: $3, Exprs: $4, From: $6, Where: NewWhere(astWhere, $7), GroupBy: GroupBy($8), Having: NewWhere(astHaving, $9), OrderBy: $10, Limit: $11, Lock: $12}
  }
| select_statement union_op select_statement %prec tokUnion
  {
    $$ = &Union{Type: $2, Left: $1, Right: $3}
  }

insert_statement:
  tokInsert comment_opt tokInto dml_table_expression column_list_opt row_list on_dup_opt
  {
    $$ = &Insert{Comments: Comments($2), Table: $4, Columns: $5, Rows: $6, OnDup: OnDup($7)}
  }
| tokInsert comment_opt tokInto dml_table_expression tokSet update_list on_dup_opt
  {
    cols := make(Columns, 0, len($6))
    vals := make(ValTuple, 0, len($6))
    for _, col := range $6 {
      cols = append(cols, &NonStarExpr{Expr: col.Name})
      vals = append(vals, col.Expr)
    }
    $$ = &Insert{Comments: Comments($2), Table: $4, Columns: cols, Rows: Values{vals}, OnDup: OnDup($7)}
  }

update_statement:
  tokUpdate comment_opt dml_table_expression tokSet update_list where_expression_opt order_by_opt limit_opt
  {
    $$ = &Update{Comments: Comments($2), Table: $3, Exprs: $5, Where: NewWhere(astWhere, $6), OrderBy: $7, Limit: $8}
  }

delete_statement:
  tokDelete comment_opt tokFrom dml_table_expression where_expression_opt order_by_opt limit_opt
  {
    $$ = &Delete{Comments: Comments($2), Table: $4, Where: NewWhere(astWhere, $5), OrderBy: $6, Limit: $7}
  }

set_statement:
  tokSet comment_opt update_list
  {
    $$ = &Set{Comments: Comments($2), Exprs: $3}
  }

use_statement:
  tokUse comment_opt sql_id
  {
    $$ = &Use{Comments: Comments($2), Name: $3}
  }

show_statement:
  tokShow tokTables
  {
    $$ = &DDL{Action: astShowTables}
  }
| tokShow tokIndex tokFrom sql_id
  {
    $$ = &DDL{Action: astShowIndex, Name: $4}
  }
| tokShow tokColumns tokFrom sql_id
  {
    $$ = &DDL{Action: astShowColumns, Name: $4}
  }
| tokShow tokFull tokColumns tokFrom sql_id
  {
    $$ = &DDL{Action: astShowFullColumns, Name: $5}
  }

create_statement:
  tokCreate tokTable not_exists_opt sql_id force_eof
  {
    $$ = &DDL{Action: astCreateTable, NewName: $4}
  }
| tokCreate constraint_opt tokIndex sql_id using_opt tokOn sql_id force_eof
  {
    $$ = &DDL{Action: astCreateIndex, Name: $4, NewName: $7}
  }
| tokCreate tokView sql_id force_eof
  {
    $$ = &DDL{Action: astCreateView, NewName: $3}
  }
| tokCreate tokDatabase not_exists_opt sql_id force_eof
  {
    $$ = &DDL{Action: astCreateDatabase, NewName: $4}
  }

alter_statement:
  tokAlter ignore_opt tokTable tokID non_rename_operation force_eof
  {
    $$ = &DDL{Action: astAlterTable, Name: $4, NewName: $4}
  }
| tokAlter ignore_opt tokTable tokID tokRename to_opt tokID
  {
    // Change this to a rename statement
    $$ = &DDL{Action: astRenameTable, Name: $4, NewName: $7}
  }
| tokAlter tokView sql_id force_eof
  {
    $$ = &DDL{Action: astAlterView, Name: $3, NewName: $3}
  }

rename_statement:
  tokRename tokTable tokID tokTo tokID
  {
    $$ = &DDL{Action: astRenameTable, Name: $3, NewName: $5}
  }

truncate_statement:
  tokTruncate tokTable tokID
  {
    $$ = &DDL{Action: astTruncateTable, Name: $3}
  }

drop_statement:
  tokDrop tokTable exists_opt sql_id
  {
    $$ = &DDL{Action: astDropTable, Name: $4}
  }
| tokDrop tokIndex sql_id tokOn sql_id
  {
    $$ = &DDL{Action: astDropIndex, Name: $3, NewName: $5}
  }
| tokDrop tokView exists_opt sql_id force_eof
  {
    $$ = &DDL{Action: astDropView, Name: $4}
  }
| tokDrop tokDatabase exists_opt sql_id force_eof
  {
    $$ = &DDL{Action: astDropDatabase, Name: $4}
  }

comment_opt:
  {
    setAllowComments(yylex, true)
  }
  comment_list
  {
    $$ = $2
    setAllowComments(yylex, false)
  }

comment_list:
  {
    $$ = nil
  }
| comment_list tokComment
  {
    $$ = append($1, $2)
  }

union_op:
  tokUnion
  {
    $$ = astUnion
  }
| tokUnion tokAll
  {
    $$ = astUnionAll
  }
| tokMinus
  {
    $$ = astSetMinus
  }
| tokExcept
  {
    $$ = astExcept
  }
| tokIntersect
  {
    $$ = astIntersect
  }

distinct_opt:
  {
    $$ = ""
  }
| tokDistinct
  {
    $$ = astDistinct
  }

select_expression_list:
  select_expression
  {
    $$ = SelectExprs{$1}
  }
| select_expression_list ',' select_expression
  {
    $$ = append($$, $3)
  }

select_expression:
  '*'
  {
    $$ = &StarExpr{}
  }
| expression as_lower_opt
  {
    $$ = &NonStarExpr{Expr: $1, As: $2}
  }
| tokID '.' '*'
  {
    $$ = &StarExpr{TableName: $1}
  }

expression:
  boolean_expression
  {
    $$ = $1
  }
| value_expression
  {
    $$ = $1
  }

as_lower_opt:
  {
    $$ = ""
  }
| sql_id
  {
    $$ = $1
  }
| tokAs sql_id
  {
    $$ = $2
  }

table_expression_list:
  table_expression
  {
    $$ = TableExprs{$1}
  }
| table_expression_list ',' table_expression
  {
    $$ = append($$, $3)
  }

table_expression:
  simple_table_expression as_opt index_hint_list
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $2, Hints: $3}
  }
| '(' table_expression ')'
  {
    $$ = &ParenTableExpr{Expr: $2}
  }
| table_expression join_type table_expression %prec tokJoin
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3}
  }
| table_expression join_type table_expression tokOn boolean_expression %prec tokOn
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, Cond: &OnJoinCond{$5}}
  }
| table_expression join_type table_expression tokUsing '(' column_list ')' %prec tokUsing
  {
    $$ = &JoinTableExpr{LeftExpr: $1, Join: $2, RightExpr: $3, Cond: &UsingJoinCond{$6}}
  }

as_opt:
  {
    $$ = ""
  }
| tokID
  {
    $$ = $1
  }
| tokAs tokID
  {
    $$ = $2
  }

join_type:
  tokJoin
  {
    $$ = astJoin
  }
| tokStraightJoin
  {
    $$ = astStraightJoin
  }
| tokLeft tokJoin
  {
    $$ = astLeftJoin
  }
| tokLeft tokOuter tokJoin
  {
    $$ = astLeftJoin
  }
| tokRight tokJoin
  {
    $$ = astRightJoin
  }
| tokRight tokOuter tokJoin
  {
    $$ = astRightJoin
  }
| tokInner tokJoin
  {
    $$ = astJoin
  }
| tokCross tokJoin
  {
    $$ = astCrossJoin
  }
| tokNatural tokJoin
  {
    $$ = astNaturalJoin
  }

simple_table_expression:
tokID
  {
    $$ = &TableName{Name: $1}
  }
| tokID '.' tokID
  {
    $$ = &TableName{Qualifier: $1, Name: $3}
  }
| subquery
  {
    $$ = $1
  }

dml_table_expression:
tokID
  {
    $$ = &TableName{Name: $1}
  }
| tokID '.' tokID
  {
    $$ = &TableName{Qualifier: $1, Name: $3}
  }

index_hint_list:
  {
    $$ = nil
  }
| tokUse tokIndex '(' index_list ')'
  {
    $$ = &IndexHints{Type: astUse, Indexes: $4}
  }
| tokIgnore tokIndex '(' index_list ')'
  {
    $$ = &IndexHints{Type: astIgnore, Indexes: $4}
  }
| tokForce tokIndex '(' index_list ')'
  {
    $$ = &IndexHints{Type: astForce, Indexes: $4}
  }

index_list:
  sql_id
  {
    $$ = []string{$1}
  }
| index_list ',' sql_id
  {
    $$ = append($1, $3)
  }

where_expression_opt:
  {
    $$ = nil
  }
| tokWhere boolean_expression
  {
    $$ = $2
  }

boolean_expression:
  condition
| boolean_expression tokAnd boolean_expression
  {
    $$ = &AndExpr{Op: string($2), Left: $1, Right: $3}
  }
| boolean_expression tokOr boolean_expression
  {
    $$ = &OrExpr{Op: string($2), Left: $1, Right: $3}
  }
| tokNot boolean_expression
  {
    $$ = &NotExpr{Op: string($1), Expr: $2}
  }
| '(' boolean_expression ')'
  {
    $$ = &ParenBoolExpr{Expr: $2}
  }

condition:
  value_expression compare value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
  }
| value_expression tokIn tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: astIn, Right: $3}
  }
| value_expression tokNot tokIn tuple
  {
    $$ = &ComparisonExpr{Left: $1, Operator: astNotIn, Right: $4}
  }
| value_expression tokLike value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: astLike, Right: $3}
  }
| value_expression tokNot tokLike value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: astNotLike, Right: $4}
  }
| value_expression tokBetween value_expression tokAnd value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: astBetween, From: $3, To: $5}
  }
| value_expression tokNot tokBetween value_expression tokAnd value_expression
  {
    $$ = &RangeCond{Left: $1, Operator: astNotBetween, From: $4, To: $6}
  }
| value_expression tokIs tokNull
  {
    $$ = &NullCheck{Operator: astIsNull, Expr: $1}
  }
| value_expression tokIs tokNot tokNull
  {
    $$ = &NullCheck{Operator: astIsNotNull, Expr: $1}
  }
| tokExists subquery
  {
    $$ = &ExistsExpr{Subquery: $2}
  }

compare:
  '='
  {
    $$ = astEQ
  }
| '<'
  {
    $$ = astLT
  }
| '>'
  {
    $$ = astGT
  }
| tokLE
  {
    $$ = astLE
  }
| tokGE
  {
    $$ = astGE
  }
| tokNE
  {
    $$ = astNE
  }
| tokNullSafeEqual
  {
    $$ = astNSE
  }

row_list:
  tokValues tuple_list
  {
    $$ = $2
  }
| select_statement
  {
    $$ = $1
  }

tuple_list:
  tuple
  {
    $$ = Values{$1}
  }
| tuple_list ',' tuple
  {
    $$ = append($1, $3)
  }

tuple:
  '(' value_expression_list ')'
  {
    $$ = ValTuple($2)
  }
| subquery
  {
    $$ = $1
  }

subquery:
  '(' select_statement ')'
  {
    $$ = &Subquery{$2}
  }

value_expression_list:
  value_expression
  {
    $$ = ValExprs{$1}
  }
| value_expression_list ',' value_expression
  {
    $$ = append($1, $3)
  }

value_expression:
  value
  {
    $$ = $1
  }
| column_name
  {
    $$ = $1
  }
| tuple
  {
    $$ = $1
  }
| value_expression '&' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astBitand, Right: $3}
  }
| value_expression '|' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astBitor, Right: $3}
  }
| value_expression '^' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astBitxor, Right: $3}
  }
| value_expression '+' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astPlus, Right: $3}
  }
| value_expression '-' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astMinus, Right: $3}
  }
| value_expression '*' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astMult, Right: $3}
  }
| value_expression '/' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astDiv, Right: $3}
  }
| value_expression '%' value_expression
  {
    $$ = &BinaryExpr{Left: $1, Operator: astMod, Right: $3}
  }
| unary_operator value_expression %prec tokUnary
  {
    if num, ok := $2.(NumVal); ok {
      switch $1 {
      case '-':
        $$ = NumVal("-" + string(num))
      case '+':
        $$ = num
      default:
        $$ = &UnaryExpr{Operator: $1, Expr: $2}
      }
    } else {
      $$ = &UnaryExpr{Operator: $1, Expr: $2}
    }
  }
| sql_id '(' ')'
  {
    $$ = &FuncExpr{Name: strings.ToUpper($1)}
  }
| sql_id '(' select_expression_list ')'
  {
    $$ = &FuncExpr{Name: strings.ToUpper($1), Exprs: $3}
  }
| sql_id '(' tokDistinct select_expression_list ')'
  {
    $$ = &FuncExpr{Name: strings.ToUpper($1), Distinct: true, Exprs: $4}
  }
| keyword_as_func '(' select_expression_list ')'
  {
    $$ = &FuncExpr{Name: strings.ToUpper($1), Exprs: $3}
  }
| case_expression
  {
    $$ = $1
  }

keyword_as_func:
  tokIf
  {
    $$ = "IF"
  }
| tokValues
  {
    $$ = "VALUES"
  }

unary_operator:
  '+'
  {
    $$ = astUnaryPlus
  }
| '-'
  {
    $$ = astUnaryMinus
  }
| '~'
  {
    $$ = astTilda
  }

case_expression:
  tokCase value_expression_opt when_expression_list else_expression_opt tokEnd
  {
    $$ = &CaseExpr{Expr: $2, Whens: $3, Else: $4}
  }

value_expression_opt:
  {
    $$ = nil
  }
| value_expression
  {
    $$ = $1
  }

when_expression_list:
  when_expression
  {
    $$ = []*When{$1}
  }
| when_expression_list when_expression
  {
    $$ = append($1, $2)
  }

when_expression:
  tokWhen boolean_expression tokThen value_expression
  {
    $$ = &When{Cond: $2, Val: $4}
  }

else_expression_opt:
  {
    $$ = nil
  }
| tokElse value_expression
  {
    $$ = $2
  }

column_name:
  sql_id
  {
    $$ = &ColName{Name: $1}
  }
| tokID '.' sql_id
  {
    $$ = &ColName{Qualifier: $1, Name: $3}
  }

value:
  tokString
  {
    $$ = StrVal($1)
  }
| tokNumber
  {
    $$ = NumVal($1)
  }
| tokValueArg
  {
    $$ = ValArg($1)
  }
| tokNull
  {
    $$ = &NullVal{}
  }

group_by_opt:
  {
    $$ = nil
  }
| tokGroup tokBy value_expression_list
  {
    $$ = $3
  }

having_opt:
  {
    $$ = nil
  }
| tokHaving boolean_expression
  {
    $$ = $2
  }

order_by_opt:
  {
    $$ = nil
  }
| tokOrder tokBy order_list
  {
    $$ = $3
  }

order_list:
  order
  {
    $$ = OrderBy{$1}
  }
| order_list ',' order
  {
    $$ = append($1, $3)
  }

order:
  value_expression asc_desc_opt
  {
    $$ = &Order{Expr: $1, Direction: $2}
  }

asc_desc_opt:
  {
    $$ = astAsc
  }
| tokAsc
  {
    $$ = astAsc
  }
| tokDesc
  {
    $$ = astDesc
  }

limit_opt:
  {
    $$ = nil
  }
| tokLimit value_expression
  {
    $$ = &Limit{Rowcount: $2}
  }
| tokLimit value_expression ',' value_expression
  {
    $$ = &Limit{Offset: $2, Rowcount: $4}
  }
| tokLimit value_expression tokOffset value_expression
  {
    $$ = &Limit{Offset: $4, Rowcount: $2}
  }

lock_opt:
  {
    $$ = ""
  }
| tokFor tokUpdate
  {
    $$ = astForUpdate
  }
| tokLock tokIn sql_id sql_id
  {
    if $3 != "share" {
      yylex.Error("expecting share")
      return 1
    }
    if $4 != "mode" {
      yylex.Error("expecting mode")
      return 1
    }
    $$ = astShareMode
  }

column_list_opt:
  {
    $$ = nil
  }
| '(' column_list ')'
  {
    $$ = $2
  }

column_list:
  column_name
  {
    $$ = Columns{&NonStarExpr{Expr: $1}}
  }
| column_list ',' column_name
  {
    $$ = append($$, &NonStarExpr{Expr: $3})
  }

on_dup_opt:
  {
    $$ = nil
  }
| tokOn tokDuplicate tokKey tokUpdate update_list
  {
    $$ = $5
  }

update_list:
  update_expression
  {
    $$ = UpdateExprs{$1}
  }
| update_list ',' update_expression
  {
    $$ = append($1, $3)
  }

update_expression:
  column_name '=' value_expression
  {
    $$ = &UpdateExpr{Name: $1, Expr: $3} 
  }

exists_opt:
  { $$ = struct{}{} }
| tokIf tokExists
  { $$ = struct{}{} }

not_exists_opt:
  { $$ = struct{}{} }
| tokIf tokNot tokExists
  { $$ = struct{}{} }

ignore_opt:
  { $$ = struct{}{} }
| tokIgnore
  { $$ = struct{}{} }

non_rename_operation:
  tokAlter
  { $$ = struct{}{} }
| tokDefault
  { $$ = struct{}{} }
| tokDrop
  { $$ = struct{}{} }
| tokOrder
  { $$ = struct{}{} }
| tokID
  { $$ = struct{}{} }

to_opt:
  { $$ = struct{}{} }
| tokTo
  { $$ = struct{}{} }

constraint_opt:
  { $$ = struct{}{} }
| tokUnique
  { $$ = struct{}{} }

using_opt:
  { $$ = struct{}{} }
| tokUsing sql_id
  { $$ = struct{}{} }

sql_id:
  tokID
  {
    $$ = strings.ToLower($1)
  }

force_eof:
{
  forceEOF(yylex)
}
