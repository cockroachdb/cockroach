// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

%{
package parser

import (
  "strconv"
  "strings"
)

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*tokenizer).parseTree = stmt
}

func setAllowComments(yylex interface{}, allow bool) {
  yylex.(*tokenizer).allowComments = allow
}

func forceEOF(yylex interface{}) {
  yylex.(*tokenizer).forceEOF = true
}

func parseInt(yylex yyLexer, s string) (int, bool) {
  i, err := strconv.Atoi(s)
  if err != nil {
    yylex.Error(err.Error())
    return -1, false
  }
  return i, true
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
  tableDefs   TableDefs
  tableDef    TableDef
  columnType  ColumnType
  intVal      int
  intVal2     [2]int
  boolVal     bool
}

%token tokLexError
%token <empty> tokSelect tokInsert tokUpdate tokDelete tokFrom tokWhere tokGroup tokHaving tokOrder tokBy tokLimit tokOffset tokFor
%token <empty> tokAll tokDistinct tokAs tokExists tokIn tokIs tokLike tokBetween tokNull tokAsc tokDesc tokValues tokInto tokDuplicate tokKey tokDefault tokSet tokLock
%token tokInt tokTinyInt tokSmallInt tokMediumInt tokBigInt tokInteger
%token tokReal tokDouble tokFloat tokDecimal tokNumeric
%token tokDate tokTime tokDateTime tokTimestamp
%token tokChar tokVarChar tokBinary tokVarBinary
%token tokText tokTinyText tokMediumText tokLongText
%token tokBlob tokTinyBlob tokMediumBlob tokLongBlob
%token tokBit tokEnum

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
%token <empty> tokDatabase tokDatabases tokTable tokTables tokIndex tokView tokColumns tokFull tokTo tokIgnore tokIf tokUnique tokUnsigned tokPrimary

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
%type <tableName> dml_table_expression ddl_table_expression
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
%type <empty> ignore_opt non_rename_operation to_opt using_opt
%type <boolVal> unsigned_opt if_exists_opt if_not_exists_opt unique_opt
%type <str> from_opt
%type <intVal> int_opt int_val precision_opt
%type <intVal2> float_opt decimal_opt 
%type <str> sql_id
%type <tableDefs> table_def_list
%type <tableDef> table_def
%type <columnType> column_type
%type <str> int_type float_type decimal_type char_type binary_type text_type blob_type
%type <intVal> column_null_opt column_constraint_opt
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
  tokShow tokDatabases
  {
    $$ = &ShowDatabases{}
  }
| tokShow tokTables from_opt
  {
    $$ = &ShowTables{Name: $3}
  }
| tokShow tokIndex tokFrom sql_id
  {
    $$ = &ShowIndex{Name: $4}
  }
| tokShow tokColumns tokFrom sql_id
  {
    $$ = &ShowColumns{Name: $4}
  }
| tokShow tokFull tokColumns tokFrom sql_id
  {
    $$ = &ShowColumns{Name: $5, Full: true}
  }

create_statement:
  tokCreate tokTable if_not_exists_opt ddl_table_expression '(' table_def_list ')' force_eof
  {
    $$ = &CreateTable{IfNotExists: $3, Name: $4, Defs: $6}
  }
| tokCreate unique_opt tokIndex sql_id using_opt tokOn sql_id force_eof
  {
    $$ = &CreateIndex{Name: $4, TableName: $7, Unique: $2}
  }
| tokCreate tokView sql_id force_eof
  {
    $$ = &CreateView{Name: $3}
  }
| tokCreate tokDatabase if_not_exists_opt sql_id
  {
    $$ = &CreateDatabase{IfNotExists: $3, Name: $4}
  }

table_def_list:
  table_def
  {
    $$ = TableDefs{$1}
  }
| table_def_list ',' table_def
  {
    $$ = append($$, $3)
  }

table_def:
  sql_id column_type column_null_opt column_constraint_opt
  {
    $$ = &ColumnTableDef{Name: $1, Type: $2, Nullable: Nullability($3), PrimaryKey: $4 == 1, Unique: $4 == 2}
  }
| unique_opt tokIndex sql_id '(' index_list ')'
  {
    $$ = &IndexTableDef{Name: $3, Unique: $1, Columns: $5}
  }
| tokPrimary tokKey '(' index_list ')'
  {
    $$ = &IndexTableDef{Name: "primary", PrimaryKey: true, Unique: true, Columns: $4}
  }

column_type:
  tokBit int_opt
  { $$ = &BitType{N: $2} }
| int_type int_opt unsigned_opt
  { $$ = &IntType{Name: $1, N: $2, Unsigned: $3} }
| float_type float_opt unsigned_opt
  { $$ = &FloatType{Name: $1, N: $2[0], Prec: $2[1], Unsigned: $3} }
| decimal_type decimal_opt
  { $$ = &DecimalType{Name: $1, N: $2[0], Prec: $2[1]} }
| tokDate
  { $$ = &DateType{} }
| tokTime
  { $$ = &TimeType{} }
| tokDateTime
  { $$ = &DateTimeType{} }
| tokTimestamp
  { $$ = &TimestampType{} }
| char_type int_opt
  { $$ = &CharType{Name: $1, N: $2} }
| binary_type int_opt
  { $$ = &BinaryType{Name: $1, N: $2} }
| text_type
  { $$ = &TextType{Name: $1} }
| blob_type
  { $$ = &BlobType{Name: $1} }
| tokEnum '(' index_list ')'
  { $$ = &EnumType{Vals: $3} }
| tokSet '(' index_list ')'
  { $$ = &SetType{Vals: $3} }

int_type:
  tokInt
  { $$ = astInt }
| tokTinyInt
  { $$ = astTinyInt }
| tokSmallInt
  { $$ = astSmallInt }
| tokMediumInt
  { $$ = astMediumInt }
| tokBigInt
  { $$ = astBigInt }
| tokInteger
  { $$ = astInteger }

float_type:
  tokReal
  { $$ = astReal }
| tokDouble
  { $$ = astDouble }
| tokFloat
  { $$ = astFloat }

decimal_type:
  tokDecimal
  { $$ = astDecimal }
| tokNumeric
  { $$ = astNumeric }

char_type:
  tokChar
  { $$ = astChar }
| tokVarChar
  { $$ = astVarChar }

binary_type:
  tokBinary
  { $$ = astBinary }
| tokVarBinary
  { $$ = astVarBinary }

text_type:
  tokText
  { $$ = astText }
| tokTinyText
  { $$ = astTinyText }
| tokMediumText
  { $$ = astMediumText }
| tokLongText
  { $$ = astLongText }

blob_type:
  tokBlob
  { $$ = astBlob }
| tokTinyBlob
  { $$ = astTinyBlob }
| tokMediumBlob
  { $$ = astMediumBlob }
| tokLongBlob
  { $$ = astLongBlob }

column_null_opt:
  { $$ = int(SilentNull) }
| tokNull
  { $$ = int(Null) }
| tokNot tokNull
  { $$ = int(NotNull) }

column_constraint_opt:
  { $$ = 0 }
| tokPrimary tokKey
  { $$ = 1 }
| tokKey
  { $$ = 1 }
| tokUnique
  { $$ = 2 }
| tokUnique tokKey
  { $$ = 2 }

alter_statement:
  tokAlter ignore_opt tokTable tokID non_rename_operation force_eof
  {
    $$ = &AlterTable{Name: $4}
  }
| tokAlter ignore_opt tokTable tokID tokRename to_opt tokID
  {
    // Change this to a rename statement
    $$ = &RenameTable{Name: $4, NewName: $7}
  }
| tokAlter tokView sql_id force_eof
  {
    $$ = &AlterView{Name: $3}
  }

rename_statement:
  tokRename tokTable tokID tokTo tokID
  {
    $$ = &RenameTable{Name: $3, NewName: $5}
  }

truncate_statement:
  tokTruncate tokTable tokID
  {
    $$ = &TruncateTable{Name: $3}
  }

drop_statement:
  tokDrop tokTable if_exists_opt sql_id
  {
    $$ = &DropTable{Name: $4, IfExists: $3}
  }
| tokDrop tokIndex sql_id tokOn sql_id
  {
    $$ = &DropIndex{Name: $3, TableName: $5}
  }
| tokDrop tokView if_exists_opt sql_id force_eof
  {
    $$ = &DropView{Name: $4, IfExists: $3}
  }
| tokDrop tokDatabase if_exists_opt sql_id force_eof
  {
    $$ = &DropDatabase{Name: $4, IfExists: $3}
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

ddl_table_expression:
sql_id
  {
    $$ = &TableName{Name: $1}
  }
| sql_id '.' sql_id
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
    $$ = &NullCheck{Operator: astNull, Expr: $1}
  }
| value_expression tokIs tokNot tokNull
  {
    $$ = &NullCheck{Operator: astNotNull, Expr: $1}
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

if_exists_opt:
  { $$ = false }
| tokIf tokExists
  { $$ = true }

if_not_exists_opt:
  { $$ = false }
| tokIf tokNot tokExists
  { $$ = true }

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

unique_opt:
  { $$ = false }
| tokUnique
  { $$ = true }

int_opt:
  { $$ = 0 }
| '(' int_val ')'
  { $$ = $2 }

int_val:
  tokNumber
  {
    i, ok := parseInt(yylex, $1)
    if !ok {
      return 1
    }
    $$ = i
  }

float_opt:
  { $$[0], $$[1] = 0, 0 }
| '(' int_val ',' int_val ')'
  { $$[0], $$[1] = $2, $4 }

decimal_opt:
  { $$[0], $$[1] = 0, 0 }
| '(' int_val precision_opt ')'
  { $$[0], $$[1] = $2, $3 }

precision_opt:
  { $$ = 0 }
| ',' int_val
  { $$ = $2 }

unsigned_opt:
  { $$ = false }
| tokUnsigned
  { $$ = true }

using_opt:
  { $$ = struct{}{} }
| tokUsing sql_id
  { $$ = struct{}{} }

from_opt:
  { $$ = "" }
| tokFrom sql_id
  { $$ = $2 }

sql_id:
  tokID
  { $$ = strings.ToLower($1) }

force_eof:
  { forceEOF(yylex) }
