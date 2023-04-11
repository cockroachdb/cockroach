%{
// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package plsql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)
%}

%union {
	decs []tree.PLDeclaration
	dec tree.PLDeclaration
	stmts []tree.PLStatement
	stmt tree.PLStatement
	typ *types.T
	ident tree.Name
	sql *tree.PLSQLExpr
	str string
}

%token <str> ASSIGNMENT BEGIN BOOL BOOL_ARRAY CONTINUE DECLARE ELSE
%token <str> END EXIT FLOAT FLOAT_ARRAY FOR IDENT IF IN INT INT_ARRAY
%token <str> LOOP RETURN SQL_EXPR TEXT TEXT_ARRAY THEN WHILE

%type <decs> opt_declarations
%type <decs> declarations
%type <dec> declaration
%type <stmts> opt_statements
%type <stmts> statements
%type <stmt> statement
%type <ident> ident
%type <sql> sql_expr
%type <typ> sql_type

%start body

%%

body:
	DECLARE opt_declarations BEGIN opt_statements END
	{
		plsqllex.(*lexer).setResult($2, $4)
	}

opt_declarations:
	declarations ';'
| ';'
	{
		$$ = []tree.PLDeclaration(nil)
	}

declarations:
	declaration
	{
		$$ = []tree.PLDeclaration{$1}
	}
|	declarations ';' declaration
	{
		$$ = append($1, $3)
	}

declaration:
	ident sql_type ASSIGNMENT sql_expr
	{
		$$ = tree.PLDeclaration{Ident: $1, Typ: $2, Assign: $4}
	}
|	ident sql_type
	{
		$$ = tree.PLDeclaration{Ident: $1, Typ: $2}
	}

opt_statements:
	statements ';'
| ';'
	{
		$$ = []tree.PLStatement(nil)
	}

statements:
	statement
	{
		$$ = []tree.PLStatement{$1}
	}
|	statements ';' statement
	{
		$$ = append($1, $3)
	}

statement:
	ident ASSIGNMENT sql_expr
	{
		$$ = &tree.PLAssignment{Ident: $1, Assign: $3}
	}
| IF sql_expr THEN statements ';' ELSE statements ';' END IF
	{
		$$ = &tree.PLIf{Cond: $2, Then: $4, Else: $7}
	}
| IF sql_expr THEN statements ';' END IF
	{
		$$ = &tree.PLIf{Cond: $2, Then: $4}
	}
| LOOP statements ';' END LOOP
	{
		$$ = &tree.PLLoop{Body: $2}
	}
| WHILE sql_expr LOOP statements ';' END LOOP
	{
		$$ = &tree.PLLoop{Cond: $2, Body: $4}
	}
| FOR ident IN sql_expr '.' '.' sql_expr LOOP statements ';' END LOOP
	{
		$$ = &tree.PLForLoop{Start: $4, End: $7, Body: $9}
	}
|	EXIT
	{
		$$ = &tree.PLExit{}
	}
| CONTINUE
	{
		$$ = &tree.PLContinue{}
	}
| RETURN sql_expr
	{
		$$ = &tree.PLReturn{Expr: $2}
	}

ident:
	IDENT
	{
		$$ = tree.Name($1)
	}

sql_expr:
	SQL_EXPR
	{
		$$ = &tree.PLSQLExpr{SQL: plsqllex.(*lexer).readSQLExpr()}
	}

sql_type:
	INT
	{
		$$ = types.Int
	}
| INT_ARRAY
	{
		$$ = types.MakeArray(types.Int)
	}
|	FLOAT
	{
		$$ = types.Float
	}
| FLOAT_ARRAY
	{
		$$ = types.MakeArray(types.Float)
	}
|	TEXT
	{
		$$ = types.String
	}
| TEXT_ARRAY
	{
		$$ = types.MakeArray(types.String)
	}
|	BOOL
	{
		$$ = types.Bool
	}
| BOOL_ARRAY
	{
		$$ = types.MakeArray(types.Bool)
	}

%%
