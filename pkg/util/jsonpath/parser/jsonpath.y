%{
package parser

import (
  "github.com/cockroachdb/cockroach/pkg/sql/scanner"
  "github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

%}

%{

var _ scanner.ScanSymType = &jsonpathSymType{}

func (s *jsonpathSymType) jsonpathScanSymType() {}

// ID implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) ID() int32 {
  return s.id
}

// SetID implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) SetID(id int32) {
  s.id = id
}

// Pos implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) Pos() int32 {
  return s.pos
}

// SetPos implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) SetPos(pos int32) {
  s.pos = pos
}

// Str implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) Str() string {
  return s.str
}

// SetStr implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) SetStr(str string) {
  s.str = str
}

// UnionVal implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) UnionVal() interface{} {
  return s.union.val
}

// SetUnionVal implements the scanner.ScanSymType interface.
func (s *jsonpathSymType) SetUnionVal(val interface{}) {
  s.union.val = val
}

type jsonpathSymUnion struct {
  val interface{}
}

func (u *jsonpathSymUnion) expr() jsonpath.Expr {
  return u.val.(jsonpath.Expr)
}

func (u *jsonpathSymUnion) accessor() jsonpath.Accessor {
  return u.val.(jsonpath.Accessor)
}

func (u *jsonpathSymUnion) query() jsonpath.Query {
  return u.val.(jsonpath.Query)
}

func (u *jsonpathSymUnion) root() jsonpath.Root {
  return u.val.(jsonpath.Root)
}

func (u *jsonpathSymUnion) key() jsonpath.Key {
  return u.val.(jsonpath.Key)
}

func (u *jsonpathSymUnion) wildcard() jsonpath.Wildcard {
  return u.val.(jsonpath.Wildcard)
}

func (u *jsonpathSymUnion) bool() bool {
  return u.val.(bool)
}

%}

%union{
  id      int32
  pos     int32
  str     string
  union   jsonpathSymUnion
}

/*
 * Basic non-keyword token types.  These are hard-wired into the core lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  Keep this list in sync with backend/parser/gram.y!
 *
 * Some of these are not directly referenced in this file, but they must be
 * here anyway.
 */
%token <str> IDENT UIDENT FCONST SCONST USCONST BCONST XCONST Op
%token <*tree.NumVal> ICONST PARAM
%token <str> TYPECAST DOT_DOT COLON_EQUALS EQUALS_GREATER
%token <str> LESS_EQUALS GREATER_EQUALS NOT_EQUALS

%token <str> ERROR

%token <str> STRICT
%token <str> LAX

%type <jsonpath.Expr> jsonpath
%type <jsonpath.Expr> expr_or_predicate
%type <jsonpath.Expr> expr
%type <jsonpath.Expr> accessor_expr
%type <jsonpath.Accessor> accessor_op
%type <jsonpath.Accessor> path_primary
%type <jsonpath.Accessor> key
%type <str> key_name
%type <jsonpath.Accessor> array_accessor
%type <str> any_identifier
%type <str> unreserved_keyword
%type <bool> mode

%%

jsonpath:
  mode expr_or_predicate
  {
    jp := jsonpath.Jsonpath{Query: $2.query(), Strict: $1.bool()}
    jsonpathlex.(*lexer).SetJsonpath(jp)
  }
;

mode:
  STRICT
  {
    $$.val = true
  }
| LAX
  {
    $$.val = false
  }
| /* empty */
  {
    $$.val = false
  }
;

expr_or_predicate:
  expr
  {
    $$.val = $1.query()
  }
;

expr:
  accessor_expr
  {
    $$.val = $1.query()
  }
;

accessor_expr:
  path_primary
  {
    $$.val = jsonpath.Query{Accessors: []jsonpath.Accessor{$1.accessor()}}
  }
| accessor_expr accessor_op
  {
    a := $1.query()
    a.Accessors = append(a.Accessors, $2.accessor())
    $$.val = a
  }
;

path_primary:
  '$'
  {
    $$.val = jsonpath.Root{}
  }
;

accessor_op:
  '.' key
  {
    $$.val = $2.key()
  }
| array_accessor
  {
    $$.val = $1.wildcard()
  }
;

key:
  key_name
  {
    $$.val = jsonpath.Key{Key: $1}
  }
;

key_name:
  any_identifier
  {
    $$ = $1
  }
;

array_accessor:
  '[' '*' ']'
  {
    $$.val = jsonpath.Wildcard{}
  }
;

any_identifier:
  IDENT
| unreserved_keyword
;

unreserved_keyword:
  STRICT
| LAX
;

%%
