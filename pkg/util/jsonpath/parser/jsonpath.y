%{
package parser

import (
  "strconv"

  "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
  "github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
  "github.com/cockroachdb/cockroach/pkg/sql/scanner"
  "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
  "github.com/cockroachdb/cockroach/pkg/util/json"
  "github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

%}

%{

func setErr(jsonpathlex jsonpathLexer, err error) int {
  jsonpathlex.(*lexer).setErr(err)
  return 1
}

// TODO(normanchenn): link meta-issue to unimplemented errors.
func unimplemented(jsonpathlex jsonpathLexer, feature string) int {
  jsonpathlex.(*lexer).Unimplemented(feature)
  return 1
}

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

func (u *jsonpathSymUnion) jsonpath() jsonpath.Jsonpath {
  return u.val.(jsonpath.Jsonpath)
}

func (u *jsonpathSymUnion) path() jsonpath.Path {
  return u.val.(jsonpath.Path)
}

func (u *jsonpathSymUnion) paths() jsonpath.Paths {
  return u.val.(jsonpath.Paths)
}

func (u *jsonpathSymUnion) pathArr() []jsonpath.Path {
  return u.val.([]jsonpath.Path)
}

func (u *jsonpathSymUnion) bool() bool {
  return u.val.(bool)
}

func (u *jsonpathSymUnion) numVal() *tree.NumVal {
  return u.val.(*tree.NumVal)
}

func (u *jsonpathSymUnion) arrayList() jsonpath.ArrayList {
  return u.val.(jsonpath.ArrayList)
}

func (u *jsonpathSymUnion) operationType() jsonpath.OperationType {
  return u.val.(jsonpath.OperationType)
}

%}

%{

func binaryOp(op jsonpath.OperationType, left jsonpath.Path, right jsonpath.Path) jsonpath.Operation {
  return jsonpath.Operation{
    Type:  op,
    Left:  left,
    Right: right,
  }
}

func unaryOp(op jsonpath.OperationType, left jsonpath.Path) jsonpath.Operation {
  return jsonpath.Operation{
    Type:  op,
    Left:  left,
    Right: nil,
  }
}

func regexBinaryOp(left jsonpath.Path, regex string) (jsonpath.Operation, error) {
  r := jsonpath.Regex{Regex: regex}
  _, err := ReCache.GetRegexp(r)
  if err != nil {
    return jsonpath.Operation{}, pgerror.Wrapf(err, pgcode.InvalidRegularExpression,
      "invalid regular expression")
  }
  return binaryOp(jsonpath.OpLikeRegex, left, r), nil
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

%token <str> VARIABLE
%token <str> TO

%token <str> TRUE
%token <str> FALSE

%token <str> EQUAL
%token <str> NOT_EQUAL
%token <str> LESS
%token <str> LESS_EQUAL
%token <str> GREATER
%token <str> GREATER_EQUAL

%token <str> ROOT

%token <str> AND
%token <str> OR
%token <str> NOT

%token <str> CURRENT

%token <str> STRING
%token <str> NULL

%token <str> LIKE_REGEX
%token <str> FLAG

%token <str> LAST
%token <str> EXISTS
%token <str> IS
%token <str> UNKNOWN
%token <str> STARTS
%token <str> WITH

%type <jsonpath.Jsonpath> jsonpath
%type <jsonpath.Path> expr_or_predicate
%type <jsonpath.Path> expr
%type <jsonpath.Path> accessor_op
%type <jsonpath.Path> path_primary
%type <jsonpath.Path> key
%type <jsonpath.Path> array_accessor
%type <jsonpath.Path> scalar_value
%type <jsonpath.Path> index_elem
%type <jsonpath.Path> predicate
%type <jsonpath.Path> delimited_predicate
%type <jsonpath.Path> starts_with_initial
%type <[]jsonpath.Path> accessor_expr
%type <[]jsonpath.Path> index_list
%type <jsonpath.OperationType> comp_op
%type <str> key_name
%type <str> any_identifier
%type <str> unreserved_keyword
%type <bool> mode

%left OR
%left AND
%right NOT

%left '+' '-'
%left '*' '/' '%'
%left UMINUS

%%

jsonpath:
  mode expr_or_predicate
  {
    jp := jsonpath.Jsonpath{Strict: $1.bool(), Path: $2.path()}
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
    $$.val = $1.path()
  }
| predicate
  {
    $$.val = $1.path()
  }
;

expr:
  accessor_expr
  {
    $$.val = jsonpath.Paths($1.pathArr())
  }
| '(' expr ')'
  {
    $$.val = $2.path()
  }
| '+' expr %prec UMINUS
  {
    $$.val = unaryOp(jsonpath.OpPlus, $2.path())
  }
| '-' expr %prec UMINUS
  {
    $$.val = unaryOp(jsonpath.OpMinus, $2.path())
  }
| expr '+' expr
  {
    $$.val = binaryOp(jsonpath.OpAdd, $1.path(), $3.path())
  }
| expr '-' expr
  {
    $$.val = binaryOp(jsonpath.OpSub, $1.path(), $3.path())
  }
| expr '*' expr
  {
    $$.val = binaryOp(jsonpath.OpMult, $1.path(), $3.path())
  }
| expr '/' expr
  {
    $$.val = binaryOp(jsonpath.OpDiv, $1.path(), $3.path())
  }
| expr '%' expr
  {
    $$.val = binaryOp(jsonpath.OpMod, $1.path(), $3.path())
  }
;

accessor_expr:
  path_primary
  {
    $$.val = []jsonpath.Path{$1.path()}
  }
| accessor_expr accessor_op
  {
    $$.val = append($1.pathArr(), $2.path())
  }
;

path_primary:
  ROOT
  {
    $$.val = jsonpath.Root{}
  }
| CURRENT
  {
    $$.val = jsonpath.Current{}
  }
| scalar_value
  {
    $$.val = $1.path()
  }
| LAST
  {
    $$.val = jsonpath.Last{}
  }
;

accessor_op:
  '.' key
  {
    $$.val = $2.path()
  }
| array_accessor
  {
    $$.val = $1.path()
  }
| '?' '(' predicate ')'
  {
    $$.val = jsonpath.Filter{Condition: $3.path()}
  }
| '.' '*'
  {
    $$.val = jsonpath.AnyKey{}
  }
;

key:
  key_name
  {
    $$.val = jsonpath.Key($1)
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
| '[' index_list ']'
  {
    $$.val = $2.path()
  }
;

index_list:
  index_elem
  {
    $$.val = jsonpath.ArrayList{$1.path()}
  }
| index_list ',' index_elem
  {
    $$.val = append($1.arrayList(), $3.path())
  }
;

index_elem:
  expr
  {
    $$.val = $1.path()
  }
| expr TO expr
  {
    $$.val = jsonpath.ArrayIndexRange{
      Start: $1.path(),
      End: $3.path(),
    }
  }
;

predicate:
  delimited_predicate
  {
    $$.val = $1.path()
  }
| expr comp_op expr
  {
    $$.val = binaryOp($2.operationType(), $1.path(), $3.path())
  }
| predicate AND predicate
  {
    $$.val = binaryOp(jsonpath.OpLogicalAnd, $1.path(), $3.path())
  }
| predicate OR predicate
  {
    $$.val = binaryOp(jsonpath.OpLogicalOr, $1.path(), $3.path())
  }
| NOT delimited_predicate
  {
    $$.val = unaryOp(jsonpath.OpLogicalNot, $2.path())
  }
| '(' predicate ')' IS UNKNOWN
  {
    $$.val = unaryOp(jsonpath.OpIsUnknown, $2.path())
  }
| expr STARTS WITH starts_with_initial
  {
    $$.val = binaryOp(jsonpath.OpStartsWith, $1.path(), $4.path())
  }
| expr LIKE_REGEX STRING
  {
    regex, err := regexBinaryOp($1.path(), $3)
    if err != nil {
      return setErr(jsonpathlex, err)
    }
    $$.val = regex
  }
| expr LIKE_REGEX STRING FLAG STRING
  {
    return unimplemented(jsonpathlex, "regex with flags")
  }
;

delimited_predicate:
  '(' predicate ')'
  {
    $$.val = $2.path()
  }
| EXISTS '(' expr ')'
  {
    $$.val = unaryOp(jsonpath.OpExists, $3.path())
  }
;

starts_with_initial:
  STRING
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarString, Value: json.FromString($1)}
  }
| VARIABLE
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarVariable, Variable: $1}
  }
;

comp_op:
  EQUAL
  {
    $$.val = jsonpath.OpCompEqual
  }
| NOT_EQUAL
  {
    $$.val = jsonpath.OpCompNotEqual
  }
| LESS
  {
    $$.val = jsonpath.OpCompLess
  }
| LESS_EQUAL
  {
    $$.val = jsonpath.OpCompLessEqual
  }
| GREATER
  {
    $$.val = jsonpath.OpCompGreater
  }
| GREATER_EQUAL
  {
    $$.val = jsonpath.OpCompGreaterEqual
  }
;

scalar_value:
  VARIABLE
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarVariable, Variable: $1}
  }
| ICONST
  {
    i, err := $1.numVal().AsInt64()
    if err != nil {
      return setErr(jsonpathlex, err)
    }
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarInt, Value: json.FromInt64(i)}
  }
| FCONST
  {
    f, err := strconv.ParseFloat($1, 64)
    if err != nil {
      return setErr(jsonpathlex, err)
    }
    j, err := json.FromFloat64(f)
    if err != nil {
      return setErr(jsonpathlex, err)
    }
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarFloat, Value: j}
  }
| TRUE
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarBool, Value: json.FromBool(true)}
  }
| FALSE
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarBool, Value: json.FromBool(false)}
  }
| STRING
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarString, Value: json.FromString($1)}
  }
| NULL
  {
    $$.val = jsonpath.Scalar{Type: jsonpath.ScalarNull, Value: json.NullJSONValue}
  }
;

any_identifier:
  IDENT
| STRING
| unreserved_keyword
;

unreserved_keyword:
  EXISTS
| FALSE
| FLAG
| IS
| LAST
| LAX
| LIKE_REGEX
| NULL
| STARTS
| STRICT
| TO
| TRUE
| UNKNOWN
| WITH
;

%%
