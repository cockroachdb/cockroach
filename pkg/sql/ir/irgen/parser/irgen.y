%{
package parser

import "io"

// Parse parses definitions from an io.Reader.
func Parse(filename string, src io.Reader) ([]Def, error) {
	l := newLexer(filename, src)
	irgenParse(l)
	return l.defs, l.firstErr
}

// Def represents either an enumeration, a primitive type, a struct, or a sum.
type Def struct {
	Kind  DefKind
	Name  TypeNameOccur
	Items []DefItem
}

type DefKind int

const (
	UnknownDef DefKind = iota
	EnumDef
	PrimDef
	StructDef
	SumDef
)

// DefItem represents either an enumeration constant, a struct field, a sum
// alternative, or a reserved tag. Enumeration constants have a name. Struct
// fields have a name and a type. Sum alternatives have a type. Reserved tags
// have neither a name nor a type.
type DefItem struct {
	Name    ItemNameOccur
	IsSlice bool
	Type    TypeNameOccur
	Tag     TagOccur
}

func (i DefItem) IsReserved() bool {
	return i.Name.Name == "" && i.Type.Name == ""
}

// TypeNameOccur represents an occurrence of a TypeName in the input.
type TypeNameOccur struct {
	Name TypeName
	Pos  Pos
}

// ItemNameOccur represents an occurrence of an ItemName in the input.
type ItemNameOccur struct {
	Name ItemName
	Pos  Pos
}

// TagOccur represents an occurrence of a Tag in the input.
type TagOccur struct {
	Tag Tag
	Pos Pos
}

// TypeName represents the name of a type (primitive or not).
type TypeName string

// ItemName represents the name of an enumeration constant or a struct field.
type ItemName string

// Tag represents a protobuf tag number.
type Tag int32

type strOccur struct {
	Str string
	Pos Pos
}
%}

%union {
	b        bool
	def      Def
	defs     []Def
	item     DefItem
	itemName ItemNameOccur
	items    []DefItem
	str      strOccur
	tag      TagOccur
	typeName TypeNameOccur
}

%type <b> opt_slice
%type <def> def enum_def prim_def struct_def sum_def
%type <defs> def_list
%type <item> enum_item reserved_item struct_item sum_item
%type <itemName> item_name
%type <items> enum_list struct_list sum_list
%type <str> opt_format qual_name
%type <typeName> simple_type_name type_name

%token <str> ERROR IDENT STR
%token <tag> TAG
%token ENUM FORMAT PRIM RESERVED STRUCT SUM

%%

top:
  def_list { irgenlex.(*lexer).defs = $1 }

def_list:
  /* EMPTY */ { $$ = nil }
| def_list def opt_semi { $$ = append($1, $2) }

def:
  enum_def
| prim_def
| struct_def
| sum_def


enum_def:
  ENUM simple_type_name '{' enum_list '}' { $$ = Def{Kind: EnumDef, Name: $2, Items: $4} }

enum_list:
  /* EMPTY */ { $$ = nil }
| enum_list enum_item opt_semi

enum_item:
  item_name '=' TAG { $$ = DefItem{Name: $1, Tag: $3} }
| reserved_item


prim_def: PRIM type_name { $$ = Def{Kind: PrimDef, Name: $2} }


struct_def:
  STRUCT simple_type_name '{' struct_list '}' opt_format { $$ = Def{Kind: StructDef, Name: $2, Items: $4} }

struct_list:
  /* EMPTY */ { $$ = nil }
| struct_list struct_item opt_semi { $$ = append($1, $2) }

struct_item:
  type_name opt_slice item_name '=' TAG { $$ = DefItem{Name: $3, IsSlice: $2, Type: $1, Tag: $5} }
| reserved_item

opt_slice:
  /* EMPTY */ { $$ = false }
| '[' ']' { $$ = true }

opt_format:
  /* EMPTY */ { $$ = strOccur{} }
| FORMAT STR { $$ = $2 }


sum_def:
  SUM simple_type_name '{' sum_list '}' { $$ = Def{Kind: SumDef, Name: $2, Items: $4} }

sum_list:
  /* EMPTY */ { $$ = nil }
| sum_list sum_item opt_semi { $$ = append($1, $2) }

sum_item:
  simple_type_name '=' TAG { $$ = DefItem{Type: $1, Tag: $3} }
| reserved_item


simple_type_name:
  IDENT { $$ = TypeNameOccur{TypeName($1.Str), $1.Pos} }

type_name:
  qual_name { $$ = TypeNameOccur{TypeName($1.Str), $1.Pos} }
| '*' qual_name { $$ = TypeNameOccur{TypeName("*" + $2.Str), $2.Pos} }

qual_name:
  IDENT
| qual_name '.' IDENT { $$ = strOccur{$1.Str + "." + $3.Str, $3.Pos} }

item_name:
  IDENT { $$ = ItemNameOccur{ItemName($1.Str), $1.Pos} }

reserved_item:
  RESERVED TAG { $$ = DefItem{Tag: $2} }

opt_semi:
  /* EMPTY */
| ';'

%%
