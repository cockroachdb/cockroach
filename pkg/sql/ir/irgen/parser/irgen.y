// Copyright 2017 The Cockroach Authors.
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
%{
package parser

import "io"

// Parse parses definitions from an io.Reader.
func Parse(filename string, src io.Reader) ([]Def, error) {
        l := newLexer(filename, src)
        irgenParse(l)
        return l.defs, l.firstErr
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
| enum_list enum_item opt_semi { $$ = append($1, $2) }

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
