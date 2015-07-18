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

package parser

import (
	"reflect"
	"testing"
)

func TestScanner(t *testing.T) {
	testData := []struct {
		sql      string
		expected []int
	}{
		{``, nil},
		{` `, nil},
		{` /* hello */`, nil},
		{`.`, []int{'.'}},
		{`..`, []int{DOT_DOT}},
		{`!`, []int{'!'}},
		{`!=`, []int{NOT_EQUALS}},
		{`<`, []int{'<'}},
		{`<>`, []int{NOT_EQUALS}},
		{`<=`, []int{LESS_EQUALS}},
		{`>`, []int{'>'}},
		{`>=`, []int{GREATER_EQUALS}},
		{`=`, []int{'='}},
		{`:`, []int{':'}},
		{`::`, []int{TYPECAST}},
		{`:: :`, []int{TYPECAST, ':'}},
		{`(`, []int{'('}},
		{`)`, []int{')'}},
		{`[`, []int{'['}},
		{`]`, []int{']'}},
		{`,`, []int{','}},
		{`;`, []int{';'}},
		{`+`, []int{'+'}},
		{`-`, []int{'-'}},
		{`*`, []int{'*'}},
		{`/`, []int{'/'}},
		{`%`, []int{'%'}},
		{`^`, []int{'^'}},
		{`$`, []int{'$'}},
		{`&`, []int{'&'}},
		{`|`, []int{'|'}},
		{`||`, []int{CONCAT}},
		{`#`, []int{'#'}},
		{`~`, []int{'~'}},
		{`$1`, []int{PARAM}},
		{`$a`, []int{'$', IDENT}},
		{`a`, []int{IDENT}},
		{`foo + bar`, []int{IDENT, '+', IDENT}},
		{`select a from b`, []int{SELECT, IDENT, FROM, IDENT}},
		{`"a" "b"`, []int{IDENT, IDENT}},
		{`'a'`, []int{SCONST}},
		{`b'a'`, []int{BCONST}},
		{`b'a'`, []int{BCONST}},
		{`e'a'`, []int{SCONST}},
		{`e'a'`, []int{SCONST}},
		{`x'a'`, []int{XCONST}},
		{`X'a'`, []int{XCONST}},
		{`NOT`, []int{NOT}},
		{`NOT BETWEEN`, []int{NOT_LA, BETWEEN}},
		{`NOT IN`, []int{NOT_LA, IN}},
		{`NOT SIMILAR`, []int{NOT_LA, SIMILAR}},
		{`NULLS`, []int{NULLS}},
		{`NULLS FIRST`, []int{NULLS_LA, FIRST}},
		{`NULLS LAST`, []int{NULLS_LA, LAST}},
		{`WITH`, []int{WITH}},
		{`WITH TIME`, []int{WITH_LA, TIME}},
		{`WITH ORDINALITY`, []int{WITH_LA, ORDINALITY}},
	}
	for i, d := range testData {
		s := newScanner(d.sql)
		var tokens []int
		for {
			var lval sqlSymType
			id := s.Lex(&lval)
			if id == 0 {
				break
			}
			tokens = append(tokens, id)
		}

		if !reflect.DeepEqual(d.expected, tokens) {
			t.Errorf("%d: expected %d, but found %d", i, d.expected, tokens)
		}
	}
}

func TestScanComment(t *testing.T) {
	testData := []struct {
		sql       string
		err       string
		remainder string
	}{
		{`/* hello */world`, "", "world"},
		{`/* hello */*`, "", "*"},
		{`/* /* deeply /* nested */ comment */ */`, "", ""},
		{`/* /* */* */`, "", ""},
		{`/* /* /*/ */ */ */`, "", ""},
		{`/* multi line
comment */`, "", ""},
		{`-- hello world`, "", ""},
		{`/*`, "unterminated comment", ""},
		{`/*/`, "unterminated comment", ""},
		{`/* /* */`, "unterminated comment", ""},
	}
	for i, d := range testData {
		s := newScanner(d.sql)
		var lval sqlSymType
		present, ok := s.scanComment(&lval)
		if d.err == "" && (!present || !ok) {
			t.Fatalf("%d: expected success, but found %s", i, lval.str)
		} else if d.err != "" && (present || ok || d.err != lval.str) {
			t.Fatalf("%d: expected %s, but found %s", i, d.err, lval.str)
		}
		if r := s.in[s.pos:]; d.remainder != r {
			t.Fatalf("%d: expected '%s', but found '%s'", i, d.remainder, r)
		}
	}
}

func TestScanKeyword(t *testing.T) {
	for kwName, kwID := range keywords {
		s := newScanner(kwName)
		var lval sqlSymType
		id := s.Lex(&lval)
		if kwID != id {
			t.Errorf("%s: expected %d, but found %d", kwName, kwID, id)
		}
	}
}

func TestScanNumber(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
		id       int
	}{
		{`1`, `1`, ICONST},
		{`12345`, `12345`, ICONST},
		{`1.`, `1.`, FCONST},
		{`.1`, `.1`, FCONST},
		{`1..2`, `1`, ICONST},
		{`1.2`, `1.2`, FCONST},
		{`1.2e3`, `1.2e3`, FCONST},
		{`1e3`, `1e3`, FCONST},
		{`1e3.4`, `1e3`, FCONST},
		{`.1e3.4`, `.1e3`, FCONST},
		{`1e-3`, `1e-3`, FCONST},
		{`1e-3-`, `1e-3`, FCONST},
		{`1e+3`, `1e+3`, FCONST},
		{`1e+3+`, `1e+3`, FCONST},
	}
	for _, d := range testData {
		s := newScanner(d.sql)
		var lval sqlSymType
		id := s.Lex(&lval)
		if d.id != id {
			t.Errorf("%s: expected %d, but found %d", d.sql, d.id, id)
		}
		if d.expected != lval.str {
			t.Errorf("%s: expected %s, but found %s", d.sql, d.expected, lval.str)
		}
	}
}

func TestScanParam(t *testing.T) {
	testData := []struct {
		sql      string
		expected int
	}{
		{`$1`, 1},
		{`$1a`, 1},
		{`$123`, 123},
	}
	for _, d := range testData {
		s := newScanner(d.sql)
		var lval sqlSymType
		id := s.Lex(&lval)
		if id != PARAM {
			t.Errorf("%s: expected %d, but found %d", d.sql, PARAM, id)
		}
		if d.expected != lval.ival {
			t.Errorf("%s: expected %d, but found %d", d.sql, d.expected, lval.ival)
		}
	}
}

func TestScanString(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		{`"a"`, `a`},
		{`'a'`, `a`},
		{`"a""b"`, `a"b`},
		{`"a''b"`, `a''b`},
		{`'a""b'`, `a""b`},
		{`'a''b'`, `a'b`},
		{`"a" "b"`, `a`},
		{`'a' 'b'`, `a`},
		{`'\n'`, "\\n"},
		{`"\""`, `"`},
		{`'\''`, `'`},
		{`'\0\'\"\b\f\n\r\t\\'`, `\0'\"\b\f\n\r\t\`},
		{`"a"
	"b"`, `ab`},
		{`"a"
	'b'`, `a`},
		{`'a'
	'b'`, `ab`},
		{`'a'
	"b"`, `a`},
		{`e'foo\"\'\\\b\f\n\r\tbar'`, "foo\"'\\\b\f\n\r\tbar"},
	}
	for _, d := range testData {
		s := newScanner(d.sql)
		var lval sqlSymType
		_ = s.Lex(&lval)
		if d.expected != lval.str {
			t.Errorf("%s: expected %q, but found %q", d.sql, d.expected, lval.str)
		}
	}
}
