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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
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
		{`<<`, []int{LSHIFT}},
		{`>`, []int{'>'}},
		{`>=`, []int{GREATER_EQUALS}},
		{`>>`, []int{RSHIFT}},
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
		{`//`, []int{FLOORDIV}},
		{`%`, []int{'%'}},
		{`^`, []int{'^'}},
		{`$`, []int{'$'}},
		{`&`, []int{'&'}},
		{`|`, []int{'|'}},
		{`||`, []int{CONCAT}},
		{`#`, []int{'#'}},
		{`~`, []int{'~'}},
		{`!~`, []int{NOT_REGMATCH}},
		{`~*`, []int{REGIMATCH}},
		{`!~*`, []int{NOT_REGIMATCH}},
		{`$1`, []int{PLACEHOLDER}},
		{`$a`, []int{'$', IDENT}},
		{`a`, []int{IDENT}},
		{`foo + bar`, []int{IDENT, '+', IDENT}},
		{`select a from b`, []int{SELECT, IDENT, FROM, IDENT}},
		{`"a" "b"`, []int{IDENT, IDENT}},
		{`'a'`, []int{SCONST}},
		{`b'a'`, []int{BCONST}},
		{`B'a'`, []int{BCONST}},
		{`e'a'`, []int{SCONST}},
		{`E'a'`, []int{SCONST}},
		{`NOT`, []int{NOT}},
		{`NOT BETWEEN`, []int{NOT_LA, BETWEEN}},
		{`NOT IN`, []int{NOT_LA, IN}},
		{`NOT SIMILAR`, []int{NOT_LA, SIMILAR}},
		{`NULLS`, []int{NULLS}},
		{`WITH`, []int{WITH}},
		{`WITH TIME`, []int{WITH_LA, TIME}},
		{`WITH ORDINALITY`, []int{WITH_LA, ORDINALITY}},
		{`1`, []int{ICONST}},
		{`0xa`, []int{ICONST}},
		{`x'ba'`, []int{SCONST}},
		{`X'ba'`, []int{SCONST}},
		{`1.0`, []int{FCONST}},
		{`1.0e1`, []int{FCONST}},
		{`1e+1`, []int{FCONST}},
		{`1e-1`, []int{FCONST}},
	}
	for i, d := range testData {
		s := MakeScanner(d.sql, Traditional)
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

func TestScannerModern(t *testing.T) {
	testData := []struct {
		sql      string
		expected []int
	}{
		{"`a`", []int{IDENT}},
		{`foo + bar`, []int{IDENT, '+', IDENT}},
		{`'a' "a"`, []int{SCONST, SCONST}},
		{`b'a' b"a"`, []int{BCONST, BCONST}},
		{`B'a' B"a"`, []int{BCONST, BCONST}},
		{`br'a' bR"a" Br'a' BR"a"`, []int{BCONST, BCONST, BCONST, BCONST}},
		{`rb'a' Rb"a" rB'a' RB"a"`, []int{BCONST, BCONST, BCONST, BCONST}},
		{`e'a' e"a"`, []int{SCONST, SCONST}},
		{`E'a' E"a"`, []int{SCONST, SCONST}},
		{`r'a' r"a"`, []int{SCONST, SCONST}},
		{`R'a' R"a"`, []int{SCONST, SCONST}},
		{`$1 $foo $select`, []int{PLACEHOLDER, PLACEHOLDER, PLACEHOLDER}},
	}
	for i, d := range testData {
		s := MakeScanner(d.sql, Modern)
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
		{`-- hello world
foo`, "", "foo"},
		{`/*`, "unterminated comment", ""},
		{`/*/`, "unterminated comment", ""},
		{`/* /* */`, "unterminated comment", ""},
	}
	for i, d := range testData {
		s := MakeScanner(d.sql, Traditional)
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

func TestScanCommentModern(t *testing.T) {
	testData := []struct {
		sql       string
		err       string
		remainder string
	}{
		{`# hello world
foo`, "", "foo"},
	}
	for i, d := range testData {
		s := MakeScanner(d.sql, Modern)
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
		s := MakeScanner(kwName, Traditional)
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
		{`0x1`, `0x1`, ICONST},
		{`0X2`, `0X2`, ICONST},
		{`0xff`, `0xff`, ICONST},
		{`0xff.`, `0xff`, ICONST},
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
		{`9223372036854775809`, `9223372036854775809`, ICONST},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql, Traditional)
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

func TestScanPlaceholder(t *testing.T) {
	testData := []struct {
		sql      string
		expected int64
	}{
		{`$1`, 1},
		{`$1a`, 1},
		{`$123`, 123},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql, Traditional)
		var lval sqlSymType
		id := s.Lex(&lval)
		if id != PLACEHOLDER {
			t.Errorf("%s: expected %d, but found %d", d.sql, PLACEHOLDER, id)
		}
		if i, err := lval.union.numVal().asInt64(); err != nil {
			t.Errorf("%s: expected success, but found %v", d.sql, err)
		} else if d.expected != i {
			t.Errorf("%s: expected %d, but found %d", d.sql, d.expected, i)
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
		{`'\n'`, `\n`},
		{`e'\n'`, "\n"},
		{`'\\n'`, `\\n`},
		{`'\'''`, `\'`},
		{`'\0\'`, `\0\`},
		{`"a"
	"b"`, `ab`},
		{`"a"
	'b'`, `a`},
		{`'a'
	'b'`, `ab`},
		{`'a'
	"b"`, `a`},
		{`e'\"'`, `"`}, // redundant escape
		{"'\n\\'", "\n\\"},
		{`e'foo\"\'\\\a\b\f\n\r\t\vbar'`,
			strings.Join([]string{`foo"'\`, "\a\b\f\n\r\t\v", `bar`}, "")},
		{`e'\\0'`, `\0`},
		{`'\0'`, `\0`},
		{`e'\x'`, `invalid syntax`},
		{`e'\x4'`, `invalid syntax`},
		{`e'\xg'`, `invalid syntax`},
		{`e'\X4'`, `invalid syntax`},
		{`e'\x41'`, `A`},
		{`e'\X41B'`, `AB`},
		{`e'\0'`, `invalid syntax`},
		{`e'\00'`, `invalid syntax`},
		{`e'\009'`, `invalid syntax`},
		{`e'\101'`, `A`},
		{`e'\101B'`, `AB`},
		{`e'\u1'`, `invalid syntax`},
		{`e'\U123'`, `invalid syntax`},
		{`e'\u0041'`, `A`},
		{`e'\u0041B'`, `AB`},
		{`e'\U00000041'`, `A`},
		{`e'\U00000041B'`, `AB`},
		{`"''"`, `''`},
		{`'""'''`, `""'`},
		{`""""`, `"`},
		{`''''`, `'`},
		{`''''''`, `''`},
		{`'hello
world'`, `hello
world`},
		{`x'666f6f'`, `foo`},
		{`X'626172'`, `bar`},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql, Traditional)
		var lval sqlSymType
		_ = s.Lex(&lval)
		if d.expected != lval.str {
			t.Errorf("%s: expected %q, but found %q", d.sql, d.expected, lval.str)
		}
	}
}

func TestScanStringModern(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
	}{
		// Modern syntax allows escapes without the 'e' or 'E' specifier.
		{`"\x41"`, `A`},
		{`'\x41'`, `A`},
		{`b"\x41"`, `A`},
		{`B'\x41'`, `A`},
		{`e"\x41"`, `A`},
		{`E'\x41'`, `A`},
		// Disable escapes with raw strings.
		{`r"\x41"`, `\x41`},
		{`R'\x41'`, `\x41`},
		// Triple-quoted strings allow non-escaped quotes.
		{`"""hello"world"""`, `hello"world`},
		{`'''hello''world'''`, `hello''world`},
		// Triple-quoted strings allow embedded newlines.
		{`'''hello
world'''`, `hello
world`},
		// Single/double-quoted strings do not allow newlines.
		{`'hello
world'`, `invalid syntax: embedded newline`},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql, Modern)
		var lval sqlSymType
		_ = s.Lex(&lval)
		if d.expected != lval.str {
			t.Errorf("%s: expected %q, but found %q", d.sql, d.expected, lval.str)
		}
	}
}

func TestScanError(t *testing.T) {
	testData := []struct {
		sql string
		err string
	}{
		{`1e`, "invalid floating point literal"},
		{`1e-`, "invalid floating point literal"},
		{`1e+`, "invalid floating point literal"},
		{`0x`, "invalid hexadecimal numeric literal"},
		{`1x`, "invalid hexadecimal numeric literal"},
		{`1.x`, "invalid hexadecimal numeric literal"},
		{`1.0x`, "invalid hexadecimal numeric literal"},
		{`0x0x`, "invalid hexadecimal numeric literal"},
		{`00x0x`, "invalid hexadecimal numeric literal"},
		{`08`, "could not make constant int from literal \"08\""},
		{`x'zzz'`, "invalid hexadecimal string literal"},
		{`X'zzz'`, "invalid hexadecimal string literal"},
		{`x'beef\x41'`, "invalid hexadecimal string literal"},
		{`X'beef\x41\x41'`, "invalid hexadecimal string literal"},
		{`x'''1'''`, "invalid hexadecimal string literal"},
		{`$9223372036854775809`, "integer value out of range"},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql, Traditional)
		var lval sqlSymType
		id := s.Lex(&lval)
		if id != ERROR {
			t.Errorf("%s: expected ERROR, but found %d", d.sql, id)
		}
		if !testutils.IsError(errors.New(lval.str), d.err) {
			t.Errorf("%s: expected %s, but found %s", d.sql, d.err, lval.str)
		}
	}
}
