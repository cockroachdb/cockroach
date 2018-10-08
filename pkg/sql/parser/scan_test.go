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

package parser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"

	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		{`<<=`, []int{INET_CONTAINED_BY_OR_EQUALS}},
		{`>`, []int{'>'}},
		{`>=`, []int{GREATER_EQUALS}},
		{`>>`, []int{RSHIFT}},
		{`>>=`, []int{INET_CONTAINS_OR_EQUALS}},
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
		{`&&`, []int{INET_CONTAINS_OR_CONTAINED_BY}},
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
		{`b'\xff'`, []int{BCONST}},
		{`B'10101'`, []int{BITCONST}},
		{`e'a'`, []int{SCONST}},
		{`E'a'`, []int{SCONST}},
		{`NOT`, []int{NOT}},
		{`NOT BETWEEN`, []int{NOT_LA, BETWEEN}},
		{`NOT IN`, []int{NOT_LA, IN}},
		{`NOT SIMILAR`, []int{NOT_LA, SIMILAR}},
		{`WITH`, []int{WITH}},
		{`WITH TIME`, []int{WITH_LA, TIME}},
		{`WITH ORDINALITY`, []int{WITH_LA, ORDINALITY}},
		{`1`, []int{ICONST}},
		{`0xa`, []int{ICONST}},
		{`x'2F'`, []int{BCONST}},
		{`X'2F'`, []int{BCONST}},
		{`1.0`, []int{FCONST}},
		{`1.0e1`, []int{FCONST}},
		{`1e+1`, []int{FCONST}},
		{`1e-1`, []int{FCONST}},
	}
	for i, d := range testData {
		s := MakeScanner(d.sql)
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
			t.Errorf("%d: %q: expected %d, but found %d", i, d.sql, d.expected, tokens)
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
		s := MakeScanner(d.sql)
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
	for kwName, kwID := range lex.Keywords {
		s := MakeScanner(kwName)
		var lval sqlSymType
		id := s.Lex(&lval)
		if kwID.Tok != id {
			t.Errorf("%s: expected %d, but found %d", kwName, kwID.Tok, id)
		}
	}
}

func TestScanNumber(t *testing.T) {
	testData := []struct {
		sql      string
		expected string
		id       int
	}{
		{`0`, `0`, ICONST},
		{`000`, `0`, ICONST},
		{`1`, `1`, ICONST},
		{`0x1`, `0x1`, ICONST},
		{`0X2`, `0X2`, ICONST},
		{`0xff`, `0xff`, ICONST},
		{`0xff.`, `0xff`, ICONST},
		{`12345`, `12345`, ICONST},
		{`08`, `8`, ICONST},
		{`0011`, `11`, ICONST},
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
		s := MakeScanner(d.sql)
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
		expected string
	}{
		{`$1`, "1"},
		{`$1a`, "1"},
		{`$123`, "123"},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql)
		var lval sqlSymType
		id := s.Lex(&lval)
		if id != PLACEHOLDER {
			t.Errorf("%s: expected %d, but found %d", d.sql, PLACEHOLDER, id)
		}
		if d.expected != lval.str {
			t.Errorf("%s: expected %s, but found %s", d.sql, d.expected, lval.str)
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
		{`e'\xff'`, `invalid UTF-8 byte sequence`},
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
		{`X'FF'`, "\xff"},
		{`B'100101'`, "100101"},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql)
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
		{`x'zzz'`, "invalid hexadecimal bytes literal"},
		{`X'zzz'`, "invalid hexadecimal bytes literal"},
		{`x'beef\x41'`, "invalid hexadecimal bytes literal"},
		{`X'beef\x41\x41'`, "invalid hexadecimal bytes literal"},
		{`x'a'`, "invalid hexadecimal bytes literal"},
		{`$9223372036854775809`, "integer value out of range"},
		{`B'123'`, `"2" is not a valid binary digit`},
	}
	for _, d := range testData {
		s := MakeScanner(d.sql)
		var lval sqlSymType
		id := s.Lex(&lval)
		if id != ERROR {
			t.Errorf("%s: expected ERROR, but found %d", d.sql, id)
		}
		if !testutils.IsError(pgerror.NewError(pgerror.CodeInternalError, lval.str), d.err) {
			t.Errorf("%s: expected %s, but found %v", d.sql, d.err, lval.str)
		}
	}
}

func TestScanUntil(t *testing.T) {
	tests := []struct {
		s     string
		until int
		pos   int
	}{
		{
			``,
			0,
			0,
		},
		{
			`;`,
			';',
			1,
		},
		{
			`;`,
			'a',
			0,
		},
		{
			"123;",
			';',
			4,
		},
		{
			`
--SELECT 1, 2, 3;
SELECT 4, 5;
--blah`,
			';',
			31,
		},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%c: %q", tc.until, tc.s), func(t *testing.T) {
			s := MakeScanner(tc.s)
			pos := s.Until(tc.until)
			if pos != tc.pos {
				t.Fatalf("got %d; expected %d", pos, tc.pos)
			}
		})
	}
}
