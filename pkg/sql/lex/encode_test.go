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

package lex_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

func TestEncodeSQLBytes(t *testing.T) {
	testEncodeSQL(t, lex.EncodeSQLBytes, false)
}

func TestEncodeSQLString(t *testing.T) {
	testEncodeSQL(t, lex.EncodeSQLString, true)
}

func TestEncodeSQLString2(t *testing.T) {
	testCases := []string{
		// Regression test for #
		"\n\t\b\x01\x10\x00\x18\x00 \xf4\x03\x12\t\b\x01\x10\x00\x18\x00 \xf4\x03",
		"The following tests provided by Markus Kuhn <http://www.cl.cam.ac.uk/~mgk25/> - ",
		"https://www.cl.cam.ac.uk/~mgk25/ucs/examples/UTF-8-test.txt ",
		"2015-08-28 - CC BY 4.0",
		"",
		"Here come the tests:                                                          |",
		"                                                                              |",
		"1  Some correct UTF-8 text                                                    |",
		"                                                                              |",
		"You should see the Greek word 'kosme':       \"κόσμε\"                          |",
		"                                                                              |",
		"2  Boundary condition test cases                                              |",
		"                                                                              |",
		"2.1  First possible sequence of a certain length                              |",
		"                                                                              |",
		"2.1.1  1 byte  (U-00000000):        \"�\"                                        ",
		"2.1.2  2 bytes (U-00000080):        \"\u0080\"                                       |",
		"2.1.3  3 bytes (U-00000800):        \"ࠀ\"                                       |",
		"2.1.4  4 bytes (U-00010000):        \"𐀀\"                                       |",
		"2.1.5  5 bytes (U-00200000):        \"�����\"                                       |",
		"2.1.6  6 bytes (U-04000000):        \"������\"                                       |",
		"                                                                              |",
		"2.2  Last possible sequence of a certain length                               |",
		"                                                                              |",
		"2.2.1  1 byte  (U-0000007F):        \"\u007f\"                                        ",
		"2.2.2  2 bytes (U-000007FF):        \"\u07ff\"                                       |",
		"2.2.3  3 bytes (U-0000FFFF):        \"\uffff\"                                       |",
		"2.2.4  4 bytes (U-001FFFFF):        \"����\"                                       |",
		"2.2.5  5 bytes (U-03FFFFFF):        \"�����\"                                       |",
		"2.2.6  6 bytes (U-7FFFFFFF):        \"������\"                                       |",
		"                                                                              |",
		"2.3  Other boundary conditions                                                |",
		"                                                                              |",
		"2.3.1  U-0000D7FF = ed 9f bf = \"\ud7ff\"                                            |",
		"2.3.2  U-0000E000 = ee 80 80 = \"\ue000\"                                            |",
		"2.3.3  U-0000FFFD = ef bf bd = \"�\"                                            |",
		"2.3.4  U-0010FFFF = f4 8f bf bf = \"\U0010ffff\"                                         |",
		"2.3.5  U-00110000 = f4 90 80 80 = \"����\"                                         |",
		"                                                                              |",
		"3  Malformed sequences                                                        |",
		"                                                                              |",
		"3.1  Unexpected continuation bytes                                            |",
		"                                                                              |",
		"Each unexpected continuation byte should be separately signaled as a         |",
		"malformed sequence of its own.                                                |",
		"                                                                              |",
		"3.1.1  First continuation byte 0x80: \"�\"                                      |",
		"3.1.2  Last  continuation byte 0xbf: \"�\"                                      |",
		"                                                                              |",
		"3.1.3  2 continuation bytes: \"��\"                                             |",
		"3.1.4  3 continuation bytes: \"���\"                                            |",
		"3.1.5  4 continuation bytes: \"����\"                                           |",
		"3.1.6  5 continuation bytes: \"�����\"                                          |",
		"3.1.7  6 continuation bytes: \"������\"                                         |",
		"3.1.8  7 continuation bytes: \"�������\"                                        |",
		"                                                                              |",
		"3.1.9  Sequence of all 64 possible continuation bytes (0x80-0xbf):            |",
		"                                                                              |",
		"   \"����������������                                                          |",
		"    ����������������                                                          |",
		"    ����������������                                                          |",
		"    ����������������\"                                                         |",
		"                                                                              |",
		"3.2  Lonely start characters                                                  |",
		"                                                                              |",
		"3.2.1  All 32 first bytes of 2-byte sequences (0xc0-0xdf),                    |",
		"       each followed by a space character:                                    |",
		"                                                                              |",
		"   \"� � � � � � � � � � � � � � � �                                           |",
		"    � � � � � � � � � � � � � � � � \"                                         |",
		"                                                                              |",
		"3.2.2  All 16 first bytes of 3-byte sequences (0xe0-0xef),                    |",
		"       each followed by a space character:                                    |",
		"                                                                              |",
		"   \"� � � � � � � � � � � � � � � � \"                                         |",
		"                                                                              |",
		"3.2.3  All 8 first bytes of 4-byte sequences (0xf0-0xf7),                     |",
		"       each followed by a space character:                                    |",
		"                                                                              |",
		"   \"� � � � � � � � \"                                                         |",
		"                                                                              |",
		"3.2.4  All 4 first bytes of 5-byte sequences (0xf8-0xfb),                     |",
		"       each followed by a space character:                                    |",
		"                                                                              |",
		"   \"� � � � \"                                                                 |",
		"                                                                              |",
		"3.2.5  All 2 first bytes of 6-byte sequences (0xfc-0xfd),                     |",
		"       each followed by a space character:                                    |",
		"                                                                              |",
		"   \"� � \"                                                                     |",
		"                                                                              |",
		"3.3  Sequences with last continuation byte missing                            |",
		"                                                                              |",
		"All bytes of an incomplete sequence should be signaled as a single           |",
		"malformed sequence, i.e., you should see only a single replacement            |",
		"character in each of the next 10 tests. (Characters as in section 2)          |",
		"                                                                              |",
		"3.3.1  2-byte sequence with last byte missing (U+0000):     \"�\"               |",
		"3.3.2  3-byte sequence with last byte missing (U+0000):     \"��\"               |",
		"3.3.3  4-byte sequence with last byte missing (U+0000):     \"���\"               |",
		"3.3.4  5-byte sequence with last byte missing (U+0000):     \"����\"               |",
		"3.3.5  6-byte sequence with last byte missing (U+0000):     \"�����\"               |",
		"3.3.6  2-byte sequence with last byte missing (U-000007FF): \"�\"               |",
		"3.3.7  3-byte sequence with last byte missing (U-0000FFFF): \"�\"               |",
		"3.3.8  4-byte sequence with last byte missing (U-001FFFFF): \"���\"               |",
		"3.3.9  5-byte sequence with last byte missing (U-03FFFFFF): \"����\"               |",
		"3.3.10 6-byte sequence with last byte missing (U-7FFFFFFF): \"�����\"               |",
		"                                                                              |",
		"3.4  Concatenation of incomplete sequences                                    |",
		"                                                                              |",
		"All the 10 sequences of 3.3 concatenated, you should see 10 malformed         |",
		"sequences being signaled:                                                    |",
		"                                                                              |",
		"   \"�����������������������������\"                                                               |",
		"                                                                              |",
		"3.5  Impossible bytes                                                         |",
		"                                                                              |",
		"The following two bytes cannot appear in a correct UTF-8 string               |",
		"                                                                              |",
		"3.5.1  fe = \"�\"                                                               |",
		"3.5.2  ff = \"�\"                                                               |",
		"3.5.3  fe fe ff ff = \"����\"                                                   |",
		"                                                                              |",
		"4  Overlong sequences                                                         |",
		"                                                                              |",
		"The following sequences are not malformed according to the letter of          |",
		"the Unicode 2.0 standard. However, they are longer then necessary and         |",
		"a correct UTF-8 encoder is not allowed to produce them. A \"safe UTF-8         |",
		"decoder\" should reject them just like malformed sequences for two             |",
		"reasons: (1) It helps to debug applications if overlong sequences are         |",
		"not treated as valid representations of characters, because this helps        |",
		"to spot problems more quickly. (2) Overlong sequences provide                 |",
		"alternative representations of characters, that could maliciously be          |",
		"used to bypass filters that check only for ASCII characters. For              |",
		"instance, a 2-byte encoded line feed (LF) would not be caught by a            |",
		"line counter that counts only 0x0a bytes, but it would still be               |",
		"processed as a line feed by an unsafe UTF-8 decoder later in the              |",
		"pipeline. From a security point of view, ASCII compatibility of UTF-8         |",
		"sequences means also, that ASCII characters are *only* allowed to be          |",
		"represented by ASCII bytes in the range 0x00-0x7f. To ensure this             |",
		"aspect of ASCII compatibility, use only \"safe UTF-8 decoders\" that            |",
		"reject overlong UTF-8 sequences for which a shorter encoding exists.          |",
		"                                                                              |",
		"4.1  Examples of an overlong ASCII character                                  |",
		"                                                                              |",
		"With a safe UTF-8 decoder, all of the following five overlong                 |",
		"representations of the ASCII character slash (\"/\") should be rejected         |",
		"like a malformed UTF-8 sequence, for instance by substituting it with         |",
		"a replacement character. If you see a slash below, you do not have a          |",
		"safe UTF-8 decoder!                                                           |",
		"                                                                              |",
		"4.1.1 U+002F = c0 af             = \"��\"                                        |",
		"4.1.2 U+002F = e0 80 af          = \"���\"                                        |",
		"4.1.3 U+002F = f0 80 80 af       = \"����\"                                        |",
		"4.1.4 U+002F = f8 80 80 80 af    = \"�����\"                                        |",
		"4.1.5 U+002F = fc 80 80 80 80 af = \"������\"                                        |",
		"                                                                              |",
		"4.2  Maximum overlong sequences                                               |",
		"                                                                              |",
		"Below you see the highest Unicode value that is still resulting in an         |",
		"overlong sequence if represented with the given number of bytes. This         |",
		"is a boundary test for safe UTF-8 decoders. All five characters should        |",
		"be rejected like malformed UTF-8 sequences.                                   |",
		"                                                                              |",
		"4.2.1  U-0000007F = c1 bf             = \"��\"                                   |",
		"4.2.2  U-000007FF = e0 9f bf          = \"���\"                                   |",
		"4.2.3  U-0000FFFF = f0 8f bf bf       = \"����\"                                   |",
		"4.2.4  U-001FFFFF = f8 87 bf bf bf    = \"�����\"                                   |",
		"4.2.5  U-03FFFFFF = fc 83 bf bf bf bf = \"������\"                                   |",
		"                                                                              |",
		"4.3  Overlong representation of the NUL character                             |",
		"                                                                              |",
		"The following five sequences should also be rejected like malformed           |",
		"UTF-8 sequences and should not be treated like the ASCII NUL                  |",
		"character.                                                                    |",
		"                                                                              |",
		"4.3.1  U+0000 = c0 80             = \"��\"                                       |",
		"4.3.2  U+0000 = e0 80 80          = \"���\"                                       |",
		"4.3.3  U+0000 = f0 80 80 80       = \"����\"                                       |",
		"4.3.4  U+0000 = f8 80 80 80 80    = \"�����\"                                       |",
		"4.3.5  U+0000 = fc 80 80 80 80 80 = \"������\"                                       |",
		"                                                                              |",
		"5  Illegal code positions                                                     |",
		"                                                                              |",
		"The following UTF-8 sequences should be rejected like malformed               |",
		"sequences, because they never represent valid ISO 10646 characters and        |",
		"a UTF-8 decoder that accepts them might introduce security problems           |",
		"comparable to overlong UTF-8 sequences.                                       |",
		"                                                                              |",
		"5.1 Single UTF-16 surrogates                                                  |",
		"                                                                              |",
		"5.1.1  U+D800 = ed a0 80 = \"���\"                                                |",
		"5.1.2  U+DB7F = ed ad bf = \"���\"                                                |",
		"5.1.3  U+DB80 = ed ae 80 = \"���\"                                                |",
		"5.1.4  U+DBFF = ed af bf = \"���\"                                                |",
		"5.1.5  U+DC00 = ed b0 80 = \"���\"                                                |",
		"5.1.6  U+DF80 = ed be 80 = \"���\"                                                |",
		"5.1.7  U+DFFF = ed bf bf = \"���\"                                                |",
		"                                                                              |",
		"5.2 Paired UTF-16 surrogates                                                  |",
		"                                                                              |",
		"5.2.1  U+D800 U+DC00 = ed a0 80 ed b0 80 = \"������\"                               |",
		"5.2.2  U+D800 U+DFFF = ed a0 80 ed bf bf = \"������\"                               |",
		"5.2.3  U+DB7F U+DC00 = ed ad bf ed b0 80 = \"������\"                               |",
		"5.2.4  U+DB7F U+DFFF = ed ad bf ed bf bf = \"������\"                               |",
		"5.2.5  U+DB80 U+DC00 = ed ae 80 ed b0 80 = \"������\"                               |",
		"5.2.6  U+DB80 U+DFFF = ed ae 80 ed bf bf = \"������\"                               |",
		"5.2.7  U+DBFF U+DC00 = ed af bf ed b0 80 = \"������\"                               |",
		"5.2.8  U+DBFF U+DFFF = ed af bf ed bf bf = \"������\"                               |",
		"                                                                              |",
		"5.3 Noncharacter code positions                                               |",
		"                                                                              |",
		"The following \"noncharacters\" are \"reserved for internal use\" by              |",
		"applications, and according to older versions of the Unicode Standard         |",
		"\"should never be interchanged\". Unicode Corrigendum #9 dropped the            |",
		"latter restriction. Nevertheless, their presence in incoming UTF-8 data       |",
		"can remain a potential security risk, depending on what use is made of        |",
		"these codes subsequently. Examples of such internal use:                      |",
		"                                                                              |",
		" - Some file APIs with 16-bit characters may use the integer value -1         |",
		"   = U+FFFF to signal an end-of-file (EOF) or error condition.                |",
		"                                                                              |",
		" - In some UTF-16 receivers, code point U+FFFE might trigger a                |",
		"   byte-swap operation (to convert between UTF-16LE and UTF-16BE).            |",
		"                                                                              |",
		"With such internal use of noncharacters, it may be desirable and safer        |",
		"to block those code points in UTF-8 decoders, as they should never            |",
		"occur legitimately in incoming UTF-8 data, and could trigger unsafe           |",
		"behavior in subsequent processing.                                           |",
		"                                                                              |",
		"Particularly problematic noncharacters in 16-bit applications:                |",
		"                                                                              |",
		"5.3.1  U+FFFE = ef bf be = \"\ufffe\"                                                |",
		"5.3.2  U+FFFF = ef bf bf = \"\uffff\"                                                |",
		"                                                                              |",
		"Other noncharacters:                                                          |",
		"                                                                              |",
		"5.3.3  U+FDD0 .. U+FDEF = \"\ufdd0\ufdd1\ufdd2\ufdd3\ufdd4\ufdd5\ufdd6\ufdd7\ufdd8\ufdd9\ufdda\ufddb\ufddc\ufddd\ufdde\ufddf\ufde0\ufde1\ufde2\ufde3\ufde4\ufde5\ufde6\ufde7\ufde8\ufde9\ufdea\ufdeb\ufdec\ufded\ufdee\ufdef\"|",
		"                                                                              |",
		"5.3.4  U+nFFFE U+nFFFF (for n = 1..10)                                        |",
		"                                                                              |",
		"       \"\U0001fffe\U0001ffff\U0002fffe\U0002ffff\U0003fffe\U0003ffff\U0004fffe\U0004ffff\U0005fffe\U0005ffff\U0006fffe\U0006ffff\U0007fffe\U0007ffff\U0008fffe\U0008ffff                                    |",
		"        \U0009fffe\U0009ffff\U000afffe\U000affff\U000bfffe\U000bffff\U000cfffe\U000cffff\U000dfffe\U000dffff\U000efffe\U000effff\U000ffffe\U000fffff\U0010fffe\U0010ffff\"                                   |",
		"                                                                              |",
		"THE END                                                                       |",
		"",
	}

	buf := bytes.Buffer{}
	for _, testCase := range testCases {
		lex.EncodeSQLString(&buf, testCase)
	}
}

func testEncodeSQL(t *testing.T, encode func(*bytes.Buffer, string), forceUTF8 bool) {
	type entry struct{ i, j int }
	seen := make(map[string]entry)
	for i := 0; i < 256; i++ {
		for j := 0; j < 256; j++ {
			bytepair := []byte{byte(i), byte(j)}
			if forceUTF8 && !utf8.Valid(bytepair) {
				continue
			}
			stmt := testEncodeString(t, bytepair, encode)
			if e, ok := seen[stmt]; ok {
				t.Fatalf("duplicate entry: %s, from %v, currently at %v, %v", stmt, e, i, j)
			}
			seen[stmt] = entry{i, j}
		}
	}
}

func TestEncodeSQLStringSpecial(t *testing.T) {
	tests := [][]byte{
		// UTF8 replacement character
		{0xEF, 0xBF, 0xBD},
	}
	for _, tc := range tests {
		testEncodeString(t, tc, lex.EncodeSQLString)
	}
}

func testEncodeString(t *testing.T, input []byte, encode func(*bytes.Buffer, string)) string {
	s := string(input)
	var buf bytes.Buffer
	encode(&buf, s)
	sql := fmt.Sprintf("SELECT %s", buf.String())
	for n := 0; n < len(sql); n++ {
		ch := sql[n]
		if ch < 0x20 || ch >= 0x7F {
			t.Fatalf("unprintable character: %v (%v): %s %v", ch, input, sql, []byte(sql))
		}
	}
	stmts, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("%s: expected success, but found %s", sql, err)
	}
	stmt := stmts.String()
	if sql != stmt {
		t.Fatalf("expected %s, but found %s", sql, stmt)
	}
	return stmt
}

func BenchmarkEncodeSQLString(b *testing.B) {
	str := strings.Repeat("foo", 10000)
	for i := 0; i < b.N; i++ {
		lex.EncodeSQLStringWithFlags(bytes.NewBuffer(nil), str, lex.EncBareStrings)
	}
}

func TestEncodeRestrictedSQLIdent(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{`foo`, `foo`},
		{``, `""`},
		{`3`, `"3"`},
		{`foo3`, `foo3`},
		{`foo"`, `"foo"""`},
		{`fo"o"`, `"fo""o"""`},
		{`fOo`, `"fOo"`},
		{`_foo`, `_foo`},
		{`-foo`, `"-foo"`},
		{`select`, `"select"`},
		{`integer`, `"integer"`},
		// N.B. These type names are examples of type names that *should* be
		// unrestricted (left out of the reserved keyword list) because they're not
		// part of the sql standard type name list. This is important for Postgres
		// compatibility. If you find yourself about to change this, don't - you can
		// convince yourself of such by looking at the output of `quote_ident`
		// against a Postgres instance.
		{`int8`, `int8`},
		{`date`, `date`},
		{`inet`, `inet`},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		lex.EncodeRestrictedSQLIdent(&buf, tc.input, lex.EncBareStrings)
		out := buf.String()

		if out != tc.output {
			t.Errorf("`%s`: expected `%s`, got `%s`", tc.input, tc.output, out)
		}
	}
}

func TestByteArrayDecoding(t *testing.T) {
	const (
		fmtHex = sessiondata.BytesEncodeHex
		fmtEsc = sessiondata.BytesEncodeEscape
		fmtB64 = sessiondata.BytesEncodeBase64
	)
	testData := []struct {
		in    string
		auto  bool
		inFmt sessiondata.BytesEncodeFormat
		out   string
		err   string
	}{
		{`a`, false, fmtHex, "", "encoding/hex: odd length hex string"},
		{`aa`, false, fmtHex, "\xaa", ""},
		{`aA`, false, fmtHex, "\xaa", ""},
		{`AA`, false, fmtHex, "\xaa", ""},
		{`x0`, false, fmtHex, "", "encoding/hex: invalid byte: U+0078 'x'"},
		{`a\nbcd`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a\'bcd`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a\00`, false, fmtEsc, "", "bytea encoded value ends with incomplete escape sequence"},
		{`a\099`, false, fmtEsc, "", "invalid bytea escape sequence"},
		{`a'b`, false, fmtEsc, "a'b", ""},
		{`a''b`, false, fmtEsc, "a''b", ""},
		{`a\\b`, false, fmtEsc, "a\\b", ""},
		{`a\000b`, false, fmtEsc, "a\x00b", ""},
		{"a\nb", false, fmtEsc, "a\nb", ""},
		{`a`, false, fmtB64, "", "illegal base64 data at input byte 0"},
		{`aa=`, false, fmtB64, "", "illegal base64 data at input byte 3"},
		{`AA==`, false, fmtB64, "\x00", ""},
		{`/w==`, false, fmtB64, "\xff", ""},
		{`AAAA`, false, fmtB64, "\x00\x00\x00", ""},
		{`a`, true, 0, "a", ""},
		{`\x`, true, 0, "", ""},
		{`\xx`, true, 0, "", "encoding/hex: invalid byte: U+0078 'x'"},
		{`\x6162`, true, 0, "ab", ""},
		{`\\x6162`, true, 0, "\\x6162", ""},
	}
	for _, s := range testData {
		t.Run(fmt.Sprintf("%s:%s", s.in, s.inFmt), func(t *testing.T) {
			var dec []byte
			var err error
			if s.auto {
				dec, err = lex.DecodeRawBytesToByteArrayAuto([]byte(s.in))
			} else {
				dec, err = lex.DecodeRawBytesToByteArray(s.in, s.inFmt)
			}
			if s.err != "" {
				if err == nil {
					t.Fatalf("expected err %q, got no error", s.err)
				}
				if s.err != err.Error() {
					t.Fatalf("expected err %q, got %q", s.err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(dec) != s.out {
				t.Fatalf("expected %q, got %q", s.out, dec)
			}
		})
	}
}

func TestByteArrayEncoding(t *testing.T) {
	testData := []struct {
		in  string
		out []string
	}{
		// The reference values were gathered from PostgreSQL.
		{"", []string{`\x`, ``, ``}},
		{"abc", []string{`\x616263`, `abc`, `YWJj`}},
		{"a\nb", []string{`\x610a62`, `a\012b`, `YQpi`}},
		{`a\nb`, []string{`\x615c6e62`, `a\\nb`, `YVxuYg==`}},
		{"a'b", []string{`\x612762`, `a'b`, `YSdi`}},
		{"a\"b", []string{`\x612262`, `a"b`, `YSJi`}},
		{"a\x00b", []string{`\x610062`, `a\000b`, `YQBi`}},
	}

	for _, s := range testData {
		t.Run(s.in, func(t *testing.T) {
			for _, format := range []sessiondata.BytesEncodeFormat{
				sessiondata.BytesEncodeHex, sessiondata.BytesEncodeEscape, sessiondata.BytesEncodeBase64} {
				t.Run(format.String(), func(t *testing.T) {
					enc := lex.EncodeByteArrayToRawBytes(s.in, format, false)

					expEnc := s.out[int(format)]
					if enc != expEnc {
						t.Fatalf("encoded %q, expected %q", enc, expEnc)
					}

					if format == sessiondata.BytesEncodeHex {
						// Check that the \x also can be skipped.
						enc2 := lex.EncodeByteArrayToRawBytes(s.in, format, true)
						if enc[2:] != enc2 {
							t.Fatal("can't skip prefix")
						}
						enc = enc[2:]
					}

					dec, err := lex.DecodeRawBytesToByteArray(enc, format)
					if err != nil {
						t.Fatal(err)
					}
					if string(dec) != s.in {
						t.Fatalf("decoded %q, expected %q", string(dec), s.in)
					}
				})
			}
		})
	}
}
