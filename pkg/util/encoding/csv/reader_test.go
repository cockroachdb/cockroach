// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

package csv

import (
	"io"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRead(t *testing.T) {
	tests := []struct {
		Name   string
		Input  string
		Output [][]Record
		Error  error

		// These fields are copied into the Reader
		Comma              rune
		Escape             rune
		Comment            rune
		UseFieldsPerRecord bool // false (default) means FieldsPerRecord is -1
		FieldsPerRecord    int
		LazyQuotes         bool
		TrimLeadingSpace   bool
		ReuseRecord        bool
	}{{
		Name:   "Simple",
		Input:  "a,b,c\n",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}}},
	}, {
		Name:   "CRLF",
		Input:  "a,b\r\nc,d\r\n",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}}, {Record{"c", false}, Record{"d", false}}},
	}, {
		Name:   "BareCR",
		Input:  "a,b\rc,d\r\n",
		Output: [][]Record{{Record{"a", false}, Record{"b\rc", false}, Record{"d", false}}},
	}, {
		Name: "RFC4180test",
		Input: `#field1,field2,field3
"aaa","bb
b","ccc"
"a,a","b""bb","ccc"
zzz,yyy,xxx
`,
		Output: [][]Record{
			{Record{"#field1", false}, Record{"field2", false}, Record{"field3", false}},
			{Record{"aaa", true}, Record{"bb\nb", true}, Record{"ccc", true}},
			{Record{"a,a", true}, Record{`b"bb`, true}, Record{"ccc", true}},
			{Record{"zzz", false}, Record{"yyy", false}, Record{"xxx", false}},
		},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    0,
	}, {
		Name:   "NoEOLTest",
		Input:  "a,b,c",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}}},
	}, {
		Name:   "Semicolon",
		Input:  "a;b;c\n",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}}},
		Comma:  ';',
	}, {
		Name: "MultiLine",
		Input: `"two
line","one line","three
line
field"`,
		Output: [][]Record{{Record{"two\nline", true}, Record{"one line", true}, Record{"three\nline\nfield", true}}},
	}, {
		Name:  "BlankLine",
		Input: "a,b,c\n\nd,e,f\n\n",
		Output: [][]Record{
			{Record{"a", false}, Record{"b", false}, Record{"c", false}},
			{Record{"d", false}, Record{"e", false}, Record{"f", false}},
		},
	}, {
		Name:  "BlankLineFieldCount",
		Input: "a,b,c\n\nd,e,f\n\n",
		Output: [][]Record{
			{Record{"a", false}, Record{"b", false}, Record{"c", false}},
			{Record{"d", false}, Record{"e", false}, Record{"f", false}},
		},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    0,
	}, {
		Name:             "TrimSpace",
		Input:            " a,  b,   c\n",
		Output:           [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}}},
		TrimLeadingSpace: true,
	}, {
		Name:   "LeadingSpace",
		Input:  " a,  b,   c\n",
		Output: [][]Record{{Record{" a", false}, Record{"  b", false}, Record{"   c", false}}},
	}, {
		Name:    "Comment",
		Input:   "#1,2,3\na,b,c\n#comment",
		Output:  [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}}},
		Comment: '#',
	}, {
		Name:  "NoComment",
		Input: "#1,2,3\na,b,c",
		Output: [][]Record{
			{Record{"#1", false}, Record{"2", false}, Record{"3", false}},
			{Record{"a", false}, Record{"b", false}, Record{"c", false}},
		},
	}, {
		Name:       "LazyQuotes",
		Input:      `a "word","1"2",a","b`,
		Output:     [][]Record{{Record{`a "word"`, false}, Record{`1"2`, true}, Record{`a"`, false}, Record{`b`, true}}},
		LazyQuotes: true,
	}, {
		Name:       "BareQuotes",
		Input:      `a "word","1"2",a"`,
		Output:     [][]Record{{Record{`a "word"`, false}, Record{`1"2`, true}, Record{`a"`, false}}},
		LazyQuotes: true,
	}, {
		Name:       "BareDoubleQuotes",
		Input:      `a""b,c`,
		Output:     [][]Record{{Record{`a""b`, false}, Record{`c`, false}}},
		LazyQuotes: true,
	}, {
		Name:  "BadDoubleQuotes",
		Input: `a""b,c`,
		Error: &ParseError{StartLine: 1, Line: 1, Column: 1, Err: ErrBareQuote},
	}, {
		Name:             "TrimQuote",
		Input:            ` "a"," b",c`,
		Output:           [][]Record{{Record{"a", true}, Record{" b", true}, Record{"c", false}}},
		TrimLeadingSpace: true,
	}, {
		Name:  "BadBareQuote",
		Input: `a "word","b"`,
		Error: &ParseError{StartLine: 1, Line: 1, Column: 2, Err: ErrBareQuote},
	}, {
		Name:  "BadTrailingQuote",
		Input: `"a word",b"`,
		Error: &ParseError{StartLine: 1, Line: 1, Column: 10, Err: ErrBareQuote},
	}, {
		Name:  "ExtraneousQuote",
		Input: `"a "word","b"`,
		Error: &ParseError{StartLine: 1, Line: 1, Column: 3, Err: ErrQuote},
	}, {
		Name:               "BadFieldCount",
		Input:              "a,b,c\nd,e",
		Error:              &ParseError{StartLine: 2, Line: 2, Err: ErrFieldCount},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    0,
	}, {
		Name:               "BadFieldCount1",
		Input:              `a,b,c`,
		Error:              &ParseError{StartLine: 1, Line: 1, Err: ErrFieldCount},
		UseFieldsPerRecord: true,
		FieldsPerRecord:    2,
	}, {
		Name:  "FieldCount",
		Input: "a,b,c\nd,e",
		Output: [][]Record{
			{Record{"a", false}, Record{"b", false}, Record{"c", false}},
			{Record{"d", false}, Record{"e", false}},
		},
	}, {
		Name:   "TrailingCommaEOF",
		Input:  "a,b,c,",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}, Record{"", false}}},
	}, {
		Name:   "TrailingCommaEOL",
		Input:  "a,b,c,\n",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}, Record{"", false}}},
	}, {
		Name:             "TrailingCommaSpaceEOF",
		Input:            "a,b,c, ",
		Output:           [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}, Record{"", false}}},
		TrimLeadingSpace: true,
	}, {
		Name:             "TrailingCommaSpaceEOL",
		Input:            "a,b,c, \n",
		Output:           [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}, Record{"", false}}},
		TrimLeadingSpace: true,
	}, {
		Name:  "TrailingCommaLine3",
		Input: "a,b,c\nd,e,f\ng,hi,",
		Output: [][]Record{
			{Record{"a", false}, Record{"b", false}, Record{"c", false}},
			{Record{"d", false}, Record{"e", false}, Record{"f", false}},
			{Record{"g", false}, Record{"hi", false}, Record{"", false}}},
		TrimLeadingSpace: true,
	}, {
		Name:   "NotTrailingComma3",
		Input:  "a,b,c, \n",
		Output: [][]Record{{Record{"a", false}, Record{"b", false}, Record{"c", false}, Record{" ", false}}},
	}, {
		Name: "CommaFieldTest",
		Input: `x,y,z,w
x,y,z,
x,y,,
x,,,
,,,
"x","y","z","w"
"x","y","z",""
"x","y","",""
"x","","",""
"","","",""
`,
		Output: [][]Record{
			{Record{"x", false}, Record{"y", false}, Record{"z", false}, Record{"w", false}},
			{Record{"x", false}, Record{"y", false}, Record{"z", false}, Record{"", false}},
			{Record{"x", false}, Record{"y", false}, Record{"", false}, Record{"", false}},
			{Record{"x", false}, Record{"", false}, Record{"", false}, Record{"", false}},
			{Record{"", false}, Record{"", false}, Record{"", false}, Record{"", false}},
			{Record{"x", true}, Record{"y", true}, Record{"z", true}, Record{"w", true}},
			{Record{"x", true}, Record{"y", true}, Record{"z", true}, Record{"", true}},
			{Record{"x", true}, Record{"y", true}, Record{"", true}, Record{"", true}},
			{Record{"x", true}, Record{"", true}, Record{"", true}, Record{"", true}},
			{Record{"", true}, Record{"", true}, Record{"", true}, Record{"", true}},
		},
	}, {
		Name:  "TrailingCommaIneffective1",
		Input: "a,b,\nc,d,e",
		Output: [][]Record{
			{Record{"a", false}, Record{"b", false}, Record{"", false}},
			{Record{"c", false}, Record{"d", false}, Record{"e", false}},
		},
		TrimLeadingSpace: true,
	}, {
		Name:  "ReadAllReuseRecord",
		Input: "a,b\nc,d",
		Output: [][]Record{
			{Record{"a", false}, Record{"b", false}},
			{Record{"c", false}, Record{"d", false}},
		},
		ReuseRecord: true,
	}, {
		Name:  "StartLine1", // Issue 19019
		Input: "a,\"b\nc\"d,e",
		Error: &ParseError{StartLine: 1, Line: 2, Column: 1, Err: ErrQuote},
	}, {
		Name:  "StartLine2",
		Input: "a,b\n\"d\n\n,e",
		Error: &ParseError{StartLine: 2, Line: 5, Column: 0, Err: ErrQuote},
	}, {
		Name:  "CRLFInQuotedField", // Issue 21201
		Input: "\"Hello\r\nHi\"",
		Output: [][]Record{
			{Record{"Hello\r\nHi", true}},
		},
	}, {
		Name:   "BinaryBlobField", // Issue 19410
		Input:  "x09\x41\xb4\x1c,aktau",
		Output: [][]Record{{Record{"x09A\xb4\x1c", false}, Record{"aktau", false}}},
	}, {
		Name:   "TrailingCR",
		Input:  "field1,field2\r",
		Output: [][]Record{{Record{"field1", false}, Record{"field2", false}}},
	}, {
		Name:   "QuotedTrailingCR",
		Input:  "\"field\"\r",
		Output: [][]Record{{Record{"field", true}}},
	}, {
		Name:  "QuotedTrailingCRCR",
		Input: "\"field\"\r\r",
		Error: &ParseError{StartLine: 1, Line: 1, Column: 6, Err: ErrQuote},
	}, {
		Name:   "FieldCR",
		Input:  "field\rfield\r",
		Output: [][]Record{{Record{"field\rfield", false}}},
	}, {
		Name:   "FieldCRCR",
		Input:  "field\r\rfield\r\r",
		Output: [][]Record{{Record{"field\r\rfield\r", false}}},
	}, {
		Name:   "FieldCRCRLF",
		Input:  "field\r\r\nfield\r\r\n",
		Output: [][]Record{{Record{"field\r", false}}, {Record{"field\r", false}}},
	}, {
		Name:   "FieldCRCRLFCR",
		Input:  "field\r\r\n\rfield\r\r\n\r",
		Output: [][]Record{{Record{"field\r", false}}, {Record{"\rfield\r", false}}},
	}, {
		Name:  "FieldCRCRLFCRCR",
		Input: "field\r\r\n\r\rfield\r\r\n\r\r",
		Output: [][]Record{
			{Record{"field\r", false}},
			{Record{"\r\rfield\r", false}},
			{Record{"\r", false}},
		},
	}, {
		Name:  "MultiFieldCRCRLFCRCR",
		Input: "field1,field2\r\r\n\r\rfield1,field2\r\r\n\r\r,",
		Output: [][]Record{
			{Record{"field1", false}, Record{"field2\r", false}},
			{Record{"\r\rfield1", false}, Record{"field2\r", false}},
			{Record{"\r\r", false}, Record{"", false}},
		},
	}, {
		Name:             "NonASCIICommaAndComment",
		Input:            "a£b,c£ \td,e\n€ comment\n",
		Output:           [][]Record{{Record{"a", false}, Record{"b,c", false}, Record{"d,e", false}}},
		TrimLeadingSpace: true,
		Comma:            '£',
		Comment:          '€',
	}, {
		Name:    "NonASCIICommaAndCommentWithQuotes",
		Input:   "a€\"  b,\"€ c\nλ comment\n",
		Output:  [][]Record{{Record{"a", false}, Record{"  b,", true}, Record{" c", false}}},
		Comma:   '€',
		Comment: 'λ',
	}, {
		// λ and θ start with the same byte.
		// This tests that the parser doesn't confuse such characters.
		Name:    "NonASCIICommaConfusion",
		Input:   "\"abθcd\"λefθgh",
		Output:  [][]Record{{Record{"abθcd", true}, Record{"efθgh", false}}},
		Comma:   'λ',
		Comment: '€',
	}, {
		Name:    "NonASCIICommentConfusion",
		Input:   "λ\nλ\nθ\nλ\n",
		Output:  [][]Record{{Record{"λ", false}}, {Record{"λ", false}}, {Record{"λ", false}}},
		Comment: 'θ',
	}, {
		Name:   "QuotedFieldMultipleLF",
		Input:  "\"\n\n\n\n\"",
		Output: [][]Record{{Record{"\n\n\n\n", true}}},
	}, {
		Name:  "MultipleCRLF",
		Input: "\r\n\r\n\r\n\r\n",
	}, {
		// The implementation may read each line in several chunks if it doesn't fit entirely
		// in the read buffer, so we should test the code to handle that condition.
		Name:    "HugeLines",
		Input:   strings.Repeat("#ignore\n", 10000) + strings.Repeat("@", 5000) + "," + strings.Repeat("*", 5000),
		Output:  [][]Record{{Record{strings.Repeat("@", 5000), false}, Record{strings.Repeat("*", 5000), false}}},
		Comment: '#',
	}, {
		Name:  "QuoteWithTrailingCRLF",
		Input: "\"foo\"bar\"\r\n",
		Error: &ParseError{StartLine: 1, Line: 1, Column: 4, Err: ErrQuote},
	}, {
		Name:       "LazyQuoteWithTrailingCRLF",
		Input:      "\"foo\"bar\"\r\n",
		Output:     [][]Record{{Record{`foo"bar`, true}}},
		LazyQuotes: true,
	}, {
		Name:   "DoubleQuoteWithTrailingCRLF",
		Input:  "\"foo\"\"bar\"\r\n",
		Output: [][]Record{{Record{`foo"bar`, true}}},
	}, {
		Name:   "EvenQuotes",
		Input:  `""""""""`,
		Output: [][]Record{{Record{`"""`, true}}},
	}, {
		Name:  "OddQuotes",
		Input: `"""""""`,
		Error: &ParseError{StartLine: 1, Line: 1, Column: 7, Err: ErrQuote},
	}, {
		Name:       "LazyOddQuotes",
		Input:      `"""""""`,
		Output:     [][]Record{{Record{`"""`, true}}},
		LazyQuotes: true,
	}, {
		Name:  "BadComma1",
		Comma: '\n',
		Error: errInvalidDelim,
	}, {
		Name:  "BadComma2",
		Comma: '\r',
		Error: errInvalidDelim,
	}, {
		Name:  "BadComma3",
		Comma: utf8.RuneError,
		Error: errInvalidDelim,
	}, {
		Name:    "BadComment1",
		Comment: '\n',
		Error:   errInvalidDelim,
	}, {
		Name:    "BadComment2",
		Comment: '\r',
		Error:   errInvalidDelim,
	}, {
		Name:    "BadComment3",
		Comment: utf8.RuneError,
		Error:   errInvalidDelim,
	}, {
		Name:    "BadCommaComment",
		Comma:   'X',
		Comment: 'X',
		Error:   errInvalidDelim,
	}, {
		Name:   "EscapeText",
		Escape: 'x',
		Input:  `"x"",",","xxx"",x,"xxxx,"` + "\n",
		Output: [][]Record{{Record{`"`, true}, Record{`,`, true}, Record{`x"`, true}, Record{`x`, false}, Record{`xx,`, true}}},
	}, {
		Name:   "EscapeTextWithComma",
		Escape: 'x',
		Comma:  'x',
		Input:  `"x""x,x"xxx""x"xx"x"xxxx,"` + "\n",
		Output: [][]Record{{Record{`"`, true}, Record{`,`, false}, Record{`x"`, true}, Record{`x`, true}, Record{`xx,`, true}}},
	}, {
		Name:   "EscapeTextWithNonEscapingCharacter",
		Escape: 'x',
		Input:  `"xxx,xa",",x,"` + "\n",
		Output: [][]Record{{Record{`xx,xa`, true}, Record{`,x,`, true}}},
	}, {
		Name:   "EscapeTextWithComma",
		Escape: 'x',
		Input:  `a,"""` + "\n",
		Error:  &ParseError{StartLine: 1, Line: 1, Column: 3, Err: ErrQuote},
	}, {
		Name:   "EscapeTrailingQuote",
		Escape: 'x',
		Input:  `"x"` + "\n",
		Error:  &ParseError{StartLine: 1, Line: 2, Column: 0, Err: ErrQuote},
	}}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.Input))

			if tt.Comma != 0 {
				r.Comma = tt.Comma
			}
			if tt.Escape != 0 {
				r.Escape = tt.Escape
			}
			r.Comment = tt.Comment
			if tt.UseFieldsPerRecord {
				r.FieldsPerRecord = tt.FieldsPerRecord
			} else {
				r.FieldsPerRecord = -1
			}
			r.LazyQuotes = tt.LazyQuotes
			r.TrimLeadingSpace = tt.TrimLeadingSpace
			r.ReuseRecord = tt.ReuseRecord

			out, err := r.ReadAll()
			if !reflect.DeepEqual(err, tt.Error) {
				t.Errorf("ReadAll() error:\ngot  %v\nwant %v", err, tt.Error)
			} else if !reflect.DeepEqual(out, tt.Output) {
				t.Errorf("ReadAll() output:\ngot  %#v\nwant %#v", out, tt.Output)
			}

			// Check that the error can be rendered.
			if err != nil {
				_ = err.Error()
			}
		})
	}
}

// nTimes is an io.Reader which yields the string s n times.
type nTimes struct {
	s   string
	n   int
	off int
}

func (r *nTimes) Read(p []byte) (n int, err error) {
	for {
		if r.n <= 0 || r.s == "" {
			return n, io.EOF
		}
		n0 := copy(p, r.s[r.off:])
		p = p[n0:]
		n += n0
		r.off += n0
		if r.off == len(r.s) {
			r.off = 0
			r.n--
		}
		if len(p) == 0 {
			return
		}
	}
}

// benchmarkRead measures reading the provided CSV rows data.
// initReader, if non-nil, modifies the Reader before it's used.
func benchmarkRead(b *testing.B, initReader func(*Reader), rows string) {
	b.ReportAllocs()
	r := NewReader(&nTimes{s: rows, n: b.N})
	if initReader != nil {
		initReader(r)
	}
	for {
		_, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

const benchmarkCSVData = `x,y,z,w
x,y,z,
x,y,,
x,,,
,,,
"x","y","z","w"
"x","y","z",""
"x","y","",""
"x","","",""
"","","",""
`

func BenchmarkRead(b *testing.B) {
	benchmarkRead(b, nil, benchmarkCSVData)
}

func BenchmarkReadWithFieldsPerRecord(b *testing.B) {
	benchmarkRead(b, func(r *Reader) { r.FieldsPerRecord = 4 }, benchmarkCSVData)
}

func BenchmarkReadWithoutFieldsPerRecord(b *testing.B) {
	benchmarkRead(b, func(r *Reader) { r.FieldsPerRecord = -1 }, benchmarkCSVData)
}

func BenchmarkReadLargeFields(b *testing.B) {
	benchmarkRead(b, nil, strings.Repeat(`xxxxxxxxxxxxxxxx,yyyyyyyyyyyyyyyy,zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
xxxxxxxxxxxxxxxxxxxxxxxx,yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy,zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvv
,,zzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy,zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
`, 3))
}

func BenchmarkReadReuseRecord(b *testing.B) {
	benchmarkRead(b, func(r *Reader) { r.ReuseRecord = true }, benchmarkCSVData)
}

func BenchmarkReadReuseRecordWithFieldsPerRecord(b *testing.B) {
	benchmarkRead(b, func(r *Reader) { r.ReuseRecord = true; r.FieldsPerRecord = 4 }, benchmarkCSVData)
}

func BenchmarkReadReuseRecordWithoutFieldsPerRecord(b *testing.B) {
	benchmarkRead(b, func(r *Reader) { r.ReuseRecord = true; r.FieldsPerRecord = -1 }, benchmarkCSVData)
}

func BenchmarkReadReuseRecordLargeFields(b *testing.B) {
	benchmarkRead(b, func(r *Reader) { r.ReuseRecord = true }, strings.Repeat(`xxxxxxxxxxxxxxxx,yyyyyyyyyyyyyyyy,zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
xxxxxxxxxxxxxxxxxxxxxxxx,yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy,zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvv
,,zzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx,yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy,zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz,wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww,vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
`, 3))
}
