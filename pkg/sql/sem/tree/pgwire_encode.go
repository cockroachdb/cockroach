// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"bytes"
	"unicode/utf8"

	"github.com/lib/pq/oid"
)

func (d *DTuple) pgwireFormat(ctx *FmtCtx) {
	// When converting a tuple to text in "postgres mode" there is
	// special behavior: values are printed in "postgres mode" then the
	// result string itself is rendered in "postgres mode".
	// Immediate NULL tuple elements are printed as the empty string.
	//
	// In this last conversion, for *tuples* the special double quote
	// and backslash characters are *doubled* (not escaped).  Other
	// special characters from C like \t \n etc are not escaped and
	// instead printed as-is. Only non-valid characters get escaped to
	// hex. So we delegate this formatting to a tuple-specific
	// string printer called pgwireFormatStringInTuple().
	ctx.WriteByte('(')
	comma := ""
	for _, v := range d.D {
		ctx.WriteString(comma)
		switch dv := UnwrapDatum(nil, v).(type) {
		case dNull:
		case *DString:
			pgwireFormatStringInTuple(&ctx.Buffer, string(*dv))
		case *DCollatedString:
			pgwireFormatStringInTuple(&ctx.Buffer, dv.Contents)
			// Bytes cannot use the default case because they will be incorrectly
			// double escaped.
		case *DBytes:
			ctx.FormatNode(dv)
		case *DJSON:
			var buf bytes.Buffer
			dv.JSON.Format(&buf)
			pgwireFormatStringInTuple(&ctx.Buffer, buf.String())
		default:
			s := AsStringWithFlags(v, ctx.flags)
			pgwireFormatStringInTuple(&ctx.Buffer, s)
		}
		comma = ","
	}
	ctx.WriteByte(')')
}

func pgwireFormatStringInTuple(buf *bytes.Buffer, in string) {
	quote := pgwireQuoteStringInTuple(in)
	if quote {
		buf.WriteByte('"')
	}
	// Loop through each unicode code point.
	for _, r := range in {
		if r == '"' || r == '\\' {
			// Strings in tuples double " and \.
			buf.WriteByte(byte(r))
			buf.WriteByte(byte(r))
		} else {
			buf.WriteRune(r)
		}
	}
	if quote {
		buf.WriteByte('"')
	}
}

func (d *DArray) pgwireFormat(ctx *FmtCtx) {
	// When converting an array to text in "postgres mode" there is
	// special behavior: values are printed in "postgres mode" then the
	// result string itself is rendered in "postgres mode".
	// Immediate NULL array elements are printed as "NULL".
	//
	// In this last conversion, for *arrays* the special double quote
	// and backslash characters are *escaped* (not doubled).  Other
	// special characters from C like \t \n etc are not escaped and
	// instead printed as-is. Only non-valid characters get escaped to
	// hex. So we delegate this formatting to a tuple-specific
	// string printer called pgwireFormatStringInArray().
	switch d.ResolvedType().Oid() {
	case oid.T_int2vector, oid.T_oidvector:
		// vectors are serialized as a string of space-separated values.
		sep := ""
		// TODO(justin): add a test for nested arrays when #32552 is
		// addressed.
		for _, d := range d.Array {
			ctx.WriteString(sep)
			ctx.FormatNode(d)
			sep = " "
		}
		return
	}

	ctx.WriteByte('{')
	comma := ""
	for _, v := range d.Array {
		ctx.WriteString(comma)
		switch dv := UnwrapDatum(nil, v).(type) {
		case dNull:
			ctx.WriteString("NULL")
		case *DString:
			pgwireFormatStringInArray(&ctx.Buffer, string(*dv))
		case *DCollatedString:
			pgwireFormatStringInArray(&ctx.Buffer, dv.Contents)
			// Bytes cannot use the default case because they will be incorrectly
			// double escaped.
		case *DBytes:
			ctx.FormatNode(dv)
		default:
			s := AsStringWithFlags(v, ctx.flags)
			pgwireFormatStringInArray(&ctx.Buffer, s)
		}
		comma = ","
	}
	ctx.WriteByte('}')
}

var tupleQuoteSet, arrayQuoteSet asciiSet

func init() {
	var ok bool
	tupleQuoteSet, ok = makeASCIISet(" \t\v\f\r\n(),\"\\")
	if !ok {
		panic("tuple asciiset")
	}
	arrayQuoteSet, ok = makeASCIISet(" \t\v\f\r\n{},\"\\")
	if !ok {
		panic("array asciiset")
	}
}

func pgwireQuoteStringInTuple(in string) bool {
	return in == "" || tupleQuoteSet.in(in)
}

func pgwireQuoteStringInArray(in string) bool {
	if in == "" || arrayQuoteSet.in(in) {
		return true
	}
	if len(in) == 4 &&
		(in[0] == 'n' || in[0] == 'N') &&
		(in[1] == 'u' || in[1] == 'U') &&
		(in[2] == 'l' || in[2] == 'L') &&
		(in[3] == 'l' || in[3] == 'L') {
		return true
	}
	return false
}

func pgwireFormatStringInArray(buf *bytes.Buffer, in string) {
	quote := pgwireQuoteStringInArray(in)
	if quote {
		buf.WriteByte('"')
	}
	// Loop through each unicode code point.
	for _, r := range in {
		if r == '"' || r == '\\' {
			// Strings in arrays escape " and \.
			buf.WriteByte('\\')
			buf.WriteByte(byte(r))
		} else {
			buf.WriteRune(r)
		}
	}
	if quote {
		buf.WriteByte('"')
	}
}

// From: https://github.com/golang/go/blob/master/src/strings/strings.go

// asciiSet is a 32-byte value, where each bit represents the presence of a
// given ASCII character in the set. The 128-bits of the lower 16 bytes,
// starting with the least-significant bit of the lowest word to the
// most-significant bit of the highest word, map to the full range of all
// 128 ASCII characters. The 128-bits of the upper 16 bytes will be zeroed,
// ensuring that any non-ASCII character will be reported as not in the set.
type asciiSet [8]uint32

// makeASCIISet creates a set of ASCII characters and reports whether all
// characters in chars are ASCII.
func makeASCIISet(chars string) (as asciiSet, ok bool) {
	for i := 0; i < len(chars); i++ {
		c := chars[i]
		if c >= utf8.RuneSelf {
			return as, false
		}
		as[c>>5] |= 1 << uint(c&31)
	}
	return as, true
}

// contains reports whether c is inside the set.
func (as *asciiSet) contains(c byte) bool {
	return (as[c>>5] & (1 << uint(c&31))) != 0
}

// in reports whether any member of the set is in s.
func (as *asciiSet) in(s string) bool {
	for i := 0; i < len(s); i++ {
		if as.contains(s[i]) {
			return true
		}
	}
	return false
}
