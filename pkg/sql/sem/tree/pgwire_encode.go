// Copyright 2018 The Cockroach Authors.
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

package tree

import (
	"bytes"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/util/stringencoding"
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
		switch dv := v.(type) {
		case *DTuple, *DArray:
			s := AsStringWithFlags(v, ctx.flags)
			pgwireFormatStringInTuple(ctx.Buffer, s)
		case *DString:
			pgwireFormatStringInTuple(ctx.Buffer, string(*dv))
		case *DCollatedString:
			pgwireFormatStringInTuple(ctx.Buffer, dv.Contents)
		default:
			ctx.FormatNode(v)
		}
		comma = ","
	}
	ctx.WriteByte(')')
}

func pgwireFormatStringInTuple(buf *bytes.Buffer, in string) {
	// TODO(knz): to be fully pg-compliant, this function should avoid
	// enclosing the string in double quotes if there is no special
	// character inside.
	buf.WriteByte('"')
	// Loop through each unicode code point.
	for i, r := range in {
		if r == '"' || r == '\\' {
			// Strings in tuples double " and \.
			buf.WriteByte(byte(r))
			buf.WriteByte(byte(r))
		} else if unicode.IsGraphic(r) {
			buf.WriteRune(r)
		} else {
			stringencoding.EncodeChar(buf, in, r, i)
		}
	}
	buf.WriteByte('"')
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
	ctx.WriteByte('{')
	comma := ""
	for _, v := range d.Array {
		ctx.WriteString(comma)
		switch dv := v.(type) {
		case dNull:
			ctx.WriteString("NULL")
		case *DTuple, *DArray:
			s := AsStringWithFlags(v, ctx.flags)
			pgwireFormatStringInArray(ctx.Buffer, s)
		case *DString:
			pgwireFormatStringInArray(ctx.Buffer, string(*dv))
		case *DCollatedString:
			pgwireFormatStringInArray(ctx.Buffer, dv.Contents)
		default:
			ctx.FormatNode(v)
		}
		comma = ","
	}
	ctx.WriteByte('}')
}

func pgwireFormatStringInArray(buf *bytes.Buffer, in string) {
	// TODO(knz): to be fully pg-compliant, this function should avoid
	// enclosing the string in double quotes if there is no special
	// character inside.
	buf.WriteByte('"')
	// Loop through each unicode code point.
	for i, r := range in {
		if r == '"' || r == '\\' {
			// Strings in arrays escape " and \.
			buf.WriteByte('\\')
			buf.WriteByte(byte(r))
		} else if unicode.IsGraphic(r) {
			buf.WriteRune(r)
		} else {
			stringencoding.EncodeChar(buf, in, r, i)
		}
	}
	buf.WriteByte('"')
}
