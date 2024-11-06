// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"bytes"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/lib/pq/oid"
)

// spaces is a static slice of spaces used for efficient padding.
var spaces = bytes.Repeat([]byte{' '}, system.CacheLineSize)

// ResolveBlankPaddedChar pads the given string with spaces if blank padding is
// required or returns the string unmodified otherwise.
func ResolveBlankPaddedChar(s string, t *types.T) string {
	if t.Oid() == oid.T_bpchar && len(s) < int(t.Width()) {
		// Pad spaces on the right of the string to make it of length specified
		// in the type t.
		res := make([]byte, t.Width())
		copy(res, s)
		pad := res[len(s):]
		for len(pad) > len(spaces) {
			copy(pad, spaces)
			pad = pad[len(spaces):]
		}
		copy(pad, spaces)
		return encoding.UnsafeConvertBytesToString(res)
	}
	return s
}

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
	tc := d.ResolvedType().TupleContents()
	for i, v := range d.D {
		ctx.WriteString(comma)
		var t *types.T
		if i < len(tc) {
			t = tc[i]
		} else {
			t = v.ResolvedType()
		}
		switch dv := UnwrapDOidWrapper(v).(type) {
		case dNull:
		case *DString:
			s := ResolveBlankPaddedChar(string(*dv), t)
			pgwireFormatStringInTuple(&ctx.Buffer, s)
		case *DCollatedString:
			s := ResolveBlankPaddedChar(dv.Contents, t)
			pgwireFormatStringInTuple(&ctx.Buffer, s)
			// Bytes cannot use the default case because they will be incorrectly
			// double escaped.
		case *DBytes:
			ctx.WriteString(`"\`)
			ctx.FormatNode(dv)
			ctx.WriteString(`"`)
		case *DJSON:
			var buf bytes.Buffer
			dv.JSON.Format(&buf)
			pgwireFormatStringInTuple(&ctx.Buffer, buf.String())
		case *DFloat:
			fl := float64(*dv)
			b := PgwireFormatFloat(nil /*buf*/, fl, ctx.dataConversionConfig, t)
			ctx.WriteString(string(b))
		default:
			s := AsStringWithFlags(v, ctx.flags, FmtDataConversionConfig(ctx.dataConversionConfig), FmtLocation(ctx.location))
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

	if ctx.HasFlags(fmtPGCatalog) {
		ctx.WriteByte('\'')
	}
	ctx.WriteByte('{')
	delimiter := ""
	for _, v := range d.Array {
		ctx.WriteString(delimiter)
		switch dv := UnwrapDOidWrapper(v).(type) {
		case dNull:
			ctx.WriteString("NULL")
		case *DString:
			pgwireFormatStringInArray(ctx, string(*dv))
		case *DCollatedString:
			pgwireFormatStringInArray(ctx, dv.Contents)
			// Bytes cannot use the default case because they will be incorrectly
			// double escaped.
		case *DBytes:
			ctx.WriteString(`"\`)
			ctx.FormatNode(dv)
			ctx.WriteString(`"`)
		case *DFloat:
			fl := float64(*dv)
			floatTyp := d.ResolvedType().ArrayContents()
			b := PgwireFormatFloat(nil /*buf*/, fl, ctx.dataConversionConfig, floatTyp)
			ctx.WriteString(string(b))
		case *DJSON:
			flags := ctx.flags | fmtRawStrings
			s := AsStringWithFlags(v, flags, FmtDataConversionConfig(ctx.dataConversionConfig), FmtLocation(ctx.location))
			pgwireFormatStringInArray(ctx, s)
		default:
			s := AsStringWithFlags(v, ctx.flags, FmtDataConversionConfig(ctx.dataConversionConfig), FmtLocation(ctx.location))
			pgwireFormatStringInArray(ctx, s)
		}
		delimiter = d.ParamTyp.Delimiter()
	}
	ctx.WriteByte('}')
	if ctx.HasFlags(fmtPGCatalog) {
		ctx.WriteByte('\'')
	}
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

// PgwireFormatFloat returns a []byte representing a float according to
// pgwire encoding. The result is appended to the given buffer.
func PgwireFormatFloat(
	buf []byte, fl float64, conv sessiondatapb.DataConversionConfig, floatTyp *types.T,
) []byte {
	// PostgreSQL supports 'Inf' as a valid literal for the floating point
	// special value Infinity, therefore handling the special cases for them.
	// (https://github.com/cockroachdb/cockroach/issues/62601)
	if math.IsInf(fl, 1) {
		return append(buf, []byte("Infinity")...)
	} else if math.IsInf(fl, -1) {
		return append(buf, []byte("-Infinity")...)
	} else {
		return strconv.AppendFloat(
			buf, fl, 'g',
			conv.GetFloatPrec(floatTyp),
			int(floatTyp.Width()),
		)
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

func pgwireFormatStringInArray(ctx *FmtCtx, in string) {
	buf := &ctx.Buffer
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
		} else if ctx.HasFlags(fmtPGCatalog) && r == '\'' {
			buf.WriteByte('\'')
			buf.WriteByte('\'')
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

// This block contains all available PG time formats.
const (
	PGTimeFormat              = "15:04:05.999999"
	PGDateFormat              = "2006-01-02"
	PGTimeStampFormatNoOffset = PGDateFormat + " " + PGTimeFormat
	PGTimeStampFormat         = PGTimeStampFormatNoOffset + "-07"
	PGTime2400Format          = "24:00:00"
	PGTimeTZFormat            = PGTimeFormat + "-07"
)

// PGWireFormatTime formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
func PGWireFormatTime(t timeofday.TimeOfDay, tmp []byte) []byte {
	return t.AppendFormat(tmp)
}

// PGWireFormatTimeTZ formats t into a format lib/pq understands, appending to the
// provided tmp buffer and reallocating if needed. The function will then return
// the resulting buffer.
func PGWireFormatTimeTZ(t timetz.TimeTZ, tmp []byte) []byte {
	format := PGTimeTZFormat
	if t.OffsetSecs%60 != 0 {
		format += ":00:00"
	} else if t.OffsetSecs%3600 != 0 {
		format += ":00"
	}
	ret := t.ToTime().AppendFormat(tmp, format)
	// time.Time's AppendFormat does not recognize 2400, so special case it accordingly.
	if t.TimeOfDay == timeofday.Time2400 {
		// It instead reads 00:00:00. Replace that text.
		var newRet []byte
		newRet = append(newRet, PGTime2400Format...)
		newRet = append(newRet, ret[len(PGTime2400Format):]...)
		ret = newRet
	}
	return ret
}

// PGWireFormatTimestamp formats t into a format lib/pq understands.
// If offset is not nil, it will not display the timezone offset.
func PGWireFormatTimestamp(t time.Time, offset *time.Location, tmp []byte) (b []byte) {
	if t == pgdate.TimeInfinity {
		return []byte("infinity")
	}
	if t == pgdate.TimeNegativeInfinity {
		return []byte("-infinity")
	}

	format := PGTimeStampFormatNoOffset
	if offset != nil {
		format = PGTimeStampFormat
		if _, offsetSeconds := t.In(offset).Zone(); offsetSeconds%60 != 0 {
			format += ":00:00"
		} else if offsetSeconds%3600 != 0 {
			format += ":00"
		}
	}

	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	if offset != nil {
		t = t.In(offset)
	}

	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}

	b = t.AppendFormat(tmp, format)
	if bc {
		b = append(b, " BC"...)
	}
	return b
}
