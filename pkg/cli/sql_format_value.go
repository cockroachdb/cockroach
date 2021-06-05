// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/lib/pq"
)

func isNotPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7e || r == '"' || r == '\\' }
func isNotGraphicUnicode(r rune) bool { return !unicode.IsGraphic(r) }
func isNotGraphicUnicodeOrTabOrNewline(r rune) bool {
	return r != '\t' && r != '\n' && !unicode.IsGraphic(r)
}

func formatVal(
	val driver.Value, colType string, showPrintableUnicode bool, showNewLinesAndTabs bool,
) string {
	log.VInfof(context.Background(), 2, "value: go %T, sql %q", val, colType)

	if b, ok := val.([]byte); ok {
		if strings.HasPrefix(colType, "_") && len(b) > 0 && b[0] == '{' {
			return formatArray(b, colType[1:], showPrintableUnicode, showNewLinesAndTabs)
		}

		if colType == "NAME" {
			val = string(b)
			colType = "VARCHAR"
		}
	}

	switch t := val.(type) {
	case nil:
		return "NULL"

	case float64:
		width := 64
		if colType == "FLOAT4" {
			width = 32
		}
		if math.IsInf(t, 1) {
			return "Infinity"
		} else if math.IsInf(t, -1) {
			return "-Infinity"
		}
		return strconv.FormatFloat(t, 'g', -1, width)

	case string:
		if showPrintableUnicode {
			pred := isNotGraphicUnicode
			if showNewLinesAndTabs {
				pred = isNotGraphicUnicodeOrTabOrNewline
			}
			if utf8.ValidString(t) && strings.IndexFunc(t, pred) == -1 {
				return t
			}
		} else {
			if strings.IndexFunc(t, isNotPrintableASCII) == -1 {
				return t
			}
		}
		s := fmt.Sprintf("%+q", t)
		// Strip the start and final quotes. The surrounding display
		// format (e.g. CSV/TSV) will add its own quotes.
		return s[1 : len(s)-1]

	case []byte:
		// Format the bytes as per bytea_output = escape.
		//
		// We use the "escape" format here because it enables printing
		// readable strings as-is -- the default hex format would always
		// render as hexadecimal digits. The escape format is also more
		// compact.
		//
		// TODO(knz): this formatting is unfortunate/incorrect, and exists
		// only because lib/pq incorrectly interprets the bytes received
		// from the server. The proper behavior would be for the driver to
		// not interpret the bytes and for us here to print that as-is, so
		// that we can let the user see and control the result using
		// `bytea_output`.
		return lex.EncodeByteArrayToRawBytes(string(t),
			sessiondatapb.BytesEncodeEscape, false /* skipHexPrefix */)

	case time.Time:
		tfmt, ok := timeOutputFormats[colType]
		if !ok {
			// Some unknown/new time-like format.
			tfmt = timeutil.FullTimeFormat
		}
		if tfmt == timeutil.TimestampWithTZFormat || tfmt == timeutil.TimeWithTZFormat {
			if _, offsetSeconds := t.Zone(); offsetSeconds%60 != 0 {
				tfmt += ":00:00"
			} else if offsetSeconds%3600 != 0 {
				tfmt += ":00"
			}
		}
		return t.Format(tfmt)
	}

	return fmt.Sprint(val)
}

func formatArray(
	b []byte, colType string, showPrintableUnicode bool, showNewLinesAndTabs bool,
) string {
	// backingArray is the array we're going to parse the server data
	// into.
	var backingArray interface{}
	// parsingArray is a helper structure provided by lib/pq to parse
	// arrays.
	var parsingArray gosql.Scanner

	// lib.pq has different array parsers for special value types.
	//
	// TODO(knz): This would better use a general-purpose parser
	// using the OID to look up an array parser in crdb's sql package.
	// However, unfortunately the OID is hidden from us.
	switch colType {
	case "BOOL":
		boolArray := []bool{}
		backingArray = &boolArray
		parsingArray = (*pq.BoolArray)(&boolArray)
	case "FLOAT4", "FLOAT8":
		floatArray := []float64{}
		backingArray = &floatArray
		parsingArray = (*pq.Float64Array)(&floatArray)
	case "INT2", "INT4", "INT8", "OID":
		intArray := []int64{}
		backingArray = &intArray
		parsingArray = (*pq.Int64Array)(&intArray)
	case "TEXT", "VARCHAR", "NAME", "CHAR", "BPCHAR":
		stringArray := []string{}
		backingArray = &stringArray
		parsingArray = (*pq.StringArray)(&stringArray)
	default:
		genArray := [][]byte{}
		backingArray = &genArray
		parsingArray = &pq.GenericArray{A: &genArray}
	}

	// Now ask the pq array parser to convert the byte slice
	// from the server into a Go array.
	if err := parsingArray.Scan(b); err != nil {
		// A parsing failure is not a catastrophe; we can still print out
		// the array as a byte slice. This will do in many cases.
		log.VInfof(context.Background(), 1, "unable to parse %q (sql %q) as array: %v", b, colType, err)
		return formatVal(b, "BYTEA", showPrintableUnicode, showNewLinesAndTabs)
	}

	// We have a go array in "backingArray". Now print it out.
	var buf strings.Builder
	buf.WriteByte('{')
	comma := "" // delimiter
	v := reflect.ValueOf(backingArray).Elem()
	for i := 0; i < v.Len(); i++ {
		buf.WriteString(comma)

		// Access the i-th element in the backingArray.
		arrayVal := driver.Value(v.Index(i).Interface())
		// Format the value recursively into a string.
		vs := formatVal(arrayVal, colType, showPrintableUnicode, showNewLinesAndTabs)

		// If the value contains special characters or a comma, enclose in double quotes.
		// Also escape the special characters.
		if strings.IndexByte(vs, ',') >= 0 || reArrayStringEscape.MatchString(vs) {
			vs = "\"" + reArrayStringEscape.ReplaceAllString(vs, "\\$1") + "\""
		}

		// Add the string for that one value to the output array representation.
		buf.WriteString(vs)
		comma = ","
	}
	buf.WriteByte('}')
	return buf.String()
}

var reArrayStringEscape = regexp.MustCompile(`(["\\])`)

var timeOutputFormats = map[string]string{
	"TIMESTAMP":   timeutil.TimestampWithoutTZFormat,
	"TIMESTAMPTZ": timeutil.TimestampWithTZFormat,
	"TIME":        timeutil.TimeWithoutTZFormat,
	"TIMETZ":      timeutil.TimeWithTZFormat,
	"DATE":        timeutil.DateFormat,
}
