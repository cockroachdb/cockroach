// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This code was derived from https://github.com/youtube/vitess.

package lex

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/language"
)

// NormalizeLocaleName returns a normalized locale identifier based on s. The
// case of the locale is normalized and any dash characters are mapped to
// underscore characters.
func NormalizeLocaleName(s string) string {
	b := bytes.NewBuffer(make([]byte, 0, len(s)))
	EncodeLocaleName(b, s)
	return b.String()
}

// EncodeLocaleName writes the locale identifier in s to buf. Any dash
// characters are mapped to underscore characters. Underscore characters do not
// need to be quoted, and they are considered equivalent to dash characters by
// the CLDR standard: http://cldr.unicode.org/.
func EncodeLocaleName(buf *bytes.Buffer, s string) {
	// If possible, try to normalize the case of the locale name.
	if normalized, err := language.Parse(s); err == nil {
		s = normalized.String()
	}
	for i, n := 0, len(s); i < n; i++ {
		ch := s[i]
		if ch == '-' {
			buf.WriteByte('_')
		} else {
			buf.WriteByte(ch)
		}
	}
}

// LocaleNamesAreEqual checks for equality of two locale names. The comparison
// is case-insensitive and treats '-' and '_' as the same.
func LocaleNamesAreEqual(a, b string) bool {
	if a == b {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i, n := 0, len(a); i < n; i++ {
		ai, bi := a[i], b[i]
		if ai == bi {
			continue
		}
		if ai == '-' && bi == '_' {
			continue
		}
		if ai == '_' && bi == '-' {
			continue
		}
		if unicode.ToLower(rune(ai)) != unicode.ToLower(rune(bi)) {
			return false
		}
	}
	return true
}

// EncodeByteArrayToRawBytes converts a SQL-level byte array into raw
// bytes according to the encoding specification in "be".
// If the skipHexPrefix argument is set, the hexadecimal encoding does not
// prefix the output with "\x". This is suitable e.g. for the encode()
// built-in.
func EncodeByteArrayToRawBytes(data string, be BytesEncodeFormat, skipHexPrefix bool) string {
	switch be {
	case BytesEncodeHex:
		head := 2
		if skipHexPrefix {
			head = 0
		}
		res := make([]byte, head+hex.EncodedLen(len(data)))
		if !skipHexPrefix {
			res[0] = '\\'
			res[1] = 'x'
		}
		hex.Encode(res[head:], []byte(data))
		return string(res)

	case BytesEncodeEscape:
		// PostgreSQL does not allow all the escapes formats recognized by
		// CockroachDB's scanner. It only recognizes octal and \\ for the
		// backslash itself.
		// See https://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
		res := make([]byte, 0, len(data))
		for _, c := range []byte(data) {
			if c == '\\' {
				res = append(res, '\\', '\\')
			} else if c < 32 || c >= 127 {
				// Escape the character in octal.
				//
				// Note: CockroachDB only supports UTF-8 for which all values
				// below 128 are ASCII. There is no locale-dependent escaping
				// in that case.
				res = append(res, '\\', '0'+(c>>6), '0'+((c>>3)&7), '0'+(c&7))
			} else {
				res = append(res, c)
			}
		}
		return string(res)

	case BytesEncodeBase64:
		return base64.StdEncoding.EncodeToString([]byte(data))

	default:
		panic(errors.AssertionFailedf("unhandled format: %s", be))
	}
}

// DecodeRawBytesToByteArray converts raw bytes to a SQL-level byte array
// according to the encoding specification in "be".
// When using the Hex format, the caller is responsible for skipping the
// "\x" prefix, if any. See DecodeRawBytesToByteArrayAuto() below for
// an alternative.
func DecodeRawBytesToByteArray(data string, be BytesEncodeFormat) ([]byte, error) {
	switch be {
	case BytesEncodeHex:
		return hex.DecodeString(data)

	case BytesEncodeEscape:
		// PostgreSQL does not allow all the escapes formats recognized by
		// CockroachDB's scanner. It only recognizes octal and \\ for the
		// backslash itself.
		// See https://www.postgresql.org/docs/current/static/datatype-binary.html#AEN5667
		res := make([]byte, 0, len(data))
		for i := 0; i < len(data); i++ {
			ch := data[i]
			if ch != '\\' {
				res = append(res, ch)
				continue
			}
			if i >= len(data)-1 {
				return nil, pgerror.New(pgcode.InvalidEscapeSequence,
					"bytea encoded value ends with escape character")
			}
			if data[i+1] == '\\' {
				res = append(res, '\\')
				i++
				continue
			}
			if i+3 >= len(data) {
				return nil, pgerror.New(pgcode.InvalidEscapeSequence,
					"bytea encoded value ends with incomplete escape sequence")
			}
			b := byte(0)
			for j := 1; j <= 3; j++ {
				octDigit := data[i+j]
				if octDigit < '0' || octDigit > '7' || (j == 1 && octDigit > '3') {
					return nil, pgerror.New(pgcode.InvalidEscapeSequence,
						"invalid bytea escape sequence")
				}
				b = (b << 3) | (octDigit - '0')
			}
			res = append(res, b)
			i += 3
		}
		return res, nil

	case BytesEncodeBase64:
		return base64.StdEncoding.DecodeString(data)

	default:
		return nil, errors.AssertionFailedf("unhandled format: %s", be)
	}
}

// DecodeRawBytesToByteArrayAuto detects which format to use with
// DecodeRawBytesToByteArray(). It only supports hex ("\x" prefix)
// and escape.
func DecodeRawBytesToByteArrayAuto(data []byte) ([]byte, error) {
	if len(data) >= 2 && data[0] == '\\' && (data[1] == 'x' || data[1] == 'X') {
		return DecodeRawBytesToByteArray(string(data[2:]), BytesEncodeHex)
	}
	return DecodeRawBytesToByteArray(string(data), BytesEncodeEscape)
}

func (f BytesEncodeFormat) String() string {
	switch f {
	case BytesEncodeHex:
		return "hex"
	case BytesEncodeEscape:
		return "escape"
	case BytesEncodeBase64:
		return "base64"
	default:
		return fmt.Sprintf("invalid (%d)", f)
	}
}

// BytesEncodeFormatFromString converts a string into a BytesEncodeFormat.
func BytesEncodeFormatFromString(val string) (_ BytesEncodeFormat, ok bool) {
	switch strings.ToUpper(val) {
	case "HEX":
		return BytesEncodeHex, true
	case "ESCAPE":
		return BytesEncodeEscape, true
	case "BASE64":
		return BytesEncodeBase64, true
	default:
		return -1, false
	}
}
