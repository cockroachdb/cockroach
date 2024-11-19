// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This code was derived from https://github.com/youtube/vitess.

package stringencoding

import (
	"bytes"
	"unicode/utf8"
)

// This is its own package so it can be shared among packages that parser
// depends on.

var (
	// DontEscape is a sentinel value for characters that don't need to be escaped.
	DontEscape = byte(255)
	// EncodeMap specifies how to escape binary data with '\'.
	EncodeMap [256]byte
	// HexMap is a mapping from each byte to the `\x%%` hex form as a []byte.
	HexMap [256][]byte
	// RawHexMap is a mapping from each byte to the `%%` hex form as a []byte.
	RawHexMap [256][]byte
)

func init() {
	encodeRef := map[byte]byte{
		'\b': 'b',
		'\f': 'f',
		'\n': 'n',
		'\r': 'r',
		'\t': 't',
		'\\': '\\',
	}

	for i := range EncodeMap {
		EncodeMap[i] = DontEscape
	}
	for i := range EncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			EncodeMap[byte(i)] = to
		}
	}

	// underlyingHexMap contains the string "\x00\x01\x02..." which HexMap and
	// RawHexMap then index into.
	var underlyingHexMap bytes.Buffer
	underlyingHexMap.Grow(1024)

	for i := 0; i < 256; i++ {
		underlyingHexMap.WriteString("\\x")
		writeHexDigit(&underlyingHexMap, i/16)
		writeHexDigit(&underlyingHexMap, i%16)
	}

	underlyingHexBytes := underlyingHexMap.Bytes()

	for i := 0; i < 256; i++ {
		HexMap[i] = underlyingHexBytes[i*4 : i*4+4]
		RawHexMap[i] = underlyingHexBytes[i*4+2 : i*4+4]
	}
}

// EncodeEscapedChar is used internally to write out a character from a larger
// string that needs to be escaped to a buffer.
func EncodeEscapedChar(
	buf *bytes.Buffer,
	entireString string,
	currentRune rune,
	currentByte byte,
	currentIdx int,
	quoteChar byte,
) {
	ln := utf8.RuneLen(currentRune)
	if currentRune == utf8.RuneError {
		// Errors are due to invalid unicode points, so escape the bytes.
		// Make sure this is run at least once in case ln == -1.
		buf.Write(HexMap[entireString[currentIdx]])
		for ri := 1; ri < ln; ri++ {
			if currentIdx+ri < len(entireString) {
				buf.Write(HexMap[entireString[currentIdx+ri]])
			}
		}
	} else if ln == 1 {
		// For single-byte runes, do the same as encodeSQLBytes.
		if encodedChar := EncodeMap[currentByte]; encodedChar != DontEscape {
			buf.WriteByte('\\')
			buf.WriteByte(encodedChar)
		} else if currentByte == quoteChar {
			buf.WriteByte('\\')
			buf.WriteByte(quoteChar)
		} else {
			// Escape non-printable characters.
			buf.Write(HexMap[currentByte])
		}
	} else {
		writeMultibyteRuneAsHex(buf, currentRune, ln)
	}
}

const uppercaseHex = `0123456789ABCDEF`

// writeMultibyteRuneAsHex is equivalent to either
// fmt.FPrintf(`\u%04X`) or fmt.FPrintf(`\U%08X`).
// We can't quite just use strconv since we need uppercase hex.
func writeMultibyteRuneAsHex(buf *bytes.Buffer, r rune, ln int) {
	if ln == 2 {
		buf.WriteString(`\u0000`)
	} else {
		buf.WriteString(`\U00000000`)
	}
	for i := 1; r > 0; r >>= 4 {
		buf.Bytes()[buf.Len()-i] = uppercaseHex[r&0x0f]
		i++
	}

}

func writeHexDigit(buf *bytes.Buffer, v int) {
	if v < 10 {
		buf.WriteByte('0' + byte(v))
	} else {
		buf.WriteByte('a' + byte(v-10))
	}
}

// NeedEscape returns whether the given byte needs to be escaped.
func NeedEscape(ch byte) bool {
	return EncodeMap[ch] != DontEscape
}
