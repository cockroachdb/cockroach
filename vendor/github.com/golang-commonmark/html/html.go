// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package html provides functions for escaping HTML and for parsing and replacing HTML entities.
package html

import (
	"io"
	"strconv"
	"strings"
	"unicode/utf8"
)

const BadEntity = string(utf8.RuneError)

var htmlEscapeReplacer = strings.NewReplacer(
	"&", "&amp;",
	"<", "&lt;",
	">", "&gt;",
	`"`, "&quot;",
)

func EscapeString(s string) string {
	return htmlEscapeReplacer.Replace(s)
}

func WriteEscapedString(w io.Writer, s string) error {
	_, err := htmlEscapeReplacer.WriteString(w, s)
	return err
}

func isValidEntityCode(c int64) bool {
	switch {
	case !utf8.ValidRune(rune(c)):
		return false

	// never used
	case c >= 0xfdd0 && c <= 0xfdef:
		return false
	case c&0xffff == 0xffff || c&0xffff == 0xfffe:
		return false
	// control codes
	case c >= 0x00 && c <= 0x08:
		return false
	case c == 0x0b:
		return false
	case c >= 0x0e && c <= 0x1f:
		return false
	case c >= 0x7f && c <= 0x9f:
		return false
	}

	return true
}

func letter(b byte) bool { return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }

func digit(b byte) bool { return b >= '0' && b <= '9' }

func alphanum(b byte) bool { return letter(b) || digit(b) }

func hexDigit(b byte) bool {
	return digit(b) || b >= 'a' && b <= 'f' || b >= 'A' && b <= 'F'
}

func ParseEntity(s string) (string, int) {
	st := 0
	var n int

	for i := 1; i < len(s); i++ {
		b := s[i]

		switch st {
		case 0: // initial state
			switch {
			case b == '#':
				st = 1
			case letter(b):
				n = 1
				st = 2
			default:
				return "", 0
			}

		case 1: // &#
			switch {
			case b == 'x' || b == 'X':
				st = 3
			case digit(b):
				n = 1
				st = 4
			default:
				return "", 0
			}

		case 2: // &q
			switch {
			case alphanum(b):
				n++
				if n > 31 {
					return "", 0
				}
			case b == ';':
				if e, ok := entities[s[i-n:i]]; ok {
					return e, i + 1
				}
				return "", 0
			default:
				return "", 0
			}

		case 3: // &#x
			switch {
			case hexDigit(b):
				n = 1
				st = 5
			default:
				return "", 0
			}

		case 4: // &#0
			switch {
			case digit(b):
				n++
				if n > 8 {
					return "", 0
				}
			case b == ';':
				c, _ := strconv.ParseInt(s[i-n:i], 10, 32)
				if !isValidEntityCode(c) {
					return BadEntity, i + 1
				}
				return string(rune(c)), i + 1
			default:
				return "", 0
			}

		case 5: // &#x0
			switch {
			case hexDigit(b):
				n++
				if n > 8 {
					return "", 0
				}
			case b == ';':
				c, err := strconv.ParseInt(s[i-n:i], 16, 32)
				if err != nil {
					return BadEntity, i + 1
				}
				if !isValidEntityCode(c) {
					return BadEntity, i + 1
				}
				return string(rune(c)), i + 1
			default:
				return "", 0
			}
		}
	}

	return "", 0
}

func ReplaceEntities(s string) string {
	i := strings.IndexByte(s, '&')
	if i < 0 {
		return s
	}

	anyChanges := false
	var entityStr string
	var entityLen int
	for i < len(s) {
		if s[i] == '&' {
			entityStr, entityLen = ParseEntity(s[i:])
			if entityLen > 0 {
				anyChanges = true
				break
			}
		}
		i++
	}

	if !anyChanges {
		return s
	}

	buf := make([]byte, len(s)-entityLen+len(entityStr))
	copy(buf[:i], s)
	n := copy(buf[i:], entityStr)
	j := i + n
	i += entityLen
	for i < len(s) {
		b := s[i]
		if b == '&' {
			entityStr, entityLen = ParseEntity(s[i:])
			if entityLen > 0 {
				n = copy(buf[j:], entityStr)
				j += n
				i += entityLen
				continue
			}
		}

		buf[j] = b
		j++
		i++
	}

	return string(buf[:j])
}
