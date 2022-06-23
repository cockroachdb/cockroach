// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package linkify

import (
	"unicode"
	"unicode/utf8"
)

func findEmailStart(s string, start int) (_ int, _ bool) {
	end := start
	allowDot := false
	for end >= 0 {
		b := s[end]
		switch {
		case emailcs[b]:
			allowDot = true
		case b == '.':
			if !allowDot {
				return
			}
			allowDot = false
		default:
			if end == start {
				return
			}
			if s[end+1] == '.' {
				return
			}
			r, _ := utf8.DecodeLastRuneInString(s[:end+1])
			if r == utf8.RuneError {
				return
			}
			if !unicode.IsSpace(r) {
				return
			}
			return end + 1, true
		}
		end--
	}
	if end < start && s[end+1] == '.' {
		return
	}
	return end + 1, true
}

func findEmailEnd(s string, start int) (_ int, _ bool) {
	end := start
	allowDot := false
loop:
	for end < len(s) {
		b := s[end]
		switch {
		case emailcs[b]:
			allowDot = true
		case b == '.':
			if !allowDot {
				return
			}
			allowDot = false
		case b == '@':
			break loop
		default:
			return
		}
		end++
	}
	if end >= len(s)-5 {
		return
	}
	if end > start && s[end-1] == '.' {
		return
	}

	var dot int
	var ok bool
	end, dot, ok = findHostnameEnd(s, end+1)
	if !ok || dot == -1 {
		return
	}

	if dot+5 <= len(s) && s[dot+1:dot+5] == "xn--" {
		return end, true
	}

	if length := match(s[dot+1:]); dot+length+1 != end {
		return
	}

	return end, true
}
