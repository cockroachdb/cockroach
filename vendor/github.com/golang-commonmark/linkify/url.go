// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package linkify

import "unicode/utf8"

func atoi3(s string, start int) (int, bool) {
	n := 0
	var i int
	for i = start; i < len(s) && digit(s[i]); i++ {
		n = n*10 + int(s[i]-'0')
		if n > 255 {
			return 0, false
		}
	}
	if i == start {
		return 0, false
	}
	return i, true
}

func skipIPv4(s string) (_ int, _ bool) {
	j := 0
	for i := 0; i < 4; i++ {
		if j >= len(s) {
			return
		}
		if i > 0 {
			if s[j] != '.' {
				return
			}
			j++
		}
		n, ok := atoi3(s, j)
		if !ok {
			return
		}
		j = n
	}
	return j, true
}

func atoi5(s string, start int) (int, bool) {
	n := 0
	var i int
	for i = start; i < len(s) && digit(s[i]); i++ {
		n = n*10 + int(s[i]-'0')
		if n > 65535 {
			return 0, false
		}
	}
	if i == start || n == 0 {
		return 0, false
	}
	return i, true
}

func skipPort(s string, start int) int {
	if start >= len(s) || s[start] != ':' {
		return start
	}
	end, ok := atoi5(s, start+1)
	if !ok {
		return start
	}
	return end
}

func skipPath(s string, start int) int {
	if start >= len(s) || s[start] != '/' {
		return start // skip empty path
	}
	var stack []rune
	var notClosedIndex int
	var nHyphen int
	end := start + 1
loop:
	for end < len(s) {
		r, rlen := utf8.DecodeRuneInString(s[end:])
		if r == utf8.RuneError {
			nHyphen = 0
			break
		}

		switch {
		case isUnreserved(r):
			if r == '-' {
				nHyphen++
				if nHyphen > 1 {
					break loop
				}
			} else {
				nHyphen = 0
			}
		case isSubDelimiter(r) || r == '[' || r == ']':
			nHyphen = 0
			switch r {
			case '[', '(':
				if len(stack) == 0 {
					notClosedIndex = end
				}
				stack = append(stack, r)
			case ']', ')':
				opening := '['
				if r == ')' {
					opening = '('
				}
				if len(stack) == 0 || stack[len(stack)-1] != opening {
					break loop
				}
				stack = stack[:len(stack)-1]
			}
		case r == '/' || r == ':' || r == '@':
			nHyphen = 0
		case r == '%':
			nHyphen = 0
			if end+2 >= len(s) {
				break loop
			}
			if !(hexDigit(s[end+1]) &&
				hexDigit(s[end+2])) {
				break loop
			}
			end += 2
		default:
			nHyphen = 0
			if r != ' ' || len(stack) == 0 {
				break loop
			}
		}
		end += rlen
	}
	if len(stack) > 0 {
		return notClosedIndex
	}
	if nHyphen > 0 {
		return end - nHyphen + 1
	}
	return end
}

func skipQuery(s string, start int) int {
	if start >= len(s) || s[start] != '?' {
		return start
	}
	var stack []rune
	var notClosedIndex int
	var nHyphen int
	end := start + 1
loop:
	for end < len(s) {
		r, rlen := utf8.DecodeRuneInString(s[end:])
		if r == utf8.RuneError {
			nHyphen = 0
			break
		}

		switch {
		case isUnreserved(r):
			if r == '-' {
				nHyphen++
				if nHyphen > 1 {
					break loop
				}
			} else {
				nHyphen = 0
			}
		case isSubDelimiter(r) || r == '[' || r == ']':
			nHyphen = 0
			switch r {
			case '[', '(':
				if len(stack) == 0 {
					notClosedIndex = end
				}
				stack = append(stack, r)
			case ']', ')':
				opening := '['
				if r == ')' {
					opening = '('
				}
				if len(stack) == 0 || stack[len(stack)-1] != opening {
					break loop
				}
				stack = stack[:len(stack)-1]
			}
		case r == '?' || r == '/' || r == ':' || r == '@':
			nHyphen = 0
		case r == '%':
			nHyphen = 0
			if end+2 >= len(s) {
				break loop
			}
			if !(hexDigit(s[end+1]) &&
				hexDigit(s[end+2])) {
				break loop
			}
			end += 2
		default:
			nHyphen = 0
			if r != ' ' || len(stack) == 0 {
				break loop
			}
		}
		end += rlen
	}
	if len(stack) > 0 {
		return notClosedIndex
	}
	if nHyphen > 0 {
		return end - nHyphen + 1
	}
	return end
}

func skipFragment(s string, start int) int {
	if start >= len(s) || s[start] != '#' {
		return start
	}
	var stack []rune
	var notClosedIndex int
	var nHyphen int
	end := start + 1
loop:
	for end < len(s) {
		r, rlen := utf8.DecodeRuneInString(s[end:])
		if r == utf8.RuneError {
			nHyphen = 0
			break
		}

		switch {
		case isUnreserved(r):
			if r == '-' {
				nHyphen++
				if nHyphen > 1 {
					break loop
				}
			} else {
				nHyphen = 0
			}
		case isSubDelimiter(r) || r == '[' || r == ']':
			nHyphen = 0
			switch r {
			case '[', '(':
				if len(stack) == 0 {
					notClosedIndex = end
				}
				stack = append(stack, r)
			case ']', ')':
				opening := '['
				if r == ')' {
					opening = '('
				}
				if len(stack) == 0 || stack[len(stack)-1] != opening {
					break loop
				}
				stack = stack[:len(stack)-1]
			}
		case r == '?' || r == '/' || r == ':' || r == '@':
			nHyphen = 0
		case r == '%':
			nHyphen = 0
			if end+2 >= len(s) {
				break loop
			}
			if !(hexDigit(s[end+1]) &&
				hexDigit(s[end+2])) {
				break loop
			}
			end += 2
		default:
			nHyphen = 0
			if r != ' ' || len(stack) == 0 {
				break loop
			}
		}
		end += rlen
	}
	if len(stack) > 0 {
		return notClosedIndex
	}
	if nHyphen > 0 {
		return end - nHyphen + 1
	}
	return end
}

func unskipPunct(s string, start int) int {
	end := start - 1
	if end < 0 || end >= len(s) || !basicPunct[s[end]] {
		return start
	}
	return end
}

func findHostnameStart(s string, start int) (_ int, _ bool) {
	end := start
	lastDot := true
	nHyphen := 0
loop:
	for end > 0 {
		r, rlen := utf8.DecodeLastRuneInString(s[:end])
		if r == utf8.RuneError {
			return
		}

		switch {
		case isLetterOrDigit(r):
			lastDot = false
			nHyphen = 0
		case r == '.':
			if nHyphen > 0 {
				return
			}
			lastDot = true
		case r == '-':
			if end == start {
				return
			}
			if lastDot {
				return
			}
			nHyphen++
			if nHyphen == 3 {
				return
			}
		case r == ':' || r == '/' || r == '\\' || r == '_':
			return
		case isPunctOrSpaceOrControl(r):
			break loop
		default:
			return
		}
		end -= rlen
	}
	if lastDot || nHyphen > 0 {
		return
	}
	return end, true
}

func findHostnameEnd(s string, start int) (_ int, _ int, _ bool) {
	end := start
	lastDot := false
	lastDotPos := -1
	nHyphen := 0
loop:
	for end < len(s) {
		r, rlen := utf8.DecodeRuneInString(s[end:])
		if r == utf8.RuneError {
			return
		}

		switch {
		case isLetterOrDigit(r):
			lastDot = false
			nHyphen = 0
		case r == '.':
			if nHyphen > 0 {
				return
			}
			if lastDot {
				break loop
			}
			lastDot = true
			lastDotPos = end
			nHyphen = 0
		case r == '-':
			lastDot = false
			if end == start {
				return
			}
			if lastDot {
				return
			}
			nHyphen++
			if nHyphen == 3 {
				break loop
			}
		case r == '\\' || r == '_':
			return
		case isPunctOrSpaceOrControl(r):
			break loop
		default:
			return
		}
		end += rlen
	}

	if nHyphen > 0 {
		end -= nHyphen
	} else if lastDot {
		if s[end-1] == '.' {
			end--
		}
		lastDotPos = end - 1
		for lastDotPos >= start && s[lastDotPos] != '.' {
			lastDotPos--
		}
		if lastDotPos < start {
			lastDotPos = -1
		}
	}

	return end, lastDotPos, true
}
