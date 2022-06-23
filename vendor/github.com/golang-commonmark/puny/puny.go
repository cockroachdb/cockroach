// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package puny provides functions for encoding/decoding to/from punycode.
package puny

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	maxInt32      int32 = 2147483647
	base          int32 = 36
	tMin          int32 = 1
	baseMinusTMin       = base - tMin
	tMax          int32 = 26
	skew          int32 = 38
	damp          int32 = 700
	initialBias   int32 = 72
	initialN      int32 = 128
)

var (
	ErrOverflow     = errors.New("overflow: input needs wider integers to process")
	ErrNotBasic     = errors.New("illegal input >= 0x80 (not a basic code point)")
	ErrInvalidInput = errors.New("invalid input")
)

func adapt(delta, numPoints int32, firstTime bool) int32 {
	if firstTime {
		delta /= damp
	} else {
		delta /= 2
	}
	delta += delta / numPoints
	k := int32(0)
	for delta > baseMinusTMin*tMax/2 {
		delta = delta / baseMinusTMin
		k += base
	}
	return k + (baseMinusTMin+1)*delta/(delta+skew)
}

func basicToDigit(b byte) int32 {
	switch {
	case b >= '0' && b <= '9':
		return int32(b - 22)
	case b >= 'A' && b <= 'Z':
		return int32(b - 'A')
	case b >= 'a' && b <= 'z':
		return int32(b - 'a')
	}
	return base
}

func digitToBasic(digit int32) byte {
	switch {
	case digit >= 0 && digit <= 25:
		return byte(digit) + 'a'
	case digit >= 26 && digit <= 35:
		return byte(digit) - 26 + '0'
	}
	panic("unreachable")
}

func lastIndex(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func ascii(s string) bool {
	for _, r := range s {
		if r > 0x7e {
			return false
		}
	}
	return true
}

// Decode converts a Punycode string of ASCII-only symbols to a string of Unicode symbols.
func Decode(s string) (string, error) {
	basic := lastIndex(s, '-')
	output := make([]rune, 0, len(s))
	for i := 0; i < basic; i++ {
		b := s[i]
		if b >= 0x80 {
			return "", ErrNotBasic
		}
		output = append(output, rune(b))
	}

	i, n, bias, pos := int32(0), initialN, initialBias, basic+1

	for pos < len(s) {
		oldi, w, k := i, int32(1), base
		for {
			digit := basicToDigit(s[pos])
			pos++

			if digit >= base || digit > (maxInt32-i)/w {
				return "", ErrOverflow
			}

			i += digit * w

			t := k - bias
			if t < tMin {
				t = tMin
			} else if t > tMax {
				t = tMax
			}

			if digit < t {
				break
			}

			if pos == len(s) {
				return "", ErrInvalidInput
			}

			baseMinusT := base - t
			if w > maxInt32/baseMinusT {
				return "", ErrOverflow
			}

			w *= baseMinusT
			k += base
		}

		out := int32(len(output) + 1)
		bias = adapt(i-oldi, out, oldi == 0)

		if i/out > maxInt32-n {
			return "", ErrOverflow
		}

		n += i / out
		i %= out

		output = append(output, 0)
		copy(output[i+1:], output[i:])
		output[i] = rune(n)

		i++
	}

	return string(output), nil
}

// Encode converts a string of Unicode symbols (e.g. a domain name label) to a
// Punycode string of ASCII-only symbols.
func Encode(input string) (string, error) {
	n := initialN
	delta := int32(0)
	bias := initialBias

	var output []byte
	runes := 0
	for _, r := range input {
		if r >= 0x80 {
			runes++
			continue
		}
		output = append(output, byte(r))
	}

	basicLength := len(output)
	handledCPCount := basicLength

	if basicLength > 0 {
		output = append(output, '-')
	}

	for runes > 0 {
		m := maxInt32
		for _, r := range input {
			if r >= n && r < m {
				m = r
			}
		}

		handledCPCountPlusOne := int32(handledCPCount + 1)
		if m-n > (maxInt32-delta)/handledCPCountPlusOne {
			return "", ErrOverflow
		}

		delta += (m - n) * handledCPCountPlusOne
		n = m

		for _, r := range input {
			if r < n {
				delta++
				if delta > maxInt32 {
					return "", ErrOverflow
				}
				continue
			}
			if r > n {
				continue
			}
			q := delta
			for k := base; ; k += base {
				t := k - bias
				if t < tMin {
					t = tMin
				} else if t > tMax {
					t = tMax
				}
				if q < t {
					break
				}
				qMinusT := q - t
				baseMinusT := base - t
				output = append(output, digitToBasic(t+qMinusT%baseMinusT))
				q = qMinusT / baseMinusT
			}

			output = append(output, digitToBasic(q))
			bias = adapt(delta, handledCPCountPlusOne, handledCPCount == basicLength)
			delta = 0
			handledCPCount++
			runes--
		}
		delta++
		n++
	}
	return string(output), nil
}

func sep(r rune) bool { return r == '.' || r == '。' || r == '．' || r == '｡' }

func mapLabels(s string, fn func(string) string) string {
	var result string
	i := strings.IndexByte(s, '@')
	if i != -1 {
		result = s[:i+1]
		s = s[i+1:]
	}
	var labels []string
	start := 0
	for i, r := range s {
		if !sep(r) {
			continue
		}
		labels = append(labels, fn(s[start:i]))
		start = i + utf8.RuneLen(r)
	}
	labels = append(labels, fn(s[start:]))
	return result + strings.Join(labels, ".")
}

// ToUnicode converts a Punycode string representing a domain name or an email address
// to Unicode. Only the Punycoded parts of the input will be converted.
func ToUnicode(s string) string {
	return mapLabels(s, func(s string) string {
		if !strings.HasPrefix(s, "xn--") {
			return s
		}
		d, err := Decode(strings.ToLower(s[4:]))
		if err != nil {
			return s
		}
		return d
	})
}

// ToASCII converts a Unicode string representing a domain name or an email address to
// Punycode. Only the non-ASCII parts of the domain name will be converted.
func ToASCII(s string) string {
	return mapLabels(s, func(s string) string {
		if ascii(s) {
			return s
		}
		d, err := Encode(s)
		if err != nil {
			return s
		}
		return "xn--" + d
	})
}
