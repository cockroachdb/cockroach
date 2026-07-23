// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"strings"
	"unicode"
)

// The soundex code consists of four characters.
const soundexLen = 4

// ABCDEFGHIJKLMNOPQRSTUVWXYZ
const soundexTable = "01230120022455012623010202"

func IsAlpha(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z')
}

func soundexCode(r rune) byte {
	letter := byte(unicode.ToUpper(r))
	if letter >= 'A' && letter <= 'Z' {
		return soundexTable[int(letter-'A')]
	}
	return 0x0
}

func soundex(source string) string {
	// Skip leading non-alphabetic characters
	source = strings.TrimLeftFunc(source, func(r rune) bool {
		return !IsAlpha(r)
	})
	code := make([]byte, soundexLen)
	// No string left
	if len(source) == 0 {
		return string(code)
	}
	runes := []rune(source)
	if unicode.IsUpper(runes[0]) || unicode.IsLower(runes[0]) {
		// Convert the first character to upper case.
		code[0] = byte(unicode.ToUpper(runes[0]))
	}
	j := 1
	for i := 1; i < len(runes) && j < soundexLen; i++ {
		if !IsAlpha(runes[i]) {
			continue
		}
		if soundexCode(runes[i]) != soundexCode(runes[i-1]) {
			c := soundexCode(runes[i])
			if c != '0' {
				code[j] = c
				j++
			}
		}
	}
	// Fill with 0's at the end
	for j < soundexLen {
		code[j] = '0'
		j++
	}
	return string(code)
}

// Soundex convert source to its Soundex code.
func Soundex(source string) string {
	code := soundex(source)
	resCode := make([]byte, 0)
	for _, b := range []byte(code) {
		if b != 0x0 {
			resCode = append(resCode, b)
		}
	}
	return string(resCode)
}

// Difference convert source and target to their Soundex codes
// and then reports the number of matching code positions.
func Difference(source, target string) int {
	sourceCode := soundex(source)
	targetCode := soundex(target)

	diff := 0
	for i := 0; i < soundexLen; i++ {
		if sourceCode[i] == targetCode[i] {
			diff++
		}
	}
	return diff
}
