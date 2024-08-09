// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"bytes"
	"fmt"
	io "io"
	"math/rand"
	"strings"
	"text/tabwriter"
	"unicode/utf8"
	"unsafe"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// GetSingleRune decodes the string s as a single rune if possible.
func GetSingleRune(s string) (rune, error) {
	if s == "" {
		return 0, nil
	}
	r, sz := utf8.DecodeRuneInString(s)
	if r == utf8.RuneError {
		return 0, errors.Errorf("invalid character: %s", s)
	}
	if sz != len(s) {
		return r, errors.New("must be only one character")
	}
	return r, nil
}

// ToLowerSingleByte returns the lowercase of a given single ASCII byte.
// A non ASCII byte is returned unchanged.
func ToLowerSingleByte(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return 'a' + (b - 'A')
	}
	return b
}

// TruncateString truncates a string to a given number of runes.
func TruncateString(s string, maxRunes int) string {
	// This is a fast path (len(s) is an upper bound for RuneCountInString).
	if len(s) <= maxRunes {
		return s
	}
	n := utf8.RuneCountInString(s)
	if n <= maxRunes {
		return s
	}
	// Fast path for ASCII strings.
	if len(s) == n {
		return s[:maxRunes]
	}
	i := 0
	for pos := range s {
		if i == maxRunes {
			return s[:pos]
		}
		i++
	}
	// This code should be unreachable.
	return s
}

// RemoveTrailingSpaces splits the input string into lines, trims any trailing
// spaces from each line, then puts the lines back together.
//
// Any newlines at the end of the input string are ignored.
//
// The output string always ends in a newline.
func RemoveTrailingSpaces(input string) string {
	lines := strings.TrimRight(input, "\n")
	var buf bytes.Buffer
	for _, line := range strings.Split(lines, "\n") {
		fmt.Fprintf(&buf, "%s\n", strings.TrimRight(line, " "))
	}
	return buf.String()
}

// StringListBuilder helps printing out lists of items. See
// MakeStringListBuilder.
type StringListBuilder struct {
	begin, separator, end string

	// started is true if we had at least one entry (and thus wrote out <begin>).
	started bool
}

// MakeStringListBuilder creates a StringListBuilder, which is used to print out
// lists of items. Sample usage:
//
//	b := MakeStringListBuilder("(", ", ", ")")
//	b.Add(&buf, "x")
//	b.Add(&buf, "y")
//	b.Finish(&buf) // By now, we wrote "(x, y)".
//
// If Add is not called, nothing is written.
func MakeStringListBuilder(begin, separator, end string) StringListBuilder {
	return StringListBuilder{
		begin:     begin,
		separator: separator,
		end:       end,
		started:   false,
	}
}

func (b *StringListBuilder) prepareToAdd(w io.Writer) {
	if b.started {
		_, _ = w.Write([]byte(b.separator))
	} else {
		_, _ = w.Write([]byte(b.begin))
		b.started = true
	}
}

// Add an item to the list.
func (b *StringListBuilder) Add(w io.Writer, val string) {
	b.prepareToAdd(w)
	_, _ = w.Write([]byte(val))
}

// Addf is a format variant of Add.
func (b *StringListBuilder) Addf(w io.Writer, format string, args ...interface{}) {
	b.prepareToAdd(w)
	fmt.Fprintf(w, format, args...)
}

// Finish must be called after all the elements have been added.
func (b *StringListBuilder) Finish(w io.Writer) {
	if b.started {
		_, _ = w.Write([]byte(b.end))
	}
}

// ExpandTabsInRedactableBytes expands tabs in the redactable byte
// slice, so that columns are aligned. The correctness of this
// function depends on the assumption that the `tabwriter` does not
// replace characters.
func ExpandTabsInRedactableBytes(s redact.RedactableBytes) (redact.RedactableBytes, error) {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)
	if _, err := tw.Write([]byte(s)); err != nil {
		return nil, err
	}
	if err := tw.Flush(); err != nil {
		return nil, err
	}
	return redact.RedactableBytes(buf.Bytes()), nil
}

// RandString generates a random string of the desired length from the
// input alphabet.
func RandString(rng *rand.Rand, length int, alphabet string) string {
	buf := make([]byte, length)
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(buf)
}

// CollapseRepeatedRune will collapse repeated, adjacent target runes in the
// input string into a single target rune. If there are no repeated, adjacent
// target runes, the input will be returned, unmodified.
func CollapseRepeatedRune(input string, target rune) string {
	targetSize := utf8.RuneLen(target)

	// First, remove repeated target runes in the prefix and suffix of the
	// string.
	start := strings.IndexFunc(input, func(r rune) bool { return r != target })
	if start > 0 {
		// Remove the prefix.
		input = input[start-targetSize:]
	}
	end := strings.LastIndexFunc(input, func(r rune) bool { return r != target })
	if end >= 0 && end < len(input)-targetSize {
		// Remove the suffix.
		_, endSize := utf8.DecodeRuneInString(input[end:])
		input = input[:end+endSize+targetSize]
	}

	// findRepeatedT finds the next occurrence of repeated, adjacent target
	// runes in s and returns the byte index of the start and the byte index of
	// the character following the repeated target runes. If there are no
	// repeated target runes in s, then ok=false is returned.
	findRepeatedTarget := func(s string) (start int, end int, ok bool) {
		rest := s
		searched := 0
		for ; len(rest) > 0; rest = s[searched:] {
			start = strings.IndexRune(rest, target)
			if start < 0 || start == len(rest)-targetSize {
				// There are no more target runes in the string, or there is a
				// single target rune at the end of the string.
				return -1, -1, false
			}
			end = strings.IndexFunc(rest[start+targetSize:], func(r rune) bool { return r != target })
			switch end {
			case 0:
				// We found a single occurrence of the target rune. Continue the
				// search.
				searched += start + targetSize
				continue
			case -1:
				// There are no more non-target runes, so the suffix of the
				// string must be repeated target runes.
				return start + searched, len(s), true
			default:
				// We found repeated target runes.
				start += searched
				end += start + targetSize
				return start, end, true
			}
		}
		return -1, -1, false
	}

	rest := input
	var buf []byte
	n := 0
	for len(rest) > 0 {
		start, end, ok := findRepeatedTarget(rest)
		if !ok {
			// There are no repeated target runes in the rest of the string.
			if buf == nil {
				// If no repeated target runes were found previously, we can
				// return the input string as-is (with the prefix and suffix
				// possibly sliced off above).
				return input
			}
			// If repeated target runes were found previously, copy the rest of
			// the string to the buffer.
			n += copy(buf[n:], rest)
			break
		}
		if buf == nil {
			// Lazily allocate the buffer.
			buf = make([]byte, len(input))
		}
		// Copy up to "start" and including the first target rune.
		n += copy(buf[n:], rest[:start+targetSize])
		// Skip over the repeated target runes.
		rest = rest[end:]
	}

	buf = buf[:n]
	return *(*string)(unsafe.Pointer(&buf))
}
