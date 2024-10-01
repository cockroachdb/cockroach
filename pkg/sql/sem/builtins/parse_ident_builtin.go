// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

// parseIdent is used to implement the parse_ident builtin function. It splits
// the input string into an array of identifiers, treating '.' as the separator.
// Individual identifiers can be quoted. If the `strict` parameter is false,
// then extra characters after the last identifier are ignored. (This is
// useful for parsing function names like "public.my_function()".)
func parseIdent(input string, strict bool) (_ []string, err error) {
	names := make([]string, 0)
	curInput := input
	afterDot := false

	if len(input) == 0 {
		return nil, errors.WithDetail(
			errors.Newf("string is not a valid identifier: %q", input),
			"Input string is empty.",
		)
	}

	for {
		var name string

		// Skip leading whitespace.
		curInput = strings.TrimLeftFunc(curInput, unicode.IsSpace)
		curRune, _ := utf8.DecodeRuneInString(curInput)
		if curRune == '"' {
			// The name is a double-quoted string.
			name, curInput, err = parseQuotedIdent(input, curInput)
			if err != nil {
				return nil, err
			}
		} else if curRune != utf8.RuneError && isIdentStart(curRune) {
			// The name is an unquoted string.
			name, curInput = parseUnquotedIdent(curInput)
		} else {
			// The input is not a valid sequence of names.
			// Different error messages based on where we failed.
			if curRune == '.' {
				return nil, errors.WithDetail(
					errors.Newf("string is not a valid identifier: %q", input),
					"No valid identifier before \".\".",
				)
			} else if afterDot {
				return nil, errors.WithDetail(
					errors.Newf("string is not a valid identifier: %q", input),
					"No valid identifier after \".\".",
				)
			} else {
				return nil, errors.Newf("string is not a valid identifier: %q", input)
			}
		}

		// Add the parsed name to the list of names.
		names = append(names, name)

		// Skip any whitespace after the identifier.
		curInput = strings.TrimLeftFunc(curInput, unicode.IsSpace)

		// Skip the dot separator, if present.
		if len(curInput) > 0 && curInput[0] == '.' {
			curInput = curInput[1:]
			afterDot = true
		} else {
			break
		}
	}

	if strict && len(curInput) > 0 {
		// The input string had any extra characters after the last identifier.
		return nil, errors.WithDetail(
			errors.Newf("string is not a valid identifier: %q", input),
			"Extra characters after last identifier.",
		)
	}

	return names, nil
}

// parseQuotedIdent parses a double-quoted string from the beginning of the
// input string. It returns the parsed string, the remaining input string, and
// any error that occurred.
func parseQuotedIdent(fullInput, curInput string) (string, string, error) {
	// Find the closing double quote.
	i := 1
	for len(curInput[i:]) > 0 {
		curRune, size := utf8.DecodeRuneInString(curInput[i:])
		if curRune == '"' {
			// Check if the double quote is escaped by another double quote.
			if i+size < len(curInput) && curInput[i+size] == '"' {
				// Skip the escaped double quote.
				curInput = curInput[:i] + curInput[i+size:]
			} else {
				// The closing double quote was found.
				break
			}
		}
		i += size
	}

	if i >= len(curInput) {
		return "", "", errors.WithDetail(
			errors.Newf("string is not a valid identifier: %q", fullInput),
			"String has unclosed double quotes.",
		)
	}
	if len(curInput[1:i]) == 0 {
		return "", "", errors.WithDetail(
			errors.Newf("string is not a valid identifier: %q", fullInput),
			"Quoted identifier must not be empty.",
		)
	}

	// Return the parsed string and the remaining input.
	return curInput[1:i], curInput[i+1:], nil
}

// parseUnquotedIdent parses an unquoted identifier from the beginning of the
// input string. It returns the parsed identifier and the remaining input
// string.
func parseUnquotedIdent(input string) (string, string) {
	// Find the end of the identifier.
	i := 1
	for i < len(input) {
		r, size := utf8.DecodeRuneInString(input[i:])
		if !isIdentCont(r) {
			break
		}
		i += size
	}

	// Return the parsed identifier and the remaining input.
	return strings.ToLower(input[:i]), input[i:]
}

// isIdentStart returns true if the given rune is a valid start character for an
// identifier.
func isIdentStart(ch rune) bool {
	// This uses the same criteria as Postgres.
	// See https://github.com/postgres/postgres/blob/3b3182195304777430d16d7967f0adcd8dbfe2ed/src/backend/utils/adt/misc.c#L648
	return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') || (ch == '_') ||
		unicode.IsLetter(ch) || unicode.IsSymbol(ch)
}

// isIdentCont returns true if the given rune is a valid continuation character
// for an identifier.
func isIdentCont(ch rune) bool {
	// This uses the same criteria as Postgres.
	// See https://github.com/postgres/postgres/blob/3b3182195304777430d16d7967f0adcd8dbfe2ed/src/backend/utils/adt/misc.c#L666
	return (ch >= '0' && ch <= '9') || (ch == '$') || isIdentStart(ch)
}
