// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hba

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// This file contains a scanner for the pg_hba.conf token syntax.
//
// The algorithm used here is as follows: first the input is split
// into lines. Then each line is scanned using a rule-based algorithm.
//

// rule represents one scanning rule.
type rule struct {
	// re is the regular expression to match at the current text position.
	re string
	// fn is the action function to call if the rule matches.
	// - if foundToken is found true, the lexer stops scanning and returns the current token.
	// - err stops the scan and returns an error.
	fn func(l *lex) (foundToken bool, err error)
}

// lex represents the state of the scanner.
// This is not meant to be used in parsing rules.
type lex struct {
	String

	// comma is set to true if the last found token was succeeded by a
	// comma.
	comma bool

	// lexed is set to the portion of the text matched by the current
	// rule, and is provided as input to the rule's action function.
	lexed string
}

// rules describes the scanning rules.
//
// As per pg's source, file src/backend/libpq/hba.c:
//   Tokens are strings of non-blank
//   characters bounded by blank characters, commas, beginning of line, and
//   end of line. Blank means space or tab. Tokens can be delimited by
//   double quotes (this allows the inclusion of blanks, but not newlines).
//
// The scanner implemented here is slightly more strict than the one
// used by PostgreSQL. For example, PostgreSQL supports tokens written
// as: abc"def"geh to represent the single string "abcdefgeh". The
// same input here will yield 3 different tokens "abc", "def"(quoted),
// "geh".
//
// PostgreSQL also accepts including special (control) characters
// inside quoted and unquoted strings, including tabs (\t) and
// carriage returns (\r) inside quoted strings. These are not accepted
// here for the sake of simplicity in the pretty-printer. If a use
// case comes up where they should be accepted, care should be taken
// to implement a new pretty-printer that does not rewrite whitespace
// in HBA strings.
//
// This difference is intended; it makes the implementation simpler
// and the result less surprising.
//
// Meanwhile, the scanner does implements some other oddities of
// PostgreSQL. For example:
//    a, b       (space after comma) counts as a single comma-delimited field.
//    a ,b       (space before comma) counts as two fields.
//
var rules = []struct {
	r  rule
	rg *regexp.Regexp
}{
	{r: rule{`[ \t\r,]*` /***********/, func(l *lex) (bool, error) { return false, nil }}},
	{r: rule{`#.*$` /****************/, func(l *lex) (bool, error) { return false, nil }}},
	{r: rule{`[^[:cntrl:] ",]+,?` /**/, func(l *lex) (bool, error) { l.checkComma(); l.Value = l.lexed; return true, nil }}},
	{r: rule{`"[^[:cntrl:]"]*",?` /**/, func(l *lex) (bool, error) { l.checkComma(); l.stripQuotes(); l.Value = l.lexed; return true, nil }}},
	{r: rule{`"[^"]*$` /*************/, func(l *lex) (bool, error) { return false, errors.New("unterminated quoted string") }}},
	{r: rule{`"[^"]*"` /*************/, func(l *lex) (bool, error) { return false, errors.New("invalid characters in quoted string") }}},
	{r: rule{`.` /*******************/, func(l *lex) (bool, error) { return false, errors.Newf("unsupported character: %q", l.lexed) }}},
}

func (l *lex) checkComma() {
	l.comma = l.lexed[len(l.lexed)-1] == ','
	if l.comma {
		l.lexed = l.lexed[:len(l.lexed)-1]
	}
}

func (l *lex) stripQuotes() {
	l.Quoted = true
	l.lexed = l.lexed[1 : len(l.lexed)-1]
}

func init() {
	for i := range rules {
		rules[i].rg = regexp.MustCompile("^" + rules[i].r.re)
	}
}

// nextToken reads the next token from buf. A token is a simple or
// quoted string. If there is no token (e.g. just whitespace), the
// returned token is empty. trailingComma indicates whether the token
// is immediately followed by a comma.
//
// Inspired from pg's src/backend/libpq/hba.c, next_token().
func nextToken(buf string) (remaining string, tok String, trailingComma bool, err error) {
	remaining = buf
	var l lex
outer:
	for remaining != "" {
		l = lex{}
	inner:
		for _, rule := range rules {
			l.lexed = rule.rg.FindString(remaining)
			remaining = remaining[len(l.lexed):]
			if l.lexed != "" {
				var foundToken bool
				foundToken, err = rule.r.fn(&l)
				if foundToken || err != nil {
					break outer
				}
				break inner
			}
		}
	}
	return remaining, l.String, l.comma, err
}

// nextFieldExpand reads the next comma-separated list of string from buf.
// commas count as separator only when they immediately follow a string.
//
// Inspired from pg's src/backend/libpq/hba.c, next_field_expand().
func nextFieldExpand(buf string) (remaining string, field []String, err error) {
	remaining = buf
	for {
		var trailingComma bool
		var tok String
		remaining, tok, trailingComma, err = nextToken(remaining)
		if tok.Empty() || err != nil {
			return
		}
		field = append(field, tok)
		if !trailingComma {
			break
		}
	}
	return
}

// tokenize splits the input into tokens.
//
// Inspired from pg's src/backend/libpq/hba.c, tokenize_file().
func tokenize(input string) (res scannedInput, err error) {
	inputLines := strings.Split(input, "\n")

	for lineIdx, lineS := range inputLines {
		var currentLine hbaLine
		currentLine.input = strings.TrimSpace(lineS)
		for remaining := lineS; remaining != ""; {
			var currentField []String
			remaining, currentField, err = nextFieldExpand(remaining)
			if err != nil {
				return res, errors.Wrapf(err, "line %d", lineIdx+1)
			}
			if len(currentField) > 0 {
				currentLine.tokens = append(currentLine.tokens, currentField)
			}
		}
		if len(currentLine.tokens) > 0 {
			res.lines = append(res.lines, currentLine)
			res.linenos = append(res.linenos, lineIdx+1)
		}
	}
	return res, err
}
