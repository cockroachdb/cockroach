// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowCompletions(n *tree.ShowCompletions) (tree.Statement, error) {
	offsetVal, ok := n.Offset.AsConstantInt()
	if !ok {
		return nil, errors.Newf("invalid offset %v", n.Offset)
	}
	offset, err := strconv.Atoi(offsetVal.String())
	if err != nil {
		return nil, err
	}

	completions, err := RunShowCompletions(n.Statement.RawString(), offset)
	if err != nil {
		return nil, err
	}

	if len(completions) == 0 {
		return parse(`SELECT '' as completions`)
	}

	var query bytes.Buffer
	fmt.Fprint(&query, "SELECT @1 AS completions FROM (VALUES ")

	comma := ""
	for _, completion := range completions {
		fmt.Fprintf(&query, "%s(", comma)
		lexbase.EncodeSQLString(&query, completion)
		query.WriteByte(')')
		comma = ", "
	}

	fmt.Fprintf(&query, ")")

	return parse(query.String())
}

// RunShowCompletions returns a list of completion keywords for the given
// statement and offset.
func RunShowCompletions(stmt string, offset int) ([]string, error) {
	if offset <= 0 || offset > len(stmt) {
		return nil, nil
	}

	// For simplicity, if we're on a whitespace, return no completions.
	// Currently, parser.TokensIgnoreErrors does not consider whitespaces
	// after the last token.
	// Ie "SELECT ", will only return one token being "SELECT".
	// If we're at the whitespace, we do not want to return completion
	// recommendations for "SELECT".
	if unicode.IsSpace([]rune(stmt)[offset-1]) {
		return nil, nil
	}

	sqlTokens := parser.TokensIgnoreErrors(string([]rune(stmt)[:offset]))
	if len(sqlTokens) == 0 {
		return nil, nil
	}

	sqlTokenStrings := make([]string, len(sqlTokens))
	for i, sqlToken := range sqlTokens {
		sqlTokenStrings[i] = sqlToken.Str
	}

	lastWordTruncated := sqlTokenStrings[len(sqlTokenStrings)-1]

	// If the offset is in the middle of a word, we return the full word.
	// For example if the stmt is SELECT with offset 2, even though SEARCH would
	// come first for "SE", we want to return "SELECT".
	// Similarly, if we have SEL with offset 2, we want to return "SEL".
	allSQLTokens := parser.TokensIgnoreErrors(stmt)
	lastWordFull := allSQLTokens[len(sqlTokenStrings)-1]
	if lastWordFull.Str != lastWordTruncated {
		return []string{strings.ToUpper(lastWordFull.Str)}, nil
	}

	return getCompletionsForWord(lastWordTruncated, lexbase.KeywordNames), nil
}

// Binary search for range with matching prefixes
// Return the range of matching prefixes for w.
func binarySearch(w string, words []string) (int, int) {
	// First binary search for the first string in the sorted lexbase.KeywordNames list
	// that matches the prefix w.
	left := sort.Search(len(words), func(i int) bool { return words[i] >= w })

	// Binary search for the last string in the sorted lexbase.KeywordNames list that matches
	// the prefix w.
	right := sort.Search(len(words), func(i int) bool { return words[i][:min(len(words[i]), len(w))] > w })

	return left, right
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func getCompletionsForWord(w string, words []string) []string {
	left, right := binarySearch(strings.ToLower(w), words)
	completions := make([]string, right-left)

	for i, word := range words[left:right] {
		completions[i] = strings.ToUpper(word)
	}
	return completions
}
