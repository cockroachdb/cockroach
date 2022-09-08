// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// completionsGenerator is the common interface between the
// completionsNode, used for SHOW COMPLETIONS invoked as statement
// source; and the observer statement logic in connExecutor.
type completionsGenerator interface {
	Next(ctx context.Context) (bool, error)
	Values() tree.Datums
	Close(ctx context.Context)
}

// completionsNode is a shim planNode around a completionsGenerator.
// The "main" logic is in completionsGenerator, which is also
// used without a planNode by the connExecutor when running SHOW
// COMPLETIONS as an observer statement.
type completionsNode struct {
	optColumnsSlot

	n       *tree.ShowCompletions
	results completionsGenerator
}

func (n *completionsNode) startExec(params runParams) (err error) {
	n.results, err = newCompletionsGenerator(n.n)
	return err
}

func (n *completionsNode) Next(params runParams) (bool, error) {
	return n.results.Next(params.ctx)
}

func (n *completionsNode) Values() tree.Datums {
	return n.results.Values()
}

func (n *completionsNode) Close(ctx context.Context) {
	if n.results == nil {
		return
	}
	n.results.Close(ctx)
}

// completions is the actual implementation of the completion logic.
type completions struct {
	results []string
	cur     int
}

var _ completionsGenerator = (*completions)(nil)

func (n *completions) Next(ctx context.Context) (bool, error) {
	if n.cur+1 >= len(n.results) {
		return false, nil
	}
	n.cur++
	return true, nil
}

func (n *completions) Values() tree.Datums {
	return tree.Datums{tree.NewDString(n.results[n.cur]), tree.DNull, tree.DNull, tree.DNull, tree.DNull}
}

func (n *completions) Close(ctx context.Context) {}

// newCompletionsGenerator creates a generator.
func newCompletionsGenerator(sc *tree.ShowCompletions) (completionsGenerator, error) {
	n := &completions{}
	n.cur = -1
	offsetVal, ok := sc.Offset.AsConstantInt()
	if !ok {
		return nil, errors.Newf("invalid offset %v", sc.Offset)
	}
	offset, err := strconv.Atoi(offsetVal.String())
	if err != nil {
		return nil, err
	}
	n.results, err = runShowCompletions(sc.Statement.RawString(), int(offset))
	return n, err
}

// runShowCompletions returns a list of completion keywords for the given
// statement and offset.
func runShowCompletions(stmt string, offset int) ([]string, error) {
	byteStmt := []byte(stmt)
	if offset <= 0 || offset > len(byteStmt) {
		return nil, nil
	}

	// For simplicity, if we're on a whitespace, return no completions.
	// Currently, parser.TokensIgnoreErrors does not consider whitespaces
	// after the last token.
	// Ie "SELECT ", will only return one token being "SELECT".
	// If we're at the whitespace, we do not want to return completion
	// recommendations for "SELECT".
	if unicode.IsSpace(rune(byteStmt[offset-1])) {
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
