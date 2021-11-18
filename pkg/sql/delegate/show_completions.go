package delegate

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
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

	completions, err := runShowCompletions(d.ctx, d.catalog, n.Statement, offset)
	if err != nil {
		return nil, err
	}

	if len(completions) == 0 {
		return parse(`SELECT '' as completions`)
	}

	var query bytes.Buffer
	fmt.Fprintf(
		&query, "SELECT @1 AS %s FROM (VALUES ",
		"completions",
	)

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

// copied from https://github.com/xo/usql/blob/master/drivers/completer/completer.go
// need to add license if this actually goes i
var commonSqlStartCommands = []string{
	"abort",
	"alter",
	"analyze",
	"begin",
	"close",
	"cluster",
	"comment",
	"commit",
	"copy",
	"create",
	"deallocate",
	"declare",
	"desc",
	"discard",
	"do",
	"drop",
	"end",
	"execute",
	"explain",
	"fetch",
	"grant",
	"import",
	"insert",
	"list",
	"prepare",
	"reassign",
	"reindex",
	"release",
	"reset",
	"revoke",
	"rollback",
	"select",
	"set",
	"show",
	"start",
	"table",
	"truncate",
	"update",
	"values",
	"with",
}

func runShowCompletions(
	ctx context.Context, catalog cat.Catalog, stmt string, offset int,
) ([]string, error) {
	if offset <= 0 || offset > len(stmt) {
		return nil, nil
	}

	// For simplicity, if we're on a whitespace, return no completions.
	// Currently, parser.Tokens does not consider whitespaces after the last token.
	// Ie "SELECT ", will only return one token being "SELECT".
	// If we're at the whitespace, we do not want to return completion
	// recommendations for "SELECT".
	if unicode.IsSpace(rune(stmt[offset-1])) {
		return nil, nil
	}

	sqlTokens := parser.TokensIgnoreErrors(stmt[:offset])
	if len(sqlTokens) == 0 {
		return nil, nil
	}

	sqlTokenStrings := make([]string, len(sqlTokens))
	for i, sqlToken := range sqlTokens {
		sqlTokenStrings[i] = sqlToken.Str
	}

	lastWord := sqlTokenStrings[len(sqlTokenStrings)-1]
	wordsBeforeOffset := sqlTokenStrings[:len(sqlTokens)-1]

	// Adhoc heuristics.
	if len(wordsBeforeOffset) > 0 {
		if strings.ToLower(wordsBeforeOffset[len(wordsBeforeOffset)-1]) == "from" {
			tableNames, err := catalog.GetAllTableNames(ctx)
			if err != nil {
				return nil, err
			}
			return getCompletionsForWord(lastWord, tableNames, false /* toUpper */), nil
		}
	} else {
		return getCompletionsForWord(lastWord, commonSqlStartCommands, true /* toUpper */), nil
	}
	return getCompletionsForWord(lastWord, lexbase.KeywordNames, true /* toUpper */), nil
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

func getCompletionsForWord(w string, words []string, toUpper bool) []string {
	left, right := binarySearch(strings.ToLower(w), words)
	completions := make([]string, right-left)

	if toUpper {
		for i, word := range words[left:right] {
			completions[i] = strings.ToUpper(word)
		}
		return completions
	}
	return words[left:right]
}
