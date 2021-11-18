package parser

import (
	"sort"
	"strings"
)

var sqlTokens = sqlToknames[:]

func init() {
	sort.Strings(sqlTokens)
}

func RunShowCompletions(stmt string, offset int, tableNames []string) []string {
	if offset <= 0 || offset > len(stmt) {
		return nil
	}
	lastWord := getLastWordFromStmt(stmt[:offset])

	if len(lastWord) == 0 {
		return nil
	}

	// If lastWord is nil do we default to SELECT?
	wordsBeforeOffset := getPreviousWords(stmt, offset)

	// Adhoc heuristics.
	if len(wordsBeforeOffset) > 0 {
		if strings.ToUpper(wordsBeforeOffset[len(wordsBeforeOffset)-1]) == "FROM" {
			left, right := binarySearch(lastWord, tableNames)

			return tableNames[left:right]
		}
	}
	completions := getCompletionsForWord(lastWord)

	return completions
}

// Binary search for range with matching prefixes
// Return the range of matching prefixes for w.
func binarySearch(w string, words []string) (int, int) {
	// First binary search for the first string in the sorted sqlTokens list
	// that matches the prefix w.
	left := sort.Search(len(words), func(i int) bool { return words[i] >= w })

	// Binary search for the last string in the sorted sqlTokens list that matches
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

func getCompletionsForWord(w string) []string {
	// Binary search and find matching prefixes.
	left, right := binarySearch(strings.ToUpper(w), sqlTokens)
	return sqlTokens[left:right]
}

// getLastWordFromStmt takes a SQL statement and extracts the last "word" from it.
// For example:
// * getLastWordFromStmt("SELECT * FRO") -> "FRO"
// * getLastWordFromStmt("SELECT * FROM movr.rid") -> "rid"
// * getLastWordFromStmt("SELECT * FROM t@prim") -> "prim"
func getLastWordFromStmt(stmt string) string {
	words := strings.Split(stmt, " ")
	lastWord := words[len(words)-1]
	for i := len(lastWord) - 1; i >= 0; i-- {
		switch lastWord[i] {
		case '[', ']', '@', '(', ')', '.':
			return lastWord[i+1:]
		}
	}
	return lastWord
}

func getPreviousWords(stmt string, offset int) []string {
	words := strings.Split(stmt[:offset], " ")
	return words[:len(words)-1]
}
