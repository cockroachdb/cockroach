// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import "slices"

// FuzzyTrie is a trie data structure optimized for fuzzy string matching using
// Levenshtein distance. It allows efficient searching for all words in the trie
// that are within a given edit distance of a query string.
//
// The key advantage over computing Levenshtein distance for each word individually
// is that the trie can prune entire subtrees when the minimum possible distance
// exceeds the threshold, significantly reducing the number of comparisons needed.
type FuzzyTrie struct {
	root *trieNode
}

// trieNode represents a node in the trie.
type trieNode struct {
	// word is non-nil if this node represents the end of a complete word
	word *string
	// children maps runes to child nodes
	children map[rune]*trieNode
}

// NewFuzzyTrie creates a new empty FuzzyTrie.
func NewFuzzyTrie() *FuzzyTrie {
	return &FuzzyTrie{
		root: &trieNode{
			children: make(map[rune]*trieNode),
		},
	}
}

// Insert adds a word to the trie. Words are stored as-is and matching is
// case-sensitive.
func (t *FuzzyTrie) Insert(word string) {
	node := t.root
	// Convert to runes to handle multi-byte characters correctly
	for _, letter := range word {
		if _, ok := node.children[letter]; !ok {
			node.children[letter] = &trieNode{
				children: make(map[rune]*trieNode),
			}
		}
		node = node.children[letter]
	}
	// Store the original word at the terminal node
	node.word = &word
}

// Search finds all words in the trie within the given maximum Levenshtein
// distance of the query string. The search is case-sensitive based on how
// words were inserted. Uses standard Levenshtein distance with uniform costs
// (insertion=1, deletion=1, substitution=1).
//
// The algorithm uses dynamic programming to compute Levenshtein distance
// incrementally as it traverses the trie, pruning branches where the minimum
// possible distance exceeds maxDist.
//
// Time complexity: O(n*m*k) in worst case where n is query length, m is average
// word length, and k is number of nodes explored. In practice, pruning makes
// this much faster than computing distance for all words.
//
// Space complexity: O(n) for the current row of the DP matrix plus recursion stack.
func (t *FuzzyTrie) Search(query string, maxDist int) []string {
	return t.SearchWithCost(query, maxDist, 1, 1, 1)
}

// SearchWithCost finds all words in the trie within the given maximum distance
// of the query string, using custom costs for edit operations.
//
// Parameters:
//   - query: the string to search for
//   - maxDist: maximum edit distance threshold
//   - insCost: cost of inserting a character
//   - delCost: cost of deleting a character
//   - subCost: cost of substituting a character
func (t *FuzzyTrie) SearchWithCost(query string, maxDist, insCost, delCost, subCost int) []string {
	// Initialize the first row of the Levenshtein DP matrix.
	// currentRow[i] represents the cost of transforming an empty string
	// into the first i characters of the query.
	queryRunes := []rune(query)
	queryLen := len(queryRunes)
	currentRow := make([]int, queryLen+1)
	for i := range currentRow {
		currentRow[i] = i * insCost
	}

	results := []string{}

	// Start recursive search from root's children
	for letter, child := range t.root.children {
		t.searchRecursive(child, letter, queryRunes, currentRow, &results, maxDist, insCost, delCost, subCost)
	}

	return results
}

// searchRecursive performs a depth-first search through the trie, computing
// Levenshtein distance incrementally and pruning branches that exceed maxDist.
func (t *FuzzyTrie) searchRecursive(
	node *trieNode,
	letter rune,
	queryRunes []rune,
	prevRow []int,
	results *[]string,
	maxDist, insCost, delCost, subCost int,
) {
	columns := len(queryRunes) + 1
	// currentRow[i] will represent the edit distance from the current path
	// in the trie to the first i characters of the query.
	currentRow := make([]int, columns)

	// First column: cost of transforming the current trie path to empty string
	// is the length of the path (number of deletions needed)
	currentRow[0] = prevRow[0] + delCost

	// Fill in the rest of the row using standard Levenshtein recurrence
	for col := 1; col < columns; col++ {
		// Cost of inserting query[col-1]
		insertCost := currentRow[col-1] + insCost
		// Cost of deleting current trie character
		deleteCost := prevRow[col] + delCost
		// Cost of replacing (0 if characters match)
		replaceCost := prevRow[col-1]
		if queryRunes[col-1] != letter {
			replaceCost += subCost
		}

		currentRow[col] = min(insertCost, deleteCost, replaceCost)
	}

	// If we've reached a complete word and its distance is within threshold,
	// add it to results. currentRow[len(queryRunes)] is the edit distance
	// from the current trie path to the full query string.
	if currentRow[len(currentRow)-1] <= maxDist && node.word != nil {
		*results = append(*results, *node.word)
	}

	// Prune: only continue searching if minimum distance in current row
	// is within threshold. If all distances exceed maxDist, no extension
	// of this path can possibly be within maxDist of the query.
	if slices.Min(currentRow) <= maxDist {
		for childLetter, child := range node.children {
			t.searchRecursive(child, childLetter, queryRunes, currentRow, results, maxDist, insCost, delCost, subCost)
		}
	}
}

// Size returns the number of words stored in the trie.
func (t *FuzzyTrie) Size() int {
	return t.countWords(t.root)
}

func (t *FuzzyTrie) countWords(node *trieNode) int {
	count := 0
	if node.word != nil {
		count = 1
	}
	for _, child := range node.children {
		count += t.countWords(child)
	}
	return count
}
