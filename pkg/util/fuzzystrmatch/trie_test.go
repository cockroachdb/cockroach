// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFuzzyTrie_Insert(t *testing.T) {
	trie := NewFuzzyTrie()
	require.Equal(t, 0, trie.Size())

	trie.Insert("hello")
	require.Equal(t, 1, trie.Size())

	trie.Insert("world")
	require.Equal(t, 2, trie.Size())

	// Inserting same word again should not increase size
	// (currently it will, but documenting current behavior)
	trie.Insert("hello")
	require.Equal(t, 2, trie.Size())
}

func TestFuzzyTrie_SearchExactMatch(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("hello")
	trie.Insert("world")

	results := trie.Search("hello", 0)
	require.Len(t, results, 1)
	require.Equal(t, "hello", results[0])

	results = trie.Search("world", 0)
	require.Len(t, results, 1)
	require.Equal(t, "world", results[0])

	// No match
	results = trie.Search("foo", 0)
	require.Len(t, results, 0)
}

func TestFuzzyTrie_SearchDistance1(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("cat")
	trie.Insert("cats")
	trie.Insert("bat")
	trie.Insert("hat")
	trie.Insert("car")
	trie.Insert("cart")
	trie.Insert("category")

	results := trie.Search("cat", 1)

	// Should find: cat (exact), cats (1 insertion), bat (1 substitution),
	// hat (1 substitution), car (1 substitution), cart (1 insertion)
	// Should NOT find: category (5 insertions from cat)

	require.Contains(t, results, "cat")
	require.Contains(t, results, "cats")
	require.Contains(t, results, "bat")
	require.Contains(t, results, "hat")
	require.Contains(t, results, "car")
	require.Contains(t, results, "cart")
	require.NotContains(t, results, "category")
}

func TestFuzzyTrie_SearchDistance2(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("kitten")
	trie.Insert("sitting")
	trie.Insert("kitchen")

	results := trie.Search("kitten", 2)

	// kitten -> kitten: 0 edits
	// kitten -> kitchen: 2 edits (substitute t->c, insert h)
	// kitten -> sitting: 3 edits

	require.Contains(t, results, "kitten")
	require.Contains(t, results, "kitchen")
	require.NotContains(t, results, "sitting")
}

func TestFuzzyTrie_SearchEmptyQuery(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("a")
	trie.Insert("ab")
	trie.Insert("abc")

	results := trie.Search("", 1)
	// Distance from "" to "a" is 1, to "ab" is 2, to "abc" is 3
	require.Len(t, results, 1)
	require.Equal(t, "a", results[0])

	results = trie.Search("", 2)
	require.Len(t, results, 2)
	require.Contains(t, results, "a")
	require.Contains(t, results, "ab")
}

func TestFuzzyTrie_SearchEmptyTrie(t *testing.T) {
	trie := NewFuzzyTrie()
	results := trie.Search("hello", 1)
	require.Len(t, results, 0)
}

func TestFuzzyTrie_SearchPrefixWords(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("test")
	trie.Insert("testing")
	trie.Insert("tester")

	results := trie.Search("test", 1)

	// test -> test: 0 edits
	// test -> testing: 3 insertions (too far)
	// test -> tester: 2 edits (too far)

	require.Len(t, results, 1)
	require.Equal(t, "test", results[0])

	results = trie.Search("test", 3)
	// Now all three should be found
	require.Len(t, results, 3)
	require.Contains(t, results, "test")
	require.Contains(t, results, "testing")
	require.Contains(t, results, "tester")
}

func TestFuzzyTrie_SearchMultiByte(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("café")
	trie.Insert("cafes")
	trie.Insert("カフェ") // Japanese for "cafe"

	results := trie.Search("café", 1)
	require.Contains(t, results, "café")
	// cafes differs by 1 character (é vs e, plus s)
	// Actually it's 2 edits: substitute é->e and insert s
	require.NotContains(t, results, "cafes")
}

func TestFuzzyTrie_SearchLongerQuery(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("cat")

	results := trie.Search("cats", 1)
	// "cats" -> "cat" requires 1 deletion, so distance is 1
	require.Contains(t, results, "cat")

	results = trie.Search("category", 5)
	// "category" -> "cat" requires deleting "egory" (5 deletions)
	require.Contains(t, results, "cat")

	results = trie.Search("category", 4)
	// Distance is 5, should not be found
	require.NotContains(t, results, "cat")
}

func TestFuzzyTrie_SearchShorterQuery(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("category")

	results := trie.Search("cat", 5)
	// "cat" -> "category" requires inserting "egory" (5 insertions)
	require.Contains(t, results, "category")

	results = trie.Search("cat", 4)
	// Distance is 5, should not be found
	require.NotContains(t, results, "category")
}

func TestFuzzyTrie_SearchDeletions(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("hello")

	results := trie.Search("hllo", 1)
	// "hllo" -> "hello" requires inserting 'e', distance 1
	require.Contains(t, results, "hello")
}

func TestFuzzyTrie_SearchTranspositions(t *testing.T) {
	// Note: Standard Levenshtein distance treats transposition as 2 edits
	// (delete + insert), not as a single transposition operation
	trie := NewFuzzyTrie()
	trie.Insert("hello")

	results := trie.Search("hlelo", 2)
	// "hlelo" -> "hello" requires delete 'l', insert 'l' = 2 edits
	require.Contains(t, results, "hello")

	results = trie.Search("hlelo", 1)
	// Should not be found with distance 1
	require.NotContains(t, results, "hello")
}

// TestFuzzyTrie_SearchVerifyLevenshteinDistance verifies that the trie search
// results match what we'd get from computing Levenshtein distance directly.
func TestFuzzyTrie_SearchVerifyLevenshteinDistance(t *testing.T) {
	words := []string{
		"apple", "apply", "application", "banana", "band",
		"can", "cat", "cats", "dog", "dogs", "hello", "help",
	}

	trie := NewFuzzyTrie()
	for _, word := range words {
		trie.Insert(word)
	}

	testCases := []struct {
		query   string
		maxDist int
	}{
		{"cat", 0},
		{"cat", 1},
		{"cat", 2},
		{"apple", 1},
		{"hello", 2},
		{"", 1},
	}

	for _, tc := range testCases {
		t.Run(tc.query, func(t *testing.T) {
			trieResults := trie.Search(tc.query, tc.maxDist)

			// Compute expected results using naive approach
			var expected []string
			for _, word := range words {
				dist := LevenshteinDistance(tc.query, word)
				if dist <= tc.maxDist {
					expected = append(expected, word)
				}
			}

			// Results should match (order doesn't matter)
			slices.Sort(trieResults)
			slices.Sort(expected)

			// Handle nil vs empty slice difference
			if len(expected) == 0 && len(trieResults) == 0 {
				return
			}

			require.Equal(t, expected, trieResults,
				"Trie search results should match naive Levenshtein for query=%q maxDist=%d",
				tc.query, tc.maxDist)
		})
	}
}

// Benchmark comparing trie-based fuzzy search vs naive approach
func BenchmarkFuzzySearch(b *testing.B) {
	// Create a large dictionary
	words := []string{
		"apple", "apply", "application", "apricot", "approve",
		"banana", "band", "bandage", "banner", "bar",
		"cat", "cats", "caterpillar", "cathedral", "cattle",
		"dog", "dogs", "dogma", "doll", "dollar",
		"elephant", "elevate", "elevator", "eleven", "eliminate",
		"fantastic", "fantasy", "far", "farm", "farmer",
		"great", "green", "greet", "grew", "grid",
		"happy", "harbor", "hard", "harden", "hardware",
		"incredible", "increase", "index", "indicate", "industry",
		"jacket", "jaguar", "jail", "jam", "jar",
		"kangaroo", "karate", "keel", "keen", "keep",
		"label", "labor", "laboratory", "ladder", "lake",
		"machine", "magazine", "magic", "magician", "magnificent",
		"narrow", "nation", "national", "native", "natural",
		"object", "objective", "obligation", "observation", "observe",
		"package", "packet", "page", "pain", "paint",
		"quality", "quantity", "quarter", "queen", "question",
		"rabbit", "race", "rack", "radar", "radiation",
		"safari", "safe", "safety", "sail", "sailor",
		"table", "tablet", "tackle", "tail", "tailor",
		"umbrella", "unable", "uncle", "under", "understand",
		"vaccine", "vacuum", "vague", "valid", "valley",
		"waffle", "wage", "wagon", "waist", "wait",
		"yellow", "yes", "yesterday", "yield", "young",
		"zebra", "zero", "zone", "zoo", "zoom",
	}

	trie := NewFuzzyTrie()
	for _, word := range words {
		trie.Insert(word)
	}

	queries := []struct {
		query   string
		maxDist int
	}{
		{"cat", 1},
		{"apple", 2},
		{"hello", 1},
		{"waffle", 1},
	}

	for _, q := range queries {
		b.Run("trie/"+q.query, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = trie.Search(q.query, q.maxDist)
			}
		})

		b.Run("naive/"+q.query, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var results []string
				for _, word := range words {
					if LevenshteinDistance(q.query, word) <= q.maxDist {
						results = append(results, word)
					}
				}
				_ = results
			}
		})
	}
}

func TestFuzzyTrie_SearchWithCost(t *testing.T) {
	trie := NewFuzzyTrie()
	trie.Insert("cat")
	trie.Insert("bat")
	trie.Insert("car")
	trie.Insert("cats")

	// Test 1: Higher substitution cost makes substitutions more expensive
	// With standard costs (1,1,1), "bat" is distance 1 from "cat" (substitute b->c)
	results := trie.SearchWithCost("cat", 1, 1, 1, 1)
	require.Contains(t, results, "bat", "With uniform costs, 'bat' should be found")

	// With high substitution cost, "bat" requires cost 5, exceeding maxDist=1
	results = trie.SearchWithCost("cat", 1, 1, 1, 5)
	require.NotContains(t, results, "bat", "With subCost=5, 'bat' should not be found within distance 1")
	require.Contains(t, results, "cat", "Exact match should still be found")

	// Test 2: Asymmetric costs - expensive insertions
	// From trie's perspective, matching "cat" (in trie) to "cats" (query) requires 1 insertion
	results = trie.SearchWithCost("cats", 1, 1, 1, 1)
	require.Contains(t, results, "cat", "With uniform costs, 'cat' should be found from 'cats'")

	// With expensive insertions (cost=5), matching "cat" to "cats" costs 5, exceeding maxDist=1
	results = trie.SearchWithCost("cats", 1, 5, 1, 1)
	require.NotContains(t, results, "cat", "With insCost=5, 'cat' should not be found from 'cats' within distance 1")
	require.Contains(t, results, "cats", "Exact match should still be found")

	// Test 3: Verify SearchWithCost with uniform costs matches Search
	resultsSearch := trie.Search("cat", 2)
	resultsWithCost := trie.SearchWithCost("cat", 2, 1, 1, 1)
	slices.Sort(resultsSearch)
	slices.Sort(resultsWithCost)
	require.Equal(t, resultsSearch, resultsWithCost,
		"SearchWithCost with uniform costs should match Search")
}

// BenchmarkFuzzyTrie_Insert measures insertion performance
func BenchmarkFuzzyTrie_Insert(b *testing.B) {
	words := []string{
		"apple", "banana", "cat", "dog", "elephant",
		"fantastic", "great", "hello", "incredible", "jacket",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trie := NewFuzzyTrie()
		for _, word := range words {
			trie.Insert(word)
		}
	}
}
