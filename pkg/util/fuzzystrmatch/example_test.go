// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch_test

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/fuzzystrmatch"
)

func ExampleFuzzyTrie() {
	// Create a new fuzzy trie
	trie := fuzzystrmatch.NewFuzzyTrie()

	// Insert some table names
	trie.Insert("users")
	trie.Insert("user_sessions")
	trie.Insert("products")
	trie.Insert("product_reviews")
	trie.Insert("orders")
	trie.Insert("order_items")

	// Search for tables similar to "user" (distance <= 1)
	results := trie.Search("user", 1)
	fmt.Println("Tables similar to 'user':", results)

	// Search for tables similar to "prodct" (typo, distance <= 1)
	results = trie.Search("prodct", 1)
	fmt.Println("Did you mean:", results)

	// Output:
	// Tables similar to 'user': [users]
	// Did you mean: []
}

func ExampleFuzzyTrie_Search() {
	trie := fuzzystrmatch.NewFuzzyTrie()

	// Build a dictionary of programming languages
	languages := []string{
		"Go", "Python", "Java", "JavaScript", "TypeScript",
		"C", "C++", "C#", "Ruby", "Rust", "Swift", "Kotlin",
	}

	for _, lang := range languages {
		trie.Insert(lang)
	}

	// Fuzzy search with distance 2
	query := "Jave" // typo for "Java"
	results := trie.Search(query, 2)
	fmt.Printf("Searching for '%s' (max distance 2): %v\n", query, results)

	// Output:
	// Searching for 'Jave' (max distance 2): [Java]
}
