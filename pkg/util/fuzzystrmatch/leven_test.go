// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fuzzystrmatch

import "testing"

func TestLevenshteinDistance(t *testing.T) {
	tt := []struct {
		Source   string
		Target   string
		Expected int
	}{
		{
			Source:   "book",
			Target:   "back",
			Expected: 2,
		},
		{
			Source:   "vacaville",
			Target:   "fairfield",
			Expected: 6,
		},
		{
			Source:   "123456789",
			Target:   "",
			Expected: 9,
		},
		{
			Source:   "123456789",
			Target:   "123",
			Expected: 6,
		},
		{
			Source:   "",
			Target:   "123456789",
			Expected: 9,
		},
		{
			Source:   "123456789",
			Target:   "123466789",
			Expected: 1,
		},
		{
			Source:   "123456789",
			Target:   "123456789",
			Expected: 0,
		},
		{
			Source:   "",
			Target:   "",
			Expected: 0,
		},
		{
			Source:   "alfaromeoaudibenz",
			Target:   "mercedesalfaromeoaudibenz",
			Expected: 8,
		},
		{
			Source:   "alfaromeoaudibenz",
			Target:   "alfaromeoaudibenzmercedes",
			Expected: 8,
		},
		{
			Source:   "mercedesalfaromeoaudibenz",
			Target:   "alfaromeoaudibenz",
			Expected: 8,
		},
		{
			Source:   "alfaromeoaudibenzmercedes",
			Target:   "alfaromeoaudibenz",
			Expected: 8,
		},
		{
			Source:   "alfaromeo",
			Target:   "alfaromeoaudibenz",
			Expected: 8,
		},
		{
			Source:   "alfaromeo",
			Target:   "bayerischemotorenwerkealfaromeoaudibenz",
			Expected: 30,
		},
		{
			Source:   "🌞",
			Target:   "a",
			Expected: 1,
		},
		{
			Source:   "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Target:   "💤 🚈 👨 💩 👲 💽 🔴 🍨 😮 😅",
			Expected: 10,
		},
	}

	for _, tc := range tt {
		got := LevenshteinDistance(tc.Source, tc.Target)
		if tc.Expected != got {
			t.Fatalf("error calculating levenshtein distance with source=%q"+
				" target=%q: expected %d got %d", tc.Source, tc.Target, tc.Expected, got)
		}
	}
}

func TestLevenshteinDistanceWithCost(t *testing.T) {
	tt := []struct {
		Source   string
		Target   string
		Expected int
	}{
		{
			Source:   "book",
			Target:   "back",
			Expected: 8,
		},
		{
			Source:   "vacaville",
			Target:   "fairfield",
			Expected: 24,
		},
		{
			Source:   "",
			Target:   "123456789",
			Expected: 18,
		},
		{
			Source:   "123456789",
			Target:   "",
			Expected: 27,
		},
		{
			Source:   "123456789",
			Target:   "123",
			Expected: 18,
		},
		{
			Source:   "",
			Target:   "123456789",
			Expected: 18,
		},
		{
			Source:   "123456789",
			Target:   "123466789",
			Expected: 4,
		},
		{
			Source:   "123456789",
			Target:   "123456789",
			Expected: 0,
		},
		{
			Source:   "",
			Target:   "",
			Expected: 0,
		},
		{
			Source:   "alfaromeoaudibenz",
			Target:   "mercedesalfaromeoaudibenz",
			Expected: 16,
		},
		{
			Source:   "alfaromeoaudibenz",
			Target:   "alfaromeoaudibenzmercedes",
			Expected: 16,
		},
		{
			Source:   "mercedesalfaromeoaudibenz",
			Target:   "alfaromeoaudibenz",
			Expected: 24,
		},
		{
			Source:   "alfaromeoaudibenzmercedes",
			Target:   "alfaromeoaudibenz",
			Expected: 24,
		},
		{
			Source:   "alfaromeo",
			Target:   "alfaromeoaudibenz",
			Expected: 16,
		},
		{
			Source:   "alfaromeo",
			Target:   "bayerischemotorenwerkealfaromeoaudibenz",
			Expected: 60,
		},
		{
			Source:   "🌞",
			Target:   "a",
			Expected: 4,
		},
		{
			Source:   "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Target:   "💤 🚈 👨 💩 👲 💽 🔴 🍨 😮 😅",
			Expected: 40,
		},
	}

	for _, tc := range tt {
		got := LevenshteinDistanceWithCost(tc.Source, tc.Target, 2, 3, 4)
		if tc.Expected != got {
			t.Fatalf("error calculating levenshtein distance with cost with "+
				"source=%q target=%q: expected %d got %d",
				tc.Source, tc.Target, tc.Expected, got)
		}
	}
}

func TestLevenshteinLessEqualDistance(t *testing.T) {
	tt := []struct {
		Source      string
		Target      string
		MaxDistance int
		Expected    int
	}{
		// Test cases for LevenshteinDistance less than max distance.
		{
			Source:      "book",
			Target:      "back",
			MaxDistance: 3,
			Expected:    2,
		},
		{
			Source:      "vacaville",
			Target:      "fairfield",
			MaxDistance: 7,
			Expected:    6,
		},

		// Test cases for LevenshteinDistance greater than max distance.
		{
			Source:      "book",
			Target:      "back",
			MaxDistance: 1,
			Expected:    2,
		},
		{
			Source:      "vacaville",
			Target:      "fairfield",
			MaxDistance: 5,
			Expected:    6,
		},
		{
			Source:      "aaaa",
			Target:      "xxxx",
			MaxDistance: 2,
			Expected:    3,
		},

		// Test cases for empty strings.
		{
			Source:      "",
			Target:      "",
			MaxDistance: 0,
			Expected:    0,
		},
		{
			Source:      "",
			Target:      "abc",
			MaxDistance: 2,
			Expected:    3,
		},
		{
			Source:      "xyz",
			Target:      "",
			MaxDistance: 3,
			Expected:    3,
		},

		// Test cases with emojis.
		{
			Source:      "🌞",
			Target:      "a",
			MaxDistance: 1,
			Expected:    1,
		},
		{
			Source:      "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Target:      "💤 🚈 👨 💩 👲 💽 🔴 🍨 😮 😅",
			MaxDistance: 15,
			Expected:    10,
		},
	}

	for _, tc := range tt {
		got := LevenshteinLessEqualDistance(tc.Source, tc.Target, tc.MaxDistance)
		if tc.Expected != got {
			t.Fatalf("error checking Levenshtein distance less than %d with "+
				"source=%q target=%q: expected %d got %d",
				tc.MaxDistance, tc.Source, tc.Target, tc.Expected, got)
		}
	}
}

func TestLevenshteinLessEqualDistanceWithCost(t *testing.T) {
	tt := []struct {
		Source      string
		Target      string
		MaxDistance int
		Expected    int
	}{
		// Test cases for LevenshteinDistanceWithCost less than distance.
		{
			Source:      "book",
			Target:      "back",
			MaxDistance: 8,
			Expected:    8,
		},
		{
			Source:      "vacaville",
			Target:      "fairfield",
			MaxDistance: 24,
			Expected:    24,
		},
		{
			Source:      "123456789",
			Target:      "123",
			MaxDistance: 18,
			Expected:    18,
		},
		{
			Source:      "",
			Target:      "123456789",
			MaxDistance: 18,
			Expected:    18,
		},
		{
			Source:      "123456789",
			Target:      "123466789",
			MaxDistance: 6,
			Expected:    4,
		},

		// Test cases for LevenshteinDistanceWithCost greater than distance.
		{
			Source:      "book",
			Target:      "back",
			MaxDistance: 1,
			Expected:    2,
		},
		{
			Source:      "vacaville",
			Target:      "fairfield",
			MaxDistance: 18,
			Expected:    19,
		},
		{
			Source:      "123456789",
			Target:      "123",
			MaxDistance: 6,
			Expected:    7,
		},
		{
			Source:      "",
			Target:      "123456789",
			MaxDistance: 10,
			Expected:    11,
		},
		{
			Source:      "123456789",
			Target:      "123466789",
			MaxDistance: 2,
			Expected:    3,
		},

		// Test cases for empty strings.
		{
			Source:      "",
			Target:      "",
			MaxDistance: 0,
			Expected:    0,
		},
		{
			Source:      "",
			Target:      "abc",
			MaxDistance: 2,
			Expected:    3,
		},
		{
			Source:      "xyz",
			Target:      "",
			MaxDistance: 3,
			Expected:    4,
		},

		// Test cases with emojis.
		{
			Source:      "🌞",
			Target:      "a",
			MaxDistance: 1,
			Expected:    2,
		},
		{
			Source:      "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Target:      "💤 🚈 👨 💩 👲 💽 🔴 🍨 😮 😅",
			MaxDistance: 29,
			Expected:    30,
		},

		// Testcases for negative max distance.
		{
			Source:      "book",
			Target:      "back",
			MaxDistance: -1,
			Expected:    8,
		},
		{
			Source:      "vacaville",
			Target:      "fairfield",
			MaxDistance: -1,
			Expected:    24,
		},
		{
			Source:      "",
			Target:      "123456789",
			MaxDistance: -1,
			Expected:    18,
		},
	}

	for _, tc := range tt {
		got := LevenshteinLessEqualDistanceWithCost(tc.Source, tc.Target, 2, 3, 4, tc.MaxDistance)
		if tc.Expected != got {
			t.Fatalf("error checking Levenshtein distance with cost less than %d with "+
				"source=%q target=%q: expected %d got %d",
				tc.MaxDistance, tc.Source, tc.Target, tc.Expected, got)
		}
	}
}
