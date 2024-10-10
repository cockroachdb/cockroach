// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func TestLevenshteinDistanceWithNegativeCost(t *testing.T) {
	
	tc:= struct {
		Source   string
		Target   string
		Expected int
	}{
		Source:   "book",
		Target:   "back",
		Expected: 8,
	}
}

func TestLevenshteinDistanceWithCostAndThreshold(t *testing.T) {
	tt := []struct {
		Source   string
		Target   string
		InsertionCost int
		DeletionCost int
		SubstitutionCost int
		Threshold int
		Expected int
	}{
		{
			Source:   "book", 
			Target:   "back", 
			InsertionCost: 1, 
			DeletionCost: 1, 
			SubstitutionCost: 1, 
			Threshold: 8, 
			Expected: 2,
		},
		{
			Source:   "vacaville", 
			Target:   "fairfield", 
			InsertionCost: 1, 
			DeletionCost: 2, 
			SubstitutionCost: 1, 
			Threshold: 24, 
			Expected: 6,
		},
		{
			Source:   " ", 
			Target:   "123456789", 
			InsertionCost: 1, 
			DeletionCost: 1, 
			SubstitutionCost: 1, 
			Threshold: 9, 
			Expected: 9,
		},
		{
			Source:   "123456789", 
			Target:   " ", 
			InsertionCost: 4, 
			DeletionCost: 2, 
			SubstitutionCost: 1, 
			Threshold: 27, 
			Expected: 17,
		},
		{
			Source:   "123456789", 
			Target:   "123", 
			InsertionCost: 1, 
			DeletionCost: 1, 
			SubstitutionCost: 1, 
			Threshold: 18, 
			Expected: 6,
		},
		{		
			Source:   " ", 
			Target:   "123456789", 
			InsertionCost: 2, 
			DeletionCost: 3, 
			SubstitutionCost: 4, 
			Threshold: 18, 
			Expected: 19,
		},
		{	
			Source:   "123456789", 
			Target:   " ", 
			InsertionCost: 5, 
			DeletionCost: 6, 
			SubstitutionCost: 7, 
			Threshold: 27, 
			Expected: 28,
		},
		{
			Source:   "alfaromeo",
			Target:   "alfaromeoaudibenz",
			InsertionCost: 1, 
			DeletionCost: 1, 
			SubstitutionCost: 1, 
			Threshold: 19,
			Expected: 8,
		},
		{
			Source:   "alfaromeo",
			Target:   "bayerischemotorenwerkealfaromeoaudibenz",
			InsertionCost: 1, 
			DeletionCost: 1, 
			SubstitutionCost: 1, 
			Threshold: 30,
			Expected: 31,
		},
		{
			Source:   "🌞",
			Target:   "a",
			InsertionCost: 1, 
			DeletionCost: 1, 
			SubstitutionCost: 1, 
			Threshold: 4,
			Expected: 1,
		},
		{
			Source:   "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Target:   "💤 🚈 👨 💩 👲 💽 🔴 🍨 😮 😅",
			InsertionCost: 2, 
			DeletionCost: 3, 
			SubstitutionCost: 4,
			Threshold: 42,
			Expected: 40,
		},
			
	}

	for _, tc := range tt {
		got := LevenshteinDistanceWithCostAndThreshold(tc.Source, tc.Target, tc.InsertionCost, tc.DeletionCost, tc.SubstitutionCost, tc.Threshold)
		if tc.Expected != got {
			t.Fatalf("error calculating levenshtein distance with cost and threshold with "+
				"source=%q target=%q: expected %d got %d",
				tc.Source, tc.Target, tc.Expected, got)
		}
	}	

}