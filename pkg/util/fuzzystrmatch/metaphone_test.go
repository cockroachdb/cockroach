// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	crypto_rand "crypto/rand"
	"math/rand"
	"testing"
)

func TestMetaphone(t *testing.T) {
	tt := []struct {
		Source   string
		Expected string
	}{
		{
			Source:   "hello world!",
			Expected: "HLWRLT",
		},
		{
			Source:   "pi",
			Expected: "P",
		},
		{
			Source:   "gumbo",
			Expected: "KM",
		},
		{
			Source:   "Night",
			Expected: "NFT",
		},
		{
			Source:   "Knight",
			Expected: "NFT",
		},
		{
			Source:   "Knives",
			Expected: "NFS",
		},
		{
			Source:   "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Expected: "",
		},
		{
			Source:   "A😋",
			Expected: "A",
		},
		{
			Source:   "zażółćx",
			Expected: "SKS",
		},
		{
			Source:   "l�qă�_��",
			Expected: "LK",
		},
		{
			Source:   "Donald",
			Expected: "TNLT",
		},
		{
			Source:   "Zach",
			Expected: "SX",
		},
		{
			Source:   "Campbel",
			Expected: "KMPBL",
		},
		{
			Source:   "Cammmppppbbbeeelll",
			Expected: "KMPBL",
		},
		{
			Source:   "David",
			Expected: "TFT",
		},
		{
			Source:   "Wat",
			Expected: "WT",
		},
		{
			Source:   "What",
			Expected: "HT",
		},
		{
			Source:   "Gaspar",
			Expected: "KSPR",
		},
		{
			Source:   "ggaspar",
			Expected: "KSPR",
		},
		{
			Source:   "ablaze",
			Expected: "ABLS",
		},
		{
			Source:   "transition",
			Expected: "TRNSXN",
		},
		{
			Source:   "astronomical",
			Expected: "ASTRNMKL",
		},
		{
			Source:   "buzzard",
			Expected: "BSRT",
		},
		{
			Source:   "wonderer",
			Expected: "WNTRR",
		},
		{
			Source:   "district",
			Expected: "TSTRKT",
		},
		{
			Source:   "hockey",
			Expected: "HK",
		},
		{
			Source:   "capital",
			Expected: "KPTL",
		},
		{
			Source:   "penguin",
			Expected: "PNKN",
		},
		{
			Source:   "garbonzo",
			Expected: "KRBNS",
		},
		{
			Source:   "lightning",
			Expected: "LFTNNK",
		},
		{
			Source:   "light",
			Expected: "LFT",
		},
		{
			Source:   "Smith",
			Expected: "SM0",
		},
		{
			Source:   "Knuth",
			Expected: "N0",
		},
		{
			Source:   "Boothe",
			Expected: "B0",
		},
	}

	// Run some random test cases to make sure we don't panic.
	for i := 0; i < 1000; i++ {
		l := rand.Int31n(10)
		b := make([]byte, l)
		_, _ = crypto_rand.Read(b)

		_ = Metaphone(string(b), 4)
	}

	for _, tc := range tt {
		t.Run(tc.Source, func(t *testing.T) {
			got := Metaphone(tc.Source, 20)
			if tc.Expected != got {
				t.Fatalf("error convert string to its Metaphone code with source=%q"+
					" expected %s got %s", tc.Source, tc.Expected, got)
			}
		})
	}
}

func TestMetaphoneMaxLength(t *testing.T) {
	tt := []struct {
		Source   string
		Length   int
		Expected string
	}{
		// Expected values are the output of PostgreSQL's metaphone().
		{Source: "Max", Length: 3, Expected: "MKS"},
		{Source: "Max", Length: 2, Expected: "MK"},
		{Source: "Fox", Length: 2, Expected: "FK"},
		{Source: "Alex", Length: 3, Expected: "ALK"},
		{Source: "Alexa", Length: 4, Expected: "ALKS"},
		{Source: "Roth", Length: 3, Expected: "R0"},
		{Source: "Beth", Length: 2, Expected: "B0"},
		{Source: "Smith", Length: 2, Expected: "SM"},
	}

	for _, tc := range tt {
		t.Run(tc.Source, func(t *testing.T) {
			got := Metaphone(tc.Source, tc.Length)
			if tc.Expected != got {
				t.Fatalf("error convert string to its Metaphone code with source=%q length=%d"+
					" expected %s got %s", tc.Source, tc.Length, tc.Expected, got)
			}
		})
	}
}
