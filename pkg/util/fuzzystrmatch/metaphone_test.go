// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
			Expected: "WT",
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
			Expected: "LTNNK",
		},
		{
			Source:   "light",
			Expected: "LT",
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
