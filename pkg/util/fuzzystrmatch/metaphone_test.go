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
			Expected: "HLWR",
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
	}

	// Run some random test cases to make sure we don't panic.
	for i := 0; i < 1000; i++ {
		l := rand.Int31n(10)
		b := make([]byte, l)
		_, _ = crypto_rand.Read(b)

		_ = Metaphone(string(b), 4)
	}

	for _, tc := range tt {
		got := Metaphone(tc.Source, 4)
		if tc.Expected != got {
			t.Fatalf("error convert string to its Metaphone code with source=%q"+
				" expected %s got %s", tc.Source, tc.Expected, got)
		}
	}
}
