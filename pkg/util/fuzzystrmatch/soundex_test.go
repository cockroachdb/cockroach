// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	crypto_rand "crypto/rand"
	"math/rand"
	"testing"
)

func TestSoundex(t *testing.T) {
	tt := []struct {
		Source   string
		Expected string
	}{
		{
			Source:   "hello world!",
			Expected: "H464",
		},
		{
			Source:   "Anne",
			Expected: "A500",
		},
		{
			Source:   "Ann",
			Expected: "A500",
		},
		{
			Source:   "Andrew",
			Expected: "A536",
		},
		{
			Source:   "Margaret",
			Expected: "M626",
		},
		{
			Source:   "🌞",
			Expected: "",
		},
		{
			Source:   "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Expected: "",
		},
		{
			Source:   "zażółćx",
			Expected: "Z200",
		},
		{
			Source:   "K😋",
			Expected: "K000",
		},
		// Regression test for #82640, just ensure we don't panic.
		{
			Source:   "l�qă�_��",
			Expected: "L200",
		},
	}

	for _, tc := range tt {
		got := Soundex(tc.Source)
		if tc.Expected != got {
			t.Fatalf("error convert string to its Soundex code with source=%q"+
				" expected %s got %s", tc.Source, tc.Expected, got)
		}
	}

	// Run some random test cases to make sure we don't panic.

	for i := 0; i < 1000; i++ {
		l := rand.Int31n(10)
		b := make([]byte, l)
		_, _ = crypto_rand.Read(b)

		soundex(string(b))
	}
}

func TestDifference(t *testing.T) {
	tt := []struct {
		Source   string
		Target   string
		Expected int
	}{
		{
			Source:   "Anne",
			Target:   "Ann",
			Expected: 4,
		},
		{
			Source:   "Anne",
			Target:   "Andrew",
			Expected: 2,
		},
		{
			Source:   "Anne",
			Target:   "Margaret",
			Expected: 0,
		},
	}

	for _, tc := range tt {
		got := Difference(tc.Source, tc.Target)
		if tc.Expected != got {
			t.Fatalf("error reports the number of matching code positions with source=%q"+
				" target=%q: expected %d got %d", tc.Source, tc.Target, tc.Expected, got)
		}
	}
}
