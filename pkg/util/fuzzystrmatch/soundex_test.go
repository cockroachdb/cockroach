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
			Expected: "000",
		},
		{
			Source:   "😄 🐃 🐯 🕣 💲 🏜 👞 🔠 🌟 📌",
			Expected: "",
		},
	}

	for _, tc := range tt {
		got := Soundex(tc.Source)
		if tc.Expected != got {
			t.Fatalf("error convert string to its Soundex code with source=%q"+
				" expected %s got %s", tc.Source, tc.Expected, got)
		}
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
