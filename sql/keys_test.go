// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestNormalizeName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		in, expected string
	}{
		{"HELLO", "hello"},                            // Lowercase is the norm
		{"ıİ", "ii"},                                  // Turkish/Azeri special cases
		{"no\u0308rmalization", "n\u00f6rmalization"}, // NFD -> NFC.
	}
	for _, test := range testCases {
		s := NormalizeName(test.in)
		if test.expected != s {
			t.Errorf("%s: expected %s, but found %s", test.in, test.expected, s)
		}
	}
}

func TestKeyAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		key roachpb.Key
	}{
		{MakeNameMetadataKey(0, "BAR")},
		{MakeNameMetadataKey(1, "BAR")},
		{MakeNameMetadataKey(1, "foo")},
		{MakeNameMetadataKey(2, "foo")},
		{MakeDescMetadataKey(123)},
		{MakeDescMetadataKey(124)},
	}
	var lastKey roachpb.Key
	for i, test := range testCases {
		resultAddr, err := keys.Addr(test.key)
		if err != nil {
			t.Fatal(err)
		}
		result := resultAddr.AsRawKey()
		if result.Compare(lastKey) <= 0 {
			t.Errorf("%d: key address %q is <= %q", i, result, lastKey)
		}
		lastKey = result
	}
}
