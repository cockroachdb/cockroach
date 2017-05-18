// Copyright 2017 The Cockroach Authors.
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
// Author: Tristan Ohlson (tsohlson@gmail.com)

package uint128

import (
	"bytes"
	"testing"
)

func TestBytes(t *testing.T) {
	b := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	i := FromBytes(b)

	if !bytes.Equal(i.GetBytes(), b) {
		t.Errorf("incorrect bytes representation for num: %v", i)
	}
}

func TestSub(t *testing.T) {
	testData := []struct {
		num      Uint128
		expected Uint128
		sub      uint64
	}{
		{Uint128{0, 1}, Uint128{0, 0}, 1},
		{Uint128{18446744073709551615, 18446744073709551615}, Uint128{18446744073709551615, 18446744073709551614}, 1},
		{Uint128{0, 18446744073709551615}, Uint128{0, 18446744073709551614}, 1},
		{Uint128{18446744073709551615, 0}, Uint128{18446744073709551614, 18446744073709551615}, 1},
		{Uint128{18446744073709551615, 0}, Uint128{18446744073709551614, 18446744073709551591}, 25},
	}

	for _, test := range testData {
		res := test.num.Sub(test.sub)
		if res != test.expected {
			t.Errorf("expected: %v - %d = %v but got %v", test.num, test.sub, test.expected, res)
		}
	}
}

func TestAdd(t *testing.T) {
	testData := []struct {
		num      Uint128
		expected Uint128
		add      uint64
	}{
		{Uint128{0, 0}, Uint128{0, 1}, 1},
		{Uint128{18446744073709551615, 18446744073709551614}, Uint128{18446744073709551615, 18446744073709551615}, 1},
		{Uint128{0, 18446744073709551615}, Uint128{1, 0}, 1},
		{Uint128{18446744073709551615, 0}, Uint128{18446744073709551615, 1}, 1},
		{Uint128{0, 18446744073709551615}, Uint128{1, 24}, 25},
	}

	for _, test := range testData {
		res := test.num.Add(test.add)
		if res != test.expected {
			t.Errorf("expected: %v + %d = %v but got %v", test.num, test.add, test.expected, res)
		}
	}
}
