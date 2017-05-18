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
		num      []byte
		expected []byte
		sub      uint64
	}{
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 14},
			1},
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 14},
			1},
		{[]byte{255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0},
			[]byte{255, 255, 255, 255, 255, 255, 255, 254, 255, 255, 255, 255, 255, 255, 255, 246},
			10},
	}

	for _, test := range testData {
		i := FromBytes(test.num)
		res := i.Sub(test.sub).GetBytes()
		if !bytes.Equal(res, test.expected) {
			t.Errorf("expected: %v got %v", test.expected, res)
		}
	}
}

func TestAdd(t *testing.T) {
	testData := []struct {
		num      []byte
		expected []byte
		add      uint64
	}{
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16},
			1},
		{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 13, 255},
			[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 0},
			1},
		{[]byte{255, 255, 255, 255, 255, 255, 255, 254, 255, 255, 255, 255, 255, 255, 255, 246},
			[]byte{255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0},
			10},
	}

	for _, test := range testData {
		i := FromBytes(test.num)
		res := i.Add(test.add).GetBytes()
		if !bytes.Equal(res, test.expected) {
			t.Errorf("expected: %v got %v", test.expected, res)
		}
	}
}
