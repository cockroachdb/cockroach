// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package uint128

import (
	"bytes"
	"strings"
	"testing"
)

func TestBytes(t *testing.T) {
	b := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	i := FromBytes(b)

	if !bytes.Equal(i.GetBytes(), b) {
		t.Errorf("incorrect bytes representation for num: %v", i)
	}
}

func TestString(t *testing.T) {
	s := "a95e31998f38490651c02b97c7f2acca"

	i, _ := FromString(s)

	if s != i.String() {
		t.Errorf("incorrect string representation for num: %v", i)
	}
}

func TestStringTooLong(t *testing.T) {
	s := "ba95e31998f38490651c02b97c7f2acca"

	_, err := FromString(s)

	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Error("did not get error for encoding invalid uint128 string")
	}
}

func TestStringInvalidHex(t *testing.T) {
	s := "bazz95e31998849051c02b97c7f2acca"

	_, err := FromString(s)

	if err == nil || !strings.Contains(err.Error(), "could not decode") {
		t.Error("did not get error for encoding invalid uint128 string")
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

func TestEqual(t *testing.T) {
	testData := []struct {
		u1       Uint128
		u2       Uint128
		expected bool
	}{
		{Uint128{0, 0}, Uint128{0, 1}, false},
		{Uint128{1, 0}, Uint128{0, 1}, false},
		{Uint128{18446744073709551615, 18446744073709551614}, Uint128{18446744073709551615, 18446744073709551615}, false},
		{Uint128{0, 1}, Uint128{0, 1}, true},
		{Uint128{0, 0}, Uint128{0, 0}, true},
		{Uint128{314, 0}, Uint128{314, 0}, true},
		{Uint128{18446744073709551615, 18446744073709551615}, Uint128{18446744073709551615, 18446744073709551615}, true},
	}

	for _, test := range testData {

		if actual := test.u1.Equal(test.u2); actual != test.expected {
			t.Errorf("expected: %v.Equal(%v) expected %v but got %v", test.u1, test.u2, test.expected, actual)
		}
	}
}

func TestAnd(t *testing.T) {
	u1 := Uint128{14799720563850130797, 11152134164166830811}
	u2 := Uint128{10868624793753271583, 6542293553298186666}

	expected := Uint128{9529907221165552909, 1927615693132931210}
	if !(u1.And(u2)).Equal(expected) {
		t.Errorf("incorrect AND computation: %v & %v != %v", u1, u2, expected)
	}
}

func TestOr(t *testing.T) {
	u1 := Uint128{14799720563850130797, 11152134164166830811}
	u2 := Uint128{10868624793753271583, 6542293553298186666}

	expected := Uint128{16138438136437849471, 15766812024332086267}
	if !(u1.Or(u2)).Equal(expected) {
		t.Errorf("incorrect OR computation: %v | %v != %v", u1, u2, expected)
	}
}

func TestXor(t *testing.T) {
	u1 := Uint128{14799720563850130797, 11152134164166830811}
	u2 := Uint128{10868624793753271583, 6542293553298186666}

	expected := Uint128{6608530915272296562, 13839196331199155057}
	if !(u1.Xor(u2)).Equal(expected) {
		t.Errorf("incorrect XOR computation: %v ^ %v != %v", u1, u2, expected)
	}
}
