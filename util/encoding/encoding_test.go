// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package encoding

import (
	"math"
	"reflect"
	"testing"
)

func TestEncoding(t *testing.T) {
	k := []byte("asd")
	n := int64(3142595)
	encoded, err := Encode(k, int64(n))
	if err != nil {
		t.Errorf("encoding error for %d", n)
	}
	decoded, err := Decode(k, encoded)
	if err != nil {
		t.Errorf("decoding error for %d", n)
	}
	switch v := decoded.(type) {
	case int64:
		if v != n {
			t.Errorf("int64 decoding error, got the wrong result")
		}
	default:
		t.Errorf("int64 decoding error, did not get a uint64 back but instead type %v: %v", reflect.TypeOf(v), v)
	}
}

func TestChecksums(t *testing.T) {
	testKey := []byte("whatever")
	doubleWrap := func(a []byte) bool {
		_, err := unwrapChecksum(testKey, wrapChecksum(testKey, a))
		return err == nil
	}

	_, err := unwrapChecksum([]byte("does not matter"), []byte("srt"))
	if err == nil {
		t.Fatalf("expected error unwrapping a too short string")
	}

	testCases := [][]byte{
		[]byte(""),
		[]byte("Hello"),
		[]byte("	tab "),
		func() []byte {
			b := make([]byte, math.MaxUint8, math.MaxUint8)
			for i := 0; i < math.MaxUint8; i++ {
				b[i] = 'z'
			}
			return b
		}(),
		[]byte("لاحول ولا قوة الا بالله"),
	}

	for i, c := range testCases {
		if !doubleWrap(c) {
			t.Errorf("unexpected integrity error for '%v'", c)
		}
		// Glue an extra byte to a copy that kills the checksum.
		distorted := append(wrapChecksum(testKey, append([]byte(nil), c...)), '1')
		if _, err := unwrapChecksum(testKey, distorted); err == nil {
			t.Errorf("%d: unexpected integrity match for corrupt value", i)
		}

	}
}

func TestWillOverflow(t *testing.T) {
	testCases := []struct {
		a, b     int64
		overflow bool // will a+b over- or underflow?
	}{
		{0, 0, false},
		{math.MaxInt64, 0, false},
		{math.MaxInt64, 1, true},
		{math.MaxInt64, math.MinInt64, false},
		{math.MinInt64, 0, false},
		{math.MinInt64, -1, true},
		{math.MinInt64, math.MinInt64, true},
	}

	for i, c := range testCases {
		if WillOverflow(c.a, c.b) != c.overflow ||
			WillOverflow(c.b, c.a) != c.overflow {
			t.Errorf("%d: overflow recognition error", i)
		}
	}
}
