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
// Author: Andrew Bonventre (andybons@gmail.com)

package encoding

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEncodeString(t *testing.T) {
	testCases := []struct {
		text   string
		length int
	}{
		{"foo", 5},
		{"baaaar", 8},
		{"bazz", 6},
	}
	for _, c := range testCases {
		buf := EncodeString(c.text)
		n := len(buf)
		if n != c.length {
			t.Errorf("short write: %d bytes written; %d expected", n, c.length)
		}
		if buf[n-1] != orderedEncodingTerminator {
			t.Errorf("expected terminating byte (%#x), got %#x", orderedEncodingTerminator, buf[n-1])
		}
	}
	invalid := string([]byte{0x00, 0x01, 0x02})
	buf := EncodeString(invalid)
	if buf != nil {
		t.Errorf("expected buf to be nil due to invalud utf8 string %s, got %v", invalid, buf)
	}
}

func TestEncodeBinary(t *testing.T) {
	// TODO(andybons): Write more of these.
	testCases := []struct{ blob, encoded []byte }{
		{[]byte{}, []byte{orderedEncodingBinary, orderedEncodingTerminator}},
		{[]byte{0xff}, []byte{orderedEncodingBinary, 0xff, 0x40}},
	}
	for _, c := range testCases {
		b := EncodeBinary(c.blob)
		println(prettyBytes(b))
		if !bytes.Equal(b, c.encoded) {
			t.Errorf("unexpected mismatch of encoded value: expected %s, got %s", prettyBytes(c.encoded), prettyBytes(b))
		}
	}
}

func prettyBytes(b []byte) string {
	str := "["
	for i, v := range b {
		str += fmt.Sprintf("%#x", v)
		if i < len(b)-1 {
			str += " "
		}
	}
	str += "]"
	return str
}

func disabledTestFloatMandE(t *testing.T) {
	testCases := []struct {
		Value float64
		E     int
		M     []byte
	}{
		{1.0, 1, []byte{0x02}},
		{10.0, 1, []byte{0x14}},
		{99.0, 1, []byte{0xc6}},
		{99.01, 1, []byte{0xc7, 0x02}},
		{99.0001, 1, []byte{0xc7, 0x01, 0x02}},
		{100.0, 2, []byte{0x02}},
		{100.01, 2, []byte{0x03, 0x01, 0x02}},
		{100.1, 2, []byte{0x03, 0x01, 0x14}},
		{1234, 2, []byte{0x19, 0x44}},
		{9999, 2, []byte{0xc7, 0xc6}},
		{9999.000001, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x02}},
		{9999.000009, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x12}},
		{9999.00001, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0x14}},
		{9999.00009, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0xb4}},
		{9999.000099, 2, []byte{0xc7, 0xc7, 0x01, 0x01, 0xc6}},
		{9999.0001, 2, []byte{0xc7, 0xc7, 0x01, 0x02}},
		{9999.001, 2, []byte{0xc7, 0xc7, 0x01, 0x14}},
		{9999.01, 2, []byte{0xc7, 0xc7, 0x02}},
		{9999.1, 2, []byte{0xc7, 0xc7, 0x14}},
		{10000, 3, []byte{0x02}},
		{10001, 3, []byte{0x03, 0x01, 0x02}},
		{12345, 3, []byte{0x03, 0x2f, 0x5a}},
		{123450, 3, []byte{0x19, 0x45, 0x64}},
		{1234.5, 2, []byte{0x19, 0x45, 0x64}},
		{12.345, 1, []byte{0x19, 0x45, 0x64}},
		{0.123, 0, []byte{0x19, 0x3c}},
		{0.0123, 0, []byte{0x03, 0x2e}},
		{0.00123, -1, []byte{0x19, 0x3c}},
		{9223372036854775807, 10, []byte{0x13, 0x2d, 0x43, 0x91, 0x07, 0x89, 0x6d, 0x9b, 0x75, 0x0e}},
	}
	for _, c := range testCases {
		if e, m := floatMandE(c.Value); e != c.E || !bytes.Equal(m, c.M) {
			t.Errorf("unexpected mismatch in E/M for %v. expected E=%v | M=%+v, got E=%v | M=%+v", c.Value, c.E, c.M, e, m)
		}
	}
}
