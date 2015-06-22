// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package parser

import "testing"

func TestEncodeSQLString(t *testing.T) {
	testCases := []struct {
		arg      string
		expected string
	}{
		{"abcd", "'abcd'"},
		{"workin' hard", "'workin\\' hard'"},
		{"\x00'\"\b\n\r\t\x1A\\", `'\0\'\"\b\n\r\t\Z\\'`},
		// Three different representations of the same unicode string.
		{"\xE7\xB1\xB3\xE6\xB4\xBE", "'米派'"},
		{"\u7C73\u6D3E", "'米派'"},
		{"米派", "'米派'"},
	}
	for _, c := range testCases {
		encoded := encodeSQLString(nil, []byte(c.arg))
		if string(encoded) != c.expected {
			t.Errorf("Expected %s, but got %s", c.expected, encoded)
		}
	}
}

func TestEncodeSQLBytes(t *testing.T) {
	testCases := []struct {
		arg      string
		expected string
	}{
		{"", "X''"},
		{"abcd", "X'61626364'"},
		{"\x00'\"\b\n\r\t\x1A\\", "X'002722080a0d091a5c'"},
	}
	for _, c := range testCases {
		encoded := encodeSQLBytes(nil, []byte(c.arg))
		if string(encoded) != c.expected {
			t.Errorf("Expected %s, but got %s", c.expected, encoded)
		}
	}
}

// Ensure dontEscape is not escaped
func TestDontEscape(t *testing.T) {
	if encodeMap[dontEscape] != dontEscape {
		t.Errorf("Encode fail: %v", encodeMap[dontEscape])
	}
	if decodeMap[dontEscape] != dontEscape {
		t.Errorf("Decode fail: %v", decodeMap[dontEscape])
	}
}
