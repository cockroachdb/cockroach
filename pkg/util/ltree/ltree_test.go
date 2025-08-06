// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ltree

import (
	"strings"
	"testing"
)

func TestParseLTree(t *testing.T) {
	tests := []struct {
		input    string
		expected string
		wantErr  bool
	}{
		{
			input:    "",
			expected: "",
		},
		{
			input:    "a",
			expected: "a",
		},
		{
			input:    "a.b.c",
			expected: "a.b.c",
		},
		{
			input:    "a-0.B_9.z",
			expected: "a-0.B_9.z",
		},
		{
			input:    "123.456",
			expected: "123.456",
		},
		{
			input:    "Top.Middle.bottom",
			expected: "Top.Middle.bottom",
		},
		{
			input:    "hello_world.test-case",
			expected: "hello_world.test-case",
		},
		{
			input:   "hello world",
			wantErr: true,
		},
		{
			input:   "hello..world",
			wantErr: true,
		},
		{
			input:   "hello@world",
			wantErr: true,
		},
		{
			input:   "test.৩.path",
			wantErr: true,
		},
		{
			input:   strings.Repeat("a", maxLabelLength+1),
			wantErr: true,
		},
		{
			input:   strings.Repeat("a.", maxNumOfLabels) + "a",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		got, err := ParseLTree(tc.input)
		if tc.wantErr && err == nil {
			t.Errorf("ParseLTree(%q) expected error, got nil", tc.input)
			continue
		}
		if tc.expected != got.String() {
			t.Errorf("expected %q, got: %q\n", tc.expected, got.String())
		}
	}
}

func TestByteSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{
			input:    "",
			expected: 0,
		},
		{
			input:    "a",
			expected: 1,
		},
		{
			input:    "aaa",
			expected: 3,
		},
		{
			input:    "a.b.c.d",
			expected: 7, // 4 labels + 3 separators
		},
		{
			input:    "a-0.B_9.z",
			expected: 9, // 3 labels (3,3,1) + 2 separators
		},
	}
	for _, tc := range tests {
		lt, err := ParseLTree(tc.input)
		if err != nil {
			t.Fatalf("unexpected error parsing input %q: %v", tc.input, err)
		}
		if got := lt.ByteSize(); got != tc.expected {
			t.Errorf("expected byte size %d, got %d", tc.expected, got)
		}
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		a        string
		b        string
		expected int
	}{
		{"", "", 0},
		{"a", "a", 0},
		{"a", "b", -1},
		{"b", "a", 1},
		{"a.b", "a.b", 0},
		{"a.b", "a.c", -1},
		{"a.c", "a.b", 1},
		{"a.b.c", "a.b.c.d", -1},
		{"a.b.c.d", "a.b.c", 1},
		{"a.b", "c.d.e", -1},
	}

	for _, tc := range tests {
		a, err := ParseLTree(tc.a)
		if err != nil {
			t.Fatalf("unexpected error parsing %q: %v", tc.a, err)
		}
		b, err := ParseLTree(tc.b)
		if err != nil {
			t.Fatalf("unexpected error parsing %q: %v", tc.b, err)
		}

		if got := a.Compare(b); got != tc.expected {
			t.Errorf("expected %d, got %d", tc.expected, got)
		}
	}
}

func TestPrev(t *testing.T) {
	tests := []struct {
		input        string
		expected     string
		doesNotExist bool
	}{
		{
			input:        "",
			expected:     "",
			doesNotExist: true,
		},
		{
			input:    "b",
			expected: "a",
		},
		{
			input:    "a",
			expected: "_",
		},
		{
			input:    "_",
			expected: "Z",
		},
		{
			input:    "Z",
			expected: "Y",
		},
		{
			input:    "A",
			expected: "9",
		},
		{
			input:    "9",
			expected: "8",
		},
		{
			input:    "0",
			expected: "-",
		},
		{
			input:    "-",
			expected: "",
		},
		{
			input:    "ab",
			expected: "aa",
		},
		{
			input:    "a-",
			expected: "a",
		},
		{
			input:    "--",
			expected: "-",
		},
		{
			input:    "A.B",
			expected: "A.A",
		},
		{
			input:    "A.A",
			expected: "A.9",
		},
		{
			input:    "A.-",
			expected: "A",
		},
		{
			input:    "a.b.c",
			expected: "a.b.b",
		},
		{
			input:    "-.-.a",
			expected: "-.-._",
		},
		{
			input:    "-.-.-",
			expected: "-.-",
		},
		{
			input:    "abc.def",
			expected: "abc.dee",
		},
		{
			input:    "A",
			expected: "9",
		},
	}

	for _, tc := range tests {
		input, err := ParseLTree(tc.input)
		if err != nil {
			t.Fatalf("unexpected error parsing input %q: %v", tc.input, err)
		}

		got, ok := input.Prev()

		if ok && tc.doesNotExist {
			t.Errorf("expected result to not exist, but got %q", got.String())
		}

		if ok && tc.expected != got.String() {
			t.Errorf("expected %q, got %q", tc.expected, got.String())
		}
	}
}
