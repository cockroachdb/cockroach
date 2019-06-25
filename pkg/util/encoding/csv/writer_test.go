// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

package csv

import (
	"bytes"
	"errors"
	"testing"
)

var writeTests = []struct {
	Input   [][]string
	Output  string
	UseCRLF bool
}{
	{Input: [][]string{{"abc"}}, Output: "abc\n"},
	{Input: [][]string{{"abc"}}, Output: "abc\r\n", UseCRLF: true},
	{Input: [][]string{{`"abc"`}}, Output: `"""abc"""` + "\n"},
	{Input: [][]string{{`a"b`}}, Output: `"a""b"` + "\n"},
	{Input: [][]string{{`"a"b"`}}, Output: `"""a""b"""` + "\n"},
	{Input: [][]string{{" abc"}}, Output: `" abc"` + "\n"},
	{Input: [][]string{{"abc,def"}}, Output: `"abc,def"` + "\n"},
	{Input: [][]string{{"abc", "def"}}, Output: "abc,def\n"},
	{Input: [][]string{{"abc"}, {"def"}}, Output: "abc\ndef\n"},
	{Input: [][]string{{"abc\ndef"}}, Output: "\"abc\ndef\"\n"},
	{Input: [][]string{{"abc\ndef"}}, Output: "\"abc\r\ndef\"\r\n", UseCRLF: true},
	{Input: [][]string{{"abc\rdef"}}, Output: "\"abcdef\"\r\n", UseCRLF: true},
	{Input: [][]string{{"abc\rdef"}}, Output: "\"abc\rdef\"\n", UseCRLF: false},
	{Input: [][]string{{""}}, Output: "\n"},
	{Input: [][]string{{"", ""}}, Output: ",\n"},
	{Input: [][]string{{"", "", ""}}, Output: ",,\n"},
	{Input: [][]string{{"", "", "a"}}, Output: ",,a\n"},
	{Input: [][]string{{"", "a", ""}}, Output: ",a,\n"},
	{Input: [][]string{{"", "a", "a"}}, Output: ",a,a\n"},
	{Input: [][]string{{"a", "", ""}}, Output: "a,,\n"},
	{Input: [][]string{{"a", "", "a"}}, Output: "a,,a\n"},
	{Input: [][]string{{"a", "a", ""}}, Output: "a,a,\n"},
	{Input: [][]string{{"a", "a", "a"}}, Output: "a,a,a\n"},
	{Input: [][]string{{`\.`}}, Output: "\"\\.\"\n"},
}

func TestWrite(t *testing.T) {
	for n, tt := range writeTests {
		b := &bytes.Buffer{}
		f := NewWriter(b)
		f.UseCRLF = tt.UseCRLF
		err := f.WriteAll(tt.Input)
		if err != nil {
			t.Errorf("Unexpected error: %s\n", err)
		}
		out := b.String()
		if out != tt.Output {
			t.Errorf("#%d: out=%q want %q", n, out, tt.Output)
		}
	}
}

type errorWriter struct{}

func (e errorWriter) Write(b []byte) (int, error) {
	return 0, errors.New("Test")
}

func TestError(t *testing.T) {
	b := &bytes.Buffer{}
	f := NewWriter(b)
	if err := f.Write([]string{"abc"}); err != nil {
		t.Errorf("Unexpected error: %s\n", err)
	}
	f.Flush()
	err := f.Error()

	if err != nil {
		t.Errorf("Unexpected error: %s\n", err)
	}

	f = NewWriter(errorWriter{})
	if err := f.Write([]string{"abc"}); err != nil {
		t.Errorf("Unexpected error: %s\n", err)
	}
	f.Flush()
	err = f.Error()

	if err == nil {
		t.Error("Error should not be nil")
	}
}
