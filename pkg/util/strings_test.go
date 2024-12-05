// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/stretchr/testify/require"
)

func TestGetSingleRune(t *testing.T) {
	tests := []struct {
		s        string
		expected rune
		err      bool
	}{
		{"a", 'a', false},
		{"", 0, false},
		{"🐛"[:1], 0, true},
		{"aa", 'a', true},
	}
	for _, tc := range tests {
		t.Run(tc.s, func(t *testing.T) {
			got, err := GetSingleRune(tc.s)
			if (err != nil) != tc.err {
				t.Fatalf("got unexpected err: %v", err)
			}
			if tc.expected != got {
				t.Fatalf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestToLowerSingleByte(t *testing.T) {
	testCases := []struct {
		from     byte
		expected byte
	}{
		{'a', 'a'},
		{'A', 'a'},
		{'c', 'c'},
		{'C', 'c'},
		{'Z', 'z'},
		{'1', '1'},
		{'\n', '\n'},
	}

	for _, tc := range testCases {
		t.Run(string(tc.from), func(t *testing.T) {
			ret := ToLowerSingleByte(tc.from)
			require.Equal(t, tc.expected, ret)
		})
	}
}

func TestTruncateString(t *testing.T) {
	testCases := []struct {
		s string
		// res stores the expected results for maxRunes=0,1,2,3,etc.
		res []string
	}{
		{"", []string{"", ""}},
		{"abcd", []string{"", "a", "ab", "abc", "abcd", "abcd", "abcd"}},
		{"🐛🏠", []string{"", "🐛", "🐛🏠", "🐛🏠", "🐛🏠"}},
		{"a🐛b🏠c", []string{"", "a", "a🐛", "a🐛b", "a🐛b🏠", "a🐛b🏠c", "a🐛b🏠c"}},
		{
			// Test with an invalid UTF-8 sequence.
			"\xf0\x90\x28\xbc",
			[]string{"", "\xf0", "\xf0\x90", "\xf0\x90\x28", "\xf0\x90\x28\xbc", "\xf0\x90\x28\xbc"},
		},
	}

	for _, tc := range testCases {
		for i := range tc.res {
			if r := TruncateString(tc.s, i); r != tc.res[i] {
				t.Errorf("TruncateString(\"%q\", %d) = \"%q\"; expected \"%q\"", tc.s, i, r, tc.res[i])
			}
		}
	}
}

func TestRemoveTrailingSpaces(t *testing.T) {
	for _, tc := range []struct{ input, expected string }{
		{
			input:    "",
			expected: "\n",
		},
		{
			input:    "line 1  \nline 2   \nline 3 \n\n",
			expected: "line 1\nline 2\nline 3\n",
		},
		{
			input:    " line 1  \nline 2   \nline 3  ",
			expected: " line 1\nline 2\nline 3\n",
		},
		{
			input:    "line 1\n\n  \nline 2   \nline 3",
			expected: "line 1\n\n\nline 2\nline 3\n",
		},
	} {
		output := RemoveTrailingSpaces(tc.input)
		if output != tc.expected {
			t.Errorf("expected:\n%s\ngot:\n%s", tc.expected, output)
		}
	}
}

func TestStringListBuilder(t *testing.T) {
	var buf bytes.Buffer
	var b StringListBuilder
	expect := func(exp string) {
		t.Helper()
		if buf.String() != exp {
			t.Errorf("expected `%s`, got `%s`", exp, buf.String())
		}
		buf.Reset()
	}

	b = MakeStringListBuilder("(", ",", ")")
	b.Finish(&buf)
	expect("")

	b = MakeStringListBuilder("(", ",", ")")
	b.Add(&buf, "one")
	b.Finish(&buf)
	expect("(one)")

	b = MakeStringListBuilder("[", ", ", "]")
	b.Add(&buf, "one")
	b.Addf(&buf, "%s", "two")
	b.Finish(&buf)
	expect("[one, two]")
}

type alloc struct {
	s []string
}

const defaultAllocSize = 16

//go:noinline
func (a *alloc) newString() *string {
	buf := &a.s
	if len(*buf) == 0 {
		*buf = make([]string, defaultAllocSize)
	}
	r := &(*buf)[0]
	*buf = (*buf)[1:]
	return r
}

//go:noinline
func (a *alloc) newDString(v dstring) *dstring {
	r := (*dstring)(a.newString())
	*r = v
	return r
}

type datum interface {
	marker()
}

type dstring string

func (d *dstring) marker() {}

//go:noinline
func getDatum(i, j int) []byte {
	return []byte(fmt.Sprintf("%d, %d", i, j))
}

func TestAlloc(t *testing.T) {
	//f, err := os.Create("/Users/yuzefovich/go/src/github.com/cockroachdb/cockroach/test.pprof")
	f, err := os.Create(filepath.Join(datapathutils.DebuggableTempDir(), "test.pprof"))
	require.NoError(t, err)
	defer f.Close()

	var a alloc
	samples := make([]datum, 120000)
	for i := range samples {
		for j := 0; j < defaultAllocSize; j++ {
			d := getDatum(i, j)
			samples[i] = a.newDString(dstring(d))
		}
	}

	// Uncomment this part to only keep truly needed references. With this part
	// commented out we see about 120k x <multiple in [8, 16] range> live
	// objects even though samples slice directly only references 120k objects
	// (perhaps that times two for interface boxing).
	//
	//for i := range samples {
	//	v := samples[i].(*dstring)
	//	samples[i] = a.newDString(*v)
	//}

	runtime.GC()
	require.NoError(t, pprof.WriteHeapProfile(f))
	// Make sure that samples aren't GC'ed.
	for i := range samples {
		samples[i] = samples[0]
	}
}
