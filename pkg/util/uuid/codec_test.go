// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright (C) 2013-2018 by Maxim Bublis <b@codemonkey.ru>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

package uuid

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// codecTestData holds []byte data for a UUID we commonly use for testing.
var codecTestData = []byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

// codecTestUUID is the UUID value corresponding to codecTestData.
var codecTestUUID = UUID{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

func TestFromBytes(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		got, err := FromBytes(codecTestData)
		if err != nil {
			t.Fatal(err)
		}
		if got != codecTestUUID {
			t.Fatalf("FromBytes(%x) = %v, want %v", codecTestData, got, codecTestUUID)
		}
	})
	t.Run("Invalid", func(t *testing.T) {
		var short [][]byte
		for i := 0; i < len(codecTestData); i++ {
			short = append(short, codecTestData[:i])
		}
		var long [][]byte
		for i := 1; i < 17; i++ {
			tmp := append(codecTestData, make([]byte, i)...)
			long = append(long, tmp)
		}
		invalid := append(short, long...)
		for _, b := range invalid {
			got, err := FromBytes(b)
			if err == nil {
				t.Fatalf("FromBytes(%x): want err != nil, got %v", b, got)
			}
		}
	})
}

func TestFromBytesOrNil(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		b := []byte{4, 8, 15, 16, 23, 42}
		got := FromBytesOrNil(b)
		if got != Nil {
			t.Errorf("FromBytesOrNil(%x): got %v, want %v", b, got, Nil)
		}
	})
	t.Run("Valid", func(t *testing.T) {
		got := FromBytesOrNil(codecTestData)
		if got != codecTestUUID {
			t.Errorf("FromBytesOrNil(%x): got %v, want %v", codecTestData, got, codecTestUUID)
		}
	})

}

type fromStringTest struct {
	input   string
	variant string
}

// Run runs the FromString test in a subtest of t, named by fst.variant.
func (fst fromStringTest) Run(t *testing.T) {
	t.Run(fst.variant, func(t *testing.T) {
		got, err := FromString(fst.input)
		if err != nil {
			t.Fatalf("FromString(%q): %v", fst.input, err)
		}
		if want := codecTestUUID; got != want {
			t.Fatalf("FromString(%q) = %v, want %v", fst.input, got, want)
		}
	})
}

// fromStringTests contains UUID variants that are expected to be parsed
// successfully by UnmarshalText / FromString.
//
// variants must be unique across elements of this slice. Please see the
// comment in fuzz.go if you change this slice or add new tests to it.
var fromStringTests = []fromStringTest{
	{
		input:   "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		variant: "Canonical",
	},
	{
		input:   "{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
		variant: "BracedCanonical",
	},
	{
		input:   "{6ba7b8109dad11d180b400c04fd430c8}",
		variant: "BracedHashlike",
	},
	{
		input:   "6ba7b8109dad11d180b400c04fd430c8",
		variant: "Hashlike",
	},
	{
		input:   "urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		variant: "URNCanonical",
	},
	{
		input:   "urn:uuid:6ba7b8109dad11d180b400c04fd430c8",
		variant: "URNHashlike",
	},
}

var invalidFromStringInputs = []string{
	// short
	"6ba7b810-9dad-11d1-80b4-00c04fd430c",
	"6ba7b8109dad11d180b400c04fd430c",

	// invalid hex
	"6ba7b8109dad11d180b400c04fd430q8",

	// long
	"6ba7b810-9dad-11d1-80b4-00c04fd430c8=",
	"6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
	"{6ba7b810-9dad-11d1-80b4-00c04fd430c8}f",
	"6ba7b810-9dad-11d1-80b4-00c04fd430c800c04fd430c8",

	// malformed in other ways
	"ba7b8109dad11d180b400c04fd430c8}",
	"6ba7b8109dad11d180b400c04fd430c86ba7b8109dad11d180b400c04fd430c8",
	"urn:uuid:{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
	"uuid:urn:6ba7b810-9dad-11d1-80b4-00c04fd430c8",
	"uuid:urn:6ba7b8109dad11d180b400c04fd430c8",
	"6ba7b8109-dad-11d1-80b4-00c04fd430c8",
	"6ba7b810-9dad1-1d1-80b4-00c04fd430c8",
	"6ba7b810-9dad-11d18-0b4-00c04fd430c8",
	"6ba7b810-9dad-11d1-80b40-0c04fd430c8",
	"6ba7b810+9dad+11d1+80b4+00c04fd430c8",
	"(6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
	"{6ba7b810-9dad-11d1-80b4-00c04fd430c8>",
	"zba7b810-9dad-11d1-80b4-00c04fd430c8",
	"6ba7b810-9dad11d180b400c04fd430c8",
	"6ba7b8109dad-11d180b400c04fd430c8",
	"6ba7b8109dad11d1-80b400c04fd430c8",
	"6ba7b8109dad11d180b4-00c04fd430c8",
}

func TestFromString(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		for _, fst := range fromStringTests {
			fst.Run(t)
		}
	})
	t.Run("Invalid", func(t *testing.T) {
		for _, s := range invalidFromStringInputs {
			got, err := FromString(s)
			if err == nil {
				t.Errorf("FromString(%q): want err != nil, got %v", s, got)
			}
		}
	})
}

func TestFromStringOrNil(t *testing.T) {
	t.Run("Invalid", func(t *testing.T) {
		s := "bad"
		got := FromStringOrNil(s)
		if got != Nil {
			t.Errorf("FromStringOrNil(%q): got %v, want Nil", s, got)
		}
	})
	t.Run("Valid", func(t *testing.T) {
		s := "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
		got := FromStringOrNil(s)
		if got != codecTestUUID {
			t.Errorf("FromStringOrNil(%q): got %v, want %v", s, got, codecTestUUID)
		}
	})
}

func TestMarshalBinary(t *testing.T) {
	got, err := codecTestUUID.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, codecTestData) {
		t.Fatalf("%v.MarshalBinary() = %x, want %x", codecTestUUID, got, codecTestData)
	}
}

func TestMarshalText(t *testing.T) {
	want := []byte("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	got, err := codecTestUUID.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%v.MarshalText(): got %s, want %s", codecTestUUID, got, want)
	}
}

func TestDecodePlainWithWrongLength(t *testing.T) {
	arg := []byte{'4', '2'}

	u := UUID{}

	if u.decodePlain(arg) == nil {
		t.Errorf("%v.decodePlain(%q): should return error, but it did not", u, arg)
	}
}

var stringBenchmarkSink string

func BenchmarkString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stringBenchmarkSink = codecTestUUID.String()
	}
}

func BenchmarkFromBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Must(FromBytes(codecTestData))
	}
}

func BenchmarkFromString(b *testing.B) {
	b.Run("canonical", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Must(FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
		}
	})
	b.Run("urn", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Must(FromString("urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8"))
		}
	})
	b.Run("braced", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Must(FromString("{6ba7b810-9dad-11d1-80b4-00c04fd430c8}"))
		}
	})
}

func BenchmarkMarshalBinary(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := codecTestUUID.MarshalBinary(); err != nil {
			panic(err)
		}
	}
}

func BenchmarkMarshalText(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := codecTestUUID.MarshalText(); err != nil {
			panic(err)
		}
	}
}

var seedFuzzCorpus = flag.Bool("seed_fuzz_corpus", false, "seed fuzz test corpus")

func TestSeedFuzzCorpus(t *testing.T) {
	// flag.Parse() is called for us by the test binary.
	if !*seedFuzzCorpus {
		t.Skip("seeding fuzz test corpus only on demand")
	}
	corpusDir := filepath.Join(".", "testdata", "corpus")
	writeSeedFile := func(name, data string) error {
		path := filepath.Join(corpusDir, name)
		return ioutil.WriteFile(path, []byte(data), os.ModePerm)
	}
	for _, fst := range fromStringTests {
		name := "seed_valid_" + fst.variant
		if err := writeSeedFile(name, fst.input); err != nil {
			t.Fatal(err)
		}
	}
	for i, s := range invalidFromStringInputs {
		name := fmt.Sprintf("seed_invalid_%d", i)
		if err := writeSeedFile(name, s); err != nil {
			t.Fatal(err)
		}
	}
}
