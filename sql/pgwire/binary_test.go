// Copyright 2016 The Cockroach Authors.
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
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package pgwire

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pq/oid"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/randutil"
)

type binaryDecimalTest struct {
	In     string
	Expect []byte
}

func TestBinaryDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var tests []binaryDecimalTest

	f, err := os.Open(filepath.Join("testdata", "decimal_test.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(f).Decode(&tests); err != nil {
		t.Fatal(err)
	}
	f.Close()

	buf := writeBuffer{bytecount: metric.NewCounter(metric.Metadata{})}
	dec := new(parser.DDecimal)
	for _, test := range tests {
		buf.wrapped.Reset()

		if _, ok := dec.SetString(test.In); !ok {
			t.Fatalf("could not set %q on decimal", test.In)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("%q: %s", test.In, r)
					panic(r)
				}
			}()
			buf.writeBinaryDatum(dec, time.UTC)
			if buf.err != nil {
				t.Fatal(buf.err)
			}
			if got := buf.wrapped.Bytes(); !bytes.Equal(got, test.Expect) {
				t.Errorf("%q:\n\t%v found,\n\t%v expected", test.In, got, test.Expect)
			} else if datum, err := decodeOidDatum(oid.T_numeric, formatBinary, got[4:]); err != nil {
				t.Fatalf("unable to decode %v: %s", got[4:], err)
			} else if dec.Compare(datum) != 0 {
				t.Errorf("expected %s, got %s", dec, datum)
			}
		}()
	}
}

var generateDecimalCmd = flag.String("generate-decimal", "", "generate-decimal command invocation")

func TestRandomBinaryDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if *generateDecimalCmd == "" {
		t.Skip("disabled")
	}

	fields := strings.Fields(*generateDecimalCmd)
	if len(fields) < 1 {
		t.Fatal("expected generate-decimal arguments")
	}
	name, args := fields[0], fields[1:]
	var tests []binaryDecimalTest

	randutil.SeedForTests()

	for {
		var b bytes.Buffer
		for n := rand.Intn(50); n >= 0; n-- {
			fmt.Fprint(&b, rand.Intn(10))
		}
		v := b.String()
		{
			z := strings.Repeat("0", rand.Intn(10))
			pos := rand.Intn(len(v) + 1)
			v = v[:pos] + z + v[pos:]
		}
		if rand.Intn(2) == 0 {
			pos := rand.Intn(len(v) + 1)
			v = v[:pos] + "." + v[pos:]
		}
		cmdargs := append(args, `"`+v+`"`)
		out, err := exec.Command(name, cmdargs...).CombinedOutput()
		if err != nil {
			t.Log(string(out))
			t.Fatal(err)
		}
		if err := json.Unmarshal(out, &tests); err != nil {
			t.Fatal(err)
		}
		if len(tests) != 1 {
			t.Fatal("expected 1 test")
		}
		test := tests[0]

		buf := writeBuffer{bytecount: metric.NewCounter(metric.Metadata{})}
		dec := new(parser.DDecimal)

		if _, ok := dec.SetString(test.In); !ok {
			t.Fatalf("could not set %q on decimal", test.In)
		}
		buf.writeBinaryDatum(dec, time.UTC)
		if buf.err != nil {
			t.Fatal(buf.err)
		}
		if got := buf.wrapped.Bytes(); !bytes.Equal(got, test.Expect) {
			t.Errorf("%q:\n\t%v found,\n\t%v expected", test.In, got, test.Expect)
		} else if datum, err := decodeOidDatum(oid.T_numeric, formatBinary, got[4:]); err != nil {
			t.Errorf("%q: unable to decode %v: %s", test.In, got[4:], err)
		} else if dec.Compare(datum) != 0 {
			t.Errorf("%q: expected %s, got %s", test.In, dec, datum)
		}
	}
}
