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

package pgwire

import (
	"bytes"
	"context"
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
)

type binaryTest struct {
	In     string
	Expect []byte
}

func testBinaryDatumType(t *testing.T, typ string, datumConstructor func(val string) tree.Datum) {
	var tests []binaryTest

	f, err := os.Open(filepath.Join("testdata", fmt.Sprintf("%s_test.json", typ)))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(f).Decode(&tests); err != nil {
		t.Fatal(err)
	}
	f.Close()

	buf := writeBuffer{bytecount: metric.NewCounter(metric.Metadata{})}
	for _, test := range tests {
		buf.wrapped.Reset()

		d := datumConstructor(test.In)
		oid := d.ResolvedType().Oid()
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("%q: %s", test.In, r)
					panic(r)
				}
			}()
			buf.writeBinaryDatum(context.Background(), d, time.UTC)
			if buf.err != nil {
				t.Fatal(buf.err)
			}
			evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			if got := buf.wrapped.Bytes(); !bytes.Equal(got, test.Expect) {
				t.Errorf("%q:\n\t%v found,\n\t%v expected", test.In, got, test.Expect)
			} else if datum, err := pgwirebase.DecodeOidDatum(
				oid, pgwirebase.FormatBinary, got[4:],
			); err != nil {
				t.Fatalf("unable to decode %v: %s", got[4:], err)
			} else if d.Compare(evalCtx, datum) != 0 {
				t.Errorf("expected %s, got %s", d, datum)
			}
		}()
	}
}

func TestBinaryDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "decimal", func(val string) tree.Datum {
		dec := new(tree.DDecimal)
		if err := dec.SetString(val); err != nil {
			t.Fatalf("could not set %q on decimal", val)
		}
		return dec
	})
}

func TestBinaryTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "timestamp", func(val string) tree.Datum {
		ts, err := tree.ParseDTimestamp(val, time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		return ts
	})
}

func TestBinaryTimestampTZ(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "timestamptz", func(val string) tree.Datum {
		tstz, err := tree.ParseDTimestampTZ(val, time.UTC, time.Microsecond)
		if err != nil {
			t.Fatal(err)
		}
		return tstz
	})
}

func TestBinaryDate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "date", func(val string) tree.Datum {
		d, err := tree.ParseDDate(val, time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		return d
	})
}

func TestBinaryTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "time", func(val string) tree.Datum {
		d, err := tree.ParseDTime(val)
		if err != nil {
			t.Fatal(err)
		}
		return d
	})
}

func TestBinaryUuid(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "uuid", func(val string) tree.Datum {
		u, err := tree.ParseDUuidFromString(val)
		if err != nil {
			t.Fatal(err)
		}
		return u
	})
}

func TestBinaryInet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "inet", func(val string) tree.Datum {
		ipAddr, err := tree.ParseDIPAddrFromINetString(val)
		if err != nil {
			t.Fatal(err)
		}
		return ipAddr
	})
}

func TestBinaryJSONB(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "jsonb", func(val string) tree.Datum {
		j, err := tree.ParseDJSON(val)
		if err != nil {
			t.Fatal(err)
		}
		return j
	})
}

func TestBinaryUUIDArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "uuid_array", func(val string) tree.Datum {
		ary, err := tree.ParseDArrayFromString(tree.NewTestingEvalContext(nil), val, coltypes.UUID)
		if err != nil {
			t.Fatal(err)
		}
		return ary
	})
}

func TestBinaryDecimalArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testBinaryDatumType(t, "decimal_array", func(val string) tree.Datum {
		ary, err := tree.ParseDArrayFromString(tree.NewTestingEvalContext(nil), val, coltypes.Decimal)
		if err != nil {
			t.Fatal(err)
		}
		return ary
	})
}

func TestBinaryIntArray(t *testing.T) {
	defer leaktest.AfterTest(t)()
	buf := writeBuffer{bytecount: metric.NewCounter(metric.Metadata{})}
	d := tree.NewDArray(types.Int)
	for i := 0; i < 10; i++ {
		if err := d.Append(tree.NewDInt(tree.DInt(i))); err != nil {
			t.Fatal(err)
		}
	}
	buf.writeBinaryDatum(context.Background(), d, time.UTC)

	b := buf.wrapped.Bytes()

	got, err := pgwirebase.DecodeOidDatum(oid.T__int8, pgwirebase.FormatBinary, b[4:])
	if err != nil {
		t.Fatal(err)
	}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	if got.Compare(evalCtx, d) != 0 {
		t.Fatalf("expected %s, got %s", d, got)
	}
}

var generateBinaryCmd = flag.String("generate-binary", "", "generate-binary command invocation")

func TestRandomBinaryDecimal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if *generateBinaryCmd == "" {
		t.Skip("disabled")
	}

	fields := strings.Fields(*generateBinaryCmd)
	if len(fields) < 1 {
		t.Fatal("expected generate-binary arguments")
	}
	name, args := fields[0], fields[1:]
	var tests []binaryTest

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
		dec := new(tree.DDecimal)

		if err := dec.SetString(test.In); err != nil {
			t.Fatalf("could not set %q on decimal", test.In)
		}
		buf.writeBinaryDatum(context.Background(), dec, time.UTC)
		if buf.err != nil {
			t.Fatal(buf.err)
		}
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		if got := buf.wrapped.Bytes(); !bytes.Equal(got, test.Expect) {
			t.Errorf("%q:\n\t%v found,\n\t%v expected", test.In, got, test.Expect)
		} else if datum, err := pgwirebase.DecodeOidDatum(
			oid.T_numeric, pgwirebase.FormatBinary, got[4:],
		); err != nil {
			t.Errorf("%q: unable to decode %v: %s", test.In, got[4:], err)
		} else if dec.Compare(evalCtx, datum) != 0 {
			t.Errorf("%q: expected %s, got %s", test.In, dec, datum)
		}
		evalCtx.Stop(context.Background())
	}
}
