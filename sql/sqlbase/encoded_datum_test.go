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
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/randutil"
)

func TestEncDatum(t *testing.T) {
	a := &DatumAlloc{}
	x := &EncDatum{}
	if !x.IsUnset() {
		t.Errorf("empty EncDatum should be unset")
	}

	if _, ok := x.Encoding(); ok {
		t.Errorf("empty EncDatum has an encoding")
	}

	x.SetDatum(ColumnType_INT, parser.NewDInt(5))
	if x.IsUnset() {
		t.Errorf("unset after SetDatum()")
	}

	encoded, err := x.Encode(a, DatumEncoding_ASCENDING_KEY, nil)
	if err != nil {
		t.Fatal(err)
	}

	y := &EncDatum{}
	y.SetEncoded(ColumnType_INT, DatumEncoding_ASCENDING_KEY, encoded)

	if y.IsUnset() {
		t.Errorf("unset after SetEncoded()")
	}
	if enc, ok := y.Encoding(); !ok {
		t.Error("no encoding after SetEncoded")
	} else if enc != DatumEncoding_ASCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	err = y.Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(x.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}

	enc2, err := y.Encode(a, DatumEncoding_DESCENDING_KEY, nil)
	if err != nil {
		t.Fatal(err)
	}
	// y's encoding should not change.
	if enc, ok := y.Encoding(); !ok {
		t.Error("no encoding")
	} else if enc != DatumEncoding_ASCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	x.SetEncoded(ColumnType_INT, DatumEncoding_DESCENDING_KEY, enc2)
	if enc, ok := x.Encoding(); !ok {
		t.Error("no encoding")
	} else if enc != DatumEncoding_DESCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	err = x.Decode(a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(x.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}
}

func TestEncDatumFromBuffer(t *testing.T) {
	var alloc DatumAlloc
	rng, _ := randutil.NewPseudoRand()
	for test := 0; test < 20; test++ {
		var err error
		// Generate a set of random datums.
		ed := make([]EncDatum, 1+rng.Intn(10))
		for i := range ed {
			ed[i] = RandEncDatum(rng)
		}
		// Encode them in a single buffer.
		var buf []byte
		enc := make([]DatumEncoding, len(ed))
		for i := range ed {
			enc[i] = DatumEncoding(rng.Intn(len(DatumEncoding_value)))
			buf, err = ed[i].Encode(&alloc, enc[i], buf)
			if err != nil {
				t.Fatal(err)
			}
		}
		// Decode the buffer.
		b := buf
		for i := range ed {
			if len(b) == 0 {
				t.Fatal("buffer ended early")
			}
			var decoded EncDatum
			b, err = decoded.SetFromBuffer(ed[i].Type, enc[i], b)
			if err != nil {
				t.Fatal(err)
			}
			err = decoded.Decode(&alloc)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.Datum.Compare(ed[i].Datum) != 0 {
				t.Errorf("decoded datum %s doesn't equal original %s", decoded.Datum, ed[i].Datum)
			}
		}
		if len(b) != 0 {
			t.Errorf("%d leftover bytes", len(b))
		}
	}
}
