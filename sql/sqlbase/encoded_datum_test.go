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
	"github.com/cockroachdb/cockroach/util/encoding"
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

// checkEncDatumCmp encodes the given values using the given encodings,
// creates EncDatums from those encodings and verifies the Compare result on
// those encodings. It also checks if the Compare resulted in decoding or not.
func checkEncDatumCmp(
	t *testing.T,
	a *DatumAlloc,
	v1, v2 *EncDatum,
	enc1, enc2 DatumEncoding,
	expectedCmp int,
	requiresDecode bool,
) {
	buf1, err := v1.Encode(a, enc1, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf2, err := v2.Encode(a, enc2, nil)
	if err != nil {
		t.Fatal(err)
	}
	dec1 := &EncDatum{}
	dec1.SetEncoded(v1.Type, enc1, buf1)

	dec2 := &EncDatum{}
	dec2.SetEncoded(v2.Type, enc2, buf2)

	if val, err := dec1.Compare(a, dec2); err != nil {
		t.Fatal(err)
	} else if val != expectedCmp {
		t.Errorf("comparing %s (%s), %s (%s) resulted in %d, expected %d",
			v1, enc1, v2, enc2, val, expectedCmp)
	}

	if requiresDecode {
		if dec1.Datum == nil || dec2.Datum == nil {
			t.Errorf("comparing %s (%s), %s (%s) did not require decoding", v1, enc1, v2, enc2)
		}
	} else {
		if dec1.Datum != nil || dec2.Datum != nil {
			t.Errorf("comparing %s (%s), %s (%s) required decoding", v1, enc1, v2, enc2)
		}
	}
}

func TestEncDatumCompare(t *testing.T) {
	a := &DatumAlloc{}
	rng, _ := randutil.NewPseudoRand()

	for typ := ColumnType_Kind(0); int(typ) < len(ColumnType_Kind_value); typ++ {
		// Generate two datums d1 < d2
		var d1, d2 parser.Datum
		for {
			d1 = RandDatum(rng, typ, false)
			d2 = RandDatum(rng, typ, false)
			if cmp := d1.Compare(d2); cmp < 0 {
				break
			}
		}
		v1 := &EncDatum{}
		v1.SetDatum(typ, d1)
		v2 := &EncDatum{}
		v2.SetDatum(typ, d2)

		if val, err := v1.Compare(a, v2); err != nil {
			t.Fatal(err)
		} else if val != -1 {
			t.Errorf("compare(1, 2) = %d", val)
		}

		asc := DatumEncoding_ASCENDING_KEY
		desc := DatumEncoding_DESCENDING_KEY
		noncmp := DatumEncoding_VALUE

		checkEncDatumCmp(t, a, v1, v2, asc, asc, -1, false)
		checkEncDatumCmp(t, a, v2, v1, asc, asc, +1, false)
		checkEncDatumCmp(t, a, v1, v1, asc, asc, 0, false)
		checkEncDatumCmp(t, a, v2, v2, asc, asc, 0, false)

		checkEncDatumCmp(t, a, v1, v2, desc, desc, -1, false)
		checkEncDatumCmp(t, a, v2, v1, desc, desc, +1, false)
		checkEncDatumCmp(t, a, v1, v1, desc, desc, 0, false)
		checkEncDatumCmp(t, a, v2, v2, desc, desc, 0, false)

		checkEncDatumCmp(t, a, v1, v2, noncmp, noncmp, -1, true)
		checkEncDatumCmp(t, a, v2, v1, desc, noncmp, +1, true)
		checkEncDatumCmp(t, a, v1, v1, asc, desc, 0, true)
		checkEncDatumCmp(t, a, v2, v2, desc, asc, 0, true)
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
			enc[i] = RandDatumEncoding(rng)
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

func TestEncDatumRowCompare(t *testing.T) {
	v := [5]EncDatum{}
	for i := range v {
		v[i].SetDatum(ColumnType_INT, parser.NewDInt(parser.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		row1, row2 EncDatumRow
		ord        ColumnOrdering
		cmp        int
	}{
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{},
			cmp:  0,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{1, desc}},
			cmp:  0,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{0, asc}, {1, desc}},
			cmp:  0,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{2, asc}},
			cmp:  -1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[3]},
			row2: EncDatumRow{v[0], v[1], v[2]},
			ord:  ColumnOrdering{{2, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{2, asc}, {0, asc}, {1, asc}},
			cmp:  -1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{0, asc}, {2, desc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[0], v[1], v[2]},
			row2: EncDatumRow{v[0], v[1], v[3]},
			ord:  ColumnOrdering{{1, desc}, {0, asc}, {2, desc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{0, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{1, desc}, {0, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{1, asc}, {0, asc}},
			cmp:  1,
		},
		{
			row1: EncDatumRow{v[2], v[3]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{1, asc}, {0, desc}},
			cmp:  -1,
		},
		{
			row1: EncDatumRow{v[2], v[3]},
			row2: EncDatumRow{v[1], v[3], v[0]},
			ord:  ColumnOrdering{{0, desc}, {1, asc}},
			cmp:  -1,
		},
	}

	a := &DatumAlloc{}
	for _, c := range testCases {
		cmp, err := c.row1.Compare(a, c.ord, c.row2)
		if err != nil {
			t.Error(err)
		} else if cmp != c.cmp {
			t.Errorf("%s cmp %s ordering %v got %d, expected %d",
				c.row1, c.row2, c.ord, cmp, c.cmp)
		}
	}
}

func TestEncDatumRowAlloc(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	for _, cols := range []int{1, 2, 4, 10, 40, 100} {
		for _, rows := range []int{1, 2, 3, 5, 10, 20} {
			var in, out EncDatumRows
			in = make(EncDatumRows, rows)
			for i := 0; i < rows; i++ {
				in[i] = make(EncDatumRow, cols)
				for j := 0; j < cols; j++ {
					in[i][j] = RandEncDatum(rng)
				}
			}
			var alloc EncDatumRowAlloc
			out = make(EncDatumRows, rows)
			for i := 0; i < rows; i++ {
				out[i] = alloc.CopyRow(in[i])
				if len(out[i]) != cols {
					t.Fatalf("allocated row has invalid length %d (expected %d)", len(out[i]), cols)
				}
			}
			// Do some random appends to make sure the buffers never overlap.
			for x := 0; x < 10; x++ {
				i := rng.Intn(rows)
				j := rng.Intn(rows)
				out[i] = append(out[i], out[j]...)
				out[i] = out[i][:cols]
			}
			for i := 0; i < rows; i++ {
				for j := 0; j < cols; j++ {
					if a, b := in[i][j].Datum, out[i][j].Datum; a.Compare(b) != 0 {
						t.Errorf("copied datum %s doesn't equal original %s", b, a)
					}
				}
			}
		}
	}
}
