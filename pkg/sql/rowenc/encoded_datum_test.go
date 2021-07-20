// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowenc_test

import (
	"context"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestEncDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := &rowenc.DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	v := rowenc.EncDatum{}
	if !v.IsUnset() {
		t.Errorf("empty rowenc.EncDatum should be unset")
	}

	if _, ok := v.Encoding(); ok {
		t.Errorf("empty rowenc.EncDatum has an encoding")
	}

	x := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(5))

	check := func(x rowenc.EncDatum) {
		if x.IsUnset() {
			t.Errorf("unset after DatumToEncDatum()")
		}
		if x.IsNull() {
			t.Errorf("null after DatumToEncDatum()")
		}
		if val, err := x.GetInt(); err != nil {
			t.Fatal(err)
		} else if val != 5 {
			t.Errorf("GetInt returned %d", val)
		}
	}
	check(x)

	encoded, err := x.Encode(types.Int, a, descpb.DatumEncoding_ASCENDING_KEY, nil)
	if err != nil {
		t.Fatal(err)
	}

	y := rowenc.EncDatumFromEncoded(descpb.DatumEncoding_ASCENDING_KEY, encoded)
	check(y)

	if enc, ok := y.Encoding(); !ok {
		t.Error("no encoding after rowenc.EncDatumFromEncoded")
	} else if enc != descpb.DatumEncoding_ASCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	err = y.EnsureDecoded(types.Int, a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(evalCtx, x.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}

	enc2, err := y.Encode(types.Int, a, descpb.DatumEncoding_DESCENDING_KEY, nil)
	if err != nil {
		t.Fatal(err)
	}
	// y's encoding should not change.
	if enc, ok := y.Encoding(); !ok {
		t.Error("no encoding")
	} else if enc != descpb.DatumEncoding_ASCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	z := rowenc.EncDatumFromEncoded(descpb.DatumEncoding_DESCENDING_KEY, enc2)
	if enc, ok := z.Encoding(); !ok {
		t.Error("no encoding")
	} else if enc != descpb.DatumEncoding_DESCENDING_KEY {
		t.Errorf("invalid encoding %d", enc)
	}
	check(z)

	err = z.EnsureDecoded(types.Int, a)
	if err != nil {
		t.Fatal(err)
	}
	if cmp := y.Datum.Compare(evalCtx, z.Datum); cmp != 0 {
		t.Errorf("Datums should be equal, cmp = %d", cmp)
	}
	y.UnsetDatum()
	if !y.IsUnset() {
		t.Error("not unset after UnsetDatum()")
	}
}

func columnTypeCompatibleWithEncoding(typ *types.T, enc descpb.DatumEncoding) bool {
	return enc == descpb.DatumEncoding_VALUE || colinfo.ColumnTypeIsIndexable(typ)
}

func TestEncDatumNull(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Verify DNull is null.
	n := rowenc.DatumToEncDatum(types.Int, tree.DNull)
	if !n.IsNull() {
		t.Error("DNull not null")
	}

	var alloc rowenc.DatumAlloc
	rng, _ := randutil.NewPseudoRand()

	// Generate random EncDatums (some of which are null), and verify that a datum
	// created from its encoding has the same IsNull() value.
	for cases := 0; cases < 100; cases++ {
		a, typ := randgen.RandEncDatum(rng)
		for enc := range descpb.DatumEncoding_name {
			if !columnTypeCompatibleWithEncoding(typ, descpb.DatumEncoding(enc)) {
				continue
			}
			encoded, err := a.Encode(typ, &alloc, descpb.DatumEncoding(enc), nil)
			if err != nil {
				t.Fatal(err)
			}
			b := rowenc.EncDatumFromEncoded(descpb.DatumEncoding(enc), encoded)
			if a.IsNull() != b.IsNull() {
				t.Errorf("before: %s (null=%t) after: %s (null=%t)",
					a.String(types.Int), a.IsNull(), b.String(types.Int), b.IsNull())
			}
		}
	}

}

// checkEncDatumCmp encodes the given values using the given encodings,
// creates EncDatums from those encodings and verifies the Compare result on
// those encodings. It also checks if the Compare resulted in decoding or not.
func checkEncDatumCmp(
	t *testing.T,
	a *rowenc.DatumAlloc,
	typ *types.T,
	v1, v2 *rowenc.EncDatum,
	enc1, enc2 descpb.DatumEncoding,
	expectedCmp int,
	requiresDecode bool,
) {
	buf1, err := v1.Encode(typ, a, enc1, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf2, err := v2.Encode(typ, a, enc2, nil)
	if err != nil {
		t.Fatal(err)
	}
	dec1 := rowenc.EncDatumFromEncoded(enc1, buf1)

	dec2 := rowenc.EncDatumFromEncoded(enc2, buf2)

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	if val, err := dec1.Compare(typ, a, evalCtx, &dec2); err != nil {
		t.Fatal(err)
	} else if val != expectedCmp {
		t.Errorf("comparing %s (%s), %s (%s) resulted in %d, expected %d",
			v1.String(typ), enc1, v2.String(typ), enc2, val, expectedCmp,
		)
	}

	if requiresDecode {
		if dec1.Datum == nil || dec2.Datum == nil {
			t.Errorf(
				"comparing %s (%s), %s (%s) did not require decoding",
				v1.String(typ), enc1, v2.String(typ), enc2,
			)
		}
	} else {
		if dec1.Datum != nil || dec2.Datum != nil {
			t.Errorf(
				"comparing %s (%s), %s (%s) required decoding",
				v1.String(typ), enc1, v2.String(typ), enc2,
			)
		}
	}
}

func TestEncDatumCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()

	a := &rowenc.DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()

	for _, typ := range types.OidToType {
		switch typ.Family() {
		case types.AnyFamily, types.UnknownFamily, types.ArrayFamily, types.JsonFamily, types.TupleFamily:
			continue
		case types.CollatedStringFamily:
			typ = types.MakeCollatedString(types.String, *randgen.RandCollationLocale(rng))
		}

		// Generate two datums d1 < d2
		var d1, d2 tree.Datum
		for {
			d1 = randgen.RandDatum(rng, typ, false)
			d2 = randgen.RandDatum(rng, typ, false)
			if cmp := d1.Compare(evalCtx, d2); cmp < 0 {
				break
			}
		}
		v1 := rowenc.DatumToEncDatum(typ, d1)
		v2 := rowenc.DatumToEncDatum(typ, d2)

		if val, err := v1.Compare(typ, a, evalCtx, &v2); err != nil {
			t.Fatal(err)
		} else if val != -1 {
			t.Errorf("compare(1, 2) = %d", val)
		}

		asc := descpb.DatumEncoding_ASCENDING_KEY
		desc := descpb.DatumEncoding_DESCENDING_KEY
		noncmp := descpb.DatumEncoding_VALUE

		checkEncDatumCmp(t, a, typ, &v1, &v2, asc, asc, -1, false)
		checkEncDatumCmp(t, a, typ, &v2, &v1, asc, asc, +1, false)
		checkEncDatumCmp(t, a, typ, &v1, &v1, asc, asc, 0, false)
		checkEncDatumCmp(t, a, typ, &v2, &v2, asc, asc, 0, false)

		checkEncDatumCmp(t, a, typ, &v1, &v2, desc, desc, -1, false)
		checkEncDatumCmp(t, a, typ, &v2, &v1, desc, desc, +1, false)
		checkEncDatumCmp(t, a, typ, &v1, &v1, desc, desc, 0, false)
		checkEncDatumCmp(t, a, typ, &v2, &v2, desc, desc, 0, false)

		// These cases require decoding. Data with a composite key encoding cannot
		// be decoded from their key part alone.
		if !colinfo.HasCompositeKeyEncoding(typ) {
			checkEncDatumCmp(t, a, typ, &v1, &v2, noncmp, noncmp, -1, true)
			checkEncDatumCmp(t, a, typ, &v2, &v1, desc, noncmp, +1, true)
			checkEncDatumCmp(t, a, typ, &v1, &v1, asc, desc, 0, true)
			checkEncDatumCmp(t, a, typ, &v2, &v2, desc, asc, 0, true)
		}
	}
}

func TestEncDatumFromBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var alloc rowenc.DatumAlloc
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()
	for test := 0; test < 20; test++ {
		var err error
		// Generate a set of random datums.
		ed := make([]rowenc.EncDatum, 1+rng.Intn(10))
		typs := make([]*types.T, len(ed))
		for i := range ed {
			d, t := randgen.RandEncDatum(rng)
			ed[i], typs[i] = d, t
		}
		// Encode them in a single buffer.
		var buf []byte
		enc := make([]descpb.DatumEncoding, len(ed))
		for i := range ed {
			if colinfo.HasCompositeKeyEncoding(typs[i]) {
				// There's no way to reconstruct data from the key part of a composite
				// encoding.
				enc[i] = descpb.DatumEncoding_VALUE
			} else {
				enc[i] = randgen.RandDatumEncoding(rng)
				for !columnTypeCompatibleWithEncoding(typs[i], enc[i]) {
					enc[i] = randgen.RandDatumEncoding(rng)
				}
			}
			buf, err = ed[i].Encode(typs[i], &alloc, enc[i], buf)
			if err != nil {
				t.Fatalf("Failed to encode type %v: %s", typs[i], err)
			}
		}
		// Decode the buffer.
		b := buf
		for i := range ed {
			if len(b) == 0 {
				t.Fatal("buffer ended early")
			}
			var decoded rowenc.EncDatum
			decoded, b, err = rowenc.EncDatumFromBuffer(typs[i], enc[i], b)
			if err != nil {
				t.Fatalf("%+v: encdatum from %+v: %+v (%+v)", ed[i].Datum, enc[i], err, typs[i])
			}
			err = decoded.EnsureDecoded(typs[i], &alloc)
			if err != nil {
				t.Fatalf("%+v: ensuredecoded: %v (%+v)", ed[i], err, typs[i])
			}
			if decoded.Datum.Compare(evalCtx, ed[i].Datum) != 0 {
				t.Errorf("decoded datum %+v doesn't equal original %+v", decoded.Datum, ed[i].Datum)
			}
		}
		if len(b) != 0 {
			t.Errorf("%d leftover bytes", len(b))
		}
	}
}

func TestEncDatumRowCompare(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [5]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		row1, row2 rowenc.EncDatumRow
		ord        colinfo.ColumnOrdering
		cmp        int
	}{
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord:  colinfo.ColumnOrdering{},
			cmp:  0,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord:  colinfo.ColumnOrdering{{ColIdx: 1, Direction: desc}},
			cmp:  0,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: desc},
			},
			cmp: 0,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord:  colinfo.ColumnOrdering{{ColIdx: 2, Direction: asc}},
			cmp:  -1,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[3]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[2]},
			ord:  colinfo.ColumnOrdering{{ColIdx: 2, Direction: asc}},
			cmp:  1,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 2, Direction: asc},
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: asc},
			},
			cmp: -1,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 2, Direction: desc},
			},
			cmp: 1,
		},
		{
			row1: rowenc.EncDatumRow{v[0], v[1], v[2]},
			row2: rowenc.EncDatumRow{v[0], v[1], v[3]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 1, Direction: desc},
				{ColIdx: 0, Direction: asc},
				{ColIdx: 2, Direction: desc},
			},
			cmp: 1,
		},
		{
			row1: rowenc.EncDatumRow{v[2], v[3], v[4]},
			row2: rowenc.EncDatumRow{v[1], v[3], v[0]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
			},
			cmp: 1,
		},
		{
			row1: rowenc.EncDatumRow{v[2], v[3], v[4]},
			row2: rowenc.EncDatumRow{v[1], v[3], v[0]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 1, Direction: desc},
				{ColIdx: 0, Direction: asc},
			},
			cmp: 1,
		},
		{
			row1: rowenc.EncDatumRow{v[2], v[3], v[4]},
			row2: rowenc.EncDatumRow{v[1], v[3], v[0]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 1, Direction: asc},
				{ColIdx: 0, Direction: asc},
			},
			cmp: 1,
		},
		{
			row1: rowenc.EncDatumRow{v[2], v[3], v[4]},
			row2: rowenc.EncDatumRow{v[1], v[3], v[0]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 1, Direction: asc},
				{ColIdx: 0, Direction: desc},
			},
			cmp: -1,
		},
		{
			row1: rowenc.EncDatumRow{v[2], v[3], v[4]},
			row2: rowenc.EncDatumRow{v[1], v[3], v[0]},
			ord: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: desc},
				{ColIdx: 1, Direction: asc},
			},
			cmp: -1,
		},
	}

	a := &rowenc.DatumAlloc{}
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	for _, c := range testCases {
		typs := make([]*types.T, len(c.row1))
		for i := range typs {
			typs[i] = types.Int
		}
		cmp, err := c.row1.Compare(typs, a, c.ord, evalCtx, c.row2)
		if err != nil {
			t.Error(err)
		} else if cmp != c.cmp {
			t.Errorf(
				"%s cmp %s ordering %v got %d, expected %d",
				c.row1.String(typs), c.row2.String(typs), c.ord, cmp, c.cmp,
			)
		}
	}
}

func TestEncDatumRowAlloc(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())
	rng, _ := randutil.NewPseudoRand()
	for _, cols := range []int{1, 2, 4, 10, 40, 100} {
		for _, rows := range []int{1, 2, 3, 5, 10, 20} {
			colTypes := randgen.RandColumnTypes(rng, cols)
			in := make(rowenc.EncDatumRows, rows)
			for i := 0; i < rows; i++ {
				in[i] = make(rowenc.EncDatumRow, cols)
				for j := 0; j < cols; j++ {
					datum := randgen.RandDatum(rng, colTypes[j], true /* nullOk */)
					in[i][j] = rowenc.DatumToEncDatum(colTypes[j], datum)
				}
			}
			var alloc rowenc.EncDatumRowAlloc
			out := make(rowenc.EncDatumRows, rows)
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
					if a, b := in[i][j].Datum, out[i][j].Datum; a.Compare(evalCtx, b) != 0 {
						t.Errorf("copied datum %s doesn't equal original %s", b, a)
					}
				}
			}
		}
	}
}

func TestValueEncodeDecodeTuple(t *testing.T) {
	rng, seed := randutil.NewPseudoRand()
	tests := make([]tree.Datum, 1000)
	colTypes := make([]*types.T, 1000)
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())

	for i := range tests {
		len := rng.Intn(5)
		contents := make([]*types.T, len)
		for j := range contents {
			contents[j] = randgen.RandEncodableType(rng)
		}
		colTypes[i] = types.MakeTuple(contents)
		tests[i] = randgen.RandDatum(rng, colTypes[i], true)
	}

	for i, test := range tests {

		switch typedTest := test.(type) {
		case *tree.DTuple:

			buf, err := rowenc.EncodeTableValue(nil, descpb.ColumnID(encoding.NoColumnID), typedTest, nil)
			if err != nil {
				t.Fatalf("seed %d: encoding tuple %v with types %v failed with error: %v",
					seed, test, colTypes[i], err)
			}
			var decodedTuple tree.Datum
			testTyp := test.ResolvedType()

			decodedTuple, buf, err = rowenc.DecodeTableValue(&rowenc.DatumAlloc{}, testTyp, buf)
			if err != nil {
				t.Fatalf("seed %d: decoding tuple %v with type (%+v, %+v) failed with error: %v",
					seed, test, colTypes[i], testTyp, err)
			}
			if len(buf) != 0 {
				t.Fatalf("seed %d: decoding tuple %v with type (%+v, %+v) left %d remaining bytes",
					seed, test, colTypes[i], testTyp, len(buf))
			}

			if cmp := decodedTuple.Compare(evalCtx, test); cmp != 0 {
				t.Fatalf("seed %d: encoded %+v, decoded %+v, expected equal, received comparison: %d", seed, test, decodedTuple, cmp)
			}
		default:
			if test == tree.DNull {
				continue
			}
			t.Fatalf("seed %d: non-null test case %v is not a tuple", seed, test)
		}
	}

}

func TestEncDatumSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		asc  = descpb.DatumEncoding_ASCENDING_KEY
		desc = descpb.DatumEncoding_DESCENDING_KEY

		DIntSize    = unsafe.Sizeof(tree.DInt(0))
		DFloatSize  = unsafe.Sizeof(tree.DFloat(0))
		DStringSize = unsafe.Sizeof(*tree.NewDString(""))
	)

	dec12300 := &tree.DDecimal{Decimal: *apd.New(123, 2)}
	decimalSize := dec12300.Size()

	testCases := []struct {
		encDatum     rowenc.EncDatum
		expectedSize uintptr
	}{
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeVarintAscending(nil, 0)),
			expectedSize: rowenc.EncDatumOverhead + 1, // 1 is encoded with length 1 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(desc, encoding.EncodeVarintDescending(nil, 123)),
			expectedSize: rowenc.EncDatumOverhead + 2, // 123 is encoded with length 2 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeVarintAscending(nil, 12345)),
			expectedSize: rowenc.EncDatumOverhead + 3, // 12345 is encoded with length 3 byte array
		},
		{
			encDatum:     rowenc.DatumToEncDatum(types.Int, tree.NewDInt(123)),
			expectedSize: rowenc.EncDatumOverhead + DIntSize,
		},
		{
			encDatum: encDatumFromEncodedWithDatum(
				asc,
				encoding.EncodeVarintAscending(nil, 123),
				tree.NewDInt(123),
			),
			expectedSize: rowenc.EncDatumOverhead + 2 + DIntSize, // 123 is encoded with length 2 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeFloatAscending(nil, 0)),
			expectedSize: rowenc.EncDatumOverhead + 1, // 0.0 is encoded with length 1 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(desc, encoding.EncodeFloatDescending(nil, 123)),
			expectedSize: rowenc.EncDatumOverhead + 9, // 123.0 is encoded with length 9 byte array
		},
		{
			encDatum:     rowenc.DatumToEncDatum(types.Float, tree.NewDFloat(123)),
			expectedSize: rowenc.EncDatumOverhead + DFloatSize,
		},
		{
			encDatum: encDatumFromEncodedWithDatum(
				asc,
				encoding.EncodeFloatAscending(nil, 123),
				tree.NewDFloat(123),
			),
			expectedSize: rowenc.EncDatumOverhead + 9 + DFloatSize, // 123.0 is encoded with length 9 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeDecimalAscending(nil, apd.New(0, 0))),
			expectedSize: rowenc.EncDatumOverhead + 1, // 0.0 is encoded with length 1 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(desc, encoding.EncodeDecimalDescending(nil, apd.New(123, 2))),
			expectedSize: rowenc.EncDatumOverhead + 4, // 123.0 is encoded with length 4 byte array
		},
		{
			encDatum:     rowenc.DatumToEncDatum(types.Decimal, dec12300),
			expectedSize: rowenc.EncDatumOverhead + decimalSize,
		},
		{
			encDatum: encDatumFromEncodedWithDatum(
				asc,
				encoding.EncodeDecimalAscending(nil, &dec12300.Decimal),
				dec12300,
			),
			expectedSize: rowenc.EncDatumOverhead + 4 + decimalSize,
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeStringAscending(nil, "")),
			expectedSize: rowenc.EncDatumOverhead + 3, // "" is encoded with length 3 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(desc, encoding.EncodeStringDescending(nil, "123⌘")),
			expectedSize: rowenc.EncDatumOverhead + 9, // "123⌘" is encoded with length 9 byte array
		},
		{
			encDatum:     rowenc.DatumToEncDatum(types.String, tree.NewDString("12")),
			expectedSize: rowenc.EncDatumOverhead + DStringSize + 2,
		},
		{
			encDatum: encDatumFromEncodedWithDatum(
				asc,
				encoding.EncodeStringAscending(nil, "1234"),
				tree.NewDString("12345"),
			),
			expectedSize: rowenc.EncDatumOverhead + 7 + DStringSize + 5, // "1234" is encoded with length 7 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeTimeAscending(nil, time.Date(2018, time.June, 26, 11, 50, 0, 0, time.FixedZone("EDT", 0)))),
			expectedSize: rowenc.EncDatumOverhead + 7, // This time is encoded with length 7 byte array
		},
		{
			encDatum:     rowenc.EncDatumFromEncoded(asc, encoding.EncodeTimeAscending(nil, time.Date(2018, time.June, 26, 11, 50, 12, 3456789, time.FixedZone("EDT", 0)))),
			expectedSize: rowenc.EncDatumOverhead + 10, // This time is encoded with length 10 byte array
		},
	}

	for _, c := range testCases {
		receivedSize := c.encDatum.Size()
		if receivedSize != c.expectedSize {
			t.Errorf("on %v\treceived %d, expected %d", c.encDatum, receivedSize, c.expectedSize)
		}
	}

	testRow := make(rowenc.EncDatumRow, len(testCases))
	expectedTotalSize := rowenc.EncDatumRowOverhead
	for idx, c := range testCases {
		testRow[idx] = c.encDatum
		expectedTotalSize += c.expectedSize
	}
	receivedTotalSize := testRow.Size()
	if receivedTotalSize != expectedTotalSize {
		t.Errorf("on %v\treceived %d, expected %d", testRow, receivedTotalSize, expectedTotalSize)
	}
}

// TestEncDatumFingerprintMemory sanity-checks the memory accounting in
// Fingerprint.
func TestEncDatumFingerprintMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		asc   = descpb.DatumEncoding_ASCENDING_KEY
		desc  = descpb.DatumEncoding_DESCENDING_KEY
		value = descpb.DatumEncoding_VALUE

		i = 123
		s = "abcde"
	)

	testCases := []struct {
		encDatum    rowenc.EncDatum
		typ         *types.T
		newMemUsage int64
	}{
		{
			encDatum: rowenc.EncDatumFromEncoded(asc, encoding.EncodeVarintAscending(nil, 0)),
			typ:      types.Int,
			// Fingerprint should reuse already existing encoded representation,
			// so it shouldn't use any new memory.
			newMemUsage: 0,
		},
		{
			encDatum:    rowenc.EncDatumFromEncoded(desc, encoding.EncodeVarintDescending(nil, i)),
			typ:         types.Int,
			newMemUsage: int64(unsafe.Sizeof(tree.DInt(i))),
		},
		{
			encDatum:    rowenc.EncDatumFromEncoded(value, encoding.EncodeBytesValue(nil, encoding.NoColumnID, []byte(s))),
			typ:         types.String,
			newMemUsage: int64(tree.NewDString(s).Size()),
		},
	}

	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)
	memAcc := evalCtx.Mon.MakeBoundAccount()
	defer memAcc.Close(ctx)
	var da rowenc.DatumAlloc
	for _, c := range testCases {
		memAcc.Clear(ctx)
		_, err := c.encDatum.Fingerprint(ctx, c.typ, &da, nil /* appendTo */, &memAcc)
		if err != nil {
			t.Fatal(err)
		}
		if memAcc.Used() != c.newMemUsage {
			t.Errorf("on %v\taccounted for %d, expected %d", c.encDatum, memAcc.Used(), c.newMemUsage)
		}
	}
}

func encDatumFromEncodedWithDatum(
	enc descpb.DatumEncoding, encoded []byte, datum tree.Datum,
) rowenc.EncDatum {
	encDatum := rowenc.EncDatumFromEncoded(enc, encoded)
	encDatum.Datum = datum
	return encDatum
}
