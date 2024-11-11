// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colencoding

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDecodeTableValueToCol(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	var (
		da           tree.DatumAlloc
		buf, scratch []byte
	)
	nCols := 1000
	datums := make([]tree.Datum, nCols)
	typs := make([]*types.T, nCols)
	for i := 0; i < nCols; i++ {
		ct := randgen.RandType(rng)
		datum := randgen.RandDatum(rng, ct, false /* nullOk */)
		typs[i] = ct
		datums[i] = datum
		var err error
		buf, scratch, err = valueside.EncodeWithScratch(buf, valueside.NoColumnID, datum, scratch[:0])
		if err != nil {
			t.Fatal(err)
		}
	}
	batch := coldata.NewMemBatchWithCapacity(typs, 1 /* capacity */, coldataext.NewExtendedColumnFactory(nil /*evalCtx */))
	var vecs coldata.TypedVecs
	vecs.SetBatch(batch)
	for i := 0; i < nCols; i++ {
		typeOffset, dataOffset, _, typ, err := encoding.DecodeValueTag(buf)
		if err != nil {
			t.Fatal(err)
		}
		buf, err = DecodeTableValueToCol(
			&da, &vecs, i /* vecIdx */, 0 /* rowIdx */, typ,
			dataOffset, typs[i], buf[typeOffset:],
		)
		if err != nil {
			t.Fatal(err)
		}

		// TODO(jordan): should actually compare the outputs as well, but this is a
		// decent enough smoke test.
	}

	if len(buf) != 0 {
		t.Fatalf("leftover bytes %s", buf)
	}
}
