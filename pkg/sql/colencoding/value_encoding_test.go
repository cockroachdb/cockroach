// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colencoding

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldataext"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestDecodeTableValueToCol(t *testing.T) {
	rng, _ := randutil.NewPseudoRand()
	var (
		da           rowenc.DatumAlloc
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
		fmt.Println(datum)
		buf, err = rowenc.EncodeTableValue(buf, descpb.ColumnID(encoding.NoColumnID), datum, scratch)
		if err != nil {
			t.Fatal(err)
		}
	}
	batch := coldata.NewMemBatchWithCapacity(typs, 1 /* capacity */, coldataext.NewExtendedColumnFactory(nil /*evalCtx */))
	for i := 0; i < nCols; i++ {
		typeOffset, dataOffset, _, typ, err := encoding.DecodeValueTag(buf)
		fmt.Println(typ)
		if err != nil {
			t.Fatal(err)
		}
		buf, err = DecodeTableValueToCol(&da, batch.ColVec(i), 0 /* rowIdx */, typ,
			dataOffset, typs[i], buf[typeOffset:])
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
