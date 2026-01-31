// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colconv_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/col/coldatatestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/colconv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func BenchmarkColVecToDatum(b *testing.B) {
	rng := randutil.NewTestRandWithSeed(-4365865412074131521)
	converted := make([]tree.Datum, coldata.BatchSize())

	for _, typ := range []*types.T{
		types.Bool,
		types.Int,
		types.Float,
		types.Decimal,
		types.Date,
		types.Bytes,
		types.Json,
		types.Uuid,
		types.Timestamp,
		types.TimestampTZ,
		types.Interval,
	} {
		vec := coldata.NewVec(typ, coldata.BatchSize(), coldata.StandardColumnFactory)
		coldatatestutils.RandomVec(coldatatestutils.RandomVecArgs{
			Rand:             rng,
			Vec:              vec,
			N:                coldata.BatchSize(),
			NullProbability:  0,
			BytesFixedLength: 16,
		})
		b.Run(typ.SQLString(), func(b *testing.B) {
			var da tree.DatumAlloc
			da.DefaultAllocSize = coldata.BatchSize()
			for b.Loop() {
				colconv.ColVecToDatum(converted, vec, coldata.BatchSize(), nil /* sel */, &da)
			}
		})
	}
}
