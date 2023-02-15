// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"math"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestDatumPrevNext verifies that tree.DatumPrev and tree.DatumNext return
// datums that are smaller and larger, respectively, that the given datum if
// ok=true is returned (modulo some edge cases).
func TestDatumPrevNext(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	var evalCtx eval.Context
	const numRuns = 1000
	for i := 0; i < numRuns; i++ {
		typ := randgen.RandType(rng)
		d := randgen.RandDatum(rng, typ, false /* nullOk */)
		// Ignore NaNs and infinities.
		if f, ok := d.(*tree.DFloat); ok {
			if math.IsNaN(float64(*f)) || math.IsInf(float64(*f), 0) {
				continue
			}
		}
		if dec, ok := d.(*tree.DDecimal); ok {
			if dec.Form == apd.NaN || dec.Form == apd.Infinite {
				continue
			}
		}
		if !d.IsMin(&evalCtx) {
			if prev, ok := tree.DatumPrev(d, &evalCtx, &evalCtx.CollationEnv); ok {
				cmp, err := d.CompareError(&evalCtx, prev)
				require.NoError(t, err)
				require.True(t, cmp > 0, "d=%s, prev=%s, type=%s", d.String(), prev.String(), d.ResolvedType().SQLString())
			}
		}
		if !d.IsMax(&evalCtx) {
			if next, ok := tree.DatumNext(d, &evalCtx, &evalCtx.CollationEnv); ok {
				cmp, err := d.CompareError(&evalCtx, next)
				require.NoError(t, err)
				require.True(t, cmp < 0, "d=%s, next=%s, type=%s", d.String(), next.String(), d.ResolvedType().SQLString())
			}
		}
	}
}
