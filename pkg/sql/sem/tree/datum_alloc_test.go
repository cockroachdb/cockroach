// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestNilDatumAlloc verifies that nil tree.DatumAlloc value acts as a no-op
// allocator. Its main purpose is to prevent us from forgetting to add a nil
// check when adding a new type.
func TestNilDatumAlloc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	rng, _ := randutil.NewPseudoRand()
	var buf []byte
	var da *tree.DatumAlloc

	for i := 0; i < 100; i++ {
		typ := randgen.RandType(rng)
		d := randgen.RandDatum(rng, typ, false /* nullOk */)
		var err error
		buf, err = valueside.Encode(buf[:0], valueside.NoColumnID, d)
		require.NoError(t, err)
		decoded, _, err := valueside.Decode(da, typ, buf)
		require.NoError(t, err)
		cmp, err := d.Compare(ctx, &evalCtx, decoded)
		require.NoError(t, err)
		require.Equal(t, 0, cmp)
	}
}
