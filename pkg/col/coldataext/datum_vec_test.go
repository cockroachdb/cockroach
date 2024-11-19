// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package coldataext

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDatumVec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	evalCtx := &eval.Context{}

	dv1 := newDatumVec(types.Jsonb, 0 /* n */, evalCtx)

	var expected coldata.Datum
	expected = tree.NewDJSON(json.FromString("str1"))
	dv1.AppendVal(expected)
	cmp, err := dv1.Get(0).(tree.Datum).Compare(ctx, evalCtx, expected.(tree.Datum))
	require.NoError(t, err)
	require.True(t, cmp == 0)

	expected = tree.NewDJSON(json.FromString("str2"))
	dv1.AppendVal(expected)
	cmp, err = dv1.Get(1).(tree.Datum).Compare(ctx, evalCtx, expected.(tree.Datum))
	require.NoError(t, err)
	require.True(t, cmp == 0)
	require.Equal(t, 2, dv1.Len())

	invalidDatum, _ := tree.ParseDInt("10")
	require.Panics(
		t,
		func() { dv1.Set(0 /* i */, invalidDatum) },
		"should not be able to set a datum of a different type",
	)

	dv1 = newDatumVec(types.Jsonb, 0 /* n */, evalCtx)
	dv2 := newDatumVec(types.Jsonb, 0 /* n */, evalCtx)

	dv1.AppendVal(tree.NewDJSON(json.FromString("str1")))
	dv1.AppendVal(tree.NewDJSON(json.FromString("str2")))

	// Truncating dv1.
	require.Equal(t, 2 /* expected */, dv1.Len())
	dv1.AppendSlice(dv2, 0 /* destIdx */, 0 /* srcStartIdx */, 0 /* srcEndIdx */)
	require.Equal(t, 0 /* expected */, dv1.Len())

	dv1.AppendVal(tree.NewDJSON(json.FromString("dv1 str")))
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str")))
	// Try appending dv2 to dv1 3 times. The first time will overwrite the
	// current present value in dv1.
	for i := 0; i < 3; i++ {
		dv1.AppendSlice(dv2, i, 0 /* srcStartIdx */, dv2.Len())
		require.Equal(t, i+1, dv1.Len())
		for j := 0; j <= i; j++ {
			cmp, err = dv1.Get(j).(tree.Datum).Compare(ctx, evalCtx, tree.NewDJSON(json.FromString("dv2 str")))
			require.NoError(t, err)
			require.True(t, cmp == 0)
		}
	}

	dv2 = newDatumVec(types.Jsonb, 0 /* n */, evalCtx)
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str1")))
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str2")))
	dv2.AppendVal(nil /* v */)
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str3")))

	dv1.AppendSlice(dv2, 1 /* destIdx */, 1 /* srcStartIdx */, 3 /* srcEndIdx */)
	require.Equal(t, 3 /* expected */, dv1.Len())
	cmp, err = dv1.Get(0).(tree.Datum).Compare(ctx, evalCtx, tree.NewDJSON(json.FromString("dv2 str")))
	require.NoError(t, err)
	require.True(t, cmp == 0)
	cmp, err = dv1.Get(1).(tree.Datum).Compare(ctx, evalCtx, tree.NewDJSON(json.FromString("dv2 str2")))
	require.NoError(t, err)
	require.True(t, cmp == 0)
	require.True(t, dv1.Get(2).(tree.Datum) == tree.DNull)

	dv2 = newDatumVec(types.Jsonb, 0 /* n */, evalCtx)
	dv2.AppendVal(tree.NewDJSON(json.FromString("string0")))
	dv2.AppendVal(nil /* v */)
	dv2.AppendVal(tree.NewDJSON(json.FromString("string2")))

	dv1.CopySlice(dv2, 0 /* destIdx */, 0 /* srcStartIdx */, 3 /* srcEndIdx */)
	require.Equal(t, 3 /* expected */, dv1.Len())
	cmp, err = dv1.Get(0).(tree.Datum).Compare(ctx, evalCtx, tree.NewDJSON(json.FromString("string0")))
	require.NoError(t, err)
	require.True(t, cmp == 0)
	require.True(t, dv1.Get(1).(tree.Datum) == tree.DNull)
	cmp, err = dv1.Get(2).(tree.Datum).Compare(ctx, evalCtx, tree.NewDJSON(json.FromString("string2")))
	require.NoError(t, err)
	require.True(t, cmp == 0)
}
