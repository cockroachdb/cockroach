// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestContextWrappedDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := &tree.EvalContext{}

	var d1 *ContextWrappedDatum
	var d2 tree.Datum
	d1 = &ContextWrappedDatum{
		Datum:   &tree.DJSON{JSON: json.FromString("string")},
		evalCtx: evalCtx,
	}
	d2 = &tree.DJSON{JSON: json.FromString("string")}

	// ContextWrappedDatum can be compared with regular datum.
	require.Equal(t, 0 /* expected */, d1.CompareDatum(d2))

	d2 = &ContextWrappedDatum{
		Datum:   d2,
		evalCtx: nil,
	}

	// ContextWrappedDatum can be compared with another ContextWrappedDatum.
	require.Equal(t, 0 /* expected */, d1.CompareDatum(d2))

	// ContextWrappedDatum implicitly views nil as tree.DNull.
	require.Equal(t, d1.CompareDatum(tree.DNull) /* expected */, d1.CompareDatum(nil /* other */))

	// ContextWrappedDatum panics if compared with incompatible type.
	d2 = tree.NewDString("s")
	require.Panics(t,
		func() { d1.CompareDatum(d2) },
		"Different datum type should cause panic when compared",
	)

	d2 = &ContextWrappedDatum{Datum: d2}
	require.Panics(t,
		func() { d1.CompareDatum(d2) },
		"Different datum type should cause panic when compared",
	)
}

func TestDatumVec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := &tree.EvalContext{}

	dv1 := NewDatumVec(0 /* n */, evalCtx)

	var expected tree.Datum
	expected = tree.NewDJSON(json.FromString("str1"))
	dv1.AppendVal(expected)
	require.Equal(t, 0 /* expected */, dv1.Get(0).Compare(evalCtx, expected))

	expected = tree.NewDJSON(json.FromString("str2"))
	dv1.AppendVal(expected)
	require.Equal(t, 0 /* expected */, dv1.Get(1).Compare(evalCtx, expected))
	require.Equal(t, 2, dv1.Len())

	invalidDatum, _ := tree.ParseDInt("10")
	require.Panics(
		t,
		func() { dv1.Set(0 /* i */, invalidDatum) },
		"should not be able to set datum of different type",
	)

	dv1 = NewDatumVec(0 /* n */, evalCtx)
	dv2 := NewDatumVec(0 /* n */, evalCtx)

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
			require.Equal(t, 0 /* expected */, dv1.Get(j).
				Compare(evalCtx, tree.NewDJSON(json.FromString("dv2 str"))))
		}
	}

	dv2 = NewDatumVec(0 /* n */, evalCtx)
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str1")))
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str2")))
	dv2.AppendVal(nil /* v */)
	dv2.AppendVal(tree.NewDJSON(json.FromString("dv2 str3")))

	dv1.AppendSlice(dv2, 1 /* destIdx */, 1 /* srcStartIdx */, 3 /* srcEndIdx */)
	require.Equal(t, 3 /* expected */, dv1.Len())
	require.Equal(t, 0 /* expected */, dv1.Get(0 /* i */).
		Compare(evalCtx, tree.NewDJSON(json.FromString("dv2 str"))))
	require.Equal(t, 0 /* expected */, dv1.Get(1 /* i */).
		Compare(evalCtx, tree.NewDJSON(json.FromString("dv2 str2"))))
	require.Equal(t, 0 /* expected */, dv1.Get(2 /* i */).
		Compare(evalCtx, tree.DNull))

	dv2 = NewDatumVec(0 /* n */, evalCtx)
	dv2.AppendVal(tree.NewDJSON(json.FromString("string0")))
	dv2.AppendVal(nil /* v */)
	dv2.AppendVal(tree.NewDJSON(json.FromString("string2")))

	dv1.CopySlice(dv2, 0 /* destIdx */, 0 /* srcStartIdx */, 3 /* srcEndIdx */)
	require.Equal(t, 3 /* expected */, dv1.Len())
	require.Equal(t, 0 /* expected */, dv1.Get(0 /* i */).
		Compare(evalCtx, tree.NewDJSON(json.FromString("string0"))))
	require.Equal(t, 0 /* expected */, dv1.Get(1 /* i */).
		Compare(evalCtx, tree.DNull))
	require.Equal(t, 0 /* expected */, dv1.Get(2 /* i */).
		Compare(evalCtx, tree.NewDJSON(json.FromString("string2"))))
}
