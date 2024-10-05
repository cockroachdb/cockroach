// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package valueside

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

type arrayEncodingTest struct {
	name     string
	datum    tree.DArray
	encoding []byte
}

func TestArrayEncoding(t *testing.T) {
	tests := []arrayEncodingTest{
		{
			"empty int array",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{},
			},
			[]byte{1, 3, 0},
		}, {
			"single int array",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1)},
			},
			[]byte{1, 3, 1, 2},
		}, {
			"multiple int array",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1), tree.NewDInt(2), tree.NewDInt(3)},
			},
			[]byte{1, 3, 3, 2, 4, 6},
		}, {
			"string array",
			tree.DArray{
				ParamTyp: types.String,
				Array:    tree.Datums{tree.NewDString("foo"), tree.NewDString("bar"), tree.NewDString("baz")},
			},
			[]byte{1, 6, 3, 3, 102, 111, 111, 3, 98, 97, 114, 3, 98, 97, 122},
		}, {
			"name array",
			tree.DArray{
				ParamTyp: types.Name,
				Array:    tree.Datums{tree.NewDName("foo"), tree.NewDName("bar"), tree.NewDName("baz")},
			},
			[]byte{1, 6, 3, 3, 102, 111, 111, 3, 98, 97, 114, 3, 98, 97, 122},
		},
		{
			"bool array",
			tree.DArray{
				ParamTyp: types.Bool,
				Array:    tree.Datums{tree.MakeDBool(true), tree.MakeDBool(false)},
			},
			[]byte{1, 10, 2, 10, 11},
		}, {
			"array containing a single null",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.DNull},
				HasNulls: true,
			},
			[]byte{17, 3, 1, 1},
		}, {
			"array containing multiple nulls",
			tree.DArray{
				ParamTyp: types.Int,
				Array:    tree.Datums{tree.NewDInt(1), tree.DNull, tree.DNull},
				HasNulls: true,
			},
			[]byte{17, 3, 3, 6, 2},
		}, {
			"array whose NULL bitmap spans exactly one byte",
			tree.DArray{
				ParamTyp: types.Int,
				Array: tree.Datums{
					tree.NewDInt(1), tree.DNull, tree.DNull, tree.NewDInt(2), tree.NewDInt(3),
					tree.NewDInt(4), tree.NewDInt(5), tree.NewDInt(6),
				},
				HasNulls: true,
			},
			[]byte{17, 3, 8, 6, 2, 4, 6, 8, 10, 12},
		}, {
			"array whose NULL bitmap spans more than one byte",
			tree.DArray{
				ParamTyp: types.Int,
				Array: tree.Datums{
					tree.NewDInt(1), tree.DNull, tree.DNull, tree.NewDInt(2), tree.NewDInt(3),
					tree.NewDInt(4), tree.NewDInt(5), tree.NewDInt(6), tree.DNull,
				},
				HasNulls: true,
			},
			[]byte{17, 3, 9, 6, 1, 2, 4, 6, 8, 10, 12},
		},
	}

	for _, test := range tests {
		t.Run("encode "+test.name, func(t *testing.T) {
			enc, err := encodeArray(&test.datum, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(enc, test.encoding) {
				t.Fatalf("expected %s to encode to %v, got %v", test.datum.String(), test.encoding, enc)
			}
		})

		t.Run("decode "+test.name, func(t *testing.T) {
			d, _, err := decodeArray(&tree.DatumAlloc{}, types.MakeArray(test.datum.ParamTyp), test.encoding)
			hasNulls := d.(*tree.DArray).HasNulls
			if test.datum.HasNulls != hasNulls {
				t.Fatalf("expected %v to have HasNulls=%t, got %t", test.encoding, test.datum.HasNulls, hasNulls)
			}
			if err != nil {
				t.Fatal(err)
			}
			evalContext := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			if cmp, err := d.Compare(context.Background(), evalContext, &test.datum); err != nil {
				t.Fatal(err)
			} else if cmp != 0 {
				t.Fatalf("expected %v to decode to %s, got %s", test.encoding, test.datum.String(), d.String())
			}
		})
	}
}

func BenchmarkArrayEncoding(b *testing.B) {
	ary := tree.DArray{ParamTyp: types.Int, Array: tree.Datums{}}
	for i := 0; i < 10000; i++ {
		_ = ary.Append(tree.NewDInt(1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = encodeArray(&ary, nil)
	}
}
