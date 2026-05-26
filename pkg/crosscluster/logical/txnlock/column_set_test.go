// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnlock

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
)

func TestColumnSet(t *testing.T) {
	rng := rand.New(rand.NewSource(0))

	randomTypes := func(numCols int) []*types.T {
		types := make([]*types.T, numCols)
		for i := range types {
			types[i] = randgen.RandColumnType(rng)
		}
		return types
	}

	randomRow := func(types []*types.T) tree.Datums {
		row := make(tree.Datums, len(types))
		for i, typ := range types {
			row[i] = randgen.RandDatum(rng, typ, false /* nullOk */)
		}
		return row
	}

	test := func(t *testing.T, types []*types.T) {
		ctx := context.Background()
		evalCtx := eval.Context{}
		colSet := columnSet{
			columns: make([]int32, len(types)),
		}
		for i := range len(types) {
			colSet.columns[i] = int32(i)
		}
		for range 1000 {
			a := randomRow(types)
			b := randomRow(types)

			hashA1, err := colSet.hash(ctx, a)
			require.NoError(t, err)
			hashA2, err := colSet.hash(ctx, a)
			require.NoError(t, err)
			if hashA1 != hashA2 {
				t.Errorf("hash(a) != hash(a)")
			}

			if !colSet.null(a) {
				eq, err := colSet.equal(ctx, &evalCtx, a, a)
				require.NoError(t, err)
				if !eq {
					t.Errorf("!equal(a, a) when !null(a)")
				}
			}

			eqAB, err := colSet.equal(ctx, &evalCtx, a, b)
			require.NoError(t, err)
			if eqAB {
				hashA, err := colSet.hash(ctx, a)
				require.NoError(t, err)
				hashB, err := colSet.hash(ctx, b)
				require.NoError(t, err)
				if hashA != hashB {
					t.Errorf("equal(a, b) but hash(a) != hash(b)")
				}
			}

			eqBA, err := colSet.equal(ctx, &evalCtx, b, a)
			require.NoError(t, err)
			if eqAB != eqBA {
				t.Errorf("equal(a, b) != equal(b, a)")
			}

			if (colSet.null(a) || colSet.null(b)) && eqAB {
				t.Errorf("null(a) or null(b) but equal(a, b) is true")
			}
		}
	}

	for range 100 {
		types := randomTypes(rand.Intn(5) + 1)
		t.Logf("types: %v", types)
		test(t, types)
	}
}
