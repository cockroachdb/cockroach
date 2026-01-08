// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestCastIdentityRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var usesFamily func(typ *types.T, family types.Family) bool
	usesFamily = func(typ *types.T, family types.Family) bool {
		switch typ.Family() {
		case family:
			return true
		case types.ArrayFamily:
			return usesFamily(typ.ArrayContents(), family)
		case types.TupleFamily:
			for _, tupleType := range typ.TupleContents() {
				if usesFamily(tupleType, family) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}

	ctx := context.Background()
	castCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	rng, _ := randutil.NewTestRand()
	for trial := 0; trial < 100; trial++ {
		typ := randgen.RandType(rng)

		// Test eval context does not support the OidFamily
		if usesFamily(typ, types.OidFamily) {
			continue
		}

		value := randgen.RandDatum(rng, typ, true /* nullOk */)

		// A value should cast to its own type and be equal to itself after the
		// cast.
		identityCastValue, err := eval.PerformCast(ctx, &castCtx, value, typ)
		require.NoError(t, err)

		cmp, err := value.Compare(ctx, &castCtx, identityCastValue)
		require.NoError(t, err)
		require.Zero(t, cmp)

		// A value should cast to its canonical type and be equal to itself after
		// the cast.
		canonicalCastValue, err := eval.PerformCast(ctx, &castCtx, value, typ.Canonical())
		require.NoError(t, err)

		cmp, err = value.Compare(ctx, &castCtx, canonicalCastValue)
		require.NoError(t, err)
		require.Zero(t, cmp)
	}
}
