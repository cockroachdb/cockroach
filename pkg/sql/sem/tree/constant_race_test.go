// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestConstantConcurrentTypeCheck verifies that calling ResolveAsType on a
// StrVal or NumVal concurrently does not race with reading a datum returned by
// a previous ResolveAsType call. This is a regression test for #166362, where
// the embedded resString field was written by ResolveAsType on one goroutine
// while another goroutine read through a *DString pointer previously returned
// (e.g. via a shared QueryCache memo).
func TestConstantConcurrentTypeCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext(nil /* resolver */)

	testCases := []struct {
		name   string
		expr   string
		typ    *types.T
		readFn func(tree.TypedExpr)
	}{
		{
			name: "string",
			expr: "'hello world'",
			typ:  types.String,
			readFn: func(e tree.TypedExpr) {
				d, ok := tree.AsDString(e)
				if !ok {
					panic("expected DString")
				}
				_ = string(d)
			},
		},
		{
			name: "int",
			expr: "42",
			typ:  types.Int,
			readFn: func(e tree.TypedExpr) {
				_ = tree.MustBeDInt(e)
			},
		},
		{
			name: "float",
			expr: "3.14",
			typ:  types.Float,
			readFn: func(e tree.TypedExpr) {
				_ = tree.MustBeDFloat(e)
			},
		},
		{
			name: "decimal",
			expr: "1.23456",
			typ:  types.Decimal,
			readFn: func(e tree.TypedExpr) {
				_ = tree.MustBeDDecimal(e)
			},
		},
		{
			name: "bytes",
			expr: "b'\\x01\\x02'",
			typ:  types.Bytes,
			readFn: func(e tree.TypedExpr) {
				_ = string(*e.(*tree.DBytes))
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.expr)
			require.NoError(t, err)

			// Initial type-check populates the embedded field and returns a
			// pointer into it (e.g. &StrVal.resString).
			typedExpr, err := tree.TypeCheck(ctx, expr, &semaCtx, tc.typ)
			require.NoError(t, err)

			readFn := tc.readFn
			const iters = 2000
			var wg sync.WaitGroup
			wg.Add(2)

			// One goroutine reads through the returned datum pointer, as
			// execution would via a shared QueryCache memo.
			go func() {
				defer wg.Done()
				for range iters {
					readFn(typedExpr)
				}
			}()

			// Another goroutine re-type-checks the same AST node, as
			// SetIndexRecommendations does via optbuilder.Build.
			var typeCheckErr error
			go func() {
				defer wg.Done()
				for range iters {
					if _, err := tree.TypeCheck(ctx, expr, &semaCtx, tc.typ); err != nil {
						typeCheckErr = err
						return
					}
				}
			}()

			wg.Wait()
			require.NoError(t, typeCheckErr)
		})
	}
}
